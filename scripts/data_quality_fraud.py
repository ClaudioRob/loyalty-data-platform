import os
import math
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lag, unix_timestamp, abs, when, lit, udf, round
from pyspark.sql.types import DoubleType
from dotenv import load_dotenv

# 1. Carregar variáveis do .env
env_path = '/opt/airflow/loyalty-data-platform/.env'
load_dotenv(dotenv_path=env_path)

storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
storage_key = os.getenv("AZURE_STORAGE_KEY")

# 2. Configuração Spark
spark = SparkSession.builder \
    .appName("Fraud-Detection-Gold-Azure") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:7.0.1") \
    .config(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_key) \
    .config("spark.hadoop.fs.azure.enable.append.support", "true") \
    .getOrCreate()

# 3. UDF para Cálculo de Distância (Haversine)
@udf(returnType=DoubleType())
def haversine_udf(lat1, lon1, lat2, lon2):
    if None in (lat1, lon1, lat2, lon2): return 0.0
    R = 6371 
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * \
        math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

# 4. Caminhos de entrada e saída
container_name = "lake"
silver_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/silver/transactions_refined/"
gold_fraud_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/gold/fraud_alerts/"

try:
    print(f"🚀 Iniciando processamento Gold no Azure: {storage_account}")
    
    df_silver = spark.read.parquet(silver_path)

    # 5. Definição da Janela e Captura de Dados Anteriores
    window_spec = Window.partitionBy("customer_id").orderBy("transaction_date")

    df_lagged = df_silver.withColumn("current_time_unix", unix_timestamp("transaction_date")) \
                         .withColumn("prev_lat", lag("latitude").over(window_spec)) \
                         .withColumn("prev_lon", lag("longitude").over(window_spec)) \
                         .withColumn("prev_time_unix", lag(unix_timestamp("transaction_date")).over(window_spec))

    # 6. Cálculos de Diferença
    df_metrics = df_lagged.withColumn("dist_km", 
        haversine_udf(col("latitude"), col("longitude"), col("prev_lat"), col("prev_lon"))) \
        .withColumn("diff_seconds", 
            col("current_time_unix") - col("prev_time_unix")) \
        .withColumn("time_diff_hours", 
            abs(col("diff_seconds")) / 3600.0)

    # 7. Regras de Fraude
    df_fraud_flagged = df_metrics.withColumn("is_impossible_travel", 
        when((col("dist_km") > 0) & (col("time_diff_hours") > 0) & 
             ((col("dist_km") / col("time_diff_hours")) > 800), True).otherwise(False)) \
        .withColumn("is_velocity_fraud", 
        when((col("diff_seconds") > 0) & (col("diff_seconds") < 60), True).otherwise(False))

    # 8. Consolidação
    fraud_alerts = df_fraud_flagged.filter(
        (col("is_impossible_travel") == True) | (col("is_velocity_fraud") == True)
    ).withColumn("fraud_reason", 
        when(col("is_impossible_travel") & col("is_velocity_fraud"), "IMPOSSIBLE_TRAVEL_AND_VELOCITY")
        .when(col("is_impossible_travel"), "IMPOSSIBLE_TRAVEL")
        .when(col("is_velocity_fraud"), "HIGH_VELOCITY")
        .otherwise("UNKNOWN")
    )

    # 10. Criar Artefato para Dashboard (Flat File)
    dashboard_artifact_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/gold/dashboard_fraud_metrics/"

    print(f"📊 Gerando artefato de BI em: {dashboard_artifact_path}")

    # Selecionamos apenas o necessário e convertemos tipos se precisar
    fraud_alerts.select(
        col("customer_id"),
        col("transaction_date"),
        col("amount").cast("double"),
        col("category"),
        col("latitude"),
        col("longitude"),
        F.round(col("dist_km"), 2).alias("distancia_km"), # Use F.round para garantir
        F.round(col("diff_seconds") / 60, 2).alias("tempo_minutos"),
        col("fraud_reason")
    ).write.mode("overwrite").parquet(dashboard_artifact_path)
    
    alert_count = fraud_alerts.count()
    print(f"⚠️ Alertas detectados: {alert_count}")
    
    # 9. Persistência Final (Removida a coluna location_city que causava o erro)
    print(f"💾 Gravando Parquet em: {gold_fraud_path}")
    
    fraud_alerts.select(
        "customer_id", "transaction_date", "amount", "category",
        "latitude", "longitude", "dist_km", "diff_seconds", "fraud_reason"
    ).write.mode("overwrite").parquet(gold_fraud_path)
    
    if alert_count > 0:
        fraud_alerts.show(5)

except Exception as e:
    print(f"❌ Erro fatal: {e}")
    raise e 
finally:
    spark.stop()