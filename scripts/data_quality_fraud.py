import os
import math
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lag, unix_timestamp, abs, when, lit, udf
from pyspark.sql.types import DoubleType
from dotenv import load_dotenv

load_dotenv()

# 1. Configuração Spark (Mesma do seu ambiente unificado)
spark = SparkSession.builder \
    .appName("Fraud-Detection-Impossible-Travel") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:7.0.1") \
    .getOrCreate()

# 2. UDF para Cálculo de Distância (Haversine)
def haversine(lat1, lon1, lat2, lon2):
    if None in (lat1, lon1, lat2, lon2): return 0.0
    R = 6371 # Raio da Terra em km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * \
        math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

haversine_udf = udf(haversine, DoubleType())

# 3. Caminhos (Usando o padrão que unificamos)
storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
silver_path = f"abfss://lake@{storage_account}.dfs.core.windows.net/silver/transactions_refined/"
fraud_output_path = "/opt/airflow/loyalty-data-platform/data/gold/fraud_alerts.parquet"

try:
    print("🔍 Lendo dados da Silver para análise de fraude...")
    df_silver = spark.read.parquet(silver_path)

    # 4. Lógica de Janela: Comparar transação atual com a anterior do mesmo cliente
    window_spec = Window.partitionBy("customer_id").orderBy("transaction_date")

    df_geo = df_silver.withColumn("prev_lat", lag("latitude").over(window_spec)) \
                      .withColumn("prev_lon", lag("longitude").over(window_spec)) \
                      .withColumn("prev_time", lag("transaction_date").over(window_spec))

    # 5. Cálculo de Distância e Tempo
    df_analysis = df_geo.withColumn("dist_km", 
        haversine_udf(col("latitude"), col("longitude"), col("prev_lat"), col("prev_lon"))) \
        .withColumn("time_diff_hours", 
            abs(unix_timestamp("transaction_date") - unix_timestamp("prev_time")) / 3600)

    # 6. Flag de Fraude: Se velocidade > 800 km/h (Avião comercial)
    df_final = df_analysis.withColumn("is_impossible_travel", 
        when((col("dist_km") > 0) & (col("time_diff_hours") > 0) & 
             ((col("dist_km") / col("time_diff_hours")) > 800), True).otherwise(False))

    # 7. Filtrar apenas alertas e salvar para o Dashboard
    fraud_alerts = df_final.filter(col("is_impossible_travel") == True)
    
    print(f"⚠️ Alertas detectados: {fraud_alerts.count()}")
    fraud_alerts.write.mode("overwrite").parquet(fraud_output_path)
    
    fraud_alerts.show()

except Exception as e:
    print(f"❌ Erro no motor de fraude: {e}")
finally:
    spark.stop()