import os
import math
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lag, unix_timestamp, abs, when, lit, udf
from pyspark.sql.types import DoubleType
from dotenv import load_dotenv

# 1. Carregar variáveis do .env (Usando caminho absoluto para o Docker)
env_path = '/opt/airflow/loyalty-data-platform/.env'
load_dotenv(dotenv_path=env_path)

# 2. Configuração Spark com Autenticação Azure
spark = SparkSession.builder \
    .appName("Fraud-Detection-Impossible-Travel") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:7.0.1") \
    .getOrCreate()

# Extrair credenciais do ambiente
storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
storage_key = os.getenv("AZURE_STORAGE_KEY")

# Injetar chaves na configuração do Hadoop para permitir leitura ABFSS
sc = spark.sparkContext
sc._jsc.hadoopConfiguration().set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", 
    storage_key
)

# 3. UDF para Cálculo de Distância (Haversine)
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

# 4. Caminhos de entrada e saída
silver_path = f"abfss://lake@{storage_account}.dfs.core.windows.net/silver/transactions_refined/"
fraud_output_path = "/opt/airflow/loyalty-data-platform/data/gold/fraud_alerts.parquet"

try:
    print(f"🔍 Conectando à conta: {storage_account}")
    print("🔍 Lendo dados da Silver para análise de fraude...")
    
    df_silver = spark.read.parquet(silver_path)

    # 5. Lógica de Janela: Comparar transação atual com a anterior do mesmo cliente
    window_spec = Window.partitionBy("customer_id").orderBy("transaction_date")

    df_geo = df_silver.withColumn("prev_lat", lag("latitude").over(window_spec)) \
                      .withColumn("prev_lon", lag("longitude").over(window_spec)) \
                      .withColumn("prev_time", lag("transaction_date").over(window_spec))

    # 6. Cálculo de Distância e Tempo
    df_analysis = df_geo.withColumn("dist_km", 
        haversine_udf(col("latitude"), col("longitude"), col("prev_lat"), col("prev_lon"))) \
        .withColumn("time_diff_hours", 
            abs(unix_timestamp("transaction_date") - unix_timestamp("prev_time")) / 3600)

    # 7. Flag de Fraude: Se velocidade > 800 km/h (Avião comercial)
    df_final = df_analysis.withColumn("is_impossible_travel", 
        when((col("dist_km") > 0) & (col("time_diff_hours") > 0) & 
             ((col("dist_km") / col("time_diff_hours")) > 800), True).otherwise(False))

    # 8. Filtrar apenas alertas e salvar para o Dashboard
    fraud_alerts = df_final.filter(col("is_impossible_travel") == True)
    
    alert_count = fraud_alerts.count()
    print(f"⚠️ Alertas detectados: {alert_count}")
    
    # Salva na Gold local para o Streamlit ler
    fraud_alerts.write.mode("overwrite").parquet(fraud_output_path)
    
    if alert_count > 0:
        fraud_alerts.show()

except Exception as e:
    print(f"❌ Erro fatal no motor de fraude: {e}")
    raise e # Garante que o Airflow marque a Task como FAILED se houver erro
finally:
    spark.stop()