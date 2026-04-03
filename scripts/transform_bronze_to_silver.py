import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, regexp_replace, to_timestamp, lit
from pyspark.sql.types import DoubleType
from dotenv import load_dotenv

# 1. Carregar credenciais
env_path = '/opt/airflow/loyalty-data-platform/.env'
load_dotenv(dotenv_path=env_path)

storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
storage_key = os.getenv("AZURE_STORAGE_KEY")

if not storage_account:
    raise ValueError(f"❌ Variável AZURE_STORAGE_ACCOUNT não encontrada em: {env_path}")

# 2. Iniciar a sessao Spark
spark = SparkSession.builder \
    .appName("Loyalty-Bronze-To-Silver") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:7.0.1") \
    .getOrCreate()

# 3. Configuracao de autenticacao Azure
h_conf = spark.sparkContext._jsc.hadoopConfiguration()
h_conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_key)
h_conf.set("fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem")

spark.sparkContext.setLogLevel("ERROR")

# 4. Caminhos
bronze_path = f"abfss://lake@{storage_account}.dfs.core.windows.net/bronze/transactions_20260331.csv"
silver_path = f"abfss://lake@{storage_account}.dfs.core.windows.net/silver/transactions_refined/"

print(f"🔍 Conectando à conta: {storage_account}")
print("🔍 Lendo dados da Camada Bronze...")

try:
    # Lemos o CSV
    df_bronze = spark.read.csv(bronze_path, header=True)
    
    # 5. Transformação
    # AJUSTE: Criando latitude/longitude como nulas se não existirem no CSV
    df_silver = df_bronze.withColumn(
        "amount", regexp_replace(col("tx_amount"), ",", ".").cast(DoubleType())
    ).withColumn(
        "transaction_date", to_timestamp(col("tx_datetime"))
    ).withColumn(
        "transaction_id", col("tx_id")
    ).withColumn(
        "customer_id", col("cust_id")
    ).withColumn(
        "category", col("tx_category")
    ).withColumn(
        "latitude", lit(None).cast(DoubleType()) # Criando coluna fake para a Gold
    ).withColumn(
        "longitude", lit(None).cast(DoubleType()) # Criando coluna fake para a Gold
    ).withColumn(
        "ingested_at", current_timestamp()
    )

    # Seleção Final
    df_final = df_silver.select(
        "transaction_id", 
        "customer_id", 
        "amount", 
        "transaction_date", 
        "category",
        "latitude",
        "longitude",
        "ingested_at"
    ).filter(col("transaction_id").isNotNull())

    print("🚀 Gravando dados na Camada Silver em formato Parquet...")
    
    df_final.write.mode("overwrite").parquet(silver_path)
    
    print(f"✅ SUCESSO! Dados salvos em: {silver_path}")
    df_final.show(5)

except Exception as e:
    print(f"❌ Erro durante o processamento: {e}")
    raise e

finally:
    spark.stop()