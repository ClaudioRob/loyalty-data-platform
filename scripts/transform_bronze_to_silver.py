import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, regexp_replace, to_timestamp
from pyspark.sql.types import DoubleType
from dotenv import load_dotenv

# 1. Carregar credenciais do .env
load_dotenv()
storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
storage_key = os.getenv("AZURE_STORAGE_KEY")

# 2. Iniciar a sessao Spark
spark = SparkSession.builder \
    .appName("Loyalty-Bronze-To-Silver") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:7.0.1") \
    .getOrCreate()

# 3. Configuracao de autenticacao Azure
h_conf = spark._jsc.hadoopConfiguration()
h_conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_key)
h_conf.set("fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem")

spark.sparkContext.setLogLevel("ERROR")

# 4. Caminhos
bronze_path = f"abfss://lake@{storage_account}.dfs.core.windows.net/bronze/transactions_20260331.csv"
silver_path = f"abfss://lake@{storage_account}.dfs.core.windows.net/silver/transactions_refined/"

print("Lendo dados da Camada Bronze...")

try:
    # Lemos o CSV (os nomes das colunas serao: tx_id, cust_id, tx_datetime, tx_amount, tx_category, etc.)
    df_bronze = spark.read.csv(bronze_path, header=True)
    
    # 5. Transformacao usando os nomes REAIS das colunas do seu CSV
    df_silver = df_bronze.withColumn(
        # Convertendo tx_amount para Double (tratando virgula)
        "amount", regexp_replace(col("tx_amount"), ",", ".").cast(DoubleType())
    ).withColumn(
        # Convertendo tx_datetime para Timestamp
        "timestamp", to_timestamp(col("tx_datetime"))
    ).withColumn(
        # Renomeando para manter o padrao da Silver
        "transaction_id", col("tx_id")
    ).withColumn(
        "customer_id", col("cust_id")
    ).withColumn(
        "category", col("tx_category")
    ).withColumn(
        "ingested_at", current_timestamp()
    )

    # Selecionamos apenas as colunas limpas para a Silver
    df_final = df_silver.select(
        "transaction_id", 
        "customer_id", 
        "amount", 
        "timestamp", 
        "category", 
        "ingested_at"
    ).filter(col("transaction_id").isNotNull())

    print("Gravando dados na Camada Silver em formato Parquet...")
    
    df_final.write.mode("overwrite").parquet(silver_path)
    
    print(f"SUCESSO! Dados salvos em: {silver_path}")
    df_final.show(5)

except Exception as e:
    print(f"Erro durante o processamento: {e}")

finally:
    spark.stop()