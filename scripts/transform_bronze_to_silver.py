import os
import sys
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
    raise ValueError(f"❌ Variável AZURE_STORAGE_ACCOUNT não encontrada.")

# 2. Mantenha a configuração de ambiente para carregar os JARs locais
# Caminhos absolutos dos JARs (verifique se esses arquivos existem no container)
JAR_REPO = "/opt/airflow/loyalty-data-platform/jars"
jars = [
    f"{JAR_REPO}/delta-core_2.12-3.0.0.jar",
    f"{JAR_REPO}/delta-storage-3.0.0.jar",
    f"{JAR_REPO}/hadoop-azure-3.3.4.jar",
    f"{JAR_REPO}/azure-storage-8.6.6.jar"
]

# A CHAVE DO SUCESSO: Configurar via SparkConf antes do SparkContext subir
spark = SparkSession.builder \
    .appName("Loyalty-Silver-Process") \
    .master("local[*]") \
    .config("spark.jars", ",".join(jars)) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.extraJavaOptions", "--add-opens=java.base/java.nio=ALL-UNNAMED") \
    .config("spark.hadoop.fs.azure.account.key.loyaltydatadl2026.dfs.core.windows.net", "SUA_CHAVE") \
    .getOrCreate()

print("🚀 JVM e Delta Lake carregados via spark.jars")

# 3. Configuracao Azure
h_conf = spark.sparkContext._jsc.hadoopConfiguration()
h_conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_key)
h_conf.set("fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem")

spark.sparkContext.setLogLevel("ERROR")

# 4. Caminhos
bronze_path = f"abfss://lake@{storage_account}.dfs.core.windows.net/bronze/transactions_20260331.csv"
silver_path = f"abfss://lake@{storage_account}.dfs.core.windows.net/silver/transactions_refined/"

print(f"🔍 Conectando à conta: {storage_account}")

try:
    df_bronze = spark.read.csv(bronze_path, header=True)
    
    # 5. Transformação e Padronização
    df_final = df_bronze.withColumn(
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
        "latitude", lit(None).cast(DoubleType()) 
    ).withColumn(
        "longitude", lit(None).cast(DoubleType()) 
    ).withColumn(
        "ingested_at", current_timestamp()
    ).select(
        "transaction_id", "customer_id", "amount", 
        "transaction_date", "category", "latitude", "longitude", "ingested_at"
    ).filter(col("transaction_id").isNotNull())

    # 6. Gravação em Delta (ou Parquet se preferir manter)
    print("🚀 Gravando na Camada Silver (Delta)...")
    
    # Se quiser manter Parquet, basta trocar .format("delta") por .parquet(silver_path)
    df_final.write.format("delta").mode("overwrite").save(silver_path)
    
    print(f"✅ SUCESSO! Silver atualizada.")
    df_final.show(5)

except Exception as e:
    print(f"❌ Erro: {e}")
    sys.exit(1)
finally:
    spark.stop()