import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, regexp_replace, to_timestamp, lit
from pyspark.sql.types import DoubleType
from dotenv import load_dotenv

def run_transformation():
    # 1. Carregar credenciais (Path do container)
    env_path = '/opt/airflow/loyalty-data-platform/.env'
    load_dotenv(dotenv_path=env_path)

    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
    storage_key = os.getenv("AZURE_STORAGE_KEY")

    if not storage_account or not storage_key:
        print("❌ Erro: Variáveis de ambiente da Azure não encontradas no .env")
        sys.exit(1)

    # 2. SparkSession com Auto-Download de dependências (Maven)
    # Isso resolve o ClassNotFoundException: io.delta.sql.DeltaSparkSessionExtension
    print("🚀 Iniciando Spark Session para Transformação Silver...")
    
    spark = SparkSession.builder \
        .appName("Loyalty-Medallion-Silver") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-azure:3.3.4") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .config("spark.hadoop.fs.azure.account.key." + f"{storage_account}.dfs.core.windows.net", storage_key) \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print("✅ SparkSession iniciada com Delta Lake!")

    # 3. Caminhos do Data Lake
    bronze_path = f"abfss://lake@{storage_account}.dfs.core.windows.net/bronze/transactions_20260331.csv"
    silver_path = f"abfss://lake@{storage_account}.dfs.core.windows.net/silver/transactions_refined/"

    print(f"🔍 Conectando à conta: {storage_account}")

    try:
        # 4. Leitura da Camada Bronze
        print(f"📂 Lendo Bronze: {bronze_path}")
        df_bronze = spark.read.csv(bronze_path, header=True)
        
        # 5. Transformação e Padronização (Camada Silver)
        # Convertendo valores, datas e renomeando para o padrão de negócio
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

        # 6. Gravação em Formato Delta (Refined)
        print(f"🚀 Gravando na Camada Silver (Delta) em: {silver_path}")
        
        df_final.write.format("delta").mode("overwrite").save(silver_path)
        
        print(f"✅ SUCESSO! Camada Silver atualizada.")
        df_final.show(5)

    except Exception as e:
        print(f"❌ Erro crítico durante a transformação: {str(e)}")
        sys.exit(1)
    finally:
        print("🛑 Encerrando sessão Spark.")
        spark.stop()

if __name__ == "__main__":
    run_transformation()