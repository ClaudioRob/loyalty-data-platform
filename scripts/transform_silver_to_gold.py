import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, round, current_date
from dotenv import load_dotenv

# 1. Carregar credenciais (caso precise ler a Silver do Azure)
load_dotenv()
storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
storage_key = os.getenv("AZURE_STORAGE_KEY")

# 2. Iniciar a sessão Spark
spark = SparkSession.builder \
    .appName("Loyalty-Silver-To-Gold") \
    .config("spark.hadoop.fs.permissions.umask-mode", "000") \
    .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:7.0.1") \
    .getOrCreate()

# 3. Configuração Hadoop para Azure (se sua Silver estiver lá)
h_conf = spark._jsc.hadoopConfiguration()
if storage_account and storage_key:
    h_conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_key)
    h_conf.set("fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem")

spark.sparkContext.setLogLevel("ERROR")

# 4. Caminhos das Camadas
# Se sua Silver estiver no Azure, mantenha o abfss. Se estiver local, mude para o path /opt/airflow/...
silver_path = f"abfss://lake@{storage_account}.dfs.core.windows.net/silver/transactions_refined/"

# CAMINHO CRÍTICO: Salvando localmente para o Streamlit ler via volume mapeado
gold_path = "/opt/airflow/loyalty-data-platform/data/gold/kpi_category_finance.parquet"

print(f"🚀 Iniciando processamento da Gold...")

try:
    # Leitura da Silver
    df_silver = spark.read.parquet(silver_path)

    # 5. Transformação: Agregação de KPIs por Categoria
    # Criamos 'category' e 'total_revenue' para bater com o seu dashboard
    df_gold = df_silver.groupBy("category") \
        .agg(
            round(sum("amount"), 2).alias("total_revenue"),
            count("transaction_id").alias("transaction_volume")
        ) \
        .withColumn("calculation_date", current_date()) \
        .orderBy(col("total_revenue").desc())

    print(f"✅ KPIs gerados. Gravando em: {gold_path}")
    
    # Gravação Local (sobrescreve a cada run da DAG)
    df_gold.write.mode("overwrite").parquet(gold_path)
    
    # Print para conferência no log do Airflow
    df_gold.show()

except Exception as e:
    print(f"❌ Erro na geração da Camada Gold: {e}")
    raise e # Garante que a Task do Airflow fique vermelha em caso de erro

finally:
    spark.stop()