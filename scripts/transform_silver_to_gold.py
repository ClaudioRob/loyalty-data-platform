import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, round, current_date
from dotenv import load_dotenv

# 1. Carregar credenciais
load_dotenv()
storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
storage_key = os.getenv("AZURE_STORAGE_KEY")

# 2. Iniciar a sessao Spark
spark = SparkSession.builder \
    .appName("Loyalty-Silver-To-Gold") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:7.0.1") \
    .getOrCreate()

# 3. Configuracao Azure
h_conf = spark._jsc.hadoopConfiguration()
h_conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_key)
h_conf.set("fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem")

spark.sparkContext.setLogLevel("ERROR")

# 4. Caminhos das Camadas
silver_path = f"abfss://lake@{storage_account}.dfs.core.windows.net/silver/transactions_refined/"
gold_path = f"abfss://lake@{storage_account}.dfs.core.windows.net/gold/kpi_category_finance/"

print("Lendo dados consolidados da Camada Silver...")

try:
    # Leitura do Parquet (ja tipado e limpo)
    df_silver = spark.read.parquet(silver_path)

    # 5. Criacao do KPI: Faturamento e Volume por Categoria
    # Foco em KPIs hierarquicos como voce prefere
    df_gold = df_silver.groupBy("category") \
        .agg(
            round(sum("amount"), 2).alias("total_revenue"),
            count("transaction_id").alias("transaction_volume")
        ) \
        .withColumn("calculation_date", current_date()) \
        .orderBy(col("total_revenue").desc())

    print("Gravando KPIs na Camada Gold...")
    
    # Gravacao em Parquet para consumo de ferramentas de BI (Power BI/Streamlit)
    df_gold.write.mode("overwrite").parquet(gold_path)
    
    print(f"SUCESSO! KPIs gerados em: {gold_path}")
    df_gold.show()

except Exception as e:
    print(f"Erro na geracao da Camada Gold: {e}")

finally:
    spark.stop()