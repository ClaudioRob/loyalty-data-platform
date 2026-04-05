import os
import sys
from pyspark.sql import SparkSession
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from dotenv import load_dotenv

def run_quality_check():
    # 1. Carregar credenciais (Caminho do container)
    env_path = '/opt/airflow/loyalty-data-platform/.env'
    load_dotenv(dotenv_path=env_path)

    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
    storage_key = os.getenv("AZURE_STORAGE_KEY")

    if not storage_account or not storage_key:
        print("❌ Erro: Variáveis de ambiente da Azure não encontradas no .env")
        sys.exit(1)

    # 2. SparkSession Ajustada (Ponto Crítico Corrigido)
    # Usando packages para evitar o ClassNotFoundException
    print("🚀 Iniciando Spark Session...")
    spark = SparkSession.builder \
        .appName("GX-Quality-Check-Bronze") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-azure:3.3.4") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.azure.account.key." + f"{storage_account}.dfs.core.windows.net", storage_key) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # 3. Leitura do Arquivo Bronze
        bronze_path = f"abfss://lake@{storage_account}.dfs.core.windows.net/bronze/transactions_20260331.csv"
        print(f"🔍 Conectando ao Data Lake: {storage_account}")
        print(f"📂 Validando arquivo: {bronze_path}")
        
        df = spark.read.csv(bronze_path, header=True, inferSchema=True)
        
        # 4. Configuração do Great Expectations
        context = gx.get_context()
        datasource_name = "loyalty_datasource"
        asset_name = "bronze_transactions_asset"
        suite_name = "bronze_loyalty_suite"

        try:
            datasource = context.data_sources.add_spark(name=datasource_name)
        except:
            datasource = context.data_sources.get(datasource_name)
        
        try:
            asset = datasource.add_dataframe_asset(name=asset_name)
        except:
            asset = datasource.get_asset(asset_name)
        
        suite = ExpectationSuite(name=suite_name)
        
        # 5. Obter o Validator
        validator = context.get_validator(
            batch_request=asset.build_batch_request(options={"dataframe": df}),
            expectation_suite=suite
        )
        
        # 6. Regras (Expectations)
        validator.expect_column_values_to_not_be_null("tx_id")
        validator.expect_column_values_to_not_be_null("cust_id")
        validator.expect_column_values_to_be_unique("tx_id")
        
        # 7. Avaliação
        results = validator.validate()
        
        if results["success"]:
            print("\n✅ Bronze aprovada! Seguindo para transformação...")
            return True
        else:
            print("\n❌ Bronze reprovada nos testes de qualidade.")
            sys.exit(1)

    except Exception as e:
        print(f"\n💥 Erro crítico durante o processamento: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()
        print("🛑 Spark Session encerrada.")

if __name__ == "__main__":
    run_quality_check()