import os
import sys
from pyspark.sql import SparkSession
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from dotenv import load_dotenv

def run_quality_check():
    # 1. Carregar credenciais e iniciar Spark
    env_path = '/home/claudio/projetos/loyalty-data-platform/.env'
    load_dotenv(dotenv_path=env_path)

    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
    storage_key = os.getenv("AZURE_STORAGE_KEY")

    spark = SparkSession.builder \
        .appName("GX-Quality-Check-Bronze") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:7.0.1") \
        .getOrCreate()

    # Configuração de autenticação Azure
    h_conf = spark.sparkContext._jsc.hadoopConfiguration()
    h_conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_key)
    
    context = gx.get_context()
    
    # 2. Leitura do Arquivo Real da Bronze (CSV)
    bronze_path = f"abfss://lake@{storage_account}.dfs.core.windows.net/bronze/transactions_20260331.csv"
    
    print(f"🔍 Validando arquivo: {bronze_path}")
    
    try:
        # Lendo o CSV real para validar
        df = spark.read.csv(bronze_path, header=True)
        
        # 3. Configuração da infraestrutura GX
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
        
        # 4. Obter o Validator
        validator = context.get_validator(
            batch_request=asset.build_batch_request(options={"dataframe": df}),
            expectation_suite=suite
        )
        
        # 5. Aplicação das Regras (Expectations ajustadas para o CSV)
        # tx_id, cust_id, tx_amount são os nomes no seu CSV original
        validator.expect_column_values_to_not_be_null("tx_id")
        validator.expect_column_values_to_not_be_null("cust_id")
        validator.expect_column_values_to_be_unique("tx_id")
        
        # 6. Avaliação
        results = validator.validate()
        
        if results["success"]:
            print("\n✅ Bronze aprovada! Seguindo para transformação...")
            sys.exit(0)
        else:
            print("\n❌ Bronze reprovada nos testes de qualidade.")
            sys.exit(1)

    except Exception as e:
        print(f"❌ Erro ao acessar Azure: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    run_quality_check()