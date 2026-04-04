import sys
from pyspark.sql import SparkSession
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite

def run_quality_check():
    # 1. Inicializa Spark e Contexto GX
    spark = SparkSession.builder.getOrCreate()
    context = gx.get_context()
    
    print("--- Preparando dados para validação (GX 1.x) ---")
    
    # ---------------------------------------------------------
    # BLOCO DE TESTE LOCAL (MOCK)
    # ---------------------------------------------------------
    data = [
        ("TXN001", "CUST01", 100, "processed"), # Válido
        ("TXN002", "CUST02", -50, "processed"), # Erro: pontos negativos
        (None, "CUST03", 20, "pending"),        # Erro: ID nulo
        ("TXN001", "CUST04", 10, "cancelled")   # Erro: ID duplicado
    ]
    columns = ["transaction_id", "customer_id", "points_earned", "status"]
    df = spark.createDataFrame(data, columns)
    # ---------------------------------------------------------

    # 2. Configuração da infraestrutura GX
    datasource_name = "loyalty_datasource"
    asset_name = "bronze_transactions_asset"
    suite_name = "bronze_loyalty_suite"

    # Criar ou obter o Datasource Spark
    try:
        datasource = context.data_sources.add_spark(name=datasource_name)
    except Exception:
        datasource = context.data_sources.get(datasource_name)
    
    # Adicionar o Asset (na v1.x para Spark, usamos add_dataframe_asset)
    try:
        asset = datasource.add_dataframe_asset(name=asset_name)
    except Exception:
        asset = datasource.get_asset(asset_name)
    
    # 3. Criar a Suite e o Validator (Sintaxe correta para Batch v1.x)
    suite = ExpectationSuite(name=suite_name)
    
    # Na v1.x, passamos o dataframe diretamente no get_validator junto com o asset
    validator = context.get_validator(
        batch_request=asset.build_batch_request(options={"dataframe": df}),
        expectation_suite=suite
    )
    
    # 4. Aplicação das Regras (Expectations)
    print("--- Aplicando regras de qualidade ---")
    validator.expect_column_values_to_not_be_null("transaction_id")
    validator.expect_column_values_to_not_be_null("customer_id")
    validator.expect_column_values_to_be_unique("transaction_id")
    validator.expect_column_values_to_be_between("points_earned", min_value=0)
    
    allowed_status = ['processed', 'pending', 'cancelled']
    validator.expect_column_values_to_be_in_set("status", allowed_status)
    
    # 5. Avaliação dos Resultados
    results = validator.validate()
    
    # 6. Geração do Relatório HTML (Data Docs)
    context.build_data_docs()
    print(f"\n✅ Relatório visual gerado em: gx/uncommitted/data_docs/local_site/index.html")
    
    # 7. Lógica de saída
    if results["success"]:
        print("\n--- Sucesso! Todos os testes de qualidade passaram. ---")
        sys.exit(0)
    else:
        print("\n--- ❌ FALHA: Dados fora do padrão de qualidade detectados. ---")
        for res in results["results"]:
            if not res["success"]:
                column = res.expectation_config.kwargs.get('column')
                rule = res.expectation_config.type
                print(f"   - Falha na regra: {rule} | Coluna: {column}")
        
        sys.exit(1)

if __name__ == "__main__":
    run_quality_check()