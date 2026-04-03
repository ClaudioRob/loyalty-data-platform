import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Carrega as credenciais do .env (Azure e Paths)
load_dotenv()

def check_nulls(df, columns):
    """Verifica se existem nulos em colunas críticas."""
    for column in columns:
        null_count = df.filter(col(column).isNull()).count()
        if null_count > 0:
            return False, f"Coluna {column} contém {null_count} valores nulos."
    return True, "Check de Nulos: OK"

def check_duplicates(df, primary_key):
    """Verifica se existem IDs duplicados."""
    total_count = df.count()
    distinct_count = df.dropDuplicates([primary_key]).count()
    if total_count != distinct_count:
        return False, f"Encontrados {total_count - distinct_count} registos duplicados."
    return True, "Check de Duplicados: OK"

def run_data_quality():
    # Inicializa a Spark Session
    spark = SparkSession.builder \
        .appName("DataQualityGatekeeper") \
        .getOrCreate()

    # Recupera o caminho da camada Silver do .env
    # Ex: abfss://silver@seudatalake.dfs.core.windows.net/transactions/
    path_silver = os.getenv("SILVER_PATH")
    
    if not path_silver:
        raise ValueError("ERRO: Variável SILVER_PATH não encontrada no arquivo .env")

    print(f"--- Iniciando Validação de Qualidade: {path_silver} ---")
    
    # Lendo os dados da Silver (Parquet)
    df_silver = spark.read.parquet(path_silver)
    
    # 1. Executa Check de Nulos em campos essenciais para o negócio
    status_null, msg_null = check_nulls(df_silver, ["tx_id", "customer_id", "tx_amount"])
    if not status_null: 
        raise ValueError(f"❌ Falha de Integridade: {msg_null}")
    
    # 2. Executa Check de Duplicados na Chave Primária da transação
    status_dup, msg_dup = check_duplicates(df_silver, "tx_id")
    if not status_dup: 
        raise ValueError(f"❌ Falha de Unicidade: {msg_dup}")
    
    print("✅ SUCESSO: Todos os testes de qualidade passaram. Liberando para Camada Gold.")

if __name__ == "__main__":
    try:
        run_data_quality()
    except Exception as e:
        print(f"🛑 PIPELINE INTERROMPIDO: {str(e)}")
        exit(1) # Força o Airflow a marcar a Task como FAILED