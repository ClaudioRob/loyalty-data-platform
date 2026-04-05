import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# --- CONFIGURAÇÕES DE AMBIENTE ---
BASE_PATH = "/opt/airflow/loyalty-data-platform"
JAR_PATH = f"{BASE_PATH}/jars"
PYTHON_EXEC = "python3"

# Listagem dos JARs necessários (Certifique-se de que o azure-storage está na pasta)
jars_list = [
    f"{JAR_PATH}/delta-core_2.12-3.0.0.jar",
    f"{JAR_PATH}/delta-storage-3.0.0.jar",
    f"{JAR_PATH}/hadoop-azure-3.3.4.jar",
    f"{JAR_PATH}/azure-storage-8.6.6.jar" # Driver essencial para o erro de ClassNotFound do Azure
]

# Formatação do Classpath para o Java (separado por ":")
jars_cp = ":".join(jars_list)

# --- ARGUMENTOS PADRÃO ---
default_args = {
    'owner': 'claudio',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- DEFINIÇÃO DA DAG ---
with DAG(
    'pipeline_loyalty_medallion',
    default_args=default_args,
    description='Pipeline Medallion para o Loyalty Data Platform',
    schedule_interval=None,
    catchup=False,
    tags=['pyspark', 'loyalty', 'delta', 'azure']
) as dag:

    # 1. SETUP: Instalação de bibliotecas no worker (evita ModuleNotFoundError)
    t_setup = BashOperator(
        task_id='install_dependencies',
        bash_command='pip install great-expectations python-dotenv pyspark'
    )

    # 2. BRONZE QUALITY: Validação dos dados brutos
    t0_quality = BashOperator(
        task_id='quality_check_bronze',
        bash_command=f'{PYTHON_EXEC} {BASE_PATH}/scripts/spark/quality_check_bronze.py'
    )

    # 3. SILVER: Transformação e Refino (Injeção de JARs via Variáveis de Ambiente)
    t1_silver = BashOperator(
        task_id='transform_bronze_to_silver',
        bash_command=f'{PYTHON_EXEC} {BASE_PATH}/scripts/transform_bronze_to_silver.py'
    )

    # 4. FRAUD: Análise de Fraude e Qualidade na Silver
    t_fraud = BashOperator(
        task_id='detect_fraud_analysis',
        bash_command=f'{PYTHON_EXEC} {BASE_PATH}/scripts/data_quality_fraud.py'
    )

    # 5. GOLD: Agregação de KPIs para Dashboards
    t2_gold = BashOperator(
        task_id='transform_silver_to_gold',
        bash_command=f'{PYTHON_EXEC} {BASE_PATH}/scripts/transform_silver_to_gold.py'
    )

    # --- FLUXO DE EXECUÇÃO ---
    t_setup >> t0_quality >> t1_silver >> t_fraud >> t2_gold