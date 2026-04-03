from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Centralizando o caminho para facilitar manutenções futuras
BASE_PATH = "/opt/airflow/loyalty-data-platform"

default_args = {
    'owner': 'claudio',
    'start_date': datetime(2026, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pipeline_loyalty_medallion',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['pyspark', 'loyalty', 'fraud']
) as dag:

    # Task 1: Refino (Bronze para Silver)
    t1 = BashOperator(
        task_id='transform_bronze_to_silver',
        bash_command=f'python3 {BASE_PATH}/scripts/transform_bronze_to_silver.py'
    )

    # Task Nova: Motor de Detecção de Fraude (Análise Geográfica)
    # Roda logo após a Silver estar pronta
    t_fraud = BashOperator(
        task_id='detect_fraud_analysis',
        bash_command=f'python3 {BASE_PATH}/scripts/data_quality_fraud.py'
    )

    # Task 2: Agregação de KPIs (Silver para Gold)
    t2 = BashOperator(
        task_id='transform_silver_to_gold',
        bash_command=f'python3 {BASE_PATH}/scripts/transform_silver_to_gold.py'
    )

    # Fluxo: Refina -> Detecta Fraude -> Gera KPIs de Negócio
    t1 >> t_fraud >> t2