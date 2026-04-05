from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Configurações de Robustez
default_args = {
    'owner': 'claudio_data_eng',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 31),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3, # Tenta 3 vezes antes de marcar como erro
    'retry_delay': timedelta(minutes=2), # Espera 2 min entre tentativas
}

with DAG(
    'pipeline_loyalty_medallion',
    default_args=default_args,
    description='Pipeline Medallion com Spark e Azure Delta Lake',
    schedule_interval='@daily', # Roda toda meia-noite
    catchup=False, # Não tenta rodar datas passadas retroativamente
    tags=['loyalty', 'pyspark', 'delta'],
) as dag:

    # 1. Instalação (Garante que o ambiente está pronto)
    t1 = BashOperator(
        task_id='install_dependencies',
        bash_command='pip install python-dotenv pyspark delta-spark great_expectations'
    )

    # 2. Qualidade na Bronze
    t2 = BashOperator(
        task_id='quality_check_bronze',
        bash_command='python3 /opt/airflow/loyalty-data-platform/scripts/spark/quality_check_bronze.py'
    )

    # 3. Bronze para Silver
    t3 = BashOperator(
        task_id='transform_bronze_to_silver',
        bash_command='python3 /opt/airflow/loyalty-data-platform/scripts/spark/transform_bronze_to_silver.py'
    )

    # 4. Análise de Fraude (Silver para Gold)
    t4 = BashOperator(
        task_id='detect_fraud_analysis',
        bash_command='python3 /opt/airflow/loyalty-data-platform/scripts/spark/detect_fraud_analysis.py'
    )

    # Fluxo de Dependência
    t1 >> t2 >> t3 >> t4