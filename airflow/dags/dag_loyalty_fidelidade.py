from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

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
    tags=['pyspark', 'loyalty']
) as dag:

    # Task 1: Bronze para Silver
    t1 = BashOperator(
        task_id='transform_bronze_to_silver',
        bash_command='python3 /opt/airflow/loyalty-data-platform/scripts/transform_bronze_to_silver.py'
    )

    # Task 2: Silver para Gold
    t2 = BashOperator(
        task_id='transform_silver_to_gold',
        bash_command='python3 /opt/airflow/loyalty-data-platform/scripts/transform_silver_to_gold.py'
    )

    t1 >> t2