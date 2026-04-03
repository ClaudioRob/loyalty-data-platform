from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
import os

# connections & variables
POSTGRES_CONN_ID = "postgres_conn"

# Define the paths to the files
USER_FILE_PATH = "/home/claudio/projetos/airflow-astro-python-sdk-workshop/dags/data/user/user_*.json"
SUBSCRIPTION_FILE_PATH = "/home/claudio/projetos/airflow-astro-python-sdk-workshop/dags/data/subscription/*.json"

# default args & init dag
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# declare dag
dag = DAG(
    dag_id="dag_local_files_to_postgres",
    start_date=days_ago(1),
    max_active_runs=1,
    schedule_interval=timedelta(hours=24),
    default_args=default_args,
    catchup=False,
)

with dag:
    # init & finish task
    init_data_load = DummyOperator(task_id="init")
    finish_data_load = DummyOperator(task_id="finish")

    # Load user file into PostgreSQL
    user_file = aql.load_file(
        task_id="user_file",
        input_file=File(
            path=USER_FILE_PATH,  # Absolute path
        ),
        output_table=Table(
            name="users",
            conn_id=POSTGRES_CONN_ID,
            metadata=Metadata(schema="public")
        ),
        if_exists="replace",
        use_native_support=True,
        columns_names_capitalization="original"
    )

    # Load subscription file into PostgreSQL
    subscription_file = aql.load_file(
        task_id="subscription_file",
        input_file=File(
            path=SUBSCRIPTION_FILE_PATH,  # Absolute path
        ),
        output_table=Table(
            name="subscriptions",
            conn_id=POSTGRES_CONN_ID,
            metadata=Metadata(schema="public")
        ),
        if_exists="replace",
        use_native_support=True,
        columns_names_capitalization="original"
    )

    # Define task sequence
    init_data_load >> [user_file, subscription_file] >> finish_data_load
