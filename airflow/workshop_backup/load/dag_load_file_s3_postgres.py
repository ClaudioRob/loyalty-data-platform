# TODO always develop your DAGs using TaskFlowAPI
"""
Tasks
"""

# import libraries
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from datetime import date, datetime, timedelta
from airflow.decorators import dag, task
import pandas as pd


from airflow.operators.empty import EmptyOperator
from pandas import DataFrame
from astro import sql as aql
from astro.files import File
from astro.constants import FileType
from astro.sql.table import Table, Metadata

# from minio import Minio
# from minio.error import S3Error

# connections
MINIO_CONN_ID = "minio_conn"
OUTPUT_CONN_ID = "postgres_conn"

# default args & init dag
default_args = {
    "owner": "claudio Souza",
    "retries": 1,
    "retry_delay": 0
}

@dag(
    dag_id='dag_load_file_s3_postgres',
    start_date=datetime(2022, 2, 12),
    schedule_interval='@daily',
    max_active_runs=1,
    catchup=False,
    default_args=default_args,
    tags=['load', 'conn', 'minio']
)

# init main function
def load_file_minio_s3():

    # init & finish task
    init_data_load = EmptyOperator(task_id="init")
    finish_data_load = EmptyOperator(task_id="finish")

    # user
    user_file = aql.load_file(
        task_id="user_file",
        input_file=File(path="s3://landing/user/user*", filetype=FileType.JSON, conn_id=MINIO_CONN_ID),
        output_table=Table(name="user", conn_id=OUTPUT_CONN_ID, metadata=Metadata(schema="astro")),
        if_exists="replace",
        use_native_support=True,
        columns_names_capitalization="original"
    )

    # define sequence
    init_data_load >> [user_file] >> finish_data_load

load_file_minio_s3()

