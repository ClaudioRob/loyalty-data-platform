# TODO always develop your DAGs using TaskFlowAPI
"""
Tasks performed by this DAG:
"""

# import libraries
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator


from astro import sql as aql
from astro.files import File
from astro.constants import FileType
from astro.sql.table import Table, Metadata

# connections & variables
SOURCE_CONN_ID = "google_cloud_default"
SNOWFLAKE_CONN_ID = "snowflake_default"
BIGQUERY_CONN_ID = "google_cloud_default"
POSTGRESS_CONN_ID = "postgres_conn"

# default args
default_args = {
    "owner": "claudio souza",
    "retries": 1,           # tentativas de excução após falha
    "retry_delay": 0        # tempo de espera para nova execução
}

# declare dag
@dag(
    dag_id="dag_load_gcp_warehouses",
    start_date=datetime(2024, 8, 8),       # data inicial da execução
    schedule_interval=timedelta(hours=24),  # intervalo de execução
    max_active_runs=1,                      # evitar nova execuçao caso esteja ativa
    catchup=False,                          # evitar que sejam reprocessados arquivos antigos
    default_args=default_args,
    # owner_links={"linkedin": "https://www.linkedin.com/in/luanmoreno/"},
    tags=['astrosdk', 'gcp', 'bigquery']
)

# declare main function
def load_files_warehouse():

    # init & finish
    init = EmptyOperator(task_id="init")
    finish = EmptyOperator(task_id="finish")

    # ingest from lake to snowflake
    users_json_files_postgres = aql.load_file(
        task_id="users_json_files_postgres",
        input_file=File(path="gs://crs-landing-zone/users", filetype=FileType.JSON, conn_id=SOURCE_CONN_ID),
        output_table=Table(name="users", metadata=Metadata(schema="CrsHQ"), conn_id=POSTGRESS_CONN_ID),
        if_exists="replace",
        # is_incremental=True,                # seleciona somente os novos arquivos
        use_native_support=True,
        columns_names_capitalization="original"
    )

    # ingest from lake to bigquery
    users_json_files_bigquery = aql.load_file(
        task_id="users_json_files_bigquery",
        input_file=File(path="gs://crs-landing-zone/users", filetype=FileType.JSON, conn_id=SOURCE_CONN_ID),
        output_table=Table(name="users", metadata=Metadata(schema="CrsHQ"), conn_id=BIGQUERY_CONN_ID),
        if_exists="replace",
        use_native_support=True,
        columns_names_capitalization="original"
    )

    # define sequence
    init >> [users_json_files_postgres, users_json_files_bigquery] >> finish

# init
dag = load_files_warehouse()