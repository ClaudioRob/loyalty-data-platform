from airflow import DAG
from datetime import datetime
from astro import sql as aql
from astro.files import File
from astro.table import Table
from astro.sql.table import Table, Metadata

# Substitua pelos seus IDs de conexão
S3_CONN_ID = 'aws_default'
POSTGRES_CONN_ID = 'postgres_conn'

# Defina a DAG
with DAG(
    dag_id='load_minio_to_postgres',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Tarefa para carregar o arquivo do MinIO para o Postgres
    new_table = aql.load_file(
        task_id="s3_to_postgres_replace",
        input_file=File(path="s3://airflow/olist_public_dataset.csv", conn_id=S3_CONN_ID),
        output_table=Table(name='resultados', metadata=Metadata(schema="corridas"), conn_id=POSTGRES_CONN_ID),
        if_exists="replace",
        # is_increental=True,
        use_native_support=True,
        columns_names_capitalization="original"
        # encoding="latin1"  # ou "ISO-8859-1", dependendo da codificação do arquivo
        # encoding="utf-8",
        # encoding_errors="ignore"  # ou "replace" para substituir caracteres inválidos
    )

new_table