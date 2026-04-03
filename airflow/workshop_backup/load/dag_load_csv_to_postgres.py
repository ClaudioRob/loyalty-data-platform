
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from datetime import datetime
from astro import sql as aql
from astro.files import File
from astro.table import Table
from astro.sql.table import Table, Metadata
import os

# Substitua pelos seus IDs de conexão
MINIO_CONN_ID = 'aws_default'
POSTGRES_CONN_ID = 'postgres_conn'

# Definição da DAG usando o Astro SDK
@dag(
    dag_id='dag_load_csv_to_postgres',
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["transfer", "minio", "postgres"]
)

def load_csv_to_postgres():
    
    # Definindo a origem do arquivo no MinIO (S3)
    minio_file = File(
        path="s3://airflow/resultados3km.csv",
        conn_id=MINIO_CONN_ID  # Este é o ID da conexão S3 configurado no Airflow
    )
    
    # Definindo a tabela de destino no PostgreSQL
    postgres_table = Table(
        name="resultados",
        conn_id=POSTGRES_CONN_ID,  # Este é o ID da conexão PostgreSQL configurado no Airflow
        metadata=Metadata(schema="corridas")
    )
    
    # Usando a função de transformação do Astro SDK para carregar o CSV no PostgreSQL
    aql.load_file(
        task_id="load_csv_to_postgres",
        input_file=minio_file,
        output_table=postgres_table,
        if_exists="replace",  # Substitui a tabela se já existir
        use_native_support=True,
        columns_names_capitalization="original"
    )

dag = load_csv_to_postgres()
