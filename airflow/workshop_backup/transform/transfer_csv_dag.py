from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine
import os
import boto3
import pandas as pd


# Configurações das conexões
MINIO_CONN_ID = "aws_default"
POSTGRES_CONN_ID = "postgres_conn"
BUCKET_NAME = "airflow"
CSV_FILE_NAME = "olist_public_dataset.cvs"
TABLE_NAME = "resultados"
SCHEMA_NAME = "corridas"

# Função para baixar o CSV do MinIO e carregar no PostgreSQL
def transfer_csv_to_postgres():
    # Conectar ao MinIO usando boto3
    s3_client = boto3.client("s3")

    # Baixar o arquivo CSV
    csv_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=CSV_FILE_NAME)
    csv_data = csv_obj['Body'].read().decode('utf-8')

    # Converter CSV para DataFrame
    df = pd.read_csv(io.StringIO(csv_data))

    # Conectar ao PostgreSQL usando SQLAlchemy
    engine = create_engine(f"postgresql+psycopg2://postgres:postgres@postgres:5432/{SCHEMA_NAME}")

    # Inserir o DataFrame na tabela do PostgreSQL
    df.to_sql(TABLE_NAME, engine, schema=SCHEMA_NAME, if_exists='replace', index=False)

    print(f"Arquivo {CSV_FILE_NAME} transferido com sucesso para a tabela {TABLE_NAME} no schema {SCHEMA_NAME}.")

# Definição da DAG
@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["transfer", "minio", "postgres"],
)
def transfer_csv_dag():
    transfer_task = PythonOperator(
        task_id="transfer_csv_to_postgres",
        python_callable=transfer_csv_to_postgres,
    )

dag = transfer_csv_dag()
