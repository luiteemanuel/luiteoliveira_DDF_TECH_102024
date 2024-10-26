from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from io import StringIO
from datetime import datetime
import boto3
import pandas as pd

s3 = boto3.client(
    's3',
    aws_access_key_id='key  aqui',
    aws_secret_access_key='chave aqui',
    region_name='us-east-1'
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def load_sandbox():
    bucket_name = 'datalake-dadosfera'  
    file_keys = [
        'curatedzone/dim_vendedor.csv',
        'curatedzone/fato_vendas.csv',
        'curatedzone/venda_total.csv',
        'curatedzone/vendas_por_marca.csv',
        'curatedzone/vendas_por_vendedor.csv'
    ]

    for file_key in file_keys:
        csv_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        body = csv_obj['Body']
        csv_string = body.read().decode('utf-8')
        csv_data = StringIO(csv_string)
        df = pd.read_csv(csv_data)

        # Enviar para o S3
        s3.put_object(Bucket=bucket_name, Key=f'analyticssandbox/{file_key.split("/")[-1]}', Body=csv_string)

        print(f'Dado {file_key} enviado para o S3')

with DAG('load_analytics_sandbox',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:

    load_sandbox = PythonOperator(
        task_id='load_sandbox',
        python_callable=load_sandbox
    )

load_sandbox
