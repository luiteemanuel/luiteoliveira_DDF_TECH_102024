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

def load_dataset():
    bucket_name = 'datalake-dadosfera'
    # boto3 usará as credenciais configuradas no AWS CLI
    s3 = boto3.client('s3')

    car_sales = pd.read_csv('data/car_sales_data.csv')
    car_sales.to_csv('data/car_sales_data.csv', index=False)
    csv_buffer = StringIO()
    car_sales.to_csv(csv_buffer, index=False)

    # Enviar para o S3
    s3.put_object(Bucket=bucket_name, Key='landingzone/car_sales_data.csv', Body=csv_buffer.getvalue())

    print('Dado enviado para o S3')


with DAG('load_landingzone', default_args=default_args, schedule_interval='@daily') as dag:

    load_landingzone = PythonOperator(
        task_id='load_dataset',
        python_callable=load_dataset
    )

load_landingzone
