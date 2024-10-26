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


def transform_staging():
    bucket_name = 'datalake-dadosfera'  
    file_key = 'standardizedzone/car_sales_data_standardized.csv'  

    csv_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')

    # Converter o CSV em um DataFrame do pandas
    csv_data = StringIO(csv_string)
    df = pd.read_csv(csv_data)


    # Remover linhas onde o valor da venda é menor ou igual a zero
    df = df[df['Valor_Venda'] > 0]

    # Remover espaços extras e padronizar para maiúsculas
    df['Vendedor_Responsavel'] = df['Vendedor_Responsavel'].str.strip().str.upper()
    df['Marca_Veiculo'] = df['Marca_Veiculo'].str.strip().str.upper()

    # Remover linhas onde a data de fabricação é posterior à data de venda
    df = df[df['Ano_Fabricacao'] <= df['Data_Venda']]

    # Gerar o CSV a partir do DataFrame
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False) 

    # Subir o CSV para o S3 na pasta stagingzone
    s3.put_object(Bucket=bucket_name, Key='stagingzone/car_sales_data_staging.csv', Body=csv_buffer.getvalue())

with DAG('transformar_staging', default_args=default_args, schedule_interval='@daily') as dag:
            
            transformar_staging = PythonOperator(
                task_id='transformar_staging',
                python_callable=transform_staging
            )   
transformar_staging