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


def transform_standard():
    bucket_name = 'datalake-dadosfera'  
    file_key = 'landingzone/car_sales_data.csv'  

    csv_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')

    # Converter o CSV em um DataFrame do pandas
    csv_data = StringIO(csv_string)
    df = pd.read_csv(csv_data)

    # renomear as colunas
    df.rename(columns={'Date': 'Data_Venda', 'Salesperson': 'Vendedor_Responsavel', 'Customer Name': 'Nome_Cliente',
                    'Car Make': 'Marca_Veiculo', 'Car Model':'Modelo_Veiculo', 'Car Year': 'Ano_Fabricacao', 'Sale Price':'Valor_Venda',
                     'Commission Rate': 'Taxa_Comissao', 'Commission Earned': 'Comissao_Recebida' }, inplace=True)

    # Converter a coluna 'Data' para datetime
    df['Ano_Fabricacao'] = pd.to_datetime(df['Ano_Fabricacao'], format='%Y')  
    # arredondar a coluna 'Taxa Comissao' para 2 casas decimais
    df['Taxa_Comissao'] = df['Taxa_Comissao'].round(2)

    print(df.head())

    # Gerar o CSV a partir do DataFrame
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False) 

    # Subir o CSV para o S3 na pasta standardizedzone
    s3.put_object(Bucket=bucket_name, Key='standardizedzone/car_sales_data_standardized.csv', Body=csv_buffer.getvalue())



with DAG('transformar_standard', default_args=default_args, schedule_interval='@daily') as dag:

    transformar_standard = PythonOperator(
        task_id='transform_data',
        python_callable=transform_standard,
    )

transformar_standard
