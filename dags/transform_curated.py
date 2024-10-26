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

def venda_por_vendedor():
    bucket_name = 'datalake-dadosfera'  
    file_key = 'stagingzone/car_sales_data_staging.csv'  

    csv_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')
    # Converter o CSV em um DataFrame do pandas
    csv_data = StringIO(csv_string)
    df = pd.read_csv(csv_data)

    #Agregação de Vendas por Vendedor
    vendas_por_vendedor = df.groupby('Vendedor_Responsavel').agg(
        total_vendas=('Valor_Venda', 'sum'),
        total_comissao=('Comissao_Recebida', 'sum'),
        carros_vendidos=('Valor_Venda', 'count')
    ).reset_index()


    # Enviar para o S3
    csv_buffer = StringIO()
    vendas_por_vendedor.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket_name, Key='curatedzone/vendas_por_vendedor.csv', Body=csv_buffer.getvalue())

def venda_por_marca():
    bucket_name = 'datalake-dadosfera'  
    file_key = 'stagingzone/car_sales_data_staging.csv'  

    csv_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')
    # Converter o CSV em um DataFrame do pandas
    csv_data = StringIO(csv_string)
    df = pd.read_csv(csv_data)

    #Agregação de Vendas por Marca
    vendas_por_marca = df.groupby('Marca_Veiculo').agg(
        total_vendas=('Valor_Venda', 'sum'),
        carros_vendidos=('Valor_Venda', 'count')
    ).reset_index()


    # Enviar para o S3
    csv_buffer = StringIO()
    vendas_por_marca.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket_name, Key='curatedzone/vendas_por_marca.csv', Body=csv_buffer.getvalue())


def fato_vendas_dim_vendedor():
    bucket_name = 'datalake-dadosfera'  
    file_key = 'stagingzone/car_sales_data_staging.csv'  

    csv_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')
    # Converter o CSV em um DataFrame do pandas
    csv_data = StringIO(csv_string)
    df = pd.read_csv(csv_data)

    #Agregação de Vendas por Marca
    fato_vendas = df[['Data_Venda', 'Vendedor_Responsavel', 'Valor_Venda', 'Comissao_Recebida']]
    # Criando a tabela dimensão (Dim_Vendedor)
    dim_vendedor = df[['Vendedor_Responsavel']].drop_duplicates().reset_index(drop=True)

    
    # Enviar para o S3
    csv_buffer = StringIO()
    fato_vendas.to_csv(csv_buffer, index=False)
    dim_vendedor.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket_name, Key='curatedzone/fato_vendas.csv', Body=csv_buffer.getvalue())
    s3.put_object(Bucket=bucket_name, Key='curatedzone/dim_vendedor.csv', Body=csv_buffer.getvalue())

def agregacao_por_vendedor():
    bucket_name = 'datalake-dadosfera'  
    file_key = 'curatedzone/fato_vendas.csv'  

    csv_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')
    # Converter o CSV em um DataFrame do pandas
    csv_data = StringIO(csv_string)
    df = pd.read_csv(csv_data)

    # Agregação de Vendas por Vendedor
    vendas_por_vendedor = df.groupby('Vendedor_Responsavel').agg(
        total_vendas=('Valor_Venda', 'sum'),
        total_comissao=('Comissao_Recebida', 'sum'),
        carros_vendidos=('Valor_Venda', 'count')
    ).reset_index()

    # Enviar para o S3
    csv_buffer = StringIO()
    vendas_por_vendedor.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket_name, Key='curatedzone/vendas_por_vendedor.csv', Body=csv_buffer.getvalue())


def venda_total():
    bucket_name = 'datalake-dadosfera'  
    file_key = 'curatedzone/fato_vendas.csv'  

    csv_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')
    # Converter o CSV em um DataFrame do pandas
    csv_data = StringIO(csv_string)
    df = pd.read_csv(csv_data)

    relatorio_vendedores = df.groupby('Vendedor_Responsavel').agg(
    total_vendas=('Valor_Venda', 'sum'),
    media_comissao=('Comissao_Recebida', 'mean'),
    carros_vendidos=('Valor_Venda', 'count')
    ).reset_index()

    # Ordenando pelo total de vendas
    relatorio_vendedores = relatorio_vendedores.sort_values(by='total_vendas', ascending=False)

    # Enviar para o S3
    csv_buffer = StringIO()
    relatorio_vendedores.to_csv(csv_buffer, index=False) 
    s3.put_object(Bucket=bucket_name, Key='curatedzone/venda_total.csv', Body=csv_buffer.getvalue())

with DAG('transform_curated', default_args=default_args, schedule_interval='@daily') as dag:
    venda_por_vendedor = PythonOperator(
        task_id='venda_por_vendedor',
        python_callable=venda_por_vendedor
    )

    venda_por_marca = PythonOperator(
        task_id='venda_por_marca',
        python_callable=venda_por_marca
    )

    fato_vendas_dim_vendedor = PythonOperator(
        task_id='fato_vendas_dim_vendedor',
        python_callable=fato_vendas_dim_vendedor
    )

    agregacao_por_vendedor = PythonOperator(
        task_id='agregacao_por_vendedor',
        python_callable=agregacao_por_vendedor
    )

    venda_total = PythonOperator(
        task_id='venda_total',
        python_callable=venda_total
    )

    venda_por_vendedor >> venda_por_marca >> fato_vendas_dim_vendedor >> agregacao_por_vendedor >> venda_total