import pandas as pd
import boto3
from io import StringIO

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
