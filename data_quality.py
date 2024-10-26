import pandas as pd
import boto3
import subprocess
import os

# Inicializar o cliente S3
s3 = boto3.client(
    's3',
    aws_access_key_id='key  aqui',
    aws_secret_access_key='chave aqui',
    region_name='us-east-1'
)

# Função para validar dados diretamente do S3
def run_soda_validation(**kwargs):
    # Caminho do arquivo YAML de verificação
    soda_scan_yml_path = 'include/check_vendas_por_vendedor.yaml' 
    
    # URL do arquivo CSV no S3
    bucket_name = 'datalake-dadosfera'
    file_key = 'curatedzone/vendas_por_vendedor.csv'
    s3_file_url = f's3://{bucket_name}/{file_key}'

    # Lendo o CSV diretamente do S3
    vendas_df = pd.read_csv(s3_file_url)

    # Salvar o DataFrame em um arquivo na mesma pasta do script
    local_file_path = 'vendas_por_vendedor_validado.csv'
    vendas_df.to_csv(local_file_path, index=False)

    soda_executable_path = '/home/luite/luiteoliveira_DDF_TECH_10_2024/env/bin/soda'

    # Rodar o Soda Core usando subprocess para chamar o CLI
    result = subprocess.run([
        soda_executable_path, 'scan', '-d', 'csv', '-c', soda_scan_yml_path, local_file_path
    ], capture_output=True, text=True)

    # Exibir resultados
    if result.returncode == 0:  
        print("Validação concluída com sucesso:")
        print(result.stdout)
    else:
        print("Erro na validação:")
        print(result.stderr)

# Exemplo de execução
if __name__ == "__main__":
    run_soda_validation()
