## **Documentação Data Engenier**
Aqui esta todo o codigo usado no Projeto. 
o projeto consite em 3 fases.
- fase 1: extração de dados, sobre venda de carros. 
- fase 2: Montagem a infraestura de um Data Lake Usando amazom AWS S3 
- fase 3: ETL (Extração, Transformação, Carregamento) dos dados 

abaixo irei detalhar todo processo do projeto..

## **Dataset Ultilizado**
Foi Usado um dataset de vendas de carros!
Confira o Dataset: [Car Sales Data](https://www.kaggle.com/datasets/suraj520/car-sales-data) 

## **Arquitetura do Datalake:** 

optei por implmentar a Arquitetura do data Lake usando o `AWS S3` 
<img src="/img/datalake.png" alt="Estrutura Montada">

### **Arquitetura de Zones**

#### **1. landingzone/**
Área inicial onde os dados brutos são armazenados após a ingestão.
#### **2. standardizedzone/**
Zona onde os dados são normalizados e padronizados para garantir consistência e qualidade.
#### **3. maskedzone/**
Zona destinada ao armazenamento de dados sensíveis com informações mascaradas para proteção.
#### **4. stagingzone/**
Espaço temporário para preparar e transformar dados antes de serem movidos para a zona final.
#### **5. curatedzone/**
Zona onde os dados são refinados e organizados para fácil acesso e análise. 
`aqui montei varios Datasets com regras de negocio para uso analiticos.` 
#### **6. analyticsandbox/**
Espaço para experimentação e análise de dados, permitindo testes e protótipos.


## **ETL** 
Nesta etapa foi a hora de codar!!

Usei o **Docker + Apacher Airflow + python** para Orquestrar toda minha Pipeline de dados. 
na pasta:`/dags` estão todos os códigos que utilizei na extração, carregamento e transformação de dados.

- *load_landingzone.py*: DAG onde transfomei meu dataset usando pandas e carregei na primeira camada  do Datalake no S3.

- *transform_standard.py*: DAG onde li os dados da camada **landingzone** e apliquei algumas **transformações**:
  - Renomear as colunas
  - Converter a coluna **'Data'** para **datetime**
  - Arredondar a coluna **'Taxa Comissão'** para 2 casas decimais
  apos isso carreguei os dados na camada ***standardizedzone***

- *transform_staging.py*: DAG Onde fiz as transformaçoes finais e apliquei também pequenas validaçoes de dados:
**transformações**:
    -Remover linhas onde a data de fabricação é posterior à data de venda 
    -Remover espaços extras e padronizar para maiúsculas
    -Remover linhas onde o valor da venda é menor ou igual a zero
    -Remover Duplicatas

- *transform_curated.py*: DAG onde apliquei as **regras de negócio** e criei alguns **insights** interessantes, como:
  - **venda_total**
  - **agregação_por_vendedor**
  - **fato_vendas_dim_vendedor**
  - **venda_por_marca**
  - **venda_por_vendedor**

Além disso, desenvolvi vários **datasets** com cada função para que o time de analistas possa consultar dados prontos.

- *load_analytics_sandbox.py*: DAG onde repliquei os **datasets** da **curatedzone** e os disponibilizei para os analistas utilizarem.

<img src="/img/dags_airflow.png" alt="Pipeline no Airflow">


## **ETL** Data Quality 

Realizei um processo de Data Quality para validar os dados e garantir que estejam 100% prontos para a equipe de Analytics. Utilizei a biblioteca Soda-core para essa validação. O script **data_quality.py** gera um novo arquivo CSV com os dados validados. 

**Observação:** O ideal é orquestrar essa etapa também com o **Airflow**, mas enfrentei problemas ao rodar localmente, pois o Soda-core ficou indisponível, dificultando essa etapa.

## **Conclusão**

Este é um projeto End to End de engenharia de dados. Posteriormente, irei realizar uma visualização dos dados utilizando o Metabase ou o Streamlit.
