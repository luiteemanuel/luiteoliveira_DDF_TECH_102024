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

## **Arquitetura de Zones**

### **1. landingzone/**
Área inicial onde os dados brutos são armazenados após a ingestão.

### **2. standardizedzone/**
Zona onde os dados são normalizados e padronizados para garantir consistência e qualidade.

### **3. maskedzone/**
Zona destinada ao armazenamento de dados sensíveis com informações mascaradas para proteção.

### **4. stagingzone/**
Espaço temporário para preparar e transformar dados antes de serem movidos para a zona final.

### **5. curatedzone/**
Zona onde os dados são refinados e organizados para fácil acesso e análise. 
`aqui montei varios Datasets com regras de negocio para uso analiticos.` 

### **6. analyticsandbox/**
Espaço para experimentação e análise de dados, permitindo testes e protótipos.

