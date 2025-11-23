# 📘 OPEA – Data Engineering Case 

Este repositório contém a solução do desafio de Engenharia de Dados proposto pela OPEA. O projeto foi estruturado de forma clara e organizada para facilitar entendimento, testes e execução, utilizando Docker e LocalStack para simular serviços da AWS.

## 🧭 Visão Geral

O projeto implementa um pipeline de dados dividido em três camadas clássicas:

- **RAW** → Ingestão dos dados brutos  
- **STAGE** → Limpeza, padronização e pré-processamento  
- **ANALYTICS** → Modelagem final e preparação para consumo analítico

## 📂 Estrutura do Projeto

```
.
├── data_pipeline/
│   ├── scripts/        # Scripts dos pipelines RAW, Stage e Analytics
│   ├── tools/          # Funções auxiliares e validadores
│   ├── tests/          # Testes automatizados (pytest)
│   ├── configs/        # Configurações e variáveis
│   └── __init__.py
├── data_base/          # Dados de entrada do desafio
├── docker-compose.yml  # Orquestração da aplicação + LocalStack
├── Dockerfile          # Ambiente com Spark + Python
└── README.md           # Este arquivo
```

## 🐳 Execução Local (Docker + LocalStack)

### 1. Construir a imagem

```bash
docker compose build
```

### 2. Subir os serviços

```bash
docker compose up -d
```

### 3. Entrar no container da aplicação

```bash
docker exec -it spark-pipeline bash
```

### 4. Executar os testes

```bash
pytest -q data_pipeline/tests
```

### 5. Executar o Pipeline RAW

```bash
python -m data_pipeline.scripts.pipeline_raw --env local --output-dir ./local_output
```

Os arquivos gerados aparecerão em:

```
./local_output/
```

## ⭐ Observações Importantes

- O projeto usa LocalStack para simular AWS S3/Glue localmente.
- O uso de Parquet nas camadas STAGE/ANALYTICS foi adotado para manter simplicidade.
- O arquivo de entrada `data_base/dados_entrada.xlsx` já está incluído.

## Logs dos testes

<img width="1291" height="459" alt="Captura de tela 2025-11-23 174951" src="https://github.com/user-attachments/assets/f39262ce-6126-4fc4-b7b1-c1b5fe551852" />
