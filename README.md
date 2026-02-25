# Pipeline ETL — Fundos de Investimento CVM

Pipeline de dados que extrai informações diárias de fundos da CVM, transforma seguindo arquitetura medalhão e disponibiliza em dashboard interativo.

## Arquitetura

```
CVM (ZIP/CSV) → S3 Bronze (CSV) → S3 Silver (Parquet limpo) → S3 Gold (Parquet modelado) → Dashboard Streamlit
```

Orquestrado pelo **Apache Airflow** (Docker local) · Armazenamento em **AWS S3 (sa-east-1)**

---

## Pré-requisitos

- Docker Desktop instalado e rodando
- Conta AWS com bucket `projeto-dados-cvm` criado em `sa-east-1`
- Credenciais AWS com permissão de S3

---

## Setup

### 1. Configurar credenciais

Copie o arquivo de exemplo e preencha com suas credenciais:

```bash
cp .env.example .env
```

Edite o `.env`:

```
AWS_ACCESS_KEY_ID=sua_chave
AWS_SECRET_ACCESS_KEY=sua_chave_secreta
AWS_DEFAULT_REGION=sa-east-1
S3_BUCKET=projeto-dados-cvm
```

### 2. Criar o bucket S3

```bash
pip install boto3 python-dotenv
python scripts/setup_s3.py
```

### 3. Subir o ambiente

```bash
# Inicializar o banco do Airflow (apenas na primeira vez)
docker compose run --rm airflow-init

# Subir todos os serviços
docker compose up -d
```

### 4. Acessar os serviços

| Serviço | URL | Credenciais |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| Dashboard | http://localhost:8501 | — |

---

## Executar o pipeline

### Via Airflow UI

1. Acesse http://localhost:8080
2. Ative a DAG `cvm_etl_pipeline`
3. Clique em **Trigger DAG** para executar manualmente

### Backfill manual (primeira carga histórica)

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger cvm_etl_pipeline
```

A DAG detecta automaticamente quais meses estão faltando e baixa apenas eles. Os dois meses mais recentes são sempre re-baixados.

---

## Estrutura do projeto

```
projeto-dados-cvm/
├── .env                      # Credenciais (não vai pro git)
├── .env.example              # Template de variáveis
├── docker-compose.yml        # Airflow + Streamlit
├── Dockerfile.dashboard      # Imagem do Streamlit
├── requirements-dashboard.txt
├── dags/
│   ├── cvm_etl_pipeline.py   # DAG principal
│   └── utils/
│       ├── extraction.py     # Download e upload para S3
│       └── transformation.py # Bronze → Silver → Gold
├── dashboard/
│   └── dashboard.py          # Dashboard Streamlit
└── scripts/
    └── setup_s3.py           # Criação do bucket S3
```

---

## Dados

- **Fonte**: [CVM — dados.cvm.gov.br](https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/)
- **Período**: Janeiro/2024 em diante
- **Atualização**: Mensal (dia 1 de cada mês às 6h)
- **Campos principais**: `CNPJ_FUNDO`, `DT_COMPTC`, `VL_PATRIM_LIQ`, `CAPTC_DIA`, `RESG_DIA`, `NR_COTST`, `TP_FUNDO_CLASSE`
