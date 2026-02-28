# Pipeline ETL — Fundos de Investimento CVM

Pipeline de dados que extrai informações diárias e cadastrais de fundos da CVM, transforma seguindo arquitetura medalhão e disponibiliza em dashboard interativo.

## Arquitetura

```
CVM (ZIP/CSV)
  ├── Informes Diários  → S3 informes-diario/bronze  → silver → gold ──┐
  └── Dados Cadastrais  → S3 informacoes-cadastrais/bronze → silver → gold ┘
                                                                         ↓
                                                              Dashboard Streamlit
                                                          (join diário + cadastral)
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
pip install boto3
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

### Via terminal

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger cvm_etl_pipeline
```

A DAG detecta automaticamente quais meses estão faltando e baixa apenas eles. Os dois meses mais recentes são sempre re-baixados.

---

## Estrutura do projeto

```
projeto-dados-cvm/
├── .env                          # Credenciais (não vai pro git)
├── .env.example                  # Template de variáveis
├── docker-compose.yml            # Airflow local
├── dags/
│   ├── cvm_etl_pipeline.py       # DAG principal (dois pipelines paralelos)
│   └── utils/
│       ├── extraction.py         # Download informes diários → S3 bronze
│       ├── transformation.py     # Informes: bronze → silver → gold
│       ├── cadastral_extraction.py     # Download cadastral CVM → S3 bronze
│       └── cadastral_transformation.py # Cadastral: bronze → silver → gold
├── dashboard/
│   └── dashboard.py              # Dashboard Streamlit com join cadastral
└── scripts/
    ├── setup_s3.py               # Criação do bucket S3
    └── limpar_s3.py              # Limpeza do bucket por camada
```

---

## Estrutura S3 (arquitetura medalhão)

```
projeto-dados-cvm/
├── informes-diario/
│   ├── bronze/   inf_diario_fi_YYYYMM.csv     (bruto da CVM)
│   ├── silver/   inf_diario_fi_YYYYMM.parquet  (limpo, sem duplicatas)
│   └── gold/     inf_diario_fi_YYYYMM.parquet  (+ CAPTC_LIQUIDA calculada)
└── informacoes-cadastrais/
    ├── bronze/   cad_fi_hist.csv               (bruto da CVM)
    ├── silver/   cad_fi_hist.parquet            (limpo, períodos corrigidos)
    └── gold/     cad_fi_hist.parquet            (comprimido snappy)
```

---

## Dados

### Informes Diários
- **Fonte**: [CVM — dados.cvm.gov.br/FI/DOC/INF_DIARIO](https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/)
- **Período**: Janeiro/2024 em diante
- **Atualização**: Mensal (dia 1 de cada mês às 6h)
- **Campos principais**: `CNPJ_FUNDO_CLASSE`, `DT_COMPTC`, `VL_PATRIM_LIQ`, `CAPTC_DIA`, `RESG_DIA`, `NR_COTST`, `TP_FUNDO_CLASSE`

### Dados Cadastrais
- **Fonte**: [CVM — dados.cvm.gov.br/FI/CAD](https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi_hist.zip)
- **Atualização**: A cada execução da DAG (substitui arquivos existentes)
- **Uso**: Enriquecimento dos informes com nome do fundo, administrador, gestor, situação e outros dados cadastrais

---

## Limpar o bucket S3

```bash
python scripts/limpar_s3.py --camada bronze   # apenas bronze
python scripts/limpar_s3.py --camada silver   # apenas silver
python scripts/limpar_s3.py --camada gold     # apenas gold
python scripts/limpar_s3.py --tudo            # tudo (bronze + silver + gold)
```
