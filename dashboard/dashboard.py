import io
import os
from pathlib import Path

import boto3

# Carrega .env da raiz do projeto
_env_path = Path(__file__).parent.parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _key, _, _value = _line.partition("=")
            os.environ.setdefault(_key.strip(), _value.strip())

import pandas as pd
import plotly.express as px
import streamlit as st

BUCKET = os.environ.get("S3_BUCKET", "projeto-dados-cvm")
ENRICHED_PREFIX = "informes-enriquecido/gold"
REGION = os.environ.get("AWS_DEFAULT_REGION", "sa-east-1")

st.set_page_config(
    page_title="CVM - Fundos de Investimento",
    page_icon="📊",
    layout="wide",
)


@st.cache_data(ttl=3600)
def load_data(n_months: int) -> pd.DataFrame:
    s3 = boto3.client("s3", region_name=REGION)
    paginator = s3.get_paginator("list_objects_v2")

    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=ENRICHED_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])

    if not keys:
        return pd.DataFrame()

    keys.sort()
    keys = keys[-n_months:]

    dfs = []
    for key in keys:
        resp = s3.get_object(Bucket=BUCKET, Key=key)
        dfs.append(pd.read_parquet(io.BytesIO(resp["Body"].read())))

    df = pd.concat(dfs, ignore_index=True)
    df["DT_COMPTC"] = pd.to_datetime(df["DT_COMPTC"])
    return df


def _multiselect_filter(df: pd.DataFrame, col: str, label: str) -> pd.DataFrame:
    """Cria multiselect na sidebar e filtra df pela coluna, se existir."""
    if col not in df.columns:
        return df
    opcoes = sorted(df[col].dropna().unique().tolist())
    if not opcoes:
        return df
    selecionados = st.sidebar.multiselect(label, options=opcoes, default=opcoes)
    if selecionados:
        return df[df[col].isin(selecionados)]
    return df


# ── Cabeçalho ────────────────────────────────────────────────────────────────
st.title("📊 Fundos de Investimento — CVM")
st.caption("Fonte: CVM · Informes Diários + Cadastral · Pipeline ETL via Apache Airflow + AWS S3")

# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.header("Filtros")

n_meses = st.sidebar.slider(
    "Meses de histórico",
    min_value=1,
    max_value=26,
    value=6,
    help="Reduz uso de memória. Aumente para ver histórico mais longo.",
)

with st.spinner(f"Carregando últimos {n_meses} meses..."):
    df_full = load_data(n_meses)

if df_full.empty:
    st.error("Nenhum dado encontrado. Execute o pipeline Airflow primeiro (incluindo a task enrich_gold).")
    st.stop()

# ── Filtros de período e tipo ─────────────────────────────────────────────────
date_min = df_full["DT_COMPTC"].min().date()
date_max = df_full["DT_COMPTC"].max().date()
date_range = st.sidebar.date_input(
    "Período",
    value=(date_min, date_max),
    min_value=date_min,
    max_value=date_max,
)

df = df_full.copy()

if len(date_range) == 2:
    df = df[
        (df["DT_COMPTC"].dt.date >= date_range[0])
        & (df["DT_COMPTC"].dt.date <= date_range[1])
    ]

df = _multiselect_filter(df, "TP_FUNDO_CLASSE", "Tipo de Fundo")
df = _multiselect_filter(df, "SIT", "Situação")
df = _multiselect_filter(df, "CLASSE", "Classe")
df = _multiselect_filter(df, "PUBLICO_ALVO", "Público-alvo")
df = _multiselect_filter(df, "CONDOM", "Condomínio")
df = _multiselect_filter(df, "ADMIN", "Administrador")
df = _multiselect_filter(df, "GESTOR", "Gestor")
df = _multiselect_filter(df, "CUSTODIANTE", "Custodiante")
df = _multiselect_filter(df, "RENTAB_FUNDO", "Benchmark")
df = _multiselect_filter(df, "TRIB_LPRAZO", "Tributação LP")

# Filtro exclusivo / fundo de cotas
if "FUNDO_EXCLUSIVO" in df.columns:
    excl = st.sidebar.selectbox("Fundo exclusivo", ["Todos", "Sim", "Não"])
    if excl != "Todos":
        df = df[df["FUNDO_EXCLUSIVO"] == excl]

if "FUNDO_COTAS" in df.columns:
    fic = st.sidebar.selectbox("Fundo de cotas (FIC)", ["Todos", "Sim", "Não"])
    if fic != "Todos":
        df = df[df["FUNDO_COTAS"] == fic]

# Busca por CNPJ ou nome
busca = st.sidebar.text_input("Busca (CNPJ ou nome do fundo)")
if busca.strip():
    mask_cnpj = df["CNPJ_FUNDO_CLASSE"].astype(str).str.contains(busca.strip(), na=False, case=False)
    if "DENOM_SOCIAL" in df.columns:
        mask_nome = df["DENOM_SOCIAL"].astype(str).str.contains(busca.strip(), na=False, case=False)
        df = df[mask_cnpj | mask_nome]
    else:
        df = df[mask_cnpj]

if df.empty:
    st.warning("Nenhum dado para os filtros selecionados.")
    st.stop()

# ── Métricas resumo ───────────────────────────────────────────────────────────
data_mais_recente = df["DT_COMPTC"].max()
col1, col2, col3 = st.columns(3)
col1.metric("Data mais recente", data_mais_recente.strftime("%d/%m/%Y"))
col2.metric("Total de fundos", f"{df['CNPJ_FUNDO_CLASSE'].nunique():,}")
col3.metric(
    "Patrimônio total (R$)",
    f"{df[df['DT_COMPTC'] == data_mais_recente]['VL_PATRIM_LIQ'].sum():,.0f}",
)

st.divider()

# ── Gráfico 1: Patrimônio Líquido ─────────────────────────────────────────────
st.subheader("Patrimônio Líquido por Tipo de Fundo")
df_patrim = (
    df.groupby(["DT_COMPTC", "TP_FUNDO_CLASSE"])["VL_PATRIM_LIQ"].sum().reset_index()
)
fig1 = px.bar(
    df_patrim,
    x="DT_COMPTC",
    y="VL_PATRIM_LIQ",
    color="TP_FUNDO_CLASSE",
    barmode="stack",
    title="Patrimônio Líquido Agregado por Data e Tipo de Fundo",
    labels={
        "VL_PATRIM_LIQ": "Patrimônio Líquido (R$)",
        "DT_COMPTC": "Data",
        "TP_FUNDO_CLASSE": "Tipo de Fundo",
    },
)
st.plotly_chart(fig1, use_container_width=True)

# ── Gráfico 2: Captação Líquida ───────────────────────────────────────────────
st.subheader("Captação Líquida por Tipo de Fundo (CAPTC_DIA − RESG_DIA)")
df_captc = (
    df.groupby(["DT_COMPTC", "TP_FUNDO_CLASSE"])["CAPTC_LIQUIDA"].sum().reset_index()
)
fig2 = px.bar(
    df_captc,
    x="DT_COMPTC",
    y="CAPTC_LIQUIDA",
    color="TP_FUNDO_CLASSE",
    barmode="stack",
    title="Captação Líquida por Data e Tipo de Fundo",
    labels={
        "CAPTC_LIQUIDA": "Captação Líquida (R$)",
        "DT_COMPTC": "Data",
        "TP_FUNDO_CLASSE": "Tipo de Fundo",
    },
)
fig2.add_hline(y=0, line_dash="dash", line_color="gray", annotation_text="Zero")
st.plotly_chart(fig2, use_container_width=True)

# ── Gráfico 3: Distribuição de cotistas ──────────────────────────────────────
st.subheader(f"Distribuição de Cotistas — {data_mais_recente.strftime('%d/%m/%Y')}")
df_cotistas = df[df["DT_COMPTC"] == data_mais_recente]
fig3 = px.histogram(
    df_cotistas,
    x="NR_COTST",
    nbins=60,
    log_y=True,
    title=f"Distribuição do Número de Cotistas por Fundo em {data_mais_recente.strftime('%d/%m/%Y')}",
    labels={"NR_COTST": "Número de Cotistas", "count": "Frequência (escala log)"},
)
st.plotly_chart(fig3, use_container_width=True)

# ── Gráfico 4: Top 10 fundos por patrimônio ───────────────────────────────────
st.subheader(f"Top 10 Fundos por Patrimônio Líquido — {data_mais_recente.strftime('%d/%m/%Y')}")
label_col = "DENOM_SOCIAL" if "DENOM_SOCIAL" in df.columns else "CNPJ_FUNDO_CLASSE"
label_name = "Nome do Fundo" if label_col == "DENOM_SOCIAL" else "CNPJ do Fundo"

df_top = (
    df[df["DT_COMPTC"] == data_mais_recente]
    .nlargest(10, "VL_PATRIM_LIQ")[[label_col, "VL_PATRIM_LIQ", "TP_FUNDO_CLASSE"]]
    .sort_values("VL_PATRIM_LIQ", ascending=True)
)
fig4 = px.bar(
    df_top,
    x="VL_PATRIM_LIQ",
    y=label_col,
    color="TP_FUNDO_CLASSE",
    orientation="h",
    title=f"Top 10 Fundos por Patrimônio Líquido em {data_mais_recente.strftime('%d/%m/%Y')}",
    labels={
        "VL_PATRIM_LIQ": "Patrimônio Líquido (R$)",
        label_col: label_name,
        "TP_FUNDO_CLASSE": "Tipo de Fundo",
    },
)
st.plotly_chart(fig4, use_container_width=True)

st.caption("Fonte: CVM — dados.cvm.gov.br | Pipeline: Apache Airflow + AWS S3 (sa-east-1)")
