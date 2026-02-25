import io
import os

import boto3
import pandas as pd
import plotly.express as px
import streamlit as st

BUCKET = os.environ.get("S3_BUCKET", "projeto-dados-cvm")
GOLD_PREFIX = "gold"
REGION = os.environ.get("AWS_DEFAULT_REGION", "sa-east-1")

st.set_page_config(
    page_title="CVM - Fundos de Investimento",
    page_icon="📊",
    layout="wide",
)


@st.cache_data(ttl=3600)
def load_data() -> pd.DataFrame:
    s3 = boto3.client("s3", region_name=REGION)
    paginator = s3.get_paginator("list_objects_v2")
    dfs = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=GOLD_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                response = s3.get_object(Bucket=BUCKET, Key=key)
                dfs.append(pd.read_parquet(io.BytesIO(response["Body"].read())))
    if not dfs:
        return pd.DataFrame()
    df = pd.concat(dfs, ignore_index=True)
    df["DT_COMPTC"] = pd.to_datetime(df["DT_COMPTC"])
    return df


# ── Cabeçalho ────────────────────────────────────────────────────────────────
st.title("📊 Fundos de Investimento — CVM")
st.caption("Fonte: CVM · Informes Diários · Pipeline ETL via Apache Airflow + AWS S3")

with st.spinner("Carregando dados do S3..."):
    df_full = load_data()

if df_full.empty:
    st.error("Nenhum dado encontrado. Execute o pipeline Airflow primeiro.")
    st.stop()

# ── Sidebar: filtros globais ──────────────────────────────────────────────────
st.sidebar.header("Filtros")

date_min = df_full["DT_COMPTC"].min().date()
date_max = df_full["DT_COMPTC"].max().date()
date_range = st.sidebar.date_input(
    "Período",
    value=(date_min, date_max),
    min_value=date_min,
    max_value=date_max,
)

tipos_disponiveis = sorted(df_full["TP_FUNDO_CLASSE"].dropna().unique().tolist())
tipos_selecionados = st.sidebar.multiselect(
    "Tipo de Fundo",
    options=tipos_disponiveis,
    default=tipos_disponiveis,
)

cnpj_busca = st.sidebar.text_input("CNPJ do Fundo (opcional)")

# ── Aplicar filtros ───────────────────────────────────────────────────────────
df = df_full.copy()

if len(date_range) == 2:
    df = df[
        (df["DT_COMPTC"].dt.date >= date_range[0])
        & (df["DT_COMPTC"].dt.date <= date_range[1])
    ]

if tipos_selecionados:
    df = df[df["TP_FUNDO_CLASSE"].isin(tipos_selecionados)]

if cnpj_busca.strip():
    df = df[df["CNPJ_FUNDO_CLASSE"].astype(str).str.contains(cnpj_busca.strip(), na=False)]

if df.empty:
    st.warning("Nenhum dado encontrado para os filtros selecionados.")
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
    df.groupby(["DT_COMPTC", "TP_FUNDO_CLASSE"])["VL_PATRIM_LIQ"]
    .sum()
    .reset_index()
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
    df.groupby(["DT_COMPTC", "TP_FUNDO_CLASSE"])["CAPTC_LIQUIDA"]
    .sum()
    .reset_index()
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
df_top = (
    df[df["DT_COMPTC"] == data_mais_recente]
    .nlargest(10, "VL_PATRIM_LIQ")[["CNPJ_FUNDO_CLASSE", "VL_PATRIM_LIQ", "TP_FUNDO_CLASSE"]]
    .sort_values("VL_PATRIM_LIQ", ascending=True)
)
fig4 = px.bar(
    df_top,
    x="VL_PATRIM_LIQ",
    y="CNPJ_FUNDO_CLASSE",
    color="TP_FUNDO_CLASSE",
    orientation="h",
    title=f"Top 10 Fundos por Patrimônio Líquido em {data_mais_recente.strftime('%d/%m/%Y')}",
    labels={
        "VL_PATRIM_LIQ": "Patrimônio Líquido (R$)",
        "CNPJ_FUNDO_CLASSE": "CNPJ do Fundo",
        "TP_FUNDO_CLASSE": "Tipo de Fundo",
    },
)
st.plotly_chart(fig4, use_container_width=True)

st.caption("Fonte: CVM — dados.cvm.gov.br | Pipeline: Apache Airflow + AWS S3 (sa-east-1)")
