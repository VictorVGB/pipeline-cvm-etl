import io
import logging

import boto3
import pandas as pd

logger = logging.getLogger(__name__)

BUCKET = "projeto-dados-cvm"
GOLD_PREFIX = "informes-diario/gold"
CADASTRAL_GOLD_PREFIX = "informacoes-cadastrais/gold"
ENRICHED_PREFIX = "informes-enriquecido/gold"

# Arquivos cadastrais a enriquecer (exerc_social tem attr=[] e é auto-pulado)
CADASTRAL_FILES = [
    "cad_fi_hist_denom_social",
    "cad_fi_hist_denom_comerc",
    "cad_fi_hist_sit",
    "cad_fi_hist_admin",
    "cad_fi_hist_gestor",
    "cad_fi_hist_custodiante",
    "cad_fi_hist_controlador",
    "cad_fi_hist_auditor",
    "cad_fi_hist_classe",
    "cad_fi_hist_condom",
    "cad_fi_hist_publico_alvo",
    "cad_fi_hist_rentab",
    "cad_fi_hist_taxa_adm",
    "cad_fi_hist_taxa_perfm",
    "cad_fi_hist_trib_lprazo",
    "cad_fi_hist_fic",
    "cad_fi_hist_exclusivo",
    "cad_fi_hist_diretor_resp",
]


def _read_parquet_from_s3(s3, bucket: str, key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def _write_parquet_to_s3(df: pd.DataFrame, s3, bucket: str, key: str) -> None:
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, compression="snappy")
    buffer.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())


def _join_with_period(
    df: pd.DataFrame,
    df_cad: pd.DataFrame,
    attr_cols: list,
    ini_col: str,
    fim_col: str,
) -> pd.DataFrame:
    """Join por CNPJ + filtro DT_INI <= DT_COMPTC <= DT_FIM."""
    df = df.copy()
    df["_rid"] = range(len(df))

    cad_sel = ["CNPJ_FUNDO_CLASSE"] + attr_cols + [ini_col, fim_col]
    merged = df[["_rid", "CNPJ_FUNDO_CLASSE", "DT_COMPTC"]].merge(
        df_cad[cad_sel], on="CNPJ_FUNDO_CLASSE", how="left"
    )
    no_match = merged[ini_col].isna()
    within = (merged["DT_COMPTC"] >= merged[ini_col]) & (merged["DT_COMPTC"] <= merged[fim_col])
    matched = (
        merged[no_match | within]
        .drop_duplicates(subset=["_rid"], keep="first")
        .set_index("_rid")
    )
    for col in attr_cols:
        if col not in df.columns:
            df[col] = df["_rid"].map(matched[col])

    return df.drop(columns=["_rid"])


def _join_latest_before(
    df: pd.DataFrame,
    df_cad: pd.DataFrame,
    attr_cols: list,
    ini_col: str,
) -> pd.DataFrame:
    """Para arquivos sem DT_FIM: usa o registro mais recente com DT_INI <= DT_COMPTC."""
    df = df.copy()
    df["_rid"] = range(len(df))

    cad_sel = ["CNPJ_FUNDO_CLASSE"] + attr_cols + [ini_col]
    merged = df[["_rid", "CNPJ_FUNDO_CLASSE", "DT_COMPTC"]].merge(
        df_cad[cad_sel], on="CNPJ_FUNDO_CLASSE", how="left"
    )
    valid = merged[merged[ini_col].isna() | (merged["DT_COMPTC"] >= merged[ini_col])]
    # Mais recente por _rid
    matched = (
        valid.sort_values(ini_col)
        .drop_duplicates(subset=["_rid"], keep="last")
        .set_index("_rid")
    )
    for col in attr_cols:
        if col not in df.columns:
            df[col] = df["_rid"].map(matched[col])

    return df.drop(columns=["_rid"])


def list_enriched_months(bucket: str = BUCKET) -> set:
    """Retorna conjunto de year_month já presentes no enriched gold."""
    s3 = boto3.client("s3", region_name="sa-east-1")
    paginator = s3.get_paginator("list_objects_v2")
    months = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=ENRICHED_PREFIX):
        for obj in page.get("Contents", []):
            name = obj["Key"].split("/")[-1]
            if name.endswith(".parquet"):
                ym = name.replace("inf_diario_fi_", "").replace(".parquet", "")
                months.add(ym)
    return months


def enrich_month(year_month: str, bucket: str = BUCKET) -> None:
    """Lê gold diário de year_month, junta todos os atributos cadastrais e salva no enriched."""
    s3 = boto3.client("s3", region_name="sa-east-1")
    gold_key = f"{GOLD_PREFIX}/inf_diario_fi_{year_month}.parquet"
    enriched_key = f"{ENRICHED_PREFIX}/inf_diario_fi_{year_month}.parquet"

    logger.info(f"Enriquecendo {year_month}...")
    df = _read_parquet_from_s3(s3, bucket, gold_key)
    df["DT_COMPTC"] = pd.to_datetime(df["DT_COMPTC"])

    for file_stem in CADASTRAL_FILES:
        cad_key = f"{CADASTRAL_GOLD_PREFIX}/{file_stem}.parquet"
        try:
            df_cad = _read_parquet_from_s3(s3, bucket, cad_key)
        except Exception as e:
            logger.warning(f"  {file_stem}: não encontrado ({e}), pulando")
            continue

        # Normaliza coluna CNPJ
        cnpj_col = next(
            (c for c in df_cad.columns if "CNPJ" in c.upper() and "FUNDO" in c.upper()), None
        )
        if cnpj_col is None:
            logger.warning(f"  {file_stem}: sem coluna CNPJ, pulando")
            continue
        if cnpj_col != "CNPJ_FUNDO_CLASSE":
            df_cad = df_cad.rename(columns={cnpj_col: "CNPJ_FUNDO_CLASSE"})

        ini_col = next((c for c in df_cad.columns if c.startswith("DT_INI_")), None)
        fim_col = next((c for c in df_cad.columns if c.startswith("DT_FIM_")), None)

        if ini_col is None:
            logger.warning(f"  {file_stem}: sem DT_INI, pulando")
            continue

        skip = {"CNPJ_FUNDO_CLASSE", "DT_REG", ini_col}
        if fim_col:
            skip.add(fim_col)
        attr_cols = [c for c in df_cad.columns if c not in skip and c not in df.columns]

        if not attr_cols:
            logger.debug(f"  {file_stem}: sem atributos novos, pulando")
            continue

        df_cad[ini_col] = pd.to_datetime(df_cad[ini_col], errors="coerce")
        if fim_col:
            df_cad[fim_col] = pd.to_datetime(df_cad[fim_col], errors="coerce")
            df = _join_with_period(df, df_cad, attr_cols, ini_col, fim_col)
        else:
            df = _join_latest_before(df, df_cad, attr_cols, ini_col)

        logger.info(f"  {file_stem}: {attr_cols} adicionados")

    _write_parquet_to_s3(df, s3, bucket, enriched_key)
    logger.info(
        f"Enriquecido salvo: s3://{bucket}/{enriched_key} "
        f"({len(df)} linhas, {len(df.columns)} colunas)"
    )
