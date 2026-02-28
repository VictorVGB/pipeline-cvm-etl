import io
import logging

import boto3
import pandas as pd

logger = logging.getLogger(__name__)

BUCKET = "projeto-dados-cvm"
BRONZE_PREFIX = "informes-diario/bronze"
SILVER_PREFIX = "informes-diario/silver"
GOLD_PREFIX = "informes-diario/gold"

# Campos obrigatórios conforme dicionário de dados da CVM
EXPECTED_COLUMNS = {
    "CNPJ_FUNDO_CLASSE",
    "DT_COMPTC",
    "VL_TOTAL",
    "VL_QUOTA",
    "VL_PATRIM_LIQ",
    "CAPTC_DIA",
    "RESG_DIA",
    "NR_COTST",
    "TP_FUNDO_CLASSE",
}


def _read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    s3 = boto3.client("s3", region_name="sa-east-1")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()), sep=";", encoding="latin-1")


def _write_parquet_to_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    s3 = boto3.client("s3", region_name="sa-east-1")
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, compression="snappy")
    buffer.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())


def _read_parquet_from_s3(bucket: str, key: str) -> pd.DataFrame:
    s3 = boto3.client("s3", region_name="sa-east-1")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def validate_schema(df: pd.DataFrame, year_month: str) -> None:
    """Valida que todos os campos esperados estão presentes. Falha com erro descritivo."""
    missing = EXPECTED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(
            f"Validação de schema falhou para {year_month}. "
            f"Colunas ausentes: {sorted(missing)}. "
            f"Colunas presentes: {sorted(df.columns.tolist())}"
        )


def transform_bronze_to_silver(year_month: str, bucket: str = BUCKET) -> None:
    """Lê CSV do bronze, remove duplicatas, ajusta tipos e salva Parquet no silver."""
    bronze_key = f"{BRONZE_PREFIX}/inf_diario_fi_{year_month}.csv"
    silver_key = f"{SILVER_PREFIX}/inf_diario_fi_{year_month}.parquet"

    logger.info(f"Lendo bronze: s3://{bucket}/{bronze_key}")
    df = _read_csv_from_s3(bucket, bronze_key)

    validate_schema(df, year_month)

    # Ajuste de tipos
    df["DT_COMPTC"] = pd.to_datetime(df["DT_COMPTC"])
    for col in ["VL_TOTAL", "VL_QUOTA", "VL_PATRIM_LIQ", "CAPTC_DIA", "RESG_DIA"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["NR_COTST"] = pd.to_numeric(df["NR_COTST"], errors="coerce").astype("Int64")

    # Remoção de duplicatas (idempotente)
    before = len(df)
    df = df.drop_duplicates()
    removed = before - len(df)
    if removed:
        logger.info(f"Removidas {removed} linhas duplicadas em {year_month}")

    _write_parquet_to_s3(df, bucket, silver_key)
    logger.info(f"Silver salvo: s3://{bucket}/{silver_key} ({len(df)} linhas)")


def transform_silver_to_gold(year_month: str, bucket: str = BUCKET) -> None:
    """Lê Parquet do silver, aplica modelagem analítica e salva no gold."""
    silver_key = f"{SILVER_PREFIX}/inf_diario_fi_{year_month}.parquet"
    gold_key = f"{GOLD_PREFIX}/inf_diario_fi_{year_month}.parquet"

    logger.info(f"Lendo silver: s3://{bucket}/{silver_key}")
    df = _read_parquet_from_s3(bucket, silver_key)

    # Coluna derivada para o dashboard
    df["CAPTC_LIQUIDA"] = df["CAPTC_DIA"] - df["RESG_DIA"]

    _write_parquet_to_s3(df, bucket, gold_key)
    logger.info(f"Gold salvo: s3://{bucket}/{gold_key} ({len(df)} linhas)")
