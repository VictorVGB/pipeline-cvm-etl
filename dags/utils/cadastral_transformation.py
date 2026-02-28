import io
import logging

import boto3
import pandas as pd

logger = logging.getLogger(__name__)

BUCKET = "projeto-dados-cvm"
CADASTRAL_BRONZE_PREFIX = "informacoes-cadastrais/bronze"
CADASTRAL_SILVER_PREFIX = "informacoes-cadastrais/silver"
CADASTRAL_GOLD_PREFIX = "informacoes-cadastrais/gold"

DATE_FILL = pd.Timestamp("2099-12-31")


def _read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    s3 = boto3.client("s3", region_name="sa-east-1")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(
        io.BytesIO(obj["Body"].read()),
        sep=";",
        encoding="latin-1",
        dtype=str,
        low_memory=False,
    )


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


def _detect_date_columns(df: pd.DataFrame) -> list:
    """Detecta colunas de data pelo nome (DT_INI_*, DT_FIM_*, Data_*)."""
    return [
        c for c in df.columns
        if c.startswith("DT_INI_") or c.startswith("DT_FIM_")
        or c.startswith("Data_") or c == "DT_COMPTC"
    ]


def _detect_ini_fim_pair(df: pd.DataFrame):
    """Retorna (col_ini, col_fim) ou (None, None) se não encontrar par DT_INI/DT_FIM."""
    ini_cols = [c for c in df.columns if c.startswith("DT_INI_")]
    fim_cols = [c for c in df.columns if c.startswith("DT_FIM_")]
    if ini_cols and fim_cols:
        return ini_cols[0], fim_cols[0]
    return None, None


def _fix_overlapping_periods(df: pd.DataFrame, cnpj_col: str, col_ini: str, col_fim: str) -> pd.DataFrame:
    """
    Corrige sobreposição de períodos para o mesmo CNPJ:
    - Ordena por CNPJ + DT_INI
    - Ajusta DT_FIM do registro anterior para DT_INI do seguinte quando há sobreposição
    - Sobreposições residuais: mantém linha com DT_FIM mais recente
    """
    df = df.sort_values([cnpj_col, col_ini]).copy()
    df[col_ini] = pd.to_datetime(df[col_ini], errors="coerce")
    df[col_fim] = pd.to_datetime(df[col_fim], errors="coerce").fillna(DATE_FILL)

    rows = df.to_dict("records")
    for i in range(len(rows) - 1):
        curr = rows[i]
        nxt = rows[i + 1]
        if curr[cnpj_col] == nxt[cnpj_col]:
            if pd.notna(curr[col_ini]) and pd.notna(nxt[col_ini]):
                if curr[col_fim] > nxt[col_ini]:
                    rows[i][col_fim] = nxt[col_ini]

    df = pd.DataFrame(rows)

    # Sobreposições residuais: remover duplicatas mantendo DT_FIM mais recente
    antes = len(df)
    df_sorted = df.sort_values([cnpj_col, col_ini, col_fim], ascending=[True, True, False])
    df_dedup = df_sorted.drop_duplicates(subset=[cnpj_col, col_ini], keep="first")
    removidos = antes - len(df_dedup)
    if removidos:
        cnpjs_afetados = df[df.duplicated(subset=[cnpj_col, col_ini], keep=False)][cnpj_col].unique()
        logger.warning(
            f"Sobreposições residuais removidas: {removidos} linha(s). "
            f"CNPJs afetados: {list(cnpjs_afetados[:10])}"
        )
    return df_dedup.reset_index(drop=True)


def transform_cadastral_bronze_to_silver(filename: str, bucket: str = BUCKET) -> None:
    """Lê CSV do bronze cadastral, limpa, corrige períodos e salva Parquet no silver."""
    bronze_key = f"{CADASTRAL_BRONZE_PREFIX}/{filename}"
    parquet_name = filename.replace(".csv", ".parquet")
    silver_key = f"{CADASTRAL_SILVER_PREFIX}/{parquet_name}"

    logger.info(f"Lendo bronze cadastral: s3://{bucket}/{bronze_key}")
    df = _read_csv_from_s3(bucket, bronze_key)

    # Converte colunas de data
    for col in _detect_date_columns(df):
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # Preenche DT_FIM_* vazia com 2099-12-31
    fim_cols = [c for c in df.columns if c.startswith("DT_FIM_")]
    for col in fim_cols:
        df[col] = df[col].fillna(DATE_FILL)

    # Detecta CNPJ e par INI/FIM para correção de sobreposição
    cnpj_col = next((c for c in df.columns if "CNPJ" in c.upper() and "FUNDO" in c.upper()), None)
    col_ini, col_fim = _detect_ini_fim_pair(df)

    if cnpj_col and col_ini and col_fim:
        df = _fix_overlapping_periods(df, cnpj_col, col_ini, col_fim)

    # Remove duplicatas
    before = len(df)
    df = df.drop_duplicates()
    if before != len(df):
        logger.info(f"Removidas {before - len(df)} duplicatas em {filename}")

    _write_parquet_to_s3(df, bucket, silver_key)
    logger.info(f"Silver cadastral salvo: s3://{bucket}/{silver_key} ({len(df)} linhas)")


def transform_cadastral_silver_to_gold(filename: str, bucket: str = BUCKET) -> None:
    """Lê Parquet do silver cadastral e salva no gold (snappy)."""
    parquet_name = filename.replace(".csv", ".parquet")
    silver_key = f"{CADASTRAL_SILVER_PREFIX}/{parquet_name}"
    gold_key = f"{CADASTRAL_GOLD_PREFIX}/{parquet_name}"

    logger.info(f"Lendo silver cadastral: s3://{bucket}/{silver_key}")
    df = _read_parquet_from_s3(bucket, silver_key)

    _write_parquet_to_s3(df, bucket, gold_key)
    logger.info(f"Gold cadastral salvo: s3://{bucket}/{gold_key} ({len(df)} linhas)")
