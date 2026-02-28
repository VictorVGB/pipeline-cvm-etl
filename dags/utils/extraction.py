import io
import logging
import zipfile
from datetime import datetime

import boto3
import requests
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)

CVM_BASE_URL = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS"
BUCKET = "projeto-dados-cvm"
BRONZE_PREFIX = "informes-diario/bronze"


def list_existing_files(bucket: str, prefix: str) -> set:
    """Retorna conjunto de strings YYYYMM já presentes no S3."""
    s3 = boto3.client("s3", region_name="sa-east-1")
    paginator = s3.get_paginator("list_objects_v2")
    existing = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            filename = obj["Key"].split("/")[-1]
            if filename.startswith("inf_diario_fi_") and filename.endswith(".csv"):
                year_month = filename.replace("inf_diario_fi_", "").replace(".csv", "")
                existing.add(year_month)
    return existing


def build_months_to_download(start: str = "202401") -> list:
    """Retorna lista de YYYYMM desde start até o mês atual."""
    start_date = datetime.strptime(start, "%Y%m")
    current_date = datetime.now().replace(day=1)
    months = []
    d = start_date
    while d <= current_date:
        months.append(d.strftime("%Y%m"))
        d += relativedelta(months=1)
    return months


def get_last_two_months() -> list:
    """Retorna os dois meses mais recentes (sempre re-baixar)."""
    current = datetime.now().replace(day=1)
    prev = current - relativedelta(months=1)
    return [prev.strftime("%Y%m"), current.strftime("%Y%m")]


def download_and_extract(year_month: str, bucket: str = BUCKET) -> bool:
    """
    Baixa o ZIP da CVM, extrai o CSV e faz upload para S3 bronze.
    Retorna True se bem-sucedido, False se o arquivo não estiver disponível.
    """
    url = f"{CVM_BASE_URL}/inf_diario_fi_{year_month}.zip"
    s3_key = f"{BRONZE_PREFIX}/inf_diario_fi_{year_month}.csv"

    logger.info(f"Baixando {url}")
    try:
        response = requests.get(url, timeout=120)
        if response.status_code == 404:
            logger.warning(f"Arquivo não disponível na CVM para {year_month}: {url}")
            return False
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.warning(f"Erro ao baixar {year_month}: {e}")
        return False

    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        csv_files = [f for f in zf.namelist() if f.endswith(".csv")]
        if not csv_files:
            logger.warning(f"Nenhum CSV encontrado no ZIP para {year_month}")
            return False
        csv_content = zf.read(csv_files[0])

    s3 = boto3.client("s3", region_name="sa-east-1")
    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=csv_content,
        ContentType="text/csv",
    )
    logger.info(f"Salvo em s3://{bucket}/{s3_key}")
    return True
