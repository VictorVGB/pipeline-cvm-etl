import io
import logging
import zipfile

import boto3
import requests

logger = logging.getLogger(__name__)

CVM_CADASTRAL_URL = "https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi_hist.zip"
BUCKET = "projeto-dados-cvm"
CADASTRAL_BRONZE_PREFIX = "informacoes-cadastrais/bronze"


def download_cadastral(bucket: str = BUCKET) -> None:
    """
    Baixa cad_fi_hist.zip da CVM, extrai todos os CSVs e faz upload
    para informacoes-cadastrais/bronze/ substituindo arquivos existentes.
    """
    logger.info(f"Baixando cadastral: {CVM_CADASTRAL_URL}")

    try:
        response = requests.get(CVM_CADASTRAL_URL, timeout=300)
        if response.status_code == 404:
            raise RuntimeError(
                f"Arquivo cadastral não encontrado na CVM: {CVM_CADASTRAL_URL}"
            )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Erro ao baixar arquivo cadastral da CVM: {e}") from e

    s3 = boto3.client("s3", region_name="sa-east-1")

    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        csv_files = [f for f in zf.namelist() if f.endswith(".csv")]
        if not csv_files:
            raise RuntimeError("Nenhum CSV encontrado no ZIP cadastral da CVM.")

        for csv_name in csv_files:
            csv_content = zf.read(csv_name)
            filename = csv_name.split("/")[-1]
            s3_key = f"{CADASTRAL_BRONZE_PREFIX}/{filename}"
            s3.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=csv_content,
                ContentType="text/csv",
            )
            logger.info(f"Salvo: s3://{bucket}/{s3_key}")

    logger.info(f"Cadastral: {len(csv_files)} arquivo(s) carregado(s) no bronze.")


def list_cadastral_bronze_files(bucket: str = BUCKET) -> list:
    """Retorna lista de nomes de arquivos CSV presentes no bronze cadastral."""
    s3 = boto3.client("s3", region_name="sa-east-1")
    paginator = s3.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=bucket, Prefix=CADASTRAL_BRONZE_PREFIX):
        for obj in page.get("Contents", []):
            filename = obj["Key"].split("/")[-1]
            if filename.endswith(".csv"):
                files.append(filename)
    return files
