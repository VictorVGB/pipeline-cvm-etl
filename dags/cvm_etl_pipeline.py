import logging
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.extraction import (
    BUCKET,
    BRONZE_PREFIX,
    build_months_to_download,
    download_and_extract,
    get_last_two_months,
    list_existing_files,
)
from utils.transformation import transform_bronze_to_silver, transform_silver_to_gold
from utils.cadastral_extraction import download_cadastral, list_cadastral_bronze_files
from utils.cadastral_transformation import (
    transform_cadastral_bronze_to_silver,
    transform_cadastral_silver_to_gold,
)
from utils.enrichment import enrich_month, list_enriched_months

logger = logging.getLogger(__name__)


def run_extraction(**context):
    existing = list_existing_files(BUCKET, BRONZE_PREFIX)
    all_months = build_months_to_download()
    last_two = set(get_last_two_months())

    to_download = [m for m in all_months if m not in existing or m in last_two]
    logger.info(f"Meses existentes no S3: {sorted(existing)}")
    logger.info(f"Meses para download: {to_download}")

    for year_month in to_download:
        download_and_extract(year_month)


def run_transformation(**context):
    bronze_months = list_existing_files(BUCKET, BRONZE_PREFIX)
    logger.info(f"Meses a transformar: {sorted(bronze_months)}")

    for year_month in sorted(bronze_months):
        logger.info(f"Transformando {year_month}...")
        transform_bronze_to_silver(year_month)
        transform_silver_to_gold(year_month)
        logger.info(f"{year_month} concluído.")


def run_cadastral_extraction(**context):
    logger.info("Iniciando extração cadastral da CVM...")
    download_cadastral(BUCKET)
    logger.info("Extração cadastral concluída.")


def run_cadastral_transformation(**context):
    files = list_cadastral_bronze_files(BUCKET)
    logger.info(f"Arquivos cadastrais a transformar: {files}")

    for filename in files:
        logger.info(f"Transformando cadastral {filename}...")
        transform_cadastral_bronze_to_silver(filename, BUCKET)
        transform_cadastral_silver_to_gold(filename, BUCKET)
        logger.info(f"{filename} concluído.")


def run_enrichment(**context):
    bronze_months = list_existing_files(BUCKET, BRONZE_PREFIX)
    enriched_months = list_enriched_months(BUCKET)
    last_two = set(get_last_two_months())

    to_enrich = [m for m in sorted(bronze_months) if m not in enriched_months or m in last_two]
    logger.info(f"Meses a enriquecer: {to_enrich}")

    for year_month in to_enrich:
        enrich_month(year_month, BUCKET)


with DAG(
    dag_id="cvm_etl_pipeline",
    description="Pipeline ETL de fundos de investimento - CVM",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 1 * *",  # Todo dia 1 do mês às 6h
    catchup=False,
    tags=["cvm", "etl", "fundos"],
    max_active_runs=1,
) as dag:

    extract = PythonOperator(
        task_id="extract_from_cvm",
        python_callable=run_extraction,
    )

    transform = PythonOperator(
        task_id="transform_bronze_to_gold",
        python_callable=run_transformation,
    )

    extract_cadastral = PythonOperator(
        task_id="extract_cadastral",
        python_callable=run_cadastral_extraction,
    )

    transform_cadastral = PythonOperator(
        task_id="transform_cadastral",
        python_callable=run_cadastral_transformation,
    )

    enrich = PythonOperator(
        task_id="enrich_gold",
        python_callable=run_enrichment,
    )

    extract >> transform >> enrich
    extract_cadastral >> transform_cadastral >> enrich
