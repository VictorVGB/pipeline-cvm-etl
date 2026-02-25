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

    extract >> transform
