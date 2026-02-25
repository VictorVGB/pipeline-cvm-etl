"""
Cria o bucket S3 projeto-dados-cvm na região sa-east-1.
Execute uma vez antes de rodar o pipeline:
    python scripts/setup_s3.py
"""
import boto3
import os
from pathlib import Path
from botocore.exceptions import ClientError

# Carrega o .env da raiz do projeto
env_path = Path(__file__).parent.parent / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())

BUCKET = os.environ.get("S3_BUCKET", "projeto-dados-cvm")
REGION = os.environ.get("AWS_DEFAULT_REGION", "sa-east-1")


def create_bucket():
    s3 = boto3.client("s3", region_name=REGION)

    try:
        s3.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": REGION},
        )
        print(f"Bucket '{BUCKET}' criado em {REGION}.")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            print(f"Bucket '{BUCKET}' já existe.")
        else:
            raise

    # Cria objetos placeholder para documentar a estrutura
    for prefix in ["bronze/", "silver/", "gold/"]:
        s3.put_object(Bucket=BUCKET, Key=prefix)
        print(f"  Prefixo criado: s3://{BUCKET}/{prefix}")

    print("\nSetup concluído. Estrutura do bucket:")
    print(f"  s3://{BUCKET}/bronze/  <- CSVs raw da CVM")
    print(f"  s3://{BUCKET}/silver/  <- Parquet limpo")
    print(f"  s3://{BUCKET}/gold/    <- Parquet modelado para análise")


if __name__ == "__main__":
    create_bucket()
