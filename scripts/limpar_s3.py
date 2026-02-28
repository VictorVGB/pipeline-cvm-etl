"""
Apaga arquivos do bucket S3 por camada ou tudo.
Uso:
    python3 scripts/limpar_s3.py --camada bronze
    python3 scripts/limpar_s3.py --camada silver
    python3 scripts/limpar_s3.py --camada gold
    python3 scripts/limpar_s3.py --tudo
"""
import argparse
import os
from pathlib import Path

import boto3

# Carrega .env
_env_path = Path(__file__).parent.parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _key, _, _value = _line.partition("=")
            os.environ.setdefault(_key.strip(), _value.strip())

BUCKET = os.environ.get("S3_BUCKET", "projeto-dados-cvm")
REGION = os.environ.get("AWS_DEFAULT_REGION", "sa-east-1")

PREFIXOS = {
    "bronze":   ["informes-diario/bronze", "informacoes-cadastrais/bronze"],
    "silver":   ["informes-diario/silver", "informacoes-cadastrais/silver"],
    "gold":     ["informes-diario/gold",   "informacoes-cadastrais/gold"],
    "enriched": ["informes-enriquecido/gold"],
}


def deletar_prefixo(s3, prefixo: str):
    paginator = s3.get_paginator("list_objects_v2")
    objetos = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefixo):
        for obj in page.get("Contents", []):
            objetos.append({"Key": obj["Key"]})

    if not objetos:
        print(f"  Nenhum arquivo encontrado em {prefixo}/")
        return

    for i in range(0, len(objetos), 1000):
        lote = objetos[i:i + 1000]
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": lote})

    print(f"  {len(objetos)} arquivo(s) removido(s) de s3://{BUCKET}/{prefixo}/")


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--camada", choices=["bronze", "silver", "gold", "enriched"])
    group.add_argument("--tudo", action="store_true")
    args = parser.parse_args()

    s3 = boto3.client("s3", region_name=REGION)

    camadas = ["bronze", "silver", "gold", "enriched"] if args.tudo else [args.camada]

    for camada in camadas:
        print(f"Apagando camada {camada}...")
        for prefixo in PREFIXOS[camada]:
            deletar_prefixo(s3, prefixo)

    print("\nConcluído.")


if __name__ == "__main__":
    main()
