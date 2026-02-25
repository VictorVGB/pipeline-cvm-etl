#!/bin/bash

# Vai para a pasta do projeto
cd "$(dirname "$0")"

echo "🛑 Parando todos os serviços..."

echo ""
echo "▶ Parando Streamlit..."
pkill -f "streamlit run" && echo "   Streamlit encerrado." || echo "   Streamlit já estava parado."

echo ""
echo "▶ Parando Airflow e banco de dados..."
docker compose down

echo ""
echo "▶ Deseja limpar os dados do S3?"
echo "   1) Não apagar nada"
echo "   2) Apagar tudo (bronze + silver + gold)"
echo "   3) Apagar apenas bronze (arquivos raw)"
echo "   4) Apagar apenas silver"
echo "   5) Apagar apenas gold"
echo ""
read -p "Escolha (1-5): " opcao

case $opcao in
    2) python3 scripts/limpar_s3.py --tudo ;;
    3) python3 scripts/limpar_s3.py --camada bronze ;;
    4) python3 scripts/limpar_s3.py --camada silver ;;
    5) python3 scripts/limpar_s3.py --camada gold ;;
    *) echo "   S3 mantido sem alterações." ;;
esac

echo ""
echo "✅ Todos os serviços encerrados."
