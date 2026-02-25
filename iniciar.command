#!/bin/bash

# Vai para a pasta do projeto
cd "$(dirname "$0")"

echo "🚀 Iniciando pipeline CVM..."

echo ""
echo "▶ Subindo Airflow e banco de dados..."
docker compose up -d

echo ""
echo "▶ Criando estrutura do bucket S3..."
python3 scripts/setup_s3.py

echo ""
echo "▶ Iniciando dashboard Streamlit..."
pkill -f "streamlit run" 2>/dev/null
sleep 1
streamlit run dashboard/dashboard.py --server.headless true --server.port 8501 &

echo ""
echo "✅ Tudo pronto!"
echo ""
echo "   Airflow:   http://localhost:8080  (admin / admin)"
echo "   Dashboard: http://localhost:8501"
echo ""

# Abre os dois no navegador após 5 segundos
sleep 5
open http://localhost:8080
open http://localhost:8501
