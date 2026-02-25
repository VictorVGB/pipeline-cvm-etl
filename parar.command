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
echo "✅ Todos os serviços encerrados."
