#!/bin/bash

# Script para configurar Prefect para usar servidor local
# Execute este script antes de usar o workflow

echo "🔧 Configurando Prefect para usar servidor local..."
echo ""

# Limpar configuração Cloud (se existir)
prefect config unset PREFECT_API_URL 2>/dev/null

# Configurar URL da API para servidor local
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"

echo "✅ Configuração aplicada!"
echo ""
echo "📋 Verificando configuração:"
prefect config view | grep PREFECT_API_URL || echo "⚠️  Prefect não está instalado ou não está no PATH"
echo ""
echo "🚀 Próximos passos:"
echo "   1. Inicie o servidor Prefect em um terminal separado:"
echo "      prefect server start"
echo ""
echo "   2. Em outro terminal, execute o workflow:"
echo "      python scripts/bigquery/prefect_workflow_bigquery.py"
echo ""
echo "   3. Acesse a interface web em: http://localhost:4200"
echo ""

