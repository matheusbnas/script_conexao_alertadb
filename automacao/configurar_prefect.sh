#!/bin/bash
set -e

# Configurador de automação Prefect (modo local API).
#
# Uso:
#   ./configurar_prefect.sh
#   ./configurar_prefect.sh local-api
#
# Este script mantém apenas o modo local-api. O monitor via cron foi removido
# porque o fluxo oficial agora é Prefect + Docker com alerta de falha por e-mail
# diretamente no scripts/prefect/service.py.

MODO="${1:-local-api}"

case "$MODO" in
  local-api)
    echo "🔧 Configurando Prefect para usar servidor local..."
    prefect config unset PREFECT_API_URL 2>/dev/null || true
    prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
    echo ""
    echo "✅ Configuração aplicada."
    echo "📋 Valor atual:"
    prefect config view | rg "PREFECT_API_URL" || echo "⚠️ Prefect não está no PATH."
    ;;

  *)
    echo "❌ Modo inválido: $MODO"
    echo "Uso: $0 [local-api]"
    exit 1
    ;;
esac

