#!/bin/bash
set -e

# Configurador de automacao Prefect + monitor de container pluviometrico.
#
# Modos:
#   ./configurar_prefect.sh local-api
#   ./configurar_prefect.sh instalar-monitor
#   ./configurar_prefect.sh remover-monitor

MODO="${1:-instalar-monitor}"
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MONITOR_SCRIPT="$PROJECT_ROOT/automacao/monitor_prefect_pluvio.sh"
CRON_JOB="*/5 * * * * $MONITOR_SCRIPT"

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

  instalar-monitor)
    echo "🔧 Instalando monitor automático do container pluviométrico..."
    if [ ! -f "$MONITOR_SCRIPT" ]; then
      echo "❌ Script não encontrado: $MONITOR_SCRIPT"
      exit 1
    fi

    chmod +x "$MONITOR_SCRIPT"

    # Remove entrada antiga para evitar duplicidade
    crontab -l 2>/dev/null | rg -v "$MONITOR_SCRIPT" | crontab - || true
    (crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -

    echo "✅ Monitor instalado no cron."
    echo ""
    echo "Linha adicionada:"
    echo "  $CRON_JOB"
    echo ""
    echo "📧 Para alerta por e-mail Gmail, adicione no .env:"
    echo "  PREFECT_ALERT_EMAIL_TO=matheusbnas@gmail.com"
    echo "  SMTP_HOST=smtp.gmail.com"
    echo "  SMTP_PORT=587"
    echo "  SMTP_USER=seu_email@gmail.com"
    echo "  SMTP_APP_PASSWORD=sua_app_password_gmail"
    echo "  ALERT_FROM=seu_email@gmail.com"
    echo ""
    echo "📋 Verificar cron:"
    echo "  crontab -l"
    ;;

  remover-monitor)
    echo "🧹 Removendo monitor do cron..."
    crontab -l 2>/dev/null | rg -v "$MONITOR_SCRIPT" | crontab - || true
    echo "✅ Monitor removido."
    ;;

  *)
    echo "❌ Modo inválido: $MODO"
    echo "Uso: $0 [local-api|instalar-monitor|remover-monitor]"
    exit 1
    ;;
esac

