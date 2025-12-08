#!/bin/bash
# ============================================================================
# Script de Automação - Sincronização Incremental Cloud SQL
# ============================================================================
#
# Este script executa a sincronização incremental do servidor 166 para
# o Cloud SQL GCP automaticamente via cron.
#
# IMPORTANTE: Adaptado do cron_linux.sh existente no projeto
#
# ============================================================================
# CONFIGURAÇÃO:
# ============================================================================
#
# 1. Torne executável: chmod +x cron_cloudsql.sh
# 2. Adicione ao crontab:
#    crontab -e
#    Adicione: */5 * * * * /caminho/completo/automacao/cron_cloudsql.sh
#
# ============================================================================

# Detectar Python automaticamente
if command -v python3 &> /dev/null; then
    PYTHON_PATH=$(command -v python3)
elif command -v python &> /dev/null; then
    PYTHON_PATH=$(command -v python)
else
    echo "❌ ERRO: Python não encontrado" >&2
    exit 1
fi

# Caminho relativo ao diretório raiz do projeto
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPT_PATH="$PROJECT_ROOT/scripts/sincronizar_para_cloudsql.py"
WORK_DIR="$PROJECT_ROOT"
LOG_DIR="$WORK_DIR/logs"
LOG_FILE="$LOG_DIR/cloudsql_$(date +%Y%m%d_%H%M%S).log"

# Verificar se script existe
if [ ! -f "$SCRIPT_PATH" ]; then
    echo "❌ ERRO: Script não encontrado: $SCRIPT_PATH" >&2
    exit 1
fi

# Criar diretório de logs
mkdir -p "$LOG_DIR"

# Mudar para diretório de trabalho
cd "$WORK_DIR" || exit 1

# Executar script em modo único (--once)
echo "==========================================" >> "$LOG_FILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Iniciando sincronização Cloud SQL..." >> "$LOG_FILE"
echo "Python: $PYTHON_PATH" >> "$LOG_FILE"
echo "Script: $SCRIPT_PATH" >> "$LOG_FILE"
echo "Diretório: $WORK_DIR" >> "$LOG_FILE"
echo "==========================================" >> "$LOG_FILE"

"$PYTHON_PATH" "$SCRIPT_PATH" --once >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Sincronização concluída. Código: $EXIT_CODE" >> "$LOG_FILE"
echo "==========================================" >> "$LOG_FILE"

exit $EXIT_CODE
