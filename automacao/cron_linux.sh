#!/bin/bash
# ============================================================================
# Script de Automação para Linux/Unix - Sincronização Incremental
# ============================================================================
#
# Este script pode ser usado com cron para executar a sincronização
# incremental automaticamente após a carga inicial dos dados históricos.
#
# ============================================================================
# CONFIGURAÇÃO:
# ============================================================================
#
# 1. Torne o script executável: chmod +x cron_linux.sh
# 2. Adicione ao crontab:
#    crontab -e
#    Adicione a linha:
#    */5 * * * * /caminho/completo/para/automacao/cron_linux.sh
#
# ============================================================================

# Detectar Python automaticamente (tenta python3, depois python)
if command -v python3 &> /dev/null; then
    PYTHON_PATH=$(command -v python3)
elif command -v python &> /dev/null; then
    PYTHON_PATH=$(command -v python)
else
    echo "❌ ERRO: Python não encontrado. Instale Python 3." >&2
    exit 1
fi

# Caminho relativo ao diretório raiz do projeto (2 níveis acima deste script)
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPT_PATH="$PROJECT_ROOT/scripts/sincronizar_pluviometricos_novos.py"
WORK_DIR="$PROJECT_ROOT"
LOG_DIR="$WORK_DIR/logs"
LOG_FILE="$LOG_DIR/sincronizacao_$(date +%Y%m%d_%H%M%S).log"

# Verificar se o script existe
if [ ! -f "$SCRIPT_PATH" ]; then
    echo "❌ ERRO: Script não encontrado: $SCRIPT_PATH" >&2
    exit 1
fi

# Criar diretório de logs se não existir
mkdir -p "$LOG_DIR"

# Mudar para o diretório de trabalho
cd "$WORK_DIR" || exit 1

# Executar script em modo único (--once)
echo "==========================================" >> "$LOG_FILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Iniciando sincronização incremental..." >> "$LOG_FILE"
echo "Python: $PYTHON_PATH" >> "$LOG_FILE"
echo "Script: $SCRIPT_PATH" >> "$LOG_FILE"
echo "Diretório: $WORK_DIR" >> "$LOG_FILE"
echo "==========================================" >> "$LOG_FILE"

"$PYTHON_PATH" "$SCRIPT_PATH" --once >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Sincronização concluída. Código de saída: $EXIT_CODE" >> "$LOG_FILE"
echo "==========================================" >> "$LOG_FILE"

exit $EXIT_CODE

