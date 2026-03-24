#!/bin/bash
# ============================================================================
# Script de Automação - Sincronização Incremental
# ============================================================================
#
# Este script executa a sincronização incremental automaticamente via cron.
#
# ============================================================================
# USO:
# ============================================================================
#
# Sincronização Normal (servidor 166):
#   ./cron.sh normal
#   ou apenas: ./cron.sh
#
# Sincronização BigQuery:
#   ./cron.sh bigquery
#   ./cron.sh bigquery_servidor166
#
# ============================================================================
# CONFIGURAÇÃO NO CRONTAB:
# ============================================================================
#
# Normal:             */5 * * * * /caminho/completo/automacao/cron.sh normal
# BigQuery:           */5 * * * * /caminho/completo/automacao/cron.sh bigquery
# BigQuery srv166:    */5 * * * * /caminho/completo/automacao/cron.sh bigquery_servidor166
#
# ============================================================================

# Tipo de sincronização (normal, bigquery ou bigquery_servidor166)
TIPO="${1:-normal}"

# Validar tipo
if [ "$TIPO" != "normal" ] && [ "$TIPO" != "bigquery" ] && [ "$TIPO" != "bigquery_servidor166" ]; then
    echo "❌ ERRO: Tipo inválido. Use 'normal', 'bigquery' ou 'bigquery_servidor166'" >&2
    echo "Uso: $0 [normal|bigquery|bigquery_servidor166]" >&2
    exit 1
fi

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
WORK_DIR="$PROJECT_ROOT"
LOG_DIR="$WORK_DIR/logs"

# Configurar script e log baseado no tipo
if [ "$TIPO" = "bigquery" ]; then
    SCRIPT_PATH="$PROJECT_ROOT/scripts/bigquery/sincronizar_pluviometricos_nimbus_bigquery.py"
    LOG_FILE="$LOG_DIR/bigquery_$(date +%Y%m%d_%H%M%S).log"
    TIPO_DESCRICAO="BigQuery (NIMBUS - Pluviométricos)"
elif [ "$TIPO" = "bigquery_servidor166" ]; then
    SCRIPT_PATH="$PROJECT_ROOT/scripts/bigquery/sincronizar_pluviometricos_servidor166_bigquery.py"
    LOG_FILE="$LOG_DIR/bigquery_servidor166_$(date +%Y%m%d_%H%M%S).log"
    TIPO_DESCRICAO="BigQuery (Servidor 166 - Pluviométricos)"
else
    SCRIPT_PATH="$PROJECT_ROOT/scripts/servidor166/sincronizar_pluviometricos_novos.py"
    LOG_FILE="$LOG_DIR/sincronizacao_$(date +%Y%m%d_%H%M%S).log"
    TIPO_DESCRICAO="Normal"
fi

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
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Iniciando sincronização $TIPO_DESCRICAO..." >> "$LOG_FILE"
echo "Python: $PYTHON_PATH" >> "$LOG_FILE"
echo "Script: $SCRIPT_PATH" >> "$LOG_FILE"
echo "Diretório: $WORK_DIR" >> "$LOG_FILE"
echo "==========================================" >> "$LOG_FILE"

"$PYTHON_PATH" "$SCRIPT_PATH" --once >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Sincronização $TIPO_DESCRICAO concluída. Código: $EXIT_CODE" >> "$LOG_FILE"
echo "==========================================" >> "$LOG_FILE"

exit $EXIT_CODE

