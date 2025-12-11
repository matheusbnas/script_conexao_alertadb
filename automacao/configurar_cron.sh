#!/bin/bash
# ============================================================================
# Script para Configurar Cron - Sincronização Automática
# ============================================================================
#
# Este script configura o cron para executar a sincronização automaticamente
# a cada 5 minutos. Suporta sincronização normal e Cloud SQL.
#
# ============================================================================
# USO:
# ============================================================================
#
# Configurar sincronização normal:
#   ./configurar_cron.sh normal
#   ou apenas: ./configurar_cron.sh
#
# Configurar sincronização Cloud SQL:
#   ./configurar_cron.sh cloudsql
#
# ============================================================================

# Tipo de sincronização (normal, cloudsql, bigquery ou bigquery_servidor166)
TIPO="${1:-normal}"

# Validar tipo
if [ "$TIPO" != "normal" ] && [ "$TIPO" != "cloudsql" ] && [ "$TIPO" != "bigquery" ] && [ "$TIPO" != "bigquery_servidor166" ]; then
    echo "❌ ERRO: Tipo inválido. Use 'normal', 'cloudsql', 'bigquery' ou 'bigquery_servidor166'"
    echo "Uso: $0 [normal|cloudsql|bigquery|bigquery_servidor166]"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CRON_SCRIPT="$SCRIPT_DIR/cron.sh"
CRON_JOB="*/5 * * * * $CRON_SCRIPT $TIPO"

# Mensagens baseadas no tipo
if [ "$TIPO" = "cloudsql" ]; then
    TIPO_DESCRICAO="Cloud SQL"
elif [ "$TIPO" = "bigquery" ]; then
    TIPO_DESCRICAO="BigQuery (NIMBUS)"
elif [ "$TIPO" = "bigquery_servidor166" ]; then
    TIPO_DESCRICAO="BigQuery (Servidor 166)"
else
    TIPO_DESCRICAO="Normal (Servidor 166)"
fi

echo "============================================================================"
echo "Configuração de Cron - Sincronização $TIPO_DESCRICAO"
echo "============================================================================"
echo ""
echo "Script de cron: $CRON_SCRIPT"
echo "Tipo: $TIPO_DESCRICAO"
echo "Intervalo: A cada 5 minutos"
echo ""
echo "Entrada a adicionar no crontab:"
echo "$CRON_JOB"
echo ""

# Verificar se script existe
if [ ! -f "$CRON_SCRIPT" ]; then
    echo "❌ ERRO: Script não encontrado: $CRON_SCRIPT"
    exit 1
fi

# Tornar executável
chmod +x "$CRON_SCRIPT"
echo "✅ Script de cron tornando executável..."

# Verificar se já existe no crontab
if crontab -l 2>/dev/null | grep -q "$CRON_SCRIPT.*$TIPO"; then
    echo "⚠️  ATENÇÃO: Já existe entrada no crontab para este tipo de sincronização."
    echo "   Entrada existente:"
    crontab -l 2>/dev/null | grep "$CRON_SCRIPT.*$TIPO"
    echo ""
    read -p "Deseja substituir? (s/N): " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Ss]$ ]]; then
        echo "Operação cancelada."
        exit 0
    fi
    # Remover entrada existente
    crontab -l 2>/dev/null | grep -v "$CRON_SCRIPT.*$TIPO" | crontab -
fi

# Adicionar ao crontab
(crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -

echo ""
echo "✅ Entrada adicionada ao crontab com sucesso!"
echo ""
echo "Para verificar:"
echo "  crontab -l"
echo ""
echo "Para remover:"
echo "  crontab -e"
echo "  (remova a linha correspondente)"
echo ""
echo "============================================================================"

