#!/bin/bash
# ============================================================================
# Script para Configurar Cron - Sincronização Cloud SQL
# ============================================================================
#
# Este script configura o cron para executar a sincronização com Cloud SQL
# automaticamente a cada 5 minutos.
#
# IMPORTANTE: Adaptado do configurar_cron_linux.sh existente no projeto
#
# ============================================================================
# USO:
# ============================================================================
#
# 1. Torne executável: chmod +x configurar_cron_cloudsql.sh
# 2. Execute: ./configurar_cron_cloudsql.sh
# 3. O cron será configurado automaticamente
#
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CRON_SCRIPT="$SCRIPT_DIR/cron_cloudsql.sh"
CRON_JOB="*/5 * * * * $CRON_SCRIPT"

echo "============================================================================"
echo "Configuração de Cron - Sincronização Cloud SQL"
echo "============================================================================"
echo ""
echo "Script de cron: $CRON_SCRIPT"
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
echo "✅ Script tornando executável..."

# Verificar se já existe no crontab
if crontab -l 2>/dev/null | grep -q "$CRON_SCRIPT"; then
    echo "⚠️  ATENÇÃO: Já existe entrada no crontab para este script."
    echo "   Entrada existente:"
    crontab -l 2>/dev/null | grep "$CRON_SCRIPT"
    echo ""
    read -p "Deseja substituir? (s/N): " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Ss]$ ]]; then
        echo "Operação cancelada."
        exit 0
    fi
    # Remover entrada existente
    crontab -l 2>/dev/null | grep -v "$CRON_SCRIPT" | crontab -
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
