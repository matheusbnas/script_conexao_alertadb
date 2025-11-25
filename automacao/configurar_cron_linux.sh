#!/bin/bash
# ============================================================================
# Script para Configurar Cron no Linux/Unix
# ============================================================================
#
# Este script ajuda a configurar o cron para executar a sincronização
# incremental automaticamente a cada 5 minutos.
#
# ============================================================================
# USO:
# ============================================================================
#
# 1. Torne o script executável: chmod +x configurar_cron_linux.sh
# 2. Execute: ./configurar_cron_linux.sh
# 3. O script adicionará a entrada ao crontab automaticamente
#
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CRON_SCRIPT="$SCRIPT_DIR/cron_linux.sh"
CRON_JOB="*/5 * * * * $CRON_SCRIPT"

echo "============================================================================"
echo "Configuração de Cron - Sincronização Incremental"
echo "============================================================================"
echo ""
echo "Script de cron: $CRON_SCRIPT"
echo "Intervalo: A cada 5 minutos"
echo ""
echo "A entrada a ser adicionada ao crontab:"
echo "$CRON_JOB"
echo ""

# Verificar se o script existe
if [ ! -f "$CRON_SCRIPT" ]; then
    echo "❌ ERRO: Script de cron não encontrado: $CRON_SCRIPT"
    exit 1
fi

# Tornar o script executável
chmod +x "$CRON_SCRIPT"
echo "✅ Script de cron tornando executável..."

# Verificar se já existe a entrada no crontab
if crontab -l 2>/dev/null | grep -q "$CRON_SCRIPT"; then
    echo "⚠️  ATENÇÃO: Já existe uma entrada no crontab para este script."
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

# Adicionar entrada ao crontab
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

