#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🌧️ PREFECT DEPLOYMENT - Configuração de Deployment

Este arquivo configura um deployment Prefect para executar o flow
automaticamente em intervalos regulares.

═══════════════════════════════════════════════════════════════════════════
🚀 COMO USAR:
═══════════════════════════════════════════════════════════════════════════

1. Certifique-se de que o Prefect está instalado e configurado
2. Execute este script para criar o deployment:
   python prefect_deployment.py

3. O deployment será criado e executará automaticamente a cada 5 minutos

4. Para visualizar e gerenciar:
   - Acesse a UI do Prefect (se estiver rodando)
   - Ou use a CLI: prefect deployment ls
"""

from prefect import serve
import os
import sys
from dotenv import load_dotenv

# Adicionar diretório scripts ao path para imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'scripts'))

# Importar do mesmo diretório
from prefect_flow import sync_pluviometricos_flow

# Carregar variáveis de ambiente (busca .env na raiz do projeto)
from pathlib import Path
# Obter diretório raiz do projeto (2 níveis acima deste arquivo)
project_root = Path(__file__).parent.parent
load_dotenv(dotenv_path=project_root / '.env')

# Intervalo padrão: 5 minutos
INTERVALO_MINUTOS = int(os.getenv('PREFECT_INTERVALO_MINUTOS', '5'))


if __name__ == "__main__":
    print("=" * 70)
    print("🌧️ CRIANDO DEPLOYMENT PREFECT")
    print("=" * 70)
    print(f"Intervalo de execução: {INTERVALO_MINUTOS} minutos")
    print()
    print("Para iniciar o servidor Prefect:")
    print("  prefect server start")
    print()
    print("Para executar este deployment:")
    print("  prefect deployment run sync-pluviometricos/sync-pluviometricos")
    print()
    print("=" * 70)
    
    # Criar deployment com execução automática
    sync_pluviometricos_flow.serve(
        name="sync-pluviometricos",
        description="Sincronização automática de dados pluviométricos",
        interval_minutes=INTERVALO_MINUTOS,
        tags=["pluviometricos", "sincronizacao", "incremental"]
    )

