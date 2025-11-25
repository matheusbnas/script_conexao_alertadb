#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🌧️ PREFECT FLOW - Automação de Sincronização Incremental

Este arquivo define um flow Prefect para automatizar a execução do
sincronizar_pluviometricos_novos.py em intervalos regulares.

═══════════════════════════════════════════════════════════════════════════
🚀 COMO USAR:
═══════════════════════════════════════════════════════════════════════════

1. Instalar Prefect:
   pip install prefect

2. Configurar Prefect (opcional, para UI):
   prefect server start

3. Executar o flow:
   python prefect_flow.py

4. Ou registrar como deployment:
   prefect deploy prefect_flow.py:sync_pluviometricos

═══════════════════════════════════════════════════════════════════════════
📋 CONFIGURAÇÃO:
═══════════════════════════════════════════════════════════════════════════

O intervalo de execução pode ser configurado via variável de ambiente:
- PREFECT_INTERVALO_MINUTOS (padrão: 5 minutos)

Ou modificando diretamente o parâmetro 'interval_minutes' no decorador @flow.
"""

import os
from datetime import timedelta
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Importar função do sincronizador
import sys
import os
# Adicionar diretório scripts ao path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'scripts'))
from sincronizar_pluviometricos_novos import executar_sincronizacao_unica, testar_conexoes

# Carregar variáveis de ambiente (busca .env na raiz do projeto)
from pathlib import Path
# Obter diretório raiz do projeto (2 níveis acima deste arquivo)
project_root = Path(__file__).parent.parent
load_dotenv(dotenv_path=project_root / '.env')

# Intervalo padrão: 5 minutos
INTERVALO_MINUTOS = int(os.getenv('PREFECT_INTERVALO_MINUTOS', '5'))


@task(
    name="testar_conexoes_db",
    description="Testa conexões com bancos de dados origem e destino",
    retries=2,
    retry_delay_seconds=30
)
def task_testar_conexoes():
    """Task para testar conexões com os bancos de dados."""
    return testar_conexoes()


@task(
    name="sincronizar_dados_pluviometricos",
    description="Sincroniza dados pluviométricos incrementais",
    retries=3,
    retry_delay_seconds=60,
    log_prints=True
)
def task_sincronizar_dados():
    """Task para executar a sincronização incremental."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Iniciando sincronização...")
    registros = executar_sincronizacao_unica()
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Sincronização concluída: {registros} registros")
    return registros


@flow(
    name="sync_pluviometricos",
    description="Flow para sincronização automática de dados pluviométricos",
    log_prints=True
)
def sync_pluviometricos_flow():
    """
    Flow principal que orquestra a sincronização incremental.
    
    Este flow:
    1. Testa conexões com os bancos de dados
    2. Executa sincronização incremental
    3. Registra resultados
    """
    print("=" * 70)
    print("🌧️ PREFECT FLOW - Sincronização Incremental")
    print("=" * 70)
    print(f"Iniciado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Testar conexões
    conexao_ok = task_testar_conexoes()
    
    if not conexao_ok:
        print("❌ Falha nos testes de conexão. Abortando flow...")
        return 0
    
    # Executar sincronização
    registros = task_sincronizar_dados()
    
    print()
    print("=" * 70)
    print(f"✅ Flow concluído: {registros} registros sincronizados")
    print(f"Finalizado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    return registros


if __name__ == "__main__":
    # Executar o flow
    sync_pluviometricos_flow()

