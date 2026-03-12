#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔄 WORKFLOW PREFECT COMBINADO - Sincronização BigQuery

Este workflow combina sincronização de pluviométricos e meteorológicos em um único
deployment, útil quando há limite de deployments no Prefect Cloud.

Tabelas:
- pluviometricos
- meteorologicos
"""

import os
import sys
# Com --run-once: usar API efêmera para não contactar Prefect Cloud (evita 401)
if '--run-once' in sys.argv:
    os.environ["PREFECT_API_URL"] = ""
    os.environ["PREFECT_SERVER_ALLOW_EPHEMERAL_MODE"] = "true"
# Configuração Prefect: Use Cloud ou Local
# os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"  # Descomente para Prefect Local

from prefect import flow, task
from prefect.exceptions import PrefectHTTPStatusError
from pathlib import Path
from datetime import datetime

# Adicionar diretório atual ao path para imports
sys.path.insert(0, str(Path(__file__).parent))

# Importar tasks e helpers comuns
from prefect_common_tasks import verificar_conexao_nimbus, verificar_credenciais_gcp
from prefect_helpers import executar_script_sincronizacao, verificar_status_bigquery_tabela

# Caminho base do projeto (scripts/prefect -> scripts -> raiz)
project_root = Path(__file__).parent.parent.parent

@task(name="Sincronização Incremental Pluviométricos", log_prints=True, retries=2, retry_delay_seconds=60)
def sincronizar_pluviometricos_incremental() -> dict:
    """Executa a sincronização incremental de dados pluviométricos."""
    script_path = project_root / 'scripts' / 'bigquery' / 'sincronizar_pluviometricos_nimbus_bigquery.py'
    return executar_script_sincronizacao(script_path, 'pluviometricos')

@task(name="Sincronização Incremental Meteorológicos", log_prints=True, retries=2, retry_delay_seconds=60)
def sincronizar_meteorologicos_incremental() -> dict:
    """Executa a sincronização incremental de dados meteorológicos."""
    script_path = project_root / 'scripts' / 'bigquery' / 'sincronizar_meteorologicos_nimbus_bigquery.py'
    return executar_script_sincronizacao(script_path, 'meteorologicos')

@task(name="Verificar Status BigQuery Pluviométricos", log_prints=True)
def verificar_status_bigquery_pluviometricos() -> dict:
    """Verifica o status da tabela pluviometricos no BigQuery."""
    credentials_path = project_root / 'credentials' / 'credentials.json'
    return verificar_status_bigquery_tabela(credentials_path, 'alertadb_cor_raw', 'pluviometricos')

@task(name="Verificar Status BigQuery Meteorológicos", log_prints=True)
def verificar_status_bigquery_meteorologicos() -> dict:
    """Verifica o status da tabela meteorologicos no BigQuery."""
    credentials_path = project_root / 'credentials' / 'credentials.json'
    return verificar_status_bigquery_tabela(credentials_path, 'alertadb_cor_raw', 'meteorologicos')

@flow(name="Sincronização BigQuery - Combinada", log_prints=True)
def sincronizacao_combinada_flow() -> dict:
    """Flow combinado que executa sincronização de ambos os tipos de dados.
    
    Útil quando há limite de deployments no Prefect Cloud.
    """
    print("=" * 80)
    print("🔄 SINCRONIZAÇÃO COMBINADA - BigQuery")
    print("=" * 80)
    print(f"⏰ Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📊 Tabelas: pluviometricos + meteorologicos")
    print()
    
    # 1. Verificar conexões
    print("📡 Verificando conexões...")
    conexao_nimbus = verificar_conexao_nimbus()
    credenciais_gcp = verificar_credenciais_gcp()
    
    if not conexao_nimbus or not credenciais_gcp:
        print("❌ Falha na verificação de conexões. Abortando flow.")
        return {
            'sucesso': False,
            'mensagem': 'Falha na verificação de conexões',
            'timestamp': datetime.now().isoformat()
        }
    
    print()
    
    # 2. Executar sincronização PLUVIOMÉTRICOS
    print("🌧️  Executando sincronização PLUVIOMÉTRICOS...")
    resultado_pluvio = sincronizar_pluviometricos_incremental()
    sucesso_pluvio = resultado_pluvio.get('sucesso', False)
    
    print()
    
    # 3. Executar sincronização METEOROLÓGICOS
    print("🌤️  Executando sincronização METEOROLÓGICOS...")
    resultado_meteo = sincronizar_meteorologicos_incremental()
    sucesso_meteo = resultado_meteo.get('sucesso', False)
    
    print()
    
    # 4. Verificar status final
    print("📊 Verificando status final no BigQuery...")
    status_pluvio = verificar_status_bigquery_pluviometricos()
    status_meteo = verificar_status_bigquery_meteorologicos()
    
    # Compilar erros e avisos
    todos_erros = []
    todos_avisos = []
    
    if resultado_pluvio.get('erros_detectados'):
        todos_erros.extend([f"PLUVIOMÉTRICOS: {e}" for e in resultado_pluvio.get('erros_detectados', [])])
    if resultado_pluvio.get('avisos'):
        todos_avisos.extend([f"PLUVIOMÉTRICOS: {a}" for a in resultado_pluvio.get('avisos', [])])
    
    if resultado_meteo.get('erros_detectados'):
        todos_erros.extend([f"METEOROLÓGICOS: {e}" for e in resultado_meteo.get('erros_detectados', [])])
    if resultado_meteo.get('avisos'):
        todos_avisos.extend([f"METEOROLÓGICOS: {a}" for a in resultado_meteo.get('avisos', [])])
    
    if not status_pluvio.get('sucesso', False):
        todos_erros.append("PLUVIOMÉTRICOS: Erro ao verificar status BigQuery")
    if not status_meteo.get('sucesso', False):
        todos_erros.append("METEOROLÓGICOS: Erro ao verificar status BigQuery")
    
    print()
    print("=" * 80)
    
    # Resumo final
    sucesso_geral = sucesso_pluvio and sucesso_meteo and not todos_erros
    
    if sucesso_geral:
        print("✅ SINCRONIZAÇÃO COMBINADA CONCLUÍDA COM SUCESSO")
        if todos_avisos:
            print(f"⚠️  Avisos: {len(todos_avisos)}")
            for aviso in todos_avisos:
                print(f"   - {aviso}")
    elif sucesso_pluvio and sucesso_meteo and todos_erros:
        print("⚠️  SINCRONIZAÇÃO COMBINADA CONCLUÍDA COM ERROS")
        print(f"❌ Erros detectados: {len(todos_erros)}")
        for erro in todos_erros:
            print(f"   - {erro}")
    else:
        print("❌ SINCRONIZAÇÃO COMBINADA FALHOU")
        print(f"   🌧️  Pluviométricos: {'✅' if sucesso_pluvio else '❌'}")
        print(f"   🌤️  Meteorológicos: {'✅' if sucesso_meteo else '❌'}")
        print(f"❌ Erros detectados: {len(todos_erros)}")
        for erro in todos_erros:
            print(f"   - {erro}")
    
    print("=" * 80)
    print(f"⏰ Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Informações de resumo
    if status_pluvio.get('sucesso'):
        print(f"📊 Status BigQuery Pluviométricos:")
        print(f"   Total de registros: {status_pluvio.get('total_registros', 0):,}")
        print(f"   Data máxima: {status_pluvio.get('data_maxima', 'N/A')}")
        print(f"   Total de estações: {status_pluvio.get('total_estacoes', 0)}")
    
    if status_meteo.get('sucesso'):
        print(f"📊 Status BigQuery Meteorológicos:")
        print(f"   Total de registros: {status_meteo.get('total_registros', 0):,}")
        print(f"   Data máxima: {status_meteo.get('data_maxima', 'N/A')}")
        print(f"   Total de estações: {status_meteo.get('total_estacoes', 0)}")
    
    if resultado_pluvio.get('registros_processados', 0) > 0:
        print(f"🌧️  Registros pluviométricos processados: {resultado_pluvio.get('registros_processados', 0):,}")
    
    if resultado_meteo.get('registros_processados', 0) > 0:
        print(f"🌤️  Registros meteorológicos processados: {resultado_meteo.get('registros_processados', 0):,}")
    
    return {
        'sucesso': sucesso_geral,
        'pluviometricos': {
            'sincronizacao': resultado_pluvio,
            'status_bigquery': status_pluvio
        },
        'meteorologicos': {
            'sincronizacao': resultado_meteo,
            'status_bigquery': status_meteo
        },
        'erros': todos_erros,
        'avisos': todos_avisos,
        'timestamp': datetime.now().isoformat()
    }

if __name__ == "__main__":
    """
    Configuração de Execução:
    
    OPÇÃO 1: Execução direta (sem criar deployment) - Para testes
    - Descomente a linha abaixo e comente o .serve()
    - sincronizacao_combinada_flow()
    
    OPÇÃO 2: Prefect Cloud (cria 1 deployment para ambos)
    - prefect deploy prefect_workflow_combinado.py:sincronizacao_combinada_flow --pool seu-work-pool
    
    OPÇÃO 3: Prefect Local
    - Descomente: os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"
    - prefect server start
    - Execute este script
    """
    
    # Para executar sem criar deployment (teste local)
    if '--run-once' in sys.argv:
        print("🔄 Executando sincronização combinada (sem criar deployment)...")
        sincronizacao_combinada_flow()
    else:
        try:
            # Criar deployment (usa apenas 1 deployment para ambos os tipos)
            sincronizacao_combinada_flow.serve(
                name="sincronizacao-bigquery-combinada",
                cron="*/5 * * * *",  # A cada 5 minutos
                description="Sincronização incremental combinada de dados pluviométricos e meteorológicos do NIMBUS para BigQuery. Usa apenas 1 deployment para ambos os tipos."
            )
        except PrefectHTTPStatusError as e:
            if e.response.status_code == 401:
                print("\n⚠️  Erro 401 Unauthorized: autenticação com Prefect Cloud falhou.")
                print("   Opções:")
                print("   1. Fazer login:  prefect cloud login")
                print("   2. Executar sem deployment:  python prefect_workflow_combinado.py --run-once")
                print("   3. Usar Prefect Local: descomente PREFECT_API_URL no script e rode 'prefect server start'\n")
            raise

