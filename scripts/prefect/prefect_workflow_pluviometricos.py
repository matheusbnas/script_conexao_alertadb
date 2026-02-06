#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🌧️ WORKFLOW PREFECT - Sincronização Pluviométricos BigQuery

Este workflow usa Prefect para orquestrar a sincronização de dados PLUVIOMÉTRICOS
do NIMBUS para o BigQuery, com suporte a execução agendada e monitoramento.

Tabela: pluviometricos
"""

import os
# Configuração Prefect: Use Cloud ou Local
# os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"  # Descomente para Prefect Local

from prefect import flow, task
from pathlib import Path
from datetime import datetime, timezone, timedelta
import sys
import os
from dotenv import load_dotenv

# Adicionar diretório atual ao path para imports
sys.path.insert(0, str(Path(__file__).parent))

# Importar tasks e helpers comuns
from prefect_common_tasks import verificar_conexao_nimbus, verificar_credenciais_gcp
from prefect_helpers import executar_script_sincronizacao, verificar_status_bigquery_tabela

# Caminho base do projeto (scripts/prefect -> scripts -> raiz)
project_root = Path(__file__).parent.parent.parent

# Carregar variáveis de ambiente
load_dotenv(dotenv_path=project_root / '.env')

@task(name="Sincronização Incremental Pluviométricos", log_prints=True, retries=2, retry_delay_seconds=60)
def sincronizar_pluviometricos_incremental() -> dict:
    """Executa a sincronização incremental de dados pluviométricos."""
    script_path = project_root / 'scripts' / 'bigquery' / 'sincronizar_pluviometricos_nimbus_bigquery.py'
    return executar_script_sincronizacao(script_path, 'pluviometricos')

@task(name="Verificar Status BigQuery Pluviométricos", log_prints=True)
def verificar_status_bigquery_pluviometricos() -> dict:
    """Verifica o status da tabela pluviometricos no BigQuery."""
    credentials_path = project_root / 'credentials' / 'credentials.json'
    return verificar_status_bigquery_tabela(credentials_path, 'alertadb_cor_raw', 'pluviometricos')

@task(name="Verificar Lacunas de Dados", log_prints=True)
def verificar_lacunas_dados() -> dict:
    """Verifica se há dados no NIMBUS antes da data atual que ainda não foram coletados.
    
    Detecta lacunas na sincronização, ou seja, dados que existem no NIMBUS
    entre a última sincronização e a data atual, mas que não foram coletados.
    """
    try:
        import psycopg2
        from google.cloud import bigquery
        from google.oauth2 import service_account
        import pytz
        
        # Obter configurações do .env
        def obter_variavel(nome, obrigatoria=True, padrao=None):
            valor = os.getenv(nome)
            if not valor or (isinstance(valor, str) and valor.strip() == ''):
                if obrigatoria:
                    raise ValueError(f"Variável obrigatória não encontrada: {nome}")
                return padrao
            return valor.strip() if isinstance(valor, str) else valor
        
        # Conectar ao BigQuery para obter última sincronização
        credentials_path = project_root / 'credentials' / 'credentials.json'
        if not credentials_path.exists():
            return {
                'sucesso': False,
                'mensagem': 'Credenciais não encontradas',
                'lacunas_detectadas': False
            }
        
        credentials = service_account.Credentials.from_service_account_file(
            str(credentials_path),
            scopes=["https://www.googleapis.com/auth/bigquery"]
        )
        client_bq = bigquery.Client(credentials=credentials, project=credentials.project_id)
        
        # Obter última sincronização do BigQuery
        query_bq = """
        SELECT MAX(dia_utc) as ultima_sincronizacao
        FROM `alertadb_cor_raw.pluviometricos`
        """
        query_job = client_bq.query(query_bq)
        results = query_job.result()
        
        ultima_sync = None
        for row in results:
            if row.ultima_sincronizacao:
                ultima_sync = row.ultima_sincronizacao
                if isinstance(ultima_sync, datetime):
                    # dia_utc está em UTC no BigQuery
                    if ultima_sync.tzinfo is None:
                        ultima_sync = ultima_sync.replace(tzinfo=timezone.utc)
                    elif ultima_sync.tzinfo != timezone.utc:
                        ultima_sync = ultima_sync.astimezone(timezone.utc)
                    break
        
        if not ultima_sync:
            return {
                'sucesso': True,
                'mensagem': 'Tabela vazia - sem dados para verificar lacunas',
                'lacunas_detectadas': False
            }
        
        # Garantir que está em UTC (dia_utc já vem em UTC do BigQuery)
        if isinstance(ultima_sync, datetime):
            if ultima_sync.tzinfo is None:
                ultima_sync = ultima_sync.replace(tzinfo=timezone.utc)
            elif ultima_sync.tzinfo != timezone.utc:
                ultima_sync = ultima_sync.astimezone(timezone.utc)
        
        # Usar horário do Brasil como padrão
        tz_brasil = pytz.timezone('America/Sao_Paulo')
        
        # Data atual em horário do Brasil
        data_atual_brasil = datetime.now(tz_brasil)
        # Converter para UTC para cálculos internos
        data_atual = data_atual_brasil.astimezone(timezone.utc)
        
        # Converter última sincronização para horário do Brasil
        ultima_sync_brasil = ultima_sync.astimezone(tz_brasil)
        
        # Calcular diferença
        diferenca = data_atual - ultima_sync
        
        print(f"📅 Última sincronização: {ultima_sync_brasil.strftime('%Y-%m-%d %H:%M:%S')} (Horário do Brasil)")
        print(f"📅 Data atual: {data_atual_brasil.strftime('%Y-%m-%d %H:%M:%S')} (Horário do Brasil)")
        print(f"⏱️  Diferença: {diferenca.days} dias, {diferenca.seconds // 3600} horas")
        print(f"📊 A sincronização irá coletar TODOS os dados desde {ultima_sync_brasil.strftime('%Y-%m-%d %H:%M')} até AGORA ({data_atual_brasil.strftime('%Y-%m-%d %H:%M')})")
        
        # Conectar ao NIMBUS para verificar se há dados no período
        origem = {
            'host': obter_variavel('DB_ORIGEM_HOST'),
            'port': obter_variavel('DB_ORIGEM_PORT', obrigatoria=False, padrao='5432'),
            'dbname': obter_variavel('DB_ORIGEM_NAME'),
            'user': obter_variavel('DB_ORIGEM_USER'),
            'password': obter_variavel('DB_ORIGEM_PASSWORD'),
            'sslmode': obter_variavel('DB_ORIGEM_SSLMODE', obrigatoria=False, padrao='disable'),
            'connect_timeout': 10
        }
        
        conn_nimbus = psycopg2.connect(**origem)
        cur_nimbus = conn_nimbus.cursor()
        
        # Verificar se há dados no NIMBUS entre última sincronização e data atual
        timestamp_str = ultima_sync.strftime('%Y-%m-%d %H:%M:%S+00:00')
        timestamp_atual_str = data_atual.strftime('%Y-%m-%d %H:%M:%S+00:00')
        
        query_nimbus = f"""
        SELECT COUNT(*) as total_registros
        FROM public.estacoes_leitura AS el
        JOIN public.estacoes_leiturachuva AS elc ON elc.leitura_id = el.id
        WHERE el."horaLeitura" > '{timestamp_str}'::timestamptz
          AND el."horaLeitura" <= '{timestamp_atual_str}'::timestamptz
        """
        
        cur_nimbus.execute(query_nimbus)
        resultado = cur_nimbus.fetchone()
        total_registros_nimbus = resultado[0] if resultado else 0
        
        cur_nimbus.close()
        conn_nimbus.close()
        
        # Verificar se há lacuna significativa (mais de 1 dia)
        lacuna_significativa = diferenca.days > 1
        
        if total_registros_nimbus > 0:
            # Atualizar data_atual no momento da exibição para garantir precisão (horário do Brasil)
            data_atual_agora_brasil = datetime.now(tz_brasil)
            data_atual_agora = data_atual_agora_brasil.astimezone(timezone.utc)
            
            print(f"⚠️  LACUNA DETECTADA: {total_registros_nimbus:,} registros no NIMBUS não coletados")
            print(f"   Período pendente: {ultima_sync_brasil.strftime('%Y-%m-%d %H:%M')} até {data_atual_agora_brasil.strftime('%Y-%m-%d %H:%M')} (Horário do Brasil)")
            print(f"   ✅ A sincronização irá coletar TODOS esses {total_registros_nimbus:,} registros pendentes")
            
            if lacuna_significativa:
                anos_pendentes = diferenca.days / 365.25
                print(f"   ⚠️  ATENÇÃO: Lacuna de {diferenca.days} dias ({anos_pendentes:.1f} anos) detectada!")
                print(f"   📦 A sincronização irá coletar TODOS os dados desde {ultima_sync.strftime('%Y-%m-%d')} até hoje")
                print(f"   ⏱️  Isso pode demorar bastante tempo devido ao volume de dados")
            
            return {
                'sucesso': True,
                'lacunas_detectadas': True,
                'total_registros_pendentes': total_registros_nimbus,
                'ultima_sincronizacao': ultima_sync.isoformat(),
                'data_atual': data_atual.isoformat(),
                'diferenca_dias': diferenca.days,
                'lacuna_significativa': lacuna_significativa
            }
        else:
            print(f"✅ Sem lacunas: Nenhum registro pendente no NIMBUS")
            print(f"   ✅ Última sincronização está atualizada até {ultima_sync_brasil.strftime('%Y-%m-%d %H:%M')} (Horário do Brasil)")
            return {
                'sucesso': True,
                'lacunas_detectadas': False,
                'total_registros_pendentes': 0,
                'ultima_sincronizacao': ultima_sync.isoformat(),
                'data_atual': data_atual.isoformat(),
                'diferenca_dias': diferenca.days
            }
            
    except Exception as e:
        print(f"❌ Erro ao verificar lacunas: {e}")
        import traceback
        traceback.print_exc()
        return {
            'sucesso': False,
            'mensagem': str(e),
            'lacunas_detectadas': False
        }

@flow(name="Sincronização BigQuery - Pluviométricos", log_prints=True)
def sincronizacao_pluviometricos_flow() -> dict:
    """Flow principal para sincronização incremental de dados pluviométricos."""
    print("=" * 80)
    print("🌧️ SINCRONIZAÇÃO INCREMENTAL PLUVIOMÉTRICOS - BigQuery")
    print("=" * 80)
    print(f"⏰ Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📊 Tabela: pluviometricos")
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
    
    # 2. Verificar lacunas de dados ANTES da sincronização
    print("🔍 Verificando lacunas de dados...")
    resultado_lacunas = verificar_lacunas_dados()
    lacunas_detectadas = resultado_lacunas.get('lacunas_detectadas', False)
    
    if lacunas_detectadas:
        registros_pendentes = resultado_lacunas.get('total_registros_pendentes', 0)
        diferenca_dias = resultado_lacunas.get('diferenca_dias', 0)
        print(f"   ⚠️  {registros_pendentes:,} registros pendentes detectados")
        if diferenca_dias > 1:
            print(f"   ⚠️  Lacuna de {diferenca_dias} dias - sincronização pode estar atrasada")
    
    print()
    
    # 3. Executar sincronização incremental
    print("📦 Executando sincronização incremental...")
    resultado_sync = sincronizar_pluviometricos_incremental()
    sucesso_sync = resultado_sync.get('sucesso', False)
    
    print()
    
    # 4. Verificar status final
    print("📊 Verificando status final no BigQuery...")
    status_bq = verificar_status_bigquery_pluviometricos()
    
    # Compilar erros e avisos
    todos_erros = resultado_sync.get('erros_detectados', []) or []
    todos_avisos = resultado_sync.get('avisos', []) or []
    
    # Adicionar aviso se houver lacunas
    if lacunas_detectadas:
        registros_pendentes = resultado_lacunas.get('total_registros_pendentes', 0)
        diferenca_dias = resultado_lacunas.get('diferenca_dias', 0)
        if diferenca_dias > 1:
            todos_avisos.append(f"Lacuna de {diferenca_dias} dias detectada ({registros_pendentes:,} registros pendentes)")
        else:
            todos_avisos.append(f"{registros_pendentes:,} registros pendentes detectados")
    
    if not status_bq.get('sucesso', False):
        todos_erros.append("Erro ao verificar status BigQuery")
    
    print()
    print("=" * 80)
    
    # Resumo final
    if sucesso_sync and not todos_erros:
        print("✅ SINCRONIZAÇÃO PLUVIOMÉTRICOS CONCLUÍDA COM SUCESSO")
    elif sucesso_sync and todos_erros:
        print("⚠️  SINCRONIZAÇÃO PLUVIOMÉTRICOS CONCLUÍDA COM ERROS")
        for erro in todos_erros:
            print(f"   - {erro}")
    else:
        print("❌ SINCRONIZAÇÃO PLUVIOMÉTRICOS FALHOU")
        for erro in todos_erros:
            print(f"   - {erro}")
    
    print("=" * 80)
    print(f"⏰ Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if status_bq.get('sucesso'):
        print(f"📊 Status BigQuery:")
        print(f"   Total de registros: {status_bq.get('total_registros', 0):,}")
        print(f"   Data máxima: {status_bq.get('data_maxima', 'N/A')}")
        print(f"   Total de estações: {status_bq.get('total_estacoes', 0)}")
    
    if resultado_sync.get('registros_processados', 0) > 0:
        print(f"📦 Registros processados: {resultado_sync.get('registros_processados', 0):,}")
    
    return {
        'sucesso': sucesso_sync and not todos_erros,
        'sincronizacao': resultado_sync,
        'status_bigquery': status_bq,
        'lacunas': resultado_lacunas,
        'erros': todos_erros,
        'avisos': todos_avisos,
        'timestamp': datetime.now().isoformat()
    }

if __name__ == "__main__":
    """
    Configuração de Execução:
    
    OPÇÃO 1: Execução direta (sem criar deployment) - Para testes ou quando limite atingido
    - python prefect_workflow_pluviometricos.py --run-once
    
    OPÇÃO 2: Prefect Cloud
    - prefect deploy prefect_workflow_pluviometricos.py:sincronizacao_pluviometricos_flow --pool seu-work-pool
    
    OPÇÃO 3: Prefect Local
    - Descomente: os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"
    - prefect server start
    - Execute este script
    """
    
    # Para executar sem criar deployment (útil quando limite de deployments atingido)
    if '--run-once' in sys.argv:
        print("🔄 Executando sincronização pluviométricos (sem criar deployment)...")
        sincronizacao_pluviometricos_flow()
    else:
        # Criar deployment (requer Prefect Cloud ou Local rodando)
        sincronizacao_pluviometricos_flow.serve(
            name="sincronizacao-bigquery-pluviometricos",
            cron="*/5 * * * *",  # A cada 5 minutos
            description="Sincronização incremental de dados pluviométricos do NIMBUS para BigQuery (tabela: pluviometricos)"
        )

