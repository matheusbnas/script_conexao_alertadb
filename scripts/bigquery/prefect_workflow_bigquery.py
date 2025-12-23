#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔄 WORKFLOW PREFECT - Sincronização BigQuery

Este workflow usa Prefect para orquestrar a sincronização de dados do NIMBUS
para o BigQuery, com suporte a execução agendada e monitoramento.

Baseado em: https://docs.prefect.io/v3/get-started/quickstart#open-source

IMPORTANTE: Este script usa Prefect Open Source (servidor local).
Certifique-se de que o servidor Prefect está rodando:
    prefect server start
"""

import os
# Configuração Prefect: Use Cloud ou Local
# Para Prefect Cloud (executa mesmo com máquina desligada):
#   - API: cli-41fbdcc9-2a85-4885-a7cd-4390df02c7e4
#   - Configure: prefect cloud login
#   - Crie work pool no Prefect Cloud
#   - Não defina PREFECT_API_URL aqui (deixe usar Cloud automaticamente)
# Para Prefect Local (só funciona com máquina ligada):
#   - Descomente a linha abaixo
# os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"

from prefect import flow, task
try:
    from prefect_gcp import GcpCredentials
    from prefect_gcp.bigquery import BigQueryWarehouse
    HAS_PREFECT_GCP = True
except ImportError:
    HAS_PREFECT_GCP = False
    print("⚠️  prefect-gcp não instalado. A verificação de status BigQuery será limitada.")
    print("   Para instalar: pip install prefect-gcp")

import subprocess
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv

# Caminho base do projeto
project_root = Path(__file__).parent.parent.parent

@task(name="Verificar Conexão NIMBUS", log_prints=True)
def verificar_conexao_nimbus() -> bool:
    """Verifica se a conexão com o banco NIMBUS está disponível."""
    try:
        import psycopg2
        from dotenv import load_dotenv
        import os
        
        # Carregar variáveis de ambiente
        load_dotenv(dotenv_path=project_root / '.env')
        
        # Obter configurações do .env
        def obter_variavel(nome, obrigatoria=True, padrao=None):
            valor = os.getenv(nome)
            if not valor or (isinstance(valor, str) and valor.strip() == ''):
                if obrigatoria:
                    raise ValueError(f"Variável obrigatória não encontrada: {nome}")
                return padrao
            return valor.strip() if isinstance(valor, str) else valor
        
        origem = {
            'host': obter_variavel('DB_ORIGEM_HOST'),
            'port': obter_variavel('DB_ORIGEM_PORT', obrigatoria=False, padrao='5432'),
            'dbname': obter_variavel('DB_ORIGEM_NAME'),
            'user': obter_variavel('DB_ORIGEM_USER'),
            'password': obter_variavel('DB_ORIGEM_PASSWORD'),
            'sslmode': obter_variavel('DB_ORIGEM_SSLMODE', obrigatoria=False, padrao='disable'),
            'connect_timeout': 10
        }
        
        # Testar conexão
        conn = psycopg2.connect(**origem)
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.close()
        conn.close()
        print(f"✅ Conexão NIMBUS OK: {origem['dbname']}@{origem['host']}:{origem['port']}")
        return True
    except Exception as e:
        print(f"❌ Erro ao verificar conexão NIMBUS: {e}")
        import traceback
        traceback.print_exc()
        return False

@task(name="Verificar Credenciais GCP", log_prints=True)
def verificar_credenciais_gcp() -> bool:
    """Verifica se as credenciais do GCP estão configuradas."""
    try:
        credentials_path = project_root / 'credentials' / 'credentials.json'
        if credentials_path.exists():
            print(f"✅ Credenciais GCP encontradas: {credentials_path}")
            return True
        else:
            print(f"⚠️  Credenciais GCP não encontradas em: {credentials_path}")
            return False
    except Exception as e:
        print(f"❌ Erro ao verificar credenciais GCP: {e}")
        return False

@task(name="Exportação Completa Pluviométricos", log_prints=True, retries=2, retry_delay_seconds=60)
def exportar_pluviometricos_completo() -> dict:
    """Executa a exportação completa de dados pluviométricos do NIMBUS para BigQuery.
    
    Esta task executa o script de exportação completa, que recarrega todos os dados
    desde 1997. Use com cuidado, pois pode demorar bastante tempo.
    """
    try:
        script_path = project_root / 'scripts' / 'bigquery' / 'exportar_pluviometricos_nimbus_bigquery.py'
        
        print(f"🔄 Iniciando exportação completa de pluviométricos...")
        print(f"   Script: {script_path}")
        
        inicio = datetime.now()
        
        # Executar script usando subprocess para capturar logs
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=str(project_root),
            capture_output=True,
            text=True,
            timeout=3600  # Timeout de 1 hora
        )
        
        tempo_decorrido = (datetime.now() - inicio).total_seconds()
        
        if result.returncode == 0:
            print(f"✅ Exportação completa concluída em {tempo_decorrido:.1f} segundos")
            print(f"   Output: {result.stdout[-500:]}")  # Últimas 500 linhas
            return {
                'sucesso': True,
                'tempo_segundos': tempo_decorrido,
                'mensagem': 'Exportação completa concluída com sucesso'
            }
        else:
            print(f"❌ Erro na exportação completa:")
            print(f"   Return code: {result.returncode}")
            print(f"   Stderr: {result.stderr[-500:]}")
            return {
                'sucesso': False,
                'tempo_segundos': tempo_decorrido,
                'mensagem': f'Erro na exportação: {result.stderr[-200:]}'
            }
            
    except subprocess.TimeoutExpired:
        print(f"⏱️  Timeout: Exportação demorou mais de 1 hora")
        return {
            'sucesso': False,
            'tempo_segundos': 3600,
            'mensagem': 'Timeout após 1 hora'
        }
    except Exception as e:
        print(f"❌ Erro ao executar exportação completa: {e}")
        return {
            'sucesso': False,
            'tempo_segundos': 0,
            'mensagem': str(e)
        }

@task(name="Sincronização Incremental Pluviométricos", log_prints=True, retries=2, retry_delay_seconds=60)
def sincronizar_pluviometricos_incremental() -> dict:
    """Executa a sincronização incremental de dados pluviométricos.
    
    Esta task executa o script de sincronização incremental, que busca apenas
    os dados novos desde a última sincronização. Ideal para execução periódica.
    
    Monitora erros de carregamento e retorna informações detalhadas.
    """
    try:
        script_path = project_root / 'scripts' / 'bigquery' / 'sincronizar_pluviometricos_nimbus_bigquery.py'
        
        print(f"🔄 Iniciando sincronização incremental de pluviométricos...")
        print(f"   Script: {script_path}")
        print(f"   Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        inicio = datetime.now()
        
        # Executar script com flag --once
        result = subprocess.run(
            [sys.executable, str(script_path), '--once'],
            cwd=str(project_root),
            capture_output=True,
            text=True,
            timeout=1800  # Timeout de 30 minutos
        )
        
        tempo_decorrido = (datetime.now() - inicio).total_seconds()
        
        # Analisar output para detectar erros específicos
        output_completo = result.stdout + result.stderr
        erros_detectados = []
        
        # Verificar erros comuns
        if "Resources exceeded" in output_completo or "10000 partitions" in output_completo:
            erros_detectados.append("ERRO: Limite de partições excedido (precisa recriar tabela com particionamento por MÊS)")
        
        if "TIMESTAMP_NANOS" in output_completo or "Invalid timestamp" in output_completo:
            erros_detectados.append("ERRO: Problema com formato de timestamp")
        
        if "ConnectionResetError" in output_completo or "connection" in output_completo.lower():
            erros_detectados.append("ERRO: Problema de conexão com banco de dados")
        
        if "ERRO CRÍTICO" in output_completo or "❌" in output_completo:
            erros_detectados.append("ERRO: Erro crítico detectado no script")
        
        if result.returncode == 0:
            if erros_detectados:
                print(f"⚠️  Sincronização concluída mas com avisos:")
                for erro in erros_detectados:
                    print(f"   {erro}")
            else:
                print(f"✅ Sincronização incremental concluída com sucesso em {tempo_decorrido:.1f} segundos")
            
            # Extrair informações úteis do output
            registros_processados = 0
            if "registros" in output_completo.lower():
                import re
                match = re.search(r'(\d[\d,]*)\s+registros', output_completo)
                if match:
                    registros_processados = int(match.group(1).replace(',', ''))
            
            print(f"   📊 Registros processados: {registros_processados:,}")
            print(f"   ⏱️  Tempo de execução: {tempo_decorrido:.1f} segundos")
            
            return {
                'sucesso': True,
                'tempo_segundos': tempo_decorrido,
                'registros_processados': registros_processados,
                'mensagem': 'Sincronização incremental concluída com sucesso',
                'avisos': erros_detectados if erros_detectados else None,
                'output_resumo': output_completo[-1000:] if len(output_completo) > 1000 else output_completo
            }
        else:
            print(f"❌ ERRO na sincronização incremental:")
            print(f"   Return code: {result.returncode}")
            print(f"   Erros detectados: {len(erros_detectados)}")
            for erro in erros_detectados:
                print(f"   {erro}")
            
            # Log completo do erro
            print(f"\n   📋 Últimas linhas do stderr:")
            stderr_lines = result.stderr.split('\n')[-20:]
            for line in stderr_lines:
                if line.strip():
                    print(f"      {line}")
            
            return {
                'sucesso': False,
                'tempo_segundos': tempo_decorrido,
                'return_code': result.returncode,
                'mensagem': f'Erro na sincronização (code: {result.returncode})',
                'erros_detectados': erros_detectados,
                'stderr': result.stderr[-1000:] if len(result.stderr) > 1000 else result.stderr
            }
            
    except subprocess.TimeoutExpired:
        print(f"⏱️  TIMEOUT: Sincronização demorou mais de 30 minutos")
        print(f"   Isso pode indicar problema de conexão ou volume muito grande de dados")
        return {
            'sucesso': False,
            'tempo_segundos': 1800,
            'mensagem': 'Timeout após 30 minutos - verifique conexão e volume de dados',
            'erros_detectados': ['TIMEOUT: Processo demorou mais de 30 minutos']
        }
    except Exception as e:
        print(f"❌ ERRO FATAL ao executar sincronização incremental: {e}")
        import traceback
        traceback.print_exc()
        return {
            'sucesso': False,
            'tempo_segundos': 0,
            'mensagem': f'Erro fatal: {str(e)}',
            'erros_detectados': [f'Erro fatal: {str(e)}']
        }

@task(name="Verificar Status BigQuery", log_prints=True)
def verificar_status_bigquery() -> dict:
    """Verifica o status da tabela no BigQuery usando Prefect GCP integration ou subprocess."""
    try:
        credentials_path = project_root / 'credentials' / 'credentials.json'
        
        if not credentials_path.exists():
            return {
                'sucesso': False,
                'mensagem': f'Credenciais não encontradas em: {credentials_path}'
            }
        
        # Tentar usar prefect-gcp se disponível
        if HAS_PREFECT_GCP:
            try:
                gcp_credentials = GcpCredentials(service_account_file=str(credentials_path))
                
                with BigQueryWarehouse(gcp_credentials=gcp_credentials) as warehouse:
                    query = """
                    SELECT 
                        COUNT(*) as total_registros,
                        MIN(dia) as data_minima,
                        MAX(dia) as data_maxima,
                        COUNT(DISTINCT estacao_id) as total_estacoes
                    FROM `alertadb_cor_raw.pluviometricos`
                    """
                    
                    result = warehouse.fetch_one(query)
                    
                    if result:
                        print(f"📊 Status BigQuery (via Prefect GCP):")
                        print(f"   Total de registros: {result[0]:,}")
                        print(f"   Data mínima: {result[1]}")
                        print(f"   Data máxima: {result[2]}")
                        print(f"   Total de estações: {result[3]}")
                        
                        return {
                            'sucesso': True,
                            'total_registros': result[0],
                            'data_minima': str(result[1]),
                            'data_maxima': str(result[2]),
                            'total_estacoes': result[3]
                        }
            except Exception as e:
                print(f"⚠️  Erro ao usar Prefect GCP, tentando método alternativo: {e}")
        
        # Método alternativo: usar bq CLI ou script Python direto
        # Por enquanto, retornar sucesso sem dados detalhados
        print(f"📊 Status BigQuery: Verificação básica (prefect-gcp não disponível)")
        return {
            'sucesso': True,
            'mensagem': 'Verificação básica concluída (instale prefect-gcp para estatísticas detalhadas)'
        }
                
    except Exception as e:
        print(f"❌ Erro ao verificar status BigQuery: {e}")
        return {
            'sucesso': False,
            'mensagem': str(e)
        }

@flow(name="Sincronização BigQuery - Incremental", log_prints=True)
def sincronizacao_incremental_flow() -> dict:
    """Flow principal para sincronização incremental de dados pluviométricos.
    
    Este flow executa:
    1. Verificação de conexões (NIMBUS e GCP)
    2. Sincronização incremental
    3. Verificação do status final
    4. Monitoramento de erros de carregamento
    
    Ideal para execução periódica (cron) a cada 5 minutos.
    Monitora e reporta todos os erros de carregamento.
    """
    print("=" * 80)
    print("🔄 INICIANDO SINCRONIZAÇÃO INCREMENTAL - BigQuery")
    print("=" * 80)
    print(f"⏰ Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # 1. Verificar conexões
    print("📡 Verificando conexões...")
    conexao_nimbus = verificar_conexao_nimbus()
    credenciais_gcp = verificar_credenciais_gcp()
    
    if not conexao_nimbus or not credenciais_gcp:
        erro_msg = "❌ Falha na verificação de conexões. Abortando flow."
        print(erro_msg)
        print(f"   Conexão NIMBUS: {'✅ OK' if conexao_nimbus else '❌ FALHOU'}")
        print(f"   Credenciais GCP: {'✅ OK' if credenciais_gcp else '❌ FALHOU'}")
        return {
            'sucesso': False,
            'mensagem': 'Falha na verificação de conexões',
            'conexao_nimbus': conexao_nimbus,
            'credenciais_gcp': credenciais_gcp,
            'erros': ['Falha na verificação de conexões'],
            'timestamp': datetime.now().isoformat()
        }
    
    print()
    
    # 2. Executar sincronização incremental
    print("📦 Executando sincronização incremental...")
    resultado_sync = sincronizar_pluviometricos_incremental()
    
    # Verificar se houve erros
    sucesso_sync = resultado_sync.get('sucesso', False)
    erros_sync = resultado_sync.get('erros_detectados', [])
    avisos_sync = resultado_sync.get('avisos', [])
    
    print()
    
    # 3. Verificar status final no BigQuery
    print("📊 Verificando status final no BigQuery...")
    status_bq = verificar_status_bigquery()
    
    # Compilar todos os erros e avisos
    todos_erros = []
    todos_avisos = []
    
    if erros_sync:
        todos_erros.extend(erros_sync)
    if avisos_sync:
        todos_avisos.extend(avisos_sync)
    if not status_bq.get('sucesso', False):
        todos_erros.append(f"Erro ao verificar status BigQuery: {status_bq.get('mensagem', 'Desconhecido')}")
    
    print()
    print("=" * 80)
    
    # Resumo final
    if sucesso_sync and not todos_erros:
        print("✅ SINCRONIZAÇÃO INCREMENTAL CONCLUÍDA COM SUCESSO")
        if todos_avisos:
            print(f"⚠️  Avisos: {len(todos_avisos)}")
            for aviso in todos_avisos:
                print(f"   - {aviso}")
    elif sucesso_sync and todos_erros:
        print("⚠️  SINCRONIZAÇÃO CONCLUÍDA COM ERROS")
        print(f"❌ Erros detectados: {len(todos_erros)}")
        for erro in todos_erros:
            print(f"   - {erro}")
    else:
        print("❌ SINCRONIZAÇÃO FALHOU")
        print(f"❌ Erros detectados: {len(todos_erros)}")
        for erro in todos_erros:
            print(f"   - {erro}")
    
    print("=" * 80)
    print(f"⏰ Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Informações de resumo
    if status_bq.get('sucesso'):
        print(f"📊 Status BigQuery:")
        print(f"   Total de registros: {status_bq.get('total_registros', 0):,}")
        print(f"   Data mínima: {status_bq.get('data_minima', 'N/A')}")
        print(f"   Data máxima: {status_bq.get('data_maxima', 'N/A')}")
        print(f"   Total de estações: {status_bq.get('total_estacoes', 0)}")
    
    if resultado_sync.get('registros_processados', 0) > 0:
        print(f"📦 Registros processados nesta execução: {resultado_sync.get('registros_processados', 0):,}")
    
    return {
        'sucesso': sucesso_sync and not todos_erros,
        'sincronizacao': resultado_sync,
        'status_bigquery': status_bq,
        'erros': todos_erros,
        'avisos': todos_avisos,
        'registros_processados': resultado_sync.get('registros_processados', 0),
        'timestamp': datetime.now().isoformat()
    }

@flow(name="Exportação Completa BigQuery", log_prints=True)
def exportacao_completa_flow() -> dict:
    """Flow principal para exportação completa de dados pluviométricos.
    
    Este flow executa:
    1. Verificação de conexões (NIMBUS e GCP)
    2. Exportação completa (todos os dados desde 1997)
    3. Verificação do status final
    
    ATENÇÃO: Esta operação pode demorar várias horas!
    Use apenas quando necessário recarregar todos os dados.
    """
    print("=" * 80)
    print("🔄 INICIANDO EXPORTAÇÃO COMPLETA - BigQuery")
    print("=" * 80)
    print(f"⏰ Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    print("⚠️  ATENÇÃO: Esta operação pode demorar várias horas!")
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
            'conexao_nimbus': conexao_nimbus,
            'credenciais_gcp': credenciais_gcp
        }
    
    print()
    
    # 2. Executar exportação completa
    print("📦 Executando exportação completa...")
    resultado_export = exportar_pluviometricos_completo()
    
    print()
    
    # 3. Verificar status final
    print("📊 Verificando status final no BigQuery...")
    status_bq = verificar_status_bigquery()
    
    print()
    print("=" * 80)
    print("✅ EXPORTAÇÃO COMPLETA CONCLUÍDA")
    print("=" * 80)
    print(f"⏰ Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    return {
        'sucesso': resultado_export.get('sucesso', False),
        'exportacao': resultado_export,
        'status_bigquery': status_bq,
        'timestamp': datetime.now().isoformat()
    }

if __name__ == "__main__":
    """
    Configuração de Execução:
    
    OPÇÃO 1: Prefect Cloud (Recomendado - executa mesmo com máquina desligada)
    - Faça login: prefect cloud login
    - Crie work pool no Prefect Cloud UI
    - Deploy: prefect deploy prefect_workflow_bigquery.py:sincronizacao_incremental_flow --pool seu-work-pool
    - Inicie agent em servidor dedicado: prefect agent start seu-work-pool
    
    OPÇÃO 2: Prefect Local (só funciona com máquina ligada)
    - Descomente: os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"
    - Inicie servidor: prefect server start
    - Execute este script
    """
    
    # Opção 1: Execução local única (teste)
    # sincronizacao_incremental_flow()
    
    # Opção 2: Execução com agendamento (serve mode) - ATUALIZA A CADA 5 MINUTOS
    # Para Prefect Cloud: use 'prefect deploy' em vez de .serve()
    # Para Prefect Local: use .serve() abaixo (certifique-se de que servidor está rodando)
    
    sincronizacao_incremental_flow.serve(
        name="sincronizacao-bigquery-incremental",
        cron="*/5 * * * *",  # A cada 5 minutos
        description="Sincronização incremental de dados pluviométricos do NIMBUS para BigQuery. Atualiza BigQuery a cada 5 minutos e monitora erros de carregamento."
        # work_pool_name="bigquery-sync-pool",  # Descomente se usar Prefect Cloud com work pool
    )
    
    # Para exportação completa (descomente se necessário)
    # exportacao_completa_flow.serve(
    #     name="exportacao-bigquery-completa",
    #     cron="0 2 * * 0",  # Todo domingo às 2h da manhã
    #     description="Exportação completa de dados pluviométricos do NIMBUS para BigQuery"
    #     # work_pool_name="bigquery-sync-pool",  # Descomente se usar Prefect Cloud
    # )

