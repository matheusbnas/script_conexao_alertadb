#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🌧️ ATUALIZAÇÃO INCREMENTAL EM TEMPO REAL - Servidor 166 → Cloud SQL GCP

═══════════════════════════════════════════════════════════════════════════
🎯 PROPÓSITO DESTE SCRIPT:
═══════════════════════════════════════════════════════════════════════════

Este script mantém o Cloud SQL GCP atualizado automaticamente com novos dados
do servidor 166 (alertadb_cor). Verifica novos registros a cada 5 minutos.

ARQUITETURA:
    NIMBUS → Servidor 166 (alertadb_cor) → Cloud SQL GCP
              ↑ [sync existente]      ↑ [ESTE SCRIPT]

IMPORTANTE: Este script foi adaptado do sincronizar_pluviometricos_novos.py
existente no projeto, mantendo a mesma lógica e estrutura.

═══════════════════════════════════════════════════════════════════════════
📋 O QUE ESTE SCRIPT FAZ:
═══════════════════════════════════════════════════════════════════════════

✅ Busca APENAS registros NOVOS desde a última sincronização
✅ Verifica novos dados a cada 5 minutos automaticamente
✅ Executa em modo contínuo até ser interrompido (Ctrl+C)
✅ Usa ON CONFLICT DO UPDATE para atualizar dados existentes
✅ Garante que os dados no Cloud SQL correspondam ao servidor 166

═══════════════════════════════════════════════════════════════════════════
⚠️ QUANDO USAR ESTE SCRIPT:
═══════════════════════════════════════════════════════════════════════════

✅ APÓS executar o carregar_para_cloudsql_inicial.py (carga inicial)
✅ Para manter dados atualizados automaticamente em tempo real
✅ Em produção/servidor para sincronização contínua
✅ Quando você precisa de dados atualizados a cada 5 minutos

═══════════════════════════════════════════════════════════════════════════
🚀 COMO USAR:
═══════════════════════════════════════════════════════════════════════════

1. PRIMEIRO: Execute carregar_para_cloudsql_inicial.py para carga inicial
2. Configure o arquivo .env com as credenciais do Cloud SQL
3. Execute: python sincronizar_para_cloudsql.py
4. O script rodará continuamente verificando novos dados
5. Para parar: Pressione Ctrl+C

OU para execução única (útil para cron):
    python sincronizar_para_cloudsql.py --once
"""

import psycopg2
from psycopg2 import errors as psycopg2_errors
from psycopg2.extras import execute_values
import time
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path

# Carregar variáveis de ambiente
project_root = Path(__file__).parent.parent.parent
load_dotenv(dotenv_path=project_root / '.env')

def obter_variavel(nome, obrigatoria=True, padrao=None):
    """Obtém variável de ambiente."""
    valor = os.getenv(nome)
    if not valor or (isinstance(valor, str) and valor.strip() == ''):
        if obrigatoria:
            raise ValueError(f"❌ Variável obrigatória não encontrada: {nome}")
        return padrao
    return valor.strip() if isinstance(valor, str) else valor

def carregar_configuracoes():
    """Carrega configurações do .env."""
    try:
        # Banco ORIGEM - alertadb_cor no servidor 166 (LOCALHOST)
        origem = {
            'host': obter_variavel('DB_DESTINO_HOST', obrigatoria=False, padrao='localhost'),
            'port': obter_variavel('DB_DESTINO_PORT', obrigatoria=False, padrao='5432'),
            'dbname': obter_variavel('DB_DESTINO_NAME', obrigatoria=False, padrao='alertadb_cor'),
            'user': obter_variavel('DB_DESTINO_USER', obrigatoria=False, padrao='postgres'),
            'password': obter_variavel('DB_DESTINO_PASSWORD'),
        }

        # Banco DESTINO - Cloud SQL GCP
        destino = {
            'host': obter_variavel('CLOUDSQL_HOST', obrigatoria=False, padrao='34.82.95.242'),
            'port': obter_variavel('CLOUDSQL_PORT', obrigatoria=False, padrao='5432'),
            'dbname': obter_variavel('CLOUDSQL_DATABASE', obrigatoria=False, padrao='alertadb_cor'),
            'user': obter_variavel('CLOUDSQL_USER', obrigatoria=False, padrao='postgres'),
            'password': obter_variavel('CLOUDSQL_PASSWORD'),
            'connect_timeout': 10,
            'sslmode': obter_variavel('CLOUDSQL_SSLMODE', obrigatoria=False, padrao='require')
        }
        
        # Intervalo de verificação (padrão: 300 segundos = 5 minutos)
        intervalo = int(obter_variavel('INTERVALO_VERIFICACAO', obrigatoria=False, padrao='300'))
        
        return origem, destino, intervalo
    
    except ValueError as e:
        print("=" * 70)
        print("❌ ERRO DE CONFIGURAÇÃO")
        print("=" * 70)
        print(str(e))
        print("\n📝 Configure no .env:")
        print("   DB_DESTINO_PASSWORD=senha_servidor_166")
        print("   CLOUDSQL_PASSWORD=senha_cloud_sql")
        print("=" * 70)
        raise

ORIGEM, DESTINO, INTERVALO_VERIFICACAO = carregar_configuracoes()

def testar_conexoes():
    """Testa conexões com ambos os bancos."""
    print("=" * 70)
    print("TESTE DE CONEXÕES")
    print("=" * 70)
    
    try:
        conn_origem = psycopg2.connect(**ORIGEM)
        print(f"   ✅ ORIGEM (Servidor 166): SUCESSO!")
        print(f"      {ORIGEM['dbname']}@{ORIGEM['host']}:{ORIGEM['port']}")
        conn_origem.close()
        
        conn_destino = psycopg2.connect(**DESTINO)
        print(f"   ✅ DESTINO (Cloud SQL): SUCESSO!")
        print(f"      {DESTINO['dbname']}@{DESTINO['host']}:{DESTINO['port']}")
        conn_destino.close()
        return True
        
    except Exception as e:
        print(f"   ❌ ERRO: {e}")
        return False

def verificar_tabela_vazia():
    """Verifica se a tabela no Cloud SQL está vazia."""
    conn_destino = None
    cur_destino = None
    
    try:
        conn_destino = psycopg2.connect(**DESTINO)
        cur_destino = conn_destino.cursor()
        
        cur_destino.execute("SET statement_timeout = '5s';")
        cur_destino.execute("SELECT EXISTS(SELECT 1 FROM pluviometricos LIMIT 1);")
        resultado = cur_destino.fetchone()
        
        return not resultado[0] if resultado else True
            
    except psycopg2_errors.QueryCanceled:
        print('⚠️ Timeout ao verificar tabela. Assumindo não vazia.')
        return False
    except Exception as e:
        print(f'⚠️ Erro ao verificar tabela: {e}')
        return True
    finally:
        if cur_destino:
            cur_destino.close()
        if conn_destino:
            conn_destino.close()

def obter_ultima_sincronizacao():
    """Obtém timestamp da última sincronização no Cloud SQL."""
    conn_destino = None
    cur_destino = None
    
    try:
        conn_destino = psycopg2.connect(**DESTINO)
        cur_destino = conn_destino.cursor()
        
        cur_destino.execute("SELECT MAX(dia) FROM pluviometricos;")
        resultado = cur_destino.fetchone()
        
        if resultado and resultado[0]:
            return resultado[0]
        else:
            return datetime.now() - timedelta(seconds=300)
            
    except Exception as e:
        print(f'⚠️ Erro ao obter última sincronização: {e}')
        return datetime.now() - timedelta(seconds=300)
    finally:
        if cur_destino:
            cur_destino.close()
        if conn_destino:
            conn_destino.close()

def atualizar_dados_incrementais():
    """Atualiza apenas novos dados desde última sincronização."""
    conn_origem = None
    cur_origem = None
    conn_destino = None
    cur_destino = None
    
    timestamp_atual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        # Verificar se tabela está vazia
        if verificar_tabela_vazia():
            print(f'\n⚠️  ATENÇÃO: Tabela no Cloud SQL está VAZIA!')
            print(f'   Execute PRIMEIRO: carregar_para_cloudsql_inicial.py')
            print(f'   Pulando esta verificação...\n')
            return 0
        
        # Obter última sincronização
        ultima_sincronizacao = obter_ultima_sincronizacao()
        
        # Conectar ao servidor 166
        conn_origem = psycopg2.connect(**ORIGEM)
        cur_origem = conn_origem.cursor()
        
        # Buscar apenas dados novos
        query = f"""
        SELECT dia, m05, m10, m15, h01, h04, h24, h96, estacao, estacao_id
        FROM pluviometricos
        WHERE dia > %s
        ORDER BY dia ASC;
        """
        
        print(f'🔍 Verificando novos registros desde {ultima_sincronizacao}...')
        cur_origem.execute(query, (ultima_sincronizacao,))
        dados = cur_origem.fetchall()

        if not dados:
            print(f'   ✓ Nenhum novo dado. [{timestamp_atual}]')
            return 0

        # Conectar ao Cloud SQL
        conn_destino = psycopg2.connect(**DESTINO)
        cur_destino = conn_destino.cursor()
        
        # Configurar timezone
        cur_destino.execute("SET timezone = 'America/Sao_Paulo';")

        # Inserir/atualizar dados
        insert_sql = '''
        INSERT INTO pluviometricos
        (dia, m05, m10, m15, h01, h04, h24, h96, estacao, estacao_id)
        VALUES %s
        ON CONFLICT (dia, estacao_id) 
        DO UPDATE SET
            m05 = EXCLUDED.m05,
            m10 = EXCLUDED.m10,
            m15 = EXCLUDED.m15,
            h01 = EXCLUDED.h01,
            h04 = EXCLUDED.h04,
            h24 = EXCLUDED.h24,
            h96 = EXCLUDED.h96,
            estacao = EXCLUDED.estacao;
        '''

        execute_values(cur_destino, insert_sql, dados)
        conn_destino.commit()
        
        total_inseridos = len(dados)
        
        # Obter último timestamp
        cur_destino.execute("SELECT MAX(dia) FROM pluviometricos;")
        ultimo_timestamp = cur_destino.fetchone()
        ultimo_ts_str = ""
        if ultimo_timestamp and ultimo_timestamp[0]:
            ultimo_ts_str = f". Último: {ultimo_timestamp[0]}"
        
        print(f'   ✅ {total_inseridos:,} novo(s) registro(s) sincronizado(s){ultimo_ts_str} [{timestamp_atual}]')
        
        return total_inseridos

    except Exception as e:
        print(f'   ❌ Erro: {e} [{timestamp_atual}]')
        return 0

    finally:
        if cur_origem:
            cur_origem.close()
        if conn_origem:
            conn_origem.close()
        if cur_destino:
            cur_destino.close()
        if conn_destino:
            conn_destino.close()

def executar_sincronizacao_unica():
    """
    Executa uma única sincronização incremental.
    Útil para cron, Prefect ou outros agendadores.
    
    Returns:
        int: Número de registros sincronizados
    """
    try:
        return atualizar_dados_incrementais()
    except Exception as e:
        print(f"❌ Erro na sincronização: {e}")
        return 0

def main(modo_continuo=True):
    """
    Função principal.
    
    Args:
        modo_continuo (bool): Se True, loop infinito. Se False, execução única.
    """
    print("=" * 70)
    print("🌧️ SINCRONIZAÇÃO INCREMENTAL - Servidor 166 → Cloud SQL")
    print("=" * 70)
    print()
    print("🎯 PROPÓSITO:")
    print("   Atualiza APENAS novos dados desde a última sincronização.")
    if modo_continuo:
        print("   Mantém dados atualizados em tempo real a cada 5 minutos.")
    else:
        print("   Executa uma única sincronização.")
    print()
    print("📋 O QUE SERÁ FEITO:")
    print("   ✅ Buscar apenas registros NOVOS desde última sincronização")
    if modo_continuo:
        print(f"   ✅ Verificar novos dados a cada {INTERVALO_VERIFICACAO}s")
        print("   ✅ Executar continuamente até Ctrl+C")
    else:
        print("   ✅ Executar uma única sincronização")
    print()
    print("⚠️  PRÉ-REQUISITO:")
    print("   ⚠️  Execute carregar_para_cloudsql_inicial.py PRIMEIRO")
    print("   ⚠️  Este script NÃO funciona se tabela estiver vazia")
    print("=" * 70)
    
    # Testar conexões
    if not testar_conexoes():
        print("\n❌ Falha nos testes de conexão. Abortando...")
        return

    if modo_continuo:
        print(f"\n🚀 Iniciando sincronização em tempo real...")
        print(f"   Verificando a cada {INTERVALO_VERIFICACAO}s")
        print(f"   Pressione Ctrl+C para parar\n")
        print("-" * 70)
        
        total_atualizado = 0
        
        try:
            while True:
                registros = atualizar_dados_incrementais()
                total_atualizado += registros
                
                print(f'\n⏱️  Aguardando {INTERVALO_VERIFICACAO}s...\n')
                time.sleep(INTERVALO_VERIFICACAO)
                
        except KeyboardInterrupt:
            print("\n" + "=" * 70)
            print(f"⚠️  Interrompido pelo usuário.")
            print(f"📊 Total atualizado: {total_atualizado:,}")
            print("=" * 70)
        except Exception as e:
            print(f"\n❌ Erro fatal: {e}")
    else:
        print(f"\n🚀 Executando sincronização única...\n")
        print("-" * 70)
        
        try:
            registros = executar_sincronizacao_unica()
            print("\n" + "=" * 70)
            print(f"✅ Sincronização concluída.")
            print(f"📊 Registros: {registros:,}")
            print("=" * 70)
        except Exception as e:
            print(f"\n❌ Erro: {e}")

if __name__ == "__main__":
    import sys
    modo_continuo = "--once" not in sys.argv
    main(modo_continuo=modo_continuo)
