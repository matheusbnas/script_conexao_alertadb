#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🌧️ ATUALIZAÇÃO INCREMENTAL EM TEMPO REAL - Dados Pluviométricos

═══════════════════════════════════════════════════════════════════════════
🎯 PROPÓSITO DESTE SCRIPT:
═══════════════════════════════════════════════════════════════════════════

Este script foi criado para ATUALIZAR APENAS OS NOVOS DADOS desde a última
sincronização. Ele mantém os dados atualizados em tempo real, verificando
novos registros a cada 5 minutos no banco alertadb (origem) e sincronizando
para o banco carioca_digital (destino).

É o SEGUNDO PASSO após executar o carregar_pluviometricos_historicos.py.

═══════════════════════════════════════════════════════════════════════════
📋 O QUE ESTE SCRIPT FAZ:
═══════════════════════════════════════════════════════════════════════════

✅ Busca APENAS registros NOVOS desde a última sincronização
✅ Verifica novos dados a cada 5 minutos automaticamente (configurável)
✅ Executa em modo contínuo até ser interrompido (Ctrl+C)
✅ Usa ON CONFLICT DO NOTHING para evitar sobrepor dados existentes
✅ Chave primária composta (dia, estacao_id) garante unicidade
✅ NUNCA modifica ou deleta dados existentes
✅ Apenas ADICIONA novos registros

═══════════════════════════════════════════════════════════════════════════
⚠️ QUANDO USAR ESTE SCRIPT:
═══════════════════════════════════════════════════════════════════════════

✅ APÓS executar o carregar_pluviometricos_historicos.py (carga inicial)
✅ Para manter os dados atualizados automaticamente em tempo real
✅ Em produção/servidor para sincronização contínua
✅ Quando você precisa de dados atualizados a cada 5 minutos

⚠️ NÃO USE se:
   ❌ A tabela pluviometricos estiver vazia (use carregar_pluviometricos_historicos.py primeiro)
   ❌ Você quer carregar dados históricos (use carregar_pluviometricos_historicos.py)

═══════════════════════════════════════════════════════════════════════════
🚀 COMO USAR:
═══════════════════════════════════════════════════════════════════════════

1. PRIMEIRO: Execute carregar_pluviometricos_historicos.py para carga inicial
2. Configure o arquivo .env com as credenciais dos bancos
3. Execute: python sincronizar_pluviometricos_novos.py
4. O script rodará continuamente verificando novos dados a cada 5 minutos
5. Para parar: Pressione Ctrl+C

═══════════════════════════════════════════════════════════════════════════
🔄 COMO FUNCIONA:
═══════════════════════════════════════════════════════════════════════════

1. Busca o último timestamp sincronizado na tabela destino (MAX(dia))
2. Consulta apenas registros com horaLeitura > último timestamp
3. Insere novos registros usando ON CONFLICT DO NOTHING
4. Aguarda 5 minutos (configurável) e repete o processo
5. Continua indefinidamente até ser interrompido

═══════════════════════════════════════════════════════════════════════════
🔒 PROTEÇÕES IMPLEMENTADAS:
═══════════════════════════════════════════════════════════════════════════

✅ ON CONFLICT DO NOTHING: Previne duplicatas e sobreposição de dados
✅ Chave primária composta (dia, estacao_id): Garante unicidade
✅ Validação: Verifica se tabela não está vazia antes de atualizar
✅ Validação: Verifica última sincronização antes de buscar novos dados
✅ Tratamento de erros: Continua rodando mesmo se houver falha temporária
✅ NUNCA modifica dados existentes
✅ NUNCA deleta dados
✅ APENAS adiciona novos registros

═══════════════════════════════════════════════════════════════════════════
⏱️ INTERVALO DE ATUALIZAÇÃO:
═══════════════════════════════════════════════════════════════════════════

📊 Padrão: 5 minutos (300 segundos)
⚙️  Configurável: Via variável INTERVALO_VERIFICACAO no arquivo .env
⚠️  Recomendação: Não usar intervalos menores que 1 minuto para evitar
   sobrecarga no banco de dados

═══════════════════════════════════════════════════════════════════════════
📋 CONFIGURAÇÃO:
═══════════════════════════════════════════════════════════════════════════

Todas as configurações devem estar no arquivo .env na raiz do projeto.

Variáveis obrigatórias:
- DB_ORIGEM_HOST, DB_ORIGEM_NAME, DB_ORIGEM_USER, DB_ORIGEM_PASSWORD
- DB_DESTINO_HOST, DB_DESTINO_NAME, DB_DESTINO_USER, DB_DESTINO_PASSWORD

Variáveis opcionais:
- DB_ORIGEM_SSLMODE (padrão: disable)
- DB_DESTINO_PORT (padrão: 5432)
- INTERVALO_VERIFICACAO (padrão: 300 segundos = 5 minutos)

═══════════════════════════════════════════════════════════════════════════
"""

# 🔧 Importar bibliotecas necessárias
import psycopg2
from psycopg2.extras import execute_values
import time
import os
import re
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Carregar variáveis de ambiente (busca .env na raiz do projeto)
import sys
from pathlib import Path
# Obter diretório raiz do projeto (2 níveis acima deste arquivo)
project_root = Path(__file__).parent.parent
load_dotenv(dotenv_path=project_root / '.env')

def tornar_datetime_naive(dt):
    """
    Converte um datetime aware (com timezone) para naive (sem timezone).
    Se já for naive, retorna como está.
    
    Args:
        dt: datetime objeto (aware ou naive)
    
    Returns:
        datetime: datetime naive
    """
    if not isinstance(dt, datetime):
        return dt
    
    # Se já é naive, retorna como está
    if dt.tzinfo is None:
        return dt
    
    # Remove timezone convertendo para UTC e depois removendo o timezone
    return dt.replace(tzinfo=None)

def obter_variavel(nome, obrigatoria=True):
    """Obtém variável de ambiente, lança erro se obrigatória e não encontrada."""
    valor = os.getenv(nome)
    if obrigatoria and not valor:
        raise ValueError(f"❌ Variável de ambiente obrigatória não encontrada: {nome}")
    return valor

def carregar_configuracoes():
    """Carrega todas as configurações do arquivo .env."""
    try:
        # ⚙️ Configurações de conexão ORIGEM
        origem = {
            'host': obter_variavel('DB_ORIGEM_HOST'),
            'dbname': obter_variavel('DB_ORIGEM_NAME'),
            'user': obter_variavel('DB_ORIGEM_USER'),
            'password': obter_variavel('DB_ORIGEM_PASSWORD'),
            'sslmode': obter_variavel('DB_ORIGEM_SSLMODE', obrigatoria=False) or 'disable'
        }

        # ⚙️ Configurações de conexão DESTINO
        destino = {
            'host': obter_variavel('DB_DESTINO_HOST'),
            'port': obter_variavel('DB_DESTINO_PORT', obrigatoria=False) or '5432',
            'dbname': obter_variavel('DB_DESTINO_NAME'),
            'user': obter_variavel('DB_DESTINO_USER'),
            'password': obter_variavel('DB_DESTINO_PASSWORD')
        }

        # ⏱️ Configurações de sincronização em tempo real
        # Intervalo padrão: 300 segundos (5 minutos)
        intervalo_verificacao = int(obter_variavel('INTERVALO_VERIFICACAO', obrigatoria=False) or '300')
        
        return origem, destino, intervalo_verificacao
    
    except ValueError as e:
        print("=" * 60)
        print("❌ ERRO DE CONFIGURAÇÃO")
        print("=" * 60)
        print(str(e))
        print("\n📝 Verifique se o arquivo .env existe e contém todas as variáveis necessárias")
        print("=" * 60)
        raise

# Carregar configurações
ORIGEM, DESTINO, INTERVALO_VERIFICACAO = carregar_configuracoes()

# 🧱 Query incremental (busca apenas registros novos)
def query_alertadb_incremental(ultima_sincronizacao):
    """Retorna query para buscar apenas registros novos desde a última sincronização.
    
    Usa DISTINCT ON para garantir apenas um registro por (dia, estacao_id),
    mantendo o registro com o maior ID (mais recente), que é exatamente como
    está no banco alertadb.
    """
    # Formatar timestamp corretamente para PostgreSQL
    if isinstance(ultima_sincronizacao, datetime):
        timestamp_str = ultima_sincronizacao.strftime('%Y-%m-%d %H:%M:%S')
    else:
        timestamp_str = str(ultima_sincronizacao)
    
    return f"""
SELECT DISTINCT ON (el."horaLeitura", el.estacao_id)
    el."horaLeitura" AS "Dia",
    elc.m05,
    elc.m10,
    elc.m15,
    elc.h01,
    elc.h04,
    elc.h24,
    elc.h96,
    ee.nome AS "Estacao",
    el.estacao_id
FROM public.estacoes_leitura AS el
JOIN public.estacoes_leiturachuva AS elc
    ON elc.leitura_id = el.id
JOIN public.estacoes_estacao AS ee
    ON ee.id = el.estacao_id
WHERE el."horaLeitura" > '{timestamp_str}'::timestamp
ORDER BY el."horaLeitura" ASC, el.estacao_id ASC, el.id DESC;
"""

def testar_conexoes():
    """Testa as conexões com ambos os bancos antes de sincronizar."""
    print("=" * 60)
    print("TESTE DE CONEXOES")
    print("=" * 60)
    
    try:
        conn_origem = psycopg2.connect(**ORIGEM)
        print("   ✅ CONEXÃO ORIGEM: SUCESSO!")
        conn_origem.close()
        
        conn_destino = psycopg2.connect(**DESTINO)
        print("   ✅ CONEXÃO DESTINO: SUCESSO!")
        conn_destino.close()
        return True
        
    except Exception as e:
        print(f"   ❌ ERRO: {e}")
        return False

def verificar_tabela_vazia():
    """Verifica se a tabela pluviometricos está vazia."""
    conn_destino = None
    cur_destino = None
    
    try:
        conn_destino = psycopg2.connect(**DESTINO)
        cur_destino = conn_destino.cursor()
        
        cur_destino.execute("SELECT COUNT(*) FROM pluviometricos;")
        resultado = cur_destino.fetchone()
        
        return resultado[0] == 0 if resultado else True
            
    except Exception as e:
        print(f'⚠️ Erro ao verificar tabela: {e}')
        return True
    finally:
        if cur_destino:
            cur_destino.close()
        if conn_destino:
            conn_destino.close()

def garantir_datetime(valor):
    """
    Garante que o valor seja um objeto datetime naive (sem timezone).
    Sempre retorna um datetime naive para evitar problemas de comparação.
    O banco de dados já trata o ajuste de horário de verão na coluna dia.
    """
    resultado = None
    
    if isinstance(valor, datetime):
        # Se já é datetime, converter para naive se necessário
        resultado = tornar_datetime_naive(valor)
    elif isinstance(valor, str):
        try:
            # Tentar remover timezone info primeiro
            valor_limpo = re.sub(r'[+-]\d{2}:\d{2}$', '', valor)
            valor_limpo = re.sub(r'[+-]\d{4}$', '', valor_limpo)  # Remove também formato +0000
            valor_limpo = valor_limpo.strip()
            
            formatos = [
                '%Y-%m-%d %H:%M:%S.%f',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%dT%H:%M:%S.%f',
                '%Y-%m-%dT%H:%M:%S',
            ]
            
            for fmt in formatos:
                try:
                    resultado = datetime.strptime(valor_limpo, fmt)
                    break
                except ValueError:
                    continue
            
            if resultado is None:
                try:
                    # Tentar usar fromisoformat e depois remover timezone
                    valor_sem_tz = valor.split('+')[0].split('-')[0] if '+' in valor or (valor.count('-') > 2) else valor
                    valor_sem_tz = re.sub(r'[+-]\d{2}:\d{2}$', '', valor_sem_tz)
                    valor_sem_tz = re.sub(r'[+-]\d{4}$', '', valor_sem_tz)
                    resultado = datetime.fromisoformat(valor_sem_tz.replace('T', ' ').split('.')[0])
                    resultado = tornar_datetime_naive(resultado)
                except:
                    resultado = datetime.now() - timedelta(seconds=300)
        except Exception:
            resultado = datetime.now() - timedelta(seconds=300)
    else:
        try:
            if hasattr(valor, 'year') and hasattr(valor, 'month') and hasattr(valor, 'day'):
                resultado = datetime.combine(valor, datetime.min.time())
            else:
                resultado = datetime.now() - timedelta(seconds=300)
        except:
            resultado = datetime.now() - timedelta(seconds=300)
    
    # Garantir que é naive (sem timezone)
    if resultado:
        return tornar_datetime_naive(resultado)
    
    return datetime.now() - timedelta(seconds=300)

def obter_ultima_sincronizacao():
    """Obtém o timestamp da última leitura sincronizada do banco de destino."""
    conn_destino = None
    cur_destino = None
    
    try:
        conn_destino = psycopg2.connect(**DESTINO)
        cur_destino = conn_destino.cursor()
        
        # Busca o último timestamp sincronizado
        cur_destino.execute("SELECT MAX(dia) FROM pluviometricos;")
        resultado = cur_destino.fetchone()
        
        if resultado and resultado[0]:
            return garantir_datetime(resultado[0])
        else:
            # Se não houver registros, retorna timestamp de 5 minutos atrás
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
    """Atualiza apenas os novos dados desde a última sincronização."""
    conn_origem = None
    cur_origem = None
    conn_destino = None
    cur_destino = None
    
    timestamp_atual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        # Verificar se a tabela está vazia
        tabela_vazia = verificar_tabela_vazia()
        
        if tabela_vazia:
            print(f'\n⚠️  ATENÇÃO: A tabela está VAZIA!')
            print(f'   Execute PRIMEIRO o script carregar_pluviometricos_historicos.py')
            print(f'   para fazer a carga inicial dos dados históricos.')
            print(f'   Pulando esta verificação...\n')
            return 0
        
        # Obter último timestamp sincronizado
        ultima_sincronizacao = obter_ultima_sincronizacao()
        
        # Validar que temos uma data válida
        if ultima_sincronizacao == datetime(1997, 1, 1) or ultima_sincronizacao < datetime(1997, 1, 1):
            print(f'\n⚠️  ATENÇÃO: Última sincronização não encontrada ou inválida!')
            print(f'   Execute PRIMEIRO o script carregar_pluviometricos_historicos.py')
            print(f'   para fazer a carga inicial dos dados históricos.')
            print(f'   Pulando esta verificação...\n')
            return 0
        
        # Conectar ao banco origem
        conn_origem = psycopg2.connect(**ORIGEM)
        cur_origem = conn_origem.cursor()
        
        # Buscar apenas registros novos desde a última sincronização
        query = query_alertadb_incremental(ultima_sincronizacao)
        print(f'🔍 Verificando novos registros desde {ultima_sincronizacao.strftime("%Y-%m-%d %H:%M:%S")}...')
        
        # Executar query
        cur_origem.execute(query)
        dados = cur_origem.fetchall()

        if not dados:
            print(f'   ✓ Nenhum novo dado encontrado. [{timestamp_atual}]')
            return 0

        # Conectar ao banco destino
        conn_destino = psycopg2.connect(**DESTINO)
        cur_destino = conn_destino.cursor()

        # Garantir que os timestamps são datetime naive (sem timezone)
        # Formato dos dados: (dia, m05, m10, m15, h01, h04, h24, h96, estacao, estacao_id)
        dados_ajustados = []
        for registro in dados:
            dia_original = registro[0]
            # Garantir que é datetime naive (o banco já trata horário de verão)
            dia_ajustado = garantir_datetime(dia_original)
            # Criar nova tupla com o timestamp ajustado
            registro_ajustado = (dia_ajustado,) + registro[1:]
            dados_ajustados.append(registro_ajustado)

        # ⚠️ IMPORTANTE: ON CONFLICT DO NOTHING pois a query já garante apenas um registro
        # por (dia, estacao_id) usando DISTINCT ON com ORDER BY id DESC (mais recente)
        insert_sql = '''
        INSERT INTO pluviometricos
        (dia, m05, m10, m15, h01, h04, h24, h96, estacao, estacao_id)
        VALUES %s
        ON CONFLICT (dia, estacao_id) DO NOTHING;
        '''

        # Inserir dados ajustados (ON CONFLICT DO NOTHING evita duplicatas automaticamente)
        execute_values(cur_destino, insert_sql, dados_ajustados)
        conn_destino.commit()
        
        total_inseridos = len(dados)
        
        # Obter o último timestamp sincronizado para exibir
        cur_destino.execute("SELECT MAX(dia) FROM pluviometricos;")
        ultimo_timestamp = cur_destino.fetchone()
        ultimo_ts_str = ""
        if ultimo_timestamp and ultimo_timestamp[0]:
            ultimo_ts = garantir_datetime(ultimo_timestamp[0])
            ultimo_ts_str = f". Último: {ultimo_ts.strftime('%Y-%m-%d %H:%M:%S')}"
        
        print(f'   ✅ {total_inseridos:,} novo(s) registro(s) sincronizado(s){ultimo_ts_str} [{timestamp_atual}]')
        
        return total_inseridos

    except Exception as e:
        print(f'   ❌ Erro na atualização: {e} [{timestamp_atual}]')
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
    Útil para ser chamada por cron, Prefect ou outros agendadores.
    
    Returns:
        int: Número de registros sincronizados (0 se nenhum ou erro)
    """
    try:
        return atualizar_dados_incrementais()
    except Exception as e:
        print(f"❌ Erro na sincronização única: {e}")
        return 0

def main(modo_continuo=True):
    """
    Função principal que executa atualização incremental.
    
    Args:
        modo_continuo (bool): Se True, executa em loop infinito. 
                             Se False, executa uma única vez e encerra.
    """
    print("=" * 70)
    print("🌧️ ATUALIZADOR INCREMENTAL EM TEMPO REAL")
    print("=" * 70)
    print()
    print("🎯 PROPÓSITO:")
    print("   Este script atualiza APENAS os NOVOS dados desde a última sincronização.")
    if modo_continuo:
        print("   Mantém os dados atualizados em tempo real a cada 5 minutos.")
    else:
        print("   Executa uma única sincronização.")
    print()
    print("📋 O QUE SERÁ FEITO:")
    print("   ✅ Buscar apenas registros NOVOS desde a última sincronização")
    if modo_continuo:
        print("   ✅ Verificar novos dados a cada {} segundos ({} minutos)".format(
            INTERVALO_VERIFICACAO, INTERVALO_VERIFICACAO // 60))
        print("   ✅ Executar em modo contínuo até ser interrompido (Ctrl+C)")
    else:
        print("   ✅ Executar uma única sincronização")
    print()
    print("🔒 PROTEÇÕES CONTRA SOBREPOSIÇÃO:")
    print("   ✅ ON CONFLICT DO NOTHING: Previne duplicatas e sobreposição")
    print("   ✅ Chave primária (dia, estacao_id): Garante unicidade")
    print("   ✅ NUNCA modifica dados existentes")
    print("   ✅ NUNCA deleta dados")
    print("   ✅ APENAS adiciona novos registros")
    print()
    print("⚠️  PRÉ-REQUISITO:")
    print("   ⚠️  Certifique-se de ter executado carregar_pluviometricos_historicos.py PRIMEIRO")
    print("   ⚠️  Este script NÃO funciona se a tabela estiver vazia")
    print("=" * 70)
    
    # Testar conexões
    if not testar_conexoes():
        print("\n❌ Falha nos testes de conexão. Abortando...")
        return

    if modo_continuo:
        # Executar atualização incremental em tempo real
        print(f"\n🚀 Iniciando atualização incremental em tempo real...")
        print(f"   Verificando novos dados a cada {INTERVALO_VERIFICACAO} segundos")
        print(f"   Pressione Ctrl+C para parar\n")
        print("-" * 60)
        
        total_atualizado = 0
        
        try:
            while True:
                registros = atualizar_dados_incrementais()
                total_atualizado += registros
                
                # Aguardar próximo ciclo
                print(f'\n⏱️  Aguardando {INTERVALO_VERIFICACAO} segundos até a próxima verificação...\n')
                time.sleep(INTERVALO_VERIFICACAO)
                
        except KeyboardInterrupt:
            print("\n" + "=" * 60)
            print(f"⚠️  Programa interrompido pelo usuário.")
            print(f"📊 Total de registros atualizados nesta sessão: {total_atualizado:,}")
            print("=" * 60)
        except Exception as e:
            print(f"\n❌ Erro fatal: {e}")
            print("Encerrando programa...")
    else:
        # Executar uma única sincronização
        print(f"\n🚀 Executando sincronização única...\n")
        print("-" * 60)
        
        try:
            registros = executar_sincronizacao_unica()
            print("\n" + "=" * 60)
            print(f"✅ Sincronização concluída.")
            print(f"📊 Registros atualizados: {registros:,}")
            print("=" * 60)
        except Exception as e:
            print(f"\n❌ Erro na sincronização: {e}")
            print("Encerrando programa...")

if __name__ == "__main__":
    import sys
    # Verificar se foi passado argumento --once para execução única
    modo_continuo = "--once" not in sys.argv
    main(modo_continuo=modo_continuo)

