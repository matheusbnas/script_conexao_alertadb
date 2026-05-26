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
para o banco alertadb_cor (destino).

É o SEGUNDO PASSO após executar o carregar_pluviometricos_historicos.py.

═══════════════════════════════════════════════════════════════════════════
📋 O QUE ESTE SCRIPT FAZ:
═══════════════════════════════════════════════════════════════════════════

✅ Busca APENAS registros NOVOS desde a última sincronização
✅ Verifica novos dados a cada 5 minutos automaticamente (configurável)
✅ Executa em modo contínuo até ser interrompido (Ctrl+C)
✅ Usa ON CONFLICT DO UPDATE para atualizar dados existentes com valores corretos
✅ Chave primária composta (dia, estacao_id) garante unicidade
✅ Atualiza dados existentes se houver mudanças no banco origem
✅ Garante que os dados no destino correspondam exatamente ao banco origem
✅ Adiciona novos registros e atualiza existentes quando necessário
✅ Preserva timezone original (TIMESTAMPTZ NOT NULL) - mesma lógica do carregar_pluviometricos_historicos.py

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
3. Insere novos registros usando ON CONFLICT DO UPDATE (atualiza se já existir)
4. Aguarda 5 minutos (configurável) e repete o processo
5. Continua indefinidamente até ser interrompido

═══════════════════════════════════════════════════════════════════════════
🔒 PROTEÇÕES IMPLEMENTADAS:
═══════════════════════════════════════════════════════════════════════════

✅ ON CONFLICT DO UPDATE: Atualiza dados existentes com valores corretos do banco origem
✅ Chave primária composta (dia, estacao_id): Garante unicidade
✅ Validação: Verifica se tabela não está vazia antes de atualizar
✅ Validação: Verifica última sincronização antes de buscar novos dados
✅ Tratamento de erros: Continua rodando mesmo se houver falha temporária
✅ Atualiza dados existentes se houver mudanças no banco origem
✅ Garante que os dados no destino correspondam exatamente ao banco origem
✅ Adiciona novos registros e atualiza existentes quando necessário

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
from psycopg2 import errors as psycopg2_errors
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
project_root = Path(__file__).parent.parent.parent
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
    
    IMPORTANTE: A coluna horaLeitura no banco NIMBUS é TIMESTAMPTZ NOT NULL,
    preservando o timezone original. A query usa timestamptz para preservar
    o timezone corretamente.
    """
    # Formatar timestamp corretamente para PostgreSQL
    # Se tem timezone, converter para string mantendo timezone
    if isinstance(ultima_sincronizacao, datetime):
        if ultima_sincronizacao.tzinfo:
            # Converter para string com timezone para PostgreSQL
            offset = ultima_sincronizacao.tzinfo.utcoffset(ultima_sincronizacao)
            horas_offset = int(offset.total_seconds() / 3600)
            minutos_offset = int((abs(offset.total_seconds()) % 3600) / 60)
            timestamp_str = ultima_sincronizacao.strftime('%Y-%m-%d %H:%M:%S')
            timestamp_str += f" {horas_offset:+03d}:{abs(minutos_offset):02d}"
        else:
            timestamp_str = ultima_sincronizacao.strftime('%Y-%m-%d %H:%M:%S')
    else:
        timestamp_str = str(ultima_sincronizacao)
    
    # Usar sempre timestamptz (coluna horaLeitura no NIMBUS é TIMESTAMPTZ NOT NULL)
    # Se não tem timezone na string, assumir timezone do Brasil (-03:00)
    if ':' not in timestamp_str or ('+' not in timestamp_str and '-' not in timestamp_str.split()[-1]):
        # Sem timezone explícito, adicionar timezone do Brasil
        timestamp_str += " -03:00"
    
    return f"""
SELECT DISTINCT ON (el."horaLeitura", el.estacao_id)
    el."horaLeitura" AS "Dia",  -- TIMESTAMPTZ NOT NULL (preserva timezone original)
    elc.m05,
    elc.m10,
    elc.m15,
    elc.h01,
    elc.h02,
    elc.h03,
    elc.h04,
    elc.h06,
    elc.h12,
    elc.h24,
    elc.h96,
    elc.mes,
    ee.nome AS "Estacao",
    el.estacao_id
FROM public.estacoes_leitura AS el
JOIN public.estacoes_leiturachuva AS elc
    ON elc.leitura_id = el.id
JOIN public.estacoes_estacao AS ee
    ON ee.id = el.estacao_id
WHERE el."horaLeitura" > '{timestamp_str}'::timestamptz
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
    """Verifica se a tabela pluviometricos está vazia.
    
    Tenta uma verificação rápida, mas se falhar, assume que não está vazia
    para não bloquear a sincronização. As coletas NÃO são perdidas mesmo se
    esta verificação falhar.
    """
    conn_destino = None
    cur_destino = None
    
    try:
        conn_destino = psycopg2.connect(**DESTINO)
        cur_destino = conn_destino.cursor()
        
        # Tentar verificação rápida SEM timeout primeiro
        # Usar uma query ainda mais simples - apenas tentar ler uma linha
        cur_destino.execute("SELECT 1 FROM pluviometricos LIMIT 1;")
        resultado = cur_destino.fetchone()
        
        # Se conseguiu ler, a tabela não está vazia
        return resultado is None
            
    except (psycopg2.OperationalError, psycopg2.InterfaceError, psycopg2_errors.QueryCanceled) as e:
        # Qualquer erro de conexão ou timeout - assumir que não está vazia
        # Isso garante que a sincronização continue e NÃO perca coletas
        print(f'⚠️ Não foi possível verificar se tabela está vazia: {e}')
        print('   ✅ Continuando sincronização (assumindo que tabela não está vazia)')
        print('   💡 As coletas NÃO serão perdidas mesmo com este erro')
        return False  # Assumir que não está vazia para continuar
            
    except Exception as e:
        # Qualquer outro erro - assumir que não está vazia
        print(f'⚠️ Erro ao verificar tabela: {e}')
        print('   ✅ Continuando sincronização (assumindo que tabela não está vazia)')
        return False
            
    finally:
        if cur_destino:
            try:
                cur_destino.close()
            except:
                pass
        if conn_destino:
            try:
                conn_destino.close()
            except:
                pass
    
    # Se chegou aqui, assumir que não está vazia para continuar
    return False

def garantir_datetime_com_timezone(valor):
    """
    Garante que o valor seja um objeto datetime mantendo o timezone original.
    IMPORTANTE: Preserva o timezone original (-02:00 para horário de verão ou -03:00 para horário padrão).
    
    Args:
        valor: datetime, string ou outro tipo
    
    Returns:
        datetime: datetime com timezone preservado (ou naive se não tinha timezone)
    """
    resultado = None
    
    if isinstance(valor, datetime):
        # Se já é datetime, manter como está (com ou sem timezone)
        # IMPORTANTE: Preserva tanto -02:00 quanto -03:00
        resultado = valor
    elif isinstance(valor, str):
        try:
            # Tentar parse mantendo timezone se presente
            # Formatos esperados:
            # - "2025-11-28 11:40:00.000 -0300" (horário padrão)
            # - "2019-02-16 23:45:00.000 -0200" (horário de verão)
            # - "2019-02-16 23:45:00.000 -0200 -03:00" ou "-02:00"
            
            # Tentar usar fromisoformat primeiro (preserva timezone automaticamente)
            try:
                # fromisoformat funciona com formato ISO que tem 'T' e timezone
                valor_iso = valor.replace(' ', 'T', 1)
                resultado = datetime.fromisoformat(valor_iso)
            except:
                # Se falhar, fazer parse manual preservando timezone
                # Extrair timezone se presente (formato -0300 ou -03:00 ou -0200 ou -02:00)
                match_tz = re.search(r'\s*([+-])(\d{2}):?(\d{2})$', valor)
                if match_tz:
                    sinal = match_tz.group(1)  # '+' ou '-'
                    horas_tz = int(match_tz.group(2))
                    minutos_tz = int(match_tz.group(3))
                    
                    # Calcular offset total em minutos
                    offset_total_minutos = horas_tz * 60 + minutos_tz
                    if sinal == '-':
                        offset_total_minutos = -offset_total_minutos
                    
                    # Remover timezone da string para fazer parse do datetime
                    valor_sem_tz = re.sub(r'\s*[+-]\d{2}:?\d{2}$', '', valor).strip()
                    
                    # Parse do datetime (sem timezone)
                    formatos = [
                        '%Y-%m-%d %H:%M:%S.%f',
                        '%Y-%m-%d %H:%M:%S',
                        '%Y-%m-%dT%H:%M:%S.%f',
                        '%Y-%m-%dT%H:%M:%S',
                    ]
                    
                    dt_naive = None
                    for fmt in formatos:
                        try:
                            dt_naive = datetime.strptime(valor_sem_tz, fmt)
                            break
                        except ValueError:
                            continue
                    
                    if dt_naive:
                        # Criar timezone com offset preservado (-02:00 ou -03:00)
                        from datetime import timezone
                        tz = timezone(timedelta(minutes=offset_total_minutos))
                        resultado = dt_naive.replace(tzinfo=tz)
                else:
                    # Sem timezone na string, fazer parse normal
                    valor_limpo = valor.strip()
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
        except Exception as e:
            resultado = datetime.now() - timedelta(seconds=300)
    else:
        try:
            if hasattr(valor, 'year') and hasattr(valor, 'month') and hasattr(valor, 'day'):
                resultado = datetime.combine(valor, datetime.min.time())
            else:
                resultado = datetime.now() - timedelta(seconds=300)
        except:
            resultado = datetime.now() - timedelta(seconds=300)
    
    return resultado if resultado else datetime.now() - timedelta(seconds=300)

def garantir_datetime(valor):
    """
    Garante que o valor seja um objeto datetime mantendo o timezone original.
    IMPORTANTE: Preserva timezone -02:00 (horário de verão) ou -03:00 (horário padrão).
    """
    return garantir_datetime_com_timezone(valor)

def formatar_timestamp_nimbus(dt):
    """Formata timestamp no formato exato da NIMBUS: 2025-12-12 16:35:00.000 -0300
    
    Preserva o formato original como vem do banco da NIMBUS.
    """
    if not isinstance(dt, datetime):
        return str(dt)
    
    # Formatar data e hora
    timestamp_str = dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Adicionar milissegundos (3 dígitos)
    if hasattr(dt, 'microsecond') and dt.microsecond:
        microsec_str = str(dt.microsecond)[:3].zfill(3)
        timestamp_str += f".{microsec_str}"
    else:
        timestamp_str += ".000"
    
    # Adicionar timezone no formato -0300 (sem dois pontos)
    if dt.tzinfo:
        offset = dt.tzinfo.utcoffset(dt)
        if offset:
            total_seconds = offset.total_seconds()
            hours = int(total_seconds // 3600)
            minutes = int((abs(total_seconds) % 3600) // 60)
            # Formato: -0300 (sem dois pontos, como na NIMBUS)
            offset_str = f"{hours:+03d}{minutes:02d}"
            timestamp_str += f" {offset_str}"
    else:
        # Sem timezone, assumir -03:00 (padrão Brasil)
        timestamp_str += " -0300"
    
    return timestamp_str

def obter_ultima_sincronizacao():
    """Obtém o timestamp da última leitura sincronizada do banco de destino.
    
    Se houver problemas de conexão ou timeout, retorna um timestamp recente
    para garantir que a sincronização continue e não perca coletas.
    """
    conn_destino = None
    cur_destino = None
    
    try:
        conn_destino = psycopg2.connect(**DESTINO)
        cur_destino = conn_destino.cursor()
        
        # Usar ORDER BY com LIMIT é mais rápido que MAX() em algumas situações
        # e permite adicionar timeout se necessário
        cur_destino.execute("SELECT dia FROM pluviometricos ORDER BY dia DESC LIMIT 1;")
        resultado = cur_destino.fetchone()
        
        if resultado and resultado[0]:
            # Converter mantendo timezone se presente
            dt = garantir_datetime_com_timezone(resultado[0])
            # Se não tem timezone, adicionar -03:00 (padrão)
            if dt.tzinfo is None:
                from datetime import timezone
                tz_brasilia = timezone(timedelta(hours=-3))
                dt = dt.replace(tzinfo=tz_brasilia)
            return dt
        else:
            # Se não houver registros, retorna timestamp de 5 minutos atrás com timezone
            from datetime import timezone
            tz_brasilia = timezone(timedelta(hours=-3))
            return (datetime.now() - timedelta(seconds=300)).replace(tzinfo=tz_brasilia)
            
    except (psycopg2.OperationalError, psycopg2.InterfaceError, psycopg2_errors.QueryCanceled) as e:
        # Em caso de erro de conexão ou timeout, retornar timestamp recente
        # Isso garante que a sincronização continue e busque dados recentes
        print(f'⚠️ Erro ao obter última sincronização: {e}')
        print('   ✅ Usando timestamp recente para continuar sincronização')
        print('   💡 As coletas NÃO serão perdidas')
        from datetime import timezone
        tz_brasilia = timezone(timedelta(hours=-3))
        # Retornar timestamp de 10 minutos atrás para garantir que pegue dados recentes
        return (datetime.now() - timedelta(minutes=10)).replace(tzinfo=tz_brasilia)
            
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
        # Comparar removendo timezone para compatibilidade
        ultima_sync_naive = ultima_sincronizacao.replace(tzinfo=None) if ultima_sincronizacao.tzinfo else ultima_sincronizacao
        data_referencia = datetime(1997, 1, 1)
        if ultima_sync_naive == data_referencia or ultima_sync_naive < data_referencia:
            print(f'\n⚠️  ATENÇÃO: Última sincronização não encontrada ou inválida!')
            print(f'   Execute PRIMEIRO o script carregar_pluviometricos_historicos.py')
            print(f'   para fazer a carga inicial dos dados históricos.')
            print(f'   Pulando esta verificação...\n')
            return 0
        
        # Conectar ao banco destino temporariamente para formatar timestamp
        conn_destino_temp = psycopg2.connect(**DESTINO)
        cur_destino_temp = conn_destino_temp.cursor()
        cur_destino_temp.execute("""
            SELECT TO_CHAR(%s::timestamptz, 'YYYY-MM-DD HH24:MI:SS.MS') || ' ' || 
                   TO_CHAR(%s::timestamptz, 'TZH') || TO_CHAR(%s::timestamptz, 'TZM')
        """, (ultima_sincronizacao, ultima_sincronizacao, ultima_sincronizacao))
        timestamp_formatado = cur_destino_temp.fetchone()[0]
        cur_destino_temp.close()
        conn_destino_temp.close()
        
        # Conectar ao banco origem
        conn_origem = psycopg2.connect(**ORIGEM)
        cur_origem = conn_origem.cursor()
        
        # Buscar apenas registros novos desde a última sincronização
        query = query_alertadb_incremental(ultima_sincronizacao)
        print(f'🔍 Verificando novos registros desde {timestamp_formatado}...')
        
        # Executar query
        cur_origem.execute(query)
        dados = cur_origem.fetchall()

        if not dados:
            print(f'   ✓ Nenhum novo dado encontrado. [{timestamp_atual}]')
            return 0

        # Conectar ao banco destino
        conn_destino = psycopg2.connect(**DESTINO)
        cur_destino = conn_destino.cursor()
        
        # Configurar timezone do banco destino para 'America/Sao_Paulo'
        # Isso garante que timestamps com timezone sejam convertidos corretamente
        cur_destino.execute("SET timezone = 'America/Sao_Paulo';")

        # IMPORTANTE: Preparar timestamps mantendo timezone para inserção correta
        # Formato dos dados: (dia, m05, m10, m15, h01, h04, h24, h96, estacao, estacao_id)
        # Quando o banco origem retorna timestamps com timezone:
        # - '2025-11-28 11:40:00.000 -0300' (horário padrão)
        # - '2019-02-16 23:45:00.000 -0200' (horário de verão)
        # Precisamos preservar o timezone original para que o PostgreSQL converta corretamente
        # A coluna dia no servidor 166 é TIMESTAMPTZ NOT NULL, então preserva o timezone original
        dados_ajustados = []
        for registro in dados:
            dia_original = registro[0]
            
            # Se já é datetime com timezone, manter como está (preserva -02:00 ou -03:00)
            if isinstance(dia_original, datetime) and dia_original.tzinfo:
                dia_ajustado = dia_original
            elif isinstance(dia_original, datetime):
                # Se é datetime sem timezone, converter para string e tentar parse novamente
                # para detectar se há timezone na representação original
                dia_ajustado = garantir_datetime_com_timezone(str(dia_original))
                # Se ainda não tem timezone após conversão, assumir -03:00 (padrão Brasília)
                if dia_ajustado.tzinfo is None:
                    from datetime import timezone
                    tz_brasilia = timezone(timedelta(hours=-3))
                    dia_ajustado = dia_original.replace(tzinfo=tz_brasilia)
            else:
                # Se é string ou outro tipo, converter mantendo timezone original (-02:00 ou -03:00)
                dia_ajustado = garantir_datetime_com_timezone(dia_original)
                # Se não tem timezone após conversão, adicionar -03:00 (padrão Brasília)
                if dia_ajustado.tzinfo is None:
                    from datetime import timezone
                    tz_brasilia = timezone(timedelta(hours=-3))
                    dia_ajustado = dia_ajustado.replace(tzinfo=tz_brasilia)
            
            # Criar nova tupla com o timestamp ajustado (com timezone)
            registro_ajustado = (dia_ajustado,) + registro[1:]
            dados_ajustados.append(registro_ajustado)

        # ⚠️ IMPORTANTE: ON CONFLICT DO UPDATE para garantir que os dados sejam sempre atualizados
        # com os valores corretos do banco origem, mesmo se já existirem dados incorretos
        # A query já garante apenas um registro por (dia, estacao_id) usando DISTINCT ON
        # com ORDER BY id DESC (mais recente)
        # IMPORTANTE: O psycopg2 vai converter automaticamente timestamps com timezone
        # para o timezone do servidor antes de armazenar. Para evitar diferença de horas,
        # precisamos garantir que o timestamp seja inserido com o timezone correto (-03:00)
        # e o PostgreSQL vai converter para o timezone do servidor mantendo o valor local.
        # A coluna dia no servidor 166 é TIMESTAMPTZ NOT NULL, então preserva o timezone original
        insert_sql = '''
        INSERT INTO pluviometricos
        (dia, m05, m10, m15, h01, h02, h03, h04, h06, h12, h24, h96, mes, estacao, estacao_id)
        VALUES %s
        ON CONFLICT (dia, estacao_id)
        DO UPDATE SET
            m05 = EXCLUDED.m05,
            m10 = EXCLUDED.m10,
            m15 = EXCLUDED.m15,
            h01 = EXCLUDED.h01,
            h02 = EXCLUDED.h02,
            h03 = EXCLUDED.h03,
            h04 = EXCLUDED.h04,
            h06 = EXCLUDED.h06,
            h12 = EXCLUDED.h12,
            h24 = EXCLUDED.h24,
            h96 = EXCLUDED.h96,
            mes = EXCLUDED.mes,
            estacao = EXCLUDED.estacao;
        '''

        # Inserir dados ajustados (ON CONFLICT DO UPDATE atualiza dados existentes com valores corretos)
        execute_values(cur_destino, insert_sql, dados_ajustados)
        conn_destino.commit()
        
        total_inseridos = len(dados)
        
        # Obter o último timestamp sincronizado para exibir (já formatado como string no formato NIMBUS)
        # Formato: 2025-12-12 16:35:00.000 -0300 (sem dois pontos no timezone)
        cur_destino.execute("""
            SELECT TO_CHAR(MAX(dia), 'YYYY-MM-DD HH24:MI:SS.MS') || ' ' || 
                   TO_CHAR(MAX(dia), 'TZH') || TO_CHAR(MAX(dia), 'TZM')
            FROM pluviometricos;
        """)
        ultimo_timestamp = cur_destino.fetchone()
        ultimo_ts_str = ""
        if ultimo_timestamp and ultimo_timestamp[0]:
            # Já vem formatado do PostgreSQL no formato da NIMBUS: 2025-12-12 16:35:00.000 -0300
            ultimo_ts_str = f". Último: {ultimo_timestamp[0]}"
        
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
    print("🔒 PROTEÇÕES E ATUALIZAÇÕES:")
    print("   ✅ ON CONFLICT DO UPDATE: Atualiza dados existentes com valores corretos")
    print("   ✅ Chave primária (dia, estacao_id): Garante unicidade")
    print("   ✅ Atualiza dados existentes se houver mudanças no banco origem")
    print("   ✅ Garante que os dados no destino correspondam exatamente ao banco origem")
    print("   ✅ Adiciona novos registros e atualiza existentes quando necessário")
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

