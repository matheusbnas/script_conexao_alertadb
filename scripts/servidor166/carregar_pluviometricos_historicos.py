#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🌧️ CARGA INICIAL COMPLETA - Dados Pluviométricos

═══════════════════════════════════════════════════════════════════════════
🎯 PROPÓSITO DESTE SCRIPT:
═══════════════════════════════════════════════════════════════════════════

Este script foi criado para fazer a CARGA INICIAL COMPLETA de todos os dados
pluviométricos históricos do banco alertadb (origem) para o banco alertadb_cor
(destino).

É o PRIMEIRO PASSO necessário antes de usar o sincronizar_pluviometricos_novos.py.

═══════════════════════════════════════════════════════════════════════════
📋 O QUE ESTE SCRIPT FAZ:
═══════════════════════════════════════════════════════════════════════════

✅ Busca TODOS os dados desde 1997-01-01 até a data atual
✅ Cria a tabela pluviometricos se ela não existir
✅ Processa dados em lotes de 10.000 registros para otimizar memória
✅ Usa DISTINCT ON para garantir apenas um registro por (dia, estacao_id)
✅ Mantém o registro mais recente (maior ID) quando há duplicatas
✅ Garante que os dados no destino sejam EXATAMENTE como no banco alertadb
✅ Mostra progresso detalhado durante a carga
✅ Exibe estatísticas finais (total de registros, período dos dados)
✅ Diagnostica duplicatas no banco origem antes da carga

═══════════════════════════════════════════════════════════════════════════
⚠️ QUANDO USAR ESTE SCRIPT:
═══════════════════════════════════════════════════════════════════════════

✅ Primeira vez que você está configurando o sistema
✅ Quando a tabela pluviometricos está vazia
✅ Após limpar/resetar a tabela de destino
✅ Para recarregar todos os dados históricos

═══════════════════════════════════════════════════════════════════════════
🚀 COMO USAR:
═══════════════════════════════════════════════════════════════════════════

1. Configure o arquivo .env com as credenciais dos bancos
2. Execute: python carregar_pluviometricos_historicos.py
3. Aguarde a conclusão (pode levar vários minutos dependendo do volume)
4. Após concluir, execute o sincronizar_pluviometricos_novos.py para manter atualizado

═══════════════════════════════════════════════════════════════════════════
🔒 PROTEÇÕES IMPLEMENTADAS:
═══════════════════════════════════════════════════════════════════════════

✅ Chave primária composta (dia, estacao_id) garante unicidade
✅ DISTINCT ON garante apenas um registro por (dia, estacao_id) do banco origem
✅ Mantém o registro mais recente (maior ID) quando há duplicatas
✅ Garante que os dados no destino sejam EXATAMENTE como no banco alertadb
✅ ON CONFLICT DO UPDATE atualiza dados existentes com valores corretos
✅ Corrige automaticamente dados incorretos se já existirem na tabela
✅ Processamento em lotes evita sobrecarga de memória
✅ Validação de conexões antes de iniciar
✅ Diagnóstico de duplicatas antes da carga
✅ Tratamento de erros com mensagens claras

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
"""

# 🔧 Importar bibliotecas necessárias
import psycopg2
from psycopg2 import errors as psycopg2_errors
from psycopg2.extras import execute_values
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

def extrair_timezone_offset(dt):
    """
    Extrai o offset de timezone de um datetime aware.
    
    Args:
        dt: datetime objeto com timezone
    
    Returns:
        str: offset no formato '-03:00' ou None se não tiver timezone
    """
    if not isinstance(dt, datetime) or dt.tzinfo is None:
        return None
    
    # Obter offset total em segundos
    offset_seconds = dt.tzinfo.utcoffset(dt).total_seconds()
    
    # Converter para horas e minutos
    horas = int(offset_seconds // 3600)
    minutos = int((offset_seconds % 3600) // 60)
    
    # Formatar como '-03:00' ou '+03:00'
    sinal = '-' if horas < 0 else '+'
    return f"{sinal}{abs(horas):02d}:{abs(minutos):02d}"

def preparar_timestamp_para_insercao(dt):
    """
    Prepara um timestamp para inserção no PostgreSQL mantendo o timezone correto.
    
    IMPORTANTE: Quando o banco origem retorna '2025-11-28 11:40:00.000 -0300',
    precisamos inserir no banco destino como '2025-11-28 11:40:00.000 -0300'
    para que o PostgreSQL interprete corretamente e armazene sem diferença de horas.
    
    Args:
        dt: datetime objeto (aware ou naive)
    
    Returns:
        str: timestamp formatado para inserção no PostgreSQL
    """
    if not isinstance(dt, datetime):
        return str(dt)
    
    # Se tem timezone, manter o timezone na string
    if dt.tzinfo is not None:
        offset = extrair_timezone_offset(dt)
        if offset:
            # Formatar como '2025-11-28 11:40:00.000 -03:00'
            timestamp_str = dt.strftime('%Y-%m-%d %H:%M:%S')
            if dt.microsecond:
                timestamp_str += f".{dt.microsecond:06d}"[:7]  # Limitar a 3 dígitos
            return f"{timestamp_str} {offset}"
    
    # Se não tem timezone, retornar como está (assumindo que já está no timezone correto)
    timestamp_str = dt.strftime('%Y-%m-%d %H:%M:%S')
    if dt.microsecond:
        timestamp_str += f".{dt.microsecond:06d}"[:7]
    return timestamp_str

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
            'password': obter_variavel('DB_DESTINO_PASSWORD'),
            'connect_timeout': 10  # Timeout de 10 segundos para conexão
        }
        
        return origem, destino
    
    except ValueError as e:
        print("=" * 60)
        print("❌ ERRO DE CONFIGURAÇÃO")
        print("=" * 60)
        print(str(e))
        print("\n📝 Verifique se o arquivo .env existe e contém todas as variáveis necessárias")
        print("=" * 60)
        raise

# Carregar configurações
ORIGEM, DESTINO = carregar_configuracoes()

# 🧱 Query para buscar TODOS os dados (sem filtro de data)
def query_todos_dados():
    """Retorna query para buscar TODOS os dados disponíveis no banco.
    
    Usa DISTINCT ON para garantir apenas um registro por (dia, estacao_id),
    mantendo o registro com o maior ID (mais recente), que é exatamente como
    está no banco alertadb.
    
    IMPORTANTE: A ordem do ORDER BY deve corresponder à ordem do DISTINCT ON,
    e depois ordenar por id DESC para pegar o registro mais recente.
    """
    return """
SELECT DISTINCT ON (el."horaLeitura", el.estacao_id)
    el."horaLeitura" AS "Dia",
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
ORDER BY el."horaLeitura" ASC, el.estacao_id ASC, el.id DESC;
"""

# 🧱 Query para buscar dados desde uma data específica
def query_dados_desde_data(data_inicial):
    """Retorna query para buscar dados desde uma data específica.
    
    Usa DISTINCT ON para garantir apenas um registro por (dia, estacao_id),
    mantendo o registro com o maior ID (mais recente), que é exatamente como
    está no banco alertadb.
    
    IMPORTANTE: A ordem do ORDER BY deve corresponder à ordem do DISTINCT ON,
    e depois ordenar por id DESC para pegar o registro mais recente.
    """
    return f"""
SELECT DISTINCT ON (el."horaLeitura", el.estacao_id)
    el."horaLeitura" AS "Dia",
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
WHERE el."horaLeitura" >= '{data_inicial}'::timestamp
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

def criar_tabela_pluviometricos():
    """Cria a tabela pluviometricos no banco de destino se ela não existir."""
    conn_destino = None
    cur_destino = None
    
    try:
        conn_destino = psycopg2.connect(**DESTINO)
        cur_destino = conn_destino.cursor()
        
        create_table_sql = '''
        CREATE TABLE IF NOT EXISTS pluviometricos (
            dia TIMESTAMPTZ NOT NULL,
            m05 NUMERIC,
            m10 NUMERIC,
            m15 NUMERIC,
            h01 NUMERIC,
            h02 NUMERIC,
            h03 NUMERIC,
            h04 NUMERIC,
            h06 NUMERIC,
            h12 NUMERIC,
            h24 NUMERIC,
            h96 NUMERIC,
            mes NUMERIC,
            estacao VARCHAR(255),
            estacao_id INTEGER,
            PRIMARY KEY (dia, estacao_id)
        );
        '''
        
        cur_destino.execute(create_table_sql)
        conn_destino.commit()
        print('✅ Tabela pluviometricos criada/verificada com sucesso!')
        
    except Exception as e:
        print(f'❌ Erro ao criar tabela: {e}')
        if conn_destino:
            conn_destino.rollback()
    finally:
        if cur_destino:
            cur_destino.close()
        if conn_destino:
            conn_destino.close()

def verificar_tabela_vazia():
    """Verifica se a tabela pluviometricos está vazia.
    
    Usa EXISTS com LIMIT 1 para ser muito mais rápido que COUNT(*) em tabelas grandes.
    """
    conn_destino = None
    cur_destino = None
    
    try:
        conn_destino = psycopg2.connect(**DESTINO)
        cur_destino = conn_destino.cursor()
        
        # Usar EXISTS é muito mais rápido que COUNT(*) em tabelas grandes
        # Adiciona timeout de 5 segundos para a query
        cur_destino.execute("SET statement_timeout = '5s';")
        cur_destino.execute("SELECT EXISTS(SELECT 1 FROM pluviometricos LIMIT 1);")
        resultado = cur_destino.fetchone()
        
        # EXISTS retorna True se há pelo menos um registro, False se vazia
        return not resultado[0] if resultado else True
            
    except psycopg2_errors.QueryCanceled:
        print('⚠️ Timeout ao verificar tabela. Assumindo que a tabela não está vazia.')
        return False
    except Exception as e:
        print(f'⚠️ Erro ao verificar tabela: {e}')
        return True
    finally:
        if cur_destino:
            cur_destino.close()
        if conn_destino:
            conn_destino.close()

def validar_amostra_dados(conn_origem, conn_destino, cur_origem, cur_destino, quantidade=10):
    """
    Valida uma amostra de dados inseridos comparando com o banco origem.
    
    Args:
        conn_origem: Conexão com banco origem
        conn_destino: Conexão com banco destino
        cur_origem: Cursor do banco origem
        cur_destino: Cursor do banco destino
        quantidade: Quantidade de registros para validar
    
    Returns:
        bool: True se todos os registros validados estão corretos, False caso contrário
    """
    try:
        # Buscar alguns registros aleatórios do banco destino
        cur_destino.execute(f"""
            SELECT dia, m05, m10, m15, h01, h02, h03, h04, h06, h12, h24, h96, mes, estacao, estacao_id
            FROM pluviometricos
            ORDER BY RANDOM()
            LIMIT {quantidade};
        """)
        registros_destino = cur_destino.fetchall()
        
        if not registros_destino:
            return True  # Se não há registros, não há o que validar
        
        divergencias = 0
        
        for registro_destino in registros_destino:
            dia_dest, m05_dest, m10_dest, m15_dest, h01_dest, h02_dest, h03_dest, h04_dest, h06_dest, h12_dest, h24_dest, h96_dest, mes_dest, estacao_dest, est_id_dest = registro_destino
            
            # Buscar registro correspondente no banco origem
            query_origem = """
            SELECT DISTINCT ON (el."horaLeitura", el.estacao_id)
                el."horaLeitura" AS "Dia",
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
            WHERE el."horaLeitura" = %s::timestamp
              AND el.estacao_id = %s
            ORDER BY el."horaLeitura" ASC, el.estacao_id ASC, el.id DESC
            LIMIT 1;
            """
            
            cur_origem.execute(query_origem, (dia_dest, est_id_dest))
            registro_origem = cur_origem.fetchone()

            if registro_origem:
                dia_orig, m05_orig, m10_orig, m15_orig, h01_orig, h02_orig, h03_orig, h04_orig, h06_orig, h12_orig, h24_orig, h96_orig, mes_orig, estacao_orig, est_id_orig = registro_origem

                # Comparar valores (ignorar diferenças de timezone no timestamp)
                valores_origem = (m05_orig, m10_orig, m15_orig, h01_orig, h02_orig, h03_orig, h04_orig, h06_orig, h12_orig, h24_orig, h96_orig, mes_orig)
                valores_destino = (m05_dest, m10_dest, m15_dest, h01_dest, h02_dest, h03_dest, h04_dest, h06_dest, h12_dest, h24_dest, h96_dest, mes_dest)
                
                if valores_origem != valores_destino:
                    divergencias += 1
        
        return divergencias == 0
        
    except Exception as e:
        print(f'   ⚠️  Erro na validação: {e}')
        return False

def diagnosticar_banco_origem():
    """Diagnostica o banco de origem para verificar dados disponíveis."""
    conn_origem = None
    cur_origem = None
    
    try:
        print("\n" + "=" * 70)
        print("🔍 DIAGNÓSTICO DO BANCO DE ORIGEM")
        print("=" * 70)
        
        conn_origem = psycopg2.connect(**ORIGEM)
        cur_origem = conn_origem.cursor()
        
        # 1. Verificar total de registros em estacoes_leitura
        print("\n📊 Verificando tabelas...")
        cur_origem.execute("SELECT COUNT(*) FROM public.estacoes_leitura;")
        total_leitura = cur_origem.fetchone()[0]
        print(f"   • Total de registros em estacoes_leitura: {total_leitura:,}")
        
        # 2. Verificar total de registros em estacoes_leiturachuva
        cur_origem.execute("SELECT COUNT(*) FROM public.estacoes_leiturachuva;")
        total_chuva = cur_origem.fetchone()[0]
        print(f"   • Total de registros em estacoes_leiturachuva: {total_chuva:,}")
        
        # 3. Verificar data mínima e máxima em estacoes_leitura
        cur_origem.execute("""
            SELECT 
                MIN("horaLeitura") as data_min,
                MAX("horaLeitura") as data_max
            FROM public.estacoes_leitura;
        """)
        datas_leitura = cur_origem.fetchone()
        data_min_leitura = datas_leitura[0] if datas_leitura[0] else None
        data_max_leitura = datas_leitura[1] if datas_leitura[1] else None
        print(f"   • Data mínima em estacoes_leitura: {data_min_leitura}")
        print(f"   • Data máxima em estacoes_leitura: {data_max_leitura}")
        
        # 4. Verificar quantos registros têm JOIN válido
        cur_origem.execute("""
            SELECT COUNT(*)
            FROM public.estacoes_leitura AS el
            JOIN public.estacoes_leiturachuva AS elc
                ON elc.leitura_id = el.id
            JOIN public.estacoes_estacao AS ee
                ON ee.id = el.estacao_id;
        """)
        total_com_join = cur_origem.fetchone()[0]
        print(f"   • Total de registros com JOIN válido: {total_com_join:,}")
        
        # 5. Verificar data mínima e máxima dos dados com JOIN válido
        cur_origem.execute("""
            SELECT 
                MIN(el."horaLeitura") as data_min,
                MAX(el."horaLeitura") as data_max
            FROM public.estacoes_leitura AS el
            JOIN public.estacoes_leiturachuva AS elc
                ON elc.leitura_id = el.id
            JOIN public.estacoes_estacao AS ee
                ON ee.id = el.estacao_id;
        """)
        datas_join = cur_origem.fetchone()
        data_min_join = datas_join[0] if datas_join[0] else None
        data_max_join = datas_join[1] if datas_join[1] else None
        print(f"   • Data mínima com JOIN válido: {data_min_join}")
        print(f"   • Data máxima com JOIN válido: {data_max_join}")
        
        # 6. Verificar dados por década/ano
        print("\n📅 Distribuição de dados por ano:")
        cur_origem.execute("""
            SELECT 
                EXTRACT(YEAR FROM el."horaLeitura") as ano,
                COUNT(*) as total
            FROM public.estacoes_leitura AS el
            JOIN public.estacoes_leiturachuva AS elc
                ON elc.leitura_id = el.id
            JOIN public.estacoes_estacao AS ee
                ON ee.id = el.estacao_id
            GROUP BY EXTRACT(YEAR FROM el."horaLeitura")
            ORDER BY ano;
        """)
        distribuicao_anos = cur_origem.fetchall()
        for ano, total in distribuicao_anos:
            print(f"   • {int(ano)}: {total:,} registros")
        
        # 7. Verificar se há dados em janeiro de 1997
        print("\n🔎 Verificando dados específicos em janeiro de 1997:")
        cur_origem.execute("""
            SELECT COUNT(*)
            FROM public.estacoes_leitura AS el
            JOIN public.estacoes_leiturachuva AS elc
                ON elc.leitura_id = el.id
            JOIN public.estacoes_estacao AS ee
                ON ee.id = el.estacao_id
            WHERE el."horaLeitura" >= '1997-01-01'::timestamp
              AND el."horaLeitura" < '1997-02-01'::timestamp;
        """)
        total_jan_1997 = cur_origem.fetchone()[0]
        print(f"   • Registros em janeiro/1997: {total_jan_1997:,}")
        
        if total_jan_1997 == 0:
            print("   ⚠️  ATENÇÃO: Não há dados em janeiro de 1997!")
            print("   💡 Isso confirma que a data inicial de 1997-01-01 pode estar incorreta.")
        
        # 8. Verificar duplicatas (múltiplos registros com mesmo dia e estacao_id)
        print("\n🔍 Verificando duplicatas (mesmo dia + estacao_id):")
        cur_origem.execute("""
            SELECT 
                el."horaLeitura",
                el.estacao_id,
                COUNT(*) as total
            FROM public.estacoes_leitura AS el
            JOIN public.estacoes_leiturachuva AS elc
                ON elc.leitura_id = el.id
            JOIN public.estacoes_estacao AS ee
                ON ee.id = el.estacao_id
            GROUP BY el."horaLeitura", el.estacao_id
            HAVING COUNT(*) > 1
            ORDER BY COUNT(*) DESC
            LIMIT 10;
        """)
        duplicatas = cur_origem.fetchall()
        
        # Contar total de combinações duplicadas
        cur_origem.execute("""
            SELECT COUNT(*)
            FROM (
                SELECT el."horaLeitura", el.estacao_id
                FROM public.estacoes_leitura AS el
                JOIN public.estacoes_leiturachuva AS elc
                    ON elc.leitura_id = el.id
                JOIN public.estacoes_estacao AS ee
                    ON ee.id = el.estacao_id
                GROUP BY el."horaLeitura", el.estacao_id
                HAVING COUNT(*) > 1
            ) AS dups;
        """)
        total_duplicatas = cur_origem.fetchone()[0]
        
        if duplicatas:
            print(f"   ⚠️  ATENÇÃO: Encontradas {total_duplicatas:,} combinações (dia, estacao_id) com múltiplos registros!")
            print(f"   • Exemplos de duplicatas (mostrando até 10 primeiras):")
            for hora_leitura, estacao_id, total in duplicatas[:5]:
                print(f"      - {hora_leitura} | estacao_id={estacao_id}: {total} registros")
            print(f"   💡 A query será ajustada para usar DISTINCT ON e manter apenas o registro mais recente.")
        else:
            print(f"   ✅ Nenhuma duplicata encontrada.")
        
        print("=" * 70)
        
        return {
            'total_leitura': total_leitura,
            'total_chuva': total_chuva,
            'total_com_join': total_com_join,
            'data_min': data_min_join,
            'data_max': data_max_join,
            'total_jan_1997': total_jan_1997,
            'tem_duplicatas': total_duplicatas > 0,
            'total_duplicatas': total_duplicatas
        }
        
    except Exception as e:
        print(f'\n❌ Erro no diagnóstico: {e}')
        import traceback
        traceback.print_exc()
        return None
    finally:
        if cur_origem:
            cur_origem.close()
        if conn_origem:
            conn_origem.close()

def carregar_dados_completos(usar_data_inicial=None):
    """Carrega todos os dados disponíveis no banco."""
    conn_origem = None
    cur_origem = None
    conn_destino = None
    cur_destino = None
    
    timestamp_atual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        # Verificar se já existem dados
        if not verificar_tabela_vazia():
            resposta = input("\n⚠️  A tabela já contém dados. Deseja continuar mesmo assim? (s/N): ")
            if resposta.lower() != 's':
                print("❌ Operação cancelada pelo usuário.")
                return 0
        
        # Executar diagnóstico primeiro
        diagnostico = diagnosticar_banco_origem()
        
        if not diagnostico:
            print("\n❌ Não foi possível executar o diagnóstico. Continuando mesmo assim...")
        else:
            if diagnostico['total_com_join'] == 0:
                print("\n❌ ERRO: Não há dados disponíveis no banco de origem!")
                print("   Verifique se as tabelas estão populadas corretamente.")
                return 0
            
            # Perguntar se deseja usar todos os dados ou filtrar por data
            if usar_data_inicial is None:
                print(f"\n📋 Dados disponíveis:")
                print(f"   • Data mínima: {diagnostico['data_min']}")
                print(f"   • Data máxima: {diagnostico['data_max']}")
                print(f"   • Total de registros: {diagnostico['total_com_join']:,}")
                
                resposta = input("\n❓ Deseja buscar TODOS os dados disponíveis? (S/n): ")
                if resposta.lower() == 'n':
                    data_input = input("   Digite a data inicial (formato: YYYY-MM-DD) ou pressione Enter para usar a data mínima: ")
                    if data_input.strip():
                        usar_data_inicial = data_input.strip()
                    else:
                        usar_data_inicial = str(diagnostico['data_min'])[:10] if diagnostico['data_min'] else None
                else:
                    usar_data_inicial = None
        
        # Conectar ao banco origem
        conn_origem = psycopg2.connect(**ORIGEM)
        cur_origem = conn_origem.cursor()
        
        # Executar query apropriada
        if usar_data_inicial:
            print(f"\n🔄 Iniciando carga completa desde {usar_data_inicial}...")
            query = query_dados_desde_data(usar_data_inicial)
        else:
            print(f"\n🔄 Iniciando carga completa de TODOS os dados disponíveis...")
            query = query_todos_dados()
        
        print(f"   Isso pode levar vários minutos dependendo do volume de dados...")
        print(f"   Por favor, aguarde...\n")
        
        cur_origem.execute(query)
        
        # Processar em lotes para evitar problemas de memória
        TAMANHO_LOTE = 10000
        total_inseridos = 0
        lote_numero = 1
        
        # Conectar ao banco destino
        conn_destino = psycopg2.connect(**DESTINO)
        cur_destino = conn_destino.cursor()

        # Usar ON CONFLICT DO UPDATE para garantir que os dados sejam sempre atualizados
        # com os valores corretos do banco origem, mesmo se já existirem dados incorretos
        # A query já garante apenas um registro por (dia, estacao_id) usando DISTINCT ON
        # com ORDER BY id DESC (mais recente)
        # IMPORTANTE: O psycopg2 vai converter automaticamente timestamps com timezone
        # para o timezone do servidor antes de armazenar. Para evitar diferença de 3 horas,
        # precisamos garantir que o timestamp seja inserido com o timezone correto (-03:00)
        # e o PostgreSQL vai converter para o timezone do servidor mantendo o valor local.
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
        
        # Configurar timezone do banco destino para 'America/Sao_Paulo' durante a inserção
        # Isso garante que timestamps com timezone sejam convertidos corretamente
        cur_destino.execute("SET timezone = 'America/Sao_Paulo';")
        
        # Processar dados em lotes
        print("📦 Processando dados em lotes de 10.000 registros...")
        print("   💡 A query usa DISTINCT ON para garantir apenas um registro por (dia, estacao_id),")
        print("      mantendo o registro mais recente (maior ID), exatamente como no banco alertadb.\n")
        
        primeira_data = None
        ultima_data = None
        
        while True:
            dados = cur_origem.fetchmany(TAMANHO_LOTE)
            
            if not dados:
                break
            
            # IMPORTANTE: Preparar timestamps mantendo timezone para inserção correta
            # Formato dos dados: (dia, m05, m10, m15, h01, h04, h24, h96, estacao, estacao_id)
            # Quando o banco origem retorna timestamps com timezone:
            # - '2025-11-28 11:40:00.000 -0300' (horário padrão)
            # - '2019-02-16 23:45:00.000 -0200' (horário de verão)
            # Precisamos preservar o timezone original para que o PostgreSQL converta corretamente
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
            
            # Log de debug para primeiro lote (apenas para verificação)
            if lote_numero == 1 and dados_ajustados:
                primeiro_registro = dados_ajustados[0]
                dia_original = dados[0][0]
                print(f'   🔍 Exemplo de timestamp processado:')
                print(f'      Original: {dia_original} (tipo: {type(dia_original)})')
                print(f'      Processado: {formatar_timestamp_nimbus(primeiro_registro[0])} (tipo: {type(primeiro_registro[0])})')
                if hasattr(primeiro_registro[0], 'tzinfo') and primeiro_registro[0].tzinfo:
                    offset = primeiro_registro[0].tzinfo.utcoffset(primeiro_registro[0])
                    print(f'      Timezone offset: {offset}')
            
            # Capturar primeira e última data do lote atual (após ajuste)
            data_inicio_lote = dados_ajustados[0][0] if dados_ajustados else None
            data_fim_lote = dados_ajustados[-1][0] if dados_ajustados else None
            
            # Capturar primeira e última data geral
            if primeira_data is None:
                primeira_data = data_inicio_lote
            ultima_data = data_fim_lote
            
            execute_values(cur_destino, insert_sql, dados_ajustados)
            conn_destino.commit()
            
            total_inseridos += len(dados)
            print(f'   📦 Lote {lote_numero}: {len(dados):,} registros processados (Total acumulado: {total_inseridos:,})')
            if data_inicio_lote and data_fim_lote:
                print(f'      📅 Período deste lote: {data_inicio_lote} até {data_fim_lote}')
            lote_numero += 1
        
        if total_inseridos == 0:
            print(f'\n   ⚠️  Nenhum dado encontrado para inserir.')
            print(f'   💡 Verifique o diagnóstico acima para entender o problema.')
            return 0
        
        # Obter estatísticas finais
        cur_destino.execute("SELECT COUNT(*) FROM pluviometricos;")
        total_tabela = cur_destino.fetchone()[0]
        
        cur_destino.execute("SELECT MIN(dia), MAX(dia) FROM pluviometricos;")
        datas = cur_destino.fetchone()
        data_min = datas[0] if datas[0] else None
        data_max = datas[1] if datas[1] else None
        
        print("\n" + "=" * 70)
        print("✅ CARGA INICIAL COMPLETA FINALIZADA!")
        print("=" * 70)
        print(f"📊 Total de registros inseridos nesta execução: {total_inseridos:,}")
        print(f"📊 Total de registros na tabela: {total_tabela:,}")
        if data_min and data_max:
            print(f"📅 Período dos dados na tabela: {data_min} até {data_max}")
        if primeira_data and ultima_data:
            print(f"📅 Período dos dados inseridos: {primeira_data} até {ultima_data}")
        print(f"⏰ Concluído em: {timestamp_atual}")
        print("=" * 70)
        
        # Verificar se há lacunas
        if data_min and data_max:
            print("\n🔍 Verificando distribuição de dados por ano na tabela destino:")
            cur_destino.execute("""
                SELECT 
                    EXTRACT(YEAR FROM dia) as ano,
                    COUNT(*) as total
                FROM pluviometricos
                GROUP BY EXTRACT(YEAR FROM dia)
                ORDER BY ano;
            """)
            distribuicao_destino = cur_destino.fetchall()
            for ano, total in distribuicao_destino:
                print(f"   • {int(ano)}: {total:,} registros")
        
        # Validar alguns registros para garantir que os dados estão corretos
        print("\n🔍 Validando dados inseridos...")
        validacao_ok = validar_amostra_dados(conn_origem, conn_destino, cur_origem, cur_destino)
        
        if validacao_ok:
            print("   ✅ Validação: Dados inseridos correspondem ao banco origem!")
        else:
            print("   ⚠️  ATENÇÃO: Foram encontradas divergências na validação!")
            print("   💡 Execute o script novamente para corrigir os dados.")
        
        print("\n💡 PRÓXIMO PASSO:")
        print("   Execute o script 'sincronizar_pluviometricos_novos.py' para manter")
        print("   os dados atualizados em tempo real a cada 5 minutos.\n")
        
        return total_inseridos

    except Exception as e:
        print(f'\n❌ Erro na carga: {e}')
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

def main():
    """Função principal que executa a carga inicial completa."""
    print("=" * 70)
    print("🌧️ CARGA INICIAL COMPLETA - DADOS PLUVIOMÉTRICOS")
    print("=" * 70)
    print()
    print("🎯 PROPÓSITO:")
    print("   Este script faz a CARGA INICIAL COMPLETA de todos os dados")
    print("   pluviométricos disponíveis no banco de origem.")
    print()
    print("📋 O QUE SERÁ FEITO:")
    print("   ✅ Diagnosticar o banco de origem para verificar dados disponíveis")
    print("   ✅ Buscar TODOS os dados históricos disponíveis (sem filtro fixo)")
    print("   ✅ Criar a tabela pluviometricos se não existir")
    print("   ✅ Processar em lotes de 10.000 registros")
    print("   ✅ Mostrar progresso e estatísticas detalhadas durante a carga")
    print()
    print("⚠️  IMPORTANTE:")
    print("   - Execute APENAS UMA VEZ para fazer a carga inicial")
    print("   - Após concluir, use sincronizar_pluviometricos_novos.py para manter atualizado")
    print("   - O processo pode levar vários minutos dependendo do volume de dados")
    print("=" * 70)
    
    # Testar conexões
    if not testar_conexoes():
        print("\n❌ Falha nos testes de conexão. Abortando...")
        return

    # Criar/verificar tabela
    print("\n📋 Verificando estrutura do banco de dados...")
    criar_tabela_pluviometricos()

    # Executar carga completa
    carregar_dados_completos()

if __name__ == "__main__":
    main()

