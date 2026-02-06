#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🌤️ SINCRONIZAÇÃO INCREMENTAL - DADOS METEOROLÓGICOS NIMBUS → BigQuery

═══════════════════════════════════════════════════════════════════════════
🎯 PROPÓSITO DESTE SCRIPT:
═══════════════════════════════════════════════════════════════════════════

Este script sincroniza APENAS os dados NOVOS desde a última sincronização
do banco NIMBUS para o BigQuery, usando EXATAMENTE a mesma lógica e estrutura
do script exportar_meteorologicos_nimbus_bigquery.py.

ARQUITETURA:
    NIMBUS (alertadb) → Parquet → BigQuery (incremental)
              ↑ [ESTE SCRIPT - SINCRONIZAÇÃO INCREMENTAL]

QUERY UTILIZADA:
    ✅ GROUP BY (l."horaLeitura", l.estacao_id, e.nome)
    ✅ WHERE horaLeitura > ultima_sincronizacao (apenas dados novos)
    ✅ ORDER BY l."horaLeitura"
    ✅ MESMA query do exportar_meteorologicos_nimbus_bigquery.py com filtro WHERE

VANTAGENS:
    ✅ Sincronização incremental (apenas dados novos)
    ✅ Execução rápida (não processa todos os dados)
    ✅ Ideal para cron (executa a cada 5 minutos)
    ✅ BigQuery sempre atualizado
    ✅ Formato Parquet (otimizado)
    ✅ MESMA lógica do script de exportação (garante consistência)

═══════════════════════════════════════════════════════════════════════════
📋 O QUE ESTE SCRIPT FAZ:
═══════════════════════════════════════════════════════════════════════════

✅ Busca último timestamp no BigQuery (MAX(dia_utc)) por estação
✅ Busca APENAS dados novos desde esses timestamps no NIMBUS
✅ Exporta para formato Parquet (mesma estrutura do script de exportação)
✅ Carrega no BigQuery usando WRITE_APPEND
✅ Processa em lotes para otimizar memória
✅ Preserva tipos de dados e timezone corretamente (UTC)
✅ Usa EXATAMENTE a mesma lógica do script de exportação

═══════════════════════════════════════════════════════════════════════════
⚠️ QUANDO USAR ESTE SCRIPT:
═══════════════════════════════════════════════════════════════════════════

✅ APÓS executar exportar_meteorologicos_nimbus_bigquery.py (carga inicial)
✅ Para manter os dados atualizados automaticamente via cron
✅ Em produção/servidor para sincronização contínua
✅ Quando você precisa de dados atualizados a cada 5 minutos

⚠️ NÃO USE se:
   ❌ A tabela BigQuery estiver vazia (use exportar_meteorologicos_nimbus_bigquery.py primeiro)
   ❌ Você quer carregar dados históricos (use exportar_meteorologicos_nimbus_bigquery.py)

═══════════════════════════════════════════════════════════════════════════
🚀 COMO USAR:
═══════════════════════════════════════════════════════════════════════════

1. PRIMEIRO: Execute exportar_meteorologicos_nimbus_bigquery.py para carga inicial
2. Configure o arquivo .env com as credenciais
3. Execute: python sincronizar_meteorologicos_nimbus_bigquery.py --once
4. Configure cron para executar a cada 5 minutos

═══════════════════════════════════════════════════════════════════════════
"""

import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import sys
from datetime import datetime, timezone
from dotenv import load_dotenv
from pathlib import Path
import tempfile
import gc
import pytz

# Configurar encoding UTF-8 para Windows (resolve problema com emojis)
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

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
        # Banco ORIGEM - NIMBUS (alertadb)
        origem = {
            'host': obter_variavel('DB_ORIGEM_HOST'),
            'port': obter_variavel('DB_ORIGEM_PORT', obrigatoria=False, padrao='5432'),
            'dbname': obter_variavel('DB_ORIGEM_NAME'),
            'user': obter_variavel('DB_ORIGEM_USER'),
            'password': obter_variavel('DB_ORIGEM_PASSWORD'),
            'sslmode': obter_variavel('DB_ORIGEM_SSLMODE', obrigatoria=False, padrao='disable'),
            'connect_timeout': 10
        }

        # BigQuery
        credentials_padrao = project_root / 'credentials' / 'credentials.json'
        credentials_path_env = obter_variavel('BIGQUERY_CREDENTIALS_PATH', obrigatoria=False)
        if credentials_path_env:
            credentials_path = Path(credentials_path_env)
            if not credentials_path.exists():
                if credentials_padrao.exists():
                    credentials_path = credentials_padrao
                else:
                    credentials_path = None
        elif credentials_padrao.exists():
            credentials_path = credentials_padrao
        else:
            credentials_path = None
        
        bigquery_config = {
            'project_id': obter_variavel('BIGQUERY_PROJECT_ID'),
            'dataset_id': obter_variavel('BIGQUERY_DATASET_ID_NIMBUS', obrigatoria=False, padrao='alertadb_cor_raw'),
            'table_id': obter_variavel('BIGQUERY_TABLE_ID_METEOROLOGICOS', obrigatoria=False, padrao='meteorologicos'),
            'credentials_path': str(credentials_path) if credentials_path else None,
        }
        
        return origem, bigquery_config
    
    except ValueError as e:
        print("=" * 70)
        print("❌ ERRO DE CONFIGURAÇÃO")
        print("=" * 70)
        print(str(e))
        raise

ORIGEM, BIGQUERY_CONFIG = carregar_configuracoes()

def testar_conexao_nimbus():
    """Testa conexão com NIMBUS."""
    try:
        conn = psycopg2.connect(**ORIGEM)
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"   ❌ NIMBUS: FALHA! Erro: {e}")
        return False

def obter_ultima_sincronizacao_bigquery(client, dataset_id, table_id):
    """Obtém o último timestamp sincronizado do BigQuery (TIMESTAMP).
    
    IMPORTANTE: O campo dia_utc está salvo em UTC no BigQuery.
    O BigQuery retorna sem timezone, mas o valor corresponde a UTC.
    """
    try:
        query = f"""
        SELECT MAX(dia_utc) as ultima_sincronizacao
        FROM `{client.project}.{dataset_id}.{table_id}`
        """
        query_job = client.query(query)
        results = query_job.result()
        
        for row in results:
            if row.ultima_sincronizacao:
                ultima_sync = row.ultima_sincronizacao
                if isinstance(ultima_sync, datetime):
                    # O timestamp do BigQuery vem sem timezone, mas representa UTC
                    if ultima_sync.tzinfo is None:
                        # Assumir que está em UTC
                        return ultima_sync.replace(tzinfo=timezone.utc)
                    else:
                        # Se já tem timezone, converter para UTC
                        return ultima_sync.astimezone(timezone.utc)
                break
        
        return datetime(1997, 1, 1, tzinfo=timezone.utc)
    except Exception as e:
        print(f"⚠️  Erro ao obter última sincronização: {e}")
        return datetime(1997, 1, 1, tzinfo=timezone.utc)

def obter_ultima_sincronizacao_por_estacao(client, dataset_id, table_id):
    """Obtém o último timestamp sincronizado POR estacao_id do BigQuery.
    
    Retorna um dicionário {estacao_id: ultimo_timestamp_utc} para evitar duplicações.
    """
    try:
        query = f"""
        SELECT 
            estacao_id,
            MAX(dia_utc) as ultima_sincronizacao
        FROM `{client.project}.{dataset_id}.{table_id}`
        GROUP BY estacao_id
        """
        query_job = client.query(query)
        results = query_job.result()
        
        ultimas_por_estacao = {}
        for row in results:
            if row.ultima_sincronizacao and row.estacao_id is not None:
                ultima_sync = row.ultima_sincronizacao
                if isinstance(ultima_sync, datetime):
                    # Converter para UTC
                    if ultima_sync.tzinfo is None:
                        # Assumir que está em UTC
                        ultimas_por_estacao[row.estacao_id] = ultima_sync.replace(tzinfo=timezone.utc)
                    else:
                        ultimas_por_estacao[row.estacao_id] = ultima_sync.astimezone(timezone.utc)
        
        return ultimas_por_estacao
    except Exception as e:
        print(f"⚠️  Erro ao obter última sincronização por estação: {e}")
        return {}

def query_dados_incrementais(ultima_sincronizacao, ultimas_por_estacao=None):
    """Retorna query para buscar apenas dados novos desde a última sincronização.
    
    MESMA query do exportar_meteorologicos_nimbus_bigquery.py, mas com WHERE.
    
    IMPORTANTE: ultima_sincronizacao deve estar em UTC (vem de obter_ultima_sincronizacao_bigquery).
    
    Se ultimas_por_estacao for fornecido, usa filtro por estação para evitar duplicações.
    """
    # Converter timestamp UTC para timestamptz do PostgreSQL
    if isinstance(ultima_sincronizacao, datetime):
        # Garantir que está em UTC
        if ultima_sincronizacao.tzinfo is None:
            ultima_sincronizacao = ultima_sincronizacao.replace(tzinfo=timezone.utc)
        elif ultima_sincronizacao.tzinfo != timezone.utc:
            # Converter para UTC se não estiver
            ultima_sincronizacao = ultima_sincronizacao.astimezone(timezone.utc)
        
        # Formatar para PostgreSQL: usar UTC diretamente no formato ISO 8601
        # Formato: 'YYYY-MM-DD HH:MM:SS+00:00'
        timestamp_str = ultima_sincronizacao.strftime('%Y-%m-%d %H:%M:%S+00:00')
    else:
        timestamp_str = str(ultima_sincronizacao)
        if ':' not in timestamp_str or ('+' not in timestamp_str and '-' not in timestamp_str.split()[-1]):
            timestamp_str += "+00:00"
    
    # Se temos últimas sincronizações por estação, usar filtro mais preciso
    if ultimas_por_estacao and len(ultimas_por_estacao) > 0:
        # Construir condições por estação
        condicoes_estacao = []
        for estacao_id, ultima_estacao in ultimas_por_estacao.items():
            if isinstance(ultima_estacao, datetime):
                if ultima_estacao.tzinfo is None:
                    ultima_estacao = ultima_estacao.replace(tzinfo=timezone.utc)
                elif ultima_estacao.tzinfo != timezone.utc:
                    ultima_estacao = ultima_estacao.astimezone(timezone.utc)
                timestamp_estacao = ultima_estacao.strftime('%Y-%m-%d %H:%M:%S+00:00')
                condicoes_estacao.append(f"(l.estacao_id = {estacao_id} AND l.\"horaLeitura\" > '{timestamp_estacao}'::timestamptz)")
        
        # Adicionar estações que não estão no BigQuery ainda (se houver estações no BigQuery)
        if len(ultimas_por_estacao) > 0:
            estacao_ids_str = ','.join(map(str, ultimas_por_estacao.keys()))
            condicoes_estacao.append(f"l.estacao_id NOT IN ({estacao_ids_str})")
        
        where_clause = "(" + " OR ".join(condicoes_estacao) + ")"
    else:
        # Usar filtro geral (fallback)
        where_clause = f"l.\"horaLeitura\" > '{timestamp_str}'::timestamptz"
    
    # MESMA query do script de exportação (GROUP BY), mas com WHERE mais preciso
    return f"""
SELECT
    l."horaLeitura" AS data_hora,  -- simples e já com timezone correto
    l.estacao_id AS id_estacao,
    e.nome       AS nome_estacao,
    MAX(CASE WHEN s.nome = 'Chuva'                 THEN ls.valor END) AS chuva,
    MAX(CASE WHEN s.nome = 'Direção Vento'         THEN ls.valor END) AS dirVento,
    MAX(CASE WHEN s.nome = 'Velocidade Vento'      THEN ls.valor END) AS velVento,
    MAX(CASE WHEN s.nome = 'Temperatura do Ar'     THEN ls.valor END) AS temperatura,
    MAX(CASE WHEN s.nome = 'Pressão Atmosférica'   THEN ls.valor END) AS pressao,
    MAX(CASE WHEN s.nome = 'Umidade do Ar'         THEN ls.valor END) AS umidade

FROM public.estacoes_leiturasensor ls
JOIN public.estacoes_leitura l
      ON ls.leitura_id = l.id
JOIN public.estacoes_sensor s
      ON ls.sensor_id = s.id
JOIN public.estacoes_estacao e
      ON e.id = l.estacao_id

WHERE {where_clause}

GROUP BY
    l."horaLeitura",
    l.estacao_id,
    e.nome

ORDER BY l."horaLeitura";
"""

def obter_schema_meteorologicos():
    """Retorna schema do BigQuery para tabela meteorologicos."""
    return [
        bigquery.SchemaField("dia_utc", "TIMESTAMP", mode="REQUIRED", description="Data e hora da medição em UTC. Origem: TIMESTAMPTZ NOT NULL do NIMBUS convertido para UTC. O sufixo _utc deixa explícita a origem do fuso horário."),
        bigquery.SchemaField("dia_original", "STRING", mode="NULLABLE", description="Data e hora no formato exato do banco original da NIMBUS (ex: 2009-02-16 02:12:20.000 -0300)"),
        bigquery.SchemaField("utc_offset", "STRING", mode="NULLABLE", description="Offset UTC do timezone original (ex: -0300 para horário padrão do Brasil, -0200 para horário de verão)"),
        bigquery.SchemaField("estacao", "STRING", mode="NULLABLE", description="Nome da estação meteorológica"),
        bigquery.SchemaField("estacao_id", "INTEGER", mode="REQUIRED", description="ID da estação meteorológica"),
        bigquery.SchemaField("chuva", "FLOAT64", mode="NULLABLE", description="Chuva (mm)"),
        bigquery.SchemaField("dirVento", "FLOAT64", mode="NULLABLE", description="Direção do vento (graus)"),
        bigquery.SchemaField("velVento", "FLOAT64", mode="NULLABLE", description="Velocidade do vento (m/s ou km/h)"),
        bigquery.SchemaField("temperatura", "FLOAT64", mode="NULLABLE", description="Temperatura do ar (°C)"),
        bigquery.SchemaField("pressao", "FLOAT64", mode="NULLABLE", description="Pressão atmosférica (hPa)"),
        bigquery.SchemaField("umidade", "FLOAT64", mode="NULLABLE", description="Umidade do ar (%)"),
    ]

def sincronizar_incremental():
    """Sincroniza apenas dados novos do NIMBUS para BigQuery.
    
    Usa EXATAMENTE a mesma lógica do script de exportação.
    """
    engine_nimbus = None
    client_bq = None
    
    try:
        print("\n🔄 Iniciando sincronização incremental NIMBUS → BigQuery (DADOS METEOROLÓGICOS)...")
        print(f"   Origem: alertadb @ NIMBUS")
        print(f"   Destino: BigQuery ({BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_id']}.{BIGQUERY_CONFIG['table_id']})")
        print()
        
        # Conectar ao NIMBUS - MESMA lógica do script de exportação
        print("📦 Conectando ao NIMBUS...")
        connection_string = (
            f"postgresql://{ORIGEM['user']}:{ORIGEM['password']}@"
            f"{ORIGEM['host']}:{ORIGEM['port']}/{ORIGEM['dbname']}"
        )
        engine_nimbus = create_engine(
            connection_string,
            connect_args={'client_encoding': 'UTF8'},
            pool_pre_ping=True
        )
        
        # Conectar ao BigQuery - MESMA lógica do script de exportação
        print("📦 Conectando ao BigQuery...")
        credentials_path = BIGQUERY_CONFIG.get('credentials_path')
        
        if not credentials_path or not Path(credentials_path).exists():
            credentials_padrao = project_root / 'credentials' / 'credentials.json'
            if credentials_padrao.exists():
                credentials_path = credentials_padrao
        
        if credentials_path and Path(credentials_path).exists():
            credentials = service_account.Credentials.from_service_account_file(str(credentials_path))
            client_bq = bigquery.Client(
                project=BIGQUERY_CONFIG['project_id'],
                credentials=credentials
            )
        else:
            raise FileNotFoundError(
                f"❌ Arquivo de credenciais não encontrado!\n"
                f"   Caminho padrão: {project_root / 'credentials' / 'credentials.json'}\n"
                f"   💡 Coloque o arquivo credentials.json em: {project_root / 'credentials' / 'credentials.json'}"
            )
        
        dataset_id = BIGQUERY_CONFIG['dataset_id']
        table_id = BIGQUERY_CONFIG['table_id']
        table_ref = client_bq.dataset(dataset_id).table(table_id)
        
        # Verificar particionamento ANTES de processar - CRÍTICO!
        print("\n🔍 Verificando particionamento da tabela...")
        try:
            table = client_bq.get_table(table_ref)
            if table.time_partitioning:
                if table.time_partitioning.type_ == bigquery.TimePartitioningType.DAY:
                    print(f"\n❌ ERRO CRÍTICO: Tabela está particionada por DIA!")
                    print(f"   ⚠️  Isso excede o limite de 10.000 partições do BigQuery!")
                    print(f"   ⚠️  A sincronização irá FALHAR se tentar carregar dados!")
                    print(f"\n💡 SOLUÇÃO OBRIGATÓRIA:")
                    print(f"   1. Execute o script de exportação para recriar a tabela com particionamento por MÊS:")
                    print(f"      python scripts/bigquery/exportar_meteorologicos_nimbus_bigquery.py")
                    print(f"   2. Isso vai recriar a tabela com particionamento por MÊS e recarregar todos os dados")
                    print(f"   3. Depois execute a sincronização novamente")
                    print(f"\n⚠️  Sincronização CANCELADA para evitar erro!")
                    return False
                elif table.time_partitioning.type_ == bigquery.TimePartitioningType.MONTH:
                    print(f"   ✅ Tabela está particionada por MÊS (correto)")
                else:
                    print(f"   ℹ️  Tabela tem particionamento tipo: {table.time_partitioning.type_}")
            else:
                print(f"   ℹ️  Tabela não tem particionamento")
        except Exception as e:
            print(f"   ⚠️  Erro ao verificar particionamento: {e}")
            print(f"   ⚠️  Continuando, mas pode haver problemas...")
        
        # Obter última sincronização por estação (evita duplicações)
        print("\n🔍 Obtendo última sincronização do BigQuery por estação...")
        ultimas_por_estacao = obter_ultima_sincronizacao_por_estacao(client_bq, dataset_id, table_id)
        
        if not ultimas_por_estacao:
            # Fallback: usar método antigo se não conseguir por estação
            print("   ⚠️  Não foi possível obter por estação, usando método geral...")
            ultima_sincronizacao = obter_ultima_sincronizacao_bigquery(client_bq, dataset_id, table_id)
            
            if ultima_sincronizacao == datetime(1997, 1, 1, tzinfo=timezone.utc):
                print("⚠️  Tabela BigQuery está vazia ou não encontrada!")
                print("   Execute PRIMEIRO: python scripts/bigquery/exportar_meteorologicos_nimbus_bigquery.py")
                print("   para fazer a carga inicial dos dados históricos.")
                return False
            
            print(f"✅ Última sincronização geral: {ultima_sincronizacao} (UTC)")
            ultima_sincronizacao_geral = ultima_sincronizacao
        else:
            # Calcular timestamp geral mínimo para exibição
            timestamps = list(ultimas_por_estacao.values())
            ultima_sincronizacao_geral = min(timestamps) if timestamps else datetime(1997, 1, 1, tzinfo=timezone.utc)
            print(f"✅ Última sincronização por estação: {len(ultimas_por_estacao)} estações")
            print(f"   Timestamp mínimo: {ultima_sincronizacao_geral} (UTC)")
        
        # Buscar dados incrementais usando filtro por estação
        data_atual = datetime.now(timezone.utc)
        diferenca_dias = (data_atual - ultima_sincronizacao_geral).days
        
        print(f"\n🔍 Buscando dados novos desde {ultima_sincronizacao_geral.strftime('%Y-%m-%d %H:%M:%S')} (UTC)...")
        print(f"   📅 Data atual: {data_atual.strftime('%Y-%m-%d %H:%M:%S')} (UTC)")
        print(f"   ⏱️  Período: {diferenca_dias} dias desde a última sincronização")
        print(f"   ✅ Coletando TODOS os dados desde {ultima_sincronizacao_geral.strftime('%Y-%m-%d')} até hoje")
        if diferenca_dias > 365:
            anos = diferenca_dias / 365.25
            print(f"   ⚠️  ATENÇÃO: Coletando {anos:.1f} anos de dados - isso pode demorar bastante!")
        
        query = query_dados_incrementais(ultima_sincronizacao_geral, ultimas_por_estacao)
        
        # Processar e carregar - EXATAMENTE a mesma lógica do script de exportação
        schema = obter_schema_meteorologicos()
        
        inicio_query = datetime.now()
        chunksize = 5000
        total_registros = 0
        chunk_numero = 1
        
        print(f"\n📦 Processando e carregando dados incrementais no BigQuery...")
        print(f"   💡 Usando formato Parquet para melhor performance")
        print(f"   💡 Query usa GROUP BY para agregar dados dos sensores")
        print(f"   💡 Removendo duplicatas por (dia_utc, estacao_id) para garantir unicidade\n")
        
        temp_dir = tempfile.mkdtemp()
        parquet_files = []
        
        chunks_list = []
        batch_size = 2
        batch_file_num = 1
        
        for chunk_df in pd.read_sql(query, engine_nimbus, chunksize=chunksize):
            print(f"   📦 Processando chunk {chunk_numero} ({len(chunk_df):,} registros)...")
            
            # Renomear colunas
            chunk_df = chunk_df.rename(columns={
                'data_hora': 'dia_utc',
                'id_estacao': 'estacao_id',
                'nome_estacao': 'estacao'
            })
            
            # Remover colunas auxiliares
            colunas_para_remover = ['TimezoneOffset', 'leitura_id']
            for col in colunas_para_remover:
                if col in chunk_df.columns:
                    chunk_df = chunk_df.drop(columns=[col])
            
            # Processar coluna dia_utc: TIMESTAMPTZ do NIMBUS → TIMESTAMP (UTC) do BigQuery
            def processar_dia_timestamp(dt):
                """Processa TIMESTAMPTZ do PostgreSQL (NIMBUS) para TIMESTAMP do BigQuery (UTC)."""
                if pd.isna(dt):
                    return None
                try:
                    if isinstance(dt, str):
                        dt_parsed = pd.to_datetime(dt)
                    elif isinstance(dt, pd.Timestamp):
                        dt_parsed = dt
                    elif hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
                        dt_parsed = pd.Timestamp(dt)
                    else:
                        dt_parsed = pd.to_datetime(dt)
                    
                    if isinstance(dt_parsed, pd.Timestamp) and dt_parsed.tz is not None:
                        dt_utc = dt_parsed.tz_convert('UTC')
                        return dt_utc.tz_localize(None)
                    elif isinstance(dt_parsed, pd.Timestamp):
                        from datetime import timezone, timedelta
                        tz_brasil = timezone(timedelta(hours=-3))
                        dt_com_tz = dt_parsed.tz_localize(tz_brasil)
                        dt_utc = dt_com_tz.tz_convert('UTC')
                        return dt_utc.tz_localize(None)
                    else:
                        return dt_parsed
                except Exception as e:
                    print(f"      ⚠️  Erro ao processar timestamp: {e}")
                    return None
            
            def formatar_dia_original(dt):
                """Formata datetime no formato exato da NIMBUS: 2009-02-16 02:12:20.000 -0300"""
                if pd.isna(dt):
                    return None
                try:
                    if isinstance(dt, str):
                        if len(dt) > 10 and (dt[-5:].startswith('-') or dt[-5:].startswith('+')):
                            return dt
                        dt_parsed = pd.to_datetime(dt)
                    elif isinstance(dt, pd.Timestamp):
                        dt_parsed = dt
                    else:
                        dt_parsed = pd.to_datetime(dt)
                    
                    offset_str = "-0300"
                    if isinstance(dt_parsed, pd.Timestamp):
                        if dt_parsed.tz is not None:
                            offset = dt_parsed.tz.utcoffset(dt_parsed)
                            if offset:
                                total_seconds = offset.total_seconds()
                                hours = int(total_seconds // 3600)
                                minutes = int((abs(total_seconds) % 3600) // 60)
                                offset_str = f"{hours:+03d}{minutes:02d}"
                    
                    timestamp_str = dt_parsed.strftime('%Y-%m-%d %H:%M:%S')
                    if isinstance(dt_parsed, pd.Timestamp) and dt_parsed.microsecond:
                        microsec_str = str(dt_parsed.microsecond)[:3].zfill(3)
                        timestamp_str += f".{microsec_str}"
                    else:
                        timestamp_str += ".000"
                    
                    return f"{timestamp_str} {offset_str}"
                except Exception:
                    return None
            
            def extrair_utc_offset(dt):
                """Extrai o offset UTC do timestamp original (ex: -0300, -0200)."""
                if pd.isna(dt):
                    return None
                try:
                    if isinstance(dt, str):
                        if len(dt) > 10 and (dt[-5:].startswith('-') or dt[-5:].startswith('+')):
                            return dt[-5:]
                        dt_parsed = pd.to_datetime(dt)
                    elif isinstance(dt, pd.Timestamp):
                        dt_parsed = dt
                    else:
                        dt_parsed = pd.to_datetime(dt)
                    
                    offset_str = "-0300"
                    if isinstance(dt_parsed, pd.Timestamp):
                        if dt_parsed.tz is not None:
                            offset = dt_parsed.tz.utcoffset(dt_parsed)
                            if offset:
                                total_seconds = offset.total_seconds()
                                hours = int(total_seconds // 3600)
                                minutes = int((abs(total_seconds) % 3600) // 60)
                                offset_str = f"{hours:+03d}{minutes:02d}"
                    
                    return offset_str
                except Exception:
                    return None
            
            # Processar todas as colunas: dia_utc (TIMESTAMP), dia_original (STRING) e utc_offset (STRING)
            chunk_df['dia_original'] = chunk_df['dia_utc'].apply(formatar_dia_original)
            chunk_df['utc_offset'] = chunk_df['dia_utc'].apply(extrair_utc_offset)
            chunk_df['dia_utc'] = chunk_df['dia_utc'].apply(processar_dia_timestamp)
            
            if 'estacao_id' in chunk_df.columns:
                chunk_df['estacao_id'] = chunk_df['estacao_id'].astype('Int64')
            
            # Converter colunas numéricas - manter tipo original do banco (não forçar float64)
            # O banco pode retornar INTEGER ou NUMERIC, manter como vem
            colunas_numericas = ['chuva', 'dirVento', 'velVento', 'temperatura', 'pressao', 'umidade']
            for col in colunas_numericas:
                if col in chunk_df.columns:
                    # Converter para numérico mas manter tipo original (int se for int, float se for float)
                    chunk_df[col] = pd.to_numeric(chunk_df[col], errors='coerce')
                    # Não forçar float64 - manter tipo original (pode ser int64 ou float64)
            
            # Filtrar registros com dia_utc NULL
            registros_antes = len(chunk_df)
            chunk_df = chunk_df[chunk_df['dia_utc'].notna()]
            registros_depois = len(chunk_df)
            if registros_antes != registros_depois:
                print(f"      ⚠️  Removidos {registros_antes - registros_depois} registros com dia_utc NULL")
            
            # IMPORTANTE: Remover duplicatas baseado em (dia_utc, estacao_id)
            # A conversão de timezone pode criar duplicatas mesmo com GROUP BY na query
            registros_antes_dedup = len(chunk_df)
            chunk_df = chunk_df.drop_duplicates(subset=['dia_utc', 'estacao_id'], keep='last')
            registros_depois_dedup = len(chunk_df)
            if registros_antes_dedup != registros_depois_dedup:
                print(f"      ⚠️  Removidas {registros_antes_dedup - registros_depois_dedup} duplicatas (dia_utc, estacao_id)")
            
            if len(chunk_df) > 0:
                chunks_list.append(chunk_df)
            total_registros += len(chunk_df)
            chunk_numero += 1
            
            # Escrever batch em arquivo Parquet
            if len(chunks_list) >= batch_size:
                df_batch = pd.concat(chunks_list, ignore_index=True)
                # Remover duplicatas antes de salvar (pode haver duplicatas entre chunks)
                registros_antes_batch = len(df_batch)
                df_batch = df_batch.drop_duplicates(subset=['dia_utc', 'estacao_id'], keep='last')
                registros_depois_batch = len(df_batch)
                if registros_antes_batch != registros_depois_batch:
                    print(f"      ⚠️  Removidas {registros_antes_batch - registros_depois_batch} duplicatas no batch {batch_file_num}")
                batch_file = Path(temp_dir) / f'{table_id}_batch_{batch_file_num:04d}.parquet'
                df_batch.to_parquet(
                    batch_file, 
                    index=False, 
                    engine='pyarrow', 
                    compression='snappy',
                    coerce_timestamps='us'
                )
                parquet_files.append(batch_file)
                print(f"      💾 Batch {batch_file_num} salvo: {batch_file.stat().st_size / (1024*1024):.2f} MB")
                chunks_list.clear()
                del df_batch
                gc.collect()
                batch_file_num += 1
        
        # Escrever chunks restantes
        if chunks_list:
            df_batch = pd.concat(chunks_list, ignore_index=True)
            df_batch = df_batch[df_batch['dia_utc'].notna()]
            # Remover duplicatas antes de salvar
            registros_antes_final = len(df_batch)
            df_batch = df_batch.drop_duplicates(subset=['dia_utc', 'estacao_id'], keep='last')
            registros_depois_final = len(df_batch)
            if registros_antes_final != registros_depois_final:
                print(f"      ⚠️  Removidas {registros_antes_final - registros_depois_final} duplicatas no batch final")
            if len(df_batch) > 0:
                batch_file = Path(temp_dir) / f'{table_id}_batch_{batch_file_num:04d}.parquet'
                df_batch.to_parquet(
                    batch_file, 
                    index=False, 
                    engine='pyarrow', 
                    compression='snappy',
                    coerce_timestamps='us'
                )
                parquet_files.append(batch_file)
                print(f"      💾 Batch {batch_file_num} salvo: {batch_file.stat().st_size / (1024*1024):.2f} MB")
                del df_batch
                gc.collect()
        
        if total_registros == 0:
            print("\n✅ Nenhum dado novo para sincronizar.")
            return True
        
        total_size = sum(f.stat().st_size for f in parquet_files) / (1024*1024)
        print(f"\n   ✅ {len(parquet_files)} arquivos Parquet criados: {total_size:.2f} MB total")
        
        tempo_query = (datetime.now() - inicio_query).total_seconds()
        print(f"   ✅ Dados processados: {total_registros:,} registros em {tempo_query:.1f} segundos")
        
        # Carregar arquivos Parquet no BigQuery
        print(f"\n📤 Carregando {len(parquet_files)} arquivos Parquet no BigQuery...")
        print(f"   Tabela: {client_bq.project}.{dataset_id}.{table_id}")
        
        inicio_carga = datetime.now()
        
        for i, parquet_file in enumerate(parquet_files, 1):
            print(f"   📤 Carregando arquivo {i}/{len(parquet_files)}: {parquet_file.name}...")
            file_job_config = bigquery.LoadJobConfig(
                # Não passar schema - BigQuery infere do Parquet e permite atualização automática
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Sempre APPEND na sincronização
                source_format=bigquery.SourceFormat.PARQUET,
                schema_update_options=[
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION  # Permite INTEGER -> FLOAT64
                ],
            )
            
            with open(parquet_file, 'rb') as source_file:
                job = client_bq.load_table_from_file(
                    source_file,
                    table_ref,
                    job_config=file_job_config
                )
                job.result()
                print(f"      ✅ Arquivo {i}/{len(parquet_files)} carregado com sucesso")
        
        tempo_carga = (datetime.now() - inicio_carga).total_seconds()
        
        # Limpar arquivos temporários
        print(f"   🧹 Limpando arquivos temporários...")
        for parquet_file in parquet_files:
            try:
                parquet_file.unlink()
            except Exception:
                pass
        try:
            os.rmdir(temp_dir)
        except Exception:
            pass
        
        print(f"\n   ✅ Tabela '{table_id}' atualizada: {total_registros:,} registros adicionados em {tempo_carga:.1f} segundos")
        
        # Obter novo último timestamp
        nova_ultima_sync = obter_ultima_sincronizacao_bigquery(client_bq, dataset_id, table_id)
        print(f"   🕐 Última sincronização atualizada: {nova_ultima_sync} (UTC)")
        
        return True
        
    except Exception as e:
        print(f'\n❌ Erro na sincronização: {e}')
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        if engine_nimbus:
            engine_nimbus.dispose()

def main():
    """Função principal."""
    try:
        if '--once' in sys.argv:
            sucesso = sincronizar_incremental()
            sys.exit(0 if sucesso else 1)
        else:
            print("=" * 80)
            print("🌤️ SINCRONIZAÇÃO INCREMENTAL - DADOS METEOROLÓGICOS NIMBUS → BigQuery")
            print("=" * 80)
            print("\n⚠️  Para usar com cron, execute com --once:")
            print("   python scripts/bigquery/sincronizar_meteorologicos_nimbus_bigquery.py --once")
            print("\n🔄 Executando sincronização única...\n")
            sucesso = sincronizar_incremental()
            if sucesso:
                print("\n✅ Sincronização concluída com sucesso!")
            else:
                print("\n❌ Sincronização falhou!")
            sys.exit(0 if sucesso else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrompido pelo usuário.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Erro fatal: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

