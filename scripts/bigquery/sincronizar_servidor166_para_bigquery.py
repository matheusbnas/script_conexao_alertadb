#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🌧️ SINCRONIZAÇÃO INCREMENTAL - Servidor 166 → BigQuery

═══════════════════════════════════════════════════════════════════════════
🎯 PROPÓSITO DESTE SCRIPT:
═══════════════════════════════════════════════════════════════════════════

Este script sincroniza APENAS os dados NOVOS desde a última sincronização
do banco alertadb_cor (servidor 166) para o BigQuery, executando de forma incremental.

ARQUITETURA:
    NIMBUS → Servidor 166 (alertadb_cor) → Parquet → BigQuery (incremental)
              ↑ [ESTE SCRIPT - SINCRONIZAÇÃO INCREMENTAL]
              Dataset: alertadb_cor166_raw (identifica origem: NIMBUS → servidor166 → BigQuery)

VANTAGENS:
    ✅ Sincronização incremental (apenas dados novos)
    ✅ Execução rápida (não processa todos os dados)
    ✅ Ideal para cron (executa a cada 5 minutos)
    ✅ BigQuery sempre atualizado
    ✅ Formato Parquet (otimizado)
    ✅ Controle total dos dados (você é admin do banco)

═══════════════════════════════════════════════════════════════════════════
📋 O QUE ESTE SCRIPT FAZ:
═══════════════════════════════════════════════════════════════════════════

✅ Cria dataset alertadb_cor166_raw se não existir
✅ Cria tabela pluviometricos com TIMESTAMP (igual ao servidor166)
✅ Busca último timestamp no BigQuery (MAX(dia))
✅ Busca APENAS dados novos desde esse timestamp no servidor 166
✅ Exporta para formato Parquet
✅ Carrega no BigQuery usando WRITE_APPEND
✅ Processa em lotes para otimizar memória
✅ Usa TIMESTAMP para coluna dia (igual ao servidor166)
✅ Converte timezone para UTC (padrão BigQuery)

═══════════════════════════════════════════════════════════════════════════
⚠️ QUANDO USAR ESTE SCRIPT:
═══════════════════════════════════════════════════════════════════════════

✅ APÓS executar exportar_servidor166_para_bigquery.py (carga inicial)
✅ Para manter os dados atualizados automaticamente via cron
✅ Em produção/servidor para sincronização contínua
✅ Quando você precisa de dados atualizados a cada 5 minutos

⚠️ NÃO USE se:
   ❌ A tabela BigQuery estiver vazia (use exportar_servidor166_para_bigquery.py primeiro)
   ❌ Você quer carregar dados históricos (use exportar_servidor166_para_bigquery.py)

═══════════════════════════════════════════════════════════════════════════
🚀 COMO USAR:
═══════════════════════════════════════════════════════════════════════════

1. PRIMEIRO: Execute exportar_servidor166_para_bigquery.py para carga inicial
2. Configure o arquivo .env com as credenciais
3. Execute: python sincronizar_servidor166_para_bigquery.py --once
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
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from pathlib import Path
import tempfile
import gc
from urllib.parse import quote_plus

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
        # Banco ORIGEM - Servidor 166 (alertadb_cor)
        origem = {
            'host': obter_variavel('DB_DESTINO_HOST'),
            'port': obter_variavel('DB_DESTINO_PORT', obrigatoria=False, padrao='5432'),
            'dbname': obter_variavel('DB_DESTINO_NAME'),
            'user': obter_variavel('DB_DESTINO_USER'),
            'password': obter_variavel('DB_DESTINO_PASSWORD'),
            'sslmode': obter_variavel('DB_DESTINO_SSLMODE', obrigatoria=False, padrao='disable'),
            'connect_timeout': 10
        }

        # BigQuery
        # Dataset específico para dados do servidor166 (NIMBUS → servidor166 → BigQuery)
        bigquery_config = {
            'project_id': obter_variavel('BIGQUERY_PROJECT_ID'),
            'dataset_id': obter_variavel('BIGQUERY_DATASET_ID_SERVIDOR166', obrigatoria=False, padrao='alertadb_166_raw'),
            'table_id': obter_variavel('BIGQUERY_TABLE_ID', obrigatoria=False, padrao='pluviometricos'),
            'credentials_path': obter_variavel('BIGQUERY_CREDENTIALS_PATH', obrigatoria=False),
        }

        return origem, bigquery_config
    except Exception as e:
        print(f"❌ Erro ao carregar configurações: {e}")
        raise

def obter_credenciais_bigquery(credentials_path=None):
    """Obtém credenciais do BigQuery."""
    if credentials_path and os.path.exists(credentials_path):
        return service_account.Credentials.from_service_account_file(credentials_path)
    
    # Tentar encontrar credentials.json na pasta credentials/
    credentials_file = project_root / 'credentials' / 'credentials.json'
    if credentials_file.exists():
        return service_account.Credentials.from_service_account_file(str(credentials_file))
    
    # Tentar usar credenciais padrão do ambiente
    try:
        return None  # BigQuery usará credenciais padrão do ambiente
    except Exception:
        raise ValueError("❌ Credenciais do BigQuery não encontradas. Configure BIGQUERY_CREDENTIALS_PATH ou coloque credentials.json em credentials/")

def testar_conexao_servidor166(origem):
    """Testa conexão com banco servidor 166."""
    try:
        # Codificar usuário e senha para URL (trata caracteres especiais)
        user_encoded = quote_plus(origem['user'])
        password_encoded = quote_plus(origem['password'])
        
        engine = create_engine(
            f"postgresql://{user_encoded}:{password_encoded}@{origem['host']}:{origem['port']}/{origem['dbname']}",
            connect_args={'sslmode': origem['sslmode'], 'connect_timeout': origem['connect_timeout']}
        )
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        return True
    except Exception as e:
        print(f"❌ Erro ao conectar ao servidor 166: {e}")
        return False

def criar_dataset_se_nao_existir(client, dataset_id):
    """Cria dataset no BigQuery se não existir."""
    try:
        dataset_ref = client.dataset(dataset_id)
        try:
            client.get_dataset(dataset_ref)
            print(f"✅ Dataset '{dataset_id}' já existe.")
        except Exception:
            # Dataset não existe, criar
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"  # Ou outra região conforme necessário
            dataset.description = f"Dataset para dados do servidor166 (NIMBUS → servidor166 → BigQuery)"
            dataset = client.create_dataset(dataset, exists_ok=False)
            print(f"✅ Dataset '{dataset_id}' criado com sucesso!")
        return True
    except Exception as e:
        print(f"⚠️  Erro ao criar dataset: {e}")
        return False

def criar_tabela_com_schema(client, dataset_id, table_id, schema):
    """Cria tabela no BigQuery com schema e particionamento se não existir."""
    try:
        table_ref = client.dataset(dataset_id).table(table_id)
        
        try:
            table = client.get_table(table_ref)
            print(f"✅ Tabela '{table_id}' já existe com schema ({len(table.schema)} campos).")
            return True
        except Exception:
            # Tabela não existe, criar
            pass
        
        # Criar tabela com schema e particionamento
        table = bigquery.Table(table_ref, schema=schema)
        table.description = "Dados pluviométricos do servidor166 (NIMBUS → servidor166 → BigQuery)"
        # Como dia é TIMESTAMP, usar particionamento por coluna
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="dia"  # Particionamento por coluna dia (TIMESTAMP)
        )
        table = client.create_table(table, exists_ok=False)
        print(f"✅ Tabela '{table_id}' criada com schema e particionamento por coluna 'dia'!")
        return True
    except Exception as e:
        print(f"⚠️  Erro ao criar tabela: {e}")
        import traceback
        traceback.print_exc()
        return False

def obter_ultima_sincronizacao_bigquery(client, dataset_id, table_id, formatado=False):
    """Obtém o último timestamp sincronizado do BigQuery (TIMESTAMP).
    
    Args:
        formatado: Se True, retorna string formatada no formato NIMBUS. Se False, retorna datetime.
    """
    try:
        if formatado:
            # Retornar já formatado no formato NIMBUS: 2025-12-12 16:35:00.000 -0300
            # Converter UTC para America/Sao_Paulo e formatar
            query = f"""
            SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S.%E3 %z', 
                DATETIME(MAX(dia), 'America/Sao_Paulo')) as ultima_sincronizacao_formatada
            FROM `{client.project}.{dataset_id}.{table_id}`
            """
            query_job = client.query(query)
            results = query_job.result()
            for row in results:
                if row.ultima_sincronizacao_formatada:
                    # Formato vem como '2025-12-12 16:35:00.000 -0300' mas pode ter dois pontos no timezone
                    # Remover dois pontos do timezone se houver
                    formatted = row.ultima_sincronizacao_formatada
                    # Se tem formato -03:00, converter para -0300
                    import re
                    formatted = re.sub(r'([+-]\d{2}):(\d{2})$', r'\1\2', formatted)
                    return formatted
                break
            return None
        else:
            # Retornar datetime (comportamento original)
            query = f"""
            SELECT MAX(dia) as ultima_sincronizacao
            FROM `{client.project}.{dataset_id}.{table_id}`
            """
            query_job = client.query(query)
            results = query_job.result()
            
            for row in results:
                if row.ultima_sincronizacao:
                    ultima_sync = row.ultima_sincronizacao
                    if isinstance(ultima_sync, datetime):
                        if ultima_sync.tzinfo is None:
                            ultima_sync = ultima_sync.replace(tzinfo=timezone.utc)
                        return ultima_sync
                break
            
            return datetime(1997, 1, 1, tzinfo=timezone.utc)
    except Exception as e:
        print(f"⚠️  Erro ao obter última sincronização: {e}")
        if formatado:
            return None
        return datetime(1997, 1, 1, tzinfo=timezone.utc)

def processar_dia_timestamp(dt):
    """Processa datetime para TIMESTAMP do BigQuery (UTC).
    
    A coluna dia no servidor 166 é TIMESTAMPTZ NOT NULL, então preserva o timezone original.
    Converte para UTC para o BigQuery (que armazena TIMESTAMP em UTC internamente).
    """
    if pd.isna(dt):
        return None
    try:
        # Converter para datetime preservando timezone
        if isinstance(dt, str):
            dt_parsed = pd.to_datetime(dt)
        elif isinstance(dt, pd.Timestamp):
            dt_parsed = dt
        else:
            dt_parsed = pd.to_datetime(dt)
        
        # Se tem timezone, converter para UTC (BigQuery armazena em UTC)
        if isinstance(dt_parsed, pd.Timestamp) and dt_parsed.tz is not None:
            # Converter para UTC
            dt_utc = dt_parsed.tz_convert('UTC')
            # Remover timezone para BigQuery (ele armazena como UTC internamente)
            return dt_utc.tz_localize(None)
        elif isinstance(dt_parsed, pd.Timestamp):
            # Sem timezone, assumir que já está no timezone correto
            # Converter para UTC assumindo que está em America/Sao_Paulo
            from datetime import timezone, timedelta
            tz_brasil = timezone(timedelta(hours=-3))
            dt_com_tz = dt_parsed.tz_localize(tz_brasil)
            dt_utc = dt_com_tz.tz_convert('UTC')
            return dt_utc.tz_localize(None)
        else:
            return dt_parsed
    except Exception as e:
        return None

def formatar_timestamp_nimbus(dt):
    """Formata timestamp no formato exato da NIMBUS: 2025-12-12 16:35:00.000 -0300
    
    Preserva o formato original como vem do banco da NIMBUS.
    """
    if not isinstance(dt, (datetime, pd.Timestamp)):
        return str(dt)
    
    # Converter para datetime se for pd.Timestamp
    if isinstance(dt, pd.Timestamp):
        dt = dt.to_pydatetime()
    
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

def formatar_dia_original(dt):
    """Formata datetime no formato exato da NIMBUS: 2009-02-16 02:12:20.000 -0300
    
    Preserva o formato STRING original como vem do banco da NIMBUS/servidor166.
    """
    if pd.isna(dt):
        return None
    try:
        # Se já é string no formato correto, retornar como está
        if isinstance(dt, str):
            # Verificar se já está no formato correto (tem timezone no final)
            if len(dt) > 10 and (dt[-5:].startswith('-') or dt[-5:].startswith('+')):
                return dt
            # Tentar converter
            dt_parsed = pd.to_datetime(dt)
        elif isinstance(dt, pd.Timestamp):
            dt_parsed = dt
        else:
            dt_parsed = pd.to_datetime(dt)
        
        # Extrair timezone offset
        offset_str = "-0300"  # Padrão Brasil
        if isinstance(dt_parsed, pd.Timestamp):
            if dt_parsed.tz is not None:
                offset = dt_parsed.tz.utcoffset(dt_parsed)
                if offset:
                    total_seconds = offset.total_seconds()
                    hours = int(total_seconds // 3600)
                    minutes = int((abs(total_seconds) % 3600) // 60)
                    # Formato: -0300 (sem dois pontos, como na NIMBUS)
                    offset_str = f"{hours:+03d}{minutes:02d}"
        
        # Formatar: 2009-02-16 02:12:20.000 -0300
        timestamp_str = dt_parsed.strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(dt_parsed, pd.Timestamp) and dt_parsed.microsecond:
            # Pegar apenas os 3 primeiros dígitos dos microsegundos
            microsec_str = str(dt_parsed.microsecond)[:3].zfill(3)
            timestamp_str += f".{microsec_str}"
        else:
            timestamp_str += ".000"
        
        return f"{timestamp_str} {offset_str}"
    except Exception:
        return None

def query_dados_incrementais(ultima_sincronizacao):
    """Retorna query para buscar apenas dados novos desde a última sincronização."""
    # Converter timestamp UTC do BigQuery para timestamptz do PostgreSQL (servidor166)
    # O servidor 166 tem coluna dia como TIMESTAMPTZ NOT NULL, então precisamos preservar o timezone
    if isinstance(ultima_sincronizacao, datetime):
        # Se tem timezone, converter para timezone do Brasil para comparar com dados do servidor166
        if ultima_sincronizacao.tzinfo:
            # Converter para timezone do Brasil
            from datetime import timedelta
            tz_brasil = timezone(timedelta(hours=-3))
            brasil_time = ultima_sincronizacao.astimezone(tz_brasil)
            # Formatar para PostgreSQL com timezone (servidor166 usa TIMESTAMPTZ)
            offset = brasil_time.tzinfo.utcoffset(brasil_time)
            horas_offset = int(offset.total_seconds() / 3600)
            minutos_offset = int((abs(offset.total_seconds()) % 3600) / 60)
            timestamp_str = brasil_time.strftime('%Y-%m-%d %H:%M:%S')
            timestamp_str += f" {horas_offset:+03d}:{abs(minutos_offset):02d}"
        else:
            # Sem timezone, assumir que já está em UTC e converter para Brasil
            from datetime import timedelta
            tz_utc = timezone.utc
            tz_brasil = timezone(timedelta(hours=-3))
            dt_utc = ultima_sincronizacao.replace(tzinfo=tz_utc)
            brasil_time = dt_utc.astimezone(tz_brasil)
            # Formatar com timezone
            offset = brasil_time.tzinfo.utcoffset(brasil_time)
            horas_offset = int(offset.total_seconds() / 3600)
            minutos_offset = int((abs(offset.total_seconds()) % 3600) / 60)
            timestamp_str = brasil_time.strftime('%Y-%m-%d %H:%M:%S')
            timestamp_str += f" {horas_offset:+03d}:{abs(minutos_offset):02d}"
    else:
        timestamp_str = str(ultima_sincronizacao)
        # Se não tem timezone na string, adicionar timezone do Brasil
        if ':' not in timestamp_str or ('+' not in timestamp_str and '-' not in timestamp_str.split()[-1]):
            timestamp_str += " -03:00"
    
    # Servidor166 usa TIMESTAMPTZ NOT NULL, então usar timestamptz
    return f"""
SELECT 
    dia,
    m05,
    m10,
    m15,
    h01,
    h04,
    h24,
    h96,
    estacao,
    estacao_id
FROM pluviometricos
WHERE dia > '{timestamp_str}'::timestamptz
ORDER BY dia ASC, estacao_id ASC;
"""

def sincronizar_incremental():
    """Sincroniza apenas dados novos do servidor 166 para BigQuery."""
    ORIGEM, BIGQUERY_CONFIG = carregar_configuracoes()
    
    # Obter credenciais BigQuery
    credentials = obter_credenciais_bigquery(BIGQUERY_CONFIG.get('credentials_path'))
    
    # Criar cliente BigQuery
    if credentials:
        client = bigquery.Client(credentials=credentials, project=BIGQUERY_CONFIG['project_id'])
    else:
        client = bigquery.Client(project=BIGQUERY_CONFIG['project_id'])
    
    dataset_id = BIGQUERY_CONFIG['dataset_id']
    table_id = BIGQUERY_CONFIG['table_id']
    table_ref = f"{BIGQUERY_CONFIG['project_id']}.{dataset_id}.{table_id}"
    
    print("=" * 80)
    print("🌧️ SINCRONIZAÇÃO INCREMENTAL - Servidor 166 → BigQuery")
    print("=" * 80)
    print(f"📊 Dataset: {dataset_id}")
    print(f"📋 Tabela: {table_id}")
    print("=" * 80)
    
    # Testar conexão servidor 166
    print("\n🔍 Testando conexão com servidor 166...")
    if not testar_conexao_servidor166(ORIGEM):
        print("❌ Falha na conexão com servidor 166!")
        return False
    
    print("✅ Conexão com servidor 166: OK")
    
    # Criar dataset se não existir
    print("\n📋 Verificando dataset...")
    criar_dataset_se_nao_existir(client, dataset_id)
    
    # Criar tabela se não existir
    print("\n📋 Verificando tabela...")
    schema = [
        bigquery.SchemaField("dia", "TIMESTAMP", mode="REQUIRED", description="Data e hora em que foi realizada a medição. Origem: TIMESTAMPTZ NOT NULL do servidor 166 (preserva timezone original da NIMBUS). Armazenado em UTC no BigQuery."),
        bigquery.SchemaField("m05", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("m10", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("m15", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("h01", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("h04", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("h24", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("h96", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("estacao", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("estacao_id", "INTEGER", mode="REQUIRED"),
    ]
    criar_tabela_com_schema(client, dataset_id, table_id, schema)
    
    # Obter última sincronização do BigQuery
    print("\n🔍 Obtendo última sincronização do BigQuery...")
    ultima_sincronizacao = obter_ultima_sincronizacao_bigquery(client, dataset_id, table_id)
    
    if ultima_sincronizacao == datetime(1997, 1, 1, tzinfo=timezone.utc):
        print("⚠️  Tabela BigQuery está vazia ou não encontrada!")
        print("   Execute PRIMEIRO: python scripts/bigquery/exportar_servidor166_para_bigquery.py")
        print("   para fazer a carga inicial dos dados históricos.")
        return False
    
    ultima_sync_formatada = obter_ultima_sincronizacao_bigquery(client, dataset_id, table_id, formatado=True)
    print(f"✅ Última sincronização: {ultima_sync_formatada}")
    
    # Criar engine SQLAlchemy para pandas
    # Codificar usuário e senha para URL (trata caracteres especiais)
    user_encoded = quote_plus(ORIGEM['user'])
    password_encoded = quote_plus(ORIGEM['password'])
    
    engine = create_engine(
        f"postgresql://{user_encoded}:{password_encoded}@{ORIGEM['host']}:{ORIGEM['port']}/{ORIGEM['dbname']}",
        connect_args={'sslmode': ORIGEM['sslmode'], 'connect_timeout': ORIGEM['connect_timeout']}
    )
    
    # Buscar dados incrementais
    print(f"\n🔍 Buscando dados novos desde {ultima_sync_formatada}...")
    query = query_dados_incrementais(ultima_sincronizacao)
    
    # Processar em chunks (reduzido para evitar problemas de memória)
    chunksize = 10000  # Reduzido de 25000 para evitar erro de memória
    total_registros = 0
    parquet_files = []
    
    try:
        # Ler dados em chunks
        for chunk_num, chunk_df in enumerate(pd.read_sql(query, engine, chunksize=chunksize), 1):
            if chunk_df.empty:
                print("   ℹ️  Nenhum dado novo encontrado.")
                break
            
            # Processar ambas as colunas: dia (TIMESTAMP) e dia_original (STRING)
            # Converter para UTC (BigQuery armazena em UTC)
            chunk_df['dia_original'] = chunk_df['dia'].apply(formatar_dia_original)
            chunk_df['dia'] = chunk_df['dia'].apply(processar_dia_timestamp)
            
            chunk_df['estacao_id'] = chunk_df['estacao_id'].astype('Int64')
            
            # Converter colunas numéricas
            numeric_cols = ['m05', 'm10', 'm15', 'h01', 'h04', 'h24', 'h96']
            for col in numeric_cols:
                chunk_df[col] = pd.to_numeric(chunk_df[col], errors='coerce').astype('float64')
            
            # Filtrar registros com dia NULL (BigQuery não aceita NULL em campo REQUIRED)
            chunk_df = chunk_df[chunk_df['dia'].notna()]
            
            if len(chunk_df) == 0:
                continue
            
            # Converter dia para datetime64[us] para Parquet (precisão de microsegundos)
            chunk_df['dia'] = pd.to_datetime(chunk_df['dia'], errors='coerce')
            
            # Salvar chunk em Parquet temporário
            with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp_file:
                tmp_path = tmp_file.name
            
            chunk_df.to_parquet(
                tmp_path, 
                index=False,
                engine='pyarrow',
                compression='snappy',
                coerce_timestamps='us'  # Precisão de microsegundos (igual ao servidor166)
            )
            parquet_files.append(tmp_path)
            
            total_registros += len(chunk_df)
            print(f"   📦 Chunk {chunk_num}: {len(chunk_df):,} registros processados (Total: {total_registros:,})")
            
            # Limpar memória
            del chunk_df
            gc.collect()
        
        if total_registros == 0:
            print("\n✅ Nenhum dado novo para sincronizar.")
            return True
        
        # Carregar arquivos Parquet no BigQuery
        print(f"\n📤 Carregando {total_registros:,} registros no BigQuery...")
        
        # Schema do BigQuery - MESMA estrutura do servidor166
        # Coluna dia como TIMESTAMP (armazena em UTC, vem de TIMESTAMPTZ NOT NULL do servidor 166)
        # O servidor 166 preserva o timezone original da NIMBUS como TIMESTAMPTZ
        # Coluna dia_original como STRING preserva formato exato do banco original
        schema = [
            bigquery.SchemaField("dia", "TIMESTAMP", mode="REQUIRED", description="Data e hora em que foi realizada a medição. Origem: TIMESTAMPTZ NOT NULL do servidor 166 (preserva timezone original da NIMBUS). Armazenado em UTC no BigQuery."),
            bigquery.SchemaField("dia_original", "STRING", mode="NULLABLE", description="Data e hora no formato exato do banco original da NIMBUS (ex: 2009-02-16 02:12:20.000 -0300)"),
            bigquery.SchemaField("m05", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("m10", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("m15", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("h01", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("h04", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("h24", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("h96", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("estacao", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("estacao_id", "INTEGER", mode="REQUIRED"),
        ]
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Adiciona dados novos
        )
        
        # Carregar cada arquivo Parquet
        for i, parquet_file in enumerate(parquet_files, 1):
            print(f"   📤 Carregando arquivo {i}/{len(parquet_files)}...")
            with open(parquet_file, 'rb') as source_file:
                job = client.load_table_from_file(
                    source_file,
                    table_ref,
                    job_config=job_config
                )
                job.result()  # Aguarda conclusão
            
            # Remover arquivo temporário
            os.unlink(parquet_file)
        
        print(f"\n✅ Sincronização concluída!")
        print(f"   📊 Total sincronizado: {total_registros:,} registros")
        
        # Obter novo último timestamp formatado
        nova_ultima_sync_formatada = obter_ultima_sincronizacao_bigquery(client, dataset_id, table_id, formatado=True)
        print(f"   🕐 Última sincronização atualizada: {nova_ultima_sync_formatada}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Erro durante sincronização: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Limpar arquivos temporários restantes
        for parquet_file in parquet_files:
            if os.path.exists(parquet_file):
                try:
                    os.unlink(parquet_file)
                except:
                    pass

def main():
    """Função principal."""
    try:
        if '--once' in sys.argv:
            # Modo único (para cron)
            sucesso = sincronizar_incremental()
            sys.exit(0 if sucesso else 1)
        else:
            # Modo interativo
            print("=" * 80)
            print("🌧️ SINCRONIZAÇÃO INCREMENTAL - Servidor 166 → BigQuery")
            print("=" * 80)
            print("\n⚠️  Para usar com cron, execute com --once:")
            print("   python scripts/bigquery/sincronizar_servidor166_para_bigquery.py --once")
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

