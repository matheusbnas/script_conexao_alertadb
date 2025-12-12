#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🌧️ EXPORTAÇÃO - Servidor 166 → BigQuery

═══════════════════════════════════════════════════════════════════════════
🎯 PROPÓSITO DESTE SCRIPT:
═══════════════════════════════════════════════════════════════════════════

Este script exporta dados do banco alertadb_cor (servidor 166) para o BigQuery.
Permite controle total dos dados já que você tem acesso como administrador ao banco.

ARQUITETURA:
    Servidor 166 (alertadb_cor) → Parquet → BigQuery
              ↑ [ESTE SCRIPT - COM CONTROLE ADMINISTRATIVO]

VANTAGENS:
    ✅ Controle total dos dados (você é admin do banco)
    ✅ Pode fazer tratamentos antes de exportar
    ✅ Dados já validados e tratados no servidor 166
    ✅ BigQuery otimizado para análises
    ✅ Formato Parquet (5-10x mais rápido que CSV)
    ✅ Coluna dia como TIMESTAMP (timestamptz NOT NULL no banco original da NIMBUS)

═══════════════════════════════════════════════════════════════════════════
📋 O QUE ESTE SCRIPT FAZ:
═══════════════════════════════════════════════════════════════════════════

✅ Conecta ao banco alertadb_cor (servidor 166)
✅ Busca TODOS os dados da tabela pluviometricos
✅ Exporta para formato Parquet completo
✅ Carrega no BigQuery automaticamente
✅ Cria/atualiza tabela no BigQuery
✅ Processa em lotes para otimizar memória
✅ Preserva formato original da coluna dia (TIMESTAMP - timestamptz NOT NULL)

═══════════════════════════════════════════════════════════════════════════
📋 CONFIGURAÇÃO:
═══════════════════════════════════════════════════════════════════════════

Variáveis obrigatórias no .env:
- DB_DESTINO_HOST, DB_DESTINO_NAME, DB_DESTINO_USER, DB_DESTINO_PASSWORD
- BIGQUERY_PROJECT_ID

Variáveis opcionais:
- BIGQUERY_DATASET_ID_SERVIDOR166 (padrão: alertadb_166_raw) - Dataset para dados servidor166 → BigQuery
- BIGQUERY_TABLE_ID (padrão: pluviometricos)
- BIGQUERY_CREDENTIALS_PATH (opcional: caminho para credentials.json)
"""

import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError as SQLAlchemyOperationalError
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from datetime import datetime
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
            from sqlalchemy import text
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        print(f"❌ Erro ao conectar ao servidor 166: {e}")
        return False

def query_todos_dados():
    """Retorna query para buscar TODOS os dados do servidor 166.
    
    A coluna 'dia' é TIMESTAMPTZ NOT NULL no servidor 166, preservando o timezone original da NIMBUS.
    O pandas/SQLAlchemy preserva automaticamente o timezone ao ler TIMESTAMPTZ.
    """
    return """
SELECT 
    dia,  -- TIMESTAMPTZ NOT NULL (preserva timezone original)
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
ORDER BY dia ASC, estacao_id ASC;
"""

def criar_dataset_se_nao_existir(client, dataset_id):
    """Cria dataset no BigQuery se não existir."""
    try:
        dataset_ref = client.dataset(dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # ou "us-west1" se preferir
        dataset.description = "Dataset para dados do servidor166 (NIMBUS → servidor166 → BigQuery)"
        
        dataset = client.create_dataset(dataset, exists_ok=True)
        print(f"✅ Dataset '{dataset_id}' criado/verificado no BigQuery!")
        return True
    except Exception as e:
        print(f"⚠️  Erro ao criar dataset: {e}")
        return False

def criar_tabela_com_schema(client, dataset_id, table_id, schema):
    """Cria tabela no BigQuery com schema e particionamento por data se não existir ou atualiza schema se necessário."""
    try:
        table_ref = client.dataset(dataset_id).table(table_id)
        
        # Verificar se a tabela já existe
        try:
            table = client.get_table(table_ref)
            # Se existe, verificar se tem schema válido
            if not table.schema or len(table.schema) == 0:
                print(f"   ⚠️  Tabela existe mas sem schema.")
                # Se tem dados, deletar e recriar (dados serão recarregados com WRITE_TRUNCATE)
                if table.num_rows > 0:
                    print(f"   📋 Tabela tem {table.num_rows:,} registros. Recriando com schema e particionamento...")
                    client.delete_table(table_ref)
                    # Criar nova tabela com schema e particionamento
                    table = bigquery.Table(table_ref, schema=schema)
                    table.description = "Dados pluviométricos do servidor 166"
                    # Particionar por coluna dia (TIMESTAMP)
                    table.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field="dia"  # Particionamento por coluna dia (TIMESTAMP)
                    )
                    table = client.create_table(table)
                    print(f"✅ Tabela '{table_id}' recriada com schema e particionamento por coluna 'dia'!")
                else:
                    # Tabela vazia, recriar com schema e particionamento
                    print(f"   📋 Tabela vazia sem schema. Recriando com schema e particionamento...")
                    client.delete_table(table_ref)
                    # Criar nova tabela com schema e particionamento
                    table = bigquery.Table(table_ref, schema=schema)
                    table.description = "Dados pluviométricos do servidor 166"
                    # Particionar por coluna dia (TIMESTAMP)
                    table.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field="dia"  # Particionamento por coluna dia (TIMESTAMP)
                    )
                    table = client.create_table(table)
                    print(f"✅ Tabela '{table_id}' recriada com schema e particionamento por coluna 'dia'!")
                return True
            else:
                # Verificar se já tem particionamento
                if table.time_partitioning and table.time_partitioning.field:
                    if table.time_partitioning.field != "dia":
                        print(f"   ⚠️  Tabela '{table_id}' existe com particionamento por campo '{table.time_partitioning.field}'.")
                        print(f"   💡 Precisamos recriar a tabela com particionamento por 'dia'.")
                        print(f"   🔄 Deletando tabela para recriar com particionamento correto...")
                        client.delete_table(table_ref)
                        # Criar nova tabela com particionamento por coluna dia
                        table = bigquery.Table(table_ref, schema=schema)
                        table.description = "Dados pluviométricos do servidor 166"
                        table.time_partitioning = bigquery.TimePartitioning(
                            type_=bigquery.TimePartitioningType.DAY,
                            field="dia"  # Particionamento por coluna dia (TIMESTAMP)
                        )
                        table = client.create_table(table)
                        print(f"✅ Tabela '{table_id}' recriada com particionamento por coluna 'dia'!")
                    else:
                        print(f"✅ Tabela '{table_id}' já existe com schema ({len(table.schema)} campos) e particionamento por coluna 'dia'!")
                elif not table.time_partitioning:
                    # BigQuery não permite converter tabela não particionada em particionada
                    # Se a tabela está vazia, podemos deletar e recriar com particionamento
                    if table.num_rows == 0:
                        print(f"   📋 Tabela existe mas sem particionamento e está vazia.")
                        print(f"   🔄 Recriando tabela com particionamento por coluna 'dia'...")
                        client.delete_table(table_ref)
                        # Criar nova tabela com particionamento por coluna dia
                        table = bigquery.Table(table_ref, schema=schema)
                        table.description = "Dados pluviométricos do servidor 166"
                        table.time_partitioning = bigquery.TimePartitioning(
                            type_=bigquery.TimePartitioningType.DAY,
                            field="dia"  # Particionamento por coluna dia (TIMESTAMP)
                        )
                        table = client.create_table(table)
                        print(f"✅ Tabela '{table_id}' recriada com particionamento por coluna 'dia'!")
                    else:
                        # Tabela tem dados, não podemos converter
                        print(f"   ⚠️  Tabela '{table_id}' existe mas SEM particionamento e tem {table.num_rows:,} registros.")
                        print(f"   💡 BigQuery não permite converter tabela não particionada em particionada.")
                        print(f"   📋 Continuando sem particionamento (dados serão substituídos com WRITE_TRUNCATE).")
                        print(f"   💡 Para ter particionamento, delete a tabela manualmente e execute o script novamente.")
                return True
        except Exception as e:
            # Tabela não existe, criar
            if "Not found" in str(e) or "404" in str(e) or "does not exist" in str(e).lower():
                print(f"   📋 Criando tabela '{table_id}' com schema e particionamento por coluna 'dia'...")
            else:
                print(f"   ⚠️  Erro ao verificar tabela: {e}")
                raise
        
        # Criar tabela com schema e particionamento
        table = bigquery.Table(table_ref, schema=schema)
        table.description = "Dados pluviométricos do servidor 166"
        # Particionar por coluna dia (TIMESTAMP)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="dia"  # Particionamento por coluna dia (TIMESTAMP)
        )
        table = client.create_table(table, exists_ok=False)
        print(f"✅ Tabela '{table_id}' criada com schema e particionamento por coluna 'dia' no BigQuery!")
        print(f"   💡 Particionamento melhora performance de queries e reduz custos")
        return True
    except Exception as e:
        print(f"⚠️  Erro ao criar/atualizar tabela: {e}")
        import traceback
        traceback.print_exc()
        return False

def processar_dia_timestamp(dt):
    """Processa TIMESTAMPTZ do PostgreSQL para TIMESTAMP do BigQuery (UTC).
    
    A coluna dia no servidor 166 é TIMESTAMPTZ NOT NULL (preserva timezone original da NIMBUS).
    O BigQuery armazena TIMESTAMP em UTC internamente, então convertemos preservando o valor correto.
    
    IMPORTANTE: Preserva o timezone original do TIMESTAMPTZ antes de converter para UTC.
    """
    if pd.isna(dt):
        return None
    try:
        # Converter para pandas Timestamp se necessário
        if isinstance(dt, str):
            # Tentar parsear preservando timezone se presente na string
            dt_parsed = pd.to_datetime(dt)
        elif isinstance(dt, pd.Timestamp):
            dt_parsed = dt
        elif hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
            # Se já é datetime com timezone (TIMESTAMPTZ do PostgreSQL)
            dt_parsed = pd.Timestamp(dt)
        else:
            dt_parsed = pd.to_datetime(dt)
        
        # Se já tem timezone (TIMESTAMPTZ do PostgreSQL), converter para UTC preservando o valor
        if isinstance(dt_parsed, pd.Timestamp) and dt_parsed.tz is not None:
            # Converter para UTC mantendo o valor absoluto correto
            dt_utc = dt_parsed.tz_convert('UTC')
            # Remover timezone info para BigQuery (ele armazena como UTC internamente)
            return dt_utc.tz_localize(None)
        elif isinstance(dt_parsed, pd.Timestamp):
            # Se não tem timezone, pode ser que o PostgreSQL retornou sem timezone
            # Neste caso, assumir que já está no timezone do servidor (Brasil -03:00)
            # e converter para UTC
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

def exportar_para_bigquery():
    """Exporta dados do servidor 166 diretamente para BigQuery."""
    engine_servidor166 = None
    client_bq = None
    
    timestamp_atual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        print("\n🔄 Iniciando exportação Servidor 166 → BigQuery...")
        print(f"   Origem: alertadb_cor @ servidor 166")
        print(f"   Destino: BigQuery ({BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_id']}.{BIGQUERY_CONFIG['table_id']})")
        print()
        
        # Conectar ao servidor 166 usando SQLAlchemy
        print("📦 Conectando ao servidor 166...")
        # Codificar usuário e senha para URL (trata caracteres especiais)
        user_encoded = quote_plus(ORIGEM['user'])
        password_encoded = quote_plus(ORIGEM['password'])
        
        connection_string = (
            f"postgresql://{user_encoded}:{password_encoded}@"
            f"{ORIGEM['host']}:{ORIGEM['port']}/{ORIGEM['dbname']}"
        )
        engine_servidor166 = create_engine(
            connection_string,
            connect_args={
                'client_encoding': 'UTF8',
                'connect_timeout': 30,
                'keepalives': 1,
                'keepalives_idle': 30,
                'keepalives_interval': 10,
                'keepalives_count': 5,
                'options': '-c statement_timeout=0'  # Desabilitar timeout de statement
            },
            pool_pre_ping=True,
            pool_recycle=3600,  # Reciclar conexões após 1 hora
            pool_size=5,
            max_overflow=10,
            echo=False
        )
        
        # Conectar ao BigQuery
        print("📦 Conectando ao BigQuery...")
        credentials_path = BIGQUERY_CONFIG.get('credentials_path')
        
        if not credentials_path or not Path(credentials_path).exists():
            credentials_padrao = project_root / 'credentials' / 'credentials.json'
            if credentials_padrao.exists():
                credentials_path = credentials_padrao
        
        if credentials_path and Path(credentials_path).exists():
            print(f"   🔑 Usando credenciais: {credentials_path}")
            credentials = service_account.Credentials.from_service_account_file(str(credentials_path))
            client_bq = bigquery.Client(
                project=BIGQUERY_CONFIG['project_id'],
                credentials=credentials
            )
        else:
            client_bq = bigquery.Client(project=BIGQUERY_CONFIG['project_id'])
        
        # Criar dataset se não existir
        criar_dataset_se_nao_existir(client_bq, BIGQUERY_CONFIG['dataset_id'])
        
        # Schema do BigQuery
        # Coluna dia como TIMESTAMP (armazena em UTC, vem de TIMESTAMPTZ NOT NULL do servidor 166)
        # O servidor 166 preserva o timezone original da NIMBUS como TIMESTAMPTZ
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
        
        # Criar tabela com schema se não existir
        criar_tabela_com_schema(
            client_bq, 
            BIGQUERY_CONFIG['dataset_id'], 
            BIGQUERY_CONFIG['table_id'], 
            schema
        )
        
        # Buscar dados do servidor 166
        print("\n📦 Buscando dados do servidor 166...")
        query = query_todos_dados()
        
        # Processar em chunks menores para evitar problemas de memória
        chunksize = 10000  # Reduzido de 25000 para evitar erro de memória
        batch_size = 4  # Escrever a cada 4 chunks
        chunks_list = []
        total_registros = 0
        parquet_files = []
        batch_file_num = 1
        chunk_numero = 0
        
        temp_dir = Path(tempfile.gettempdir()) / 'bigquery_export'
        temp_dir.mkdir(exist_ok=True)
        
        print(f"   📊 Processando em chunks de {chunksize:,} registros...")
        print(f"   💡 Usando chunks menores para evitar problemas de memória")
        print(f"   🔄 Configurações de conexão otimizadas para queries longas")
        
        # Função auxiliar para ler chunk com retry
        def ler_chunks_com_retry(query, engine_ref, chunksize, max_retries=3):
            """Lê chunks com retry automático em caso de erro de conexão."""
            tentativa_global = 0
            
            while tentativa_global < max_retries:
                try:
                    # Tentar criar iterator de chunks
                    chunk_iterator = pd.read_sql(query, engine_ref['engine'], chunksize=chunksize)
                    
                    # Processar cada chunk
                    for chunk_df in chunk_iterator:
                        yield chunk_df
                    
                    # Se chegou aqui, leitura completa com sucesso
                    return
                    
                except (psycopg2.OperationalError, psycopg2.InterfaceError,
                        SQLAlchemyOperationalError) as e:
                    tentativa_global += 1
                    if tentativa_global < max_retries:
                        wait_time = tentativa_global * 5
                        print(f"      ⚠️  Erro de conexão (tentativa {tentativa_global}/{max_retries}): {str(e)[:100]}")
                        print(f"      🔄 Reconectando em {wait_time}s...")
                        import time
                        time.sleep(wait_time)
                        # Forçar reciclagem do pool de conexões
                        try:
                            engine_ref['engine'].dispose()
                        except:
                            pass
                        # Recriar engine
                        print(f"      🔄 Recriando conexão...")
                        user_encoded = quote_plus(ORIGEM['user'])
                        password_encoded = quote_plus(ORIGEM['password'])
                        connection_string = (
                            f"postgresql://{user_encoded}:{password_encoded}@"
                            f"{ORIGEM['host']}:{ORIGEM['port']}/{ORIGEM['dbname']}"
                        )
                        engine_ref['engine'] = create_engine(
                            connection_string,
                            connect_args={
                                'client_encoding': 'UTF8',
                                'connect_timeout': 30,
                                'keepalives': 1,
                                'keepalives_idle': 30,
                                'keepalives_interval': 10,
                                'keepalives_count': 5,
                                'options': '-c statement_timeout=0'
                            },
                            pool_pre_ping=True,
                            pool_recycle=3600,
                            pool_size=5,
                            max_overflow=10
                        )
                    else:
                        print(f"      ❌ Falha após {max_retries} tentativas")
                        raise
        
        # Usar dicionário mutável para permitir atualização do engine
        engine_ref = {'engine': engine_servidor166}
        
        # Ler chunks com retry automático
        for chunk_df in ler_chunks_com_retry(query, engine_ref, chunksize):
            chunk_numero += 1
            
            if chunk_df.empty:
                continue
            
            # Processar coluna dia: TIMESTAMPTZ do servidor 166 → TIMESTAMP (UTC) do BigQuery
            # O servidor 166 tem TIMESTAMPTZ NOT NULL que preserva o timezone original da NIMBUS
            # O BigQuery armazena TIMESTAMP em UTC, então convertemos preservando o valor correto
            chunk_df['dia'] = chunk_df['dia'].apply(processar_dia_timestamp)
            
            # Converter tipos
            chunk_df['estacao_id'] = chunk_df['estacao_id'].astype('Int64')
            
            # Converter colunas numéricas
            colunas_numericas = ['m05', 'm10', 'm15', 'h01', 'h04', 'h24', 'h96']
            for col in colunas_numericas:
                chunk_df[col] = pd.to_numeric(chunk_df[col], errors='coerce')
            
            # Filtrar registros com dia NULL
            registros_antes = len(chunk_df)
            chunk_df = chunk_df[chunk_df['dia'].notna()]
            registros_depois = len(chunk_df)
            if registros_antes != registros_depois:
                print(f"      ⚠️  Removidos {registros_antes - registros_depois} registros com dia NULL")
            
            # Só adicionar se ainda tiver registros válidos
            if len(chunk_df) > 0:
                chunks_list.append(chunk_df.copy())  # Usar copy() para evitar referências
            total_registros += len(chunk_df)
            
            # Limpar memória após processar cada chunk
            del chunk_df
            gc.collect()
            
            # Escrever batch em arquivo Parquet separado quando atingir batch_size
            if len(chunks_list) >= batch_size:
                df_batch = pd.concat(chunks_list, ignore_index=True)
                
                # Garantir que coluna dia está como datetime64[us] (microsegundos) para BigQuery
                if 'dia' in df_batch.columns:
                    if not pd.api.types.is_datetime64_any_dtype(df_batch['dia']):
                        # Se não for datetime, tentar converter
                        df_batch['dia'] = pd.to_datetime(df_batch['dia'], errors='coerce')
                    # Converter para microsegundos (precisão do BigQuery TIMESTAMP)
                    df_batch['dia'] = df_batch['dia'].astype('datetime64[us]')
                
                batch_file = Path(temp_dir) / f'pluviometricos_batch_{batch_file_num:04d}.parquet'
                df_batch.to_parquet(
                    batch_file, 
                    index=False, 
                    engine='pyarrow', 
                    compression='snappy',
                    coerce_timestamps='us'  # Forçar microsegundos para TIMESTAMP
                )
                parquet_files.append(batch_file)
                print(f"      💾 Batch {batch_file_num} salvo: {batch_file.stat().st_size / (1024*1024):.2f} MB")
                
                # Limpar lista e liberar memória
                chunks_list.clear()
                del df_batch
                gc.collect()
                batch_file_num += 1
        
        # Escrever chunks restantes
        if chunks_list:
            df_batch = pd.concat(chunks_list, ignore_index=True)
            df_batch = df_batch[df_batch['dia'].notna()]
            
            if len(df_batch) > 0:
                # Garantir que coluna dia está como datetime64[us] (microsegundos) para BigQuery
                if 'dia' in df_batch.columns:
                    if not pd.api.types.is_datetime64_any_dtype(df_batch['dia']):
                        # Se não for datetime, tentar converter
                        df_batch['dia'] = pd.to_datetime(df_batch['dia'], errors='coerce')
                    # Converter para microsegundos (precisão do BigQuery TIMESTAMP)
                    df_batch['dia'] = df_batch['dia'].astype('datetime64[us]')
                
                batch_file = Path(temp_dir) / f'pluviometricos_batch_{batch_file_num:04d}.parquet'
                df_batch.to_parquet(
                    batch_file, 
                    index=False, 
                    engine='pyarrow', 
                    compression='snappy',
                    coerce_timestamps='us'  # Forçar microsegundos para TIMESTAMP
                )
                parquet_files.append(batch_file)
                print(f"      💾 Batch {batch_file_num} salvo: {batch_file.stat().st_size / (1024*1024):.2f} MB")
                del df_batch
                gc.collect()
        
        if total_registros == 0:
            print("   ⚠️  Nenhum dado encontrado!")
            return 0
        
        # Carregar arquivos Parquet no BigQuery
        print(f"\n📤 Carregando {total_registros:,} registros no BigQuery...")
        table_ref = f"{BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_id']}.{BIGQUERY_CONFIG['table_id']}"
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Substitui dados existentes
        )
        
        # Carregar cada arquivo Parquet
        for i, parquet_file in enumerate(parquet_files, 1):
            print(f"   📤 Carregando arquivo {i}/{len(parquet_files)}...")
            with open(parquet_file, 'rb') as source_file:
                job = client_bq.load_table_from_file(
                    source_file,
                    table_ref,
                    job_config=job_config
                )
                job.result()  # Aguarda conclusão
            
            # Remover arquivo temporário
            os.unlink(parquet_file)
        
        print(f"\n✅ Exportação concluída!")
        print(f"   📊 Total exportado: {total_registros:,} registros")
        print(f"   🕐 Concluído em: {timestamp_atual}")
        
        return total_registros
        
    except Exception as e:
        print(f"\n❌ Erro durante exportação: {e}")
        import traceback
        traceback.print_exc()
        return 0
    
    finally:
        # Limpar conexões
        if 'engine_ref' in locals() and engine_ref.get('engine'):
            try:
                engine_ref['engine'].dispose()
            except:
                pass
        elif 'engine_servidor166' in locals() and engine_servidor166:
            try:
                engine_servidor166.dispose()
            except:
                pass

def main():
    """Função principal."""
    global ORIGEM, BIGQUERY_CONFIG
    
    try:
        print("=" * 80)
        print("🌧️ EXPORTAÇÃO - Servidor 166 → BigQuery")
        print("=" * 80)
        print("🎯 PROPÓSITO: Exportar dados do alertadb_cor (servidor 166) para BigQuery")
        print("📋 O QUE SERÁ FEITO:")
        print("   ✅ Buscar todos os dados do servidor 166")
        print("   ✅ Criar tabela no BigQuery se não existir")
        print("   ✅ Processar em lotes de 25.000 registros")
        print("   ✅ Preservar formato original da coluna dia (STRING)")
        print("=" * 80)
        
        ORIGEM, BIGQUERY_CONFIG = carregar_configuracoes()
        
        # Testar conexões
        print("\n" + "=" * 80)
        print("TESTE DE CONEXÕES")
        print("=" * 80)
        
        print("\n🔍 Testando conexão com servidor 166...")
        if not testar_conexao_servidor166(ORIGEM):
            print("❌ Falha na conexão com servidor 166!")
            return
        
        print("✅ ORIGEM (Servidor 166): SUCESSO!")
        print(f"   {ORIGEM['dbname']}@{ORIGEM['host']}:{ORIGEM['port']}")
        
        # Exportar dados
        total = exportar_para_bigquery()
        
        if total > 0:
            print("\n" + "=" * 80)
            print("✅ EXPORTAÇÃO CONCLUÍDA COM SUCESSO!")
            print("=" * 80)
            print(f"📊 Total de registros exportados: {total:,}")
        else:
            print("\n⚠️  Nenhum dado foi exportado.")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrompido pelo usuário.")
    except Exception as e:
        print(f"\n❌ Erro fatal: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

