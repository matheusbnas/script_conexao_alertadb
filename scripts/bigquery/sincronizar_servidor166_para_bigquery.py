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

def obter_ultima_sincronizacao_bigquery(client, dataset_id, table_id):
    """Obtém o último timestamp sincronizado do BigQuery (TIMESTAMP)."""
    try:
        # Como dia é TIMESTAMP, podemos usar MAX diretamente
        query = f"""
        SELECT MAX(dia) as ultima_sincronizacao
        FROM `{client.project}.{dataset_id}.{table_id}`
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        for row in results:
            if row.ultima_sincronizacao:
                # Converter para datetime com timezone
                ultima_sync = row.ultima_sincronizacao
                if isinstance(ultima_sync, datetime):
                    # BigQuery retorna TIMESTAMP em UTC, mas sem timezone info
                    # Assumir UTC
                    if ultima_sync.tzinfo is None:
                        ultima_sync = ultima_sync.replace(tzinfo=timezone.utc)
                    return ultima_sync
            break
        
        # Se não encontrou, retornar data de referência (1997-01-01)
        return datetime(1997, 1, 1, tzinfo=timezone.utc)
    except Exception as e:
        print(f"⚠️  Erro ao obter última sincronização: {e}")
        # Retornar data de referência em caso de erro
        return datetime(1997, 1, 1, tzinfo=timezone.utc)

def processar_dia_timestamp(dt):
    """Processa datetime para TIMESTAMP do BigQuery (UTC) - igual ao servidor166"""
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

def query_dados_incrementais(ultima_sincronizacao):
    """Retorna query para buscar apenas dados novos desde a última sincronização."""
    # Converter timestamp UTC do BigQuery para timestamp do PostgreSQL (servidor166)
    # O servidor 166 tem coluna dia como TIMESTAMP (sem timezone), então precisamos converter
    if isinstance(ultima_sincronizacao, datetime):
        # Se tem timezone, converter para UTC primeiro
        if ultima_sincronizacao.tzinfo:
            # Converter para UTC
            utc_time = ultima_sincronizacao.astimezone(timezone.utc)
            # Converter para timezone do Brasil para comparar com dados do servidor166
            from datetime import timedelta
            tz_brasil = timezone(timedelta(hours=-3))
            brasil_time = utc_time.astimezone(tz_brasil)
            # Formatar para PostgreSQL (sem timezone, pois servidor166 usa TIMESTAMP sem timezone)
            timestamp_str = brasil_time.strftime('%Y-%m-%d %H:%M:%S')
        else:
            # Sem timezone, assumir que já está em UTC e converter para Brasil
            from datetime import timedelta
            tz_utc = timezone.utc
            tz_brasil = timezone(timedelta(hours=-3))
            dt_utc = ultima_sincronizacao.replace(tzinfo=tz_utc)
            brasil_time = dt_utc.astimezone(tz_brasil)
            timestamp_str = brasil_time.strftime('%Y-%m-%d %H:%M:%S')
    else:
        timestamp_str = str(ultima_sincronizacao)
    
    # Servidor166 usa TIMESTAMP (sem timezone), então usar timestamp simples
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
WHERE dia > '{timestamp_str}'::timestamp
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
        bigquery.SchemaField("dia", "TIMESTAMP", mode="REQUIRED", description="Data e hora em que foi realizada a medição (no formato Y-m-d H:M:S)"),
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
    
    print(f"✅ Última sincronização: {ultima_sincronizacao.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Criar engine SQLAlchemy para pandas
    # Codificar usuário e senha para URL (trata caracteres especiais)
    user_encoded = quote_plus(ORIGEM['user'])
    password_encoded = quote_plus(ORIGEM['password'])
    
    engine = create_engine(
        f"postgresql://{user_encoded}:{password_encoded}@{ORIGEM['host']}:{ORIGEM['port']}/{ORIGEM['dbname']}",
        connect_args={'sslmode': ORIGEM['sslmode'], 'connect_timeout': ORIGEM['connect_timeout']}
    )
    
    # Buscar dados incrementais
    print(f"\n🔍 Buscando dados novos desde {ultima_sincronizacao.strftime('%Y-%m-%d %H:%M:%S %Z')}...")
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
            
            # Processar coluna dia como TIMESTAMP (igual ao servidor166)
            # Converter para UTC (BigQuery armazena em UTC)
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
        # Coluna dia como TIMESTAMP (igual ao servidor166 que usa TIMESTAMP)
        schema = [
            bigquery.SchemaField("dia", "TIMESTAMP", mode="REQUIRED", description="Data e hora em que foi realizada a medição (no formato Y-m-d H:M:S)"),
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
        
        # Obter novo último timestamp
        nova_ultima_sincronizacao = obter_ultima_sincronizacao_bigquery(client, dataset_id, table_id)
        print(f"   🕐 Última sincronização atualizada: {nova_ultima_sincronizacao.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        
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

