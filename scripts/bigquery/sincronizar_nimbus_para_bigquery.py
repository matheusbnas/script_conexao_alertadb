#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🌧️ SINCRONIZAÇÃO INCREMENTAL - NIMBUS → BigQuery

═══════════════════════════════════════════════════════════════════════════
🎯 PROPÓSITO DESTE SCRIPT:
═══════════════════════════════════════════════════════════════════════════

Este script sincroniza APENAS os dados NOVOS desde a última sincronização
do banco NIMBUS para o BigQuery, executando de forma incremental.

ARQUITETURA:
    NIMBUS (alertadb) → Parquet → BigQuery (incremental)
              ↑ [ESTE SCRIPT - SINCRONIZAÇÃO INCREMENTAL]

QUERY UTILIZADA:
    ✅ DISTINCT ON (el."horaLeitura", el.estacao_id)
    ✅ WHERE horaLeitura > ultima_sincronizacao
    ✅ ORDER BY el."horaLeitura" ASC, el.estacao_id ASC, el.id DESC
    ✅ Mesma lógica de sincronizar_pluviometricos_novos.py

VANTAGENS:
    ✅ Sincronização incremental (apenas dados novos)
    ✅ Execução rápida (não processa todos os dados)
    ✅ Ideal para cron (executa a cada 5 minutos)
    ✅ BigQuery sempre atualizado
    ✅ Formato Parquet (otimizado)

═══════════════════════════════════════════════════════════════════════════
📋 O QUE ESTE SCRIPT FAZ:
═══════════════════════════════════════════════════════════════════════════

✅ Busca último timestamp no BigQuery (MAX(dia))
✅ Busca APENAS dados novos desde esse timestamp no NIMBUS
✅ Exporta para formato Parquet
✅ Carrega no BigQuery usando WRITE_APPEND
✅ Processa em lotes para otimizar memória
✅ Preserva tipos de dados e timezone corretamente

═══════════════════════════════════════════════════════════════════════════
⚠️ QUANDO USAR ESTE SCRIPT:
═══════════════════════════════════════════════════════════════════════════

✅ APÓS executar exportar_nimbus_para_bigquery.py (carga inicial)
✅ Para manter os dados atualizados automaticamente via cron
✅ Em produção/servidor para sincronização contínua
✅ Quando você precisa de dados atualizados a cada 5 minutos

⚠️ NÃO USE se:
   ❌ A tabela BigQuery estiver vazia (use exportar_nimbus_para_bigquery.py primeiro)
   ❌ Você quer carregar dados históricos (use exportar_nimbus_para_bigquery.py)

═══════════════════════════════════════════════════════════════════════════
🚀 COMO USAR:
═══════════════════════════════════════════════════════════════════════════

1. PRIMEIRO: Execute exportar_nimbus_para_bigquery.py para carga inicial
2. Configure o arquivo .env com as credenciais
3. Execute: python sincronizar_nimbus_para_bigquery.py --once
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
        bigquery_config = {
            'project_id': obter_variavel('BIGQUERY_PROJECT_ID'),
            'dataset_id': obter_variavel('BIGQUERY_DATASET_ID', obrigatoria=False, padrao='alertadb_cor_raw'),
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

def testar_conexao_nimbus(origem):
    """Testa conexão com banco NIMBUS."""
    try:
        engine = create_engine(
            f"postgresql://{origem['user']}:{origem['password']}@{origem['host']}:{origem['port']}/{origem['dbname']}",
            connect_args={'sslmode': origem['sslmode'], 'connect_timeout': origem['connect_timeout']}
        )
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        return True
    except Exception as e:
        print(f"❌ Erro ao conectar ao NIMBUS: {e}")
        return False

def obter_ultima_sincronizacao_bigquery(client, dataset_id, table_id):
    """Obtém o último timestamp sincronizado do BigQuery."""
    try:
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
                    # Se não tem timezone, assumir UTC
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

def query_dados_incrementais(ultima_sincronizacao):
    """Retorna query para buscar apenas dados novos desde a última sincronização."""
    # Converter timestamp para string formatada para PostgreSQL
    if isinstance(ultima_sincronizacao, datetime):
        # Converter para timezone do Brasil se necessário
        if ultima_sincronizacao.tzinfo:
            # Converter para UTC primeiro, depois para timezone do Brasil
            utc_time = ultima_sincronizacao.astimezone(timezone.utc)
            # Formatar para PostgreSQL com timezone
            timestamp_str = utc_time.strftime('%Y-%m-%d %H:%M:%S')
        else:
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
WHERE el."horaLeitura" > '{timestamp_str}'::timestamptz
ORDER BY el."horaLeitura" ASC, el.estacao_id ASC, el.id DESC;
"""

def sincronizar_incremental():
    """Sincroniza apenas dados novos do NIMBUS para BigQuery."""
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
    print("🌧️ SINCRONIZAÇÃO INCREMENTAL - NIMBUS → BigQuery")
    print("=" * 80)
    print(f"📊 Dataset: {dataset_id}")
    print(f"📋 Tabela: {table_id}")
    print("=" * 80)
    
    # Testar conexão NIMBUS
    print("\n🔍 Testando conexão com NIMBUS...")
    if not testar_conexao_nimbus(ORIGEM):
        print("❌ Falha na conexão com NIMBUS!")
        return False
    
    print("✅ Conexão com NIMBUS: OK")
    
    # Obter última sincronização do BigQuery
    print("\n🔍 Obtendo última sincronização do BigQuery...")
    ultima_sincronizacao = obter_ultima_sincronizacao_bigquery(client, dataset_id, table_id)
    
    if ultima_sincronizacao == datetime(1997, 1, 1, tzinfo=timezone.utc):
        print("⚠️  Tabela BigQuery está vazia ou não encontrada!")
        print("   Execute PRIMEIRO: python scripts/bigquery/exportar_nimbus_para_bigquery.py")
        print("   para fazer a carga inicial dos dados históricos.")
        return False
    
    print(f"✅ Última sincronização: {ultima_sincronizacao.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Criar engine SQLAlchemy para pandas
    engine = create_engine(
        f"postgresql://{ORIGEM['user']}:{ORIGEM['password']}@{ORIGEM['host']}:{ORIGEM['port']}/{ORIGEM['dbname']}",
        connect_args={'sslmode': ORIGEM['sslmode'], 'connect_timeout': ORIGEM['connect_timeout']}
    )
    
    # Buscar dados incrementais
    print(f"\n🔍 Buscando dados novos desde {ultima_sincronizacao.strftime('%Y-%m-%d %H:%M:%S %Z')}...")
    query = query_dados_incrementais(ultima_sincronizacao)
    
    # Processar em chunks
    chunksize = 25000
    total_registros = 0
    parquet_files = []
    
    try:
        # Ler dados em chunks
        for chunk_num, chunk_df in enumerate(pd.read_sql(query, engine, chunksize=chunksize), 1):
            if chunk_df.empty:
                print("   ℹ️  Nenhum dado novo encontrado.")
                break
            
            # Processar chunk
            chunk_df = chunk_df.rename(columns={
                'Dia': 'dia',
                'Estacao': 'estacao'
            })
            
            # Criar coluna dia_original ANTES de converter para UTC
            def formatar_dia_original(dt):
                if pd.isna(dt):
                    return None
                try:
                    if isinstance(dt, str):
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
            
            chunk_df['dia_original'] = chunk_df['dia'].apply(formatar_dia_original)
            
            # Converter dia para UTC
            try:
                chunk_df['dia'] = pd.to_datetime(chunk_df['dia'], errors='coerce')
                if hasattr(chunk_df['dia'].dtype, 'tz') and chunk_df['dia'].dtype.tz is not None:
                    chunk_df['dia'] = chunk_df['dia'].dt.tz_convert('UTC').dt.tz_localize(None)
                else:
                    chunk_df['dia'] = pd.to_datetime(chunk_df['dia'], utc=True, errors='coerce')
                    if hasattr(chunk_df['dia'].dtype, 'tz') and chunk_df['dia'].dtype.tz is not None:
                        chunk_df['dia'] = chunk_df['dia'].dt.tz_localize(None)
            except (ValueError, AttributeError):
                chunk_df['dia'] = pd.to_datetime(chunk_df['dia'], utc=True, errors='coerce')
                try:
                    if hasattr(chunk_df['dia'].dtype, 'tz') and chunk_df['dia'].dtype.tz is not None:
                        chunk_df['dia'] = chunk_df['dia'].dt.tz_localize(None)
                except:
                    pass
            
            # Converter para datetime64[us] (microsecond precision)
            chunk_df['dia'] = chunk_df['dia'].astype('datetime64[us]')
            
            chunk_df['estacao_id'] = chunk_df['estacao_id'].astype('Int64')
            
            # Converter colunas numéricas
            numeric_cols = ['m05', 'm10', 'm15', 'h01', 'h04', 'h24', 'h96']
            for col in numeric_cols:
                chunk_df[col] = pd.to_numeric(chunk_df[col], errors='coerce').astype('float64')
            
            # Salvar chunk em Parquet temporário
            with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp_file:
                tmp_path = tmp_file.name
            
            chunk_df.to_parquet(tmp_path, index=False, coerce_timestamps='us')
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
        
        schema = [
            bigquery.SchemaField("dia", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("dia_original", "STRING", mode="NULLABLE", description="Data/hora original do NIMBUS com timezone (ex: 2009-02-18 00:57:20.000 -0300)"),
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
            print("🌧️ SINCRONIZAÇÃO INCREMENTAL - NIMBUS → BigQuery")
            print("=" * 80)
            print("\n⚠️  Para usar com cron, execute com --once:")
            print("   python scripts/bigquery/sincronizar_nimbus_para_bigquery.py --once")
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

