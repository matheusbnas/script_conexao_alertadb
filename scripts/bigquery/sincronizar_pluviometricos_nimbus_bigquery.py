#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🌧️ SINCRONIZAÇÃO INCREMENTAL - NIMBUS → BigQuery

═══════════════════════════════════════════════════════════════════════════
🎯 PROPÓSITO DESTE SCRIPT:
═══════════════════════════════════════════════════════════════════════════

Este script sincroniza APENAS os dados NOVOS desde a última sincronização
do banco NIMBUS para o BigQuery, usando EXATAMENTE a mesma lógica e estrutura
do script exportar_pluviometricos_nimbus_bigquery.py.

ARQUITETURA:
    NIMBUS (alertadb) → Parquet → BigQuery (incremental)
              ↑ [ESTE SCRIPT - SINCRONIZAÇÃO INCREMENTAL]

QUERY UTILIZADA:
    ✅ DISTINCT ON (el."horaLeitura", el.estacao_id)
    ✅ WHERE horaLeitura > ultima_sincronizacao (apenas dados novos)
    ✅ ORDER BY el."horaLeitura" ASC, el.estacao_id ASC, el.id DESC
    ✅ MESMA query do exportar_pluviometricos_nimbus_bigquery.py com filtro WHERE

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

✅ Busca último timestamp no BigQuery (MAX(dia))
✅ Busca APENAS dados novos desde esse timestamp no NIMBUS
✅ Exporta para formato Parquet (mesma estrutura do script de exportação)
✅ Carrega no BigQuery usando WRITE_APPEND
✅ Processa em lotes para otimizar memória
✅ Preserva tipos de dados e timezone corretamente
✅ Usa EXATAMENTE a mesma lógica do script de exportação

═══════════════════════════════════════════════════════════════════════════
⚠️ QUANDO USAR ESTE SCRIPT:
═══════════════════════════════════════════════════════════════════════════

✅ APÓS executar exportar_pluviometricos_nimbus_bigquery.py (carga inicial)
✅ Para manter os dados atualizados automaticamente via cron
✅ Em produção/servidor para sincronização contínua
✅ Quando você precisa de dados atualizados a cada 5 minutos

⚠️ NÃO USE se:
   ❌ A tabela BigQuery estiver vazia (use exportar_pluviometricos_nimbus_bigquery.py primeiro)
   ❌ Você quer carregar dados históricos (use exportar_pluviometricos_nimbus_bigquery.py)

═══════════════════════════════════════════════════════════════════════════
🚀 COMO USAR:
═══════════════════════════════════════════════════════════════════════════

1. PRIMEIRO: Execute exportar_pluviometricos_nimbus_bigquery.py para carga inicial
2. Configure o arquivo .env com as credenciais
3. Execute: python sincronizar_pluviometricos_nimbus_bigquery.py --once
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
            'table_id': obter_variavel('BIGQUERY_TABLE_ID', obrigatoria=False, padrao='pluviometricos'),
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
    """Obtém o último timestamp sincronizado do BigQuery (TIMESTAMP)."""
    try:
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
        return datetime(1997, 1, 1, tzinfo=timezone.utc)

def query_dados_incrementais(ultima_sincronizacao):
    """Retorna query para buscar apenas dados novos desde a última sincronização.
    
    MESMA query do exportar_pluviometricos_nimbus_bigquery.py, mas com WHERE.
    """
    # Converter timestamp UTC do BigQuery para timestamptz do PostgreSQL
    if isinstance(ultima_sincronizacao, datetime):
        if ultima_sincronizacao.tzinfo is None:
            ultima_sincronizacao = ultima_sincronizacao.replace(tzinfo=timezone.utc)
        
        # Formatar para PostgreSQL: usar UTC diretamente
        timestamp_str = ultima_sincronizacao.strftime('%Y-%m-%d %H:%M:%S')
        timestamp_str += "+00:00"
    else:
        timestamp_str = str(ultima_sincronizacao)
        if ':' not in timestamp_str or ('+' not in timestamp_str and '-' not in timestamp_str.split()[-1]):
            timestamp_str += "+00:00"
    
    # MESMA query do script de exportação, mas com WHERE
    return f"""
SELECT DISTINCT ON (el."horaLeitura", el.estacao_id)
    el."horaLeitura" AS "Dia",  -- TIMESTAMPTZ NOT NULL (preserva timezone original)
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

def obter_schema_pluviometricos():
    """Retorna schema do BigQuery para tabela pluviometricos."""
    return [
        bigquery.SchemaField("dia", "TIMESTAMP", mode="REQUIRED", description="Data e hora em que foi realizada a medição. Origem: TIMESTAMPTZ NOT NULL do NIMBUS (preserva timezone original). Armazenado em UTC no BigQuery."),
        bigquery.SchemaField("dia_original", "STRING", mode="NULLABLE", description="Data e hora no formato exato do banco original da NIMBUS (ex: 2009-02-16 02:12:20.000 -0300)"),
        bigquery.SchemaField("utc_offset", "STRING", mode="NULLABLE", description="Offset UTC do timezone original (ex: -0300 para horário padrão do Brasil, -0200 para horário de verão)"),
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

def sincronizar_incremental():
    """Sincroniza apenas dados novos do NIMBUS para BigQuery.
    
    Usa EXATAMENTE a mesma lógica do script de exportação.
    """
    engine_nimbus = None
    client_bq = None
    
    try:
        print("\n🔄 Iniciando sincronização incremental NIMBUS → BigQuery...")
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
                    print(f"      python scripts/bigquery/exportar_pluviometricos_nimbus_bigquery.py")
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
        
        # Obter última sincronização
        print("\n🔍 Obtendo última sincronização do BigQuery...")
        ultima_sincronizacao = obter_ultima_sincronizacao_bigquery(client_bq, dataset_id, table_id)
        
        if ultima_sincronizacao == datetime(1997, 1, 1, tzinfo=timezone.utc):
            print("⚠️  Tabela BigQuery está vazia ou não encontrada!")
            print("   Execute PRIMEIRO: python scripts/bigquery/exportar_pluviometricos_nimbus_bigquery.py")
            print("   para fazer a carga inicial dos dados históricos.")
            return False
        
        print(f"✅ Última sincronização: {ultima_sincronizacao} (UTC)")
        
        # Buscar dados incrementais
        print(f"\n🔍 Buscando dados novos desde {ultima_sincronizacao}...")
        query = query_dados_incrementais(ultima_sincronizacao)
        
        # Processar e carregar - EXATAMENTE a mesma lógica do script de exportação
        schema = obter_schema_pluviometricos()
        
        inicio_query = datetime.now()
        chunksize = 10000
        total_registros = 0
        chunk_numero = 1
        
        print(f"\n📦 Processando e carregando dados incrementais no BigQuery...")
        print(f"   💡 Usando formato Parquet para melhor performance")
        print(f"   💡 Query usa DISTINCT ON (mesma lógica dos scripts servidor166)\n")
        
        temp_dir = tempfile.mkdtemp()
        parquet_files = []
        
        chunks_list = []
        batch_size = 4
        batch_file_num = 1
        
        for chunk_df in pd.read_sql(query, engine_nimbus, chunksize=chunksize):
            print(f"   📦 Processando chunk {chunk_numero} ({len(chunk_df):,} registros)...")
            
            # Renomear colunas
            chunk_df = chunk_df.rename(columns={
                'Dia': 'dia',
                'Estacao': 'estacao'
            })
            
            # Remover colunas auxiliares
            colunas_para_remover = ['TimezoneOffset', 'leitura_id']
            for col in colunas_para_remover:
                if col in chunk_df.columns:
                    chunk_df = chunk_df.drop(columns=[col])
            
            # Processar coluna dia - MESMA função do script de exportação
            def processar_dia_timestamp(dt):
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
                """Extrai o offset UTC do timestamp original (ex: -0300, -0200).
                
                Retorna apenas o offset como string no formato -0300 ou -0200.
                """
                if pd.isna(dt):
                    return None
                try:
                    # Se já é string no formato correto, extrair offset
                    if isinstance(dt, str):
                        # Verificar se já está no formato correto (tem timezone no final)
                        if len(dt) > 10 and (dt[-5:].startswith('-') or dt[-5:].startswith('+')):
                            # Extrair os últimos 5 caracteres (ex: -0300)
                            return dt[-5:]
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
                    
                    return offset_str
                except Exception:
                    return None
            
            # Processar todas as colunas: dia (TIMESTAMP), dia_original (STRING) e utc_offset (STRING)
            chunk_df['dia_original'] = chunk_df['dia'].apply(formatar_dia_original)
            chunk_df['utc_offset'] = chunk_df['dia'].apply(extrair_utc_offset)
            chunk_df['dia'] = chunk_df['dia'].apply(processar_dia_timestamp)
            
            if 'estacao_id' in chunk_df.columns:
                chunk_df['estacao_id'] = chunk_df['estacao_id'].astype('Int64')
            
            # Converter colunas numéricas
            colunas_numericas = ['m05', 'm10', 'm15', 'h01', 'h04', 'h24', 'h96']
            for col in colunas_numericas:
                if col in chunk_df.columns:
                    chunk_df[col] = pd.to_numeric(chunk_df[col], errors='coerce').astype('float64')
            
            # Filtrar registros com dia NULL
            registros_antes = len(chunk_df)
            chunk_df = chunk_df[chunk_df['dia'].notna()]
            registros_depois = len(chunk_df)
            if registros_antes != registros_depois:
                print(f"      ⚠️  Removidos {registros_antes - registros_depois} registros com dia NULL")
            
            if len(chunk_df) > 0:
                chunks_list.append(chunk_df)
            total_registros += len(chunk_df)
            chunk_numero += 1
            
            # Escrever batch em arquivo Parquet
            if len(chunks_list) >= batch_size:
                df_batch = pd.concat(chunks_list, ignore_index=True)
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
            df_batch = df_batch[df_batch['dia'].notna()]
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
                schema=schema,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Sempre APPEND na sincronização
                source_format=bigquery.SourceFormat.PARQUET,
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
            print("🌧️ SINCRONIZAÇÃO INCREMENTAL - NIMBUS → BigQuery")
            print("=" * 80)
            print("\n⚠️  Para usar com cron, execute com --once:")
            print("   python scripts/bigquery/sincronizar_pluviometricos_nimbus_bigquery.py --once")
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

