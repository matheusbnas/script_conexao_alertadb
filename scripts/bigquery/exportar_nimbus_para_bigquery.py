#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🌧️ EXPORTAÇÃO DIRETA - NIMBUS → BigQuery

═══════════════════════════════════════════════════════════════════════════
🎯 PROPÓSITO DESTE SCRIPT:
═══════════════════════════════════════════════════════════════════════════

Este script exporta dados diretamente do banco NIMBUS (alertadb) para o 
BigQuery, usando a MESMA lógica de coleta dos scripts servidor166.

ARQUITETURA:
    NIMBUS (alertadb) → Parquet → BigQuery
              ↑ [ESTE SCRIPT - DIRETO]

QUERY UTILIZADA:
    ✅ DISTINCT ON (el."horaLeitura", el.estacao_id)
    ✅ ORDER BY el."horaLeitura" ASC, el.estacao_id ASC, el.id DESC
    ✅ Mesma lógica de carregar_pluviometricos_historicos.py
    ✅ Garante apenas um registro por (dia, estacao_id) - o mais recente

VANTAGENS:
    ✅ Mais rápido (menos camadas)
    ✅ Dados sempre da fonte original (NIMBUS)
    ✅ BigQuery otimizado para análises
    ✅ Ideal para stakeholders
    ✅ Formato Parquet (5-10x mais rápido que CSV)
    ✅ Exportação completa (todos os dados desde 1997)

═══════════════════════════════════════════════════════════════════════════
📋 O QUE ESTE SCRIPT FAZ:
═══════════════════════════════════════════════════════════════════════════

✅ Conecta diretamente ao banco NIMBUS (alertadb)
✅ Busca TODOS os dados usando DISTINCT ON (mesma lógica dos scripts servidor166)
✅ Exporta para formato Parquet completo (não dividido por ano)
✅ Carrega no BigQuery automaticamente
✅ Cria/atualiza tabela no BigQuery
✅ Processa em lotes de 100.000 registros para otimizar memória
✅ Preserva tipos de dados e timezone corretamente

═══════════════════════════════════════════════════════════════════════════
📋 CONFIGURAÇÃO:
═══════════════════════════════════════════════════════════════════════════

Variáveis obrigatórias no .env:
- DB_ORIGEM_HOST, DB_ORIGEM_NAME, DB_ORIGEM_USER, DB_ORIGEM_PASSWORD
- BIGQUERY_PROJECT_ID

Variáveis opcionais:
- BIGQUERY_DATASET_ID (padrão: pluviometricos)
- BIGQUERY_TABLE_ID (padrão: pluviometricos)
- BIGQUERY_CREDENTIALS_PATH (opcional: caminho para credentials.json)
- BIGQUERY_CONNECTION_ID (opcional: ID da conexão BigQuery existente)

📚 GUIA COMPLETO: Veja docs/BIGQUERY_CONFIGURAR_VARIAVEIS.md para saber
   onde encontrar cada configuração no console GCP/BigQuery.
"""

import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
import tempfile

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
        # Verificar se existe credentials.json na pasta credentials (padrão)
        # project_root já está definido no escopo do módulo
        credentials_padrao = project_root / 'credentials' / 'credentials.json'
        
        # Se não foi especificado no .env, usar o padrão se existir
        credentials_path_env = obter_variavel('BIGQUERY_CREDENTIALS_PATH', obrigatoria=False)
        if credentials_path_env:
            credentials_path = Path(credentials_path_env)
            if not credentials_path.exists():
                print(f"   ⚠️  Caminho no .env não encontrado: {credentials_path}")
                print(f"   💡 Tentando usar caminho padrão: {credentials_padrao}")
                # Tentar o padrão mesmo se o .env especificou um caminho inválido
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
            'dataset_id': obter_variavel('BIGQUERY_DATASET_ID', obrigatoria=False, padrao='pluviometricos'),
            'table_id': obter_variavel('BIGQUERY_TABLE_ID', obrigatoria=False, padrao='pluviometricos'),
            'credentials_path': str(credentials_path) if credentials_path else None,
            'connection_id': obter_variavel('BIGQUERY_CONNECTION_ID', obrigatoria=False)  # Opcional: conexão existente
        }
        
        return origem, bigquery_config
    
    except ValueError as e:
        print("=" * 70)
        print("❌ ERRO DE CONFIGURAÇÃO")
        print("=" * 70)
        print(str(e))
        print("\n📝 Configure no .env:")
        print("   # Banco NIMBUS (origem)")
        print("   DB_ORIGEM_HOST=10.2.223.114")
        print("   DB_ORIGEM_NAME=alertadb")
        print("   DB_ORIGEM_USER=planejamento_cor")
        print("   DB_ORIGEM_PASSWORD=sua_senha")
        print("")
        print("   # BigQuery (destino)")
        print("   BIGQUERY_PROJECT_ID=seu-projeto-gcp")
        print("   BIGQUERY_DATASET_ID=pluviometricos (opcional)")
        print("   BIGQUERY_TABLE_ID=pluviometricos (opcional)")
        print("   BIGQUERY_CREDENTIALS_PATH=/caminho/credentials.json (opcional)")
        print("   BIGQUERY_CONNECTION_ID=projects/.../connections/... (opcional)")
        print("=" * 70)
        raise

ORIGEM, BIGQUERY_CONFIG = carregar_configuracoes()

def testar_conexao_nimbus():
    """Testa conexão com NIMBUS."""
    print("=" * 70)
    print("TESTE DE CONEXÃO")
    print("=" * 70)
    
    try:
        conn = psycopg2.connect(**ORIGEM)
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        print(f"   ✅ NIMBUS: SUCESSO!")
        print(f"      {ORIGEM['dbname']}@{ORIGEM['host']}:{ORIGEM['port']}")
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"   ❌ NIMBUS: FALHA!")
        print(f"      Erro: {e}")
        return False

def query_todos_dados():
    """Retorna query para buscar TODOS os dados disponíveis no banco NIMBUS.
    
    Usa DISTINCT ON para garantir apenas um registro por (dia, estacao_id),
    mantendo o registro com o maior ID (mais recente), que é exatamente como
    está no banco alertadb.
    
    IMPORTANTE: A ordem do ORDER BY deve corresponder à ordem do DISTINCT ON,
    e depois ordenar por id DESC para pegar o registro mais recente.
    
    Esta é a MESMA query usada em carregar_pluviometricos_historicos.py e
    sincronizar_pluviometricos_novos.py para garantir consistência.
    """
    return """
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
ORDER BY el."horaLeitura" ASC, el.estacao_id ASC, el.id DESC;
"""

def criar_dataset_se_nao_existir(client, dataset_id):
    """Cria dataset no BigQuery se não existir."""
    try:
        dataset_ref = client.dataset(dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # ou "us-west1" se preferir
        dataset.description = "Dados pluviométricos do NIMBUS"
        
        dataset = client.create_dataset(dataset, exists_ok=True)
        print(f"✅ Dataset '{dataset_id}' criado/verificado no BigQuery!")
        return True
    except Exception as e:
        print(f"⚠️  Erro ao criar dataset: {e}")
        return False

def criar_tabela_com_schema(client, dataset_id, table_id, schema):
    """Cria tabela no BigQuery com schema se não existir ou atualiza schema se necessário."""
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
                    print(f"   📋 Tabela tem {table.num_rows:,} registros. Recriando com schema...")
                    client.delete_table(table_ref)
                    # Criar nova tabela com schema
                    table = bigquery.Table(table_ref, schema=schema)
                    table.description = "Dados pluviométricos do NIMBUS (desde 1997)"
                    table = client.create_table(table)
                    print(f"✅ Tabela '{table_id}' recriada com schema!")
                else:
                    # Tabela vazia, apenas atualizar schema
                    print(f"   📋 Atualizando schema da tabela vazia...")
                    table.schema = schema
                    table.description = "Dados pluviométricos do NIMBUS (desde 1997)"
                    table = client.update_table(table, ["schema", "description"])
                    print(f"✅ Schema atualizado na tabela '{table_id}'!")
                return True
            else:
                print(f"✅ Tabela '{table_id}' já existe com schema ({len(table.schema)} campos)!")
                return True
        except Exception as e:
            # Tabela não existe, criar
            if "Not found" in str(e) or "404" in str(e) or "does not exist" in str(e).lower():
                print(f"   📋 Criando tabela '{table_id}' com schema...")
            else:
                print(f"   ⚠️  Erro ao verificar tabela: {e}")
                raise
        
        # Criar tabela com schema
        table = bigquery.Table(table_ref, schema=schema)
        table.description = "Dados pluviométricos do NIMBUS (desde 1997)"
        table = client.create_table(table, exists_ok=False)
        print(f"✅ Tabela '{table_id}' criada com schema no BigQuery!")
        return True
    except Exception as e:
        print(f"⚠️  Erro ao criar/atualizar tabela: {e}")
        import traceback
        traceback.print_exc()
        return False

def exportar_para_bigquery():
    """Exporta dados do NIMBUS diretamente para BigQuery."""
    engine_nimbus = None
    client_bq = None
    
    timestamp_atual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        print("\n🔄 Iniciando exportação direta NIMBUS → BigQuery...")
        print(f"   Origem: alertadb @ NIMBUS")
        print(f"   Destino: BigQuery ({BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_id']}.{BIGQUERY_CONFIG['table_id']})")
        print()
        
        # Conectar ao NIMBUS usando SQLAlchemy (recomendado pelo pandas)
        print("📦 Conectando ao NIMBUS...")
        # Criar string de conexão PostgreSQL para SQLAlchemy
        connection_string = (
            f"postgresql://{ORIGEM['user']}:{ORIGEM['password']}@"
            f"{ORIGEM['host']}:{ORIGEM['port']}/{ORIGEM['dbname']}"
        )
        engine_nimbus = create_engine(
            connection_string,
            connect_args={'client_encoding': 'UTF8'},
            pool_pre_ping=True  # Verifica conexão antes de usar
        )
        
        # Conectar ao BigQuery
        print("📦 Conectando ao BigQuery...")
        credentials_path = BIGQUERY_CONFIG.get('credentials_path')
        
        # Se não foi encontrado no carregamento, tentar novamente o caminho padrão
        if not credentials_path or not Path(credentials_path).exists():
            credentials_padrao = project_root / 'credentials' / 'credentials.json'
            print(f"   🔍 Verificando caminho padrão: {credentials_padrao}")
            if credentials_padrao.exists():
                credentials_path = credentials_padrao
                print(f"   ✅ Arquivo encontrado no caminho padrão!")
            else:
                print(f"   ⚠️  Arquivo não encontrado no caminho padrão")
        
        if credentials_path and Path(credentials_path).exists():
            print(f"   🔑 Usando credenciais: {credentials_path}")
            credentials = service_account.Credentials.from_service_account_file(
                str(credentials_path)  # Garantir que é string
            )
            client_bq = bigquery.Client(
                project=BIGQUERY_CONFIG['project_id'],
                credentials=credentials
            )
            print("   ✅ Credenciais carregadas com sucesso!")
        elif credentials_path:
            print(f"   ⚠️  Arquivo de credenciais não encontrado: {credentials_path}")
            print(f"   🔍 Tentando caminho padrão: {project_root / 'credentials' / 'credentials.json'}")
            raise FileNotFoundError(
                f"❌ Arquivo de credenciais não encontrado!\n"
                f"   Procurado em: {credentials_path}\n"
                f"   Caminho padrão: {project_root / 'credentials' / 'credentials.json'}\n"
                f"   💡 Coloque o arquivo credentials.json em: {project_root / 'credentials' / 'credentials.json'}"
            )
        else:
            print("   ⚠️  Nenhum arquivo de credenciais encontrado")
            print(f"   🔍 Tentando caminho padrão: {project_root / 'credentials' / 'credentials.json'}")
            raise FileNotFoundError(
                f"❌ Arquivo de credenciais não encontrado!\n"
                f"   Caminho padrão: {project_root / 'credentials' / 'credentials.json'}\n"
                f"   💡 Coloque o arquivo credentials.json em: {project_root / 'credentials' / 'credentials.json'}"
            )
        
        # Criar dataset se não existir
        criar_dataset_se_nao_existir(client_bq, BIGQUERY_CONFIG['dataset_id'])
        
        # Schema do BigQuery
        # Usar FLOAT64 ao invés de NUMERIC porque:
        # - pandas.to_numeric() cria float64 (DOUBLE) no Parquet
        # - BigQuery não converte DOUBLE → NUMERIC automaticamente
        # - FLOAT64 tem precisão suficiente para dados de precipitação (15-17 dígitos)
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
        
        # Criar tabela com schema se não existir
        criar_tabela_com_schema(
            client_bq, 
            BIGQUERY_CONFIG['dataset_id'], 
            BIGQUERY_CONFIG['table_id'], 
            schema
        )
        
        # Buscar dados do NIMBUS
        query = query_todos_dados()
        print("📦 Buscando dados do NIMBUS...")
        print("   💡 Isso pode levar alguns minutos dependendo do volume de dados...")
        
        inicio_query = datetime.now()
        
        # Ler dados em chunks para não sobrecarregar memória
        # Reduzir chunksize para evitar erro de memória
        chunksize = 25000  # Reduzido de 100000 para evitar problemas de memória
        total_registros = 0
        chunk_numero = 1
        
        table_ref = client_bq.dataset(BIGQUERY_CONFIG['dataset_id']).table(BIGQUERY_CONFIG['table_id'])
        
        # Configurar job de carga
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Substitui dados existentes
            source_format=bigquery.SourceFormat.PARQUET,  # Usar Parquet para melhor performance
        )
        
        print("\n📦 Processando e carregando dados no BigQuery...")
        print("   💡 Usando formato Parquet para melhor performance")
        print("   💡 Query usa DISTINCT ON (mesma lógica dos scripts servidor166)")
        print("   💡 Exportação completa (todos os dados desde 1997)\n")
        
        # Criar diretório temporário para múltiplos arquivos Parquet
        # Estratégia: escrever múltiplos arquivos e carregar todos no BigQuery de uma vez
        # Isso evita problemas de memória ao não precisar ler arquivos grandes de volta
        temp_dir = tempfile.mkdtemp()
        parquet_files = []  # Lista de arquivos Parquet criados
        
        # Processar chunks e escrever em arquivos Parquet separados
        import gc  # Para liberar memória
        
        chunks_list = []
        batch_size = 4  # Escrever a cada 4 chunks (100k registros total)
        batch_file_num = 1
        
        for chunk_df in pd.read_sql(query, engine_nimbus, chunksize=chunksize):
            print(f"   📦 Processando chunk {chunk_numero} ({len(chunk_df):,} registros)...")
            
            # Renomear colunas para corresponder ao schema BigQuery
            # A query retorna "Dia" e "Estacao" (com aspas)
            chunk_df = chunk_df.rename(columns={
                'Dia': 'dia',
                'Estacao': 'estacao'
            })
            
            # Remover coluna TimezoneOffset se existir (não vamos usar)
            if 'TimezoneOffset' in chunk_df.columns:
                chunk_df = chunk_df.drop(columns=['TimezoneOffset'])
            
            # Criar coluna dia_original ANTES de converter para UTC
            # Formato: 2009-02-18 00:57:20.000 -0300 (igual ao banco NIMBUS)
            def formatar_dia_original(dt):
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
                    
                    # Extrair timezone offset
                    offset_str = "-0300"  # Padrão Brasil
                    if isinstance(dt_parsed, pd.Timestamp):
                        if dt_parsed.tz is not None:
                            offset = dt_parsed.tz.utcoffset(dt_parsed)
                            if offset:
                                total_seconds = offset.total_seconds()
                                hours = int(total_seconds // 3600)
                                minutes = int((abs(total_seconds) % 3600) // 60)
                                offset_str = f"{hours:+03d}{minutes:02d}"
                    
                    # Formatar: 2009-02-18 00:57:20.000 -0300
                    timestamp_str = dt_parsed.strftime('%Y-%m-%d %H:%M:%S')
                    if isinstance(dt_parsed, pd.Timestamp) and dt_parsed.microsecond:
                        # Pegar apenas os 3 primeiros dígitos dos microsegundos
                        microsec_str = str(dt_parsed.microsecond)[:3].zfill(3)
                        timestamp_str += f".{microsec_str}"
                    else:
                        timestamp_str += ".000"
                    
                    return f"{timestamp_str} {offset_str}"
                except Exception as e:
                    return None
            
            # Criar dia_original ANTES de converter dia para UTC
            # IMPORTANTE: Preservar o datetime original com timezone antes de converter
            chunk_df['dia_original'] = chunk_df['dia'].apply(formatar_dia_original)
            
            # Converter tipos de data para UTC (padrão BigQuery)
            # Converter para UTC mas preservar formato original na coluna dia_original
            try:
                chunk_df['dia'] = pd.to_datetime(chunk_df['dia'], errors='coerce')
                # Se tem timezone, converter para UTC primeiro, depois remover
                if hasattr(chunk_df['dia'].dtype, 'tz') and chunk_df['dia'].dtype.tz is not None:
                    chunk_df['dia'] = chunk_df['dia'].dt.tz_convert('UTC').dt.tz_localize(None)
                else:
                    # Se não tem timezone, converter para UTC
                    chunk_df['dia'] = pd.to_datetime(chunk_df['dia'], utc=True, errors='coerce')
                    # Remover timezone se existir
                    if hasattr(chunk_df['dia'].dtype, 'tz') and chunk_df['dia'].dtype.tz is not None:
                        chunk_df['dia'] = chunk_df['dia'].dt.tz_localize(None)
            except (ValueError, AttributeError):
                # Se falhar, tentar converter diretamente
                chunk_df['dia'] = pd.to_datetime(chunk_df['dia'], utc=True, errors='coerce')
                # Tentar remover timezone se existir
                try:
                    if hasattr(chunk_df['dia'].dtype, 'tz') and chunk_df['dia'].dtype.tz is not None:
                        chunk_df['dia'] = chunk_df['dia'].dt.tz_localize(None)
                except:
                    pass
            
            chunk_df['estacao_id'] = chunk_df['estacao_id'].astype('Int64')
            
            # Garantir que valores numéricos sejam do tipo correto
            colunas_numericas = ['m05', 'm10', 'm15', 'h01', 'h04', 'h24', 'h96']
            for col in colunas_numericas:
                chunk_df[col] = pd.to_numeric(chunk_df[col], errors='coerce')
            
            # Filtrar registros com dia NULL (BigQuery não aceita NULL em campo REQUIRED)
            registros_antes = len(chunk_df)
            chunk_df = chunk_df[chunk_df['dia'].notna()]
            registros_depois = len(chunk_df)
            if registros_antes != registros_depois:
                print(f"      ⚠️  Removidos {registros_antes - registros_depois} registros com dia NULL")
            
            # Só adicionar se ainda tiver registros válidos
            if len(chunk_df) > 0:
                chunks_list.append(chunk_df)
            total_registros += len(chunk_df)
            chunk_numero += 1
            
            # Escrever batch em arquivo Parquet separado quando atingir batch_size
            if len(chunks_list) >= batch_size:
                df_batch = pd.concat(chunks_list, ignore_index=True)
                
                # Converter timestamp para microsegundos (BigQuery espera microsegundos, não nanossegundos)
                # Verificar se tem timezone antes de tentar acessar
                if 'dia' in df_batch.columns and pd.api.types.is_datetime64_any_dtype(df_batch['dia']):
                    # Verificar se tem timezone usando hasattr (mais seguro)
                    if hasattr(df_batch['dia'].dtype, 'tz') and df_batch['dia'].dtype.tz is not None:
                        # Se tem timezone, converter para UTC primeiro, depois remover
                        df_batch['dia'] = df_batch['dia'].dt.tz_convert('UTC').dt.tz_localize(None)
                    # Converter para microsegundos explicitamente
                    df_batch['dia'] = df_batch['dia'].astype('datetime64[us]')
                
                batch_file = Path(temp_dir) / f'pluviometricos_batch_{batch_file_num:04d}.parquet'
                df_batch.to_parquet(
                    batch_file, 
                    index=False, 
                    engine='pyarrow', 
                    compression='snappy',
                    coerce_timestamps='us'  # Forçar microsegundos
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
            
            # Garantir que não há valores NULL em dia (campo REQUIRED)
            df_batch = df_batch[df_batch['dia'].notna()]
            
            if len(df_batch) > 0:
                # Converter timestamp para microsegundos (BigQuery espera microsegundos, não nanossegundos)
                # Verificar se tem timezone antes de tentar acessar
                if 'dia' in df_batch.columns and pd.api.types.is_datetime64_any_dtype(df_batch['dia']):
                    # Verificar se tem timezone usando hasattr (mais seguro)
                    if hasattr(df_batch['dia'].dtype, 'tz') and df_batch['dia'].dtype.tz is not None:
                        # Se tem timezone, converter para UTC primeiro, depois remover
                        df_batch['dia'] = df_batch['dia'].dt.tz_convert('UTC').dt.tz_localize(None)
                    # Converter para microsegundos explicitamente
                    df_batch['dia'] = df_batch['dia'].astype('datetime64[us]')
                
                batch_file = Path(temp_dir) / f'pluviometricos_batch_{batch_file_num:04d}.parquet'
                df_batch.to_parquet(
                    batch_file, 
                    index=False, 
                    engine='pyarrow', 
                    compression='snappy',
                    coerce_timestamps='us'  # Forçar microsegundos
                )
                parquet_files.append(batch_file)
                print(f"      💾 Batch {batch_file_num} salvo: {batch_file.stat().st_size / (1024*1024):.2f} MB")
                del df_batch
                gc.collect()
        
        if total_registros == 0:
            print("   ⚠️  Nenhum dado encontrado!")
            return 0
        
        total_size = sum(f.stat().st_size for f in parquet_files) / (1024*1024)
        print(f"\n   ✅ {len(parquet_files)} arquivos Parquet criados: {total_size:.2f} MB total")
        
        tempo_query = (datetime.now() - inicio_query).total_seconds()
        print(f"\n   ✅ Dados processados: {total_registros:,} registros em {tempo_query:.1f} segundos")
        
        # Carregar múltiplos arquivos Parquet no BigQuery
        print(f"\n📤 Carregando {len(parquet_files)} arquivos Parquet no BigQuery...")
        print(f"   Tabela: {BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_id']}.{BIGQUERY_CONFIG['table_id']}")
        
        inicio_carga = datetime.now()
        
        # Carregar cada arquivo Parquet no BigQuery
        # Estratégia: carregar um por vez com WRITE_APPEND após o primeiro (WRITE_TRUNCATE)
        # Isso evita problemas de memória ao não precisar concatenar arquivos grandes
        
        for i, parquet_file in enumerate(parquet_files, 1):
            print(f"   📤 Carregando arquivo {i}/{len(parquet_files)}: {parquet_file.name}...")
            
            # Criar novo job_config para cada arquivo
            # Para o primeiro arquivo, usar WRITE_TRUNCATE (substitui dados existentes)
            # Para os demais, usar WRITE_APPEND (adiciona aos dados existentes)
            file_job_config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE if i == 1 else bigquery.WriteDisposition.WRITE_APPEND,
                source_format=bigquery.SourceFormat.PARQUET,
            )
            
            with open(parquet_file, 'rb') as source_file:
                job = client_bq.load_table_from_file(
                    source_file,
                    table_ref,
                    job_config=file_job_config
                )
                # Aguardar conclusão antes de carregar o próximo (evita sobrecarga)
                job.result()
                print(f"      ✅ Arquivo {i}/{len(parquet_files)} carregado com sucesso")
        
        tempo_carga = (datetime.now() - inicio_carga).total_seconds()
        
        # Limpar arquivos temporários
        print(f"   🧹 Limpando arquivos temporários...")
        for parquet_file in parquet_files:
            try:
                parquet_file.unlink()
            except Exception as e:
                print(f"      ⚠️  Erro ao deletar {parquet_file.name}: {e}")
        try:
            os.rmdir(temp_dir)
        except Exception as e:
            print(f"      ⚠️  Erro ao deletar diretório temporário: {e}")
        
        # Estatísticas finais
        table = client_bq.get_table(table_ref)
        
        print("\n" + "=" * 70)
        print("✅ EXPORTAÇÃO PARA BIGQUERY CONCLUÍDA!")
        print("=" * 70)
        print(f"📊 Total de registros: {total_registros:,}")
        print(f"📊 Registros no BigQuery: {table.num_rows:,}")
        print(f"📊 Tamanho da tabela: {table.num_bytes / (1024*1024):.2f} MB")
        print(f"⏱️  Tempo de processamento: {tempo_query:.1f} segundos")
        print(f"⏱️  Tempo de carga no BigQuery: {tempo_carga:.1f} segundos")
        print(f"⏰ Concluído em: {timestamp_atual}")
        print("=" * 70)
        
        print(f"\n📊 Tabela disponível em:")
        print(f"   {BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_id']}.{BIGQUERY_CONFIG['table_id']}")
        print(f"\n💡 Você pode consultar no BigQuery Console:")
        print(f"   https://console.cloud.google.com/bigquery?project={BIGQUERY_CONFIG['project_id']}")
        
        return total_registros

    except Exception as e:
        print(f'\n❌ Erro na exportação: {e}')
        import traceback
        traceback.print_exc()
        return 0

    finally:
        if engine_nimbus:
            engine_nimbus.dispose()  # Fecha todas as conexões do pool SQLAlchemy

def main():
    """Função principal."""
    print("=" * 70)
    print("🌧️ EXPORTAÇÃO DIRETA - NIMBUS → BigQuery")
    print("=" * 70)
    print()
    print("🎯 PROPÓSITO:")
    print("   Exportar TODOS os dados diretamente do NIMBUS")
    print("   para o BigQuery (pulando todas as camadas intermediárias)")
    print()
    print("📋 O QUE SERÁ FEITO:")
    print("   ✅ Buscar TODOS os dados do NIMBUS (desde 1997)")
    print("   ✅ Usar DISTINCT ON (mesma lógica dos scripts servidor166)")
    print("   ✅ Criar dataset/tabela no BigQuery se não existir")
    print("   ✅ Exportar em formato Parquet completo (não dividido por ano)")
    print("   ✅ Carregar no BigQuery automaticamente")
    print()
    print("⚠️  IMPORTANTE:")
    print("   - Requer credenciais do GCP configuradas")
    print("   - Formato: Parquet (mais eficiente para BigQuery)")
    print("   - Exportação completa: todos os dados em um único arquivo")
    print("   - Query usa DISTINCT ON para garantir unicidade")
    print("=" * 70)
    
    # Testar conexão
    if not testar_conexao_nimbus():
        print("\n❌ Não foi possível conectar ao NIMBUS. Verifique as configurações.")
        return
    
    # Exportar
    exportar_para_bigquery()
    
    print("\n💡 PRÓXIMO PASSO:")
    print("   Configure exportação periódica ou use este script")
    print("   quando precisar atualizar os dados no BigQuery.\n")

if __name__ == '__main__':
    main()

