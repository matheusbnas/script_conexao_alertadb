#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🌤️ EXPORTAÇÃO DIRETA - DADOS METEOROLÓGICOS NIMBUS → BigQuery

═══════════════════════════════════════════════════════════════════════════
🎯 PROPÓSITO DESTE SCRIPT:
═══════════════════════════════════════════════════════════════════════════

Este script exporta dados METEOROLÓGICOS diretamente do banco NIMBUS (alertadb) 
para o BigQuery, usando DISTINCT ON (mesma lógica dos dados pluviométricos).

ARQUITETURA:
    NIMBUS (alertadb) → Parquet → BigQuery
              ↑ [ESTE SCRIPT - DIRETO]

QUERY UTILIZADA:
    ✅ DISTINCT ON (l."horaLeitura", l.estacao_id)
    ✅ ORDER BY l."horaLeitura" ASC, l.estacao_id ASC, l.id DESC
    ✅ Garante apenas um registro por (data_hora, estacao_id) - o mais recente
    ✅ Usa subquery com ROW_NUMBER para pivotar dados de sensores

VANTAGENS:
    ✅ Mais rápido (menos camadas)
    ✅ Dados sempre da fonte original (NIMBUS)
    ✅ BigQuery otimizado para análises
    ✅ Formato Parquet (5-10x mais rápido que CSV)
    ✅ Exportação completa (todos os dados desde 1997)

═══════════════════════════════════════════════════════════════════════════
📋 O QUE ESTE SCRIPT FAZ:
═══════════════════════════════════════════════════════════════════════════

✅ Conecta diretamente ao banco NIMBUS (alertadb)
✅ Busca TODOS os dados meteorológicos usando DISTINCT ON
✅ Exporta para formato Parquet completo
✅ Carrega no BigQuery automaticamente
✅ Cria/atualiza tabela no BigQuery
✅ Processa em lotes de 10.000 registros para otimizar memória
✅ Usa TIMESTAMP para coluna dia (igual aos pluviométricos)
✅ Converte timezone para UTC (padrão BigQuery)
✅ Particionamento por coluna dia (melhora performance)

═══════════════════════════════════════════════════════════════════════════
📋 CONFIGURAÇÃO:
═══════════════════════════════════════════════════════════════════════════

Variáveis obrigatórias no .env:
- DB_ORIGEM_HOST, DB_ORIGEM_NAME, DB_ORIGEM_USER, DB_ORIGEM_PASSWORD
- BIGQUERY_PROJECT_ID

Variáveis opcionais:
- BIGQUERY_DATASET_ID_NIMBUS (padrão: alertadb_cor_raw)
- BIGQUERY_TABLE_ID_METEOROLOGICOS (padrão: meteorologicos)
- BIGQUERY_CREDENTIALS_PATH (opcional: caminho para credentials.json)

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
        credentials_padrao = project_root / 'credentials' / 'credentials.json'
        
        credentials_path_env = obter_variavel('BIGQUERY_CREDENTIALS_PATH', obrigatoria=False)
        if credentials_path_env:
            credentials_path = Path(credentials_path_env)
            if not credentials_path.exists():
                print(f"   ⚠️  Caminho no .env não encontrado: {credentials_path}")
                print(f"   💡 Tentando usar caminho padrão: {credentials_padrao}")
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
        print("\n📝 Configure no .env:")
        print("   # Banco NIMBUS (origem)")
        print("   DB_ORIGEM_HOST=10.2.223.114")
        print("   DB_ORIGEM_NAME=alertadb")
        print("   DB_ORIGEM_USER=planejamento_cor")
        print("   DB_ORIGEM_PASSWORD=sua_senha")
        print("")
        print("   # BigQuery (destino)")
        print("   BIGQUERY_PROJECT_ID=seu-projeto-gcp")
        print("   BIGQUERY_DATASET_ID_NIMBUS=alertadb_cor_raw (opcional)")
        print("   BIGQUERY_TABLE_ID_METEOROLOGICOS=meteorologicos (opcional)")
        print("   BIGQUERY_CREDENTIALS_PATH=/caminho/credentials.json (opcional)")
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

def query_todos_dados_meteorologicos():
    """Retorna query para buscar TODOS os dados meteorológicos disponíveis no banco NIMBUS.
    
    Usa DISTINCT ON para garantir apenas um registro por (horaLeitura, estacao_id),
    mantendo o registro com o maior ID (mais recente), seguindo a mesma lógica
    dos dados pluviométricos.
    
    A query usa uma CTE para pivotar os dados dos sensores
    (Chuva, Direção Vento, Velocidade Vento, Temperatura, Pressão, Umidade)
    em colunas, agrupando por leitura_id. Depois usa DISTINCT ON para garantir
    apenas uma leitura por (horaLeitura, estacao_id), pegando a mais recente.
    
    IMPORTANTE: A ordem do ORDER BY deve corresponder à ordem do DISTINCT ON,
    e depois ordenar por leitura_id DESC para pegar o registro mais recente.
    
    A coluna horaLeitura é TIMESTAMPTZ NOT NULL no NIMBUS, preservando o timezone original.
    Esta é a MESMA lógica usada em query_todos_dados_pluviometricos() para garantir consistência.
    """
    return """
WITH sensores_pivotados AS (
    SELECT
        l."horaLeitura" AS data_hora,
        l.estacao_id,
        e.nome AS nome_estacao,
        l.id AS leitura_id,
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
    WHERE l."horaLeitura" >= '1997-01-01'
    GROUP BY l."horaLeitura", l.estacao_id, e.nome, l.id
)
SELECT DISTINCT ON (data_hora, estacao_id)
    data_hora AS "Dia",  -- TIMESTAMPTZ NOT NULL (preserva timezone original)
    estacao_id,
    nome_estacao AS "Estacao",
    chuva,
    dirVento,
    velVento,
    temperatura,
    pressao,
    umidade
FROM sensores_pivotados
ORDER BY data_hora ASC, estacao_id ASC, leitura_id DESC;
"""

def obter_schema_meteorologicos():
    """Retorna schema do BigQuery para tabela meteorologicos."""
    return [
        bigquery.SchemaField("dia", "TIMESTAMP", mode="REQUIRED", description="Data e hora em que foi realizada a medição. Origem: TIMESTAMPTZ NOT NULL do NIMBUS (preserva timezone original). Armazenado em UTC no BigQuery."),
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

def criar_dataset_se_nao_existir(client, dataset_id):
    """Cria dataset no BigQuery se não existir."""
    try:
        dataset_ref = client.dataset(dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        dataset.description = "Dados meteorológicos do NIMBUS"
        
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
                if table.num_rows > 0:
                    print(f"   📋 Tabela tem {table.num_rows:,} registros. Recriando com schema e particionamento...")
                    client.delete_table(table_ref)
                    table = bigquery.Table(table_ref, schema=schema)
                    table.description = "Dados meteorológicos do NIMBUS (desde 1997)"
                    table.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.MONTH,
                        field="dia"  # Particionamento por coluna dia (TIMESTAMP) - agrupado por MÊS
                    )
                    table = client.create_table(table)
                    print(f"✅ Tabela '{table_id}' recriada com schema e particionamento!")
                else:
                    print(f"   📋 Tabela vazia sem schema. Recriando com schema e particionamento...")
                    client.delete_table(table_ref)
                    table = bigquery.Table(table_ref, schema=schema)
                    table.description = "Dados meteorológicos do NIMBUS (desde 1997)"
                    table.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.MONTH,
                        field="dia"  # Particionamento por coluna dia (TIMESTAMP) - agrupado por MÊS
                    )
                    table = client.create_table(table)
                    print(f"✅ Tabela '{table_id}' recriada com schema e particionamento!")
                return True
            else:
                # Verificar se já tem particionamento
                # Verificar se o particionamento está correto (por coluna dia, tipo MONTH)
                if table.time_partitioning and table.time_partitioning.field:
                    # Verificar se está usando DAY (precisa mudar para MONTH) ou se já está MONTH
                    if table.time_partitioning.field != "dia":
                        print(f"   ⚠️  Tabela '{table_id}' existe com particionamento por campo '{table.time_partitioning.field}'.")
                        print(f"   💡 Precisamos recriar a tabela com particionamento por 'dia' (MÊS).")
                        print(f"   🔄 Deletando tabela para recriar com particionamento correto...")
                        client.delete_table(table_ref)
                        table = bigquery.Table(table_ref, schema=schema)
                        table.description = "Dados meteorológicos do NIMBUS (desde 1997)"
                        table.time_partitioning = bigquery.TimePartitioning(
                            type_=bigquery.TimePartitioningType.MONTH,
                            field="dia"  # Particionamento por coluna dia (TIMESTAMP) - agrupado por MÊS
                        )
                        table = client.create_table(table)
                        print(f"✅ Tabela '{table_id}' recriada com particionamento por coluna 'dia' (MÊS)!")
                    elif table.time_partitioning.type_ != bigquery.TimePartitioningType.MONTH:
                        # Tabela tem particionamento por DIA, mas precisa ser MONTH
                        print(f"   ⚠️  Tabela '{table_id}' existe com particionamento por DIA.")
                        print(f"   ⚠️  Isso pode exceder o limite de 10.000 partições!")
                        print(f"   💡 Precisamos recriar a tabela com particionamento por MÊS.")
                        print(f"   🔄 Deletando tabela para recriar com particionamento por MÊS...")
                        client.delete_table(table_ref)
                        table = bigquery.Table(table_ref, schema=schema)
                        table.description = "Dados meteorológicos do NIMBUS (desde 1997)"
                        table.time_partitioning = bigquery.TimePartitioning(
                            type_=bigquery.TimePartitioningType.MONTH,
                            field="dia"  # Particionamento por coluna dia (TIMESTAMP) - agrupado por MÊS
                        )
                        table = client.create_table(table)
                        print(f"✅ Tabela '{table_id}' recriada com particionamento por MÊS!")
                    else:
                        print(f"✅ Tabela '{table_id}' já existe com particionamento por coluna 'dia' (MÊS)!")
                elif not table.time_partitioning:
                    if table.num_rows == 0:
                        print(f"   📋 Tabela existe mas sem particionamento e está vazia.")
                        print(f"   🔄 Recriando tabela com particionamento por MÊS...")
                        client.delete_table(table_ref)
                        table = bigquery.Table(table_ref, schema=schema)
                        table.description = "Dados meteorológicos do NIMBUS (desde 1997)"
                        table.time_partitioning = bigquery.TimePartitioning(
                            type_=bigquery.TimePartitioningType.MONTH,
                            field="dia"  # Particionamento por coluna dia (TIMESTAMP) - agrupado por MÊS
                        )
                        table = client.create_table(table)
                        print(f"✅ Tabela '{table_id}' recriada com particionamento por MÊS!")
                    else:
                        print(f"   ⚠️  Tabela '{table_id}' existe mas SEM particionamento e tem {table.num_rows:,} registros.")
                        print(f"   💡 BigQuery não permite converter tabela não particionada em particionada.")
                        print(f"   📋 Continuando sem particionamento (dados serão substituídos com WRITE_TRUNCATE).")
                        print(f"   💡 Para ter particionamento, delete a tabela manualmente e execute o script novamente.")
                else:
                    print(f"✅ Tabela '{table_id}' já existe com schema ({len(table.schema)} campos) e particionamento por MÊS!")
                return True
        except Exception as e:
            # Tabela não existe, criar
            if "Not found" in str(e) or "404" in str(e) or "does not exist" in str(e).lower():
                print(f"   📋 Criando tabela '{table_id}' com schema e particionamento...")
            else:
                print(f"   ⚠️  Erro ao verificar tabela: {e}")
                raise
        
        # Criar tabela com schema e particionamento
        table = bigquery.Table(table_ref, schema=schema)
        table.description = "Dados meteorológicos do NIMBUS (desde 1997)"
        # Como dia é TIMESTAMP, usar particionamento por MÊS para evitar exceder limite de 10.000 partições
        # Com dados desde 1997, particionamento por DIA excederia o limite (mais de 10.000 dias)
        # Particionamento por MÊS reduz para ~340 partições (desde 1997 até hoje)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="dia"  # Particionamento por coluna dia (TIMESTAMP) - agrupado por MÊS
        )
        table = client.create_table(table, exists_ok=False)
        print(f"✅ Tabela '{table_id}' criada com schema e particionamento por MÊS no BigQuery!")
        print(f"   💡 Particionamento melhora performance de queries e reduz custos")
        return True
    except Exception as e:
        print(f"⚠️  Erro ao criar/atualizar tabela: {e}")
        import traceback
        traceback.print_exc()
        return False

def processar_e_carregar_tabela(engine_nimbus, client_bq, dataset_id, table_id, schema, query, descricao):
    """Processa e carrega uma tabela específica no BigQuery."""
    print(f"📦 Buscando dados {descricao} do NIMBUS...")
    print(f"   💡 Isso pode levar alguns minutos dependendo do volume de dados...")
    
    inicio_query = datetime.now()
    chunksize = 10000
    total_registros = 0
    chunk_numero = 1
    
    table_ref = client_bq.dataset(dataset_id).table(table_id)
    
    # Configurar job de carga
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.PARQUET,
    )
    
    print(f"\n📦 Processando e carregando dados {descricao} no BigQuery...")
    print(f"   💡 Usando formato Parquet para melhor performance")
    print(f"   💡 Query usa DISTINCT ON (mesma lógica dos dados pluviométricos)\n")
    
    # Criar diretório temporário para múltiplos arquivos Parquet
    temp_dir = tempfile.mkdtemp()
    parquet_files = []
    
    import gc
    chunks_list = []
    batch_size = 4
    batch_file_num = 1
    
    for chunk_df in pd.read_sql(query, engine_nimbus, chunksize=chunksize):
        print(f"   📦 Processando chunk {chunk_numero} ({len(chunk_df):,} registros)...")
        
        # Renomear colunas para corresponder ao schema BigQuery
        chunk_df = chunk_df.rename(columns={
            'Dia': 'dia',
            'Estacao': 'estacao'
        })
        
        # Remover colunas auxiliares se existirem
        colunas_para_remover = ['TimezoneOffset', 'leitura_id']
        for col in colunas_para_remover:
            if col in chunk_df.columns:
                chunk_df = chunk_df.drop(columns=[col])
        
        # Processar coluna dia: TIMESTAMPTZ do NIMBUS → TIMESTAMP (UTC) do BigQuery
        def processar_dia_timestamp(dt):
            """Processa TIMESTAMPTZ do PostgreSQL (NIMBUS) para TIMESTAMP do BigQuery (UTC).
            
            A coluna horaLeitura no banco NIMBUS é TIMESTAMPTZ NOT NULL (preserva timezone original).
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
        
        # Converter colunas numéricas se existirem
        colunas_numericas = ['chuva', 'dirVento', 'velVento', 'temperatura', 'pressao', 'umidade']
        for col in colunas_numericas:
            if col in chunk_df.columns:
                chunk_df[col] = pd.to_numeric(chunk_df[col], errors='coerce').astype('float64')
        
        # Filtrar registros com dia NULL
        registros_antes = len(chunk_df)
        chunk_df = chunk_df[chunk_df['dia'].notna()]
        registros_depois = len(chunk_df)
        if registros_antes != registros_depois:
            print(f"      ⚠️  Removidos {registros_antes - registros_depois} registros com dia NULL")
        
        # IMPORTANTE: NÃO converter novamente para datetime aqui!
        # A função processar_dia_timestamp() já retorna o tipo correto (datetime sem timezone)
        # Converter novamente pode causar problemas de precisão (nanossegundos vs microssegundos)
        
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
        print(f"   ⚠️  Nenhum dado {descricao} encontrado!")
        return 0
    
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
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE if i == 1 else bigquery.WriteDisposition.WRITE_APPEND,
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
    
    print(f"\n   ✅ Tabela '{table_id}' carregada: {total_registros:,} registros em {tempo_carga:.1f} segundos")
    
    return total_registros

def exportar_para_bigquery():
    """Exporta dados meteorológicos do NIMBUS diretamente para BigQuery."""
    engine_nimbus = None
    client_bq = None
    
    try:
        print("\n🔄 Iniciando exportação direta NIMBUS → BigQuery (DADOS METEOROLÓGICOS)...")
        print(f"   Origem: alertadb @ NIMBUS")
        print(f"   Destino: BigQuery ({BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_id']}.{BIGQUERY_CONFIG['table_id']})")
        print()
        
        # Conectar ao NIMBUS
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
        
        # Conectar ao BigQuery
        print("📦 Conectando ao BigQuery...")
        credentials_path = BIGQUERY_CONFIG.get('credentials_path')
        
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
                str(credentials_path)
            )
            client_bq = bigquery.Client(
                project=BIGQUERY_CONFIG['project_id'],
                credentials=credentials
            )
            print("   ✅ Credenciais carregadas com sucesso!")
        else:
            raise FileNotFoundError(
                f"❌ Arquivo de credenciais não encontrado!\n"
                f"   Caminho padrão: {project_root / 'credentials' / 'credentials.json'}\n"
                f"   💡 Coloque o arquivo credentials.json em: {project_root / 'credentials' / 'credentials.json'}"
            )
        
        # Criar dataset se não existir
        criar_dataset_se_nao_existir(client_bq, BIGQUERY_CONFIG['dataset_id'])
        
        # Criar tabela
        print("\n📋 Criando/verificando tabela no BigQuery...")
        schema_meteorologicos = obter_schema_meteorologicos()
        criar_tabela_com_schema(
            client_bq, 
            BIGQUERY_CONFIG['dataset_id'], 
            BIGQUERY_CONFIG['table_id'], 
            schema_meteorologicos
        )
        
        # Processar tabela meteorologicos
        print("\n" + "=" * 80)
        print("🌤️ PROCESSANDO TABELA: meteorologicos")
        print("=" * 80)
        
        query_meteorologicos = query_todos_dados_meteorologicos()
        total_meteorologicos = processar_e_carregar_tabela(
            engine_nimbus=engine_nimbus,
            client_bq=client_bq,
            dataset_id=BIGQUERY_CONFIG['dataset_id'],
            table_id=BIGQUERY_CONFIG['table_id'],
            schema=schema_meteorologicos,
            query=query_meteorologicos,
            descricao="meteorológicos"
        )
        
        # Resumo final
        print("\n" + "=" * 80)
        print("✅ EXPORTAÇÃO CONCLUÍDA")
        print("=" * 80)
        print(f"🌤️ meteorologicos: {total_meteorologicos:,} registros")
        
        return total_meteorologicos

    except Exception as e:
        print(f'\n❌ Erro na exportação: {e}')
        import traceback
        traceback.print_exc()
        return 0

    finally:
        if engine_nimbus:
            engine_nimbus.dispose()

def main():
    """Função principal."""
    print("=" * 70)
    print("🌤️ EXPORTAÇÃO DIRETA - DADOS METEOROLÓGICOS NIMBUS → BigQuery")
    print("=" * 70)
    print()
    print("🎯 PROPÓSITO:")
    print("   Exportar TODOS os dados METEOROLÓGICOS diretamente do NIMBUS")
    print("   para o BigQuery (pulando todas as camadas intermediárias)")
    print()
    print("📋 O QUE SERÁ FEITO:")
    print("   ✅ Buscar TODOS os dados meteorológicos do NIMBUS (desde 1997)")
    print("   ✅ Usar DISTINCT ON (mesma lógica dos dados pluviométricos)")
    print("   ✅ Criar dataset/tabela no BigQuery se não existir")
    print("   ✅ Exportar em formato Parquet completo")
    print("   ✅ Carregar no BigQuery automaticamente")
    print()
    print("⚠️  IMPORTANTE:")
    print("   - Requer credenciais do GCP configuradas")
    print("   - Formato: Parquet (mais eficiente para BigQuery)")
    print("   - Exportação completa: todos os dados")
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

