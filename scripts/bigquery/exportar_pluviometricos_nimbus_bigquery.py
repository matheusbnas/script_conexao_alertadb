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
    ✅ MESMA query de carregar_pluviometricos_historicos.py
    ✅ Garante apenas um registro por (dia, estacao_id) - o mais recente
    ✅ Estrutura de dados IDÊNTICA ao servidor166 (TIMESTAMP para dia)

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
✅ Busca TODOS os dados usando DISTINCT ON (MESMA query do servidor166)
✅ Exporta para formato Parquet completo (não dividido por ano)
✅ Carrega no BigQuery automaticamente
✅ Cria/atualiza tabela no BigQuery com estrutura IDÊNTICA ao servidor166
✅ Processa em lotes de 10.000 registros para otimizar memória
✅ Usa TIMESTAMP para coluna dia (igual ao servidor166)
✅ Converte timezone para horário local de SP (America/Sao_Paulo) - dia e dia_original com mesmo horário
✅ Particionamento por coluna dia (melhora performance)

═══════════════════════════════════════════════════════════════════════════
📋 CONFIGURAÇÃO:
═══════════════════════════════════════════════════════════════════════════

Variáveis obrigatórias no .env:
- DB_ORIGEM_HOST, DB_ORIGEM_NAME, DB_ORIGEM_USER, DB_ORIGEM_PASSWORD
- BIGQUERY_PROJECT_ID

Variáveis opcionais:
- BIGQUERY_DATASET_ID_NIMBUS (padrão: alertadb_cor_raw) - Dataset para dados NIMBUS → BigQuery
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
            'dataset_id': obter_variavel('BIGQUERY_DATASET_ID_NIMBUS', obrigatoria=False, padrao='alertadb_cor_raw'),
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
        print("   BIGQUERY_DATASET_ID_NIMBUS=alertadb_cor_raw (opcional)")
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

def query_todos_dados_pluviometricos():
    """Retorna query para buscar TODOS os dados pluviométricos disponíveis no banco NIMBUS.
    
    Usa DISTINCT ON para garantir apenas um registro por (dia, estacao_id),
    mantendo o registro com o maior ID (mais recente), que é exatamente como
    está no banco alertadb.
    
    IMPORTANTE: A ordem do ORDER BY deve corresponder à ordem do DISTINCT ON,
    e depois ordenar por id DESC para pegar o registro mais recente.
    
    A coluna horaLeitura é TIMESTAMPTZ NOT NULL no NIMBUS, preservando o timezone original.
    O pandas/SQLAlchemy preserva automaticamente o timezone ao ler TIMESTAMPTZ.
    
    Esta é a MESMA query usada em carregar_pluviometricos_historicos.py e
    sincronizar_pluviometricos_novos.py para garantir consistência.
    """
    return """
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
ORDER BY el."horaLeitura" ASC, el.estacao_id ASC, el.id DESC;
"""

def obter_schema_pluviometricos():
    """Retorna schema do BigQuery para tabela pluviometricos."""
    return [
        bigquery.SchemaField("dia", "TIMESTAMP", mode="REQUIRED", description="Data e hora em horário local de São Paulo (America/Sao_Paulo). IMPORTANTE: O BigQuery não tem TIMESTAMPTZ, então este campo armazena o horário local de SP como se fosse UTC. O valor numérico corresponde ao horário local de SP (mesmo que dia_original). Exemplo: se dia_original é '1997-01-02 11:08:40.000 -0300', então dia é '1997-01-02 11:08:40' (horário local de SP, não UTC). Para ver o horário correto com offset, use dia_original."),
        bigquery.SchemaField("dia_original", "STRING", mode="NULLABLE", description="Data e hora no formato exato com timezone de SP (ex: 1997-01-02 11:08:40.000 -0300). Mesmo horário que dia, mas formatado como string com offset UTC. Use este campo para ver o horário correto com timezone."),
        bigquery.SchemaField("utc_offset", "STRING", mode="NULLABLE", description="Offset UTC do timezone de São Paulo (ex: -0300 para horário padrão do Brasil, -0200 para horário de verão). Use este campo para converter dia de horário local de SP para UTC se necessário."),
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
                    table.description = "Dados pluviométricos do NIMBUS (desde 1997)"
                    # Como dia é TIMESTAMP, usar particionamento por MÊS para evitar exceder limite de 10.000 partições
                    table.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.MONTH,
                        field="dia"  # Particionamento por coluna dia (TIMESTAMP) - agrupado por MÊS
                    )
                    table = client.create_table(table)
                    print(f"✅ Tabela '{table_id}' recriada com schema e particionamento por MÊS!")
                else:
                    # Tabela vazia, recriar com schema e particionamento
                    print(f"   📋 Tabela vazia sem schema. Recriando com schema e particionamento...")
                    client.delete_table(table_ref)
                    # Criar nova tabela com schema e particionamento
                    table = bigquery.Table(table_ref, schema=schema)
                    table.description = "Dados pluviométricos do NIMBUS (desde 1997)"
                    # Como dia é TIMESTAMP, usar particionamento por MÊS para evitar exceder limite de 10.000 partições
                    table.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.MONTH,
                        field="dia"  # Particionamento por coluna dia (TIMESTAMP) - agrupado por MÊS
                    )
                    table = client.create_table(table)
                    print(f"✅ Tabela '{table_id}' recriada com schema e particionamento por data de ingestão!")
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
                        # Criar nova tabela com particionamento por coluna dia (MÊS)
                        table = bigquery.Table(table_ref, schema=schema)
                        table.description = "Dados pluviométricos do NIMBUS (desde 1997)"
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
                        # Criar nova tabela com particionamento por coluna dia (MÊS)
                        table = bigquery.Table(table_ref, schema=schema)
                        table.description = "Dados pluviométricos do NIMBUS (desde 1997)"
                        table.time_partitioning = bigquery.TimePartitioning(
                            type_=bigquery.TimePartitioningType.MONTH,
                            field="dia"  # Particionamento por coluna dia (TIMESTAMP) - agrupado por MÊS
                        )
                        table = client.create_table(table)
                        print(f"✅ Tabela '{table_id}' recriada com particionamento por MÊS!")
                    else:
                        print(f"✅ Tabela '{table_id}' já existe com particionamento por coluna 'dia' (MÊS)!")
                elif not table.time_partitioning:
                    # BigQuery não permite converter tabela não particionada em particionada
                    # Se a tabela está vazia, podemos deletar e recriar com particionamento
                    if table.num_rows == 0:
                        print(f"   📋 Tabela existe mas sem particionamento e está vazia.")
                        print(f"   🔄 Recriando tabela com particionamento por MÊS...")
                        client.delete_table(table_ref)
                        # Criar nova tabela com schema e particionamento
                        table = bigquery.Table(table_ref, schema=schema)
                        table.description = "Dados pluviométricos do NIMBUS (desde 1997)"
                        # Como dia é TIMESTAMP, usar particionamento por MÊS para evitar exceder limite
                        table.time_partitioning = bigquery.TimePartitioning(
                            type_=bigquery.TimePartitioningType.MONTH,
                            field="dia"  # Particionamento por coluna dia (TIMESTAMP) - agrupado por MÊS
                        )
                        table = client.create_table(table)
                        print(f"✅ Tabela '{table_id}' recriada com particionamento por MÊS!")
                    else:
                        # Tabela tem dados, não podemos converter
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
                print(f"   📋 Criando tabela '{table_id}' com schema e particionamento por MÊS...")
            else:
                print(f"   ⚠️  Erro ao verificar tabela: {e}")
                raise
        
        # Criar tabela com schema e particionamento
        table = bigquery.Table(table_ref, schema=schema)
        # Descrição da tabela
        table.description = "Dados pluviométricos do NIMBUS (desde 1997)"
        # Como dia é TIMESTAMP, usar particionamento por MÊS para evitar exceder limite de 10.000 partições
        # Com dados desde 1997, particionamento por DIA excederia o limite (mais de 10.000 dias)
        # Particionamento por MÊS reduz para ~340 partições (desde 1997 até hoje)
        # Isso ainda melhora performance de queries e reduz custos
        # Conforme: https://docs.cloud.google.com/bigquery/docs/partitioned-tables?hl=pt-br
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
    chunksize = 5000  # Reduzido de 10000 para 5000 para evitar problemas de memória
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
    print(f"   💡 Query usa DISTINCT ON (mesma lógica dos scripts servidor166)\n")
    
    # Criar diretório temporário para múltiplos arquivos Parquet
    temp_dir = tempfile.mkdtemp()
    parquet_files = []
    
    import gc
    chunks_list = []
    batch_size = 2  # Reduzido de 4 para 2 para evitar problemas de memória
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
        
        # Processar todas as colunas: dia (TIMESTAMP), dia_original (STRING) e utc_offset (STRING)
        # IMPORTANTE: Converter para timezone de SP (America/Sao_Paulo) primeiro
        # para garantir que dia e dia_original tenham o mesmo horário local
        
        def converter_para_sp_e_processar(dt):
            """Converte timestamp para timezone de SP e retorna (dia_local, dia_original_str, utc_offset_str).
            
            IMPORTANTE: O BigQuery não tem TIMESTAMPTZ, apenas TIMESTAMP (que é armazenado em UTC).
            Para que dia e dia_original representem o mesmo horário local de SP, salvamos:
            - dia: horário local de SP (sem timezone) - o BigQuery vai interpretar como UTC, mas o valor
              numérico corresponde ao horário local de SP (ex: "1997-01-02 11:08:40")
            - dia_original: string com horário local de SP e offset (ex: "1997-01-02 11:08:40.000 -0300")
            
            Quando lido do BigQuery, dia aparecerá como se fosse UTC, mas na verdade é horário local de SP.
            Use dia_original para ver o horário correto com offset, ou converta dia usando utc_offset.
            
            Retorna uma tupla com:
            - dia_local: timestamp sem timezone (horário local de SP, não UTC)
            - dia_original_str: string formatada como "YYYY-MM-DD HH:MM:SS.mmm -0300" (horário local de SP)
            - utc_offset_str: string com offset UTC (ex: "-0300" ou "-0200")
            """
            if pd.isna(dt):
                return (None, None, None)
            try:
                # Converter para pandas Timestamp se necessário
                if isinstance(dt, str):
                    dt_parsed = pd.to_datetime(dt)
                elif isinstance(dt, pd.Timestamp):
                    dt_parsed = dt
                elif hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
                    dt_parsed = pd.Timestamp(dt)
                else:
                    dt_parsed = pd.to_datetime(dt)
                
                # Se tem timezone, converter para timezone de SP (America/Sao_Paulo)
                if isinstance(dt_parsed, pd.Timestamp) and dt_parsed.tz is not None:
                    # Converter para timezone de SP
                    dt_sp = dt_parsed.tz_convert('America/Sao_Paulo')
                else:
                    # Sem timezone, assumir que já está em UTC e converter para SP
                    dt_parsed = dt_parsed.tz_localize('UTC')
                    dt_sp = dt_parsed.tz_convert('America/Sao_Paulo')
                
                # Extrair offset UTC do timezone de SP
                offset = dt_sp.tz.utcoffset(dt_sp)
                if offset:
                    total_seconds = offset.total_seconds()
                    hours = int(total_seconds // 3600)
                    minutes = int((abs(total_seconds) % 3600) // 60)
                    # Formato: -0300 (sem dois pontos, como na NIMBUS)
                    utc_offset_str = f"{hours:+03d}{minutes:02d}"
                else:
                    utc_offset_str = "-0300"  # Padrão Brasil
                
                # Formatar dia_original como string: "YYYY-MM-DD HH:MM:SS.mmm -0300" (horário local de SP)
                timestamp_str = dt_sp.strftime('%Y-%m-%d %H:%M:%S')
                if dt_sp.microsecond:
                    # Pegar apenas os 3 primeiros dígitos dos microsegundos
                    microsec_str = str(dt_sp.microsecond)[:3].zfill(3)
                    timestamp_str += f".{microsec_str}"
                else:
                    timestamp_str += ".000"
                
                dia_original_str = f"{timestamp_str} {utc_offset_str}"
                
                # Salvar horário local de SP (sem timezone) - mesmo horário que dia_original
                # IMPORTANTE: O BigQuery vai interpretar como UTC, mas o valor é horário local de SP
                dia_local = dt_sp.tz_localize(None)  # Remover timezone, mantendo horário local de SP
                
                return (dia_local, dia_original_str, utc_offset_str)
            except Exception as e:
                print(f"      ⚠️  Erro ao processar timestamp: {e}")
                return (None, None, None)
        
        # Processar todos os timestamps de uma vez
        resultados = chunk_df['dia'].apply(converter_para_sp_e_processar)
        chunk_df['dia'] = resultados.apply(lambda x: x[0])
        chunk_df['dia_original'] = resultados.apply(lambda x: x[1])
        chunk_df['utc_offset'] = resultados.apply(lambda x: x[2])
        
        if 'estacao_id' in chunk_df.columns:
            chunk_df['estacao_id'] = chunk_df['estacao_id'].astype('Int64')
        
        # Converter colunas numéricas se existirem
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
        
        # IMPORTANTE: NÃO converter novamente para datetime aqui!
        # A função processar_dia_timestamp() já retorna o tipo correto (datetime sem timezone)
        # Converter novamente pode causar problemas de precisão (nanossegundos vs microssegundos)
        
        if len(chunk_df) > 0:
            chunks_list.append(chunk_df)
        total_registros += len(chunk_df)
        chunk_numero += 1
        
        # Escrever batch em arquivo Parquet (processar em lotes menores para evitar problemas de memória)
        if len(chunks_list) >= batch_size:
            try:
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
            except MemoryError:
                # Se houver erro de memória, tentar processar chunks individuais
                print(f"      ⚠️  Erro de memória ao concatenar batch. Processando chunks individuais...")
                for idx, single_chunk in enumerate(chunks_list):
                    single_file = Path(temp_dir) / f'{table_id}_batch_{batch_file_num:04d}_chunk_{idx}.parquet'
                    single_chunk.to_parquet(
                        single_file,
                        index=False,
                        engine='pyarrow',
                        compression='snappy',
                        coerce_timestamps='us'
                    )
                    parquet_files.append(single_file)
                chunks_list.clear()
                gc.collect()
                batch_file_num += 1
    
    # Escrever chunks restantes
    if chunks_list:
        try:
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
        except MemoryError:
            # Se houver erro de memória, processar chunks individuais
            print(f"      ⚠️  Erro de memória. Processando chunks restantes individualmente...")
            for idx, single_chunk in enumerate(chunks_list):
                single_chunk = single_chunk[single_chunk['dia'].notna()]
                if len(single_chunk) > 0:
                    single_file = Path(temp_dir) / f'{table_id}_batch_{batch_file_num:04d}_chunk_{idx}.parquet'
                    single_chunk.to_parquet(
                        single_file,
                        index=False,
                        engine='pyarrow',
                        compression='snappy',
                        coerce_timestamps='us'
                    )
                    parquet_files.append(single_file)
            chunks_list.clear()
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
        
        # Criar tabela pluviometricos
        print("\n📋 Criando/verificando tabela no BigQuery...")
        
        schema_pluviometricos = obter_schema_pluviometricos()
        criar_tabela_com_schema(
            client_bq, 
            BIGQUERY_CONFIG['dataset_id'], 
            'pluviometricos', 
            schema_pluviometricos
        )
        
        # Processar tabela pluviometricos
        print("\n" + "=" * 80)
        print("📊 PROCESSANDO TABELA: pluviometricos")
        print("=" * 80)
        
        query_pluviometricos = query_todos_dados_pluviometricos()
        total_pluviometricos = processar_e_carregar_tabela(
            engine_nimbus=engine_nimbus,
            client_bq=client_bq,
            dataset_id=BIGQUERY_CONFIG['dataset_id'],
            table_id='pluviometricos',
            schema=schema_pluviometricos,
            query=query_pluviometricos,
            descricao="pluviométricos"
        )
        
        # Resumo final
        print("\n" + "=" * 80)
        print("✅ EXPORTAÇÃO CONCLUÍDA")
        print("=" * 80)
        print(f"📊 pluviometricos: {total_pluviometricos:,} registros")
        
        return total_pluviometricos

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

