#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🌤️ EXPORTAÇÃO DIRETA - DADOS METEOROLÓGICOS NIMBUS → BigQuery

═══════════════════════════════════════════════════════════════════════════
🎯 PROPÓSITO DESTE SCRIPT:
═══════════════════════════════════════════════════════════════════════════

Este script exporta dados METEOROLÓGICOS diretamente do banco NIMBUS (alertadb) 
para o BigQuery, usando GROUP BY para agregar dados dos sensores.

ARQUITETURA:
    NIMBUS (alertadb) → Parquet → BigQuery
              ↑ [ESTE SCRIPT - DIRETO]

QUERY UTILIZADA:
    ✅ GROUP BY (l."horaLeitura", l.estacao_id, e.nome)
    ✅ ORDER BY l."horaLeitura"
    ✅ Agrega dados dos sensores usando MAX(CASE WHEN...)
    ✅ Query simplificada sem CTE ou DISTINCT ON

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
✅ Busca TODOS os dados meteorológicos usando GROUP BY
✅ Exporta para formato Parquet completo
✅ Carrega no BigQuery automaticamente
✅ Cria/atualiza tabela no BigQuery
✅ Processa em lotes de 10.000 registros para otimizar memória
✅ Usa TIMESTAMP para coluna dia_utc (UTC; igual convenção aos pluviométricos que usam dia em SP)
✅ Converte timezone para UTC (padrão BigQuery)
✅ Particionamento por coluna dia_utc em MÊS (como pluviométricos; horaLeitura → dia_utc) + clustering por estacao_id

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
from sqlalchemy.exc import OperationalError as SQLAlchemyOperationalError
from urllib.parse import quote_plus
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
import tempfile
import time

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

def query_dados_meteorologicos_por_periodo(data_inicio, data_fim):
    """Retorna query para buscar dados meteorológicos em um período específico.
    
    Query simplificada usando GROUP BY para agregar dados dos sensores
    por (horaLeitura, estacao_id, nome_estacao).

    A coluna horaLeitura é TIMESTAMPTZ NOT NULL no NIMBUS, preservando o timezone original.

    Args:
        data_inicio: Data de início do período (formato: 'YYYY-MM-DD')
        data_fim: Data de fim do período (formato: 'YYYY-MM-DD', exclusivo)
    """
    return f"""
SELECT
    l."horaLeitura" AS data_hora,  -- simples e já com timezone correto
    l.estacao_id AS id_estacao,
    e.nome       AS nome_estacao,
    MAX(
        CASE
            -- Até jan/2020 todas as estações usam coleta de 15 min (m15).
            WHEN l."horaLeitura" < TIMESTAMPTZ '2020-02-01 00:00:00+00' THEN elc.m15
            -- Guaratiba/São Cristóvão iniciam coleta de 5 min em mar/2020.
            WHEN (
                e.nome ILIKE 'Guaratiba%%'
                OR e.nome ILIKE 'Sao Cristovao%%'
                OR e.nome ILIKE 'São Cristóvão%%'
            ) AND l."horaLeitura" < TIMESTAMPTZ '2020-03-01 00:00:00+00' THEN elc.m15
            -- Demais casos já em 5 min.
            ELSE elc.m05
        END
    ) AS chuva,
    MAX(CASE WHEN s.nome = 'Direcao do Vento'      THEN ls.valor END) AS dirVento,
    MAX(CASE WHEN s.nome = 'Velocidade do Vento'   THEN ls.valor END) AS velVento,
    MAX(CASE WHEN s.nome = 'Temperatuda do Ar'     THEN ls.valor END) AS temperatura,
    MAX(CASE WHEN s.nome = 'Pressao ATM'           THEN ls.valor END) AS pressao,
    MAX(CASE WHEN s.nome = 'Umidade do Ar'         THEN ls.valor END) AS umidade,
    CASE
        WHEN MAX(CASE WHEN s.nome = 'Temperatuda do Ar' THEN ls.valor END) IS NULL
          OR MAX(CASE WHEN s.nome = 'Umidade do Ar' THEN ls.valor END) IS NULL
          OR MAX(CASE WHEN s.nome = 'Umidade do Ar' THEN ls.valor END) <= 0
        THEN NULL
        ELSE (
            243.04 * (
                LN(MAX(CASE WHEN s.nome = 'Umidade do Ar' THEN ls.valor END) / 100.0) +
                (
                    17.625 * MAX(CASE WHEN s.nome = 'Temperatuda do Ar' THEN ls.valor END)
                ) / (
                    243.04 + MAX(CASE WHEN s.nome = 'Temperatuda do Ar' THEN ls.valor END)
                )
            )
        ) / (
            17.625 - (
                LN(MAX(CASE WHEN s.nome = 'Umidade do Ar' THEN ls.valor END) / 100.0) +
                (
                    17.625 * MAX(CASE WHEN s.nome = 'Temperatuda do Ar' THEN ls.valor END)
                ) / (
                    243.04 + MAX(CASE WHEN s.nome = 'Temperatuda do Ar' THEN ls.valor END)
                )
            )
        )
    END AS ponto_orvalho

FROM public.estacoes_leiturasensor ls
JOIN public.estacoes_leitura l
      ON ls.leitura_id = l.id
LEFT JOIN public.estacoes_leiturachuva elc
      ON elc.leitura_id = l.id
JOIN public.estacoes_sensor s
      ON ls.sensor_id = s.id
JOIN public.estacoes_estacao e
      ON e.id = l.estacao_id

WHERE l."horaLeitura" >= '{data_inicio}'
  AND l."horaLeitura" < '{data_fim}'
  AND e.id IN (1,11,16,19,20,22,28,32)

GROUP BY
    l."horaLeitura",
    l.estacao_id,
    e.nome

ORDER BY l."horaLeitura";
"""

def gerar_periodos_anuais():
    """Gera lista de períodos anuais desde 1997 até o ano atual."""
    from datetime import date
    periodos = []
    ano_atual = date.today().year
    ano_inicio = 1997
    
    for ano in range(ano_inicio, ano_atual + 1):
        data_inicio = f"{ano}-01-01"
        data_fim = f"{ano + 1}-01-01"
        periodos.append((ano, data_inicio, data_fim))
    
    return periodos

def obter_schema_meteorologicos():
    """Retorna schema do BigQuery para tabela meteorologicos."""
    return [
        bigquery.SchemaField("dia_utc", "TIMESTAMP", mode="REQUIRED", description="Data e hora da medição em UTC. Origem: TIMESTAMPTZ NOT NULL do NIMBUS convertido para UTC. O sufixo _utc deixa explícita a origem do fuso horário."),
        bigquery.SchemaField("dia", "DATETIME", mode="NULLABLE", description="Data e hora local de São Paulo (America/Sao_Paulo), sem timezone, para leitura operacional. Derivada de dia_utc convertido para o fuso de SP."),
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
        bigquery.SchemaField("ponto_orvalho", "FLOAT64", mode="NULLABLE", description="Ponto de orvalho (°C), calculado por Magnus-Tetens a partir de temperatura e umidade relativa"),
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

def _aplicar_partitionamento_tabela_meteorologicos(table):
    """Mesma convenção que pluviométricos: horaLeitura → dia_utc (UTC) no Parquet; BQ particiona por mês em dia_utc.

    Clustering por estacao_id (id da estação; na query SQL costuma aparecer como id_estacao / nome_estacao)
    para otimizar filtros por estação dentro de cada partição temporal.
    """
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.MONTH,
        field="dia_utc",
    )
    table.clustering_fields = ["estacao_id"]


def _tabela_meteo_clustering_ok(table):
    return list(table.clustering_fields or []) == ["estacao_id"]


def _detectar_incompatibilidades_schema(schema_atual, schema_esperado):
    """Retorna lista de incompatibilidades de tipo entre schema atual e esperado."""
    esperado_por_nome = {f.name.lower(): f for f in schema_esperado}
    incompatibilidades = []
    for campo_atual in schema_atual:
        nome = campo_atual.name.lower()
        if nome not in esperado_por_nome:
            continue
        tipo_esperado = esperado_por_nome[nome].field_type.upper()
        tipo_atual = campo_atual.field_type.upper()
        if tipo_atual != tipo_esperado:
            incompatibilidades.append((campo_atual.name, tipo_atual, tipo_esperado))
    return incompatibilidades


def criar_tabela_com_schema(client, dataset_id, table_id, schema):
    """Cria tabela no BigQuery com schema, particionamento por dia_utc (MÊS) e clustering por estacao_id."""
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
                    _aplicar_partitionamento_tabela_meteorologicos(table)
                    table = client.create_table(table)
                    print(f"✅ Tabela '{table_id}' recriada com schema, particionamento (dia_utc MÊS) e clustering (estacao_id)!")
                else:
                    print(f"   📋 Tabela vazia sem schema. Recriando com schema e particionamento...")
                    client.delete_table(table_ref)
                    table = bigquery.Table(table_ref, schema=schema)
                    table.description = "Dados meteorológicos do NIMBUS (desde 1997)"
                    _aplicar_partitionamento_tabela_meteorologicos(table)
                    table = client.create_table(table)
                    print(f"✅ Tabela '{table_id}' recriada com schema, particionamento (dia_utc MÊS) e clustering (estacao_id)!")
                return True
            else:
                incompatibilidades_schema = _detectar_incompatibilidades_schema(table.schema, schema)
                if incompatibilidades_schema:
                    print(f"   ⚠️  Tabela '{table_id}' com schema incompatível.")
                    for nome, atual, esperado in incompatibilidades_schema:
                        print(f"      - Campo '{nome}': atual={atual}, esperado={esperado}")
                    print("   🔄 Recriando tabela para alinhar tipos e evitar erro de carga no BigQuery...")
                    client.delete_table(table_ref)
                    table = bigquery.Table(table_ref, schema=schema)
                    table.description = "Dados meteorológicos do NIMBUS (desde 1997)"
                    _aplicar_partitionamento_tabela_meteorologicos(table)
                    table = client.create_table(table)
                    print(f"✅ Tabela '{table_id}' recriada com schema atualizado, particionamento e clustering!")
                    return True

                # Verificar se já tem particionamento
                # Verificar se o particionamento está correto (por coluna dia_utc, tipo MONTH)
                if table.time_partitioning and table.time_partitioning.field:
                    # Verificar se está usando DAY (precisa mudar para MONTH) ou se já está MONTH
                    if table.time_partitioning.field != "dia_utc":
                        print(f"   ⚠️  Tabela '{table_id}' existe com particionamento por campo '{table.time_partitioning.field}'.")
                        print(f"   💡 Precisamos recriar a tabela com particionamento por 'dia_utc' (MÊS).")
                        print(f"   🔄 Deletando tabela para recriar com particionamento correto...")
                        client.delete_table(table_ref)
                        table = bigquery.Table(table_ref, schema=schema)
                        table.description = "Dados meteorológicos do NIMBUS (desde 1997)"
                        _aplicar_partitionamento_tabela_meteorologicos(table)
                        table = client.create_table(table)
                        print(f"✅ Tabela '{table_id}' recriada com particionamento por coluna 'dia_utc' (MÊS) e clustering (estacao_id)!")
                    elif table.time_partitioning.type_ != bigquery.TimePartitioningType.MONTH:
                        # Tabela tem particionamento por DIA, mas precisa ser MONTH
                        print(f"   ⚠️  Tabela '{table_id}' existe com particionamento por DIA.")
                        print(f"   ⚠️  Isso pode exceder o limite de 10.000 partições!")
                        print(f"   💡 Precisamos recriar a tabela com particionamento por MÊS.")
                        print(f"   🔄 Deletando tabela para recriar com particionamento por MÊS...")
                        client.delete_table(table_ref)
                        table = bigquery.Table(table_ref, schema=schema)
                        table.description = "Dados meteorológicos do NIMBUS (desde 1997)"
                        _aplicar_partitionamento_tabela_meteorologicos(table)
                        table = client.create_table(table)
                        print(f"✅ Tabela '{table_id}' recriada com particionamento por MÊS e clustering (estacao_id)!")
                    else:
                        if not _tabela_meteo_clustering_ok(table):
                            if table.num_rows == 0:
                                print(f"   📋 Tabela vazia sem clustering em estacao_id. Recriando...")
                                client.delete_table(table_ref)
                                table = bigquery.Table(table_ref, schema=schema)
                                table.description = "Dados meteorológicos do NIMBUS (desde 1997)"
                                _aplicar_partitionamento_tabela_meteorologicos(table)
                                table = client.create_table(table)
                                print(f"✅ Tabela '{table_id}' recriada com particionamento (MÊS) e clustering (estacao_id)!")
                            else:
                                print(f"   ⚠️  Tabela sem clustering em estacao_id ({table.num_rows:,} linhas).")
                                print(f"   💡 Para aplicar: ALTER TABLE ... SET OPTIONS (clustering_fields = ['estacao_id']) ou recarga completa.")
                        else:
                            print(f"✅ Tabela '{table_id}' já existe com particionamento por coluna 'dia_utc' (MÊS) e clustering (estacao_id)!")
                elif not table.time_partitioning:
                    if table.num_rows == 0:
                        print(f"   📋 Tabela existe mas sem particionamento e está vazia.")
                        print(f"   🔄 Recriando tabela com particionamento por MÊS...")
                        client.delete_table(table_ref)
                        table = bigquery.Table(table_ref, schema=schema)
                        table.description = "Dados meteorológicos do NIMBUS (desde 1997)"
                        _aplicar_partitionamento_tabela_meteorologicos(table)
                        table = client.create_table(table)
                        print(f"✅ Tabela '{table_id}' recriada com particionamento por MÊS e clustering (estacao_id)!")
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
        # Como dia_utc é TIMESTAMP, usar particionamento por MÊS (igual pluviométricos); clustering por estacao_id.
        _aplicar_partitionamento_tabela_meteorologicos(table)
        table = client.create_table(table, exists_ok=False)
        print(f"✅ Tabela '{table_id}' criada com schema, particionamento por MÊS (dia_utc) e clustering (estacao_id)!")
        print(f"   💡 Particionamento e clustering melhoram performance de queries e reduzem custos")
        return True
    except Exception as e:
        print(f"⚠️  Erro ao criar/atualizar tabela: {e}")
        import traceback
        traceback.print_exc()
        return False

def processar_e_carregar_tabela_por_periodo(engine_nimbus, client_bq, dataset_id, table_id, schema, query, descricao, write_truncate=False):
    """Processa e carrega dados de um período específico no BigQuery.
    
    Similar a processar_e_carregar_tabela, mas permite controlar write_disposition.
    """
    inicio_query = datetime.now()
    chunksize = 5000
    total_registros = 0
    chunk_numero = 1
    
    table_ref = client_bq.dataset(dataset_id).table(table_id)
    
    # Criar diretório temporário para múltiplos arquivos Parquet
    temp_dir = tempfile.mkdtemp()
    parquet_files = []
    
    import gc
    chunks_list = []
    batch_size = 2
    batch_file_num = 1
    
    # Função auxiliar para ler chunk com retry
    def ler_chunks_com_retry(query, engine_ref, chunksize, max_retries=3):
        """Lê chunks com retry automático em caso de erro de conexão."""
        tentativa_global = 0
        
        while tentativa_global < max_retries:
            try:
                chunk_iterator = pd.read_sql(query, engine_ref['engine'], chunksize=chunksize)
                for chunk_df in chunk_iterator:
                    yield chunk_df
                return
            except (psycopg2.OperationalError, psycopg2.InterfaceError,
                    SQLAlchemyOperationalError) as e:
                tentativa_global += 1
                if tentativa_global < max_retries:
                    wait_time = tentativa_global * 5
                    print(f"      ⚠️  Erro de conexão (tentativa {tentativa_global}/{max_retries}): {str(e)[:100]}")
                    print(f"      🔄 Reconectando em {wait_time}s...")
                    time.sleep(wait_time)
                    try:
                        engine_ref['engine'].dispose()
                    except:
                        pass
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
    
    engine_ref = {'engine': engine_nimbus}
    
    # Ler chunks com retry automático
    for chunk_df in ler_chunks_com_retry(query, engine_ref, chunksize):
        if chunk_df.empty:
            continue
        
        # Renomear colunas
        chunk_df = chunk_df.rename(columns={
            'data_hora': 'dia_utc',
            'id_estacao': 'estacao_id',
            'nome_estacao': 'estacao'
        })
        
        # Remover colunas auxiliares
        for col in ['TimezoneOffset', 'leitura_id']:
            if col in chunk_df.columns:
                chunk_df = chunk_df.drop(columns=[col])
        
        # Processar coluna dia_utc (mesmas funções da função original)
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
        
        # Processar colunas
        chunk_df['dia_original'] = chunk_df['dia_utc'].apply(formatar_dia_original)
        chunk_df['utc_offset'] = chunk_df['dia_utc'].apply(extrair_utc_offset)
        chunk_df['dia_utc'] = chunk_df['dia_utc'].apply(processar_dia_timestamp)
        
        if 'estacao_id' in chunk_df.columns:
            chunk_df['estacao_id'] = chunk_df['estacao_id'].astype('Int64')
        
        colunas_numericas = ['chuva', 'dirVento', 'velVento', 'temperatura', 'pressao', 'umidade', 'ponto_orvalho']
        colunas_numericas_lower = {c.lower() for c in colunas_numericas}
        for col in colunas_numericas:
            if col in chunk_df.columns:
                # Converter para float64 explicitamente
                # Primeiro converter para numérico, depois forçar float64
                chunk_df[col] = pd.to_numeric(chunk_df[col], errors='coerce')
                # Forçar explicitamente float64 (evita inferência como INT32 pelo pyarrow)
                # Usar astype('float64') que garante o tipo mesmo com valores inteiros
                chunk_df[col] = chunk_df[col].astype('float64')
        # PostgreSQL reduz aliases sem aspas para minúsculas (ex.: dirVento -> dirvento).
        # Garantir coerção também nesses casos para evitar INT32 no Parquet.
        for col in chunk_df.columns:
            if col.lower() in colunas_numericas_lower:
                chunk_df[col] = pd.to_numeric(chunk_df[col], errors='coerce').astype('float64')
        
        chunk_df = chunk_df[chunk_df['dia_utc'].notna()]
        
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
            # Garantir que todas as colunas numéricas sejam float64 antes de salvar
            for col in colunas_numericas:
                if col in df_batch.columns:
                    # Forçar float64 explicitamente
                    df_batch[col] = df_batch[col].astype('float64')
            for col in df_batch.columns:
                if col.lower() in colunas_numericas_lower:
                    df_batch[col] = pd.to_numeric(df_batch[col], errors='coerce').astype('float64')
            batch_file = Path(temp_dir) / f'{table_id}_batch_{batch_file_num:04d}.parquet'
            # Usar schema explícito do pyarrow para garantir tipos corretos (float64)
            import pyarrow as pa
            import pyarrow.parquet as pq
            table = pa.Table.from_pandas(df_batch)
            # Garantir que colunas numéricas sejam double (float64) preservando nomes exatos
            pa_schema = table.schema
            new_fields = []
            for field in pa_schema:
                if field.name.lower() in colunas_numericas_lower:
                    # Forçar tipo double (float64) preservando o nome exato da coluna
                    new_fields.append(pa.field(field.name, pa.float64(), nullable=True))
                else:
                    new_fields.append(field)
            new_schema = pa.schema(new_fields)
            table = table.cast(new_schema)
            pq.write_table(table, batch_file, compression='snappy', coerce_timestamps='us', use_deprecated_int96_timestamps=False)
            parquet_files.append(batch_file)
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
            # Garantir que todas as colunas numéricas sejam float64 antes de salvar
            colunas_numericas = ['chuva', 'dirVento', 'velVento', 'temperatura', 'pressao', 'umidade', 'ponto_orvalho']
            colunas_numericas_lower = {c.lower() for c in colunas_numericas}
            for col in colunas_numericas:
                if col in df_batch.columns:
                    df_batch[col] = df_batch[col].astype('float64')
            for col in df_batch.columns:
                if col.lower() in colunas_numericas_lower:
                    df_batch[col] = pd.to_numeric(df_batch[col], errors='coerce').astype('float64')
            batch_file = Path(temp_dir) / f'{table_id}_batch_{batch_file_num:04d}.parquet'
            # Usar schema explícito do pyarrow para garantir tipos corretos (float64)
            import pyarrow as pa
            import pyarrow.parquet as pq
            table = pa.Table.from_pandas(df_batch)
            # Garantir que colunas numéricas sejam double (float64) preservando nomes exatos
            pa_schema = table.schema
            new_fields = []
            for field in pa_schema:
                if field.name.lower() in colunas_numericas_lower:
                    # Forçar tipo double (float64) preservando o nome exato da coluna
                    new_fields.append(pa.field(field.name, pa.float64(), nullable=True))
                else:
                    new_fields.append(field)
            new_schema = pa.schema(new_fields)
            table = table.cast(new_schema)
            pq.write_table(table, batch_file, compression='snappy', coerce_timestamps='us', use_deprecated_int96_timestamps=False)
            parquet_files.append(batch_file)
            del df_batch
            gc.collect()
    
    if total_registros == 0:
        return 0
    
    # Carregar arquivos Parquet no BigQuery
    inicio_carga = datetime.now()
    
    for i, parquet_file in enumerate(parquet_files, 1):
        # IMPORTANTE: fixar schema explicitamente evita drift de tipos (ex.: INTEGER vs FLOAT)
        # quando um Parquet anual contém apenas valores inteiros em colunas numéricas.
        file_job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE if (write_truncate and i == 1) else bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.PARQUET,
        )
        
        with open(parquet_file, 'rb') as source_file:
            job = client_bq.load_table_from_file(
                source_file,
                table_ref,
                job_config=file_job_config
            )
            job.result()
    
    tempo_carga = (datetime.now() - inicio_carga).total_seconds()
    
    # Limpar arquivos temporários
    for parquet_file in parquet_files:
        try:
            parquet_file.unlink()
        except Exception:
            pass
    try:
        os.rmdir(temp_dir)
    except Exception:
        pass
    
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
        print("   🔄 Configurações otimizadas para queries longas (keepalives, timeout estendido)")
        user_encoded = quote_plus(ORIGEM['user'])
        password_encoded = quote_plus(ORIGEM['password'])
        connection_string = (
            f"postgresql://{user_encoded}:{password_encoded}@"
            f"{ORIGEM['host']}:{ORIGEM['port']}/{ORIGEM['dbname']}"
        )
        engine_nimbus = create_engine(
            connection_string,
            connect_args={
                'client_encoding': 'UTF8',
                'connect_timeout': 30,
                'keepalives': 1,
                'keepalives_idle': 30,
                'keepalives_interval': 10,
                'keepalives_count': 5,
                'options': '-c statement_timeout=0'  # Desabilita timeout de statement
            },
            pool_pre_ping=True,
            pool_recycle=3600,
            pool_size=5,
            max_overflow=10
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
        
        # Processar tabela meteorologicos por períodos anuais
        print("\n" + "=" * 80)
        print("🌤️ PROCESSANDO TABELA: meteorologicos")
        print("=" * 80)
        print("   💡 Processando por períodos anuais para evitar timeout do servidor")
        print("   💡 Removendo duplicatas por (dia_utc, estacao_id) para garantir unicidade")
        
        periodos = gerar_periodos_anuais()
        total_meteorologicos = 0
        total_periodos = len(periodos)
        
        # Configurar write_disposition apenas no primeiro período (WRITE_TRUNCATE)
        # Depois usar WRITE_APPEND
        table_ref = client_bq.dataset(BIGQUERY_CONFIG['dataset_id']).table(BIGQUERY_CONFIG['table_id'])
        
        for idx, (ano, data_inicio, data_fim) in enumerate(periodos, 1):
            print(f"\n📅 Processando ano {ano} ({idx}/{total_periodos})...")
            print(f"   Período: {data_inicio} até {data_fim}")
            
            query_periodo = query_dados_meteorologicos_por_periodo(data_inicio, data_fim)
            
            # Processar período
            registros_periodo = processar_e_carregar_tabela_por_periodo(
                engine_nimbus=engine_nimbus,
                client_bq=client_bq,
                dataset_id=BIGQUERY_CONFIG['dataset_id'],
                table_id=BIGQUERY_CONFIG['table_id'],
                schema=schema_meteorologicos,
                query=query_periodo,
                descricao=f"meteorológicos ({ano})",
                write_truncate=(idx == 1)  # Apenas no primeiro período
            )
            
            total_meteorologicos += registros_periodo
            print(f"   ✅ Ano {ano} processado: {registros_periodo:,} registros")
        
        # Resumo final
        print("\n" + "=" * 80)
        print("✅ EXPORTAÇÃO CONCLUÍDA")
        print("=" * 80)
        print(f"🌤️ meteorologicos: {total_meteorologicos:,} registros totais")
        print(f"   Processados {total_periodos} períodos anuais")
        
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
    print("   ✅ Usar GROUP BY para agregar dados dos sensores")
    print("   ✅ Criar dataset/tabela no BigQuery se não existir")
    print("   ✅ Exportar em formato Parquet completo")
    print("   ✅ Carregar no BigQuery automaticamente")
    print()
    print("⚠️  IMPORTANTE:")
    print("   - Requer credenciais do GCP configuradas")
    print("   - Formato: Parquet (mais eficiente para BigQuery)")
    print("   - Exportação completa: todos os dados")
    print("   - Query usa GROUP BY para agregar dados dos sensores")
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

