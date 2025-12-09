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

def exportar_para_bigquery():
    """Exporta dados do NIMBUS diretamente para BigQuery."""
    conn_nimbus = None
    client_bq = None
    
    timestamp_atual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        print("\n🔄 Iniciando exportação direta NIMBUS → BigQuery...")
        print(f"   Origem: alertadb @ NIMBUS")
        print(f"   Destino: BigQuery ({BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_id']}.{BIGQUERY_CONFIG['table_id']})")
        print()
        
        # Conectar ao NIMBUS
        print("📦 Conectando ao NIMBUS...")
        conn_nimbus = psycopg2.connect(**ORIGEM)
        conn_nimbus.set_client_encoding('UTF8')
        
        # Conectar ao BigQuery
        print("📦 Conectando ao BigQuery...")
        credentials_path = BIGQUERY_CONFIG.get('credentials_path')
        
        if credentials_path and Path(credentials_path).exists():
            print(f"   🔑 Usando credenciais: {credentials_path}")
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path
            )
            client_bq = bigquery.Client(
                project=BIGQUERY_CONFIG['project_id'],
                credentials=credentials
            )
        elif credentials_path:
            print(f"   ⚠️  Arquivo de credenciais não encontrado: {credentials_path}")
            print("   💡 Tentando usar credenciais padrão do ambiente...")
            # Usar credenciais padrão do ambiente (gcloud auth application-default login)
            client_bq = bigquery.Client(project=BIGQUERY_CONFIG['project_id'])
        else:
            print("   🔑 Usando credenciais padrão do ambiente (gcloud auth)")
            # Usar credenciais padrão do ambiente (gcloud auth application-default login)
            client_bq = bigquery.Client(project=BIGQUERY_CONFIG['project_id'])
        
        # Criar dataset se não existir
        criar_dataset_se_nao_existir(client_bq, BIGQUERY_CONFIG['dataset_id'])
        
        # Buscar dados do NIMBUS
        query = query_todos_dados()
        print("📦 Buscando dados do NIMBUS...")
        print("   💡 Isso pode levar alguns minutos dependendo do volume de dados...")
        
        inicio_query = datetime.now()
        
        # Ler dados em chunks para não sobrecarregar memória
        chunksize = 100000
        total_registros = 0
        chunk_numero = 1
        
        # Schema do BigQuery
        schema = [
            bigquery.SchemaField("dia", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("m05", "NUMERIC", mode="NULLABLE"),
            bigquery.SchemaField("m10", "NUMERIC", mode="NULLABLE"),
            bigquery.SchemaField("m15", "NUMERIC", mode="NULLABLE"),
            bigquery.SchemaField("h01", "NUMERIC", mode="NULLABLE"),
            bigquery.SchemaField("h04", "NUMERIC", mode="NULLABLE"),
            bigquery.SchemaField("h24", "NUMERIC", mode="NULLABLE"),
            bigquery.SchemaField("h96", "NUMERIC", mode="NULLABLE"),
            bigquery.SchemaField("estacao", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("estacao_id", "INTEGER", mode="REQUIRED"),
        ]
        
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
        
        # Criar arquivo temporário Parquet
        temp_dir = tempfile.mkdtemp()
        temp_file = Path(temp_dir) / 'pluviometricos.parquet'
        
        # Ler dados em chunks e acumular em lista (mais eficiente)
        # Depois escrever tudo de uma vez no Parquet
        chunks_list = []
        for chunk_df in pd.read_sql(query, conn_nimbus, chunksize=chunksize):
            print(f"   📦 Processando chunk {chunk_numero} ({len(chunk_df):,} registros)...")
            
            # Renomear colunas para corresponder ao schema BigQuery
            # A query retorna "Dia" e "Estacao" (com aspas), mas precisamos "dia" e "estacao"
            chunk_df = chunk_df.rename(columns={
                'Dia': 'dia',
                'Estacao': 'estacao'
            })
            
            # Converter tipos preservando timezone
            chunk_df['dia'] = pd.to_datetime(chunk_df['dia'], utc=False)
            chunk_df['estacao_id'] = chunk_df['estacao_id'].astype('Int64')
            
            # Garantir que valores numéricos sejam do tipo correto
            colunas_numericas = ['m05', 'm10', 'm15', 'h01', 'h04', 'h24', 'h96']
            for col in colunas_numericas:
                chunk_df[col] = pd.to_numeric(chunk_df[col], errors='coerce')
            
            chunks_list.append(chunk_df)
            total_registros += len(chunk_df)
            chunk_numero += 1
        
        # Concatenar todos os chunks e escrever Parquet de uma vez (mais eficiente)
        print(f"\n   💾 Consolidando {len(chunks_list)} chunks em arquivo Parquet...")
        if chunks_list:
            df_completo = pd.concat(chunks_list, ignore_index=True)
            df_completo.to_parquet(temp_file, index=False, engine='pyarrow', compression='snappy')
            print(f"   ✅ Arquivo Parquet criado: {temp_file.stat().st_size / (1024*1024):.2f} MB")
        else:
            print("   ⚠️  Nenhum dado encontrado!")
            return 0
        
        tempo_query = (datetime.now() - inicio_query).total_seconds()
        print(f"\n   ✅ Dados processados: {total_registros:,} registros em {tempo_query:.1f} segundos")
        
        # Carregar no BigQuery
        print(f"\n📤 Carregando no BigQuery...")
        print(f"   Tabela: {BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_id']}.{BIGQUERY_CONFIG['table_id']}")
        
        inicio_carga = datetime.now()
        
        with open(temp_file, 'rb') as source_file:
            job = client_bq.load_table_from_file(
                source_file,
                table_ref,
                job_config=job_config
            )
        
        # Aguardar conclusão
        job.result()  # Aguarda conclusão do job
        
        tempo_carga = (datetime.now() - inicio_carga).total_seconds()
        
        # Limpar arquivo temporário
        temp_file.unlink()
        os.rmdir(temp_dir)
        
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
        if conn_nimbus:
            conn_nimbus.close()

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

