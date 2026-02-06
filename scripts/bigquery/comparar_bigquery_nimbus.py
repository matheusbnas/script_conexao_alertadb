#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para comparar diretamente os dados do BigQuery com os do NIMBUS
"""

import psycopg2
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv
from pathlib import Path
import os

# Carregar variáveis de ambiente
project_root = Path(__file__).parent.parent.parent
load_dotenv(dotenv_path=project_root / '.env')

def obter_variavel(nome, obrigatoria=True, default=None):
    """Obtém variável de ambiente."""
    valor = os.getenv(nome)
    if obrigatoria and not valor:
        if default:
            return default
        raise ValueError(f"❌ Variável de ambiente obrigatória não encontrada: {nome}")
    return valor or default

# Configurações NIMBUS
ORIGEM = {
    'host': obter_variavel('DB_ORIGEM_HOST'),
    'dbname': obter_variavel('DB_ORIGEM_NAME'),
    'user': obter_variavel('DB_ORIGEM_USER'),
    'password': obter_variavel('DB_ORIGEM_PASSWORD'),
    'sslmode': obter_variavel('DB_ORIGEM_SSLMODE', obrigatoria=False) or 'disable'
}

# Configurações BigQuery
BIGQUERY_PROJECT_ID = obter_variavel('BIGQUERY_PROJECT_ID')
BIGQUERY_DATASET_ID = obter_variavel('BIGQUERY_DATASET_ID', default='alertadb_cor_raw')
BIGQUERY_TABLE_ID = obter_variavel('BIGQUERY_TABLE_ID', default='pluviometricos')

def query_nimbus():
    """Query no NIMBUS usando DISTINCT ON (como o script usa)"""
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
WHERE el."horaLeitura" >= '2009-02-15 22:00:00.000' 
  AND el."horaLeitura" <= '2009-02-18 01:00:00.000' 
  AND el.estacao_id = 14
ORDER BY el."horaLeitura" ASC, el.estacao_id ASC, el.id DESC;
"""

def query_bigquery():
    """Query no BigQuery"""
    return f"""
SELECT 
    dia_utc,
    m05,
    m10,
    m15,
    h01,
    h04,
    h24,
    h96,
    estacao,
    estacao_id
FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID}`
WHERE dia_utc >= '2009-02-15 22:00:00.000' 
  AND dia_utc <= '2009-02-18 01:00:00.000' 
  AND estacao_id = 14
ORDER BY dia_utc DESC;
"""

def comparar_dados():
    """Compara dados do BigQuery com NIMBUS"""
    print("=" * 80)
    print("COMPARAÇÃO: BigQuery vs NIMBUS")
    print("=" * 80)
    
    # Buscar dados do NIMBUS
    print("\n1️⃣ Buscando dados do NIMBUS...")
    conn_nimbus = psycopg2.connect(**ORIGEM)
    df_nimbus = pd.read_sql(query_nimbus(), conn_nimbus)
    conn_nimbus.close()
    print(f"   ✅ {len(df_nimbus)} registros encontrados")
    
    # Buscar dados do BigQuery
    print("\n2️⃣ Buscando dados do BigQuery...")
    
    # Carregar credenciais
    credentials_path = project_root / 'credentials' / 'credentials.json'
    if credentials_path.exists():
        from google.oauth2 import service_account
        credentials = service_account.Credentials.from_service_account_file(
            str(credentials_path),
            scopes=["https://www.googleapis.com/auth/bigquery"]
        )
        client = bigquery.Client(credentials=credentials, project=BIGQUERY_PROJECT_ID)
    else:
        client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    
    df_bigquery = client.query(query_bigquery()).to_dataframe()
    print(f"   ✅ {len(df_bigquery)} registros encontrados")
    
    print("\n" + "=" * 80)
    print("ANÁLISE DE DIFERENÇAS")
    print("=" * 80)
    
    print(f"\n📊 Total de registros:")
    print(f"   NIMBUS: {len(df_nimbus)}")
    print(f"   BigQuery: {len(df_bigquery)}")
    print(f"   Diferença: {abs(len(df_nimbus) - len(df_bigquery))}")
    
    # Normalizar timestamps para comparação
    # NIMBUS retorna datetime com timezone (pode estar em UTC)
    # BigQuery retorna STRING no formato "2009-02-15 22:12:20.000 -0300"
    
    def normalizar_timestamp_nimbus(ts):
        """Normaliza timestamp do NIMBUS para formato comparável"""
        if pd.isna(ts):
            return None
        # Converter para string e depois para datetime para normalizar timezone
        ts_str = str(ts)
        try:
            # Se tem timezone, converter para timezone do Brasil
            if isinstance(ts, pd.Timestamp) and ts.tz is not None:
                # Converter para timezone do Brasil (-03:00)
                from datetime import timezone, timedelta
                tz_brasil = timezone(timedelta(hours=-3))
                ts_brasil = ts.astimezone(tz_brasil)
                # Formatar como string no formato do BigQuery
                return ts_brasil.strftime('%Y-%m-%d %H:%M:%S.000 -0300')
            else:
                # Sem timezone, assumir que já está no formato correto
                return ts_str
        except:
            return ts_str
    
    def normalizar_timestamp_bigquery(ts_str):
        """Normaliza timestamp do BigQuery para formato comparável"""
        if pd.isna(ts_str) or ts_str is None:
            return None
        # BigQuery já retorna como STRING no formato "2009-02-15 22:12:20.000 -0300"
        return str(ts_str).strip()
    
    # Normalizar timestamps
    df_nimbus['dia_normalizado'] = df_nimbus['Dia'].apply(normalizar_timestamp_nimbus)
    df_bigquery['dia_normalizado'] = df_bigquery['dia_utc'].apply(normalizar_timestamp_bigquery)
    
    # Criar dicionários para comparação usando timestamps normalizados
    dict_nimbus = {row['dia_normalizado']: row for _, row in df_nimbus.iterrows() if row['dia_normalizado']}
    dict_bigquery = {row['dia_normalizado']: row for _, row in df_bigquery.iterrows() if row['dia_normalizado']}
    
    # Comparar timestamps
    timestamps_nimbus = set(dict_nimbus.keys())
    timestamps_bigquery = set(dict_bigquery.keys())
    
    apenas_nimbus = timestamps_nimbus - timestamps_bigquery
    apenas_bigquery = timestamps_bigquery - timestamps_nimbus
    em_comum = timestamps_nimbus & timestamps_bigquery
    
    print(f"\n🔍 Análise de timestamps:")
    print(f"   Timestamps apenas no NIMBUS: {len(apenas_nimbus)}")
    print(f"   Timestamps apenas no BigQuery: {len(apenas_bigquery)}")
    print(f"   Timestamps em comum: {len(em_comum)}")
    
    if apenas_nimbus:
        print(f"\n⚠️  {len(apenas_nimbus)} registros no NIMBUS que NÃO estão no BigQuery:")
        for ts in sorted(apenas_nimbus)[:10]:
            row = dict_nimbus[ts]
            print(f"   - {ts}: h96={row['h96']}, h24={row['h24']}")
        if len(apenas_nimbus) > 10:
            print(f"   ... e mais {len(apenas_nimbus) - 10} registros")
    
    if apenas_bigquery:
        print(f"\n⚠️  {len(apenas_bigquery)} registros no BigQuery que NÃO estão no NIMBUS:")
        for ts in sorted(apenas_bigquery)[:10]:
            row = dict_bigquery[ts]
            print(f"   - {ts}: h96={row['h96']}, h24={row['h24']}")
        if len(apenas_bigquery) > 10:
            print(f"   ... e mais {len(apenas_bigquery) - 10} registros")
    
    # Comparar valores para timestamps em comum
    print(f"\n🔍 Comparando valores para {len(em_comum)} timestamps em comum...")
    diferencas = []
    
    for ts in em_comum:
        row_nimbus = dict_nimbus[ts]
        row_bq = dict_bigquery[ts]
        
        valores_diferentes = []
        for col_nimbus, col_bq in [('m05', 'm05'), ('m10', 'm10'), ('m15', 'm15'), 
                                    ('h01', 'h01'), ('h04', 'h04'), ('h24', 'h24'), ('h96', 'h96')]:
            val_nimbus = row_nimbus[col_nimbus]
            val_bq = row_bq[col_bq]
            
            # Comparar considerando None/NaN
            if pd.isna(val_nimbus) and pd.isna(val_bq):
                continue
            if val_nimbus != val_bq:
                valores_diferentes.append({
                    'coluna': col_nimbus,
                    'nimbus': val_nimbus,
                    'bigquery': val_bq
                })
        
        if valores_diferentes:
            diferencas.append({
                'timestamp': ts,
                'valores': valores_diferentes
            })
    
    if diferencas:
        print(f"\n⚠️  Encontradas {len(diferencas)} diferenças nos valores:")
        for diff in diferencas[:10]:
            print(f"\n   📅 Timestamp: {diff['timestamp']}")
            for val in diff['valores']:
                print(f"      {val['coluna']}: NIMBUS={val['nimbus']}, BigQuery={val['bigquery']}")
        if len(diferencas) > 10:
            print(f"\n   ... e mais {len(diferencas) - 10} diferenças")
    else:
        print("\n✅ Todos os valores são idênticos para os timestamps em comum!")
    
    print("\n" + "=" * 80)
    print("CONCLUSÃO")
    print("=" * 80)
    
    if len(df_nimbus) == len(df_bigquery) and not diferencas and not apenas_nimbus and not apenas_bigquery:
        print("✅ DADOS IDÊNTICOS ENTRE NIMBUS E BIGQUERY!")
        print("✅ Não há inconsistências encontradas")
    else:
        print("⚠️  INCONSISTÊNCIAS ENCONTRADAS:")
        if apenas_nimbus:
            print(f"   - {len(apenas_nimbus)} registros faltando no BigQuery")
        if apenas_bigquery:
            print(f"   - {len(apenas_bigquery)} registros extras no BigQuery")
        if diferencas:
            print(f"   - {len(diferencas)} registros com valores diferentes")
        
        print("\n💡 RECOMENDAÇÃO:")
        print("   Execute novamente: python scripts/bigquery/exportar_pluviometricos_nimbus_bigquery.py")
        print("   Isso irá recarregar todos os dados do NIMBUS para o BigQuery")

if __name__ == "__main__":
    comparar_dados()

