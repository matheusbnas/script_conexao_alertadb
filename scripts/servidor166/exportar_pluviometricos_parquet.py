#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
📦 EXPORTAR TABELA PLUVIOMÉTRICOS PARA PARQUET - BANCO LOCAL COR

═══════════════════════════════════════════════════════════════════════════
🎯 PROPÓSITO:
═══════════════════════════════════════════════════════════════════════════

Este script exporta dados da tabela 'pluviometricos' do banco local alertadb_cor
para arquivos Parquet, mantendo a estrutura original das colunas.

═══════════════════════════════════════════════════════════════════════════
📋 ESTRUTURA DAS COLUNAS ORIGINAIS:
═══════════════════════════════════════════════════════════════════════════
- dia: timestamp da leitura
- m05: precipitação 5 minutos
- m10: precipitação 10 minutos
- m15: precipitação 15 minutos
- h01: precipitação 1 hora
- h04: precipitação 4 horas
- h24: precipitação 24 horas
- h96: precipitação 96 horas (4 dias)
- estacao: nome da estação
- estacao_id: ID da estação

═══════════════════════════════════════════════════════════════════════════
📦 DEPENDÊNCIAS:
═══════════════════════════════════════════════════════════════════════════

pip install pandas pyarrow psycopg2-binary sqlalchemy python-dotenv

═══════════════════════════════════════════════════════════════════════════
"""

import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
import os
from datetime import datetime
from dotenv import load_dotenv
from urllib.parse import quote
import warnings
import pyarrow as pa
import pyarrow.parquet as pq

warnings.filterwarnings('ignore', category=UserWarning, module='pandas')

# Setup de caminhos
project_root = Path(__file__).parent.parent.parent
load_dotenv(dotenv_path=project_root / '.env')

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÕES E FUNÇÕES AUXILIARES
# ═══════════════════════════════════════════════════════════════════════════

def get_env(name, required=True):
    """Obtém variável de ambiente."""
    valor = os.getenv(name)
    if required and not valor:
        raise ValueError(f"❌ Variável obrigatória não encontrada: {name}")
    return valor

def get_config():
    """Carrega configurações do banco COR."""
    try:
        config = {
            'host': get_env('DB_COPIA_ORIGEM_HOST'),
            'port': get_env('DB_COPIA_ORIGEM_PORT', required=False) or '5432',
            'dbname': get_env('DB_COPIA_ORIGEM_NAME'),
            'user': get_env('DB_COPIA_ORIGEM_USER'),
            'password': get_env('DB_COPIA_ORIGEM_PASSWORD'),
            'connect_timeout': 10
        }
        
        user_enc = quote(config['user'], safe='')
        pass_enc = quote(config['password'], safe='')
        conn_str = f"postgresql://{user_enc}:{pass_enc}@{config['host']}:{config['port']}/{config['dbname']}"
        
        return config, conn_str
    except ValueError as e:
        print(f"❌ {e}")
        print("\nVariáveis esperadas no .env:")
        print("  - DB_COPIA_ORIGEM_HOST")
        print("  - DB_COPIA_ORIGEM_NAME")
        print("  - DB_COPIA_ORIGEM_USER")
        print("  - DB_COPIA_ORIGEM_PASSWORD")
        raise

ORIGEM, CONNECTION_STRING = get_config()

def test_connection(config):
    """Testa conexão com o banco."""
    print("=" * 60)
    print("TESTE DE CONEXÃO")
    print("=" * 60)
    try:
        conn = psycopg2.connect(**config)
        print(f"✅ Conectado a {config['host']}:{config['port']}/{config['dbname']}")
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Erro: {e}")
        return False

def check_table(config):
    """Verifica tabela e retorna estatísticas."""
    conn = cur = None
    try:
        conn = psycopg2.connect(**config)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT EXISTS (SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = 'pluviometricos')
        """)
        
        if not cur.fetchone()[0]:
            print("❌ Tabela 'pluviometricos' não encontrada!")
            return False, None
        
        cur.execute("SELECT COUNT(*) FROM pluviometricos")
        count = cur.fetchone()[0]
        
        cur.execute("SELECT MIN(dia), MAX(dia) FROM pluviometricos")
        date_min, date_max = cur.fetchone()
        
        print(f"✅ Tabela encontrada")
        print(f"   Registros: {count:,}")
        print(f"   Período: {date_min} até {date_max}")
        
        return True, {'count': count, 'min': date_min, 'max': date_max}
    except Exception as e:
        print(f"❌ Erro: {e}")
        return False, None
    finally:
        if cur: cur.close()
        if conn: conn.close()

def get_export_dir():
    """Cria e retorna diretório de exports."""
    d = project_root / 'exports'
    d.mkdir(exist_ok=True)
    return d

def ensure_timestamptz(df):
    """Garante que coluna 'dia' seja timestamptz no Parquet."""
    if 'dia' in df.columns:
        # Converter para datetime com UTC se necessário
        if not pd.api.types.is_datetime64_any_dtype(df['dia']):
            df['dia'] = pd.to_datetime(df['dia'], utc=True)
        elif df['dia'].dt.tz is None:
            # Se não tem timezone, assumir UTC
            df['dia'] = df['dia'].dt.tz_localize('UTC')
        else:
            # Se já tem timezone diferente de UTC, converter para UTC
            df['dia'] = df['dia'].dt.tz_convert('UTC')
    return df

def save_parquet_with_timestamptz(df, fpath):
    """Salva DataFrame em Parquet preservando timestamptz no dia."""
    # Garantir tipo correto
    df = ensure_timestamptz(df)
    
    # Converter para PyArrow com schema explícito para timestamptz
    # build schema: force 'dia' as timestamptz UTC, infer others from numpy dtype
    fields = []
    for name in df.columns:
        if name == 'dia':
            fields.append(pa.field('dia', pa.timestamp('ns', tz='UTC')))
        else:
            # use numpy dtype inference
            dtype = df[name].dtype
            try:
                arrow_type = pa.from_numpy_dtype(dtype)
            except Exception:
                arrow_type = None
            if arrow_type is None:
                # fallback to automatic inference via Table.from_pandas
                fields.append(pa.field(name, pa.null()))
            else:
                fields.append(pa.field(name, arrow_type))
    schema = pa.schema(fields)
    
    table = pa.Table.from_pandas(df, schema=schema)
    pq.write_table(table, fpath, compression='snappy')

# ═══════════════════════════════════════════════════════════════════════════
# FUNÇÕES DE EXPORTAÇÃO
# ═══════════════════════════════════════════════════════════════════════════

def export_by_year(engine, export_dir):
    """Exporta dividindo por ano."""
    print("\n" + "=" * 60)
    print("EXPORTANDO POR ANO")
    print("=" * 60)
    
    years_df = pd.read_sql_query(
        "SELECT DISTINCT EXTRACT(YEAR FROM dia)::INTEGER as ano FROM pluviometricos ORDER BY ano",
        engine
    )
    years = years_df['ano'].tolist()
    
    print(f"\n{len(years)} anos encontrados: {years}\n")
    
    files = []
    total_rows = 0
    total_size = 0
    
    for year in years:
        print(f"  {year}...", end=" ", flush=True)
        
        df = pd.read_sql_query(f"""
            SELECT dia::timestamptz AS dia, m05, m10, m15, h01, h04, h24, h96, estacao, estacao_id
            FROM pluviometricos
            WHERE EXTRACT(YEAR FROM dia) = {year}
            ORDER BY dia, estacao_id
        """, engine)
        
        if df.empty:
            print("sem dados")
            continue
        
        fpath = export_dir / f'pluviometricos_{year}.parquet'
        save_parquet_with_timestamptz(df, fpath)
        
        fsize = fpath.stat().st_size / (1024 * 1024)
        files.append(fpath)
        total_rows += len(df)
        total_size += fsize
        
        print(f"✅ {len(df):,} registros ({fsize:.2f} MB)")
    
    return files, total_rows, total_size

def export_interval(engine, export_dir, year_start, year_end):
    """Exporta intervalo de anos."""
    print("\n" + "=" * 60)
    print(f"EXPORTANDO {year_start} A {year_end}")
    print("=" * 60)
    
    if year_start > year_end:
        raise ValueError("Ano inicial deve ser ≤ ano final")
    
    print("\nLendo dados...", end=" ", flush=True)
    df = pd.read_sql_query(f"""
        SELECT dia::timestamptz AS dia, m05, m10, m15, h01, h04, h24, h96, estacao, estacao_id
        FROM pluviometricos
        WHERE EXTRACT(YEAR FROM dia) BETWEEN {year_start} AND {year_end}
        ORDER BY dia, estacao_id
    """, engine)
    
    if df.empty:
        print("nenhum dado encontrado")
        return [], 0, 0
    
    print(f"✅ {len(df):,} registros")
    
    fpath = export_dir / f'pluviometricos_{year_start}_{year_end}.parquet'
    print("Salvando...", end=" ", flush=True)
    save_parquet_with_timestamptz(df, fpath)
    
    fsize = fpath.stat().st_size / (1024 * 1024)
    print(f"✅ {fsize:.2f} MB")
    
    return [fpath], len(df), fsize

def export_all(engine, export_dir):
    """Exporta todos os dados em um arquivo."""
    print("\n" + "=" * 60)
    print("EXPORTANDO TODOS OS DADOS")
    print("=" * 60 + "\n")
    
    inicio = datetime.now()
    fpath = export_dir / 'pluviometricos_completo.parquet'
    chunk_num = 1
    total_rows = 0
    
    for i, chunk_df in enumerate(pd.read_sql_query("""
        SELECT dia::timestamptz AS dia, m05, m10, m15, h01, h04, h24, h96, estacao, estacao_id
        FROM pluviometricos ORDER BY dia, estacao_id
    """, engine, chunksize=100000), 1):
        
        print(f"  Chunk {i}: {len(chunk_df):,} registros...", end=" ", flush=True)
        
        if i == 1:
            save_parquet_with_timestamptz(chunk_df, fpath)
        else:
            df_old = pd.read_parquet(fpath)
            df_combined = pd.concat([df_old, chunk_df], ignore_index=True)
            save_parquet_with_timestamptz(df_combined, fpath)
        
        print("✅")
        total_rows += len(chunk_df)
    
    elapsed = (datetime.now() - inicio).total_seconds()
    fsize = fpath.stat().st_size / (1024 * 1024)
    
    print(f"\n✅ Concluído em {elapsed:.0f}s")
    print(f"   Registros: {total_rows:,}")
    print(f"   Tamanho: {fsize:.2f} MB")
    
    return [fpath], total_rows, fsize

# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("📦 EXPORTAR PLUVIOMÉTRICOS PARA PARQUET")
    print("   Banco: alertadb_cor (local)")
    print("=" * 60 + "\n")
    
    if not test_connection(ORIGEM):
        print("\n❌ Abortando...")
        return
    
    print("\n📋 Verificando tabela...")
    exists, stats = check_table(ORIGEM)
    if not exists:
        print("\n❌ Abortando...")
        return
    
    export_dir = get_export_dir()
    print(f"\n📁 Exports: {export_dir}")
    
    print("\n❓ Opção de exportação:")
    print("  1. Por ano (arquivo para cada ano)")
    print("  2. Arquivo único (todos os dados)")
    print("  3. Intervalo de anos")
    
    opt = input("\nEscolha (1-3): ").strip()
    
    engine = create_engine(CONNECTION_STRING, pool_pre_ping=True)
    
    try:
        if opt == '1':
            files, rows, size = export_by_year(engine, export_dir)
        elif opt == '3':
            while True:
                try:
                    y1 = int(input("\nAno inicial (YYYY): "))
                    y2 = int(input("Ano final   (YYYY): "))
                    if y1 <= y2:
                        break
                    print("Ano inicial deve ser ≤ ano final")
                except ValueError:
                    print("Digite anos válidos")
            files, rows, size = export_interval(engine, export_dir, y1, y2)
        else:
            files, rows, size = export_all(engine, export_dir)
        
        if files:
            print("\n" + "=" * 60)
            print("✅ EXPORTAÇÃO CONCLUÍDA!")
            print("=" * 60)
            print(f"Registros: {rows:,}")
            print(f"Arquivos: {len(files)}")
            print(f"Tamanho: {size:.2f} MB")
            print(f"\nArquivos criados:")
            for f in files:
                if f.exists():
                    sz = f.stat().st_size / (1024 * 1024)
                    print(f"  • {f.name} ({sz:.2f} MB)")
            print("=" * 60)
        else:
            print("\n⚠️  Nenhum arquivo criado")
    
    except Exception as e:
        print(f"\n❌ Erro: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        engine.dispose()

if __name__ == "__main__":
    main()

