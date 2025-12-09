#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
📦 EXPORTAR TABELA PLUVIOMÉTRICOS PARA PARQUET

═══════════════════════════════════════════════════════════════════════════
🎯 PROPÓSITO DESTE SCRIPT:
═══════════════════════════════════════════════════════════════════════════

Este script exporta a tabela "pluviometricos" do banco alertadb_cor para 
arquivos Parquet. Os arquivos Parquet são eficientes, comprimidos e ideais 
para backup, transferência de dados ou análise offline.

═══════════════════════════════════════════════════════════════════════════
📋 O QUE ESTE SCRIPT FAZ:
═══════════════════════════════════════════════════════════════════════════

✅ Conecta ao banco alertadb_cor
✅ Exporta dados da tabela pluviometricos
✅ Salva em formato Parquet (comprimido e eficiente)
✅ Divide dados por ano (opcional) para facilitar gerenciamento
✅ Mostra progresso detalhado durante a exportação
✅ Exibe estatísticas finais (tamanho dos arquivos, total de registros)

═══════════════════════════════════════════════════════════════════════════
🚀 COMO USAR:
═══════════════════════════════════════════════════════════════════════════

1. Configure o arquivo .env com as credenciais:
   
   # Banco ORIGEM para EXPORTAÇÃO (alertadb_cor)
   DB_COPIA_ORIGEM_HOST=10.50.30.166
   DB_COPIA_ORIGEM_PORT=5432
   DB_COPIA_ORIGEM_NAME=alertadb_cor
   DB_COPIA_ORIGEM_USER=postgres
   DB_COPIA_ORIGEM_PASSWORD=

2. Execute: python scripts/exportar_pluviometricos_parquet.py

3. Os arquivos serão salvos em: exports/pluviometricos_YYYY.parquet

═══════════════════════════════════════════════════════════════════════════
📦 DEPENDÊNCIAS:
═══════════════════════════════════════════════════════════════════════════

pip install pandas pyarrow psycopg2-binary sqlalchemy python-dotenv

═══════════════════════════════════════════════════════════════════════════
"""

# 🔧 Importar bibliotecas necessárias
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import os
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
import sys
import warnings

# Suprimir warnings do pandas sobre DBAPI2
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')

# Carregar variáveis de ambiente
project_root = Path(__file__).parent.parent.parent
load_dotenv(dotenv_path=project_root / '.env')

def obter_variavel(nome, obrigatoria=True):
    """Obtém variável de ambiente, lança erro se obrigatória e não encontrada."""
    valor = os.getenv(nome)
    if obrigatoria and not valor:
        raise ValueError(f"❌ Variável de ambiente obrigatória não encontrada: {nome}")
    return valor

def carregar_configuracoes():
    """Carrega todas as configurações do arquivo .env."""
    try:
        # ⚙️ Configurações de conexão ORIGEM (alertadb_cor)
        origem = {
            'host': obter_variavel('DB_COPIA_ORIGEM_HOST'),
            'port': obter_variavel('DB_COPIA_ORIGEM_PORT', obrigatoria=False) or '5432',
            'dbname': obter_variavel('DB_COPIA_ORIGEM_NAME'),
            'user': obter_variavel('DB_COPIA_ORIGEM_USER'),
            'password': obter_variavel('DB_COPIA_ORIGEM_PASSWORD'),
            'connect_timeout': 10
        }
        
        # Criar string de conexão SQLAlchemy
        connection_string = f"postgresql://{origem['user']}:{origem['password']}@{origem['host']}:{origem['port']}/{origem['dbname']}"
        
        return origem, connection_string
    
    except ValueError as e:
        print("=" * 60)
        print("❌ ERRO DE CONFIGURAÇÃO")
        print("=" * 60)
        print(str(e))
        print("\n📝 Verifique se o arquivo .env existe e contém todas as variáveis necessárias")
        print("\n💡 Variáveis necessárias:")
        print("   - DB_COPIA_ORIGEM_HOST, DB_COPIA_ORIGEM_NAME, DB_COPIA_ORIGEM_USER, DB_COPIA_ORIGEM_PASSWORD")
        print("=" * 60)
        raise

# Carregar configurações
ORIGEM, CONNECTION_STRING = carregar_configuracoes()

def criar_diretorio_exports():
    """Cria o diretório exports se não existir."""
    exports_dir = project_root / 'exports'
    exports_dir.mkdir(exist_ok=True)
    return exports_dir

def testar_conexao():
    """Testa a conexão com o banco."""
    print("=" * 60)
    print("TESTE DE CONEXÃO")
    print("=" * 60)
    
    try:
        conn = psycopg2.connect(**ORIGEM)
        print(f"   ✅ CONEXÃO ({ORIGEM['host']}:{ORIGEM['port']}/{ORIGEM['dbname']}): SUCESSO!")
        conn.close()
        return True
        
    except Exception as e:
        print(f"   ❌ ERRO: {e}")
        return False

def verificar_tabela():
    """Verifica se a tabela existe e retorna estatísticas."""
    conn = None
    cur = None
    
    try:
        conn = psycopg2.connect(**ORIGEM)
        cur = conn.cursor()
        
        # Verificar se a tabela existe
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'pluviometricos'
            );
        """)
        existe = cur.fetchone()[0]
        
        if not existe:
            print("   ❌ ERRO: A tabela 'pluviometricos' não existe no banco!")
            return False, None
        
        # Contar registros
        cur.execute("SELECT COUNT(*) FROM pluviometricos;")
        total = cur.fetchone()[0]
        
        # Obter período dos dados
        cur.execute("SELECT MIN(dia), MAX(dia) FROM pluviometricos;")
        datas = cur.fetchone()
        data_min = datas[0] if datas[0] else None
        data_max = datas[1] if datas[1] else None
        
        print(f"   ✅ Tabela encontrada!")
        print(f"   📊 Total de registros: {total:,}")
        if data_min and data_max:
            print(f"   📅 Período: {data_min} até {data_max}")
        
        return True, {'total': total, 'data_min': data_min, 'data_max': data_max}
        
    except Exception as e:
        print(f"   ❌ Erro ao verificar tabela: {e}")
        return False, None
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def exportar_por_ano(engine, exports_dir):
    """Exporta dados divididos por ano."""
    print("\n" + "=" * 60)
    print("EXPORTANDO DADOS POR ANO")
    print("=" * 60)
    
    # Obter lista de anos disponíveis
    query_anos = """
        SELECT DISTINCT EXTRACT(YEAR FROM dia)::INTEGER as ano
        FROM pluviometricos
        ORDER BY ano;
    """
    df_anos = pd.read_sql_query(query_anos, engine)
    anos = df_anos['ano'].tolist()
    
    print(f"\n📅 Encontrados {len(anos)} anos: {anos}")
    print("   Exportando cada ano em um arquivo separado...\n")
    
    arquivos_criados = []
    total_registros = 0
    tamanho_total = 0
    
    for ano in anos:
        print(f"   📦 Exportando ano {ano}...")
        
        # Ler dados do ano
        query = f"""
        SELECT dia, m05, m10, m15, h01, h04, h24, h96, estacao, estacao_id
        FROM pluviometricos
        WHERE EXTRACT(YEAR FROM dia) = {ano}
        ORDER BY dia, estacao_id;
        """
        
        df = pd.read_sql_query(query, engine)
        
        if len(df) == 0:
            print(f"      ⚠️  Nenhum dado encontrado para {ano}")
            continue
        
        # Salvar em Parquet
        arquivo = exports_dir / f'pluviometricos_{ano}.parquet'
        df.to_parquet(arquivo, compression='snappy', index=False)
        
        tamanho_mb = arquivo.stat().st_size / (1024 * 1024)
        arquivos_criados.append(arquivo)
        total_registros += len(df)
        tamanho_total += tamanho_mb
        
        print(f"      ✅ {len(df):,} registros exportados → {arquivo.name} ({tamanho_mb:.2f} MB)")
    
    return arquivos_criados, total_registros, tamanho_total

def exportar_tudo(engine, exports_dir):
    """Exporta todos os dados em um único arquivo."""
    print("\n" + "=" * 60)
    print("EXPORTANDO TODOS OS DADOS")
    print("=" * 60)
    
    print("\n📥 Lendo dados do banco...")
    print("   Isso pode levar alguns minutos dependendo do volume...\n")
    
    inicio = datetime.now()
    
    # Ler todos os dados
    query = """
    SELECT dia, m05, m10, m15, h01, h04, h24, h96, estacao, estacao_id
    FROM pluviometricos
    ORDER BY dia, estacao_id;
    """
    
    # Usar chunksize para processar em lotes e não sobrecarregar memória
    chunksize = 100000
    arquivos_criados = []
    total_registros = 0
    chunk_numero = 1
    
    for chunk_df in pd.read_sql_query(query, engine, chunksize=chunksize):
        print(f"   📦 Processando chunk {chunk_numero} ({len(chunk_df):,} registros)...")
        
        # Se é o primeiro chunk, criar arquivo novo
        if chunk_numero == 1:
            arquivo = exports_dir / 'pluviometricos_completo.parquet'
            chunk_df.to_parquet(arquivo, compression='snappy', index=False, engine='pyarrow')
        else:
            # Para chunks subsequentes, ler arquivo existente, concatenar e salvar
            df_existente = pd.read_parquet(arquivo)
            df_combinado = pd.concat([df_existente, chunk_df], ignore_index=True)
            df_combinado.to_parquet(arquivo, compression='snappy', index=False, engine='pyarrow')
        
        total_registros += len(chunk_df)
        chunk_numero += 1
    
    tempo_decorrido = (datetime.now() - inicio).total_seconds()
    tamanho_mb = arquivo.stat().st_size / (1024 * 1024)
    arquivos_criados.append(arquivo)
    
    print(f"\n   ✅ Exportação concluída!")
    print(f"      Arquivo: {arquivo.name}")
    print(f"      Registros: {total_registros:,}")
    print(f"      Tamanho: {tamanho_mb:.2f} MB")
    print(f"      Tempo: {tempo_decorrido:.1f} segundos ({tempo_decorrido/60:.1f} minutos)")
    
    return arquivos_criados, total_registros, tamanho_mb

def main():
    """Função principal que executa a exportação."""
    print("=" * 60)
    print("📦 EXPORTAR TABELA PLUVIOMÉTRICOS PARA PARQUET")
    print("=" * 60)
    print()
    print("🎯 PROPÓSITO:")
    print("   Este script exporta a tabela 'pluviometricos' do banco alertadb_cor")
    print("   para arquivos Parquet (formato eficiente e comprimido).")
    print()
    print("📋 O QUE SERÁ FEITO:")
    print("   ✅ Verificar conexão com o banco")
    print("   ✅ Verificar se a tabela existe")
    print("   ✅ Exportar dados para arquivos Parquet")
    print("   ✅ Mostrar estatísticas finais")
    print()
    print("=" * 60)
    
    # Testar conexão
    if not testar_conexao():
        print("\n❌ Falha no teste de conexão. Abortando...")
        return
    
    # Verificar tabela
    print("\n📋 Verificando tabela...")
    existe, stats = verificar_tabela()
    if not existe:
        print("\n❌ Não foi possível continuar. Abortando...")
        return
    
    # Criar diretório de exports
    exports_dir = criar_diretorio_exports()
    print(f"\n📁 Diretório de exportação: {exports_dir}")
    
    # Perguntar se deseja dividir por ano
    print("\n❓ Como deseja exportar os dados?")
    print("   1. Dividir por ano (um arquivo por ano) - Recomendado para grandes volumes")
    print("   2. Um único arquivo (todos os dados)")
    
    opcao = input("\n   Escolha (1 ou 2): ").strip()
    
    # Criar engine SQLAlchemy (recomendado pelo pandas)
    engine = create_engine(CONNECTION_STRING, pool_pre_ping=True)
    
    try:
        if opcao == '1':
            arquivos, total, tamanho = exportar_por_ano(engine, exports_dir)
        else:
            arquivos, total, tamanho = exportar_tudo(engine, exports_dir)
        
        # Estatísticas finais
        print("\n" + "=" * 60)
        print("✅ EXPORTAÇÃO FINALIZADA COM SUCESSO!")
        print("=" * 60)
        print(f"📊 Total de registros exportados: {total:,}")
        print(f"📁 Arquivos criados: {len(arquivos)}")
        print(f"💾 Tamanho total: {tamanho:.2f} MB")
        print(f"📂 Localização: {exports_dir}")
        print("\n📋 Arquivos criados:")
        for arquivo in arquivos:
            tamanho_arquivo = arquivo.stat().st_size / (1024 * 1024)
            print(f"   • {arquivo.name} ({tamanho_arquivo:.2f} MB)")
        print("=" * 60)
        
    finally:
        engine.dispose()

if __name__ == "__main__":
    main()

