#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🌧️ ETL PLUVIOMÉTRICOS
Extrai dados do banco origem → Replica diretamente para destino
"""

import psycopg2
from psycopg2.extras import execute_values
import os
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

# Carregar .env (procura na raiz do projeto e no diretório setup)
project_root = Path(__file__).parent.parent
setup_dir = Path(__file__).parent
load_dotenv(dotenv_path=project_root / '.env')
load_dotenv(dotenv_path=setup_dir / '.env', override=False)

# ========================================
# CONFIGURAÇÕES
# ========================================

def obter_variavel(nome, obrigatoria=True, padrao=None):
    """Obtém variável de ambiente com validação"""
    valor = os.getenv(nome, padrao)
    if obrigatoria and not valor:
        raise ValueError(f"❌ Variável de ambiente obrigatória não encontrada: {nome}. Verifique o arquivo .env")
    return valor

def validar_configuracao(nome, config):
    """Valida se todas as configurações necessárias estão definidas"""
    campos_obrigatorios = ['host', 'dbname', 'user', 'password']
    campos_faltando = [campo for campo in campos_obrigatorios if not config.get(campo)]
    
    if campos_faltando:
        raise ValueError(
            f"❌ Configuração {nome} incompleta! "
            f"Variáveis faltando: {', '.join(campos_faltando)}. "
            f"Verifique o arquivo .env"
        )
    return config

# Configurações ORIGEM (suporta ambos os padrões: DB_ORIGEM_* e ORIGEM_*)
ORIGEM = {
    'host': os.getenv('DB_ORIGEM_HOST') or os.getenv('ORIGEM_HOST'),
    'dbname': os.getenv('DB_ORIGEM_NAME') or os.getenv('ORIGEM_DBNAME'),
    'user': os.getenv('DB_ORIGEM_USER') or os.getenv('ORIGEM_USER'),
    'password': os.getenv('DB_ORIGEM_PASSWORD') or os.getenv('ORIGEM_PASSWORD')
}

# Configurações DESTINO (suporta ambos os padrões: DB_DESTINO_* e DESTINO_*)
DESTINO = {
    'host': os.getenv('DB_DESTINO_HOST') or os.getenv('DESTINO_HOST'),
    'port': os.getenv('DB_DESTINO_PORT') or os.getenv('DESTINO_PORT') or '5432',
    'dbname': os.getenv('DB_DESTINO_NAME') or os.getenv('DESTINO_DBNAME'),
    'user': os.getenv('DB_DESTINO_USER') or os.getenv('DESTINO_USER'),
    'password': os.getenv('DB_DESTINO_PASSWORD') or os.getenv('DESTINO_PASSWORD')
}

# Validar configurações ao carregar o módulo
try:
    ORIGEM = validar_configuracao('ORIGEM', ORIGEM)
    DESTINO = validar_configuracao('DESTINO', DESTINO)
except ValueError as e:
    print(f"\n{e}\n")
    raise

# ========================================
# FUNÇÕES
# ========================================

def extrair_dados_origem(data_inicial=None):
    """Extrai dados do banco origem"""
    print("\n🔄 PASSO 1: Extraindo dados do banco ORIGEM...")
    
    conn = psycopg2.connect(**ORIGEM)
    cur = conn.cursor()
    
    if data_inicial:
        query = f"""
        SELECT 
            el."horaLeitura" AS dia,
            elc.m05, elc.m10, elc.m15,
            elc.h01, elc.h04, elc.h24, elc.h96,
            ee.nome AS estacao,
            el.estacao_id
        FROM public.estacoes_leitura AS el
        JOIN public.estacoes_leiturachuva AS elc ON elc.leitura_id = el.id
        JOIN public.estacoes_estacao AS ee ON ee.id = el.estacao_id
        WHERE el."horaLeitura" >= '{data_inicial}'::timestamp
        ORDER BY el."horaLeitura" ASC;
        """
    else:
        query = """
        SELECT 
            el."horaLeitura" AS dia,
            elc.m05, elc.m10, elc.m15,
            elc.h01, elc.h04, elc.h24, elc.h96,
            ee.nome AS estacao,
            el.estacao_id
        FROM public.estacoes_leitura AS el
        JOIN public.estacoes_leiturachuva AS elc ON elc.leitura_id = el.id
        JOIN public.estacoes_estacao AS ee ON ee.id = el.estacao_id
        ORDER BY el."horaLeitura" ASC;
        """
    
    cur.execute(query)
    dados = cur.fetchall()
    
    print(f"   ✅ Extraídos {len(dados):,} registros")
    
    cur.close()
    conn.close()
    
    return dados

def replicar_para_destino(dados):
    """Replica dados diretamente da origem para o destino"""
    print("\n🚀 PASSO 2: Replicando para banco DESTINO...")
    
    if not dados:
        print("   ⚠️  Nenhum dado para replicar")
        return 0
    
    # Conectar DESTINO
    conn_destino = psycopg2.connect(**DESTINO)
    cur_destino = conn_destino.cursor()
    
    insert_sql = """
    INSERT INTO pluviometricos
    (dia, m05, m10, m15, h01, h04, h24, h96, estacao, estacao_id)
    VALUES %s
    ON CONFLICT (dia, estacao_id) DO NOTHING;
    """
    
    execute_values(cur_destino, insert_sql, dados)
    conn_destino.commit()
    
    print(f"   ✅ Replicados {len(dados):,} registros para destino")
    
    cur_destino.close()
    conn_destino.close()
    
    return len(dados)

def sincronizacao_completa():
    """Executa sincronização completa"""
    print("="*70)
    print("🌧️  SINCRONIZAÇÃO COMPLETA - DADOS PLUVIOMÉTRICOS")
    print("="*70)
    print(f"⏰ Iniciado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Passo 1: Extrair
        dados = extrair_dados_origem()
        
        # Passo 2: Replicar diretamente para destino
        total_destino = replicar_para_destino(dados)
        
        print("\n" + "="*70)
        print("✅ SINCRONIZAÇÃO CONCLUÍDA!")
        print("="*70)
        print(f"📊 Total processado: {len(dados):,} registros")
        print(f"🚀 Replicados para destino: {total_destino:,} registros")
        print(f"⏰ Concluído em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)
        
    except Exception as e:
        print(f"\n❌ ERRO: {e}")
        import traceback
        traceback.print_exc()

def sincronizacao_incremental():
    """Sincroniza apenas dados novos (últimas 24h)"""
    print("="*70)
    print("🔄 SINCRONIZAÇÃO INCREMENTAL - Últimos dados")
    print("="*70)
    
    try:
        # Buscar última data no destino
        conn_destino = psycopg2.connect(**DESTINO)
        cur_destino = conn_destino.cursor()
        cur_destino.execute("SELECT MAX(dia) FROM pluviometricos;")
        ultima_data = cur_destino.fetchone()[0]
        cur_destino.close()
        conn_destino.close()
        
        if ultima_data:
            print(f"📅 Última data no destino: {ultima_data}")
            data_inicial = ultima_data.strftime('%Y-%m-%d %H:%M:%S')
        else:
            print("⚠️  Tabela destino vazia, fazendo carga completa")
            data_inicial = None
        
        # Extrair apenas dados novos
        dados = extrair_dados_origem(data_inicial)
        
        if not dados:
            print("   ✅ Nenhum dado novo para sincronizar")
            return
        
        # Replicar diretamente para destino
        replicar_para_destino(dados)
        
        print("\n✅ Sincronização incremental concluída!")
        
    except Exception as e:
        print(f"\n❌ ERRO: {e}")
        import traceback
        traceback.print_exc()

# ========================================
# MENU PRINCIPAL
# ========================================

def main():
    print("\n" + "="*70)
    print("🌧️  ETL PLUVIOMÉTRICOS - MENU")
    print("="*70)
    print("\n1 - Sincronização COMPLETA (todos os dados)")
    print("2 - Sincronização INCREMENTAL (apenas novos)")
    print("3 - Sair")
    print("\n" + "="*70)
    
    opcao = input("\nEscolha uma opção: ")
    
    if opcao == '1':
        sincronizacao_completa()
    elif opcao == '2':
        sincronizacao_incremental()
    elif opcao == '3':
        print("👋 Até logo!")
    else:
        print("❌ Opção inválida!")

if __name__ == "__main__":
    main()