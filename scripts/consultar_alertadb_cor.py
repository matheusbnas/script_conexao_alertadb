#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para consultar dados do banco alertadb_cor (destino).
Similar à query do banco origem, mas adaptado para a tabela pluviometricos.
"""

import psycopg2
from dotenv import load_dotenv
import os
from pathlib import Path
from datetime import datetime

# Carregar variáveis de ambiente
project_root = Path(__file__).parent.parent
load_dotenv(dotenv_path=project_root / '.env')

def obter_variavel(nome, obrigatoria=True):
    """Obtém variável de ambiente, lança erro se obrigatória e não encontrada."""
    valor = os.getenv(nome)
    if obrigatoria and not valor:
        raise ValueError(f"Variável de ambiente obrigatória não encontrada: {nome}")
    return valor

def carregar_configuracoes():
    """Carrega todas as configurações do arquivo .env."""
    destino = {
        'host': obter_variavel('DB_DESTINO_HOST'),
        'port': obter_variavel('DB_DESTINO_PORT', obrigatoria=False) or '5432',
        'dbname': obter_variavel('DB_DESTINO_NAME'),
        'user': obter_variavel('DB_DESTINO_USER'),
        'password': obter_variavel('DB_DESTINO_PASSWORD')
    }
    return destino

DESTINO = carregar_configuracoes()

def query_pluviometricos(data_inicial=None, data_final=None, estacao_id=None, order_by='DESC', incluir_data_sincronizacao=False):
    """
    Retorna query para buscar dados da tabela pluviometricos.
    
    Args:
        data_inicial: Data inicial para filtrar (formato: 'YYYY-MM-DD HH:MM:SS')
        data_final: Data final para filtrar (formato: 'YYYY-MM-DD HH:MM:SS')
        estacao_id: ID da estação para filtrar
        order_by: 'ASC' ou 'DESC' para ordenação por dia
        incluir_data_sincronizacao: Se True, inclui a coluna data_sincronizacao no SELECT
    
    Returns:
        str: Query SQL
    """
    where_clauses = []
    
    if data_inicial:
        where_clauses.append(f"dia >= '{data_inicial}'::timestamp")
    if data_final:
        where_clauses.append(f"dia <= '{data_final}'::timestamp")
    if estacao_id:
        where_clauses.append(f"estacao_id = {estacao_id}")
    
    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)
    
    order_direction = order_by.upper() if order_by.upper() in ['ASC', 'DESC'] else 'DESC'
    
    # Construir SELECT com ou sem data_sincronizacao
    select_cols = """
        dia,
        m05, m10, m15,
        h01, h04, h24, h96,
        estacao,
        estacao_id"""
    
    if incluir_data_sincronizacao:
        select_cols += ",\n        data_sincronizacao"
    
    return f"""
    SELECT {select_cols}
    FROM public.pluviometricos
    {where_sql}
    ORDER BY dia {order_direction}
    """

def consultar_dados(data_inicial=None, data_final=None, estacao_id=None, order_by='DESC', limite=None):
    """
    Consulta dados da tabela pluviometricos no banco alertadb_cor.
    
    Args:
        data_inicial: Data inicial para filtrar
        data_final: Data final para filtrar
        estacao_id: ID da estação para filtrar
        order_by: 'ASC' ou 'DESC' para ordenação
        limite: Limite de registros a retornar (None = sem limite)
    """
    conn_destino = None
    cur_destino = None
    
    try:
        print("=" * 80)
        print("CONSULTA - BANCO ALERTADB_COR")
        print("=" * 80)
        print()
        
        if data_inicial:
            print(f"Data inicial: {data_inicial}")
        if data_final:
            print(f"Data final: {data_final}")
        if estacao_id:
            print(f"Estacao ID: {estacao_id}")
        if limite:
            print(f"Limite: {limite} registros")
        print(f"Ordenacao: {order_by}")
        print()
        
        # Conectar ao banco destino
        conn_destino = psycopg2.connect(**DESTINO)
        cur_destino = conn_destino.cursor()
        
        # Verificar se a coluna data_sincronizacao existe
        cur_destino.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
              AND table_name = 'pluviometricos' 
              AND column_name = 'data_sincronizacao';
        """)
        tem_data_sincronizacao = cur_destino.fetchone() is not None
        
        # Construir query (incluir data_sincronizacao se existir)
        query = query_pluviometricos(data_inicial, data_final, estacao_id, order_by, tem_data_sincronizacao)
        
        if limite:
            # Remover ponto e vírgula final se existir e adicionar LIMIT
            query = query.rstrip().rstrip(';') + f" LIMIT {limite};"
        else:
            # Garantir que termina com ponto e vírgula
            query = query.rstrip().rstrip(';') + ";"
        
        print("Executando query...")
        print()
        cur_destino.execute(query)
        dados = cur_destino.fetchall()
        
        print(f"Total de registros encontrados: {len(dados)}")
        print()
        
        if not dados:
            print("Nenhum registro encontrado.")
            return
        
        # Exibir resultados
        print("=" * 80)
        print("RESULTADOS")
        print("=" * 80)
        print()
        
        # Cabeçalho
        if tem_data_sincronizacao:
            print(f"{'Dia':<25} {'m05':<8} {'m10':<8} {'m15':<8} {'h01':<8} {'h04':<8} {'h24':<8} {'h96':<8} {'Estacao':<25} {'ID':<5} {'Sincronizado':<20}")
            print("-" * 100)
        else:
            print(f"{'Dia':<25} {'m05':<8} {'m10':<8} {'m15':<8} {'h01':<8} {'h04':<8} {'h24':<8} {'h96':<8} {'Estacao':<30} {'ID':<5}")
            print("-" * 80)
        
        # Dados
        for registro in dados:
            # Verificar se tem data_sincronizacao (última coluna se existir)
            if tem_data_sincronizacao and len(registro) == 11:
                dia, m05, m10, m15, h01, h04, h24, h96, estacao, estacao_id, data_sincronizacao = registro
            else:
                dia, m05, m10, m15, h01, h04, h24, h96, estacao, estacao_id = registro
                data_sincronizacao = None
            
            # Formatar dia
            if isinstance(dia, datetime):
                dia_str = dia.strftime('%Y-%m-%d %H:%M:%S')
            else:
                dia_str = str(dia)
            
            # Formatar valores (None vira vazio)
            m05_str = f"{m05:.2f}" if m05 is not None else ""
            m10_str = f"{m10:.2f}" if m10 is not None else ""
            m15_str = f"{m15:.2f}" if m15 is not None else ""
            h01_str = f"{h01:.2f}" if h01 is not None else ""
            h04_str = f"{h04:.2f}" if h04 is not None else ""
            h24_str = f"{h24:.2f}" if h24 is not None else ""
            h96_str = f"{h96:.2f}" if h96 is not None else ""
            
            if tem_data_sincronizacao and data_sincronizacao:
                data_sync_str = str(data_sincronizacao)[:19] if isinstance(data_sincronizacao, datetime) else str(data_sincronizacao)
                print(f"{dia_str:<25} {m05_str:<8} {m10_str:<8} {m15_str:<8} {h01_str:<8} {h04_str:<8} {h24_str:<8} {h96_str:<8} {estacao:<25} {estacao_id:<5} {data_sync_str:<20}")
            else:
                print(f"{dia_str:<25} {m05_str:<8} {m10_str:<8} {m15_str:<8} {h01_str:<8} {h04_str:<8} {h24_str:<8} {h96_str:<8} {estacao:<30} {estacao_id:<5}")
        
        print()
        print("=" * 80)
        
        # Estatísticas
        if len(dados) > 0:
            print("\nESTATISTICAS:")
            print(f"  Total de registros: {len(dados)}")
            
            # Primeira e última data
            if order_by.upper() == 'DESC':
                primeira_data = dados[-1][0]
                ultima_data = dados[0][0]
            else:
                primeira_data = dados[0][0]
                ultima_data = dados[-1][0]
            
            print(f"  Primeira data: {primeira_data}")
            print(f"  Ultima data: {ultima_data}")
            
            # Estações únicas
            estacoes_unicas = set(reg[8] for reg in dados if reg[8])
            print(f"  Estacoes diferentes: {len(estacoes_unicas)}")
            if len(estacoes_unicas) <= 10:
                print(f"  Estacoes: {', '.join(sorted(estacoes_unicas))}")
        
        print("=" * 80)
        
    except Exception as e:
        print(f'\n[ERRO] Erro na consulta: {e}')
        import traceback
        traceback.print_exc()
    finally:
        if cur_destino:
            cur_destino.close()
        if conn_destino:
            conn_destino.close()

def main():
    """Função principal."""
    import sys
    
    print("=" * 80)
    print("CONSULTA - BANCO ALERTADB_COR")
    print("=" * 80)
    print()
    print("Este script consulta dados da tabela pluviometricos no banco alertadb_cor.")
    print()
    
    # Exemplo do usuário
    data_inicial = None
    data_final = None
    estacao_id = None
    order_by = 'DESC'
    limite = None
    
    # Processar argumentos de linha de comando
    if len(sys.argv) > 1:
        if sys.argv[1] in ['--help', '-h']:
            print("Uso:")
            print("  python consultar_alertadb_cor.py [data_inicial] [data_final] [estacao_id] [--asc|--desc] [--limit N]")
            print()
            print("Exemplos:")
            print("  python consultar_alertadb_cor.py")
            print("  python consultar_alertadb_cor.py '2009-10-27 23:00:00.000' '2009-10-28 01:00:00.000' 11")
            print("  python consultar_alertadb_cor.py '2009-10-27 23:00:00.000' '2009-10-28 01:00:00.000' 11 --asc")
            print("  python consultar_alertadb_cor.py '2009-10-27 23:00:00.000' '2009-10-28 01:00:00.000' 11 --limit 10")
            return
        
        # Processar argumentos
        args = sys.argv[1:]
        i = 0
        while i < len(args):
            if args[i] == '--asc':
                order_by = 'ASC'
            elif args[i] == '--desc':
                order_by = 'DESC'
            elif args[i] == '--limit' and i + 1 < len(args):
                limite = int(args[i + 1])
                i += 1
            elif data_inicial is None:
                data_inicial = args[i]
            elif data_final is None:
                data_final = args[i]
            elif estacao_id is None:
                try:
                    estacao_id = int(args[i])
                except ValueError:
                    print(f"[AVISO] Ignorando argumento invalido: {args[i]}")
            i += 1
    
    # Se não foram passados argumentos, usar exemplo do usuário
    if not data_inicial and not data_final and not estacao_id:
        print("Exemplo de uso:")
        print("  python consultar_alertadb_cor.py '2009-10-27 23:00:00.000' '2009-10-28 01:00:00.000' 11")
        print()
        resposta = input("Deseja usar o exemplo acima? (S/n): ")
        if resposta.lower() != 'n':
            data_inicial = '2009-10-27 23:00:00.000'
            data_final = '2009-10-28 01:00:00.000'
            estacao_id = 11
    
    consultar_dados(data_inicial, data_final, estacao_id, order_by, limite)

if __name__ == "__main__":
    main()

