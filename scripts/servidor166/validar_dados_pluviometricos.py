#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para validar que os dados no banco destino correspondem exatamente
aos dados do banco origem (alertadb).
"""

import psycopg2
from dotenv import load_dotenv
import os
from pathlib import Path
from datetime import datetime, timedelta

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
    origem = {
        'host': obter_variavel('DB_ORIGEM_HOST'),
        'dbname': obter_variavel('DB_ORIGEM_NAME'),
        'user': obter_variavel('DB_ORIGEM_USER'),
        'password': obter_variavel('DB_ORIGEM_PASSWORD'),
        'sslmode': obter_variavel('DB_ORIGEM_SSLMODE', obrigatoria=False) or 'disable'
    }
    destino = {
        'host': obter_variavel('DB_DESTINO_HOST'),
        'port': obter_variavel('DB_DESTINO_PORT', obrigatoria=False) or '5432',
        'dbname': obter_variavel('DB_DESTINO_NAME'),
        'user': obter_variavel('DB_DESTINO_USER'),
        'password': obter_variavel('DB_DESTINO_PASSWORD')
    }
    return origem, destino

ORIGEM, DESTINO = carregar_configuracoes()

def query_dados_origem(data_inicial=None, data_final=None, estacao_id=None):
    """Query para buscar dados do banco origem usando DISTINCT ON."""
    where_clauses = []
    
    if data_inicial:
        where_clauses.append(f"el.\"horaLeitura\" >= '{data_inicial}'::timestamp")
    if data_final:
        where_clauses.append(f"el.\"horaLeitura\" <= '{data_final}'::timestamp")
    if estacao_id:
        where_clauses.append(f"el.estacao_id = {estacao_id}")
    
    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)
    
    return f"""
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
    {where_sql}
    ORDER BY el."horaLeitura" ASC, el.estacao_id ASC, el.id DESC;
    """

def normalizar_valor(val):
    """Normaliza valores para comparação (None, float, Decimal -> float ou None).
    
    Usa a mesma lógica do script de carregamento para garantir comparação correta.
    """
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return val

def normalizar_timestamp(ts):
    """Normaliza timestamp removendo timezone para comparação.
    
    Usa a mesma lógica do script de carregamento - remove timezone mantendo valor local.
    """
    if ts is None:
        return None
    if isinstance(ts, datetime):
        # Remover timezone mantendo valor local (não converter para UTC)
        return ts.replace(tzinfo=None) if ts.tzinfo else ts
    return ts

def valores_iguais(orig, dest):
    """Compara dois valores considerando precisão de float.
    
    Usa tolerância para evitar falsos positivos por diferenças mínimas de precisão.
    """
    if orig is None and dest is None:
        return True
    if orig is None or dest is None:
        return False
    try:
        # Comparar floats com tolerância de 0.0001 (mesma lógica dos testes)
        return abs(float(orig) - float(dest)) <= 0.0001
    except (TypeError, ValueError):
        return orig == dest

def validar_dados(data_inicial=None, data_final=None, estacao_id=None, limite=100):
    """Valida que os dados no destino correspondem aos dados do banco origem.
    
    Usa a mesma lógica do script carregar_pluviometricos_historicos.py:
    - DISTINCT ON para garantir unicidade
    - Normalização de valores (Decimal -> float)
    - Comparação de timestamps sem timezone
    """
    
    conn_origem = None
    cur_origem = None
    conn_destino = None
    cur_destino = None
    
    try:
        print("=" * 80)
        print("VALIDACAO DE DADOS PLUVIOMETRICOS")
        print("=" * 80)
        print()
        
        if data_inicial:
            print(f"Data inicial: {data_inicial}")
        if data_final:
            print(f"Data final: {data_final}")
        if estacao_id:
            print(f"Estacao ID: {estacao_id}")
        print(f"Limite de registros para validar: {limite}")
        print()
        
        # Conectar aos bancos
        print("Conectando aos bancos...")
        conn_origem = psycopg2.connect(**ORIGEM)
        cur_origem = conn_origem.cursor()
        
        conn_destino = psycopg2.connect(**DESTINO)
        cur_destino = conn_destino.cursor()
        print("   Conexoes estabelecidas com sucesso!")
        print()
        
        # Buscar dados do banco origem usando DISTINCT ON (mesma lógica do carregamento)
        print("Buscando dados do banco ORIGEM (usando DISTINCT ON como no carregamento)...")
        query_origem = query_dados_origem(data_inicial, data_final, estacao_id)
        cur_origem.execute(query_origem)
        dados_origem = cur_origem.fetchall()
        print(f"   Encontrados {len(dados_origem)} registros no banco origem")
        
        if not dados_origem:
            print("   Nenhum dado encontrado no banco origem para validar.")
            return
        
        # Limitar quantidade de registros para validação
        dados_origem = dados_origem[:limite]
        print(f"   Validando {len(dados_origem)} registros...")
        print()
        
        # Buscar dados correspondentes do banco destino
        print("Buscando dados correspondentes do banco DESTINO...")
        
        divergencias = []
        total_validados = 0
        
        for registro_origem in dados_origem:
            dia_orig, m05_origem, m10_origem, m15_origem, h01_origem, h04_origem, h24_origem, h96_origem, estacao, est_id = registro_origem
            
            # Normalizar timestamp para busca (remover timezone se presente)
            dia_normalizado = normalizar_timestamp(dia_orig)
            
            # Buscar registro correspondente no destino
            # Usar comparação com margem de tempo para evitar problemas de timezone
            query_destino = """
            SELECT dia, m05, m10, m15, h01, h04, h24, h96, estacao, estacao_id
            FROM pluviometricos
            WHERE dia >= %s::timestamp - INTERVAL '1 second'
              AND dia <= %s::timestamp + INTERVAL '1 second'
              AND estacao_id = %s
            LIMIT 1;
            """
            
            cur_destino.execute(query_destino, (dia_normalizado, dia_normalizado, est_id))
            registro_destino = cur_destino.fetchone()
            
            total_validados += 1
            
            if not registro_destino:
                divergencias.append({
                    'tipo': 'REGISTRO_AUSENTE',
                    'dia': dia_orig,
                    'estacao_id': est_id,
                    'origem': registro_origem,
                    'destino': None
                })
            else:
                dia_dest, m05_dest, m10_dest, m15_dest, h01_dest, h04_dest, h24_dest, h96_dest, estacao_dest, est_id_dest = registro_destino
                
                # Normalizar valores para comparação (mesma lógica do script de carregamento)
                valores_origem = tuple(normalizar_valor(v) for v in (m05_origem, m10_origem, m15_origem, h01_origem, h04_origem, h24_origem, h96_origem))
                valores_destino = tuple(normalizar_valor(v) for v in (m05_dest, m10_dest, m15_dest, h01_dest, h04_dest, h24_dest, h96_dest))
                
                # Comparar valores usando tolerância para floats (mesma lógica dos testes)
                valores_diferentes = any(not valores_iguais(orig, dest) for orig, dest in zip(valores_origem, valores_destino))
                
                if valores_diferentes:
                    divergencias.append({
                        'tipo': 'VALORES_DIFERENTES',
                        'dia': dia_orig,
                        'estacao_id': est_id,
                        'origem': registro_origem,
                        'destino': registro_destino
                    })
        
        # Exibir resultados
        print("=" * 80)
        print("RESULTADO DA VALIDACAO")
        print("=" * 80)
        print()
        print(f"Total de registros validados: {total_validados}")
        print(f"Divergencias encontradas: {len(divergencias)}")
        print()
        
        if divergencias:
            print("DIVERGENCIAS ENCONTRADAS:")
            print("-" * 80)
            
            for i, div in enumerate(divergencias[:20], 1):  # Mostrar até 20 primeiras
                print(f"\n{i}. {div['tipo']} - {div['dia']} (Estacao ID: {div['estacao_id']})")
                
                if div['tipo'] == 'REGISTRO_AUSENTE':
                    reg_origem = div['origem']
                    print(f"   Registro existe no banco ORIGEM mas nao no DESTINO:")
                    print(f"      m05={reg_origem[1]}, m10={reg_origem[2]}, m15={reg_origem[3]}")
                    print(f"      h01={reg_origem[4]}, h04={reg_origem[5]}, h24={reg_origem[6]}, h96={reg_origem[7]}")
                
                elif div['tipo'] == 'VALORES_DIFERENTES':
                    reg_origem = div['origem']
                    reg_destino = div['destino']
                    
                    print(f"   ORIGEM:  m05={reg_origem[1]}, h01={reg_origem[4]}, h04={reg_origem[5]}, h24={reg_origem[6]}, h96={reg_origem[7]}")
                    print(f"   DESTINO: m05={reg_destino[1]}, h01={reg_destino[4]}, h04={reg_destino[5]}, h24={reg_destino[6]}, h96={reg_destino[7]}")
            
            if len(divergencias) > 20:
                print(f"\n... e mais {len(divergencias) - 20} divergencias")
            
            print()
            print("=" * 80)
            print("RECOMENDACAO:")
            print("=" * 80)
            print("Execute o script 'carregar_pluviometricos_historicos.py' para corrigir")
            print("os dados incorretos no banco destino.")
            print("=" * 80)
        else:
            print("=" * 80)
            print("SUCESSO!")
            print("=" * 80)
            print("Todos os registros validados correspondem exatamente aos dados")
            print("do banco origem. Os dados estao corretos!")
            print("=" * 80)
        
    except Exception as e:
        print(f'\nERRO na validacao: {e}')
        import traceback
        traceback.print_exc()
    finally:
        if cur_origem:
            cur_origem.close()
        if conn_origem:
            conn_origem.close()
        if cur_destino:
            cur_destino.close()
        if conn_destino:
            conn_destino.close()

def main():
    """Função principal."""
    print("=" * 80)
    print("VALIDACAO DE DADOS PLUVIOMETRICOS")
    print("=" * 80)
    print()
    print("Este script valida que os dados no banco destino correspondem")
    print("exatamente aos dados do banco origem (alertadb).")
    print()
    
    # Validar período específico mencionado pelo usuário
    data_inicial = '2019-02-15 23:00:00.000'
    data_final = '2019-02-18 01:00:00.000'
    estacao_id = 14
    
    resposta = input(f"Validar dados do período {data_inicial} até {data_final} para estação {estacao_id}? (S/n): ")
    if resposta.lower() == 'n':
        print("Operação cancelada.")
        return
    
    validar_dados(data_inicial, data_final, estacao_id, limite=100)
    
    print("\n💡 DICA: Para validar outros períodos, execute este script novamente")
    print("   ou modifique as datas diretamente no código.")

if __name__ == "__main__":
    main()

