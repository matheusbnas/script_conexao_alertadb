#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para verificar se há duplicatas no período específico que está causando problemas
"""

import psycopg2
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
import os

# Carregar variáveis de ambiente
project_root = Path(__file__).parent.parent.parent.parent
load_dotenv(dotenv_path=project_root / '.env')

def obter_variavel(nome, obrigatoria=True):
    """Obtém variável de ambiente."""
    valor = os.getenv(nome)
    if obrigatoria and not valor:
        raise ValueError(f"❌ Variável de ambiente obrigatória não encontrada: {nome}")
    return valor

# Configurações de conexão NIMBUS
ORIGEM = {
    'host': obter_variavel('DB_ORIGEM_HOST'),
    'dbname': obter_variavel('DB_ORIGEM_NAME'),
    'user': obter_variavel('DB_ORIGEM_USER'),
    'password': obter_variavel('DB_ORIGEM_PASSWORD'),
    'sslmode': obter_variavel('DB_ORIGEM_SSLMODE', obrigatoria=False) or 'disable'
}

def verificar_duplicatas_detalhadas():
    """Verifica duplicatas com detalhes dos valores"""
    conn = psycopg2.connect(**ORIGEM)
    cur = conn.cursor()
    
    query = """
    SELECT 
        el."horaLeitura",
        el.estacao_id,
        COUNT(*) as quantidade,
        array_agg(el.id) as ids,
        array_agg(elc.h96) as valores_h96,
        array_agg(elc.h24) as valores_h24
    FROM public.estacoes_leitura AS el
    JOIN public.estacoes_leiturachuva AS elc
        ON elc.leitura_id = el.id
    JOIN public.estacoes_estacao AS ee 
        ON ee.id = el.estacao_id
    WHERE el."horaLeitura" >= '2009-02-15 22:00:00.000' 
      AND el."horaLeitura" <= '2009-02-18 01:00:00.000' 
      AND el.estacao_id = 14
    GROUP BY el."horaLeitura", el.estacao_id
    HAVING COUNT(*) > 1
    ORDER BY el."horaLeitura" DESC;
    """
    
    cur.execute(query)
    duplicatas = cur.fetchall()
    
    conn.close()
    return duplicatas

def comparar_registros_duplicados():
    """Compara quais registros o DISTINCT ON está pegando vs o que deveria pegar"""
    conn = psycopg2.connect(**ORIGEM)
    cur = conn.cursor()
    
    # Buscar todos os registros (sem DISTINCT ON) para ver duplicatas
    query_todos = """
    SELECT 
        el.id,
        el."horaLeitura",
        el.estacao_id,
        elc.h96,
        elc.h24,
        elc.m05,
        elc.m10,
        elc.m15,
        elc.h01,
        elc.h04
    FROM public.estacoes_leitura AS el
    JOIN public.estacoes_leiturachuva AS elc
        ON elc.leitura_id = el.id
    JOIN public.estacoes_estacao AS ee 
        ON ee.id = el.estacao_id
    WHERE el."horaLeitura" >= '2009-02-15 22:00:00.000' 
      AND el."horaLeitura" <= '2009-02-18 01:00:00.000' 
      AND el.estacao_id = 14
    ORDER BY el."horaLeitura" DESC, el.id DESC;
    """
    
    cur.execute(query_todos)
    todos_registros = cur.fetchall()
    
    conn.close()
    
    # Agrupar por timestamp
    from collections import defaultdict
    por_timestamp = defaultdict(list)
    
    for reg in todos_registros:
        reg_id, hora_leitura, estacao_id, h96, h24, m05, m10, m15, h01, h04 = reg
        por_timestamp[str(hora_leitura)].append({
            'id': reg_id,
            'hora_leitura': hora_leitura,
            'h96': h96,
            'h24': h24,
            'm05': m05,
            'm10': m10,
            'm15': m15,
            'h01': h01,
            'h04': h04
        })
    
    # Encontrar duplicatas
    duplicatas_encontradas = {ts: regs for ts, regs in por_timestamp.items() if len(regs) > 1}
    
    return duplicatas_encontradas

def main():
    print("=" * 80)
    print("VERIFICAÇÃO DE DUPLICATAS - PERÍODO ESPECÍFICO")
    print("=" * 80)
    
    print("\n1️⃣ Verificando duplicatas com query SQL...")
    duplicatas_sql = verificar_duplicatas_detalhadas()
    
    if duplicatas_sql:
        print(f"\n⚠️  Encontradas {len(duplicatas_sql)} combinações (timestamp, estacao_id) com múltiplos registros:")
        for hora_leitura, estacao_id, quantidade, ids, valores_h96, valores_h24 in duplicatas_sql[:10]:
            print(f"\n   📅 Timestamp: {hora_leitura}")
            print(f"   Estação ID: {estacao_id}")
            print(f"   Quantidade: {quantidade}")
            print(f"   IDs: {ids}")
            print(f"   Valores h96: {valores_h96}")
            print(f"   Valores h24: {valores_h24}")
            print(f"   → DISTINCT ON vai pegar ID {ids[0]} (h96={valores_h96[0]}, h24={valores_h24[0]})")
        if len(duplicatas_sql) > 10:
            print(f"\n   ... e mais {len(duplicatas_sql) - 10} duplicatas")
    else:
        print("\n✅ Nenhuma duplicata encontrada com query SQL")
    
    print("\n2️⃣ Verificando duplicatas comparando todos os registros...")
    duplicatas_detalhadas = comparar_registros_duplicados()
    
    if duplicatas_detalhadas:
        print(f"\n⚠️  Encontradas {len(duplicatas_detalhadas)} timestamps com múltiplos registros:")
        for ts, regs in list(duplicatas_detalhadas.items())[:10]:
            print(f"\n   📅 Timestamp: {ts}")
            print(f"   Quantidade de registros: {len(regs)}")
            for reg in regs:
                print(f"      ID {reg['id']}: h96={reg['h96']}, h24={reg['h24']}")
            # DISTINCT ON com ORDER BY id DESC vai pegar o maior ID
            maior_id = max(regs, key=lambda x: x['id'])
            print(f"   → DISTINCT ON vai pegar ID {maior_id['id']} (h96={maior_id['h96']}, h24={maior_id['h24']})")
        if len(duplicatas_detalhadas) > 10:
            print(f"\n   ... e mais {len(duplicatas_detalhadas) - 10} timestamps com duplicatas")
    else:
        print("\n✅ Nenhuma duplicata encontrada")
    
    print("\n" + "=" * 80)
    print("CONCLUSÃO")
    print("=" * 80)
    
    if duplicatas_sql or duplicatas_detalhadas:
        print("⚠️  HÁ DUPLICATAS NESTE PERÍODO!")
        print("   Isso explica as diferenças entre NIMBUS e BigQuery")
        print("   O DISTINCT ON está pegando o registro com maior ID (mais recente)")
        print("   Mas pode haver registros com IDs diferentes mas valores diferentes")
        print("\n💡 SOLUÇÃO:")
        print("   Execute novamente: python scripts/bigquery/pluviometricos/exportar_pluviometricos_nimbus_bigquery.py")
        print("   Isso garantirá que os dados mais recentes sejam exportados")
    else:
        print("✅ NÃO HÁ DUPLICATAS")
        print("   O problema deve estar em outro lugar")
        print("   Verifique se os dados foram exportados corretamente")

if __name__ == "__main__":
    main()

