#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script de debug para verificar exatamente o que está sendo retornado do banco.
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

# Testar com o registro específico que está dando problema
timestamp = '2009-10-27 23:54:00.000'
estacao_id = 11

print("=" * 80)
print("DEBUG - COMPARACAO DETALHADA")
print("=" * 80)
print()

# Conectar aos bancos
conn_origem = psycopg2.connect(**ORIGEM)
cur_origem = conn_origem.cursor()

conn_destino = psycopg2.connect(**DESTINO)
cur_destino = conn_destino.cursor()

# Query origem - DISTINCT ON (como o script usa)
print("BANCO ORIGEM - DISTINCT ON:")
print("-" * 80)
query_origem = """
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
WHERE el."horaLeitura" = %s::timestamp
  AND el.estacao_id = %s
ORDER BY el."horaLeitura" ASC, el.estacao_id ASC, el.id DESC
LIMIT 1;
"""

cur_origem.execute(query_origem, (timestamp, estacao_id))
reg_origem = cur_origem.fetchone()

if reg_origem:
    print(f"Dia: {reg_origem[0]} (tipo: {type(reg_origem[0])})")
    print(f"m05: {reg_origem[1]} (tipo: {type(reg_origem[1])})")
    print(f"m10: {reg_origem[2]} (tipo: {type(reg_origem[2])})")
    print(f"m15: {reg_origem[3]} (tipo: {type(reg_origem[3])})")
    print(f"h01: {reg_origem[4]} (tipo: {type(reg_origem[4])})")
    print(f"h04: {reg_origem[5]} (tipo: {type(reg_origem[5])})")
    print(f"h24: {reg_origem[6]} (tipo: {type(reg_origem[6])})")
    print(f"h96: {reg_origem[7]} (tipo: {type(reg_origem[7])})")
    print(f"Estacao: {reg_origem[8]}")
    print(f"Estacao ID: {reg_origem[9]}")
else:
    print("Nenhum registro encontrado!")

print()
print("BANCO DESTINO:")
print("-" * 80)
query_destino = """
SELECT dia, m05, m10, m15, h01, h04, h24, h96, estacao, estacao_id
FROM pluviometricos
WHERE dia = %s::timestamp
  AND estacao_id = %s;
"""

cur_destino.execute(query_destino, (timestamp, estacao_id))
reg_destino = cur_destino.fetchone()

if reg_destino:
    print(f"Dia: {reg_destino[0]} (tipo: {type(reg_destino[0])})")
    print(f"m05: {reg_destino[1]} (tipo: {type(reg_destino[1])})")
    print(f"m10: {reg_destino[2]} (tipo: {type(reg_destino[2])})")
    print(f"m15: {reg_destino[3]} (tipo: {type(reg_destino[3])})")
    print(f"h01: {reg_destino[4]} (tipo: {type(reg_destino[4])})")
    print(f"h04: {reg_destino[5]} (tipo: {type(reg_destino[5])})")
    print(f"h24: {reg_destino[6]} (tipo: {type(reg_destino[6])})")
    print(f"h96: {reg_destino[7]} (tipo: {type(reg_destino[7])})")
    print(f"Estacao: {reg_destino[8]}")
    print(f"Estacao ID: {reg_destino[9]}")
else:
    print("Nenhum registro encontrado!")

print()
print("=" * 80)
print("COMPARACAO DIRETA:")
print("=" * 80)

if reg_origem and reg_destino:
    campos = ['m05', 'm10', 'm15', 'h01', 'h04', 'h24', 'h96']
    for i, campo in enumerate(campos):
        idx = i + 1  # +1 porque o primeiro campo é 'dia'
        orig_val = reg_origem[idx]
        dest_val = reg_destino[idx]
        
        # Comparar diretamente
        igual_direto = (orig_val == dest_val)
        
        # Comparar como float
        try:
            orig_float = float(orig_val) if orig_val is not None else None
            dest_float = float(dest_val) if dest_val is not None else None
            if orig_float is not None and dest_float is not None:
                diff = abs(orig_float - dest_float)
                igual_float = diff <= 0.0001
            else:
                igual_float = (orig_float == dest_float)
                diff = None
        except:
            igual_float = False
            diff = None
        
        status = "OK" if (igual_direto or igual_float) else "DIFERENTE"
        print(f"{campo}:")
        print(f"  Origem: {orig_val} ({type(orig_val).__name__})")
        print(f"  Destino: {dest_val} ({type(dest_val).__name__})")
        print(f"  Igual direto: {igual_direto}")
        print(f"  Igual float: {igual_float}")
        if diff is not None:
            print(f"  Diferença: {diff}")
        print(f"  Status: {status}")
        print()

conn_origem.close()
conn_destino.close()

