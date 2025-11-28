#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Script rápido para verificar um registro específico."""

import psycopg2
from dotenv import load_dotenv
import os
from pathlib import Path

project_root = Path(__file__).parent.parent
load_dotenv(dotenv_path=project_root / '.env')

origem = {
    'host': os.getenv('DB_ORIGEM_HOST'),
    'dbname': os.getenv('DB_ORIGEM_NAME'),
    'user': os.getenv('DB_ORIGEM_USER'),
    'password': os.getenv('DB_ORIGEM_PASSWORD'),
    'sslmode': os.getenv('DB_ORIGEM_SSLMODE', 'disable')
}

destino = {
    'host': os.getenv('DB_DESTINO_HOST'),
    'port': os.getenv('DB_DESTINO_PORT', '5432'),
    'dbname': os.getenv('DB_DESTINO_NAME'),
    'user': os.getenv('DB_DESTINO_USER'),
    'password': os.getenv('DB_DESTINO_PASSWORD')
}

# Verificar origem - TODOS os registros
conn_origem = psycopg2.connect(**origem)
cur_origem = conn_origem.cursor()

print("=" * 80)
print("BANCO ORIGEM - TODOS OS REGISTROS")
print("=" * 80)
cur_origem.execute("""
    SELECT el.id, el."horaLeitura", elc.m05, elc.m10, elc.m15, elc.h01, elc.h04, elc.h24, elc.h96
    FROM public.estacoes_leitura AS el
    JOIN public.estacoes_leiturachuva AS elc ON elc.leitura_id = el.id
    WHERE el."horaLeitura" = %s::timestamp AND el.estacao_id = %s
    ORDER BY el.id DESC
""", ('2009-10-27 23:54:00.000', 11))

regs_origem = cur_origem.fetchall()
print(f"Total de registros: {len(regs_origem)}")
for r in regs_origem:
    print(f"ID: {r[0]}, Dia: {r[1]}, m05={r[2]}, m10={r[3]}, m15={r[4]}, h01={r[5]}, h04={r[6]}, h24={r[7]}, h96={r[8]}")

# Verificar origem - DISTINCT ON (como o script usa)
print()
print("=" * 80)
print("BANCO ORIGEM - DISTINCT ON (como o script usa)")
print("=" * 80)
cur_origem.execute("""
    SELECT DISTINCT ON (el."horaLeitura", el.estacao_id)
        el.id, el."horaLeitura", elc.m05, elc.m10, elc.m15, elc.h01, elc.h04, elc.h24, elc.h96
    FROM public.estacoes_leitura AS el
    JOIN public.estacoes_leiturachuva AS elc ON elc.leitura_id = el.id
    WHERE el."horaLeitura" = %s::timestamp AND el.estacao_id = %s
    ORDER BY el."horaLeitura" ASC, el.estacao_id ASC, el.id DESC
    LIMIT 1
""", ('2009-10-27 23:54:00.000', 11))

reg_distinct = cur_origem.fetchone()
if reg_distinct:
    print(f"ID: {reg_distinct[0]}, Dia: {reg_distinct[1]}, m05={reg_distinct[2]}, m10={reg_distinct[3]}, m15={reg_distinct[4]}, h01={reg_distinct[5]}, h04={reg_distinct[6]}, h24={reg_distinct[7]}, h96={reg_distinct[8]}")

# Verificar destino
print()
print("=" * 80)
print("BANCO DESTINO")
print("=" * 80)
conn_destino = psycopg2.connect(**destino)
cur_destino = conn_destino.cursor()

cur_destino.execute("""
    SELECT dia, m05, m10, m15, h01, h04, h24, h96
    FROM pluviometricos
    WHERE dia = %s::timestamp AND estacao_id = %s
""", ('2009-10-27 23:54:00.000', 11))

reg_destino = cur_destino.fetchone()
if reg_destino:
    print(f"Dia: {reg_destino[0]}, m05={reg_destino[1]}, m10={reg_destino[2]}, m15={reg_destino[3]}, h01={reg_destino[4]}, h04={reg_destino[5]}, h24={reg_destino[6]}, h96={reg_destino[7]}")

# Comparar (usando mesma lógica do script principal)
print()
print("=" * 80)
print("COMPARACAO")
print("=" * 80)
if reg_distinct and reg_destino:
    # Normalizar valores para comparação (tratando None, float e Decimal)
    def normalizar_valor(val):
        """Normaliza valores para comparação (None, float, Decimal -> float ou None)"""
        if val is None:
            return None
        try:
            return float(val)
        except (TypeError, ValueError):
            return val
    
    # Comparar todos os valores, não apenas h24
    valores_origem = tuple(normalizar_valor(v) for v in (reg_distinct[2], reg_distinct[3], reg_distinct[4], reg_distinct[5], reg_distinct[6], reg_distinct[7], reg_distinct[8]))
    valores_destino = tuple(normalizar_valor(v) for v in (reg_destino[1], reg_destino[2], reg_destino[3], reg_destino[4], reg_destino[5], reg_destino[6], reg_destino[7]))
    
    # Comparar com tolerância para floats (evitar problemas de precisão)
    def valores_iguais(orig, dest):
        """Verifica se dois valores são iguais, considerando precisão de float"""
        if orig is None and dest is None:
            return True
        if orig is None or dest is None:
            return False
        # Comparar floats com tolerância de 0.0001
        try:
            return abs(float(orig) - float(dest)) <= 0.0001
        except (TypeError, ValueError):
            return orig == dest
    
    if all(valores_iguais(orig, dest) for orig, dest in zip(valores_origem, valores_destino)):
        print("[OK] Todos os valores correspondem!")
    else:
        print("[DIFERENTE] Valores diferentes:")
        campos = ['m05', 'm10', 'm15', 'h01', 'h04', 'h24', 'h96']
        valores_origem_raw = (reg_distinct[2], reg_distinct[3], reg_distinct[4], reg_distinct[5], reg_distinct[6], reg_distinct[7], reg_distinct[8])
        valores_destino_raw = (reg_destino[1], reg_destino[2], reg_destino[3], reg_destino[4], reg_destino[5], reg_destino[6], reg_destino[7])
        for i, (orig, dest) in enumerate(zip(valores_origem, valores_destino)):
            if not valores_iguais(orig, dest):
                print(f"  {campos[i]}: ORIGEM={valores_origem_raw[i]} | DESTINO={valores_destino_raw[i]}")

conn_origem.close()
conn_destino.close()

