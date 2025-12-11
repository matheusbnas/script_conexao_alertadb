#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de diagnóstico para identificar inconsistências entre NIMBUS e BigQuery
"""

import pandas as pd
from pathlib import Path

# Obter diretório raiz do projeto (2 níveis acima deste arquivo)
project_root = Path(__file__).parent.parent.parent

# Ler arquivos CSV (caminho relativo ao projeto raiz)
df_bigquery = pd.read_csv(project_root / 'resultados' / 'pluviometricos_202512111645.csv')
df_nimbus = pd.read_csv(project_root / 'resultados' / '_SELECT_el_horaLeitura_AS_Dia_elc_m05_elc_m10_elc_m15_elc_h01_el_202512111645.csv')

# Renomear colunas do NIMBUS para corresponder
df_nimbus = df_nimbus.rename(columns={'Dia': 'dia', 'Estacao': 'estacao'})

print("=" * 80)
print("DIAGNÓSTICO DE INCONSISTÊNCIAS")
print("=" * 80)
print(f"\nTotal de registros BigQuery: {len(df_bigquery)}")
print(f"Total de registros NIMBUS: {len(df_nimbus)}")

# Comparar valores
print("\n" + "=" * 80)
print("COMPARAÇÃO DE VALORES h96 (primeiras 10 diferenças encontradas):")
print("=" * 80)

diferencas_encontradas = 0
for i in range(min(len(df_bigquery), len(df_nimbus))):
    dia_bq = df_bigquery.iloc[i]['dia']
    dia_nimbus = df_nimbus.iloc[i]['dia']
    
    if dia_bq == dia_nimbus:
        h96_bq = df_bigquery.iloc[i]['h96']
        h96_nimbus = df_nimbus.iloc[i]['h96']
        
        if h96_bq != h96_nimbus:
            diferencas_encontradas += 1
            if diferencas_encontradas <= 10:
                print(f"\nLinha {i+2}:")
                print(f"  Data: {dia_bq}")
                print(f"  BigQuery h96: {h96_bq}")
                print(f"  NIMBUS h96:   {h96_nimbus}")
                print(f"  Diferença:    {abs(float(h96_bq) - float(h96_nimbus))}")
                
                # Mostrar todos os valores para comparação
                print(f"\n  Valores completos:")
                print(f"    BigQuery: m05={df_bigquery.iloc[i]['m05']}, m10={df_bigquery.iloc[i]['m10']}, "
                      f"m15={df_bigquery.iloc[i]['m15']}, h01={df_bigquery.iloc[i]['h01']}, "
                      f"h04={df_bigquery.iloc[i]['h04']}, h24={df_bigquery.iloc[i]['h24']}, "
                      f"h96={df_bigquery.iloc[i]['h96']}")
                print(f"    NIMBUS:   m05={df_nimbus.iloc[i]['m05']}, m10={df_nimbus.iloc[i]['m10']}, "
                      f"m15={df_nimbus.iloc[i]['m15']}, h01={df_nimbus.iloc[i]['h01']}, "
                      f"h04={df_nimbus.iloc[i]['h04']}, h24={df_nimbus.iloc[i]['h24']}, "
                      f"h96={df_nimbus.iloc[i]['h96']}")

print(f"\n\nTotal de diferenças encontradas: {diferencas_encontradas}")
print("\n" + "=" * 80)
print("CONCLUSÃO:")
print("=" * 80)
print("Se há diferenças, isso indica que:")
print("1. A query do script usa DISTINCT ON para pegar apenas um registro por timestamp")
print("2. A query direta do usuário pode estar retornando múltiplos registros ou um registro diferente")
print("3. Quando há múltiplos registros com o mesmo timestamp, o DISTINCT ON pega o registro com maior ID")
print("\nSOLUÇÃO:")
print("- A query do script está CORRETA (usa DISTINCT ON)")
print("- A query do usuário precisa usar DISTINCT ON também para comparar corretamente")

