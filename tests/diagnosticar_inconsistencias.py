#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para diagnosticar inconsistências entre banco origem e destino.
Mostra detalhadamente quais registros têm divergências e como corrigir.
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

def diagnosticar_inconsistencias(quantidade=50):
    """
    Diagnostica inconsistências entre banco origem e destino.
    
    Args:
        quantidade: Quantidade de registros aleatórios para verificar
    """
    conn_origem = None
    cur_origem = None
    conn_destino = None
    cur_destino = None
    
    try:
        print("=" * 80)
        print("DIAGNOSTICO DE INCONSISTENCIAS")
        print("=" * 80)
        print()
        print(f"Verificando {quantidade} registros aleatórios...")
        print()
        
        # Conectar aos bancos
        conn_origem = psycopg2.connect(**ORIGEM)
        cur_origem = conn_origem.cursor()
        
        conn_destino = psycopg2.connect(**DESTINO)
        cur_destino = conn_destino.cursor()
        
        # Buscar registros aleatórios do banco destino
        cur_destino.execute(f"""
            SELECT dia, m05, m10, m15, h01, h04, h24, h96, estacao, estacao_id
            FROM pluviometricos
            ORDER BY RANDOM()
            LIMIT {quantidade};
        """)
        registros_destino = cur_destino.fetchall()
        
        if not registros_destino:
            print("[AVISO] Nenhum registro encontrado no banco destino.")
            return
        
        print(f"[OK] Encontrados {len(registros_destino)} registros no banco destino")
        print()
        
        divergencias = []
        registros_ausentes = []
        registros_ok = 0
        
        for registro_destino in registros_destino:
            dia_dest, m05_dest, m10_dest, m15_dest, h01_dest, h04_dest, h24_dest, h96_dest, estacao_dest, est_id_dest = registro_destino
            
            # Buscar registro correspondente no banco origem
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
            
            cur_origem.execute(query_origem, (dia_dest, est_id_dest))
            registro_origem = cur_origem.fetchone()
            
            if not registro_origem:
                registros_ausentes.append({
                    'dia': dia_dest,
                    'estacao_id': est_id_dest,
                    'estacao': estacao_dest,
                    'valores_destino': (m05_dest, m10_dest, m15_dest, h01_dest, h04_dest, h24_dest, h96_dest)
                })
            else:
                dia_orig, m05_orig, m10_orig, m15_orig, h01_orig, h04_orig, h24_orig, h96_orig, estacao_orig, est_id_orig = registro_origem
                
                # Comparar valores (tratando None, float e Decimal)
                def normalizar_valor(val):
                    """Normaliza valores para comparação (None, float, Decimal -> float ou None)"""
                    if val is None:
                        return None
                    try:
                        return float(val)
                    except (TypeError, ValueError):
                        return val
                
                valores_origem = tuple(normalizar_valor(v) for v in (m05_orig, m10_orig, m15_orig, h01_orig, h04_orig, h24_orig, h96_orig))
                valores_destino = tuple(normalizar_valor(v) for v in (m05_dest, m10_dest, m15_dest, h01_dest, h04_dest, h24_dest, h96_dest))
                
                # Comparar com tolerância para floats (evitar problemas de precisão)
                def valores_diferentes(orig, dest):
                    """Verifica se dois valores são diferentes, considerando precisão de float"""
                    if orig is None and dest is None:
                        return False
                    if orig is None or dest is None:
                        return True
                    # Comparar floats com tolerância de 0.0001
                    try:
                        return abs(float(orig) - float(dest)) > 0.0001
                    except (TypeError, ValueError):
                        return orig != dest
                
                if any(valores_diferentes(orig, dest) for orig, dest in zip(valores_origem, valores_destino)):
                    # Detalhar quais valores são diferentes
                    campos_diferentes = []
                    campos = ['m05', 'm10', 'm15', 'h01', 'h04', 'h24', 'h96']
                    for i, (orig, dest) in enumerate(zip(valores_origem, valores_destino)):
                        if valores_diferentes(orig, dest):
                            campos_diferentes.append({
                                'campo': campos[i],
                                'origem': m05_orig if i == 0 else m10_orig if i == 1 else m15_orig if i == 2 else h01_orig if i == 3 else h04_orig if i == 4 else h24_orig if i == 5 else h96_orig,
                                'destino': m05_dest if i == 0 else m10_dest if i == 1 else m15_dest if i == 2 else h01_dest if i == 3 else h04_dest if i == 4 else h24_dest if i == 5 else h96_dest
                            })
                    
                    divergencias.append({
                        'dia': dia_dest,
                        'estacao_id': est_id_dest,
                        'estacao': estacao_dest,
                        'campos_diferentes': campos_diferentes,
                        'valores_origem': valores_origem,
                        'valores_destino': valores_destino
                    })
                else:
                    registros_ok += 1
        
        # Exibir resultados
        print("=" * 80)
        print("RESULTADO DO DIAGNOSTICO")
        print("=" * 80)
        print()
        print(f"Total de registros verificados: {len(registros_destino)}")
        print(f"[OK] Registros corretos: {registros_ok}")
        print(f"[AVISO] Registros com valores diferentes: {len(divergencias)}")
        print(f"[ERRO] Registros ausentes no banco origem: {len(registros_ausentes)}")
        print()
        
        if divergencias:
            print("=" * 80)
            print("[AVISO] INCONSISTENCIAS ENCONTRADAS (Valores Diferentes)")
            print("=" * 80)
            print()
            
            for i, div in enumerate(divergencias[:20], 1):  # Mostrar até 20 primeiras
                print(f"{i}. Data: {div['dia']} | Estação: {div['estacao']} (ID: {div['estacao_id']})")
                print("   Campos com valores diferentes:")
                for campo in div['campos_diferentes']:
                    print(f"      • {campo['campo']}: ORIGEM={campo['origem']} | DESTINO={campo['destino']}")
                print()
            
            if len(divergencias) > 20:
                print(f"... e mais {len(divergencias) - 20} inconsistências")
                print()
        
        if registros_ausentes:
            print("=" * 80)
            print("[ERRO] REGISTROS AUSENTES NO BANCO ORIGEM")
            print("=" * 80)
            print()
            print("Estes registros existem no banco destino mas não foram encontrados")
            print("no banco origem (pode ser normal se foram deletados no banco origem):")
            print()
            
            for i, ausente in enumerate(registros_ausentes[:10], 1):  # Mostrar até 10 primeiras
                print(f"{i}. Data: {ausente['dia']} | Estacao: {ausente['estacao']} (ID: {ausente['estacao_id']})")
                vals = ausente['valores_destino']
                print(f"   Valores no destino: m05={vals['m05']}, h01={vals['h01']}, h04={vals['h04']}, h24={vals['h24']}, h96={vals['h96']}")
                print()
            
            if len(registros_ausentes) > 10:
                print(f"... e mais {len(registros_ausentes) - 10} registros ausentes")
                print()
        
        # Recomendações
        print("=" * 80)
        print("COMO RESOLVER")
        print("=" * 80)
        print()
        
        if divergencias or registros_ausentes:
            print("Para corrigir as inconsistências:")
            print()
            print("1. Execute novamente o script de carga inicial:")
            print("   python scripts/carregar_pluviometricos_historicos.py")
            print()
            print("   O script usa ON CONFLICT DO UPDATE, então vai atualizar")
            print("   automaticamente os registros com valores incorretos.")
            print()
            print("2. Ou execute o script de correção para um período específico:")
            print("   python scripts/corrigir_dados_pluviometricos.py")
            print()
            print("3. Após corrigir, execute este diagnóstico novamente para verificar:")
            print("   python scripts/diagnosticar_inconsistencias.py")
        else:
            print("[OK] Nenhuma inconsistencia encontrada!")
            print("   Todos os registros verificados estão corretos.")
        
        print()
        print("=" * 80)
        
    except Exception as e:
        print(f'\n[ERRO] Erro no diagnostico: {e}')
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
    import sys
    
    print("=" * 80)
    print("DIAGNOSTICO DE INCONSISTENCIAS")
    print("=" * 80)
    print()
    print("Este script verifica inconsistências entre o banco origem e destino.")
    print("Ele compara uma amostra aleatória de registros para identificar")
    print("quais têm valores diferentes ou estão ausentes.")
    print()
    
    # Aceitar quantidade via argumento de linha de comando
    quantidade = 50
    if len(sys.argv) > 1:
        try:
            quantidade = int(sys.argv[1])
        except ValueError:
            print(f"[AVISO] Argumento invalido, usando padrao: 50")
    
    print(f"Verificando {quantidade} registros...")
    print()
    
    diagnosticar_inconsistencias(quantidade)

if __name__ == "__main__":
    main()

