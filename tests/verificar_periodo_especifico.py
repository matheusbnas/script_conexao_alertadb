#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para verificar um período específico entre banco origem e destino.
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

def verificar_periodo(data_inicial, data_final, estacao_id):
    """Verifica um período específico comparando origem e destino."""
    
    conn_origem = None
    cur_origem = None
    conn_destino = None
    cur_destino = None
    
    try:
        print("=" * 80)
        print("VERIFICACAO DE PERIODO ESPECIFICO")
        print("=" * 80)
        print()
        print(f"Data inicial: {data_inicial}")
        print(f"Data final: {data_final}")
        print(f"Estacao ID: {estacao_id}")
        print()
        
        # Conectar aos bancos
        conn_origem = psycopg2.connect(**ORIGEM)
        cur_origem = conn_origem.cursor()
        
        conn_destino = psycopg2.connect(**DESTINO)
        cur_destino = conn_destino.cursor()
        
        # Query destino primeiro (mesma estrutura do script principal)
        query_destino = """
        SELECT dia, m05, m10, m15, h01, h04, h24, h96, estacao, estacao_id
        FROM pluviometricos
        WHERE dia >= %s::timestamp
          AND dia <= %s::timestamp
          AND estacao_id = %s
        ORDER BY dia ASC;
        """
        
        cur_destino.execute(query_destino, (data_inicial, data_final, estacao_id))
        dados_destino = cur_destino.fetchall()
        
        # Para cada registro do destino, buscar o correspondente no origem
        # IMPORTANTE: Usar mesma estrutura do script principal (validar_dados_pluviometricos.py)
        # Buscar no origem usando o timestamp do destino como referência
        dados_origem = []
        for reg_destino in dados_destino:
            dia_dest = reg_destino[0]
            est_id_dest = reg_destino[9]
            
            # Buscar no origem usando timestamp do destino
            # IMPORTANTE: Considerar ambos os timezones possíveis (-0200 e -0300)
            # O registro pode estar armazenado com qualquer um dos timezones
            # Tentar buscar com ambos os timezones para encontrar o registro correto
            reg_origem = None
            
            # Tentar buscar com -0200 (horário de verão)
            query_origem_tz1 = """
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
            WHERE el."horaLeitura" = (%s::timestamp AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE '-02:00')::timestamp
              AND el.estacao_id = %s
            ORDER BY el."horaLeitura" ASC, el.estacao_id ASC, el.id DESC
            LIMIT 1;
            """
            
            # Tentar buscar com -0300 (horário padrão)
            query_origem_tz2 = """
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
            WHERE el."horaLeitura" = (%s::timestamp AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE '-03:00')::timestamp
              AND el.estacao_id = %s
            ORDER BY el."horaLeitura" ASC, el.estacao_id ASC, el.id DESC
            LIMIT 1;
            """
            
            # Buscar usando intervalo para considerar ambos os timezones possíveis (-0200 e -0300)
            # IMPORTANTE: Usar mesma estrutura do script principal, mas com busca flexível por intervalo
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
            WHERE el."horaLeitura" >= (%s::timestamp - INTERVAL '1 hour')
              AND el."horaLeitura" <= (%s::timestamp + INTERVAL '1 hour')
              AND el.estacao_id = %s
            ORDER BY el."horaLeitura" ASC, el.estacao_id ASC, el.id DESC
            LIMIT 1;
            """
            
            cur_origem.execute(query_origem, (dia_dest, dia_dest, est_id_dest))
            reg_origem = cur_origem.fetchone()
            
            if reg_origem:
                dados_origem.append(reg_origem)
        
        print(f"Registros no banco ORIGEM: {len(dados_origem)}")
        print(f"Registros no banco DESTINO: {len(dados_destino)}")
        print()
        
        if len(dados_origem) != len(dados_destino):
            print(f"[AVISO] Numero de registros diferente! Origem: {len(dados_origem)}, Destino: {len(dados_destino)}")
            print()
        
        # Comparar registros
        print("=" * 80)
        print("COMPARACAO DETALHADA")
        print("=" * 80)
        print()
        
        # Função para normalizar timestamp (remover timezone para comparação)
        # IMPORTANTE: Preserva o valor local do timestamp, igual ao script principal
        def normalizar_timestamp(ts):
            """Remove timezone do timestamp mantendo o valor local (como o script principal faz)."""
            if ts is None:
                return None
            # Se tem timezone, remover mantendo o valor local (não converter para UTC)
            # Isso é igual ao que o script principal faz em tornar_datetime_naive()
            if hasattr(ts, 'tzinfo') and ts.tzinfo is not None:
                # Remover timezone mantendo o valor local (não converter)
                return ts.replace(tzinfo=None)
            return ts
        
        # Criar dicionário de destino por timestamp normalizado
        # IMPORTANTE: Usar mesma lógica do script principal para comparação
        destino_dict = {}
        for reg in dados_destino:
            dia = reg[0]
            dia_normalizado = normalizar_timestamp(dia)
            # Criar chave usando data/hora/minuto (ignorar segundos para comparação flexível)
            if isinstance(dia_normalizado, datetime):
                chave = dia_normalizado.replace(second=0, microsecond=0)
            else:
                chave = dia_normalizado
            if chave not in destino_dict:
                destino_dict[chave] = []
            destino_dict[chave].append(reg)
        
        divergencias = 0
        registros_ok = 0
        
        # Normalizar para comparação (tratando None, float e Decimal)
        def normalizar_valor(val):
            """Normaliza valores para comparação (None, float, Decimal -> float ou None)"""
            if val is None:
                return None
            try:
                return float(val)
            except (TypeError, ValueError):
                return val
        
        # Comparar com tolerância para floats (evitar problemas de precisão)
        # IMPORTANTE: Usar mesma lógica do script principal, mas com normalização
        def valores_iguais(orig, dest):
            """Verifica se dois valores são iguais, considerando precisão de float"""
            if orig is None and dest is None:
                return True
            if orig is None or dest is None:
                return False
            # Primeiro tentar comparação direta (como o script principal faz)
            if orig == dest:
                return True
            # Se não for igual direto, normalizar para float e comparar com tolerância
            try:
                orig_float = float(orig)
                dest_float = float(dest)
                diff = abs(orig_float - dest_float)
                # Tolerância de 0.0001 para diferenças de precisão
                return diff <= 0.0001
            except (TypeError, ValueError):
                return orig == dest
        
        # Comparar registros - usar mesma lógica do script principal
        # IMPORTANTE: Buscar no destino usando timestamp normalizado do origem
        for reg_origem in dados_origem:
            dia_orig = reg_origem[0]
            dia_orig_normalizado = normalizar_timestamp(dia_orig)
            m05_orig, m10_orig, m15_orig, h01_orig, h04_orig, h24_orig, h96_orig = reg_origem[1:8]
            estacao_orig = reg_origem[8]
            est_id_orig = reg_origem[9]
            
            # Buscar registro correspondente no destino usando timestamp normalizado
            # Criar chave de busca usando data/hora/minuto (ignorar segundos)
            if isinstance(dia_orig_normalizado, datetime):
                chave_busca = dia_orig_normalizado.replace(second=0, microsecond=0)
            else:
                chave_busca = dia_orig_normalizado
            
            reg_destino = None
            dia_dest_match = None
            
            # Tentar encontrar por chave exata
            if chave_busca in destino_dict:
                candidatos = destino_dict[chave_busca]
                # Se houver apenas um, usar ele
                if len(candidatos) == 1:
                    reg_destino = candidatos[0]
                    dia_dest_match = candidatos[0][0]
                else:
                    # Se houver múltiplos, encontrar o mais próximo
                    melhor_match = None
                    menor_diff = None
                    for candidato in candidatos:
                        dia_cand = normalizar_timestamp(candidato[0])
                        if isinstance(dia_orig_normalizado, datetime) and isinstance(dia_cand, datetime):
                            diff = abs((dia_orig_normalizado - dia_cand).total_seconds())
                            if menor_diff is None or diff < menor_diff:
                                menor_diff = diff
                                melhor_match = candidato
                    if melhor_match and (menor_diff is None or menor_diff <= 60):
                        reg_destino = melhor_match
                        dia_dest_match = melhor_match[0]
            else:
                # Se não encontrou exato, buscar por proximidade (até 1 hora para considerar timezone)
                melhor_match = None
                menor_diff = None
                for chave_dest, candidatos in destino_dict.items():
                    if isinstance(chave_busca, datetime) and isinstance(chave_dest, datetime):
                        try:
                            diff_seconds = abs((chave_busca - chave_dest).total_seconds())
                            # Tolerância de 1 hora para considerar diferenças de timezone (-0200 vs -0300)
                            if diff_seconds <= 3600:
                                if menor_diff is None or diff_seconds < menor_diff:
                                    menor_diff = diff_seconds
                                    melhor_match = candidatos[0]
                        except (TypeError, AttributeError):
                            continue
                if melhor_match:
                    reg_destino = melhor_match
                    dia_dest_match = melhor_match[0]
            
            if reg_destino:
                dia_dest = reg_destino[0]
                m05_dest, m10_dest, m15_dest, h01_dest, h04_dest, h24_dest, h96_dest = reg_destino[1:8]
                m05_dest, m10_dest, m15_dest, h01_dest, h04_dest, h24_dest, h96_dest = reg_destino[1:8]
                
                # Comparar valores
                valores_origem = (m05_orig, m10_orig, m15_orig, h01_orig, h04_orig, h24_orig, h96_orig)
                valores_destino = (m05_dest, m10_dest, m15_dest, h01_dest, h04_dest, h24_dest, h96_dest)
                
                valores_origem_norm = tuple(normalizar_valor(v) for v in valores_origem)
                valores_destino_norm = tuple(normalizar_valor(v) for v in valores_destino)
                
                # Verificar se todos os valores são iguais
                todos_iguais = all(valores_iguais(orig, dest) for orig, dest in zip(valores_origem_norm, valores_destino_norm))
                
                if todos_iguais:
                    registros_ok += 1
                    print(f"[OK] {dia_orig_normalizado} - Valores corretos")
                else:
                    divergencias += 1
                    print(f"[ERRO] {dia_orig_normalizado} - Valores diferentes:")
                    print(f"   Timestamp origem: {dia_orig} (tipo: {type(dia_orig)}, normalizado: {dia_orig_normalizado})")
                    print(f"   Timestamp destino: {dia_dest_match} (tipo: {type(dia_dest_match)}, normalizado: {normalizar_timestamp(dia_dest_match)})")
                    campos = ['m05', 'm10', 'm15', 'h01', 'h04', 'h24', 'h96']
                    for i, (orig, dest) in enumerate(zip(valores_origem_norm, valores_destino_norm)):
                        if not valores_iguais(orig, dest):
                            diff = abs(float(orig) - float(dest)) if orig is not None and dest is not None else None
                            diff_str = f" (diff: {diff:.6f})" if diff is not None else ""
                            print(f"   {campos[i]}: ORIGEM={valores_origem[i]} (tipo: {type(valores_origem[i])}) | DESTINO={valores_destino[i]} (tipo: {type(valores_destino[i])}){diff_str}")
            else:
                divergencias += 1
                print(f"[ERRO] {dia_orig_normalizado} - Registro ausente no banco destino")
                print(f"   Timestamp origem: {dia_orig} (tipo: {type(dia_orig)})")
                print(f"   Timestamps disponíveis no destino: {[normalizar_timestamp(r[0]) for r in dados_destino[:5]]}")
        
        print()
        print("=" * 80)
        print("RESUMO")
        print("=" * 80)
        print(f"Registros corretos: {registros_ok}")
        print(f"Divergencias: {divergencias}")
        print()
        
        if divergencias == 0:
            print("[OK] Todos os registros estao corretos!")
        else:
            print("[AVISO] Foram encontradas divergencias.")
            print("Execute o script de carga novamente para corrigir:")
            print("  python scripts/carregar_pluviometricos_historicos.py")
        
        print("=" * 80)
        
    except Exception as e:
        print(f'\n[ERRO] Erro na verificacao: {e}')
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
    
    # Período do exemplo do usuário
    data_inicial = '2009-10-27 23:00:00.000'
    data_final = '2009-10-28 01:00:00.000'
    estacao_id = 11
    
    if len(sys.argv) > 1:
        print("Uso: python verificar_periodo_especifico.py [data_inicial] [data_final] [estacao_id]")
        print()
        print("Exemplo:")
        print("  python verificar_periodo_especifico.py '2009-10-27 23:00:00.000' '2009-10-28 01:00:00.000' 11")
        print()
        if sys.argv[1] == '--help' or sys.argv[1] == '-h':
            return
        
        if len(sys.argv) >= 4:
            data_inicial = sys.argv[1]
            data_final = sys.argv[2]
            estacao_id = int(sys.argv[3])
    
    verificar_periodo(data_inicial, data_final, estacao_id)

if __name__ == "__main__":
    main()

