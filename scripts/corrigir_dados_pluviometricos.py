#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para corrigir dados pluviométricos já inseridos no banco destino,
garantindo que correspondam exatamente aos dados do banco original.
"""

import psycopg2
from psycopg2.extras import execute_values
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
        raise ValueError(f"❌ Variável de ambiente obrigatória não encontrada: {nome}")
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

def query_dados_origem_com_distinct():
    """Query que usa DISTINCT ON para garantir apenas um registro por (dia, estacao_id),
    mantendo o registro com o maior ID (mais recente).
    """
    return """
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
    ORDER BY el."horaLeitura" ASC, el.estacao_id ASC, el.id DESC;
    """

def corrigir_dados_periodo(data_inicial, data_final, estacao_id=None):
    """Corrige dados de um período específico."""
    
    conn_origem = None
    cur_origem = None
    conn_destino = None
    cur_destino = None
    
    try:
        print("=" * 80)
        print("🔧 CORREÇÃO DE DADOS PLUVIOMÉTRICOS")
        print("=" * 80)
        print(f"Período: {data_inicial} até {data_final}")
        if estacao_id:
            print(f"Estação ID: {estacao_id}")
        print()
        
        # Conectar ao banco origem
        conn_origem = psycopg2.connect(**ORIGEM)
        cur_origem = conn_origem.cursor()
        
        # Buscar dados do banco origem usando DISTINCT ON
        print("📥 Buscando dados do banco origem...")
        query_origem = f"""
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
        WHERE el."horaLeitura" >= '{data_inicial}'::timestamp
          AND el."horaLeitura" <= '{data_final}'::timestamp
        """
        
        if estacao_id:
            query_origem += f" AND el.estacao_id = {estacao_id}"
        
        query_origem += """
        ORDER BY el."horaLeitura" ASC, el.estacao_id ASC, el.id DESC;
        """
        
        cur_origem.execute(query_origem)
        dados_origem = cur_origem.fetchall()
        
        print(f"   ✅ Encontrados {len(dados_origem):,} registros no banco origem")
        
        if not dados_origem:
            print("   ⚠️  Nenhum dado encontrado para corrigir")
            return 0
        
        # Conectar ao banco destino
        conn_destino = psycopg2.connect(**DESTINO)
        cur_destino = conn_destino.cursor()
        
        # Usar UPDATE ... ON CONFLICT para atualizar ou inserir
        print("\n📤 Atualizando dados no banco destino...")
        
        # Preparar dados para inserção/atualização
        # Formato: (dia, m05, m10, m15, h01, h04, h24, h96, estacao, estacao_id)
        
        update_sql = """
        INSERT INTO pluviometricos
        (dia, m05, m10, m15, h01, h04, h24, h96, estacao, estacao_id)
        VALUES %s
        ON CONFLICT (dia, estacao_id) 
        DO UPDATE SET
            m05 = EXCLUDED.m05,
            m10 = EXCLUDED.m10,
            m15 = EXCLUDED.m15,
            h01 = EXCLUDED.h01,
            h04 = EXCLUDED.h04,
            h24 = EXCLUDED.h24,
            h96 = EXCLUDED.h96,
            estacao = EXCLUDED.estacao;
        """
        
        # Processar em lotes
        TAMANHO_LOTE = 10000
        total_atualizados = 0
        lote_numero = 1
        
        for i in range(0, len(dados_origem), TAMANHO_LOTE):
            lote = dados_origem[i:i + TAMANHO_LOTE]
            execute_values(cur_destino, update_sql, lote)
            conn_destino.commit()
            
            total_atualizados += len(lote)
            print(f"   📦 Lote {lote_numero}: {len(lote):,} registros processados (Total: {total_atualizados:,})")
            lote_numero += 1
        
        print("\n" + "=" * 80)
        print("✅ CORREÇÃO CONCLUÍDA!")
        print("=" * 80)
        print(f"📊 Total de registros atualizados: {total_atualizados:,}")
        print(f"⏰ Concluído em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        return total_atualizados
        
    except Exception as e:
        print(f'\n❌ Erro na correção: {e}')
        import traceback
        traceback.print_exc()
        if conn_destino:
            conn_destino.rollback()
        return 0
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
    print("🔧 CORREÇÃO DE DADOS PLUVIOMÉTRICOS")
    print("=" * 80)
    print()
    print("Este script corrige dados já inseridos no banco destino,")
    print("garantindo que correspondam exatamente aos dados do banco original.")
    print()
    
    # Corrigir o período específico mencionado pelo usuário
    data_inicial = '2019-02-15 23:00:00.000'
    data_final = '2019-02-18 01:00:00.000'
    estacao_id = 14
    
    resposta = input(f"Corrigir dados do período {data_inicial} até {data_final} para estação {estacao_id}? (S/n): ")
    if resposta.lower() == 'n':
        print("Operação cancelada.")
        return
    
    corrigir_dados_periodo(data_inicial, data_final, estacao_id)
    
    print("\n💡 DICA: Para corrigir outros períodos, execute este script novamente")
    print("   ou modifique as datas diretamente no código.")

if __name__ == "__main__":
    main()

