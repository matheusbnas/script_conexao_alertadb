#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🌧️ CARGA INICIAL COMPLETA - Servidor 166 → Cloud SQL GCP

═══════════════════════════════════════════════════════════════════════════
🎯 PROPÓSITO DESTE SCRIPT:
═══════════════════════════════════════════════════════════════════════════

Este script faz a CARGA INICIAL COMPLETA de todos os dados pluviométricos
do banco alertadb_cor (servidor 166) para o Cloud SQL GCP.

É o PRIMEIRO PASSO necessário antes de usar o sincronizar_para_cloudsql.py.

ARQUITETURA:
    NIMBUS → Servidor 166 (alertadb_cor) → Cloud SQL GCP
              ↑ [sync existente]      ↑ [ESTE SCRIPT]

IMPORTANTE: Este script foi adaptado do carregar_pluviometricos_historicos.py
existente no projeto, mantendo a mesma lógica e estrutura.

═══════════════════════════════════════════════════════════════════════════
📋 O QUE ESTE SCRIPT FAZ:
═══════════════════════════════════════════════════════════════════════════

✅ Busca TODOS os dados do alertadb_cor (servidor 166)
✅ Cria a tabela pluviometricos no Cloud SQL se não existir
✅ Processa dados em lotes de 10.000 registros
✅ Usa ON CONFLICT DO UPDATE para garantir unicidade
✅ Mantém timezone original (-02:00 e -03:00)
✅ Mostra progresso detalhado durante a carga

═══════════════════════════════════════════════════════════════════════════
🚀 COMO USAR:
═══════════════════════════════════════════════════════════════════════════

1. Configure o arquivo .env com as credenciais do Cloud SQL
2. Execute: python carregar_para_cloudsql_inicial.py
3. Aguarde a conclusão
4. Depois execute sincronizar_para_cloudsql.py para manter atualizado
"""

import psycopg2
from psycopg2.extras import execute_values
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path

# Carregar variáveis de ambiente
project_root = Path(__file__).parent.parent
load_dotenv(dotenv_path=project_root / '.env')

def obter_variavel(nome, obrigatoria=True, padrao=None):
    """Obtém variável de ambiente."""
    valor = os.getenv(nome)
    if not valor or (isinstance(valor, str) and valor.strip() == ''):
        if obrigatoria:
            raise ValueError(f"❌ Variável obrigatória não encontrada: {nome}")
        return padrao
    return valor.strip() if isinstance(valor, str) else valor

def carregar_configuracoes():
    """Carrega configurações do .env."""
    try:
        # Banco ORIGEM - alertadb_cor no servidor 166 (LOCALHOST)
        origem = {
            'host': obter_variavel('DB_DESTINO_HOST', obrigatoria=False, padrao='localhost'),
            'port': obter_variavel('DB_DESTINO_PORT', obrigatoria=False, padrao='5432'),
            'dbname': obter_variavel('DB_DESTINO_NAME', obrigatoria=False, padrao='alertadb_cor'),
            'user': obter_variavel('DB_DESTINO_USER', obrigatoria=False, padrao='postgres'),
            'password': obter_variavel('DB_DESTINO_PASSWORD'),
        }

        # Banco DESTINO - Cloud SQL GCP
        destino = {
            'host': obter_variavel('CLOUDSQL_HOST', obrigatoria=False) or obter_variavel('DB_COPIA_DESTINO_HOST', obrigatoria=False, padrao='34.82.95.242'),
            'port': obter_variavel('CLOUDSQL_PORT', obrigatoria=False, padrao='5432'),
            'dbname': obter_variavel('CLOUDSQL_DATABASE', obrigatoria=False) or obter_variavel('DB_COPIA_DESTINO_NAME', obrigatoria=False, padrao='alertadb_cor'),
            'user': obter_variavel('CLOUDSQL_USER', obrigatoria=False) or obter_variavel('DB_COPIA_DESTINO_USER', obrigatoria=False, padrao='postgres'),
            'password': obter_variavel('CLOUDSQL_PASSWORD') or obter_variavel('DB_COPIA_DESTINO_PASSWORD'),
            'connect_timeout': 10,
            'sslmode': obter_variavel('CLOUDSQL_SSLMODE', obrigatoria=False, padrao='require')
        }
        
        return origem, destino
    
    except ValueError as e:
        print("=" * 70)
        print("❌ ERRO DE CONFIGURAÇÃO")
        print("=" * 70)
        print(str(e))
        print("\n📝 Configure no .env:")
        print("   DB_DESTINO_PASSWORD=senha_servidor_166")
        print("   CLOUDSQL_HOST=34.82.95.242")
        print("   CLOUDSQL_PASSWORD=senha_cloud_sql")
        print("=" * 70)
        raise

ORIGEM, DESTINO = carregar_configuracoes()

def testar_conexoes():
    """Testa conexões com ambos os bancos."""
    print("=" * 70)
    print("TESTE DE CONEXÕES")
    print("=" * 70)
    
    try:
        conn_origem = psycopg2.connect(**ORIGEM)
        print(f"   ✅ ORIGEM (Servidor 166): SUCESSO!")
        print(f"      {ORIGEM['dbname']}@{ORIGEM['host']}:{ORIGEM['port']}")
        conn_origem.close()
        
        conn_destino = psycopg2.connect(**DESTINO)
        print(f"   ✅ DESTINO (Cloud SQL): SUCESSO!")
        print(f"      {DESTINO['dbname']}@{DESTINO['host']}:{DESTINO['port']}")
        conn_destino.close()
        return True
        
    except Exception as e:
        print(f"   ❌ ERRO: {e}")
        return False

def criar_tabela_cloudsql():
    """Cria tabela no Cloud SQL se não existir."""
    conn_destino = None
    cur_destino = None
    
    try:
        conn_destino = psycopg2.connect(**DESTINO)
        cur_destino = conn_destino.cursor()
        
        create_table_sql = '''
        CREATE TABLE IF NOT EXISTS pluviometricos (
            dia TIMESTAMP WITH TIME ZONE NOT NULL,
            m05 NUMERIC,
            m10 NUMERIC,
            m15 NUMERIC,
            h01 NUMERIC,
            h04 NUMERIC,
            h24 NUMERIC,
            h96 NUMERIC,
            estacao VARCHAR(255),
            estacao_id INTEGER NOT NULL,
            PRIMARY KEY (dia, estacao_id)
        );
        
        CREATE INDEX IF NOT EXISTS idx_pluviometricos_dia ON pluviometricos(dia);
        CREATE INDEX IF NOT EXISTS idx_pluviometricos_estacao ON pluviometricos(estacao_id);
        '''
        
        cur_destino.execute(create_table_sql)
        conn_destino.commit()
        print('✅ Tabela pluviometricos criada/verificada no Cloud SQL!')
        
    except Exception as e:
        print(f'❌ Erro ao criar tabela: {e}')
        if conn_destino:
            conn_destino.rollback()
    finally:
        if cur_destino:
            cur_destino.close()
        if conn_destino:
            conn_destino.close()

def verificar_tabela_vazia_cloudsql():
    """Verifica se a tabela no Cloud SQL está vazia."""
    conn_destino = None
    cur_destino = None
    
    try:
        conn_destino = psycopg2.connect(**DESTINO)
        cur_destino = conn_destino.cursor()
        
        cur_destino.execute("SELECT EXISTS(SELECT 1 FROM pluviometricos LIMIT 1);")
        resultado = cur_destino.fetchone()
        
        return not resultado[0] if resultado else True
            
    except Exception as e:
        print(f'⚠️ Erro ao verificar tabela: {e}')
        return True
    finally:
        if cur_destino:
            cur_destino.close()
        if conn_destino:
            conn_destino.close()

def carregar_dados_completos():
    """Carrega todos os dados do servidor 166 para Cloud SQL."""
    conn_origem = None
    cur_origem = None
    conn_destino = None
    cur_destino = None
    
    timestamp_atual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        # Verificar se Cloud SQL já tem dados
        if not verificar_tabela_vazia_cloudsql():
            resposta = input("\n⚠️  Cloud SQL já contém dados. Continuar? (s/N): ")
            if resposta.lower() != 's':
                print("❌ Operação cancelada.")
                return 0
        
        print("\n🔄 Iniciando carga completa...")
        print("   Origem: alertadb_cor @ servidor 166")
        print("   Destino: alertadb_cor @ Cloud SQL GCP")
        print()
        
        # Conectar ao servidor 166
        conn_origem = psycopg2.connect(**ORIGEM)
        cur_origem = conn_origem.cursor()
        
        # Buscar TODOS os dados
        query = """
        SELECT dia, m05, m10, m15, h01, h04, h24, h96, estacao, estacao_id
        FROM pluviometricos
        ORDER BY dia ASC;
        """
        
        print("📦 Buscando dados do servidor 166...")
        cur_origem.execute(query)
        
        # Processar em lotes
        TAMANHO_LOTE = 10000
        total_inseridos = 0
        lote_numero = 1
        
        # Conectar ao Cloud SQL
        conn_destino = psycopg2.connect(**DESTINO)
        cur_destino = conn_destino.cursor()
        
        # Configurar timezone
        cur_destino.execute("SET timezone = 'America/Sao_Paulo';")
        
        # SQL de inserção
        insert_sql = '''
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
        '''
        
        print("📦 Processando dados em lotes de 10.000 registros...\n")
        
        primeira_data = None
        ultima_data = None
        
        while True:
            dados = cur_origem.fetchmany(TAMANHO_LOTE)
            
            if not dados:
                break
            
            # Capturar datas
            data_inicio_lote = dados[0][0] if dados else None
            data_fim_lote = dados[-1][0] if dados else None
            
            if primeira_data is None:
                primeira_data = data_inicio_lote
            ultima_data = data_fim_lote
            
            # Inserir no Cloud SQL
            execute_values(cur_destino, insert_sql, dados)
            conn_destino.commit()
            
            total_inseridos += len(dados)
            print(f'   📦 Lote {lote_numero}: {len(dados):,} registros (Total: {total_inseridos:,})')
            if data_inicio_lote and data_fim_lote:
                print(f'      📅 {data_inicio_lote} até {data_fim_lote}')
            lote_numero += 1
        
        if total_inseridos == 0:
            print('\n   ⚠️  Nenhum dado encontrado para inserir.')
            return 0
        
        # Estatísticas finais
        cur_destino.execute("SELECT COUNT(*), MIN(dia), MAX(dia) FROM pluviometricos;")
        stats = cur_destino.fetchone()
        total_tabela, data_min, data_max = stats
        
        print("\n" + "=" * 70)
        print("✅ CARGA INICIAL COMPLETA FINALIZADA!")
        print("=" * 70)
        print(f"📊 Total inserido: {total_inseridos:,}")
        print(f"📊 Total no Cloud SQL: {total_tabela:,}")
        if data_min and data_max:
            print(f"📅 Período: {data_min} até {data_max}")
        print(f"⏰ Concluído em: {timestamp_atual}")
        print("=" * 70)
        
        print("\n💡 PRÓXIMO PASSO:")
        print("   Execute 'sincronizar_para_cloudsql.py' para manter")
        print("   os dados atualizados em tempo real.\n")
        
        return total_inseridos

    except Exception as e:
        print(f'\n❌ Erro na carga: {e}')
        import traceback
        traceback.print_exc()
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
    print("=" * 70)
    print("🌧️ CARGA INICIAL - Servidor 166 → Cloud SQL GCP")
    print("=" * 70)
    print()
    print("🎯 PROPÓSITO:")
    print("   Copiar TODOS os dados do alertadb_cor (servidor 166)")
    print("   para o Cloud SQL GCP")
    print()
    print("📋 O QUE SERÁ FEITO:")
    print("   ✅ Buscar todos os dados do servidor 166")
    print("   ✅ Criar tabela no Cloud SQL se não existir")
    print("   ✅ Processar em lotes de 10.000 registros")
    print("   ✅ Manter timezone e formato original")
    print()
    print("⚠️  IMPORTANTE:")
    print("   - Execute APENAS UMA VEZ para carga inicial")
    print("   - Depois use sincronizar_para_cloudsql.py")
    print("=" * 70)
    
    # Testar conexões
    if not testar_conexoes():
        print("\n❌ Falha nas conexões. Abortando...")
        return

    # Criar tabela
    print("\n📋 Verificando estrutura do Cloud SQL...")
    criar_tabela_cloudsql()

    # Executar carga
    carregar_dados_completos()

if __name__ == "__main__":
    main()
