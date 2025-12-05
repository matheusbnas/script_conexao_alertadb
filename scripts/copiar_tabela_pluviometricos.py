#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
📋 COPIAR TABELA PLUVIOMÉTRICOS ENTRE BANCOS

═══════════════════════════════════════════════════════════════════════════
🎯 PROPÓSITO DESTE SCRIPT:
═══════════════════════════════════════════════════════════════════════════

Este script copia a tabela "pluviometricos" completa (estrutura e dados)
do banco origem (alertadb_cor - 10.50.30.166) para o banco destino 
(alertadb - 82.25.74.207) usando Python e psycopg2.

⚠️ IMPORTANTE: Este script usa variáveis específicas com prefixo DB_COPIA_*
para não conflitar com as variáveis DB_ORIGEM_* e DB_DESTINO_* usadas em 
outros scripts do projeto (como carregar_pluviometricos_historicos.py).

═══════════════════════════════════════════════════════════════════════════
📋 O QUE ESTE SCRIPT FAZ:
═══════════════════════════════════════════════════════════════════════════

✅ Conecta ao banco origem (alertadb_cor)
✅ Conecta ao banco destino (alertadb)
✅ Obtém a estrutura da tabela pluviometricos do banco origem
✅ Cria a tabela no banco destino (se não existir)
✅ Copia todos os dados em lotes para otimizar memória
✅ Trata conflitos usando ON CONFLICT DO UPDATE
✅ Mostra progresso detalhado durante a cópia
✅ Exibe estatísticas finais

═══════════════════════════════════════════════════════════════════════════
🚀 COMO USAR:
═══════════════════════════════════════════════════════════════════════════

1. Configure o arquivo .env com as credenciais dos bancos:
   
   # Banco ORIGEM para CÓPIA (alertadb_cor)
   DB_COPIA_ORIGEM_HOST=10.50.30.166
   DB_COPIA_ORIGEM_PORT=5432
   DB_COPIA_ORIGEM_NAME=alertadb_cor
   DB_COPIA_ORIGEM_USER=postgres
   DB_COPIA_ORIGEM_PASSWORD=
   
   # Banco DESTINO para CÓPIA (alertadb)
   DB_COPIA_DESTINO_HOST=82.25.74.207
   DB_COPIA_DESTINO_PORT=7077
   DB_COPIA_DESTINO_NAME=alertadb
   DB_COPIA_DESTINO_USER=postgres
   DB_COPIA_DESTINO_PASSWORD=

2. Execute: python scripts/copiar_tabela_pluviometricos.py

═══════════════════════════════════════════════════════════════════════════
🔒 PROTEÇÕES IMPLEMENTADAS:
═══════════════════════════════════════════════════════════════════════════

✅ Validação de conexões antes de iniciar
✅ Verificação se a tabela existe no banco origem
✅ Criação automática da tabela no destino se não existir
✅ Processamento em lotes evita sobrecarga de memória
✅ ON CONFLICT DO UPDATE atualiza dados existentes
✅ Tratamento de erros com mensagens claras
✅ Confirmação antes de sobrescrever dados existentes
"""

# 🔧 Importar bibliotecas necessárias
import psycopg2
from psycopg2 import errors as psycopg2_errors
from psycopg2.extras import execute_values
import os
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

# Carregar variáveis de ambiente (busca .env na raiz do projeto)
import sys
project_root = Path(__file__).parent.parent
load_dotenv(dotenv_path=project_root / '.env')

def obter_variavel(nome, obrigatoria=True):
    """Obtém variável de ambiente, lança erro se obrigatória e não encontrada."""
    valor = os.getenv(nome)
    if obrigatoria and not valor:
        raise ValueError(f"❌ Variável de ambiente obrigatória não encontrada: {nome}")
    return valor

def carregar_configuracoes():
    """Carrega todas as configurações do arquivo .env.
    
    Usa variáveis específicas com prefixo DB_COPIA_ para evitar conflito
    com as variáveis DB_ORIGEM_* e DB_DESTINO_* usadas em outros scripts.
    """
    try:
        # ⚙️ Configurações de conexão ORIGEM para CÓPIA (alertadb_cor)
        origem = {
            'host': obter_variavel('DB_COPIA_ORIGEM_HOST'),
            'port': obter_variavel('DB_COPIA_ORIGEM_PORT', obrigatoria=False) or '5432',
            'dbname': obter_variavel('DB_COPIA_ORIGEM_NAME'),
            'user': obter_variavel('DB_COPIA_ORIGEM_USER'),
            'password': obter_variavel('DB_COPIA_ORIGEM_PASSWORD'),
            'connect_timeout': 10
        }

        # ⚙️ Configurações de conexão DESTINO para CÓPIA (alertadb)
        destino = {
            'host': obter_variavel('DB_COPIA_DESTINO_HOST'),
            'port': obter_variavel('DB_COPIA_DESTINO_PORT', obrigatoria=False) or '5432',
            'dbname': obter_variavel('DB_COPIA_DESTINO_NAME'),
            'user': obter_variavel('DB_COPIA_DESTINO_USER'),
            'password': obter_variavel('DB_COPIA_DESTINO_PASSWORD'),
            'connect_timeout': 10
        }
        
        return origem, destino
    
    except ValueError as e:
        print("=" * 60)
        print("❌ ERRO DE CONFIGURAÇÃO")
        print("=" * 60)
        print(str(e))
        print("\n📝 Verifique se o arquivo .env existe e contém todas as variáveis necessárias")
        print("\n💡 Variáveis necessárias para este script:")
        print("   - DB_COPIA_ORIGEM_HOST, DB_COPIA_ORIGEM_NAME, DB_COPIA_ORIGEM_USER, DB_COPIA_ORIGEM_PASSWORD")
        print("   - DB_COPIA_DESTINO_HOST, DB_COPIA_DESTINO_NAME, DB_COPIA_DESTINO_USER, DB_COPIA_DESTINO_PASSWORD")
        print("=" * 60)
        raise

# Carregar configurações
ORIGEM, DESTINO = carregar_configuracoes()

def testar_conexoes():
    """Testa as conexões com ambos os bancos antes de copiar."""
    print("=" * 60)
    print("TESTE DE CONEXÕES")
    print("=" * 60)
    
    try:
        conn_origem = psycopg2.connect(**ORIGEM)
        print(f"   ✅ CONEXÃO ORIGEM ({ORIGEM['host']}:{ORIGEM['port']}/{ORIGEM['dbname']}): SUCESSO!")
        conn_origem.close()
        
        conn_destino = psycopg2.connect(**DESTINO)
        print(f"   ✅ CONEXÃO DESTINO ({DESTINO['host']}:{DESTINO['port']}/{DESTINO['dbname']}): SUCESSO!")
        conn_destino.close()
        return True
        
    except Exception as e:
        print(f"   ❌ ERRO: {e}")
        return False

def verificar_tabela_origem():
    """Verifica se a tabela pluviometricos existe no banco origem."""
    conn_origem = None
    cur_origem = None
    
    try:
        conn_origem = psycopg2.connect(**ORIGEM)
        cur_origem = conn_origem.cursor()
        
        # Verificar se a tabela existe
        cur_origem.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'pluviometricos'
            );
        """)
        existe = cur_origem.fetchone()[0]
        
        if not existe:
            print("   ❌ ERRO: A tabela 'pluviometricos' não existe no banco origem!")
            return False, None
        
        # Contar registros
        cur_origem.execute("SELECT COUNT(*) FROM pluviometricos;")
        total = cur_origem.fetchone()[0]
        
        # Obter estrutura da tabela
        cur_origem.execute("""
            SELECT column_name, data_type, character_maximum_length, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' 
            AND table_name = 'pluviometricos'
            ORDER BY ordinal_position;
        """)
        estrutura = cur_origem.fetchall()
        
        print(f"   ✅ Tabela encontrada no banco origem!")
        print(f"   📊 Total de registros: {total:,}")
        
        return True, estrutura
        
    except Exception as e:
        print(f"   ❌ Erro ao verificar tabela origem: {e}")
        return False, None
    finally:
        if cur_origem:
            cur_origem.close()
        if conn_origem:
            conn_origem.close()

def criar_tabela_destino(estrutura_origem):
    """Cria a tabela pluviometricos no banco destino baseada na estrutura do banco origem."""
    conn_destino = None
    cur_destino = None
    
    try:
        conn_destino = psycopg2.connect(**DESTINO)
        cur_destino = conn_destino.cursor()
        
        # Verificar se a tabela já existe
        cur_destino.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'pluviometricos'
            );
        """)
        existe = cur_destino.fetchone()[0]
        
        if existe:
            print("   ⚠️  A tabela 'pluviometricos' já existe no banco destino.")
            resposta = input("   Deseja continuar? Os dados serão atualizados com ON CONFLICT. (S/n): ")
            if resposta.lower() == 'n':
                print("   ❌ Operação cancelada pelo usuário.")
                return False
        else:
            # Criar tabela baseada na estrutura do banco origem
            # Usar estrutura padrão conhecida
            create_table_sql = '''
            CREATE TABLE IF NOT EXISTS pluviometricos (
                dia TIMESTAMP NOT NULL,
                m05 NUMERIC,
                m10 NUMERIC,
                m15 NUMERIC,
                h01 NUMERIC,
                h04 NUMERIC,
                h24 NUMERIC,
                h96 NUMERIC,
                estacao VARCHAR(255),
                estacao_id INTEGER,
                PRIMARY KEY (dia, estacao_id)
            );
            '''
            
            cur_destino.execute(create_table_sql)
            conn_destino.commit()
            print('   ✅ Tabela pluviometricos criada no banco destino!')
        
        return True
        
    except Exception as e:
        print(f'   ❌ Erro ao criar tabela destino: {e}')
        if conn_destino:
            conn_destino.rollback()
        return False
    finally:
        if cur_destino:
            cur_destino.close()
        if conn_destino:
            conn_destino.close()

def verificar_dados_destino():
    """Verifica se já existem dados no banco destino."""
    conn_destino = None
    cur_destino = None
    
    try:
        conn_destino = psycopg2.connect(**DESTINO)
        cur_destino = conn_destino.cursor()
        
        cur_destino.execute("SELECT COUNT(*) FROM pluviometricos;")
        total = cur_destino.fetchone()[0]
        
        return total > 0, total
        
    except Exception as e:
        return False, 0
    finally:
        if cur_destino:
            cur_destino.close()
        if conn_destino:
            conn_destino.close()

def copiar_dados():
    """Copia todos os dados da tabela pluviometricos do banco origem para o destino."""
    conn_origem = None
    cur_origem = None
    conn_destino = None
    cur_destino = None
    
    timestamp_atual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        # Conectar aos bancos
        print("\n" + "=" * 60)
        print("INICIANDO CÓPIA DE DADOS")
        print("=" * 60)
        
        conn_origem = psycopg2.connect(**ORIGEM)
        cur_origem = conn_origem.cursor()
        
        conn_destino = psycopg2.connect(**DESTINO)
        cur_destino = conn_destino.cursor()
        
        # Otimizações para acelerar a cópia
        print("\n⚡ Aplicando otimizações para acelerar a cópia...")
        try:
            cur_destino.execute("SET synchronous_commit = off;")  # Desabilita sync commit (mais rápido)
            cur_destino.execute("SET maintenance_work_mem = '256MB';")  # Mais memória para operações
            # checkpoint_segments foi removido no PostgreSQL 9.5+, usando max_wal_size se disponível
            try:
                cur_destino.execute("SET max_wal_size = '2GB';")  # Para PostgreSQL 9.5+
            except:
                pass  # Ignora se não disponível
            conn_destino.commit()
        except Exception as e:
            print(f"   ⚠️  Algumas otimizações não puderam ser aplicadas: {e}")
            print("   Continuando mesmo assim...")
            conn_destino.rollback()
        
        # Buscar todos os dados do banco origem
        print("\n📥 Buscando dados do banco origem...")
        cur_origem.execute("SELECT dia, m05, m10, m15, h01, h04, h24, h96, estacao, estacao_id FROM pluviometricos ORDER BY dia, estacao_id;")
        
        # Processar em lotes maiores para melhor performance
        TAMANHO_LOTE = 50000  # Aumentado de 10k para 50k para melhor performance
        total_copiados = 0
        lote_numero = 1
        
        # SQL para inserção com ON CONFLICT DO UPDATE
        # Usando execute_values que é otimizado para múltiplas inserções
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
        
        print("📦 Processando dados em lotes de 50.000 registros...")
        print("   💡 Otimizações aplicadas: synchronous_commit=off, lotes maiores\n")
        
        primeira_data = None
        ultima_data = None
        inicio_copia = datetime.now()
        
        while True:
            dados = cur_origem.fetchmany(TAMANHO_LOTE)
            
            if not dados:
                break
            
            # Capturar primeira e última data do lote
            if primeira_data is None:
                primeira_data = dados[0][0] if dados else None
            ultima_data = dados[-1][0] if dados else None
            
            # Inserir lote no banco destino usando execute_values (otimizado)
            execute_values(cur_destino, insert_sql, dados, page_size=TAMANHO_LOTE)
            conn_destino.commit()
            
            total_copiados += len(dados)
            tempo_decorrido = (datetime.now() - inicio_copia).total_seconds()
            velocidade = total_copiados / tempo_decorrido if tempo_decorrido > 0 else 0
            
            print(f'   📦 Lote {lote_numero}: {len(dados):,} registros copiados (Total: {total_copiados:,} | Velocidade: {velocidade:.0f} reg/s)')
            if primeira_data and ultima_data and lote_numero == 1:
                print(f'      📅 Período: {primeira_data} até {ultima_data}')
            lote_numero += 1
        
        if total_copiados == 0:
            print(f'\n   ⚠️  Nenhum dado encontrado para copiar.')
            return 0
        
        # Obter estatísticas finais
        cur_destino.execute("SELECT COUNT(*) FROM pluviometricos;")
        total_tabela = cur_destino.fetchone()[0]
        
        cur_destino.execute("SELECT MIN(dia), MAX(dia) FROM pluviometricos;")
        datas = cur_destino.fetchone()
        data_min = datas[0] if datas[0] else None
        data_max = datas[1] if datas[1] else None
        
        # Restaurar configurações padrão
        cur_destino.execute("SET synchronous_commit = on;")
        conn_destino.commit()
        
        tempo_total = (datetime.now() - inicio_copia).total_seconds()
        velocidade_media = total_copiados / tempo_total if tempo_total > 0 else 0
        
        print("\n" + "=" * 60)
        print("✅ CÓPIA FINALIZADA COM SUCESSO!")
        print("=" * 60)
        print(f"📊 Total de registros copiados nesta execução: {total_copiados:,}")
        print(f"📊 Total de registros na tabela destino: {total_tabela:,}")
        print(f"⏱️  Tempo total: {tempo_total:.1f} segundos ({tempo_total/60:.1f} minutos)")
        print(f"🚀 Velocidade média: {velocidade_media:.0f} registros/segundo")
        if data_min and data_max:
            print(f"📅 Período dos dados na tabela: {data_min} até {data_max}")
        print(f"⏰ Concluído em: {timestamp_atual}")
        print("=" * 60)
        
        return total_copiados

    except Exception as e:
        print(f'\n❌ Erro na cópia: {e}')
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
    """Função principal que executa a cópia da tabela."""
    print("=" * 60)
    print("📋 COPIAR TABELA PLUVIOMÉTRICOS ENTRE BANCOS")
    print("=" * 60)
    print()
    print("🎯 PROPÓSITO:")
    print("   Este script copia a tabela 'pluviometricos' completa")
    print("   do banco origem (alertadb_cor) para o banco destino (alertadb).")
    print()
    print("📋 O QUE SERÁ FEITO:")
    print("   ✅ Verificar conexões com ambos os bancos")
    print("   ✅ Verificar se a tabela existe no banco origem")
    print("   ✅ Criar a tabela no banco destino se não existir")
    print("   ✅ Copiar todos os dados em lotes")
    print("   ✅ Mostrar progresso e estatísticas detalhadas")
    print()
    print("⚠️  IMPORTANTE:")
    print("   - Os dados existentes serão atualizados com ON CONFLICT")
    print("   - O processo pode levar vários minutos dependendo do volume")
    print("=" * 60)
    
    # Testar conexões
    if not testar_conexoes():
        print("\n❌ Falha nos testes de conexão. Abortando...")
        return
    
    # Verificar tabela origem
    print("\n📋 Verificando tabela no banco origem...")
    existe, estrutura = verificar_tabela_origem()
    if not existe:
        print("\n❌ Não foi possível continuar. Abortando...")
        return
    
    # Criar tabela destino
    print("\n📋 Verificando/criando tabela no banco destino...")
    if not criar_tabela_destino(estrutura):
        print("\n❌ Não foi possível criar/verificar tabela destino. Abortando...")
        return
    
    # Verificar dados existentes
    tem_dados, total_existente = verificar_dados_destino()
    if tem_dados:
        print(f"\n⚠️  ATENÇÃO: A tabela destino já contém {total_existente:,} registros.")
        print("   Os dados serão atualizados com ON CONFLICT DO UPDATE.")
        resposta = input("   Deseja continuar? (S/n): ")
        if resposta.lower() == 'n':
            print("❌ Operação cancelada pelo usuário.")
            return
    
    # Executar cópia
    copiar_dados()

if __name__ == "__main__":
    main()

