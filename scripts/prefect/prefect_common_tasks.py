#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔄 TASKS COMUNS PREFECT - BigQuery

Módulo com tasks compartilhadas entre workflows de pluviométricos e meteorológicos.
"""

from prefect import task
from pathlib import Path
import os
from dotenv import load_dotenv

# Caminho base do projeto (scripts/prefect -> scripts -> raiz)
project_root = Path(__file__).parent.parent.parent

@task(name="Verificar Conexão NIMBUS", log_prints=True)
def verificar_conexao_nimbus() -> bool:
    """Verifica se a conexão com o banco NIMBUS está disponível."""
    try:
        import psycopg2
        
        # Carregar variáveis de ambiente
        load_dotenv(dotenv_path=project_root / '.env')
        
        # Obter configurações do .env
        def obter_variavel(nome, obrigatoria=True, padrao=None):
            valor = os.getenv(nome)
            if not valor or (isinstance(valor, str) and valor.strip() == ''):
                if obrigatoria:
                    raise ValueError(f"Variável obrigatória não encontrada: {nome}")
                return padrao
            return valor.strip() if isinstance(valor, str) else valor
        
        origem = {
            'host': obter_variavel('DB_ORIGEM_HOST'),
            'port': obter_variavel('DB_ORIGEM_PORT', obrigatoria=False, padrao='5432'),
            'dbname': obter_variavel('DB_ORIGEM_NAME'),
            'user': obter_variavel('DB_ORIGEM_USER'),
            'password': obter_variavel('DB_ORIGEM_PASSWORD'),
            'sslmode': obter_variavel('DB_ORIGEM_SSLMODE', obrigatoria=False, padrao='disable'),
            'connect_timeout': 10
        }
        
        # Testar conexão
        conn = psycopg2.connect(**origem)
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.close()
        conn.close()
        print(f"✅ Conexão NIMBUS OK: {origem['dbname']}@{origem['host']}:{origem['port']}")
        return True
    except Exception as e:
        print(f"❌ Erro ao verificar conexão NIMBUS: {e}")
        import traceback
        traceback.print_exc()
        return False

@task(name="Verificar Credenciais GCP", log_prints=True)
def verificar_credenciais_gcp() -> bool:
    """Verifica se as credenciais do GCP estão configuradas."""
    try:
        credentials_path = project_root / 'credentials' / 'credentials.json'
        if credentials_path.exists():
            print(f"✅ Credenciais GCP encontradas: {credentials_path}")
            return True
        else:
            print(f"⚠️  Credenciais GCP não encontradas em: {credentials_path}")
            return False
    except Exception as e:
        print(f"❌ Erro ao verificar credenciais GCP: {e}")
        return False

