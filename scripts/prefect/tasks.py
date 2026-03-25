#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Tasks Prefect compartilhadas pelos workflows de sincronização NIMBUS → BigQuery.

Organização:
  - Utilitários de ambiente
  - Tasks de verificação de infraestrutura
  - Tasks de sincronização (pluviométricos e meteorológicos)
  - Tasks de monitoramento (status BigQuery, lacunas)
  - [STUB] Task de inferência IA — integração futura LNCC/Gypscie
"""

import os
import traceback
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from prefect import task

from constants import (
    BIGQUERY_DEFAULT_DATASET,
    PROJECT_ROOT,
    QUERY_NIMBUS_METEO,
    QUERY_NIMBUS_PLUVIO,
    SCRIPT_SYNC_METEO,
    SCRIPT_SYNC_PLUVIO,
    Tabela,
)
from utils import (
    executar_script_sincronizacao,
    verificar_lacunas_tabela,
    verificar_status_bigquery_tabela,
)


# ---------------------------------------------------------------------------
# Utilitários de ambiente
# ---------------------------------------------------------------------------

def obter_variavel(nome: str, obrigatoria: bool = True, padrao: str = None) -> str:
    """Retorna variável de ambiente com validação opcional."""
    valor = os.getenv(nome, padrao)
    if obrigatoria and (not valor or (isinstance(valor, str) and not valor.strip())):
        raise ValueError(f"Variável obrigatória não encontrada: {nome}. Verifique o arquivo .env")
    return valor.strip() if isinstance(valor, str) else valor


# ---------------------------------------------------------------------------
# Verificação de infraestrutura
# ---------------------------------------------------------------------------

@task(name="Verificar Conexão NIMBUS", log_prints=True)
def verificar_conexao_nimbus() -> bool:
    """Verifica se a conexão com o banco NIMBUS está disponível."""
    try:
        load_dotenv(dotenv_path=PROJECT_ROOT / '.env')

        conn = psycopg2.connect(
            host=obter_variavel('DB_ORIGEM_HOST'),
            port=obter_variavel('DB_ORIGEM_PORT', obrigatoria=False, padrao='5432'),
            dbname=obter_variavel('DB_ORIGEM_NAME'),
            user=obter_variavel('DB_ORIGEM_USER'),
            password=obter_variavel('DB_ORIGEM_PASSWORD'),
            sslmode=obter_variavel('DB_ORIGEM_SSLMODE', obrigatoria=False, padrao='disable'),
            connect_timeout=10,
        )
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.close()
        conn.close()

        host   = os.getenv('DB_ORIGEM_HOST', '')
        dbname = os.getenv('DB_ORIGEM_NAME', '')
        port   = os.getenv('DB_ORIGEM_PORT', '5432')
        print(f"✅ Conexão NIMBUS OK: {dbname}@{host}:{port}")
        return True

    except Exception as e:
        print(f"❌ Erro ao verificar conexão NIMBUS: {e}")
        traceback.print_exc()
        return False


@task(name="Verificar Credenciais GCP", log_prints=True)
def verificar_credenciais_gcp() -> bool:
    """Verifica se as credenciais do GCP estão configuradas."""
    try:
        credentials_path = PROJECT_ROOT / 'credentials' / 'credentials.json'
        if credentials_path.exists():
            print(f"✅ Credenciais GCP encontradas: {credentials_path}")
            return True
        print(f"⚠️  Credenciais GCP não encontradas em: {credentials_path}")
        return False
    except Exception as e:
        print(f"❌ Erro ao verificar credenciais GCP: {e}")
        return False


# ---------------------------------------------------------------------------
# Sincronização incremental
# ---------------------------------------------------------------------------

@task(name="Sincronização Incremental Pluviométricos", log_prints=True, retries=2, retry_delay_seconds=60)
def sincronizar_pluviometricos() -> dict:
    """Executa a sincronização incremental de dados pluviométricos."""
    script_path = PROJECT_ROOT / SCRIPT_SYNC_PLUVIO
    return executar_script_sincronizacao(script_path, Tabela.PLUVIOMETRICOS)


@task(name="Sincronização Incremental Meteorológicos", log_prints=True, retries=2, retry_delay_seconds=60)
def sincronizar_meteorologicos() -> dict:
    """Executa a sincronização incremental de dados meteorológicos."""
    script_path = PROJECT_ROOT / SCRIPT_SYNC_METEO
    return executar_script_sincronizacao(script_path, Tabela.METEOROLOGICOS)


# ---------------------------------------------------------------------------
# Monitoramento: status BigQuery
# ---------------------------------------------------------------------------

@task(name="Verificar Status BigQuery Pluviométricos", log_prints=True)
def verificar_status_pluviometricos() -> dict:
    """Verifica o status da tabela pluviometricos no BigQuery."""
    dataset_id       = os.getenv('BIGQUERY_DATASET_ID_NIMBUS', BIGQUERY_DEFAULT_DATASET)
    credentials_path = PROJECT_ROOT / 'credentials' / 'credentials.json'
    return verificar_status_bigquery_tabela(credentials_path, dataset_id, Tabela.PLUVIOMETRICOS)


@task(name="Verificar Status BigQuery Meteorológicos", log_prints=True)
def verificar_status_meteorologicos() -> dict:
    """Verifica o status da tabela meteorologicos no BigQuery."""
    dataset_id       = os.getenv('BIGQUERY_DATASET_ID_NIMBUS', BIGQUERY_DEFAULT_DATASET)
    credentials_path = PROJECT_ROOT / 'credentials' / 'credentials.json'
    return verificar_status_bigquery_tabela(credentials_path, dataset_id, Tabela.METEOROLOGICOS)


# ---------------------------------------------------------------------------
# Monitoramento: detecção de lacunas
# ---------------------------------------------------------------------------

@task(name="Verificar Lacunas Pluviométricos", log_prints=True)
def verificar_lacunas_pluviometricos() -> dict:
    """Verifica se há dados pluviométricos no NIMBUS ainda não sincronizados."""
    dataset_id       = os.getenv('BIGQUERY_DATASET_ID_NIMBUS', BIGQUERY_DEFAULT_DATASET)
    credentials_path = PROJECT_ROOT / 'credentials' / 'credentials.json'
    return verificar_lacunas_tabela(dataset_id, Tabela.PLUVIOMETRICOS, QUERY_NIMBUS_PLUVIO, credentials_path)


@task(name="Verificar Lacunas Meteorológicos", log_prints=True)
def verificar_lacunas_meteorologicos() -> dict:
    """Verifica se há dados meteorológicos no NIMBUS ainda não sincronizados."""
    dataset_id       = os.getenv('BIGQUERY_DATASET_ID_NIMBUS', BIGQUERY_DEFAULT_DATASET)
    credentials_path = PROJECT_ROOT / 'credentials' / 'credentials.json'
    return verificar_lacunas_tabela(dataset_id, Tabela.METEOROLOGICOS, QUERY_NIMBUS_METEO, credentials_path)


# ---------------------------------------------------------------------------
# [STUB] Inferência IA — Integração futura LNCC / Gypscie
#
# O COR (Centro de Operações Rio) receberá futuramente um modelo de IA
# desenvolvido pelo LNCC (Laboratório Nacional de Computação Científica)
# para predição de chuvas/alertas a partir dos dados do NIMBUS.
#
# O fluxo planejado é:
#   1. Dados brutos chegam ao BigQuery (feito pelo flow de sincronização)
#   2. Esta task prepara o payload no formato esperado pelo modelo
#   3. Envia para a API Gypscie (plataforma de orquestração de ML da IplanRio)
#   4. Aguarda o processamento e baixa as predições
#   5. Salva os resultados na tabela `predicoes_lncc` no BigQuery
#
# Variáveis de ambiente necessárias (configurar no .env quando disponível):
#   GYPSCIE_API_URL        — endpoint da API
#   GYPSCIE_API_KEY        — chave de autenticação
#   GYPSCIE_WORKFLOW_ID    — ID do workflow de predição
#   GYPSCIE_ENVIRONMENT_ID — ID do ambiente Gypscie
# ---------------------------------------------------------------------------

@task(name="Inferência IA LNCC [STUB]", log_prints=True)
def executar_inferencia_ia(dados: dict = None) -> dict:
    """Placeholder para integração futura com o modelo de IA do LNCC via Gypscie.

    Esta task será ativada quando as credenciais GYPSCIE_API_URL e
    GYPSCIE_API_KEY estiverem configuradas no .env.
    """
    gypscie_url = os.getenv('GYPSCIE_API_URL')
    gypscie_key = os.getenv('GYPSCIE_API_KEY')

    if not gypscie_url or not gypscie_key:
        print("ℹ️  Inferência IA (LNCC/Gypscie) não configurada — task em modo STUB.")
        print("   Configure GYPSCIE_API_URL e GYPSCIE_API_KEY no .env para ativar.")
        return {
            'ativo': False,
            'mensagem': 'Integração Gypscie/LNCC pendente de configuração',
        }

    # --- implementação futura ---
    # 1. Autenticar na API Gypscie
    # 2. Registrar dataset com os dados do BigQuery
    # 3. Disparar workflow_id = GYPSCIE_WORKFLOW_ID
    # 4. Polling até conclusão
    # 5. Baixar predições e salvar no BigQuery (tabela predicoes_lncc)
    print("⚠️  Gypscie configurado mas integração ainda não implementada.")
    return {
        'ativo': False,
        'mensagem': 'Implementação pendente',
    }
