#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Constantes e configurações compartilhadas pelos workflows Prefect.

Centraliza nomes de tabelas, queries SQL e caminhos de scripts para evitar
repetição entre flows.py, tasks.py e utils.py.
"""

from enum import Enum
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent

# ---------------------------------------------------------------------------
# BigQuery
# ---------------------------------------------------------------------------

BIGQUERY_DEFAULT_DATASET = 'alertadb_cor_raw'


class Tabela(str, Enum):
    PLUVIOMETRICOS = 'pluviometricos'
    METEOROLOGICOS = 'meteorologicos'


# ---------------------------------------------------------------------------
# Scripts de sincronização (caminhos relativos a PROJECT_ROOT)
# ---------------------------------------------------------------------------

SCRIPT_SYNC_PLUVIO = 'scripts/bigquery/sincronizar_pluviometricos_nimbus_bigquery.py'
SCRIPT_SYNC_METEO  = 'scripts/bigquery/sincronizar_meteorologicos_nimbus_bigquery.py'

# ---------------------------------------------------------------------------
# Queries NIMBUS para detecção de lacunas
#
# Placeholders obrigatórios:
#   {timestamp_str}       — timestamp da última sincronização (UTC)
#   {timestamp_atual_str} — timestamp atual (UTC)
# ---------------------------------------------------------------------------

QUERY_NIMBUS_PLUVIO = """
SELECT COUNT(*) AS total_registros
FROM public.estacoes_leitura AS el
JOIN public.estacoes_leiturachuva AS elc ON elc.leitura_id = el.id
WHERE el."horaLeitura" > '{timestamp_str}'::timestamptz
  AND el."horaLeitura" <= '{timestamp_atual_str}'::timestamptz
"""

QUERY_NIMBUS_METEO = """
SELECT COUNT(DISTINCT l."horaLeitura" || '_' || l.estacao_id::text) AS total_registros
FROM public.estacoes_leiturasensor ls
JOIN public.estacoes_leitura l ON ls.leitura_id = l.id
JOIN public.estacoes_sensor s ON ls.sensor_id = s.id
JOIN public.estacoes_estacao e ON e.id = l.estacao_id
WHERE l."horaLeitura" > '{timestamp_str}'::timestamptz
  AND l."horaLeitura" <= '{timestamp_atual_str}'::timestamptz
"""

# ---------------------------------------------------------------------------
# Integração futura: Gypscie / Modelo LNCC (COR)
#
# Quando o modelo de IA do LNCC estiver disponível via Gypscie, as variáveis
# abaixo habilitarão a task `executar_inferencia_ia` em tasks.py.
# Configure no .env:
#   GYPSCIE_API_URL        — endpoint da API Gypscie
#   GYPSCIE_API_KEY        — chave de autenticação
#   GYPSCIE_WORKFLOW_ID    — ID do workflow de predição (ex: 41)
#   GYPSCIE_ENVIRONMENT_ID — ID do ambiente (ex: 1)
# ---------------------------------------------------------------------------

GYPSCIE_PROCESSOR_NAME   = 'etl_alertario22'  # nome provisório; confirmar com LNCC
GYPSCIE_DATASET_TABLE_BQ = 'predicoes_lncc'   # tabela destino no BigQuery para predições
