#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Flows Prefect — Sincronização NIMBUS → BigQuery

Contém os três flows de sincronização:
  - sincronizacao_pluviometricos_flow
  - sincronizacao_meteorologicos_flow
  - sincronizacao_combinada_flow  (pluviométricos + meteorológicos juntos)

Deployments são gerenciados pelo prefect.yaml na raiz do projeto.

Execução manual:
  python flows.py --run-once                        # flow combinado (padrão)
  python flows.py --run-once --flow pluviometricos
  python flows.py --run-once --flow meteorologicos
  python flows.py --run-once --flow combinado
"""

import os
import sys

# Habilita modo efêmero quando não há servidor Prefect disponível (ex: cron/teste local)
if '--run-once' in sys.argv and not os.getenv("PREFECT_API_URL"):
    os.environ["PREFECT_API_URL"] = ""
    os.environ["PREFECT_SERVER_ALLOW_EPHEMERAL_MODE"] = "true"

from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from prefect import flow
from prefect.exceptions import PrefectHTTPStatusError

sys.path.insert(0, str(Path(__file__).parent))

from tasks import (
    executar_inferencia_ia,
    sincronizar_meteorologicos,
    sincronizar_pluviometricos,
    verificar_conexao_nimbus,
    verificar_credenciais_gcp,
    verificar_lacunas_meteorologicos,
    verificar_lacunas_pluviometricos,
    verificar_status_meteorologicos,
    verificar_status_pluviometricos,
)

project_root = Path(__file__).parent.parent.parent
load_dotenv(dotenv_path=project_root / '.env')


# ---------------------------------------------------------------------------
# Flow: Pluviométricos
# ---------------------------------------------------------------------------

@flow(name="Sincronização BigQuery - Pluviométricos", log_prints=True)
def sincronizacao_pluviometricos_flow() -> dict:
    """Flow incremental de dados pluviométricos (NIMBUS → BigQuery)."""
    print("=" * 80)
    print("🌧️ SINCRONIZAÇÃO INCREMENTAL PLUVIOMÉTRICOS - BigQuery")
    print("=" * 80)
    print(f"⏰ Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    print("📡 Verificando conexões...")
    conexao_nimbus  = verificar_conexao_nimbus()
    credenciais_gcp = verificar_credenciais_gcp()

    if not conexao_nimbus or not credenciais_gcp:
        print("❌ Falha na verificação de conexões. Abortando flow.")
        return {
            'sucesso': False,
            'mensagem': 'Falha na verificação de conexões',
            'timestamp': datetime.now().isoformat(),
        }

    print()

    print("🔍 Verificando lacunas de dados...")
    resultado_lacunas   = verificar_lacunas_pluviometricos()
    lacunas_detectadas  = resultado_lacunas.get('lacunas_detectadas', False)

    if lacunas_detectadas:
        registros_pendentes = resultado_lacunas.get('total_registros_pendentes', 0)
        diferenca_dias      = resultado_lacunas.get('diferenca_dias', 0)
        print(f"   ⚠️  {registros_pendentes:,} registros pendentes detectados")
        if diferenca_dias > 1:
            print(f"   ⚠️  Lacuna de {diferenca_dias} dias — sincronização pode estar atrasada")

    print()

    print("📦 Executando sincronização incremental...")
    resultado_sync = sincronizar_pluviometricos()
    sucesso_sync   = resultado_sync.get('sucesso', False)

    print()

    print("📊 Verificando status final no BigQuery...")
    status_bq = verificar_status_pluviometricos()

    todos_erros  = list(resultado_sync.get('erros_detectados') or [])
    todos_avisos = list(resultado_sync.get('avisos') or [])

    if lacunas_detectadas:
        pendentes = resultado_lacunas.get('total_registros_pendentes', 0)
        dias      = resultado_lacunas.get('diferenca_dias', 0)
        msg = (f"Lacuna de {dias} dias ({pendentes:,} registros pendentes)"
               if dias > 1 else f"{pendentes:,} registros pendentes detectados")
        todos_avisos.append(msg)

    if not status_bq.get('sucesso', False):
        todos_erros.append("Erro ao verificar status BigQuery")

    print()
    print("=" * 80)

    if sucesso_sync and not todos_erros:
        print("✅ SINCRONIZAÇÃO PLUVIOMÉTRICOS CONCLUÍDA COM SUCESSO")
    elif sucesso_sync:
        print("⚠️  SINCRONIZAÇÃO PLUVIOMÉTRICOS CONCLUÍDA COM ERROS")
        for erro in todos_erros:
            print(f"   - {erro}")
    else:
        print("❌ SINCRONIZAÇÃO PLUVIOMÉTRICOS FALHOU")
        for erro in todos_erros:
            print(f"   - {erro}")

    print("=" * 80)
    print(f"⏰ Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if status_bq.get('sucesso'):
        print(f"📊 Status BigQuery:")
        print(f"   Total de registros: {status_bq.get('total_registros', 0):,}")
        print(f"   Data máxima: {status_bq.get('data_maxima', 'N/A')}")
        print(f"   Total de estações: {status_bq.get('total_estacoes', 0)}")

    if resultado_sync.get('registros_processados', 0) > 0:
        print(f"📦 Registros processados: {resultado_sync.get('registros_processados', 0):,}")

    return {
        'sucesso': sucesso_sync and not todos_erros,
        'sincronizacao': resultado_sync,
        'status_bigquery': status_bq,
        'lacunas': resultado_lacunas,
        'erros': todos_erros,
        'avisos': todos_avisos,
        'timestamp': datetime.now().isoformat(),
    }


# ---------------------------------------------------------------------------
# Flow: Meteorológicos
# ---------------------------------------------------------------------------

@flow(name="Sincronização BigQuery - Meteorológicos", log_prints=True)
def sincronizacao_meteorologicos_flow() -> dict:
    """Flow incremental de dados meteorológicos (NIMBUS → BigQuery)."""
    print("=" * 80)
    print("🌤️ SINCRONIZAÇÃO INCREMENTAL METEOROLÓGICOS - BigQuery")
    print("=" * 80)
    print(f"⏰ Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    print("📡 Verificando conexões...")
    conexao_nimbus  = verificar_conexao_nimbus()
    credenciais_gcp = verificar_credenciais_gcp()

    if not conexao_nimbus or not credenciais_gcp:
        print("❌ Falha na verificação de conexões. Abortando flow.")
        return {
            'sucesso': False,
            'mensagem': 'Falha na verificação de conexões',
            'timestamp': datetime.now().isoformat(),
        }

    print()

    print("🔍 Verificando lacunas de dados...")
    resultado_lacunas  = verificar_lacunas_meteorologicos()
    lacunas_detectadas = resultado_lacunas.get('lacunas_detectadas', False)

    if lacunas_detectadas:
        registros_pendentes = resultado_lacunas.get('total_registros_pendentes', 0)
        diferenca_dias      = resultado_lacunas.get('diferenca_dias', 0)
        print(f"   ⚠️  {registros_pendentes:,} registros pendentes detectados")
        if diferenca_dias > 1:
            print(f"   ⚠️  Lacuna de {diferenca_dias} dias — sincronização pode estar atrasada")

    print()

    print("📦 Executando sincronização incremental...")
    resultado_sync = sincronizar_meteorologicos()
    sucesso_sync   = resultado_sync.get('sucesso', False)

    print()

    print("📊 Verificando status final no BigQuery...")
    status_bq = verificar_status_meteorologicos()

    todos_erros  = list(resultado_sync.get('erros_detectados') or [])
    todos_avisos = list(resultado_sync.get('avisos') or [])

    if lacunas_detectadas:
        pendentes = resultado_lacunas.get('total_registros_pendentes', 0)
        dias      = resultado_lacunas.get('diferenca_dias', 0)
        msg = (f"Lacuna de {dias} dias ({pendentes:,} registros pendentes)"
               if dias > 1 else f"{pendentes:,} registros pendentes detectados")
        todos_avisos.append(msg)

    if not status_bq.get('sucesso', False):
        todos_erros.append("Erro ao verificar status BigQuery")

    print()
    print("=" * 80)

    if sucesso_sync and not todos_erros:
        print("✅ SINCRONIZAÇÃO METEOROLÓGICOS CONCLUÍDA COM SUCESSO")
    elif sucesso_sync:
        print("⚠️  SINCRONIZAÇÃO METEOROLÓGICOS CONCLUÍDA COM ERROS")
        for erro in todos_erros:
            print(f"   - {erro}")
    else:
        print("❌ SINCRONIZAÇÃO METEOROLÓGICOS FALHOU")
        for erro in todos_erros:
            print(f"   - {erro}")

    print("=" * 80)
    print(f"⏰ Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if status_bq.get('sucesso'):
        print(f"📊 Status BigQuery:")
        print(f"   Total de registros: {status_bq.get('total_registros', 0):,}")
        print(f"   Data máxima: {status_bq.get('data_maxima', 'N/A')}")
        print(f"   Total de estações: {status_bq.get('total_estacoes', 0)}")

    if resultado_sync.get('registros_processados', 0) > 0:
        print(f"📦 Registros processados: {resultado_sync.get('registros_processados', 0):,}")

    return {
        'sucesso': sucesso_sync and not todos_erros,
        'sincronizacao': resultado_sync,
        'status_bigquery': status_bq,
        'lacunas': resultado_lacunas,
        'erros': todos_erros,
        'avisos': todos_avisos,
        'timestamp': datetime.now().isoformat(),
    }


# ---------------------------------------------------------------------------
# Flow: Combinado (pluviométricos + meteorológicos)
# ---------------------------------------------------------------------------

@flow(name="Sincronização BigQuery - Combinada", log_prints=True)
def sincronizacao_combinada_flow() -> dict:
    """Flow combinado que sincroniza pluviométricos e meteorológicos.

    Ideal quando há limite de deployments no Prefect Cloud (plano gratuito).
    """
    print("=" * 80)
    print("🔄 SINCRONIZAÇÃO COMBINADA - BigQuery")
    print("=" * 80)
    print(f"⏰ Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📊 Tabelas: pluviometricos + meteorologicos")
    print()

    print("📡 Verificando conexões...")
    conexao_nimbus  = verificar_conexao_nimbus()
    credenciais_gcp = verificar_credenciais_gcp()

    if not conexao_nimbus or not credenciais_gcp:
        print("❌ Falha na verificação de conexões. Abortando flow.")
        return {
            'sucesso': False,
            'mensagem': 'Falha na verificação de conexões',
            'timestamp': datetime.now().isoformat(),
        }

    print()

    print("🌧️  Executando sincronização PLUVIOMÉTRICOS...")
    resultado_pluvio = sincronizar_pluviometricos()
    sucesso_pluvio   = resultado_pluvio.get('sucesso', False)

    print()

    print("🌤️  Executando sincronização METEOROLÓGICOS...")
    resultado_meteo = sincronizar_meteorologicos()
    sucesso_meteo   = resultado_meteo.get('sucesso', False)

    print()

    print("📊 Verificando status final no BigQuery...")
    status_pluvio = verificar_status_pluviometricos()
    status_meteo  = verificar_status_meteorologicos()

    # Inferência IA (ativa apenas quando Gypscie/LNCC estiver configurado)
    inferencia = executar_inferencia_ia()

    todos_erros  = []
    todos_avisos = []

    if resultado_pluvio.get('erros_detectados'):
        todos_erros.extend([f"PLUVIOMÉTRICOS: {e}" for e in resultado_pluvio['erros_detectados']])
    if resultado_pluvio.get('avisos'):
        todos_avisos.extend([f"PLUVIOMÉTRICOS: {a}" for a in resultado_pluvio['avisos']])

    if resultado_meteo.get('erros_detectados'):
        todos_erros.extend([f"METEOROLÓGICOS: {e}" for e in resultado_meteo['erros_detectados']])
    if resultado_meteo.get('avisos'):
        todos_avisos.extend([f"METEOROLÓGICOS: {a}" for a in resultado_meteo['avisos']])

    if not status_pluvio.get('sucesso', False):
        todos_erros.append("PLUVIOMÉTRICOS: Erro ao verificar status BigQuery")
    if not status_meteo.get('sucesso', False):
        todos_erros.append("METEOROLÓGICOS: Erro ao verificar status BigQuery")

    print()
    print("=" * 80)

    sucesso_geral = sucesso_pluvio and sucesso_meteo and not todos_erros

    if sucesso_geral:
        print("✅ SINCRONIZAÇÃO COMBINADA CONCLUÍDA COM SUCESSO")
        if todos_avisos:
            print(f"⚠️  Avisos: {len(todos_avisos)}")
            for aviso in todos_avisos:
                print(f"   - {aviso}")
    elif sucesso_pluvio and sucesso_meteo and todos_erros:
        print("⚠️  SINCRONIZAÇÃO COMBINADA CONCLUÍDA COM ERROS")
        for erro in todos_erros:
            print(f"   - {erro}")
    else:
        print("❌ SINCRONIZAÇÃO COMBINADA FALHOU")
        print(f"   🌧️  Pluviométricos: {'✅' if sucesso_pluvio else '❌'}")
        print(f"   🌤️  Meteorológicos: {'✅' if sucesso_meteo else '❌'}")
        for erro in todos_erros:
            print(f"   - {erro}")

    print("=" * 80)
    print(f"⏰ Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if status_pluvio.get('sucesso'):
        print(f"📊 Pluviométricos — registros: {status_pluvio.get('total_registros', 0):,}"
              f"  |  máx: {status_pluvio.get('data_maxima', 'N/A')}")
    if status_meteo.get('sucesso'):
        print(f"📊 Meteorológicos  — registros: {status_meteo.get('total_registros', 0):,}"
              f"  |  máx: {status_meteo.get('data_maxima', 'N/A')}")

    return {
        'sucesso': sucesso_geral,
        'pluviometricos': {'sincronizacao': resultado_pluvio, 'status_bigquery': status_pluvio},
        'meteorologicos': {'sincronizacao': resultado_meteo, 'status_bigquery': status_meteo},
        'inferencia_ia': inferencia,
        'erros': todos_erros,
        'avisos': todos_avisos,
        'timestamp': datetime.now().isoformat(),
    }


# ---------------------------------------------------------------------------
# Execução manual
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Execução manual dos flows de sincronização')
    parser.add_argument('--run-once', action='store_true',
                        help='Executa o flow uma vez sem criar deployment')
    parser.add_argument('--flow',
                        choices=['combinado', 'pluviometricos', 'meteorologicos'],
                        default='combinado',
                        help='Qual flow executar (padrão: combinado)')
    args = parser.parse_args()

    _flow_map = {
        'combinado':      sincronizacao_combinada_flow,
        'pluviometricos': sincronizacao_pluviometricos_flow,
        'meteorologicos': sincronizacao_meteorologicos_flow,
    }

    if args.run_once:
        print(f"🔄 Executando flow '{args.flow}' (--run-once)...")
        resultado = _flow_map[args.flow]()
        sys.exit(0 if resultado.get('sucesso', False) else 1)
    else:
        try:
            sincronizacao_combinada_flow.serve(
                name="sincronizacao-bigquery-combinada",
                cron="*/5 * * * *",
                description="Sincronização incremental combinada (NIMBUS → BigQuery)",
            )
        except PrefectHTTPStatusError as e:
            if e.response.status_code == 401:
                print("\n⚠️  Erro 401 Unauthorized: autenticação com Prefect Cloud falhou.")
                print("   1. Fazer login:        prefect cloud login")
                print("   2. Executar sem deploy: python flows.py --run-once")
                print("   3. Prefect Local:       defina PREFECT_API_URL e rode 'prefect server start'\n")
            raise
