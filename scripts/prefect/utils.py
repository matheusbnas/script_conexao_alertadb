#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Funções auxiliares reutilizáveis para os workflows Prefect.

Contém lógica de baixo nível (subprocessos, BigQuery, NIMBUS) que é
compartilhada entre tasks.py e flows.py sem depender do Prefect em si.
"""

import os
import re
import subprocess
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

import psycopg2
import pytz
from dotenv import load_dotenv
from google.cloud import bigquery as bq_client
from google.oauth2 import service_account

from constants import PROJECT_ROOT


# ---------------------------------------------------------------------------
# Execução de scripts de sincronização
# ---------------------------------------------------------------------------

def executar_script_sincronizacao(script_path: Path, tipo_dado: str, timeout: int = 1800) -> Dict:
    """Executa um script de sincronização e retorna resultado padronizado.

    Args:
        script_path: Caminho para o script Python a ser executado.
        tipo_dado: Tipo de dado ('pluviometricos' ou 'meteorologicos').
        timeout: Timeout em segundos (padrão: 1800 = 30 min).

    Returns:
        Dict com 'sucesso', 'tempo_segundos', 'registros_processados', etc.
    """
    try:
        print(f"🔄 Iniciando sincronização incremental de {tipo_dado}...")
        print(f"   Script: {script_path}")
        print(f"   Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        inicio = datetime.now()

        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'

        process = subprocess.Popen(
            [sys.executable, str(script_path), '--once'],
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            bufsize=0,
        )

        stdout_bytes, stderr_bytes = process.communicate(timeout=timeout)
        return_code = process.returncode

        tempo_decorrido = (datetime.now() - inicio).total_seconds()

        try:
            stdout_text = stdout_bytes.decode('utf-8', errors='replace') if stdout_bytes else ""
        except Exception as e:
            stdout_text = f"[Erro ao decodificar stdout: {type(e).__name__}: {str(e)[:100]}]"

        try:
            stderr_text = stderr_bytes.decode('utf-8', errors='replace') if stderr_bytes else ""
        except Exception as e:
            stderr_text = f"[Erro ao decodificar stderr: {type(e).__name__}: {str(e)[:100]}]"

        output_completo = stdout_text + stderr_text
        erros_detectados = []

        if "Resources exceeded" in output_completo or "10000 partitions" in output_completo:
            erros_detectados.append("ERRO: Limite de partições excedido (precisa recriar tabela com particionamento por MÊS)")

        if "TIMESTAMP_NANOS" in output_completo or "Invalid timestamp" in output_completo:
            erros_detectados.append("ERRO: Problema com formato de timestamp")

        if "ConnectionResetError" in output_completo or "connection" in output_completo.lower():
            erros_detectados.append("ERRO: Problema de conexão com banco de dados")

        if "ERRO CRÍTICO" in output_completo or "❌" in output_completo:
            erros_detectados.append("ERRO: Erro crítico detectado no script")

        if return_code == 0:
            if erros_detectados:
                print(f"⚠️  Sincronização concluída mas com avisos:")
                for erro in erros_detectados:
                    print(f"   {erro}")
            else:
                print(f"✅ Sincronização incremental concluída com sucesso em {tempo_decorrido:.1f} segundos")

            registros_processados = 0
            if "registros" in output_completo.lower():
                match = re.search(r'(\d[\d,]*)\s+registros', output_completo)
                if match:
                    registros_processados = int(match.group(1).replace(',', ''))

            print(f"   📊 Registros processados: {registros_processados:,}")
            print(f"   ⏱️  Tempo de execução: {tempo_decorrido:.1f} segundos")

            return {
                'sucesso': True,
                'tempo_segundos': tempo_decorrido,
                'registros_processados': registros_processados,
                'mensagem': 'Sincronização incremental concluída com sucesso',
                'avisos': erros_detectados if erros_detectados else [],
                'output_resumo': output_completo[-1000:] if len(output_completo) > 1000 else output_completo,
            }
        else:
            print(f"❌ ERRO na sincronização incremental:")
            print(f"   Return code: {return_code}")
            print(f"   Erros detectados: {len(erros_detectados)}")
            for erro in erros_detectados:
                print(f"   {erro}")

            print(f"\n   📋 Últimas linhas do stderr:")
            if stderr_text:
                for line in stderr_text.split('\n')[-20:]:
                    if line.strip():
                        print(f"      {line}")
            else:
                print(f"      (stderr vazio)")

            return {
                'sucesso': False,
                'tempo_segundos': tempo_decorrido,
                'return_code': return_code,
                'mensagem': f'Erro na sincronização (code: {return_code})',
                'erros_detectados': erros_detectados,
                'stderr': stderr_text[-1000:] if len(stderr_text) > 1000 else stderr_text,
            }

    except subprocess.TimeoutExpired:
        print(f"⏱️  TIMEOUT: Sincronização demorou mais de {timeout // 60} minutos")
        print(f"   Isso pode indicar problema de conexão ou volume muito grande de dados")
        return {
            'sucesso': False,
            'tempo_segundos': timeout,
            'mensagem': f'Timeout após {timeout // 60} minutos',
            'erros_detectados': [f'TIMEOUT: Processo demorou mais de {timeout // 60} minutos'],
        }
    except Exception as e:
        print(f"❌ ERRO FATAL ao executar sincronização incremental: {e}")
        traceback.print_exc()
        return {
            'sucesso': False,
            'tempo_segundos': 0,
            'mensagem': f'Erro fatal: {str(e)}',
            'erros_detectados': [f'Erro fatal: {str(e)}'],
        }


# ---------------------------------------------------------------------------
# Detecção de lacunas entre BigQuery e NIMBUS
# ---------------------------------------------------------------------------

def verificar_lacunas_tabela(
    dataset_id: str,
    tabela_bq: str,
    query_nimbus_count: str,
    credentials_path: Path,
) -> Dict:
    """Verifica lacunas de sincronização entre BigQuery e NIMBUS.

    Reutilizável para qualquer tabela. A query NIMBUS deve conter os
    placeholders ``{timestamp_str}`` e ``{timestamp_atual_str}``.

    Args:
        dataset_id: ID do dataset BigQuery (ex: 'alertadb_cor_raw').
        tabela_bq: Nome da tabela BigQuery (ex: 'pluviometricos').
        query_nimbus_count: SQL para contar registros pendentes no NIMBUS.
        credentials_path: Caminho para credentials.json.

    Returns:
        Dict com chaves: sucesso, lacunas_detectadas, total_registros_pendentes,
        ultima_sincronizacao, data_atual, diferenca_dias, lacuna_significativa.
    """
    try:
        load_dotenv(dotenv_path=PROJECT_ROOT / '.env')

        if not credentials_path.exists():
            return {
                'sucesso': False,
                'mensagem': f'Credenciais não encontradas em: {credentials_path}',
                'lacunas_detectadas': False,
            }

        credentials = service_account.Credentials.from_service_account_file(
            str(credentials_path),
            scopes=["https://www.googleapis.com/auth/bigquery"],
        )
        client = bq_client.Client(credentials=credentials, project=credentials.project_id)

        query_bq = f"SELECT MAX(dia_utc) AS ultima_sincronizacao FROM `{dataset_id}.{tabela_bq}`"
        results = client.query(query_bq).result()

        ultima_sync: Optional[datetime] = None
        for row in results:
            if row.ultima_sincronizacao:
                ultima_sync = row.ultima_sincronizacao
                if isinstance(ultima_sync, datetime):
                    if ultima_sync.tzinfo is None:
                        ultima_sync = ultima_sync.replace(tzinfo=timezone.utc)
                    else:
                        ultima_sync = ultima_sync.astimezone(timezone.utc)
                break

        if not ultima_sync:
            return {
                'sucesso': True,
                'mensagem': 'Tabela vazia — sem dados para verificar lacunas',
                'lacunas_detectadas': False,
            }

        tz_brasil = pytz.timezone('America/Sao_Paulo')
        data_atual = datetime.now(timezone.utc)
        diferenca = data_atual - ultima_sync

        ultima_sync_br = ultima_sync.astimezone(tz_brasil)
        data_atual_br = data_atual.astimezone(tz_brasil)

        print(f"📅 Última sincronização ({tabela_bq}): {ultima_sync_br.strftime('%Y-%m-%d %H:%M:%S')} (Brasília)")
        print(f"📅 Data atual: {data_atual_br.strftime('%Y-%m-%d %H:%M:%S')} (Brasília)")
        print(f"⏱️  Diferença: {diferenca.days} dias, {diferenca.seconds // 3600} horas")

        conn = psycopg2.connect(
            host=os.environ['DB_ORIGEM_HOST'],
            port=os.getenv('DB_ORIGEM_PORT', '5432'),
            dbname=os.environ['DB_ORIGEM_NAME'],
            user=os.environ['DB_ORIGEM_USER'],
            password=os.environ['DB_ORIGEM_PASSWORD'],
            sslmode=os.getenv('DB_ORIGEM_SSLMODE', 'disable'),
            connect_timeout=10,
        )
        cur = conn.cursor()
        ts_str = ultima_sync.strftime('%Y-%m-%d %H:%M:%S+00:00')
        ts_atual_str = data_atual.strftime('%Y-%m-%d %H:%M:%S+00:00')
        cur.execute(query_nimbus_count.format(timestamp_str=ts_str, timestamp_atual_str=ts_atual_str))
        resultado = cur.fetchone()
        cur.close()
        conn.close()

        total_pendentes = resultado[0] if resultado else 0
        lacuna_significativa = diferenca.days > 1

        if total_pendentes > 0:
            print(f"⚠️  LACUNA DETECTADA: {total_pendentes:,} registros pendentes em '{tabela_bq}'")
            print(f"   Período: {ultima_sync_br.strftime('%Y-%m-%d %H:%M')} → {data_atual_br.strftime('%Y-%m-%d %H:%M')} (Brasília)")
            if lacuna_significativa:
                anos = diferenca.days / 365.25
                print(f"   ⚠️  Lacuna de {diferenca.days} dias ({anos:.1f} anos) — sincronização pode demorar")
        else:
            print(f"✅ Sem lacunas em '{tabela_bq}' — última sync: {ultima_sync_br.strftime('%Y-%m-%d %H:%M')} (Brasília)")

        return {
            'sucesso': True,
            'lacunas_detectadas': total_pendentes > 0,
            'total_registros_pendentes': total_pendentes,
            'ultima_sincronizacao': ultima_sync.isoformat(),
            'data_atual': data_atual.isoformat(),
            'diferenca_dias': diferenca.days,
            'lacuna_significativa': lacuna_significativa,
        }

    except Exception as e:
        print(f"❌ Erro ao verificar lacunas em '{tabela_bq}': {e}")
        traceback.print_exc()
        return {
            'sucesso': False,
            'mensagem': str(e),
            'lacunas_detectadas': False,
        }


# ---------------------------------------------------------------------------
# Status de tabela no BigQuery
# ---------------------------------------------------------------------------

def verificar_status_bigquery_tabela(credentials_path: Path, dataset_id: str, table_id: str) -> Dict:
    """Verifica o status de uma tabela específica no BigQuery.

    Args:
        credentials_path: Caminho para credentials.json.
        dataset_id: ID do dataset BigQuery.
        table_id: ID da tabela BigQuery.

    Returns:
        Dict com 'sucesso', 'total_registros', 'data_maxima', 'total_estacoes'.
    """
    try:
        if not credentials_path.exists():
            return {
                'sucesso': False,
                'mensagem': f'Credenciais não encontradas em: {credentials_path}',
            }

        try:
            from prefect_gcp import GcpCredentials
            from prefect_gcp.bigquery import BigQueryWarehouse

            gcp_credentials = GcpCredentials(service_account_file=str(credentials_path))
            with BigQueryWarehouse(gcp_credentials=gcp_credentials) as warehouse:
                query = f"""
                SELECT
                    COUNT(*) AS total_registros,
                    MIN(dia_utc) AS data_minima,
                    MAX(dia_utc) AS data_maxima,
                    COUNT(DISTINCT estacao_id) AS total_estacoes
                FROM `{dataset_id}.{table_id}`
                """
                result = warehouse.fetch_one(query)
                if result:
                    return {
                        'sucesso': True,
                        'total_registros': result[0],
                        'data_minima': str(result[1]),
                        'data_maxima': str(result[2]),
                        'total_estacoes': result[3],
                    }
        except ImportError:
            pass
        except Exception as e:
            print(f"⚠️  Erro ao usar Prefect GCP: {e}")

        return {
            'sucesso': True,
            'mensagem': 'Verificação básica concluída (instale prefect-gcp para estatísticas detalhadas)',
        }

    except Exception as e:
        print(f"❌ Erro ao verificar status BigQuery: {e}")
        return {'sucesso': False, 'mensagem': str(e)}
