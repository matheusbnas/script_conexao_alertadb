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
import threading
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import psycopg2
import pytz
from dotenv import load_dotenv
from google.cloud import bigquery as bq_client
from google.oauth2 import service_account

from constants import PROJECT_ROOT


# ---------------------------------------------------------------------------
# Execução de scripts de sincronização
# ---------------------------------------------------------------------------

_ALIAS_TIPO_DADO_TIMEOUT = {
    "pluviometricos": ("PLUVIOMETRICOS", "PLUVIO"),
    "meteorologicos": ("METEOROLOGICOS", "METEO"),
}


def timeout_floor_sync_meteorologicos(resultado_lacunas: Optional[Dict]) -> Optional[int]:
    """Piso de timeout (segundos) quando a lacuna é grande.

    Evita que um ``PREFECT_SYNC_TIMEOUT_SECONDS`` baixo (ex.: 30 min) mate o
    subprocesso durante recuperação de atraso com muitos registros pendentes.

    O piso usa **pendentes** e **diferença em dias**, não só ``lacunas_detectadas``:
    após serialização (Prefect/API), booleanos às vezes não voltam fiéis e o piso
    seria ignorado, reaplicando um timeout curto do ambiente.
    """
    if not resultado_lacunas:
        return None
    try:
        pendentes = int(resultado_lacunas.get('total_registros_pendentes') or 0)
    except (TypeError, ValueError):
        pendentes = 0
    try:
        dias = int(resultado_lacunas.get('diferenca_dias') or 0)
    except (TypeError, ValueError):
        dias = 0
    lacunas_flag = resultado_lacunas.get('lacunas_detectadas')
    tem_flag = lacunas_flag in (True, 1, '1', 'true', 'True')
    if pendentes <= 0 and not tem_flag and dias < 1:
        return None

    # Patamares: cargas de dezenas de milhares de linhas costumam exceder 1–2 h
    # (NIMBUS + MERGE no BigQuery).
    if pendentes >= 25_000 or dias >= 3:
        return 14_400  # 4 h
    if pendentes >= 15_000 or dias >= 2:
        return 10_800  # 3 h
    if pendentes >= 5_000 or dias >= 1:
        return 7200  # 2 h
    if pendentes > 0:
        return 5400  # 90 min — alinhado ao padrão docker-compose
    return None


def timeout_efetivo_sync_meteorologicos(resultado_lacunas: Optional[Dict]) -> int:
    """Timeout (segundos) do subprocesso meteorológico: max(env, piso por lacuna).

    Calculado no *flow* e repassado como inteiro explícito à task para evitar que
    ``timeout_floor`` se perca em serialização/registro antigo do Prefect, deixando
    apenas o valor curto do ``.env`` (ex.: 30 min).
    """
    base = _resolver_timeout_sincronizacao("meteorologicos", 5400)
    floor_val = timeout_floor_sync_meteorologicos(resultado_lacunas) or 0
    return max(base, floor_val)


def _resolver_timeout_sincronizacao(tipo_dado: str, timeout_padrao: int) -> int:
    """Resolve timeout por variável de ambiente, mantendo padrão conservador.

    Aceita aliases comuns no .env (ex.: ``PREFECT_SYNC_TIMEOUT_SECONDS_PLUVIO``
    além do nome canônico ``PREFECT_SYNC_TIMEOUT_SECONDS_PLUVIOMETRICOS``),
    para tolerar pequenas inconsistências de nomenclatura sem quebrar.
    """
    aliases = _ALIAS_TIPO_DADO_TIMEOUT.get(tipo_dado.lower(), (tipo_dado.upper(),))
    chaves: List[str] = [f"PREFECT_SYNC_TIMEOUT_SECONDS_{alias}" for alias in aliases]
    chaves.append("PREFECT_SYNC_TIMEOUT_SECONDS")

    chave_usada: Optional[str] = None
    valor: Optional[str] = None
    for chave in chaves:
        candidato = os.getenv(chave)
        if candidato:
            chave_usada = chave
            valor = candidato
            break

    if not valor:
        return timeout_padrao

    try:
        timeout_configurado = int(valor)
        if timeout_configurado <= 0:
            raise ValueError
        return timeout_configurado
    except ValueError:
        print(f"⚠️  Timeout inválido em {chave_usada}: {valor!r}")
        print(f"   Usando timeout padrão de {timeout_padrao // 60} minutos")
        return timeout_padrao


# Maior valor retornado por ``timeout_floor_sync_meteorologicos`` (lacunas grandes).
_METEO_SYNC_TIMEOUT_LACUNA_MAX_S = 14_400


def timeout_communicate_workflow_service(workflow_tipo: str) -> int:
    """Segundos para ``subprocess.Popen.communicate`` no ``service.py`` (wrapper do flow).

    O limite antigo fixo (3600 s) era menor que o timeout padrão da sincronização
    pluviométrica (5400 s), o que gerava falha com ``return_code -1``, duração ~1 h
    e stdout/stderr vazios (timeout do pai antes do filho terminar).

    Ordem de precedência:
    1. ``PREFECT_SERVICE_WORKFLOW_TIMEOUT_SECONDS`` — inteiro > 0 substitui o cálculo.
    2. Soma (ou máximo) dos timeouts de sync configurados + margem para lacunas/Prefect.
    """
    raw = (os.getenv("PREFECT_SERVICE_WORKFLOW_TIMEOUT_SECONDS") or "").strip()
    if raw:
        try:
            v = int(raw)
            if v > 0:
                return v
        except ValueError:
            pass

    try:
        margem = int(os.getenv("PREFECT_SERVICE_WORKFLOW_TIMEOUT_MARGIN_SECONDS", "900"))
    except ValueError:
        margem = 900
    margem = max(120, margem)

    pluv = _resolver_timeout_sincronizacao("pluviometricos", 5400)
    mete = _resolver_timeout_sincronizacao("meteorologicos", 5400)
    mete_pior = max(mete, _METEO_SYNC_TIMEOUT_LACUNA_MAX_S)

    w = (workflow_tipo or "").strip().lower()
    if w == "pluviometricos":
        return pluv + margem
    if w == "meteorologicos":
        return mete_pior + margem
    if w == "combinado":
        return pluv + mete_pior + margem
    return max(pluv, mete_pior) + margem


def _consumir_stream_em_thread(stream, buffer: List[str], prefixo: str) -> threading.Thread:
    """Lê linhas de um stream em background, reemite com prefixo e armazena no buffer.

    Permite acompanhar logs do subprocesso em tempo real (no stdout do pai)
    mesmo que o subprocesso fique pendurado por minutos antes de terminar.
    """
    def _worker():
        try:
            for raw in iter(stream.readline, b""):
                try:
                    linha = raw.decode("utf-8", errors="replace")
                except Exception:
                    linha = repr(raw)
                if not linha:
                    continue
                buffer.append(linha)
                texto = linha.rstrip("\n")
                if texto:
                    print(f"{prefixo}{texto}", flush=True)
        finally:
            try:
                stream.close()
            except Exception:
                pass

    thread = threading.Thread(target=_worker, daemon=True)
    thread.start()
    return thread


def _heartbeat_em_thread(processo, stop_event: threading.Event, intervalo_segundos: int = 60) -> threading.Thread:
    """Imprime um 'heartbeat' periódico enquanto o subprocesso estiver vivo.

    Sem isso, ficamos sem nenhum sinal de vida quando o filho fica preso
    em uma query NIMBUS por 30 min.
    """
    inicio = datetime.now()

    def _worker():
        while not stop_event.wait(intervalo_segundos):
            if processo.poll() is not None:
                return
            decorrido = (datetime.now() - inicio).total_seconds()
            print(
                f"   ⏳ Subprocesso ainda em execução após {decorrido / 60:.1f} min "
                f"(pid={processo.pid}). Aguardando saída...",
                flush=True,
            )

    thread = threading.Thread(target=_worker, daemon=True)
    thread.start()
    return thread


def executar_script_sincronizacao(
    script_path: Path,
    tipo_dado: str,
    timeout: int = 5400,
    timeout_floor: Optional[int] = None,
    timeout_segundos_efetivo: Optional[int] = None,
) -> Dict:
    """Executa um script de sincronização e retorna resultado padronizado.

    Args:
        script_path: Caminho para o script Python a ser executado.
        tipo_dado: Tipo de dado ('pluviometricos' ou 'meteorologicos').
        timeout: Timeout em segundos (padrão: 5400 = 90 min, alinhado ao docker-compose).
        timeout_floor: Se definido, o timeout efetivo não fica abaixo deste valor (segundos).
        timeout_segundos_efetivo: Se definido (>0), substitui env+piso (uso pelo flow meteo).

    Returns:
        Dict com 'sucesso', 'tempo_segundos', 'registros_processados', etc.
    """
    process: Optional[subprocess.Popen] = None
    stdout_buffer: List[str] = []
    stderr_buffer: List[str] = []
    stop_heartbeat = threading.Event()

    try:
        if timeout_segundos_efetivo is not None and int(timeout_segundos_efetivo) > 0:
            timeout = int(timeout_segundos_efetivo)
            print(
                f"   ⏱️  Timeout efetivo (definido pelo flow): {timeout // 60} minutos",
                flush=True,
            )
        else:
            timeout = _resolver_timeout_sincronizacao(tipo_dado, timeout)
            if timeout_floor is not None and timeout_floor > 0:
                antes = timeout
                timeout = max(timeout, timeout_floor)
                if timeout > antes:
                    print(
                        f"   ⏱️  Lacuna grande: elevando timeout de {antes // 60} para {timeout // 60} minutos",
                        flush=True,
                    )
        print(f"🔄 Iniciando sincronização incremental de {tipo_dado}...")
        print(f"   Script: {script_path}")
        print(f"   Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   Timeout configurado: {timeout // 60} minutos")

        inicio = datetime.now()

        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'
        env['PYTHONUNBUFFERED'] = '1'

        process = subprocess.Popen(
            [sys.executable, '-u', str(script_path), '--once'],
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            bufsize=1,
        )

        thread_stdout = _consumir_stream_em_thread(process.stdout, stdout_buffer, prefixo="   │ ")
        thread_stderr = _consumir_stream_em_thread(process.stderr, stderr_buffer, prefixo="   ! ")
        thread_heartbeat = _heartbeat_em_thread(process, stop_heartbeat, intervalo_segundos=60)

        try:
            return_code = process.wait(timeout=timeout)
        finally:
            stop_heartbeat.set()
            thread_stdout.join(timeout=5)
            thread_stderr.join(timeout=5)
            thread_heartbeat.join(timeout=2)

        tempo_decorrido = (datetime.now() - inicio).total_seconds()

        stdout_text = "".join(stdout_buffer)
        stderr_text = "".join(stderr_buffer)

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

        stop_heartbeat.set()

        if process is not None and process.poll() is None:
            try:
                process.kill()
                process.wait(timeout=10)
            except Exception as kill_err:
                print(f"   ⚠️  Falha ao encerrar subprocesso travado: {kill_err}")

        stdout_text = "".join(stdout_buffer)
        stderr_text = "".join(stderr_buffer)

        if stdout_text.strip():
            print("\n   📋 Últimas linhas do stdout antes do timeout:")
            for line in stdout_text.split('\n')[-20:]:
                if line.strip():
                    print(f"      {line}")
        else:
            print("\n   📋 stdout do filho ficou vazio até o timeout (provável travamento antes do primeiro print).")

        if stderr_text.strip():
            print("\n   📋 Últimas linhas do stderr antes do timeout:")
            for line in stderr_text.split('\n')[-20:]:
                if line.strip():
                    print(f"      {line}")

        return {
            'sucesso': False,
            'tempo_segundos': timeout,
            'mensagem': f'Timeout após {timeout // 60} minutos',
            'erros_detectados': [f'TIMEOUT: Processo demorou mais de {timeout // 60} minutos'],
            'stdout': stdout_text[-1000:] if len(stdout_text) > 1000 else stdout_text,
            'stderr': stderr_text[-1000:] if len(stderr_text) > 1000 else stderr_text,
        }
    except Exception as e:
        stop_heartbeat.set()
        if process is not None and process.poll() is None:
            try:
                process.kill()
            except Exception:
                pass
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

        # MAX(dia_utc) global pode ser dominado por 1 linha com data futura inválida (ex.: 2103),
        # gerando "lacunas" negativas e mensagens absurdas. Limitamos ao que é plausível.
        tabela_ref = f"`{dataset_id}.{tabela_bq}`"
        query_bq_sane = f"""
        SELECT MAX(dia_utc) AS ultima_sincronizacao
        FROM {tabela_ref}
        WHERE dia_utc IS NOT NULL
          AND dia_utc <= TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)
        """
        results = client.query(query_bq_sane).result()

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

        data_atual = datetime.now(timezone.utc)
        limite_futuro = data_atual + timedelta(days=3)

        if not ultima_sync:
            # Fallback: tabela só com datas futuras ou filtro muito restrito — tenta MAX bruto para diagnóstico
            query_bq_raw = f"SELECT MAX(dia_utc) AS ultima_sincronizacao FROM {tabela_ref}"
            for row in client.query(query_bq_raw).result():
                if row.ultima_sincronizacao:
                    raw = row.ultima_sincronizacao
                    if isinstance(raw, datetime):
                        if raw.tzinfo is None:
                            raw = raw.replace(tzinfo=timezone.utc)
                        else:
                            raw = raw.astimezone(timezone.utc)
                        if raw > limite_futuro:
                            print(
                                "❌ Dados inconsistentes: MAX(dia_utc) na tabela está no futuro "
                                f"({raw.isoformat()}). Corrija outliers em `{tabela_bq}.dia_utc` no BigQuery."
                            )
                            return {
                                'sucesso': False,
                                'mensagem': (
                                    'MAX(dia_utc) futuro — provável dado corrompido; '
                                    'a verificação de lacunas foi abortada.'
                                ),
                                'lacunas_detectadas': False,
                            }
                        ultima_sync = raw
                break

        if ultima_sync and ultima_sync > limite_futuro:
            print(
                "❌ Última sincronização calculada ainda está no futuro após saneamento — "
                f"verifique `{tabela_bq}.dia_utc`."
            )
            return {
                'sucesso': False,
                'mensagem': 'ultima_sincronizacao inválida (futuro)',
                'lacunas_detectadas': False,
            }

        if not ultima_sync:
            return {
                'sucesso': True,
                'mensagem': 'Tabela vazia — sem dados para verificar lacunas',
                'lacunas_detectadas': False,
            }

        tz_brasil = pytz.timezone('America/Sao_Paulo')
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

        credentials = service_account.Credentials.from_service_account_file(
            str(credentials_path),
            scopes=["https://www.googleapis.com/auth/bigquery"],
        )
        project_id = os.getenv('BIGQUERY_PROJECT_ID') or credentials.project_id
        client = bq_client.Client(credentials=credentials, project=project_id)

        query = f"""
        SELECT
            COUNT(*) AS total_registros,
            MIN(dia_utc) AS data_minima,
            MAX(dia_utc) AS data_maxima,
            COUNT(DISTINCT estacao_id) AS total_estacoes
        FROM `{project_id}.{dataset_id}.{table_id}`
        """
        result = client.query(query).result()
        row = next(iter(result), None)
        if row:
            return {
                'sucesso': True,
                'total_registros': row.total_registros,
                'data_minima': str(row.data_minima),
                'data_maxima': str(row.data_maxima),
                'total_estacoes': row.total_estacoes,
            }
        return {
            'sucesso': True,
            'total_registros': 0,
            'data_minima': None,
            'data_maxima': None,
            'total_estacoes': 0,
        }

    except Exception as e:
        print(f"❌ Erro ao verificar status BigQuery: {e}")
        return {'sucesso': False, 'mensagem': str(e)}
