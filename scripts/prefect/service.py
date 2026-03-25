#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Serviço de execução contínua dos workflows Prefect.

Gerencia o loop de execução com:
  - Ajuste automático de intervalo baseado em tempo de execução
  - Reinício automático em caso de falha
  - Detecção de lacunas grandes e ajuste de intervalo
  - Execução como serviço (systemd) ou Docker

Uso:
    python service.py --workflow combinado       (padrão)
    python service.py --workflow pluviometricos
    python service.py --workflow meteorologicos
    python service.py --workflow combinado --intervalo 10
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(Path(__file__).parent))

from interval_manager import calcular_intervalo_ideal

# ---------------------------------------------------------------------------
# Estado do serviço
# ---------------------------------------------------------------------------

estado_servico = {
    'rodando': True,
    'ultima_execucao': None,
    'intervalo_atual_minutos': 5,
    'execucoes_consecutivas_rapidas': 0,
    'arquivo_estado': Path(
        os.getenv(
            'PREFECT_STATE_FILE',
            str(project_root / 'scripts' / 'prefect' / '.prefect_service_state.json')
        )
    ),
}


def carregar_estado():
    """Carrega estado do serviço de arquivo."""
    try:
        if estado_servico['arquivo_estado'].exists():
            with open(estado_servico['arquivo_estado'], 'r') as f:
                estado_servico.update(json.load(f))
    except Exception as e:
        print(f"⚠️  Erro ao carregar estado: {e}")


def salvar_estado():
    """Salva estado do serviço em arquivo."""
    try:
        estado_servico['arquivo_estado'].parent.mkdir(parents=True, exist_ok=True)

        ultima_execucao = estado_servico.get('ultima_execucao')
        if isinstance(ultima_execucao, datetime):
            ultima_execucao_str = ultima_execucao.isoformat()
        elif isinstance(ultima_execucao, str):
            ultima_execucao_str = ultima_execucao
        else:
            ultima_execucao_str = None

        with open(estado_servico['arquivo_estado'], 'w') as f:
            json.dump({
                'ultima_execucao': ultima_execucao_str,
                'intervalo_atual_minutos': estado_servico['intervalo_atual_minutos'],
            }, f, indent=2)
    except Exception as e:
        print(f"⚠️  Erro ao salvar estado: {e}")


def handler_sinal(signum, frame):
    """Handler para sinais de interrupção."""
    print(f"\n⚠️  Recebido sinal {signum}. Encerrando serviço...")
    estado_servico['rodando'] = False
    salvar_estado()
    sys.exit(0)


# ---------------------------------------------------------------------------
# Execução de workflow
# ---------------------------------------------------------------------------

def executar_workflow(workflow_tipo: str) -> Dict:
    """Executa um workflow específico via subprocess (flows.py --run-once).

    Args:
        workflow_tipo: 'combinado', 'pluviometricos' ou 'meteorologicos'.

    Returns:
        Dict com 'sucesso', 'tempo_segundos', 'return_code', etc.
    """
    inicio       = datetime.now()
    script_path  = project_root / 'scripts' / 'prefect' / 'flows.py'

    print(f"🔄 Executando workflow: {workflow_tipo}")
    print(f"   Script: {script_path}")
    print(f"   Início: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'

        process = subprocess.Popen(
            [sys.executable, str(script_path), '--run-once', f'--flow={workflow_tipo}'],
            cwd=str(project_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )

        stdout_bytes, stderr_bytes = process.communicate(timeout=3600)

        tempo_decorrido = (datetime.now() - inicio).total_seconds()
        stdout_text = stdout_bytes.decode('utf-8', errors='replace') if stdout_bytes else ""
        stderr_text = stderr_bytes.decode('utf-8', errors='replace') if stderr_bytes else ""
        sucesso     = process.returncode == 0

        print(f"   {'✅' if sucesso else '❌'} Execução concluída em {tempo_decorrido:.1f} segundos")

        if not sucesso:
            print("\n   📄 Saída (stdout):")
            if stdout_text.strip():
                for line in stdout_text.splitlines()[-20:]:
                    print(f"      {line}")
            else:
                print("      (vazio)")

            print("\n   📄 Erros (stderr):")
            if stderr_text.strip():
                for line in stderr_text.splitlines()[-20:]:
                    print(f"      {line}")
            else:
                print("      (vazio)")
            print()

        return {
            'sucesso': sucesso,
            'tempo_segundos': tempo_decorrido,
            'return_code': process.returncode,
            'stdout': stdout_text,
            'stderr': stderr_text,
        }

    except subprocess.TimeoutExpired:
        print(f"   ⏱️  TIMEOUT: Execução demorou mais de 1 hora")
        return {'sucesso': False, 'tempo_segundos': 3600, 'return_code': -1, 'erro': 'Timeout após 1 hora'}
    except Exception as e:
        print(f"   ❌ Erro ao executar: {e}")
        return {'sucesso': False, 'tempo_segundos': 0, 'return_code': -1, 'erro': str(e)}


# ---------------------------------------------------------------------------
# Loop principal
# ---------------------------------------------------------------------------

def loop_servico(workflow_tipo: str, intervalo_inicial_minutos: int = 5):
    """Loop principal do serviço com ajuste automático de intervalo.

    Args:
        workflow_tipo: Tipo de workflow a executar.
        intervalo_inicial_minutos: Intervalo inicial em minutos.
    """
    print("=" * 80)
    print("🚀 SERVIÇO PREFECT - Execução Contínua")
    print("=" * 80)
    print(f"📊 Workflow: {workflow_tipo}")
    print(f"⏱️  Intervalo inicial: {intervalo_inicial_minutos} minutos")
    print(f"🔄 Modo: Execução contínua com ajuste automático")
    print("=" * 80)
    print()

    signal.signal(signal.SIGINT,  handler_sinal)
    signal.signal(signal.SIGTERM, handler_sinal)

    carregar_estado()

    intervalo_atual = intervalo_inicial_minutos

    while estado_servico['rodando']:
        try:
            inicio_ciclo = datetime.now()

            resultado     = executar_workflow(workflow_tipo)
            estado_servico['ultima_execucao'] = inicio_ciclo

            tempo_execucao         = resultado.get('tempo_segundos', 0)
            tempo_execucao_minutos = tempo_execucao / 60

            intervalo_calculado = calcular_intervalo_ideal(
                tempo_execucao_segundos=tempo_execucao,
                diferenca_dias=0,
                total_registros_pendentes=0,
                intervalo_padrao_minutos=intervalo_inicial_minutos,
            )

            novo_intervalo = intervalo_calculado['intervalo_minutos']

            if novo_intervalo != intervalo_atual:
                if intervalo_calculado.get('ajustado', False):
                    print(f"\n⚠️  AJUSTE DE INTERVALO:")
                    print(f"   {intervalo_calculado.get('motivo', 'Intervalo ajustado')}")
                    print(f"   Anterior: {intervalo_atual} min  →  Novo: {novo_intervalo} min")
                    print(f"   {intervalo_calculado.get('recomendacao', '')}")
                else:
                    # Redução gradual quando execuções estão rápidas
                    if intervalo_atual > intervalo_inicial_minutos and tempo_execucao_minutos < intervalo_atual * 0.5:
                        novo_intervalo = max(intervalo_inicial_minutos, intervalo_atual - 5)
                        if novo_intervalo != intervalo_atual:
                            print(f"\n✅ REDUZINDO INTERVALO:")
                            print(f"   Execução rápida: {tempo_execucao_minutos:.1f} min")
                            print(f"   Anterior: {intervalo_atual} min  →  Novo: {novo_intervalo} min")

                if novo_intervalo != intervalo_atual:
                    intervalo_atual = novo_intervalo
                    estado_servico['intervalo_atual_minutos'] = intervalo_atual
                    salvar_estado()

            tempo_decorrido_ciclo = (datetime.now() - inicio_ciclo).total_seconds()
            tempo_restante        = (intervalo_atual * 60) - tempo_decorrido_ciclo

            if tempo_restante > 0:
                proxima = datetime.now() + timedelta(seconds=tempo_restante)
                print(f"\n⏱️  Próxima execução em {tempo_restante / 60:.1f} min ({proxima})")
                print(f"   Intervalo atual: {intervalo_atual} minutos")
                print()

                while tempo_restante > 0 and estado_servico['rodando']:
                    sleep_time  = min(60, tempo_restante)
                    time.sleep(sleep_time)
                    tempo_restante -= sleep_time
            else:
                print(f"\n⚠️  Execução demorou mais que o intervalo! Continuando imediatamente...")
                time.sleep(10)

        except KeyboardInterrupt:
            print("\n⚠️  Interrompido pelo usuário")
            break
        except Exception as e:
            print(f"\n❌ Erro no loop do serviço: {e}")
            traceback.print_exc()
            time.sleep(60)

    print("\n🛑 Serviço encerrado")
    salvar_estado()


# ---------------------------------------------------------------------------
# Entrada
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='Serviço Prefect — Execução Contínua')
    parser.add_argument(
        '--workflow',
        choices=['combinado', 'pluviometricos', 'meteorologicos'],
        default='combinado',
        help='Tipo de workflow a executar (padrão: combinado)',
    )
    parser.add_argument(
        '--intervalo',
        type=int,
        default=5,
        help='Intervalo inicial em minutos (padrão: 5)',
    )
    args = parser.parse_args()
    loop_servico(args.workflow, args.intervalo)


if __name__ == "__main__":
    main()
