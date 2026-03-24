#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔄 SERVIÇO PREFECT - Execução Contínua com Ajuste Automático

Este script gerencia a execução contínua dos workflows Prefect com:
- Ajuste automático de intervalo baseado em tempo de execução
- Reinício automático em caso de falha
- Detecção de lacunas grandes e ajuste de intervalo
- Execução como serviço (systemd) ou Docker

Uso:
    python prefect_service.py --workflow combinado
    python prefect_service.py --workflow pluviometricos
    python prefect_service.py --workflow meteorologicos
"""

import os
import sys
import time
import signal
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict
import json

# Caminho base do projeto
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(Path(__file__).parent))

from prefect_interval_manager import calcular_intervalo_ideal

# Estado do serviço
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
    )
}

def carregar_estado():
    """Carrega estado do serviço de arquivo."""
    try:
        if estado_servico['arquivo_estado'].exists():
            with open(estado_servico['arquivo_estado'], 'r') as f:
                estado = json.load(f)
                estado_servico.update(estado)
    except Exception as e:
        print(f"⚠️  Erro ao carregar estado: {e}")

def salvar_estado():
    """Salva estado do serviço em arquivo."""
    try:
        estado_servico['arquivo_estado'].parent.mkdir(parents=True, exist_ok=True)
        
        # Garantir que ultima_execucao esteja em formato serializável
        ultima_execucao = estado_servico.get('ultima_execucao')
        if isinstance(ultima_execucao, datetime):
            ultima_execucao_str = ultima_execucao.isoformat()
        elif isinstance(ultima_execucao, str):
            # Já está serializado
            ultima_execucao_str = ultima_execucao
        else:
            ultima_execucao_str = None
        
        with open(estado_servico['arquivo_estado'], 'w') as f:
            json.dump({
                'ultima_execucao': ultima_execucao_str,
                'intervalo_atual_minutos': estado_servico['intervalo_atual_minutos']
            }, f, indent=2)
    except Exception as e:
        print(f"⚠️  Erro ao salvar estado: {e}")

def handler_sinal(signum, frame):
    """Handler para sinais de interrupção."""
    print(f"\n⚠️  Recebido sinal {signum}. Encerrando serviço...")
    estado_servico['rodando'] = False
    salvar_estado()
    sys.exit(0)

def executar_workflow(workflow_tipo: str) -> Dict:
    """Executa um workflow específico.
    
    Args:
        workflow_tipo: 'combinado', 'pluviometricos' ou 'meteorologicos'
    
    Returns:
        Dict com resultado da execução
    """
    inicio = datetime.now()
    
    if workflow_tipo == 'combinado':
        script_path = project_root / 'scripts' / 'prefect' / 'prefect_workflow_combinado.py'
    elif workflow_tipo == 'pluviometricos':
        script_path = project_root / 'scripts' / 'prefect' / 'prefect_workflow_pluviometricos.py'
    elif workflow_tipo == 'meteorologicos':
        script_path = project_root / 'scripts' / 'prefect' / 'prefect_workflow_meteorologicos.py'
    else:
        raise ValueError(f"Workflow tipo inválido: {workflow_tipo}")
    
    print(f"🔄 Executando workflow: {workflow_tipo}")
    print(f"   Script: {script_path}")
    print(f"   Início: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'
        
        process = subprocess.Popen(
            [sys.executable, str(script_path), '--run-once'],
            cwd=str(project_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env
        )
        
        stdout_bytes, stderr_bytes = process.communicate(timeout=3600)  # 1 hora timeout
        
        tempo_decorrido = (datetime.now() - inicio).total_seconds()
        
        stdout_text = stdout_bytes.decode('utf-8', errors='replace') if stdout_bytes else ""
        stderr_text = stderr_bytes.decode('utf-8', errors='replace') if stderr_bytes else ""
        
        sucesso = process.returncode == 0
        
        print(f"   {'✅' if sucesso else '❌'} Execução concluída em {tempo_decorrido:.1f} segundos")

        # Em caso de falha, imprimir um resumo do stdout/stderr para facilitar o diagnóstico
        if not sucesso:
            print("\n   📄 Saída (stdout) do workflow:")
            if stdout_text.strip():
                for line in stdout_text.splitlines()[-20:]:
                    print(f"      {line}")
            else:
                print("      (stdout vazio)")

            print("\n   📄 Erros (stderr) do workflow:")
            if stderr_text.strip():
                for line in stderr_text.splitlines()[-20:]:
                    print(f"      {line}")
            else:
                print("      (stderr vazio)")
            print()
        
        return {
            'sucesso': sucesso,
            'tempo_segundos': tempo_decorrido,
            'return_code': process.returncode,
            'stdout': stdout_text,
            'stderr': stderr_text
        }
        
    except subprocess.TimeoutExpired:
        print(f"   ⏱️  TIMEOUT: Execução demorou mais de 1 hora")
        return {
            'sucesso': False,
            'tempo_segundos': 3600,
            'return_code': -1,
            'erro': 'Timeout após 1 hora'
        }
    except Exception as e:
        print(f"   ❌ Erro ao executar: {e}")
        return {
            'sucesso': False,
            'tempo_segundos': 0,
            'return_code': -1,
            'erro': str(e)
        }

def loop_servico(workflow_tipo: str, intervalo_inicial_minutos: int = 5):
    """Loop principal do serviço com ajuste automático de intervalo.
    
    Args:
        workflow_tipo: Tipo de workflow a executar
        intervalo_inicial_minutos: Intervalo inicial em minutos
    """
    print("=" * 80)
    print("🚀 SERVIÇO PREFECT - Execução Contínua")
    print("=" * 80)
    print(f"📊 Workflow: {workflow_tipo}")
    print(f"⏱️  Intervalo inicial: {intervalo_inicial_minutos} minutos")
    print(f"🔄 Modo: Execução contínua com ajuste automático")
    print("=" * 80)
    print()
    
    # Configurar handlers de sinal
    signal.signal(signal.SIGINT, handler_sinal)
    signal.signal(signal.SIGTERM, handler_sinal)
    
    # Carregar estado anterior
    carregar_estado()
    
    intervalo_atual = intervalo_inicial_minutos
    ultimo_resultado = None
    
    while estado_servico['rodando']:
        try:
            inicio_ciclo = datetime.now()
            
            # Executar workflow
            resultado = executar_workflow(workflow_tipo)
            ultimo_resultado = resultado
            estado_servico['ultima_execucao'] = inicio_ciclo
            
            # Calcular intervalo ideal baseado no resultado usando o gerenciador
            tempo_execucao = resultado.get('tempo_segundos', 0)
            tempo_execucao_minutos = tempo_execucao / 60
            
            # Usar gerenciador para calcular intervalo ideal
            intervalo_calculado = calcular_intervalo_ideal(
                tempo_execucao_segundos=tempo_execucao,
                diferenca_dias=0,  # Não temos info de lacunas aqui, mas o gerenciador ajusta por tempo
                total_registros_pendentes=0,
                intervalo_padrao_minutos=intervalo_inicial_minutos
            )
            
            novo_intervalo = intervalo_calculado['intervalo_minutos']
            
            # Se intervalo foi ajustado, atualizar
            if novo_intervalo != intervalo_atual:
                if intervalo_calculado.get('ajustado', False):
                    print(f"\n⚠️  AJUSTE DE INTERVALO:")
                    print(f"   {intervalo_calculado.get('motivo', 'Intervalo ajustado')}")
                    print(f"   Intervalo anterior: {intervalo_atual} minutos")
                    print(f"   Novo intervalo: {novo_intervalo} minutos")
                    print(f"   {intervalo_calculado.get('recomendacao', '')}")
                else:
                    # Reduzir gradualmente se execução está rápida
                    if intervalo_atual > intervalo_inicial_minutos and tempo_execucao_minutos < intervalo_atual * 0.5:
                        novo_intervalo = max(intervalo_inicial_minutos, intervalo_atual - 5)
                        if novo_intervalo != intervalo_atual:
                            print(f"\n✅ REDUZINDO INTERVALO:")
                            print(f"   Execução rápida: {tempo_execucao_minutos:.1f} minutos")
                            print(f"   Intervalo anterior: {intervalo_atual} minutos")
                            print(f"   Novo intervalo: {novo_intervalo} minutos")
                
                if novo_intervalo != intervalo_atual:
                    intervalo_atual = novo_intervalo
                    estado_servico['intervalo_atual_minutos'] = intervalo_atual
                    salvar_estado()
            
            # Calcular próximo ciclo
            tempo_decorrido_ciclo = (datetime.now() - inicio_ciclo).total_seconds()
            tempo_restante = (intervalo_atual * 60) - tempo_decorrido_ciclo
            
            if tempo_restante > 0:
                print(f"\n⏱️  Próxima execução em {tempo_restante / 60:.1f} minutos ({datetime.now() + timedelta(seconds=tempo_restante)})")
                print(f"   Intervalo atual: {intervalo_atual} minutos")
                print()
                
                # Aguardar até próxima execução
                while tempo_restante > 0 and estado_servico['rodando']:
                    sleep_time = min(60, tempo_restante)  # Verificar a cada minuto
                    time.sleep(sleep_time)
                    tempo_restante -= sleep_time
            else:
                print(f"\n⚠️  Execução demorou mais que o intervalo! Continuando imediatamente...")
                time.sleep(10)  # Pequena pausa antes de continuar
            
        except KeyboardInterrupt:
            print("\n⚠️  Interrompido pelo usuário")
            break
        except Exception as e:
            print(f"\n❌ Erro no loop do serviço: {e}")
            import traceback
            traceback.print_exc()
            # Aguardar antes de tentar novamente
            time.sleep(60)
    
    print("\n🛑 Serviço encerrado")
    salvar_estado()

def main():
    """Função principal."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Serviço Prefect - Execução Contínua')
    parser.add_argument(
        '--workflow',
        choices=['combinado', 'pluviometricos', 'meteorologicos'],
        default='combinado',
        help='Tipo de workflow a executar (padrão: combinado)'
    )
    parser.add_argument(
        '--intervalo',
        type=int,
        default=5,
        help='Intervalo inicial em minutos (padrão: 5)'
    )
    
    args = parser.parse_args()
    
    loop_servico(args.workflow, args.intervalo)

if __name__ == "__main__":
    main()
