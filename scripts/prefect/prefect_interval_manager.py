#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
⏱️ GERENCIADOR DE INTERVALO DINÂMICO - Prefect

Calcula o intervalo ideal de sincronização baseado em:
- Lacunas de dados detectadas
- Tempo de execução da última sincronização
- Volume de dados pendentes

Se a coleta demorar mais de 5 minutos, ajusta o intervalo para garantir
que não haja sobreposição de execuções.
"""

from datetime import datetime, timedelta
from typing import Dict, Optional

def calcular_intervalo_ideal(
    tempo_execucao_segundos: float,
    diferenca_dias: int,
    total_registros_pendentes: int = 0,
    intervalo_padrao_minutos: int = 5
) -> Dict:
    """Calcula o intervalo ideal de sincronização baseado em métricas.
    
    Args:
        tempo_execucao_segundos: Tempo que a última sincronização levou (segundos)
        diferenca_dias: Diferença em dias entre última sync e data atual
        total_registros_pendentes: Total de registros pendentes no NIMBUS
        intervalo_padrao_minutos: Intervalo padrão em minutos (padrão: 5)
    
    Returns:
        Dict com intervalo calculado e recomendações
    """
    tempo_execucao_minutos = tempo_execucao_segundos / 60
    
    # Se a execução demorou mais que o intervalo padrão, ajustar
    if tempo_execucao_minutos > intervalo_padrao_minutos:
        # Intervalo mínimo = tempo de execução + margem de segurança (50%)
        intervalo_minimo = int(tempo_execucao_minutos * 1.5)
        # Arredondar para cima para múltiplo de 5 minutos
        intervalo_ajustado = ((intervalo_minimo // 5) + 1) * 5
        
        return {
            'intervalo_minutos': intervalo_ajustado,
            'intervalo_cron': f"*/{intervalo_ajustado} * * * *",
            'ajustado': True,
            'motivo': f'Execução demorou {tempo_execucao_minutos:.1f} minutos (mais que {intervalo_padrao_minutos} min)',
            'tempo_execucao_minutos': tempo_execucao_minutos,
            'recomendacao': f'Usar intervalo de {intervalo_ajustado} minutos para evitar sobreposição'
        }
    
    # Se há lacuna grande (mais de 1 dia), usar intervalo maior temporariamente
    if diferenca_dias > 1:
        # Para lacunas grandes, usar intervalo maior para dar tempo de processar
        if diferenca_dias > 365:
            intervalo_lacuna = 60  # 1 hora para lacunas muito grandes
        elif diferenca_dias > 30:
            intervalo_lacuna = 30  # 30 minutos para lacunas grandes
        else:
            intervalo_lacuna = 15  # 15 minutos para lacunas médias
        
        return {
            'intervalo_minutos': intervalo_lacuna,
            'intervalo_cron': f"*/{intervalo_lacuna} * * * *",
            'ajustado': True,
            'motivo': f'Lacuna de {diferenca_dias} dias detectada - usando intervalo maior temporariamente',
            'tempo_execucao_minutos': tempo_execucao_minutos,
            'recomendacao': f'Usar intervalo de {intervalo_lacuna} minutos até lacuna ser resolvida'
        }
    
    # Se há muitos registros pendentes, aumentar intervalo temporariamente
    if total_registros_pendentes > 100000:
        intervalo_volume = 15  # 15 minutos para volumes grandes
        
        return {
            'intervalo_minutos': intervalo_volume,
            'intervalo_cron': f"*/{intervalo_volume} * * * *",
            'ajustado': True,
            'motivo': f'{total_registros_pendentes:,} registros pendentes - usando intervalo maior',
            'tempo_execucao_minutos': tempo_execucao_minutos,
            'recomendacao': f'Usar intervalo de {intervalo_volume} minutos até volume reduzir'
        }
    
    # Caso normal: usar intervalo padrão
    return {
        'intervalo_minutos': intervalo_padrao_minutos,
        'intervalo_cron': f"*/{intervalo_padrao_minutos} * * * *",
        'ajustado': False,
        'motivo': 'Condições normais - usando intervalo padrão',
        'tempo_execucao_minutos': tempo_execucao_minutos,
        'recomendacao': f'Manter intervalo de {intervalo_padrao_minutos} minutos'
    }

# Função removida: obter_intervalo_da_ultima_execucao
# Não estava sendo usada. Use calcular_intervalo_ideal diretamente.
