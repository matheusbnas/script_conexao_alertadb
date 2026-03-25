#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gerenciador de intervalo dinâmico para os workflows Prefect.

Calcula o intervalo ideal de sincronização com base em:
  - Tempo de execução da última sincronização
  - Lacunas de dados detectadas
  - Volume de registros pendentes

Se a coleta demorar mais que o intervalo padrão, ajusta automaticamente
para evitar sobreposição de execuções.
"""

from typing import Dict


def calcular_intervalo_ideal(
    tempo_execucao_segundos: float,
    diferenca_dias: int,
    total_registros_pendentes: int = 0,
    intervalo_padrao_minutos: int = 5,
) -> Dict:
    """Calcula o intervalo ideal de sincronização baseado em métricas.

    Args:
        tempo_execucao_segundos: Tempo que a última sincronização levou (segundos).
        diferenca_dias: Diferença em dias entre a última sync e a data atual.
        total_registros_pendentes: Total de registros pendentes no NIMBUS.
        intervalo_padrao_minutos: Intervalo padrão em minutos (padrão: 5).

    Returns:
        Dict com 'intervalo_minutos', 'intervalo_cron', 'ajustado', 'motivo',
        'tempo_execucao_minutos' e 'recomendacao'.
    """
    tempo_execucao_minutos = tempo_execucao_segundos / 60

    # Execução mais lenta que o intervalo → ampliar para evitar sobreposição
    if tempo_execucao_minutos > intervalo_padrao_minutos:
        intervalo_minimo  = int(tempo_execucao_minutos * 1.5)
        intervalo_ajustado = ((intervalo_minimo // 5) + 1) * 5

        return {
            'intervalo_minutos': intervalo_ajustado,
            'intervalo_cron': f"*/{intervalo_ajustado} * * * *",
            'ajustado': True,
            'motivo': (
                f'Execução demorou {tempo_execucao_minutos:.1f} minutos '
                f'(mais que {intervalo_padrao_minutos} min)'
            ),
            'tempo_execucao_minutos': tempo_execucao_minutos,
            'recomendacao': f'Usar intervalo de {intervalo_ajustado} minutos para evitar sobreposição',
        }

    # Lacuna grande → intervalo maior para dar tempo ao backfill
    if diferenca_dias > 1:
        if diferenca_dias > 365:
            intervalo_lacuna = 60
        elif diferenca_dias > 30:
            intervalo_lacuna = 30
        else:
            intervalo_lacuna = 15

        return {
            'intervalo_minutos': intervalo_lacuna,
            'intervalo_cron': f"*/{intervalo_lacuna} * * * *",
            'ajustado': True,
            'motivo': f'Lacuna de {diferenca_dias} dias detectada — usando intervalo maior temporariamente',
            'tempo_execucao_minutos': tempo_execucao_minutos,
            'recomendacao': f'Usar intervalo de {intervalo_lacuna} minutos até lacuna ser resolvida',
        }

    # Volume alto de registros pendentes → intervalo maior para processar backlog
    if total_registros_pendentes > 100_000:
        intervalo_volume = 15

        return {
            'intervalo_minutos': intervalo_volume,
            'intervalo_cron': f"*/{intervalo_volume} * * * *",
            'ajustado': True,
            'motivo': f'{total_registros_pendentes:,} registros pendentes — usando intervalo maior',
            'tempo_execucao_minutos': tempo_execucao_minutos,
            'recomendacao': f'Usar intervalo de {intervalo_volume} minutos até volume reduzir',
        }

    # Condições normais
    return {
        'intervalo_minutos': intervalo_padrao_minutos,
        'intervalo_cron': f"*/{intervalo_padrao_minutos} * * * *",
        'ajustado': False,
        'motivo': 'Condições normais — usando intervalo padrão',
        'tempo_execucao_minutos': tempo_execucao_minutos,
        'recomendacao': f'Manter intervalo de {intervalo_padrao_minutos} minutos',
    }
