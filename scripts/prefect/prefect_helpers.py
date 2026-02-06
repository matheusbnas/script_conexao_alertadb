#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔄 HELPERS PREFECT - BigQuery

Funções auxiliares reutilizáveis para workflows Prefect.
"""

import subprocess
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional

# Caminho base do projeto (scripts/prefect -> scripts -> raiz)
project_root = Path(__file__).parent.parent.parent

def executar_script_sincronizacao(script_path: Path, tipo_dado: str, timeout: int = 1800) -> Dict:
    """Executa um script de sincronização e retorna resultado padronizado.
    
    Args:
        script_path: Caminho para o script Python a ser executado
        tipo_dado: Tipo de dado ('pluviometricos' ou 'meteorologicos')
        timeout: Timeout em segundos (padrão: 1800 = 30 minutos)
    
    Returns:
        Dict com informações sobre a execução
    """
    try:
        print(f"🔄 Iniciando sincronização incremental de {tipo_dado}...")
        print(f"   Script: {script_path}")
        print(f"   Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        inicio = datetime.now()
        
        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'
        
        import subprocess as sp
        process = sp.Popen(
            [sys.executable, str(script_path), '--once'],
            cwd=str(project_root),
            stdout=sp.PIPE,
            stderr=sp.PIPE,
            env=env,
            bufsize=0
        )
        
        stdout_bytes, stderr_bytes = process.communicate(timeout=timeout)
        return_code = process.returncode
        
        tempo_decorrido = (datetime.now() - inicio).total_seconds()
        
        # Decodificar manualmente com UTF-8
        stdout_text = ""
        stderr_text = ""
        
        try:
            if stdout_bytes:
                stdout_text = stdout_bytes.decode('utf-8', errors='replace')
        except Exception as e:
            stdout_text = f"[Erro ao decodificar stdout: {type(e).__name__}: {str(e)[:100]}]"
        
        try:
            if stderr_bytes:
                stderr_text = stderr_bytes.decode('utf-8', errors='replace')
        except Exception as e:
            stderr_text = f"[Erro ao decodificar stderr: {type(e).__name__}: {str(e)[:100]}]"
        
        stdout_text = stdout_text or ""
        stderr_text = stderr_text or ""
        output_completo = stdout_text + stderr_text
        
        class Result:
            def __init__(self, returncode, stdout, stderr):
                self.returncode = returncode
                self.stdout = stdout
                self.stderr = stderr
        
        result = Result(return_code, stdout_text, stderr_text)
        erros_detectados = []
        
        # Verificar erros comuns
        if "Resources exceeded" in output_completo or "10000 partitions" in output_completo:
            erros_detectados.append("ERRO: Limite de partições excedido (precisa recriar tabela com particionamento por MÊS)")
        
        if "TIMESTAMP_NANOS" in output_completo or "Invalid timestamp" in output_completo:
            erros_detectados.append("ERRO: Problema com formato de timestamp")
        
        if "ConnectionResetError" in output_completo or "connection" in output_completo.lower():
            erros_detectados.append("ERRO: Problema de conexão com banco de dados")
        
        if "ERRO CRÍTICO" in output_completo or "❌" in output_completo:
            erros_detectados.append("ERRO: Erro crítico detectado no script")
        
        if result.returncode == 0:
            if erros_detectados:
                print(f"⚠️  Sincronização concluída mas com avisos:")
                for erro in erros_detectados:
                    print(f"   {erro}")
            else:
                print(f"✅ Sincronização incremental concluída com sucesso em {tempo_decorrido:.1f} segundos")
            
            # Extrair informações úteis do output
            registros_processados = 0
            if "registros" in output_completo.lower():
                import re
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
                'output_resumo': output_completo[-1000:] if len(output_completo) > 1000 else output_completo
            }
        else:
            print(f"❌ ERRO na sincronização incremental:")
            print(f"   Return code: {result.returncode}")
            print(f"   Erros detectados: {len(erros_detectados)}")
            for erro in erros_detectados:
                print(f"   {erro}")
            
            print(f"\n   📋 Últimas linhas do stderr:")
            if stderr_text:
                stderr_lines = stderr_text.split('\n')[-20:]
                for line in stderr_lines:
                    if line.strip():
                        print(f"      {line}")
            else:
                print(f"      (stderr vazio)")
            
            return {
                'sucesso': False,
                'tempo_segundos': tempo_decorrido,
                'return_code': result.returncode,
                'mensagem': f'Erro na sincronização (code: {result.returncode})',
                'erros_detectados': erros_detectados,
                'stderr': stderr_text[-1000:] if len(stderr_text) > 1000 else stderr_text
            }
            
    except subprocess.TimeoutExpired:
        print(f"⏱️  TIMEOUT: Sincronização demorou mais de {timeout // 60} minutos")
        print(f"   Isso pode indicar problema de conexão ou volume muito grande de dados")
        return {
            'sucesso': False,
            'tempo_segundos': timeout,
            'mensagem': f'Timeout após {timeout // 60} minutos - verifique conexão e volume de dados',
            'erros_detectados': [f'TIMEOUT: Processo demorou mais de {timeout // 60} minutos']
        }
    except Exception as e:
        print(f"❌ ERRO FATAL ao executar sincronização incremental: {e}")
        import traceback
        traceback.print_exc()
        return {
            'sucesso': False,
            'tempo_segundos': 0,
            'mensagem': f'Erro fatal: {str(e)}',
            'erros_detectados': [f'Erro fatal: {str(e)}']
        }

def verificar_status_bigquery_tabela(credentials_path: Path, dataset_id: str, table_id: str) -> Dict:
    """Verifica o status de uma tabela específica no BigQuery.
    
    Args:
        credentials_path: Caminho para credentials.json
        dataset_id: ID do dataset BigQuery
        table_id: ID da tabela BigQuery
    
    Returns:
        Dict com informações sobre a tabela
    """
    try:
        if not credentials_path.exists():
            return {
                'sucesso': False,
                'mensagem': f'Credenciais não encontradas em: {credentials_path}'
            }
        
        try:
            from prefect_gcp import GcpCredentials
            from prefect_gcp.bigquery import BigQueryWarehouse
            
            gcp_credentials = GcpCredentials(service_account_file=str(credentials_path))
            # Ambas as tabelas usam coluna dia_utc (UTC)
            coluna_data = "dia_utc"
            with BigQueryWarehouse(gcp_credentials=gcp_credentials) as warehouse:
                query = f"""
                SELECT 
                    COUNT(*) as total_registros,
                    MIN({coluna_data}) as data_minima,
                    MAX({coluna_data}) as data_maxima,
                    COUNT(DISTINCT estacao_id) as total_estacoes
                FROM `{dataset_id}.{table_id}`
                """
                
                result = warehouse.fetch_one(query)
                
                if result:
                    return {
                        'sucesso': True,
                        'total_registros': result[0],
                        'data_minima': str(result[1]),
                        'data_maxima': str(result[2]),
                        'total_estacoes': result[3]
                    }
        except ImportError:
            pass
        except Exception as e:
            print(f"⚠️  Erro ao usar Prefect GCP: {e}")
        
        return {
            'sucesso': True,
            'mensagem': 'Verificação básica concluída (instale prefect-gcp para estatísticas detalhadas)'
        }
                
    except Exception as e:
        print(f"❌ Erro ao verificar status BigQuery: {e}")
        return {
            'sucesso': False,
            'mensagem': str(e)
        }

