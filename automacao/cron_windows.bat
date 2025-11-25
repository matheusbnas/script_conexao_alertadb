@echo off
REM ============================================================================
REM Script de Automação para Windows - Sincronização Incremental
REM ============================================================================
REM
REM Este script pode ser usado com o Agendador de Tarefas do Windows
REM para executar a sincronização incremental automaticamente.
REM
REM ============================================================================
REM CONFIGURAÇÃO:
REM ============================================================================
REM
REM 1. Ajuste o caminho do Python abaixo (PYTHON_PATH)
REM 2. Ajuste o caminho do script (SCRIPT_PATH)
REM 3. Ajuste o caminho do diretório de trabalho (WORK_DIR)
REM 4. Configure no Agendador de Tarefas do Windows para executar a cada 5 minutos
REM
REM ============================================================================

REM Configurações - AJUSTE ESTES VALORES
set PYTHON_PATH=C:\Python39\python.exe
REM Caminho relativo ao diretório raiz do projeto
set PROJECT_ROOT=%~dp0..
set SCRIPT_PATH=%PROJECT_ROOT%\scripts\sincronizar_pluviometricos_novos.py
set WORK_DIR=%PROJECT_ROOT%

REM Mudar para o diretório de trabalho
cd /d "%WORK_DIR%"

REM Executar script em modo único (--once)
"%PYTHON_PATH%" "%SCRIPT_PATH%" --once >> "%WORK_DIR%logs\sincronizacao_%date:~-4,4%%date:~-7,2%%date:~-10,2%_%time:~0,2%%time:~3,2%%time:~6,2%.log" 2>&1

REM Código de saída
exit /b %ERRORLEVEL%

