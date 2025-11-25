# ============================================================================
# Script de Automação para Windows PowerShell - Sincronização Incremental
# ============================================================================
#
# Este script pode ser usado com o Agendador de Tarefas do Windows
# para executar a sincronização incremental automaticamente.
#
# ============================================================================
# CONFIGURAÇÃO:
# ============================================================================
#
# 1. Ajuste o caminho do Python abaixo ($pythonPath)
# 2. Ajuste o caminho do script ($scriptPath)
# 3. Configure no Agendador de Tarefas do Windows para executar a cada 5 minutos
#
# ============================================================================

# Configurações - AJUSTE ESTES VALORES
$pythonPath = "C:\Python39\python.exe"
# Caminho relativo ao diretório raiz do projeto
$projectRoot = Split-Path -Parent $PSScriptRoot
$scriptPath = Join-Path $projectRoot "scripts\sincronizar_pluviometricos_novos.py"
$workDir = $projectRoot
$logDir = Join-Path $workDir "logs"
$logFile = Join-Path $logDir "sincronizacao_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"

# Criar diretório de logs se não existir
if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir -Force | Out-Null
}

# Mudar para o diretório de trabalho
Set-Location $workDir

# Executar script em modo único (--once)
try {
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Add-Content -Path $logFile -Value "=========================================="
    Add-Content -Path $logFile -Value "[$timestamp] Iniciando sincronização..."
    Add-Content -Path $logFile -Value "=========================================="
    
    & $pythonPath $scriptPath --once 2>&1 | Tee-Object -FilePath $logFile -Append
    
    $exitCode = $LASTEXITCODE
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Add-Content -Path $logFile -Value "[$timestamp] Sincronização concluída. Código de saída: $exitCode"
    
    exit $exitCode
}
catch {
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Add-Content -Path $logFile -Value "[$timestamp] ERRO: $($_.Exception.Message)"
    exit 1
}

