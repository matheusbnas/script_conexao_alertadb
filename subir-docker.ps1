# Subir sincronização NIMBUS -> BigQuery (Docker)
$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "============================================"
Write-Host " Subir sincronização NIMBUS -> BigQuery (Docker)"
Write-Host "============================================"
Write-Host ""

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host "ERRO: Docker não encontrado." -ForegroundColor Red
    Write-Host "Instale o Docker Desktop: https://www.docker.com/products/docker-desktop/"
    exit 1
}

Write-Host "Construindo imagem (primeira vez pode demorar)..."
docker compose build
if ($LASTEXITCODE -ne 0) { exit 1 }

Write-Host ""
Write-Host "Subindo container em background..."
docker compose up -d
if ($LASTEXITCODE -ne 0) { exit 1 }

Write-Host ""
Write-Host "OK. Serviço rodando a cada 5 minutos." -ForegroundColor Green
Write-Host "Para ver logs: docker compose logs -f prefect-service"
Write-Host "Para parar:     docker compose stop"
Write-Host ""
