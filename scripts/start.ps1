# Demarrage de la plateforme entreprises belges (Windows PowerShell)
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

Write-Host "=== Plateforme Entreprises Belges ===" -ForegroundColor Cyan

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Error "Docker n'est pas installe ou pas dans le PATH. Demarrez Docker Desktop."
}

$dockerInfo = docker info 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Error "Docker Desktop n'est pas demarre. Lancez Docker Desktop puis relancez ce script."
}

$kboPath = Join-Path (Split-Path -Parent $Root) "KboOpenData_0335_2026_04_19_Full"
if (-not (Test-Path $kboPath)) {
    Write-Warning "Dossier KBO Open Data introuvable: $kboPath"
    Write-Warning "Le scraping utilisera data/companies.csv en repli."
}

Write-Host "Construction et demarrage des conteneurs..." -ForegroundColor Yellow
docker compose up -d --build

Write-Host ""
Write-Host "Services disponibles:" -ForegroundColor Green
Write-Host "  Airflow UI  : http://localhost:8080  (admin / admin)"
Write-Host "  Streamlit   : http://localhost:8501"
Write-Host "  HDFS        : http://localhost:19870"
Write-Host "  PostgreSQL  : localhost:5432"
Write-Host ""
Write-Host "Premier run KBO (import CSV) : dag_pipeline_kbo dans Airflow"
Write-Host "Verifier les DAGs : docker compose exec airflow-apiserver airflow dags list"
