#!/usr/bin/env bash
# Démarrage de la plateforme entreprises belges (Linux / macOS)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "=== Plateforme Entreprises Belges ==="

if ! command -v docker &>/dev/null; then
  echo "Erreur: Docker n'est pas installé." >&2
  exit 1
fi

if ! docker info &>/dev/null; then
  echo "Erreur: Docker n'est pas démarré." >&2
  exit 1
fi

KBO_PATH="$(dirname "$ROOT")/KboOpenData_0335_2026_04_19_Full"
if [ ! -d "$KBO_PATH" ]; then
  echo "Avertissement: dossier KBO Open Data absent ($KBO_PATH) — repli sur data/companies.csv"
fi

docker compose up -d --build

echo ""
echo "Services:"
echo "  Airflow UI  : http://localhost:8080  (admin / admin)"
echo "  Streamlit   : http://localhost:8501"
echo "  HDFS        : http://localhost:19870"
echo "  PostgreSQL  : localhost:5432"
