#!/usr/bin/env bash
set -euo pipefail

# Run the parallel fetch based on config, then generate the HTML report.

usage() {
  echo "Usage: $0 [-c config.json]";
}

CONFIG="config.json"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -c|--config)
      CONFIG="$2"; shift 2 ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

command -v python3 >/dev/null 2>&1 || { echo "python3 is required." >&2; exit 1; }

echo "[1/2] Fetch parallèle -> SQLite ..."
PYTHONUNBUFFERED=1 python3 -u "$(dirname "$0")/getprice_parallel.py" --config "$CONFIG"

echo "[2/2] Génération du rapport HTML ..."
python3 "$(dirname "$0")/generate_report.py" --config "$CONFIG"

echo "Terminé. Rapport dans le dossier 'reports/'."
