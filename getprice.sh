#!/usr/bin/env bash
set -euo pipefail

# Unified runner: fetch in parallel into SQLite and generate HTML report,
# or ingest an existing CSV into SQLite then generate the report.

usage() {
  cat <<EOF
Usage:
  $0 [-c config.json] [--debug-csv <path>]
  $0 --from-csv outputprice.csv [-c config.json] [--debug-csv <path>]

Description:
  - Par défaut: fetch parallèle via config.json -> SQLite -> rapport HTML.
  - Option --from-csv: ingère le CSV fourni -> SQLite -> rapport HTML.

Options:
  -c, --config   Chemin du fichier de config (défaut: config.json)
      --debug-csv Chemin du CSV debug à écrire (défaut: outputprice_debug.csv)
  -h, --help     Affiche cette aide
      --from-csv Ingestion à partir d'un CSV existant
EOF
}

CONFIG="config.json"
CSV=""
DEBUG_CSV="outputprice_debug.csv"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -c|--config)
      CONFIG="$2"; shift 2 ;;
    --from-csv)
      CSV="$2"; shift 2 ;;
    --debug-csv)
      DEBUG_CSV="$2"; shift 2 ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "Option inconnue: $1" >&2
      usage; exit 1 ;;
  esac
done

command -v python3 >/dev/null 2>&1 || { echo "python3 est requis. Aborting."; exit 1; }

if [[ -n "$CSV" ]]; then
  echo "[1/2] Ingestion CSV -> SQLite ..."
  python3 scripts/ingest_csv.py --csv "$CSV" --config "$CONFIG" --note "csv run"
else
  echo "[1/2] Fetch parallèle -> SQLite ..."
  python3 scripts/getprice_parallel.py --config "$CONFIG"
fi

echo "[2/2] Génération du rapport HTML ..."
python3 scripts/generate_report.py --config "$CONFIG"

echo "Terminé. Ouvrez le fichier le plus récent dans le dossier reports/."

# Écriture du CSV debug (toujours, pour inspection)
python3 scripts/export_csv.py --config "$CONFIG" --out "$DEBUG_CSV" >/dev/null 2>&1 || true
echo "CSV debug écrit dans: $DEBUG_CSV"
