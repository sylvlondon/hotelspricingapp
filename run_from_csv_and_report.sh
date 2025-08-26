#!/usr/bin/env bash
set -euo pipefail

CFG="${1:-config.json}"
CSV="${2:-outputprice.csv}"

DB_PATH=$(jq -r '.db_path' "$CFG" 2>/dev/null || echo "pricing.db")

echo "[1/2] Ingesting CSV into SQLite ($DB_PATH) ..."
python3 scripts/ingest_csv.py --csv "$CSV" --db "$DB_PATH" --note "manual csv ingest"

echo "[2/2] Generating HTML report ..."
python3 scripts/generate_report.py --db "$DB_PATH" --config "$CFG"

echo "Done. See reports/ for the generated HTML."

