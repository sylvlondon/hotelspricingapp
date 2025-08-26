#!/usr/bin/env bash
set -euo pipefail

CFG="${1:-config.json}"
CSV="${2:-outputprice.csv}"

DB_PATH=$(jq -r '.db_path' "$CFG" 2>/dev/null || echo "pricing.db")

echo "[1/3] Generating 3 fake runs from $CSV ..."
python3 scripts/generate_fake_runs.py --csv "$CSV" --db "$DB_PATH"

echo "[2/3] Generating HTML report ..."
python3 scripts/generate_report.py --db "$DB_PATH" --config "$CFG"

echo "[3/3] Done. See reports/ for the generated HTML."

