#!/usr/bin/env bash
set -euo pipefail

# Generate the HTML report from the current SQLite DB using config.

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

python3 "$(dirname "$0")/generate_report.py" --config "$CONFIG"

