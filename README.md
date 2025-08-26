Pricing App — SQLite + Report

What's included
- `config.json`: configuration of hotels, spike rules (low/medium/high thresholds), run window dates, and fetch options.
- `scripts/db.py`: SQLite schema and helpers.
- `scripts/ingest_csv.py`: import a CSV (like your `outputprice.csv`) as a new run in the DB.
- `scripts/generate_fake_runs.py`: create 3 fake runs (3-day intervals) from `outputprice.csv` with realistic noise and spikes.
- `scripts/generate_report.py`: generate a dated HTML report from the DB, highlighting spikes.
- `scripts/getprice_parallel.py`: optional parallel fetcher that hits the API and saves directly into SQLite.
- `scripts/run_parallel_and_report.sh` and `run_parallel_and_report.sh`: run the parallel fetch and generate the HTML report (prints live progress).
- `scripts/report.sh` and `report.sh`: generate only the HTML report.
- `scripts/make_reports_index.py`: build `reports/index.html` linking to the latest and all reports (used by GitHub Pages).
- `bootstrap_fake_data.sh`: convenience script to generate fake runs from `outputprice.csv` and produce a report.
- `run_from_csv_and_report.sh`: ingest a CSV into SQLite and immediately generate a report.

Quick start (fake data + report)
1. Adjust `config.json` if needed (dates, thresholds, hotels).
2. Run:
   - `./bootstrap_fake_data.sh`
3. Open the HTML from `reports/` in your browser.

Ingest your next CSV run and report
- `./run_from_csv_and_report.sh` (uses `outputprice.csv` by default)

Parallel API fetch into DB (optional)
If you prefer to fetch directly (parallelized by hotel) instead of the shell script:
- `python3 scripts/getprice_parallel.py --config config.json`

This creates a new run in SQLite without writing an intermediate CSV.

Report details
- Shows, for each stay date:
  - Latest price per hotel (from the most recent run)
  - Row average across hotels
  - Delta vs previous run for each hotel and for the row average
- Highlighting is applied to specific cells:
-  - Each hotel cell shows an inline delta vs previous run.
-  - `Avg` is highlighted if current row-average spikes vs the trailing average of the previous `lookback_days_avg` dates.
-  - `Δ Avg vs prev` is highlighted if the row-average spikes vs run n-`avg_prev_offset` (default 1).
- Default thresholds: low 10%, medium 20%, high 30% (editable in `config.json`).

Keeping your existing `getprice.sh`
You can keep using `getprice.sh` to produce a CSV as before. After it finishes:
- Import the CSV to DB and generate a report via `./run_from_csv_and_report.sh`.

DB file
- Default DB is `pricing.db` (override in `config.json`).

**CLI Reference**
- **Common option**: most scripts accept `--db` (override DB path) and `--config` (path to config). If omitted, scripts read `db_path` from `config.json`.

- **Ingest CSV to DB** (create a run from `outputprice.csv`):
  - Basic: `python3 scripts/ingest_csv.py --csv outputprice.csv --config config.json`
  - With extras: `python3 scripts/ingest_csv.py --csv outputprice.csv --config config.json --start 2025-01-01 --end 2025-01-31 --note "manual import" --timestamp 2025-01-31T18:30:00`
  - Override DB path: `python3 scripts/ingest_csv.py --csv outputprice.csv --db pricing.db`

- **Generate HTML Report** (latest run is the reference):
  - Command: `python3 scripts/generate_report.py --config config.json`
  - Optional DB path: `python3 scripts/generate_report.py --config config.json --db pricing.db`
  - Output is written under `reports/` with a timestamped filename.

- **Export a Run to CSV**:
  - Latest run: `python3 scripts/export_csv.py --config config.json`
  - Specific run: `python3 scripts/export_csv.py --config config.json --run-id 42`
  - Custom output path: `python3 scripts/export_csv.py --config config.json --out reports/run42.csv`

- **Fetch Prices in Parallel (API)**:
  - Command: `python3 scripts/getprice_parallel.py --config config.json`
  - Optionally override DB path: `--db pricing.db`

- **Show Runs** (list runs with counts):
  - All runs: `python3 scripts/show_runs.py --config config.json`
  - Limit: `python3 scripts/show_runs.py --config config.json --limit 5`
  - Filter by timestamp: `python3 scripts/show_runs.py --config config.json --since 2025-01-01 --until 2025-01-31`
  - JSON output: `python3 scripts/show_runs.py --config config.json --json`

- **Delete Run(s)** (remove runs and their prices):
  - By ID: `python3 scripts/delete_run.py --config config.json --run-id 123`
  - Latest run: `python3 scripts/delete_run.py --config config.json --latest`
  - By timestamp range (inclusive): `python3 scripts/delete_run.py --config config.json --between 2025-01-01 2025-01-31`
    - Accepts `YYYY-MM-DD` (whole day) or full ISO timestamps.
  - Override DB path: `--db pricing.db`

- **Reset DB** (delete ALL data: runs, prices, hotels):
  - Prompted confirmation: `python3 scripts/reset_db.py --config config.json`
  - Non-interactive: `python3 scripts/reset_db.py --config config.json --yes`
  - With space reclamation: `python3 scripts/reset_db.py --config config.json --yes --vacuum`

Notes
- Missing/invalid prices are ignored for averages and deltas.
- The code uses only the Python standard library.

CI (GitHub Actions + Pages)
- Workflow: `.github/workflows/run-and-deploy.yml`
-  - Runs every 3 days at 03:00 UTC and on manual dispatch.
-  - Dynamic date window: start = today + 1 day, end = today + 15 days (override when dispatching via inputs `start_offset`, `end_offset`).
-  - Executes the parallel fetch and report generation.
-  - Builds `reports/index.html` and deploys the `reports/` folder to GitHub Pages.
-  - Uploads `reports/`, `pricing.db`, and CSVs as a build artifact.
 -  - Persists `pricing.db` across runs using Actions Cache (restore before run, save after run). Retention: 30 days.

Enable Pages
- In GitHub Settings → Pages, set Source to “GitHub Actions”. The workflow included here handles deploy.

Push this code to GitHub
1. Initialize git and set origin (first time):
   - `git init`
   - `git add .`
   - `git commit -m "Initial import of hotels pricing app"`
   - `git branch -M main`
   - `git remote add origin https://github.com/sylvlondon/hotelspricingapp.git`
   - `git push -u origin main`
2. Trigger the workflow: Actions → “Fetch Rates, Build Report, Deploy Pages” → Run workflow.
3. See Pages site at: `https://sylvlondon.github.io/hotelspricingapp/`
