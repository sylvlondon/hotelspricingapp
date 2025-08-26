import argparse
import csv
from datetime import datetime
from pathlib import Path
from typing import Optional

from db import db_session, init_db, get_or_create_hotels_from_list, create_run, upsert_price, read_config


def parse_float(val: str) -> Optional[float]:
    if val is None:
        return None
    s = str(val).strip()
    if s == "" or s.lower() in {"n/a", "na", "null", "none"}:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def ingest_csv(csv_path: Path, db_path: str, config_path: str = "config.json", start_date: Optional[str] = None, end_date: Optional[str] = None, note: Optional[str] = None, timestamp: Optional[str] = None) -> int:
    cfg = read_config(config_path)
    hotels_cfg = cfg.get("hotels", [])
    with db_session(db_path) as conn:
        init_db(conn)
        hotel_ids = get_or_create_hotels_from_list(conn, hotels_cfg)

        # Read CSV
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            if not header:
                raise ValueError("CSV is empty")
            if header[0].lower() != "date":
                raise ValueError("First column must be 'Date'")
            hotel_names = header[1:]
            # Ensure hotels from CSV exist
            for name in hotel_names:
                if name not in hotel_ids:
                    # Create if not present
                    from db import ensure_hotels

                    hotel_ids = ensure_hotels(conn, {name: None})

            # Infer dates if not provided
            dates = []
            rows = []
            for row in reader:
                if not row:
                    continue
                date = row[0]
                dates.append(date)
                rows.append(row)
            if not start_date:
                start_date = min(dates)
            if not end_date:
                end_date = max(dates)

        ts_dt = datetime.fromisoformat(timestamp) if timestamp else None
        run_id = create_run(conn, start_date, end_date, note=note, timestamp=ts_dt)

        # Insert data
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for rec in reader:
                stay_date = rec["Date"].strip()
                for hotel_name in hotel_names:
                    price = parse_float(rec.get(hotel_name))
                    hid = hotel_ids.get(hotel_name)
                    if hid is None:
                        continue
                    upsert_price(conn, run_id, hid, stay_date, currency=cfg.get("fetch", {}).get("currency", "EUR"), price=price, source="csv")
        return run_id


def main():
    ap = argparse.ArgumentParser(description="Ingest a prices CSV into SQLite as a new run")
    ap.add_argument("--csv", required=True, type=Path, help="Path to CSV file (like outputprice.csv)")
    ap.add_argument("--db", default=None, help="Path to SQLite DB (defaults to config db_path)")
    ap.add_argument("--config", default="config.json", help="Path to config JSON")
    ap.add_argument("--start", default=None, help="Run start date YYYY-MM-DD (optional)")
    ap.add_argument("--end", default=None, help="Run end date YYYY-MM-DD (optional)")
    ap.add_argument("--note", default=None, help="Optional note for the run")
    ap.add_argument("--timestamp", default=None, help="ISO timestamp for the run (optional)")
    args = ap.parse_args()

    cfg = read_config(args.config)
    db_path = args.db or cfg.get("db_path", "pricing.db")
    run_id = ingest_csv(args.csv, db_path, config_path=args.config, start_date=args.start, end_date=args.end, note=args.note, timestamp=args.timestamp)
    print(f"Ingested run_id={run_id} into {db_path}")


if __name__ == "__main__":
    main()
