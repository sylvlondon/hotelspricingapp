import argparse
import csv
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from db import db_session, init_db, read_config


def date_range(start: str, end: str) -> List[str]:
    ds = datetime.fromisoformat(start).date()
    de = datetime.fromisoformat(end).date()
    n = (de - ds).days
    return [(ds + timedelta(days=i)).isoformat() for i in range(n + 1)]


def export_run_to_csv(db_path: str, cfg_path: str, out_path: Path, run_id: Optional[int] = None):
    cfg = read_config(cfg_path)
    with db_session(db_path) as conn:
        init_db(conn)
        cur = conn.cursor()

        # Determine run
        if run_id is None:
            cur.execute("SELECT * FROM runs ORDER BY datetime(run_timestamp) DESC LIMIT 1")
        else:
            cur.execute("SELECT * FROM runs WHERE id = ?", (run_id,))
        run = cur.fetchone()
        if not run:
            raise RuntimeError("No run found to export")

        # Hotels order: config order first, then any others by name
        hotels_rows = list(cur.execute("SELECT id, name FROM hotels ORDER BY name"))
        id_by_name = {r["name"]: r["id"] for r in hotels_rows}
        name_by_id = {r["id"]: r["name"] for r in hotels_rows}
        cfg_names = [h["name"] for h in cfg.get("hotels", [])]
        ordered_names = [n for n in cfg_names if n in id_by_name]
        for n in sorted(id_by_name.keys()):
            if n not in ordered_names:
                ordered_names.append(n)
        ordered_ids = [id_by_name[n] for n in ordered_names]

        # Determine dates to output
        start_date = run["start_date"]
        end_date = run["end_date"]
        if not start_date or not end_date:
            # Fallback to min/max dates in prices for this run
            cur.execute("SELECT MIN(stay_date), MAX(stay_date) FROM prices WHERE run_id = ?", (run["id"],))
            m = cur.fetchone()
            if not m or not m[0]:
                raise RuntimeError("Run has no prices to export")
            start_date, end_date = m[0], m[1]
        dates = date_range(start_date, end_date)

        # Fetch prices
        cur.execute(
            "SELECT hotel_id, stay_date, price FROM prices WHERE run_id = ?",
            (run["id"],),
        )
        by_hotel_date: Dict[int, Dict[str, Optional[float]]] = {hid: {} for hid in ordered_ids}
        for row in cur.fetchall():
            by_hotel_date.setdefault(row["hotel_id"], {})[row["stay_date"]] = row["price"]

        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Date", *ordered_names])
            for d in dates:
                row = [d]
                for hid in ordered_ids:
                    v = by_hotel_date.get(hid, {}).get(d)
                    row.append("null" if v is None else (int(v) if abs(v - round(v)) < 1e-6 else v))
                writer.writerow(row)

        print(f"Exported run_id={run['id']} to {out_path}")


def main():
    ap = argparse.ArgumentParser(description="Export a run from SQLite back to CSV")
    ap.add_argument("--db", default=None, help="Path to DB (defaults to config)")
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--out", type=Path, default=Path("outputprice_debug.csv"))
    ap.add_argument("--run-id", type=int, default=None)
    args = ap.parse_args()

    cfg = read_config(args.config)
    db_path = args.db or cfg.get("db_path", "pricing.db")
    export_run_to_csv(db_path, args.config, args.out, run_id=args.run_id)


if __name__ == "__main__":
    main()

