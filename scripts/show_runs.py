import argparse
import json
from datetime import datetime
from typing import Dict

from db import db_session, init_db, read_config


def main():
    ap = argparse.ArgumentParser(description="Show runs stored in SQLite, newest first")
    ap.add_argument("--config", default="config.json", help="Path to config JSON")
    ap.add_argument("--db", default=None, help="Path to DB (defaults to config)")
    ap.add_argument("--limit", type=int, default=None, help="Max number of runs to show")
    ap.add_argument("--since", default=None, help="Only runs with timestamp >= this (YYYY-MM-DD or ISO)")
    ap.add_argument("--until", default=None, help="Only runs with timestamp <= this (YYYY-MM-DD or ISO)")
    ap.add_argument("--json", action="store_true", help="Output as JSON instead of table")
    args = ap.parse_args()

    cfg = read_config(args.config)
    db_path = args.db or cfg.get("db_path", "pricing.db")

    def parse_bound(val: str) -> str:
        try:
            dt = datetime.fromisoformat(val)
        except ValueError:
            dt = datetime.fromisoformat(val + "T00:00:00")
        return dt.isoformat(timespec="seconds")

    with db_session(db_path) as conn:
        init_db(conn)
        cur = conn.cursor()

        where = []
        params = []
        if args.since:
            where.append("datetime(run_timestamp) >= datetime(?)")
            params.append(parse_bound(args.since))
        if args.until:
            where.append("datetime(run_timestamp) <= datetime(?)")
            params.append(parse_bound(args.until))
        q = "SELECT id, run_timestamp, start_date, end_date, note FROM runs"
        if where:
            q += " WHERE " + " AND ".join(where)
        q += " ORDER BY datetime(run_timestamp) DESC"
        if args.limit:
            q += f" LIMIT {int(args.limit)}"

        rows = list(cur.execute(q, params))
        # Preload counts per run
        run_ids = [r["id"] for r in rows]
        counts: Dict[int, int] = {}
        if run_ids:
            placeholders = ",".join(["?"] * len(run_ids))
            for r in cur.execute(f"SELECT run_id, COUNT(*) AS c FROM prices WHERE run_id IN ({placeholders}) GROUP BY run_id", run_ids):
                counts[r["run_id"]] = r["c"]

        if args.json:
            data = [
                {
                    "id": r["id"],
                    "run_timestamp": r["run_timestamp"],
                    "start_date": r["start_date"],
                    "end_date": r["end_date"],
                    "note": r["note"],
                    "price_rows": counts.get(r["id"], 0),
                }
                for r in rows
            ]
            print(json.dumps(data, indent=2))
            return

        # Text table
        if not rows:
            print("No runs found.")
            return
        header = f"{'ID':>5}  {'Timestamp':<19}  {'Start':<10}  {'End':<10}  {'Prices':>6}  Note"
        print(header)
        print("-" * len(header))
        for r in rows:
            rid = r["id"]
            ts = (r["run_timestamp"] or "")[:19]
            start = (r["start_date"] or "")
            end = (r["end_date"] or "")
            cnt = counts.get(rid, 0)
            note = r["note"] or ""
            print(f"{rid:>5}  {ts:<19}  {start:<10}  {end:<10}  {cnt:>6}  {note}")


if __name__ == "__main__":
    main()

