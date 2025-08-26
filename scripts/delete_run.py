import argparse
from datetime import datetime, timedelta
from typing import List

from db import db_session, init_db, delete_run, read_config, latest_run_id


def parse_iso(value: str) -> str:
    """Accept YYYY-MM-DD or full ISO timestamp; return ISO datetime string.
    If only date is provided, use day bounds (caller decides which bound).
    """
    try:
        # Try datetime
        dt = datetime.fromisoformat(value)
        return dt.isoformat(timespec="seconds")
    except ValueError:
        # Try date only
        dt = datetime.fromisoformat(value + "T00:00:00")
        return dt.isoformat(timespec="seconds")


def main():
    ap = argparse.ArgumentParser(description="Delete runs (and their prices) from SQLite")
    ap.add_argument("--config", default="config.json", help="Path to config.json")
    ap.add_argument("--db", default=None, help="Path to DB (defaults to config)")
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--run-id", type=int, help="Run ID to delete")
    group.add_argument("--latest", action="store_true", help="Delete the most recent run")
    group.add_argument("--between", nargs=2, metavar=("START", "END"), help="Delete runs with run_timestamp between START and END (YYYY-MM-DD or ISO)")
    args = ap.parse_args()

    cfg = read_config(args.config)
    db_path = args.db or cfg.get("db_path", "pricing.db")

    with db_session(db_path) as conn:
        init_db(conn)

        run_ids: List[int] = []
        if args.run_id is not None:
            run_ids = [args.run_id]
        elif args.latest:
            rid = latest_run_id(conn)
            if rid is None:
                print("No runs found in DB.")
                return
            run_ids = [rid]
        elif args.between:
            start_raw, end_raw = args.between
            # Normalize bounds
            start_iso = parse_iso(start_raw)
            # End bound: if only a date was provided, use end-of-day; else use as-is
            try:
                end_dt = datetime.fromisoformat(end_raw)
                if len(end_raw) <= 10:  # likely YYYY-MM-DD
                    end_dt = end_dt.replace(hour=23, minute=59, second=59)
            except ValueError:
                end_dt = datetime.fromisoformat(end_raw + "T23:59:59")
            end_iso = end_dt.isoformat(timespec="seconds")

            cur = conn.cursor()
            cur.execute(
                "SELECT id FROM runs WHERE datetime(run_timestamp) BETWEEN datetime(?) AND datetime(?) ORDER BY datetime(run_timestamp)",
                (start_iso, end_iso),
            )
            run_ids = [r["id"] for r in cur.fetchall()]

        if not run_ids:
            if args.between:
                print("No runs found in given range; nothing deleted.")
            else:
                print("Run not found; nothing deleted.")
            return

        total_prices = 0
        for rid in run_ids:
            prices_deleted, runs_deleted = delete_run(conn, rid)
            if runs_deleted:
                print(f"Deleted run id={rid} and {prices_deleted} price rows.")
                total_prices += prices_deleted
            else:
                print(f"Run id={rid} not found; skipped.")
        if len(run_ids) > 1:
            print(f"Deleted {len([r for r in run_ids if r is not None])} runs, {total_prices} price rows in total.")


if __name__ == "__main__":
    main()
