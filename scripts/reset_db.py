import argparse
from db import db_session, init_db, read_config


def main():
    ap = argparse.ArgumentParser(description="Reset the SQLite DB: delete all data (runs, prices, hotels)")
    ap.add_argument("--config", default="config.json", help="Path to config JSON")
    ap.add_argument("--db", default=None, help="Path to DB (defaults to config)")
    ap.add_argument("--yes", action="store_true", help="Skip confirmation prompt")
    ap.add_argument("--vacuum", action="store_true", help="Run VACUUM after deletion")
    args = ap.parse_args()

    cfg = read_config(args.config)
    db_path = args.db or cfg.get("db_path", "pricing.db")

    if not args.yes:
        print(f"About to DELETE ALL DATA from {db_path} (tables: prices, runs, hotels). This cannot be undone.")
        resp = input("Type 'yes' to confirm: ")
        if resp.strip().lower() != "yes":
            print("Aborted.")
            return

    with db_session(db_path) as conn:
        init_db(conn)
        cur = conn.cursor()
        # Count rows before
        counts = {}
        for t in ("prices", "runs", "hotels"):
            try:
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                counts[t] = cur.fetchone()[0]
            except Exception:
                counts[t] = 0

        # Delete in FK-safe order
        cur.execute("DELETE FROM prices")
        cur.execute("DELETE FROM runs")
        cur.execute("DELETE FROM hotels")
        if args.vacuum:
            cur.execute("VACUUM")
        print(
            "DB reset complete. Deleted: "
            f"prices={counts.get('prices',0)}, runs={counts.get('runs',0)}, hotels={counts.get('hotels',0)}"
        )


if __name__ == "__main__":
    main()

