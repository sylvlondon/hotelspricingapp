import argparse
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from db import db_session, init_db, get_or_create_hotels_from_list, create_run, upsert_price, read_config


def mutate_price(base: Optional[float], rng: random.Random) -> Optional[float]:
    if base is None:
        return None
    # Base noise +/-5%
    noise = rng.uniform(-0.05, 0.05)
    price = base * (1 + noise)
    # Inject spikes with some probability
    p = rng.random()
    if p < 0.05:
        # high spike 30-60%
        spike = rng.uniform(0.30, 0.60)
        price = base * (1 + spike)
    elif p < 0.10:
        # medium spike 20-30%
        spike = rng.uniform(0.20, 0.30)
        price = base * (1 + spike)
    elif p < 0.18:
        # low spike 10-20%
        spike = rng.uniform(0.10, 0.20)
        price = base * (1 + spike)
    # Minimum floor to avoid too small values
    price = max(price, 40.0)
    return round(price, 2)


def read_csv_prices(csv_path: Path):
    import csv

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        hotel_names = [h for h in reader.fieldnames if h != "Date"]
        rows = []
        for rec in reader:
            date = rec["Date"].strip()
            rows.append((date, {h: rec.get(h) for h in hotel_names}))
    return hotel_names, rows


def to_float(v: str) -> Optional[float]:
    if v is None:
        return None
    s = str(v).strip()
    if s == "" or s.lower() in {"n/a", "na", "null", "none"}:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def generate_runs_from_csv(csv_path: Path, db_path: str, base_timestamp: Optional[datetime] = None, seed: int = 42):
    cfg = read_config()
    if base_timestamp is None:
        base_timestamp = datetime.utcnow() - timedelta(days=6)

    with db_session(db_path) as conn:
        init_db(conn)
        hotel_ids = get_or_create_hotels_from_list(conn, cfg.get("hotels", []))
        hotel_order, rows = read_csv_prices(csv_path)

        # Run 1: base from CSV
        run1 = create_run(conn, cfg["runs"]["start_date"], cfg["runs"]["end_date"], note="fake run 1 from CSV", timestamp=base_timestamp)
        for date, rec in rows:
            for h in hotel_order:
                price = to_float(rec.get(h))
                hid = hotel_ids.get(h)
                if hid is None:
                    continue
                upsert_price(conn, run1, hid, date, cfg.get("fetch", {}).get("currency", "EUR"), price, source="csv-base")

        # Run 2: slight mutations + some spikes
        rng = random.Random(seed)
        run2 = create_run(conn, cfg["runs"]["start_date"], cfg["runs"]["end_date"], note="fake run 2 mutated", timestamp=base_timestamp + timedelta(days=3))
        for date, rec in rows:
            for h in hotel_order:
                base = to_float(rec.get(h))
                price = mutate_price(base, rng)
                hid = hotel_ids.get(h)
                upsert_price(conn, run2, hid, date, cfg.get("fetch", {}).get("currency", "EUR"), price, source="csv-mut2")

        # Run 3: further mutations + spikes
        rng3 = random.Random(seed + 1)
        run3 = create_run(conn, cfg["runs"]["start_date"], cfg["runs"]["end_date"], note="fake run 3 mutated", timestamp=base_timestamp + timedelta(days=6))
        for date, rec in rows:
            for h in hotel_order:
                base = to_float(rec.get(h))
                # Use run2 price as base if available
                price = mutate_price(base, rng3)
                hid = hotel_ids.get(h)
                upsert_price(conn, run3, hid, date, cfg.get("fetch", {}).get("currency", "EUR"), price, source="csv-mut3")

    print(f"Generated fake runs: run1={run1}, run2={run2}, run3={run3}")


def main():
    ap = argparse.ArgumentParser(description="Generate 3 fake runs from an existing CSV")
    ap.add_argument("--csv", required=True, type=Path, help="Path to base CSV (outputprice.csv)")
    ap.add_argument("--db", default=None, help="Path to SQLite DB (defaults to config)")
    ap.add_argument("--seed", type=int, default=42, help="RNG seed for reproducibility")
    args = ap.parse_args()

    cfg = read_config()
    db_path = args.db or cfg.get("db_path", "pricing.db")
    generate_runs_from_csv(args.csv, db_path, seed=args.seed)


if __name__ == "__main__":
    main()

