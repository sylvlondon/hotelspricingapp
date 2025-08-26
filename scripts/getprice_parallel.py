import argparse
import concurrent.futures
import json
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import urlopen

from db import db_session, init_db, get_or_create_hotels_from_list, create_run, upsert_price, read_config


API_BASE = "https://data.xotelo.com/api/rates"


def daterange(start: str, end: str) -> List[str]:
    ds = datetime.fromisoformat(start).date()
    de = datetime.fromisoformat(end).date()
    days = (de - ds).days
    if days < 0:
        return []
    return [(ds + timedelta(days=i)).isoformat() for i in range(days + 1)]


def fetch_rate(hotel_key: str, chk_in: str, chk_out: str, currency: str, adults: int, rooms: int, timeout: int = 10) -> Optional[float]:
    qs = urlencode({
        "hotel_key": hotel_key,
        "chk_in": chk_in,
        "chk_out": chk_out,
        "currency": currency,
        "adults": adults,
        "rooms": rooms,
    })
    url = f"{API_BASE}?{qs}"
    try:
        with urlopen(url, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        if data.get("error") is None:
            rates = data.get("result", {}).get("rates", [])
            if rates:
                return float(rates[0].get("rate"))
        return None
    except Exception:
        return None


def run_fetch_to_db(cfg_path: str, db_path: Optional[str] = None) -> int:
    cfg = read_config(cfg_path)
    db_path = db_path or cfg.get("db_path", "pricing.db")
    currency = cfg.get("fetch", {}).get("currency", "EUR")
    adults = int(cfg.get("fetch", {}).get("adults", 2))
    rooms = int(cfg.get("fetch", {}).get("rooms", 1))
    sleep_seconds = int(cfg.get("fetch", {}).get("sleep_seconds", 0))
    parallelism = int(cfg.get("fetch", {}).get("parallelism", 8))
    start_date = cfg.get("runs", {}).get("start_date")
    end_date = cfg.get("runs", {}).get("end_date")
    hotels = cfg.get("hotels", [])

    if not start_date or not end_date:
        raise ValueError("runs.start_date and runs.end_date must be configured")

    dates = daterange(start_date, end_date)
    total_dates = len(dates)
    with db_session(db_path) as conn:
        init_db(conn)
        hotel_ids = get_or_create_hotels_from_list(conn, hotels)
        run_id = create_run(conn, start_date, end_date, note="api fetch")

        # Build tasks: one per stay date x hotel
        tasks: List[Tuple[str, str, str, int]] = []  # (hotel_name, key, date, hotel_id)
        for h in hotels:
            for d in dates:
                chk_in = d
                chk_out = (datetime.fromisoformat(d).date() + timedelta(days=1)).isoformat()
                tasks.append((h["name"], h.get("key"), d, hotel_ids[h["name"]]))
        total_tasks = len(tasks)

        # Progress reporting setup (by dates, not tasks)
        # We count a date as "done" when all hotels for that date have been processed
        per_date_counts: Dict[str, int] = {d: 0 for d in dates}
        hotels_count = max(1, len(hotels))
        dates_done = 0

        print(
            f"Starting parallel fetch: dates={total_dates}, hotels={len(hotels)}, tasks={total_tasks}, pool={parallelism}",
            flush=True,
        )

        def worker(tup: Tuple[str, str, str, int]) -> Tuple[str, str, int, Optional[float]]:
            name, key, date, hid = tup
            price = fetch_rate(key, date, (datetime.fromisoformat(date).date() + timedelta(days=1)).isoformat(), currency, adults, rooms)
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
            return (name, date, hid, price)

        with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as ex:
            processed_tasks = 0
            futures = {ex.submit(worker, t): t for t in tasks}
            for fut in concurrent.futures.as_completed(futures):
                name, date, hid, price = fut.result()
                upsert_price(conn, run_id, hid, date, currency, price, source="api")
                processed_tasks += 1

                # Update date completion progress
                per_date_counts[date] += 1
                if per_date_counts[date] == hotels_count:
                    dates_done += 1
                    pct = int((dates_done / total_dates) * 100) if total_dates > 0 else 100
                    ts = datetime.now().strftime('%H:%M:%S')
                    print(f"[{ts}] Completed {date}: {dates_done}/{total_dates} dates ({pct}%)", flush=True)

                # Optional light heartbeat every ~10% tasks if there are many hotels
                # (kept minimal to avoid noise when there are few dates)
                # if processed_tasks % max(1, total_tasks // 10) == 0:
                #     print(f"Tasks: {processed_tasks}/{total_tasks}", flush=True)

        return run_id


def main():
    ap = argparse.ArgumentParser(description="Fetch hotel rates in parallel and store into SQLite")
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--db", default=None)
    args = ap.parse_args()
    run_id = run_fetch_to_db(args.config, args.db)
    print(f"Done. Fetched and stored run_id={run_id}")


if __name__ == "__main__":
    main()
