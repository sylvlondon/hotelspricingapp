import argparse
import json
from datetime import datetime, timedelta


def main():
    ap = argparse.ArgumentParser(description="Update config.json runs.start_date and runs.end_date with dynamic offsets")
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--start-offset", type=int, default=1, help="Days from today for start_date (default 1)")
    ap.add_argument("--end-offset", type=int, default=15, help="Days from today for end_date (default 15)")
    args = ap.parse_args()

    with open(args.config, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    today = datetime.utcnow().date()
    start_date = (today + timedelta(days=args.start_offset)).isoformat()
    end_date = (today + timedelta(days=args.end_offset)).isoformat()

    runs = cfg.setdefault("runs", {})
    runs["start_date"] = start_date
    runs["end_date"] = end_date

    with open(args.config, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)
        f.write("\n")

    print(f"Updated {args.config}: start_date={start_date}, end_date={end_date}")


if __name__ == "__main__":
    main()

