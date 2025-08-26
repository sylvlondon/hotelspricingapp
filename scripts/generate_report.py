import argparse
import os
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from db import db_session, init_db, fetch_runs, read_config


def fmt_money(v: Optional[float]) -> str:
    if v is None:
        return "-"
    if abs(v - round(v)) < 1e-6:
        return f"{int(round(v))}"
    return f"{v:.2f}"


def fmt_delta(pct: Optional[float]) -> str:
    if pct is None:
        return ""
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct*100:.0f}%"


def severity_from_delta(pct_increase: float, levels: Dict[str, float]) -> Optional[str]:
    # pct_increase is positive ratio (e.g., 0.25 for +25%)
    if pct_increase is None or pct_increase <= 0:
        return None
    # Order from highest to lowest
    if pct_increase >= levels.get("high", 0.30):
        return "high"
    if pct_increase >= levels.get("medium", 0.20):
        return "medium"
    if pct_increase >= levels.get("low", 0.10):
        return "low"
    return None


def compute_trailing_avg(current_run_prices_by_hotel_date: Dict[int, Dict[str, float]], hotel_id: int, stay_date: str, lookback_days: int) -> Optional[float]:
    # stay_date is YYYY-MM-DD
    date = datetime.fromisoformat(stay_date)
    vals: List[float] = []
    for i in range(1, lookback_days + 1):
        d = (date - timedelta(days=i)).date().isoformat()
        v = current_run_prices_by_hotel_date.get(hotel_id, {}).get(d)
        if v is not None:
            vals.append(v)
    if len(vals) == 0:
        return None
    return sum(vals) / len(vals)


def generate_report(db_path: str, cfg_path: str):
    cfg = read_config(cfg_path)
    report_dir = Path(cfg.get("report_dir", "reports"))
    report_dir.mkdir(parents=True, exist_ok=True)
    start_date = cfg.get("runs", {}).get("start_date")
    end_date = cfg.get("runs", {}).get("end_date")
    lookback_runs = int(cfg.get("runs", {}).get("lookback_runs", 3))
    lookback_days_avg = int(cfg.get("runs", {}).get("lookback_days_avg", 5))
    avg_prev_offset = int(cfg.get("runs", {}).get("avg_prev_offset", 1))  # compare Δ Avg vs run n-k (k=offset)
    levels = cfg.get("spike", {}).get("levels", {"low": 0.10, "medium": 0.20, "high": 0.30})

    with db_session(db_path) as conn:
        init_db(conn)
        runs = fetch_runs(conn, limit=max(lookback_runs, avg_prev_offset) + 3)  # fetch a few more for safety
        if not runs:
            raise RuntimeError("No runs found in DB. Ingest data first.")
        current_run = runs[0]
        prev_runs = runs[1:lookback_runs+1]
        run_ids = [r["id"] for r in runs]

        # Hotels order: from config if available, else DB order by name
        cur = conn.cursor()
        hotels_rows = list(cur.execute("SELECT id, name FROM hotels ORDER BY name"))
        hotel_name_by_id = {r["id"]: r["name"] for r in hotels_rows}
        hotel_id_by_name = {r["name"]: r["id"] for r in hotels_rows}
        cfg_hotels = [h["name"] for h in cfg.get("hotels", [])]
        hotel_order_names = [h for h in cfg_hotels if h in hotel_id_by_name]
        # Append any DB hotels not in config
        for name in hotel_name_by_id.values():
            if name not in hotel_order_names:
                hotel_order_names.append(name)
        hotel_order_ids = [hotel_id_by_name[n] for n in hotel_order_names]

        # Prefetch prices for relevant runs
        q_marks = ",".join(["?"] * len(run_ids))
        prices_rows = list(
            cur.execute(
                f"SELECT run_id, hotel_id, stay_date, price FROM prices WHERE run_id IN ({q_marks})",
                run_ids,
            )
        )

        # Organize
        prices_by_run_hotel_date: Dict[int, Dict[int, Dict[str, Optional[float]]]] = defaultdict(lambda: defaultdict(dict))
        dates_set = set()
        for r in prices_rows:
            prices_by_run_hotel_date[r["run_id"]][r["hotel_id"]][r["stay_date"]] = r["price"]
            dates_set.add(r["stay_date"])

        # Date range filter
        all_dates = sorted(dates_set)
        if start_date:
            all_dates = [d for d in all_dates if d >= start_date]
        if end_date:
            all_dates = [d for d in all_dates if d <= end_date]

        # Build dict for current run prices by hotel/date
        current_run_id = current_run["id"]
        current_prices_by_hotel_date: Dict[int, Dict[str, Optional[float]]] = prices_by_run_hotel_date.get(current_run_id, {})

        # HTML assembly
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
        out_path = report_dir / f"report_{datetime.utcnow().strftime('%Y%m%d_%H%M')}.html"

        css = """
        <style>
          body { font-family: -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 20px; }
          table { border-collapse: collapse; width: 100%; }
          th, td { border: 1px solid #ddd; padding: 6px 8px; text-align: right; font-variant-numeric: tabular-nums; }
          th.sticky { position: sticky; top: 0; background: #fafafa; z-index: 1; }
          td.date, th.date { text-align: left; }
          /* Cell-level spike highlighting */
          td.sev-low { background-color: #ffef99; }
          td.sev-medium { background-color: #ffc78f; }
          td.sev-high { background-color: #ff9aa2; }
          .delta-pos { color: #b45309; font-size: 0.85em; }
          .delta-neg { color: #065f46; font-size: 0.85em; }
          .muted { color: #6b7280; font-size: 0.85em; }
          .legend { margin: 8px 0 16px; font-size: 0.9em; }
          .legend span { display: inline-block; margin-right: 12px; }
          .pill { border-radius: 999px; padding: 2px 8px; font-size: 0.75em; }
          .pill.low { background: #ffef99; }
          .pill.medium { background: #ffc78f; }
          .pill.high { background: #ff9aa2; }
        </style>
        """

        # Build helper for row-average trailing computation (Avg column highlighting)
        # We'll compute the average across hotels for each date of the current run,
        # then compare today's Avg vs the trailing average of the previous N dates.

        # Begin HTML
        parts: List[str] = []
        parts.append("<html><head><meta charset='utf-8'>" + css + "</head><body>")
        parts.append(f"<h2>Hotel Prices Report — {ts}</h2>")
        parts.append("<div class='legend'>" \
                     + f"<span><strong>Window:</strong> {start_date} → {end_date}</span>" \
                     + f"<span><strong>Compare runs (cells):</strong> last {lookback_runs}</span>" \
                     + f"<span><strong>Avg spike vs trailing:</strong> {lookback_days_avg} prior days</span>" \
                     + f"<span><strong>Δ Avg vs run:</strong> n-{avg_prev_offset}</span>" \
                     + f"<span><strong>Spikes:</strong> <span class='pill low'>low ≥ {int(levels.get('low',0.1)*100)}%</span> "
                       f"<span class='pill medium'>med ≥ {int(levels.get('medium',0.2)*100)}%</span> "
                       f"<span class='pill high'>high ≥ {int(levels.get('high',0.3)*100)}%</span></span>"
                     + "</div>")

        # Header
        parts.append("<table>")
        parts.append("<tr>")
        parts.append("<th class='sticky date'>Date</th>")
        for name in hotel_order_names:
            parts.append(f"<th class='sticky'>{name}</th>")
        parts.append("<th class='sticky'>Avg</th>")
        parts.append("<th class='sticky'>Δ Avg vs prev</th>")
        parts.append("</tr>")

        # Precompute row averages per date for current run (for Avg cell and its spike level)
        current_run_id = current_run["id"]
        current_prices_by_hotel_date: Dict[int, Dict[str, Optional[float]]] = prices_by_run_hotel_date.get(current_run_id, {})
        row_avg_by_date: Dict[str, Optional[float]] = {}
        for date_str in all_dates:
            vals = []
            for hid in hotel_order_ids:
                v = current_prices_by_hotel_date.get(hid, {}).get(date_str)
                if v is not None:
                    vals.append(v)
            row_avg_by_date[date_str] = (sum(vals) / len(vals)) if vals else None

        # Precompute trailing average of row averages for each date
        trailing_row_avg_by_date: Dict[str, Optional[float]] = {}
        if lookback_days_avg > 0:
            for i, date_str in enumerate(all_dates):
                # Take strictly previous dates within window
                prev_dates = all_dates[max(0, i - lookback_days_avg):i]
                prev_vals = [row_avg_by_date.get(d) for d in prev_dates]
                prev_vals = [v for v in prev_vals if v is not None]
                trailing_row_avg_by_date[date_str] = (sum(prev_vals) / len(prev_vals)) if prev_vals else None

        # Determine which previous run to use for Δ Avg vs prev (offset k)
        prev_runs = runs[1:lookback_runs+1]
        prev_run_for_avg: Optional[int] = None
        if avg_prev_offset >= 1 and len(runs) > avg_prev_offset:
            prev_run_for_avg = runs[avg_prev_offset]["id"]
        elif prev_runs:
            prev_run_for_avg = prev_runs[0]["id"]

        # Rows
        for date_str in all_dates:
            row_prices: List[float] = []
            parts.append("<tr>")
            parts.append(f"<td class='date'>{date_str}</td>")
            # Cells
            for hid in hotel_order_ids:
                cur_price = prices_by_run_hotel_date.get(current_run_id, {}).get(hid, {}).get(date_str)
                # delta vs immediate previous run
                delta_str = ""
                if prev_runs:
                    prev_price = prices_by_run_hotel_date.get(prev_runs[0]["id"], {}).get(hid, {}).get(date_str)
                    if prev_price is not None and cur_price is not None and prev_price > 0:
                        delta = (cur_price - prev_price) / prev_price
                        delta_str = f" <span class='{'delta-pos' if delta>=0 else 'delta-neg'}'>({fmt_delta(delta)})</span>"
                parts.append(f"<td>{fmt_money(cur_price)}{delta_str}</td>")
                if cur_price is not None:
                    row_prices.append(cur_price)

            # Row avg and Δ Avg vs selected previous run avg
            cur_avg = row_avg_by_date.get(date_str)

            # Avg cell severity vs trailing row average
            avg_cell_class = ""
            trailing_avg = trailing_row_avg_by_date.get(date_str)
            if cur_avg is not None and trailing_avg is not None and trailing_avg > 0:
                delta_vs_trailing = (cur_avg - trailing_avg) / trailing_avg
                sev = severity_from_delta(delta_vs_trailing, levels)
                if sev:
                    avg_cell_class = f" class='sev-{sev}'"
            parts.append(f"<td{avg_cell_class}>{fmt_money(cur_avg)}</td>")

            # Δ Avg vs prev (run offset)
            prev_avg: Optional[float] = None
            if prev_run_for_avg is not None:
                vals = []
                for hid in hotel_order_ids:
                    pv = prices_by_run_hotel_date.get(prev_run_for_avg, {}).get(hid, {}).get(date_str)
                    if pv is not None:
                        vals.append(pv)
                if vals:
                    prev_avg = sum(vals) / len(vals)

            delta_avg_str = ""
            delta_cell_class = " class='muted'"
            if prev_avg is not None and cur_avg is not None and prev_avg > 0:
                delta_avg = (cur_avg - prev_avg) / prev_avg
                delta_avg_str = fmt_delta(delta_avg)
                sev = severity_from_delta(delta_avg, levels)
                if sev:
                    delta_cell_class = f" class='sev-{sev}'"
            parts.append(f"<td{delta_cell_class}>{delta_avg_str}</td>")
            parts.append("</tr>")

        parts.append("</table>")
        parts.append("<p class='muted'>Notes: Only the Avg cell (vs trailing {lookback_days_avg} prior days) and the Δ Avg vs run n-{avg_prev_offset} cell are highlighted when a spike is detected based on configured thresholds. Missing values are ignored in averages.</p>")
        parts.append("</body></html>")

        html = "\n".join(parts)
        out_path.write_text(html, encoding="utf-8")
        print(f"Report written to {out_path}")


def main():
    ap = argparse.ArgumentParser(description="Generate a dated HTML report from SQLite data")
    ap.add_argument("--db", default=None, help="Path to DB (defaults to config)")
    ap.add_argument("--config", default="config.json", help="Path to config JSON")
    args = ap.parse_args()

    cfg = read_config(args.config)
    db_path = args.db or cfg.get("db_path", "pricing.db")
    generate_report(db_path, args.config)


if __name__ == "__main__":
    main()
