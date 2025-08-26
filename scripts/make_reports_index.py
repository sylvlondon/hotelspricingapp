import re
from pathlib import Path

REPORTS_DIR = Path("reports")


def main():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    entries = []
    pat = re.compile(r"report_(\d{8}_\d{4})\.html$")
    for p in sorted(REPORTS_DIR.glob("report_*.html")):
        m = pat.search(p.name)
        if not m:
            continue
        stamp = m.group(1)
        entries.append((stamp, p.name))

    # sort by timestamp desc
    entries.sort(key=lambda x: x[0], reverse=True)

    latest_link = entries[0][1] if entries else None

    parts = []
    parts.append("<html><head><meta charset='utf-8'><title>Reports</title>\n")
    parts.append("<style>body{font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:20px} ul{line-height:1.8}</style>")
    parts.append("</head><body>")
    parts.append("<h2>Hotel Prices Reports</h2>")
    if latest_link:
        parts.append(f"<p><strong>Latest:</strong> <a href='{latest_link}'>{latest_link}</a></p>")
    parts.append("<h3>All Reports</h3>")
    parts.append("<ul>")
    for stamp, name in entries:
        parts.append(f"<li><a href='{name}'>{name}</a></li>")
    parts.append("</ul>")
    parts.append("</body></html>")

    (REPORTS_DIR / "index.html").write_text("\n".join(parts), encoding="utf-8")


if __name__ == "__main__":
    main()

