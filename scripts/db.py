import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime


DEFAULT_DB_PATH = os.environ.get("PRICING_DB", "pricing.db")


def _connect(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def db_session(db_path: str = DEFAULT_DB_PATH):
    conn = _connect(db_path)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_timestamp TEXT NOT NULL,
            start_date TEXT,
            end_date TEXT,
            note TEXT
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS hotels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            api_key TEXT
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            hotel_id INTEGER NOT NULL,
            stay_date TEXT NOT NULL,
            currency TEXT,
            price REAL,
            source TEXT,
            UNIQUE(run_id, hotel_id, stay_date),
            FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE,
            FOREIGN KEY(hotel_id) REFERENCES hotels(id) ON DELETE CASCADE
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_prices_date ON prices(stay_date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_prices_hotel_date ON prices(hotel_id, stay_date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_prices_run ON prices(run_id);")
    conn.commit()


def ensure_hotels(conn: sqlite3.Connection, hotel_names_to_keys: dict[str, str] | None = None) -> dict[str, int]:
    """Ensure hotel rows exist. Returns mapping name -> hotel_id.
    hotel_names_to_keys: optional mapping of hotel name to API key.
    """
    cur = conn.cursor()
    existing = {row["name"]: row["id"] for row in cur.execute("SELECT id, name FROM hotels")}
    mapping: dict[str, int] = {}
    if hotel_names_to_keys is None:
        hotel_names_to_keys = {}
    for name, api_key in hotel_names_to_keys.items():
        if name in existing:
            mapping[name] = existing[name]
        else:
            cur.execute("INSERT INTO hotels(name, api_key) VALUES (?, ?)", (name, api_key))
            mapping[name] = cur.lastrowid
    # Also add names without keys if present
    for name in hotel_names_to_keys.keys():
        if name not in mapping:
            cur.execute("INSERT OR IGNORE INTO hotels(name) VALUES (?)", (name,))
            # refresh id
            cur.execute("SELECT id FROM hotels WHERE name = ?", (name,))
            mapping[name] = cur.fetchone()[0]
    conn.commit()
    # Return full mapping including preexisting
    cur.execute("SELECT id, name FROM hotels")
    return {row["name"]: row["id"] for row in cur.fetchall()}


def get_or_create_hotels_from_list(conn: sqlite3.Connection, hotels: list[dict]) -> dict[str, int]:
    by_name = {h["name"]: h.get("key") for h in hotels}
    return ensure_hotels(conn, by_name)


def create_run(conn: sqlite3.Connection, start_date: str | None, end_date: str | None, note: str | None = None, timestamp: datetime | None = None) -> int:
    ts = (timestamp or datetime.utcnow()).isoformat(timespec="seconds")
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO runs(run_timestamp, start_date, end_date, note) VALUES (?, ?, ?, ?)",
        (ts, start_date, end_date, note),
    )
    conn.commit()
    return cur.lastrowid


def upsert_price(conn: sqlite3.Connection, run_id: int, hotel_id: int, stay_date: str, currency: str | None, price: float | None, source: str | None = None):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO prices(run_id, hotel_id, stay_date, currency, price, source)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (run_id, hotel_id, stay_date, currency, price, source),
    )
    # commit deferred by caller


def latest_run_id(conn: sqlite3.Connection) -> int | None:
    cur = conn.cursor()
    cur.execute("SELECT id FROM runs ORDER BY datetime(run_timestamp) DESC LIMIT 1")
    row = cur.fetchone()
    return row["id"] if row else None


def fetch_runs(conn: sqlite3.Connection, limit: int | None = None) -> list[sqlite3.Row]:
    cur = conn.cursor()
    q = "SELECT * FROM runs ORDER BY datetime(run_timestamp) DESC"
    if limit:
        q += f" LIMIT {int(limit)}"
    return list(cur.execute(q))


def read_config(path: str = "config.json") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def delete_run(conn: sqlite3.Connection, run_id: int) -> tuple[int, int]:
    """Delete a run and its related prices.
    Returns (prices_deleted, runs_deleted).
    """
    cur = conn.cursor()
    # Explicitly delete prices to be safe even if PRAGMA foreign_keys is off
    cur.execute("DELETE FROM prices WHERE run_id = ?", (run_id,))
    prices_deleted = cur.rowcount or 0
    cur.execute("DELETE FROM runs WHERE id = ?", (run_id,))
    runs_deleted = cur.rowcount or 0
    conn.commit()
    return prices_deleted, runs_deleted
