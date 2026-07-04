import sqlite3
from pathlib import Path
from typing import Iterable, Dict, Any, List
from .config import DB_PATH

SCHEMA = """
CREATE TABLE IF NOT EXISTS max_pain_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    collected_at TEXT NOT NULL,
    source TEXT NOT NULL,
    collector_version TEXT NOT NULL,
    scrape_duration_seconds REAL,
    is_valid INTEGER NOT NULL DEFAULT 1,
    validation_errors TEXT,
    symbol TEXT NOT NULL,
    rank INTEGER,
    timeframe TEXT NOT NULL,
    current_price REAL,
    short_max_pain REAL,
    long_max_pain REAL,
    distance_short_abs REAL,
    distance_short_pct REAL,
    distance_long_abs REAL,
    distance_long_pct REAL,
    delta_short_abs REAL,
    delta_short_pct REAL,
    delta_long_abs REAL,
    delta_long_pct REAL,
    alert_level TEXT,
    UNIQUE(collected_at, symbol, timeframe)
);

CREATE INDEX IF NOT EXISTS idx_symbol_time ON max_pain_snapshots(symbol, collected_at);
CREATE INDEX IF NOT EXISTS idx_timeframe_time ON max_pain_snapshots(timeframe, collected_at);
CREATE INDEX IF NOT EXISTS idx_alert_level ON max_pain_snapshots(alert_level);
"""

def connect(db_path: str = DB_PATH) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn

def insert_snapshots(rows: Iterable[Dict[str, Any]], db_path: str = DB_PATH) -> int:
    rows = list(rows)
    if not rows:
        return 0

    columns = [
        "collected_at", "source", "collector_version", "scrape_duration_seconds",
        "is_valid", "validation_errors", "symbol", "rank", "timeframe",
        "current_price", "short_max_pain", "long_max_pain",
        "distance_short_abs", "distance_short_pct", "distance_long_abs", "distance_long_pct",
        "delta_short_abs", "delta_short_pct", "delta_long_abs", "delta_long_pct",
        "alert_level"
    ]
    placeholders = ",".join(["?"] * len(columns))
    sql = f"INSERT OR REPLACE INTO max_pain_snapshots ({','.join(columns)}) VALUES ({placeholders})"
    values = [[row.get(col) for col in columns] for row in rows]

    with connect(db_path) as conn:
        conn.executemany(sql, values)
        conn.commit()
    return len(rows)

def query(sql: str, params: tuple = (), db_path: str = DB_PATH) -> List[sqlite3.Row]:
    with connect(db_path) as conn:
        return conn.execute(sql, params).fetchall()
