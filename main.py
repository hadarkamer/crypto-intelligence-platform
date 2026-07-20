import asyncio
import base64
import html
import json
import os
import re
import sqlite3
import time
import zlib
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Optional, List, Dict, Any

from aiohttp import web
from dotenv import load_dotenv
import requests
from tabulate import tabulate
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from coinglass_dom_reader import collect_coinglass_dom_snapshot
import analysis
import decision_engine
import alert_engine
import live_price_provider
import counter_score
import alert_summary
import technical_signal_store
from collections import defaultdict

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:
    psycopg = None
    dict_row = None

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
DB_PATH = os.getenv("DB_PATH", "data/coinglass.db")
PORT = int(os.getenv("PORT", "10000"))
PUBLIC_URL = os.getenv("PUBLIC_URL", "").rstrip("/")
COINGLASS_MAX_PAIN_URL = os.getenv("COINGLASS_MAX_PAIN_URL", "https://www.coinglass.com/liquidation-maxpain")
COINGLASS_API_URL = os.getenv("COINGLASS_API_URL", "https://fapi.coinglass.com/api/liqHeatMap/list")
TOP_COINS_LIMIT = int(os.getenv("TOP_COINS_LIMIT", "50"))
COLLECT_INTERVAL_MINUTES = int(os.getenv("COLLECT_INTERVAL_MINUTES", "60"))
MAX_SECONDS_PER_TIMEFRAME = int(os.getenv("MAX_SECONDS_PER_TIMEFRAME", "120"))
RETRY_SLEEP_SECONDS = float(os.getenv("RETRY_SLEEP_SECONDS", "4"))
TRADINGVIEW_WEBHOOK_SECRET = os.getenv("TRADINGVIEW_WEBHOOK_SECRET", "")

TIMEFRAMES = ["12h", "24h", "48h", "3d", "1w", "2w", "1m"]
TIMEFRAME_ORDER_SQL = "CASE timeframe WHEN '12h' THEN 1 WHEN '24h' THEN 2 WHEN '48h' THEN 3 WHEN '3d' THEN 4 WHEN '1w' THEN 5 WHEN '2w' THEN 6 WHEN '1m' THEN 7 ELSE 99 END"
# CoinGlass may mix non-crypto assets into the Max Pain table. Exclude known non-crypto symbols.
NON_CRYPTO_SYMBOLS = {"CL", "SPCX", "XAG", "PAXG", "XAU", "MU", "XAUT", "NVDA", "SOXL", "MRVL", "SKHYNIX", "SKHY", "SNDK", "MSFT", "AAPL", "TSLA", "GOOGL", "AMZN", "META", "COIN", "MSTR"}
API_TIMEFRAME_MAP = {
    "12h": "12h", "24h": "24h", "48h": "48h", "3d": "3d",
    "1w": "7d", "2w": "14d", "1m": "30d",
}
TIMEFRAME_LABELS = {
    "12h": "12 hour",
    "24h": "24 hour",
    "48h": "48 hour",
    "3d": "3 day",
    "1w": "1 week",
    "2w": "2 week",
    "1m": "1 month",
}
NETWORK_CAPTURE_LIMIT = 80
SOURCE_NAME = "coinglass_liquidation_max_pain"
COLLECTOR_VERSION = "v3-dom-reader"
COLLECT_LOCK = None
SCRAPE_LOCK = None
WATCH_TASK = None
WATCH_SCAN_TASK = None
ALERT_COMMAND_LOCK = None
PROCESSED_UPDATE_IDS = set()
PROCESSED_UPDATE_ORDER = []
MAX_PROCESSED_UPDATE_IDS = 500
ALERT_ACTIVE = False
WATCH_INTERVAL_MINUTES = int(os.getenv("WATCH_INTERVAL_MINUTES", "15"))
WATCH_PRIORITY_THRESHOLD = float(os.getenv("WATCH_PRIORITY_THRESHOLD", "70"))
MIN_DISPLAY_DISTANCE_PCT = float(
    os.getenv("MIN_DISPLAY_DISTANCE_PCT", "0.5")
)
WATCH_COOLDOWN_MINUTES = int(os.getenv("WATCH_COOLDOWN_MINUTES", "60"))
WATCH_RUNTIME = {
    "last_scan_utc": None,
    "next_scan_utc": None,
    "last_found": 0,
    "last_candidates": 0,
    "last_sent": 0,
    "last_error": None,
    "last_cycle_status": None,
    "top_score": None,
    "top_symbol": None,
    "top_timeframe": None,
    "scan_in_progress": False,
    "scan_owner": None,
    "cycle_number": 0,
}

SQLITE_SCHEMA = """
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
    short_liquidation_amount REAL,
    long_liquidation_amount REAL,
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

CREATE TABLE IF NOT EXISTS bot_settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS alert_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    side TEXT NOT NULL,
    alert_types TEXT NOT NULL,
    priority REAL NOT NULL,
    UNIQUE(fingerprint)
);
CREATE INDEX IF NOT EXISTS idx_alert_created_at ON alert_history(created_at);

""" + technical_signal_store.sqlite_schema()

POSTGRES_SCHEMA = """
CREATE TABLE IF NOT EXISTS max_pain_snapshots (
    id BIGSERIAL PRIMARY KEY,
    collected_at TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    collector_version TEXT NOT NULL,
    scrape_duration_seconds DOUBLE PRECISION,
    is_valid BOOLEAN NOT NULL DEFAULT TRUE,
    validation_errors TEXT,
    symbol TEXT NOT NULL,
    rank INTEGER,
    timeframe TEXT NOT NULL,
    current_price DOUBLE PRECISION,
    short_max_pain DOUBLE PRECISION,
    long_max_pain DOUBLE PRECISION,
    short_liquidation_amount DOUBLE PRECISION,
    long_liquidation_amount DOUBLE PRECISION,
    distance_short_abs DOUBLE PRECISION,
    distance_short_pct DOUBLE PRECISION,
    distance_long_abs DOUBLE PRECISION,
    distance_long_pct DOUBLE PRECISION,
    delta_short_abs DOUBLE PRECISION,
    delta_short_pct DOUBLE PRECISION,
    delta_long_abs DOUBLE PRECISION,
    delta_long_pct DOUBLE PRECISION,
    alert_level TEXT,
    UNIQUE(collected_at, symbol, timeframe)
);
CREATE INDEX IF NOT EXISTS idx_symbol_time ON max_pain_snapshots(symbol, collected_at);
CREATE INDEX IF NOT EXISTS idx_timeframe_time ON max_pain_snapshots(timeframe, collected_at);
CREATE INDEX IF NOT EXISTS idx_alert_level ON max_pain_snapshots(alert_level);

CREATE TABLE IF NOT EXISTS bot_settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS alert_history (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL,
    fingerprint TEXT NOT NULL UNIQUE,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    side TEXT NOT NULL,
    alert_types TEXT NOT NULL,
    priority DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_alert_created_at ON alert_history(created_at);

""" + technical_signal_store.postgres_schema()

def use_postgres() -> bool:
    return bool(DATABASE_URL and psycopg)

def ensure_amount_columns():
    """Add amount columns to existing tables created before this version."""
    if use_postgres():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            conn.execute("ALTER TABLE max_pain_snapshots ADD COLUMN IF NOT EXISTS short_liquidation_amount DOUBLE PRECISION")
            conn.execute("ALTER TABLE max_pain_snapshots ADD COLUMN IF NOT EXISTS long_liquidation_amount DOUBLE PRECISION")
            conn.commit()
    else:
        Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(DB_PATH) as conn:
            conn.executescript(SQLITE_SCHEMA)
            existing = {row[1] for row in conn.execute("PRAGMA table_info(max_pain_snapshots)").fetchall()}
            if "short_liquidation_amount" not in existing:
                conn.execute("ALTER TABLE max_pain_snapshots ADD COLUMN short_liquidation_amount REAL")
            if "long_liquidation_amount" not in existing:
                conn.execute("ALTER TABLE max_pain_snapshots ADD COLUMN long_liquidation_amount REAL")
            conn.commit()

def init_db():
    if use_postgres():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            conn.execute(POSTGRES_SCHEMA)
            conn.commit()
    else:
        Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(DB_PATH) as conn:
            conn.executescript(SQLITE_SCHEMA)
            conn.commit()
    ensure_amount_columns()

def query(sql: str, params: tuple = ()):
    init_db()
    if use_postgres():
        sql = sql.replace("?", "%s")
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()
    else:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            return conn.execute(sql, params).fetchall()


def execute_write(sql: str, params: tuple = ()) -> None:
    init_db()
    if use_postgres():
        sql = sql.replace("?", "%s")
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            conn.execute(sql, params)
            conn.commit()
    else:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(sql, params)
            conn.commit()


def set_setting(key: str, value: str) -> None:
    if use_postgres():
        execute_write(
            "INSERT INTO bot_settings(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",
            (key, value),
        )
    else:
        execute_write(
            "INSERT OR REPLACE INTO bot_settings(key, value) VALUES (?, ?)",
            (key, value),
        )


def get_setting(key: str, default: Optional[str] = None) -> Optional[str]:
    rows = query("SELECT value FROM bot_settings WHERE key = ?", (key,))
    return rows[0]["value"] if rows else default


def watch_enabled() -> bool:
    return get_setting("watch_enabled", "0") == "1"


def _parse_utc_setting(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _persist_watch_runtime() -> None:
    """Persist enough state for /watch_status and restart recovery."""
    mapping = {
        "watch_last_scan_utc": WATCH_RUNTIME.get("last_scan_utc"),
        "watch_next_scan_utc": WATCH_RUNTIME.get("next_scan_utc"),
        "watch_last_cycle_status": WATCH_RUNTIME.get("last_cycle_status"),
        "watch_last_found": WATCH_RUNTIME.get("last_found", 0),
        "watch_last_candidates": WATCH_RUNTIME.get("last_candidates", 0),
        "watch_last_sent": WATCH_RUNTIME.get("last_sent", 0),
        "watch_top_score": WATCH_RUNTIME.get("top_score"),
        "watch_top_symbol": WATCH_RUNTIME.get("top_symbol"),
        "watch_top_timeframe": WATCH_RUNTIME.get("top_timeframe"),
        "watch_last_error": WATCH_RUNTIME.get("last_error"),
    }
    for key, value in mapping.items():
        set_setting(key, "" if value is None else str(value))


def _restore_watch_runtime() -> None:
    WATCH_RUNTIME["last_scan_utc"] = get_setting("watch_last_scan_utc") or None
    WATCH_RUNTIME["next_scan_utc"] = get_setting("watch_next_scan_utc") or None
    WATCH_RUNTIME["last_cycle_status"] = (
        get_setting("watch_last_cycle_status") or None
    )
    WATCH_RUNTIME["last_found"] = int(get_setting("watch_last_found", "0") or 0)
    WATCH_RUNTIME["last_candidates"] = int(
        get_setting("watch_last_candidates", "0") or 0
    )
    WATCH_RUNTIME["last_sent"] = int(get_setting("watch_last_sent", "0") or 0)

    top_score = get_setting("watch_top_score")
    WATCH_RUNTIME["top_score"] = float(top_score) if top_score else None
    WATCH_RUNTIME["top_symbol"] = get_setting("watch_top_symbol") or None
    WATCH_RUNTIME["top_timeframe"] = get_setting("watch_top_timeframe") or None
    WATCH_RUNTIME["last_error"] = get_setting("watch_last_error") or None


def _actual_watch_active() -> bool:
    return WATCH_TASK is not None and not WATCH_TASK.done()




def insert_snapshots(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    init_db()

    columns = [
        "collected_at", "source", "collector_version", "scrape_duration_seconds",
        "is_valid", "validation_errors", "symbol", "rank", "timeframe",
        "current_price", "short_max_pain", "long_max_pain",
        "short_liquidation_amount", "long_liquidation_amount",
        "distance_short_abs", "distance_short_pct", "distance_long_abs", "distance_long_pct",
        "delta_short_abs", "delta_short_pct", "delta_long_abs", "delta_long_pct",
        "alert_level"
    ]

    values = [[row.get(col) for col in columns] for row in rows]

    if use_postgres():
        placeholders = ", ".join(["%s"] * len(columns))
        col_sql = ", ".join(columns)
        update_sql = ", ".join([f"{c}=EXCLUDED.{c}" for c in columns if c not in ["collected_at", "symbol", "timeframe"]])
        sql = f"""
        INSERT INTO max_pain_snapshots ({col_sql})
        VALUES ({placeholders})
        ON CONFLICT (collected_at, symbol, timeframe)
        DO UPDATE SET {update_sql}
        """
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, values)
            conn.commit()
    else:
        placeholders = ",".join(["?"] * len(columns))
        sql = f"INSERT OR REPLACE INTO max_pain_snapshots ({','.join(columns)}) VALUES ({placeholders})"
        with sqlite3.connect(DB_PATH) as conn:
            conn.executemany(sql, values)
            conn.commit()

    return len(rows)

def parse_number(value: str) -> Optional[float]:
    if value is None:
        return None

    raw = value.replace(",", "").replace("$", "").strip()
    match = re.search(r"-?\d+(\.\d+)?", raw)
    if not match:
        return None

    num = float(match.group(0))
    lower = raw.lower()
    if lower.endswith("k"):
        num *= 1_000
    elif lower.endswith("m"):
        num *= 1_000_000
    elif lower.endswith("b"):
        num *= 1_000_000_000
    return num

def pct_change(new, old):
    if new is None or old is None or old == 0:
        return None
    return ((new - old) / old) * 100

def distance_pct(price, target):
    if price is None or target is None or price == 0:
        return None
    return abs((target - price) / price) * 100

def distance_abs(price, target):
    if price is None or target is None:
        return None
    return abs(target - price)

def alert_level(delta_short_pct, delta_long_pct):
    values = [abs(v) for v in [delta_short_pct, delta_long_pct] if v is not None]
    if not values:
        return "none"
    max_delta = max(values)
    if max_delta >= 7:
        return "high"
    if max_delta >= 3:
        return "medium"
    if max_delta >= 1:
        return "low"
    return "none"

def previous_row(symbol, timeframe, before_collected_at):
    rows = query(
        """
        SELECT * FROM max_pain_snapshots
        WHERE symbol = ? AND timeframe = ? AND collected_at < ?
        ORDER BY collected_at DESC
        LIMIT 1
        """,
        (symbol, timeframe, before_collected_at)
    )
    return rows[0] if rows else None

def enrich_rows(rows):
    """Enrich rows without overwriting CoinGlass distance percentages.

    The DOM reader extracts Short/Long Distance directly from the site.
    We keep those values because they match what CoinGlass displays at collection time.
    Only fill missing distance values as fallback.
    """
    output = []
    for row in rows:
        price = row.get("current_price")
        short_mp = row.get("short_max_pain")
        long_mp = row.get("long_max_pain")

        if row.get("distance_short_abs") is None:
            row["distance_short_abs"] = distance_abs(price, short_mp)
        if row.get("distance_short_pct") is None:
            row["distance_short_pct"] = distance_pct(price, short_mp)
        if row.get("distance_long_abs") is None:
            row["distance_long_abs"] = distance_abs(price, long_mp)
        if row.get("distance_long_pct") is None:
            row["distance_long_pct"] = distance_pct(price, long_mp)

        # Deltas are intentionally hidden in the UI until historical comparison is defined.
        row["delta_short_abs"] = None
        row["delta_short_pct"] = None
        row["delta_long_abs"] = None
        row["delta_long_pct"] = None
        row["alert_level"] = "none"
        output.append(row)
    return output


def aes_decrypt_raw(ciphertext_b64: str, key: str) -> bytes:
    encrypted = base64.b64decode(ciphertext_b64)
    cipher = AES.new(key.encode("utf-8"), AES.MODE_ECB)
    return unpad(cipher.decrypt(encrypted), AES.block_size)

def gzip_to_text(raw: bytes) -> str:
    return zlib.decompress(raw, 16 + zlib.MAX_WBITS).decode("utf-8")

def decode_coinglass_payload(ciphertext_b64: str, key: str):
    return json.loads(gzip_to_text(aes_decrypt_raw(ciphertext_b64, key)))

def fetch_coinglass_timeframe(timeframe: str) -> List[Dict[str, Any]]:
    """
    Keep retrying a timeframe until it succeeds or until MAX_SECONDS_PER_TIMEFRAME is reached.
    This avoids losing data because of transient CoinGlass encryption/cache mismatches,
    but still prevents the bot from hanging forever.
    """
    api_range = API_TIMEFRAME_MAP.get(timeframe, timeframe)
    deadline = time.time() + MAX_SECONDS_PER_TIMEFRAME
    attempt = 0
    last_error = None

    while time.time() < deadline:
        attempt += 1

        headers = {
            "accept": "application/json",
            "accept-language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
            "origin": "https://www.coinglass.com",
            "referer": "https://www.coinglass.com/",
            "language": "en",
            "encryption": "true",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "connection": "close",
            "cache-ts-v2": str(int(time.time() * 1000)),
            "user-agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/150.0.0.0 Safari/537.36"
            ),
        }

        try:
            with requests.Session() as session:
                response = session.get(
                    COINGLASS_API_URL,
                    params={
                        "range": api_range,
                        "_": f"{int(time.time() * 1000)}-{attempt}",
                    },
                    headers=headers,
                    timeout=12,
                )

            response.raise_for_status()
            payload = response.json()

            if payload.get("code") != "0" or "data" not in payload:
                raise RuntimeError(f"Unexpected CoinGlass API response for {timeframe}: {payload}")

            if response.headers.get("encryption") != "true":
                data = payload["data"]
                return data if isinstance(data, list) else json.loads(data)

            temp_key = base64.b64encode(b"d6537d845a964081").decode("utf-8")[:16]
            real_key = gzip_to_text(aes_decrypt_raw(response.headers["user"], temp_key))[:16]
            return decode_coinglass_payload(payload["data"], real_key)

        except Exception as e:
            last_error = e
            remaining = max(0, int(deadline - time.time()))
            print(f"[collector] {timeframe} attempt {attempt} failed: {e}; retrying in {RETRY_SLEEP_SECONDS}s; {remaining}s left")
            time.sleep(RETRY_SLEEP_SECONDS)

    raise TimeoutError(f"{timeframe} failed after {attempt} attempts over {MAX_SECONDS_PER_TIMEFRAME}s. Last error: {last_error}")

async def scrape_timeframe(timeframe: str, collected_at, scrape_duration: float) -> List[Dict[str, Any]]:
    api_rows = await asyncio.to_thread(fetch_coinglass_timeframe, timeframe)
    output = []

    for idx, item in enumerate(api_rows[:TOP_COINS_LIMIT], start=1):
        output.append({
            "collected_at": collected_at,
            "source": SOURCE_NAME,
            "collector_version": COLLECTOR_VERSION,
            "scrape_duration_seconds": scrape_duration,
            "is_valid": True if use_postgres() else 1,
            "validation_errors": None,
            "symbol": str(item.get("symbol", "")).upper(),
            "rank": idx,
            "timeframe": timeframe,
            "current_price": item.get("price"),
            "short_max_pain": item.get("maxShortLiquidationPrice"),
            "long_max_pain": item.get("maxLongLiquidationPrice"),
            "short_liquidation_amount": item.get("maxShortLiquidationLevel"),
            "long_liquidation_amount": item.get("maxLongLiquidationLevel"),
        })

    return output

def normalize_current_prices(rows):
    """
    Use one current price per symbol for the whole snapshot.

    The API returns a price inside each timeframe response. Because we collect ranges
    sequentially, the last successful response is the freshest price we fetched now.
    Therefore, for each symbol, use the latest price from the current collection,
    not a fixed timeframe such as 24h.
    """
    price_by_symbol = {}

    for row in rows:
        symbol = row.get("symbol")
        price = row.get("current_price")
        if symbol and price is not None:
            price_by_symbol[symbol] = price

    for row in rows:
        symbol = row.get("symbol")
        if symbol in price_by_symbol:
            row["current_price"] = price_by_symbol[symbol]

    return rows

def validate_snapshot(rows):
    """Validate the filtered Binance-backed snapshot.

    The raw DOM normally contains about 50 assets × 7 timeframes = 350 rows.
    After non-crypto filtering and Binance coverage checks, fewer symbols are
    intentionally saved. Therefore the expected saved-row count must be based
    on the symbols that remain, not the raw CoinGlass row count.
    """
    global_errors = []

    symbols = {
        str(row.get("symbol", "")).upper()
        for row in rows
        if row.get("symbol")
    }
    expected_saved_rows = len(symbols) * len(TIMEFRAMES)

    if rows and len(rows) != expected_saved_rows:
        global_errors.append(
            f"Filtered snapshot incomplete: expected {expected_saved_rows} rows "
            f"for {len(symbols)} saved symbols across {len(TIMEFRAMES)} timeframes, "
            f"got {len(rows)}"
        )

    seen_timeframes = {r["timeframe"] for r in rows if r.get("timeframe")}
    missing_timeframes = set(TIMEFRAMES) - seen_timeframes
    if missing_timeframes:
        global_errors.append(f"Missing timeframes: {sorted(missing_timeframes)}")

    for row in rows:
        row_errors = []

        if not row.get("symbol"):
            row_errors.append("missing symbol")
        if row.get("current_price") is None:
            row_errors.append("missing Binance current_price")
        if row.get("short_max_pain") is None:
            row_errors.append("missing short_max_pain")
        if row.get("long_max_pain") is None:
            row_errors.append("missing long_max_pain")

        if global_errors or row_errors:
            row["is_valid"] = False if use_postgres() else 0
            row["validation_errors"] = "; ".join(global_errors + row_errors)[:1000]
        else:
            row["is_valid"] = True if use_postgres() else 1
            row["validation_errors"] = None

    return rows


def _complete_symbol_audit(
    rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Keep only symbols that have exactly one row in all seven timeframes."""
    expected = set(TIMEFRAMES)
    rows_by_symbol: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    present_by_symbol: Dict[str, set] = defaultdict(set)
    duplicate_pairs: List[str] = []
    seen_pairs = set()

    for row in rows:
        symbol = str(row.get("symbol") or "").upper()
        timeframe = str(row.get("timeframe") or "")
        if not symbol or timeframe not in expected:
            continue

        pair = (symbol, timeframe)
        if pair in seen_pairs:
            duplicate_pairs.append(f"{symbol}/{timeframe}")
            continue

        seen_pairs.add(pair)
        present_by_symbol[symbol].add(timeframe)
        rows_by_symbol[symbol].append(row)

    complete_symbols = sorted(
        symbol
        for symbol, present in present_by_symbol.items()
        if present == expected
    )
    incomplete_symbols = {
        symbol: sorted(expected - present, key=TIMEFRAMES.index)
        for symbol, present in sorted(present_by_symbol.items())
        if present != expected
    }

    complete_rows = [
        row
        for symbol in complete_symbols
        for row in sorted(
            rows_by_symbol[symbol],
            key=lambda item: TIMEFRAMES.index(str(item.get("timeframe"))),
        )
    ]

    return {
        "complete_rows": complete_rows,
        "complete_symbols": complete_symbols,
        "incomplete_symbols": incomplete_symbols,
        "duplicate_pairs": sorted(set(duplicate_pairs)),
        "input_rows": len(rows),
        "expected_rows": len(complete_symbols) * len(TIMEFRAMES),
        "complete_row_count": len(complete_rows),
    }


def _format_incomplete_symbols(incomplete: Dict[str, List[str]]) -> str:
    if not incomplete:
        return "אין"
    return ", ".join(
        f"{symbol}({','.join(missing)})"
        for symbol, missing in incomplete.items()
    )


async def collect_once():
    """Collect and save one coherent seven-timeframe Binance-backed snapshot."""
    start = time.time()
    collected_dt = datetime.now(timezone.utc)
    collected_at = collected_dt if use_postgres() else collected_dt.isoformat()

    print(f"[collector] starting DOM collection at {collected_at}")

    snapshot = await collect_coinglass_dom_snapshot(
        timeframes=TIMEFRAMES,
        headless=True,
        url=COINGLASS_MAX_PAIN_URL,
    )

    reader_missing = list(snapshot.get("missing_timeframes", []))
    if reader_missing:
        raise RuntimeError(
            "CoinGlass snapshot incomplete after retries: "
            + ", ".join(reader_missing)
        )

    raw_rows = []
    market_only_count = 0

    for item in snapshot.get("rows", []):
        short_mp = item.get("max_short_price")
        long_mp = item.get("max_long_price")
        if short_mp is None or long_mp is None:
            market_only_count += 1
            continue

        symbol = str(item.get("symbol", "")).upper()
        if not symbol or symbol in NON_CRYPTO_SYMBOLS:
            continue

        raw_rows.append({
            "collected_at": collected_at,
            "source": SOURCE_NAME + "_dom_binance",
            "collector_version": COLLECTOR_VERSION,
            "scrape_duration_seconds": time.time() - start,
            "is_valid": True if use_postgres() else 1,
            "validation_errors": None,
            "symbol": symbol,
            "rank": item.get("rank"),
            "timeframe": item.get("timeframe"),
            "current_price": item.get("price"),
            "short_max_pain": short_mp,
            "long_max_pain": long_mp,
            "short_liquidation_amount": item.get("short_amount_usd"),
            "long_liquidation_amount": item.get("long_amount_usd"),
            "distance_short_abs": None,
            "distance_short_pct": None,
            "distance_long_abs": None,
            "distance_long_pct": None,
        })

    live_result = live_price_provider.enrich_snapshot_rows(
        raw_rows,
        excluded_symbols=NON_CRYPTO_SYMBOLS,
    )
    priced_rows = live_result.get("rows", [])
    skipped_symbols = live_result.get("skipped_symbols", [])
    price_result = live_result.get("price_result", {})

    elapsed = time.time() - start
    for row in priced_rows:
        row["collected_at"] = collected_at
        row["source"] = SOURCE_NAME + "_dom_binance"
        row["collector_version"] = COLLECTOR_VERSION
        row["scrape_duration_seconds"] = elapsed
        row["is_valid"] = True if use_postgres() else 1
        row["validation_errors"] = None

    audit = _complete_symbol_audit(priced_rows)
    rows = audit["complete_rows"]

    if not rows:
        raise RuntimeError(
            "No complete seven-timeframe symbols remained after Binance pricing"
        )

    rows = validate_snapshot(rows)
    rows = enrich_rows(rows)

    invalid_pairs = [
        f"{row.get('symbol')}/{row.get('timeframe')}"
        for row in rows
        if not bool(row.get("is_valid"))
    ]
    if invalid_pairs:
        raise RuntimeError(
            "Validation rejected complete snapshot rows: "
            + ", ".join(invalid_pairs[:20])
        )

    inserted = insert_snapshots(rows)
    expected_inserted = len(rows)

    report = {
        "raw_dom_rows": int(snapshot.get("row_count", 0) or 0),
        "prepared_rows": len(raw_rows),
        "priced_rows": len(priced_rows),
        "complete_symbols": len(audit["complete_symbols"]),
        "complete_symbol_names": audit["complete_symbols"],
        "expected_inserted": expected_inserted,
        "inserted": inserted,
        "incomplete_symbols": audit["incomplete_symbols"],
        "duplicate_pairs": audit["duplicate_pairs"],
        "binance_found": int(price_result.get("found_count", 0) or 0),
        "binance_missing": int(price_result.get("missing_count", 0) or 0),
        "skipped_symbols": skipped_symbols,
        "market_only_rows_seen": market_only_count,
        "missing_timeframes": [],
    }

    print(
        "[collector] audit "
        f"raw_dom_rows={report['raw_dom_rows']}; "
        f"prepared_rows={report['prepared_rows']}; "
        f"priced_rows={report['priced_rows']}; "
        f"complete_symbols={report['complete_symbols']}; "
        f"expected_inserted={report['expected_inserted']}; "
        f"inserted={report['inserted']}; "
        f"incomplete_symbols={report['incomplete_symbols']}; "
        f"duplicate_pairs={report['duplicate_pairs']}; "
        f"binance_found={report['binance_found']}; "
        f"binance_missing={report['binance_missing']}; "
        f"skipped_symbols={report['skipped_symbols']}"
    )

    if inserted != expected_inserted:
        raise RuntimeError(
            f"Database write mismatch: expected {expected_inserted}, "
            f"inserted {inserted}"
        )

    return report



def _timeframe_integrity(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    counts = {tf: 0 for tf in TIMEFRAMES}
    symbols_by_tf = {tf: set() for tf in TIMEFRAMES}

    for row in rows:
        tf = str(row.get("timeframe") or "")
        symbol = str(row.get("symbol") or "").upper()
        if tf in counts:
            counts[tf] += 1
            if symbol:
                symbols_by_tf[tf].add(symbol)

    missing = [tf for tf in TIMEFRAMES if counts[tf] == 0]
    minimum_rows = min(counts.values()) if counts else 0

    return {
        "ok": not missing and minimum_rows > 0,
        "counts": counts,
        "missing_timeframes": missing,
        "minimum_rows_per_timeframe": minimum_rows,
        "symbols_by_timeframe": symbols_by_tf,
    }


def _assert_complete_live_scan(rows: List[Dict[str, Any]], source: str) -> Dict[str, Any]:
    integrity = _timeframe_integrity(rows)
    if not integrity["ok"]:
        raise RuntimeError(
            f"{source} incomplete: missing timeframes="
            f"{integrity['missing_timeframes']}; counts={integrity['counts']}"
        )
    return integrity


async def collect_live_rows_for_watch():
    """Collect one complete seven-timeframe live snapshot without DB writes."""
    print("[scan] opening fresh CoinGlass snapshot", flush=True)

    snapshot = await collect_coinglass_dom_snapshot(
        timeframes=TIMEFRAMES,
        headless=True,
        url=COINGLASS_MAX_PAIN_URL,
    )

    missing_from_reader = list(snapshot.get("missing_timeframes", []))
    if missing_from_reader:
        raise RuntimeError(
            "CoinGlass scan incomplete after retries. Missing: "
            + ", ".join(missing_from_reader)
        )

    raw_rows = []
    for item in snapshot.get("rows", []):
        short_mp = item.get("max_short_price")
        long_mp = item.get("max_long_price")
        if short_mp is None or long_mp is None:
            continue

        symbol = str(item.get("symbol", "")).upper()
        if not symbol or symbol in NON_CRYPTO_SYMBOLS:
            continue

        raw_rows.append({
            "symbol": symbol,
            "rank": item.get("rank"),
            "timeframe": item.get("timeframe"),
            "current_price": item.get("price"),
            "short_max_pain": short_mp,
            "long_max_pain": long_mp,
            "short_liquidation_amount": item.get("short_amount_usd"),
            "long_liquidation_amount": item.get("long_amount_usd"),
            "distance_short_abs": None,
            "distance_short_pct": None,
            "distance_long_abs": None,
            "distance_long_pct": None,
            "alert_level": None,
        })

    live_result = live_price_provider.enrich_snapshot_rows(
        raw_rows,
        excluded_symbols=NON_CRYPTO_SYMBOLS,
    )
    rows = live_result.get("rows", [])
    integrity = _assert_complete_live_scan(rows, "Live scan")
    symbol_audit = _complete_symbol_audit(rows)

    if not symbol_audit["complete_symbols"]:
        raise RuntimeError(
            "Live scan has no symbol with all seven timeframes"
        )

    rows = symbol_audit["complete_rows"]
    integrity = _assert_complete_live_scan(
        rows,
        "Complete-symbol live scan",
    )

    live_result["rows"] = rows
    live_result["timeframe_integrity"] = integrity
    live_result["symbol_integrity"] = symbol_audit

    print(
        f"[scan] complete rows={len(rows)}; "
        f"complete_symbols={len(symbol_audit['complete_symbols'])}; "
        f"incomplete_symbols={symbol_audit['incomplete_symbols']}; "
        f"duplicates={symbol_audit['duplicate_pairs']}; "
        f"counts={integrity['counts']}; "
        f"skipped={live_result.get('skipped_symbols', [])}",
        flush=True,
    )
    return rows, live_result



def fmt_price(value):
    """Display full available price precision without scientific notation."""
    if value is None:
        return "-"
    try:
        from decimal import Decimal, InvalidOperation
        d = Decimal(str(value))
    except Exception:
        return str(value)

    text = format(d, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def fmt(value, digits=2):
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:,.{digits}f}"
    return str(value)

def short_time(value):
    s = str(value)
    return s[11:16] if len(s) >= 16 else s


def raw_latest_snapshot_rows():
    """Latest saved Binance-backed snapshot with validation metadata."""
    return query(
        f"""
        WITH latest AS (SELECT MAX(collected_at) AS max_time FROM max_pain_snapshots)
        SELECT symbol, timeframe, collected_at, source, is_valid, validation_errors,
               current_price, short_max_pain, long_max_pain,
               short_liquidation_amount, long_liquidation_amount,
               distance_short_abs, distance_short_pct,
               distance_long_abs, distance_long_pct,
               alert_level
        FROM max_pain_snapshots, latest
        WHERE collected_at = latest.max_time
        ORDER BY symbol, {TIMEFRAME_ORDER_SQL}
        """
    )


def latest_snapshot_live_result():
    rows = raw_latest_snapshot_rows()
    return {
        "rows": rows,
        "price_result": {
            "source": "binance_saved_at_collect",
            "found_count": len({r["symbol"] for r in rows}) if rows else 0,
            "missing_count": 0,
            "fetched_at_utc": "-",
        },
        "skipped_symbols": [],
    }


def latest_snapshot_rows():
    return raw_latest_snapshot_rows()



def side_from_row(row):
    """Return which Max Pain side is closer for one row."""
    ds = row["distance_short_pct"]
    dl = row["distance_long_pct"]
    if ds is None or dl is None:
        return None
    return "SHORT" if abs(ds) <= abs(dl) else "LONG"

def safe_avg(values):
    vals = [v for v in values if v is not None]
    return sum(vals) / len(vals) if vals else None

def tf_order_value(tf: str) -> int:
    try:
        return TIMEFRAMES.index(tf) + 1
    except ValueError:
        return 99


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Crypto Intelligence Bot פעיל.\n\n"
        "פקודות:\n"
        "/collect — איסוף מלא ושמירת Snapshot חדש\n"
        "/alerts — סריקה חיה חד-פעמית והצגת הזדמנויות\n"
        "/alert BTC — סריקה חיה והצגת כל 7 הטווחים של מטבע אחד\n"
        "/coin BTC — הצגת המטבע מה-Snapshot השמור האחרון\n"
        "/watch_on — הפעלת לולאת Watch אחת\n"
        "/watch_status — הצגת מצב בלבד\n"
        "/watch_stop — עצירת Watch"
    )



def _get_alert_command_lock() -> asyncio.Lock:
    global ALERT_COMMAND_LOCK
    if ALERT_COMMAND_LOCK is None:
        ALERT_COMMAND_LOCK = asyncio.Lock()
    return ALERT_COMMAND_LOCK


def _get_scrape_lock():
    """Shared lock for any CoinGlass/Binance scraping."""
    global SCRAPE_LOCK
    if SCRAPE_LOCK is None:
        import asyncio
        SCRAPE_LOCK = asyncio.Lock()
    return SCRAPE_LOCK

async def collect_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Run one manual collection and save it. Never starts Watch."""
    global COLLECT_LOCK

    if COLLECT_LOCK is None:
        COLLECT_LOCK = asyncio.Lock()

    if COLLECT_LOCK.locked():
        await update.message.reply_text(
            "⏳ פקודת /collect כבר פעילה. לא נפתח איסוף נוסף."
        )
        return

    scrape_lock = _get_scrape_lock()
    if scrape_lock.locked():
        owner = WATCH_RUNTIME.get("scan_owner") or "פקודה אחרת"
        await update.message.reply_text(
            f"⏳ הסורק תפוס כרגע על ידי {owner}. "
            "יש להמתין לסיום וללחוץ שוב על /collect."
        )
        return

    async with COLLECT_LOCK:
        async with scrape_lock:
            WATCH_RUNTIME["scan_owner"] = "/collect"
            await update.message.reply_text(
                "🔄 מתחיל איסוף מלא של 7 טווחי הזמן. "
                "הנתונים יישמרו רק אם כל הטווחים נקלטו."
            )
            try:
                report = await collect_once()

                incomplete_text = _format_incomplete_symbols(
                    report["incomplete_symbols"]
                )
                skipped_text = (
                    ", ".join(report["skipped_symbols"])
                    if report["skipped_symbols"]
                    else "אין"
                )

                await update.message.reply_text(
                    "✅ /collect הסתיים בהצלחה מלאה\n"
                    f"שורות DOM גולמיות: {report['raw_dom_rows']}\n"
                    f"שורות לאחר מחיר Binance: {report['priced_rows']}\n"
                    f"מטבעות מלאים ב-7/7 טווחים: "
                    f"{report['complete_symbols']}\n"
                    f"שורות צפויות לשמירה: "
                    f"{report['expected_inserted']}\n"
                    f"שורות שנשמרו בפועל: {report['inserted']}\n"
                    f"מטבעות חלקיים שלא נשמרו: {incomplete_text}\n"
                    f"סמלים ללא מחיר Binance/שדולגו: {skipped_text}\n"
                    "המרחקים חושבו ממחיר Binance."
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await update.message.reply_text(
                    f"❌ /collect נכשל ולא אושר כאיסוף מלא: {exc!r}"
                )
            finally:
                WATCH_RUNTIME["scan_owner"] = None



async def latest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = query("SELECT MAX(collected_at) AS latest_time, COUNT(*) AS rows_count FROM max_pain_snapshots")
    r = rows[0]
    await update.message.reply_text(f"Snapshot אחרון: {r['latest_time']}\\nמספר שורות: {r['rows_count']}")

async def coin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("שימוש: /coin BTC")
        return

    symbol = context.args[0].upper()
    snapshot_rows = [
        r for r in raw_latest_snapshot_rows()
        if str(r["symbol"]).upper() == symbol
    ]

    if not snapshot_rows:
        await update.message.reply_text(
            f"לא נמצא {symbol} ב-snapshot האחרון. הריצו /collect קודם."
        )
        return

    live_result = live_price_provider.enrich_snapshot_rows(snapshot_rows)
    rows = live_result.get("rows", [])
    price_result = live_result.get("price_result", {})
    if not rows:
        await update.message.reply_text(
            f"לא ניתן היה למשוך כעת מחיר Binance חי עבור {symbol}."
        )
        return

    rows.sort(key=lambda r: tf_order_value(r["timeframe"]))

    table = [[
        r["timeframe"],
        fmt_price(r["current_price"]),
        fmt_price(r["short_max_pain"]),
        fmt_price(r["long_max_pain"]),
        fmt(r["short_liquidation_amount"], 0),
        fmt(r["long_liquidation_amount"], 0),
        fmt(r["distance_short_pct"]),
        fmt(r["distance_long_pct"]),
        r.get("closest_side"),
    ] for r in rows]

    text = tabulate(
        table,
        headers=["TF", "BinancePx", "ShortMP", "LongMP", "Short$", "Long$", "ToShort%", "ToLong%", "Closest"],
        tablefmt="plain",
    )

    source = rows[0].get("price_source", "binance")
    fetched = price_result.get("fetched_at_utc") or rows[0].get("price_fetched_at_utc", "-")
    await update.message.reply_text(
        f"Price source: {source}\nFetched UTC: {fetched}\n"
        f"<pre>{html.escape(text)}</pre>",
        parse_mode="HTML",
    )


async def range_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await update.message.reply_text("שימוש: /range BTC 24h")
        return

    symbol = context.args[0].upper()
    timeframe = context.args[1].lower()

    rows = [
        r for r in latest_snapshot_rows()
        if str(r["symbol"]).upper() == symbol
        and str(r["timeframe"]).lower() == timeframe
    ]

    if not rows:
        await update.message.reply_text(
            f"לא נמצאו נתוני Binance חיים עבור {symbol}/{timeframe}."
        )
        return

    r = rows[0]
    table = [[
        fmt_price(r["current_price"]),
        fmt_price(r["short_max_pain"]),
        fmt_price(r["long_max_pain"]),
        fmt(r["short_liquidation_amount"], 0),
        fmt(r["long_liquidation_amount"], 0),
        fmt(r["distance_short_pct"]),
        fmt(r["distance_long_pct"]),
        r.get("closest_side"),
    ]]

    text = tabulate(
        table,
        headers=["BinancePx", "ShortMP", "LongMP", "Short$", "Long$", "ToShort%", "ToLong%", "Closest"],
        tablefmt="plain",
    )

    await update.message.reply_text(
        f"Price source: {r.get('price_source', 'binance')}\n"
        f"Fetched UTC: {r.get('price_fetched_at_utc', '-')}\n"
        f"<pre>{html.escape(text)}</pre>",
        parse_mode="HTML",
    )


async def top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        limit = int(context.args[0]) if context.args else 10
    except Exception:
        limit = 10
    limit = max(1, min(50, limit))

    rows = latest_snapshot_rows()
    if not rows:
        await update.message.reply_text("אין נתונים חיים זמינים. הריצו /collect ואז נסו שוב.")
        return

    candidates = []
    for r in rows:
        ds = r.get("distance_short_pct")
        dl = r.get("distance_long_pct")
        if ds is None or dl is None:
            continue
        closest_side = "SHORT" if abs(ds) <= abs(dl) else "LONG"
        closest_distance = min(abs(ds), abs(dl))
        candidates.append((closest_distance, closest_side, r))

    candidates.sort(key=lambda item: item[0])
    selected = candidates[:limit]

    table = [[
        r["symbol"],
        r["timeframe"],
        side,
        fmt_price(r["current_price"]),
        fmt_price(r["short_max_pain"]),
        fmt_price(r["long_max_pain"]),
        fmt(r["distance_short_pct"]),
        fmt(r["distance_long_pct"]),
    ] for distance, side, r in selected]

    text = tabulate(
        table,
        headers=["Coin", "TF", "Side", "BinancePx", "ShortMP", "LongMP", "ToShort%", "ToLong%"],
        tablefmt="plain",
    )
    await update.message.reply_text(
        "כל המרחקים חושבו מחדש לפי מחיר Binance חי.\n"
        f"<pre>{html.escape(text)}</pre>",
        parse_mode="HTML",
    )


async def consensus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display coins whose closest Max Pain side is consistent across timeframes."""
    min_hits = 7
    limit = 20

    if context.args:
        try:
            min_hits = max(1, min(7, int(context.args[0])))
        except Exception:
            min_hits = 7
    if len(context.args) >= 2:
        try:
            limit = max(1, min(50, int(context.args[1])))
        except Exception:
            limit = 20

    rows = latest_snapshot_rows()
    if not rows:
        await update.message.reply_text("אין נתונים לניתוח. הריצו /collect קודם.")
        return

    results = analysis.calculate_consensus(rows, min_hits=min_hits, limit=limit)
    if not results:
        await update.message.reply_text(f"לא נמצאו מטבעות עם קונצנזוס של {min_hits}/7. נסו /consensus 6")
        return

    table = [[
        r["symbol"],
        r["side"],
        f'{r["hits"]}/{r["total"]}',
        fmt(r["avg_dist"]),
        r["tfs"],
    ] for r in results]

    output = tabulate(table, headers=["Coin", "Side", "Score", "AvgDist%", "TFs"], tablefmt="plain")
    await update.message.reply_text(f"<pre>{html.escape(output)}</pre>", parse_mode="HTML")

async def gap(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display average percentage gap between Short/Long Max Pain."""
    limit = 20
    if context.args:
        try:
            limit = max(1, min(50, int(context.args[0])))
        except Exception:
            limit = 20

    rows = latest_snapshot_rows()
    if not rows:
        await update.message.reply_text("אין נתונים לחישוב. הריצו /collect קודם.")
        return

    results = analysis.calculate_gap(rows, limit=limit)
    if not results:
        await update.message.reply_text("אין מספיק נתונים לחישוב Gap.")
        return

    table = [[
        r["symbol"],
        f'{r["count"]}/7',
        fmt(r["avg_gap"]),
        fmt(r.get("avg_gap_abs")),
        f'{r["max_gap_tf"]}:{fmt(r["max_gap"])}',
        f'{r["min_gap_tf"]}:{fmt(r["min_gap"])}',
    ] for r in results]

    output = tabulate(table, headers=["Coin", "TFs", "AvgGap%", "AvgGap$", "MaxGap", "MinGap"], tablefmt="plain")
    await update.message.reply_text(f"<pre>{html.escape(output)}</pre>", parse_mode="HTML")

async def liqsum(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display liquidation amount balance.

    Usage:
    - /liqsum            market totals by timeframe + TOTAL
    - /liqsum top [n]    top coins by total liquidity across all timeframes
    - /liqsum BTC        liquidity balance for a specific coin by timeframe + TOTAL
    """
    rows = latest_snapshot_rows()
    if not rows:
        await update.message.reply_text("אין נתוני נזילות. הריצו /collect קודם.")
        return

    # /liqsum top [limit]
    if context.args and context.args[0].lower() == "top":
        limit = 20
        if len(context.args) >= 2:
            try:
                limit = max(1, min(50, int(context.args[1])))
            except Exception:
                limit = 20

        results = analysis.calculate_liquidity_by_coin(rows, limit=limit)
        if not results:
            await update.message.reply_text("אין נתוני נזילות להצגה.")
            return

        table = [[
            r["symbol"],
            f'{r["count"]}/7',
            fmt(r["total"], 0),
            fmt(r["short_total"], 0),
            fmt(r["long_total"], 0),
            r["dominant"],
            fmt(r["ratio"]),
        ] for r in results]

        output = tabulate(
            table,
            headers=["Coin", "TFs", "Total$", "Short$", "Long$", "Dominant", "Ratio"],
            tablefmt="plain",
        )
        await update.message.reply_text(f"<pre>{html.escape(output)}</pre>", parse_mode="HTML")
        return

    # /liqsum BTC
    if context.args:
        symbol = context.args[0].upper()
        result = analysis.calculate_liquidity_for_symbol_by_timeframe(rows, symbol)
        tf_rows = result["timeframes"]
        if not tf_rows:
            await update.message.reply_text(f"לא נמצאו נתוני נזילות עבור {symbol}.")
            return
    else:
        result = analysis.calculate_liquidity_balance(rows)
        tf_rows = result["timeframes"]

    if not tf_rows:
        await update.message.reply_text("אין נתוני נזילות להצגה.")
        return

    table = []
    for r in tf_rows + [result["total"]]:
        table.append([
            r["timeframe"],
            fmt(r["short_total"], 0),
            fmt(r["long_total"], 0),
            r["dominant"],
            fmt(r["diff"], 0),
            fmt(r["ratio"]),
        ])

    output = tabulate(
        table,
        headers=["TF", "Short$", "Long$", "Dominant", "Long-Short$", "Ratio"],
        tablefmt="plain",
    )
    await update.message.reply_text(f"<pre>{html.escape(output)}</pre>", parse_mode="HTML")

async def market(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display overall market bias by timeframe and total.

    LONG means more coins are closer to their Long Max Pain side.
    SHORT means more coins are closer to their Short Max Pain side.
    """
    rows = latest_snapshot_rows()
    if not rows:
        await update.message.reply_text("אין נתונים לניתוח. הריצו /collect קודם.")
        return

    result = analysis.calculate_market_bias(rows)
    tf_rows = result.get("timeframes", [])
    overall = result.get("overall", {})

    if not tf_rows:
        await update.message.reply_text("אין מספיק נתונים לחישוב Market Bias.")
        return

    table = []
    for r in tf_rows:
        table.append([
            r["timeframe"],
            r["bias"],
            r["long_count"],
            r["short_count"],
            fmt(r["long_pct"]),
            fmt(r["short_pct"]),
        ])

    table.append([
        "TOTAL",
        overall.get("bias"),
        overall.get("long_count"),
        overall.get("short_count"),
        fmt(overall.get("long_pct")),
        fmt(overall.get("short_pct")),
    ])

    output = tabulate(
        table,
        headers=["TF", "Bias", "LONG", "SHORT", "Long%", "Short%"],
        tablefmt="plain",
    )
    await update.message.reply_text(f"<pre>{html.escape(output)}</pre>", parse_mode="HTML")


async def btc_like(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display coins whose closest Max Pain side is similar to BTC."""
    min_hits = 5
    limit = 20

    if context.args:
        try:
            min_hits = max(1, min(7, int(context.args[0])))
        except Exception:
            min_hits = 5
    if len(context.args) >= 2:
        try:
            limit = max(1, min(50, int(context.args[1])))
        except Exception:
            limit = 20

    rows = latest_snapshot_rows()
    if not rows:
        await update.message.reply_text("אין נתונים לניתוח. הריצו /collect קודם.")
        return

    results = analysis.calculate_btc_similarity(rows, min_hits=min_hits, limit=limit)
    if not results:
        await update.message.reply_text(f"לא נמצאו מטבעות עם התאמה ל-BTC של {min_hits}/7. נסו /btc_like 4")
        return

    table = [[
        r["symbol"],
        f'{r["hits"]}/{r["total"]}',
        r["same_tfs"],
        r["different_tfs"],
    ] for r in results]

    output = tabulate(
        table,
        headers=["Coin", "Match", "SameTFs", "DiffTFs"],
        tablefmt="plain",
    )
    await update.message.reply_text(f"<pre>{html.escape(output)}</pre>", parse_mode="HTML")

async def score(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show transparent setup strength breakdown for one coin."""
    if not context.args:
        await update.message.reply_text("שימוש: /score BTC")
        return

    symbol = context.args[0].upper()
    rows = latest_snapshot_rows()
    if not rows:
        await update.message.reply_text("אין נתונים לניתוח. הריצו /collect קודם.")
        return

    result = decision_engine.calculate_score_for_symbol(rows, symbol)
    if not result.get("ok"):
        await update.message.reply_text(f"לא נמצאו נתונים עבור {symbol}.")
        return

    header = [
        ["Coin", result["symbol"]],
        ["Direction", result["direction"]],
        ["SetupStrength", result["setup_strength"]],
        ["Confidence", result["confidence"]],
        ["Consensus", f'{result["consensus_hits"]}/{result["consensus_total"]}'],
        ["AvgDist%", fmt(result.get("avg_distance"))],
        ["AvgGap%", fmt(result.get("gap_avg_pct"))],
    ]

    comp_table = [[
        c["name"],
        f'{fmt(c["score"])}/{c["max"]}',
        c["direction"],
        c["reason"],
    ] for c in result["components"]]

    text1 = tabulate(header, tablefmt="plain")
    text2 = tabulate(comp_table, headers=["Component", "Score", "Dir", "Reason"], tablefmt="plain")
    await update.message.reply_text(f"<pre>{html.escape(text1 + chr(10) + chr(10) + text2)}</pre>", parse_mode="HTML")


async def score_top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show top coins by setup strength."""
    limit = 15
    if context.args:
        try:
            limit = max(1, min(50, int(context.args[0])))
        except Exception:
            limit = 15

    rows = latest_snapshot_rows()
    if not rows:
        await update.message.reply_text("אין נתונים לניתוח. הריצו /collect קודם.")
        return

    results = decision_engine.calculate_scores(rows, limit=limit)
    if not results:
        await update.message.reply_text("אין מספיק נתונים לחישוב Setup Strength.")
        return

    table = [[
        r["symbol"],
        r["direction"],
        r["setup_strength"],
        r["confidence"],
        f'{r["consensus_hits"]}/{r["consensus_total"]}',
        fmt(r.get("avg_distance")),
        fmt(r.get("gap_avg_pct")),
    ] for r in results]

    output = tabulate(table, headers=["Coin", "Dir", "Strength", "Conf", "Cons", "AvgDist%", "AvgGap%"], tablefmt="plain")
    await update.message.reply_text(f"<pre>{html.escape(output)}</pre>", parse_mode="HTML")



def _row_get(row, key, default=None):
    try:
        return row[key]
    except Exception:
        return default


def _quality_result(item: Dict[str, Any], rows: List[Any]) -> Dict[str, Any]:
    """Quality is descriptive only and never changes Priority."""
    symbol = item.get("symbol")
    timeframe = item.get("timeframe")
    symbol_rows = [
        row for row in rows
        if str(_row_get(row, "symbol", "")).upper() == str(symbol).upper()
    ]
    row = next(
        (r for r in symbol_rows if str(_row_get(r, "timeframe", "")) == str(timeframe)),
        None,
    )

    yellow = []
    orange = []
    red = []

    available_tfs = {
        str(_row_get(r, "timeframe"))
        for r in symbol_rows if _row_get(r, "timeframe")
    }
    missing = [tf for tf in TIMEFRAMES if tf not in available_tfs]
    if len(missing) == 1:
        yellow.append(
            "חסר טווח הזמן " + missing[0]
            + "; הקונצנזוס מבוסס על 6 מתוך 7 טווחים."
        )
    elif 2 <= len(missing) <= 3:
        orange.append(
            "חסרים טווחי הזמן " + ", ".join(missing)
            + "; הקונצנזוס מבוסס על מידע חלקי."
        )
    elif len(missing) >= 4:
        red.append(
            "קיימים פחות מארבעה טווחים תקינים; אמינות ההתראה נמוכה מאוד."
        )

    if row is None:
        red.append("לא נמצאה שורת מקור תואמת למטבע ולטווח הזמן.")
    else:
        if _row_get(row, "current_price") in (None, 0):
            red.append("מחיר Binance חסר או אינו תקין.")
        if _row_get(row, "short_max_pain") is None:
            red.append("יעד Short Max Pain חסר.")
        if _row_get(row, "long_max_pain") is None:
            red.append("יעד Long Max Pain חסר.")
        if (
            _row_get(row, "short_liquidation_amount") in (None, 0)
            or _row_get(row, "long_liquidation_amount") in (None, 0)
        ):
            orange.append(
                "אחד מסכומי הנזילות חסר או שווה לאפס; "
                "מאזן וצפיפות הנזילות פחות אמינים."
            )
        if (
            _row_get(row, "distance_short_pct") is None
            or _row_get(row, "distance_long_pct") is None
        ):
            red.append("אחד מחישובי המרחק ל-Max Pain חסר.")

        validation_errors = _row_get(row, "validation_errors")
        if validation_errors:
            validation_text = str(validation_errors)
            stale_row_warning = (
                "expected around 350 rows" in validation_text.lower()
                and "got 231" in validation_text.lower()
            )
            if not stale_row_warning:
                orange.append("בדיקת האיסוף דיווחה: " + validation_text)
        elif _row_get(row, "is_valid", True) in (False, 0):
            orange.append("שורת הנתונים סומנה כלא תקינה בבדיקת האיסוף.")


    calculation_errors = item.get("calculation_validation_errors") or []
    for error in calculation_errors:
        red.append("בדיקת חישוב נכשלה: " + str(error))
    duplicates_removed = int(item.get("duplicate_rows_removed", 0) or 0)
    if duplicates_removed:
        orange.append(
            f"הוסרו {duplicates_removed} שורות כפולות של מטבע/טווח לפני החישוב."
        )

    if red:
        return {"level": "red", "title": "🔴 בעיית נתונים קריטית", "notes": red + orange + yellow}
    if orange:
        return {"level": "orange", "title": "🟠 אזהרת איכות נתונים", "notes": orange + yellow}
    if yellow:
        return {"level": "yellow", "title": "🟡 הערת איכות נתונים", "notes": yellow}
    return {"level": None, "title": None, "notes": []}


def _quality_block(item: Dict[str, Any], rows: List[Any]) -> str:
    result = _quality_result(item, rows)
    if not result["notes"]:
        return ""
    return (
        "\n\n" + result["title"] + ":\n"
        + "\n".join(f"• {note}" for note in result["notes"])
    )


def _all_timeframe_scores_block(item: Dict[str, Any], all_items, rows) -> str:
    """Show score/status for all seven timeframes only at card bottom."""
    symbol = str(item.get("symbol") or "").upper()
    by_timeframe = {
        str(other.get("timeframe")): other
        for other in all_items
        if str(other.get("symbol") or "").upper() == symbol
    }
    source_rows = {
        str(_row_get(row, "timeframe")): row
        for row in rows
        if str(_row_get(row, "symbol", "") or "").upper() == symbol
    }

    lines = [f"📊 מצב {symbol} בכל טווחי הזמן", ""]
    values = []

    for timeframe in TIMEFRAMES:
        other = by_timeframe.get(timeframe)
        row = source_rows.get(timeframe)

        price = _row_get(row, "current_price") if row is not None else None
        short_mp = _row_get(row, "short_max_pain") if row is not None else None
        long_mp = _row_get(row, "long_max_pain") if row is not None else None

        active_distances = []
        try:
            price_value = float(price)
            if price_value > 0:
                if short_mp is not None and float(short_mp) > price_value:
                    active_distances.append(
                        (float(short_mp) - price_value) / price_value * 100.0
                    )
                if long_mp is not None and float(long_mp) < price_value:
                    active_distances.append(
                        (price_value - float(long_mp)) / price_value * 100.0
                    )
        except (TypeError, ValueError):
            active_distances = []

        nearest_active_distance = min(active_distances) if active_distances else None

        if not active_distances:
            lines.append(
                f"🔴 {timeframe:<3}  אין יעד פעיל (Max Pain נלקח)"
            )
            continue

        if nearest_active_distance is not None and nearest_active_distance < MIN_DISPLAY_DISTANCE_PCT:
            lines.append(
                f"🟡 {timeframe:<3}  {nearest_active_distance:.2f}% "
                f"(מתחת לסף {MIN_DISPLAY_DISTANCE_PCT:.1f}%)"
            )
            if other is not None:
                values.append(float(other.get("score", other.get("priority", 0)) or 0))
            continue

        if other is None:
            lines.append(f"🔴 {timeframe:<3}  אין ציון זמין")
            continue

        value = float(other.get("score", other.get("priority", 0)) or 0)
        values.append(value)
        lines.append(f"🟢 {timeframe:<3}  {value:.2f}")

    average = sum(values) / len(values) if values else 0.0
    lines.append("")
    lines.append(f"ממוצע: {average:.2f}/100")
    return "\n\n" + "\n".join(lines)


def _alert_card(index: int, item: Dict[str, Any], all_items, rows) -> str:
    c = item.get("components", {})
    types = item.get("types", [])
    type_prefix = "🟢 " if len(types) > 1 else ""
    types_text = (
        "\n".join(f"{type_prefix}• {type_name}" for type_name in types)
        if types else "• ללא סוג חריגה"
    )

    near_share = item.get("near_share_pct")
    if near_share is None:
        balance_text = "⚪ Liquidity Balance: אין נתון"
    elif float(near_share) >= 60.0:
        balance_text = f"🟢 Liquidity Balance: {fmt(near_share)}% לצד הקרוב"
    elif float(near_share) <= 40.0:
        balance_text = f"🔴 Liquidity Balance: {fmt(near_share)}% לצד הקרוב"
    else:
        balance_text = f"⚪ Liquidity Balance: {fmt(near_share)}% לצד הקרוב"

    btc_direction_line = ""
    btc_reference_score = c.get("btc_reference_score")
    btc_reference_side = c.get("btc_reference_side")
    btc_relation = c.get("btc_relation")
    if btc_relation == "ALIGNED":
        btc_direction_line = (
            f"  - אישור BTC ({btc_reference_side}, "
            f"Score {fmt(btc_reference_score)}): "
            f"+{fmt(c.get('btc_approval'))}/"
            f"{fmt(c.get('btc_approval_max'))}\n"
        )
    elif btc_relation == "OPPOSITE":
        btc_direction_line = (
            f"  - התנגדות BTC ({btc_reference_side}, "
            f"Score {fmt(btc_reference_score)}): "
            f"-{fmt(c.get('btc_conflict_penalty'))}/"
            f"{fmt(c.get('btc_conflict_penalty_max'))}\n"
        )
    elif btc_relation == "MISSING":
        btc_direction_line = "  - BTC: אין נתון באותו טווח\n"

    average_score = item.get("average_score_all_timeframes")
    if average_score is None:
        symbol_items = [
            other for other in all_items
            if other.get("symbol") == item.get("symbol")
        ]
        average_score = (
            sum(
                float(other.get("score", other.get("priority", 0)) or 0)
                for other in symbol_items
            ) / len(symbol_items)
            if symbol_items else float(
                item.get("score", item.get("priority", 0)) or 0
            )
        )

    current_price = item.get("current_price")
    target_price = item.get("target_price")

    cluster_count = int(item.get("cluster_count", 0) or 0)
    same_direction_count = int(item.get("cluster_same_direction_count", 0) or 0)
    if cluster_count >= 3:
        cluster_summary = (
            f"Cluster Confidence: {cluster_count} טווחים בקלאסטר "
            f"(מתוך {same_direction_count} באותו כיוון)\n"
            f"סטייה ממוצעת מה-Median: "
            f"{fmt(item.get('cluster_mean_deviation_pct'))}%\n"
        )
    else:
        cluster_summary = f"אין קלאסטר בכיוון {item.get('side')}\n"

    def liquidity_line(label: str, amount: Any) -> str:
        try:
            value = float(amount or 0)
        except (TypeError, ValueError):
            value = 0.0
        marker = "🔴 " if value < 500_000 else ""
        return f"{marker}{label}: ${fmt(value, 0)}"

    card = (
        f"🚨 #{index} — {item['symbol']} / {item['timeframe']}\n"
        f"צד קרוב: {item['side']}\n"
        f"Score לטווח הנוכחי: "
        f"{fmt(item.get('score', item.get('priority')))}/100\n"
        f"ממוצע Score בכל 7 הטווחים: {fmt(average_score)}/100\n"
        f"מחיר נוכחי — "
        f"{'Hyperliquid' if item.get('price_source') == 'hyperliquid_all_mids' else 'Binance'}: "
        f"${fmt_price(current_price)}\n"
        f"יעד Max Pain הקרוב: ${fmt_price(target_price)}\n"
        f"מרחק ל-Max Pain: {fmt(item.get('distance_pct'))}% "
        f"(סף ניקוד דינמי: {fmt(item.get('allowed_distance_pct'))}%)\n"
        f"סיווג מרחק למסחר: "
        f"{_distance_trade_label(item.get('distance_pct'))}\n"
        f"קונצנזוס: {item.get('consensus_hits', 0)}/"
        f"{item.get('consensus_total', 0)}\n"
        f"Market: {fmt(item.get('market_support_pct'))}% תמיכה ב-{item['side']} "
        f"({item.get('market_support_count', 0)}/"
        f"{item.get('market_total_count', 0)})\n"
        + cluster_summary
        + f"Relative Gap Advantage: "
        f"{fmt((item.get('relative_gap_advantage') or 0) * 100)}%\n"
        "\n"
        "פירוט הניקוד:\n"
        f"• Directional Alignment: "
        f"{fmt(c.get('directional_alignment'))}/30\n"
        f"  - Consensus: {fmt(c.get('consensus'))}/"
        f"{fmt(c.get('consensus_max'))}\n"
        + btc_direction_line
        + f"• Target Proximity: "
        f"{fmt(c.get('target_proximity'))}/25\n"
        f"• Cluster Confidence: "
        f"{fmt(c.get('cluster_confidence'))}/30\n"
        f"  - צפיפות יעדים: "
        f"{fmt(c.get('cluster_density'))}/12\n"
        f"  - מספר טווחים: "
        f"{fmt(c.get('cluster_coverage'))}/8\n"
        f"  - הצטברות נזילות: "
        f"{fmt(c.get('cluster_liquidity_growth'))}/10 "
        f"(מכפיל x{fmt(c.get('cluster_liquidity_multiplier'))})\n"
        f"• Relative Gap: {fmt(c.get('relative_gap'))}/15\n"
        "\n"
        f"סוגי חריגה:\n{types_text}\n\n"
        f"{balance_text}\n"
        + liquidity_line("נזילות בצד הקרוב", item.get("near_amount")) + "\n"
        + liquidity_line("נזילות בצד השני", item.get("far_amount"))
    )
    counter = counter_score.calculate_counter_score(item, rows, all_items)
    if counter.get("available"):
        primary_score = float(item.get("score", item.get("priority", 0)) or 0)
        counter_value = float(counter.get("score", 0) or 0)
        edge = round(primary_score - counter_value, 2)
        card += (
            "\n\nניקוד לכיוון הנגדי:\n"
            f"• {counter.get('side')}: {fmt(counter_value)}/100\n"
            f"• פער כיווני לטובת {item.get('side')}: {fmt(edge)} נקודות"
        )
    else:
        card += (
            "\n\nניקוד לכיוון הנגדי:\n"
            f"• {counter.get('side', '-')}: לא פעיל — יעד הכיוון הנגדי כבר נחצה או חסר"
        )

    card += _quality_block(item, rows)
    card += _all_timeframe_scores_block(item, all_items, rows)
    return card



def _is_displayable_opportunity(item: Dict[str, Any]) -> bool:
    """Return whether an already-scored opportunity is still tradable.

    Targets closer than MIN_DISPLAY_DISTANCE_PCT are not practical enough for
    an alert after fees, slippage and execution risk, so they are omitted from
    /alerts and Watch output. Crossed targets are already excluded by the
    scoring engine before this display filter runs.
    """
    try:
        distance = float(item.get("distance_pct"))
    except (TypeError, ValueError):
        return False

    return distance >= MIN_DISPLAY_DISTANCE_PCT


def _distance_trade_label(distance_pct: Any) -> str:
    try:
        distance = float(distance_pct)
    except (TypeError, ValueError):
        return "לא ידוע"
    if distance < 0.7:
        return "גבולי"
    if distance <= 1.3:
        return "טווח מועדף"
    return "רחוק יותר"


async def alert_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Run exactly one manual live scan for /alerts."""
    limit = 10
    if context.args:
        try:
            limit = max(1, min(15, int(context.args[0])))
        except (TypeError, ValueError):
            limit = 10

    command_lock = _get_alert_command_lock()
    if command_lock.locked():
        await update.message.reply_text(
            "⏳ /alerts כבר מבצע סריקה. לא נפתחה סריקה נוספת."
        )
        return

    scrape_lock = _get_scrape_lock()
    if scrape_lock.locked():
        owner = WATCH_RUNTIME.get("scan_owner") or "פקודה אחרת"
        if owner == "Watch":
            wait_text = (
                "⏳ סריקת Watch פעילה כרגע. פקודת Alerts ממתינה "
                "לסיומה ותתחיל אוטומטית כשהסורק יתפנה."
            )
        else:
            wait_text = (
                f"⏳ הסורק תפוס כרגע על ידי {owner}. "
                "פקודת Alerts ממתינה ותתחיל אוטומטית "
                "כשהסורק יתפנה."
            )
        await update.message.reply_text(wait_text)

    async with command_lock:
        try:
            async with scrape_lock:
                WATCH_RUNTIME["scan_owner"] = "/alerts"
                await update.message.reply_text(
                    "🔎 /alerts התחיל סריקה חיה מלאה של 7 טווחי הזמן."
                )
                rows, live_result = await collect_live_rows_for_watch()

            all_items = alert_engine.build_opportunities(rows, limit=500)
            displayable_items = [
                item
                for item in all_items
                if _is_displayable_opportunity(item)
            ]
            items = displayable_items[:limit]

            if not items:
                await update.message.reply_text(
                    "⚠️ הסריקה הסתיימה ללא הזדמנויות חדשות להצגה.\n"
                    f"יעדים במרחק קטן מ-{MIN_DISPLAY_DISTANCE_PCT:.2f}% "
                    "אינם מוצגים כהזדמנות מסחר רלוונטית."
                )
                return

            counts = live_result.get("timeframe_integrity", {}).get("counts", {})
            await update.message.reply_text(
                "✅ /alerts הסתיים\n"
                f"מוצגות {len(items)} התוצאות המובילות.\n"
                f"טווחים שנקלטו: {', '.join(f'{tf}:{counts.get(tf, 0)}' for tf in TIMEFRAMES)}"
            )

            for index, item in enumerate(items, start=1):
                await update.message.reply_text(
                    _alert_card(index, item, all_items, rows)
                )
            await update.message.reply_text(alert_summary.format_alert_count_summary(items))

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await update.message.reply_text(
                f"❌ /alerts נכשל: {exc!r}"
            )
        finally:
            if WATCH_RUNTIME.get("scan_owner") == "/alerts":
                WATCH_RUNTIME["scan_owner"] = None


async def alert_coin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Run one live scan and send a separate alert card for each timeframe."""
    if not context.args:
        await update.message.reply_text(
            "שימוש: /alert BTC\n"
            "אפשר להחליף את BTC בכל סימול מטבע אחר."
        )
        return

    symbol = str(context.args[0]).strip().upper()
    if not re.fullmatch(r"[A-Z0-9]{2,20}", symbol):
        await update.message.reply_text("סימול המטבע אינו תקין. לדוגמה: /alert BTC")
        return

    command_lock = _get_alert_command_lock()
    if command_lock.locked():
        await update.message.reply_text(
            "⏳ סריקת Alerts אחרת פעילה כרגע. נסו שוב לאחר שתסתיים."
        )
        return

    scrape_lock = _get_scrape_lock()
    if scrape_lock.locked():
        owner = WATCH_RUNTIME.get("scan_owner") or "פקודה אחרת"
        await update.message.reply_text(
            f"⏳ הסורק תפוס כרגע על ידי {owner}. "
            "הפקודה תמתין ותתחיל כשהסורק יתפנה."
        )

    async with command_lock:
        try:
            async with scrape_lock:
                WATCH_RUNTIME["scan_owner"] = f"/alert {symbol}"
                await update.message.reply_text(
                    f"🔎 מתחילה סריקה חיה של 7 הטווחים עבור {symbol}."
                )
                rows, _live_result = await collect_live_rows_for_watch()

            all_items = alert_engine.build_opportunities(rows, limit=500)
            symbol_items = [
                item for item in all_items
                if str(item.get("symbol") or "").upper() == symbol
            ]
            symbol_items.sort(
                key=lambda item: (
                    TIMEFRAMES.index(item.get("timeframe"))
                    if item.get("timeframe") in TIMEFRAMES else 99
                )
            )

            if not symbol_items:
                await update.message.reply_text(
                    f"⚠️ לא נמצאו טווחים ניתנים לחישוב עבור {symbol}. "
                    "ייתכן שאין מחיר Binance, שחסרים נתוני Max Pain, "
                    "או שכל היעדים כבר נחצו."
                )
                return

            await update.message.reply_text(
                f"✅ נמצאו {len(symbol_items)}/7 טווחים מחושבים עבור {symbol}. "
                "כל טווח יוצג בהודעה נפרדת."
            )

            item_by_tf = {item.get("timeframe"): item for item in symbol_items}
            sent_index = 0
            for timeframe in TIMEFRAMES:
                item = item_by_tf.get(timeframe)
                if item is None:
                    await update.message.reply_text(
                        f"⚪ {symbol} / {timeframe}: אין יעד פעיל שניתן לניקוד."
                    )
                    continue
                sent_index += 1
                await update.message.reply_text(
                    _alert_card(sent_index, item, all_items, rows)
                )

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await update.message.reply_text(
                f"❌ /alert {symbol} נכשל: {exc!r}"
            )
        finally:
            if WATCH_RUNTIME.get("scan_owner") == f"/alert {symbol}":
                WATCH_RUNTIME["scan_owner"] = None


async def debug_coin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Run a live transparent validation report for one symbol."""
    if not context.args:
        await update.message.reply_text("שימוש: /debug BTC")
        return
    symbol = str(context.args[0]).strip().upper()
    if not re.fullmatch(r"[A-Z0-9]{2,20}", symbol):
        await update.message.reply_text("סימול המטבע אינו תקין. לדוגמה: /debug BTC")
        return

    command_lock = _get_alert_command_lock()
    if command_lock.locked():
        await update.message.reply_text("⏳ סריקת Alerts אחרת פעילה כרגע. נסו שוב לאחר שתסתיים.")
        return

    async with command_lock:
        try:
            async with _get_scrape_lock():
                WATCH_RUNTIME["scan_owner"] = f"/debug {symbol}"
                await update.message.reply_text(
                    f"🔬 מתחילה בדיקת חישובים חיה עבור {symbol}."
                )
                rows, _ = await collect_live_rows_for_watch()

            report = alert_engine.debug_symbol(rows, symbol)
            items = report.get("items", [])
            if not items:
                await update.message.reply_text(f"לא נמצאו נתונים ניתנים לחישוב עבור {symbol}.")
                return

            lines = [
                f"🔬 DEBUG {symbol}",
                f"Consensus: LONG {report['LONG']}/{report['total']} | SHORT {report['SHORT']}/{report['total']}",
                f"כפילויות שהוסרו: {report['duplicates_removed']}",
                "",
            ]
            for item in items:
                c = item.get("components", {})
                members = ",".join(item.get("cluster_members") or []) or "-"
                status = "✅" if not item.get("calculation_validation_errors") else "❌"
                lines.extend([
                    f"{status} {item['timeframe']} {item['side']} | Score {float(item['score']):.2f}",
                    f"  Consensus {item.get('consensus_hits',0)}/{item.get('consensus_total',0)} = {float(c.get('consensus',0)):.2f}/{float(c.get('consensus_max',0)):.0f}",
                    (f"  BTC aligned {c.get('btc_reference_side')} Score {float(c.get('btc_reference_score') or 0):.2f}: +{float(c.get('btc_approval') or 0):.2f}/15"
                     if c.get('btc_relation') == 'ALIGNED' else
                     f"  BTC opposite {c.get('btc_reference_side')} Score {float(c.get('btc_reference_score') or 0):.2f}: -{float(c.get('btc_conflict_penalty') or 0):.2f}/10"
                     if c.get('btc_relation') == 'OPPOSITE' else
                     "  BTC self: consensus only" if c.get('btc_relation') == 'SELF' else
                     "  BTC reference missing"),
                    f"  Cluster {item.get('cluster_count',0)}/{item.get('cluster_same_direction_count',0)} [{members}] = {float(c.get('cluster_confidence',0)):.2f}/30",
                    f"  Sum check {float(item.get('component_sum_check',0)):.2f} = Score {float(item['score']):.2f}",
                ])
            lines.append("")
            if report.get("errors"):
                lines.append("❌ שגיאות:")
                lines.extend(f"• {err}" for err in report["errors"])
            else:
                lines.append("✅ כל בדיקות העקביות עברו.")

            text = "\n".join(lines)
            for start in range(0, len(text), 3800):
                await update.message.reply_text(text[start:start+3800])
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await update.message.reply_text(f"❌ /debug נכשל: {exc!r}")
        finally:
            if WATCH_RUNTIME.get("scan_owner") == f"/debug {symbol}":
                WATCH_RUNTIME["scan_owner"] = None


async def alert_explain(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "שימוש: /alert_explain BTC או /alert_explain BTC 24h"
        )
        return

    symbol = context.args[0].upper()
    timeframe = context.args[1].lower() if len(context.args) > 1 else None
    rows = latest_snapshot_rows()
    all_items = alert_engine.build_opportunities(rows, limit=500)

    matches = [
        item for item in all_items
        if item.get("symbol") == symbol
        and (timeframe is None or item.get("timeframe") == timeframe)
    ]
    if not matches:
        await update.message.reply_text("לא נמצאה כרגע התראה מתאימה.")
        return

    item = sorted(matches, key=lambda x: -x.get("priority", 0))[0]
    await update.message.reply_text(_alert_card(1, item, all_items, rows))


async def price_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check Binance live-price coverage. This command does not modify DB data."""
    rows = raw_latest_snapshot_rows()
    if not rows:
        await update.message.reply_text("אין snapshot קיים. הריצו /collect קודם.")
        return

    symbols = sorted({
        str(r["symbol"]).upper()
        for r in rows
        if r["symbol"] and str(r["symbol"]).upper() not in NON_CRYPTO_SYMBOLS
    })

    try:
        result = live_price_provider.fetch_binance_usdt_prices(symbols)
    except Exception as exc:
        await update.message.reply_text(
            "בדיקת החיבור ל-Binance נכשלה.\n"
            f"שגיאה: {exc!r}"
        )
        return

    # Specific coin: compare one live Binance price with all seven Max Pain targets.
    if context.args:
        symbol = context.args[0].upper()

        if symbol in NON_CRYPTO_SYMBOLS:
            await update.message.reply_text(f"{symbol} מסונן ואינו נחשב מטבע קריפטו במערכת.")
            return

        live = result["prices"].get(symbol)
        if not live:
            await update.message.reply_text(
                f"לא נמצא זוג {symbol}USDT ב-Binance.\n"
                "בשלב הבא נוסיף מקור גיבוי למטבעות שאינם נסחרים שם."
            )
            return

        symbol_rows = [
            r for r in rows
            if str(r["symbol"]).upper() == symbol
        ]

        table = []
        for r in symbol_rows:
            calc = live_price_provider.recalculate_distances(
                live["price"],
                r["short_max_pain"],
                r["long_max_pain"],
            )
            table.append([
                r["timeframe"],
                fmt_price(live["price"]),
                fmt_price(r["short_max_pain"]),
                fmt_price(r["long_max_pain"]),
                fmt(calc["short_signed_pct"]),
                fmt(calc["long_signed_pct"]),
                calc["closest_side"],
            ])

        output = tabulate(
            table,
            headers=["TF", "LivePrice", "ShortMP", "LongMP", "ToShort%", "ToLong%", "Closest"],
            tablefmt="plain",
        )

        intro = (
            f"בדיקת מחיר חי עבור {symbol}\n"
            f"מקור: Binance ({live['pair']})\n"
            f"זמן משיכה UTC: {result['fetched_at_utc']}\n"
            "המחיר עדיין לא נשמר ולא משנה את ההתראות בשלב זה.\n\n"
        )

        await update.message.reply_text(
            intro + f"<pre>{html.escape(output)}</pre>",
            parse_mode="HTML",
        )
        return

    found_symbols = sorted(result["prices"].keys())
    sample = found_symbols[:12]
    sample_table = [
        [
            symbol,
            result["prices"][symbol]["pair"],
            fmt_price(result["prices"][symbol]["price"]),
        ]
        for symbol in sample
    ]

    summary = (
        "בדיקת חיבור למחירי Binance\n"
        "--------------------------------\n"
        f"מטבעות קריפטו שנבדקו: {result['requested_count']}\n"
        f"נמצא מחיר חי: {result['found_count']}\n"
        f"חסרים ב-Binance: {result['missing_count']}\n"
        f"זמן משיכה UTC: {result['fetched_at_utc']}\n\n"
        "זו בדיקת כיסוי בלבד — המחירים עדיין לא משנים את החישובים או ההתראות.\n"
    )

    missing_text = ", ".join(result["missing_symbols"]) or "אין"
    sample_output = tabulate(
        sample_table,
        headers=["Coin", "Binance Pair", "Live Price"],
        tablefmt="plain",
    )

    await update.message.reply_text(
        summary
        + f"\nמטבעות חסרים: {missing_text}\n\n"
        + "דוגמת מחירים שנמצאו:\n"
        + f"<pre>{html.escape(sample_output)}</pre>",
        parse_mode="HTML",
    )


async def live_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Describe Binance-backed data saved by the latest collection."""
    rows = latest_snapshot_rows()
    if not rows:
        await update.message.reply_text("אין snapshot שמור. הריצו /collect קודם.")
        return

    symbols_used = sorted({str(r["symbol"]).upper() for r in rows if r["symbol"]})
    collected = query(
        "SELECT MAX(collected_at) AS latest_time FROM max_pain_snapshots"
    )[0]["latest_time"]

    text = (
        "Binance collection status\n"
        f"Latest snapshot: {collected}\n"
        f"Symbols saved with Binance price: {len(symbols_used)}\n"
        f"Rows saved: {len(rows)}\n"
        "Current price and all Max Pain distances were calculated during /collect.\n"
        "CoinGlass current price is not used as fallback."
    )
    await update.message.reply_text(text)




def _alert_fingerprint(item: Dict[str, Any]) -> str:
    # Fingerprint ignores exact priority so small score changes do not spam.
    payload = "|".join([
        item["symbol"],
        item["timeframe"],
        item["side"],
        ",".join(sorted(item["types"])),
    ])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _alert_recently_sent(fingerprint: str) -> bool:
    if use_postgres():
        rows = query(
            "SELECT 1 FROM alert_history "
            "WHERE fingerprint = ? AND created_at >= NOW() - (? * INTERVAL '1 minute') "
            "LIMIT 1",
            (fingerprint, WATCH_COOLDOWN_MINUTES),
        )
    else:
        rows = query(
            "SELECT 1 FROM alert_history "
            "WHERE fingerprint = ? AND datetime(created_at) >= datetime('now', ?) "
            "LIMIT 1",
            (fingerprint, f"-{WATCH_COOLDOWN_MINUTES} minutes"),
        )
    return bool(rows)


def _remember_alert(item: Dict[str, Any], fingerprint: str) -> None:
    now_value = datetime.now(timezone.utc)
    if not use_postgres():
        now_value = now_value.isoformat()

    try:
        execute_write(
            "INSERT INTO alert_history "
            "(created_at, fingerprint, symbol, timeframe, side, alert_types, priority) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                now_value,
                fingerprint,
                item["symbol"],
                item["timeframe"],
                item["side"],
                ",".join(item["types"]),
                item["priority"],
            ),
        )
    except Exception:
        # Existing fingerprint is acceptable; cooldown prevents normal duplicates.
        pass


def _watch_message(item, all_items, rows) -> str:
    return (
        "🚨 הזדמנות חדשה\n\n"
        + _alert_card(1, item, all_items, rows)
        + "\n\nזו התראת נתונים לבדיקה, לא הוראת מסחר."
    )


async def run_watch_cycle(bot_app, chat_id: int) -> Dict[str, Any]:
    """Run one complete Watch cycle and always send a Telegram outcome."""
    WATCH_RUNTIME["last_scan_utc"] = datetime.now(timezone.utc).isoformat()
    WATCH_RUNTIME["scan_in_progress"] = True
    WATCH_RUNTIME["scan_owner"] = "Watch"
    WATCH_RUNTIME["last_cycle_status"] = "running"
    WATCH_RUNTIME["last_error"] = None
    WATCH_RUNTIME["cycle_number"] = int(WATCH_RUNTIME.get("cycle_number", 0)) + 1
    cycle_number = WATCH_RUNTIME["cycle_number"]

    try:
        scrape_lock = _get_scrape_lock()
        async with scrape_lock:
            rows, live_result = await collect_live_rows_for_watch()

        all_items = alert_engine.build_opportunities(rows, limit=500)
        displayable_items = [
            item
            for item in all_items
            if _is_displayable_opportunity(item)
        ]
        candidates = [
            item
            for item in displayable_items
            if float(item.get("score", item.get("priority", 0)) or 0)
            >= WATCH_PRIORITY_THRESHOLD
        ]

        if candidates:
            result_items = candidates[:10]
            header = (
                f"✅ סריקת Watch #{cycle_number} הסתיימה\n"
                f"נמצאו {len(candidates)} תוצאות בציון "
                f"{WATCH_PRIORITY_THRESHOLD:.0f} ומעלה.\n"
                f"מוצגות {len(result_items)} התוצאות המובילות."
            )
        elif displayable_items:
            result_items = [displayable_items[0]]
            header = (
                f"✅ סריקת Watch #{cycle_number} הסתיימה\n"
                f"אין תוצאה בציון {WATCH_PRIORITY_THRESHOLD:.0f} ומעלה.\n"
                "מוצגת התוצאה בעלת הציון הגבוה ביותר."
            )
        else:
            result_items = []
            header = (
                f"⚠️ סריקת Watch #{cycle_number} הסתיימה ללא "
                "הזדמנויות חדשות להצגה.\n"
                f"יעדים במרחק קטן מ-{MIN_DISPLAY_DISTANCE_PCT:.2f}% "
                "אינם מוצגים כהזדמנות מסחר רלוונטית."
            )

        await bot_app.bot.send_message(chat_id=chat_id, text=header)
        for index, item in enumerate(result_items, start=1):
            await bot_app.bot.send_message(
                chat_id=chat_id,
                text=_alert_card(index, item, all_items, rows),
            )
        if result_items:
            await bot_app.bot.send_message(
                chat_id=chat_id,
                text=alert_summary.format_alert_count_summary(result_items),
            )

        top_item = (
            displayable_items[0]
            if displayable_items
            else None
        )
        WATCH_RUNTIME["last_found"] = len(displayable_items)
        WATCH_RUNTIME["last_candidates"] = len(candidates)
        WATCH_RUNTIME["last_sent"] = len(result_items)
        WATCH_RUNTIME["top_score"] = (
            top_item.get("score", top_item.get("priority"))
            if top_item else None
        )
        WATCH_RUNTIME["top_symbol"] = top_item.get("symbol") if top_item else None
        WATCH_RUNTIME["top_timeframe"] = (
            top_item.get("timeframe") if top_item else None
        )
        WATCH_RUNTIME["last_cycle_status"] = "completed"

        return {
            "ok": True,
            "found": len(all_items),
            "candidates": len(candidates),
            "sent": len(result_items),
            "timeframe_integrity": live_result.get("timeframe_integrity"),
        }

    except asyncio.CancelledError:
        WATCH_RUNTIME["last_cycle_status"] = "cancelled"
        raise
    except Exception as exc:
        WATCH_RUNTIME["last_cycle_status"] = "failed"
        WATCH_RUNTIME["last_error"] = repr(exc)
        try:
            await bot_app.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"❌ סריקת Watch #{cycle_number} נכשלה\n"
                    f"{exc!r}\n"
                    "הלולאה נשארת פעילה ותנסה שוב בעוד 15 דקות."
                ),
            )
        except Exception:
            pass
        return {"ok": False, "reason": repr(exc)}
    finally:
        WATCH_RUNTIME["scan_in_progress"] = False
        WATCH_RUNTIME["scan_owner"] = None


async def watch_loop(bot_app, chat_id: int):
    """Persistent single Watch loop; only /watch_stop cancels it."""
    global WATCH_SCAN_TASK

    print(
        f"[watch] loop started; chat_id={chat_id}; "
        f"interval={WATCH_INTERVAL_MINUTES}m",
        flush=True,
    )

    try:
        while True:
            WATCH_RUNTIME["last_cycle_status"] = "starting_cycle"
            WATCH_RUNTIME["next_scan_utc"] = datetime.now(
                timezone.utc
            ).isoformat()

            WATCH_SCAN_TASK = asyncio.create_task(
                run_watch_cycle(bot_app, chat_id),
                name="watch-scan-cycle",
            )
            try:
                await WATCH_SCAN_TASK
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Defensive: a cycle error must never kill the persistent loop.
                WATCH_RUNTIME["last_error"] = repr(exc)
                WATCH_RUNTIME["last_cycle_status"] = "cycle_crashed"
                print(f"[watch] uncaught cycle error: {exc!r}", flush=True)
            finally:
                WATCH_SCAN_TASK = None

            next_scan = datetime.now(timezone.utc) + timedelta(
                minutes=WATCH_INTERVAL_MINUTES
            )
            WATCH_RUNTIME["next_scan_utc"] = next_scan.isoformat()
            WATCH_RUNTIME["last_cycle_status"] = "waiting"

            await asyncio.sleep(WATCH_INTERVAL_MINUTES * 60)

    except asyncio.CancelledError:
        current_scan = WATCH_SCAN_TASK
        if current_scan is not None and not current_scan.done():
            current_scan.cancel()
            try:
                await current_scan
            except asyncio.CancelledError:
                pass
        raise
    finally:
        WATCH_SCAN_TASK = None
        WATCH_RUNTIME["scan_in_progress"] = False
        WATCH_RUNTIME["scan_owner"] = None
        WATCH_RUNTIME["next_scan_utc"] = None
        WATCH_RUNTIME["last_cycle_status"] = "stopped"
        print("[watch] loop stopped", flush=True)


ISRAEL_TZ = ZoneInfo("Asia/Jerusalem")


def _format_watch_time(value) -> str:
    if not value:
        return "-"
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(ISRAEL_TZ).strftime("%d/%m/%Y %H:%M:%S")
    except Exception:
        return str(value)


async def watch_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """The only command allowed to create the Watch loop."""
    global WATCH_TASK

    if WATCH_TASK is not None and not WATCH_TASK.done():
        await update.message.reply_text(
            "👁 Watch כבר פעיל. לא נפתחה לולאה נוספת."
        )
        return

    chat_id = int(update.effective_chat.id)
    WATCH_RUNTIME["last_error"] = None
    WATCH_RUNTIME["last_cycle_status"] = "starting"
    WATCH_RUNTIME["next_scan_utc"] = datetime.now(timezone.utc).isoformat()

    WATCH_TASK = asyncio.create_task(
        watch_loop(context.application, chat_id),
        name="persistent-watch-loop",
    )

    # Give the task one event-loop turn and verify that it remained alive.
    await asyncio.sleep(0)
    if WATCH_TASK.done():
        try:
            error = WATCH_TASK.exception()
        except Exception as exc:
            error = exc
        WATCH_TASK = None
        WATCH_RUNTIME["last_cycle_status"] = "failed_to_start"
        WATCH_RUNTIME["last_error"] = repr(error)
        await update.message.reply_text(
            f"❌ Watch לא הצליח להתחיל: {error!r}"
        )
        return

    await update.message.reply_text(
        "✅ Watch הופעל\n"
        "לולאה אחת פעילה. הסריקה הראשונה מתחילה כעת.\n"
        f"לאחר סיום כל סריקה תתחיל סריקה נוספת בעוד "
        f"{WATCH_INTERVAL_MINUTES} דקות.\n"
        "העצירה מתבצעת רק באמצעות /watch_stop."
    )


async def watch_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel the one Watch loop and any active Watch scan."""
    global WATCH_TASK, WATCH_SCAN_TASK

    loop_task = WATCH_TASK
    scan_task = WATCH_SCAN_TASK
    was_active = (
        loop_task is not None and not loop_task.done()
    ) or (
        scan_task is not None and not scan_task.done()
    )

    if scan_task is not None and not scan_task.done():
        scan_task.cancel()

    if loop_task is not None and not loop_task.done():
        loop_task.cancel()
        try:
            await asyncio.wait_for(loop_task, timeout=30)
        except asyncio.CancelledError:
            pass
        except asyncio.TimeoutError:
            print("[watch] stop timed out", flush=True)

    WATCH_TASK = None
    WATCH_SCAN_TASK = None
    WATCH_RUNTIME["scan_in_progress"] = False
    WATCH_RUNTIME["scan_owner"] = None
    WATCH_RUNTIME["next_scan_utc"] = None
    WATCH_RUNTIME["last_cycle_status"] = "stopped"

    await update.message.reply_text(
        "🛑 Watch הופסק."
        if was_active
        else "🛑 Watch כבר היה כבוי."
    )


async def watch_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Read-only status. This function never starts a scan."""
    loop_active = WATCH_TASK is not None and not WATCH_TASK.done()
    scan_active = WATCH_SCAN_TASK is not None and not WATCH_SCAN_TASK.done()

    next_dt = _parse_utc_setting(WATCH_RUNTIME.get("next_scan_utc"))
    countdown = "-"
    if next_dt is not None:
        seconds_left = max(
            0,
            int((next_dt - datetime.now(timezone.utc)).total_seconds()),
        )
        countdown = f"{seconds_left // 60} דקות ו-{seconds_left % 60} שניות"

    top_score = WATCH_RUNTIME.get("top_score")
    top_text = (
        "-"
        if top_score is None
        else (
            f"{WATCH_RUNTIME.get('top_symbol')} / "
            f"{WATCH_RUNTIME.get('top_timeframe')} "
            f"({fmt(top_score)}/100)"
        )
    )

    await update.message.reply_text(
        f"👁 Watch: {'פעיל' if loop_active else 'כבוי'}\n\n"
        f"לולאה פעילה: {'כן' if loop_active else 'לא'}\n"
        f"סריקה פעילה כרגע: {'כן' if scan_active else 'לא'}\n"
        f"בעל הסורק: {WATCH_RUNTIME.get('scan_owner') or '-'}\n"
        f"סטטוס מחזור: {WATCH_RUNTIME.get('last_cycle_status') or '-'}\n"
        f"מספר מחזור: {WATCH_RUNTIME.get('cycle_number', 0)}\n"
        f"סריקה אחרונה — שעון ישראל: "
        f"{_format_watch_time(WATCH_RUNTIME.get('last_scan_utc'))}\n"
        f"סריקה הבאה — שעון ישראל: "
        f"{_format_watch_time(WATCH_RUNTIME.get('next_scan_utc'))}\n"
        f"זמן נותר: {countdown}\n"
        f"מעל הסף במחזור האחרון: "
        f"{WATCH_RUNTIME.get('last_candidates', 0)}\n"
        f"תוצאות שנשלחו: {WATCH_RUNTIME.get('last_sent', 0)}\n"
        f"מועמד מוביל: {top_text}"
        + (
            f"\nשגיאה אחרונה: {WATCH_RUNTIME['last_error']}"
            if WATCH_RUNTIME.get("last_error")
            else ""
        )
    )


async def telegram_error_handler(
    update: object,
    context: ContextTypes.DEFAULT_TYPE,
):
    print(
        f"[telegram] handler error: {context.error!r}; update={update!r}",
        flush=True,
    )
    message = getattr(update, "effective_message", None)
    if message:
        try:
            await message.reply_text(
                "❌ אירעה תקלה בטיפול בפקודה. הפרטים נרשמו בלוג."
            )
        except Exception:
            pass


def _tradingview_authorized(request: web.Request, payload: Dict[str, Any]) -> bool:
    """Accept secret from header, query string, or JSON body.

    Body support is necessary because TradingView alert webhooks do not allow
    arbitrary HTTP headers. The body secret is removed from stored raw data.
    """
    if not TRADINGVIEW_WEBHOOK_SECRET:
        return False
    provided = (
        request.headers.get("X-Webhook-Secret")
        or request.query.get("secret")
        or payload.get("secret")
    )
    return bool(provided and str(provided) == TRADINGVIEW_WEBHOOK_SECRET)


def _insert_technical_signal(signal: technical_signal_store.NormalizedTechnicalSignal) -> bool:
    """Insert a normalized signal. Return False when it is a duplicate."""
    received_at = datetime.now(timezone.utc).isoformat()
    params = (
        received_at,
        signal.source,
        signal.symbol,
        signal.exchange,
        signal.timeframe,
        signal.direction,
        signal.technical_score,
        signal.signal_timestamp,
        signal.bar_close_timestamp,
        signal.is_confirmed,
        signal.indicator_version,
        signal.settings_profile,
        signal.fingerprint,
        signal.raw_payload,
    )

    if use_postgres():
        sql = """
        INSERT INTO technical_signals (
            received_at, source, symbol, exchange, timeframe, direction,
            technical_score, signal_timestamp, bar_close_timestamp,
            is_confirmed, indicator_version, settings_profile, fingerprint,
            raw_payload
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (fingerprint) DO NOTHING
        RETURNING id
        """
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                inserted = cur.fetchone() is not None
            conn.commit()
        return inserted

    sql = """
    INSERT OR IGNORE INTO technical_signals (
        received_at, source, symbol, exchange, timeframe, direction,
        technical_score, signal_timestamp, bar_close_timestamp,
        is_confirmed, indicator_version, settings_profile, fingerprint,
        raw_payload
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute(sql, params)
        conn.commit()
        return cursor.rowcount == 1


async def tradingview_webhook(request: web.Request):
    """Receive and persist one TradingView technical signal in Shadow Mode."""
    try:
        payload = await request.json()
    except Exception:
        return web.json_response(
            {"ok": False, "error": "body must be valid JSON"}, status=400
        )

    if not isinstance(payload, dict):
        return web.json_response(
            {"ok": False, "error": "body must be a JSON object"}, status=400
        )

    if not _tradingview_authorized(request, payload):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

    safe_payload = dict(payload)
    safe_payload.pop("secret", None)

    try:
        signal = technical_signal_store.normalize_payload(safe_payload)
        inserted = _insert_technical_signal(signal)
    except ValueError as exc:
        print(f"[tradingview] rejected payload: {exc}; payload={safe_payload!r}", flush=True)
        return web.json_response({"ok": False, "error": str(exc)}, status=422)
    except Exception as exc:
        print(f"[tradingview] persistence error: {exc!r}", flush=True)
        return web.json_response({"ok": False, "error": "persistence failure"}, status=500)

    print(
        f"[tradingview] {'stored' if inserted else 'duplicate'} "
        f"{signal.symbol} {signal.timeframe} {signal.direction} "
        f"score={signal.technical_score} confirmed={signal.is_confirmed}",
        flush=True,
    )
    return web.json_response({
        "ok": True,
        "stored": inserted,
        "duplicate": not inserted,
        "signal": {
            "symbol": signal.symbol,
            "exchange": signal.exchange,
            "timeframe": signal.timeframe,
            "direction": signal.direction,
            "technical_score": signal.technical_score,
            "signal_timestamp": signal.signal_timestamp,
            "bar_close_timestamp": signal.bar_close_timestamp,
            "is_confirmed": signal.is_confirmed,
            "indicator_version": signal.indicator_version,
            "settings_profile": signal.settings_profile,
            "fingerprint": signal.fingerprint,
        },
    })


async def technical_status_api(request: web.Request):
    """Read-only ingestion status for deployment checks."""
    rows = query(
        """
        SELECT symbol, exchange, timeframe, direction, technical_score,
               signal_timestamp, received_at, is_confirmed, indicator_version
        FROM technical_signals
        ORDER BY received_at DESC
        LIMIT 20
        """
    )
    items = [dict(row) for row in rows]
    for item in items:
        if "is_confirmed" in item:
            item["is_confirmed"] = bool(item["is_confirmed"])
    return web.json_response({"ok": True, "count": len(items), "latest": items})


async def technical_status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = query(
        """
        SELECT symbol, timeframe, direction, technical_score,
               signal_timestamp, received_at, is_confirmed, indicator_version
        FROM technical_signals
        ORDER BY received_at DESC
        LIMIT 10
        """
    )
    if not rows:
        await update.message.reply_text(
            "📡 עדיין לא התקבלו אותות מהאינדיקטור של TradingView."
        )
        return

    lines = ["📡 אותות טכניים אחרונים — Shadow Mode", ""]
    for row in rows:
        confirmed = "✅" if bool(row["is_confirmed"]) else "⏳"
        lines.append(
            f"{confirmed} {row['symbol']} | {row['timeframe']} | "
            f"{row['direction']} | {float(row['technical_score']):.1f}/100"
        )
        lines.append(f"   Signal: {row['signal_timestamp']}")
        if row["indicator_version"]:
            lines.append(f"   Version: {row['indicator_version']}")
    await update.message.reply_text("\n".join(lines))

async def health(request):
    return web.json_response({"status": "ok", "service": "crypto-intelligence-v1"})

async def telegram_webhook(request):
    """Acknowledge Telegram immediately and process each update once."""
    bot_app = request.app["bot_app"]

    try:
        payload = await request.json()
        update_id = payload.get("update_id")

        if update_id is not None and update_id in PROCESSED_UPDATE_IDS:
            print(
                f"[webhook] duplicate update ignored; update_id={update_id}",
                flush=True,
            )
            return web.json_response({"ok": True, "duplicate": True})

        if update_id is not None:
            PROCESSED_UPDATE_IDS.add(update_id)
            PROCESSED_UPDATE_ORDER.append(update_id)

            while len(PROCESSED_UPDATE_ORDER) > MAX_PROCESSED_UPDATE_IDS:
                old_update_id = PROCESSED_UPDATE_ORDER.pop(0)
                PROCESSED_UPDATE_IDS.discard(old_update_id)

        update = Update.de_json(payload, bot_app.bot)
        text = (
            update.effective_message.text
            if update.effective_message is not None
            else None
        )
        chat_id = (
            update.effective_chat.id
            if update.effective_chat is not None
            else None
        )

        print(
            f"[webhook] accepted update_id={update_id}; "
            f"chat_id={chat_id}; text={text!r}",
            flush=True,
        )

        # Return HTTP 200 immediately so Telegram does not retry long scans.
        bot_app.create_task(
            bot_app.process_update(update),
            update=update,
            name=f"telegram-update-{update_id}",
        )

        return web.json_response({"ok": True})

    except Exception as exc:
        print(f"[webhook] error: {exc!r}", flush=True)
        return web.json_response(
            {"ok": False, "error": str(exc)},
            status=500,
        )


async def start_web_server(bot_app):
    app = web.Application()
    app["bot_app"] = bot_app
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    app.router.add_post("/telegram", telegram_webhook)
    app.router.add_post("/webhooks/tradingview", tradingview_webhook)
    app.router.add_get("/technical/status", technical_status_api)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"[health] server running on port {PORT}")

async def main():
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN environment variable")
    if not PUBLIC_URL:
        raise RuntimeError(
            "Missing PUBLIC_URL environment variable. "
            "Example: https://crypto-intelligence-platform-1.onrender.com"
        )

    init_db()

    global WATCH_TASK, WATCH_SCAN_TASK
    WATCH_TASK = None
    WATCH_SCAN_TASK = None
    WATCH_RUNTIME.update({
        "last_scan_utc": None,
        "next_scan_utc": None,
        "last_found": 0,
        "last_candidates": 0,
        "last_sent": 0,
        "last_error": None,
        "last_cycle_status": "off_after_startup",
        "top_score": None,
        "top_symbol": None,
        "top_timeframe": None,
        "scan_in_progress": False,
        "scan_owner": None,
        "cycle_number": 0,
    })

    # Remove legacy activation flags. Startup never launches a scan.
    try:
        set_setting("watch_enabled", "0")
        set_setting("watch_next_scan_utc", "")
    except Exception as exc:
        print(f"[startup] legacy watch reset warning: {exc!r}", flush=True)

    bot_app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    bot_app.add_handler(CommandHandler("start", start))
    bot_app.add_handler(CommandHandler("help", start))
    bot_app.add_handler(CommandHandler("collect", collect_cmd))
    bot_app.add_handler(CommandHandler("coin", coin))
    bot_app.add_handler(CommandHandler("alerts", alert_check))
    bot_app.add_handler(CommandHandler("alert", alert_coin))
    bot_app.add_handler(CommandHandler("debug", debug_coin))
    bot_app.add_handler(CommandHandler("watch_on", watch_on))
    bot_app.add_handler(CommandHandler("watch_status", watch_status))
    bot_app.add_handler(CommandHandler("watch_stop", watch_off))
    bot_app.add_handler(CommandHandler("technical_status", technical_status_cmd))
    bot_app.add_error_handler(telegram_error_handler)

    await bot_app.initialize()
    await bot_app.start()

    print(
        "[startup] manual-only mode; no collection, alert, or Watch task created",
        flush=True,
    )

    webhook_url = f"{PUBLIC_URL}/telegram"
    await bot_app.bot.delete_webhook(drop_pending_updates=True)
    await bot_app.bot.set_webhook(
        url=webhook_url,
        drop_pending_updates=True,
    )
    print(f"[bot] webhook set to {webhook_url}", flush=True)

    await start_web_server(bot_app)

    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        if WATCH_TASK is not None and not WATCH_TASK.done():
            WATCH_TASK.cancel()
            try:
                await WATCH_TASK
            except asyncio.CancelledError:
                pass

        await bot_app.bot.delete_webhook(drop_pending_updates=False)
        await bot_app.stop()
        await bot_app.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
