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
import coinglass_oi_regime_service
import coinglass_history_backfill
import coinglass_flow_foundation
import coinglass_flow_engine
import market_confidence_engine
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
SPECIFIC_WATCH_TASK = None
OI_REGIME_TASK = None
HISTORY_BACKFILL_TASK = None
FLOW_COLLECTION_TASK = None
HISTORY_BACKFILL_LOCK = None
FLOW_BACKFILL_LOCK = None
SPECIFIC_WATCHES: Dict[str, Dict[str, Any]] = {}
SPECIFIC_WATCH_INTERVAL_MINUTES = 5
ALERT_COMMAND_LOCK = None
PROCESSED_UPDATE_IDS = set()
PROCESSED_UPDATE_ORDER = []
MAX_PROCESSED_UPDATE_IDS = 500
ALERT_ACTIVE = False
WATCH_INTERVAL_MINUTES = int(os.getenv("WATCH_INTERVAL_MINUTES", "15"))
HISTORY_BACKFILL_INTERVAL_HOURS = max(1, int(os.getenv("HISTORY_BACKFILL_INTERVAL_HOURS", "24")))
HISTORY_BACKFILL_STARTUP_DELAY_SECONDS = max(0, int(os.getenv("HISTORY_BACKFILL_STARTUP_DELAY_SECONDS", "60")))
HISTORY_BACKFILL_CHECK_INTERVAL_MINUTES = max(5, int(os.getenv("HISTORY_BACKFILL_CHECK_INTERVAL_MINUTES", "60")))
WATCH_PRIORITY_THRESHOLD = float(os.getenv("WATCH_PRIORITY_THRESHOLD", "70"))
TOP8_SYMBOLS = {"BTC", "ETH", "SOL", "HYPE", "DOGE", "ZEC", "BNB", "XRP"}
MIN_DISPLAY_DISTANCE_PCT = float(
    os.getenv("MIN_DISPLAY_DISTANCE_PCT", "0.8")
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
    "mode": "all",
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
        "/alerts_top8 — סריקה חיה רק עבור 8 מטבעות הליבה\n"
        "/alerts_liq 1000000 — Alerts רק מעל סך נזילות מינימלי בדולרים\n"
        "/alert BTC — סריקה חיה והצגת כל 7 הטווחים של מטבע אחד\n"
        "/coin BTC — הצגת המטבע מה-Snapshot השמור האחרון\n"
        "/watch_on — הפעלת צפייה כללית\n"
        "/watch_on_top8 — הפעלת Watch רק עבור 8 מטבעות הליבה\n"
        "/watch_on SOL 160 — צפייה ב-SOL עד יעד 160, כל 5 דקות\n"
        "/watch_status — הצגת מצב הצפיות\n"
        "/watch_stop — עצירת הצפייה הכללית\n"
        "/watch_stop SOL — עצירת הצפייה ב-SOL\n"
        "/oi_backfill [180|365] — Backfill היסטורי Price+OI (ברירת מחדל 180 יום)\n"
        "/oi_stats BTC — סטטיסטיקת Price+OI לפי 30m/1h/4h/12h/24h/48h/72h/7d\n"
        "/flow_backfill [180|365] — שמירת Buy/Sell + CVD רשמי בחוזים ובספוט\n"
        "/flow_state BTC — ניתוח Futures Flow ו-Spot Flow לקריאה בלבד\n"
        "/flow_stats BTC — P25/P50/P75/P90 של שינויי CVD לפי טווח\n"
        "/oi_validation BTC — בדיקת איכות timestamp ונקודות הייחוס\n"
        "/oi_state BTC — הצגת חישוב Price+OI Regime השמור האחרון\n"
        "/oi_regime BTC — Alias להצגת אותו חישוב Price+OI Regime"
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
    """Show compact score/status for all seven Max Pain timeframes at card bottom."""
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

    alert_side = str(item.get("side") or "").upper()
    directional_scores = (
        item.get("directional_scores_all_timeframes", {}).get(alert_side, {})
        or {}
    )
    lines = []
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
        value = directional_scores.get(timeframe)

        if not active_distances:
            lines.append(f"🔴 {timeframe:<3}  אין יעד פעיל (Max Pain נלקח)")
            continue

        if value is None:
            lines.append(f"🔴 {timeframe:<3}  אין ציון זמין לכיוון {alert_side}")
            continue

        value = float(value)
        values.append(value)

        if nearest_active_distance is not None and nearest_active_distance < MIN_DISPLAY_DISTANCE_PCT:
            # Below-threshold rows remain visible: the yellow marker is the warning,
            # while both actual proximity and the directional score stay transparent.
            lines.append(
                f"🟡 {timeframe:<3}  {nearest_active_distance:.2f}% | {value:.2f}"
            )
            continue

        marker = (
            "🟢"
            if other is not None
            and str(other.get("side") or "").upper() == alert_side
            else "🟡"
        )
        lines.append(f"{marker} {timeframe:<3}  {value:.2f}")

    average = sum(values) / len(values) if values else 0.0
    lines.append(f"ממוצע: {average:.2f}/100")
    return "\n\n" + "\n".join(lines)


def _build_opportunities_with_regime(rows, limit=500):
    """Build opportunities and attach read-only OI + Flow market evidence."""
    items = alert_engine.build_opportunities(rows, limit=limit)
    items = coinglass_oi_regime_service.attach_to_opportunities(items)
    return market_confidence_engine.attach_to_opportunities(items)


def _regime_block(item: Dict[str, Any]) -> str:
    """Render Price+OI context compactly while preserving full strength detail."""
    regime = item.get("market_regime") or {}
    windows = regime.get("windows") or {}
    overall = regime.get("overall") or {}
    if not windows:
        return (
            "\n\n<b>אין מספיק נתוני Price + OI</b>\n"
            f"{html.escape(str(regime.get('reason') or 'טרם נאספה דגימת Price + OI.'))}\n\n"
            "🧩 מסקנה משולבת\n"
            f"<b>{html.escape(str(item.get('composite_conclusion') or '—'))}</b>"
        )

    clock = {"30m":"🕒","1h":"🕐","4h":"🕓","12h":"🕛","24h":"🕛","48h":"🕑","72h":"🕒","7d":"🗓️"}
    indent = "\u00a0\u00a0\u00a0\u00a0"
    lines = ["", "━━━━━━━━━━━━━━━━━━━━", "📊 <b>Price + OI</b>"]
    for label in ("30m", "1h", "4h", "12h", "24h", "48h", "72h", "7d"):
        w = windows.get(label) or {}
        icon = clock.get(label, "🕒")
        if not w.get("available"):
            lines.append(f"{icon} {label} | אין עדיין היסטוריה מספקת")
            continue

        pd = w.get("price_change_pct")
        od = w.get("oi_change_pct")
        ptxt = "—" if pd is None else f"{float(pd):+.4f}%"
        otxt = "—" if od is None else f"{float(od):+.4f}%"
        state = html.escape(str(w.get("label") or "—"))
        ps = (w.get("price_strength") or {}).get("label")
        os_ = (w.get("oi_strength") or {}).get("label")

        lines.append(f"{icon} {label} | <b>{state}</b>")
        if w.get("historical_reference_available") and ps and os_:
            lines.append(
                f"{indent}Price: {ptxt} [{html.escape(str(ps))}]"
            )
            lines.append(
                f"{indent}OI   : {otxt} [{html.escape(str(os_))}]"
            )
        else:
            lines.append(f"{indent}Price: {ptxt}")
            lines.append(f"{indent}OI   : {otxt}")

    time_families = regime.get("time_families") or {}
    if time_families:
        lines.extend(["", "<b>משפחות זמן משוקללות</b>"])
        for key in ("now", "short", "medium", "long"):
            family = time_families.get(key) or {}
            direction = str(family.get("direction") or "NEUTRAL").upper()
            icon = _flow_direction_icon(direction)
            label = html.escape(str(family.get("label") or key))
            quality = float(family.get("quality") or 0.0) * 100.0
            agreement = float(family.get("agreement") or 0.0) * 100.0
            weight = float(family.get("weight") or 0.0)
            lines.append(f"{icon} {label}: <b>{direction.title()}</b> | משקל {weight:.0f}% | איכות {quality:.0f}% | הסכמה {agreement:.0f}%")

    overall_label = html.escape(str(overall.get("label") or "אין מסקנה"))
    strength = html.escape(str(overall.get("strength") or "—"))
    agreement = int(overall.get("agreement") or 0)
    valid_windows = int(overall.get("valid_windows") or 0)
    lines.extend(["", f"Overall: <b>{overall_label} — {strength}</b> ({agreement}/{valid_windows or 8})"])
    if regime.get("early_transition"):
        lines.append("⚠️ <b>Early Transition:</b> 30m + 1h סוטים מהמבנה הרחב")
    observations = regime.get("significance_observations") or []
    if observations:
        lines.append("🔎 <b>Historical significance</b>")
        for obs in observations[:3]:
            lines.append(f"• {html.escape(str(obs.get('text') or ''))}")
    lines.extend(["", "🧩 מסקנה משולבת", f"<b>{html.escape(str(item.get('composite_conclusion') or '—'))}</b>"])
    return "\n".join(lines)



def _flow_direction_icon(direction: str) -> str:
    return {"BULLISH":"🟢","BEARISH":"🔴","MIXED":"🟡"}.get(str(direction or "").upper(),"⚪")

def _flow_snapshot_line(market: Dict[str, Any]) -> str:
    quality = market.get("quality") or {}
    raw = quality.get("latest_time")
    if not raw:
        return "🕒 Snapshot: לא זמין"
    try:
        stamp = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        stamp = stamp.astimezone(timezone.utc)
        age_minutes = max(0, int((datetime.now(timezone.utc) - stamp).total_seconds() // 60))
        freshness = "טרי" if age_minutes <= coinglass_flow_foundation.FRESHNESS_TOLERANCE_MINUTES else "⚠️ ישן"
        return f"🕒 Snapshot: {stamp.strftime('%Y-%m-%d %H:%M UTC')} | גיל {age_minutes} דק׳ | {freshness}"
    except Exception:
        return f"🕒 Snapshot: {html.escape(str(raw))}"

def _flow_detail_block(item: Dict[str, Any]) -> str:
    context = item.get("flow_context") or {}
    if not context:
        return ""

    sections = []
    for key, title, heading in (
        ("futures", "Futures", "📈 <b>Futures CVD</b>"),
        ("spot", "Spot", "💱 <b>Spot CVD</b>"),
    ):
        market = context.get(key) or {}
        windows = market.get("windows") or {}
        lines = ["", "━━━━━━━━━━━━━━━━━━━━", heading, _flow_snapshot_line(market)]
        for label in ("30m", "1h", "4h", "12h", "24h", "48h", "72h", "7d"):
            w = windows.get(label) or {}
            if not w.get("available"):
                reason = html.escape(str(w.get("reason") or "אין היסטוריה מספקת"))
                lines.append(f"⚪ {label}: אין נתון ({reason})")
                continue
            direction = str(w.get("direction") or "NEUTRAL").upper()
            icon = _flow_direction_icon(direction)
            change = _fmt_flow_money(w.get("cvd_change_usd"))
            magnitude = html.escape(str(w.get("magnitude") or "—").title())
            state = html.escape(str(w.get("state") or "NEUTRAL").replace("_", " ").title())
            lines.append(f"{icon} {label}: <b>{change}</b> [{magnitude}] — {state}")

        groups = market.get("groups") or {}
        if groups:
            lines.append(f"<b>משפחות זמן — {title}</b>")
            for family_key in ("now", "short", "medium", "long"):
                family = groups.get(family_key) or {}
                direction = str(family.get("direction") or "NEUTRAL").upper()
                icon = _flow_direction_icon(direction)
                label = html.escape(str(family.get("label") or family_key))
                quality = float(family.get("quality") or 0.0) * 100.0
                agreement = float(family.get("agreement") or 0.0) * 100.0
                weight = float(family.get("weight") or 0.0)
                lines.append(
                    f"{icon} {label}: <b>{direction.title()}</b> | "
                    f"{weight:.0f}% | איכות {quality:.0f}% | הסכמה {agreement:.0f}%"
                )

        overall = market.get("overall") or {}
        weighted_score = float(overall.get("weighted_score") or 0.0)
        state_text = html.escape(str(overall.get("state") or "NO DATA").replace("_", " ").title())
        lines.append(f"Overall {title}: <b>{state_text}</b> ({weighted_score:+.1f})")
        early = market.get("early_shift")
        if early:
            lines.append(
                f"⚠️ Early Shift: {html.escape(str(early.get('new_direction')))} "
                f"מול {html.escape(str(early.get('established_direction')))}"
            )
        sections.append("\n".join(lines))

    return "\n".join(sections).rstrip()

def _market_evidence_block(item: Dict[str, Any]) -> str:
    evidence=item.get("market_evidence") or {}
    if not evidence: return ""
    expected=str(evidence.get("expected_price_direction") or "NEUTRAL").upper()
    counts=evidence.get("counts") or {}
    modules=evidence.get("modules") or {}
    confirmation=evidence.get("confirmation") or item.get("maxpain_confirmation") or {}
    status=str(confirmation.get("status") or "UNCONFIRMED")
    status_icon={"STRONG_CONFIRMED":"🔥","CONFIRMED":"✅","CONFLICT":"⚠️","BELOW_SCORE":"⚪"}.get(status,"🟡")
    lines=["", "━━━━━━━━━━━━━━━━━━━━", "🧭 <b>סיכום Price+OI, Futures ו-Spot</b>"]
    for key,title in (("positioning","Price+OI"),("futures_flow","Futures Flow"),("spot_flow","Spot Flow")):
        module=modules.get(key) or {}; direction=str(module.get("direction") or "NEUTRAL").upper()
        icon=_flow_direction_icon(direction); label=html.escape(str(module.get("label") or module.get("state") or "No data"))
        score=float(module.get("score") or 0.0)
        lines.append(f"{icon} {title}: <b>{direction.title()}</b> ({score:+.1f}) — {label}")
    lines.extend([
        "",
        f"הסכמה: 🟢 {int(counts.get('BULLISH',0))} | ⚪ {int(counts.get('NEUTRAL',0))} | 🔴 {int(counts.get('BEARISH',0))}",
        f"מסקנה: <b>{html.escape(str(evidence.get('classification_label') or '—'))}</b>",
        f"{status_icon} <b>{html.escape(str(confirmation.get('label') or 'Max Pain לא מאומת כרגע'))}</b>",
    ])
    spot_context=evidence.get("spot_context") or {}
    if spot_context:
        spot_icon={"SUPPORTS":"✅","NEUTRAL":"⚪","DIVERGING":"⚠️"}.get(str(spot_context.get("status") or "NEUTRAL"),"⚪")
        lines.append(f"{spot_icon} Spot משני: <b>{html.escape(str(spot_context.get('label') or 'Spot ניטרלי'))}</b> — ללא השפעה על ה-Confirmation")
    return "\n".join(lines)


def _alert_card(index: int, item: Dict[str, Any], all_items, rows) -> str:
    """Build the Stage 76 HTML-formatted Telegram alert card."""
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
        balance_text = f"🟢 Liquidity Balance: {fmt(near_share)}% לצד הנבחר"
    elif float(near_share) <= 40.0:
        balance_text = f"🔴 Liquidity Balance: {fmt(near_share)}% לצד הנבחר"
    else:
        balance_text = f"⚪ Liquidity Balance: {fmt(near_share)}% לצד הנבחר"

    average_score = item.get("average_score_all_timeframes")
    if average_score is None:
        average_score = float(item.get("score", item.get("priority", 0)) or 0)

    current_price = item.get("current_price")
    target_price = item.get("target_price")

    btc_reference_score = c.get("btc_reference_score")
    btc_reference_side = c.get("btc_reference_side")
    btc_relation = c.get("btc_relation")
    if btc_relation == "ALIGNED":
        btc_label = f"אישור BTC: {btc_reference_side}, Score {fmt(btc_reference_score)}"
        btc_points = c.get("btc_approval")
    elif btc_relation == "OPPOSITE":
        btc_label = f"התנגדות BTC: {btc_reference_side}, Score {fmt(btc_reference_score)}"
        btc_points = -float(c.get("btc_conflict_penalty", 0) or 0)
    else:
        btc_label = "BTC: אין נתון באותו טווח"
        btc_points = None

    def score_block(label: str, points: Any, suffix: str = " נקודות") -> str:
        if points is None:
            return f"{label}\n<b>—</b>"
        return f"{label}\n<b>{fmt(points)}{suffix}</b>"

    def liquidity_line(label: str, amount: Any) -> str:
        try:
            value = float(amount or 0)
        except (TypeError, ValueError):
            value = 0.0
        marker = "🔴 " if value < 500_000 else ""
        return f"{marker}{label}: ${fmt(value, 0)}"

    confirmation = (item.get("maxpain_confirmation") or (item.get("market_evidence") or {}).get("confirmation") or {})
    conflict_banner = ""
    if str(confirmation.get("status") or "").upper() == "CONFLICT":
        conflict_banner = f"⚠️ <b>{html.escape(str(confirmation.get('label') or 'Max Pain Conflict'))}</b>\n\n"

    card = (
        conflict_banner
        + "━━━━━━━━━━━━━━━━━━━━\n🎯 <b>Max Pain</b>\n"
        + f"#{index} {item['symbol']} / {item['timeframe']} | "
        f"{'🔴' if item.get('side') == 'SHORT' else '🟢'} {item['side']} | "
        f"<b>{fmt(item.get('score', item.get('priority')))}</b>\n"
        f"ממוצע {item['side']} בכל הטווחים: <b>{fmt(average_score)}</b>\n\n"
        f"מחיר נוכחי: ${fmt_price(current_price)}\n"
        + score_block(
            f"🎯 Max Pain: ${fmt_price(target_price)} ({fmt(item.get('distance_pct'))}%)",
            c.get("target_proximity"),
        )
        + "\n\n"
        + score_block(
            f"קונצנזוס: {item.get('consensus_hits', 0)}/{item.get('consensus_total', 0)}",
            c.get("consensus"),
        )
        + "\n\n"
        + score_block(btc_label, btc_points)
        + "\n\n"
        + score_block("Cluster", c.get("cluster_confidence"), " / 30")
        + "\n"
        + score_block("צפיפות יעדים", c.get("cluster_density"), " / 12")
        + "\n"
        + score_block("מספר טווחים", c.get("cluster_coverage"), " / 8")
        + "\n"
        + score_block(
            f"הצטברות נזילות (מכפיל {fmt(c.get('cluster_liquidity_multiplier'))}x)",
            c.get("cluster_liquidity_growth"), " / 10",
        )
        + "\n\n"
        + score_block("Gap", c.get("relative_gap"), " / 15")
        + "\n\n"
        f"סוגי חריגה:\n{types_text}\n\n"
        f"{balance_text}\n"
        + liquidity_line("נזילות בכיוון הנבחר", item.get("near_amount")) + "\n"
        + liquidity_line("נזילות בכיוון ההפוך", item.get("far_amount"))
    )

    counter_side = item.get("opposite_side")
    counter_value = item.get("opposite_score")
    if counter_value is None:
        counter = counter_score.calculate_counter_score(item, rows, all_items)
        counter_side = counter.get("side", counter_side)
        counter_value = counter.get("score") if counter.get("available") else None

    opposite_average = item.get("opposite_average_score_all_timeframes")
    if counter_value is not None:
        counter_line = (
            f"ניקוד לכיוון הנגדי — {counter_side}: <b>{fmt(counter_value)}</b>"
        )
    else:
        counter_line = (
            f"ניקוד לכיוון הנגדי — {counter_side or '-'}: "
            "לא פעיל — יעד הכיוון הנגדי כבר נחצה או חסר"
        )
    if opposite_average is not None:
        counter_line += (
            f" | ממוצע {counter_side} בכל הטווחים: "
            f"<b>{fmt(opposite_average)}</b>"
        )
    card += "\n\n" + counter_line

    card += _quality_block(item, rows)
    card += "\n\n<b>ציוני Max Pain בכל הטווחים</b>"
    card += _all_timeframe_scores_block(item, all_items, rows)
    card += _regime_block(item)
    card += _flow_detail_block(item)
    card += _market_evidence_block(item)
    return card



def _is_displayable_opportunity(item: Dict[str, Any]) -> bool:
    """Keep every scored opportunity with a valid active target visible.

    Distance affects only the Max Pain proximity component. A target below
    0.8% or above its symbol-specific allowed distance receives 0 proximity
    points, but is not removed from /alerts, /alerts_top8 or Watch output.
    """
    try:
        distance = float(item.get("distance_pct"))
    except (TypeError, ValueError):
        return False
    return distance >= 0.0


def _price_source_label(source: Any) -> str:
    labels = {
        "bybit_futures_mark": "Bybit Futures",
        "bybit_spot": "Bybit Spot",
        "hyperliquid": "Hyperliquid",
        "coingecko": "CoinGecko",
        "coinpaprika": "CoinPaprika",
        "coinglass_dom": "CoinGlass DOM",
        "binance_spot": "Binance Spot",
        "binance_futures_mark": "Binance Futures",
    }
    return labels.get(str(source or ""), "Live market")


def _distance_trade_label(distance_pct: Any) -> str:
    try:
        distance = float(distance_pct)
    except (TypeError, ValueError):
        return "לא ידוע"
    if distance < 0.8:
        return "גבולי"
    if distance <= 1.3:
        return "טווח מועדף"
    return "רחוק יותר"


async def alert_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Run /alerts, or route /alerts SYMBOL to the single-coin scan."""
    limit = 10
    if context.args:
        first_arg = str(context.args[0]).strip()
        if not first_arg.isdigit():
            await alert_coin(update, context)
            return
        limit = max(1, min(15, int(first_arg)))

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

            all_items = _build_opportunities_with_regime(rows, limit=500)
            displayable_items = [
                item
                for item in all_items
                if _is_displayable_opportunity(item)
            ]
            items = displayable_items[:limit]

            if not items:
                await update.message.reply_text(
                    "⚠️ הסריקה הסתיימה ללא הזדמנויות פעילות להצגה."
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
                    _alert_card(index, item, all_items, rows),
                    parse_mode="HTML",
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


def _filter_top8_items(items):
    """Keep only opportunities whose symbol belongs to the fixed Top-8 core list."""
    return [
        item for item in items
        if str(item.get("symbol") or "").upper() in TOP8_SYMBOLS
    ]


async def alert_check_top8(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Run one live alert scan and display only the fixed Top-8 symbols."""
    limit = 10
    if context.args:
        first_arg = str(context.args[0]).strip()
        if not first_arg.isdigit():
            await update.message.reply_text("שימוש: /alerts_top8 [מספר תוצאות]")
            return
        limit = max(1, min(15, int(first_arg)))

    command_lock = _get_alert_command_lock()
    if command_lock.locked():
        await update.message.reply_text(
            "⏳ סריקת Alerts כבר פעילה. לא נפתחה סריקה נוספת."
        )
        return

    scrape_lock = _get_scrape_lock()
    if scrape_lock.locked():
        owner = WATCH_RUNTIME.get("scan_owner") or "פקודה אחרת"
        await update.message.reply_text(
            f"⏳ הסורק תפוס כרגע על ידי {owner}. "
            "/alerts_top8 יתחיל אוטומטית כשהסורק יתפנה."
        )

    async with command_lock:
        try:
            async with scrape_lock:
                WATCH_RUNTIME["scan_owner"] = "/alerts_top8"
                await update.message.reply_text(
                    "🔎 /alerts_top8 התחיל סריקה חיה עבור "
                    "BTC, ETH, SOL, HYPE, DOGE, ZEC, BNB ו-XRP."
                )
                rows, live_result = await collect_live_rows_for_watch()

            all_items = _build_opportunities_with_regime(rows, limit=500)
            top8_all_items = _filter_top8_items(all_items)
            displayable_items = [
                item for item in top8_all_items
                if _is_displayable_opportunity(item)
            ]
            items = displayable_items[:limit]

            if not items:
                await update.message.reply_text(
                    "⚠️ הסריקה הסתיימה ללא הזדמנויות פעילות ב-Top 8."
                )
                return

            counts = live_result.get("timeframe_integrity", {}).get("counts", {})
            await update.message.reply_text(
                "✅ /alerts_top8 הסתיים\n"
                f"מוצגות {len(items)} התוצאות המובילות מתוך 8 מטבעות הליבה.\n"
                f"טווחים שנקלטו: {', '.join(f'{tf}:{counts.get(tf, 0)}' for tf in TIMEFRAMES)}"
            )

            for index, item in enumerate(items, start=1):
                await update.message.reply_text(
                    _alert_card(index, item, top8_all_items, rows),
                    parse_mode="HTML",
                )
            await update.message.reply_text(
                alert_summary.format_alert_count_summary(items)
            )

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await update.message.reply_text(
                f"❌ /alerts_top8 נכשל: {exc!r}"
            )
        finally:
            if WATCH_RUNTIME.get("scan_owner") == "/alerts_top8":
                WATCH_RUNTIME["scan_owner"] = None


async def alert_check_min_liquidity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Run /alerts with a user-defined minimum total liquidity per result."""
    if not context.args:
        await update.message.reply_text(
            "שימוש: /alerts_liq 1000000 [מספר תוצאות]\n"
            "הסכום הוא מינימום סך הנזילות בדולרים: הצד הקרוב + הצד השני."
        )
        return

    try:
        minimum_liquidity = float(str(context.args[0]).replace(",", ""))
        if minimum_liquidity < 0:
            raise ValueError
    except (TypeError, ValueError):
        await update.message.reply_text(
            "סף הנזילות אינו תקין. דוגמה: /alerts_liq 1000000"
        )
        return

    limit = 10
    if len(context.args) > 1:
        try:
            limit = max(1, min(15, int(context.args[1])))
        except (TypeError, ValueError):
            limit = 10

    command_lock = _get_alert_command_lock()
    if command_lock.locked():
        await update.message.reply_text(
            "⏳ פקודת Alerts כבר מבצעת סריקה. לא נפתחה סריקה נוספת."
        )
        return

    scrape_lock = _get_scrape_lock()
    if scrape_lock.locked():
        owner = WATCH_RUNTIME.get("scan_owner") or "פקודה אחרת"
        await update.message.reply_text(
            f"⏳ הסורק תפוס כרגע על ידי {owner}. הפקודה ממתינה לסיומו."
        )

    async with command_lock:
        try:
            async with scrape_lock:
                WATCH_RUNTIME["scan_owner"] = "/alerts_liq"
                await update.message.reply_text(
                    "🔎 התחילה סריקה חיה מלאה של 7 טווחי הזמן "
                    f"עם סף נזילות מעל ${minimum_liquidity:,.0f}."
                )
                rows, live_result = await collect_live_rows_for_watch()

            all_items = _build_opportunities_with_regime(rows, limit=500)
            filtered_items = []
            for item in all_items:
                if not _is_displayable_opportunity(item):
                    continue
                total_liquidity = float(item.get("near_amount", 0) or 0) + float(
                    item.get("far_amount", 0) or 0
                )
                if total_liquidity > minimum_liquidity:
                    enriched = dict(item)
                    enriched["total_liquidity"] = total_liquidity
                    filtered_items.append(enriched)

            items = filtered_items[:limit]
            if not items:
                await update.message.reply_text(
                    "⚠️ לא נמצאו הזדמנויות שעוברות גם את תנאי ההתראה "
                    f"וגם סך נזילות מעל ${minimum_liquidity:,.0f}."
                )
                return

            counts = live_result.get("timeframe_integrity", {}).get("counts", {})
            await update.message.reply_text(
                "✅ /alerts_liq הסתיים\n"
                f"מוצגות {len(items)} התוצאות המובילות מעל "
                f"${minimum_liquidity:,.0f} נזילות.\n"
                f"טווחים שנקלטו: {', '.join(f'{tf}:{counts.get(tf, 0)}' for tf in TIMEFRAMES)}"
            )
            for index, item in enumerate(items, start=1):
                await update.message.reply_text(
                    _alert_card(index, item, all_items, rows),
                    parse_mode="HTML",
                )
            await update.message.reply_text(
                alert_summary.format_alert_count_summary(items)
            )

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await update.message.reply_text(
                f"❌ /alerts_liq נכשל: {exc!r}"
            )
        finally:
            if WATCH_RUNTIME.get("scan_owner") == "/alerts_liq":
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

            all_items = _build_opportunities_with_regime(rows, limit=500)
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
                    _alert_card(sent_index, item, all_items, rows),
                    parse_mode="HTML",
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
    all_items = _build_opportunities_with_regime(rows, limit=500)

    matches = [
        item for item in all_items
        if item.get("symbol") == symbol
        and (timeframe is None or item.get("timeframe") == timeframe)
    ]
    if not matches:
        await update.message.reply_text("לא נמצאה כרגע התראה מתאימה.")
        return

    item = sorted(matches, key=lambda x: -x.get("priority", 0))[0]
    await update.message.reply_text(_alert_card(1, item, all_items, rows), parse_mode="HTML")


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


async def run_watch_cycle(bot_app, chat_id: int, top8_only: bool = False) -> Dict[str, Any]:
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

        all_items = _build_opportunities_with_regime(rows, limit=500)
        if top8_only:
            all_items = _filter_top8_items(all_items)
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

        watch_label = "Watch Top 8" if top8_only else "Watch"
        if candidates:
            result_items = candidates[:10]
            header = (
                f"✅ סריקת {watch_label} #{cycle_number} הסתיימה\n"
                f"נמצאו {len(candidates)} תוצאות בציון "
                f"{WATCH_PRIORITY_THRESHOLD:.0f} ומעלה.\n"
                f"מוצגות {len(result_items)} התוצאות המובילות."
            )
        elif displayable_items:
            result_items = [displayable_items[0]]
            header = (
                f"✅ סריקת {watch_label} #{cycle_number} הסתיימה\n"
                f"אין תוצאה בציון {WATCH_PRIORITY_THRESHOLD:.0f} ומעלה.\n"
                "מוצגת התוצאה בעלת הציון הגבוה ביותר."
            )
        else:
            result_items = []
            header = (
                f"⚠️ סריקת {watch_label} #{cycle_number} הסתיימה ללא "
                "הזדמנויות פעילות להצגה."
            )

        await bot_app.bot.send_message(chat_id=chat_id, text=header)
        for index, item in enumerate(result_items, start=1):
            await bot_app.bot.send_message(
                chat_id=chat_id,
                text=_alert_card(index, item, all_items, rows),
                parse_mode="HTML",
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
                    f"❌ סריקת {'Watch Top 8' if top8_only else 'Watch'} #{cycle_number} נכשלה\n"
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


async def watch_loop(bot_app, chat_id: int, top8_only: bool = False):
    """Persistent single Watch loop; only /watch_stop cancels it."""
    global WATCH_SCAN_TASK

    print(
        f"[watch] loop started; chat_id={chat_id}; "
        f"interval={WATCH_INTERVAL_MINUTES}m; mode={'top8' if top8_only else 'all'}",
        flush=True,
    )

    try:
        while True:
            WATCH_RUNTIME["last_cycle_status"] = "starting_cycle"
            WATCH_RUNTIME["next_scan_utc"] = datetime.now(
                timezone.utc
            ).isoformat()

            WATCH_SCAN_TASK = asyncio.create_task(
                run_watch_cycle(bot_app, chat_id, top8_only=top8_only),
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
        WATCH_RUNTIME["mode"] = "all"
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



def _find_symbol_current_price(rows, symbol: str) -> Optional[float]:
    for row in rows:
        if str(row.get("symbol") or "").upper() != symbol:
            continue
        try:
            value = float(row.get("current_price"))
            if value > 0:
                return value
        except (TypeError, ValueError):
            continue
    return None


def _specific_watch_progress_text(watch: Dict[str, Any], current_price: float) -> str:
    target = float(watch["target_price"])
    previous = watch.get("last_price")
    start = float(watch["start_price"])
    current_distance = abs(target - current_price)
    start_distance = abs(target - start)
    remaining_pct = (current_distance / current_price * 100) if current_price else 0.0

    if previous is None:
        movement = "בדיקה ראשונה"
    else:
        previous_distance = abs(target - float(previous))
        if current_distance < previous_distance:
            movement = "מתקרב ליעד"
        elif current_distance > previous_distance:
            movement = "מתרחק מהיעד"
        else:
            movement = "ללא שינוי במרחק מהיעד"

    progress = 0.0
    if start_distance > 0:
        progress = max(0.0, min(100.0, (1 - current_distance / start_distance) * 100))

    return (
        f"\n\n👁 מעקב יעד: ${fmt_price(target)}\n"
        f"מצב: {movement}\n"
        f"מרחק נותר: {fmt(remaining_pct)}%\n"
        f"התקדמות מההפעלה: {fmt(progress)}%"
    )


def _specific_target_reached(watch: Dict[str, Any], current_price: float) -> bool:
    start = float(watch["start_price"])
    target = float(watch["target_price"])
    if target >= start:
        return current_price >= target
    return current_price <= target


def _specific_watch_summary(symbol: str, watch: Dict[str, Any], current_price: float) -> str:
    start = float(watch["start_price"])
    target = float(watch["target_price"])
    change_pct = ((current_price - start) / start * 100) if start else 0.0
    started_at = _parse_utc_setting(watch.get("started_at"))
    elapsed = "-"
    if started_at is not None:
        seconds = max(0, int((datetime.now(timezone.utc) - started_at).total_seconds()))
        elapsed = f"{seconds // 3600} שעות ו-{(seconds % 3600) // 60} דקות"
    return (
        f"🎯 {symbol} הגיע ליעד\n\n"
        f"מחיר בהפעלת הצפייה: ${fmt_price(start)}\n"
        f"מחיר יעד: ${fmt_price(target)}\n"
        f"מחיר נוכחי: ${fmt_price(current_price)}\n"
        f"שינוי מההפעלה: {fmt(change_pct)}%\n"
        f"משך המעקב: {elapsed}\n\n"
        "הצפייה במטבע הופסקה אוטומטית."
    )


async def run_specific_watch_cycle(bot_app, chat_id: int) -> None:
    """Send the normal alert card for every timeframe of each active symbol watch."""
    if not SPECIFIC_WATCHES:
        return

    scrape_lock = _get_scrape_lock()
    async with scrape_lock:
        WATCH_RUNTIME["scan_in_progress"] = True
        WATCH_RUNTIME["scan_owner"] = "Specific Watch"
        try:
            rows, _live_result = await collect_live_rows_for_watch()
        finally:
            WATCH_RUNTIME["scan_in_progress"] = False
            WATCH_RUNTIME["scan_owner"] = None

    all_items = _build_opportunities_with_regime(rows, limit=500)
    timeframe_order = list(getattr(alert_engine, "TIMEFRAMES", [
        "12h", "24h", "48h", "3d", "1w", "2w", "1m"
    ]))
    completed = []

    for symbol in list(SPECIFIC_WATCHES.keys()):
        watch = SPECIFIC_WATCHES.get(symbol)
        if not watch:
            continue

        current_price = _find_symbol_current_price(rows, symbol)
        if current_price is None:
            await bot_app.bot.send_message(
                chat_id=chat_id,
                text=f"⚠️ צפייה ב-{symbol}: לא נמצא מחיר חי בסריקה הנוכחית.",
            )
            continue

        # Select one normal alert per timeframe. If both directions exist for the
        # same timeframe, show the stronger one, exactly as an ordinary alert card.
        symbol_items = [
            item for item in all_items
            if str(item.get("symbol") or "").upper() == symbol
        ]
        best_by_timeframe: Dict[str, Dict[str, Any]] = {}
        for item in symbol_items:
            timeframe = str(item.get("timeframe") or "")
            if timeframe not in timeframe_order:
                continue
            previous = best_by_timeframe.get(timeframe)
            item_score = float(item.get("score", item.get("priority", 0)) or 0)
            previous_score = (
                float(previous.get("score", previous.get("priority", 0)) or 0)
                if previous else float("-inf")
            )
            if previous is None or item_score > previous_score:
                best_by_timeframe[timeframe] = item

        ordered_items = [
            best_by_timeframe[timeframe]
            for timeframe in timeframe_order
            if timeframe in best_by_timeframe
        ]

        target = float(watch["target_price"])
        await bot_app.bot.send_message(
            chat_id=chat_id,
            text=(
                f"✅ צפייה ממוקדת — {symbol}\n"
                f"מחיר נוכחי: ${fmt_price(current_price)} | "
                f"יעד: ${fmt_price(target)}\n"
                f"התראות זמינות: {len(ordered_items)}/{len(timeframe_order)} טווחים"
            ),
        )

        for index, item in enumerate(ordered_items, start=1):
            card = _alert_card(index, item, all_items, rows)
            # Add the compact target status only once, after the final normal card.
            if index == len(ordered_items):
                card += _specific_watch_progress_text(watch, current_price)
            await bot_app.bot.send_message(chat_id=chat_id, text=card, parse_mode="HTML")

        missing_timeframes = [
            timeframe for timeframe in timeframe_order
            if timeframe not in best_by_timeframe
        ]
        if missing_timeframes:
            await bot_app.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"⚠️ {symbol}: לא נבנתה התראת Max Pain מלאה עבור: "
                    + ", ".join(missing_timeframes)
                ),
            )

        watch["last_price"] = current_price
        watch["last_scan_utc"] = datetime.now(timezone.utc).isoformat()

        # The final message is deliberately separate and is sent only after all
        # ordinary timeframe alerts from the target-reaching cycle.
        if _specific_target_reached(watch, current_price):
            await bot_app.bot.send_message(
                chat_id=chat_id,
                text=_specific_watch_summary(symbol, watch, current_price),
            )
            completed.append(symbol)

    for symbol in completed:
        SPECIFIC_WATCHES.pop(symbol, None)


async def specific_watch_loop(bot_app, chat_id: int):
    """One manager loop serves all symbol watches with one shared scan every 5 minutes."""
    global SPECIFIC_WATCH_TASK
    try:
        while SPECIFIC_WATCHES:
            try:
                await run_specific_watch_cycle(bot_app, chat_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"[specific-watch] cycle error: {exc!r}", flush=True)
                try:
                    await bot_app.bot.send_message(
                        chat_id=chat_id,
                        text=(
                            "❌ סריקת הצפייה הספציפית נכשלה. "
                            "הצפיות נשארות פעילות וינוסו שוב בעוד 5 דקות.\n"
                            f"{exc!r}"
                        ),
                    )
                except Exception:
                    pass
            if SPECIFIC_WATCHES:
                await asyncio.sleep(SPECIFIC_WATCH_INTERVAL_MINUTES * 60)
    except asyncio.CancelledError:
        raise
    finally:
        SPECIFIC_WATCH_TASK = None


async def _start_specific_watch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global SPECIFIC_WATCH_TASK
    if len(context.args) != 2:
        await update.message.reply_text(
            "שימוש נכון: /watch_on SOL 160"
        )
        return

    symbol = str(context.args[0]).upper().strip()
    try:
        target = float(str(context.args[1]).replace(",", ""))
    except ValueError:
        await update.message.reply_text("מחיר היעד חייב להיות מספר.")
        return
    if not symbol or target <= 0:
        await update.message.reply_text("יש להזין מטבע ומחיר יעד חיובי.")
        return

    scrape_lock = _get_scrape_lock()
    async with scrape_lock:
        WATCH_RUNTIME["scan_in_progress"] = True
        WATCH_RUNTIME["scan_owner"] = f"Watch setup {symbol}"
        try:
            rows, _ = await collect_live_rows_for_watch()
        finally:
            WATCH_RUNTIME["scan_in_progress"] = False
            WATCH_RUNTIME["scan_owner"] = None

    start_price = _find_symbol_current_price(rows, symbol)
    if start_price is None:
        await update.message.reply_text(
            f"לא נמצא מחיר חי עבור {symbol}; הצפייה לא הופעלה."
        )
        return

    SPECIFIC_WATCHES[symbol] = {
        "symbol": symbol,
        "target_price": target,
        "start_price": start_price,
        "last_price": start_price,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "last_scan_utc": None,
    }

    chat_id = int(update.effective_chat.id)
    if SPECIFIC_WATCH_TASK is None or SPECIFIC_WATCH_TASK.done():
        SPECIFIC_WATCH_TASK = asyncio.create_task(
            specific_watch_loop(context.application, chat_id),
            name="specific-watch-manager",
        )

    direction = "מעלה" if target >= start_price else "מטה"
    await update.message.reply_text(
        f"✅ צפייה ב-{symbol} הופעלה\n"
        f"מחיר התחלה: ${fmt_price(start_price)}\n"
        f"מחיר יעד: ${fmt_price(target)} ({direction})\n"
        "תישלח תמונת מצב כל 5 דקות. בהגעה ליעד תישלח הודעת סיכום."
    )


async def watch_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start general Watch, or /watch_on SYMBOL TARGET for an independent watch."""
    global WATCH_TASK

    if context.args:
        await _start_specific_watch(update, context)
        return

    if WATCH_TASK is not None and not WATCH_TASK.done():
        await update.message.reply_text(
            "👁 Watch כבר פעיל. לא נפתחה לולאה נוספת."
        )
        return

    chat_id = int(update.effective_chat.id)
    WATCH_RUNTIME["last_error"] = None
    WATCH_RUNTIME["last_cycle_status"] = "starting"
    WATCH_RUNTIME["next_scan_utc"] = datetime.now(timezone.utc).isoformat()
    WATCH_RUNTIME["mode"] = "all"

    WATCH_TASK = asyncio.create_task(
        watch_loop(context.application, chat_id, top8_only=False),
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


async def watch_on_top8(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start the persistent Watch loop restricted to the fixed Top-8 list."""
    global WATCH_TASK

    if context.args:
        await update.message.reply_text("שימוש: /watch_on_top8")
        return

    if WATCH_TASK is not None and not WATCH_TASK.done():
        mode = WATCH_RUNTIME.get("mode", "all")
        active_label = "Watch Top 8" if mode == "top8" else "Watch רגיל"
        await update.message.reply_text(
            f"👁 {active_label} כבר פעיל. יש לעצור אותו קודם באמצעות /watch_stop."
        )
        return

    chat_id = int(update.effective_chat.id)
    WATCH_RUNTIME["last_error"] = None
    WATCH_RUNTIME["last_cycle_status"] = "starting"
    WATCH_RUNTIME["next_scan_utc"] = datetime.now(timezone.utc).isoformat()
    WATCH_RUNTIME["mode"] = "top8"

    WATCH_TASK = asyncio.create_task(
        watch_loop(context.application, chat_id, top8_only=True),
        name="persistent-watch-top8-loop",
    )

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
            f"❌ Watch Top 8 לא הצליח להתחיל: {error!r}"
        )
        return

    await update.message.reply_text(
        "✅ Watch Top 8 הופעל\n"
        "המעקב מוגבל ל-BTC, ETH, SOL, HYPE, DOGE, ZEC, BNB ו-XRP.\n"
        "הסריקה הראשונה מתחילה כעת.\n"
        f"לאחר סיום כל סריקה תתחיל סריקה נוספת בעוד "
        f"{WATCH_INTERVAL_MINUTES} דקות.\n"
        "העצירה מתבצעת באמצעות /watch_stop."
    )


async def watch_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Stop general Watch, or only one symbol when /watch_stop SYMBOL is used."""
    global WATCH_TASK, WATCH_SCAN_TASK, SPECIFIC_WATCH_TASK

    if context.args:
        symbol = str(context.args[0]).upper().strip()
        removed = SPECIFIC_WATCHES.pop(symbol, None)
        if not SPECIFIC_WATCHES and SPECIFIC_WATCH_TASK is not None and not SPECIFIC_WATCH_TASK.done():
            SPECIFIC_WATCH_TASK.cancel()
            try:
                await SPECIFIC_WATCH_TASK
            except asyncio.CancelledError:
                pass
            SPECIFIC_WATCH_TASK = None
        await update.message.reply_text(
            f"🛑 הצפייה ב-{symbol} הופסקה."
            if removed else f"לא קיימת צפייה פעילה ב-{symbol}."
        )
        return

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
        f"מועמד מוביל: {top_text}\n"
        f"צפיות מטבע פעילות: {specific_text}"
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


def _latest_active_symbols() -> List[str]:
    """Symbols from the latest saved Max Pain snapshot, excluding known non-crypto rows."""
    rows = query(
        "WITH latest AS (SELECT MAX(collected_at) AS max_time FROM max_pain_snapshots) "
        "SELECT DISTINCT symbol FROM max_pain_snapshots, latest "
        "WHERE collected_at = latest.max_time ORDER BY symbol"
    )
    return [
        str(row["symbol"]).upper()
        for row in rows
        if str(row["symbol"]).upper() not in NON_CRYPTO_SYMBOLS
    ]




def _fmt_hist_stat(value: Any) -> str:
    try:
        if value is None:
            return "-"
        return f"{float(value):.4f}%"
    except (TypeError, ValueError):
        return "-"


async def _run_history_backfill_once(source: str = "automatic", days: int = coinglass_history_backfill.BACKFILL_DAYS) -> Dict[str, Dict[str, Any]]:
    """Run the isolated historical Price+OI refresh without overlapping runs."""
    global HISTORY_BACKFILL_LOCK
    if HISTORY_BACKFILL_LOCK is None:
        HISTORY_BACKFILL_LOCK = asyncio.Lock()
    if HISTORY_BACKFILL_LOCK.locked():
        print(f"[oi-backfill] {source} skipped: another backfill is active", flush=True)
        return {}

    async with HISTORY_BACKFILL_LOCK:
        started = datetime.now(timezone.utc)
        print(f"[oi-backfill] {source} started at {started.isoformat()}", flush=True)
        results = await asyncio.to_thread(coinglass_history_backfill.backfill_all, days)
        ok_count = sum(1 for result in results.values() if result.get("ok"))
        completed = datetime.now(timezone.utc)
        await asyncio.to_thread(
            coinglass_history_backfill.record_backfill_run,
            source,
            ok_count,
            len(coinglass_history_backfill.TARGET_SYMBOLS),
            completed,
        )
        print(
            f"[oi-backfill] {source} completed: "
            f"{ok_count}/{len(coinglass_history_backfill.TARGET_SYMBOLS)} symbols",
            flush=True,
        )
        return results


async def _history_backfill_loop() -> None:
    """Run only when the persisted last completion is at least 24 hours old."""
    if HISTORY_BACKFILL_STARTUP_DELAY_SECONDS:
        await asyncio.sleep(HISTORY_BACKFILL_STARTUP_DELAY_SECONDS)

    due_after = timedelta(hours=HISTORY_BACKFILL_INTERVAL_HOURS)
    check_seconds = HISTORY_BACKFILL_CHECK_INTERVAL_MINUTES * 60
    while True:
        try:
            last = await asyncio.to_thread(coinglass_history_backfill.last_backfill_run)
            now = datetime.now(timezone.utc)
            last_at = None
            if last and last.get("completed_at"):
                value = last["completed_at"]
                if isinstance(value, datetime):
                    last_at = value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
                else:
                    last_at = datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)

            if last_at is None or now - last_at >= due_after:
                await _run_history_backfill_once("automatic_due", coinglass_history_backfill.DAILY_REFRESH_DAYS)
            else:
                remaining = due_after - (now - last_at)
                print(
                    f"[oi-backfill] automatic skipped: last run {last_at.isoformat()}, "
                    f"next due in {remaining}",
                    flush=True,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[oi-backfill] automatic check failed: {exc!r}", flush=True)
        await asyncio.sleep(check_seconds)


async def oi_backfill_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manual, isolated CoinGlass historical Price+OI backfill."""
    global HISTORY_BACKFILL_LOCK
    if HISTORY_BACKFILL_LOCK is None:
        HISTORY_BACKFILL_LOCK = asyncio.Lock()
    if HISTORY_BACKFILL_LOCK.locked():
        await update.message.reply_text("⏳ /oi_backfill כבר פעיל. לא נפתחה הורדה נוספת.")
        return

    days = coinglass_history_backfill.BACKFILL_DAYS
    if context.args:
        try:
            days = int(context.args[0])
        except ValueError:
            await update.message.reply_text("שימוש: /oi_backfill 180 או /oi_backfill 365")
            return
    days = max(1, min(days, coinglass_history_backfill.MAX_BACKFILL_DAYS))
    await update.message.reply_text(
        f"📚 מתחיל Backfill היסטורי של Price + OI ל-{days} יום.\n"
        "מטבעות: BTC, ETH, SOL, HYPE, DOGE, ZEC, BNB, XRP.\n"
        "הפעולה מבודדת ואינה משנה Max Pain או ציונים קיימים."
    )

    try:
        results = await _run_history_backfill_once("manual", days)
        lines = ["✅ OI + Price Historical Backfill הסתיים", ""]
        ok_count = 0
        for symbol in coinglass_history_backfill.TARGET_SYMBOLS:
            result = results.get(symbol) or {}
            if result.get("ok"):
                ok_count += 1
                status = "✅"
            else:
                status = "⚠️"
            lines.append(
                f"{status} {symbol}: Price {result.get('price_rows', 0)} | "
                f"OI {result.get('oi_rows', 0)} | Matched {result.get('matched_rows', 0)}"
            )
            if result.get("price_exchange"):
                lines.append(
                    f"   מחיר: {result.get('price_exchange')} / {result.get('price_pair')}"
                )
            if not result.get("ok"):
                lines.append(f"   {result.get('message', 'לא הושלם')}")
        lines.extend([
            "",
            f"הושלמו בהצלחה: {ok_count}/{len(coinglass_history_backfill.TARGET_SYMBOLS)}",
            "הנתונים נשמרו בטבלה נפרדת oi_price_history בלבד.",
            "כעת אפשר לבדוק למשל: /oi_stats BTC",
        ])
        await update.message.reply_text("\n".join(lines))
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        await update.message.reply_text(f"❌ /oi_backfill נכשל: {exc!r}")


async def oi_stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show the historical Price/OI reference ranges used by live Regime significance."""
    if not context.args:
        await update.message.reply_text(
            "שימוש: /oi_stats BTC\n"
            "מטבעות: BTC ETH SOL HYPE DOGE ZEC BNB XRP"
        )
        return
    symbol = str(context.args[0]).strip().upper()
    if symbol not in coinglass_history_backfill.TARGET_SYMBOLS:
        await update.message.reply_text(
            "המטבע אינו ברשימת ה-Backfill.\n"
            "אפשרויות: " + " ".join(coinglass_history_backfill.TARGET_SYMBOLS)
        )
        return

    stats = await asyncio.to_thread(coinglass_history_backfill.calculate_reference_ranges, symbol)
    if not stats.get("available"):
        await update.message.reply_text(
            f"אין עדיין נתוני Backfill עבור {symbol}. הריצו קודם /oi_backfill."
        )
        return

    lines = [
        f"📊 {symbol} — Price + OI Historical Reference",
        f"דגימות 30m שמורות: {stats.get('rows', 0)}",
        "",
        "P25–P75 = הטווח האמצעי/טיפוסי בהיסטוריה.",
        "P90/P95 = תנועות גדולות יותר היסטורית.",
    ]
    for label in ("30m", "1h", "4h", "12h", "24h", "48h", "72h", "7d"):
        window = (stats.get("windows") or {}).get(label) or {}
        price = window.get("price_abs_change_pct") or {}
        oi = window.get("oi_abs_change_pct") or {}
        lines.extend([
            "",
            f"⏱ {label} — {window.get('samples', 0)} השוואות",
            "Price | "
            f"P25 {_fmt_hist_stat(price.get('p25'))} | "
            f"Median {_fmt_hist_stat(price.get('median'))} | "
            f"P75 {_fmt_hist_stat(price.get('p75'))} | "
            f"P90 {_fmt_hist_stat(price.get('p90'))} | "
            f"P95 {_fmt_hist_stat(price.get('p95'))}",
            "OI    | "
            f"P25 {_fmt_hist_stat(oi.get('p25'))} | "
            f"Median {_fmt_hist_stat(oi.get('median'))} | "
            f"P75 {_fmt_hist_stat(oi.get('p75'))} | "
            f"P90 {_fmt_hist_stat(oi.get('p90'))} | "
            f"P95 {_fmt_hist_stat(oi.get('p95'))}",
        ])
    lines.extend([
        "",
        "ℹ️ ה-Reference הזה משמש לקביעת מינימום/עוצמה ב-Price+OI Regime בלבד. הוא אינו משנה את ציון ה-Max Pain."
    ])
    await update.message.reply_text("\n".join(lines))


def _fmt_signed_pct(value):
    if value is None:
        return "N/A"
    try:
        return f"{float(value):+.4f}%"
    except (TypeError, ValueError):
        return "N/A"


async def oi_state_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display the latest already-computed Price+OI regime; never triggers collection."""
    if not context.args:
        await update.message.reply_text("שימוש: /oi_state BTC")
        return

    symbol = str(context.args[0]).strip().upper()
    regime = await asyncio.to_thread(coinglass_oi_regime_service.latest, symbol)
    windows = regime.get("windows") or {}
    overall = regime.get("overall") or {}

    if not regime.get("available"):
        reason = regime.get("reason") or "אין עדיין מספיק דגימות חיות להשוואה."
        await update.message.reply_text(
            f"📊 {symbol} — Price + OI Regime\n\n"
            f"אין עדיין חישוב זמין.\n{reason}\n\n"
            "הפקודה מציגה נתונים שכבר נאספו ואינה מפעילה איסוף חדש."
        )
        return

    lines = [
        f"📊 {symbol} — Price + OI Regime",
        "הפקודה מציגה את החישוב השמור האחרון; היא אינה אוספת נתונים מחדש.",
    ]
    for label in ("30m", "1h", "4h", "12h", "24h", "48h", "72h", "7d"):
        w = windows.get(label) or {}
        lines.append("")
        if not w.get("available"):
            lines.extend([f"⏱ {label}", "אין עדיין דגימת עבר מספקת לטווח הזה."])
            continue
        ps = w.get("price_strength") or {}
        os_ = w.get("oi_strength") or {}
        p_strength = ps.get("label") or "ללא Reference"
        o_strength = os_.get("label") or "ללא Reference"
        lines.extend([
            f"⏱ {label}",
            f"Price: {_fmt_signed_pct(w.get('price_change_pct'))} — {p_strength}",
            f"OI: {_fmt_signed_pct(w.get('oi_change_pct'))} — {o_strength}",
            f"State: {w.get('label') or w.get('state') or 'לא ידוע'}",
        ])

    lines.extend([
        "",
        f"מסקנה כוללת: {overall.get('label') or 'לא זמינה'}",
        f"עוצמה: {overall.get('strength') or 'לא זמינה'}",
        f"הסכמה: {overall.get('agreement', 0)}/{overall.get('valid_windows', 0) or 8}",
        f"Early Transition: {'כן' if regime.get('early_transition') else 'לא'}",
    ])

    observations = regime.get("significance_observations") or []
    if observations:
        lines.extend(["", "הערות משמעותיות:"] + [f"• {x.get('text')}" for x in observations if x.get('text')])

    await update.message.reply_text("\n".join(lines))



def _fmt_flow_money(value):
    if value is None:
        return "-"
    value=float(value)
    sign="+" if value>0 else ""
    av=abs(value)
    if av>=1_000_000_000:
        return f"{sign}{value/1_000_000_000:.2f}B$"
    if av>=1_000_000:
        return f"{sign}{value/1_000_000:.2f}M$"
    if av>=1_000:
        return f"{sign}{value/1_000:.2f}K$"
    return f"{sign}{value:.2f}$"


def _flow_market_lines(title, data):
    lines=[f"{title}"]
    quality=(data.get("quality") or {})
    lines.append(_flow_snapshot_line(data))
    lines.append(f"Data quality: {quality.get('status','NO DATA')} | rows {quality.get('rows',0)}")
    for reason in quality.get("reasons") or []:
        lines.append(f"Quality reason: {reason}")
    impulse=data.get("current_impulse_30m") or {}
    if impulse:
        lines.append(
            f"30m impulse: {impulse.get('direction','-')} "
            f"{_fmt_flow_money(impulse.get('delta_usd'))} — {impulse.get('magnitude','-')}"
        )
    else:
        lines.append("30m impulse: unavailable")
    groups=data.get("groups") or {}
    for key,label in (("momentum","Momentum 30m/1h"),("trend","Trend 4h/12h/24h"),("structure","Structure 48h/72h/7d")):
        g=groups.get(key) or {}
        lines.append(f"{label}: {g.get('state','NO DATA')}")
    overall=data.get("overall") or {}
    lines.append(f"Overall: {overall.get('state','NO DATA')}")
    early=data.get("early_shift")
    if early:
        lines.append(
            f"⚠️ Early Shift: {early.get('new_direction')} מול "
            f"{early.get('established_direction')} הרחב"
        )
    return lines


async def flow_state_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Read-only Futures and Spot CVD flow analysis; never changes alerts."""
    if not context.args:
        await update.message.reply_text("שימוש: /flow_state BTC")
        return
    symbol=str(context.args[0]).strip().upper()
    try:
        result=await asyncio.to_thread(coinglass_flow_engine.analyze_symbol, symbol)
    except Exception as exc:
        await update.message.reply_text(f"❌ /flow_state נכשל: {exc!r}")
        return
    lines=[
        f"📈 {symbol} — CVD Flow Engine",
        "הניתוח לקריאה בלבד ואינו משנה Alerts, Watch או Score.",
        "",
    ]
    lines.extend(_flow_market_lines("Futures Flow", result.get("futures") or {}))
    lines.append("")
    lines.extend(_flow_market_lines("Spot Flow", result.get("spot") or {}))
    lines.extend([
        "",
        "כלל: Buy/Sell ו-CVD הם משפחת נתונים אחת; 30m impulse אינו נספר כאישור נוסף.",
    ])
    await update.message.reply_text("\n".join(lines))



async def market_state_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show Stage 89 evidence families; optional LONG/SHORT is price direction."""
    if not context.args:
        await update.message.reply_text("שימוש: /market_state BTC [LONG|SHORT]")
        return
    symbol = str(context.args[0]).strip().upper()
    expected = str(context.args[1]).strip().upper() if len(context.args) > 1 else "NEUTRAL"
    if expected not in {"LONG", "SHORT", "NEUTRAL"}:
        await update.message.reply_text("הכיוון חייב להיות LONG או SHORT (כיוון מחיר).")
        return
    try:
        result = await asyncio.to_thread(market_confidence_engine.combine, symbol, expected)
    except Exception as exc:
        await update.message.reply_text(f"❌ /market_state נכשל: {exc!r}")
        return
    confirmation=result.get("confirmation") or {}
    lines = [
        f"🧭 {symbol} — Market Evidence",
        "המדד לקריאה בלבד ואינו משנה Score, Alerts או Watch.",
        f"Expected price direction: {result.get('expected_price_direction')}",
        f"Conclusion: {result.get('classification_label')}",
        f"Core support: {result.get('supporting_families',0)}/2 | Core opposition: {result.get('opposing_families',0)}",
        f"Confirmation: {confirmation.get('label')}",
        "",
    ]
    for key, title in (("positioning", "Price+OI Positioning"), ("futures_flow", "Futures Flow"), ("spot_flow", "Spot Flow")):
        module = (result.get("modules") or {}).get(key) or {}
        lines.extend([
            title,
            f"State: {module.get('label') or module.get('state')}",
            f"Direction: {module.get('direction')} | Relation: {module.get('relation')}",
            f"Weighted time-family score: {float(module.get('score') or 0):+.2f}/100",
            "",
        ])
        for family_key in ("now","short","medium","long"):
            family=(module.get("time_families") or {}).get(family_key) or {}
            if family:
                lines.append(f"  {family.get('label')}: {family.get('direction')} | weight {float(family.get('weight') or 0):.0f}% | quality {float(family.get('quality') or 0)*100:.0f}% | agreement {float(family.get('agreement') or 0)*100:.0f}%")
        lines.append("")
    if expected == "NEUTRAL":
        lines.append("כדי לחשב Alignment לכיוון מסוים: /market_state BTC LONG")
    await update.message.reply_text("\n".join(lines))

async def flow_stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show P25/P50/P75/P90 CVD-change baselines for both markets."""
    if not context.args:
        await update.message.reply_text("שימוש: /flow_stats BTC")
        return
    symbol=str(context.args[0]).strip().upper()
    lines=[f"📊 {symbol} — Historical CVD Baselines"]
    for market,title in (("futures","Futures"),("spot","Spot")):
        try:
            data=await asyncio.to_thread(coinglass_flow_engine.stats, symbol, market)
        except Exception as exc:
            lines.extend(["",f"{title}: ERROR {exc!r}"])
            continue
        lines.extend(["",title])
        for label,_ in coinglass_flow_engine.WINDOWS:
            p=(data.get("windows") or {}).get(label)
            if not p:
                lines.append(f"{label}: No baseline")
                continue
            positive=p.get("positive") or {}
            negative=p.get("negative") or {}
            if positive:
                lines.append(
                    f"{label} Bullish: P25 {_fmt_flow_money(positive['p25'])} | "
                    f"P50 {_fmt_flow_money(positive['p50'])} | "
                    f"P75 {_fmt_flow_money(positive['p75'])} | "
                    f"P90 {_fmt_flow_money(positive['p90'])}"
                )
            else:
                lines.append(f"{label} Bullish: No baseline")
            if negative:
                lines.append(
                    f"{label} Bearish: P25 -{_fmt_flow_money(negative['p25']).lstrip('+')} | "
                    f"P50 -{_fmt_flow_money(negative['p50']).lstrip('+')} | "
                    f"P75 -{_fmt_flow_money(negative['p75']).lstrip('+')} | "
                    f"P90 -{_fmt_flow_money(negative['p90']).lstrip('+')}"
                )
            else:
                lines.append(f"{label} Bearish: No baseline")
    lines.extend(["", "Bullish ו-Bearish מחושבים מול התפלגויות היסטוריות נפרדות.", "P75 = עדות משמעותית; P90 = עדות חזקה."] )
    await update.message.reply_text("\n".join(lines))


async def flow_backfill_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Store official 30m aggregated Futures+Spot Buy/Sell and CVD history."""
    global FLOW_BACKFILL_LOCK
    if FLOW_BACKFILL_LOCK is None:
        FLOW_BACKFILL_LOCK = asyncio.Lock()
    if FLOW_BACKFILL_LOCK.locked():
        await update.message.reply_text("⏳ /flow_backfill כבר פעיל.")
        return
    days = coinglass_flow_foundation.DEFAULT_BACKFILL_DAYS
    if context.args:
        try:
            days = int(context.args[0])
        except ValueError:
            await update.message.reply_text("שימוש: /flow_backfill 180 או /flow_backfill 365")
            return
    days = max(1, min(days, coinglass_flow_foundation.MAX_BACKFILL_DAYS))
    force = any(str(arg).strip().lower() == "force" for arg in context.args[1:])
    await update.message.reply_text(
        f"📥 מתחיל Foundation Backfill ל-{days} יום של Buy/Sell + CVD רשמי.\n"
        "ההורדה מתבצעת בתור יציב עם השהיה, Retry ו-Resume.\n"
        "נתונים שכבר מעודכנים ידולגו אוטומטית. אין השפעה על Alerts או Watch."
        + ("\nמצב FORCE פעיל: גם נתונים מעודכנים ירועננו." if force else "")
    )
    try:
        async with FLOW_BACKFILL_LOCK:
            results = await asyncio.to_thread(coinglass_flow_foundation.backfill_all, days, force)
        lines=["✅ Flow Foundation Backfill הסתיים", ""]
        for symbol in coinglass_flow_foundation.TARGET_SYMBOLS:
            pair=results.get(symbol) or {}
            fut=pair.get("futures") or {}; spot=pair.get("spot") or {}
            def _flow_part(name, data):
                if data.get("skipped"):
                    return f"{name} {data.get('total_rows',0)} ⏭️"
                return f"{name} {data.get('total_rows',data.get('stored_rows',0))} {'✅' if data.get('ok') else '⚠️'}"
            lines.append(
                f"{symbol}: {_flow_part('Futures', fut)} | {_flow_part('Spot', spot)}"
            )
            if not fut.get("ok"): lines.append(f"  Futures: {fut.get('message','לא זמין')}")
            if not spot.get("ok"): lines.append(f"  Spot: {spot.get('message','לא זמין')}")
        lines.extend(["", "הנתונים נשמרו בטבלאות נפרדות בלבד.", "נשמר CVD רשמי של CoinGlass; לא נבנתה מסקנת Flow ולא שונתה לוגיקת המסחר."])
        await update.message.reply_text("\n".join(lines))
    except Exception as exc:
        await update.message.reply_text(f"❌ /flow_backfill נכשל: {exc!r}")


async def oi_validation_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Read-only validation of latest Price/OI timestamps and reference windows."""
    if not context.args:
        await update.message.reply_text("שימוש: /oi_validation BTC")
        return
    symbol=str(context.args[0]).strip().upper()
    regime=await asyncio.to_thread(coinglass_oi_regime_service.latest, symbol)
    if not regime.get("price_fetched_at") and not regime.get("windows"):
        await update.message.reply_text(f"אין נתוני Price+OI שמורים עבור {symbol}.")
        return
    lines=[f"🔎 {symbol} — Price/OI Data Validation"]
    lines.append(f"Price source: {regime.get('price_source','-')}")
    lines.append(f"OI source: {regime.get('oi_source','-')}")
    lines.append(f"Price fetched: {regime.get('price_fetched_at','-')}")
    lines.append(f"OI fetched: {regime.get('oi_fetched_at','-')}")
    gap=regime.get('time_gap_seconds')
    lines.append(f"Time gap: {float(gap):.1f}s" if gap is not None else "Time gap: -")
    lines.append(f"Quality: {regime.get('data_quality_status','-')}")
    lines.append("")
    for label in ("30m","1h","4h","12h","24h"):
        w=(regime.get("windows") or {}).get(label) or {}
        ref=w.get("reference_time")
        offset=w.get("reference_offset_seconds")
        if ref:
            target=w.get("reference_target_time") or "-"
            signed=w.get("reference_signed_offset_seconds")
            side = "אחרי" if signed is not None and float(signed) > 0 else "לפני" if signed is not None and float(signed) < 0 else "בדיוק"
            lines.append(f"{label}: reference ✅")
            lines.append(f"  Target: {target}")
            lines.append(f"  Chosen: {ref}")
            lines.append(f"  Offset: {float(offset or 0):.0f}s ({side} היעד)")
        else:
            lines.append(f"{label}: reference ❌ / No Data")
    lines.append("Reference tolerance: עד 20 דקות מהחלון המבוקש.")
    await update.message.reply_text("\n".join(lines))


async def _collect_oi_regime_once() -> Dict[str, Dict[str, Any]]:
    # HYPE can be absent from the latest Max Pain snapshot even though it is a
    # supported Price+OI asset. Keep it in the collector explicitly so the
    # dedicated live-price fallbacks and CoinGlass OI logic can run.
    symbols = sorted(set(_latest_active_symbols()) | {"HYPE"})
    if not symbols:
        print("[oi-regime] skipped: no saved crypto symbols", flush=True)
        return {}

    price_result = await asyncio.to_thread(
        live_price_provider.fetch_binance_usdt_prices,
        symbols,
    )
    prices = price_result.get("prices") or {}
    usable_prices = {
        symbol: dict(prices[symbol])
        for symbol in symbols
        if isinstance(prices.get(symbol), dict)
        and prices[symbol].get("price") is not None
    }
    if not usable_prices:
        print("[oi-regime] skipped: no live prices available", flush=True)
        return {}

    results = await asyncio.to_thread(
        coinglass_oi_regime_service.collect_many,
        usable_prices,
    )
    available = sum(1 for value in results.values() if value.get("available"))
    print(
        f"[oi-regime] collected={len(results)} classified={available} "
        f"interval={coinglass_oi_regime_service.COLLECTION_INTERVAL_MINUTES}m",
        flush=True,
    )
    return results


async def _oi_regime_loop() -> None:
    interval_seconds = coinglass_oi_regime_service.COLLECTION_INTERVAL_MINUTES * 60
    while True:
        try:
            await _collect_oi_regime_once()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Fail-safe: CoinGlass/API/DB failure never stops Telegram alerts.
            print(f"[oi-regime] collection failed: {exc!r}", flush=True)
        await asyncio.sleep(interval_seconds)


async def _collect_flow_once() -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Refresh official 30m Futures and Spot CVD rows for all target symbols.

    The collector is independent of Max-Pain DOM scans. Database primary keys
    prevent duplicate candles, while one-candle overlap refreshes the boundary.
    """
    global FLOW_BACKFILL_LOCK
    if FLOW_BACKFILL_LOCK is None:
        FLOW_BACKFILL_LOCK = asyncio.Lock()
    if FLOW_BACKFILL_LOCK.locked():
        print("[flow-live] skipped: flow backfill/refresh already running", flush=True)
        return {}
    started = datetime.now(timezone.utc)
    print(f"[flow-live] refresh started at {started.isoformat()}", flush=True)
    async with FLOW_BACKFILL_LOCK:
        results = await asyncio.to_thread(
            coinglass_flow_foundation.backfill_all,
            coinglass_flow_foundation.DEFAULT_BACKFILL_DAYS,
            False,
        )
    ok_count = 0
    for symbol in coinglass_flow_foundation.TARGET_SYMBOLS:
        pair = results.get(symbol) or {}
        parts = []
        for market in ("futures", "spot"):
            data = pair.get(market) or {}
            coverage = await asyncio.to_thread(coinglass_flow_foundation.coverage, symbol, market)
            latest = coverage.get("max_time")
            latest_text = latest.isoformat() if latest else "none"
            ok = bool(data.get("ok"))
            ok_count += int(ok)
            action = "skipped-current" if data.get("skipped") else f"received={data.get('received_rows', 0)}"
            parts.append(f"{market}:{'ok' if ok else 'warning'} {action} latest={latest_text}")
        print(f"[flow-live] {symbol} | " + " | ".join(parts), flush=True)
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    print(f"[flow-live] refresh finished ok={ok_count}/16 duration={elapsed:.1f}s", flush=True)
    return results


async def _flow_collection_loop() -> None:
    interval_seconds = coinglass_flow_foundation.FLOW_COLLECTION_INTERVAL_MINUTES * 60
    while True:
        try:
            await _collect_flow_once()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[flow-live] refresh failed: {exc!r}", flush=True)
        await asyncio.sleep(interval_seconds)


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

    global WATCH_TASK, WATCH_SCAN_TASK, OI_REGIME_TASK, HISTORY_BACKFILL_TASK, FLOW_COLLECTION_TASK
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
    bot_app.add_handler(CommandHandler("alerts_top8", alert_check_top8))
    bot_app.add_handler(CommandHandler("alerts_liq", alert_check_min_liquidity))
    bot_app.add_handler(CommandHandler("alert", alert_coin))
    bot_app.add_handler(CommandHandler("debug", debug_coin))
    bot_app.add_handler(CommandHandler("watch_on", watch_on))
    bot_app.add_handler(CommandHandler("watch_on_top8", watch_on_top8))
    bot_app.add_handler(CommandHandler("watch_status", watch_status))
    bot_app.add_handler(CommandHandler("watch_stop", watch_off))
    bot_app.add_handler(CommandHandler("technical_status", technical_status_cmd))
    bot_app.add_handler(CommandHandler("oi_backfill", oi_backfill_cmd))
    bot_app.add_handler(CommandHandler("flow_backfill", flow_backfill_cmd))
    bot_app.add_handler(CommandHandler("flow_state", flow_state_cmd))
    bot_app.add_handler(CommandHandler("flow_stats", flow_stats_cmd))
    bot_app.add_handler(CommandHandler("oi_validation", oi_validation_cmd))
    bot_app.add_handler(CommandHandler("oi_stats", oi_stats_cmd))
    bot_app.add_handler(CommandHandler("oi_state", oi_state_cmd))
    bot_app.add_handler(CommandHandler("oi_regime", oi_state_cmd))
    bot_app.add_error_handler(telegram_error_handler)

    await bot_app.initialize()
    await bot_app.start()

    print(
        "[startup] manual-only trading mode; no Max-Pain alert or Watch scan started automatically",
        flush=True,
    )

    # Stage 77 is data collection only; it does not start alerts or Watch.
    # The API key is read from Render. No additional environment variables are required.
    if os.getenv("COINGLASS_API_KEY", "").strip():
        coinglass_oi_regime_service.init_db()
        OI_REGIME_TASK = asyncio.create_task(_oi_regime_loop())
        HISTORY_BACKFILL_TASK = asyncio.create_task(_history_backfill_loop())
        FLOW_COLLECTION_TASK = asyncio.create_task(_flow_collection_loop())
        print("[startup] Price+OI collector enabled (30m)", flush=True)
        print(
            f"[startup] Futures+Spot CVD collector enabled "
            f"({coinglass_flow_foundation.FLOW_COLLECTION_INTERVAL_MINUTES}m; "
            f"freshness tolerance {coinglass_flow_foundation.FRESHNESS_TOLERANCE_MINUTES}m)",
            flush=True,
        )
        print(
            f"[startup] Historical Price+OI backfill freshness check enabled "
            f"(due after {HISTORY_BACKFILL_INTERVAL_HOURS}h; check every "
            f"{HISTORY_BACKFILL_CHECK_INTERVAL_MINUTES}m; startup check after "
            f"{HISTORY_BACKFILL_STARTUP_DELAY_SECONDS}s)",
            flush=True,
        )
    else:
        OI_REGIME_TASK = None
        HISTORY_BACKFILL_TASK = None
        FLOW_COLLECTION_TASK = None
        print("[startup] Price+OI and CVD collectors disabled: COINGLASS_API_KEY missing", flush=True)

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

        if OI_REGIME_TASK is not None and not OI_REGIME_TASK.done():
            OI_REGIME_TASK.cancel()
            try:
                await OI_REGIME_TASK
            except asyncio.CancelledError:
                pass

        if HISTORY_BACKFILL_TASK is not None and not HISTORY_BACKFILL_TASK.done():
            HISTORY_BACKFILL_TASK.cancel()
            try:
                await HISTORY_BACKFILL_TASK
            except asyncio.CancelledError:
                pass

        if FLOW_COLLECTION_TASK is not None and not FLOW_COLLECTION_TASK.done():
            FLOW_COLLECTION_TASK.cancel()
            try:
                await FLOW_COLLECTION_TASK
            except asyncio.CancelledError:
                pass

        await bot_app.bot.delete_webhook(drop_pending_updates=False)
        await bot_app.stop()
        await bot_app.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
