import asyncio
import base64
import html
import json
import os
import re
import sqlite3
import time
import zlib
from datetime import datetime, timezone
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
WATCH_INTERVAL_MINUTES = int(os.getenv("WATCH_INTERVAL_MINUTES", "15"))
WATCH_PRIORITY_THRESHOLD = float(os.getenv("WATCH_PRIORITY_THRESHOLD", "70"))
WATCH_COOLDOWN_MINUTES = int(os.getenv("WATCH_COOLDOWN_MINUTES", "60"))
WATCH_RUNTIME = {
    "last_scan_utc": None,
    "next_scan_utc": None,
    "last_found": 0,
    "last_candidates": 0,
    "last_sent": 0,
    "last_error": None,
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
"""

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
"""

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


async def collect_once():
    """Collect CoinGlass Max Pain targets, attach Binance live prices, and save one coherent snapshot.

    CoinGlass supplies:
    - Short/Long Max Pain targets
    - Short/Long liquidation amounts

    Binance supplies:
    - current_price
    - all distance calculations

    Rows without a Binance price are not saved, so later commands never mix
    stale CoinGlass prices with Binance-based calculations.
    """
    start = time.time()
    collected_dt = datetime.now(timezone.utc)
    collected_at = collected_dt if use_postgres() else collected_dt.isoformat()

    print(f"[collector] starting DOM collection at {collected_at}")

    snapshot = await collect_coinglass_dom_snapshot(
        timeframes=TIMEFRAMES,
        headless=True,
        url=COINGLASS_MAX_PAIN_URL,
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
            # Temporary CoinGlass price; replaced by Binance before DB insertion.
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

    # Fetch Binance once for all symbols, then overlay one consistent price
    # on all seven timeframes of each symbol.
    live_result = live_price_provider.enrich_snapshot_rows(
        raw_rows,
        excluded_symbols=NON_CRYPTO_SYMBOLS,
    )
    rows = live_result.get("rows", [])
    skipped_symbols = live_result.get("skipped_symbols", [])
    price_result = live_result.get("price_result", {})

    # Restore DB metadata fields after the provider copied/enriched rows.
    elapsed = time.time() - start
    for row in rows:
        row["collected_at"] = collected_at
        row["source"] = SOURCE_NAME + "_dom_binance"
        row["collector_version"] = COLLECTOR_VERSION
        row["scrape_duration_seconds"] = elapsed
        row["is_valid"] = True if use_postgres() else 1
        row["validation_errors"] = None

    missing_timeframes = list(snapshot.get("missing_timeframes", []))
    seen_timeframes = {r.get("timeframe") for r in rows}
    for tf in TIMEFRAMES:
        if tf not in seen_timeframes and tf not in missing_timeframes:
            missing_timeframes.append(tf)

    rows = validate_snapshot(rows)
    rows = enrich_rows(rows)
    inserted = insert_snapshots(rows)

    print(
        f"[collector] DOM+Binance inserted {inserted} rows; "
        f"binance_found={price_result.get('found_count', 0)}; "
        f"binance_missing={price_result.get('missing_count', 0)}; "
        f"skipped_symbols={skipped_symbols}; "
        f"missing_timeframes={missing_timeframes}; "
        f"market_only_rows_seen={market_only_count}; "
        f"raw_rows_seen={snapshot.get('row_count', 0)}"
    )

    if inserted == 0:
        print(
            "[collector] no rows inserted. Check DOM parsing and Binance coverage; "
            "CoinGlass prices were not used as fallback."
        )

    return inserted, missing_timeframes



async def collect_live_rows_for_watch():
    """Read fresh CoinGlass + Binance data in memory without saving a snapshot."""
    print("[watch] opening fresh CoinGlass snapshot", flush=True)

    snapshot = await collect_coinglass_dom_snapshot(
        timeframes=TIMEFRAMES,
        headless=True,
        url=COINGLASS_MAX_PAIN_URL,
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
    print(
        f"[watch] fresh rows={len(rows)}; "
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
        "פקודות שימוש יומיומיות:\n"
        "/collect - איסוף נתונים חדש\n"
        "/alerts - הצגת הזדמנויות מדורגות\n"
        "/coin BTC - פירוט מלא למטבע\n"
        "/watch_on - הפעלת התראות אוטומטיות\n"
        "/watch_status - מצב מערכת ההתראות\n"
        "/watch_stop - עצירת התראות אוטומטיות"
    )



def _get_scrape_lock():
    """Shared lock for any CoinGlass/Binance scraping."""
    global SCRAPE_LOCK
    if SCRAPE_LOCK is None:
        import asyncio
        SCRAPE_LOCK = asyncio.Lock()
    return SCRAPE_LOCK

async def collect_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global COLLECT_LOCK

    if COLLECT_LOCK is None:
        COLLECT_LOCK = asyncio.Lock()

    try:
        scrape_lock = _get_scrape_lock()
    except Exception as exc:
        await update.message.reply_text(f"❌ Failed to initialize scrape lock: {exc}")
        return

    if COLLECT_LOCK.locked() or scrape_lock.locked():
        await update.message.reply_text(
            "איסוף או סריקת Watch כבר פועלים כרגע. יש להמתין לסיום."
        )
        return

    async with COLLECT_LOCK:
        async with scrape_lock:
            await update.message.reply_text(
                "מתחיל איסוף: יעדי Max Pain מ-CoinGlass ומחירים חיים מ-Binance. "
                "זה יכול לקחת כמה דקות..."
            )
            try:
                inserted, missing_timeframes = await collect_once()
                missing_note = (
                    f"\nטווחים חסרים: {', '.join(missing_timeframes)}"
                    if missing_timeframes else ""
                )

                await update.message.reply_text(
                    f"✅ האיסוף הסתיים\n\n"
                    f"שורות שנשמרו: {inserted}\n"
                    f"המרחקים חושבו מחדש לפי מחיר Binance."
                    f"{missing_note}\n\n"
                    "פקודות שימוש יומיומיות:\n"
                    "/collect - איסוף נתונים חדש\n"
                    "/alerts - הצגת הזדמנויות מדורגות\n"
                    "/coin BTC - פירוט מלא למטבע\n"
                    "/watch_on - הפעלת התראות אוטומטיות\n"
                    "/watch_status - מצב מערכת ההתראות\n"
                    "/watch_stop - עצירת התראות אוטומטיות"
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                await update.message.reply_text(f"שגיאה באיסוף: {e}")



async def latest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = query("SELECT MAX(collected_at) AS latest_time, COUNT(*) AS rows_count FROM max_pain_snapshots")
    r = rows[0]
    await update.message.reply_text(f"Snapshot אחרון: {r['latest_time']}\\nמספר שורות: {r['rows_count']}")

async def coin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("שימוש: /coin BTC")
        return

    symbol = context.args[0].upper()
    rows = [
        r for r in latest_snapshot_rows()
        if str(r["symbol"]).upper() == symbol
    ]

    if not rows:
        await update.message.reply_text(
            f"לא נמצאו נתוני Binance חיים עבור {symbol}, או שהמטבע אינו קיים ב-snapshot האחרון."
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
    fetched = rows[0].get("price_fetched_at_utc", "-")
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


def _other_alerts_block(item: Dict[str, Any], all_items: List[Dict[str, Any]]) -> str:
    others = [
        other for other in all_items
        if other.get("symbol") == item.get("symbol")
        and other.get("timeframe") != item.get("timeframe")
    ]
    if not others:
        return ""

    same_side = sorted(
        {
            str(other.get("timeframe"))
            for other in others
            if other.get("side") == item.get("side")
            and other.get("timeframe")
            and float(other.get("priority") or 0) > 50.0
        },
        key=tf_order_value,
    )
    opposite = sorted(
        {
            (str(other.get("timeframe")), str(other.get("side")))
            for other in others
            if other.get("side")
            and other.get("side") != item.get("side")
            and other.get("timeframe")
        },
        key=lambda pair: tf_order_value(pair[0]),
    )

    lines = ["🔔 מטבע עם כמה התראות"]
    if same_side:
        lines.append("טווחים נוספים באותו כיוון: " + ", ".join(same_side))
    if opposite:
        lines.append(
            "⚠️ התראות בכיוון הפוך: "
            + ", ".join(f"{tf} {side}" for tf, side in opposite)
        )
    return "\n\n" + "\n".join(lines)


def _alert_card(index: int, item: Dict[str, Any], all_items, rows) -> str:
    c = item.get("components", {})
    types = item.get("types", [])
    if types:
        type_prefix = "🟢 " if len(types) > 1 else ""
        types_text = "\n".join(f"{type_prefix}• {t}" for t in types)
    else:
        types_text = "• ללא סוג חריגה"

    near_share = item.get("near_share_pct")
    if near_share is None:
        balance_text = "⚪ Liquidity Balance: אין נתון"
    elif float(near_share) >= 60.0:
        balance_text = (
            "🟢 Liquidity Balance תומך בצד ה-Max Pain הקרוב: "
            f"{fmt(near_share)}%"
        )
    elif float(near_share) <= 40.0:
        balance_text = (
            "🔴 Liquidity Balance מנוגד לצד ה-Max Pain הקרוב: "
            f"{fmt(near_share)}%"
        )
    else:
        balance_text = f"⚪ Liquidity Balance ניטרלי: {fmt(near_share)}%"

    btc_like_line = (
        "BTC Like: לא נכלל בניקוד של BTC\n"
        if item.get("symbol") == "BTC"
        else (
            f"BTC Like: {item.get('btc_like_hits', 0)}/"
            f"{item.get('btc_like_total', 0)}\n"
        )
    )

    btc_like_score_line = ""
    if float(c.get("btc_like_max", 5) or 0) > 0:
        btc_like_score_line = (
            f"  - BTC Like: {fmt(c.get('btc_like'))}/"
            f"{fmt(c.get('btc_like_max', 5))}\n"
        )

    card = (
        f"🚨 #{index} — {item['symbol']} / {item['timeframe']}\n"
        f"צד קרוב: {item['side']}\n"
        f"Priority: {fmt(item.get('priority'))}/100\n"
        f"Raw Score: {fmt(item.get('raw_score'))}/{fmt(item.get('raw_max_score'))}\n"
        f"מרחק: {fmt(item.get('distance_pct'))}%\n"
        f"קונצנזוס: {item.get('consensus_hits', 0)}/{item.get('consensus_total', 0)}\n"
        + btc_like_line
        + f"Market Schema: {fmt(item.get('market_support_pct'))}% "
        f"תמיכה ב-{item['side']} "
        f"({item.get('market_support_count', 0)}/{item.get('market_total_count', 0)})\n"
        f"Target Cluster: {fmt(item.get('cluster_spread_pct'))}% "
        f"({item.get('cluster_count', 0)} טווחים)\n"
        f"נזילות בצד הקרוב: ${fmt(item.get('near_amount'), 0)}\n"
        f"נזילות בצד השני: ${fmt(item.get('far_amount'), 0)}\n"
        f"Near Share: {fmt(item.get('near_share_pct'))}%\n"
        f"Adjusted Near Liquidity Ratio: {fmt(item.get('adjusted_near_liquidity_ratio'))}x\n"
        "\n"
        "פירוט הניקוד:\n"
        f"• קרבה ל-Max Pain: {fmt(c.get('proximity'))}/30\n"
        f"• Directional Alignment: {fmt(c.get('directional_alignment'))}/20\n"
        f"  - Consensus: {fmt(c.get('consensus'))}/{fmt(c.get('consensus_max', 12))}\n"
        + btc_like_score_line
        + f"  - Market Schema: {fmt(c.get('market'))}/{fmt(c.get('market_max', 3))}\n"
        f"• Target Clustering: {fmt(c.get('target_clustering'))}/20\n"
        f"• High Liquidity Close Distance: "
        f"{fmt(c.get('high_liquidity_close_distance'))}/30\n\n"
        f"סוגי חריגה:\n{types_text}\n\n"
        f"{balance_text}"
    )
    card += _other_alerts_block(item, all_items)
    card += _quality_block(item, rows)
    return card



async def alert_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    limit = 10
    if context.args:
        try:
            limit = max(1, min(25, int(context.args[0])))
        except Exception:
            limit = 10

    rows = latest_snapshot_rows()
    if not rows:
        await update.message.reply_text("אין נתונים שמורים. הריצו /collect קודם.")
        return

    all_items = alert_engine.build_opportunities(rows, limit=500)
    items = all_items[:limit]
    if not items:
        await update.message.reply_text("לא נמצאו הזדמנויות לפי הספים הנוכחיים.")
        return

    await update.message.reply_text(
        "📊 הזדמנויות מדורגות\n"
        "איכות הנתונים וריבוי התראות אינם משפיעים על הציון.\n"
        "כל התראה נשארת נפרדת לפי מטבע וטווח זמן."
    )

    for index, item in enumerate(items, start=1):
        await update.message.reply_text(_alert_card(index, item, all_items, rows))


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


async def run_watch_cycle(bot_app, force_send: bool = False) -> Dict[str, Any]:
    """Run one in-memory scan and send only new alerts above the threshold."""
    chat_id = get_setting("watch_chat_id")
    if not chat_id:
        return {"ok": False, "reason": "no_chat_id", "sent": 0, "found": 0}

    WATCH_RUNTIME["last_scan_utc"] = datetime.now(timezone.utc).isoformat()
    WATCH_RUNTIME["last_error"] = None

    rows, live_result = await collect_live_rows_for_watch()
    items = alert_engine.build_opportunities(rows, limit=500)
    candidates = [
        item for item in items
        if item["priority"] >= WATCH_PRIORITY_THRESHOLD
    ]

    sent = 0
    for item in candidates:
        fingerprint = _alert_fingerprint(item)
        if not force_send and _alert_recently_sent(fingerprint):
            continue

        await bot_app.bot.send_message(
            chat_id=int(chat_id),
            text=_watch_message(item, items, rows),
        )
        _remember_alert(item, fingerprint)
        sent += 1

    WATCH_RUNTIME["last_found"] = len(items)
    WATCH_RUNTIME["last_candidates"] = len(candidates)
    WATCH_RUNTIME["last_sent"] = sent

    print(
        f"[watch] cycle done; opportunities={len(items)}; "
        f"candidates={len(candidates)}; sent={sent}",
        flush=True,
    )

    return {
        "ok": True,
        "found": len(items),
        "candidates": len(candidates),
        "sent": sent,
        "skipped_symbols": live_result.get("skipped_symbols", []),
    }


async def watch_loop(bot_app):
    """Manager loop. Never activates Watch without explicit /watch_on."""
    global WATCH_SCAN_TASK
    interval_seconds = max(1, WATCH_INTERVAL_MINUTES) * 60
    next_run = None
    print(f"[watch] manager ready; interval={WATCH_INTERVAL_MINUTES}m; threshold={WATCH_PRIORITY_THRESHOLD}; startup_state=OFF", flush=True)
    while True:
        try:
            if not watch_enabled() or WATCH_RUNTIME.get("activation_source") != "manual":
                WATCH_RUNTIME["next_scan_utc"] = None
                next_run = None
                await asyncio.sleep(2)
                continue
            now = datetime.now(timezone.utc)
            if next_run is None:
                next_run = now
            if now < next_run:
                WATCH_RUNTIME["next_scan_utc"] = next_run.isoformat()
                await asyncio.sleep(min(2, max(0.1, (next_run-now).total_seconds())))
                continue
            if WATCH_SCAN_TASK and not WATCH_SCAN_TASK.done():
                await asyncio.sleep(2)
                continue
            WATCH_RUNTIME["next_scan_utc"] = next_run.isoformat()
            print("[watch] scan started", flush=True)
            WATCH_SCAN_TASK = asyncio.create_task(run_watch_cycle(bot_app))
            try:
                await WATCH_SCAN_TASK
            finally:
                WATCH_SCAN_TASK = None
            next_run = datetime.fromtimestamp(next_run.timestamp()+interval_seconds,tz=timezone.utc)
            while next_run <= datetime.now(timezone.utc):
                next_run = datetime.fromtimestamp(next_run.timestamp()+interval_seconds,tz=timezone.utc)
            WATCH_RUNTIME["next_scan_utc"] = next_run.isoformat()
            print(f"[watch] next scan {_format_watch_time(WATCH_RUNTIME['next_scan_utc'])} Israel time", flush=True)
        except asyncio.CancelledError:
            if WATCH_SCAN_TASK and not WATCH_SCAN_TASK.done():
                WATCH_SCAN_TASK.cancel()
                try:
                    await WATCH_SCAN_TASK
                except asyncio.CancelledError:
                    pass
            raise
        except Exception as exc:
            WATCH_RUNTIME["last_error"] = repr(exc)
            WATCH_RUNTIME["last_cycle_status"] = "failed"
            print(f"[watch] manager error: {exc!r}", flush=True)
            await asyncio.sleep(2)


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
    if watch_enabled() and WATCH_RUNTIME.get("activation_source") == "manual":
        await update.message.reply_text("👁 הצפייה כבר פעילה. לא נפתחה משימה נוספת.")
        return
    set_setting("watch_chat_id", str(update.effective_chat.id))
    set_setting("watch_enabled", "1")
    WATCH_RUNTIME["activation_source"] = "manual"
    WATCH_RUNTIME["next_scan_utc"] = datetime.now(timezone.utc).isoformat()
    WATCH_RUNTIME["last_error"] = None
    await update.message.reply_text(
        "✅ הצפייה הופעלה ידנית\n\n"
        "הסריקה הראשונה תתחיל כעת.\n"
        f"לאחר מכן תתבצע סריקה בכל {WATCH_INTERVAL_MINUTES} דקות.\n"
        f"סף Priority: {WATCH_PRIORITY_THRESHOLD:.0f}\n"
        "בסיום כל סריקה תישלח הודעת סיכום בטלגרם."
    )


async def watch_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global WATCH_SCAN_TASK
    set_setting("watch_enabled", "0")
    WATCH_RUNTIME["activation_source"] = None
    WATCH_RUNTIME["next_scan_utc"] = None
    cancelled=False
    if WATCH_SCAN_TASK and not WATCH_SCAN_TASK.done():
        WATCH_SCAN_TASK.cancel()
        try:
            await asyncio.wait_for(WATCH_SCAN_TASK, timeout=15)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
        finally:
            WATCH_SCAN_TASK=None
        cancelled=True
    WATCH_RUNTIME["scan_in_progress"] = False
    WATCH_RUNTIME["last_cycle_status"] = "stopped"
    await update.message.reply_text("🛑 הצפייה הופסקה והסריקה הפעילה בוטלה." if cancelled else "🛑 הצפייה הופסקה.")


async def watch_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = "פעיל" if watch_enabled() and WATCH_RUNTIME.get("activation_source") == "manual" else "כבוי"
    active_scan = "כן" if WATCH_RUNTIME.get("scan_in_progress") else "לא"
    top_score=WATCH_RUNTIME.get("top_score")
    top_line = "המועמד המוביל: -" if top_score is None else f"המועמד המוביל: {WATCH_RUNTIME.get('top_symbol')} / {WATCH_RUNTIME.get('top_timeframe')} ({fmt(top_score)}/100)"
    missing=WATCH_RUNTIME.get("last_missing_timeframes") or []
    missing_line = "טווחים חסרים במחזור האחרון: " + ", ".join(missing) if missing else "טווחים חסרים במחזור האחרון: אין"
    await update.message.reply_text(
        f"👁 Watch: {state}\n\n"
        f"הופעל ידנית בסשן הנוכחי: {'כן' if WATCH_RUNTIME.get('activation_source') == 'manual' else 'לא'}\n"
        f"סריקה פעילה כרגע: {active_scan}\n"
        f"סטטוס המחזור האחרון: {WATCH_RUNTIME.get('last_cycle_status') or '-'}\n"
        f"בדיקה: כל {WATCH_INTERVAL_MINUTES} דקות\n"
        f"Priority מינימלי: {WATCH_PRIORITY_THRESHOLD:.0f}\n"
        f"סריקה אחרונה — שעון ישראל: {_format_watch_time(WATCH_RUNTIME.get('last_scan_utc'))}\n"
        f"סריקה הבאה — שעון ישראל: {_format_watch_time(WATCH_RUNTIME.get('next_scan_utc'))}\n"
        f"הזדמנויות בסריקה האחרונה: {WATCH_RUNTIME.get('last_found',0)}\n"
        f"מעל הסף: {WATCH_RUNTIME.get('last_candidates',0)}\n"
        f"התראות שנשלחו: {WATCH_RUNTIME.get('last_sent',0)}\n"
        f"{top_line}\n{missing_line}"
        + (f"\nשגיאה אחרונה: {WATCH_RUNTIME['last_error']}" if WATCH_RUNTIME.get('last_error') else "")
    )


async def watch_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    set_setting("watch_chat_id", str(update.effective_chat.id))
    await update.message.reply_text(
        "🔎 מריץ בדיקת Watch חד-פעמית ללא שמירת Snapshot..."
    )
    try:
        result = await run_watch_cycle(context.application)
        await update.message.reply_text(
            "✅ בדיקת Watch הסתיימה\n"
            f"הזדמנויות שנמצאו: {result.get('found', 0)}\n"
            f"מעל הסף: {result.get('candidates', 0)}\n"
            f"התראות חדשות שנשלחו: {result.get('sent', 0)}"
        )
    except Exception as exc:
        WATCH_RUNTIME["last_error"] = repr(exc)
        await update.message.reply_text(f"❌ שגיאה בבדיקת Watch: {exc!r}")


async def alerts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Alerts are disabled until we define a meaningful historical comparison.
    await update.message.reply_text("אין חריגות כרגע. מנגנון חריגות היסטורי יוגדר רק אחרי שנייצב את תצוגת הנתונים.")


async def health(request):
    return web.json_response({"status": "ok", "service": "crypto-intelligence-v1"})

async def telegram_webhook(request):
    bot_app = request.app["bot_app"]
    try:
        payload = await request.json()
        update = Update.de_json(payload, bot_app.bot)
        await bot_app.process_update(update)
        return web.json_response({"ok": True})
    except Exception as e:
        print(f"[webhook] error: {e}")
        return web.json_response({"ok": False, "error": str(e)}, status=500)

async def start_web_server(bot_app):
    app = web.Application()
    app["bot_app"] = bot_app
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    app.router.add_post("/telegram", telegram_webhook)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"[health] server running on port {PORT}")

async def scheduled_collection():
    try:
        await collect_once()
    except Exception as e:
        print(f"[collector] scheduled collection failed: {e}")

async def main():
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN environment variable")
    if not PUBLIC_URL:
        raise RuntimeError("Missing PUBLIC_URL environment variable. Example: https://crypto-intelligence-platform-1.onrender.com")

    init_db()
    set_setting("watch_enabled", "0")
    WATCH_RUNTIME["activation_source"] = None
    WATCH_RUNTIME["next_scan_utc"] = None
    WATCH_RUNTIME["scan_in_progress"] = False
    print("[watch] startup state forced OFF; waiting for /watch_on", flush=True)

    # Automatic scheduled collection is disabled for the trial phase.
    # Data collection now runs only when you send /collect in Telegram.

    bot_app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    bot_app.add_handler(CommandHandler("start", start))
    bot_app.add_handler(CommandHandler("help", start))
    bot_app.add_handler(CommandHandler("collect", collect_cmd))
    bot_app.add_handler(CommandHandler("coin", coin))
    bot_app.add_handler(CommandHandler("alerts", alert_check))
    bot_app.add_handler(CommandHandler("watch_on", watch_on))
    bot_app.add_handler(CommandHandler("watch_stop", watch_off))
    bot_app.add_handler(CommandHandler("watch_status", watch_status))

    await bot_app.initialize()
    await bot_app.start()

    global WATCH_TASK
    WATCH_TASK = asyncio.create_task(watch_loop(bot_app))

    webhook_url = f"{PUBLIC_URL}/telegram"
    await bot_app.bot.delete_webhook(drop_pending_updates=True)
    await bot_app.bot.set_webhook(url=webhook_url, drop_pending_updates=True)
    print(f"[bot] webhook set to {webhook_url}")

    await start_web_server(bot_app)

    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        if WATCH_TASK:
            WATCH_TASK.cancel()
            try:
                await WATCH_TASK
            except asyncio.CancelledError:
                pass
        await bot_app.bot.delete_webhook(drop_pending_updates=True)
        await bot_app.stop()
        await bot_app.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
