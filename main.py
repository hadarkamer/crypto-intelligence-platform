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
import hyperliquid_reader

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
NON_CRYPTO_SYMBOLS = {"CL", "SPCX", "XAG", "PAXG", "XAU", "MU", "XAUT", "NVDA", "SOXL", "MRVL", "SKHYNIX", "MSFT", "AAPL", "TSLA", "GOOGL", "AMZN", "META", "COIN", "MSTR"}
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
    global_errors = []
    expected_min = TOP_COINS_LIMIT * len(TIMEFRAMES) * 0.8
    if len(rows) < expected_min:
        global_errors.append(f"Expected around {TOP_COINS_LIMIT * len(TIMEFRAMES)} rows, got {len(rows)}")

    seen_timeframes = {r["timeframe"] for r in rows}
    missing_timeframes = set(TIMEFRAMES) - seen_timeframes
    if missing_timeframes:
        global_errors.append(f"Missing timeframes: {sorted(missing_timeframes)}")

    for row in rows:
        row_errors = []
        if not row.get("symbol"):
            row_errors.append("missing symbol")
        if row.get("current_price") is None:
            row_errors.append("missing current_price")
        if row.get("short_max_pain") is None:
            row_errors.append("missing short_max_pain")
        if row.get("long_max_pain") is None:
            row_errors.append("missing long_max_pain")
        if global_errors or row_errors:
            row["is_valid"] = False if use_postgres() else 0
            row["validation_errors"] = "; ".join(global_errors + row_errors)[:1000]
    return rows


async def collect_once():
    """Collect through CoinGlass rendered page/DOM instead of direct encrypted API.

    Safety rule: insert only rows that contain parsed Max Pain fields.
    If the DOM reader only sees generic market-table rows, do not pollute the DB;
    return 0 rows and mark the timeframes as missing so we can inspect logs/debug.
    """
    start = time.time()
    collected_at = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    if not use_postgres():
        collected_at = collected_at.isoformat()

    print(f"[collector] starting DOM collection at {collected_at}")

    snapshot = await collect_coinglass_dom_snapshot(
        timeframes=TIMEFRAMES,
        headless=True,
        url=COINGLASS_MAX_PAIN_URL,
    )

    rows = []
    market_only_count = 0

    for item in snapshot.get("rows", []):
        # The DOM reader can currently see generic market rows too.
        # Only save rows when Max Pain-specific fields are present.
        short_mp = item.get("max_short_price")
        long_mp = item.get("max_long_price")
        if short_mp is None or long_mp is None:
            market_only_count += 1
            continue

        symbol = str(item.get("symbol", "")).upper()
        if symbol in NON_CRYPTO_SYMBOLS:
            continue

        rows.append({
            "collected_at": collected_at,
            "source": SOURCE_NAME + "_dom",
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
            "distance_short_abs": item.get("short_distance_usd"),
            "distance_short_pct": item.get("short_distance_pct"),
            "distance_long_abs": item.get("long_distance_usd"),
            "distance_long_pct": item.get("long_distance_pct"),
        })

    missing_timeframes = list(snapshot.get("missing_timeframes", []))
    seen_timeframes = {r.get("timeframe") for r in rows}
    for tf in TIMEFRAMES:
        if tf not in seen_timeframes and tf not in missing_timeframes:
            missing_timeframes.append(tf)

    # Do not normalize prices across timeframes; keep CoinGlass values and distances as rendered.
    rows = validate_snapshot(rows)
    rows = enrich_rows(rows)
    inserted = insert_snapshots(rows)

    print(
        f"[collector] DOM inserted {inserted} rows; "
        f"missing={missing_timeframes}; "
        f"market_only_rows_seen={market_only_count}; "
        f"raw_rows_seen={snapshot.get('row_count', 0)}"
    )

    if inserted == 0:
        print("[collector] DOM reader did not find parseable Max Pain rows yet. It did find raw DOM rows/text; no fake DB rows inserted.")

    return inserted, missing_timeframes

def fmt(value, digits=2):
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:,.{digits}f}"
    return str(value)

def short_time(value):
    s = str(value)
    return s[11:16] if len(s) >= 16 else s


def latest_snapshot_rows():
    """Fetch all rows from the latest snapshot for analysis.py."""
    return query(
        f"""
        WITH latest AS (SELECT MAX(collected_at) AS max_time FROM max_pain_snapshots)
        SELECT symbol, timeframe, current_price,
               short_max_pain, long_max_pain,
               short_liquidation_amount, long_liquidation_amount,
               distance_short_pct, distance_long_pct
        FROM max_pain_snapshots, latest
        WHERE collected_at = latest.max_time
        ORDER BY symbol, {TIMEFRAME_ORDER_SQL}
        """
    )


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
        "Crypto Intelligence Bot פעיל.\n"
        "פקודות:\n"
        "/collect - איסוף ידני עכשיו\n"
        "/latest - snapshot אחרון\n"
        "/coin BTC - מטבע בכל הטווחים\n"
        "/range BTC 24h - מטבע וטווח\n"
        "/top 10 - הכי קרובים ל-Max Pain\n"
        "/consensus - מטבעות עם קרבה עקבית לאותו צד בכל הטווחים\n"
        "/gap - פער ממוצע בין Short/Long Max Pain\n"
        "/liqsum - מאזן סכומי הנזילות לפי טווח וסך הכול\n"
        "/market - נטיית שוק לפי קרבה ל-Max Pain בכל טווח\n"
        "/btc_like - מטבעות שהכיוון שלהם דומה ל-BTC\n"
        "/score BTC - פירוק Setup Strength למטבע\n"
        "/score_top - דירוג Setup Strength\n"
        "/hyper_debug BTC - בדיקת מבנה עמוד Hyperliquid\n"
        "/symbols - רשימת מטבעות מה-snapshot האחרון\n"
        "/alerts - חריגות"
    )

async def collect_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global COLLECT_LOCK

    if COLLECT_LOCK is None:
        COLLECT_LOCK = asyncio.Lock()

    if COLLECT_LOCK.locked():
        await update.message.reply_text("איסוף כבר רץ כרגע. חכי שהוא יסתיים, או בצעי Restart service ב-Render אם הוא נתקע.")
        return

    async with COLLECT_LOCK:
        await update.message.reply_text("מתחיל איסוף ידני. זה יכול לקחת כמה דקות...")
        try:
            inserted, missing_timeframes = await collect_once()
            if missing_timeframes:
                missing = ", ".join(missing_timeframes)
                await update.message.reply_text(f"האיסוף הסתיים. נשמרו {inserted} שורות. חסר טווח: {missing}")
            else:
                await update.message.reply_text(f"האיסוף הסתיים. נשמרו {inserted} שורות.")
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
    rows = query(
        f"""
        WITH latest AS (SELECT MAX(collected_at) AS max_time FROM max_pain_snapshots)
        SELECT timeframe, current_price, short_max_pain, long_max_pain,
               short_liquidation_amount, long_liquidation_amount,
               distance_short_pct, distance_long_pct, alert_level
        FROM max_pain_snapshots, latest
        WHERE symbol = ? AND collected_at = latest.max_time
        ORDER BY {TIMEFRAME_ORDER_SQL}
        """,
        (symbol,)
    )
    if not rows:
        await update.message.reply_text(f"לא נמצאו נתונים עדכניים עבור {symbol}. הריצו /collect קודם.")
        return

    table = [[
        r["timeframe"], fmt(r["current_price"]),
        fmt(r["short_max_pain"]), fmt(r["long_max_pain"]),
        fmt(r["short_liquidation_amount"], 0), fmt(r["long_liquidation_amount"], 0),
        fmt(r["distance_short_pct"]), fmt(r["distance_long_pct"]), r["alert_level"]
    ] for r in rows]

    text = tabulate(
        table,
        headers=["TF", "Price", "ShortPx", "LongPx", "Short$", "Long$", "DistS%", "DistL%", "Alert"],
        tablefmt="plain",
    )
    await update.message.reply_text(f"<pre>{html.escape(text)}</pre>", parse_mode="HTML")


async def range_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await update.message.reply_text("שימוש: /range BTC 24h")
        return
    symbol = context.args[0].upper()
    timeframe = context.args[1].lower()
    rows = query(
        """
        SELECT current_price, short_max_pain, long_max_pain,
               short_liquidation_amount, long_liquidation_amount,
               distance_short_pct, distance_long_pct, alert_level
        FROM max_pain_snapshots
        WHERE symbol = ? AND timeframe = ?
        ORDER BY collected_at DESC
        LIMIT 1
        """,
        (symbol, timeframe)
    )
    if not rows:
        await update.message.reply_text(f"לא נמצאו נתונים עדכניים עבור {symbol}/{timeframe}.")
        return

    table = [[
        fmt(r["current_price"]), fmt(r["short_max_pain"]),
        fmt(r["long_max_pain"]), fmt(r["short_liquidation_amount"], 0), fmt(r["long_liquidation_amount"], 0),
        fmt(r["distance_short_pct"]), fmt(r["distance_long_pct"]), r["alert_level"]
    ] for r in rows]
    text = tabulate(
        table,
        headers=["Price", "ShortPx", "LongPx", "Short$", "Long$", "DistS%", "DistL%", "Alert"],
        tablefmt="plain",
    )
    await update.message.reply_text(f"<pre>{html.escape(text)}</pre>", parse_mode="HTML")


async def top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    limit = int(context.args[0]) if context.args else 10
    closest_expr = (
        "LEAST(ABS(distance_short_pct), ABS(distance_long_pct))"
        if use_postgres()
        else "MIN(ABS(distance_short_pct), ABS(distance_long_pct))"
    )
    rows = query(
        f"""
        WITH latest AS (SELECT MAX(collected_at) AS max_time FROM max_pain_snapshots)
        SELECT symbol, timeframe, current_price, short_max_pain, long_max_pain,
               short_liquidation_amount, long_liquidation_amount,
               distance_short_pct, distance_long_pct,
               {closest_expr} AS closest_distance_pct,
               CASE
                 WHEN ABS(distance_short_pct) <= ABS(distance_long_pct) THEN 'SHORT'
                 ELSE 'LONG'
               END AS closest_side,
               alert_level
        FROM max_pain_snapshots, latest
        WHERE collected_at = latest.max_time
          AND distance_short_pct IS NOT NULL
          AND distance_long_pct IS NOT NULL
        ORDER BY closest_distance_pct ASC
        LIMIT ?
        """,
        (limit,)
    )
    if not rows:
        await update.message.reply_text("עדיין אין נתונים. הריצו /collect קודם.")
        return
    table = [[
        r["symbol"], r["timeframe"], r["closest_side"], fmt(r["current_price"]),
        fmt(r["short_max_pain"]), fmt(r["long_max_pain"]),
        fmt(r["distance_short_pct"]), fmt(r["distance_long_pct"]), r["alert_level"]
    ] for r in rows]
    text = tabulate(
        table,
        headers=["Coin", "TF", "Side", "Price", "ShortPx", "LongPx", "DistS%", "DistL%", "Alert"],
        tablefmt="plain",
    )
    await update.message.reply_text(f"<pre>{html.escape(text)}</pre>", parse_mode="HTML")


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

async def symbols(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show symbols available in latest snapshot."""
    rows = latest_snapshot_rows()
    if not rows:
        await update.message.reply_text("אין נתונים. הריצו /collect קודם.")
        return
    symbols = sorted({r["symbol"] for r in rows if r["symbol"]})
    text = ", ".join(symbols)
    await update.message.reply_text(f"{len(symbols)} symbols:\n{text}")


async def hyper_debug(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Diagnostic control probe for Hyperliquid liquidation map page."""
    symbol = context.args[0].upper() if context.args else "BTC"
    await update.message.reply_text(f"בודק שליטה בעמוד Hyperliquid עבור {symbol}: פתיחה, בחירת מטבע ורענון. עד דקה...")

    try:
        result = await hyperliquid_reader.probe_hyperliquid_symbol(symbol)
    except Exception as exc:
        await update.message.reply_text(f"שגיאה בבדיקת Hyperliquid: {exc!r}")
        return

    if not result.get("ok"):
        preview = "\n".join(result.get("body_preview", [])[:20])
        await update.message.reply_text(
            f"<pre>{html.escape('Hyper debug failed\\n' + str(result.get('error')) + '\\n\\n' + preview[:1800])}</pre>",
            parse_mode="HTML"
        )
        return

    structure = result.get("structure", {})
    selected = result.get("selected", {})
    refreshed = result.get("refreshed", {})

    summary = [
        ["Symbol", result.get("symbol")],
        ["OK", result.get("ok")],
        ["Selected", selected.get("ok")],
        ["SelectMethod", selected.get("method")],
        ["Refresh", refreshed.get("ok")],
        ["Canvas", structure.get("canvasCount")],
        ["SVG", structure.get("svgCount")],
        ["Inputs", structure.get("inputCount")],
        ["Buttons", structure.get("buttonCount")],
    ]
    text1 = tabulate(summary, tablefmt="plain")
    preview = "\n".join(result.get("body_preview", [])[:28])

    await update.message.reply_text(
        f"<pre>{html.escape(text1 + chr(10) + chr(10) + preview[:2300])}</pre>",
        parse_mode="HTML"
    )


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

    # Automatic scheduled collection is disabled for the trial phase.
    # Data collection now runs only when you send /collect in Telegram.

    bot_app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    bot_app.add_handler(CommandHandler("start", start))
    bot_app.add_handler(CommandHandler("help", start))
    bot_app.add_handler(CommandHandler("collect", collect_cmd))
    bot_app.add_handler(CommandHandler("latest", latest))
    bot_app.add_handler(CommandHandler("coin", coin))
    bot_app.add_handler(CommandHandler("range", range_cmd))
    bot_app.add_handler(CommandHandler("top", top))
    bot_app.add_handler(CommandHandler("consensus", consensus))
    bot_app.add_handler(CommandHandler("gap", gap))
    bot_app.add_handler(CommandHandler("liqsum", liqsum))
    bot_app.add_handler(CommandHandler("market", market))
    bot_app.add_handler(CommandHandler("btc_like", btc_like))
    bot_app.add_handler(CommandHandler("score", score))
    bot_app.add_handler(CommandHandler("score_top", score_top))
    bot_app.add_handler(CommandHandler("symbols", symbols))
    bot_app.add_handler(CommandHandler("hyper_debug", hyper_debug))
    bot_app.add_handler(CommandHandler("alerts", alerts))

    await bot_app.initialize()
    await bot_app.start()

    webhook_url = f"{PUBLIC_URL}/telegram"
    await bot_app.bot.delete_webhook(drop_pending_updates=True)
    await bot_app.bot.set_webhook(url=webhook_url, drop_pending_updates=True)
    print(f"[bot] webhook set to {webhook_url}")

    await start_web_server(bot_app)

    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await bot_app.bot.delete_webhook(drop_pending_updates=True)
        await bot_app.stop()
        await bot_app.shutdown()

if __name__ == "__main__":
    asyncio.run(main())