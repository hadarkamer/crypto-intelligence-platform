import asyncio
import html
import json
import os
import re
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any

from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from playwright.async_api import async_playwright
from tabulate import tabulate
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

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
COINGLASS_MAX_PAIN_URL = os.getenv("COINGLASS_MAX_PAIN_URL", "https://www.coinglass.com/liquidation-maxpain")
TOP_COINS_LIMIT = int(os.getenv("TOP_COINS_LIMIT", "50"))
COLLECT_INTERVAL_MINUTES = int(os.getenv("COLLECT_INTERVAL_MINUTES", "60"))

TIMEFRAMES = ["12h", "24h", "48h", "3d", "1w", "2w", "1m"]
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
COLLECTOR_VERSION = "v1-cloud-webservice"

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
    output = []
    for row in rows:
        price = row.get("current_price")
        short_mp = row.get("short_max_pain")
        long_mp = row.get("long_max_pain")
        row["distance_short_abs"] = distance_abs(price, short_mp)
        row["distance_short_pct"] = distance_pct(price, short_mp)
        row["distance_long_abs"] = distance_abs(price, long_mp)
        row["distance_long_pct"] = distance_pct(price, long_mp)

        prev = previous_row(row["symbol"], row["timeframe"], row["collected_at"])
        if prev:
            row["delta_short_abs"] = None if short_mp is None or prev["short_max_pain"] is None else short_mp - prev["short_max_pain"]
            row["delta_short_pct"] = pct_change(short_mp, prev["short_max_pain"])
            row["delta_long_abs"] = None if long_mp is None or prev["long_max_pain"] is None else long_mp - prev["long_max_pain"]
            row["delta_long_pct"] = pct_change(long_mp, prev["long_max_pain"])
        else:
            row["delta_short_abs"] = row["delta_short_pct"] = None
            row["delta_long_abs"] = row["delta_long_pct"] = None

        row["alert_level"] = alert_level(row["delta_short_pct"], row["delta_long_pct"])
        output.append(row)
    return output


def normalize_symbol(value: str) -> str:
    if not value:
        return ""
    match = re.search(r"[A-Z0-9]{2,12}", str(value).upper())
    return match.group(0) if match else str(value).split()[0].upper()

def pick_number(obj: Dict[str, Any], keywords: List[str]) -> Optional[float]:
    for key, value in obj.items():
        key_l = str(key).lower()
        if all(k in key_l for k in keywords):
            if isinstance(value, (int, float)):
                return float(value)
            if isinstance(value, str):
                return parse_number(value)
    return None

def extract_rows_from_json_payload(payload, timeframe, collected_at, scrape_duration):
    candidates = []

    def walk(node):
        if isinstance(node, list):
            if node and all(isinstance(x, dict) for x in node):
                sample = " ".join(" ".join(map(str, x.keys())) for x in node[:3]).lower()
                if "symbol" in sample and ("pain" in sample or "liq" in sample or "price" in sample):
                    candidates.append(node)
            for item in node:
                walk(item)
        elif isinstance(node, dict):
            for value in node.values():
                walk(value)

    walk(payload)

    output = []
    for arr in candidates:
        for idx, item in enumerate(arr[:TOP_COINS_LIMIT], start=1):
            if not isinstance(item, dict):
                continue

            symbol = normalize_symbol(
                item.get("symbol")
                or item.get("baseAsset")
                or item.get("coin")
                or item.get("currency")
                or item.get("name")
            )
            if not symbol:
                continue

            current_price = (
                pick_number(item, ["price"])
                or pick_number(item, ["current"])
                or pick_number(item, ["last"])
            )

            short_max_pain = (
                pick_number(item, ["short", "max", "pain", "price"])
                or pick_number(item, ["short", "pain"])
                or pick_number(item, ["short", "liq", "price"])
            )

            long_max_pain = (
                pick_number(item, ["long", "max", "pain", "price"])
                or pick_number(item, ["long", "pain"])
                or pick_number(item, ["long", "liq", "price"])
            )

            if current_price is None or (short_max_pain is None and long_max_pain is None):
                continue

            output.append({
                "collected_at": collected_at,
                "source": SOURCE_NAME,
                "collector_version": COLLECTOR_VERSION,
                "scrape_duration_seconds": scrape_duration,
                "is_valid": True if use_postgres() else 1,
                "validation_errors": None,
                "symbol": symbol,
                "rank": idx,
                "timeframe": timeframe,
                "current_price": current_price,
                "short_max_pain": short_max_pain,
                "long_max_pain": long_max_pain,
            })

        if output:
            return output

    return []

async def scrape_timeframe(page, timeframe, collected_at, scrape_duration):
    label = TIMEFRAME_LABELS.get(timeframe, timeframe)

    try:
        await page.get_by_text(label, exact=True).click(timeout=5000)
        await page.wait_for_timeout(4500)
    except Exception as e:
        print(f"[collector] could not click timeframe {timeframe}/{label}: {e}")

    rows = await page.locator("table tbody tr").all()
    output = []

    for idx, row in enumerate(rows[:TOP_COINS_LIMIT], start=1):
        cells = [await c.inner_text() for c in await row.locator("td").all()]
        cells = [c.strip() for c in cells if c.strip()]
        if len(cells) < 4:
            continue

        symbol = normalize_symbol(cells[0])
        numbers = [parse_number(c) for c in cells]
        numbers = [n for n in numbers if n is not None]

        output.append({
            "collected_at": collected_at,
            "source": SOURCE_NAME,
            "collector_version": COLLECTOR_VERSION,
            "scrape_duration_seconds": scrape_duration,
            "is_valid": True if use_postgres() else 1,
            "validation_errors": None,
            "symbol": symbol,
            "rank": idx,
            "timeframe": timeframe,
            "current_price": numbers[0] if len(numbers) > 0 else None,
            "short_max_pain": numbers[1] if len(numbers) > 1 else None,
            "long_max_pain": numbers[2] if len(numbers) > 2 else None,
        })

    print(f"[collector] html table rows for {timeframe}: {len(output)}")
    return output

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
    start = time.time()
    collected_at = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    if not use_postgres():
        collected_at = collected_at.isoformat()

    print(f"[collector] starting collection at {collected_at}")

    network_payloads = []
    network_urls = []

    async def capture_response(response):
        try:
            url = response.url
            content_type = response.headers.get("content-type", "")
            if "json" not in content_type.lower():
                return
            if len(network_payloads) >= NETWORK_CAPTURE_LIMIT:
                return
            payload = await response.json()
            text_url = url.lower()
            payload_preview = json.dumps(payload, ensure_ascii=False)[:1500].lower()
            if (
                "liquidation" in text_url
                or "max-pain" in text_url
                or "maxpain" in text_url
                or "max_pain" in payload_preview
                or "max pain" in payload_preview
                or ("short" in payload_preview and "long" in payload_preview and "symbol" in payload_preview)
            ):
                network_payloads.append(payload)
                network_urls.append(url)
                print(f"[collector] captured json: {url}")
        except Exception as e:
            print(f"[collector] response capture error: {e}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        page = await browser.new_page(viewport={"width": 1440, "height": 1200})
        page.on("response", capture_response)

        await page.goto(COINGLASS_MAX_PAIN_URL, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(8000)

        try:
            print(f"[collector] page title: {await page.title()}")
            print(f"[collector] page url: {page.url}")
        except Exception:
            pass

        all_rows = []

        for timeframe in TIMEFRAMES:
            rows = []

            for payload in network_payloads:
                rows = extract_rows_from_json_payload(payload, timeframe, collected_at, time.time() - start)
                if rows:
                    print(f"[collector] network rows for {timeframe}: {len(rows)}")
                    break

            if not rows:
                rows = await scrape_timeframe(page, timeframe, collected_at, time.time() - start)

            print(f"[collector] {timeframe}: {len(rows)} rows")
            all_rows.extend(rows)

        if not all_rows:
            try:
                visible_text = await page.locator("body").inner_text(timeout=5000)
                print("[collector] visible text preview:")
                print(visible_text[:2000])
            except Exception as e:
                print(f"[collector] could not read body text: {e}")

            print("[collector] captured json urls:")
            for url in network_urls[:20]:
                print(f"[collector] url: {url}")

        await browser.close()

    all_rows = validate_snapshot(all_rows)
    all_rows = enrich_rows(all_rows)
    inserted = insert_snapshots(all_rows)
    print(f"[collector] inserted {inserted} rows")
    return inserted

def fmt(value, digits=2):
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:,.{digits}f}"
    return str(value)

def short_time(value):
    s = str(value)
    return s[11:16] if len(s) >= 16 else s

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Crypto Intelligence Bot פעיל.\\n"
        "פקודות:\\n"
        "/collect - איסוף ידני עכשיו\\n"
        "/latest - snapshot אחרון\\n"
        "/coin BTC - מטבע בכל הטווחים\\n"
        "/range BTC 24h - מטבע וטווח\\n"
        "/top 10 - הכי קרובים ל-Max Pain\\n"
        "/alerts - חריגות"
    )

async def collect_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("מתחיל איסוף ידני. זה יכול לקחת דקה...")
    try:
        inserted = await collect_once()
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
        """
        SELECT collected_at, timeframe, current_price, short_max_pain, long_max_pain,
               delta_short_pct, delta_long_pct, distance_short_pct, distance_long_pct, alert_level
        FROM max_pain_snapshots
        WHERE symbol = ?
        ORDER BY collected_at DESC, timeframe ASC
        LIMIT 70
        """,
        (symbol,)
    )
    if not rows:
        await update.message.reply_text(f"לא נמצאו נתונים עבור {symbol}. הריצו /collect קודם.")
        return

    table = [[
        short_time(r["collected_at"]), r["timeframe"], fmt(r["current_price"]),
        fmt(r["short_max_pain"]), fmt(r["long_max_pain"]),
        fmt(r["delta_short_pct"]), fmt(r["delta_long_pct"]),
        fmt(r["distance_short_pct"]), fmt(r["distance_long_pct"]), r["alert_level"]
    ] for r in rows]

    text = tabulate(table, headers=["Hour", "TF", "Price", "Short", "Long", "ΔS%", "ΔL%", "DistS%", "DistL%", "Alert"], tablefmt="plain")
    await update.message.reply_text(f"<pre>{html.escape(text)}</pre>", parse_mode="HTML")

async def range_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await update.message.reply_text("שימוש: /range BTC 24h")
        return
    symbol = context.args[0].upper()
    timeframe = context.args[1].lower()
    rows = query(
        """
        SELECT collected_at, current_price, short_max_pain, long_max_pain,
               delta_short_pct, delta_long_pct, distance_short_pct, distance_long_pct, alert_level
        FROM max_pain_snapshots
        WHERE symbol = ? AND timeframe = ?
        ORDER BY collected_at DESC
        LIMIT 24
        """,
        (symbol, timeframe)
    )
    if not rows:
        await update.message.reply_text(f"לא נמצאו נתונים עבור {symbol}/{timeframe}.")
        return
    table = [[
        short_time(r["collected_at"]), fmt(r["current_price"]), fmt(r["short_max_pain"]),
        fmt(r["long_max_pain"]), fmt(r["delta_short_pct"]), fmt(r["delta_long_pct"]),
        fmt(r["distance_short_pct"]), fmt(r["distance_long_pct"]), r["alert_level"]
    ] for r in rows]
    text = tabulate(table, headers=["Hour", "Price", "Short", "Long", "ΔS%", "ΔL%", "DistS%", "DistL%", "Alert"], tablefmt="plain")
    await update.message.reply_text(f"<pre>{html.escape(text)}</pre>", parse_mode="HTML")

async def top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    limit = int(context.args[0]) if context.args else 10
    rows = query(
        """
        WITH latest AS (SELECT MAX(collected_at) AS max_time FROM max_pain_snapshots)
        SELECT symbol, timeframe, current_price, short_max_pain, long_max_pain,
               """ + ("LEAST(distance_short_pct, distance_long_pct)" if use_postgres() else "MIN(distance_short_pct, distance_long_pct)") + """ AS closest_distance_pct,
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
    table = [[r["symbol"], r["timeframe"], fmt(r["current_price"]), fmt(r["short_max_pain"]), fmt(r["long_max_pain"]), fmt(r["closest_distance_pct"]), r["alert_level"]] for r in rows]
    text = tabulate(table, headers=["Coin", "TF", "Price", "Short", "Long", "Closest%", "Alert"], tablefmt="plain")
    await update.message.reply_text(f"<pre>{html.escape(text)}</pre>", parse_mode="HTML")

async def alerts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = query(
        """
        SELECT collected_at, symbol, timeframe, delta_short_pct, delta_long_pct, alert_level
        FROM max_pain_snapshots
        WHERE alert_level IN ('low', 'medium', 'high')
        ORDER BY collected_at DESC
        LIMIT 50
        """
    )
    if not rows:
        await update.message.reply_text("אין חריגות כרגע או שעדיין אין מספיק מדידות להשוואה.")
        return
    table = [[short_time(r["collected_at"]), r["symbol"], r["timeframe"], fmt(r["delta_short_pct"]), fmt(r["delta_long_pct"]), r["alert_level"]] for r in rows]
    text = tabulate(table, headers=["Hour", "Coin", "TF", "ΔS%", "ΔL%", "Alert"], tablefmt="plain")
    await update.message.reply_text(f"<pre>{html.escape(text)}</pre>", parse_mode="HTML")

async def health(request):
    return web.json_response({"status": "ok", "service": "crypto-intelligence-v1"})

async def start_health_server():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
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

    init_db()
    await start_health_server()

    scheduler = AsyncIOScheduler()
    scheduler.add_job(scheduled_collection, "interval", minutes=COLLECT_INTERVAL_MINUTES)
    scheduler.start()

    bot_app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    bot_app.add_handler(CommandHandler("start", start))
    bot_app.add_handler(CommandHandler("help", start))
    bot_app.add_handler(CommandHandler("collect", collect_cmd))
    bot_app.add_handler(CommandHandler("latest", latest))
    bot_app.add_handler(CommandHandler("coin", coin))
    bot_app.add_handler(CommandHandler("range", range_cmd))
    bot_app.add_handler(CommandHandler("top", top))
    bot_app.add_handler(CommandHandler("alerts", alerts))

    print("[bot] starting polling")
    await bot_app.initialize()
    await bot_app.start()
    await bot_app.updater.start_polling()

    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await bot_app.updater.stop()
        await bot_app.stop()
        await bot_app.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
