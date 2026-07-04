import html
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from tabulate import tabulate
from .config import TELEGRAM_BOT_TOKEN
from .storage import query

def fmt(value, digits=2):
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:,.{digits}f}"
    return str(value)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "CoinGlass Collector Bot פעיל.\n"
        "פקודות:\n"
        "/coin BTC - הצגת מטבע לפי כל הטווחים\n"
        "/range BTC 24h - מטבע וטווח מסוים\n"
        "/top 10 - המטבעות הכי קרובים ל-Max Pain\n"
        "/alerts - חריגות אחרונות\n"
        "/latest - Snapshot אחרון"
    )

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
        await update.message.reply_text(f"לא נמצאו נתונים עבור {symbol}.")
        return

    table = [[
        r["collected_at"][11:16],
        r["timeframe"],
        fmt(r["current_price"]),
        fmt(r["short_max_pain"]),
        fmt(r["long_max_pain"]),
        fmt(r["delta_short_pct"]),
        fmt(r["delta_long_pct"]),
        fmt(r["distance_short_pct"]),
        fmt(r["distance_long_pct"]),
        r["alert_level"]
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

    table = [[
        r["collected_at"][11:16],
        fmt(r["current_price"]),
        fmt(r["short_max_pain"]),
        fmt(r["long_max_pain"]),
        fmt(r["delta_short_pct"]),
        fmt(r["delta_long_pct"]),
        fmt(r["distance_short_pct"]),
        fmt(r["distance_long_pct"]),
        r["alert_level"]
    ] for r in rows]

    text = tabulate(table, headers=["Hour", "Price", "Short", "Long", "ΔS%", "ΔL%", "DistS%", "DistL%", "Alert"], tablefmt="plain")
    await update.message.reply_text(f"<pre>{html.escape(text)}</pre>", parse_mode="HTML")

async def top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    limit = int(context.args[0]) if context.args else 10
    rows = query(
        """
        WITH latest AS (
            SELECT MAX(collected_at) AS max_time FROM max_pain_snapshots
        )
        SELECT symbol, timeframe, current_price, short_max_pain, long_max_pain,
               MIN(distance_short_pct, distance_long_pct) AS closest_distance_pct,
               alert_level
        FROM max_pain_snapshots, latest
        WHERE collected_at = latest.max_time
        ORDER BY closest_distance_pct ASC
        LIMIT ?
        """,
        (limit,)
    )

    table = [[
        r["symbol"], r["timeframe"], fmt(r["current_price"]),
        fmt(r["short_max_pain"]), fmt(r["long_max_pain"]),
        fmt(r["closest_distance_pct"]), r["alert_level"]
    ] for r in rows]

    text = tabulate(table, headers=["Coin", "TF", "Price", "Short", "Long", "Closest%", "Alert"], tablefmt="plain")
    await update.message.reply_text(f"<pre>{html.escape(text)}</pre>", parse_mode="HTML")

async def alerts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = query(
        """
        SELECT collected_at, symbol, timeframe, delta_short_pct, delta_long_pct, alert_level
        FROM max_pain_snapshots
        WHERE alert_level IN ('low', 'medium', 'high')
        ORDER BY collected_at DESC,
                 CASE alert_level WHEN 'high' THEN 1 WHEN 'medium' THEN 2 WHEN 'low' THEN 3 ELSE 4 END
        LIMIT 50
        """
    )

    table = [[
        r["collected_at"][11:16],
        r["symbol"],
        r["timeframe"],
        fmt(r["delta_short_pct"]),
        fmt(r["delta_long_pct"]),
        r["alert_level"]
    ] for r in rows]

    text = tabulate(table, headers=["Hour", "Coin", "TF", "ΔS%", "ΔL%", "Alert"], tablefmt="plain")
    await update.message.reply_text(f"<pre>{html.escape(text)}</pre>", parse_mode="HTML")

async def latest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = query("SELECT MAX(collected_at) AS latest_time, COUNT(*) AS rows_count FROM max_pain_snapshots")
    r = rows[0]
    await update.message.reply_text(f"Snapshot אחרון: {r['latest_time']}\nמספר שורות: {r['rows_count']}")

def main():
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN in .env")
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", start))
    app.add_handler(CommandHandler("coin", coin))
    app.add_handler(CommandHandler("range", range_cmd))
    app.add_handler(CommandHandler("top", top))
    app.add_handler(CommandHandler("alerts", alerts))
    app.add_handler(CommandHandler("latest", latest))
    app.run_polling()

if __name__ == "__main__":
    main()
