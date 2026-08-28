"""Telegram surface for the candidate GPT agent.

The production bot is untouched unless a staging entrypoint explicitly registers
these handlers. Plain non-command messages are deliberately not intercepted yet.
"""

from __future__ import annotations

import asyncio
import re
from typing import Iterable

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

import ai_agent
import research_shadow_replay

TELEGRAM_MESSAGE_LIMIT = 3900


def _telegram_text(text: str) -> str:
    """Make model Markdown readable in Telegram's safe plain-text mode."""
    value = str(text or "").replace("\r\n", "\n")
    value = re.sub(r"(?m)^\s{0,3}#{1,6}\s*", "", value)
    value = re.sub(r"```(?:[A-Za-z0-9_+-]+)?\n?", "", value)
    value = re.sub(r"\[([^\]]+)\]\((https?://[^)]+)\)", r"\1\n\2", value)
    value = value.replace("**", "").replace("__", "")
    value = re.sub(r"`([^`\n]+)`", r"\1", value)
    value = re.sub(r"\n{3,}", "\n\n", value)
    return value.strip()


def _chunks(text: str, limit: int = TELEGRAM_MESSAGE_LIMIT) -> Iterable[str]:
    value = _telegram_text(text)
    if not value:
        return []
    chunks = []
    while len(value) > limit:
        split_at = value.rfind("\n", 0, limit)
        if split_at < limit // 2:
            split_at = value.rfind(" ", 0, limit)
        if split_at < limit // 2:
            split_at = limit
        chunks.append(value[:split_at].rstrip())
        value = value[split_at:].lstrip()
    if value:
        chunks.append(value)
    return chunks


async def ai_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message is None:
        return
    prompt = " ".join(context.args or []).strip()
    if not prompt:
        await update.message.reply_text(
            "שימוש: /ai <הוראה או שאלה>\n\n"
            "דוגמאות:\n"
            "/ai מה מצב ה-OI של BTC?\n"
            "/ai תשווה לי בין Futures CVD ל-Spot CVD של SOL\n"
            "/ai סרוק את מפות החיסולים של BTC\n"
            "/ai מה החדשות החשובות היום על שוק הקריפטו?\n\n"
            "גרסת ה-Candidate כרגע לקריאה בלבד."
        )
        return

    conversation_id = update.effective_chat.id if update.effective_chat else update.message.chat_id
    try:
        answer = await ai_agent.ask(prompt, conversation_id=conversation_id)
    except Exception as exc:
        await update.message.reply_text(
            "❌ שכבת ה-AI לא הצליחה להשלים את הבקשה.\n"
            f"{type(exc).__name__}: {exc}"
        )
        return

    for part in _chunks(answer):
        await update.message.reply_text(part)


async def ai_status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message is None:
        return
    state = ai_agent.status()
    key_status = "מחובר" if state.get("configured") else "לא מוגדר"
    tools = ", ".join(state.get("tools") or []) or "אין"
    vision = state.get("coinglass_vision") or {}
    vision_status = "פעיל" if vision.get("enabled") else "כבוי בהגדרות"
    await update.message.reply_text(
        "🧠 AI Candidate\n"
        f"OpenAI API: {key_status}\n"
        f"Model: {state.get('model')}\n"
        "Mode: READ ONLY\n"
        f"Tools: {tools}\n"
        f"Web search: {'פעיל' if state.get('web_search') == 'enabled_live_not_persisted' else 'כבוי'}\n"
        f"Python לחישובים: {'פעיל' if state.get('code_interpreter') == 'enabled_ephemeral_calculation_only' else 'כבוי'}\n"
        f"CoinGlass Vision: {vision_status}\n"
        "זיכרון שיחה: זמני ומוגבל; יתאפס בעת restart."
    )


async def ai_reset_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message is None:
        return
    conversation_id = update.effective_chat.id if update.effective_chat else update.message.chat_id
    ai_agent.reset_conversation(conversation_id)
    await update.message.reply_text("🧹 זיכרון השיחה הזמני של ה-AI אופס.")


async def ai_research_test_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Run a bounded, read-only Shadow Replay against stored historical data."""
    if update.message is None:
        return
    symbol = str((context.args or ["BTC"])[0]).strip().upper()
    hours = 24
    if len(context.args or []) >= 2:
        try:
            hours = int(context.args[1])
        except ValueError:
            await update.message.reply_text("שימוש: /ai_research_test BTC 24")
            return
    await update.message.reply_text(
        f"🧪 מתחיל Shadow Replay ל-{symbol} על {hours} שעות.\n"
        "הבדיקה קוראת היסטוריה קיימת בלבד ולא כותבת למסד הנתונים."
    )
    try:
        result = await asyncio.to_thread(research_shadow_replay.run_shadow_replay, symbol, hours)
    except Exception as exc:
        await update.message.reply_text(
            "❌ בדיקת Research Event נכשלה.\n"
            f"{type(exc).__name__}: {exc}"
        )
        return

    checks = result.get("checks") or {}
    check_text = " | ".join(
        f"{name}: {'✅' if passed else '❌'}"
        for name, passed in checks.items()
    )
    rows = result.get("raw_rows") or {}
    streams = result.get("events_by_stream") or {}
    await update.message.reply_text(
        ("✅" if result.get("ok") else "⚠️") + " Research Event Shadow Replay\n"
        f"מטבע: {result.get('symbol')} | חלון: {result.get('requested_hours')}h\n"
        f"שורות אמיתיות שנקראו: Price/OI {rows.get('price_oi', 0)}, "
        f"Futures CVD {rows.get('futures', 0)}, Spot CVD {rows.get('spot', 0)}\n"
        f"Research Events שנוצרו בזיכרון: {result.get('events_created', 0)}\n"
        f"Price/OI: {streams.get('price_oi', 0)} | Futures: {streams.get('futures_cvd', 0)} | "
        f"Spot: {streams.get('spot_cvd', 0)}\n"
        f"אירוע ראשון: {result.get('first_event_utc') or '—'}\n"
        f"אירוע אחרון: {result.get('last_event_utc') or '—'}\n"
        f"בדיקות: {check_text}\n\n"
        "הערה: זו בדיקת איכות על נתוני עבר אמיתיים; היא אינה טוענת שהאירועים היו התראות Telegram היסטוריות."
    )


def register_ai_handlers(app: Application) -> None:
    app.add_handler(CommandHandler("ai", ai_cmd))
    app.add_handler(CommandHandler("ai_status", ai_status_cmd))
    app.add_handler(CommandHandler("ai_reset", ai_reset_cmd))
    app.add_handler(CommandHandler("ai_research_test", ai_research_test_cmd))
