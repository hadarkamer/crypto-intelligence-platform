"""Telegram surface for the production read-only analytical GPT layer.

Plain non-command messages are deliberately not intercepted; users opt in with
the /ai command and the existing bot commands keep their current behavior.
"""

from __future__ import annotations

import asyncio
import re
from typing import Iterable

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

import ai_agent
import ai_alert_research
import research_event_runtime
import research_outcome_worker

TELEGRAM_MESSAGE_LIMIT = 3900


def _plain_telegram_text(text: str) -> str:
    """Remove common Markdown tokens because replies use Telegram plain text."""
    value = str(text or "")
    value = re.sub(r"```(?:[A-Za-z0-9_+-]+)?\n?", "", value)
    value = value.replace("```", "")
    value = re.sub(r"(?m)^\s{0,3}#{1,6}\s+", "", value)
    value = re.sub(r"\*\*(.*?)\*\*", r"\1", value, flags=re.DOTALL)
    value = re.sub(r"__(.*?)__", r"\1", value, flags=re.DOTALL)
    value = re.sub(r"`([^`]+)`", r"\1", value)
    value = re.sub(r"\[([^\]]+)\]\((https?://[^)]+)\)", r"\1\n\2", value)
    return value.strip()


def _chunks(text: str, limit: int = TELEGRAM_MESSAGE_LIMIT) -> Iterable[str]:
    value = str(text or "").strip()
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
            "/ai נתח את ביצועי התראות BTC ב-30 הימים האחרונים אחרי 4 שעות\n"
            "/ai חפש שילובי התראות עם MAE נמוך והתקדמות מהירה ב-4 שעות\n"
            "/ai השווה נתוני OI/CVD גולמיים מול ציוני הבוט בהתראות האחרונות\n\n"
            "שכבת ה-AI מנתחת בלבד ואינה משנה את מנגנון ההתראות."
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

    for part in _chunks(_plain_telegram_text(answer)):
        await update.message.reply_text(part)


async def ai_status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message is None:
        return
    state = ai_agent.status()
    archive = await asyncio.to_thread(ai_alert_research.archive_status)
    capture = research_event_runtime.status()
    outcomes = research_outcome_worker.WORKER.status()
    key_status = "מחובר" if state.get("configured") else "לא מוגדר"
    tools = ", ".join(state.get("tools") or []) or "אין"
    await update.message.reply_text(
        "🧠 AI — הבוט הקיים\n"
        f"OpenAI API: {key_status}\n"
        f"Model: {state.get('model')}\n"
        "Mode: ANALYSIS READ ONLY\n"
        f"Tools: {tools}\n"
        f"ארכיון Research: {'פעיל' if archive.get('schema_present') else 'לא הותקן'} | "
        f"התראות שנמסרו: {archive.get('delivered_alerts', 0)}\n"
        f"שמירת אירועים: {'פעילה' if (capture.get('persistence') or {}).get('enabled') else 'כבויה'} | "
        f"תוצאות 1/4/12/24h: {'פעילות' if outcomes.get('enabled') else 'כבויות'}\n"
        f"מסלול תוצאה: Binance Spot {((outcomes.get('price_path') or {}).get('interval') or '-')} | "
        f"שיטה: {outcomes.get('method', '-')}\n"
        "Web/CoinGlass Vision: מעבדה בלבד, לא מחוברים לייצור\n"
        "זיכרון שיחה: זמני ומוגבל; יתאפס בעת restart."
    )


async def ai_reset_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message is None:
        return
    conversation_id = update.effective_chat.id if update.effective_chat else update.message.chat_id
    ai_agent.reset_conversation(conversation_id)
    await update.message.reply_text("🧹 זיכרון השיחה הזמני של ה-AI אופס.")


def register_ai_handlers(app: Application) -> None:
    app.add_handler(CommandHandler("ai", ai_cmd))
    app.add_handler(CommandHandler("ai_status", ai_status_cmd))
    app.add_handler(CommandHandler("ai_reset", ai_reset_cmd))
