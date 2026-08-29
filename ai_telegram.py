"""Telegram surface for the production read-only analytical GPT layer.

Plain non-command messages are deliberately not intercepted; users opt in with
the /ai command and the existing bot commands keep their current behavior.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import re
from typing import Iterable

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

import ai_agent
import ai_alert_research
import research_event_runtime
import research_outcome_worker
import research_formula_store
import research_formula_worker

TELEGRAM_MESSAGE_LIMIT = 3900


def _plain_markdown_tables(value: str) -> str:
    """Turn Markdown tables into compact plain lines Telegram can render."""
    lines = str(value or "").splitlines()
    output: list[str] = []
    index = 0
    separator = re.compile(r"^\s*\|?\s*:?-{3,}:?\s*(?:\|\s*:?-{3,}:?\s*)+\|?\s*$")

    def cells(line: str) -> list[str]:
        return [part.strip() for part in line.strip().strip("|").split("|")]

    while index < len(lines):
        if (
            index + 1 < len(lines)
            and "|" in lines[index]
            and separator.match(lines[index + 1])
        ):
            headers = cells(lines[index])
            index += 2
            output.append(" — ".join(headers))
            while index < len(lines) and "|" in lines[index]:
                row = cells(lines[index])
                pairs = [
                    f"{headers[offset]}: {item}"
                    if offset < len(headers) and headers[offset]
                    else item
                    for offset, item in enumerate(row)
                ]
                output.append("• " + " | ".join(pairs))
                index += 1
            continue
        output.append(lines[index])
        index += 1
    return "\n".join(output)


def _plain_telegram_text(text: str) -> str:
    """Remove common Markdown tokens because replies use Telegram plain text."""
    value = _plain_markdown_tables(str(text or ""))
    value = re.sub(r"```(?:[A-Za-z0-9_+-]+)?\n?", "", value)
    value = value.replace("```", "")
    value = re.sub(r"(?m)^\s{0,3}#{1,6}\s+", "", value)
    value = re.sub(r"\*\*(.*?)\*\*", r"\1", value, flags=re.DOTALL)
    value = re.sub(r"__(.*?)__", r"\1", value, flags=re.DOTALL)
    value = re.sub(r"`([^`]+)`", r"\1", value)
    value = re.sub(r"\[([^\]]+)\]\((https?://[^)]+)\)", r"\1\n\2", value)
    # A slash-separated p75/p90/p95 sequence is visually reordered by
    # Telegram's RTL layout.  One percentile per line preserves the numerical
    # mapping in both Hebrew and English clients.
    value = re.sub(
        r"(?i)MAE\s+p75\s*/\s*p90\s*/\s*p95\s*[:：-]?\s*"
        r"([+-]?\d+(?:\.\d+)?%?)\s*/\s*"
        r"([+-]?\d+(?:\.\d+)?%?)\s*/\s*"
        r"([+-]?\d+(?:\.\d+)?%?)",
        r"MAE p75: \1\nMAE p90: \2\nMAE p95: \3",
        value,
    )
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
            "שכבת ה-AI מנתחת בלבד ואינה משנה את מנגנון ההתראות.\n"
            "להתראות AI אוטונומיות מנוסחאות מאומתות: /ai_alerts_on"
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
    formula_schema = await asyncio.to_thread(research_formula_store.schema_status)
    formulas = research_formula_worker.WORKER.status()
    subscription = (
        await asyncio.to_thread(
            research_formula_store.alert_subscription_status,
            int(update.effective_chat.id),
        )
        if formula_schema.get("schema_present") and update.effective_chat
        else {"active": False}
    )
    key_status = "מחובר" if state.get("configured") else "לא מוגדר"
    tools = ", ".join(state.get("tools") or []) or "אין"
    current_regime = (
        "סוף שבוע (UTC)"
        if datetime.now(timezone.utc).weekday() >= 5
        else "אמצע שבוע (UTC)"
    )
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
        f"מסלול תוצאה: Binance Spot + HYPE Hyperliquid "
        f"{((outcomes.get('price_paths') or {}).get('interval') or '-')} | "
        f"שיטה: {outcomes.get('method', '-')}\n"
        f"מנוע נוסחאות: {'פעיל' if formulas.get('discovery_enabled') else 'כבוי'} | "
        f"Shadow: {'פעיל' if formulas.get('shadow_enabled') else 'כבוי'} | "
        f"מאגר: {'מותקן' if formula_schema.get('schema_present') else 'לא הותקן'}\n"
        f"משטר ניתוח: {current_regime}; השוואה היסטורית מול אותו משטר\n"
        f"מנוע מסירת נוסחאות: {'פעיל' if formulas.get('live_alerts_enabled') else 'כבוי'} | "
        f"נוסחאות Shadow פעילות: {formula_schema.get('shadow_formulas', 0)} | "
        f"נוסחאות LIVE פעילות: {formula_schema.get('live_formulas', 0)}\n"
        f"הצ'אט הזה: {'רשום' if subscription.get('active') else 'לא רשום'}\n"
        "Web/CoinGlass Vision: מעבדה בלבד, לא מחוברים לייצור\n"
        "זיכרון שיחה: זמני ומוגבל; יתאפס בעת restart."
    )


async def ai_reset_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message is None:
        return
    conversation_id = update.effective_chat.id if update.effective_chat else update.message.chat_id
    ai_agent.reset_conversation(conversation_id)
    await update.message.reply_text("🧹 זיכרון השיחה הזמני של ה-AI אופס.")


async def ai_alerts_on_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message is None or update.effective_chat is None:
        return
    schema = await asyncio.to_thread(research_formula_store.schema_status)
    if not schema.get("schema_present"):
        await update.message.reply_text(
            "❌ מנגנון התראות הנוסחה עדיין לא הותקן במסד הנתונים."
        )
        return
    user_id = update.effective_user.id if update.effective_user else None
    await asyncio.to_thread(
        research_formula_store.set_alert_subscription,
        int(update.effective_chat.id),
        active=True,
        requested_by_user_id=user_id,
    )
    await update.message.reply_text(
        "✅ הצ'אט נרשם להתראות טרייד של מנוע ה-AI.\n"
        "התראה תישלח אוטומטית רק כשנוסחה עברה Holdout כרונולוגי, "
        "אימות עתידי ב-Shadow ותנאי סיכון/סיכוי ורוחב מהלך.\n"
        "הבוט אינו מבצע עסקה אוטומטית."
    )


async def ai_alerts_off_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message is None or update.effective_chat is None:
        return
    schema = await asyncio.to_thread(research_formula_store.schema_status)
    if not schema.get("schema_present"):
        await update.message.reply_text("מנגנון התראות ה-AI עדיין לא הותקן.")
        return
    user_id = update.effective_user.id if update.effective_user else None
    await asyncio.to_thread(
        research_formula_store.set_alert_subscription,
        int(update.effective_chat.id),
        active=False,
        requested_by_user_id=user_id,
    )
    await update.message.reply_text("🔕 התראות הטרייד האוטונומיות של ה-AI הופסקו בצ'אט הזה.")


async def ai_alerts_status_cmd(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if update.message is None or update.effective_chat is None:
        return
    schema = await asyncio.to_thread(research_formula_store.schema_status)
    subscription = (
        await asyncio.to_thread(
            research_formula_store.alert_subscription_status,
            int(update.effective_chat.id),
        )
        if schema.get("schema_present")
        else {"active": False}
    )
    formulas = research_formula_worker.WORKER.status()
    await update.message.reply_text(
        "🔔 מצב התראות AI\n"
        f"הצ'אט הזה: {'פעיל' if subscription.get('active') else 'כבוי'}\n"
        f"מנוע מסירה: {'פעיל' if formulas.get('live_alerts_enabled') else 'כבוי'}\n"
        f"נוסחאות Shadow פעילות: {schema.get('shadow_formulas', 0)}\n"
        f"נוסחאות LIVE פעילות: {schema.get('live_formulas', 0)}\n"
        f"Telegram מחובר: {'כן' if (formulas.get('live_delivery_gate') or {}).get('telegram_delivery_connected') else 'לא'}\n"
        "נוסחאות נדרשות לעבור Holdout ואימות עתידי ב-Shadow לפני מסירה."
    )


def register_ai_handlers(app: Application) -> None:
    app.add_handler(CommandHandler("ai", ai_cmd))
    app.add_handler(CommandHandler("ai_status", ai_status_cmd))
    app.add_handler(CommandHandler("ai_reset", ai_reset_cmd))
    app.add_handler(CommandHandler("ai_alerts_on", ai_alerts_on_cmd))
    app.add_handler(CommandHandler("ai_alerts_off", ai_alerts_off_cmd))
    app.add_handler(CommandHandler("ai_alerts_status", ai_alerts_status_cmd))
