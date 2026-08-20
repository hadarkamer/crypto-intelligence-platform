"""Telegram surface for the candidate GPT agent.

The production bot is untouched unless a staging entrypoint explicitly registers
these handlers.  The first candidate exposes /ai, /ai_status and /ai_reset only;
plain non-command messages are deliberately not intercepted yet.
"""

from __future__ import annotations

from typing import Iterable

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

import ai_agent

TELEGRAM_MESSAGE_LIMIT = 3900


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
            "/ai תשווה לי בין Futures CVD ל-Spot CVD של SOL\n\n"
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
    await update.message.reply_text(
        "🧠 AI Candidate\n"
        f"OpenAI API: {key_status}\n"
        f"Model: {state.get('model')}\n"
        "Mode: READ ONLY\n"
        f"Tools: {tools}\n"
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
