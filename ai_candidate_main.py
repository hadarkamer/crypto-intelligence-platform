"""Standalone staging service for the GPT candidate.

This process intentionally DOES NOT start production main.py, Watch, collectors,
backfills, alerts, or trading jobs.  It only exposes the candidate AI Telegram
commands and read-only analysis tools against the configured database.

Use a dedicated staging Telegram bot token. Telegram supports one webhook per
bot token, so the production bot token must never be shared with this service.
"""

from __future__ import annotations

import asyncio
import os

from aiohttp import web
from telegram import Update
from telegram.ext import ApplicationBuilder

import ai_agent
import ai_telegram

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
PUBLIC_URL = os.getenv("PUBLIC_URL", "").strip().rstrip("/")
PORT = int(os.getenv("PORT", "10000"))


async def health(_: web.Request) -> web.Response:
    state = ai_agent.status()
    return web.json_response(
        {
            "ok": True,
            "service": "crypto-ai-agent-candidate",
            "mode": "read_only",
            "openai_configured": bool(state.get("configured")),
            "model": state.get("model"),
            "tools": state.get("tools") or [],
        }
    )


async def telegram_webhook(request: web.Request) -> web.Response:
    bot_app = request.app["bot_app"]
    try:
        payload = await request.json()
        update = Update.de_json(payload, bot_app.bot)
        if update is not None:
            # Application.start() consumes update_queue in the background. Queue
            # the update and return to Telegram immediately instead of making the
            # webhook HTTP request wait for a potentially long model response.
            await bot_app.update_queue.put(update)
    except Exception as exc:
        print(f"[ai-candidate] webhook error: {exc!r}", flush=True)
        return web.Response(status=400, text="bad update")
    return web.Response(text="ok")


async def start_web_server(bot_app) -> web.AppRunner:
    app = web.Application()
    app["bot_app"] = bot_app
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    app.router.add_post("/telegram", telegram_webhook)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"[ai-candidate] health server running on port {PORT}", flush=True)
    return runner


async def main() -> None:
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN for the dedicated staging bot")
    if not PUBLIC_URL:
        raise RuntimeError("Missing PUBLIC_URL for the dedicated staging service")
    if not ai_agent.AGENT.configured:
        raise RuntimeError("Missing OPENAI_API_KEY")

    bot_app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    ai_telegram.register_ai_handlers(bot_app)

    await bot_app.initialize()
    await bot_app.start()

    webhook_url = f"{PUBLIC_URL}/telegram"
    await bot_app.bot.delete_webhook(drop_pending_updates=True)
    await bot_app.bot.set_webhook(url=webhook_url, drop_pending_updates=True)
    print(f"[ai-candidate] staging webhook set to {webhook_url}", flush=True)

    runner = await start_web_server(bot_app)

    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await bot_app.bot.delete_webhook(drop_pending_updates=False)
        await bot_app.stop()
        await bot_app.shutdown()
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
