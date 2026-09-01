"""Standalone staging entrypoint for the production analytical AI surface.

This service uses a dedicated Telegram bot and the read-only production AI
tools.  It deliberately does not start Watch, collectors, alert delivery,
Research writers or the outcome worker.  Its purpose is to verify an integration
branch before that branch is eligible for the existing production bot.
"""

from __future__ import annotations

import asyncio
import os

from aiohttp import web
from telegram import Update
from telegram.ext import ApplicationBuilder

import ai_agent
import ai_alert_research
import ai_telegram
import research_outcome_worker
import research_experimental_preview_staging_registration as preview_staging
import research_formula_store
import research_formula_worker


TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
PUBLIC_URL = os.getenv("PUBLIC_URL", "").strip().rstrip("/")
PORT = int(os.getenv("PORT", "10000"))


async def health(_: web.Request) -> web.Response:
    state = ai_agent.status()
    try:
        archive = await asyncio.to_thread(ai_alert_research.archive_status)
    except Exception as exc:
        archive = {
            "configured": bool(os.getenv("RESEARCH_DATABASE_URL"))
            or os.getenv("RESEARCH_USE_PRIMARY_DATABASE", "").strip().lower()
            in {"1", "true", "yes", "on"},
            "schema_present": False,
            "error": f"{type(exc).__name__}: {exc}",
        }
    return web.json_response(
        {
            "ok": True,
            "service": "crypto-ai-production-analytics-test",
            "mode": "analysis_read_only",
            "openai_configured": bool(state.get("configured")),
            "model": state.get("model"),
            "tools": state.get("tools") or [],
            "research_archive": archive,
            "outcome_reader": research_outcome_worker.WORKER.status(),
            "formula_registry": await asyncio.to_thread(
                research_formula_store.schema_status
            ),
            "formula_research": research_formula_worker.WORKER.status(),
            "writers_started_by_this_process": False,
        }
    )


async def telegram_webhook(request: web.Request) -> web.Response:
    bot_app = request.app["bot_app"]
    try:
        payload = await request.json()
        update = Update.de_json(payload, bot_app.bot)
        if update is not None:
            await bot_app.update_queue.put(update)
    except Exception as exc:
        print(f"[ai-analytics-test] webhook error: {exc!r}", flush=True)
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
    print(f"[ai-analytics-test] health server running on port {PORT}", flush=True)
    return runner


async def main() -> None:
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN for the dedicated staging bot")
    if not PUBLIC_URL:
        raise RuntimeError("Missing PUBLIC_URL for the dedicated staging service")
    if not ai_agent.AGENT.configured:
        raise RuntimeError("Missing OPENAI_API_KEY")

    bot_app = (
        ApplicationBuilder()
        .token(TELEGRAM_BOT_TOKEN)
        .connect_timeout(30.0)
        .read_timeout(30.0)
        .write_timeout(30.0)
        .pool_timeout(30.0)
        .build()
    )
    ai_telegram.register_ai_handlers(bot_app)

    await bot_app.initialize()
    await bot_app.start()
    preview_registration = (
        preview_staging.REGISTRATION.bind_disabled_runtime_bot(bot_app.bot)
    )
    print(
        "[ai-analytics-test] PREVIEW staging binding prepared; "
        f"enabled={preview_registration['enabled']} "
        f"registered={preview_registration['connector_registered']} "
        f"chat_configured={preview_registration['test_chat_configured']}",
        flush=True,
    )

    webhook_url = f"{PUBLIC_URL}/telegram"
    await bot_app.bot.delete_webhook(drop_pending_updates=False)
    await bot_app.bot.set_webhook(url=webhook_url, drop_pending_updates=False)
    print(f"[ai-analytics-test] staging webhook set to {webhook_url}", flush=True)

    runner = await start_web_server(bot_app)
    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        preview_staging.REGISTRATION.unbind_runtime_bot(bot_app.bot)
        await bot_app.bot.delete_webhook(drop_pending_updates=False)
        await bot_app.stop()
        await bot_app.shutdown()
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
