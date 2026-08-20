"""Isolated staging entrypoint for the GPT candidate.

Run this file only in a dedicated staging Render service. It leaves production
main.py unchanged. The wrapper registers the candidate AI Telegram handlers on
the ApplicationBuilder instance and then starts the existing bot runtime.

IMPORTANT: do not use the production Telegram bot token in a second live service;
Telegram supports one webhook per bot token. Use a dedicated staging bot token.
"""

from __future__ import annotations

import asyncio

from telegram.ext import ApplicationBuilder

import ai_telegram


_original_build = ApplicationBuilder.build


def _build_with_candidate_ai(self):
    app = _original_build(self)
    ai_telegram.register_ai_handlers(app)
    return app


ApplicationBuilder.build = _build_with_candidate_ai

import main as core_main  # noqa: E402  (import after candidate hook is intentional)


if __name__ == "__main__":
    asyncio.run(core_main.main())
