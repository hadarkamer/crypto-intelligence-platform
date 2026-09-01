"""Disabled PREVIEW_ONLY registration for the dedicated staging Bot.

The registration may retain the staging Bot interface so lifecycle wiring can
be verified, but it creates the dispatcher in runtime-unregistered mode and
exposes no dispatch method.  It adds no command, handler, scheduler, chat id,
environment flag, token lookup or Telegram API operation.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict

import research_experimental_preview_telegram_dispatcher as dispatcher_module


REGISTRATION_VERSION = "preview-staging-registration-v1-disabled"
MODE = "PREVIEW_STAGING_RUNTIME_BOT_BOUND_DISABLED"
BASE_COMMIT = "a49bd9d0bbba19426e9ec361014520d257510acf"
LIFECYCLE_STATUS = "STAGING_BOUND_DISABLED_NOT_ACTIVATED"


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


class DisabledStagingPreviewRegistration:
    """Idempotently bind one runtime Bot without granting dispatch authority."""

    def __init__(self) -> None:
        self._bot: Any | None = None
        self._dispatcher: dispatcher_module.PreviewTelegramDispatcher | None = None
        self._binding_id: str | None = None

    def bind_disabled_runtime_bot(self, bot: Any) -> Dict[str, Any]:
        if bot is None or not callable(getattr(bot, "send_message", None)):
            raise ValueError("staging Bot must expose an async send_message method")
        if self._bot is not None and self._bot is not bot:
            raise RuntimeError("a different staging Bot is already bound")
        if self._bot is None:
            self._bot = bot
            self._dispatcher = dispatcher_module.PreviewTelegramDispatcher(bot)
            self._binding_id = _hash(
                {
                    "registration_version": REGISTRATION_VERSION,
                    "mode": MODE,
                    "base_commit": BASE_COMMIT,
                    "client_classification": (
                        dispatcher_module.RUNTIME_BOT_UNREGISTERED
                    ),
                    "connector_registered": False,
                    "enabled": False,
                    "kill_switch_engaged": True,
                }
            )
        return self.status()

    def unbind_runtime_bot(self, bot: Any) -> Dict[str, Any]:
        if self._bot is not None and self._bot is not bot:
            raise RuntimeError("cannot unbind a different staging Bot")
        self._dispatcher = None
        self._bot = None
        self._binding_id = None
        return self.status()

    def status(self) -> Dict[str, Any]:
        bound = self._bot is not None
        return {
            "registration_version": REGISTRATION_VERSION,
            "mode": MODE,
            "owner": dispatcher_module.OWNER,
            "lifecycle_status": LIFECYCLE_STATUS,
            "base_commit": BASE_COMMIT,
            "binding_id": self._binding_id,
            "runtime_bot_bound": bound,
            "client_classification": (
                dispatcher_module.RUNTIME_BOT_UNREGISTERED if bound else "NONE"
            ),
            "enabled": False,
            "kill_switch_engaged": True,
            "test_chat_configured": False,
            "connector_registered": False,
            "activation_allowed": False,
            "handler_registered": False,
            "scheduler_registered": False,
            "worker_registered": False,
            "public_opt_in": False,
            "stage6_activated": False,
            "delivery_attempts": 0,
            "telegram_api_calls": 0,
            "database_writes": 0,
            "research_evidence_writes": 0,
            "research_evidence_effect": "NONE",
            "delivery_channel": "NONE",
            "live_effect": "NONE",
        }


REGISTRATION = DisabledStagingPreviewRegistration()
