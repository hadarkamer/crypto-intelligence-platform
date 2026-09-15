"""Disabled-by-default fan-out of delivered bot notifications to LOCAL paper.

This adapter has no provider, wallet or order API. It cannot create a price plan.
The producer must freeze an execution_plan in the existing notification payload.
Never parse Telegram text, infer a research threshold, or invert direction here.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from typing import Mapping

VERSION = "paper-bot-bridge-v1"
PLAN_FIELDS = {"symbol", "side", "entry", "stop", "take_profit", "at"}
SOURCES = frozenset({"dual-cvd65", "manual-formulas"})
_ID = re.compile(r"^[A-Za-z0-9_.:-]{1,100}$")


class BridgeRejected(ValueError):
    """Invalid boundary data. Never include the input in the exception."""


def _utc(value) -> datetime:
    if not isinstance(value, str) or len(value) > 40:
        raise BridgeRejected("INVALID_TIME")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BridgeRejected("INVALID_TIME") from exc
    if result.utcoffset() is None:
        raise BridgeRejected("INVALID_TIME")
    return result.astimezone(timezone.utc)


def signal_from_intent(intent: dict, source: str, *, now: datetime) -> dict | None:
    """Validate lineage, then COPY only a complete explicit six-field plan.

    Return None when the producer has not supplied a plan. The ID is bound to
    the durable notification identity, not the prices, retries or Telegram ID.
    The final notification direction is authoritative (no Max-Pain inversion).
    """
    if source not in SOURCES or not isinstance(intent, dict):
        raise BridgeRejected("INVALID_SOURCE")
    identity, payload = intent.get("intent_id"), intent.get("payload")
    if not isinstance(identity, str) or not _ID.fullmatch(identity) or not isinstance(payload, dict):
        raise BridgeRejected("INVALID_INTENT")
    plan = payload.get("execution_plan")
    if plan is None:
        return None
    if not isinstance(plan, dict) or set(plan) != PLAN_FIELDS:
        raise BridgeRejected("INVALID_PLAN")
    if any(not isinstance(plan[key], str) for key in PLAN_FIELDS):
        raise BridgeRejected("INVALID_PLAN")
    if source == "dual-cvd65":
        anchor = payload.get("observation")
        if not isinstance(anchor, dict) or anchor.get("status") != "MATCH":
            raise BridgeRejected("INVALID_ANCHOR")
        source_at = payload.get("source_at_utc")
    else:
        anchor, source_at = payload, payload.get("event_time")
    if plan["symbol"] != anchor.get("symbol") or plan["side"] != anchor.get("direction"):
        raise BridgeRejected("CONTRADICTING_PLAN")
    if now.utcoffset() is None:
        raise BridgeRejected("INVALID_CLOCK")
    if _utc(plan["at"]) != _utc(source_at) or _utc(plan["at"]) > now:
        raise BridgeRejected("CONTRADICTING_TIME")
    expiry = intent.get("expires_at")
    # The existing publisher owns expiry; this bridge does not invent a TTL.
    if isinstance(expiry, datetime):
        if expiry.utcoffset() is None:
            raise BridgeRejected("INVALID_TIME")
    else:
        expiry = _utc(expiry)
    if expiry <= now:
        raise BridgeRejected("EXPIRED_INTENT")
    encoded = json.dumps([source, identity], separators=(",", ":"))
    event = {"kind": "SIGNAL", "event_id": "bot." + hashlib.sha256(encoded.encode()).hexdigest(), **plan}
    from paper_execution_feed import signal_message
    return signal_message(event)


def _private_path(value: str) -> Path:
    """Configured absolute local path only, under a private real directory."""
    if not isinstance(value, str) or len(value) > 4096:
        raise BridgeRejected("INVALID_PATH")
    path = Path(value)
    if not path.is_absolute() or not path.name.endswith(".paper.jsonl"):
        raise BridgeRejected("INVALID_PATH")
    parent = path.parent
    if parent.resolve(strict=True) != parent or path.is_symlink():
        raise BridgeRejected("INVALID_PATH")
    meta = parent.stat()
    if not stat.S_ISDIR(meta.st_mode) or meta.st_mode & 0o077 or meta.st_uid != os.geteuid():
        raise BridgeRejected("PRIVATE_DIRECTORY_REQUIRED")
    # Keep runtime plans outside tracked source directories.
    root = Path(__file__).resolve().parent
    if path.is_relative_to(root):
        raise BridgeRejected("RUNTIME_PATH_INSIDE_SOURCE")
    if path.exists() and path.stat().st_uid != os.geteuid():
        raise BridgeRejected("FOREIGN_INBOX")
    return path


def _publish(intent: dict, source: str, inbox: str) -> str:
    path = _private_path(inbox)
    event = signal_from_intent(intent, source, now=datetime.now(timezone.utc))
    if event is None:
        return "WAITING_FOR_PRICES"
    from paper_execution_feed import publish_signal
    publish_signal(path, event)
    return "PUBLISHED"


async def forward_delivered(intent: dict, *, source: str,
                            environ: Mapping[str, str] | None = None) -> dict:
    """Called ONLY after Telegram confirms delivery; never changes its outcome.

    Opt in using PAPER_BOT_BRIDGE=paper and a private PAPER_BOT_INBOX path.
    There is no background task on import and no live/testnet option. This
    prototype is a best-effort paper fan-out, NOT an atomic dual-system commit.
    A shutdown between Telegram and this append may miss a paper record. Do not
    promote this delivery hook unchanged into a real-money execution path.
    """
    env = os.environ if environ is None else environ
    mode = env.get("PAPER_BOT_BRIDGE", "")
    if mode in ("", "off"):
        return {"mode": "paper", "status": "DISABLED", "exchange_orders_sent": 0}
    if mode != "paper" or not env.get("PAPER_BOT_INBOX") or source not in SOURCES:
        return {"mode": "paper", "status": "BLOCKED_CONFIG", "exchange_orders_sent": 0}
    try:
        status = await asyncio.to_thread(_publish, intent, source, env["PAPER_BOT_INBOX"])
    except asyncio.CancelledError:
        raise
    except Exception:
        # No raw payload, credential, filename or exception text enters logs.
        status = "BLOCKED_INPUT_OR_STORAGE"
    return {"mode": "paper", "status": status, "exchange_orders_sent": 0}
