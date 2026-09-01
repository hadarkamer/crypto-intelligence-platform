"""Unregistered Telegram request adapter for PREVIEW_ONLY transport envelopes.

The adapter maps verified envelopes to the exact keyword arguments expected by
``python-telegram-bot`` 21.9, including deterministic message chunking.  It
does not accept a bot or token and contains no API invocation.  Every request
remains blocked until a separate future connector-registration step.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Iterable, Mapping

import research_experimental_preview_transport_contract as transport_contract


ADAPTER_VERSION = "preview-telegram-adapter-v1-unregistered"
MODE = "PREVIEW_TELEGRAM_REQUESTS_PREPARED_NOT_SENT"
REGISTRATION_STATE = "UNREGISTERED"
REQUEST_STATUS = "BLOCKED_CONNECTOR_UNREGISTERED"
TELEGRAM_MESSAGE_LIMIT = 3900


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


def _chunks(text: str, *, limit: int = TELEGRAM_MESSAGE_LIMIT) -> list[str]:
    value = str(text or "").strip()
    if not value:
        raise ValueError("PREVIEW_ONLY Telegram text may not be empty")
    if type(limit) is not int or limit <= 0 or limit > 4096:
        raise ValueError("Telegram message limit must be between 1 and 4096")
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


def _telegram_requests(envelope: Mapping[str, Any]) -> list[Dict[str, Any]]:
    text = str(envelope["text"])
    chunks = _chunks(text)
    requests = []
    for index, chunk in enumerate(chunks, start=1):
        request_key = _hash(
            {
                "adapter_version": ADAPTER_VERSION,
                "transport_key": envelope["transport_key"],
                "chunk_index": index,
                "chunk_count": len(chunks),
                "chunk_sha256": hashlib.sha256(
                    chunk.encode("utf-8")
                ).hexdigest(),
            }
        )
        kwargs = {
            "chat_id": envelope["chat_id"],
            "text": chunk,
            "parse_mode": envelope["parse_mode"],
            "disable_web_page_preview": envelope[
                "disable_web_page_preview"
            ],
            "protect_content": envelope["protect_content"],
        }
        request_payload = {
            "adapter_version": ADAPTER_VERSION,
            "transport_envelope_id": envelope["transport_envelope_id"],
            "transport_key": envelope["transport_key"],
            "request_key": request_key,
            "method": "Bot.send_message",
            "kwargs": kwargs,
            "chunk_index": index,
            "chunk_count": len(chunks),
            "status": REQUEST_STATUS,
            "research_evidence_effect": "NONE",
        }
        requests.append(
            {**request_payload, "adapter_request_id": _hash(request_payload)}
        )
    return requests


def prepare_unregistered_telegram_requests(
    plan: Mapping[str, Any],
    *,
    transport_policy: Mapping[str, Any] | None = None,
    existing_transport_keys: Iterable[str] = (),
) -> Dict[str, Any]:
    """Prepare SDK-compatible calls while keeping the connector absent."""

    transport = transport_contract.prepare_private_test_chat_envelopes(
        plan,
        policy=transport_policy,
        existing_transport_keys=existing_transport_keys,
    )
    requests = []
    for envelope in transport["transport_envelopes"]:
        requests.extend(_telegram_requests(envelope))
    request_keys = [request["request_key"] for request in requests]
    if len(set(request_keys)) != len(request_keys):
        raise ValueError("PREVIEW_ONLY Telegram request keys contain duplicates")

    batch_payload = {
        "adapter_version": ADAPTER_VERSION,
        "transport_batch_id": transport["transport_batch_id"],
        "registration_state": REGISTRATION_STATE,
        "adapter_request_ids": [
            request["adapter_request_id"] for request in requests
        ],
    }
    return {
        "adapter_version": ADAPTER_VERSION,
        "mode": MODE,
        "adapter_batch_id": _hash(batch_payload),
        "transport_batch_id": transport["transport_batch_id"],
        "registration_state": REGISTRATION_STATE,
        "connector_registered": False,
        "activation_allowed": False,
        "transport_connected": False,
        "transport_envelopes_received": len(
            transport["transport_envelopes"]
        ),
        "adapter_requests_prepared": len(requests),
        "adapter_requests_blocked": len(requests),
        "public_opt_in": False,
        "stage6_activated": False,
        "delivery_attempts": 0,
        "telegram_api_calls": 0,
        "database_writes": 0,
        "research_evidence_writes": 0,
        "research_evidence_effect": "NONE",
        "delivery_channel": "NONE",
        "live_effect": "NONE",
        "requests": requests,
    }
