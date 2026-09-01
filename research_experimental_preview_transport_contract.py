"""Fail-closed transport envelopes for verified PREVIEW_ONLY decisions.

This pure boundary prepares the exact payload that a future private test-chat
connector could consume.  It has a second, independent authorization policy
and remains disconnected: no Telegram client, token, environment, database,
worker, scheduler, production command, Stage 6 or LIVE path is available here.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, Iterable, Mapping, Sequence

import research_experimental_preview_contract as preview_contract
import research_experimental_preview_storage_contract as preview_storage


TRANSPORT_CONTRACT_VERSION = "preview-test-chat-transport-v1-disconnected"
IDEMPOTENCY_POLICY_VERSION = "preview-transport-chat-decision-v1"
MODE = "PREVIEW_TEST_CHAT_TRANSPORT_ENVELOPE_ONLY"
TRANSPORT = "TELEGRAM_PRIVATE_TEST_CHAT_UNCONNECTED"

ENVELOPE_PREPARED = "TRANSPORT_ENVELOPE_PREPARED"
SUPPRESSED = "TRANSPORT_ENVELOPE_SUPPRESSED"

_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_POLICY_KEYS = {
    "enabled",
    "kill_switch_engaged",
    "owner_transport_approved",
    "test_chat_id",
}


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


def _hex(value: Any, *, name: str) -> str:
    normalized = str(value or "").strip().lower()
    if not _HEX_64.fullmatch(normalized):
        raise ValueError(f"{name} must be a lowercase 64-character SHA-256 id")
    return normalized


def _chat_id(value: Any, *, optional: bool = False) -> int | None:
    if value is None and optional:
        return None
    if type(value) is not int or value == 0:
        raise ValueError("transport test_chat_id must be a non-zero integer")
    return value


def _policy(value: Mapping[str, Any] | None) -> Dict[str, Any]:
    supplied = {} if value is None else value
    if not isinstance(supplied, Mapping):
        raise ValueError("preview transport policy must be an object")
    unknown = sorted(set(supplied).difference(_POLICY_KEYS))
    if unknown:
        raise ValueError(
            "preview transport policy contains unknown fields: "
            + ", ".join(unknown)
        )
    enabled = supplied.get("enabled", False)
    kill_switch = supplied.get("kill_switch_engaged", True)
    owner_approved = supplied.get("owner_transport_approved", False)
    for name, item in (
        ("enabled", enabled),
        ("kill_switch_engaged", kill_switch),
        ("owner_transport_approved", owner_approved),
    ):
        if type(item) is not bool:
            raise ValueError(f"preview transport policy {name} must be boolean")
    return {
        "enabled": enabled,
        "kill_switch_engaged": kill_switch,
        "owner_transport_approved": owner_approved,
        "test_chat_id": _chat_id(
            supplied.get("test_chat_id"), optional=True
        ),
    }


def _transport_key(*, chat_id: int, preview_decision_id: str) -> str:
    return _hash(
        {
            "idempotency_policy_version": IDEMPOTENCY_POLICY_VERSION,
            "chat_id": chat_id,
            "preview_decision_id": preview_decision_id,
        }
    )


def prepare_private_test_chat_envelopes(
    plan: Mapping[str, Any],
    *,
    policy: Mapping[str, Any] | None = None,
    existing_transport_keys: Iterable[str] = (),
) -> Dict[str, Any]:
    """Return verified test-chat envelopes without registering a connector."""

    prepared = preview_storage.prepare_preview_append_only_records(plan)
    batch = prepared["batch_record"]
    resolved_policy = _policy(policy)
    if isinstance(existing_transport_keys, (str, bytes, bytearray, Mapping)):
        raise ValueError("existing_transport_keys must be an iterable of ids")
    existing = {
        _hex(value, name="existing transport key")
        for value in existing_transport_keys
    }

    preview_policy = json.loads(batch["batch_payload_json"])["policy"]
    preview_test_chats = preview_policy.get("test_chat_ids")
    if not isinstance(preview_test_chats, Sequence) or isinstance(
        preview_test_chats, (str, bytes, bytearray)
    ):
        raise ValueError("verified PREVIEW_ONLY test chat allowlist is invalid")

    global_blockers = []
    if not resolved_policy["enabled"]:
        global_blockers.append("preview transport feature flag is disabled")
    if resolved_policy["kill_switch_engaged"]:
        global_blockers.append("preview transport kill switch is engaged")
    if not resolved_policy["owner_transport_approved"]:
        global_blockers.append("owner approval for preview transport is absent")
    if resolved_policy["test_chat_id"] is None:
        global_blockers.append("private test chat is not configured")
    elif resolved_policy["test_chat_id"] != batch["chat_id"]:
        global_blockers.append("transport destination differs from preview chat")
    if batch["chat_id"] not in preview_test_chats:
        global_blockers.append("transport destination is outside preview allowlist")
    if batch["stage5_status"] != "WAITING_DATA":
        global_blockers.append("PREVIEW_ONLY transport closes when Stage 5 is READY")
    if batch["route"] != "TEST_ALLOWLIST":
        global_blockers.append("preview route is not TEST_ALLOWLIST")
    if batch["public_opt_in"] is not False:
        global_blockers.append("public opt-in is forbidden")
    if batch["stage6_activated"] is not False:
        global_blockers.append("Stage 6 must remain inactive")

    decisions = []
    envelopes = []
    family_ids = []
    for record in prepared["decision_records"]:
        family_ids.append(record["formula_family_id"])
        blockers = list(global_blockers)
        if record["status"] != preview_contract.PREVIEW_SIMULATED_ELIGIBLE:
            blockers.append("source PREVIEW_ONLY decision is suppressed")
        transport_key = _transport_key(
            chat_id=batch["chat_id"],
            preview_decision_id=record["preview_decision_id"],
        )
        if transport_key in existing:
            blockers.append("preview transport idempotency key already exists")
        blockers = list(dict.fromkeys(blockers))
        status = ENVELOPE_PREPARED if not blockers else SUPPRESSED

        envelope_id = None
        if status == ENVELOPE_PREPARED:
            payload = json.loads(record["decision_payload_json"])
            envelope_payload = {
                "transport_contract_version": TRANSPORT_CONTRACT_VERSION,
                "transport": TRANSPORT,
                "chat_id": batch["chat_id"],
                "text": payload["text"],
                "parse_mode": None,
                "disable_web_page_preview": True,
                "protect_content": True,
                "preview_batch_id": batch["preview_batch_id"],
                "preview_decision_id": record["preview_decision_id"],
                "source_audit_decision_id": record[
                    "source_audit_decision_id"
                ],
                "formula_family_id": record["formula_family_id"],
                "representative_snapshot_id": record[
                    "representative_snapshot_id"
                ],
                "preview_message_sha256": record[
                    "preview_message_sha256"
                ],
                "transport_key": transport_key,
                "public_opt_in": False,
                "stage6_activated": False,
                "research_evidence_effect": "NONE",
            }
            envelope_id = _hash(envelope_payload)
            envelopes.append(
                {**envelope_payload, "transport_envelope_id": envelope_id}
            )

        decision_payload = {
            "transport_contract_version": TRANSPORT_CONTRACT_VERSION,
            "preview_decision_id": record["preview_decision_id"],
            "transport_key": transport_key,
            "transport_envelope_id": envelope_id,
            "status": status,
            "blockers": blockers,
            "research_evidence_effect": "NONE",
        }
        decisions.append(
            {
                **decision_payload,
                "transport_decision_id": _hash(decision_payload),
            }
        )

    if len(set(family_ids)) != len(family_ids):
        raise ValueError("PREVIEW_ONLY transport requires one decision per family")
    transport_keys = [decision["transport_key"] for decision in decisions]
    if len(set(transport_keys)) != len(transport_keys):
        raise ValueError("PREVIEW_ONLY transport keys contain duplicates")

    batch_payload = {
        "transport_contract_version": TRANSPORT_CONTRACT_VERSION,
        "preview_batch_id": batch["preview_batch_id"],
        "policy": resolved_policy,
        "transport_decision_ids": [
            decision["transport_decision_id"] for decision in decisions
        ],
    }
    return {
        "transport_contract_version": TRANSPORT_CONTRACT_VERSION,
        "idempotency_policy_version": IDEMPOTENCY_POLICY_VERSION,
        "mode": MODE,
        "transport_batch_id": _hash(batch_payload),
        "preview_batch_id": batch["preview_batch_id"],
        "policy": resolved_policy,
        "decisions_considered": len(decisions),
        "transport_envelopes_prepared": len(envelopes),
        "suppressed": sum(
            decision["status"] == SUPPRESSED for decision in decisions
        ),
        "connector_registered": False,
        "transport_connected": False,
        "public_opt_in": False,
        "stage6_activated": False,
        "delivery_attempts": 0,
        "telegram_api_calls": 0,
        "database_writes": 0,
        "research_evidence_writes": 0,
        "research_evidence_effect": "NONE",
        "delivery_channel": "NONE",
        "live_effect": "NONE",
        "decisions": decisions,
        "transport_envelopes": envelopes,
    }
