"""Disconnected PREVIEW_ONLY authorization for a single test-chat route.

This pure contract may simulate a pre-validation preview only when the normal
Experimental gate is suppressed solely because Stage 5 is ``WAITING_DATA``.
It cannot send, persist, opt in a public chat, add research evidence, approve a
formula or affect LIVE.  Once Stage 5 is READY, the preview route closes and the
normal Experimental review remains authoritative.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, Iterable, Mapping, Sequence

import research_experimental_delivery_gate as delivery_gate
import research_experimental_storage_contract as storage_contract


POLICY_VERSION = "experimental-preview-only-v1-disconnected"
IDEMPOTENCY_POLICY_VERSION = "preview-test-chat-family-snapshot-v1"
MODE = "PREVIEW_ONLY_DRY_RUN"
LABEL = "🧪 PREVIEW טרום־אימות — אינו ראיה ואינו המלצת מסחר"

PREVIEW_SIMULATED_ELIGIBLE = "PREVIEW_SIMULATED_ELIGIBLE"
PREVIEW_SUPPRESSED = "PREVIEW_SUPPRESSED"

_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_POLICY_KEYS = {
    "enabled",
    "kill_switch_engaged",
    "owner_preview_approved",
    "test_chat_ids",
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


def _chat_id(value: Any) -> int:
    if type(value) is not int or value == 0:
        raise ValueError("preview chat ids must be non-zero integers")
    return value


def _chat_ids(value: Any) -> tuple[int, ...]:
    if value is None:
        return ()
    if not isinstance(value, Sequence) or isinstance(
        value, (str, bytes, bytearray)
    ):
        raise ValueError("preview test_chat_ids must be a list")
    normalized = tuple(sorted({_chat_id(item) for item in value}))
    if len(normalized) != len(value):
        raise ValueError("preview test_chat_ids contains duplicates")
    return normalized


def _policy(value: Mapping[str, Any] | None) -> Dict[str, Any]:
    supplied = {} if value is None else value
    if not isinstance(supplied, Mapping):
        raise ValueError("preview policy must be an object")
    unknown = sorted(set(supplied).difference(_POLICY_KEYS))
    if unknown:
        raise ValueError(
            "preview policy contains unknown fields: " + ", ".join(unknown)
        )
    enabled = supplied.get("enabled", False)
    kill_switch = supplied.get("kill_switch_engaged", True)
    owner_approved = supplied.get("owner_preview_approved", False)
    for name, item in (
        ("enabled", enabled),
        ("kill_switch_engaged", kill_switch),
        ("owner_preview_approved", owner_approved),
    ):
        if type(item) is not bool:
            raise ValueError(f"preview policy {name} must be boolean")
    return {
        "enabled": enabled,
        "kill_switch_engaged": kill_switch,
        "owner_preview_approved": owner_approved,
        "test_chat_ids": _chat_ids(supplied.get("test_chat_ids")),
    }


def _preview_key(*, chat_id: int, family_id: str, snapshot_id: str) -> str:
    return _hash(
        {
            "policy_version": IDEMPOTENCY_POLICY_VERSION,
            "chat_id": chat_id,
            "formula_family_id": family_id,
            "representative_snapshot_id": snapshot_id,
        }
    )


def plan_preview_only(
    gate_plan: Mapping[str, Any],
    *,
    policy: Mapping[str, Any] | None = None,
    existing_preview_keys: Iterable[str] = (),
) -> Dict[str, Any]:
    """Return a test-chat-only preview simulation with no delivery authority."""

    verified = storage_contract.prepare_append_only_records(gate_plan)
    resolved_policy = _policy(policy)
    if isinstance(existing_preview_keys, (str, bytes, bytearray, Mapping)):
        raise ValueError("existing_preview_keys must be an iterable of ids")
    existing = {
        _hex(value, name="existing preview key") for value in existing_preview_keys
    }
    batch = verified["batch_record"]
    stage5_status = str(batch["stage5_status"])
    chat_id = _chat_id(batch["chat_id"])
    route = str(batch["route"])
    gate_policy = gate_plan.get("policy")
    if not isinstance(gate_policy, Mapping):
        raise ValueError("verified Experimental gate policy is unavailable")
    output_policy = {
        **resolved_policy,
        "test_chat_ids": list(resolved_policy["test_chat_ids"]),
    }

    global_blockers = []
    if not resolved_policy["enabled"]:
        global_blockers.append("PREVIEW_ONLY feature flag is disabled")
    if resolved_policy["kill_switch_engaged"]:
        global_blockers.append("PREVIEW_ONLY kill switch is engaged")
    if not resolved_policy["owner_preview_approved"]:
        global_blockers.append("owner approval for the test-chat preview is absent")
    if chat_id not in resolved_policy["test_chat_ids"]:
        global_blockers.append("chat is not in the PREVIEW_ONLY test allowlist")
    if stage5_status != "WAITING_DATA":
        global_blockers.append(
            "Stage 5 is not WAITING_DATA; use the normal Experimental review"
        )
    if route != "TEST_ALLOWLIST":
        global_blockers.append("source gate route is not TEST_ALLOWLIST")
    if gate_policy.get("allow_opt_in") is not False or gate_policy.get(
        "opted_in_chat_ids"
    ) != []:
        global_blockers.append("public opt-in is forbidden in PREVIEW_ONLY")
    if gate_policy.get("enabled") is not True:
        global_blockers.append("source Experimental gate is not explicitly enabled")
    if gate_policy.get("kill_switch_engaged") is not False:
        global_blockers.append("source Experimental kill switch is engaged")
    if gate_policy.get("cooldown_seconds") is None:
        global_blockers.append("source Experimental cooldown is not configured")

    previews = []
    for record in verified["decision_records"]:
        payload = json.loads(record["decision_payload_json"])
        blockers = list(global_blockers)
        source_blockers = payload.get("blockers")
        if source_blockers != ["Stage 5 is not READY"]:
            blockers.append(
                "source gate has blockers beyond the unfinished Stage 5 comparison"
            )
        if record["status"] != delivery_gate.SUPPRESSED:
            blockers.append("source gate decision is not safely suppressed")
        key = _preview_key(
            chat_id=chat_id,
            family_id=record["formula_family_id"],
            snapshot_id=record["representative_snapshot_id"],
        )
        if key in existing:
            blockers.append("preview idempotency key already exists")
        blockers = list(dict.fromkeys(blockers))
        status = (
            PREVIEW_SIMULATED_ELIGIBLE if not blockers else PREVIEW_SUPPRESSED
        )
        text = f"{LABEL}\n\n{payload['text']}"
        preview_payload = {
            "policy_version": POLICY_VERSION,
            "idempotency_policy_version": IDEMPOTENCY_POLICY_VERSION,
            "preview_policy": output_policy,
            "source_audit_batch_id": batch["audit_batch_id"],
            "source_audit_decision_id": record["audit_decision_id"],
            "status": status,
            "blockers": blockers,
            "chat_id": chat_id,
            "route": "TEST_ALLOWLIST",
            "formula_family_id": record["formula_family_id"],
            "representative_snapshot_id": record[
                "representative_snapshot_id"
            ],
            "preview_key": key,
            "preview_message_sha256": hashlib.sha256(
                text.encode("utf-8")
            ).hexdigest(),
            "text": text,
            "research_evidence_effect": "NONE",
        }
        previews.append(
            {
                **preview_payload,
                "preview_decision_id": _hash(preview_payload),
            }
        )

    batch_payload = {
        "policy_version": POLICY_VERSION,
        "source_audit_batch_id": batch["audit_batch_id"],
        "policy": output_policy,
        "preview_decision_ids": [
            preview["preview_decision_id"] for preview in previews
        ],
    }
    return {
        "policy_version": POLICY_VERSION,
        "idempotency_policy_version": IDEMPOTENCY_POLICY_VERSION,
        "mode": MODE,
        "preview_batch_id": _hash(batch_payload),
        "source_audit_batch_id": batch["audit_batch_id"],
        "stage5_status": stage5_status,
        "chat_id": chat_id,
        "route": "TEST_ALLOWLIST" if route == "TEST_ALLOWLIST" else "NONE",
        "policy": output_policy,
        "families_considered": len(previews),
        "preview_simulated_eligible": sum(
            preview["status"] == PREVIEW_SIMULATED_ELIGIBLE
            for preview in previews
        ),
        "preview_suppressed": sum(
            preview["status"] == PREVIEW_SUPPRESSED for preview in previews
        ),
        "public_opt_in": False,
        "stage6_activated": False,
        "delivery_attempts": 0,
        "telegram_api_calls": 0,
        "database_writes": 0,
        "research_evidence_writes": 0,
        "research_evidence_effect": "NONE",
        "delivery_channel": "NONE",
        "live_effect": "NONE",
        "previews": previews,
    }
