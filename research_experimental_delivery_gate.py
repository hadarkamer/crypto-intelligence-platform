"""Side-effect-free preparation for the controlled Experimental route.

This module is deliberately disconnected from external delivery, workers, environment
variables and the database.  It consumes fingerprint-verified
``EvidenceSnapshot`` values plus already-persisted relevance decisions and
builds an auditable *simulation* of the Experimental delivery gate.

Even an eligible result cannot send anything: every output keeps
``delivery_channel=NONE``, ``delivery_attempts=0`` and ``live_effect=NONE``.
The eventual Stage 6 integration must bind these decisions to a separate,
durable Experimental store only after Stage 5 is READY and the owner approves
deployment/activation.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import re
from typing import Any, Dict, Iterable, Mapping, Sequence

import research_evidence_contract as evidence_contract
import research_evidence_telegram_renderer as evidence_renderer
import research_formula_relevance


POLICY_VERSION = "formula-experimental-delivery-gate-v1-disabled-preparation"
IDEMPOTENCY_POLICY_VERSION = "experimental-chat-family-snapshot-v1"
COOLDOWN_POLICY_VERSION = "experimental-chat-family-cooldown-v1"
MODE = "EXPERIMENTAL_GATE_DRY_RUN_ONLY"

SIMULATED_ELIGIBLE = "SIMULATED_ELIGIBLE"
SUPPRESSED = "SUPPRESSED"

_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_RELEVANCE_STATES = {
    research_formula_relevance.RELEVANT,
    research_formula_relevance.WEAKENING,
}
_POLICY_KEYS = {
    "enabled",
    "kill_switch_engaged",
    "allow_opt_in",
    "test_chat_ids",
    "opted_in_chat_ids",
    "cooldown_seconds",
}


def _utc(value: Any, *, name: str) -> datetime:
    try:
        parsed = (
            value
            if isinstance(value, datetime)
            else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be a valid timestamp") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(value: Any, *, name: str) -> str:
    return _utc(value, name=name).isoformat().replace("+00:00", "Z")


def _chat_id(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError("chat ids must be integers")
    try:
        identifier = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("chat ids must be integers") from exc
    if identifier == 0:
        raise ValueError("chat ids may not be zero")
    return identifier


def _chat_ids(value: Any, *, name: str) -> tuple[int, ...]:
    if value is None:
        return ()
    if isinstance(value, (str, bytes, bytearray, Mapping)) or not isinstance(
        value, Sequence
    ):
        raise ValueError(f"{name} must be a list of chat ids")
    normalized = tuple(sorted({_chat_id(item) for item in value}))
    if len(normalized) != len(value):
        raise ValueError(f"{name} contains duplicate chat ids")
    return normalized


def _hex(value: Any, *, name: str) -> str:
    normalized = str(value or "").strip().lower()
    if not _HEX_64.fullmatch(normalized):
        raise ValueError(f"{name} must be a lowercase 64-character SHA-256 id")
    return normalized


def _hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _policy(value: Mapping[str, Any] | None) -> Dict[str, Any]:
    supplied = {} if value is None else value
    if not isinstance(supplied, Mapping):
        raise ValueError("experimental policy must be an object")
    unknown = sorted(set(supplied).difference(_POLICY_KEYS))
    if unknown:
        raise ValueError("experimental policy contains unknown fields: " + ", ".join(unknown))
    enabled = supplied.get("enabled", False)
    kill_switch = supplied.get("kill_switch_engaged", True)
    allow_opt_in = supplied.get("allow_opt_in", False)
    for name, item in (
        ("enabled", enabled),
        ("kill_switch_engaged", kill_switch),
        ("allow_opt_in", allow_opt_in),
    ):
        if type(item) is not bool:
            raise ValueError(f"experimental policy {name} must be boolean")
    cooldown = supplied.get("cooldown_seconds")
    if cooldown is not None and (
        type(cooldown) is not int or cooldown <= 0
    ):
        raise ValueError("experimental cooldown_seconds must be a positive integer")
    test_chats = _chat_ids(supplied.get("test_chat_ids"), name="test_chat_ids")
    opted_in_chats = _chat_ids(
        supplied.get("opted_in_chat_ids"), name="opted_in_chat_ids"
    )
    if set(test_chats).intersection(opted_in_chats):
        raise ValueError("a chat may not be both TEST_ALLOWLIST and OPT_IN")
    return {
        "enabled": enabled,
        "kill_switch_engaged": kill_switch,
        "allow_opt_in": allow_opt_in,
        "test_chat_ids": test_chats,
        "opted_in_chat_ids": opted_in_chats,
        "cooldown_seconds": cooldown,
    }


def _route(chat_id: int, policy: Mapping[str, Any]) -> str:
    if chat_id in policy["test_chat_ids"]:
        return "TEST_ALLOWLIST"
    if bool(policy["allow_opt_in"]) and chat_id in policy["opted_in_chat_ids"]:
        return "OPT_IN"
    return "NONE"


def _verified_snapshots(
    values: Iterable[evidence_contract.EvidenceSnapshot | Mapping[str, Any]],
) -> list[evidence_contract.EvidenceSnapshot]:
    if isinstance(values, (str, bytes, bytearray, Mapping)):
        raise ValueError("experimental input must be an iterable of EvidenceSnapshots")
    supplied = list(values)
    return [
        evidence_contract.EvidenceSnapshot.from_dict(
            value.to_dict()
            if isinstance(value, evidence_contract.EvidenceSnapshot)
            else value
        )
        for value in supplied
    ]


def _relevance_blockers(
    *,
    snapshot: evidence_contract.EvidenceSnapshot,
    relevance: Any,
) -> list[str]:
    if not isinstance(relevance, Mapping):
        return ["matching relevance decision is unavailable"]
    blockers = []
    payload = snapshot.to_dict()
    assessment = payload["assessment"]
    if str(relevance.get("policy_version") or "") != research_formula_relevance.POLICY_VERSION:
        blockers.append("relevance policy version is incompatible")
    if str(relevance.get("snapshot_id") or "") != snapshot.snapshot_id:
        blockers.append("relevance decision is not bound to the representative snapshot")
    if str(relevance.get("compatibility") or "") != snapshot.compatibility:
        blockers.append("relevance compatibility differs from the snapshot")
    if str(relevance.get("state") or "") not in _RELEVANCE_STATES:
        blockers.append("current relevance state is not delivery-eligible")
    if relevance.get("experimental_relevance_eligible") is not True:
        blockers.append("relevance decision blocks Experimental delivery")
    if sorted(relevance.get("accepted_paths") or []) != sorted(
        assessment.get("accepted_paths") or []
    ):
        blockers.append("relevance accepted paths differ from the snapshot")
    if assessment.get("research_ready") is not True or not assessment.get(
        "accepted_paths"
    ):
        blockers.append("snapshot research acceptance is not ready")
    return blockers


def _delivery_key(
    *, chat_id: int, formula_family_id: str, snapshot_id: str
) -> str:
    return _hash(
        {
            "policy_version": IDEMPOTENCY_POLICY_VERSION,
            "chat_id": chat_id,
            "formula_family_id": formula_family_id,
            "representative_snapshot_id": snapshot_id,
        }
    )


def plan_experimental_dry_run(
    values: Iterable[evidence_contract.EvidenceSnapshot | Mapping[str, Any]],
    *,
    relevance_by_snapshot: Mapping[str, Mapping[str, Any]],
    chat_id: Any,
    stage5_status: Any,
    policy: Mapping[str, Any] | None = None,
    existing_delivery_keys: Iterable[str] = (),
    last_delivery_at_by_chat_family: Mapping[str, Any] | None = None,
    now_utc: Any,
) -> Dict[str, Any]:
    """Return a deterministic, non-delivering Experimental eligibility audit."""

    snapshots = _verified_snapshots(values)
    if not isinstance(relevance_by_snapshot, Mapping):
        raise ValueError("relevance_by_snapshot must be an object")
    resolved_policy = _policy(policy)
    identifier = _chat_id(chat_id)
    now = _utc(now_utc, name="now_utc")
    normalized_stage5 = str(stage5_status or "").strip().upper()
    if normalized_stage5 not in {"READY", "WAITING_DATA"}:
        raise ValueError("stage5_status must be READY or WAITING_DATA")
    if isinstance(existing_delivery_keys, (str, bytes, bytearray, Mapping)):
        raise ValueError("existing_delivery_keys must be an iterable of ids")
    existing = {
        _hex(item, name="existing delivery key") for item in existing_delivery_keys
    }
    previous_deliveries = (
        {} if last_delivery_at_by_chat_family is None else last_delivery_at_by_chat_family
    )
    if not isinstance(previous_deliveries, Mapping):
        raise ValueError("last_delivery_at_by_chat_family must be an object")

    rendered = evidence_renderer.dry_run_evidence_snapshots(snapshots)
    snapshot_by_id = {snapshot.snapshot_id: snapshot for snapshot in snapshots}
    route = _route(identifier, resolved_policy)
    global_blockers = []
    if not resolved_policy["enabled"]:
        global_blockers.append("Experimental feature flag is disabled")
    if resolved_policy["kill_switch_engaged"]:
        global_blockers.append("Experimental kill switch is engaged")
    if normalized_stage5 != "READY":
        global_blockers.append("Stage 5 is not READY")
    if resolved_policy["cooldown_seconds"] is None:
        global_blockers.append("Experimental cooldown is not configured")
    if route == "NONE":
        global_blockers.append("chat is neither test-allowlisted nor separately opted in")

    audits = []
    for message in rendered["messages"]:
        snapshot_id = str(message["representative_snapshot_id"])
        snapshot = snapshot_by_id[snapshot_id]
        family_id = str(message["formula_family_id"])
        blockers = list(global_blockers)
        if snapshot.compatibility != evidence_contract.CURRENT_V7:
            blockers.append("Legacy Shadow evidence is read-only")
        blockers.extend(
            _relevance_blockers(
                snapshot=snapshot,
                relevance=relevance_by_snapshot.get(snapshot_id),
            )
        )
        key = _delivery_key(
            chat_id=identifier,
            formula_family_id=family_id,
            snapshot_id=snapshot_id,
        )
        if key in existing:
            blockers.append("delivery idempotency key already exists")
        cooldown_lookup = f"{identifier}:{family_id}"
        last_delivery_at = previous_deliveries.get(cooldown_lookup)
        cooldown_remaining = None
        if last_delivery_at is not None and resolved_policy["cooldown_seconds"] is not None:
            previous = _utc(last_delivery_at, name="last delivery timestamp")
            if previous > now:
                raise ValueError("last delivery timestamp may not be in the future")
            elapsed = int((now - previous).total_seconds())
            cooldown_remaining = max(
                0, int(resolved_policy["cooldown_seconds"]) - elapsed
            )
            if cooldown_remaining > 0:
                blockers.append("chat/family cooldown is active")
        blockers = list(dict.fromkeys(blockers))
        status = SIMULATED_ELIGIBLE if not blockers else SUPPRESSED
        audits.append(
            {
                "status": status,
                "blockers": blockers,
                "route": route,
                "chat_id": identifier,
                "formula_family_id": family_id,
                "representative_snapshot_id": snapshot_id,
                "aggregated_snapshot_ids": list(message["snapshot_ids"]),
                "delivery_key": key,
                "rendered_message_sha256": hashlib.sha256(
                    str(message["text"]).encode("utf-8")
                ).hexdigest(),
                "cooldown_remaining_seconds": cooldown_remaining,
                "text": message["text"],
            }
        )

    return {
        "policy_version": POLICY_VERSION,
        "idempotency_policy_version": IDEMPOTENCY_POLICY_VERSION,
        "cooldown_policy_version": COOLDOWN_POLICY_VERSION,
        "mode": MODE,
        "evaluated_at_utc": _iso(now, name="now_utc"),
        "stage5_status": normalized_stage5,
        "chat_id": identifier,
        "route": route,
        "policy": {
            **resolved_policy,
            "test_chat_ids": list(resolved_policy["test_chat_ids"]),
            "opted_in_chat_ids": list(resolved_policy["opted_in_chat_ids"]),
        },
        "families_considered": len(audits),
        "simulated_eligible": sum(
            1 for audit in audits if audit["status"] == SIMULATED_ELIGIBLE
        ),
        "suppressed": sum(1 for audit in audits if audit["status"] == SUPPRESSED),
        "delivery_attempts": 0,
        "telegram_api_calls": 0,
        "database_writes": 0,
        "delivery_channel": "NONE",
        "live_effect": "NONE",
        "audits": audits,
    }
