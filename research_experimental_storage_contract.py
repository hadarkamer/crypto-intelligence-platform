"""Pure, non-persisting contract for future Experimental audit storage.

The module verifies content-addressed output from the disconnected Experimental
gate and prepares canonical rows for a future append-only store.  It has no
database, environment, worker, delivery or LIVE integration and cannot write a
row.  The accompanying migration is deliberately not registered for schema
application while Stage 5 remains ``WAITING_DATA``.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import re
from typing import Any, Dict, Mapping, Sequence

import research_experimental_delivery_gate as delivery_gate


STORAGE_CONTRACT_VERSION = "experimental-storage-contract-v1-unapplied"
MODE = "PREPARED_NOT_PERSISTED"
MIGRATION_NAME = "018_formula_experimental_audit_v1.sql"

_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_SAFETY_INVARIANTS = {
    "delivery_attempts": 0,
    "telegram_api_calls": 0,
    "database_writes": 0,
    "research_evidence_writes": 0,
    "research_evidence_effect": "NONE",
    "delivery_channel": "NONE",
    "live_effect": "NONE",
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


def _integer(value: Any, *, name: str) -> int:
    if type(value) is not int:
        raise ValueError(f"{name} must be an integer")
    if value == 0:
        raise ValueError(f"{name} may not be zero")
    return value


def _count(value: Any, *, name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _utc_iso(value: Any, *, name: str) -> str:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be a valid timestamp") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _verified_decision(
    value: Any,
    *,
    batch_id: str,
    batch_plan: Mapping[str, Any],
) -> Dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("Experimental audit decision must be an object")
    decision = dict(value)
    decision_id = _hex(
        decision.pop("audit_decision_id", None), name="audit_decision_id"
    )
    if _hash(decision) != decision_id:
        raise ValueError("Experimental audit decision fingerprint mismatch")
    if decision.get("audit_contract_version") != delivery_gate.AUDIT_CONTRACT_VERSION:
        raise ValueError("Experimental audit contract version is incompatible")
    if decision.get("gate_policy_version") != delivery_gate.POLICY_VERSION:
        raise ValueError("Experimental gate policy version is incompatible")
    if decision.get("evaluated_at_utc") != batch_plan["evaluated_at_utc"]:
        raise ValueError("Experimental audit decision time differs from its batch")
    if decision.get("stage5_status") != batch_plan["stage5_status"]:
        raise ValueError("Experimental audit Stage 5 status differs from its batch")
    if decision.get("policy") != batch_plan["policy"]:
        raise ValueError("Experimental audit policy differs from its batch")
    if decision.get("route") != batch_plan["route"]:
        raise ValueError("Experimental audit route differs from its batch")
    if _integer(decision.get("chat_id"), name="decision chat_id") != batch_plan[
        "chat_id"
    ]:
        raise ValueError("Experimental audit chat differs from its batch")
    if decision.get("research_evidence_effect") != "NONE":
        raise ValueError("Experimental audit may not affect research evidence")

    status = str(decision.get("status") or "")
    if status not in {
        delivery_gate.SIMULATED_ELIGIBLE,
        delivery_gate.SUPPRESSED,
    }:
        raise ValueError("Experimental audit decision status is invalid")
    blockers = decision.get("blockers")
    if not isinstance(blockers, Sequence) or isinstance(
        blockers, (str, bytes, bytearray)
    ):
        raise ValueError("Experimental audit blockers must be a list")
    if (status == delivery_gate.SIMULATED_ELIGIBLE) == bool(blockers):
        raise ValueError("Experimental audit status and blockers disagree")
    if status == delivery_gate.SIMULATED_ELIGIBLE and batch_plan[
        "stage5_status"
    ] != "READY":
        raise ValueError("Experimental audit cannot be eligible before Stage 5 READY")

    relevance_sha256 = decision.get("relevance_decision_sha256")
    if relevance_sha256 is not None:
        relevance_sha256 = _hex(
            relevance_sha256, name="relevance_decision_sha256"
        )
    family_id = _hex(decision.get("formula_family_id"), name="formula_family_id")
    snapshot_id = _hex(
        decision.get("representative_snapshot_id"),
        name="representative_snapshot_id",
    )
    aggregated = decision.get("aggregated_snapshot_ids")
    if not isinstance(aggregated, Sequence) or isinstance(
        aggregated, (str, bytes, bytearray)
    ):
        raise ValueError("aggregated_snapshot_ids must be a list")
    aggregated_ids = [
        _hex(item, name="aggregated_snapshot_id") for item in aggregated
    ]
    if len(set(aggregated_ids)) != len(aggregated_ids):
        raise ValueError("aggregated_snapshot_ids contains duplicates")
    if snapshot_id not in aggregated_ids:
        raise ValueError("representative snapshot is absent from its aggregation")
    text = decision.get("text")
    if not isinstance(text, str) or not text:
        raise ValueError("Experimental rendered message must be non-empty text")
    rendered_sha256 = _hex(
        decision.get("rendered_message_sha256"),
        name="rendered_message_sha256",
    )
    if hashlib.sha256(text.encode("utf-8")).hexdigest() != rendered_sha256:
        raise ValueError("Experimental rendered message fingerprint mismatch")
    delivery_key = _hex(decision.get("delivery_key"), name="delivery_key")
    expected_delivery_key = _hash(
        {
            "policy_version": delivery_gate.IDEMPOTENCY_POLICY_VERSION,
            "chat_id": batch_plan["chat_id"],
            "formula_family_id": family_id,
            "representative_snapshot_id": snapshot_id,
        }
    )
    if delivery_key != expected_delivery_key:
        raise ValueError("Experimental delivery key fingerprint mismatch")

    record = {
        "audit_decision_id": decision_id,
        "audit_batch_id": batch_id,
        "delivery_key": delivery_key,
        "status": status,
        "chat_id": batch_plan["chat_id"],
        "route": batch_plan["route"],
        "formula_family_id": family_id,
        "representative_snapshot_id": snapshot_id,
        "relevance_decision_sha256": relevance_sha256,
        "rendered_message_sha256": rendered_sha256,
        "evaluated_at_utc": batch_plan["evaluated_at_utc"],
        "decision_payload_json": _canonical_json({
            **decision,
            "audit_decision_id": decision_id,
        }),
    }
    return record


def prepare_append_only_records(plan: Mapping[str, Any]) -> Dict[str, Any]:
    """Verify one gate plan and return canonical, non-persisted audit rows."""

    if not isinstance(plan, Mapping):
        raise ValueError("Experimental gate plan must be an object")
    supplied = dict(plan)
    if supplied.get("mode") != delivery_gate.MODE:
        raise ValueError("Experimental gate plan mode is incompatible")
    if supplied.get("policy_version") != delivery_gate.POLICY_VERSION:
        raise ValueError("Experimental gate plan policy is incompatible")
    if supplied.get("audit_contract_version") != delivery_gate.AUDIT_CONTRACT_VERSION:
        raise ValueError("Experimental gate audit contract is incompatible")
    for key, expected in _SAFETY_INVARIANTS.items():
        if supplied.get(key) != expected:
            raise ValueError(f"Experimental storage safety invariant failed: {key}")

    stage5_status = str(supplied.get("stage5_status") or "").strip().upper()
    if stage5_status not in {"READY", "WAITING_DATA"}:
        raise ValueError("Experimental gate plan has invalid Stage 5 status")
    route = str(supplied.get("route") or "")
    if route not in {"TEST_ALLOWLIST", "OPT_IN", "NONE"}:
        raise ValueError("Experimental gate plan route is invalid")
    policy = supplied.get("policy")
    if not isinstance(policy, Mapping):
        raise ValueError("Experimental gate plan policy must be an object")
    policy = dict(policy)
    evaluated_at = _utc_iso(supplied.get("evaluated_at_utc"), name="evaluated_at_utc")
    chat_id = _integer(supplied.get("chat_id"), name="chat_id")
    audits = supplied.get("audits")
    if not isinstance(audits, Sequence) or isinstance(
        audits, (str, bytes, bytearray)
    ):
        raise ValueError("Experimental gate plan audits must be a list")

    batch_id = _hex(supplied.get("audit_batch_id"), name="audit_batch_id")
    batch_plan = {
        "evaluated_at_utc": evaluated_at,
        "stage5_status": stage5_status,
        "chat_id": chat_id,
        "route": route,
        "policy": policy,
    }
    decisions = [
        _verified_decision(value, batch_id=batch_id, batch_plan=batch_plan)
        for value in audits
    ]
    decision_ids = [record["audit_decision_id"] for record in decisions]
    expected_batch_payload = {
        "audit_contract_version": delivery_gate.AUDIT_CONTRACT_VERSION,
        "gate_policy_version": delivery_gate.POLICY_VERSION,
        "evaluated_at_utc": evaluated_at,
        "stage5_status": stage5_status,
        "chat_id": chat_id,
        "route": route,
        "policy": policy,
        "audit_decision_ids": decision_ids,
    }
    if _hash(expected_batch_payload) != batch_id:
        raise ValueError("Experimental audit batch fingerprint mismatch")
    families_considered = _count(
        supplied.get("families_considered"), name="families_considered"
    )
    if families_considered != len(decisions):
        raise ValueError("Experimental audit family count is inconsistent")
    eligible = sum(
        record["status"] == delivery_gate.SIMULATED_ELIGIBLE
        for record in decisions
    )
    suppressed = len(decisions) - eligible
    if _count(
        supplied.get("simulated_eligible"), name="simulated_eligible"
    ) != eligible or _count(supplied.get("suppressed"), name="suppressed") != suppressed:
        raise ValueError("Experimental audit status counts are inconsistent")

    batch_record = {
        "audit_batch_id": batch_id,
        "audit_contract_version": delivery_gate.AUDIT_CONTRACT_VERSION,
        "gate_policy_version": delivery_gate.POLICY_VERSION,
        "evaluated_at_utc": evaluated_at,
        "stage5_status": stage5_status,
        "chat_id": chat_id,
        "route": route,
        "batch_payload_json": _canonical_json(expected_batch_payload),
    }
    return {
        "storage_contract_version": STORAGE_CONTRACT_VERSION,
        "mode": MODE,
        "migration_name": MIGRATION_NAME,
        "migration_registered": False,
        "database_writes": 0,
        "research_evidence_writes": 0,
        "research_evidence_effect": "NONE",
        "delivery_attempts": 0,
        "delivery_channel": "NONE",
        "live_effect": "NONE",
        "batch_record": batch_record,
        "decision_records": decisions,
    }
