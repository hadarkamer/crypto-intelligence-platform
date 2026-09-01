"""Pure preparation of append-only PREVIEW_ONLY audit records.

The contract fingerprint-verifies the complete disconnected preview output and
returns canonical rows for the unapplied Experimental audit schema.  It has no
database, environment, delivery, worker or LIVE integration and cannot persist
anything.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, Mapping, Sequence

import research_experimental_preview_contract as preview_contract
import research_experimental_storage_contract as experimental_storage


STORAGE_CONTRACT_VERSION = "experimental-preview-storage-v1-unapplied"
MODE = "PREVIEW_AUDIT_PREPARED_NOT_PERSISTED"
MIGRATION_NAME = experimental_storage.MIGRATION_NAME

_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_SAFETY_INVARIANTS = {
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
    if type(value) is not int or value == 0:
        raise ValueError(f"{name} must be a non-zero integer")
    return value


def _count(value: Any, *, name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _verified_preview(
    value: Any,
    *,
    batch_plan: Mapping[str, Any],
) -> Dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("PREVIEW_ONLY audit decision must be an object")
    preview = dict(value)
    decision_id = _hex(
        preview.pop("preview_decision_id", None), name="preview_decision_id"
    )
    if _hash(preview) != decision_id:
        raise ValueError("PREVIEW_ONLY audit decision fingerprint mismatch")
    if preview.get("policy_version") != preview_contract.POLICY_VERSION:
        raise ValueError("PREVIEW_ONLY policy version is incompatible")
    if (
        preview.get("idempotency_policy_version")
        != preview_contract.IDEMPOTENCY_POLICY_VERSION
    ):
        raise ValueError("PREVIEW_ONLY idempotency policy is incompatible")
    if preview.get("preview_policy") != batch_plan["policy"]:
        raise ValueError("PREVIEW_ONLY decision policy differs from its batch")
    if preview.get("source_audit_batch_id") != batch_plan[
        "source_audit_batch_id"
    ]:
        raise ValueError("PREVIEW_ONLY source batch differs from its batch")
    if _integer(preview.get("chat_id"), name="preview chat_id") != batch_plan[
        "chat_id"
    ]:
        raise ValueError("PREVIEW_ONLY chat differs from its batch")
    if preview.get("route") != batch_plan["route"]:
        raise ValueError("PREVIEW_ONLY route differs from its batch")
    if preview.get("research_evidence_effect") != "NONE":
        raise ValueError("PREVIEW_ONLY may not affect research evidence")

    status = str(preview.get("status") or "")
    if status not in {
        preview_contract.PREVIEW_SIMULATED_ELIGIBLE,
        preview_contract.PREVIEW_SUPPRESSED,
    }:
        raise ValueError("PREVIEW_ONLY status is invalid")
    blockers = preview.get("blockers")
    if not isinstance(blockers, Sequence) or isinstance(
        blockers, (str, bytes, bytearray)
    ):
        raise ValueError("PREVIEW_ONLY blockers must be a list")
    if (status == preview_contract.PREVIEW_SIMULATED_ELIGIBLE) == bool(blockers):
        raise ValueError("PREVIEW_ONLY status and blockers disagree")
    if status == preview_contract.PREVIEW_SIMULATED_ELIGIBLE and (
        batch_plan["stage5_status"] != "WAITING_DATA"
        or batch_plan["route"] != "TEST_ALLOWLIST"
    ):
        raise ValueError("PREVIEW_ONLY eligibility violates the test-chat boundary")

    family_id = _hex(preview.get("formula_family_id"), name="formula_family_id")
    snapshot_id = _hex(
        preview.get("representative_snapshot_id"),
        name="representative_snapshot_id",
    )
    source_decision_id = _hex(
        preview.get("source_audit_decision_id"),
        name="source_audit_decision_id",
    )
    key = _hex(preview.get("preview_key"), name="preview_key")
    expected_key = _hash(
        {
            "policy_version": preview_contract.IDEMPOTENCY_POLICY_VERSION,
            "chat_id": batch_plan["chat_id"],
            "formula_family_id": family_id,
            "representative_snapshot_id": snapshot_id,
        }
    )
    if key != expected_key:
        raise ValueError("PREVIEW_ONLY idempotency key fingerprint mismatch")
    text = preview.get("text")
    if not isinstance(text, str) or not text.startswith(
        preview_contract.LABEL + "\n\n"
    ):
        raise ValueError("PREVIEW_ONLY message label is missing")
    message_sha256 = _hex(
        preview.get("preview_message_sha256"), name="preview_message_sha256"
    )
    if hashlib.sha256(text.encode("utf-8")).hexdigest() != message_sha256:
        raise ValueError("PREVIEW_ONLY message fingerprint mismatch")

    return {
        "preview_decision_id": decision_id,
        "preview_batch_id": batch_plan["preview_batch_id"],
        "source_audit_decision_id": source_decision_id,
        "preview_key": key,
        "status": status,
        "chat_id": batch_plan["chat_id"],
        "route": batch_plan["route"],
        "formula_family_id": family_id,
        "representative_snapshot_id": snapshot_id,
        "preview_message_sha256": message_sha256,
        "decision_payload_json": _canonical_json(
            {**preview, "preview_decision_id": decision_id}
        ),
    }


def prepare_preview_append_only_records(plan: Mapping[str, Any]) -> Dict[str, Any]:
    """Verify PREVIEW_ONLY output and return non-persisted canonical rows."""

    if not isinstance(plan, Mapping):
        raise ValueError("PREVIEW_ONLY plan must be an object")
    supplied = dict(plan)
    if supplied.get("mode") != preview_contract.MODE:
        raise ValueError("PREVIEW_ONLY plan mode is incompatible")
    if supplied.get("policy_version") != preview_contract.POLICY_VERSION:
        raise ValueError("PREVIEW_ONLY plan policy is incompatible")
    if (
        supplied.get("idempotency_policy_version")
        != preview_contract.IDEMPOTENCY_POLICY_VERSION
    ):
        raise ValueError("PREVIEW_ONLY plan idempotency policy is incompatible")
    for key, expected in _SAFETY_INVARIANTS.items():
        if supplied.get(key) != expected:
            raise ValueError(f"PREVIEW_ONLY storage safety invariant failed: {key}")

    stage5_status = str(supplied.get("stage5_status") or "").strip().upper()
    if stage5_status not in {"READY", "WAITING_DATA"}:
        raise ValueError("PREVIEW_ONLY Stage 5 status is invalid")
    route = str(supplied.get("route") or "")
    if route not in {"TEST_ALLOWLIST", "NONE"}:
        raise ValueError("PREVIEW_ONLY route is invalid")
    chat_id = _integer(supplied.get("chat_id"), name="chat_id")
    policy = supplied.get("policy")
    if not isinstance(policy, Mapping):
        raise ValueError("PREVIEW_ONLY policy must be an object")
    policy = dict(policy)
    source_batch_id = _hex(
        supplied.get("source_audit_batch_id"), name="source_audit_batch_id"
    )
    preview_batch_id = _hex(
        supplied.get("preview_batch_id"), name="preview_batch_id"
    )
    previews = supplied.get("previews")
    if not isinstance(previews, Sequence) or isinstance(
        previews, (str, bytes, bytearray)
    ):
        raise ValueError("PREVIEW_ONLY decisions must be a list")
    batch_plan = {
        "preview_batch_id": preview_batch_id,
        "source_audit_batch_id": source_batch_id,
        "stage5_status": stage5_status,
        "chat_id": chat_id,
        "route": route,
        "policy": policy,
    }
    decisions = [
        _verified_preview(value, batch_plan=batch_plan) for value in previews
    ]
    decision_ids = [record["preview_decision_id"] for record in decisions]
    preview_keys = [record["preview_key"] for record in decisions]
    if len(set(decision_ids)) != len(decision_ids):
        raise ValueError("PREVIEW_ONLY decision ids contain duplicates")
    if len(set(preview_keys)) != len(preview_keys):
        raise ValueError("PREVIEW_ONLY idempotency keys contain duplicates")
    expected_batch_payload = {
        "policy_version": preview_contract.POLICY_VERSION,
        "source_audit_batch_id": source_batch_id,
        "stage5_status": stage5_status,
        "chat_id": chat_id,
        "route": route,
        "policy": policy,
        "public_opt_in": False,
        "stage6_activated": False,
        "preview_decision_ids": decision_ids,
    }
    if _hash(expected_batch_payload) != preview_batch_id:
        raise ValueError("PREVIEW_ONLY audit batch fingerprint mismatch")
    families = _count(
        supplied.get("families_considered"), name="families_considered"
    )
    if families != len(decisions):
        raise ValueError("PREVIEW_ONLY family count is inconsistent")
    eligible = sum(
        record["status"] == preview_contract.PREVIEW_SIMULATED_ELIGIBLE
        for record in decisions
    )
    suppressed = len(decisions) - eligible
    if _count(
        supplied.get("preview_simulated_eligible"),
        name="preview_simulated_eligible",
    ) != eligible or _count(
        supplied.get("preview_suppressed"), name="preview_suppressed"
    ) != suppressed:
        raise ValueError("PREVIEW_ONLY status counts are inconsistent")

    batch_record = {
        "preview_batch_id": preview_batch_id,
        "preview_policy_version": preview_contract.POLICY_VERSION,
        "source_audit_batch_id": source_batch_id,
        "stage5_status": stage5_status,
        "chat_id": chat_id,
        "route": route,
        "public_opt_in": False,
        "stage6_activated": False,
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
