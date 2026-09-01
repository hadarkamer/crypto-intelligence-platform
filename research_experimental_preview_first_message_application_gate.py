"""Observe-only application gate for one approved PREVIEW message.

The gate verifies the exact authorization candidate and owner approval, checks
the observation time, and rejects already-consumed approval or one-shot ids.
Even a ready result is only ``READY_NOT_APPLIED``: this module exposes no
application, consumption, persistence, dispatch, or Telegram operation.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import re
from typing import Any, Dict, Iterable, Mapping, Sequence

import research_experimental_preview_first_message_authorization as authorization
import research_experimental_preview_first_message_owner_approval as owner_approval


APPLICATION_GATE_VERSION = "preview-first-message-application-gate-v1-observe-only"
MODE = "PREVIEW_FIRST_MESSAGE_APPLICATION_GATE_OBSERVE_ONLY"
LIFECYCLE_STATUS = "READINESS_ONLY_APPLICATION_FORBIDDEN"
READY = "READY_NOT_APPLIED"
BLOCKED = "BLOCKED_NOT_APPLIED"

_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_UTC_SECOND = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")
_SAFETY = {
    "application_allowed": False,
    "approval_applied": False,
    "authorization_consumed": False,
    "dispatch_allowed": False,
    "delivery_allowed": False,
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


def _utc(value: Any, *, name: str) -> tuple[str, datetime]:
    normalized = str(value or "").strip()
    if not _UTC_SECOND.fullmatch(normalized):
        raise ValueError(f"{name} must use YYYY-MM-DDTHH:MM:SSZ")
    parsed = datetime.strptime(normalized, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=timezone.utc
    )
    return normalized, parsed


def _consumed_ids(values: Iterable[str], *, name: str) -> set[str]:
    if isinstance(values, (str, bytes, bytearray, Mapping)):
        raise ValueError(f"{name} must be an iterable of ids")
    return {_hex(value, name=f"{name} item") for value in values}


def evaluate_first_message_application_gate(
    approval_record: Mapping[str, Any],
    *,
    authorization_candidate: Mapping[str, Any],
    observed_at_utc: str,
    consumed_owner_approval_ids: Iterable[str] = (),
    consumed_one_shot_keys: Iterable[str] = (),
) -> Dict[str, Any]:
    """Return signed readiness while keeping application structurally false."""

    candidate = authorization.verify_first_message_authorization_candidate(
        authorization_candidate
    )
    approval = owner_approval.verify_first_message_owner_approval(
        approval_record,
        authorization_candidate=candidate,
    )
    observed_at, observed_dt = _utc(
        observed_at_utc,
        name="observed_at_utc",
    )
    approved_at, approved_dt = _utc(
        approval["approved_at_utc"],
        name="approved_at_utc",
    )
    expires_at, expires_dt = _utc(
        approval["expires_at_utc"],
        name="expires_at_utc",
    )
    consumed_approvals = _consumed_ids(
        consumed_owner_approval_ids,
        name="consumed_owner_approval_ids",
    )
    consumed_keys = _consumed_ids(
        consumed_one_shot_keys,
        name="consumed_one_shot_keys",
    )

    approval_current = approved_dt <= observed_dt < expires_dt
    approval_unconsumed = approval["owner_approval_id"] not in consumed_approvals
    one_shot_unconsumed = approval["one_shot_key"] not in consumed_keys
    blockers = []
    if observed_dt < approved_dt:
        blockers.append("first-message owner approval is not yet valid")
    if observed_dt >= expires_dt:
        blockers.append("first-message owner approval has expired")
    if not approval_unconsumed:
        blockers.append("first-message owner approval id is already consumed")
    if not one_shot_unconsumed:
        blockers.append("first-message one-shot key is already consumed")
    prerequisites = not blockers
    status = READY if prerequisites else BLOCKED

    payload = {
        "application_gate_version": APPLICATION_GATE_VERSION,
        "mode": MODE,
        "lifecycle_status": LIFECYCLE_STATUS,
        "status": status,
        "owner_approval_version": approval["owner_approval_version"],
        "owner_approval_id": approval["owner_approval_id"],
        "authorization_candidate_id": approval["authorization_candidate_id"],
        "one_shot_key": approval["one_shot_key"],
        "runtime_connector_registration_id": approval[
            "runtime_connector_registration_id"
        ],
        "activation_gate_id": approval["activation_gate_id"],
        "approved_at_utc": approved_at,
        "expires_at_utc": expires_at,
        "observed_at_utc": observed_at,
        "approval_current": approval_current,
        "owner_approval_unconsumed": approval_unconsumed,
        "one_shot_unconsumed": one_shot_unconsumed,
        "application_prerequisites_satisfied": prerequisites,
        "application_blockers": blockers,
    }
    return {
        **payload,
        "application_gate_id": _hash(payload),
        **_SAFETY,
    }


def verify_first_message_application_gate_status(
    status: Mapping[str, Any],
) -> Dict[str, Any]:
    """Verify the complete observe-only result before another layer uses it."""

    if not isinstance(status, Mapping):
        raise ValueError("first-message application gate status must be an object")
    supplied = dict(status)
    expected_keys = {
        "application_gate_version",
        "mode",
        "lifecycle_status",
        "status",
        "owner_approval_version",
        "owner_approval_id",
        "authorization_candidate_id",
        "one_shot_key",
        "runtime_connector_registration_id",
        "activation_gate_id",
        "approved_at_utc",
        "expires_at_utc",
        "observed_at_utc",
        "approval_current",
        "owner_approval_unconsumed",
        "one_shot_unconsumed",
        "application_prerequisites_satisfied",
        "application_blockers",
        "application_gate_id",
        *set(_SAFETY),
    }
    if set(supplied) != expected_keys:
        raise ValueError("first-message application gate fields are invalid")
    if supplied.get("application_gate_version") != APPLICATION_GATE_VERSION:
        raise ValueError("first-message application gate version is incompatible")
    if supplied.get("mode") != MODE:
        raise ValueError("first-message application gate mode is incompatible")
    if supplied.get("lifecycle_status") != LIFECYCLE_STATUS:
        raise ValueError("first-message application gate lifecycle is incompatible")
    if supplied.get("owner_approval_version") != (
        owner_approval.OWNER_APPROVAL_VERSION
    ):
        raise ValueError("first-message owner approval version is incompatible")

    for name in (
        "owner_approval_id",
        "authorization_candidate_id",
        "one_shot_key",
        "runtime_connector_registration_id",
        "activation_gate_id",
        "application_gate_id",
    ):
        _hex(supplied.get(name), name=name)
    approved_at, approved_dt = _utc(
        supplied.get("approved_at_utc"),
        name="approved_at_utc",
    )
    expires_at, expires_dt = _utc(
        supplied.get("expires_at_utc"),
        name="expires_at_utc",
    )
    observed_at, observed_dt = _utc(
        supplied.get("observed_at_utc"),
        name="observed_at_utc",
    )
    approval_window = int((expires_dt - approved_dt).total_seconds())
    if approval_window <= 0:
        raise ValueError("first-message application gate approval window is invalid")
    if approval_window > owner_approval.MAX_APPROVAL_WINDOW_SECONDS:
        raise ValueError("first-message application gate approval window is too long")
    for name in (
        "approval_current",
        "owner_approval_unconsumed",
        "one_shot_unconsumed",
        "application_prerequisites_satisfied",
    ):
        if type(supplied.get(name)) is not bool:
            raise ValueError(f"first-message application gate {name} must be boolean")
    blockers = supplied.get("application_blockers")
    if not isinstance(blockers, Sequence) or isinstance(
        blockers,
        (str, bytes, bytearray),
    ) or any(not isinstance(item, str) or not item for item in blockers):
        raise ValueError("first-message application gate blockers are invalid")
    expected_current = approved_dt <= observed_dt < expires_dt
    if supplied["approval_current"] != expected_current:
        raise ValueError("first-message application gate time state is inconsistent")
    expected_ready = (
        supplied["approval_current"]
        and supplied["owner_approval_unconsumed"]
        and supplied["one_shot_unconsumed"]
        and not blockers
    )
    if supplied["application_prerequisites_satisfied"] != expected_ready:
        raise ValueError("first-message application gate readiness is inconsistent")
    expected_status = READY if expected_ready else BLOCKED
    if supplied.get("status") != expected_status:
        raise ValueError("first-message application gate status is inconsistent")
    if observed_dt < approved_dt and (
        "first-message owner approval is not yet valid" not in blockers
    ):
        raise ValueError("first-message application gate missing early-time blocker")
    if observed_dt >= expires_dt and (
        "first-message owner approval has expired" not in blockers
    ):
        raise ValueError("first-message application gate missing expiry blocker")
    if not supplied["owner_approval_unconsumed"] and (
        "first-message owner approval id is already consumed" not in blockers
    ):
        raise ValueError("first-message application gate missing approval replay blocker")
    if not supplied["one_shot_unconsumed"] and (
        "first-message one-shot key is already consumed" not in blockers
    ):
        raise ValueError("first-message application gate missing one-shot replay blocker")

    payload = {
        "application_gate_version": APPLICATION_GATE_VERSION,
        "mode": MODE,
        "lifecycle_status": LIFECYCLE_STATUS,
        "status": supplied["status"],
        "owner_approval_version": supplied["owner_approval_version"],
        "owner_approval_id": supplied["owner_approval_id"],
        "authorization_candidate_id": supplied["authorization_candidate_id"],
        "one_shot_key": supplied["one_shot_key"],
        "runtime_connector_registration_id": supplied[
            "runtime_connector_registration_id"
        ],
        "activation_gate_id": supplied["activation_gate_id"],
        "approved_at_utc": approved_at,
        "expires_at_utc": expires_at,
        "observed_at_utc": observed_at,
        "approval_current": supplied["approval_current"],
        "owner_approval_unconsumed": supplied["owner_approval_unconsumed"],
        "one_shot_unconsumed": supplied["one_shot_unconsumed"],
        "application_prerequisites_satisfied": supplied[
            "application_prerequisites_satisfied"
        ],
        "application_blockers": list(blockers),
    }
    if supplied["application_gate_id"] != _hash(payload):
        raise ValueError("first-message application gate fingerprint mismatch")
    for name, expected in _SAFETY.items():
        if supplied.get(name) != expected:
            raise ValueError(
                f"first-message application gate safety invariant failed: {name}"
            )
    return {
        **payload,
        "application_gate_id": supplied["application_gate_id"],
        **_SAFETY,
    }
