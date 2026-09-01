"""Separate owner approval record for one PREVIEW message, never applied.

The contract accepts only a fully verified first-message authorization
candidate and explicit, short-lived owner input.  It can prepare and verify an
``APPROVED_NOT_APPLIED`` record, but it exposes no application or dispatch
operation and cannot send, persist, schedule, or activate any runtime path.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import re
from typing import Any, Dict, Mapping

import research_experimental_preview_first_message_authorization as authorization
import research_experimental_preview_staging_activation_gate as activation_gate


OWNER_APPROVAL_VERSION = "preview-first-message-owner-approval-v1-not-applied"
MODE = "PREVIEW_FIRST_MESSAGE_OWNER_APPROVAL_RECORD_ONLY"
STATUS = "APPROVED_NOT_APPLIED"
OWNER = authorization.OWNER
OWNER_ROLE = activation_gate.OWNER_ROLE
SCOPE = authorization.SCOPE
ROUTE = authorization.ROUTE
MAX_APPROVAL_WINDOW_SECONDS = 15 * 60

_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_UTC_SECOND = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")
_INPUT_KEYS = {
    "owner_approved",
    "approver_name",
    "approver_role",
    "approved_at_utc",
    "expires_at_utc",
    "approval_statement",
    "single_use_required",
    "telegram_dispatch_authorized",
    "first_preview_message_authorized",
    "production_authorized",
    "public_opt_in_authorized",
    "stage6_authorized",
    "research_evidence_authorized",
    "live_authorized",
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


def _boolean(mapping: Mapping[str, Any], name: str) -> bool:
    value = mapping.get(name)
    if type(value) is not bool:
        raise ValueError(f"first-message owner approval {name} must be boolean")
    return value


def _candidate_binding(candidate: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "authorization_candidate_id": candidate["authorization_candidate_id"],
        "authorization_contract_version": candidate[
            "authorization_contract_version"
        ],
        "candidate_status": candidate["status"],
        "runtime_connector_registration_id": candidate[
            "runtime_connector_registration_id"
        ],
        "activation_gate_id": candidate["activation_gate_id"],
        "adapter_batch_id": candidate["adapter_batch_id"],
        "adapter_request_id": candidate["adapter_request_id"],
        "transport_envelope_id": candidate["transport_envelope_id"],
        "transport_key": candidate["transport_key"],
        "request_key": candidate["request_key"],
        "test_chat_binding_sha256": candidate["test_chat_binding_sha256"],
        "message_sha256": candidate["message_sha256"],
        "one_shot_key": candidate["one_shot_key"],
        "chunk_count": candidate["chunk_count"],
    }


def verify_first_message_owner_approval(
    approval_record: Mapping[str, Any],
    *,
    authorization_candidate: Mapping[str, Any],
) -> Dict[str, Any]:
    """Verify exact owner approval while retaining zero application authority."""

    candidate = authorization.verify_first_message_authorization_candidate(
        authorization_candidate
    )
    if not isinstance(approval_record, Mapping):
        raise ValueError("first-message owner approval must be an object")
    supplied = dict(approval_record)
    approval_id = _hex(
        supplied.pop("owner_approval_id", None),
        name="owner_approval_id",
    )
    if approval_id != _hash(supplied):
        raise ValueError("first-message owner approval fingerprint mismatch")
    if set(supplied) != {
        "owner_approval_version",
        "mode",
        "status",
        "approved_at_utc",
        "expires_at_utc",
        "approver",
        "approval_statement",
        "candidate_binding",
        "authorization",
        "application_state",
    }:
        raise ValueError("first-message owner approval fields are invalid")
    if supplied.get("owner_approval_version") != OWNER_APPROVAL_VERSION:
        raise ValueError("first-message owner approval version is incompatible")
    if supplied.get("mode") != MODE or supplied.get("status") != STATUS:
        raise ValueError("first-message owner approval lifecycle is incompatible")

    approved_at, approved_dt = _utc(
        supplied.get("approved_at_utc"),
        name="approved_at_utc",
    )
    expires_at, expires_dt = _utc(
        supplied.get("expires_at_utc"),
        name="expires_at_utc",
    )
    approval_window = int((expires_dt - approved_dt).total_seconds())
    if approval_window <= 0:
        raise ValueError("first-message owner approval must expire after approval")
    if approval_window > MAX_APPROVAL_WINDOW_SECONDS:
        raise ValueError("first-message owner approval window exceeds 15 minutes")

    approver = supplied.get("approver")
    if approver != {"name": OWNER, "role": OWNER_ROLE}:
        raise ValueError("first-message owner approval approver is incompatible")
    statement = supplied.get("approval_statement")
    if not isinstance(statement, str) or not statement.strip():
        raise ValueError("first-message owner approval statement is required")
    binding = supplied.get("candidate_binding")
    if binding != _candidate_binding(candidate):
        raise ValueError("first-message owner approval candidate binding mismatch")

    expected_authorization = {
        "scope": SCOPE,
        "route": ROUTE,
        "single_use_required": True,
        "telegram_dispatch_authorized": True,
        "first_preview_message_authorized": True,
        "production_authorized": False,
        "public_opt_in_authorized": False,
        "stage6_authorized": False,
        "research_evidence_authorized": False,
        "live_authorized": False,
    }
    if supplied.get("authorization") != expected_authorization:
        raise ValueError("first-message owner approval authorization is incompatible")
    expected_application_state = {
        "applied_to_runtime": False,
        "authorization_consumed": False,
        "dispatch_allowed": False,
        "delivery_allowed": False,
        "handler_registered": False,
        "scheduler_registered": False,
        "worker_registered": False,
        "delivery_attempts": 0,
        "telegram_api_calls": 0,
        "database_writes": 0,
        "research_evidence_writes": 0,
        "research_evidence_effect": "NONE",
        "delivery_channel": "NONE",
        "live_effect": "NONE",
    }
    if supplied.get("application_state") != expected_application_state:
        raise ValueError("first-message owner approval applied state is forbidden")

    return {
        "owner_approval_version": OWNER_APPROVAL_VERSION,
        "mode": MODE,
        "status": STATUS,
        "owner_approval_id": approval_id,
        "authorization_candidate_id": candidate["authorization_candidate_id"],
        "one_shot_key": candidate["one_shot_key"],
        "runtime_connector_registration_id": candidate[
            "runtime_connector_registration_id"
        ],
        "activation_gate_id": candidate["activation_gate_id"],
        "approved_at_utc": approved_at,
        "expires_at_utc": expires_at,
        "approval_window_seconds": approval_window,
        "owner_approval_verified": True,
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


def prepare_first_message_owner_approval(
    authorization_candidate: Mapping[str, Any],
    *,
    approval_input: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    """Prepare approval only from explicit owner input; never apply it."""

    candidate = authorization.verify_first_message_authorization_candidate(
        authorization_candidate
    )
    if approval_input is None:
        supplied: Mapping[str, Any] = {}
    elif not isinstance(approval_input, Mapping):
        raise ValueError("first-message owner approval input must be an object")
    else:
        supplied = dict(approval_input)
    unknown = sorted(set(supplied).difference(_INPUT_KEYS))
    if unknown:
        raise ValueError(
            "first-message owner approval input contains unknown fields: "
            + ", ".join(unknown)
        )

    blockers = []
    record = None
    if not supplied:
        blockers.append("explicit first-message owner approval is absent")
    else:
        if not _boolean(supplied, "owner_approved"):
            blockers.append("first-message owner approval is not granted")
        if supplied.get("approver_name") != OWNER:
            raise ValueError("first-message owner approval name is incompatible")
        if supplied.get("approver_role") != OWNER_ROLE:
            raise ValueError("first-message owner approval role is incompatible")
        statement = supplied.get("approval_statement")
        if not isinstance(statement, str) or not statement.strip():
            raise ValueError("first-message owner approval statement is required")
        approved_at, approved_dt = _utc(
            supplied.get("approved_at_utc"),
            name="approved_at_utc",
        )
        expires_at, expires_dt = _utc(
            supplied.get("expires_at_utc"),
            name="expires_at_utc",
        )
        approval_window = int((expires_dt - approved_dt).total_seconds())
        if approval_window <= 0:
            raise ValueError(
                "first-message owner approval must expire after approval"
            )
        if approval_window > MAX_APPROVAL_WINDOW_SECONDS:
            raise ValueError(
                "first-message owner approval window exceeds 15 minutes"
            )
        for name in (
            "single_use_required",
            "telegram_dispatch_authorized",
            "first_preview_message_authorized",
        ):
            if not _boolean(supplied, name):
                blockers.append(f"first-message owner approval requires {name}")
        for name in (
            "production_authorized",
            "public_opt_in_authorized",
            "stage6_authorized",
            "research_evidence_authorized",
            "live_authorized",
        ):
            if _boolean(supplied, name):
                raise ValueError(f"first-message owner approval may not authorize {name}")

        blockers = list(dict.fromkeys(blockers))
        if not blockers:
            record_payload = {
                "owner_approval_version": OWNER_APPROVAL_VERSION,
                "mode": MODE,
                "status": STATUS,
                "approved_at_utc": approved_at,
                "expires_at_utc": expires_at,
                "approver": {"name": OWNER, "role": OWNER_ROLE},
                "approval_statement": statement.strip(),
                "candidate_binding": _candidate_binding(candidate),
                "authorization": {
                    "scope": SCOPE,
                    "route": ROUTE,
                    "single_use_required": True,
                    "telegram_dispatch_authorized": True,
                    "first_preview_message_authorized": True,
                    "production_authorized": False,
                    "public_opt_in_authorized": False,
                    "stage6_authorized": False,
                    "research_evidence_authorized": False,
                    "live_authorized": False,
                },
                "application_state": {
                    "applied_to_runtime": False,
                    "authorization_consumed": False,
                    "dispatch_allowed": False,
                    "delivery_allowed": False,
                    "handler_registered": False,
                    "scheduler_registered": False,
                    "worker_registered": False,
                    "delivery_attempts": 0,
                    "telegram_api_calls": 0,
                    "database_writes": 0,
                    "research_evidence_writes": 0,
                    "research_evidence_effect": "NONE",
                    "delivery_channel": "NONE",
                    "live_effect": "NONE",
                },
            }
            record = {
                **record_payload,
                "owner_approval_id": _hash(record_payload),
            }
            verify_first_message_owner_approval(
                record,
                authorization_candidate=candidate,
            )

    return {
        "owner_approval_version": OWNER_APPROVAL_VERSION,
        "mode": MODE,
        "status": STATUS if record is not None else "NOT_APPROVED",
        "authorization_candidate_id": candidate["authorization_candidate_id"],
        "one_shot_key": candidate["one_shot_key"],
        "owner_approval_record_prepared": record is not None,
        "owner_approval_id": (
            record["owner_approval_id"] if record is not None else None
        ),
        "owner_approval_record": record,
        "approval_blockers": blockers,
        "owner_approval_verified": record is not None,
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
