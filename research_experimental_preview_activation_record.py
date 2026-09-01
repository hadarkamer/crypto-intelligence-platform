"""Prepared, non-authoritative activation record for one staging chat.

This pure contract binds the dedicated staging service, deployed commit and
sanitized private-chat fingerprint into a deterministic approval candidate.
The candidate is not an activation approval and cannot register a connector,
dispatch a Telegram request, persist data or affect Stage 6, research evidence
or LIVE.  A later, explicit approval step must remain separate.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Mapping

import research_experimental_preview_staging_config as staging_config


ACTIVATION_RECORD_VERSION = "preview-staging-activation-record-v1-prepared"
STATUS = "PREPARED_NOT_APPROVED"
APPROVAL_AUTHORITY = "SEPARATE_EXPLICIT_APPROVAL_REQUIRED"
SCOPE = "PRIVATE_TEST_CHAT_PREVIEW_ONLY"
ROUTE = "TEST_ALLOWLIST"

STAGING_SERVICE_NAME = "crypto-ai-agent-candidate"
STAGING_SERVICE_ID = "srv-da3bd1lg1s2s73d867qg"
STAGING_BRANCH = "ai-production-analytics"
STAGING_ENTRYPOINT = "python ai_candidate_main.py"

_SAFETY_INVARIANTS = {
    "approval_granted": False,
    "activation_approval_id": None,
    "connector_registration_allowed": False,
    "delivery_allowed": False,
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


def _prepared_configuration(configuration: Mapping[str, Any]) -> Dict[str, Any]:
    if not isinstance(configuration, Mapping):
        raise ValueError("staging activation configuration must be an object")
    supplied = dict(configuration)
    status = staging_config.sanitized_status(supplied)
    if supplied.get("test_chat_id") is None:
        raise ValueError("private staging test chat must be configured")
    if supplied.get("runtime_commit") is None:
        raise ValueError("staging runtime commit must be configured")
    if status["requested_enabled"] is not False:
        raise ValueError("activation candidate requires the preview flag disabled")
    if status["kill_switch_engaged"] is not True:
        raise ValueError("activation candidate requires the kill switch engaged")
    if status["owner_staging_approved"] is not False:
        raise ValueError("owner approval must remain absent while preparing")
    if status["activation_approval_configured"] is not False:
        raise ValueError("activation approval must remain absent while preparing")
    if status["effective_enabled"] is not False:
        raise ValueError("activation candidate requires effective disabled state")
    if status["connector_registration_allowed"] is not False:
        raise ValueError("activation candidate may not register a connector")
    if status["delivery_allowed"] is not False:
        raise ValueError("activation candidate may not allow delivery")
    return supplied


def prepare_activation_candidate(
    configuration: Mapping[str, Any],
) -> Dict[str, Any]:
    """Return a deterministic approval candidate with zero authority."""

    supplied = _prepared_configuration(configuration)
    configuration_status = staging_config.sanitized_status(supplied)
    target = {
        "service_name": STAGING_SERVICE_NAME,
        "service_id": STAGING_SERVICE_ID,
        "branch": STAGING_BRANCH,
        "entrypoint": STAGING_ENTRYPOINT,
        "runtime_commit": supplied["runtime_commit"],
    }
    private_route = {
        "scope": SCOPE,
        "route": ROUTE,
        "test_chat_count": 1,
        "test_chat_binding_sha256": configuration_status[
            "test_chat_binding_sha256"
        ],
        "public_opt_in": False,
    }
    approval_boundary = {
        "authority": APPROVAL_AUTHORITY,
        "owner_approved": False,
        "activation_approval_id": None,
        "approved_at_utc": None,
        "candidate_id_may_be_used_as_approval_id": False,
    }
    payload = {
        "activation_record_version": ACTIVATION_RECORD_VERSION,
        "status": STATUS,
        "target": target,
        "private_route": private_route,
        "configuration_id": supplied["configuration_id"],
        "approval_boundary": approval_boundary,
        **_SAFETY_INVARIANTS,
    }
    return {**payload, "activation_candidate_id": _hash(payload)}


def verify_activation_candidate(
    candidate: Mapping[str, Any],
    *,
    configuration: Mapping[str, Any],
) -> Dict[str, Any]:
    """Fingerprint-check a candidate and return a health-safe summary."""

    if not isinstance(candidate, Mapping):
        raise ValueError("staging activation candidate must be an object")
    supplied = dict(candidate)
    candidate_id = supplied.pop("activation_candidate_id", None)
    if candidate_id != _hash(supplied):
        raise ValueError("staging activation candidate fingerprint mismatch")
    expected = prepare_activation_candidate(configuration)
    if dict(candidate) != expected:
        raise ValueError("staging activation candidate binding mismatch")
    for name, expected_value in _SAFETY_INVARIANTS.items():
        if candidate.get(name) != expected_value:
            raise ValueError(
                f"staging activation candidate safety invariant failed: {name}"
            )
    return {
        "activation_record_version": ACTIVATION_RECORD_VERSION,
        "activation_candidate_id": candidate_id,
        "status": STATUS,
        "approval_authority": APPROVAL_AUTHORITY,
        "approval_granted": False,
        "activation_approval_configured": False,
        "service_id": STAGING_SERVICE_ID,
        "runtime_commit": candidate["target"]["runtime_commit"],
        "scope": SCOPE,
        "route": ROUTE,
        "test_chat_count": 1,
        "test_chat_binding_sha256": candidate["private_route"][
            "test_chat_binding_sha256"
        ],
        "connector_registration_allowed": False,
        "delivery_allowed": False,
        "public_opt_in": False,
        "stage6_activated": False,
        "research_evidence_effect": "NONE",
        "live_effect": "NONE",
    }
