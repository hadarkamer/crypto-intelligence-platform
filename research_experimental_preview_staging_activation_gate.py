"""Pure activation-gate preparation for the private staging PREVIEW route.

The gate verifies the content-addressed owner approval, deployed commit and
single private-chat binding before reporting that activation prerequisites
would be satisfied.  This local preparation is deliberately unregistered: it
imports no Telegram client, exposes no dispatch method and can never register
a connector or deliver a message.  Runtime registration remains a separate,
explicitly approved step.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, Mapping, Sequence

import research_experimental_preview_staging_config as staging_config


ACTIVATION_GATE_VERSION = "preview-staging-activation-gate-v1-local-only"
MODE = "PREVIEW_STAGING_ACTIVATION_GATE_LOCAL_UNREGISTERED"
LIFECYCLE_STATUS = "PREPARED_NOT_REGISTERED_NOT_DEPLOYED"
OBSERVE_MODE = "PREVIEW_STAGING_ACTIVATION_GATE_OBSERVE_ONLY"
OBSERVE_LIFECYCLE_STATUS = "OBSERVE_ONLY_NOT_REGISTERED"
APPROVAL_RECORD_VERSION = (
    "preview-staging-activation-approval-v1-local-not-applied"
)
APPROVAL_STATUS = "APPROVED_NOT_APPLIED"
OWNER = "Hadar Kamar"
OWNER_ROLE = "OWNER"
SCOPE = "PRIVATE_TEST_CHAT_PREVIEW_ONLY"
ROUTE = "TEST_ALLOWLIST"

STAGING_SERVICE_NAME = "crypto-ai-agent-candidate"
STAGING_SERVICE_ID = "srv-da3bd1lg1s2s73d867qg"
STAGING_BRANCH = "ai-production-analytics"
STAGING_ENTRYPOINT = "python ai_candidate_main.py"
APPROVAL_RECORD_ENV = "FORMULA_PREVIEW_STAGING_ACTIVATION_APPROVAL_RECORD_JSON"
ACTUAL_RUNTIME_COMMIT_ENV = "RENDER_GIT_COMMIT"

_HEX_40 = re.compile(r"^[0-9a-f]{40}$")
_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_REQUIRED_ACTION_AUTHORIZATIONS = (
    "render_configuration_application_authorized",
    "deployment_authorized",
    "connector_registration_authorized",
    "telegram_dispatch_authorized",
    "first_preview_message_authorized",
)
_FORBIDDEN_AUTHORIZATIONS = (
    "production_authorized",
    "public_opt_in_authorized",
    "stage6_authorized",
    "research_evidence_authorized",
    "live_authorized",
)
_UNREGISTERED_SAFETY = {
    "registration_required": True,
    "activation_allowed": False,
    "connector_registration_allowed": False,
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


def _fingerprint(value: Any, *, pattern: re.Pattern[str], name: str) -> str:
    normalized = str(value or "").strip().lower()
    if not pattern.fullmatch(normalized):
        raise ValueError(f"{name} has an invalid fingerprint")
    return normalized


def _boolean(mapping: Mapping[str, Any], name: str) -> bool:
    value = mapping.get(name)
    if type(value) is not bool:
        raise ValueError(f"activation approval {name} must be boolean")
    return value


def verify_activation_approval(
    approval_record: Mapping[str, Any],
) -> Dict[str, Any]:
    """Fingerprint-check one exact, still-unapplied owner approval record."""

    if not isinstance(approval_record, Mapping):
        raise ValueError("staging activation approval must be an object")
    supplied = dict(approval_record)
    approval_id = _fingerprint(
        supplied.pop("activation_approval_id", None),
        pattern=_HEX_64,
        name="activation_approval_id",
    )
    if approval_id != _hash(supplied):
        raise ValueError("staging activation approval fingerprint mismatch")
    if supplied.get("activation_approval_record_version") != (
        APPROVAL_RECORD_VERSION
    ):
        raise ValueError("staging activation approval version is incompatible")
    if supplied.get("status") != APPROVAL_STATUS:
        raise ValueError("staging activation approval status is incompatible")

    approver = supplied.get("approver")
    if not isinstance(approver, Mapping):
        raise ValueError("staging activation approver is missing")
    if approver.get("name") != OWNER or approver.get("role") != OWNER_ROLE:
        raise ValueError("staging activation approver is incompatible")

    candidate = supplied.get("candidate_binding")
    if not isinstance(candidate, Mapping):
        raise ValueError("staging activation candidate binding is missing")
    candidate_id = _fingerprint(
        candidate.get("activation_candidate_id"),
        pattern=_HEX_64,
        name="activation_candidate_id",
    )
    configuration_id = _fingerprint(
        candidate.get("configuration_id"),
        pattern=_HEX_64,
        name="candidate configuration_id",
    )

    target = supplied.get("target")
    if not isinstance(target, Mapping):
        raise ValueError("staging activation target is missing")
    expected_target = {
        "service_name": STAGING_SERVICE_NAME,
        "service_id": STAGING_SERVICE_ID,
        "branch": STAGING_BRANCH,
        "entrypoint": STAGING_ENTRYPOINT,
    }
    for name, expected in expected_target.items():
        if target.get(name) != expected:
            raise ValueError(f"staging activation target {name} is incompatible")
    runtime_commit = _fingerprint(
        target.get("runtime_commit"),
        pattern=_HEX_40,
        name="approval runtime_commit",
    )

    private_route = supplied.get("private_route")
    if not isinstance(private_route, Mapping):
        raise ValueError("staging activation private route is missing")
    if private_route.get("scope") != SCOPE:
        raise ValueError("staging activation scope is incompatible")
    if private_route.get("route") != ROUTE:
        raise ValueError("staging activation route is incompatible")
    if private_route.get("test_chat_count") != 1:
        raise ValueError("staging activation requires exactly one test chat")
    if private_route.get("public_opt_in") is not False:
        raise ValueError("staging activation public opt-in must remain disabled")
    test_chat_binding = _fingerprint(
        private_route.get("test_chat_binding_sha256"),
        pattern=_HEX_64,
        name="test_chat_binding_sha256",
    )

    authorization = supplied.get("authorization")
    if not isinstance(authorization, Mapping):
        raise ValueError("staging activation authorization is missing")
    if authorization.get("scope") != SCOPE:
        raise ValueError("staging activation authorization scope is incompatible")
    if _boolean(authorization, "approval_prerequisite_granted") is not True:
        raise ValueError("staging activation owner prerequisite is absent")
    action_authorizations = {
        name: _boolean(authorization, name)
        for name in _REQUIRED_ACTION_AUTHORIZATIONS
    }
    for name in _FORBIDDEN_AUTHORIZATIONS:
        if _boolean(authorization, name) is not False:
            raise ValueError(f"staging activation may not authorize {name}")

    application_state = supplied.get("application_state")
    if not isinstance(application_state, Mapping):
        raise ValueError("staging activation application state is missing")
    action_authorizations["kill_switch_release_authorized"] = _boolean(
        application_state,
        "kill_switch_release_authorized",
    )
    zero_state = {
        "effective_enabled": False,
        "connector_registered": False,
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
    for name, expected in zero_state.items():
        if application_state.get(name) != expected:
            raise ValueError(
                f"staging activation unapplied state is invalid: {name}"
            )

    return {
        "activation_approval_id": approval_id,
        "activation_candidate_id": candidate_id,
        "candidate_configuration_id": configuration_id,
        "runtime_commit": runtime_commit,
        "test_chat_binding_sha256": test_chat_binding,
        "action_authorizations": action_authorizations,
    }


def evaluate_activation_gate(
    configuration: Mapping[str, Any],
    *,
    actual_runtime_commit: str,
    approval_record: Mapping[str, Any],
) -> Dict[str, Any]:
    """Return readiness for a future registration without granting authority."""

    if not isinstance(configuration, Mapping):
        raise ValueError("staging activation configuration must be an object")
    supplied = dict(configuration)
    configuration_status = staging_config.sanitized_status(supplied)
    runtime_commit = _fingerprint(
        actual_runtime_commit,
        pattern=_HEX_40,
        name="actual_runtime_commit",
    )
    approval = verify_activation_approval(approval_record)

    blockers = []
    if not configuration_status["prerequisites_complete"]:
        blockers.extend(configuration_status["activation_blockers"])
    if not configuration_status["requested_enabled"]:
        blockers.append("staging PREVIEW flag is disabled")
    if configuration_status["kill_switch_engaged"]:
        blockers.append("staging PREVIEW kill switch is engaged")
    if not configuration_status["owner_staging_approved"]:
        blockers.append("owner staging approval is absent")
    if supplied.get("runtime_commit") != runtime_commit:
        blockers.append("configured runtime commit differs from deployed commit")
    if approval["runtime_commit"] != runtime_commit:
        blockers.append("approval runtime commit differs from deployed commit")
    if supplied.get("activation_approval_id") != approval[
        "activation_approval_id"
    ]:
        blockers.append("configured activation approval id differs from record")
    if configuration_status["test_chat_binding_sha256"] != approval[
        "test_chat_binding_sha256"
    ]:
        blockers.append("approval test chat differs from configured test chat")
    for name, authorized in approval["action_authorizations"].items():
        if not authorized:
            blockers.append(f"approval does not authorize {name}")
    blockers = list(dict.fromkeys(blockers))
    prerequisites_satisfied = not blockers

    payload = {
        "activation_gate_version": ACTIVATION_GATE_VERSION,
        "mode": MODE,
        "lifecycle_status": LIFECYCLE_STATUS,
        "configuration_id": supplied["configuration_id"],
        "activation_approval_id": approval["activation_approval_id"],
        "actual_runtime_commit": runtime_commit,
        "test_chat_binding_sha256": configuration_status[
            "test_chat_binding_sha256"
        ],
        "activation_prerequisites_satisfied": prerequisites_satisfied,
        "activation_blockers": blockers,
    }
    return {
        **payload,
        "activation_gate_id": _hash(payload),
        **_UNREGISTERED_SAFETY,
    }


def observe_activation_gate(
    configuration: Mapping[str, Any],
    *,
    actual_runtime_commit: Any = None,
    approval_record_json: Any = None,
) -> Dict[str, Any]:
    """Return a health-safe, fail-closed observation of the local gate."""

    if not isinstance(configuration, Mapping):
        raise ValueError("staging activation configuration must be an object")
    supplied = dict(configuration)
    configuration_status = staging_config.sanitized_status(supplied)
    blockers = []

    runtime_commit = None
    raw_runtime_commit = str(actual_runtime_commit or "").strip()
    runtime_commit_configured = bool(raw_runtime_commit)
    if runtime_commit_configured:
        try:
            runtime_commit = _fingerprint(
                raw_runtime_commit,
                pattern=_HEX_40,
                name="actual_runtime_commit",
            )
        except ValueError:
            blockers.append("runtime commit metadata is invalid")
    else:
        blockers.append("runtime commit metadata is unavailable")

    record_configured = approval_record_json is not None and (
        not isinstance(approval_record_json, str)
        or bool(approval_record_json.strip())
    )
    approval = None
    record = None
    record_valid = False
    if record_configured:
        try:
            if isinstance(approval_record_json, Mapping):
                record = dict(approval_record_json)
            elif isinstance(approval_record_json, str):
                record = json.loads(approval_record_json)
            else:
                raise ValueError("activation approval record type is invalid")
            approval = verify_activation_approval(record)
            record_valid = True
        except (TypeError, ValueError, json.JSONDecodeError):
            blockers.append("activation approval record is invalid")
    else:
        blockers.append("activation approval record is not configured")

    runtime_matches_configuration = (
        runtime_commit is not None
        and supplied.get("runtime_commit") == runtime_commit
    )
    approval_id_matches_configuration = (
        approval is not None
        and supplied.get("activation_approval_id")
        == approval["activation_approval_id"]
    )
    approval_runtime_commit_matches = (
        approval is not None
        and runtime_commit is not None
        and approval["runtime_commit"] == runtime_commit
    )
    approval_test_chat_matches = (
        approval is not None
        and configuration_status["test_chat_binding_sha256"]
        == approval["test_chat_binding_sha256"]
    )
    action_authorizations_complete = approval is not None and all(
        approval["action_authorizations"].values()
    )

    prerequisites_satisfied = False
    if runtime_commit is not None and approval is not None:
        evaluated = evaluate_activation_gate(
            supplied,
            actual_runtime_commit=runtime_commit,
            approval_record=record,
        )
        blockers.extend(evaluated["activation_blockers"])
        prerequisites_satisfied = evaluated[
            "activation_prerequisites_satisfied"
        ]
    else:
        blockers.extend(configuration_status["activation_blockers"])
    blockers = list(dict.fromkeys(blockers))

    payload = {
        "activation_gate_version": ACTIVATION_GATE_VERSION,
        "mode": OBSERVE_MODE,
        "lifecycle_status": OBSERVE_LIFECYCLE_STATUS,
        "approval_record_configured": record_configured,
        "approval_record_valid": record_valid,
        "runtime_commit_metadata_configured": runtime_commit_configured,
        "runtime_commit_matches_configuration": (
            runtime_matches_configuration
        ),
        "approval_id_matches_configuration": (
            approval_id_matches_configuration
        ),
        "approval_runtime_commit_matches": approval_runtime_commit_matches,
        "approval_test_chat_matches": approval_test_chat_matches,
        "action_authorizations_complete": action_authorizations_complete,
        "activation_prerequisites_satisfied": prerequisites_satisfied,
        "activation_blockers": blockers,
    }
    return {
        **payload,
        "activation_gate_id": _hash(payload),
        **_UNREGISTERED_SAFETY,
    }


def verify_observe_activation_gate_status(
    status: Mapping[str, Any],
) -> Dict[str, Any]:
    """Verify the complete health-safe observation before another layer uses it."""

    if not isinstance(status, Mapping):
        raise ValueError("observe-only activation gate status must be an object")
    supplied = dict(status)
    boolean_fields = (
        "approval_record_configured",
        "approval_record_valid",
        "runtime_commit_metadata_configured",
        "runtime_commit_matches_configuration",
        "approval_id_matches_configuration",
        "approval_runtime_commit_matches",
        "approval_test_chat_matches",
        "action_authorizations_complete",
        "activation_prerequisites_satisfied",
    )
    if supplied.get("activation_gate_version") != ACTIVATION_GATE_VERSION:
        raise ValueError("observe-only activation gate version is incompatible")
    if supplied.get("mode") != OBSERVE_MODE:
        raise ValueError("observe-only activation gate mode is incompatible")
    if supplied.get("lifecycle_status") != OBSERVE_LIFECYCLE_STATUS:
        raise ValueError("observe-only activation gate lifecycle is incompatible")
    for name in boolean_fields:
        if type(supplied.get(name)) is not bool:
            raise ValueError(f"observe-only activation gate {name} must be boolean")
    blockers = supplied.get("activation_blockers")
    if not isinstance(blockers, Sequence) or isinstance(
        blockers,
        (str, bytes, bytearray),
    ) or any(not isinstance(item, str) or not item for item in blockers):
        raise ValueError("observe-only activation gate blockers are invalid")

    payload = {
        "activation_gate_version": ACTIVATION_GATE_VERSION,
        "mode": OBSERVE_MODE,
        "lifecycle_status": OBSERVE_LIFECYCLE_STATUS,
        **{name: supplied[name] for name in boolean_fields},
        "activation_blockers": list(blockers),
    }
    if supplied.get("activation_gate_id") != _hash(payload):
        raise ValueError("observe-only activation gate fingerprint mismatch")
    for name, expected in _UNREGISTERED_SAFETY.items():
        if supplied.get(name) != expected:
            raise ValueError(
                f"observe-only activation gate safety invariant failed: {name}"
            )
    if supplied["approval_record_valid"] and not supplied[
        "approval_record_configured"
    ]:
        raise ValueError("valid activation approval must be configured")
    if supplied["runtime_commit_matches_configuration"] and not supplied[
        "runtime_commit_metadata_configured"
    ]:
        raise ValueError("matching runtime commit metadata must be configured")
    if supplied["activation_prerequisites_satisfied"]:
        required_ready = (
            "approval_record_configured",
            "approval_record_valid",
            "runtime_commit_metadata_configured",
            "runtime_commit_matches_configuration",
            "approval_id_matches_configuration",
            "approval_runtime_commit_matches",
            "approval_test_chat_matches",
            "action_authorizations_complete",
        )
        if blockers or any(not supplied[name] for name in required_ready):
            raise ValueError(
                "observe-only activation gate readiness is inconsistent"
            )
    expected_keys = set(payload) | {"activation_gate_id"} | set(
        _UNREGISTERED_SAFETY
    )
    if set(supplied) != expected_keys:
        raise ValueError("observe-only activation gate fields are incompatible")
    return {
        **payload,
        "activation_gate_id": supplied["activation_gate_id"],
        **_UNREGISTERED_SAFETY,
    }
