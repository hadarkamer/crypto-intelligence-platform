"""Strict, fail-closed configuration contract for PREVIEW_ONLY staging.

The resolver accepts an environment-like mapping but never reads the process
environment itself.  It validates one private chat plus owner, commit and
approval bindings.  This contract cannot activate delivery: even a complete
activation request remains blocked until a later version explicitly changes
the lifecycle authority.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, Mapping, Sequence


CONFIG_CONTRACT_VERSION = "preview-staging-config-v1-activation-forbidden"
LIFECYCLE_AUTHORITY = "CONFIGURE_ONLY_ACTIVATION_FORBIDDEN"

ENABLED_ENV = "FORMULA_PREVIEW_STAGING_ENABLED"
KILL_SWITCH_ENV = "FORMULA_PREVIEW_STAGING_KILL_SWITCH"
OWNER_APPROVED_ENV = "FORMULA_PREVIEW_STAGING_OWNER_APPROVED"
TEST_CHAT_ID_ENV = "FORMULA_PREVIEW_STAGING_TEST_CHAT_ID"
RUNTIME_COMMIT_ENV = "FORMULA_PREVIEW_STAGING_RUNTIME_COMMIT"
ACTIVATION_APPROVAL_ENV = "FORMULA_PREVIEW_STAGING_ACTIVATION_APPROVAL_ID"

ENVIRONMENT_KEYS = (
    ENABLED_ENV,
    KILL_SWITCH_ENV,
    OWNER_APPROVED_ENV,
    TEST_CHAT_ID_ENV,
    RUNTIME_COMMIT_ENV,
    ACTIVATION_APPROVAL_ENV,
)

_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}
_HEX_40 = re.compile(r"^[0-9a-f]{40}$")
_HEX_64 = re.compile(r"^[0-9a-f]{64}$")


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


def _boolean(environment: Mapping[str, Any], name: str, *, default: bool) -> bool:
    raw = environment.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    normalized = str(raw).strip().lower()
    if normalized in _TRUE:
        return True
    if normalized in _FALSE:
        return False
    raise ValueError(f"{name} must be an explicit boolean")


def _chat_id(environment: Mapping[str, Any]) -> int | None:
    raw = environment.get(TEST_CHAT_ID_ENV)
    if raw is None or str(raw).strip() == "":
        return None
    normalized = str(raw).strip()
    if not re.fullmatch(r"-?[0-9]+", normalized):
        raise ValueError(f"{TEST_CHAT_ID_ENV} must be an integer")
    value = int(normalized)
    if value == 0:
        raise ValueError(f"{TEST_CHAT_ID_ENV} may not be zero")
    return value


def _fingerprint(
    environment: Mapping[str, Any],
    name: str,
    *,
    pattern: re.Pattern[str],
) -> str | None:
    raw = environment.get(name)
    if raw is None or str(raw).strip() == "":
        return None
    normalized = str(raw).strip().lower()
    if not pattern.fullmatch(normalized):
        raise ValueError(f"{name} has an invalid fingerprint")
    return normalized


def resolve_staging_configuration(
    environment: Mapping[str, Any],
) -> Dict[str, Any]:
    """Validate requested settings while forcing effective activation off."""

    if not isinstance(environment, Mapping):
        raise ValueError("staging configuration source must be a mapping")
    requested_enabled = _boolean(environment, ENABLED_ENV, default=False)
    kill_switch = _boolean(environment, KILL_SWITCH_ENV, default=True)
    owner_approved = _boolean(
        environment,
        OWNER_APPROVED_ENV,
        default=False,
    )
    chat_id = _chat_id(environment)
    runtime_commit = _fingerprint(
        environment,
        RUNTIME_COMMIT_ENV,
        pattern=_HEX_40,
    )
    activation_approval_id = _fingerprint(
        environment,
        ACTIVATION_APPROVAL_ENV,
        pattern=_HEX_64,
    )

    prerequisite_blockers = []
    if not owner_approved:
        prerequisite_blockers.append("owner staging approval is absent")
    if chat_id is None:
        prerequisite_blockers.append("private staging test chat is not configured")
    if runtime_commit is None:
        prerequisite_blockers.append("staging runtime commit is not configured")
    if activation_approval_id is None:
        prerequisite_blockers.append("staging activation approval is not configured")
    prerequisites_complete = not prerequisite_blockers

    activation_blockers = list(prerequisite_blockers)
    if not requested_enabled:
        activation_blockers.append("staging PREVIEW flag is disabled")
    if kill_switch:
        activation_blockers.append("staging PREVIEW kill switch is engaged")
    activation_blockers.append(
        "current configuration lifecycle forbids activation"
    )
    configuration_payload = {
        "config_contract_version": CONFIG_CONTRACT_VERSION,
        "lifecycle_authority": LIFECYCLE_AUTHORITY,
        "requested_enabled": requested_enabled,
        "kill_switch_engaged": kill_switch,
        "owner_staging_approved": owner_approved,
        "test_chat_id": chat_id,
        "runtime_commit": runtime_commit,
        "activation_approval_id": activation_approval_id,
        "prerequisites_complete": prerequisites_complete,
        "activation_blockers": activation_blockers,
    }
    return {
        **configuration_payload,
        "configuration_id": _hash(configuration_payload),
        "effective_enabled": False,
        "connector_registration_allowed": False,
        "delivery_allowed": False,
        "public_opt_in": False,
        "stage6_activated": False,
        "research_evidence_effect": "NONE",
        "live_effect": "NONE",
    }


def sanitized_status(configuration: Mapping[str, Any]) -> Dict[str, Any]:
    """Return health-safe status without chat or approval identifiers."""

    if not isinstance(configuration, Mapping):
        raise ValueError("staging configuration must be a mapping")
    if configuration.get("config_contract_version") != CONFIG_CONTRACT_VERSION:
        raise ValueError("staging configuration contract is incompatible")
    if configuration.get("lifecycle_authority") != LIFECYCLE_AUTHORITY:
        raise ValueError("staging configuration lifecycle is incompatible")
    chat_id = configuration.get("test_chat_id")
    if chat_id is not None and (type(chat_id) is not int or chat_id == 0):
        raise ValueError("staging configuration chat id is invalid")
    runtime_commit = configuration.get("runtime_commit")
    if runtime_commit is not None and not _HEX_40.fullmatch(str(runtime_commit)):
        raise ValueError("staging configuration runtime commit is invalid")
    approval_id = configuration.get("activation_approval_id")
    if approval_id is not None and not _HEX_64.fullmatch(str(approval_id)):
        raise ValueError("staging configuration approval id is invalid")
    for name in (
        "requested_enabled",
        "kill_switch_engaged",
        "owner_staging_approved",
        "prerequisites_complete",
    ):
        if type(configuration.get(name)) is not bool:
            raise ValueError(f"staging configuration {name} must be boolean")
    blockers = configuration.get("activation_blockers")
    if not isinstance(blockers, Sequence) or isinstance(
        blockers, (str, bytes, bytearray)
    ) or any(not isinstance(blocker, str) or not blocker for blocker in blockers):
        raise ValueError("staging configuration blockers are invalid")
    configuration_payload = {
        "config_contract_version": CONFIG_CONTRACT_VERSION,
        "lifecycle_authority": LIFECYCLE_AUTHORITY,
        "requested_enabled": configuration["requested_enabled"],
        "kill_switch_engaged": configuration["kill_switch_engaged"],
        "owner_staging_approved": configuration["owner_staging_approved"],
        "test_chat_id": chat_id,
        "runtime_commit": runtime_commit,
        "activation_approval_id": approval_id,
        "prerequisites_complete": configuration["prerequisites_complete"],
        "activation_blockers": list(blockers),
    }
    if configuration.get("configuration_id") != _hash(configuration_payload):
        raise ValueError("staging configuration fingerprint mismatch")
    safety = {
        "effective_enabled": False,
        "connector_registration_allowed": False,
        "delivery_allowed": False,
        "public_opt_in": False,
        "stage6_activated": False,
        "research_evidence_effect": "NONE",
        "live_effect": "NONE",
    }
    for name, expected in safety.items():
        if configuration.get(name) != expected:
            raise ValueError(f"staging configuration safety invariant failed: {name}")
    return {
        "config_contract_version": CONFIG_CONTRACT_VERSION,
        "lifecycle_authority": LIFECYCLE_AUTHORITY,
        "configuration_id": configuration["configuration_id"],
        "requested_enabled": configuration["requested_enabled"],
        "effective_enabled": False,
        "kill_switch_engaged": configuration["kill_switch_engaged"],
        "owner_staging_approved": configuration["owner_staging_approved"],
        "test_chat_configured": chat_id is not None,
        "test_chat_binding_sha256": (
            _hash({"test_chat_id": chat_id}) if chat_id is not None else None
        ),
        "runtime_commit_configured": configuration["runtime_commit"] is not None,
        "activation_approval_configured": (
            configuration["activation_approval_id"] is not None
        ),
        "prerequisites_complete": configuration["prerequisites_complete"],
        "activation_blockers": list(blockers),
        "connector_registration_allowed": False,
        "delivery_allowed": False,
        "public_opt_in": False,
        "stage6_activated": False,
        "research_evidence_effect": "NONE",
        "live_effect": "NONE",
    }
