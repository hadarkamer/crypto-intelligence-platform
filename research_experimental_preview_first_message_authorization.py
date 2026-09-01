"""Prepare one exact first-message authorization candidate with zero authority.

This pure boundary binds a verified registered-no-dispatch runtime receipt, the
unchanged activation gate, and one prepared private-chat request.  It produces
only a content-addressed candidate for a later explicit approval.  It exposes
no dispatch operation, imports no Telegram client, and cannot send or persist.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, Iterable, Mapping

import research_experimental_preview_staging_activation_gate as activation_gate
import research_experimental_preview_staging_registration as staging_registration
import research_experimental_preview_telegram_adapter as telegram_adapter


AUTHORIZATION_CONTRACT_VERSION = (
    "preview-first-message-authorization-v1-candidate-only"
)
IDEMPOTENCY_POLICY_VERSION = "preview-first-message-registration-request-v1"
MODE = "PREVIEW_FIRST_MESSAGE_AUTHORIZATION_CANDIDATE_ONLY"
LIFECYCLE_STATUS = "PREPARED_NOT_AUTHORIZED_NO_DISPATCH"
STATUS = "PREPARED_NOT_AUTHORIZED"
OWNER = activation_gate.OWNER
SCOPE = activation_gate.SCOPE
ROUTE = activation_gate.ROUTE

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


def _hex(value: Any, *, name: str) -> str:
    normalized = str(value or "").strip().lower()
    if not _HEX_64.fullmatch(normalized):
        raise ValueError(f"{name} must be a lowercase 64-character SHA-256 id")
    return normalized


def _existing_keys(values: Iterable[str]) -> set[str]:
    if isinstance(values, (str, bytes, bytearray, Mapping)):
        raise ValueError("existing_one_shot_keys must be an iterable of ids")
    return {_hex(value, name="existing one-shot key") for value in values}


def _chat_binding(chat_id: int) -> str:
    if type(chat_id) is not int or chat_id == 0:
        raise ValueError("first-message chat id must be a non-zero integer")
    return _hash({"test_chat_id": chat_id})


def _one_shot_key(
    *,
    registration_id: str,
    activation_gate_id: str,
    adapter_request_id: str,
    request_key: str,
    chat_binding_sha256: str,
    message_sha256: str,
) -> str:
    return _hash(
        {
            "idempotency_policy_version": IDEMPOTENCY_POLICY_VERSION,
            "runtime_connector_registration_id": registration_id,
            "activation_gate_id": activation_gate_id,
            "adapter_request_id": adapter_request_id,
            "request_key": request_key,
            "test_chat_binding_sha256": chat_binding_sha256,
            "message_sha256": message_sha256,
        }
    )


def verify_first_message_authorization_candidate(
    candidate: Mapping[str, Any],
) -> Dict[str, Any]:
    """Verify a prepared candidate without converting it into authority."""

    if not isinstance(candidate, Mapping):
        raise ValueError("first-message authorization candidate must be an object")
    supplied = dict(candidate)
    candidate_id = _hex(
        supplied.pop("authorization_candidate_id", None),
        name="authorization_candidate_id",
    )
    expected_keys = {
        "authorization_contract_version",
        "idempotency_policy_version",
        "mode",
        "lifecycle_status",
        "status",
        "owner",
        "scope",
        "route",
        "runtime_connector_registration_id",
        "activation_gate_id",
        "adapter_batch_id",
        "adapter_request_id",
        "transport_envelope_id",
        "transport_key",
        "request_key",
        "test_chat_binding_sha256",
        "message_sha256",
        "chunk_count",
        "one_shot_key",
        "authorization_granted",
        "authorization_consumed",
        "candidate_id_may_be_used_as_authorization_id",
        "dispatch_allowed",
        "delivery_allowed",
        "handler_registered",
        "scheduler_registered",
        "worker_registered",
        "public_opt_in",
        "stage6_activated",
        "delivery_attempts",
        "telegram_api_calls",
        "database_writes",
        "research_evidence_writes",
        "research_evidence_effect",
        "delivery_channel",
        "live_effect",
    }
    if set(supplied) != expected_keys:
        raise ValueError("first-message authorization candidate fields are invalid")
    constants = {
        "authorization_contract_version": AUTHORIZATION_CONTRACT_VERSION,
        "idempotency_policy_version": IDEMPOTENCY_POLICY_VERSION,
        "mode": MODE,
        "lifecycle_status": LIFECYCLE_STATUS,
        "status": STATUS,
        "owner": OWNER,
        "scope": SCOPE,
        "route": ROUTE,
        "research_evidence_effect": "NONE",
        "delivery_channel": "NONE",
        "live_effect": "NONE",
    }
    for name, expected in constants.items():
        if supplied.get(name) != expected:
            raise ValueError(f"first-message authorization {name} is incompatible")
    identifiers = {}
    for name in (
        "runtime_connector_registration_id",
        "activation_gate_id",
        "adapter_batch_id",
        "adapter_request_id",
        "transport_envelope_id",
        "transport_key",
        "request_key",
        "test_chat_binding_sha256",
        "message_sha256",
        "one_shot_key",
    ):
        identifiers[name] = _hex(supplied.get(name), name=name)
    if supplied.get("chunk_count") != 1:
        raise ValueError("first-message authorization requires exactly one chunk")
    for name in (
        "authorization_granted",
        "authorization_consumed",
        "candidate_id_may_be_used_as_authorization_id",
        "dispatch_allowed",
        "delivery_allowed",
        "handler_registered",
        "scheduler_registered",
        "worker_registered",
        "public_opt_in",
        "stage6_activated",
    ):
        if supplied.get(name) is not False:
            raise ValueError(f"first-message authorization candidate forbids {name}")
    for name in (
        "delivery_attempts",
        "telegram_api_calls",
        "database_writes",
        "research_evidence_writes",
    ):
        if supplied.get(name) != 0:
            raise ValueError(f"first-message authorization requires zero {name}")
    expected_key = _one_shot_key(
        registration_id=identifiers["runtime_connector_registration_id"],
        activation_gate_id=identifiers["activation_gate_id"],
        adapter_request_id=identifiers["adapter_request_id"],
        request_key=identifiers["request_key"],
        chat_binding_sha256=identifiers["test_chat_binding_sha256"],
        message_sha256=identifiers["message_sha256"],
    )
    if identifiers["one_shot_key"] != expected_key:
        raise ValueError("first-message one-shot key fingerprint mismatch")
    if candidate_id != _hash(supplied):
        raise ValueError("first-message authorization candidate fingerprint mismatch")
    return {**supplied, "authorization_candidate_id": candidate_id}


def prepare_first_message_authorization_candidate(
    plan: Mapping[str, Any],
    *,
    registration_status: Mapping[str, Any],
    activation_gate_status: Mapping[str, Any],
    transport_policy: Mapping[str, Any] | None = None,
    existing_one_shot_keys: Iterable[str] = (),
) -> Dict[str, Any]:
    """Bind one exact request while keeping authorization and dispatch false."""

    if not isinstance(registration_status, Mapping):
        raise ValueError("staging registration status must be an object")
    registered_flag = registration_status.get("connector_registered")
    if type(registered_flag) is not bool:
        raise ValueError("staging connector registration flag must be boolean")
    gate = activation_gate.verify_observe_activation_gate_status(
        activation_gate_status
    )
    existing = _existing_keys(existing_one_shot_keys)
    blockers = list(gate["activation_blockers"])
    if not gate["activation_prerequisites_satisfied"]:
        blockers.append("activation gate prerequisites are incomplete")

    registration = None
    if registered_flag:
        registration = (
            staging_registration.verify_registered_no_dispatch_status(
                registration_status
            )
        )
        if registration["activation_gate_id"] != gate["activation_gate_id"]:
            blockers.append("runtime registration is bound to another activation gate")
    else:
        blockers.append("registered-no-dispatch runtime receipt is absent")

    adapter = telegram_adapter.prepare_unregistered_telegram_requests(
        plan,
        transport_policy=transport_policy,
    )
    request = None
    if (
        adapter["transport_envelopes_received"] != 1
        or adapter["adapter_requests_prepared"] != 1
        or len(adapter["requests"]) != 1
    ):
        blockers.append("first-message authorization requires one exact request")
    else:
        request = adapter["requests"][0]
        if request.get("method") != "Bot.send_message":
            blockers.append("first-message adapter method is incompatible")
        if request.get("status") != telegram_adapter.REQUEST_STATUS:
            blockers.append("first-message adapter request is not safely blocked")
        if request.get("chunk_index") != 1 or request.get("chunk_count") != 1:
            blockers.append("first-message authorization requires one exact chunk")
        if request.get("research_evidence_effect") != "NONE":
            blockers.append("first-message request may not affect research evidence")

    registration_id = None
    one_shot_key = None
    chat_binding = None
    message_sha256 = None
    if registration is not None and request is not None:
        registration_id = registration["runtime_connector_registration_id"]
        chat_binding = _chat_binding(request["kwargs"]["chat_id"])
        message_sha256 = hashlib.sha256(
            str(request["kwargs"]["text"]).encode("utf-8")
        ).hexdigest()
        if chat_binding != registration["test_chat_binding_sha256"]:
            blockers.append("first-message destination differs from registration")
        one_shot_key = _one_shot_key(
            registration_id=registration_id,
            activation_gate_id=gate["activation_gate_id"],
            adapter_request_id=_hex(
                request.get("adapter_request_id"), name="adapter_request_id"
            ),
            request_key=_hex(request.get("request_key"), name="request_key"),
            chat_binding_sha256=chat_binding,
            message_sha256=message_sha256,
        )
        if one_shot_key in existing:
            blockers.append("first-message one-shot key already exists")

    blockers = list(dict.fromkeys(blockers))
    candidate = None
    if not blockers and registration is not None and request is not None:
        candidate_payload = {
            "authorization_contract_version": AUTHORIZATION_CONTRACT_VERSION,
            "idempotency_policy_version": IDEMPOTENCY_POLICY_VERSION,
            "mode": MODE,
            "lifecycle_status": LIFECYCLE_STATUS,
            "status": STATUS,
            "owner": OWNER,
            "scope": SCOPE,
            "route": ROUTE,
            "runtime_connector_registration_id": registration_id,
            "activation_gate_id": gate["activation_gate_id"],
            "adapter_batch_id": _hex(
                adapter["adapter_batch_id"], name="adapter_batch_id"
            ),
            "adapter_request_id": _hex(
                request["adapter_request_id"], name="adapter_request_id"
            ),
            "transport_envelope_id": _hex(
                request["transport_envelope_id"], name="transport_envelope_id"
            ),
            "transport_key": _hex(request["transport_key"], name="transport_key"),
            "request_key": _hex(request["request_key"], name="request_key"),
            "test_chat_binding_sha256": chat_binding,
            "message_sha256": message_sha256,
            "chunk_count": 1,
            "one_shot_key": one_shot_key,
            "authorization_granted": False,
            "authorization_consumed": False,
            "candidate_id_may_be_used_as_authorization_id": False,
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
        candidate = {
            **candidate_payload,
            "authorization_candidate_id": _hash(candidate_payload),
        }
        candidate = verify_first_message_authorization_candidate(candidate)

    return {
        "authorization_contract_version": AUTHORIZATION_CONTRACT_VERSION,
        "mode": MODE,
        "lifecycle_status": LIFECYCLE_STATUS,
        "activation_gate_id": gate["activation_gate_id"],
        "runtime_connector_registration_id": registration_id,
        "adapter_batch_id": adapter["adapter_batch_id"],
        "one_shot_key": one_shot_key,
        "authorization_candidate_prepared": candidate is not None,
        "authorization_candidate": candidate,
        "authorization_blockers": blockers,
        "authorization_required": True,
        "authorization_granted": False,
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
