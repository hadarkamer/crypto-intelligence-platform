"""Non-persisting reservation/consumption contract for one PREVIEW message.

The pure contract prepares content-addressed reservation and consumption
candidates and declares the uniqueness/compare-and-set rules a future storage
adapter must enforce atomically.  It performs no database write or dispatch.
An uncertain delivery outcome keeps the reservation closed and forbids
automatic retry so a crash cannot silently create a duplicate message.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import re
from typing import Any, Dict, Iterable, Mapping

import research_experimental_preview_first_message_application_gate as application_gate
import research_experimental_preview_first_message_authorization as authorization


CONTRACT_VERSION = "preview-first-message-consumption-v1-not-persisted"
RESERVATION_POLICY_VERSION = "preview-first-message-reservation-unique-v1"
CONSUMPTION_POLICY_VERSION = "preview-first-message-consumption-unique-v1"
MODE = "PREVIEW_FIRST_MESSAGE_RESERVATION_CONSUMPTION_CONTRACT_ONLY"

RESERVATION_STATUS = "RESERVATION_PREPARED_NOT_PERSISTED"
CONSUMPTION_STATUS = "CONSUMPTION_PREPARED_NOT_PERSISTED"
NO_CANDIDATE = "NO_PERSISTENCE_CANDIDATE"

DELIVERY_NOT_ATTEMPTED = "NOT_ATTEMPTED"
DELIVERY_CONFIRMED = "CONFIRMED"
DELIVERY_UNCERTAIN = "UNCERTAIN"

_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_UTC_SECOND = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")
_OUTCOME_KEYS = {
    "delivery_status",
    "reservation_candidate_id",
    "reservation_persisted",
    "atomic_insert_succeeded",
    "request_key",
    "one_shot_key",
    "delivery_attempt_id",
    "telegram_message_id",
    "confirmed_at_utc",
    "outcome_uncertain",
}
_SAFETY = {
    "persistence_applied": False,
    "reservation_persisted": False,
    "consumption_persisted": False,
    "authorization_consumed": False,
    "dispatch_allowed": False,
    "delivery_allowed": False,
    "handler_registered": False,
    "scheduler_registered": False,
    "worker_registered": False,
    "public_opt_in": False,
    "stage6_activated": False,
    "delivery_attempts_by_this_contract": 0,
    "telegram_api_calls_by_this_contract": 0,
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


def _ids(values: Iterable[str], *, name: str) -> set[str]:
    if isinstance(values, (str, bytes, bytearray, Mapping)):
        raise ValueError(f"{name} must be an iterable of ids")
    return {_hex(value, name=f"{name} item") for value in values}


def _reservation_key(gate: Mapping[str, Any]) -> str:
    return _hash(
        {
            "reservation_policy_version": RESERVATION_POLICY_VERSION,
            "application_gate_id": gate["application_gate_id"],
            "owner_approval_id": gate["owner_approval_id"],
            "one_shot_key": gate["one_shot_key"],
        }
    )


def _consumption_key(reservation: Mapping[str, Any]) -> str:
    return _hash(
        {
            "consumption_policy_version": CONSUMPTION_POLICY_VERSION,
            "reservation_key": reservation["reservation_key"],
            "owner_approval_id": reservation["owner_approval_id"],
            "one_shot_key": reservation["one_shot_key"],
        }
    )


def verify_reservation_candidate(candidate: Mapping[str, Any]) -> Dict[str, Any]:
    """Verify one reservation candidate without treating it as persisted."""

    if not isinstance(candidate, Mapping):
        raise ValueError("first-message reservation candidate must be an object")
    supplied = dict(candidate)
    candidate_id = _hex(
        supplied.pop("reservation_candidate_id", None),
        name="reservation_candidate_id",
    )
    expected_keys = {
        "contract_version",
        "reservation_policy_version",
        "mode",
        "status",
        "application_gate_id",
        "owner_approval_id",
        "authorization_candidate_id",
        "one_shot_key",
        "runtime_connector_registration_id",
        "activation_gate_id",
        "adapter_request_id",
        "request_key",
        "observed_at_utc",
        "expires_at_utc",
        "reservation_key",
        "atomic_persistence_required",
        "append_only_required",
        "unique_owner_approval_required",
        "unique_one_shot_required",
        "compare_and_set_required",
        "automatic_retry_after_uncertain_allowed",
        "manual_reconciliation_after_uncertain_required",
        *set(_SAFETY),
    }
    if set(supplied) != expected_keys:
        raise ValueError("first-message reservation candidate fields are invalid")
    constants = {
        "contract_version": CONTRACT_VERSION,
        "reservation_policy_version": RESERVATION_POLICY_VERSION,
        "mode": MODE,
        "status": RESERVATION_STATUS,
    }
    for name, expected in constants.items():
        if supplied.get(name) != expected:
            raise ValueError(f"first-message reservation {name} is incompatible")
    identifiers = {}
    for name in (
        "application_gate_id",
        "owner_approval_id",
        "authorization_candidate_id",
        "one_shot_key",
        "runtime_connector_registration_id",
        "activation_gate_id",
        "adapter_request_id",
        "request_key",
        "reservation_key",
    ):
        identifiers[name] = _hex(supplied.get(name), name=name)
    _, observed_dt = _utc(supplied.get("observed_at_utc"), name="observed_at_utc")
    _, expires_dt = _utc(supplied.get("expires_at_utc"), name="expires_at_utc")
    if observed_dt >= expires_dt:
        raise ValueError("first-message reservation candidate is already expired")
    for name in (
        "atomic_persistence_required",
        "append_only_required",
        "unique_owner_approval_required",
        "unique_one_shot_required",
        "compare_and_set_required",
        "manual_reconciliation_after_uncertain_required",
    ):
        if supplied.get(name) is not True:
            raise ValueError(f"first-message reservation requires {name}")
    if supplied.get("automatic_retry_after_uncertain_allowed") is not False:
        raise ValueError("first-message reservation forbids uncertain automatic retry")
    expected_key = _hash(
        {
            "reservation_policy_version": RESERVATION_POLICY_VERSION,
            "application_gate_id": identifiers["application_gate_id"],
            "owner_approval_id": identifiers["owner_approval_id"],
            "one_shot_key": identifiers["one_shot_key"],
        }
    )
    if identifiers["reservation_key"] != expected_key:
        raise ValueError("first-message reservation key fingerprint mismatch")
    for name, expected in _SAFETY.items():
        if supplied.get(name) != expected:
            raise ValueError(f"first-message reservation safety failed: {name}")
    if candidate_id != _hash(supplied):
        raise ValueError("first-message reservation candidate fingerprint mismatch")
    return {**supplied, "reservation_candidate_id": candidate_id}


def prepare_reservation_candidate(
    application_gate_status: Mapping[str, Any],
    *,
    authorization_candidate: Mapping[str, Any],
    existing_reservation_keys: Iterable[str] = (),
    existing_owner_approval_ids: Iterable[str] = (),
    existing_one_shot_keys: Iterable[str] = (),
) -> Dict[str, Any]:
    """Prepare a unique reservation row without persisting or applying it."""

    gate = application_gate.verify_first_message_application_gate_status(
        application_gate_status
    )
    candidate = authorization.verify_first_message_authorization_candidate(
        authorization_candidate
    )
    if candidate["authorization_candidate_id"] != gate[
        "authorization_candidate_id"
    ]:
        raise ValueError("reservation candidate differs from application gate")
    for name in (
        "one_shot_key",
        "runtime_connector_registration_id",
        "activation_gate_id",
    ):
        if candidate[name] != gate[name]:
            raise ValueError(f"reservation candidate {name} differs from gate")

    existing_reservations = _ids(
        existing_reservation_keys,
        name="existing_reservation_keys",
    )
    existing_approvals = _ids(
        existing_owner_approval_ids,
        name="existing_owner_approval_ids",
    )
    existing_one_shots = _ids(
        existing_one_shot_keys,
        name="existing_one_shot_keys",
    )
    reservation_key = _reservation_key(gate)
    blockers = list(gate["application_blockers"])
    if gate["status"] != application_gate.READY or not gate[
        "application_prerequisites_satisfied"
    ]:
        blockers.append("first-message application gate is not ready")
    if reservation_key in existing_reservations:
        blockers.append("first-message reservation key already exists")
    if gate["owner_approval_id"] in existing_approvals:
        blockers.append("first-message owner approval already has a reservation")
    if gate["one_shot_key"] in existing_one_shots:
        blockers.append("first-message one-shot key already has a reservation")
    blockers = list(dict.fromkeys(blockers))

    reservation = None
    if not blockers:
        payload = {
            "contract_version": CONTRACT_VERSION,
            "reservation_policy_version": RESERVATION_POLICY_VERSION,
            "mode": MODE,
            "status": RESERVATION_STATUS,
            "application_gate_id": gate["application_gate_id"],
            "owner_approval_id": gate["owner_approval_id"],
            "authorization_candidate_id": gate["authorization_candidate_id"],
            "one_shot_key": gate["one_shot_key"],
            "runtime_connector_registration_id": gate[
                "runtime_connector_registration_id"
            ],
            "activation_gate_id": gate["activation_gate_id"],
            "adapter_request_id": candidate["adapter_request_id"],
            "request_key": candidate["request_key"],
            "observed_at_utc": gate["observed_at_utc"],
            "expires_at_utc": gate["expires_at_utc"],
            "reservation_key": reservation_key,
            "atomic_persistence_required": True,
            "append_only_required": True,
            "unique_owner_approval_required": True,
            "unique_one_shot_required": True,
            "compare_and_set_required": True,
            "automatic_retry_after_uncertain_allowed": False,
            "manual_reconciliation_after_uncertain_required": True,
            **_SAFETY,
        }
        reservation = {
            **payload,
            "reservation_candidate_id": _hash(payload),
        }
        reservation = verify_reservation_candidate(reservation)
    return {
        "contract_version": CONTRACT_VERSION,
        "mode": MODE,
        "status": RESERVATION_STATUS if reservation is not None else NO_CANDIDATE,
        "reservation_candidate_prepared": reservation is not None,
        "reservation_candidate": reservation,
        "reservation_key": reservation_key,
        "reservation_blockers": blockers,
        "atomic_persistence_required": True,
        "automatic_retry_after_uncertain_allowed": False,
        "manual_reconciliation_after_uncertain_required": True,
        **_SAFETY,
    }


def _delivery_outcome(
    value: Mapping[str, Any],
    *,
    reservation: Mapping[str, Any],
) -> Dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("first-message delivery outcome must be an object")
    supplied = dict(value)
    if set(supplied) != _OUTCOME_KEYS:
        raise ValueError("first-message delivery outcome fields are invalid")
    status = str(supplied.get("delivery_status") or "")
    if status not in {
        DELIVERY_NOT_ATTEMPTED,
        DELIVERY_CONFIRMED,
        DELIVERY_UNCERTAIN,
    }:
        raise ValueError("first-message delivery outcome status is invalid")
    if _hex(
        supplied.get("reservation_candidate_id"),
        name="outcome reservation_candidate_id",
    ) != reservation["reservation_candidate_id"]:
        raise ValueError("delivery outcome differs from reservation candidate")
    if _hex(supplied.get("request_key"), name="outcome request_key") != (
        reservation["request_key"]
    ):
        raise ValueError("delivery outcome request differs from reservation")
    if _hex(supplied.get("one_shot_key"), name="outcome one_shot_key") != (
        reservation["one_shot_key"]
    ):
        raise ValueError("delivery outcome one-shot key differs from reservation")
    for name in (
        "reservation_persisted",
        "atomic_insert_succeeded",
        "outcome_uncertain",
    ):
        if type(supplied.get(name)) is not bool:
            raise ValueError(f"first-message delivery outcome {name} must be boolean")

    attempt_id = supplied.get("delivery_attempt_id")
    message_id = supplied.get("telegram_message_id")
    confirmed_at = supplied.get("confirmed_at_utc")
    if status == DELIVERY_NOT_ATTEMPTED:
        if attempt_id is not None or message_id is not None or confirmed_at is not None:
            raise ValueError("not-attempted delivery may not contain a receipt")
        if supplied["outcome_uncertain"]:
            raise ValueError("not-attempted delivery may not be uncertain")
    else:
        attempt_id = _hex(attempt_id, name="delivery_attempt_id")
    if status == DELIVERY_CONFIRMED:
        if not supplied["reservation_persisted"] or not supplied[
            "atomic_insert_succeeded"
        ]:
            raise ValueError("confirmed delivery requires a persisted reservation")
        if supplied["outcome_uncertain"]:
            raise ValueError("confirmed delivery may not be uncertain")
        if type(message_id) is not int or message_id == 0:
            raise ValueError("confirmed delivery requires a Telegram message id")
        confirmed_at, confirmed_dt = _utc(
            confirmed_at,
            name="confirmed_at_utc",
        )
        _, observed_dt = _utc(
            reservation["observed_at_utc"],
            name="reservation observed_at_utc",
        )
        _, expires_dt = _utc(
            reservation["expires_at_utc"],
            name="reservation expires_at_utc",
        )
        if not observed_dt <= confirmed_dt < expires_dt:
            raise ValueError("confirmed delivery falls outside approval window")
    if status == DELIVERY_UNCERTAIN:
        if not supplied["reservation_persisted"] or not supplied[
            "atomic_insert_succeeded"
        ]:
            raise ValueError("uncertain delivery requires a persisted reservation")
        if not supplied["outcome_uncertain"]:
            raise ValueError("uncertain delivery must set outcome_uncertain")
        if message_id is not None or confirmed_at is not None:
            raise ValueError("uncertain delivery may not claim confirmation")
    return {
        **supplied,
        "delivery_attempt_id": attempt_id,
        "confirmed_at_utc": confirmed_at,
    }


def verify_consumption_candidate(candidate: Mapping[str, Any]) -> Dict[str, Any]:
    """Verify a confirmed-delivery consumption candidate, still not persisted."""

    if not isinstance(candidate, Mapping):
        raise ValueError("first-message consumption candidate must be an object")
    supplied = dict(candidate)
    candidate_id = _hex(
        supplied.pop("consumption_candidate_id", None),
        name="consumption_candidate_id",
    )
    expected_keys = {
        "contract_version",
        "consumption_policy_version",
        "mode",
        "status",
        "reservation_candidate_id",
        "reservation_key",
        "owner_approval_id",
        "one_shot_key",
        "adapter_request_id",
        "request_key",
        "delivery_attempt_id",
        "telegram_message_id",
        "confirmed_at_utc",
        "consumption_key",
        "atomic_persistence_required",
        "append_only_required",
        "compare_and_set_from_reserved_required",
        "automatic_retry_allowed",
        *set(_SAFETY),
    }
    if set(supplied) != expected_keys:
        raise ValueError("first-message consumption candidate fields are invalid")
    if supplied.get("contract_version") != CONTRACT_VERSION or supplied.get(
        "consumption_policy_version"
    ) != CONSUMPTION_POLICY_VERSION:
        raise ValueError("first-message consumption version is incompatible")
    if supplied.get("mode") != MODE or supplied.get("status") != CONSUMPTION_STATUS:
        raise ValueError("first-message consumption lifecycle is incompatible")
    identifiers = {}
    for name in (
        "reservation_candidate_id",
        "reservation_key",
        "owner_approval_id",
        "one_shot_key",
        "adapter_request_id",
        "request_key",
        "delivery_attempt_id",
        "consumption_key",
    ):
        identifiers[name] = _hex(supplied.get(name), name=name)
    if type(supplied.get("telegram_message_id")) is not int or supplied[
        "telegram_message_id"
    ] == 0:
        raise ValueError("first-message consumption message id is invalid")
    _utc(supplied.get("confirmed_at_utc"), name="confirmed_at_utc")
    for name in (
        "atomic_persistence_required",
        "append_only_required",
        "compare_and_set_from_reserved_required",
    ):
        if supplied.get(name) is not True:
            raise ValueError(f"first-message consumption requires {name}")
    if supplied.get("automatic_retry_allowed") is not False:
        raise ValueError("first-message consumption forbids automatic retry")
    expected_key = _hash(
        {
            "consumption_policy_version": CONSUMPTION_POLICY_VERSION,
            "reservation_key": identifiers["reservation_key"],
            "owner_approval_id": identifiers["owner_approval_id"],
            "one_shot_key": identifiers["one_shot_key"],
        }
    )
    if identifiers["consumption_key"] != expected_key:
        raise ValueError("first-message consumption key fingerprint mismatch")
    for name, expected in _SAFETY.items():
        if supplied.get(name) != expected:
            raise ValueError(f"first-message consumption safety failed: {name}")
    if candidate_id != _hash(supplied):
        raise ValueError("first-message consumption candidate fingerprint mismatch")
    return {**supplied, "consumption_candidate_id": candidate_id}


def prepare_consumption_candidate(
    reservation_candidate: Mapping[str, Any],
    *,
    delivery_outcome: Mapping[str, Any],
    existing_consumption_keys: Iterable[str] = (),
    consumed_owner_approval_ids: Iterable[str] = (),
    consumed_one_shot_keys: Iterable[str] = (),
) -> Dict[str, Any]:
    """Prepare consumption only for confirmed delivery; never persist it."""

    reservation = verify_reservation_candidate(reservation_candidate)
    outcome = _delivery_outcome(delivery_outcome, reservation=reservation)
    existing_consumptions = _ids(
        existing_consumption_keys,
        name="existing_consumption_keys",
    )
    consumed_approvals = _ids(
        consumed_owner_approval_ids,
        name="consumed_owner_approval_ids",
    )
    consumed_one_shots = _ids(
        consumed_one_shot_keys,
        name="consumed_one_shot_keys",
    )
    consumption_key = _consumption_key(reservation)
    blockers = []
    manual_reconciliation = False
    if outcome["delivery_status"] == DELIVERY_NOT_ATTEMPTED:
        blockers.append("first-message delivery has not been attempted")
    if outcome["delivery_status"] == DELIVERY_UNCERTAIN:
        blockers.append("first-message delivery outcome is uncertain")
        blockers.append("automatic retry is forbidden after uncertain delivery")
        manual_reconciliation = True
    if not outcome["reservation_persisted"] or not outcome[
        "atomic_insert_succeeded"
    ]:
        blockers.append("first-message reservation is not atomically persisted")
    if consumption_key in existing_consumptions:
        blockers.append("first-message consumption key already exists")
    if reservation["owner_approval_id"] in consumed_approvals:
        blockers.append("first-message owner approval is already consumed")
    if reservation["one_shot_key"] in consumed_one_shots:
        blockers.append("first-message one-shot key is already consumed")
    blockers = list(dict.fromkeys(blockers))

    consumption = None
    if not blockers and outcome["delivery_status"] == DELIVERY_CONFIRMED:
        payload = {
            "contract_version": CONTRACT_VERSION,
            "consumption_policy_version": CONSUMPTION_POLICY_VERSION,
            "mode": MODE,
            "status": CONSUMPTION_STATUS,
            "reservation_candidate_id": reservation[
                "reservation_candidate_id"
            ],
            "reservation_key": reservation["reservation_key"],
            "owner_approval_id": reservation["owner_approval_id"],
            "one_shot_key": reservation["one_shot_key"],
            "adapter_request_id": reservation["adapter_request_id"],
            "request_key": reservation["request_key"],
            "delivery_attempt_id": outcome["delivery_attempt_id"],
            "telegram_message_id": outcome["telegram_message_id"],
            "confirmed_at_utc": outcome["confirmed_at_utc"],
            "consumption_key": consumption_key,
            "atomic_persistence_required": True,
            "append_only_required": True,
            "compare_and_set_from_reserved_required": True,
            "automatic_retry_allowed": False,
            **_SAFETY,
        }
        consumption = {
            **payload,
            "consumption_candidate_id": _hash(payload),
        }
        consumption = verify_consumption_candidate(consumption)
    return {
        "contract_version": CONTRACT_VERSION,
        "mode": MODE,
        "status": CONSUMPTION_STATUS if consumption is not None else NO_CANDIDATE,
        "delivery_status": outcome["delivery_status"],
        "consumption_candidate_prepared": consumption is not None,
        "consumption_candidate": consumption,
        "consumption_key": consumption_key,
        "consumption_blockers": blockers,
        "atomic_persistence_required": True,
        "automatic_retry_allowed": False,
        "manual_reconciliation_required": manual_reconciliation,
        **_SAFETY,
    }
