"""Regressions for the non-persisting reservation/consumption contract."""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import research_experimental_preview_first_message_application_gate as application_gate
import research_experimental_preview_first_message_application_gate_selftest as gate_fixtures
import research_experimental_preview_first_message_consumption_contract as contract


ROOT = Path(__file__).resolve().parent


def _ready():
    candidate, approval_record, bot = gate_fixtures._approved()
    gate = application_gate.evaluate_first_message_application_gate(
        approval_record,
        authorization_candidate=candidate,
        observed_at_utc="2026-09-01T09:05:00Z",
    )
    assert gate["status"] == application_gate.READY
    return candidate, gate, bot


def _confirmed(reservation: dict, **overrides) -> dict:
    outcome = {
        "delivery_status": contract.DELIVERY_CONFIRMED,
        "reservation_candidate_id": reservation["reservation_candidate_id"],
        "reservation_persisted": True,
        "atomic_insert_succeeded": True,
        "request_key": reservation["request_key"],
        "one_shot_key": reservation["one_shot_key"],
        "delivery_attempt_id": "d" * 64,
        "telegram_message_id": 12345,
        "confirmed_at_utc": "2026-09-01T09:06:00Z",
        "outcome_uncertain": False,
    }
    outcome.update(overrides)
    return outcome


def _not_attempted(reservation: dict) -> dict:
    return {
        "delivery_status": contract.DELIVERY_NOT_ATTEMPTED,
        "reservation_candidate_id": reservation["reservation_candidate_id"],
        "reservation_persisted": False,
        "atomic_insert_succeeded": False,
        "request_key": reservation["request_key"],
        "one_shot_key": reservation["one_shot_key"],
        "delivery_attempt_id": None,
        "telegram_message_id": None,
        "confirmed_at_utc": None,
        "outcome_uncertain": False,
    }


def _uncertain(reservation: dict) -> dict:
    return {
        "delivery_status": contract.DELIVERY_UNCERTAIN,
        "reservation_candidate_id": reservation["reservation_candidate_id"],
        "reservation_persisted": True,
        "atomic_insert_succeeded": True,
        "request_key": reservation["request_key"],
        "one_shot_key": reservation["one_shot_key"],
        "delivery_attempt_id": "e" * 64,
        "telegram_message_id": None,
        "confirmed_at_utc": None,
        "outcome_uncertain": True,
    }


def _expect_error(fragment: str, callback) -> None:
    try:
        callback()
    except ValueError as exc:
        assert fragment in str(exc), exc
    else:
        raise AssertionError(f"expected ValueError containing {fragment!r}")


def run() -> None:
    candidate, gate, bot = _ready()

    prepared = contract.prepare_reservation_candidate(
        gate,
        authorization_candidate=candidate,
    )
    assert prepared["mode"] == contract.MODE
    assert prepared["status"] == contract.RESERVATION_STATUS
    assert prepared["reservation_candidate_prepared"] is True
    assert prepared["reservation_blockers"] == []
    assert len(prepared["reservation_key"]) == 64
    assert prepared["atomic_persistence_required"] is True
    assert prepared["automatic_retry_after_uncertain_allowed"] is False
    assert prepared["manual_reconciliation_after_uncertain_required"] is True
    assert prepared["persistence_applied"] is False
    assert prepared["reservation_persisted"] is False
    assert prepared["consumption_persisted"] is False
    assert prepared["authorization_consumed"] is False
    assert prepared["dispatch_allowed"] is False
    assert prepared["delivery_allowed"] is False
    assert prepared["delivery_attempts_by_this_contract"] == 0
    assert prepared["telegram_api_calls_by_this_contract"] == 0
    assert prepared["database_writes"] == 0
    assert prepared["research_evidence_writes"] == 0
    assert prepared["stage6_activated"] is False
    assert prepared["live_effect"] == "NONE"
    assert bot.calls == []

    reservation = prepared["reservation_candidate"]
    assert reservation["status"] == contract.RESERVATION_STATUS
    assert len(reservation["reservation_candidate_id"]) == 64
    assert reservation["application_gate_id"] == gate["application_gate_id"]
    assert reservation["owner_approval_id"] == gate["owner_approval_id"]
    assert reservation["one_shot_key"] == candidate["one_shot_key"]
    assert reservation["adapter_request_id"] == candidate["adapter_request_id"]
    assert reservation["request_key"] == candidate["request_key"]
    assert reservation["atomic_persistence_required"] is True
    assert reservation["append_only_required"] is True
    assert reservation["unique_owner_approval_required"] is True
    assert reservation["unique_one_shot_required"] is True
    assert reservation["compare_and_set_required"] is True
    assert reservation["automatic_retry_after_uncertain_allowed"] is False
    assert reservation["persistence_applied"] is False
    assert contract.verify_reservation_candidate(reservation) == reservation
    serialized = json.dumps(reservation, ensure_ascii=False, sort_keys=True)
    assert '"chat_id"' not in serialized
    assert '"text"' not in serialized

    repeated = contract.prepare_reservation_candidate(
        deepcopy(gate),
        authorization_candidate=deepcopy(candidate),
    )
    assert repeated == prepared

    duplicate_reservation = contract.prepare_reservation_candidate(
        gate,
        authorization_candidate=candidate,
        existing_reservation_keys=[reservation["reservation_key"]],
    )
    assert duplicate_reservation["reservation_candidate_prepared"] is False
    assert duplicate_reservation["reservation_candidate"] is None
    assert "first-message reservation key already exists" in (
        duplicate_reservation["reservation_blockers"]
    )
    duplicate_approval = contract.prepare_reservation_candidate(
        gate,
        authorization_candidate=candidate,
        existing_owner_approval_ids=[gate["owner_approval_id"]],
    )
    assert duplicate_approval["reservation_candidate_prepared"] is False
    assert "first-message owner approval already has a reservation" in (
        duplicate_approval["reservation_blockers"]
    )
    duplicate_one_shot = contract.prepare_reservation_candidate(
        gate,
        authorization_candidate=candidate,
        existing_one_shot_keys=[gate["one_shot_key"]],
    )
    assert duplicate_one_shot["reservation_candidate_prepared"] is False
    assert "first-message one-shot key already has a reservation" in (
        duplicate_one_shot["reservation_blockers"]
    )

    blocked_gate = deepcopy(gate)
    blocked_gate["status"] = application_gate.BLOCKED
    blocked_gate["approval_current"] = False
    blocked_gate["application_prerequisites_satisfied"] = False
    blocked_gate["application_blockers"] = [
        "first-message owner approval has expired"
    ]
    blocked_gate["observed_at_utc"] = blocked_gate["expires_at_utc"]
    payload = {
        key: value
        for key, value in blocked_gate.items()
        if key not in {"application_gate_id", *set(application_gate._SAFETY)}
    }
    blocked_gate["application_gate_id"] = application_gate._hash(payload)
    blocked_reservation = contract.prepare_reservation_candidate(
        blocked_gate,
        authorization_candidate=candidate,
    )
    assert blocked_reservation["reservation_candidate_prepared"] is False
    assert "first-message application gate is not ready" in (
        blocked_reservation["reservation_blockers"]
    )
    assert blocked_reservation["dispatch_allowed"] is False
    assert blocked_reservation["database_writes"] == 0

    confirmed = contract.prepare_consumption_candidate(
        reservation,
        delivery_outcome=_confirmed(reservation),
    )
    assert confirmed["status"] == contract.CONSUMPTION_STATUS
    assert confirmed["delivery_status"] == contract.DELIVERY_CONFIRMED
    assert confirmed["consumption_candidate_prepared"] is True
    assert confirmed["consumption_blockers"] == []
    assert len(confirmed["consumption_key"]) == 64
    assert confirmed["atomic_persistence_required"] is True
    assert confirmed["automatic_retry_allowed"] is False
    assert confirmed["manual_reconciliation_required"] is False
    assert confirmed["persistence_applied"] is False
    assert confirmed["consumption_persisted"] is False
    assert confirmed["authorization_consumed"] is False
    assert confirmed["dispatch_allowed"] is False
    assert confirmed["database_writes"] == 0
    assert confirmed["telegram_api_calls_by_this_contract"] == 0
    assert bot.calls == []

    consumption = confirmed["consumption_candidate"]
    assert consumption["status"] == contract.CONSUMPTION_STATUS
    assert consumption["reservation_candidate_id"] == reservation[
        "reservation_candidate_id"
    ]
    assert consumption["reservation_key"] == reservation["reservation_key"]
    assert consumption["one_shot_key"] == reservation["one_shot_key"]
    assert consumption["delivery_attempt_id"] == "d" * 64
    assert consumption["telegram_message_id"] == 12345
    assert consumption["atomic_persistence_required"] is True
    assert consumption["append_only_required"] is True
    assert consumption["compare_and_set_from_reserved_required"] is True
    assert consumption["automatic_retry_allowed"] is False
    assert consumption["consumption_persisted"] is False
    assert contract.verify_consumption_candidate(consumption) == consumption

    confirmed_repeated = contract.prepare_consumption_candidate(
        deepcopy(reservation),
        delivery_outcome=deepcopy(_confirmed(reservation)),
    )
    assert confirmed_repeated == confirmed

    duplicate_consumption = contract.prepare_consumption_candidate(
        reservation,
        delivery_outcome=_confirmed(reservation),
        existing_consumption_keys=[consumption["consumption_key"]],
    )
    assert duplicate_consumption["consumption_candidate_prepared"] is False
    assert "first-message consumption key already exists" in (
        duplicate_consumption["consumption_blockers"]
    )
    already_consumed = contract.prepare_consumption_candidate(
        reservation,
        delivery_outcome=_confirmed(reservation),
        consumed_owner_approval_ids=[reservation["owner_approval_id"]],
        consumed_one_shot_keys=[reservation["one_shot_key"]],
    )
    assert already_consumed["consumption_candidate_prepared"] is False
    assert len(already_consumed["consumption_blockers"]) == 2
    assert already_consumed["automatic_retry_allowed"] is False

    not_attempted = contract.prepare_consumption_candidate(
        reservation,
        delivery_outcome=_not_attempted(reservation),
    )
    assert not_attempted["consumption_candidate_prepared"] is False
    assert "first-message delivery has not been attempted" in (
        not_attempted["consumption_blockers"]
    )
    assert "first-message reservation is not atomically persisted" in (
        not_attempted["consumption_blockers"]
    )
    assert not_attempted["manual_reconciliation_required"] is False
    assert not_attempted["automatic_retry_allowed"] is False

    uncertain = contract.prepare_consumption_candidate(
        reservation,
        delivery_outcome=_uncertain(reservation),
    )
    assert uncertain["delivery_status"] == contract.DELIVERY_UNCERTAIN
    assert uncertain["consumption_candidate_prepared"] is False
    assert "first-message delivery outcome is uncertain" in uncertain[
        "consumption_blockers"
    ]
    assert "automatic retry is forbidden after uncertain delivery" in uncertain[
        "consumption_blockers"
    ]
    assert uncertain["manual_reconciliation_required"] is True
    assert uncertain["automatic_retry_allowed"] is False
    assert uncertain["authorization_consumed"] is False
    assert uncertain["dispatch_allowed"] is False
    assert bot.calls == []

    _expect_error(
        "requires a persisted reservation",
        lambda: contract.prepare_consumption_candidate(
            reservation,
            delivery_outcome=_confirmed(
                reservation,
                reservation_persisted=False,
                atomic_insert_succeeded=False,
            ),
        ),
    )
    _expect_error(
        "outside approval window",
        lambda: contract.prepare_consumption_candidate(
            reservation,
            delivery_outcome=_confirmed(
                reservation,
                confirmed_at_utc=reservation["expires_at_utc"],
            ),
        ),
    )
    tampered_consumption = deepcopy(consumption)
    tampered_consumption["telegram_message_id"] = 99999
    _expect_error(
        "consumption candidate fingerprint mismatch",
        lambda: contract.verify_consumption_candidate(tampered_consumption),
    )
    assert bot.calls == []

    source = (
        ROOT / "research_experimental_preview_first_message_consumption_contract.py"
    ).read_text(encoding="utf-8").lower()
    for forbidden in (
        "import telegram",
        "from telegram",
        "import psycopg",
        "import sqlite",
        "os.getenv",
        "execute(",
        "send_message(",
        "reply_text(",
        "create_task(",
        "commandhandler(",
        "research_formula_store",
        "research_formula_worker",
    ):
        assert forbidden not in source

    for disconnected_file in (
        "research_experimental_preview_first_message_authorization.py",
        "research_experimental_preview_first_message_owner_approval.py",
        "research_experimental_preview_first_message_application_gate.py",
        "ai_candidate_main.py",
        "main.py",
        "ai_telegram.py",
        "research_formula_worker.py",
        "research_formula_store.py",
        "research_formula_schema_admin.py",
    ):
        disconnected_source = (ROOT / disconnected_file).read_text(
            encoding="utf-8"
        )
        assert (
            "research_experimental_preview_first_message_consumption_contract"
            not in disconnected_source
        )

    print("research_experimental_preview_first_message_consumption_contract_selftest: ok")


if __name__ == "__main__":
    run()
