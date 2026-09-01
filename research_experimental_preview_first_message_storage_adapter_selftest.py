"""Regressions for the database-unregistered first-message storage adapter."""

from __future__ import annotations

from copy import deepcopy
import json
import os
from pathlib import Path
from unittest.mock import patch

import research_experimental_preview_first_message_consumption_contract as contract
import research_experimental_preview_first_message_consumption_contract_selftest as fixtures
import research_experimental_preview_first_message_storage_adapter as adapter
import research_formula_schema_admin


ROOT = Path(__file__).resolve().parent


class _Cursor:
    def __init__(self, row):
        self._row = row
        self.fetchone_calls = 0

    def fetchone(self):
        self.fetchone_calls += 1
        return self._row


class _TransactionDouble:
    preview_first_message_transaction_double = True

    def __init__(self, *rows, active=True):
        self.transaction_active = active
        self._rows = list(rows)
        self.calls = []

    def execute(self, sql, params):
        self.calls.append((sql, deepcopy(params)))
        row = self._rows.pop(0) if self._rows else None
        return _Cursor(row)


def _expect_error(error_type, fragment: str, callback) -> None:
    try:
        callback()
    except error_type as exc:
        assert fragment in str(exc), exc
    else:
        raise AssertionError(
            f"expected {error_type.__name__} containing {fragment!r}"
        )


def _candidates():
    authorization, gate, bot = fixtures._ready()
    prepared_reservation = contract.prepare_reservation_candidate(
        gate,
        authorization_candidate=authorization,
    )
    reservation = prepared_reservation["reservation_candidate"]
    prepared_consumption = contract.prepare_consumption_candidate(
        reservation,
        delivery_outcome=fixtures._confirmed(reservation),
    )
    consumption = prepared_consumption["consumption_candidate"]
    assert reservation is not None
    assert consumption is not None
    assert bot.calls == []
    return reservation, consumption, bot


def _assert_closed(result: dict) -> None:
    assert result["adapter_version"] == adapter.ADAPTER_VERSION
    assert result["mode"] == adapter.MODE
    assert result["lifecycle_status"] == adapter.DATABASE_UNREGISTERED
    assert result["migration_name"] == adapter.MIGRATION_NAME
    assert result["migration_registered"] is True
    assert result["migration_applied"] is False
    assert result["database_registered"] is False
    assert result["real_database_access"] is False
    assert result["persistence_applied"] is False
    assert result["authorization_consumed"] is False
    assert result["dispatch_allowed"] is False
    assert result["delivery_allowed"] is False
    assert result["handler_registered"] is False
    assert result["scheduler_registered"] is False
    assert result["worker_registered"] is False
    assert result["stage6_activated"] is False
    assert result["telegram_api_calls"] == 0
    assert result["database_connections"] == 0
    assert result["database_writes"] == 0
    assert result["research_evidence_writes"] == 0
    assert result["research_evidence_effect"] == "NONE"
    assert result["delivery_channel"] == "NONE"
    assert result["live_effect"] == "NONE"


def run() -> None:
    reservation, consumption, bot = _candidates()

    lifecycle = adapter.status()
    _assert_closed(lifecycle)
    assert lifecycle["default_execution_scope"] == adapter.DATABASE_UNREGISTERED
    assert lifecycle["transaction_scope_required"] is True
    assert lifecycle["caller_commit_required"] is True
    assert lifecycle["append_only_required"] is True
    assert lifecycle["unique_constraints_required"] is True
    assert lifecycle["compare_and_set_required"] is True
    assert lifecycle["transaction_double_execution_supported"] is True
    assert lifecycle["runtime_execution_supported"] is False

    blocked_transaction = _TransactionDouble(
        {
            "reservation_key": reservation["reservation_key"],
            "reservation_candidate_id": reservation["reservation_candidate_id"],
        }
    )
    blocked_reservation = adapter.reserve(
        reservation,
        transaction=blocked_transaction,
    )
    _assert_closed(blocked_reservation)
    assert blocked_reservation["status"] == adapter.BLOCKED_DATABASE_UNREGISTERED
    assert blocked_reservation["compare_and_set_succeeded"] is False
    assert blocked_reservation["transaction_double_effect_applied"] is False
    assert blocked_reservation["transaction_double_calls"] == 0
    assert blocked_reservation["transaction_double_writes"] == 0
    assert blocked_reservation["commit_attempts"] == 0
    assert blocked_transaction.calls == []

    blocked_consumption = adapter.consume(
        consumption,
        transaction=blocked_transaction,
    )
    _assert_closed(blocked_consumption)
    assert blocked_consumption["status"] == adapter.BLOCKED_DATABASE_UNREGISTERED
    assert blocked_transaction.calls == []

    reserve_transaction = _TransactionDouble(
        {
            "reservation_key": reservation["reservation_key"],
            "reservation_candidate_id": reservation["reservation_candidate_id"],
        }
    )
    reserved = adapter.reserve(
        reservation,
        transaction=reserve_transaction,
        execution_scope=adapter.TEST_TRANSACTION_DOUBLE,
    )
    _assert_closed(reserved)
    assert reserved["status"] == adapter.TEST_COMPARE_AND_SET_SUCCEEDED
    assert reserved["operation"] == adapter.RESERVATION_OPERATION
    assert reserved["candidate_key"] == reservation["reservation_key"]
    assert reserved["compare_and_set_succeeded"] is True
    assert reserved["transaction_double_effect_applied"] is True
    assert reserved["transaction_double_calls"] == 1
    assert reserved["transaction_double_writes"] == 1
    assert reserved["commit_attempts"] == 0
    assert len(reserve_transaction.calls) == 1
    reserve_sql, reserve_params = reserve_transaction.calls[0]
    assert reserve_sql == adapter.RESERVE_SQL
    assert "ON CONFLICT DO NOTHING" in reserve_sql
    assert "RETURNING reservation_key, reservation_candidate_id" in reserve_sql
    assert "UPDATE" not in reserve_sql.upper()
    assert "DELETE" not in reserve_sql.upper()
    assert reserve_params["reservation_key"] == reservation["reservation_key"]
    assert reserve_params["owner_approval_id"] == reservation["owner_approval_id"]
    assert reserve_params["one_shot_key"] == reservation["one_shot_key"]
    assert json.loads(reserve_params["reservation_payload"]) == reservation

    duplicate_reservation_transaction = _TransactionDouble(None)
    duplicate_reservation = adapter.reserve(
        reservation,
        transaction=duplicate_reservation_transaction,
        execution_scope=adapter.TEST_TRANSACTION_DOUBLE,
    )
    _assert_closed(duplicate_reservation)
    assert duplicate_reservation["status"] == (
        adapter.TEST_COMPARE_AND_SET_NOT_CLAIMED
    )
    assert duplicate_reservation["compare_and_set_succeeded"] is False
    assert duplicate_reservation["transaction_double_effect_applied"] is False
    assert duplicate_reservation["transaction_double_writes"] == 0
    assert duplicate_reservation["commit_attempts"] == 0

    consume_transaction = _TransactionDouble(
        (
            consumption["consumption_key"],
            consumption["consumption_candidate_id"],
        )
    )
    consumed = adapter.consume(
        consumption,
        transaction=consume_transaction,
        execution_scope=adapter.TEST_TRANSACTION_DOUBLE,
    )
    _assert_closed(consumed)
    assert consumed["status"] == adapter.TEST_COMPARE_AND_SET_SUCCEEDED
    assert consumed["operation"] == adapter.CONSUMPTION_OPERATION
    assert consumed["candidate_key"] == consumption["consumption_key"]
    assert consumed["compare_and_set_succeeded"] is True
    assert consumed["transaction_double_writes"] == 1
    assert consumed["commit_attempts"] == 0
    assert len(consume_transaction.calls) == 1
    consume_sql, consume_params = consume_transaction.calls[0]
    assert consume_sql == adapter.CONSUME_SQL
    assert "WITH eligible_reservation AS" in consume_sql
    assert "FOR UPDATE" in consume_sql
    assert "WHERE NOT EXISTS" in consume_sql
    assert "ON CONFLICT DO NOTHING" in consume_sql
    assert "confirmed_at_utc)s::TIMESTAMPTZ < expires_at_utc" in consume_sql
    assert consume_params["reservation_candidate_id"] == reservation[
        "reservation_candidate_id"
    ]
    assert consume_params["owner_approval_id"] == reservation["owner_approval_id"]
    assert consume_params["one_shot_key"] == reservation["one_shot_key"]
    assert json.loads(consume_params["consumption_payload"]) == consumption

    missing_or_duplicate_transaction = _TransactionDouble(None)
    not_consumed = adapter.consume(
        consumption,
        transaction=missing_or_duplicate_transaction,
        execution_scope=adapter.TEST_TRANSACTION_DOUBLE,
    )
    _assert_closed(not_consumed)
    assert not_consumed["status"] == adapter.TEST_COMPARE_AND_SET_NOT_CLAIMED
    assert not_consumed["compare_and_set_succeeded"] is False
    assert not_consumed["transaction_double_writes"] == 0

    _expect_error(
        RuntimeError,
        "transaction double required",
        lambda: adapter.reserve(
            reservation,
            transaction=object(),
            execution_scope=adapter.TEST_TRANSACTION_DOUBLE,
        ),
    )
    _expect_error(
        RuntimeError,
        "must already be active",
        lambda: adapter.consume(
            consumption,
            transaction=_TransactionDouble(active=False),
            execution_scope=adapter.TEST_TRANSACTION_DOUBLE,
        ),
    )
    _expect_error(
        ValueError,
        "execution scope is unsupported",
        lambda: adapter.reserve(
            reservation,
            transaction=_TransactionDouble(),
            execution_scope="RUNTIME_DATABASE_REGISTERED",
        ),
    )
    tampered_reservation = deepcopy(reservation)
    tampered_reservation["one_shot_key"] = "f" * 64
    _expect_error(
        ValueError,
        "reservation key fingerprint mismatch",
        lambda: adapter.reserve(tampered_reservation),
    )
    tampered_consumption = deepcopy(consumption)
    tampered_consumption["telegram_message_id"] = 99999
    _expect_error(
        ValueError,
        "consumption candidate fingerprint mismatch",
        lambda: adapter.consume(tampered_consumption),
    )

    bad_receipt_transaction = _TransactionDouble(
        {
            "reservation_key": "f" * 64,
            "reservation_candidate_id": reservation["reservation_candidate_id"],
        }
    )
    _expect_error(
        RuntimeError,
        "reservation receipt mismatch",
        lambda: adapter.reserve(
            reservation,
            transaction=bad_receipt_transaction,
            execution_scope=adapter.TEST_TRANSACTION_DOUBLE,
        ),
    )

    migration_path = ROOT / "migrations" / adapter.MIGRATION_NAME
    migration_text = migration_path.read_text(encoding="utf-8")
    migration_lower = migration_text.lower()
    assert "explicit" in migration_lower
    assert "registration does not apply" in migration_lower
    assert "research_preview_first_message_reservations" in migration_text
    assert "research_preview_first_message_consumptions" in migration_text
    assert "owner_approval_id CHAR(64) NOT NULL UNIQUE" in migration_text
    assert "one_shot_key CHAR(64) NOT NULL UNIQUE" in migration_text
    assert "reservation_key CHAR(64) NOT NULL UNIQUE" in migration_text
    assert "delivery_attempt_id CHAR(64) NOT NULL UNIQUE" in migration_text
    assert "expires_at_utc > observed_at_utc" in migration_text
    assert "BEFORE UPDATE OR DELETE" in migration_text
    assert "BEFORE TRUNCATE" in migration_text
    assert "validate_preview_first_message_consumption" in migration_text
    assert "NEW.confirmed_at_utc < reservation.expires_at_utc" in migration_text

    registered_path_names = [
        path.name for path in research_formula_schema_admin.MIGRATION_PATHS
    ]
    assert adapter.MIGRATION_NAME in registered_path_names
    assert registered_path_names.count(adapter.MIGRATION_NAME) == 1
    schema_admin_source = (ROOT / "research_formula_schema_admin.py").read_text(
        encoding="utf-8"
    )
    assert adapter.MIGRATION_NAME in schema_admin_source
    assert "FORMULA_SCHEMA_APPLY" in schema_admin_source
    assert "pg_advisory_xact_lock" in schema_admin_source
    with patch.dict(os.environ, {}, clear=True):
        closed_schema_status = research_formula_schema_admin.status()
        assert closed_schema_status["schema_apply_enabled"] is False
        assert closed_schema_status["database_configured"] is False
        _expect_error(
            RuntimeError,
            "set FORMULA_SCHEMA_APPLY=1 explicitly",
            research_formula_schema_admin.apply_schema,
        )

    adapter_source = (
        ROOT / "research_experimental_preview_first_message_storage_adapter.py"
    ).read_text(encoding="utf-8").lower()
    for forbidden in (
        "import psycopg",
        "from psycopg",
        "import telegram",
        "from telegram",
        "os.getenv",
        "connect(",
        ".commit(",
        "send_message(",
        "reply_text(",
        "create_task(",
        "commandhandler(",
        "research_formula_store",
        "research_formula_worker",
    ):
        assert forbidden not in adapter_source

    for disconnected_file in (
        "research_experimental_preview_first_message_authorization.py",
        "research_experimental_preview_first_message_owner_approval.py",
        "research_experimental_preview_first_message_application_gate.py",
        "research_experimental_preview_first_message_consumption_contract.py",
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
            "research_experimental_preview_first_message_storage_adapter"
            not in disconnected_source
        )

    assert bot.calls == []
    print("research_experimental_preview_first_message_storage_adapter_selftest: ok")


if __name__ == "__main__":
    run()
