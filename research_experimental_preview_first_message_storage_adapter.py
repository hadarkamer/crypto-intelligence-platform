"""Database-unregistered transaction adapter for one PREVIEW message claim.

The adapter contains the SQL needed for append-only reservation and
consumption compare-and-set operations, but the runtime database lifecycle is
deliberately unregistered.  Its default path is fail-closed.  The only
executable path in this version requires an explicitly marked, already-active
transaction double so the SQL contract can be tested without a database.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Mapping

import research_experimental_preview_first_message_consumption_contract as contract


ADAPTER_VERSION = "preview-first-message-storage-adapter-v1-database-unregistered"
MODE = "PREVIEW_FIRST_MESSAGE_STORAGE_ADAPTER_DATABASE_UNREGISTERED"
MIGRATION_NAME = "019_preview_first_message_reservation_consumption_v1.sql"

DATABASE_UNREGISTERED = "DATABASE_UNREGISTERED"
TEST_TRANSACTION_DOUBLE = "TEST_TRANSACTION_DOUBLE"
BLOCKED_DATABASE_UNREGISTERED = "BLOCKED_DATABASE_UNREGISTERED"
TEST_COMPARE_AND_SET_SUCCEEDED = "TEST_COMPARE_AND_SET_SUCCEEDED"
TEST_COMPARE_AND_SET_NOT_CLAIMED = "TEST_COMPARE_AND_SET_NOT_CLAIMED"

RESERVATION_OPERATION = "RESERVATION"
CONSUMPTION_OPERATION = "CONSUMPTION"

_SAFETY = {
    "migration_registered": True,
    "migration_applied": False,
    "database_registered": False,
    "real_database_access": False,
    "persistence_applied": False,
    "authorization_consumed": False,
    "dispatch_allowed": False,
    "delivery_allowed": False,
    "handler_registered": False,
    "scheduler_registered": False,
    "worker_registered": False,
    "public_opt_in": False,
    "stage6_activated": False,
    "telegram_api_calls": 0,
    "database_connections": 0,
    "database_writes": 0,
    "research_evidence_writes": 0,
    "research_evidence_effect": "NONE",
    "delivery_channel": "NONE",
    "live_effect": "NONE",
}


RESERVE_SQL = """
INSERT INTO research_preview_first_message_reservations (
    reservation_key,
    reservation_candidate_id,
    contract_version,
    reservation_policy_version,
    application_gate_id,
    owner_approval_id,
    authorization_candidate_id,
    one_shot_key,
    runtime_connector_registration_id,
    activation_gate_id,
    adapter_request_id,
    request_key,
    observed_at_utc,
    expires_at_utc,
    reservation_payload
) VALUES (
    %(reservation_key)s,
    %(reservation_candidate_id)s,
    %(contract_version)s,
    %(reservation_policy_version)s,
    %(application_gate_id)s,
    %(owner_approval_id)s,
    %(authorization_candidate_id)s,
    %(one_shot_key)s,
    %(runtime_connector_registration_id)s,
    %(activation_gate_id)s,
    %(adapter_request_id)s,
    %(request_key)s,
    %(observed_at_utc)s,
    %(expires_at_utc)s,
    %(reservation_payload)s::JSONB
)
ON CONFLICT DO NOTHING
RETURNING reservation_key, reservation_candidate_id
""".strip()


CONSUME_SQL = """
WITH eligible_reservation AS (
    SELECT reservation_key
    FROM research_preview_first_message_reservations
    WHERE reservation_key = %(reservation_key)s
      AND reservation_candidate_id = %(reservation_candidate_id)s
      AND owner_approval_id = %(owner_approval_id)s
      AND one_shot_key = %(one_shot_key)s
      AND adapter_request_id = %(adapter_request_id)s
      AND request_key = %(request_key)s
      AND %(confirmed_at_utc)s::TIMESTAMPTZ >= observed_at_utc
      AND %(confirmed_at_utc)s::TIMESTAMPTZ < expires_at_utc
    FOR UPDATE
), inserted_consumption AS (
    INSERT INTO research_preview_first_message_consumptions (
        consumption_key,
        consumption_candidate_id,
        contract_version,
        consumption_policy_version,
        reservation_key,
        owner_approval_id,
        one_shot_key,
        adapter_request_id,
        request_key,
        delivery_attempt_id,
        telegram_message_id,
        confirmed_at_utc,
        consumption_payload
    )
    SELECT
        %(consumption_key)s,
        %(consumption_candidate_id)s,
        %(contract_version)s,
        %(consumption_policy_version)s,
        %(reservation_key)s,
        %(owner_approval_id)s,
        %(one_shot_key)s,
        %(adapter_request_id)s,
        %(request_key)s,
        %(delivery_attempt_id)s,
        %(telegram_message_id)s,
        %(confirmed_at_utc)s,
        %(consumption_payload)s::JSONB
    FROM eligible_reservation
    WHERE NOT EXISTS (
        SELECT 1
        FROM research_preview_first_message_consumptions
        WHERE reservation_key = %(reservation_key)s
           OR owner_approval_id = %(owner_approval_id)s
           OR one_shot_key = %(one_shot_key)s
    )
    ON CONFLICT DO NOTHING
    RETURNING consumption_key, consumption_candidate_id
)
SELECT consumption_key, consumption_candidate_id
FROM inserted_consumption
""".strip()


def status() -> Dict[str, Any]:
    """Describe the closed adapter lifecycle without touching a database."""

    return {
        "adapter_version": ADAPTER_VERSION,
        "mode": MODE,
        "lifecycle_status": DATABASE_UNREGISTERED,
        "migration_name": MIGRATION_NAME,
        "default_execution_scope": DATABASE_UNREGISTERED,
        "transaction_scope_required": True,
        "caller_commit_required": True,
        "append_only_required": True,
        "unique_constraints_required": True,
        "compare_and_set_required": True,
        "transaction_double_execution_supported": True,
        "runtime_execution_supported": False,
        **_SAFETY,
    }


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _blocked(*, operation: str, candidate_key: str) -> Dict[str, Any]:
    return {
        **status(),
        "operation": operation,
        "status": BLOCKED_DATABASE_UNREGISTERED,
        "candidate_key": candidate_key,
        "compare_and_set_succeeded": False,
        "transaction_double_effect_applied": False,
        "transaction_double_calls": 0,
        "transaction_double_writes": 0,
        "commit_attempts": 0,
        "blockers": ["PREVIEW first-message database adapter is unregistered"],
    }


def _require_transaction_double(transaction: Any) -> None:
    if transaction is None or getattr(
        transaction,
        "preview_first_message_transaction_double",
        None,
    ) is not True:
        raise RuntimeError("explicit PREVIEW first-message transaction double required")
    if getattr(transaction, "transaction_active", None) is not True:
        raise RuntimeError("PREVIEW first-message transaction must already be active")
    if not callable(getattr(transaction, "execute", None)):
        raise RuntimeError("PREVIEW first-message transaction double lacks execute")


def _returned_value(row: Any, name: str, position: int) -> Any:
    if isinstance(row, Mapping):
        return row.get(name)
    if isinstance(row, (tuple, list)) and len(row) > position:
        return row[position]
    raise RuntimeError("PREVIEW first-message transaction returned an invalid row")


def _test_result(
    *,
    operation: str,
    candidate_key: str,
    claimed: bool,
) -> Dict[str, Any]:
    return {
        **status(),
        "operation": operation,
        "status": (
            TEST_COMPARE_AND_SET_SUCCEEDED
            if claimed
            else TEST_COMPARE_AND_SET_NOT_CLAIMED
        ),
        "candidate_key": candidate_key,
        "compare_and_set_succeeded": claimed,
        "transaction_double_effect_applied": claimed,
        "transaction_double_calls": 1,
        "transaction_double_writes": 1 if claimed else 0,
        "commit_attempts": 0,
        "blockers": [] if claimed else [
            "PREVIEW first-message compare-and-set did not claim a row"
        ],
    }


def reserve(
    reservation_candidate: Mapping[str, Any],
    *,
    transaction: Any = None,
    execution_scope: str = DATABASE_UNREGISTERED,
) -> Dict[str, Any]:
    """Fail closed by default; exercise reservation SQL only on a test double."""

    reservation = contract.verify_reservation_candidate(reservation_candidate)
    candidate_key = reservation["reservation_key"]
    if execution_scope == DATABASE_UNREGISTERED:
        return _blocked(
            operation=RESERVATION_OPERATION,
            candidate_key=candidate_key,
        )
    if execution_scope != TEST_TRANSACTION_DOUBLE:
        raise ValueError("PREVIEW first-message execution scope is unsupported")
    _require_transaction_double(transaction)
    params = {
        name: reservation[name]
        for name in (
            "reservation_key",
            "reservation_candidate_id",
            "contract_version",
            "reservation_policy_version",
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
        )
    }
    params["reservation_payload"] = _canonical_json(reservation)
    row = transaction.execute(RESERVE_SQL, params).fetchone()
    claimed = row is not None
    if claimed:
        returned_key = str(_returned_value(row, "reservation_key", 0) or "")
        returned_id = str(
            _returned_value(row, "reservation_candidate_id", 1) or ""
        )
        if returned_key != candidate_key or returned_id != reservation[
            "reservation_candidate_id"
        ]:
            raise RuntimeError("PREVIEW first-message reservation receipt mismatch")
    return _test_result(
        operation=RESERVATION_OPERATION,
        candidate_key=candidate_key,
        claimed=claimed,
    )


def consume(
    consumption_candidate: Mapping[str, Any],
    *,
    transaction: Any = None,
    execution_scope: str = DATABASE_UNREGISTERED,
) -> Dict[str, Any]:
    """Fail closed by default; exercise consumption CAS only on a test double."""

    consumption = contract.verify_consumption_candidate(consumption_candidate)
    candidate_key = consumption["consumption_key"]
    if execution_scope == DATABASE_UNREGISTERED:
        return _blocked(
            operation=CONSUMPTION_OPERATION,
            candidate_key=candidate_key,
        )
    if execution_scope != TEST_TRANSACTION_DOUBLE:
        raise ValueError("PREVIEW first-message execution scope is unsupported")
    _require_transaction_double(transaction)
    params = {
        name: consumption[name]
        for name in (
            "consumption_key",
            "consumption_candidate_id",
            "contract_version",
            "consumption_policy_version",
            "reservation_candidate_id",
            "reservation_key",
            "owner_approval_id",
            "one_shot_key",
            "adapter_request_id",
            "request_key",
            "delivery_attempt_id",
            "telegram_message_id",
            "confirmed_at_utc",
        )
    }
    params["consumption_payload"] = _canonical_json(consumption)
    row = transaction.execute(CONSUME_SQL, params).fetchone()
    claimed = row is not None
    if claimed:
        returned_key = str(_returned_value(row, "consumption_key", 0) or "")
        returned_id = str(
            _returned_value(row, "consumption_candidate_id", 1) or ""
        )
        if returned_key != candidate_key or returned_id != consumption[
            "consumption_candidate_id"
        ]:
            raise RuntimeError("PREVIEW first-message consumption receipt mismatch")
    return _test_result(
        operation=CONSUMPTION_OPERATION,
        candidate_key=candidate_key,
        claimed=claimed,
    )
