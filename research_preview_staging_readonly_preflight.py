"""One-shot, fail-closed PREVIEW staging database preflight.

This module is deliberately disconnected from every application runtime.  It
accepts only the dedicated staging database's Render-internal URL, forces a
read-only PostgreSQL session and explicit read-only transaction, performs one
identity/schema query, and always rolls the transaction back.
"""

from __future__ import annotations

import json
import os
from typing import Any, Callable, Dict, Mapping, Sequence
from urllib.parse import urlparse

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None


PREFLIGHT_VERSION = "preview-staging-readonly-preflight-v1"
MODE = "ONE_SHOT_RENDER_INTERNAL_READ_ONLY"

PREFLIGHT_ENABLED_ENV = "FORMULA_PREVIEW_STAGING_DATABASE_PREFLIGHT"
DATABASE_URL_ENV = "FORMULA_PREVIEW_STAGING_DATABASE_URL"

EXPECTED_RENDER_POSTGRES_ID = "dpg-dab7rc2d0e5s73dkb9l0-a"
EXPECTED_INTERNAL_HOST = EXPECTED_RENDER_POSTGRES_ID
EXPECTED_DATABASE_NAME = "crypto_intelligence_staging_db"

BEGIN_SQL = (
    "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
)
ROLLBACK_SQL = "ROLLBACK"
PREFLIGHT_SQL = """
SELECT
    current_database() AS database_name,
    current_setting('server_version') AS postgres_version,
    current_schema() AS current_schema,
    current_setting('transaction_read_only') AS transaction_read_only,
    has_schema_privilege(current_user, 'public', 'USAGE') AS public_schema_usage,
    has_schema_privilege(current_user, 'public', 'CREATE') AS public_schema_create,
    EXISTS (
        SELECT 1 FROM pg_extension WHERE extname = 'plpgsql'
    ) AS plpgsql_available,
    to_regclass(
        'public.research_preview_first_message_reservations'
    ) IS NOT NULL AS reservation_table_exists,
    to_regclass(
        'public.research_preview_first_message_consumptions'
    ) IS NOT NULL AS consumption_table_exists,
    to_regprocedure(
        'public.validate_preview_first_message_consumption()'
    ) IS NOT NULL AS validation_function_exists,
    to_regprocedure(
        'public.prevent_preview_first_message_storage_mutation()'
    ) IS NOT NULL AS append_only_function_exists,
    (
        SELECT count(*)
        FROM pg_trigger
        WHERE tgname IN (
            'trg_validate_preview_first_message_consumption',
            'trg_preview_first_message_reservations_append_only',
            'trg_preview_first_message_reservations_no_truncate',
            'trg_preview_first_message_consumptions_append_only',
            'trg_preview_first_message_consumptions_no_truncate'
        )
          AND NOT tgisinternal
    ) AS migration_trigger_count
""".strip()

_ROW_FIELDS = (
    "database_name",
    "postgres_version",
    "current_schema",
    "transaction_read_only",
    "public_schema_usage",
    "public_schema_create",
    "plpgsql_available",
    "reservation_table_exists",
    "consumption_table_exists",
    "validation_function_exists",
    "append_only_function_exists",
    "migration_trigger_count",
)

_SAFETY = {
    "connection_scope": "RENDER_INTERNAL_ONLY",
    "read_only_session_required": True,
    "read_only_transaction_required": True,
    "rollback_required": True,
    "schema_mutation_allowed": False,
    "migration_apply_allowed": False,
    "candidate_service_connected": False,
    "runtime_registered": False,
    "handler_registered": False,
    "scheduler_registered": False,
    "worker_registered": False,
    "telegram_api_calls": 0,
    "database_writes": 0,
    "delivery_allowed": False,
    "stage6_activated": False,
    "research_evidence_effect": "NONE",
    "live_effect": "NONE",
}


def _is_enabled(environment: Mapping[str, Any], name: str) -> bool:
    return str(environment.get(name, "")).strip() == "1"


def resolve_configuration(environment: Mapping[str, Any]) -> Dict[str, Any]:
    """Validate the dedicated internal target without exposing its URL."""

    if not isinstance(environment, Mapping):
        raise ValueError("preflight environment must be a mapping")
    if not _is_enabled(environment, PREFLIGHT_ENABLED_ENV):
        raise RuntimeError(
            f"refusing database preflight: set {PREFLIGHT_ENABLED_ENV}=1 explicitly"
        )
    for mutation_flag in (
        "FORMULA_SCHEMA_APPLY",
        "RESEARCH_SCHEMA_APPLY",
        "RESEARCH_USE_PRIMARY_DATABASE",
    ):
        if _is_enabled(environment, mutation_flag):
            raise RuntimeError(
                f"refusing database preflight while {mutation_flag}=1"
            )

    database_url = str(environment.get(DATABASE_URL_ENV, "")).strip()
    if not database_url:
        raise RuntimeError(
            f"refusing database preflight: {DATABASE_URL_ENV} is required"
        )
    parsed = urlparse(database_url)
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise ValueError("staging database URL must use the PostgreSQL scheme")
    if parsed.hostname != EXPECTED_INTERNAL_HOST:
        raise ValueError("staging database URL is not the expected internal host")
    if parsed.port not in (None, 5432):
        raise ValueError("staging database URL uses an unexpected port")
    if parsed.path != f"/{EXPECTED_DATABASE_NAME}":
        raise ValueError("staging database URL names an unexpected database")
    if not parsed.username or not parsed.password:
        raise ValueError("staging database URL requires dedicated credentials")
    if parsed.query or parsed.fragment:
        raise ValueError("staging database URL may not contain query or fragment data")

    return {
        "database_url": database_url,
        "database_url_source": DATABASE_URL_ENV,
        "render_postgres_id": EXPECTED_RENDER_POSTGRES_ID,
        "expected_database_name": EXPECTED_DATABASE_NAME,
        "internal_target_verified": True,
    }


def _row_mapping(row: Any) -> Dict[str, Any]:
    if isinstance(row, Mapping):
        missing = [name for name in _ROW_FIELDS if name not in row]
        if missing:
            raise RuntimeError("preflight query omitted required fields")
        values = {name: row.get(name) for name in _ROW_FIELDS}
    elif isinstance(row, Sequence) and not isinstance(
        row, (str, bytes, bytearray)
    ):
        if len(row) != len(_ROW_FIELDS):
            raise RuntimeError("preflight query returned an unexpected row shape")
        values = dict(zip(_ROW_FIELDS, row))
    else:
        raise RuntimeError("preflight query returned an invalid row")
    return values


def _evaluate(values: Mapping[str, Any]) -> Dict[str, Any]:
    if values.get("database_name") != EXPECTED_DATABASE_NAME:
        raise RuntimeError("connected database identity does not match staging")
    if values.get("current_schema") != "public":
        raise RuntimeError("connected database schema is not public")
    if values.get("transaction_read_only") != "on":
        raise RuntimeError("database transaction is not read-only")

    trigger_count = values.get("migration_trigger_count")
    if type(trigger_count) is not int or trigger_count < 0:
        raise RuntimeError("preflight trigger count is invalid")
    object_flags = {
        "reservation_table_exists": values.get("reservation_table_exists") is True,
        "consumption_table_exists": values.get("consumption_table_exists") is True,
        "validation_function_exists": values.get("validation_function_exists") is True,
        "append_only_function_exists": values.get("append_only_function_exists") is True,
    }
    schema_object_count = sum(object_flags.values()) + trigger_count
    migration_fully_applied = all(object_flags.values()) and trigger_count == 5
    blockers = []
    if values.get("public_schema_usage") is not True:
        blockers.append("public schema usage privilege is absent")
    if values.get("public_schema_create") is not True:
        blockers.append("public schema create privilege is absent")
    if values.get("plpgsql_available") is not True:
        blockers.append("plpgsql is unavailable")
    if schema_object_count:
        blockers.append("migration 019 schema objects already exist")
    ready = not blockers
    return {
        "preflight_version": PREFLIGHT_VERSION,
        "mode": MODE,
        "status": (
            "READY_FOR_SEPARATE_MIGRATION_019_DECISION"
            if ready
            else "PREFLIGHT_BLOCKED"
        ),
        "render_postgres_id": EXPECTED_RENDER_POSTGRES_ID,
        "database_name": EXPECTED_DATABASE_NAME,
        "postgres_version": str(values.get("postgres_version") or ""),
        "current_schema": "public",
        "transaction_read_only": "on",
        "public_schema_usage": values.get("public_schema_usage") is True,
        "public_schema_create": values.get("public_schema_create") is True,
        "plpgsql_available": values.get("plpgsql_available") is True,
        **object_flags,
        "migration_trigger_count": trigger_count,
        "schema_object_count": schema_object_count,
        "migration_019_objects_present": schema_object_count > 0,
        "migration_019_applied": migration_fully_applied,
        "ready_for_separate_migration_019_decision": ready,
        "blockers": blockers,
        "database_connections": 1,
        "transactions_started": 1,
        "read_only_queries_executed": 1,
        "transaction_rolled_back": True,
        **_SAFETY,
    }


def run_preflight(
    environment: Mapping[str, Any],
    *,
    connect: Callable[..., Any] | None = None,
) -> Dict[str, Any]:
    """Run one read-only query and unconditionally roll back and close."""

    configuration = resolve_configuration(environment)
    connector = connect
    if connector is None:
        if psycopg is None:
            raise RuntimeError("psycopg is unavailable")
        connector = psycopg.connect

    connection = connector(
        configuration["database_url"],
        connect_timeout=5,
        autocommit=True,
        options=(
            "-c default_transaction_read_only=on "
            "-c statement_timeout=8000 "
            "-c lock_timeout=2000"
        ),
    )
    began = False
    try:
        connection.execute(BEGIN_SQL)
        began = True
        row = connection.execute(PREFLIGHT_SQL).fetchone()
        if row is None:
            raise RuntimeError("preflight query returned no row")
        return _evaluate(_row_mapping(row))
    finally:
        try:
            if began:
                connection.execute(ROLLBACK_SQL)
        finally:
            connection.close()


def _failed_closed(error: BaseException) -> Dict[str, Any]:
    return {
        "preflight_version": PREFLIGHT_VERSION,
        "mode": MODE,
        "status": "PREFLIGHT_FAILED_CLOSED",
        "error_type": type(error).__name__,
        "render_postgres_id": EXPECTED_RENDER_POSTGRES_ID,
        "database_url_exposed": False,
        **_SAFETY,
    }


def main() -> int:
    try:
        result = run_preflight(os.environ)
    except Exception as exc:
        print(json.dumps(_failed_closed(exc), sort_keys=True), flush=True)
        return 1
    print(json.dumps(result, sort_keys=True), flush=True)
    return 0 if result["ready_for_separate_migration_019_decision"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
