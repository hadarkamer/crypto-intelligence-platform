"""Fail-closed, one-shot installer for PREVIEW staging migration 019 only.

This module is deliberately disconnected from every application runtime. It
pins the exact Render-internal staging target and migration checksum, applies
one SQL file inside one explicit transaction, verifies the catalog before
commit, and never grants runtime or delivery authority.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Sequence
from urllib.parse import urlparse

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None


INSTALLER_VERSION = "preview-staging-migration-019-installer-v1"
MODE = "ONE_SHOT_RENDER_INTERNAL_MIGRATION_019_ONLY"

APPLY_ENABLED_ENV = "FORMULA_PREVIEW_STAGING_MIGRATION_019_APPLY"
DATABASE_URL_ENV = "FORMULA_PREVIEW_STAGING_DATABASE_URL"

EXPECTED_RENDER_POSTGRES_ID = "dpg-dab7rc2d0e5s73dkb9l0-a"
EXPECTED_INTERNAL_HOST = EXPECTED_RENDER_POSTGRES_ID
EXPECTED_DATABASE_NAME = "crypto_intelligence_staging_db"
EXPECTED_DATABASE_USER = "crypto_intelligence_staging_migration_019"
EXPECTED_POSTGRES_MAJOR = 18

ROOT = Path(__file__).resolve().parent
MIGRATION_FILENAME = "019_preview_first_message_reservation_consumption_v1.sql"
MIGRATION_PATH = ROOT / "migrations" / MIGRATION_FILENAME
MIGRATION_SHA256 = (
    "81690a298a029b3bb131f7906e496d17748dbfb32124d87f438882e14e7e9c05"
)

SCHEMA_LOCK_ID = 94837242
BEGIN_SQL = "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE"
SET_LOCK_TIMEOUT_SQL = "SET LOCAL lock_timeout = '2000ms'"
SET_STATEMENT_TIMEOUT_SQL = "SET LOCAL statement_timeout = '15000ms'"
SET_SEARCH_PATH_SQL = "SET LOCAL search_path TO public, pg_catalog"
LOCK_SQL = "SELECT pg_advisory_xact_lock(%s)"
COMMIT_SQL = "COMMIT"
ROLLBACK_SQL = "ROLLBACK"

_FORBIDDEN_ENABLED_FLAGS = (
    "FORMULA_SCHEMA_APPLY",
    "RESEARCH_SCHEMA_APPLY",
    "RESEARCH_USE_PRIMARY_DATABASE",
    "FORMULA_PREVIEW_STAGING_DATABASE_PREFLIGHT",
)
_FORBIDDEN_DATABASE_URLS = (
    "DATABASE_URL",
    "RESEARCH_DATABASE_URL",
)

PRECONDITION_SQL = """
SELECT
    current_database() AS database_name,
    current_user AS database_user,
    current_setting('server_version_num')::INTEGER AS postgres_version_num,
    current_schema() AS current_schema,
    current_setting('transaction_read_only') AS transaction_read_only,
    has_schema_privilege(current_user, 'public', 'USAGE') AS public_schema_usage,
    has_schema_privilege(current_user, 'public', 'CREATE') AS public_schema_create,
    EXISTS (
        SELECT 1 FROM pg_extension WHERE extname = 'plpgsql'
    ) AS plpgsql_available,
    (
        SELECT count(*)::INTEGER
        FROM pg_class AS relation
        JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
        WHERE relation.relname IN (
            'research_preview_first_message_reservations',
            'research_preview_first_message_consumptions'
        )
          AND relation.relkind IN ('r', 'p')
          AND namespace.nspname <> 'information_schema'
          AND namespace.nspname !~ '^pg_'
    ) AS migration_relation_count,
    (
        SELECT count(*)::INTEGER
        FROM pg_proc AS procedure
        JOIN pg_namespace AS namespace ON namespace.oid = procedure.pronamespace
        WHERE procedure.proname IN (
            'validate_preview_first_message_consumption',
            'prevent_preview_first_message_storage_mutation'
        )
          AND namespace.nspname <> 'information_schema'
          AND namespace.nspname !~ '^pg_'
    ) AS migration_function_count,
    (
        SELECT count(*)::INTEGER
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

VERIFICATION_SQL = """
WITH relation_state AS (
    SELECT
        count(*)::INTEGER AS named_relation_count
    FROM pg_class AS relation
    JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
    WHERE relation.relname IN (
        'research_preview_first_message_reservations',
        'research_preview_first_message_consumptions'
    )
      AND relation.relkind IN ('r', 'p')
      AND namespace.nspname <> 'information_schema'
      AND namespace.nspname !~ '^pg_'
),
function_state AS (
    SELECT
        count(*)::INTEGER AS named_function_count,
        count(*) FILTER (
            WHERE namespace.nspname = 'public'
              AND procedure.pronargs = 0
              AND procedure.prorettype = 'trigger'::regtype
              AND language.lanname = 'plpgsql'
        )::INTEGER AS exact_function_count
    FROM pg_proc AS procedure
    JOIN pg_namespace AS namespace ON namespace.oid = procedure.pronamespace
    JOIN pg_language AS language ON language.oid = procedure.prolang
    WHERE procedure.proname IN (
        'validate_preview_first_message_consumption',
        'prevent_preview_first_message_storage_mutation'
    )
      AND namespace.nspname <> 'information_schema'
      AND namespace.nspname !~ '^pg_'
),
column_state AS (
    SELECT
        count(*) FILTER (
            WHERE attribute.attrelid =
                to_regclass('public.research_preview_first_message_reservations')
        )::INTEGER AS reservation_column_count,
        count(*) FILTER (
            WHERE attribute.attrelid =
                to_regclass('public.research_preview_first_message_reservations')
              AND attribute.attnotnull
        )::INTEGER AS reservation_not_null_column_count,
        count(*) FILTER (
            WHERE attribute.attrelid =
                to_regclass('public.research_preview_first_message_consumptions')
        )::INTEGER AS consumption_column_count,
        count(*) FILTER (
            WHERE attribute.attrelid =
                to_regclass('public.research_preview_first_message_consumptions')
              AND attribute.attnotnull
        )::INTEGER AS consumption_not_null_column_count
    FROM pg_attribute AS attribute
    WHERE attribute.attnum > 0
      AND NOT attribute.attisdropped
      AND attribute.attrelid IN (
          to_regclass('public.research_preview_first_message_reservations'),
          to_regclass('public.research_preview_first_message_consumptions')
      )
),
constraint_state AS (
    SELECT
        count(*) FILTER (
            WHERE constraint_record.conrelid =
                to_regclass('public.research_preview_first_message_reservations')
              AND constraint_record.contype = 'p'
        )::INTEGER AS reservation_primary_key_count,
        count(*) FILTER (
            WHERE constraint_record.conrelid =
                to_regclass('public.research_preview_first_message_reservations')
              AND constraint_record.contype = 'u'
        )::INTEGER AS reservation_unique_count,
        count(*) FILTER (
            WHERE constraint_record.conrelid =
                to_regclass('public.research_preview_first_message_reservations')
              AND constraint_record.contype = 'c'
        )::INTEGER AS reservation_check_count,
        count(*) FILTER (
            WHERE constraint_record.conrelid =
                to_regclass('public.research_preview_first_message_consumptions')
              AND constraint_record.contype = 'p'
        )::INTEGER AS consumption_primary_key_count,
        count(*) FILTER (
            WHERE constraint_record.conrelid =
                to_regclass('public.research_preview_first_message_consumptions')
              AND constraint_record.contype = 'u'
        )::INTEGER AS consumption_unique_count,
        count(*) FILTER (
            WHERE constraint_record.conrelid =
                to_regclass('public.research_preview_first_message_consumptions')
              AND constraint_record.contype = 'c'
        )::INTEGER AS consumption_check_count,
        count(*) FILTER (
            WHERE constraint_record.conrelid =
                to_regclass('public.research_preview_first_message_consumptions')
              AND constraint_record.contype = 'f'
        )::INTEGER AS consumption_foreign_key_count
    FROM pg_constraint AS constraint_record
    WHERE constraint_record.conrelid IN (
        to_regclass('public.research_preview_first_message_reservations'),
        to_regclass('public.research_preview_first_message_consumptions')
    )
),
trigger_state AS (
    SELECT
        count(*)::INTEGER AS named_trigger_count,
        count(*) FILTER (WHERE trigger_record.tgenabled = 'O')::INTEGER
            AS enabled_trigger_count,
        count(*) FILTER (
            WHERE trigger_record.tgenabled = 'O'
              AND (
                  (
                      trigger_record.tgname =
                          'trg_validate_preview_first_message_consumption'
                      AND trigger_record.tgrelid = to_regclass(
                          'public.research_preview_first_message_consumptions'
                      )
                      AND trigger_record.tgfoid = to_regprocedure(
                          'public.validate_preview_first_message_consumption()'
                      )
                      AND trigger_record.tgtype = 7
                  ) OR (
                      trigger_record.tgname =
                          'trg_preview_first_message_reservations_append_only'
                      AND trigger_record.tgrelid = to_regclass(
                          'public.research_preview_first_message_reservations'
                      )
                      AND trigger_record.tgfoid = to_regprocedure(
                          'public.prevent_preview_first_message_storage_mutation()'
                      )
                      AND trigger_record.tgtype = 27
                  ) OR (
                      trigger_record.tgname =
                          'trg_preview_first_message_reservations_no_truncate'
                      AND trigger_record.tgrelid = to_regclass(
                          'public.research_preview_first_message_reservations'
                      )
                      AND trigger_record.tgfoid = to_regprocedure(
                          'public.prevent_preview_first_message_storage_mutation()'
                      )
                      AND trigger_record.tgtype = 34
                  ) OR (
                      trigger_record.tgname =
                          'trg_preview_first_message_consumptions_append_only'
                      AND trigger_record.tgrelid = to_regclass(
                          'public.research_preview_first_message_consumptions'
                      )
                      AND trigger_record.tgfoid = to_regprocedure(
                          'public.prevent_preview_first_message_storage_mutation()'
                      )
                      AND trigger_record.tgtype = 27
                  ) OR (
                      trigger_record.tgname =
                          'trg_preview_first_message_consumptions_no_truncate'
                      AND trigger_record.tgrelid = to_regclass(
                          'public.research_preview_first_message_consumptions'
                      )
                      AND trigger_record.tgfoid = to_regprocedure(
                          'public.prevent_preview_first_message_storage_mutation()'
                      )
                      AND trigger_record.tgtype = 34
                  )
              )
        )::INTEGER AS exact_trigger_mapping_count
    FROM pg_trigger AS trigger_record
    WHERE trigger_record.tgname IN (
        'trg_validate_preview_first_message_consumption',
        'trg_preview_first_message_reservations_append_only',
        'trg_preview_first_message_reservations_no_truncate',
        'trg_preview_first_message_consumptions_append_only',
        'trg_preview_first_message_consumptions_no_truncate'
    )
      AND NOT trigger_record.tgisinternal
)
SELECT
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
        SELECT count(*)::BIGINT
        FROM public.research_preview_first_message_reservations
    ) AS reservation_row_count,
    (
        SELECT count(*)::BIGINT
        FROM public.research_preview_first_message_consumptions
    ) AS consumption_row_count,
    relation_state.named_relation_count,
    function_state.named_function_count,
    function_state.exact_function_count,
    column_state.reservation_column_count,
    column_state.reservation_not_null_column_count,
    column_state.consumption_column_count,
    column_state.consumption_not_null_column_count,
    constraint_state.reservation_primary_key_count,
    constraint_state.reservation_unique_count,
    constraint_state.reservation_check_count,
    constraint_state.consumption_primary_key_count,
    constraint_state.consumption_unique_count,
    constraint_state.consumption_check_count,
    constraint_state.consumption_foreign_key_count,
    EXISTS (
        SELECT 1
        FROM pg_constraint AS binding
        WHERE binding.conname =
            'uq_preview_first_message_reservation_binding'
          AND binding.contype = 'u'
          AND binding.conrelid = to_regclass(
              'public.research_preview_first_message_reservations'
          )
          AND (
              SELECT array_agg(attribute.attname::TEXT ORDER BY key.ordinality)
              FROM unnest(binding.conkey) WITH ORDINALITY AS key(attnum, ordinality)
              JOIN pg_attribute AS attribute
                ON attribute.attrelid = binding.conrelid
               AND attribute.attnum = key.attnum
          ) = ARRAY[
              'reservation_key',
              'owner_approval_id',
              'one_shot_key',
              'adapter_request_id',
              'request_key'
          ]::TEXT[]
    ) AS reservation_binding_exact,
    EXISTS (
        SELECT 1
        FROM pg_constraint AS binding
        WHERE binding.conname =
            'fk_preview_first_message_consumption_reservation'
          AND binding.contype = 'f'
          AND binding.conrelid = to_regclass(
              'public.research_preview_first_message_consumptions'
          )
          AND binding.confrelid = to_regclass(
              'public.research_preview_first_message_reservations'
          )
          AND (
              SELECT array_agg(attribute.attname::TEXT ORDER BY key.ordinality)
              FROM unnest(binding.conkey) WITH ORDINALITY AS key(attnum, ordinality)
              JOIN pg_attribute AS attribute
                ON attribute.attrelid = binding.conrelid
               AND attribute.attnum = key.attnum
          ) = ARRAY[
              'reservation_key',
              'owner_approval_id',
              'one_shot_key',
              'adapter_request_id',
              'request_key'
          ]::TEXT[]
          AND (
              SELECT array_agg(attribute.attname::TEXT ORDER BY key.ordinality)
              FROM unnest(binding.confkey) WITH ORDINALITY AS key(attnum, ordinality)
              JOIN pg_attribute AS attribute
                ON attribute.attrelid = binding.confrelid
               AND attribute.attnum = key.attnum
          ) = ARRAY[
              'reservation_key',
              'owner_approval_id',
              'one_shot_key',
              'adapter_request_id',
              'request_key'
          ]::TEXT[]
    ) AS consumption_foreign_key_binding_exact,
    trigger_state.named_trigger_count,
    trigger_state.enabled_trigger_count,
    trigger_state.exact_trigger_mapping_count
FROM relation_state, function_state, column_state, constraint_state, trigger_state
""".strip()

_PRECONDITION_FIELDS = (
    "database_name",
    "database_user",
    "postgres_version_num",
    "current_schema",
    "transaction_read_only",
    "public_schema_usage",
    "public_schema_create",
    "plpgsql_available",
    "migration_relation_count",
    "migration_function_count",
    "migration_trigger_count",
)

_VERIFICATION_FIELDS = (
    "reservation_table_exists",
    "consumption_table_exists",
    "validation_function_exists",
    "append_only_function_exists",
    "reservation_row_count",
    "consumption_row_count",
    "named_relation_count",
    "named_function_count",
    "exact_function_count",
    "reservation_column_count",
    "reservation_not_null_column_count",
    "consumption_column_count",
    "consumption_not_null_column_count",
    "reservation_primary_key_count",
    "reservation_unique_count",
    "reservation_check_count",
    "consumption_primary_key_count",
    "consumption_unique_count",
    "consumption_check_count",
    "consumption_foreign_key_count",
    "reservation_binding_exact",
    "consumption_foreign_key_binding_exact",
    "named_trigger_count",
    "enabled_trigger_count",
    "exact_trigger_mapping_count",
)

_EXPECTED_VERIFICATION = {
    "reservation_table_exists": True,
    "consumption_table_exists": True,
    "validation_function_exists": True,
    "append_only_function_exists": True,
    "reservation_row_count": 0,
    "consumption_row_count": 0,
    "named_relation_count": 2,
    "named_function_count": 2,
    "exact_function_count": 2,
    "reservation_column_count": 16,
    "reservation_not_null_column_count": 16,
    "consumption_column_count": 14,
    "consumption_not_null_column_count": 14,
    "reservation_primary_key_count": 1,
    "reservation_unique_count": 8,
    "reservation_check_count": 14,
    "consumption_primary_key_count": 1,
    "consumption_unique_count": 8,
    "consumption_check_count": 12,
    "consumption_foreign_key_count": 1,
    "reservation_binding_exact": True,
    "consumption_foreign_key_binding_exact": True,
    "named_trigger_count": 5,
    "enabled_trigger_count": 5,
    "exact_trigger_mapping_count": 5,
}

_NO_RUNTIME_EFFECT = {
    "candidate_service_connected": False,
    "runtime_database_registered": False,
    "handler_registered": False,
    "scheduler_registered": False,
    "worker_registered": False,
    "automatic_retry_allowed": False,
    "dispatch_allowed": False,
    "delivery_allowed": False,
    "telegram_api_calls": 0,
    "application_rows_written": 0,
    "stage6_activated": False,
    "research_evidence_effect": "NONE",
    "live_effect": "NONE",
}


class Migration019CommitOutcomeUncertain(RuntimeError):
    """The commit was attempted but its server-side outcome is unknown."""


def _is_enabled(environment: Mapping[str, Any], name: str) -> bool:
    return str(environment.get(name, "")).strip() == "1"


def resolve_configuration(environment: Mapping[str, Any]) -> Dict[str, Any]:
    """Validate the isolated exact target without exposing its URL."""

    if not isinstance(environment, Mapping):
        raise ValueError("migration environment must be a mapping")
    if not _is_enabled(environment, APPLY_ENABLED_ENV):
        raise RuntimeError(
            f"refusing migration 019: set {APPLY_ENABLED_ENV}=1 explicitly"
        )
    for flag in _FORBIDDEN_ENABLED_FLAGS:
        if _is_enabled(environment, flag):
            raise RuntimeError(f"refusing migration 019 while {flag}=1")
    for name in _FORBIDDEN_DATABASE_URLS:
        if str(environment.get(name, "")).strip():
            raise RuntimeError(
                f"refusing migration 019 while ambiguous {name} is configured"
            )

    database_url = str(environment.get(DATABASE_URL_ENV, "")).strip()
    if not database_url:
        raise RuntimeError(f"refusing migration 019: {DATABASE_URL_ENV} is required")
    parsed = urlparse(database_url)
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise ValueError("staging database URL must use the PostgreSQL scheme")
    if parsed.hostname != EXPECTED_INTERNAL_HOST:
        raise ValueError("staging database URL is not the expected internal host")
    if parsed.port not in (None, 5432):
        raise ValueError("staging database URL uses an unexpected port")
    if parsed.path != f"/{EXPECTED_DATABASE_NAME}":
        raise ValueError("staging database URL names an unexpected database")
    if parsed.username != EXPECTED_DATABASE_USER or not parsed.password:
        raise ValueError("staging database URL requires the exact dedicated credentials")
    if parsed.query or parsed.fragment:
        raise ValueError("staging database URL may not contain query or fragment data")

    return {
        "database_url": database_url,
        "database_url_source": DATABASE_URL_ENV,
        "render_postgres_id": EXPECTED_RENDER_POSTGRES_ID,
        "expected_database_name": EXPECTED_DATABASE_NAME,
        "expected_database_user": EXPECTED_DATABASE_USER,
        "internal_target_verified": True,
    }


def load_migration() -> str:
    """Read only the pinned migration file and verify its content identity."""

    expected_directory = (ROOT / "migrations").resolve(strict=True)
    resolved_path = MIGRATION_PATH.resolve(strict=True)
    if resolved_path.parent != expected_directory or resolved_path.name != MIGRATION_FILENAME:
        raise RuntimeError("migration 019 path is not the pinned repository path")
    content = resolved_path.read_bytes()
    actual_sha256 = hashlib.sha256(content).hexdigest()
    if actual_sha256 != MIGRATION_SHA256:
        raise RuntimeError("migration 019 checksum mismatch")
    return content.decode("utf-8")


def _row_mapping(row: Any, fields: Sequence[str], *, phase: str) -> Dict[str, Any]:
    if isinstance(row, Mapping):
        missing = [name for name in fields if name not in row]
        if missing:
            raise RuntimeError(f"migration 019 {phase} omitted required fields")
        return {name: row.get(name) for name in fields}
    if isinstance(row, Sequence) and not isinstance(row, (str, bytes, bytearray)):
        if len(row) != len(fields):
            raise RuntimeError(f"migration 019 {phase} returned an unexpected row shape")
        return dict(zip(fields, row))
    raise RuntimeError(f"migration 019 {phase} returned an invalid row")


def _evaluate_precondition(values: Mapping[str, Any]) -> Dict[str, Any]:
    if values.get("database_name") != EXPECTED_DATABASE_NAME:
        raise RuntimeError("connected database identity does not match staging")
    if values.get("database_user") != EXPECTED_DATABASE_USER:
        raise RuntimeError("connected database user does not match staging")
    version = values.get("postgres_version_num")
    if type(version) is not int or version // 10000 != EXPECTED_POSTGRES_MAJOR:
        raise RuntimeError("connected PostgreSQL major version does not match staging")
    if values.get("current_schema") != "public":
        raise RuntimeError("migration 019 search path did not select public")
    if values.get("transaction_read_only") != "off":
        raise RuntimeError("migration 019 transaction is not read-write")
    if values.get("public_schema_usage") is not True:
        raise RuntimeError("public schema usage privilege is absent")
    if values.get("public_schema_create") is not True:
        raise RuntimeError("public schema create privilege is absent")
    if values.get("plpgsql_available") is not True:
        raise RuntimeError("plpgsql is unavailable")

    object_counts = {}
    for name in (
        "migration_relation_count",
        "migration_function_count",
        "migration_trigger_count",
    ):
        value = values.get(name)
        if type(value) is not int or value < 0:
            raise RuntimeError("migration 019 precondition object count is invalid")
        object_counts[name] = value
    existing_object_count = sum(object_counts.values())
    if existing_object_count:
        raise RuntimeError("migration 019 precondition is not clean")

    return {
        "precondition_clean": True,
        "preexisting_object_count": 0,
        "postgres_version_num": version,
        **object_counts,
    }


def _evaluate_verification(values: Mapping[str, Any]) -> Dict[str, Any]:
    mismatches = [
        name
        for name, expected in _EXPECTED_VERIFICATION.items()
        if values.get(name) != expected or type(values.get(name)) is not type(expected)
    ]
    if mismatches:
        raise RuntimeError("migration 019 catalog verification mismatch")
    return {
        "catalog_verification_passed": True,
        "verified_table_count": 2,
        "verified_function_count": 2,
        "verified_trigger_count": 5,
        "verified_schema_object_count": 9,
        "verified_constraint_bindings": True,
    }


def run_installer(
    environment: Mapping[str, Any],
    *,
    connect: Callable[..., Any] | None = None,
) -> Dict[str, Any]:
    """Apply and verify the pinned migration once; never retry automatically."""

    configuration = resolve_configuration(environment)
    migration_sql = load_migration()
    connector = connect
    if connector is None:
        if psycopg is None:
            raise RuntimeError("psycopg is unavailable")
        connector = psycopg.connect

    connection = connector(
        configuration["database_url"],
        connect_timeout=5,
        autocommit=True,
        options="-c application_name=preview_staging_migration_019_once",
    )
    began = False
    commit_attempted = False
    try:
        connection.execute(BEGIN_SQL)
        began = True
        connection.execute(SET_LOCK_TIMEOUT_SQL)
        connection.execute(SET_STATEMENT_TIMEOUT_SQL)
        connection.execute(SET_SEARCH_PATH_SQL)
        connection.execute(LOCK_SQL, (SCHEMA_LOCK_ID,))

        precondition_row = connection.execute(PRECONDITION_SQL).fetchone()
        if precondition_row is None:
            raise RuntimeError("migration 019 precondition returned no row")
        precondition = _evaluate_precondition(
            _row_mapping(
                precondition_row,
                _PRECONDITION_FIELDS,
                phase="precondition",
            )
        )

        connection.execute(migration_sql)

        verification_row = connection.execute(VERIFICATION_SQL).fetchone()
        if verification_row is None:
            raise RuntimeError("migration 019 verification returned no row")
        verification = _evaluate_verification(
            _row_mapping(
                verification_row,
                _VERIFICATION_FIELDS,
                phase="verification",
            )
        )

        commit_attempted = True
        try:
            connection.execute(COMMIT_SQL)
        except Exception as exc:
            raise Migration019CommitOutcomeUncertain(
                "migration 019 commit outcome requires read-only reconciliation"
            ) from exc

        return {
            "installer_version": INSTALLER_VERSION,
            "mode": MODE,
            "status": "MIGRATION_019_APPLIED_AND_VERIFIED",
            "render_postgres_id": EXPECTED_RENDER_POSTGRES_ID,
            "database_name": EXPECTED_DATABASE_NAME,
            "database_user_verified": True,
            "internal_target_verified": True,
            "migration_filename": MIGRATION_FILENAME,
            "migration_sha256": MIGRATION_SHA256,
            **precondition,
            **verification,
            "database_connections": 1,
            "transactions_started": 1,
            "migration_files_executed": 1,
            "commit_attempts": 1,
            "transaction_committed": True,
            "schema_mutation_committed": True,
            "migration_019_applied": True,
            **_NO_RUNTIME_EFFECT,
        }
    except Exception:
        if began and not commit_attempted:
            try:
                connection.execute(ROLLBACK_SQL)
            except Exception:
                pass
        raise
    finally:
        try:
            connection.close()
        except Exception:
            # A close error must not replace a known commit result or the
            # original pre-commit/uncertain exception classification.
            pass


def _failed_closed(error: BaseException) -> Dict[str, Any]:
    uncertain = isinstance(error, Migration019CommitOutcomeUncertain)
    return {
        "installer_version": INSTALLER_VERSION,
        "mode": MODE,
        "status": (
            "MIGRATION_019_COMMIT_OUTCOME_UNCERTAIN"
            if uncertain
            else "MIGRATION_019_FAILED_CLOSED"
        ),
        "error_type": type(error).__name__,
        "render_postgres_id": EXPECTED_RENDER_POSTGRES_ID,
        "database_url_exposed": False,
        "migration_019_applied": None if uncertain else False,
        "manual_read_only_reconciliation_required": uncertain,
        "schema_mutation_outcome_known": not uncertain,
        **_NO_RUNTIME_EFFECT,
    }


def main() -> int:
    try:
        result = run_installer(os.environ)
    except Exception as exc:
        print(json.dumps(_failed_closed(exc), sort_keys=True), flush=True)
        return 3 if isinstance(exc, Migration019CommitOutcomeUncertain) else 1
    print(json.dumps(result, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
