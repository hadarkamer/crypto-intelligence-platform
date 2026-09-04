"""Conflict-checking persistence for silent Stage-4 signal snapshots.

This adapter reuses the existing ``research_events`` schema but deliberately
does not use the lossy asynchronous alert queue.  One batch is transactional;
an identity conflict is accepted only when every immutable event field matches.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import json
import math
import os
from typing import Any, Dict, Iterable, Mapping, Optional
from urllib.parse import parse_qsl, urlsplit

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - optional in pure/self-test environments
    psycopg = None
    dict_row = None

import research_event_capture
import research_event_store
import research_signal_snapshot


CONNECTION_OPTIONS = (
    "-c statement_timeout=10000 -c lock_timeout=2000 "
    "-c search_path=pg_catalog,public"
)
TRUSTED_WRITER_ROLE = "research_signal_snapshot_writer_v1"
DATABASE_URL_ENV = "RESEARCH_SIGNAL_SNAPSHOT_DATABASE_URL"

_INSERT_SQL = """
INSERT INTO public.research_events (
    schema_version, event_kind, event_type, alert_time_utc, symbol, direction,
    source_side, timeframe, score, current_price, target_price,
    initial_target_distance_pct, categories, setup_key, event_fingerprint,
    strategy_version, code_version, runtime_session_id, capture_stage,
    delivery_status, delivery_attempted_at_utc, delivered_at_utc, engine_snapshot
) VALUES (
    %(schema_version)s, %(event_kind)s, %(event_type)s, %(alert_time_utc)s,
    %(symbol)s, %(direction)s, %(source_side)s, %(timeframe)s, %(score)s,
    %(current_price)s, %(target_price)s, %(initial_target_distance_pct)s,
    %(categories)s::jsonb, %(setup_key)s, %(event_fingerprint)s,
    %(strategy_version)s, %(code_version)s, %(runtime_session_id)s,
    %(capture_stage)s, %(delivery_status)s, %(delivery_attempted_at_utc)s,
    %(delivered_at_utc)s, %(engine_snapshot)s::jsonb
)
ON CONFLICT (event_fingerprint) DO NOTHING
RETURNING event_id
"""

_LOAD_SQL = """
SELECT event_id, schema_version, event_kind, event_type, alert_time_utc, symbol, direction,
       source_side, timeframe, score, current_price, target_price,
       initial_target_distance_pct, categories, setup_key, event_fingerprint,
       strategy_version, code_version, runtime_session_id, capture_stage, delivery_status,
       delivery_attempted_at_utc, delivered_at_utc, engine_snapshot
 FROM public.research_events
 WHERE event_fingerprint=%(event_fingerprint)s
"""

_SCHEMA_READINESS_SQL = """
/* signal_snapshot:schema_readiness */
SELECT
    to_regclass('public.research_events') AS events,
    session_user::TEXT = 'research_signal_snapshot_writer_v1'
        AND current_user::TEXT = 'research_signal_snapshot_writer_v1'
        AND EXISTS (
            SELECT 1
            FROM pg_catalog.pg_roles role_row
            WHERE role_row.rolname = 'research_signal_snapshot_writer_v1'
              AND role_row.rolcanlogin
              AND NOT role_row.rolinherit
              AND NOT role_row.rolsuper
              AND NOT role_row.rolcreatedb
              AND NOT role_row.rolcreaterole
              AND NOT role_row.rolreplication
              AND NOT role_row.rolbypassrls
        )
        AND NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_auth_members membership
            JOIN pg_catalog.pg_roles role_row
              ON role_row.rolname = 'research_signal_snapshot_writer_v1'
            WHERE membership.member = role_row.oid
               OR membership.roleid = role_row.oid
        )
        AND NOT has_schema_privilege(
            'research_signal_snapshot_writer_v1', 'public', 'CREATE'
        )
        AND has_schema_privilege(
            'research_signal_snapshot_writer_v1', 'public', 'USAGE'
        )
        AND has_table_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_events',
            'SELECT'
        )
        AND has_table_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_events',
            'INSERT'
        )
        AND NOT has_table_privilege(
            'research_signal_snapshot_writer_v1', 'public.research_events', 'UPDATE'
        )
        AND NOT has_table_privilege(
            'research_signal_snapshot_writer_v1', 'public.research_events', 'DELETE'
        )
        AND NOT has_table_privilege(
            'research_signal_snapshot_writer_v1', 'public.research_events', 'TRUNCATE'
        )
        AND NOT has_table_privilege(
            'research_signal_snapshot_writer_v1', 'public.research_events', 'REFERENCES'
        )
        AND NOT has_table_privilege(
            'research_signal_snapshot_writer_v1', 'public.research_events', 'TRIGGER'
        )
        AND NOT has_any_column_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_events',
            'UPDATE, REFERENCES'
        )
        AND has_sequence_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_events_event_id_seq',
            'USAGE'
        )
        AND NOT has_sequence_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_events_event_id_seq',
            'SELECT'
        )
        AND NOT has_sequence_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_events_event_id_seq',
            'UPDATE'
        )
        AND has_table_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_sets',
            'SELECT'
        )
        AND has_table_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_symbols',
            'SELECT'
        )
        AND has_table_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_rows',
            'SELECT'
        )
        AND NOT has_table_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_sets', 'INSERT'
        )
        AND NOT has_table_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_sets', 'UPDATE'
        )
        AND NOT has_table_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_sets', 'DELETE'
        )
        AND NOT has_table_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_sets', 'TRUNCATE'
        )
        AND NOT has_table_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_sets', 'REFERENCES'
        )
        AND NOT has_table_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_sets', 'TRIGGER'
        )
        AND NOT has_table_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_symbols', 'INSERT'
        )
        AND NOT has_table_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_symbols', 'UPDATE'
        )
        AND NOT has_table_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_symbols', 'DELETE'
        )
        AND NOT has_table_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_symbols', 'TRUNCATE'
        )
        AND NOT has_table_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_symbols', 'REFERENCES'
        )
        AND NOT has_table_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_symbols', 'TRIGGER'
        )
        AND NOT has_table_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_rows', 'INSERT'
        )
        AND NOT has_table_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_rows', 'UPDATE'
        )
        AND NOT has_table_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_rows', 'DELETE'
        )
        AND NOT has_table_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_rows', 'TRUNCATE'
        )
        AND NOT has_table_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_rows', 'REFERENCES'
        )
        AND NOT has_table_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_rows', 'TRIGGER'
        )
        AND NOT has_any_column_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_sets',
            'INSERT, UPDATE, REFERENCES'
        )
        AND NOT has_any_column_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_symbols',
            'INSERT, UPDATE, REFERENCES'
        )
        AND NOT has_any_column_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_rows',
            'INSERT, UPDATE, REFERENCES'
        )
        AND NOT has_sequence_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_sets_snapshot_set_id_seq',
            'USAGE'
        )
        AND NOT has_sequence_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_sets_snapshot_set_id_seq',
            'SELECT'
        )
        AND NOT has_sequence_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_sets_snapshot_set_id_seq',
            'UPDATE'
        )
        AND NOT has_sequence_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_rows_snapshot_row_id_seq',
            'USAGE'
        )
        AND NOT has_sequence_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_rows_snapshot_row_id_seq',
            'SELECT'
        )
        AND NOT has_sequence_privilege(
            'research_signal_snapshot_writer_v1',
            'public.research_max_pain_snapshot_rows_snapshot_row_id_seq',
            'UPDATE'
        )
        AND NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_database database_row
            JOIN pg_catalog.pg_roles owner_row
              ON owner_row.oid = database_row.datdba
            WHERE database_row.datname = pg_catalog.current_database()
              AND owner_row.rolname = 'research_signal_snapshot_writer_v1'
        )
        AND NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_class relation_row
            JOIN pg_catalog.pg_roles owner_row
              ON owner_row.oid = relation_row.relowner
            WHERE relation_row.oid IN (
                'public.research_events'::regclass,
                'public.research_events_event_id_seq'::regclass,
                'public.research_max_pain_snapshot_sets'::regclass,
                'public.research_max_pain_snapshot_sets_snapshot_set_id_seq'::regclass,
                'public.research_max_pain_snapshot_symbols'::regclass,
                'public.research_max_pain_snapshot_rows'::regclass,
                'public.research_max_pain_snapshot_rows_snapshot_row_id_seq'::regclass
            )
              AND owner_row.rolname = 'research_signal_snapshot_writer_v1'
        )
        AND NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_class relation_row
            CROSS JOIN LATERAL pg_catalog.aclexplode(
                COALESCE(
                    relation_row.relacl,
                    pg_catalog.acldefault('r', relation_row.relowner)
                )
            ) acl
            WHERE relation_row.oid = 'public.research_events'::regclass
              AND acl.privilege_type = 'TRIGGER'
              AND acl.grantee <> relation_row.relowner
        )
        AND NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_attribute attribute
            CROSS JOIN LATERAL pg_catalog.aclexplode(attribute.attacl) acl
            CROSS JOIN pg_catalog.pg_roles writer_row
            WHERE attribute.attrelid IN (
                'public.research_events'::regclass,
                'public.research_max_pain_snapshot_sets'::regclass,
                'public.research_max_pain_snapshot_symbols'::regclass,
                'public.research_max_pain_snapshot_rows'::regclass
            )
              AND attribute.attnum > 0
              AND NOT attribute.attisdropped
              AND writer_row.rolname = 'research_signal_snapshot_writer_v1'
              AND acl.grantee IN (0, writer_row.oid)
        ) AS writer_ready,
    COALESCE((
        SELECT COUNT(*) = 5
           AND BOOL_AND(
                trigger_row.tgenabled = 'A'
                AND function_namespace.nspname = 'public'
                AND trigger_row.tgtype::INTEGER = CASE trigger_row.tgname
                    WHEN 'trg_research_signal_snapshot_v1_writer' THEN 23
                    WHEN 'trg_research_signal_snapshot_v1_envelope' THEN 23
                    WHEN 'trg_research_signal_snapshot_v1_set_complete' THEN 5
                    WHEN 'trg_research_signal_snapshot_v1_immutable' THEN 27
                    WHEN 'trg_research_signal_snapshot_v1_no_truncate' THEN 34
                END
                AND trigger_row.tgdeferrable = (
                    trigger_row.tgname =
                        'trg_research_signal_snapshot_v1_set_complete'
                )
                AND trigger_row.tginitdeferred = (
                    trigger_row.tgname =
                        'trg_research_signal_snapshot_v1_set_complete'
                )
                AND (trigger_row.tgconstraint <> 0) = (
                    trigger_row.tgname =
                        'trg_research_signal_snapshot_v1_set_complete'
                )
                AND trigger_row.tgqual IS NULL
                AND function_row.proname = CASE trigger_row.tgname
                    WHEN 'trg_research_signal_snapshot_v1_writer'
                        THEN 'assert_research_signal_snapshot_v1_writer'
                    WHEN 'trg_research_signal_snapshot_v1_envelope'
                        THEN 'validate_research_signal_snapshot_v1_envelope'
                    WHEN 'trg_research_signal_snapshot_v1_set_complete'
                        THEN 'validate_research_signal_snapshot_v1_set_complete'
                    WHEN 'trg_research_signal_snapshot_v1_immutable'
                        THEN 'prevent_research_signal_snapshot_v1_mutation'
                    WHEN 'trg_research_signal_snapshot_v1_no_truncate'
                        THEN 'prevent_research_signal_snapshot_v1_truncate'
                END
           )
          FROM pg_catalog.pg_trigger trigger_row
          JOIN pg_catalog.pg_proc function_row
            ON function_row.oid = trigger_row.tgfoid
          JOIN pg_catalog.pg_namespace function_namespace
            ON function_namespace.oid = function_row.pronamespace
         WHERE trigger_row.tgrelid = 'public.research_events'::regclass
           AND NOT trigger_row.tgisinternal
           AND trigger_row.tgname IN (
                'trg_research_signal_snapshot_v1_writer',
                'trg_research_signal_snapshot_v1_envelope',
                'trg_research_signal_snapshot_v1_set_complete',
                'trg_research_signal_snapshot_v1_immutable',
                'trg_research_signal_snapshot_v1_no_truncate'
           )
    ), FALSE)
    AND NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_trigger trigger_row
        WHERE trigger_row.tgrelid = 'public.research_events'::regclass
          AND NOT trigger_row.tgisinternal
          AND (trigger_row.tgtype::INTEGER & 4) = 4
          AND trigger_row.tgname NOT IN (
                'trg_research_signal_snapshot_v1_writer',
                'trg_research_signal_snapshot_v1_envelope',
                'trg_research_signal_snapshot_v1_set_complete'
          )
    ) AS triggers_ready,
    COALESCE((
        SELECT COUNT(*) = 2
           AND BOOL_AND(index_row.indisvalid AND index_row.indisready)
           AND BOOL_AND(index_row.indnatts = 1 AND index_row.indnkeyatts = 1)
           AND BOOL_AND(
                CASE class_row.relname
                    WHEN 'uq_research_signal_snapshot_projection_key_v1' THEN
                        index_row.indisunique
                        AND POSITION(
                            '{projection,snapshot_key}' IN
                            pg_catalog.pg_get_indexdef(index_row.indexrelid)
                        ) > 0
                        AND POSITION(
                            'SIGNAL_SNAPSHOT_PROJECTION' IN
                            pg_catalog.pg_get_indexdef(index_row.indexrelid)
                        ) > 0
                    WHEN 'idx_research_signal_snapshot_archive_key_v1' THEN
                        NOT index_row.indisunique
                        AND POSITION(
                            '{signal_snapshot,archive_reference,snapshot_key}' IN
                            pg_catalog.pg_get_indexdef(index_row.indexrelid)
                        ) > 0
                        AND POSITION(
                            'SILENT_COMBINED_CONFIRMATION_SNAPSHOT' IN
                            pg_catalog.pg_get_indexdef(index_row.indexrelid)
                        ) > 0
                END
           )
          FROM pg_catalog.pg_index index_row
          JOIN pg_catalog.pg_class class_row
            ON class_row.oid = index_row.indexrelid
         WHERE index_row.indrelid = 'public.research_events'::regclass
           AND class_row.relnamespace = 'public'::regnamespace
           AND class_row.relname IN (
                'uq_research_signal_snapshot_projection_key_v1',
                'idx_research_signal_snapshot_archive_key_v1'
           )
    ), FALSE) AS indexes_ready,
    COALESCE((
        SELECT COUNT(*) = 21
           AND BOOL_AND(
                function_row.proowner = relation_row.relowner
                AND NOT function_row.prosecdef
                AND function_row.proconfig IS NOT DISTINCT FROM ARRAY[
                    'search_path=pg_catalog, public'
                ]::TEXT[]
           )
        FROM pg_catalog.pg_proc function_row
        JOIN pg_catalog.pg_namespace function_namespace
          ON function_namespace.oid = function_row.pronamespace
        CROSS JOIN pg_catalog.pg_class relation_row
        WHERE function_namespace.nspname = 'public'
          AND relation_row.oid = 'public.research_events'::regclass
          AND function_row.proname = ANY (ARRAY[
              'research_signal_snapshot_v1_reserved_type',
              'assert_research_signal_snapshot_v1_writer',
              'research_signal_snapshot_v1_sha256',
              'research_signal_snapshot_v1_text_sha256',
              'research_signal_snapshot_v1_commitment_canonical',
              'research_signal_snapshot_v1_event_commitment_payload',
              'research_signal_snapshot_v1_event_payload_sha256',
              'research_signal_snapshot_v1_identity_canonical',
              'research_signal_snapshot_v1_magnet_members',
              'research_signal_snapshot_v1_expected_setup_key',
              'research_signal_snapshot_v1_expected_fingerprint',
              'research_signal_snapshot_v1_nonnegative_integer',
              'research_signal_snapshot_v1_positive_bigint',
              'research_signal_snapshot_v1_finite_number',
              'research_signal_snapshot_v1_key_count',
              'assert_research_signal_snapshot_v1_envelope',
              'validate_research_signal_snapshot_v1_envelope',
              'assert_research_signal_snapshot_v1_set_complete',
              'validate_research_signal_snapshot_v1_set_complete',
              'prevent_research_signal_snapshot_v1_mutation',
              'prevent_research_signal_snapshot_v1_truncate'
          ]::NAME[])
    ), FALSE) AS functions_ready,
    NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class relation_row
        WHERE relation_row.oid IN (
            'public.research_events'::regclass,
            'public.research_max_pain_snapshot_sets'::regclass,
            'public.research_max_pain_snapshot_symbols'::regclass,
            'public.research_max_pain_snapshot_rows'::regclass
        )
          AND (relation_row.relrowsecurity OR relation_row.relforcerowsecurity)
    ) AND NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_policy policy_row
        WHERE policy_row.polrelid IN (
            'public.research_events'::regclass,
            'public.research_max_pain_snapshot_sets'::regclass,
            'public.research_max_pain_snapshot_symbols'::regclass,
            'public.research_max_pain_snapshot_rows'::regclass
        )
    ) AND NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_rewrite rule_row
        WHERE rule_row.ev_class IN (
            'public.research_events'::regclass,
            'public.research_max_pain_snapshot_sets'::regclass,
            'public.research_max_pain_snapshot_symbols'::regclass,
            'public.research_max_pain_snapshot_rows'::regclass
        )
          AND rule_row.rulename <> '_RETURN'
    ) AS visibility_ready,
    to_regprocedure(
        'public.assert_research_signal_snapshot_v1_envelope(public.research_events)'
    ) IS NOT NULL AS envelope_function_ready,
    to_regprocedure(
        'public.assert_research_signal_snapshot_v1_set_complete(text)'
    ) IS NOT NULL AS completeness_function_ready,
    to_regprocedure(
        'public.research_signal_snapshot_v1_event_payload_sha256(public.research_events)'
    ) IS NOT NULL AS commitment_function_ready,
    to_regprocedure(
        'public.research_signal_snapshot_v1_expected_fingerprint(public.research_events)'
    ) IS NOT NULL AS identity_function_ready
"""


class SignalSnapshotConflictError(RuntimeError):
    pass


def status() -> Dict[str, Any]:
    configured_url = os.getenv(DATABASE_URL_ENV, "").strip()
    archive_url = os.getenv("DATABASE_URL", "").strip()
    persistence = research_event_store.persistence_status()
    configured_target = _database_target(configured_url)
    archive_target = _database_target(archive_url)
    return {
        "configured": bool(configured_url),
        "persistence_enabled": persistence.get("enabled") is True,
        "database_source": DATABASE_URL_ENV if configured_url else None,
        "archive_database_aligned": bool(
            configured_url
            and archive_url
            and all(configured_target)
            and all(archive_target)
            and configured_target == archive_target
        ),
        "driver_available": psycopg is not None,
        "trusted_writer_required": True,
        "trusted_writer_role": TRUSTED_WRITER_ROLE,
        "schema_enforcement": "VERIFIED_ON_EVERY_CONNECTION",
        "schema_auto_create": False,
        "capture_stage": research_signal_snapshot.CAPTURE_STAGE,
        "transactional_batch": True,
        "conflict_policy": "LOCK_AND_VERIFY_FULL_IMMUTABLE_EVENT",
        "async_queue_used": False,
    }


def _database_target(url: str) -> tuple[str, str, int, str]:
    try:
        parsed = urlsplit(str(url or "").strip())
        hostname = parsed.hostname
        port = int(parsed.port or 5432)
        target_overrides = {
            key.lower()
            for key, _value in parse_qsl(parsed.query, keep_blank_values=True)
            if key.lower() in {
                "host",
                "hostaddr",
                "port",
                "dbname",
                "service",
                "servicefile",
            }
        }
    except (TypeError, ValueError):
        return ("", "", 0, "")
    database_name = parsed.path.lstrip("/").split("/", 1)[0]
    if (
        parsed.scheme not in {"postgres", "postgresql"}
        or not hostname
        or not database_name
        or "," in hostname
        or target_overrides
    ):
        return ("", "", 0, "")
    return (
        parsed.scheme.replace("postgresql", "postgres"),
        hostname.lower(),
        port,
        database_name,
    )


def _database_url(
    database_url: Optional[str], *, require_archive_alignment: bool = True
) -> str:
    url = str(
        database_url or os.getenv(DATABASE_URL_ENV, "")
    ).strip()
    if not url:
        raise RuntimeError("signal snapshot research database is not configured")
    if not all(_database_target(url)):
        raise RuntimeError("signal snapshot research database target is invalid")
    if research_event_store.persistence_status().get("enabled") is not True:
        raise RuntimeError("research persistence is not enabled")
    if require_archive_alignment:
        archive_url = os.getenv("DATABASE_URL", "").strip()
        if (
            not archive_url
            or not all(_database_target(archive_url))
            or _database_target(url) != _database_target(archive_url)
        ):
            raise RuntimeError(
                "signal snapshot storage must use the same DATABASE_URL as "
                "the persisted Max-Pain archive"
            )
    if psycopg is None:
        raise RuntimeError("psycopg is unavailable")
    return url


def _json(value: Any) -> Any:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            pass
    return json.loads(
        json.dumps(value, default=str, ensure_ascii=False, sort_keys=True, allow_nan=False)
    )


def _time(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _number(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        raise SignalSnapshotConflictError("boolean stored in numeric event field")
    if isinstance(value, Decimal):
        value = float(value)
    number = float(value)
    if not math.isfinite(number):
        raise SignalSnapshotConflictError("non-finite stored in numeric event field")
    return number


def _same_number(left: Any, right: Any) -> bool:
    a = _number(left)
    b = _number(right)
    if a is None or b is None:
        return a is b
    return a == b


def _assert_equal(label: str, existing: Any, expected: Any) -> None:
    if existing != expected:
        raise SignalSnapshotConflictError(
            f"signal snapshot conflict at {label}: "
            f"existing={existing!r} expected={expected!r}"
        )


def _verify_existing(existing: Mapping[str, Any], expected: Mapping[str, Any]) -> None:
    for key in (
        "schema_version",
        "event_kind",
        "event_type",
        "symbol",
        "direction",
        "source_side",
        "timeframe",
        "setup_key",
        "event_fingerprint",
        "strategy_version",
        "code_version",
        "capture_stage",
        "delivery_status",
    ):
        existing_value = existing.get(key)
        expected_value = expected.get(key)
        if key in {"setup_key", "event_fingerprint"}:
            existing_value = str(existing_value or "").rstrip()
            expected_value = str(expected_value or "").rstrip()
        _assert_equal(key, existing_value, expected_value)
    for key in (
        "alert_time_utc",
        "delivery_attempted_at_utc",
        "delivered_at_utc",
    ):
        _assert_equal(key, _time(existing.get(key)), _time(expected.get(key)))
    for key in (
        "score",
        "current_price",
        "target_price",
        "initial_target_distance_pct",
    ):
        if not _same_number(existing.get(key), expected.get(key)):
            raise SignalSnapshotConflictError(f"signal snapshot conflict at {key}")
    for key in ("categories", "engine_snapshot"):
        _assert_equal(key, _json(existing.get(key)), _json(expected.get(key)))


def _serialize(
    event: research_event_capture.ResearchEvent,
) -> Dict[str, Any]:
    if event.event_kind != "DECISION_SAMPLE":
        raise ValueError("signal snapshot store accepts DECISION_SAMPLE only")
    if event.event_type not in {
        research_signal_snapshot.MAX_PAIN_EVENT_TYPE,
        research_signal_snapshot.MAGNET_EVENT_TYPE,
        research_signal_snapshot.COMBINED_EVENT_TYPE,
        research_signal_snapshot.PROJECTION_EVENT_TYPE,
    }:
        raise ValueError("unsupported signal snapshot event type")
    return research_event_store.serialize_event(
        event,
        capture_stage=research_signal_snapshot.CAPTURE_STAGE,
        delivery_status="NOT_APPLICABLE",
    )


def _ensure_schema(conn: Any) -> None:
    readiness = conn.execute(_SCHEMA_READINESS_SQL).fetchone()
    required = (
        "events",
        "writer_ready",
        "triggers_ready",
        "indexes_ready",
        "functions_ready",
        "visibility_ready",
        "envelope_function_ready",
        "completeness_function_ready",
        "commitment_function_ready",
        "identity_function_ready",
    )
    if not readiness or any(not readiness.get(key) for key in required):
        raise RuntimeError(
            "migration 023 signal snapshot enforcement is not fully installed"
        )


def _projection_result(
    row: Optional[Mapping[str, Any]], *, snapshot_key: str
) -> Dict[str, Any]:
    normalized_key = str(snapshot_key).strip()
    if not row:
        return {"terminal": False, "snapshot_key": normalized_key}
    expected_fingerprint = research_signal_snapshot.projection_event_fingerprint(
        normalized_key
    )
    if str(row.get("event_fingerprint") or "").rstrip() != expected_fingerprint:
        raise SignalSnapshotConflictError(
            "projection receipt has invalid event fingerprint"
        )
    if str(row.get("setup_key") or "").rstrip() != (
        research_signal_snapshot.projection_setup_key()
    ):
        raise SignalSnapshotConflictError("projection receipt has invalid setup key")
    if row.get("schema_version") != research_event_capture.SCHEMA_VERSION:
        raise SignalSnapshotConflictError("projection receipt has invalid schema version")
    if row.get("event_kind") != "DECISION_SAMPLE":
        raise SignalSnapshotConflictError("projection fingerprint has invalid event kind")
    if row.get("event_type") != research_signal_snapshot.PROJECTION_EVENT_TYPE:
        raise SignalSnapshotConflictError("projection fingerprint has invalid event type")
    if row.get("capture_stage") != research_signal_snapshot.CAPTURE_STAGE:
        raise SignalSnapshotConflictError("projection fingerprint has invalid capture stage")
    if row.get("delivery_status") != "NOT_APPLICABLE":
        raise SignalSnapshotConflictError("projection fingerprint has invalid delivery status")
    if row.get("symbol") != research_signal_snapshot.PROJECTION_SYMBOL:
        raise SignalSnapshotConflictError("projection receipt has invalid symbol")
    if row.get("direction") != "NEUTRAL":
        raise SignalSnapshotConflictError("projection receipt has invalid direction")
    if row.get("source_side") is not None or row.get("timeframe") is not None:
        raise SignalSnapshotConflictError(
            "projection receipt has invalid directional locator"
        )
    if any(
        row.get(key) is not None
        for key in (
            "score",
            "current_price",
            "target_price",
            "initial_target_distance_pct",
            "delivery_attempted_at_utc",
            "delivered_at_utc",
        )
    ):
        raise SignalSnapshotConflictError(
            "projection receipt has invalid numeric or delivery state"
        )
    categories = _json(row.get("categories"))
    if not isinstance(categories, list) or not {
        "DECISION_SAMPLE",
        "SILENT",
    }.issubset(set(categories)):
        raise SignalSnapshotConflictError("projection receipt has invalid categories")
    engine = _json(row.get("engine_snapshot"))
    signal = dict(engine.get("signal_snapshot") or {})
    projection = dict(engine.get("projection") or {})
    if signal.get("contract_version") != research_signal_snapshot.CONTRACT_VERSION:
        raise SignalSnapshotConflictError("projection receipt has invalid contract version")
    if signal.get("signal_family") != "PROJECTION":
        raise SignalSnapshotConflictError("projection receipt has invalid signal family")
    for flag in (
        "formula_authorized",
        "outcome_authorized",
        "telegram_delivery_allowed",
        "trade_execution_allowed",
    ):
        if signal.get(flag) is not False:
            raise SignalSnapshotConflictError(
                f"projection receipt has invalid authority flag: {flag}"
            )
    if projection.get("snapshot_key") != normalized_key:
        raise SignalSnapshotConflictError("projection receipt has invalid snapshot key")
    terminal_status = str(projection.get("status") or "").upper()
    if terminal_status not in {"COMPLETED", "MISSED_CAUSAL_WINDOW"}:
        raise SignalSnapshotConflictError("projection receipt has invalid status")
    if str(signal.get("tier") or "").upper() != terminal_status:
        raise SignalSnapshotConflictError("projection receipt tier/status mismatch")
    eligible_symbols = projection.get("eligible_symbols") or []
    symbol_evaluations = projection.get("symbol_evaluations") or []
    if (
        not isinstance(eligible_symbols, list)
        or not isinstance(symbol_evaluations, list)
        or eligible_symbols != sorted(set(eligible_symbols))
    ):
        raise SignalSnapshotConflictError(
            "projection receipt has invalid eligible-symbol set"
        )
    normalized_evaluations: list[Dict[str, Any]] = []
    for item in symbol_evaluations:
        if not isinstance(item, Mapping) or set(item) != {
            "symbol",
            "status",
            "reason",
        }:
            raise SignalSnapshotConflictError(
                "projection receipt has invalid symbol evaluation"
            )
        normalized = {
            "symbol": str(item.get("symbol") or ""),
            "status": str(item.get("status") or "").upper(),
            "reason": item.get("reason"),
        }
        if normalized["status"] == "EVALUABLE":
            if normalized["reason"] is not None:
                raise SignalSnapshotConflictError(
                    "evaluable projection symbol carries a rejection reason"
                )
        elif normalized["status"] == "UNEVALUABLE":
            if normalized["reason"] not in {
                "DERIVATIVES_SNAPSHOT_MISSING",
                "DERIVATIVES_SNAPSHOT_INVALID",
                "PRICE_OI_UNAVAILABLE",
                "PRICE_OI_STALE",
                "FUTURES_CVD_UNAVAILABLE",
                "MISSED_CAUSAL_WINDOW",
            }:
                raise SignalSnapshotConflictError(
                    "unevaluable projection symbol has an invalid reason"
                )
        else:
            raise SignalSnapshotConflictError(
                "projection receipt has invalid symbol evaluation status"
            )
        normalized_evaluations.append(normalized)
    if [item["symbol"] for item in normalized_evaluations] != eligible_symbols:
        raise SignalSnapshotConflictError(
            "projection symbol evaluations do not partition eligible symbols"
        )
    evaluated_count = sum(
        int(item["status"] == "EVALUABLE") for item in normalized_evaluations
    )
    if terminal_status == "MISSED_CAUSAL_WINDOW":
        expected_evaluation = "UNEVALUABLE"
        if evaluated_count or any(
            item["reason"] != "MISSED_CAUSAL_WINDOW"
            for item in normalized_evaluations
        ):
            raise SignalSnapshotConflictError(
                "missed projection has an invalid symbol partition"
            )
    elif evaluated_count == len(normalized_evaluations):
        expected_evaluation = "EVALUABLE"
    elif evaluated_count:
        expected_evaluation = "PARTIAL"
    else:
        expected_evaluation = "UNEVALUABLE"
    evaluation_status = str(projection.get("evaluation_status") or "").upper()
    if evaluation_status != expected_evaluation:
        raise SignalSnapshotConflictError(
            "projection receipt has invalid evaluation status"
        )
    set_hash = str(projection.get("set_payload_sha256") or "")
    if len(set_hash) != 64 or any(
        character not in "0123456789abcdef" for character in set_hash
    ):
        raise SignalSnapshotConflictError("projection receipt has invalid set hash")
    set_id = projection.get("snapshot_set_id")
    if type(set_id) is not int or set_id <= 0:
        raise SignalSnapshotConflictError("projection receipt has invalid snapshot set id")
    counts = projection.get("counts") or {}
    if not isinstance(counts, Mapping) or set(counts) != {
        "max_pain",
        "magnet",
        "combined",
    }:
        raise SignalSnapshotConflictError("projection receipt has invalid counts")
    if any(type(value) is not int or value < 0 for value in counts.values()):
        raise SignalSnapshotConflictError("projection receipt has invalid count value")
    signal_event_count = projection.get("signal_event_count")
    if type(signal_event_count) is not int or signal_event_count != sum(
        counts.values()
    ):
        raise SignalSnapshotConflictError(
            "projection receipt signal count does not match family counts"
        )
    decision_time = _time(projection.get("decision_time_utc"))
    if decision_time is None or decision_time != _time(row.get("alert_time_utc")):
        raise SignalSnapshotConflictError(
            "projection receipt decision time does not match event time"
        )
    return {
        "terminal": True,
        "event_id": int(row["event_id"]),
        "snapshot_key": normalized_key,
        "status": terminal_status,
        "decision_time_utc": projection.get("decision_time_utc"),
        "counts": dict(counts),
        "signal_event_count": signal_event_count,
        "evaluation_status": evaluation_status,
        "evaluated_symbols": [
            item["symbol"]
            for item in normalized_evaluations
            if item["status"] == "EVALUABLE"
        ],
        "unevaluable_symbols": [
            item["symbol"]
            for item in normalized_evaluations
            if item["status"] == "UNEVALUABLE"
        ],
    }


def _load_projection_on_connection(conn: Any, snapshot_key: str) -> Dict[str, Any]:
    fingerprint = research_signal_snapshot.projection_event_fingerprint(snapshot_key)
    row = conn.execute(
        _LOAD_SQL, {"event_fingerprint": fingerprint}
    ).fetchone()
    return _projection_result(row, snapshot_key=snapshot_key)


def _serialized_rows(
    events: Iterable[research_event_capture.ResearchEvent],
) -> list[Dict[str, Any]]:
    rows = [_serialize(event) for event in events]
    fingerprints = [str(row["event_fingerprint"]).strip() for row in rows]
    if len(fingerprints) != len(set(fingerprints)):
        raise ValueError("signal snapshot persistence batch has duplicate fingerprints")
    return rows


def _persist_rows(conn: Any, rows: list[Dict[str, Any]]) -> Dict[str, Any]:
    inserted = 0
    existing_count = 0
    event_ids: list[int] = []
    try:
        # Acquire a DDL-conflicting table lock before the catalog attestation,
        # then retain it through commit.  This closes the gap in which a
        # non-owner trigger could otherwise appear between readiness and INSERT.
        conn.execute("LOCK TABLE public.research_events IN ROW EXCLUSIVE MODE")
        _ensure_schema(conn)
        for row in rows:
            result = conn.execute(_INSERT_SQL, row).fetchone()
            if result:
                inserted_row = conn.execute(
                    _LOAD_SQL,
                    {"event_fingerprint": row["event_fingerprint"]},
                ).fetchone()
                if not inserted_row:
                    raise SignalSnapshotConflictError(
                        "inserted event could not be reloaded for verification"
                    )
                _verify_existing(inserted_row, row)
                _assert_equal(
                    "runtime_session_id",
                    inserted_row.get("runtime_session_id"),
                    row.get("runtime_session_id"),
                )
                _assert_equal(
                    "event_id",
                    int(inserted_row["event_id"]),
                    int(result["event_id"]),
                )
                event_ids.append(int(inserted_row["event_id"]))
                inserted += 1
                continue
            existing = conn.execute(
                _LOAD_SQL, {"event_fingerprint": row["event_fingerprint"]}
            ).fetchone()
            if not existing:
                raise SignalSnapshotConflictError(
                    "event fingerprint conflict returned no stored row"
                )
            _verify_existing(existing, row)
            event_ids.append(int(existing["event_id"]))
            existing_count += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {
        "persisted": True,
        "inserted": inserted,
        "idempotent_existing": existing_count,
        "event_ids": event_ids,
    }


def _advisory_key(snapshot_key: str) -> int:
    digest = bytes.fromhex(
        research_signal_snapshot.projection_event_fingerprint(snapshot_key)[:16]
    )
    unsigned = int.from_bytes(digest, byteorder="big", signed=False)
    return unsigned - (1 << 64) if unsigned >= (1 << 63) else unsigned


class ProjectionLease:
    """Session advisory lock spanning lookup, calculation and atomic commit."""

    def __init__(self, conn: Any, snapshot_key: str, lock_key: int) -> None:
        self._conn = conn
        self.snapshot_key = str(snapshot_key).strip()
        self._lock_key = lock_key
        self._closed = False

    def load(self) -> Dict[str, Any]:
        result = _load_projection_on_connection(self._conn, self.snapshot_key)
        self._conn.commit()
        return result

    def persist(
        self, events: Iterable[research_event_capture.ResearchEvent]
    ) -> Dict[str, Any]:
        return _persist_rows(self._conn, _serialized_rows(events))

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._conn.rollback()
            self._conn.execute(
                "SELECT pg_advisory_unlock(%s) AS unlocked", (self._lock_key,)
            )
            self._conn.commit()
        finally:
            self._conn.close()


def acquire_projection_lease(
    snapshot_key: str,
    *,
    database_url: Optional[str] = None,
) -> Optional[ProjectionLease]:
    """Try to claim one snapshot across replicas without blocking the scheduler."""

    normalized_key = str(snapshot_key or "").strip()
    research_signal_snapshot.projection_event_fingerprint(normalized_key)
    url = _database_url(database_url)
    conn = psycopg.connect(
        url,
        row_factory=dict_row,
        connect_timeout=5,
        options=CONNECTION_OPTIONS,
    )
    try:
        _ensure_schema(conn)
        lock_key = _advisory_key(normalized_key)
        row = conn.execute(
            "SELECT pg_try_advisory_lock(%s) AS acquired", (lock_key,)
        ).fetchone()
        conn.commit()
        if not row or row.get("acquired") is not True:
            conn.close()
            return None
        return ProjectionLease(conn, normalized_key, lock_key)
    except Exception:
        conn.close()
        raise


def load_projection(
    snapshot_key: str,
    *,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    """Return one validated terminal receipt without invoking data providers."""

    url = _database_url(database_url)
    with psycopg.connect(
        url,
        row_factory=dict_row,
        connect_timeout=5,
        options=CONNECTION_OPTIONS,
    ) as conn:
        _ensure_schema(conn)
        result = _load_projection_on_connection(conn, snapshot_key)
        conn.commit()
    return result


def persist_events(
    events: Iterable[research_event_capture.ResearchEvent],
    *,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    """Persist one batch atomically and verify every idempotent conflict."""

    rows = _serialized_rows(events)
    if not rows:
        return {
            "persisted": True,
            "inserted": 0,
            "idempotent_existing": 0,
            "event_ids": [],
        }
    url = _database_url(database_url)
    with psycopg.connect(
        url,
        row_factory=dict_row,
        connect_timeout=5,
        options=CONNECTION_OPTIONS,
    ) as conn:
        return _persist_rows(conn, rows)
