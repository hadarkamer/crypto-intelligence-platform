"""Isolated PostgreSQL store and outbox for Stage-4 experimental alerts.

This module is intentionally separate from the Formula registry, Shadow, and
LIVE delivery stores.  It accepts only content-addressed Stage-4 candidate
searches and current-match alerts, fans new occurrences out only to chats that
explicitly opted in to the experimental channel, and claims delivery work with
a lease.  A lease that expires while a send may have happened is terminally
``AMBIGUOUS`` and is never retried automatically.

Nothing here grants LIVE or trading authority.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import secrets
from typing import Any, Dict, Mapping, Optional, Sequence
from urllib.parse import parse_qsl, unquote, urlsplit

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - exercised by configuration status
    psycopg = None
    dict_row = None

import research_experimental_formula_alert as experimental_alert
import research_stage4_candidate_search as candidate_search


DATABASE_URL_ENV = "RESEARCH_FORMULA_EXPERIMENTAL_DATABASE_URL"
READER_DATABASE_URL_ENV = "RESEARCH_FORMULA_EXPLORATION_DATABASE_URL"
DISPATCHER_ROLE = "research_formula_experimental_dispatcher_v1"
SUBSCRIPTION_POLICY_VERSION = "stage4-experimental-telegram-subscription-v1"
CONSENT_SOURCE = "EXPLICIT_TELEGRAM_COMMAND"
DELIVERY_SCOPE = "TELEGRAM_EXPERIMENTAL_ONLY"
EXPERIMENTAL_LABEL = experimental_alert.EXPERIMENTAL_LABEL
SEARCH_RUN_ID_VERSION = "stage4-experimental-search-run-id-v1"
SEARCH_PAYLOAD_HASH_VERSION = "stage4-experimental-search-payload-v1"
DELIVERY_KEY_VERSION = "stage4-experimental-delivery-key-v1"
ATTEMPT_EVENT_KEY_VERSION = "stage4-experimental-attempt-event-key-v1"

_OPERATIONAL_STATEMENT_TIMEOUT_MS = 20_000
_LOCK_TIMEOUT_MS = 3_000
_CLAIM_LEASE_SECONDS = max(
    30,
    min(
        600,
        int(os.getenv("FORMULA_EXPERIMENTAL_CLAIM_LEASE_SECONDS", "120")),
    ),
)
_DELIVERY_MAX_ATTEMPTS = max(
    1,
    min(10, int(os.getenv("FORMULA_EXPERIMENTAL_MAX_ATTEMPTS", "3"))),
)
_RETRY_BASE_SECONDS = max(
    5,
    min(600, int(os.getenv("FORMULA_EXPERIMENTAL_RETRY_BASE_SECONDS", "30"))),
)
_MAX_CLAIM_BATCH = 50
_HEX = frozenset("0123456789abcdef")
_UTC = timezone.utc

_TABLES: Dict[str, tuple[str, ...]] = {
    "research_formula_experimental_search_runs_v1": (
        "search_run_id",
        "search_receipt_sha256",
        "source_corpus_receipt_sha256",
        "input_observation_chain_sha256",
        "engine_version",
        "candidate_schema_version",
        "feature_schema_version",
        "label_policy_version",
        "independence_policy_version",
        "multiple_testing_policy_version",
        "schedule_slot_utc",
        "analysis_as_of_utc",
        "horizon_minutes",
        "input_observation_count",
        "eligible_candidate_count",
        "search_status",
        "search_payload",
        "search_payload_sha256",
        "formula_registry_effect",
        "delivery_channel",
        "live_eligible",
        "telegram_delivery_allowed",
        "trade_execution_allowed",
        "created_at_utc",
    ),
    "research_formula_experimental_alerts_v1": (
        "alert_occurrence_id",
        "search_run_id",
        "candidate_key",
        "search_receipt_sha256",
        "candidate_snapshot",
        "trigger_key",
        "trigger_observation_id",
        "projection_event_id",
        "projection_event_fingerprint",
        "btc_parent_movement_id",
        "symbol",
        "direction",
        "horizon_minutes",
        "decision_time_utc",
        "expires_at_utc",
        "trigger_snapshot",
        "trigger_snapshot_sha256",
        "current_trigger_receipt_sha256",
        "current_trigger_policy_version",
        "formula_text",
        "conditions",
        "independent_movement_count",
        "accepted_paths",
        "metrics",
        "experimental_reasons",
        "renderer_version",
        "rendered_message",
        "rendered_message_sha256",
        "disclaimer",
        "delivery_channel",
        "formula_registry_effect",
        "human_formula_approval_required",
        "live_eligible",
        "trade_execution_allowed",
        "telegram_delivery_allowed",
        "created_at_utc",
    ),
    "research_formula_experimental_subscriptions_v1": (
        "chat_id",
        "active",
        "requested_by_user_id",
        "subscription_policy_version",
        "consent_source",
        "delivery_scope",
        "disclaimer_acknowledged",
        "disclaimer_acknowledged_at_utc",
        "subscribed_at_utc",
        "updated_at_utc",
    ),
    "research_formula_experimental_deliveries_v1": (
        "delivery_key",
        "alert_occurrence_id",
        "chat_id",
        "status",
        "attempt_count",
        "available_at_utc",
        "claim_token",
        "claimed_at_utc",
        "claim_expires_at_utc",
        "sent_at_utc",
        "telegram_message_id",
        "last_failure_kind",
        "last_error",
        "created_at_utc",
        "updated_at_utc",
    ),
    "research_formula_experimental_delivery_attempt_events_v1": (
        "attempt_event_key",
        "delivery_key",
        "attempt_number",
        "event_phase",
        "terminal_result",
        "claim_token",
        "event_time_utc",
        "telegram_message_id",
        "error_text",
        "event_payload",
        "created_at_utc",
    ),
}

_TABLE_COMMENTS = {
    "research_formula_experimental_search_runs_v1": (
        "contract=stage4-experimental-search-run-v1;immutable=true;"
        "registry=none;delivery=none;live=false;trading=false"
    ),
    "research_formula_experimental_alerts_v1": (
        "contract=stage4-experimental-alert-occurrence-v1;immutable=true;"
        "channel=telegram-experimental-only;live=false;trading=false"
    ),
    "research_formula_experimental_subscriptions_v1": (
        "contract=stage4-experimental-subscription-v1;explicit-opt-in=true;"
        "live-subscription-backfill=false"
    ),
    "research_formula_experimental_deliveries_v1": (
        "contract=stage4-experimental-delivery-outbox-v1;"
        "stale-in-flight=ambiguous;automatic-live=false;trading=false"
    ),
    "research_formula_experimental_delivery_attempt_events_v1": (
        "contract=stage4-experimental-delivery-attempt-event-v1;immutable=true;"
        "two-phase=claimed-terminal"
    ),
}

_CONSTRAINTS = {
    "research_formula_experimental_search_runs_v1": (
        "research_stage4_experimental_search_authority_ck",
        "research_stage4_experimental_search_counts_ck",
        "research_stage4_experimental_search_horizon_ck",
        "research_stage4_experimental_search_identity_uk",
        "research_stage4_experimental_search_payload_ck",
        "research_stage4_experimental_search_receipt_ck",
        "research_stage4_experimental_search_receipt_uk",
        "research_stage4_experimental_search_run_id_ck",
        "research_stage4_experimental_search_run_pk",
        "research_stage4_experimental_search_schedule_slot_uk",
        "research_stage4_experimental_search_status_ck",
        "research_stage4_experimental_search_time_ck",
        "research_stage4_experimental_search_versions_ck",
    ),
    "research_formula_experimental_alerts_v1": (
        "research_stage4_experimental_alert_authority_ck",
        "research_stage4_experimental_alert_candidate_cell_uk",
        "research_stage4_experimental_alert_candidate_ck",
        "research_stage4_experimental_alert_direction_ck",
        "research_stage4_experimental_alert_formula_ck",
        "research_stage4_experimental_alert_freshness_ck",
        "research_stage4_experimental_alert_horizon_ck",
        "research_stage4_experimental_alert_id_ck",
        "research_stage4_experimental_alert_message_ck",
        "research_stage4_experimental_alert_pk",
        "research_stage4_experimental_alert_projection_fk",
        "research_stage4_experimental_alert_search_fk",
        "research_stage4_experimental_alert_snapshot_ck",
        "research_stage4_experimental_alert_symbol_ck",
    ),
    "research_formula_experimental_subscriptions_v1": (
        "research_stage4_experimental_subscription_pk",
        "research_stage4_experimental_subscription_policy_ck",
        "research_stage4_experimental_subscription_time_ck",
        "research_stage4_experimental_subscription_user_ck",
    ),
    "research_formula_experimental_deliveries_v1": (
        "research_stage4_experimental_delivery_alert_fk",
        "research_stage4_experimental_delivery_attempt_ck",
        "research_stage4_experimental_delivery_key_ck",
        "research_stage4_experimental_delivery_occurrence_chat_uk",
        "research_stage4_experimental_delivery_pk",
        "research_stage4_experimental_delivery_state_ck",
        "research_stage4_experimental_delivery_status_ck",
        "research_stage4_experimental_delivery_subscription_fk",
        "research_stage4_experimental_delivery_time_ck",
    ),
    "research_formula_experimental_delivery_attempt_events_v1": (
        "research_stage4_experimental_attempt_delivery_fk",
        "research_stage4_experimental_attempt_delivery_phase_uk",
        "research_stage4_experimental_attempt_event_key_ck",
        "research_stage4_experimental_attempt_event_pk",
        "research_stage4_experimental_attempt_number_ck",
        "research_stage4_experimental_attempt_payload_ck",
        "research_stage4_experimental_attempt_result_ck",
        "research_stage4_experimental_attempt_time_ck",
    ),
}

_INDEXES = {
    "research_formula_experimental_search_runs_v1": (
        "idx_stage4_experimental_search_time_v1",
        "research_stage4_experimental_search_identity_uk",
        "research_stage4_experimental_search_receipt_uk",
        "research_stage4_experimental_search_run_pk",
        "research_stage4_experimental_search_schedule_slot_uk",
    ),
    "research_formula_experimental_alerts_v1": (
        "idx_stage4_experimental_alert_time_v1",
        "research_stage4_experimental_alert_candidate_cell_uk",
        "research_stage4_experimental_alert_pk",
    ),
    "research_formula_experimental_subscriptions_v1": (
        "idx_stage4_experimental_subscription_active_v1",
        "research_stage4_experimental_subscription_pk",
    ),
    "research_formula_experimental_deliveries_v1": (
        "idx_stage4_experimental_delivery_due_v1",
        "idx_stage4_experimental_delivery_inflight_v1",
        "research_stage4_experimental_delivery_occurrence_chat_uk",
        "research_stage4_experimental_delivery_pk",
    ),
    "research_formula_experimental_delivery_attempt_events_v1": (
        "idx_stage4_experimental_attempt_delivery_v1",
        "research_stage4_experimental_attempt_delivery_phase_uk",
        "research_stage4_experimental_attempt_event_pk",
    ),
}

_TRIGGERS = {
    "research_formula_experimental_search_runs_v1": (
        "trg_stage4_experimental_search_runs_immutable_v1",
        "trg_stage4_experimental_search_runs_no_truncate_v1",
    ),
    "research_formula_experimental_alerts_v1": (
        "trg_stage4_experimental_alerts_immutable_v1",
        "trg_stage4_experimental_alerts_no_truncate_v1",
    ),
    "research_formula_experimental_subscriptions_v1": (
        "trg_stage4_experimental_subscriptions_no_truncate_v1",
        "trg_stage4_experimental_subscriptions_validate_v1",
    ),
    "research_formula_experimental_deliveries_v1": (
        "trg_stage4_experimental_deliveries_no_truncate_v1",
        "trg_stage4_experimental_deliveries_validate_v1",
        "trg_stage4_experimental_delivery_attempt_audit_v1",
    ),
    "research_formula_experimental_delivery_attempt_events_v1": (
        "trg_stage4_experimental_attempts_immutable_v1",
        "trg_stage4_experimental_attempts_no_truncate_v1",
    ),
}

_PROTECTED_TABLES = (
    "research_events",
    "research_formulas",
    "research_formula_live_approvals",
    "research_formula_live_deliveries",
    "research_formula_alert_subscriptions",
    "research_formula_exploration_stage4_v1",
)

_TRIGGER_FUNCTIONS = (
    "prevent_research_stage4_experimental_immutable_v1",
    "require_research_stage4_experimental_attempt_audit_v1",
    "validate_research_stage4_experimental_attempt_v1",
    "validate_research_stage4_experimental_delivery_v1",
    "validate_research_stage4_experimental_subscription_v1",
)


class ExperimentalStoreError(RuntimeError):
    """Base error for the isolated experimental persistence boundary."""


class ExperimentalStoreIntegrityError(ExperimentalStoreError):
    """A persisted or supplied content-addressed row failed verification."""


class ExperimentalStoreConflictError(ExperimentalStoreIntegrityError):
    """A compare-and-set delivery or immutable insert conflict occurred."""


def _database_url() -> str:
    return os.getenv(DATABASE_URL_ENV, "").strip()


def _database_target(url: str) -> tuple[str, str, int, str]:
    try:
        parsed = urlsplit(str(url or "").strip())
        hostname = parsed.hostname
        port = int(parsed.port or 5432)
        target_overrides = {
            key.lower()
            for key, _value in parse_qsl(parsed.query, keep_blank_values=True)
            if key.lower()
            in {"host", "hostaddr", "port", "dbname", "service", "servicefile"}
        }
    except (TypeError, ValueError):
        return ("", "", 0, "")
    database_name = unquote(parsed.path.lstrip("/").split("/", 1)[0])
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


def _database_targets_aligned() -> bool:
    dispatcher_target = _database_target(_database_url())
    reader_target = _database_target(os.getenv(READER_DATABASE_URL_ENV, "").strip())
    return bool(all(dispatcher_target) and dispatcher_target == reader_target)


def _connect(*, read_only: bool = False):
    url = _database_url()
    if not url:
        raise ExperimentalStoreError(
            f"experimental Formula database is not configured in {DATABASE_URL_ENV}"
        )
    if psycopg is None:
        raise ExperimentalStoreError("psycopg is unavailable")
    if not _database_targets_aligned():
        raise ExperimentalStoreIntegrityError(
            "experimental dispatcher and authoritative reader database targets differ"
        )
    options = (
        f"-c statement_timeout={_OPERATIONAL_STATEMENT_TIMEOUT_MS} "
        f"-c lock_timeout={_LOCK_TIMEOUT_MS} "
        "-c search_path=pg_catalog,public -c timezone=UTC"
    )
    if read_only:
        options += " -c default_transaction_read_only=on"
    conn = psycopg.connect(
        url,
        row_factory=dict_row,
        connect_timeout=5,
        options=options,
    )
    try:
        row = conn.execute(
            """
            SELECT current_user AS current_user_name,
                   session_user AS session_user_name,
                   role_row.rolcanlogin, role_row.rolinherit,
                   role_row.rolsuper, role_row.rolcreatedb,
                   role_row.rolcreaterole, role_row.rolreplication,
                   role_row.rolbypassrls,
                   EXISTS (
                       SELECT 1 FROM pg_catalog.pg_auth_members membership
                        WHERE membership.member=role_row.oid
                           OR membership.roleid=role_row.oid
                           OR membership.grantor=role_row.oid
                   ) AS has_membership,
                   pg_catalog.has_database_privilege(
                       current_user, pg_catalog.current_database(), 'CREATE'
                   ) AS database_create,
                   pg_catalog.has_schema_privilege(
                       current_user, 'public', 'CREATE'
                   ) AS schema_create
              FROM pg_catalog.pg_roles role_row
             WHERE role_row.rolname=current_user
            """
        ).fetchone()
        if not row or (
            row.get("current_user_name") != DISPATCHER_ROLE
            or row.get("session_user_name") != DISPATCHER_ROLE
            or row.get("rolcanlogin") is not True
            or any(
                row.get(name) is not False
                for name in (
                    "rolinherit",
                    "rolsuper",
                    "rolcreatedb",
                    "rolcreaterole",
                    "rolreplication",
                    "rolbypassrls",
                    "has_membership",
                    "database_create",
                    "schema_create",
                )
            )
        ):
            raise ExperimentalStoreIntegrityError(
                "experimental database connection is not the isolated dispatcher"
            )
        # The attestation SELECT opens an implicit transaction.  End it here
        # so every caller's first business statement receives a fresh
        # transaction_timestamp() instead of inheriting connection setup time.
        conn.commit()
    except Exception:
        conn.close()
        raise
    return conn


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise ExperimentalStoreIntegrityError(
            "experimental persistence value is not canonical JSON"
        ) from exc


def _json_copy(value: Any) -> Any:
    return json.loads(_canonical_json(value))


def _comparison_value(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ExperimentalStoreIntegrityError(
                "database comparison timestamp is timezone-naive"
            )
        return value.astimezone(_UTC).isoformat()
    if isinstance(value, Mapping):
        return {
            str(key): _comparison_value(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_comparison_value(item) for item in value]
    return value


def _strict_equal(left: Any, right: Any) -> bool:
    return _canonical_json(_comparison_value(left)) == _canonical_json(
        _comparison_value(right)
    )


def _fingerprint(kind: str, value: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        f"{kind}:{_canonical_json(value)}".encode("utf-8")
    ).hexdigest()


def _payload_sha256(value: Any) -> str:
    return hashlib.sha256(
        f"{SEARCH_PAYLOAD_HASH_VERSION}:{_canonical_json(value)}".encode("utf-8")
    ).hexdigest()


def _text_sha256(value: str) -> str:
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def _is_sha256(value: Any) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(character in _HEX for character in value)
    )


def _utc(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        result = value
    elif type(value) is str and value.strip():
        try:
            result = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise ExperimentalStoreIntegrityError(
                f"{field_name} is not an ISO timestamp"
            ) from exc
    else:
        raise ExperimentalStoreIntegrityError(f"{field_name} is required")
    if result.tzinfo is None or result.utcoffset() is None:
        raise ExperimentalStoreIntegrityError(f"{field_name} must be timezone-aware")
    return result.astimezone(_UTC)


def _row_value(row: Mapping[str, Any], name: str) -> Any:
    value = row.get(name)
    return (
        value.strip()
        if isinstance(value, str)
        and name.endswith(
            ("_id", "_sha256", "_key", "_token", "_fingerprint")
        )
        else value
    )


def _expected_table_grants() -> set[tuple[str, str]]:
    return {
        (table, privilege)
        for table in _TABLES
        for privilege in ("SELECT", "INSERT")
    }


def _expected_column_grants() -> set[tuple[str, str, str]]:
    grants = {
        (
            "research_formula_experimental_subscriptions_v1",
            "active",
            "UPDATE",
        )
    }
    grants.update(
        (
            "research_formula_experimental_deliveries_v1",
            column,
            "UPDATE",
        )
        for column in (
            "status",
            "attempt_count",
            "available_at_utc",
            "claim_token",
            "claimed_at_utc",
            "claim_expires_at_utc",
            "sent_at_utc",
            "telegram_message_id",
            "last_failure_kind",
            "last_error",
        )
    )
    return grants


def schema_status() -> Dict[str, Any]:
    """Attest the dedicated role, exact tables, ACLs, and queue counts."""

    base: Dict[str, Any] = {
        "configured": bool(_database_url()),
        "database_url_env": DATABASE_URL_ENV,
        "expected_role": DISPATCHER_ROLE,
        "reader_database_url_env": READER_DATABASE_URL_ENV,
        "database_aligned": _database_targets_aligned(),
        "schema_present": False,
        "ready": False,
        "missing": [],
        "experimental_only": True,
        "live_delivery_allowed": False,
        "trade_execution_allowed": False,
        "stale_in_flight_policy": "AMBIGUOUS_NO_AUTOMATIC_RETRY",
    }
    if not base["configured"] or psycopg is None:
        base["missing"] = [
            "database_configuration" if not base["configured"] else "psycopg"
        ]
        return base
    if not base["database_aligned"]:
        base["missing"] = ["authoritative_reader_database_alignment"]
        return base

    try:
        with _connect(read_only=True) as conn:
            identity = conn.execute(
                """
                SELECT current_user AS current_user_name,
                       session_user AS session_user_name,
                       pg_catalog.current_database() AS database_name,
                       pg_catalog.has_schema_privilege(
                           current_user, 'public', 'USAGE'
                       ) AS schema_usage,
                       pg_catalog.has_schema_privilege(
                           current_user, 'public', 'CREATE'
                       ) AS schema_create,
                       pg_catalog.has_database_privilege(
                           current_user, pg_catalog.current_database(), 'CREATE'
                       ) AS database_create,
                       (
                           SELECT pg_catalog.pg_get_userbyid(source.relowner)
                             FROM pg_catalog.pg_class source
                             JOIN pg_catalog.pg_namespace source_namespace
                               ON source_namespace.oid=source.relnamespace
                            WHERE source_namespace.nspname='public'
                              AND source.relname='research_events'
                       ) AS trusted_owner_name
                """
            ).fetchone()
            role = conn.execute(
                """
                SELECT role_row.oid AS role_oid,
                       rolname, rolcanlogin, rolinherit, rolsuper, rolcreatedb,
                       rolcreaterole, rolreplication, rolbypassrls,
                       EXISTS (
                           SELECT 1 FROM pg_catalog.pg_auth_members membership
                            WHERE membership.member = role_row.oid
                               OR membership.roleid = role_row.oid
                               OR membership.grantor = role_row.oid
                       ) AS has_membership
                  FROM pg_catalog.pg_roles role_row
                 WHERE rolname = current_user
                """
            ).fetchone()
            relations = conn.execute(
                """
                SELECT relation.relname AS table_name,
                       relation.relkind, relation.relpersistence,
                       relation.relrowsecurity, relation.relforcerowsecurity,
                       pg_catalog.pg_get_userbyid(relation.relowner) AS owner_name,
                       pg_catalog.obj_description(relation.oid, 'pg_class') AS comment,
                       EXISTS (
                           SELECT 1 FROM pg_catalog.pg_policy policy_row
                            WHERE policy_row.polrelid=relation.oid
                       ) AS has_policy,
                       EXISTS (
                           SELECT 1 FROM pg_catalog.pg_rewrite rewrite_row
                            WHERE rewrite_row.ev_class=relation.oid
                              AND rewrite_row.rulename <> '_RETURN'
                       ) AS has_rewrite
                  FROM pg_catalog.pg_class relation
                  JOIN pg_catalog.pg_namespace namespace
                    ON namespace.oid = relation.relnamespace
                 WHERE namespace.nspname = 'public'
                   AND relation.relname = ANY(%s)
                """,
                (list(_TABLES),),
            ).fetchall()
            columns = conn.execute(
                """
                SELECT table_name,
                       ARRAY_AGG(column_name ORDER BY ordinal_position) AS columns
                  FROM information_schema.columns
                 WHERE table_schema = 'public' AND table_name = ANY(%s)
                 GROUP BY table_name
                """,
                (list(_TABLES),),
            ).fetchall()
            constraints = conn.execute(
                """
                SELECT relation.relname AS table_name,
                       ARRAY_AGG(constraint_row.conname::TEXT
                                 ORDER BY constraint_row.conname) AS names,
                       BOOL_OR(
                           NOT constraint_row.convalidated
                           OR constraint_row.condeferrable
                       ) AS has_weak
                  FROM pg_catalog.pg_constraint constraint_row
                  JOIN pg_catalog.pg_class relation
                    ON relation.oid=constraint_row.conrelid
                  JOIN pg_catalog.pg_namespace namespace
                    ON namespace.oid=relation.relnamespace
                 WHERE namespace.nspname='public'
                   AND relation.relname=ANY(%s)
                   AND constraint_row.contype IN ('c','p','u','f')
                 GROUP BY relation.relname
                """,
                (list(_TABLES),),
            ).fetchall()
            indexes = conn.execute(
                """
                SELECT table_relation.relname AS table_name,
                       ARRAY_AGG(index_relation.relname::TEXT
                                 ORDER BY index_relation.relname) AS names
                  FROM pg_catalog.pg_index index_row
                  JOIN pg_catalog.pg_class table_relation
                    ON table_relation.oid=index_row.indrelid
                  JOIN pg_catalog.pg_class index_relation
                    ON index_relation.oid=index_row.indexrelid
                  JOIN pg_catalog.pg_namespace namespace
                    ON namespace.oid=table_relation.relnamespace
                 WHERE namespace.nspname='public'
                   AND table_relation.relname=ANY(%s)
                   AND index_row.indisvalid AND index_row.indisready
                 GROUP BY table_relation.relname
                """,
                (list(_TABLES),),
            ).fetchall()
            triggers = conn.execute(
                """
                SELECT relation.relname AS table_name,
                       ARRAY_AGG(trigger_row.tgname::TEXT
                                 ORDER BY trigger_row.tgname) AS names
                  FROM pg_catalog.pg_trigger trigger_row
                  JOIN pg_catalog.pg_class relation
                    ON relation.oid=trigger_row.tgrelid
                  JOIN pg_catalog.pg_namespace namespace
                    ON namespace.oid=relation.relnamespace
                 WHERE namespace.nspname='public'
                   AND relation.relname=ANY(%s)
                   AND NOT trigger_row.tgisinternal
                   AND trigger_row.tgenabled='A'
                 GROUP BY relation.relname
                """,
                (list(_TABLES),),
            ).fetchall()
            table_grants = conn.execute(
                """
                SELECT relation.relname AS table_name,
                       relation.relowner AS owner_oid,
                       acl.grantee, acl.privilege_type, acl.is_grantable
                  FROM pg_catalog.pg_class relation
                  JOIN pg_catalog.pg_namespace namespace
                    ON namespace.oid = relation.relnamespace
                  CROSS JOIN LATERAL pg_catalog.aclexplode(
                      COALESCE(
                          relation.relacl,
                          pg_catalog.acldefault('r', relation.relowner)
                      )
                  ) acl
                 WHERE namespace.nspname = 'public'
                   AND relation.relname = ANY(%s)
                """,
                (list(_TABLES),),
            ).fetchall()
            column_grants = conn.execute(
                """
                SELECT relation.relname AS table_name,
                       attribute.attname AS column_name,
                       acl.grantee, acl.privilege_type, acl.is_grantable
                  FROM pg_catalog.pg_attribute attribute
                  JOIN pg_catalog.pg_class relation
                    ON relation.oid = attribute.attrelid
                  JOIN pg_catalog.pg_namespace namespace
                    ON namespace.oid = relation.relnamespace
                  CROSS JOIN LATERAL pg_catalog.aclexplode(attribute.attacl) acl
                 WHERE namespace.nspname = 'public'
                   AND relation.relname = ANY(%s)
                   AND attribute.attnum > 0 AND NOT attribute.attisdropped
                """,
                (list(_TABLES),),
            ).fetchall()
            protected = conn.execute(
                """
                SELECT protected.table_name,
                       privilege.privilege_name,
                       pg_catalog.has_table_privilege(
                           current_user,
                           'public.' || protected.table_name,
                           privilege.privilege_name
                       ) AS granted
                  FROM UNNEST(%s::TEXT[]) protected(table_name)
                  CROSS JOIN UNNEST(%s::TEXT[]) privilege(privilege_name)
                """,
                (
                    list(_PROTECTED_TABLES),
                    [
                        "SELECT",
                        "INSERT",
                        "UPDATE",
                        "DELETE",
                        "TRUNCATE",
                        "REFERENCES",
                        "TRIGGER",
                    ],
                ),
            ).fetchall()
            protected_columns = conn.execute(
                """
                SELECT protected.table_name,
                       privilege.privilege_name,
                       pg_catalog.has_any_column_privilege(
                           current_user,
                           'public.' || protected.table_name,
                           privilege.privilege_name
                       ) AS granted
                  FROM UNNEST(%s::TEXT[]) protected(table_name)
                  CROSS JOIN UNNEST(%s::TEXT[]) privilege(privilege_name)
                """,
                (
                    list(_PROTECTED_TABLES),
                    ["SELECT", "INSERT", "UPDATE", "REFERENCES"],
                ),
            ).fetchall()
            trigger_functions = conn.execute(
                """
                SELECT function_row.proname AS function_name,
                       pg_catalog.pg_get_userbyid(
                           function_row.proowner
                       ) AS owner_name,
                       function_row.prosecdef,
                       function_row.provolatile,
                       function_row.proconfig,
                       EXISTS (
                           SELECT 1
                             FROM pg_catalog.aclexplode(
                                 COALESCE(
                                     function_row.proacl,
                                     pg_catalog.acldefault(
                                         'f', function_row.proowner
                                     )
                                 )
                             ) function_acl
                            WHERE function_acl.grantee <>
                                  function_row.proowner
                       ) AS has_nonowner_acl
                  FROM pg_catalog.pg_proc function_row
                  JOIN pg_catalog.pg_namespace namespace
                    ON namespace.oid=function_row.pronamespace
                 WHERE namespace.nspname='public'
                   AND function_row.proname=ANY(%s)
                   AND function_row.pronargs=0
                 ORDER BY function_row.proname
                """,
                (list(_TRIGGER_FUNCTIONS),),
            ).fetchall()

            missing: list[str] = []
            if not identity or (
                identity.get("current_user_name") != DISPATCHER_ROLE
                or identity.get("session_user_name") != DISPATCHER_ROLE
                or identity.get("schema_usage") is not True
                or identity.get("schema_create") is not False
                or identity.get("database_create") is not False
            ):
                missing.append("dedicated_dispatcher_identity")
            if not role or (
                role.get("rolname") != DISPATCHER_ROLE
                or role.get("rolcanlogin") is not True
                or any(
                    role.get(name) is not False
                    for name in (
                        "rolinherit",
                        "rolsuper",
                        "rolcreatedb",
                        "rolcreaterole",
                        "rolreplication",
                        "rolbypassrls",
                        "has_membership",
                    )
                )
            ):
                missing.append("dedicated_dispatcher_role_contract")

            relation_map = {row["table_name"]: row for row in relations}
            column_map = {
                row["table_name"]: tuple(row.get("columns") or ())
                for row in columns
            }
            constraint_map = {
                row["table_name"]: tuple(row.get("names") or ())
                for row in constraints
            }
            weak_constraint_map = {
                row["table_name"]: bool(row.get("has_weak"))
                for row in constraints
            }
            index_map = {
                row["table_name"]: tuple(row.get("names") or ())
                for row in indexes
            }
            trigger_map = {
                row["table_name"]: tuple(row.get("names") or ())
                for row in triggers
            }
            trusted_owner_name = (
                identity.get("trusted_owner_name") if identity else None
            )
            for table, expected_columns in _TABLES.items():
                relation = relation_map.get(table)
                if relation is None:
                    missing.append(f"table:{table}")
                    continue
                if (
                    relation.get("relkind") != "r"
                    or relation.get("relpersistence") != "p"
                    or relation.get("relrowsecurity") is not False
                    or relation.get("relforcerowsecurity") is not False
                    or not trusted_owner_name
                    or relation.get("owner_name") != trusted_owner_name
                    or relation.get("has_policy") is not False
                    or relation.get("has_rewrite") is not False
                    or relation.get("comment") != _TABLE_COMMENTS[table]
                ):
                    missing.append(f"table_contract:{table}")
                if column_map.get(table) != expected_columns:
                    missing.append(f"columns:{table}")
                if constraint_map.get(table) != _CONSTRAINTS[table]:
                    missing.append(f"constraints:{table}")
                if weak_constraint_map.get(table) is not False:
                    missing.append(f"weak_constraints:{table}")
                if index_map.get(table) != _INDEXES[table]:
                    missing.append(f"indexes:{table}")
                if trigger_map.get(table) != _TRIGGERS[table]:
                    missing.append(f"triggers:{table}")

            dispatcher_oid = role.get("role_oid") if role else None
            actual_table_grants = {
                (row["table_name"], row["privilege_type"])
                for row in table_grants
                if row.get("grantee") == dispatcher_oid
                and row.get("is_grantable") is False
            }
            unexpected_table_grant = any(
                row.get("grantee") != row.get("owner_oid")
                and not (
                    row.get("grantee") == dispatcher_oid
                    and row.get("privilege_type") in {"SELECT", "INSERT"}
                    and row.get("is_grantable") is False
                )
                for row in table_grants
            )
            if (
                actual_table_grants != _expected_table_grants()
                or unexpected_table_grant
            ):
                missing.append("exact_table_acl")
            actual_column_grants = {
                (
                    row["table_name"],
                    row["column_name"],
                    row["privilege_type"],
                )
                for row in column_grants
                if row.get("grantee") == dispatcher_oid
                and row.get("is_grantable") is False
            }
            unexpected_column_grant = any(
                row.get("grantee") != dispatcher_oid
                or row.get("is_grantable") is not False
                or (
                    row.get("table_name"),
                    row.get("column_name"),
                    row.get("privilege_type"),
                )
                not in _expected_column_grants()
                for row in column_grants
            )
            if (
                actual_column_grants != _expected_column_grants()
                or unexpected_column_grant
            ):
                missing.append("exact_column_acl")
            function_map = {
                row["function_name"]: row for row in trigger_functions
            }
            if tuple(sorted(function_map)) != _TRIGGER_FUNCTIONS or any(
                row.get("owner_name") != trusted_owner_name
                or row.get("prosecdef") is not False
                or row.get("provolatile") != "v"
                or tuple(row.get("proconfig") or ())
                != ("search_path=pg_catalog, public",)
                or row.get("has_nonowner_acl") is not False
                for row in trigger_functions
            ):
                missing.append("trigger_function_contract")
            if any(row.get("granted") is True for row in protected):
                missing.append("forbidden_existing_formula_or_source_authority")
            if any(row.get("granted") is True for row in protected_columns):
                missing.append(
                    "forbidden_existing_formula_or_source_column_authority"
                )

            base["missing"] = sorted(set(missing))
            base["schema_present"] = not missing
            base["ready"] = not missing
            base["current_user"] = identity.get("current_user_name") if identity else None
            base["database_name"] = identity.get("database_name") if identity else None
            if not missing:
                counts = conn.execute(
                    """
                    SELECT
                      (SELECT COUNT(*) FROM public.research_formula_experimental_search_runs_v1) AS search_runs,
                      (SELECT COUNT(*) FROM public.research_formula_experimental_alerts_v1) AS alert_occurrences,
                      (SELECT COUNT(*) FROM public.research_formula_experimental_subscriptions_v1 WHERE active) AS active_subscriptions,
                      (SELECT COUNT(*) FROM public.research_formula_experimental_deliveries_v1 WHERE status='PENDING') AS pending_deliveries,
                      (SELECT COUNT(*) FROM public.research_formula_experimental_deliveries_v1 WHERE status='IN_FLIGHT') AS in_flight_deliveries,
                      (SELECT COUNT(*) FROM public.research_formula_experimental_deliveries_v1 WHERE status='SENT') AS sent_deliveries,
                      (SELECT COUNT(*) FROM public.research_formula_experimental_deliveries_v1 WHERE status='AMBIGUOUS') AS ambiguous_deliveries
                    """
                ).fetchone()
                base.update(
                    {name: int(value or 0) for name, value in dict(counts).items()}
                )
    except Exception as exc:
        base["missing"] = ["schema_attestation_failed"]
        base["error"] = f"{type(exc).__name__}: {exc}"[:1000]
        base["schema_present"] = False
        base["ready"] = False
    return _json_copy(base)


def _validated_search(
    result: Mapping[str, Any],
    *,
    source_corpus_receipt_sha256: str,
    schedule_slot_utc: Any,
) -> Dict[str, Any]:
    if not isinstance(result, Mapping):
        raise ExperimentalStoreIntegrityError("candidate search result is not a mapping")
    payload = _json_copy(dict(result))
    envelope = experimental_alert.compact_eligible_search_envelope(payload)
    envelope_payload = envelope.to_dict()
    source_receipt = str(source_corpus_receipt_sha256 or "").strip()
    if not _is_sha256(source_receipt):
        raise ExperimentalStoreIntegrityError("source corpus receipt is invalid")
    receipt = str(payload.get("search_receipt_sha256") or "").strip()
    if not _is_sha256(receipt):
        raise ExperimentalStoreIntegrityError("candidate search receipt is invalid")
    analysis_as_of = _utc(payload.get("analysis_as_of_utc"), field_name="analysis_as_of_utc")
    schedule_slot = _utc(schedule_slot_utc, field_name="schedule_slot_utc")
    if schedule_slot > analysis_as_of:
        raise ExperimentalStoreIntegrityError("search schedule slot is after analysis time")
    input_count = payload.get("input_observation_count")
    eligible_count = len(envelope_payload["eligible_candidates"])
    if type(input_count) is not int or not (
        0 <= input_count <= candidate_search.MAX_OBSERVATIONS
    ):
        raise ExperimentalStoreIntegrityError("candidate search input count is invalid")
    configured_max = (payload.get("config") or {}).get("max_candidates_evaluated")
    if type(configured_max) is not int or not (1 <= configured_max <= 4096):
        raise ExperimentalStoreIntegrityError("candidate search budget is invalid")
    if not (0 <= eligible_count <= configured_max):
        raise ExperimentalStoreIntegrityError("eligible candidate count exceeds search bound")
    search_status = str(payload.get("status") or "")
    expected_status = (
        "EMPTY_CORPUS"
        if input_count == 0
        else "ELIGIBLE_EXPERIMENTAL_CANDIDATES_FOUND"
        if eligible_count
        else "NO_ELIGIBLE_EXPERIMENTAL_CANDIDATES"
    )
    if search_status != expected_status:
        raise ExperimentalStoreIntegrityError("candidate search status is inconsistent")
    identity = {
        "search_receipt_sha256": receipt,
        "source_corpus_receipt_sha256": source_receipt,
        "schedule_slot_utc": schedule_slot.isoformat(),
        "analysis_as_of_utc": analysis_as_of.isoformat(),
        "horizon_minutes": int(payload["horizon_minutes"]),
    }
    return {
        "search_run_id": _fingerprint(SEARCH_RUN_ID_VERSION, identity),
        "search_receipt_sha256": receipt,
        "source_corpus_receipt_sha256": source_receipt,
        "input_observation_chain_sha256": payload[
            "input_observation_chain_sha256"
        ],
        "engine_version": payload["engine_version"],
        "candidate_schema_version": payload["candidate_schema_version"],
        "feature_schema_version": payload["feature_schema_version"],
        "label_policy_version": payload["label_policy_version"],
        "independence_policy_version": payload["independence_policy_version"],
        "multiple_testing_policy_version": payload[
            "multiple_testing_policy_version"
        ],
        "schedule_slot_utc": schedule_slot,
        "analysis_as_of_utc": analysis_as_of,
        "horizon_minutes": int(payload["horizon_minutes"]),
        "input_observation_count": input_count,
        "eligible_candidate_count": eligible_count,
        "search_status": search_status,
        "search_payload": payload,
        "search_payload_sha256": _payload_sha256(payload),
        "compact_envelope": envelope,
    }


def _search_row_projection(value: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        key: _row_value(value, key)
        for key in (
            "search_run_id",
            "search_receipt_sha256",
            "source_corpus_receipt_sha256",
            "input_observation_chain_sha256",
            "engine_version",
            "candidate_schema_version",
            "feature_schema_version",
            "label_policy_version",
            "independence_policy_version",
            "multiple_testing_policy_version",
            "schedule_slot_utc",
            "analysis_as_of_utc",
            "horizon_minutes",
            "input_observation_count",
            "eligible_candidate_count",
            "search_status",
            "search_payload",
            "search_payload_sha256",
        )
    }


def _assert_search_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    expected = _validated_search(
        row.get("search_payload") or {},
        source_corpus_receipt_sha256=str(
            _row_value(row, "source_corpus_receipt_sha256") or ""
        ),
        schedule_slot_utc=row.get("schedule_slot_utc"),
    )
    stored = _search_row_projection(row)
    comparable = {
        key: expected[key]
        for key in stored
    }
    if not _strict_equal(stored, comparable):
        raise ExperimentalStoreIntegrityError("stored candidate search row mismatch")
    if (
        row.get("formula_registry_effect") != "NONE"
        or row.get("delivery_channel") != "NONE"
        or row.get("live_eligible") is not False
        or row.get("telegram_delivery_allowed") is not False
        or row.get("trade_execution_allowed") is not False
    ):
        raise ExperimentalStoreIntegrityError(
            "stored candidate search exceeded its authority boundary"
        )
    return expected


def persist_search_run(
    search_result: Mapping[str, Any],
    *,
    source_corpus_receipt_sha256: str,
    schedule_slot_utc: Any,
) -> Dict[str, Any]:
    """Insert or byte-verify one immutable, ready candidate search."""

    value = _validated_search(
        search_result,
        source_corpus_receipt_sha256=source_corpus_receipt_sha256,
        schedule_slot_utc=schedule_slot_utc,
    )
    with _connect(read_only=False) as conn:
        row = conn.execute(
            """
            INSERT INTO public.research_formula_experimental_search_runs_v1 (
                search_run_id, search_receipt_sha256,
                source_corpus_receipt_sha256,
                input_observation_chain_sha256, engine_version,
                candidate_schema_version, feature_schema_version,
                label_policy_version, independence_policy_version,
                multiple_testing_policy_version, schedule_slot_utc,
                analysis_as_of_utc, horizon_minutes,
                input_observation_count, eligible_candidate_count,
                search_status, search_payload, search_payload_sha256,
                formula_registry_effect, delivery_channel, live_eligible,
                telegram_delivery_allowed, trade_execution_allowed
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s::jsonb,%s,'NONE','NONE',FALSE,FALSE,FALSE
            )
            ON CONFLICT DO NOTHING
            RETURNING *
            """,
            (
                value["search_run_id"],
                value["search_receipt_sha256"],
                value["source_corpus_receipt_sha256"],
                value["input_observation_chain_sha256"],
                value["engine_version"],
                value["candidate_schema_version"],
                value["feature_schema_version"],
                value["label_policy_version"],
                value["independence_policy_version"],
                value["multiple_testing_policy_version"],
                value["schedule_slot_utc"],
                value["analysis_as_of_utc"],
                value["horizon_minutes"],
                value["input_observation_count"],
                value["eligible_candidate_count"],
                value["search_status"],
                _canonical_json(value["search_payload"]),
                value["search_payload_sha256"],
            ),
        ).fetchone()
        inserted = row is not None
        if row is None:
            row = conn.execute(
                """
                SELECT *
                  FROM public.research_formula_experimental_search_runs_v1
                 WHERE search_receipt_sha256=%s
                """,
                (value["search_receipt_sha256"],),
            ).fetchone()
        if row is None:
            raise ExperimentalStoreConflictError(
                "candidate search conflict did not resolve to an immutable row"
            )
        verified = _assert_search_row(row)
        if verified["search_run_id"] != value["search_run_id"]:
            raise ExperimentalStoreConflictError(
                "candidate search receipt was reused with different provenance"
            )
        conn.commit()
    return {
        "inserted": inserted,
        "search_run_id": value["search_run_id"],
        "search_receipt_sha256": value["search_receipt_sha256"],
        "horizon_minutes": value["horizon_minutes"],
        "eligible_candidate_count": value["eligible_candidate_count"],
        "search_status": value["search_status"],
        "formula_registry_effect": "NONE",
        "live_eligible": False,
        "trade_execution_allowed": False,
    }


def load_latest_search_runs() -> list[Dict[str, Any]]:
    """Load and verify the newest immutable search for each horizon."""

    with _connect(read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT ON (horizon_minutes) *
              FROM public.research_formula_experimental_search_runs_v1
             ORDER BY horizon_minutes, analysis_as_of_utc DESC,
                      created_at_utc DESC, search_run_id DESC
            """
        ).fetchall()
    results = []
    for row in rows:
        verified = _assert_search_row(row)
        results.append(
            {
                "search_run_id": verified["search_run_id"],
                "search_receipt_sha256": verified["search_receipt_sha256"],
                "source_corpus_receipt_sha256": verified[
                    "source_corpus_receipt_sha256"
                ],
                "schedule_slot_utc": verified["schedule_slot_utc"].isoformat(),
                "analysis_as_of_utc": verified["analysis_as_of_utc"].isoformat(),
                "horizon_minutes": verified["horizon_minutes"],
                "eligible_candidate_count": verified[
                    "eligible_candidate_count"
                ],
                "search_result": _json_copy(verified["search_payload"]),
                "compact_envelope": verified["compact_envelope"],
            }
        )
    return sorted(results, key=lambda item: int(item["horizon_minutes"]))


def set_alert_subscription(
    chat_id: int,
    *,
    active: bool,
    requested_by_user_id: Optional[int] = None,
) -> Dict[str, Any]:
    """Set the explicit experimental opt-in without touching LIVE consent."""

    identifier = int(chat_id)
    if identifier == 0:
        raise ValueError("chat_id cannot be zero")
    if active:
        if requested_by_user_id is None or int(requested_by_user_id) <= 0:
            raise ValueError("a positive Telegram user id is required for opt-in")
        with _connect(read_only=False) as conn:
            row = conn.execute(
                """
                INSERT INTO public.research_formula_experimental_subscriptions_v1 (
                    chat_id, active, requested_by_user_id,
                    subscription_policy_version, consent_source,
                    delivery_scope, disclaimer_acknowledged,
                    disclaimer_acknowledged_at_utc,
                    subscribed_at_utc, updated_at_utc
                ) VALUES (
                    %s, TRUE, %s, %s, %s, %s, %s,
                    transaction_timestamp(), transaction_timestamp(),
                    transaction_timestamp()
                )
                ON CONFLICT (chat_id) DO UPDATE SET active=TRUE
                  WHERE public.research_formula_experimental_subscriptions_v1.active
                        IS FALSE
                RETURNING *
                """,
                (
                    identifier,
                    int(requested_by_user_id),
                    SUBSCRIPTION_POLICY_VERSION,
                    CONSENT_SOURCE,
                    DELIVERY_SCOPE,
                    EXPERIMENTAL_LABEL,
                ),
            ).fetchone()
            if row is None:
                row = conn.execute(
                    """
                    SELECT *
                      FROM public.research_formula_experimental_subscriptions_v1
                     WHERE chat_id=%s AND active=TRUE
                    """,
                    (identifier,),
                ).fetchone()
                if row is None:
                    raise ExperimentalStoreConflictError(
                        "experimental opt-in conflict did not resolve to an active row"
                    )
            conn.commit()
    else:
        with _connect(read_only=False) as conn:
            row = conn.execute(
                """
                UPDATE public.research_formula_experimental_subscriptions_v1
                   SET active=FALSE
                 WHERE chat_id=%s
                RETURNING *
                """,
                (identifier,),
            ).fetchone()
            conn.commit()
        if row is None:
            return {
                "chat_id": identifier,
                "active": False,
                "subscribed": False,
                "delivery_scope": DELIVERY_SCOPE,
            }
    return {
        "chat_id": int(row["chat_id"]),
        "active": bool(row["active"]),
        "subscribed": True,
        "requested_by_user_id": int(row["requested_by_user_id"]),
        "subscription_policy_version": row["subscription_policy_version"],
        "delivery_scope": row["delivery_scope"],
        "disclaimer_acknowledged": row["disclaimer_acknowledged"],
        "subscribed_at_utc": row["subscribed_at_utc"],
        "updated_at_utc": row["updated_at_utc"],
    }


def alert_subscription_status(chat_id: int) -> Dict[str, Any]:
    identifier = int(chat_id)
    with _connect(read_only=True) as conn:
        row = conn.execute(
            """
            SELECT *
              FROM public.research_formula_experimental_subscriptions_v1
             WHERE chat_id=%s
            """,
            (identifier,),
        ).fetchone()
    if row is None:
        return {
            "chat_id": identifier,
            "active": False,
            "subscribed": False,
            "delivery_scope": DELIVERY_SCOPE,
        }
    return {
        "chat_id": int(row["chat_id"]),
        "active": bool(row["active"]),
        "subscribed": True,
        "requested_by_user_id": int(row["requested_by_user_id"]),
        "subscription_policy_version": row["subscription_policy_version"],
        "delivery_scope": row["delivery_scope"],
        "disclaimer_acknowledged": row["disclaimer_acknowledged"],
        "subscribed_at_utc": row["subscribed_at_utc"],
        "updated_at_utc": row["updated_at_utc"],
    }


def _find_envelope_candidate(
    envelope: experimental_alert.CompactEligibleSearchEnvelope,
    candidate_key: str,
) -> Dict[str, Any]:
    matches = [
        candidate
        for candidate in envelope.to_dict()["eligible_candidates"]
        if candidate.get("candidate_key") == candidate_key
    ]
    if len(matches) != 1:
        raise ExperimentalStoreIntegrityError(
            "experimental alert candidate is not unique in its search envelope"
        )
    return _json_copy(matches[0])


def _validated_alert_against_search(
    value: experimental_alert.ExperimentalFormulaAlert | Mapping[str, Any],
    search: Mapping[str, Any],
) -> Dict[str, Any]:
    alert = (
        value
        if type(value) is experimental_alert.ExperimentalFormulaAlert
        else experimental_alert.ExperimentalFormulaAlert.from_dict(value)
    )
    payload = alert.to_dict()
    envelope = search["compact_envelope"]
    if type(envelope) is not experimental_alert.CompactEligibleSearchEnvelope:
        raise ExperimentalStoreIntegrityError("stored search envelope type is invalid")
    candidate_key = str((payload.get("formula") or {}).get("candidate_key") or "")
    candidate = _find_envelope_candidate(envelope, candidate_key)
    provenance = payload.get("provenance")
    formula = payload.get("formula")
    current = payload.get("current_snapshot")
    evidence = payload.get("evidence")
    authority = payload.get("authority")
    candidate_snapshot = payload.get("candidate_snapshot")
    if not all(
        isinstance(item, Mapping)
        for item in (
            provenance,
            formula,
            current,
            evidence,
            authority,
            candidate_snapshot,
        )
    ):
        raise ExperimentalStoreIntegrityError("experimental alert structure is invalid")
    if not _strict_equal(candidate_snapshot, candidate):
        raise ExperimentalStoreIntegrityError(
            "experimental alert candidate snapshot differs from its search"
        )
    if (
        provenance.get("search_receipt_sha256")
        != search["search_receipt_sha256"]
        or provenance.get("compact_search_envelope_sha256")
        != envelope.envelope_sha256
        or provenance.get("input_observation_count")
        != search["input_observation_count"]
        or provenance.get("input_observation_chain_sha256")
        != search["input_observation_chain_sha256"]
        or payload.get("analysis_as_of_utc")
        != search["analysis_as_of_utc"].isoformat()
        or payload.get("horizon_minutes") != search["horizon_minutes"]
    ):
        raise ExperimentalStoreIntegrityError(
            "experimental alert does not bind its immutable search"
        )
    if (
        formula.get("candidate_key") != candidate["candidate_key"]
        or formula.get("formula_text") != candidate["formula_text"]
        or not _strict_equal(formula.get("conditions"), candidate["conditions"])
        or not _strict_equal(
            formula.get("condition_source_closure"),
            candidate["condition_source_closure"],
        )
        or not _strict_equal(
            formula.get("condition_evidence_sources"),
            candidate["condition_evidence_sources"],
        )
    ):
        raise ExperimentalStoreIntegrityError(
            "experimental alert formula differs from its candidate"
        )
    if (
        evidence.get("independence_unit")
        != "DISTINCT_BTC_PARENT_MARKET_MOVEMENT"
        or evidence.get("independent_movement_count")
        != candidate["independent_movement_count"]
        or evidence.get("independent_parent_movements_seen")
        != candidate["independent_parent_movements_seen"]
        or evidence.get("raw_match_count") != candidate["raw_match_count"]
        or not _strict_equal(
            evidence.get("accepted_paths"), candidate["accepted_paths"]
        )
        or not _strict_equal(evidence.get("metrics"), candidate["metrics"])
        or not _strict_equal(
            evidence.get("multiple_testing"), candidate["multiple_testing"]
        )
    ):
        raise ExperimentalStoreIntegrityError(
            "experimental alert evidence differs from its candidate"
        )

    decision = _utc(payload.get("decision_time_utc"), field_name="decision_time_utc")
    expires = _utc(payload.get("expires_at_utc"), field_name="expires_at_utc")
    if expires != decision + timedelta(minutes=experimental_alert.ALERT_EXPIRY_MINUTES):
        raise ExperimentalStoreIntegrityError("experimental alert expiry is invalid")
    if (
        current.get("status") != "FROZEN_BOUND_FRESH"
        or current.get("projection_decision_time_utc") != decision.isoformat()
        or current.get("symbol") != payload.get("symbol")
        or current.get("direction") != payload.get("direction")
        or current.get("btc_parent_movement_id")
        != payload.get("btc_parent_movement_id")
        or current.get("trigger_snapshot_sha256")
        != current.get("current_snapshot_sha256")
        or not _is_sha256(current.get("current_snapshot_sha256"))
        or any(name in current for name in ("outcome", "path"))
    ):
        raise ExperimentalStoreIntegrityError(
            "experimental current snapshot binding is invalid"
        )
    commitment = {
        "contract_version": experimental_alert.CURRENT_SNAPSHOT_COMMITMENT_VERSION,
        "compact_observation_schema_version": (
            candidate_search.CURRENT_OBSERVATION_SCHEMA_VERSION
        ),
        "observation_id": current.get("observation_id"),
        "projection_event_id": current.get("projection_event_id"),
        "projection_event_fingerprint": current.get(
            "projection_event_fingerprint"
        ),
        "snapshot_set_id": current.get("snapshot_set_id"),
        "snapshot_key": current.get("snapshot_key"),
        "projection_decision_time_utc": current.get(
            "projection_decision_time_utc"
        ),
        "archive_cycle_time_utc": current.get("archive_cycle_time_utc"),
        "symbol": current.get("symbol"),
        "direction": current.get("direction"),
        "feature_true_mask": current.get("feature_true_mask"),
        "combined_vote_count": current.get("combined_vote_count"),
        "source_event_ids": current.get("source_event_ids"),
        "source_event_fingerprints": current.get("source_event_fingerprints"),
        "wave_binding_status": "BOUND",
        "btc_parent_movement_id": current.get("btc_parent_movement_id"),
    }
    expected_snapshot_sha = _fingerprint(
        experimental_alert.CURRENT_SNAPSHOT_COMMITMENT_VERSION,
        commitment,
    )
    if current.get("current_snapshot_sha256") != expected_snapshot_sha:
        raise ExperimentalStoreIntegrityError(
            "experimental current snapshot hash is invalid"
        )
    conditions = current.get("condition_results")
    if not isinstance(conditions, list) or not conditions or any(
        not isinstance(condition, Mapping) or condition.get("passed") is not True
        for condition in conditions
    ):
        raise ExperimentalStoreIntegrityError(
            "experimental current condition receipt is invalid"
        )
    expected_trigger_key = _fingerprint(
        experimental_alert.CURRENT_TRIGGER_POLICY_VERSION,
        {
            "candidate_key": candidate_key,
            "symbol": payload.get("symbol"),
            "direction": payload.get("direction"),
            "horizon_minutes": payload.get("horizon_minutes"),
            "btc_parent_movement_id": payload.get("btc_parent_movement_id"),
        },
    )
    if payload.get("trigger_key") != expected_trigger_key:
        raise ExperimentalStoreIntegrityError("experimental trigger key is invalid")
    expected_trigger_receipt = _fingerprint(
        "stage4-experimental-current-trigger-receipt-v1",
        {
            "selection_policy_version": experimental_alert.SELECTION_POLICY_VERSION,
            "trigger_key": expected_trigger_key,
            "current_snapshot_sha256": expected_snapshot_sha,
            "condition_results": conditions,
            "expires_at_utc": expires.isoformat(),
        },
    )
    if (
        payload.get("current_trigger_receipt_sha256")
        != expected_trigger_receipt
        or payload.get("current_trigger_policy_version")
        != experimental_alert.CURRENT_TRIGGER_POLICY_VERSION
    ):
        raise ExperimentalStoreIntegrityError(
            "experimental current trigger receipt is invalid"
        )
    if (
        payload.get("disclaimer") != EXPERIMENTAL_LABEL
        or payload.get("experimental_label") != EXPERIMENTAL_LABEL
        or authority
        != {
            "delivery_channel": DELIVERY_SCOPE,
            "formula_registry_effect": "NONE",
            "human_formula_approval_required": False,
            "live_eligible": False,
            "trade_execution_allowed": False,
            "telegram_delivery_allowed": True,
        }
    ):
        raise ExperimentalStoreIntegrityError(
            "experimental alert exceeded its delivery authority"
        )
    reasons = payload.get("experimental_reasons")
    if not isinstance(reasons, list) or not reasons or any(
        type(reason) is not str or not reason.strip() for reason in reasons
    ):
        raise ExperimentalStoreIntegrityError("experimental reasons are invalid")
    rendered = experimental_alert.render_experimental_telegram_alert(alert)
    if not (
        rendered.startswith(EXPERIMENTAL_LABEL)
        or rendered.startswith(f"🧪 {EXPERIMENTAL_LABEL}")
    ) or rendered.splitlines()[-1] != EXPERIMENTAL_LABEL:
        raise ExperimentalStoreIntegrityError(
            "experimental renderer omitted its exact disclaimer"
        )
    return {
        "alert": alert,
        "payload": payload,
        "candidate": candidate,
        "rendered_message": rendered,
        "rendered_message_sha256": _text_sha256(rendered),
        "decision_time_utc": decision,
        "expires_at_utc": expires,
    }


def persist_experimental_alerts(
    alerts: Sequence[
        experimental_alert.ExperimentalFormulaAlert | Mapping[str, Any]
    ],
) -> Dict[str, Any]:
    """Persist new alert occurrences and fan out only to current opt-ins."""

    if not isinstance(alerts, (list, tuple)) or len(alerts) > 1024:
        raise ValueError("experimental alerts must be a bounded sequence")
    inserted_alerts = 0
    duplicate_alerts = 0
    queued_deliveries = 0
    if not alerts:
        return {
            "alerts_supplied": 0,
            "alerts_inserted": 0,
            "same_wave_duplicates": 0,
            "deliveries_queued": 0,
        }

    with _connect(read_only=False) as conn:
        search_cache: Dict[str, Dict[str, Any]] = {}
        for raw_alert in alerts:
            parsed = (
                raw_alert
                if type(raw_alert) is experimental_alert.ExperimentalFormulaAlert
                else experimental_alert.ExperimentalFormulaAlert.from_dict(raw_alert)
            )
            initial_payload = parsed.to_dict()
            search_receipt = str(
                (initial_payload.get("provenance") or {}).get(
                    "search_receipt_sha256"
                )
                or ""
            )
            if search_receipt not in search_cache:
                search_row = conn.execute(
                    """
                    SELECT *
                      FROM public.research_formula_experimental_search_runs_v1
                     WHERE search_receipt_sha256=%s
                    """,
                    (search_receipt,),
                ).fetchone()
                if search_row is None:
                    raise ExperimentalStoreIntegrityError(
                        "experimental alert search run is missing"
                    )
                search_cache[search_receipt] = _assert_search_row(search_row)
            search = search_cache[search_receipt]
            verified = _validated_alert_against_search(parsed, search)
            payload = verified["payload"]
            current = payload["current_snapshot"]
            candidate = verified["candidate"]
            evidence = payload["evidence"]
            expected_columns = {
                "alert_occurrence_id": parsed.alert_id,
                "search_run_id": search["search_run_id"],
                "candidate_key": candidate["candidate_key"],
                "search_receipt_sha256": search["search_receipt_sha256"],
                "candidate_snapshot": candidate,
                "trigger_key": payload["trigger_key"],
                "trigger_observation_id": current["observation_id"],
                "projection_event_id": int(current["projection_event_id"]),
                "projection_event_fingerprint": current[
                    "projection_event_fingerprint"
                ],
                "btc_parent_movement_id": payload["btc_parent_movement_id"],
                "symbol": payload["symbol"],
                "direction": payload["direction"],
                "horizon_minutes": int(payload["horizon_minutes"]),
                "decision_time_utc": verified["decision_time_utc"],
                "expires_at_utc": verified["expires_at_utc"],
                "trigger_snapshot": current,
                "trigger_snapshot_sha256": current[
                    "trigger_snapshot_sha256"
                ],
                "current_trigger_receipt_sha256": payload[
                    "current_trigger_receipt_sha256"
                ],
                "current_trigger_policy_version": payload[
                    "current_trigger_policy_version"
                ],
                "formula_text": payload["formula"]["formula_text"],
                "conditions": payload["formula"]["conditions"],
                "independent_movement_count": int(
                    evidence["independent_movement_count"]
                ),
                "accepted_paths": evidence["accepted_paths"],
                "metrics": evidence["metrics"],
                "experimental_reasons": payload["experimental_reasons"],
                "renderer_version": payload["renderer_version"],
                "rendered_message": verified["rendered_message"],
                "rendered_message_sha256": verified[
                    "rendered_message_sha256"
                ],
                "disclaimer": EXPERIMENTAL_LABEL,
                "delivery_channel": DELIVERY_SCOPE,
                "formula_registry_effect": "NONE",
                "human_formula_approval_required": False,
                "live_eligible": False,
                "trade_execution_allowed": False,
                "telegram_delivery_allowed": True,
            }
            inserted = conn.execute(
                """
                INSERT INTO public.research_formula_experimental_alerts_v1 (
                    alert_occurrence_id, search_run_id, candidate_key,
                    search_receipt_sha256, candidate_snapshot, trigger_key,
                    trigger_observation_id, projection_event_id,
                    projection_event_fingerprint, btc_parent_movement_id,
                    symbol, direction, horizon_minutes, decision_time_utc,
                    expires_at_utc, trigger_snapshot,
                    trigger_snapshot_sha256, current_trigger_receipt_sha256,
                    current_trigger_policy_version, formula_text, conditions,
                    independent_movement_count, accepted_paths, metrics,
                    experimental_reasons, renderer_version, rendered_message,
                    rendered_message_sha256, disclaimer, delivery_channel,
                    formula_registry_effect, human_formula_approval_required,
                    live_eligible, trade_execution_allowed,
                    telegram_delivery_allowed
                ) VALUES (
                    %s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s::jsonb,%s,%s,%s,%s,%s::jsonb,%s,%s::jsonb,%s::jsonb,
                    %s::jsonb,%s,%s,%s,%s,%s,'NONE',FALSE,FALSE,FALSE,TRUE
                )
                ON CONFLICT DO NOTHING
                RETURNING alert_occurrence_id, created_at_utc
                """,
                (
                    parsed.alert_id,
                    search["search_run_id"],
                    candidate["candidate_key"],
                    search["search_receipt_sha256"],
                    _canonical_json(candidate),
                    payload["trigger_key"],
                    current["observation_id"],
                    int(current["projection_event_id"]),
                    current["projection_event_fingerprint"],
                    payload["btc_parent_movement_id"],
                    payload["symbol"],
                    payload["direction"],
                    int(payload["horizon_minutes"]),
                    verified["decision_time_utc"],
                    verified["expires_at_utc"],
                    _canonical_json(current),
                    current["trigger_snapshot_sha256"],
                    payload["current_trigger_receipt_sha256"],
                    payload["current_trigger_policy_version"],
                    payload["formula"]["formula_text"],
                    _canonical_json(payload["formula"]["conditions"]),
                    int(evidence["independent_movement_count"]),
                    _canonical_json(evidence["accepted_paths"]),
                    _canonical_json(evidence["metrics"]),
                    _canonical_json(payload["experimental_reasons"]),
                    payload["renderer_version"],
                    verified["rendered_message"],
                    verified["rendered_message_sha256"],
                    EXPERIMENTAL_LABEL,
                    DELIVERY_SCOPE,
                ),
            ).fetchone()
            if inserted is None:
                existing = conn.execute(
                    """
                    SELECT *
                      FROM public.research_formula_experimental_alerts_v1
                     WHERE alert_occurrence_id=%s
                        OR (candidate_key=%s AND trigger_key=%s)
                     ORDER BY (alert_occurrence_id=%s) DESC
                     LIMIT 1
                    """,
                    (
                        parsed.alert_id,
                        candidate["candidate_key"],
                        payload["trigger_key"],
                        parsed.alert_id,
                    ),
                ).fetchone()
                if existing is None:
                    raise ExperimentalStoreConflictError(
                        "experimental alert conflict did not resolve"
                    )
                existing_id = str(_row_value(existing, "alert_occurrence_id"))
                if existing_id == parsed.alert_id:
                    actual_columns = {
                        key: _row_value(existing, key)
                        for key in expected_columns
                    }
                    if not _strict_equal(actual_columns, expected_columns):
                        raise ExperimentalStoreConflictError(
                            "stored experimental alert row mismatch on immutable retry"
                        )
                else:
                    same_wave_identity = {
                        key: _row_value(existing, key)
                        for key in (
                            "candidate_key",
                            "trigger_key",
                            "btc_parent_movement_id",
                            "symbol",
                            "direction",
                            "horizon_minutes",
                        )
                    }
                    expected_identity = {
                        key: expected_columns[key]
                        for key in same_wave_identity
                    }
                    if not _strict_equal(same_wave_identity, expected_identity):
                        raise ExperimentalStoreConflictError(
                            "same-wave alert conflict has a different identity"
                        )
                duplicate_alerts += 1
                continue

            inserted_alerts += 1
            subscriptions = conn.execute(
                """
                SELECT chat_id
                  FROM public.research_formula_experimental_subscriptions_v1
                 WHERE active=TRUE
                   AND updated_at_utc <= %s
                   AND %s > transaction_timestamp()
                 ORDER BY chat_id
                """,
                (verified["decision_time_utc"], verified["expires_at_utc"]),
            ).fetchall()
            for subscription in subscriptions:
                chat_id = int(subscription["chat_id"])
                delivery_key = _fingerprint(
                    DELIVERY_KEY_VERSION,
                    {
                        "alert_occurrence_id": parsed.alert_id,
                        "chat_id": chat_id,
                    },
                )
                queued = conn.execute(
                    """
                    INSERT INTO public.research_formula_experimental_deliveries_v1 (
                        delivery_key, alert_occurrence_id, chat_id, status,
                        attempt_count, available_at_utc
                    ) VALUES (%s,%s,%s,'PENDING',0,transaction_timestamp())
                    ON CONFLICT DO NOTHING
                    RETURNING delivery_key
                    """,
                    (delivery_key, parsed.alert_id, chat_id),
                ).fetchone()
                if queued is None:
                    conflicts = conn.execute(
                        """
                        SELECT delivery_key, alert_occurrence_id, chat_id
                          FROM public.research_formula_experimental_deliveries_v1
                         WHERE delivery_key=%s
                            OR (alert_occurrence_id=%s AND chat_id=%s)
                         ORDER BY delivery_key
                        """,
                        (delivery_key, parsed.alert_id, chat_id),
                    ).fetchall()
                    expected_delivery_identity = {
                        "delivery_key": delivery_key,
                        "alert_occurrence_id": parsed.alert_id,
                        "chat_id": chat_id,
                    }
                    if len(conflicts) != 1 or not _strict_equal(
                        {
                            key: _row_value(conflicts[0], key)
                            for key in expected_delivery_identity
                        }
                        if len(conflicts) == 1
                        else {},
                        expected_delivery_identity,
                    ):
                        raise ExperimentalStoreConflictError(
                            "experimental delivery conflict did not resolve "
                            "to the expected immutable identity"
                        )
                else:
                    queued_deliveries += 1
        conn.commit()
    return {
        "alerts_supplied": len(alerts),
        "alerts_inserted": inserted_alerts,
        "same_wave_duplicates": duplicate_alerts,
        "deliveries_queued": queued_deliveries,
        "formula_registry_effect": "NONE",
        "live_eligible": False,
        "trade_execution_allowed": False,
    }


def _attempt_event(
    *,
    delivery_key: str,
    attempt_number: int,
    event_phase: str,
    claim_token: str,
    terminal_result: Optional[str] = None,
    telegram_message_id: Optional[int] = None,
    error_text: Optional[str] = None,
) -> tuple[str, Dict[str, Any]]:
    payload = {
        "delivery_key": delivery_key,
        "attempt_number": int(attempt_number),
        "event_phase": event_phase,
        "terminal_result": terminal_result,
        "claim_token": claim_token,
        "telegram_message_id": telegram_message_id,
        "error_text": error_text,
    }
    event_key = _fingerprint(ATTEMPT_EVENT_KEY_VERSION, payload)
    payload["attempt_event_key"] = event_key
    return event_key, payload


def _insert_attempt_event(conn: Any, payload: Mapping[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO public.research_formula_experimental_delivery_attempt_events_v1 (
            attempt_event_key, delivery_key, attempt_number, event_phase,
            terminal_result, claim_token, event_time_utc,
            telegram_message_id, error_text, event_payload
        ) VALUES (%s,%s,%s,%s,%s,%s,transaction_timestamp(),%s,%s,%s::jsonb)
        """,
        (
            payload["attempt_event_key"],
            payload["delivery_key"],
            payload["attempt_number"],
            payload["event_phase"],
            payload["terminal_result"],
            payload["claim_token"],
            payload["telegram_message_id"],
            payload["error_text"],
            _canonical_json(payload),
        ),
    )


def claim_pending_deliveries(*, limit: int = 20) -> list[Dict[str, Any]]:
    """Commit stale cleanup, then lease due deliveries with a fresh DB clock."""

    batch_limit = int(limit)
    if not (1 <= batch_limit <= _MAX_CLAIM_BATCH):
        raise ValueError(f"delivery claim limit must be within 1..{_MAX_CLAIM_BATCH}")
    claimed: list[Dict[str, Any]] = []
    with _connect(read_only=False) as conn:
        stale = conn.execute(
            """
            SELECT delivery_key, attempt_count, claim_token
              FROM public.research_formula_experimental_deliveries_v1
             WHERE status='IN_FLIGHT'
               AND claim_expires_at_utc <= transaction_timestamp()
             ORDER BY claim_expires_at_utc, delivery_key
             LIMIT 200
             FOR UPDATE SKIP LOCKED
            """
        ).fetchall()
        for row in stale:
            delivery_key = str(_row_value(row, "delivery_key"))
            claim_token = str(_row_value(row, "claim_token"))
            error = "claim lease expired after a potentially attempted Telegram send"
            conn.execute(
                """
                UPDATE public.research_formula_experimental_deliveries_v1
                   SET status='AMBIGUOUS', last_failure_kind='AMBIGUOUS_SEND',
                       last_error=%s
                 WHERE delivery_key=%s AND status='IN_FLIGHT'
                   AND claim_token=%s
                """,
                (error, delivery_key, claim_token),
            )
            _, event = _attempt_event(
                delivery_key=delivery_key,
                attempt_number=int(row["attempt_count"]),
                event_phase="TERMINAL",
                terminal_result="AMBIGUOUS",
                claim_token=claim_token,
                error_text=error,
            )
            _insert_attempt_event(conn, event)

        conn.execute(
            """
            WITH expired AS (
                SELECT delivery.delivery_key
                  FROM public.research_formula_experimental_deliveries_v1 delivery
                  JOIN public.research_formula_experimental_alerts_v1 alert
                    ON alert.alert_occurrence_id=delivery.alert_occurrence_id
                 WHERE delivery.status IN ('PENDING','RETRYABLE')
                   AND alert.expires_at_utc <= transaction_timestamp()
                 ORDER BY alert.expires_at_utc, delivery.delivery_key
                 LIMIT 500
                 FOR UPDATE OF delivery SKIP LOCKED
            )
            UPDATE public.research_formula_experimental_deliveries_v1 delivery
               SET status='EXPIRED',
                   last_failure_kind='EXPIRED_BEFORE_SEND',
                   last_error='experimental alert expired before a safe send claim'
              FROM expired
             WHERE delivery.delivery_key=expired.delivery_key
            """
        )
        # Cleanup can touch hundreds of rows and append one audit row per stale
        # attempt.  Commit it before selecting new work so claim timestamps and
        # leases cannot inherit the maintenance transaction's age.
        conn.commit()
        due = conn.execute(
            """
            SELECT delivery.delivery_key, delivery.alert_occurrence_id,
                   delivery.chat_id, delivery.attempt_count,
                   alert.expires_at_utc, alert.rendered_message,
                   alert.rendered_message_sha256
              FROM public.research_formula_experimental_deliveries_v1 delivery
              JOIN public.research_formula_experimental_alerts_v1 alert
                ON alert.alert_occurrence_id=delivery.alert_occurrence_id
              JOIN public.research_formula_experimental_subscriptions_v1 subscription
                ON subscription.chat_id=delivery.chat_id
             WHERE delivery.status IN ('PENDING','RETRYABLE')
               AND delivery.attempt_count < %s
               AND delivery.available_at_utc <= transaction_timestamp()
               AND alert.expires_at_utc > transaction_timestamp() +
                   (%s * INTERVAL '1 second')
               AND subscription.active=TRUE
               AND subscription.updated_at_utc <= alert.decision_time_utc
             ORDER BY delivery.available_at_utc, delivery.created_at_utc,
                      delivery.delivery_key
             LIMIT %s
             FOR UPDATE OF delivery SKIP LOCKED
            """,
            (_DELIVERY_MAX_ATTEMPTS, _CLAIM_LEASE_SECONDS, batch_limit),
        ).fetchall()
        for row in due:
            delivery_key = str(_row_value(row, "delivery_key"))
            if (
                _text_sha256(str(row["rendered_message"]))
                != str(_row_value(row, "rendered_message_sha256"))
                or str(row["rendered_message"]).splitlines()[-1]
                != EXPERIMENTAL_LABEL
            ):
                raise ExperimentalStoreIntegrityError(
                    "queued experimental Telegram message failed verification"
                )
            claim_token = secrets.token_hex(32)
            updated = conn.execute(
                """
                UPDATE public.research_formula_experimental_deliveries_v1
                   SET status='IN_FLIGHT', attempt_count=attempt_count+1,
                       claim_token=%s, claimed_at_utc=transaction_timestamp(),
                       claim_expires_at_utc=transaction_timestamp() +
                           (%s * INTERVAL '1 second'),
                       last_failure_kind=NULL, last_error=NULL
                 WHERE delivery_key=%s AND status IN ('PENDING','RETRYABLE')
                RETURNING attempt_count, claimed_at_utc, claim_expires_at_utc
                """,
                (claim_token, _CLAIM_LEASE_SECONDS, delivery_key),
            ).fetchone()
            if updated is None:
                raise ExperimentalStoreConflictError(
                    "experimental delivery disappeared while row-locked"
                )
            _, event = _attempt_event(
                delivery_key=delivery_key,
                attempt_number=int(updated["attempt_count"]),
                event_phase="CLAIMED",
                claim_token=claim_token,
            )
            _insert_attempt_event(conn, event)
            claimed.append(
                {
                    "delivery_key": delivery_key,
                    "alert_occurrence_id": str(
                        _row_value(row, "alert_occurrence_id")
                    ),
                    "chat_id": int(row["chat_id"]),
                    "attempt_count": int(updated["attempt_count"]),
                    "claim_token": claim_token,
                    "claimed_at_utc": updated["claimed_at_utc"],
                    "claim_expires_at_utc": updated["claim_expires_at_utc"],
                    "expires_at_utc": row["expires_at_utc"],
                    "rendered_message": row["rendered_message"],
                    "rendered_message_sha256": str(
                        _row_value(row, "rendered_message_sha256")
                    ),
                }
            )
        conn.commit()
    for row in claimed:
        if _text_sha256(row["rendered_message"]) != row["rendered_message_sha256"]:
            raise ExperimentalStoreIntegrityError(
                "claimed experimental Telegram message hash mismatch"
            )
        if row["rendered_message"].splitlines()[-1] != EXPERIMENTAL_LABEL:
            raise ExperimentalStoreIntegrityError(
                "claimed experimental Telegram message lacks disclaimer"
            )
    return claimed


def complete_delivery(
    delivery_key: str,
    claim_token: str,
    *,
    sent: bool,
    telegram_message_id: Optional[int] = None,
    error: Optional[str] = None,
    ambiguous: bool = False,
) -> Dict[str, Any]:
    """CAS one claimed send into SENT, definite failure, or AMBIGUOUS."""

    normalized_key = str(delivery_key or "").strip()
    normalized_token = str(claim_token or "").strip()
    if not _is_sha256(normalized_key) or not _is_sha256(normalized_token):
        raise ValueError("delivery key and claim token must be SHA-256 values")
    if sent and ambiguous:
        raise ValueError("a delivery cannot be both sent and ambiguous")
    if sent:
        if error is not None:
            raise ValueError("SENT completion cannot carry an error")
        if type(telegram_message_id) is not int or telegram_message_id <= 0:
            raise ValueError("a positive Telegram message id is required for SENT")
        terminal_status = "SENT"
        failure_kind = None
        normalized_error = None
        terminal_result = "SENT"
    else:
        if telegram_message_id is not None:
            raise ValueError("non-SENT completion cannot carry a Telegram message id")
        normalized_error = str(error or "unknown Telegram delivery failure")[:1000]
        if not normalized_error.strip():
            normalized_error = "unknown Telegram delivery failure"
        terminal_status = "AMBIGUOUS" if ambiguous else "DEFINITE_FAILURE"
        failure_kind = "AMBIGUOUS_SEND" if ambiguous else "DEFINITE_NOT_SENT"
        terminal_result = "AMBIGUOUS" if ambiguous else "DEFINITE_FAILURE"

    with _connect(read_only=False) as conn:
        row = conn.execute(
            """
            SELECT delivery.*, alert.expires_at_utc
              FROM public.research_formula_experimental_deliveries_v1 delivery
              JOIN public.research_formula_experimental_alerts_v1 alert
                ON alert.alert_occurrence_id=delivery.alert_occurrence_id
             WHERE delivery.delivery_key=%s
             FOR UPDATE OF delivery
            """,
            (normalized_key,),
        ).fetchone()
        if row is None or row.get("status") != "IN_FLIGHT" or str(
            _row_value(row, "claim_token") or ""
        ) != normalized_token:
            raise ExperimentalStoreConflictError(
                "experimental delivery claim no longer matches"
            )
        attempt_number = int(row["attempt_count"])
        if not sent and not ambiguous:
            clock_row = conn.execute(
                "SELECT clock_timestamp() AS database_now_utc"
            ).fetchone()
            if not clock_row or clock_row.get("database_now_utc") is None:
                raise ExperimentalStoreIntegrityError(
                    "database clock is unavailable for delivery completion"
                )
            database_now = _utc(
                clock_row["database_now_utc"], field_name="database_now_utc"
            )
            lease_expired = _utc(
                row["claim_expires_at_utc"],
                field_name="claim_expires_at_utc",
            ) <= database_now
            can_retry = (
                not lease_expired
                and attempt_number < _DELIVERY_MAX_ATTEMPTS
                and _utc(row["expires_at_utc"], field_name="expires_at_utc")
                > database_now
            )
            if lease_expired:
                terminal_status = "AMBIGUOUS"
                failure_kind = "AMBIGUOUS_SEND"
                terminal_result = "AMBIGUOUS"
                normalized_error = (
                    f"{normalized_error}; claim lease expired before completion, "
                    "so the Telegram result is ambiguous"
                )[:1000]
            elif can_retry:
                terminal_status = "RETRYABLE"
            else:
                terminal_status = "FAILED_FINAL"
                failure_kind = (
                    "ATTEMPTS_EXHAUSTED"
                    if attempt_number >= _DELIVERY_MAX_ATTEMPTS
                    else "DEFINITE_NOT_SENT"
                )

        if terminal_status == "SENT":
            updated = conn.execute(
                """
                UPDATE public.research_formula_experimental_deliveries_v1
                   SET status='SENT', sent_at_utc=transaction_timestamp(),
                       telegram_message_id=%s, last_failure_kind=NULL,
                       last_error=NULL
                 WHERE delivery_key=%s AND status='IN_FLIGHT'
                   AND claim_token=%s
                RETURNING *
                """,
                (int(telegram_message_id), normalized_key, normalized_token),
            ).fetchone()
        elif terminal_status == "RETRYABLE":
            delay = min(600, _RETRY_BASE_SECONDS * (2 ** (attempt_number - 1)))
            updated = conn.execute(
                """
                UPDATE public.research_formula_experimental_deliveries_v1
                   SET status='RETRYABLE',
                       available_at_utc=transaction_timestamp() +
                           (%s * INTERVAL '1 second'),
                       last_failure_kind='DEFINITE_NOT_SENT', last_error=%s
                 WHERE delivery_key=%s AND status='IN_FLIGHT'
                   AND claim_token=%s
                RETURNING *
                """,
                (delay, normalized_error, normalized_key, normalized_token),
            ).fetchone()
        else:
            updated = conn.execute(
                """
                UPDATE public.research_formula_experimental_deliveries_v1
                   SET status=%s, last_failure_kind=%s, last_error=%s
                 WHERE delivery_key=%s AND status='IN_FLIGHT'
                   AND claim_token=%s
                RETURNING *
                """,
                (
                    terminal_status,
                    failure_kind,
                    normalized_error,
                    normalized_key,
                    normalized_token,
                ),
            ).fetchone()
        if updated is None:
            raise ExperimentalStoreConflictError(
                "experimental delivery completion lost its claim"
            )
        _, event = _attempt_event(
            delivery_key=normalized_key,
            attempt_number=attempt_number,
            event_phase="TERMINAL",
            terminal_result=terminal_result,
            claim_token=normalized_token,
            telegram_message_id=(int(telegram_message_id) if sent else None),
            error_text=(None if sent else normalized_error),
        )
        _insert_attempt_event(conn, event)
        conn.commit()
    return {
        "delivery_key": normalized_key,
        "attempt_count": attempt_number,
        "status": terminal_status,
        "sent": terminal_status == "SENT",
        "ambiguous": terminal_status == "AMBIGUOUS",
    }


def delivery_status() -> Dict[str, Any]:
    """Return bounded operational counts without exposing message bodies."""

    with _connect(read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT status, COUNT(*) AS count
              FROM public.research_formula_experimental_deliveries_v1
             GROUP BY status ORDER BY status
            """
        ).fetchall()
    counts = {str(row["status"]): int(row["count"] or 0) for row in rows}
    return {
        "counts": counts,
        "pending_or_retryable": counts.get("PENDING", 0)
        + counts.get("RETRYABLE", 0),
        "stale_in_flight_policy": "AMBIGUOUS_NO_AUTOMATIC_RETRY",
        "delivery_scope": DELIVERY_SCOPE,
        "live_eligible": False,
        "trade_execution_allowed": False,
    }
