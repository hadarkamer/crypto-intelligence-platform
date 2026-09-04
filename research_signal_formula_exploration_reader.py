"""Authoritative read-only Stage-4/Wave-v5/outcome source adapter.

The pure exploration contract intentionally accepts ordinary mappings.  This
module is the database trust boundary: it reads only migration-024/025/026
interface views through one fixed reader identity, attests migrations
022/023/024/025/026 in the same repeatable-read snapshot, reconstructs every Wave
receipt, and only then hands rows to
:mod:`research_signal_formula_exploration`.

There is no Formula, delivery, LIVE, Telegram, or trading wiring in this
adapter.  Closed outcomes are labels only and are never exposed as decision
features.  A failed or incomplete attestation never returns partial data.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import os
import re
import struct
from typing import Any, Dict, Mapping, Optional, Sequence
from urllib.parse import parse_qsl, unquote, urlsplit

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - optional in pure/self-test environments
    psycopg = None
    dict_row = None

import research_market_movement as movement
import research_signal_formula_exploration as exploration


SOURCE_CONTRACT_VERSION = "stage4-wave-v5-authoritative-source-v1"
CORPUS_SOURCE_CONTRACT_VERSION = (
    "stage4-wave-v5-closed-outcome-authoritative-corpus-v2"
)
TRUSTED_READER_ROLE = "research_formula_exploration_reader_v1"
DATABASE_URL_ENV = "RESEARCH_FORMULA_EXPLORATION_DATABASE_URL"
STAGE4_VIEW = "public.research_formula_exploration_stage4_v1"
WAVE_VIEW = "public.research_formula_exploration_wave_v5_v1"
OUTCOME_VIEW = "public.research_formula_exploration_outcomes_v1"
OUTCOME_VIEW_CONTRACT_VERSION = "stage4-formula-exploration-outcomes-v1"
NO_SIGNAL_OUTCOME_VIEW = (
    "public.research_formula_exploration_no_signal_outcomes_v1"
)
NO_SIGNAL_OUTCOME_VIEW_CONTRACT_VERSION = (
    "stage4-formula-exploration-no-signal-outcomes-v1"
)
NO_SIGNAL_REFERENCE_HASH_CONTRACT_VERSION = (
    "stage4-no-signal-reference-receipt-hash-v1"
)
NO_SIGNAL_OUTCOME_HASH_CONTRACT_VERSION = (
    "stage4-no-signal-outcome-payload-hash-v1"
)
NO_SIGNAL_CARRIER_CONTRACT_VERSION = (
    "stage4-explicit-no-signal-outcome-carrier-v1"
)
NO_SIGNAL_REFERENCE_HASH_TAGS = (
    "hash_contract_version",
    "reference_contract_version",
    "projection_event_id",
    "projection_event_fingerprint",
    "snapshot_set_id",
    "snapshot_key",
    "set_payload_sha256",
    "symbol",
    "symbol_manifest_payload_sha256",
    "source_timeframe",
    "snapshot_row_id",
    "snapshot_row_payload_sha256",
    "official_price_float8_hex",
    "official_price_source",
    "official_price_exchange",
    "official_price_market",
    "official_price_pair",
    "official_price_instrument",
    "official_price_interval",
    "official_price_fetched_at_utc",
    "official_price_observed_at_utc",
    "official_price_candle_open_time_utc",
    "official_price_candle_close_time_utc",
    "official_price_policy_status",
)
NO_SIGNAL_OUTCOME_HASH_TAGS = (
    "hash_contract_version",
    "carrier_contract_version",
    "projection_event_id",
    "projection_event_fingerprint",
    "snapshot_set_id",
    "snapshot_key",
    "symbol",
    "direction",
    "horizon_minutes",
    "decision_time_utc",
    "absence_basis",
    "cell_identity_sha256",
    "reference_receipt_sha256",
    "measured_at_utc",
    "reference_price_float8_hex",
    "price_at_horizon_float8_hex",
    "raw_return_pct_float8_hex",
    "directional_return_pct_float8_hex",
    "max_favorable_price_float8_hex",
    "max_adverse_price_float8_hex",
    "mfe_pct_float8_hex",
    "mae_pct_float8_hex",
    "time_to_first_progress_seconds",
    "time_to_mfe_seconds",
    "path_resolution_seconds",
    "path_samples",
    "outcome_method_version",
    "price_source",
    "data_quality_status",
)
SCHEMA_LOCK_ID = 94837242

MAX_STAGE4_ROWS = 4096
MAX_WAVE_ROWS = 4096
MAX_PROJECTION_LIMIT = 128
MAX_LOOKBACK_DAYS = 365
MAX_CORPUS_STAGE4_ROWS = 32768
MAX_CORPUS_WAVE_ROWS = 32768
CANDIDATE_SEARCH_BLOCKERS: tuple[str, ...] = ()
CONNECTION_OPTIONS = (
    "-c default_transaction_read_only=on "
    "-c statement_timeout=20000 -c lock_timeout=2000 "
    "-c idle_in_transaction_session_timeout=30000 "
    "-c search_path=pg_catalog -c timezone=UTC -c row_security=off "
    "-c DateStyle=ISO,YMD -c IntervalStyle=postgres "
    "-c extra_float_digits=3 -c quote_all_identifiers=off"
)

_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_UTC = timezone.utc

EVENT_COLUMNS = (
    "event_id",
    "schema_version",
    "event_kind",
    "event_type",
    "alert_time_utc",
    "symbol",
    "direction",
    "source_side",
    "timeframe",
    "score",
    "current_price",
    "target_price",
    "initial_target_distance_pct",
    "categories",
    "setup_key",
    "event_fingerprint",
    "strategy_version",
    "code_version",
    "runtime_session_id",
    "capture_stage",
    "delivery_status",
    "delivery_attempted_at_utc",
    "delivered_at_utc",
    "engine_snapshot",
)

STAGE4_VIEW_COLUMNS = (
    *EVENT_COLUMNS,
    "event_created_at",
    "claimed_snapshot_set_id",
    "claimed_snapshot_key",
    "archive_snapshot_set_id",
    "archive_snapshot_key",
    "archive_payload_sha256",
    "archive_cycle_time_utc",
    "archive_available_at_utc",
    "archive_source",
    "archive_research_eligible",
    "archive_created_at_utc",
)

MEMBERSHIP_VIEW_COLUMNS = (
    "membership_receipt_sha256",
    "emitted_by_transition_receipt_sha256",
    "membership_contract_version",
    "membership_stream_id",
    "membership_movement_id",
    "membership_anchor_id",
    "membership_anchor_receipt_sha256",
    "membership_ordinal",
    "membership_classification",
    "membership_eligible_at_utc",
    "membership_decision_time_utc",
    "membership_price",
    "membership_receipt",
    "membership_created_at_utc",
)

TRANSITION_VIEW_COLUMNS = (
    "transition_receipt_sha256",
    "previous_transition_receipt_sha256",
    "transition_contract_version",
    "transition_chain_ordinal",
    "transition_type",
    "transition_stream_id",
    "transition_namespace",
    "transition_symbol",
    "transition_movement_id",
    "transition_trigger_anchor_id",
    "transition_trigger_eligible_at_utc",
    "transition_trigger_decision_time_utc",
    "transition_pre_state_sha256",
    "transition_post_state_sha256",
    "transition_post_state",
    "transition_receipt",
    "transition_created_at_utc",
)

ANCHOR_VIEW_COLUMNS = (
    "anchor_id",
    "anchor_receipt_sha256",
    "anchor_contract_version",
    "anchor_symbol",
    "anchor_origin",
    "anchor_sampler_version",
    "anchor_eligible_at_utc",
    "anchor_decision_time_utc",
    "anchor_source_price_candle_open_utc",
    "anchor_source_price_candle_close_utc",
    "anchor_observed_at_utc",
    "anchor_refresh_completed_at_utc",
    "anchor_price",
    "anchor_source",
    "anchor_upstream_source",
    "anchor_price_exchange",
    "anchor_price_market",
    "anchor_price_pair",
    "anchor_price_instrument_id",
    "anchor_price_timeframe",
    "anchor_quality_status",
    "anchor_fallback_used",
    "anchor_fallback_policy",
    "anchor_price_candle_identity_basis",
    "anchor_source_input_fingerprint",
    "anchor_source_record_created_at_utc",
    "anchor_receipt",
    "anchor_created_at_utc",
)

WAVE_VIEW_COLUMNS = (
    *MEMBERSHIP_VIEW_COLUMNS,
    *TRANSITION_VIEW_COLUMNS,
    *ANCHOR_VIEW_COLUMNS,
)

OUTCOME_VIEW_COLUMNS = (
    "event_id",
    "horizon_minutes",
    "measured_at_utc",
    "reference_price",
    "price_at_horizon",
    "raw_return_pct",
    "directional_return_pct",
    "max_favorable_price",
    "max_adverse_price",
    "mfe_pct",
    "mae_pct",
    "time_to_first_progress_seconds",
    "time_to_mfe_seconds",
    "path_resolution_seconds",
    "path_samples",
    "outcome_method_version",
    "price_source",
    "data_quality_status",
    "outcome_created_at",
)

OUTCOME_VIEW_COLUMN_TYPES = (
    "event_id|bigint",
    "horizon_minutes|integer",
    "measured_at_utc|timestamp with time zone",
    "reference_price|double precision",
    "price_at_horizon|double precision",
    "raw_return_pct|double precision",
    "directional_return_pct|double precision",
    "max_favorable_price|double precision",
    "max_adverse_price|double precision",
    "mfe_pct|double precision",
    "mae_pct|double precision",
    "time_to_first_progress_seconds|integer",
    "time_to_mfe_seconds|integer",
    "path_resolution_seconds|integer",
    "path_samples|integer",
    "outcome_method_version|text",
    "price_source|text",
    "data_quality_status|text",
    "outcome_created_at|timestamp with time zone",
)

NO_SIGNAL_OUTCOME_VIEW_COLUMNS = (
    "projection_event_id",
    "projection_event_fingerprint",
    "snapshot_set_id",
    "snapshot_key",
    "symbol",
    "direction",
    "horizon_minutes",
    "decision_time_utc",
    "absence_basis",
    "reference_receipt",
    "reference_receipt_sha256",
    "cell_identity_sha256",
    "measured_at_utc",
    "reference_price",
    "price_at_horizon",
    "raw_return_pct",
    "directional_return_pct",
    "max_favorable_price",
    "max_adverse_price",
    "mfe_pct",
    "mae_pct",
    "time_to_first_progress_seconds",
    "time_to_mfe_seconds",
    "path_resolution_seconds",
    "path_samples",
    "outcome_method_version",
    "price_source",
    "data_quality_status",
    "outcome_payload_sha256",
    "outcome_created_at",
)

NO_SIGNAL_OUTCOME_VIEW_COLUMN_TYPES = (
    "projection_event_id|bigint",
    "projection_event_fingerprint|character(64)",
    "snapshot_set_id|bigint",
    "snapshot_key|character(64)",
    "symbol|text",
    "direction|text",
    "horizon_minutes|integer",
    "decision_time_utc|timestamp with time zone",
    "absence_basis|text",
    "reference_receipt|jsonb",
    "reference_receipt_sha256|character(64)",
    "cell_identity_sha256|character(64)",
    "measured_at_utc|timestamp with time zone",
    "reference_price|double precision",
    "price_at_horizon|double precision",
    "raw_return_pct|double precision",
    "directional_return_pct|double precision",
    "max_favorable_price|double precision",
    "max_adverse_price|double precision",
    "mfe_pct|double precision",
    "mae_pct|double precision",
    "time_to_first_progress_seconds|integer",
    "time_to_mfe_seconds|integer",
    "path_resolution_seconds|integer",
    "path_samples|integer",
    "outcome_method_version|text",
    "price_source|text",
    "data_quality_status|text",
    "outcome_payload_sha256|character(64)",
    "outcome_created_at|timestamp with time zone",
)

# ``format_type``/``attnotnull`` signatures freeze the physical source
# boundary.  A view column list alone is insufficient: a replaced source
# column can retain the same view output name while changing coercion or null
# semantics underneath it.
SOURCE_TABLE_SHAPES = (
    (
        "research_events",
        "stage4",
        (
            "event_id|bigint|true",
            "schema_version|text|true",
            "event_kind|text|true",
            "event_type|text|true",
            "alert_time_utc|timestamp with time zone|true",
            "symbol|text|true",
            "direction|text|true",
            "source_side|text|false",
            "timeframe|text|false",
            "score|double precision|false",
            "current_price|double precision|false",
            "target_price|double precision|false",
            "initial_target_distance_pct|double precision|false",
            "categories|jsonb|true",
            "setup_key|character(64)|true",
            "event_fingerprint|character(64)|true",
            "strategy_version|text|true",
            "code_version|text|true",
            "runtime_session_id|text|true",
            "capture_stage|text|true",
            "delivery_status|text|true",
            "delivery_attempted_at_utc|timestamp with time zone|false",
            "delivered_at_utc|timestamp with time zone|false",
            "engine_snapshot|jsonb|true",
            "created_at|timestamp with time zone|true",
        ),
    ),
    (
        "research_max_pain_snapshot_sets",
        "stage4",
        (
            "snapshot_set_id|bigint|true",
            "snapshot_key|character(64)|true",
            "archive_schema_version|text|true",
            "method_version|text|true",
            "cutover_marker|text|true",
            "cutover_time_utc|timestamp with time zone|true",
            "cycle_id|text|true",
            "cycle_time_utc|timestamp with time zone|true",
            "collection_started_at_utc|timestamp with time zone|true",
            "collection_completed_at_utc|timestamp with time zone|true",
            "available_at_utc|timestamp with time zone|true",
            "source|text|true",
            "collector_version|text|true",
            "expected_timeframes|text[]|true",
            "expected_timeframe_count|integer|true",
            "observed_timeframe_count|integer|true",
            "observed_symbol_count|integer|true",
            "complete_symbol_count|integer|true",
            "incomplete_symbol_count|integer|true",
            "eligible_symbol_count|integer|true",
            "ineligible_symbol_count|integer|true",
            "row_count|integer|true",
            "invalid_row_count|integer|true",
            "missing_timeframes|text[]|true",
            "duplicate_pairs|text[]|true",
            "skipped_symbols|text[]|true",
            "collection_status|text|true",
            "validation_status|text|true",
            "freshness_status|text|true",
            "set_complete_7of7|boolean|true",
            "research_eligible|boolean|true",
            "completeness_report|jsonb|true",
            "validation_errors|jsonb|true",
            "source_metadata|jsonb|true",
            "payload_sha256|character(64)|true",
            "created_at_utc|timestamp with time zone|true",
        ),
    ),
    (
        "research_max_pain_snapshot_symbols",
        "stage4",
        (
            "snapshot_set_id|bigint|true",
            "symbol|text|true",
            "observed_timeframe_count|integer|true",
            "missing_timeframes|text[]|true",
            "duplicate_timeframes|text[]|true",
            "invalid_row_count|integer|true",
            "complete_7of7|boolean|true",
            "price_overlay_coherent|boolean|true",
            "validation_status|text|true",
            "freshness_status|text|true",
            "research_eligible|boolean|true",
            "validation_errors|jsonb|true",
            "payload_sha256|character(64)|true",
            "created_at_utc|timestamp with time zone|true",
        ),
    ),
    (
        "research_max_pain_snapshot_rows",
        "stage4",
        (
            "snapshot_row_id|bigint|true",
            "snapshot_set_id|bigint|true",
            "symbol|text|true",
            "timeframe|text|true",
            "rank|integer|false",
            "source_observed_at_utc|timestamp with time zone|false",
            "current_price|double precision|false",
            "coinglass_current_price|double precision|false",
            "price_source|text|false",
            "price_exchange|text|false",
            "price_market|text|false",
            "price_pair|text|false",
            "price_instrument|text|false",
            "price_fetched_at_utc|timestamp with time zone|false",
            "price_source_policy_status|text|true",
            "short_max_pain|double precision|false",
            "long_max_pain|double precision|false",
            "short_liquidation_amount|double precision|false",
            "long_liquidation_amount|double precision|false",
            "short_target_signed_distance_pct|double precision|false",
            "long_target_signed_distance_pct|double precision|false",
            "short_target_abs_distance_pct|double precision|false",
            "long_target_abs_distance_pct|double precision|false",
            "row_valid|boolean|true",
            "freshness_status|text|true",
            "validation_errors|jsonb|true",
            "raw_provenance|jsonb|true",
            "payload_sha256|character(64)|true",
            "created_at_utc|timestamp with time zone|true",
        ),
    ),
    (
        "research_price_collection_attempts",
        "wave",
        (
            "attempt_receipt_sha256|character(64)|true",
            "contract_version|text|true",
            "symbol|text|true",
            "eligible_at_utc|timestamp with time zone|true",
            "decision_time_utc|timestamp with time zone|true",
            "evaluation_status|text|true",
            "evaluation_reason|text|true",
            "anchor_id|character(64)|false",
            "anchor_receipt_sha256|character(64)|false",
            "attempt_receipt|jsonb|true",
            "created_at_utc|timestamp with time zone|true",
        ),
    ),
    (
        "research_neutral_price_anchors",
        "wave",
        (
            "anchor_id|character(64)|true",
            "anchor_receipt_sha256|character(64)|true",
            "contract_version|text|true",
            "symbol|text|true",
            "origin|text|true",
            "sampler_version|text|true",
            "eligible_at_utc|timestamp with time zone|true",
            "decision_time_utc|timestamp with time zone|true",
            "source_price_candle_open_utc|timestamp with time zone|true",
            "source_price_candle_close_utc|timestamp with time zone|true",
            "observed_at_utc|timestamp with time zone|true",
            "refresh_completed_at_utc|timestamp with time zone|true",
            "price|numeric|true",
            "source|text|true",
            "upstream_source|text|true",
            "price_exchange|text|true",
            "price_market|text|true",
            "price_pair|text|true",
            "price_instrument_id|text|true",
            "price_timeframe|text|true",
            "quality_status|text|true",
            "fallback_used|boolean|true",
            "fallback_policy|text|true",
            "price_candle_identity_basis|text|true",
            "source_input_fingerprint|character(64)|false",
            "source_record_created_at_utc|timestamp with time zone|false",
            "anchor_receipt|jsonb|true",
            "created_at_utc|timestamp with time zone|true",
        ),
    ),
    (
        "research_market_movement_transitions",
        "wave",
        (
            "transition_receipt_sha256|character(64)|true",
            "contract_version|text|true",
            "previous_transition_receipt_sha256|character(64)|false",
            "chain_ordinal|bigint|true",
            "transition_type|text|true",
            "stream_id|character(64)|true",
            "namespace|text|true",
            "symbol|text|true",
            "movement_id|character(64)|true",
            "trigger_anchor_id|character(64)|true",
            "trigger_eligible_at_utc|timestamp with time zone|true",
            "trigger_decision_time_utc|timestamp with time zone|true",
            "pre_state_sha256|character(64)|false",
            "post_state_sha256|character(64)|true",
            "post_state|jsonb|true",
            "transition_receipt|jsonb|true",
            "created_at_utc|timestamp with time zone|true",
        ),
    ),
    (
        "research_market_movement_memberships",
        "wave",
        (
            "membership_receipt_sha256|character(64)|true",
            "emitted_by_transition_receipt_sha256|character(64)|true",
            "contract_version|text|true",
            "stream_id|character(64)|true",
            "movement_id|character(64)|true",
            "anchor_id|character(64)|true",
            "anchor_receipt_sha256|character(64)|true",
            "ordinal|bigint|true",
            "classification|text|true",
            "eligible_at_utc|timestamp with time zone|true",
            "decision_time_utc|timestamp with time zone|true",
            "price|numeric|true",
            "membership_receipt|jsonb|true",
            "created_at_utc|timestamp with time zone|true",
        ),
    ),
)


def _sql_text_array(values: Sequence[str]) -> str:
    """Render only compile-time literals used by the catalog attestation."""

    return "ARRAY[" + ",".join("'" + value.replace("'", "''") + "'" for value in values) + "]::text[]"


_SOURCE_SHAPE_VALUES_SQL = ",\n".join(
    "(" + ",".join(
        (
            "'" + relation_name + "'::text",
            "'" + owner_group + "'::text",
            _sql_text_array(column_shape),
        )
    ) + ")"
    for relation_name, owner_group, column_shape in SOURCE_TABLE_SHAPES
)

_BEGIN_SQL = """
/* formula_exploration_reader:begin */
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY
"""

_SCHEMA_LOCK_SQL = """
/* formula_exploration_reader:schema_lock */
SELECT pg_catalog.pg_advisory_xact_lock_shared(%s)
"""

_SESSION_SQL = """
/* formula_exploration_reader:session */
SELECT pg_catalog.transaction_timestamp() AS analysis_as_of_utc,
       pg_catalog.pg_current_snapshot()::text AS database_snapshot_id,
       session_user::text AS session_user,
       current_user::text AS current_user,
       pg_catalog.current_database()::text AS database_name,
       pg_catalog.current_setting('transaction_isolation')::text
           AS transaction_isolation,
       pg_catalog.current_setting('transaction_read_only')::text
           AS transaction_read_only,
       pg_catalog.pg_is_in_recovery() AS in_recovery
"""

_PROBE_STAGE4_SQL = f"""
/* formula_exploration_reader:probe_stage4 */
SELECT * FROM {STAGE4_VIEW} LIMIT 0
"""

_PROBE_WAVE_SQL = f"""
/* formula_exploration_reader:probe_wave */
SELECT * FROM {WAVE_VIEW} LIMIT 0
"""

_PROBE_OUTCOMES_SQL = f"""
/* formula_exploration_reader:probe_outcomes */
SELECT * FROM {OUTCOME_VIEW} LIMIT 0
"""

_PROBE_NO_SIGNAL_OUTCOMES_SQL = f"""
/* formula_exploration_reader:probe_no_signal_outcomes */
SELECT * FROM {NO_SIGNAL_OUTCOME_VIEW} LIMIT 0
"""

# This is the SELECT-form twin of migration 024's ``$source_catalog_receipt$``
# payload.  Keep every field and ORDER BY identical: the installed hash is a
# receipt for the complete source boundary, not merely for object names.
_CATALOG_DEPARSE_GUCS_CTE = """
catalog_deparser_gucs_payload AS (
    SELECT pg_catalog.jsonb_build_object(
               'date_style', pg_catalog.current_setting('DateStyle'),
               'interval_style', pg_catalog.current_setting('IntervalStyle'),
               'extra_float_digits',
                   pg_catalog.current_setting('extra_float_digits'),
               'quote_all_identifiers',
                   pg_catalog.current_setting('quote_all_identifiers'),
               'search_path', pg_catalog.current_setting('search_path'),
               'time_zone', pg_catalog.current_setting('TimeZone')
           ) AS payload
)
"""

_CATALOG_ROLES_CTE = """
catalog_roles_payload AS (
    WITH RECURSIVE
    authority_roots(authority, role_oid) AS (
        VALUES
          ('stage4'::text, (
              SELECT relation_row.relowner
                FROM pg_catalog.pg_class relation_row
               WHERE relation_row.oid = pg_catalog.to_regclass(
                   'public.research_events'
               )
          )),
          ('wave_v5'::text, (
              SELECT role_row.oid
                FROM pg_catalog.pg_roles role_row
               WHERE role_row.rolname = 'research_market_movement_owner'
          ))
    ), authority_reachable(role_oid) AS (
        SELECT root.role_oid
          FROM authority_roots root
        UNION
        SELECT membership.member
          FROM authority_reachable reachable
          JOIN pg_catalog.pg_auth_members membership
            ON membership.roleid = reachable.role_oid
    ), required_role_ids(role_oid) AS (
        SELECT role_row.oid
          FROM pg_catalog.pg_roles role_row
         WHERE role_row.rolname IN (
             'research_signal_snapshot_writer_v1',
             'research_market_movement_owner',
             'research_market_movement_writer_v5',
             'research_formula_exploration_reader_v1'
         )
        UNION
        SELECT root.role_oid FROM authority_roots root
    ), authority_membership_edges AS (
        SELECT membership.roleid,
               membership.member,
               membership.grantor,
               membership.admin_option,
               COALESCE(
                   (pg_catalog.to_jsonb(membership) ->>
                       'inherit_option')::boolean,
                   true
               ) AS inherit_option,
               COALESCE(
                   (pg_catalog.to_jsonb(membership) ->>
                       'set_option')::boolean,
                   true
               ) AS set_option
          FROM pg_catalog.pg_auth_members membership
         WHERE membership.roleid IN (
             SELECT reachable.role_oid FROM authority_reachable reachable
         )
            OR membership.roleid IN (
                 SELECT required.role_oid FROM required_role_ids required
            )
            OR membership.member IN (
                 SELECT required.role_oid FROM required_role_ids required
            )
    ), graph_role_ids(role_oid) AS (
        SELECT required.role_oid FROM required_role_ids required
        UNION
        SELECT reachable.role_oid FROM authority_reachable reachable
        UNION
        SELECT edge.roleid FROM authority_membership_edges edge
        UNION
        SELECT edge.member FROM authority_membership_edges edge
        UNION
        SELECT edge.grantor FROM authority_membership_edges edge
    )
    SELECT pg_catalog.jsonb_build_object(
               'authority_roots', COALESCE((
                   SELECT pg_catalog.jsonb_agg(
                       pg_catalog.jsonb_build_object(
                           'authority', root.authority,
                           'role', pg_catalog.pg_get_userbyid(root.role_oid)
                       ) ORDER BY root.authority
                   )
                     FROM authority_roots root
               ), '[]'::jsonb),
               'required_roles', COALESCE((
                   SELECT pg_catalog.jsonb_agg(
                       pg_catalog.pg_get_userbyid(required.role_oid)
                       ORDER BY pg_catalog.pg_get_userbyid(required.role_oid)
                   )
                     FROM required_role_ids required
               ), '[]'::jsonb),
               'nodes', COALESCE((
                   SELECT pg_catalog.jsonb_agg(
                       pg_catalog.jsonb_build_object(
                           'name', role_row.rolname,
                           'can_login', role_row.rolcanlogin,
                           'inherit', role_row.rolinherit,
                           'superuser', role_row.rolsuper,
                           'create_db', role_row.rolcreatedb,
                           'create_role', role_row.rolcreaterole,
                           'replication', role_row.rolreplication,
                           'bypass_rls', role_row.rolbypassrls
                       ) ORDER BY role_row.rolname
                   )
                     FROM graph_role_ids graph_role
                     JOIN pg_catalog.pg_roles role_row
                       ON role_row.oid = graph_role.role_oid
               ), '[]'::jsonb),
               'membership_edges', COALESCE((
                   SELECT pg_catalog.jsonb_agg(
                       pg_catalog.jsonb_build_object(
                           'role', pg_catalog.pg_get_userbyid(edge.roleid),
                           'member', pg_catalog.pg_get_userbyid(edge.member),
                           'grantor', pg_catalog.pg_get_userbyid(edge.grantor),
                           'admin_option', edge.admin_option,
                           'inherit_option', edge.inherit_option,
                           'set_option', edge.set_option
                       ) ORDER BY
                           pg_catalog.pg_get_userbyid(edge.roleid),
                           pg_catalog.pg_get_userbyid(edge.member),
                           pg_catalog.pg_get_userbyid(edge.grantor)
                   )
                     FROM authority_membership_edges edge
               ), '[]'::jsonb)
           ) AS payload
)
"""

_CATALOG_SCHEMA_CTE = """
catalog_schema_payload AS (
    SELECT pg_catalog.jsonb_build_object(
               'name', namespace_row.nspname,
               'owner', pg_catalog.pg_get_userbyid(namespace_row.nspowner),
               'acl', COALESCE((
                   SELECT pg_catalog.jsonb_agg(
                       pg_catalog.jsonb_build_object(
                           'grantee', CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                                           ELSE pg_catalog.pg_get_userbyid(
                                               acl.grantee
                                           ) END,
                           'privilege', acl.privilege_type,
                           'grantable', acl.is_grantable
                       ) ORDER BY
                           CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                                ELSE pg_catalog.pg_get_userbyid(acl.grantee)
                           END,
                           acl.privilege_type
                   )
                     FROM pg_catalog.aclexplode(COALESCE(
                         namespace_row.nspacl,
                         pg_catalog.acldefault('n', namespace_row.nspowner)
                     )) acl
                    -- Migration 026 owns and independently attests this one
                    -- delegated schema ACL.  Excluding only its exact
                    -- non-grantable shape preserves migration 024's frozen
                    -- source receipt; every other ACL change stays hashed.
                    WHERE NOT (
                        acl.grantee <> 0
                        AND pg_catalog.pg_get_userbyid(acl.grantee) =
                            'research_stage4_no_signal_outcome_writer_v1'
                        AND acl.privilege_type = 'USAGE'
                        AND NOT acl.is_grantable
                    )
               ), '[]'::jsonb)
           ) AS payload
      FROM pg_catalog.pg_namespace namespace_row
     WHERE namespace_row.nspname = 'public'
)
"""

_CATALOG_RELATIONS_CTE = """
catalog_relations_payload AS (
    SELECT COALESCE(pg_catalog.jsonb_agg(entry ORDER BY relation_name),
                    '[]'::jsonb) AS payload
      FROM (
          SELECT relation_row.relname::text AS relation_name,
                 pg_catalog.jsonb_build_object(
                     'schema', relation_namespace.nspname,
                     'name', relation_row.relname,
                     'owner', pg_catalog.pg_get_userbyid(relation_row.relowner),
                     'kind', relation_row.relkind,
                     'persistence', relation_row.relpersistence,
                     'is_partition', relation_row.relispartition,
                     'rls', relation_row.relrowsecurity,
                     'force_rls', relation_row.relforcerowsecurity,
                     'columns', COALESCE((
                         SELECT pg_catalog.jsonb_agg(
                             pg_catalog.jsonb_build_object(
                                 'name', attribute.attname,
                                 'type', pg_catalog.format_type(
                                     attribute.atttypid, attribute.atttypmod
                                 ),
                                 'not_null', attribute.attnotnull,
                                 'collation', CASE
                                     WHEN attribute.attcollation = 0 THEN NULL
                                     ELSE attribute.attcollation::regcollation::text
                                 END,
                                 'identity', attribute.attidentity,
                                 'generated', attribute.attgenerated,
                                 'default', pg_catalog.pg_get_expr(
                                     default_value.adbin, default_value.adrelid
                                 ),
                                 'acl', COALESCE((
                                     SELECT pg_catalog.jsonb_agg(
                                         pg_catalog.jsonb_build_object(
                                             'grantee', CASE
                                                 WHEN column_acl.grantee = 0
                                                     THEN 'PUBLIC'
                                                 ELSE pg_catalog.pg_get_userbyid(
                                                     column_acl.grantee
                                                 ) END,
                                             'privilege',
                                                 column_acl.privilege_type,
                                             'grantable', column_acl.is_grantable
                                         ) ORDER BY
                                             CASE WHEN column_acl.grantee = 0
                                                 THEN 'PUBLIC'
                                                 ELSE pg_catalog.pg_get_userbyid(
                                                     column_acl.grantee
                                                 ) END,
                                             column_acl.privilege_type
                                     )
                                       FROM pg_catalog.aclexplode(
                                           attribute.attacl
                                       ) column_acl
                                 ), '[]'::jsonb)
                             ) ORDER BY attribute.attnum
                         )
                           FROM pg_catalog.pg_attribute attribute
                           LEFT JOIN pg_catalog.pg_attrdef default_value
                             ON default_value.adrelid = attribute.attrelid
                            AND default_value.adnum = attribute.attnum
                          WHERE attribute.attrelid = relation_row.oid
                            AND attribute.attnum > 0
                            AND NOT attribute.attisdropped
                     ), '[]'::jsonb),
                     'acl', COALESCE((
                         SELECT pg_catalog.jsonb_agg(
                             pg_catalog.jsonb_build_object(
                                 'grantee', CASE WHEN acl.grantee = 0
                                                   THEN 'PUBLIC'
                                               ELSE pg_catalog.pg_get_userbyid(
                                                   acl.grantee
                                               ) END,
                                 'privilege', acl.privilege_type,
                                 'grantable', acl.is_grantable
                             ) ORDER BY
                                 CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                                      ELSE pg_catalog.pg_get_userbyid(
                                          acl.grantee
                                      ) END,
                                 acl.privilege_type
                         )
                           FROM pg_catalog.aclexplode(COALESCE(
                               relation_row.relacl,
                               pg_catalog.acldefault(
                                   'r', relation_row.relowner
                               )
                           )) acl
                          -- Migration 026 separately attests these four exact
                          -- non-grantable SELECT entries.  They post-date the
                          -- migration 024 receipt and are the only normalized
                          -- relation ACL entries here.
                          WHERE NOT (
                              acl.grantee <> 0
                              AND pg_catalog.pg_get_userbyid(acl.grantee) =
                                  'research_stage4_no_signal_outcome_writer_v1'
                              AND relation_row.relname IN (
                                  'research_events',
                                  'research_max_pain_snapshot_sets',
                                  'research_max_pain_snapshot_symbols',
                                  'research_max_pain_snapshot_rows'
                              )
                              AND acl.privilege_type = 'SELECT'
                              AND NOT acl.is_grantable
                          )
                     ), '[]'::jsonb),
                     'policies', COALESCE((
                         SELECT pg_catalog.jsonb_agg(
                             pg_catalog.jsonb_build_object(
                                 'name', policy_row.polname,
                                 'command', policy_row.polcmd,
                                 'permissive', policy_row.polpermissive,
                                 'roles', COALESCE((
                                     SELECT pg_catalog.jsonb_agg(
                                         pg_catalog.pg_get_userbyid(role_id)
                                         ORDER BY pg_catalog.pg_get_userbyid(
                                             role_id
                                         )
                                     )
                                       FROM pg_catalog.unnest(
                                           policy_row.polroles
                                       ) role_id
                                 ), '[]'::jsonb),
                                 'using', pg_catalog.pg_get_expr(
                                     policy_row.polqual,
                                     policy_row.polrelid,
                                     false
                                 ),
                                 'check', pg_catalog.pg_get_expr(
                                     policy_row.polwithcheck,
                                     policy_row.polrelid,
                                     false
                                 )
                             ) ORDER BY policy_row.polname
                         )
                           FROM pg_catalog.pg_policy policy_row
                          WHERE policy_row.polrelid = relation_row.oid
                     ), '[]'::jsonb),
                     'rules', COALESCE((
                         SELECT pg_catalog.jsonb_agg(
                             pg_catalog.jsonb_build_object(
                                 'name', rule_row.rulename,
                                 'event', rule_row.ev_type,
                                 'enabled', rule_row.ev_enabled,
                                 'instead', rule_row.is_instead,
                                 'definition', pg_catalog.pg_get_ruledef(
                                     rule_row.oid, false
                                 )
                             ) ORDER BY rule_row.rulename
                         )
                           FROM pg_catalog.pg_rewrite rule_row
                          WHERE rule_row.ev_class = relation_row.oid
                     ), '[]'::jsonb)
                 ) AS entry
            FROM pg_catalog.pg_class relation_row
            JOIN pg_catalog.pg_namespace relation_namespace
              ON relation_namespace.oid = relation_row.relnamespace
           WHERE relation_namespace.nspname = 'public'
             AND relation_row.relname IN (
                 'research_events',
                 'research_max_pain_snapshot_sets',
                 'research_max_pain_snapshot_symbols',
                 'research_max_pain_snapshot_rows',
                 'research_price_collection_attempts',
                 'research_neutral_price_anchors',
                 'research_market_movement_transitions',
                 'research_market_movement_memberships'
             )
      ) ordered_relations
)
"""

_CATALOG_SEQUENCES_CTE = """
catalog_sequences_payload AS (
    SELECT COALESCE(pg_catalog.jsonb_agg(entry ORDER BY sequence_name),
                    '[]'::jsonb) AS payload
      FROM (
          SELECT sequence_row.relname::text AS sequence_name,
                 pg_catalog.jsonb_build_object(
                     'schema', sequence_namespace.nspname,
                     'name', sequence_row.relname,
                     'owner', pg_catalog.pg_get_userbyid(sequence_row.relowner),
                     'persistence', sequence_row.relpersistence,
                     'acl', COALESCE((
                         SELECT pg_catalog.jsonb_agg(
                             pg_catalog.jsonb_build_object(
                                 'grantee', CASE WHEN acl.grantee = 0
                                                   THEN 'PUBLIC'
                                               ELSE pg_catalog.pg_get_userbyid(
                                                   acl.grantee
                                               ) END,
                                 'privilege', acl.privilege_type,
                                 'grantable', acl.is_grantable
                             ) ORDER BY
                                 CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                                      ELSE pg_catalog.pg_get_userbyid(
                                          acl.grantee
                                      ) END,
                                 acl.privilege_type
                         )
                           FROM pg_catalog.aclexplode(COALESCE(
                               sequence_row.relacl,
                               pg_catalog.acldefault(
                                   's', sequence_row.relowner
                               )
                           )) acl
                     ), '[]'::jsonb)
                 ) AS entry
            FROM pg_catalog.pg_class sequence_row
            JOIN pg_catalog.pg_namespace sequence_namespace
              ON sequence_namespace.oid = sequence_row.relnamespace
           WHERE sequence_namespace.nspname = 'public'
             AND sequence_row.relkind = 'S'
             AND sequence_row.relname IN (
                 'research_events_event_id_seq',
                 'research_max_pain_snapshot_sets_snapshot_set_id_seq',
                 'research_max_pain_snapshot_rows_snapshot_row_id_seq'
             )
      ) ordered_sequences
)
"""

_CATALOG_CONSTRAINTS_CTE = """
catalog_constraints_payload AS (
    SELECT COALESCE(pg_catalog.jsonb_agg(
               entry ORDER BY relation_name, definition
           ), '[]'::jsonb) AS payload
      FROM (
          SELECT relation_row.relname::text AS relation_name,
                 pg_catalog.pg_get_constraintdef(
                     constraint_row.oid, false
                 ) AS definition,
                 pg_catalog.jsonb_build_object(
                     'relation', relation_row.relname,
                     'type', constraint_row.contype,
                     'local_columns', COALESCE((
                         SELECT pg_catalog.jsonb_agg(
                             attribute.attname ORDER BY key_column.ordinality
                         )
                           FROM pg_catalog.unnest(constraint_row.conkey)
                                WITH ORDINALITY AS key_column(attnum, ordinality)
                           JOIN pg_catalog.pg_attribute attribute
                             ON attribute.attrelid = constraint_row.conrelid
                            AND attribute.attnum = key_column.attnum
                     ), '[]'::jsonb),
                     'reference_relation', CASE
                         WHEN constraint_row.confrelid = 0 THEN NULL
                         ELSE reference_namespace.nspname || '.' ||
                              reference_relation.relname
                     END,
                     'reference_columns', COALESCE((
                         SELECT pg_catalog.jsonb_agg(
                             attribute.attname ORDER BY key_column.ordinality
                         )
                           FROM pg_catalog.unnest(constraint_row.confkey)
                                WITH ORDINALITY AS key_column(attnum, ordinality)
                           JOIN pg_catalog.pg_attribute attribute
                             ON attribute.attrelid = constraint_row.confrelid
                            AND attribute.attnum = key_column.attnum
                     ), '[]'::jsonb),
                     'deferrable', constraint_row.condeferrable,
                     'deferred', constraint_row.condeferred,
                     'validated', constraint_row.convalidated,
                     'local', constraint_row.conislocal,
                     'inherit_count', constraint_row.coninhcount,
                     'no_inherit', constraint_row.connoinherit,
                     'definition', pg_catalog.pg_get_constraintdef(
                         constraint_row.oid, false
                     )
                 ) AS entry
            FROM pg_catalog.pg_constraint constraint_row
            JOIN pg_catalog.pg_class relation_row
              ON relation_row.oid = constraint_row.conrelid
            JOIN pg_catalog.pg_namespace relation_namespace
              ON relation_namespace.oid = relation_row.relnamespace
            LEFT JOIN pg_catalog.pg_class reference_relation
              ON reference_relation.oid = constraint_row.confrelid
            LEFT JOIN pg_catalog.pg_namespace reference_namespace
              ON reference_namespace.oid = reference_relation.relnamespace
           WHERE relation_namespace.nspname = 'public'
             AND relation_row.relname IN (
                 'research_events',
                 'research_max_pain_snapshot_sets',
                 'research_max_pain_snapshot_symbols',
                 'research_max_pain_snapshot_rows',
                 'research_price_collection_attempts',
                 'research_neutral_price_anchors',
                 'research_market_movement_transitions',
                 'research_market_movement_memberships'
             )
      ) ordered_constraints
)
"""

_CATALOG_INDEXES_CTE = """
catalog_indexes_payload AS (
    SELECT COALESCE(pg_catalog.jsonb_agg(
               entry ORDER BY relation_name, index_name
           ), '[]'::jsonb) AS payload
      FROM (
          SELECT relation_row.relname::text AS relation_name,
                 index_relation.relname::text AS index_name,
                 pg_catalog.jsonb_build_object(
                     'relation', relation_row.relname,
                     'name', index_relation.relname,
                     'owner', pg_catalog.pg_get_userbyid(
                         index_relation.relowner
                     ),
                     'access_method', access_method.amname,
                     'unique', index_row.indisunique,
                     'primary', index_row.indisprimary,
                     'valid', index_row.indisvalid,
                     'ready', index_row.indisready,
                     'live', index_row.indislive,
                     'immediate', index_row.indimmediate,
                     'definition', pg_catalog.pg_get_indexdef(
                         index_row.indexrelid
                     ),
                     'predicate', pg_catalog.pg_get_expr(
                         index_row.indpred, index_row.indrelid, false
                     )
                 ) AS entry
            FROM pg_catalog.pg_index index_row
            JOIN pg_catalog.pg_class relation_row
              ON relation_row.oid = index_row.indrelid
            JOIN pg_catalog.pg_namespace relation_namespace
              ON relation_namespace.oid = relation_row.relnamespace
            JOIN pg_catalog.pg_class index_relation
              ON index_relation.oid = index_row.indexrelid
            JOIN pg_catalog.pg_am access_method
              ON access_method.oid = index_relation.relam
           WHERE relation_namespace.nspname = 'public'
             AND relation_row.relname IN (
                 'research_events',
                 'research_max_pain_snapshot_sets',
                 'research_max_pain_snapshot_symbols',
                 'research_max_pain_snapshot_rows',
                 'research_price_collection_attempts',
                 'research_neutral_price_anchors',
                 'research_market_movement_transitions',
                 'research_market_movement_memberships'
             )
      ) ordered_indexes
)
"""

_CATALOG_TRIGGERS_CTE = """
catalog_triggers_payload AS (
    SELECT COALESCE(pg_catalog.jsonb_agg(
               entry ORDER BY relation_name, trigger_name
           ), '[]'::jsonb) AS payload
      FROM (
          SELECT relation_row.relname::text AS relation_name,
                 trigger_row.tgname::text AS trigger_name,
                 pg_catalog.jsonb_build_object(
                     'relation', relation_row.relname,
                     'name', trigger_row.tgname,
                     'enabled', trigger_row.tgenabled,
                     'type', trigger_row.tgtype,
                     'deferrable', trigger_row.tgdeferrable,
                     'deferred', trigger_row.tginitdeferred,
                     'constraint_trigger', trigger_row.tgconstraint <> 0,
                     'when', pg_catalog.pg_get_expr(
                         trigger_row.tgqual, trigger_row.tgrelid, false
                     ),
                     'definition', pg_catalog.pg_get_triggerdef(
                         trigger_row.oid, false
                     ),
                     'function_schema', function_namespace.nspname,
                     'function_name', function_row.proname,
                     'function_owner', pg_catalog.pg_get_userbyid(
                         function_row.proowner
                     ),
                     'function_security_definer', function_row.prosecdef,
                     'function_config', COALESCE(
                         pg_catalog.to_jsonb(function_row.proconfig),
                         '[]'::jsonb
                     )
                 ) AS entry
            FROM pg_catalog.pg_trigger trigger_row
            JOIN pg_catalog.pg_class relation_row
              ON relation_row.oid = trigger_row.tgrelid
            JOIN pg_catalog.pg_namespace relation_namespace
              ON relation_namespace.oid = relation_row.relnamespace
            JOIN pg_catalog.pg_proc function_row
              ON function_row.oid = trigger_row.tgfoid
            JOIN pg_catalog.pg_namespace function_namespace
              ON function_namespace.oid = function_row.pronamespace
           WHERE relation_namespace.nspname = 'public'
             AND NOT trigger_row.tgisinternal
             AND relation_row.relname IN (
                 'research_events',
                 'research_max_pain_snapshot_sets',
                 'research_max_pain_snapshot_symbols',
                 'research_max_pain_snapshot_rows',
                 'research_price_collection_attempts',
                 'research_neutral_price_anchors',
                 'research_market_movement_transitions',
                 'research_market_movement_memberships'
             )
      ) ordered_triggers
)
"""

_CATALOG_FUNCTIONS_CTE = """
catalog_functions_payload AS (
    SELECT COALESCE(pg_catalog.jsonb_agg(
               entry ORDER BY function_name, identity_args
           ), '[]'::jsonb) AS payload
      FROM (
          SELECT function_row.proname::text AS function_name,
                 pg_catalog.pg_get_function_identity_arguments(
                     function_row.oid
                 ) AS identity_args,
                 pg_catalog.jsonb_build_object(
                     'schema', function_namespace.nspname,
                     'name', function_row.proname,
                     'identity_arguments',
                         pg_catalog.pg_get_function_identity_arguments(
                             function_row.oid
                         ),
                     'result', pg_catalog.pg_get_function_result(
                         function_row.oid
                     ),
                     'owner', pg_catalog.pg_get_userbyid(function_row.proowner),
                     'language', language_row.lanname,
                     'security_definer', function_row.prosecdef,
                     'volatility', function_row.provolatile,
                     'strict', function_row.proisstrict,
                     'parallel', function_row.proparallel,
                     'config', COALESCE(
                         pg_catalog.to_jsonb(function_row.proconfig),
                         '[]'::jsonb
                     ),
                     'acl', COALESCE((
                         SELECT pg_catalog.jsonb_agg(
                             pg_catalog.jsonb_build_object(
                                 'grantee', CASE WHEN acl.grantee = 0
                                                   THEN 'PUBLIC'
                                               ELSE pg_catalog.pg_get_userbyid(
                                                   acl.grantee
                                               ) END,
                                 'privilege', acl.privilege_type,
                                 'grantable', acl.is_grantable
                             ) ORDER BY
                                 CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                                      ELSE pg_catalog.pg_get_userbyid(
                                          acl.grantee
                                      ) END,
                                 acl.privilege_type
                         )
                           FROM pg_catalog.aclexplode(COALESCE(
                               function_row.proacl,
                               pg_catalog.acldefault(
                                   'f', function_row.proowner
                               )
                           )) acl
                     ), '[]'::jsonb),
                     'definition', pg_catalog.pg_get_functiondef(
                         function_row.oid
                     )
                 ) AS entry
            FROM pg_catalog.pg_proc function_row
            JOIN pg_catalog.pg_namespace function_namespace
              ON function_namespace.oid = function_row.pronamespace
            JOIN pg_catalog.pg_language language_row
              ON language_row.oid = function_row.prolang
           WHERE function_namespace.nspname = 'public'
             AND function_row.proname = ANY(ARRAY[
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
                 'prevent_research_signal_snapshot_v1_truncate',
                 'assert_research_max_pain_snapshot_complete',
                 'prevent_research_max_pain_archive_mutation',
                 'assert_market_movement_writer_v5',
                 'prevent_market_movement_archive_mutation',
                 'assert_neutral_price_attempt_anchor_complete',
                 'validate_market_movement_transition_insert',
                 'assert_market_movement_receipt_complete'
             ]::name[])
      ) ordered_functions
)
"""

_CATALOG_VIEWS_CTE = """
catalog_views_payload AS (
    SELECT COALESCE(pg_catalog.jsonb_agg(entry ORDER BY view_name),
                    '[]'::jsonb) AS payload
      FROM (
          SELECT view_row.relname::text AS view_name,
                 pg_catalog.jsonb_build_object(
                     'schema', view_namespace.nspname,
                     'name', view_row.relname,
                     'owner', pg_catalog.pg_get_userbyid(view_row.relowner),
                     'kind', view_row.relkind,
                     'options', COALESCE((
                         SELECT pg_catalog.jsonb_agg(
                             option_value ORDER BY option_value
                         )
                           FROM pg_catalog.unnest(
                               view_row.reloptions
                           ) option_value
                     ), '[]'::jsonb),
                     'columns', COALESCE((
                         SELECT pg_catalog.jsonb_agg(
                             pg_catalog.jsonb_build_object(
                                 'name', attribute.attname,
                                 'type', pg_catalog.format_type(
                                     attribute.atttypid, attribute.atttypmod
                                 )
                             ) ORDER BY attribute.attnum
                         )
                           FROM pg_catalog.pg_attribute attribute
                          WHERE attribute.attrelid = view_row.oid
                            AND attribute.attnum > 0
                            AND NOT attribute.attisdropped
                     ), '[]'::jsonb),
                     'acl', COALESCE((
                         SELECT pg_catalog.jsonb_agg(
                             pg_catalog.jsonb_build_object(
                                 'grantee', CASE WHEN acl.grantee = 0
                                                   THEN 'PUBLIC'
                                               ELSE pg_catalog.pg_get_userbyid(
                                                   acl.grantee
                                               ) END,
                                 'privilege', acl.privilege_type,
                                 'grantable', acl.is_grantable
                             ) ORDER BY
                                 CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                                      ELSE pg_catalog.pg_get_userbyid(
                                          acl.grantee
                                      ) END,
                                 acl.privilege_type
                         )
                           FROM pg_catalog.aclexplode(COALESCE(
                               view_row.relacl,
                               pg_catalog.acldefault('r', view_row.relowner)
                           )) acl
                     ), '[]'::jsonb),
                     'definition', pg_catalog.pg_get_viewdef(
                         view_row.oid, false
                     ),
                     'dependencies', COALESCE((
                         SELECT pg_catalog.jsonb_agg(
                             DISTINCT dependency_namespace.nspname || '.' ||
                                      dependency_relation.relname
                             ORDER BY dependency_namespace.nspname || '.' ||
                                      dependency_relation.relname
                         )
                           FROM pg_catalog.pg_rewrite rule_row
                           JOIN pg_catalog.pg_depend dependency_row
                             ON dependency_row.classid =
                                'pg_catalog.pg_rewrite'::regclass
                            AND dependency_row.objid = rule_row.oid
                           JOIN pg_catalog.pg_class dependency_relation
                             ON dependency_relation.oid = dependency_row.refobjid
                           JOIN pg_catalog.pg_namespace dependency_namespace
                             ON dependency_namespace.oid =
                                dependency_relation.relnamespace
                          WHERE rule_row.ev_class = view_row.oid
                            AND rule_row.rulename = '_RETURN'
                            AND dependency_relation.oid <> view_row.oid
                            AND dependency_relation.relkind IN ('r', 'p')
                     ), '[]'::jsonb)
                 ) AS entry
            FROM pg_catalog.pg_class view_row
            JOIN pg_catalog.pg_namespace view_namespace
              ON view_namespace.oid = view_row.relnamespace
           WHERE view_namespace.nspname = 'public'
             AND view_row.relname IN (
                 'research_formula_exploration_stage4_v1',
                 'research_formula_exploration_wave_v5_v1'
             )
      ) ordered_views
)
"""

_CATALOG_RECEIPT_CTES = f"""
catalog_payload AS (
    SELECT pg_catalog.jsonb_build_object(
               'contract_version', '{SOURCE_CONTRACT_VERSION}',
               'deparser_gucs', catalog_deparser_gucs_payload.payload,
               'roles', catalog_roles_payload.payload,
               'schema', catalog_schema_payload.payload,
               'relations', catalog_relations_payload.payload,
               'sequences', catalog_sequences_payload.payload,
               'constraints', catalog_constraints_payload.payload,
               'indexes', catalog_indexes_payload.payload,
               'triggers', catalog_triggers_payload.payload,
               'functions', catalog_functions_payload.payload,
               'views', catalog_views_payload.payload
           ) AS payload
      FROM catalog_deparser_gucs_payload
      CROSS JOIN catalog_roles_payload
      CROSS JOIN catalog_schema_payload
      CROSS JOIN catalog_relations_payload
      CROSS JOIN catalog_sequences_payload
      CROSS JOIN catalog_constraints_payload
      CROSS JOIN catalog_indexes_payload
      CROSS JOIN catalog_triggers_payload
      CROSS JOIN catalog_functions_payload
      CROSS JOIN catalog_views_payload
), catalog_receipt_status AS (
    SELECT digest.source_catalog_sha256,
           digest.source_catalog_sha256 ~ '^[0-9a-f]{{64}}$'
           AND catalog_deparser_gucs_payload.payload =
               pg_catalog.jsonb_build_object(
                   'date_style', 'ISO, YMD',
                   'interval_style', 'postgres',
                   'extra_float_digits', '3',
                   'quote_all_identifiers', 'off',
                   'search_path', 'pg_catalog',
                   'time_zone', 'UTC'
               )
           AND pg_catalog.jsonb_typeof(
                   catalog_roles_payload.payload) = 'object'
           AND pg_catalog.jsonb_array_length(
                   catalog_roles_payload.payload -> 'authority_roots') = 2
           AND pg_catalog.jsonb_array_length(
                   catalog_roles_payload.payload -> 'required_roles') = 5
           AND pg_catalog.jsonb_array_length(
                   catalog_roles_payload.payload -> 'nodes') >= 5
           AND pg_catalog.jsonb_typeof(
                   catalog_roles_payload.payload -> 'membership_edges') =
               'array'
           AND pg_catalog.jsonb_array_length(
                   catalog_relations_payload.payload) = 8
           AND pg_catalog.jsonb_array_length(
                   catalog_sequences_payload.payload) = 3
           AND pg_catalog.jsonb_array_length(
                   catalog_functions_payload.payload) = 28
           AND pg_catalog.jsonb_array_length(
                   catalog_views_payload.payload) = 2
           AND digest.source_catalog_sha256 IS NOT DISTINCT FROM
               pg_catalog.substring(
                   pg_catalog.obj_description(
                       pg_catalog.to_regclass(
                           'public.research_formula_exploration_stage4_v1'
                       ),
                       'pg_class'
                   ),
                   'source_catalog_sha256=([0-9a-f]{{64}})$'
               )
           AND digest.source_catalog_sha256 IS NOT DISTINCT FROM
               pg_catalog.substring(
                   pg_catalog.obj_description(
                       pg_catalog.to_regclass(
                           'public.research_formula_exploration_wave_v5_v1'
                       ),
                       'pg_class'
                   ),
                   'source_catalog_sha256=([0-9a-f]{{64}})$'
               ) AS ready
      FROM catalog_deparser_gucs_payload
      CROSS JOIN catalog_roles_payload
      CROSS JOIN catalog_relations_payload
      CROSS JOIN catalog_sequences_payload
      CROSS JOIN catalog_functions_payload
      CROSS JOIN catalog_views_payload
      CROSS JOIN LATERAL (
          SELECT pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                     catalog_payload.payload::text, 'UTF8'
                 )), 'hex') AS source_catalog_sha256
            FROM catalog_payload
      ) digest
)
"""

_ATTESTATION_SQL = f"""
/* formula_exploration_reader:attestation */
WITH expected_views(view_name, expected_columns, expected_owner_source,
                    expected_comment) AS (
    VALUES
      (
        'research_formula_exploration_stage4_v1'::text,
        ARRAY[{','.join(repr(value) for value in STAGE4_VIEW_COLUMNS)}]::text[],
        'research_events'::text,
        '{SOURCE_CONTRACT_VERSION}; read-only Stage-4 cohort source; no Formula, outcome, delivery, LIVE or trading authority'::text
      ),
      (
        'research_formula_exploration_wave_v5_v1'::text,
        ARRAY[{','.join(repr(value) for value in WAVE_VIEW_COLUMNS)}]::text[],
        'research_market_movement_memberships'::text,
        '{SOURCE_CONTRACT_VERSION}; read-only Wave-v5 grouping source; no Formula, outcome, delivery, LIVE or trading authority'::text
      )
), view_status AS (
    SELECT expected.view_name,
           relation.oid,
           relation.relowner,
           source.relowner AS expected_owner,
           relation.relkind = 'v'
           AND relation.relowner = source.relowner
           AND COALESCE(relation.reloptions, ARRAY[]::text[])
                   @> ARRAY['security_barrier=true', 'security_invoker=false']::text[]
           AND pg_catalog.cardinality(relation.reloptions) = 2
           AND pg_catalog.obj_description(relation.oid, 'pg_class')
                   IS NOT DISTINCT FROM expected.expected_comment
                       || '; view_definition_sha256=' || COALESCE((
                            SELECT pg_catalog.encode(pg_catalog.sha256(
                                pg_catalog.convert_to(pg_catalog.pg_get_viewdef(
                                    rewrite_rule.ev_class, false
                                ), 'UTF8')
                            ), 'hex')
                              FROM pg_catalog.pg_rewrite rewrite_rule
                             WHERE rewrite_rule.ev_class = relation.oid
                               AND rewrite_rule.rulename = '_RETURN'
                       ), '')
                       || '; source_catalog_sha256=' || COALESCE(
                            pg_catalog.substring(
                                pg_catalog.obj_description(
                                    relation.oid, 'pg_class'
                                ),
                                'source_catalog_sha256=([0-9a-f]{{64}})$'
                            ),
                            ''
                       )
           AND COALESCE((
                SELECT pg_catalog.array_agg(attribute.attname::text
                                            ORDER BY attribute.attnum)
                  FROM pg_catalog.pg_attribute attribute
                 WHERE attribute.attrelid = relation.oid
                   AND attribute.attnum > 0
                   AND NOT attribute.attisdropped
           ), ARRAY[]::text[]) = expected.expected_columns
           AND pg_catalog.has_table_privilege(
                   '{TRUSTED_READER_ROLE}', relation.oid, 'SELECT')
           AND NOT pg_catalog.has_table_privilege(
                   '{TRUSTED_READER_ROLE}', relation.oid,
                   'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
           AND NOT EXISTS (
                SELECT 1
                  FROM pg_catalog.aclexplode(COALESCE(
                       relation.relacl,
                       pg_catalog.acldefault('r', relation.relowner)
                  )) acl
                 WHERE acl.grantee <> relation.relowner
                   AND NOT (
                        acl.grantee = (
                            SELECT role_row.oid FROM pg_catalog.pg_roles role_row
                             WHERE role_row.rolname = '{TRUSTED_READER_ROLE}'
                        )
                        AND acl.privilege_type = 'SELECT'
                        AND NOT acl.is_grantable
                   )
           )
           AND NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_attribute attribute
                 WHERE attribute.attrelid = relation.oid
                   AND attribute.attnum > 0
                   AND NOT attribute.attisdropped
                   AND COALESCE(pg_catalog.cardinality(attribute.attacl), 0) <> 0
           ) AS ready
      FROM expected_views expected
      LEFT JOIN pg_catalog.pg_class relation
        ON relation.oid = pg_catalog.to_regclass(
             'public.' || expected.view_name)
      LEFT JOIN pg_catalog.pg_class source
        ON source.oid = pg_catalog.to_regclass(
             'public.' || expected.expected_owner_source)
), expected_sources(relation_name, owner_group, expected_columns) AS (
    VALUES {_SOURCE_SHAPE_VALUES_SQL}
), source_relation_status AS (
    SELECT COALESCE(bool_and(
               source_ready.ready
           ) FILTER (WHERE expected.owner_group = 'stage4'), false)
               AS stage4_ready,
           COALESCE(bool_and(
               source_ready.ready
           ) FILTER (WHERE expected.owner_group = 'wave'), false)
               AS wave_ready
      FROM expected_sources expected
      LEFT JOIN pg_catalog.pg_class relation
        ON relation.oid = pg_catalog.to_regclass(
             'public.' || expected.relation_name)
      LEFT JOIN pg_catalog.pg_namespace relation_namespace
        ON relation_namespace.oid = relation.relnamespace
      CROSS JOIN LATERAL (
          SELECT COALESCE(
              relation_namespace.nspname = 'public'
              AND relation.relkind = 'r'
              AND relation.relpersistence = 'p'
              AND NOT relation.relispartition
              AND relation.relowner = CASE expected.owner_group
                  WHEN 'stage4' THEN (
                      SELECT event_relation.relowner
                        FROM pg_catalog.pg_class event_relation
                       WHERE event_relation.oid = pg_catalog.to_regclass(
                           'public.research_events'
                       )
                  )
                  WHEN 'wave' THEN (
                      SELECT role_row.oid
                        FROM pg_catalog.pg_roles role_row
                       WHERE role_row.rolname =
                           'research_market_movement_owner'
                  )
              END
              AND NOT EXISTS (
                  SELECT 1 FROM pg_catalog.pg_inherits inheritance
                   WHERE inheritance.inhrelid = relation.oid
                      OR inheritance.inhparent = relation.oid
              )
              AND COALESCE((
                  SELECT pg_catalog.array_agg(
                      attribute.attname::text || '|' ||
                      pg_catalog.format_type(
                          attribute.atttypid, attribute.atttypmod
                      ) || '|' || attribute.attnotnull::text
                      ORDER BY attribute.attnum
                  )
                    FROM pg_catalog.pg_attribute attribute
                   WHERE attribute.attrelid = relation.oid
                     AND attribute.attnum > 0
                     AND NOT attribute.attisdropped
              ), ARRAY[]::text[]) = expected.expected_columns
              AND NOT EXISTS (
                  SELECT 1 FROM pg_catalog.pg_attribute attribute
                   WHERE attribute.attrelid = relation.oid
                     AND attribute.attnum > 0
                     AND NOT attribute.attisdropped
                     AND COALESCE(
                         pg_catalog.cardinality(attribute.attacl), 0
                     ) <> 0
              ),
              false
          ) AS ready
      ) source_ready
), stage4_trigger_status AS (
    SELECT COUNT(*) = 5
           AND bool_and(
                trigger_row.tgenabled = 'A'
                AND trigger_row.tgqual IS NULL
                AND trigger_row.tgtype::integer = CASE trigger_row.tgname
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
                AND function_namespace.nspname = 'public'
                AND function_row.proowner = event_relation.relowner
                AND NOT function_row.prosecdef
                AND function_row.proconfig IS NOT DISTINCT FROM
                    ARRAY['search_path=pg_catalog, public']::text[]
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
           AND NOT EXISTS (
                SELECT 1
                  FROM pg_catalog.pg_trigger unexpected
                 WHERE unexpected.tgrelid = pg_catalog.to_regclass(
                           'public.research_events'
                       )
                   AND NOT unexpected.tgisinternal
                   AND (unexpected.tgtype::integer & 4) = 4
                   AND unexpected.tgname <> ALL(ARRAY[
                        'trg_research_signal_snapshot_v1_writer',
                        'trg_research_signal_snapshot_v1_envelope',
                        'trg_research_signal_snapshot_v1_set_complete',
                        'trg_research_signal_snapshot_v1_immutable',
                        'trg_research_signal_snapshot_v1_no_truncate'
                   ]::name[])
           ) AS ready
      FROM pg_catalog.pg_trigger trigger_row
      JOIN pg_catalog.pg_proc function_row ON function_row.oid = trigger_row.tgfoid
      JOIN pg_catalog.pg_namespace function_namespace
        ON function_namespace.oid = function_row.pronamespace
      CROSS JOIN pg_catalog.pg_class event_relation
     WHERE trigger_row.tgrelid = pg_catalog.to_regclass('public.research_events')
       AND event_relation.oid = pg_catalog.to_regclass('public.research_events')
       AND NOT trigger_row.tgisinternal
       AND trigger_row.tgname = ANY(ARRAY[
            'trg_research_signal_snapshot_v1_writer',
            'trg_research_signal_snapshot_v1_envelope',
            'trg_research_signal_snapshot_v1_set_complete',
            'trg_research_signal_snapshot_v1_immutable',
            'trg_research_signal_snapshot_v1_no_truncate'
       ]::name[])
), stage4_index_status AS (
    SELECT COUNT(*) = 2
           AND bool_and(
                index_row.indisvalid
                AND index_row.indisready
                AND index_row.indnatts = 1
                AND index_row.indnkeyatts = 1
                AND CASE index_relation.relname
                    WHEN 'uq_research_signal_snapshot_projection_key_v1' THEN
                        index_row.indisunique
                        AND pg_catalog.pg_get_indexdef(index_row.indexrelid)
                            LIKE '%{{projection,snapshot_key}}%'
                        AND pg_catalog.pg_get_indexdef(index_row.indexrelid)
                            LIKE '%SIGNAL_SNAPSHOT_PROJECTION%'
                    WHEN 'idx_research_signal_snapshot_archive_key_v1' THEN
                        NOT index_row.indisunique
                        AND pg_catalog.pg_get_indexdef(index_row.indexrelid)
                            LIKE '%{{signal_snapshot,archive_reference,snapshot_key}}%'
                        AND pg_catalog.pg_get_indexdef(index_row.indexrelid)
                            LIKE '%SILENT_COMBINED_CONFIRMATION_SNAPSHOT%'
                END
           ) AS ready
      FROM pg_catalog.pg_index index_row
      JOIN pg_catalog.pg_class index_relation
        ON index_relation.oid = index_row.indexrelid
     WHERE index_row.indrelid = pg_catalog.to_regclass('public.research_events')
       AND index_relation.relname = ANY(ARRAY[
            'uq_research_signal_snapshot_projection_key_v1',
            'idx_research_signal_snapshot_archive_key_v1'
       ]::name[])
), stage4_function_status AS (
    SELECT COUNT(*) = 21
           AND bool_and(
                function_row.proowner = event_relation.relowner
                AND NOT function_row.prosecdef
                AND function_row.proconfig IS NOT DISTINCT FROM
                    ARRAY['search_path=pg_catalog, public']::text[]
                AND pg_catalog.pg_get_functiondef(function_row.oid) IS NOT NULL
           ) AS ready
      FROM pg_catalog.pg_proc function_row
      JOIN pg_catalog.pg_namespace function_namespace
        ON function_namespace.oid = function_row.pronamespace
      CROSS JOIN pg_catalog.pg_class event_relation
     WHERE event_relation.oid = pg_catalog.to_regclass('public.research_events')
       AND function_namespace.nspname = 'public'
       AND function_row.proname = ANY(ARRAY[
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
       ]::name[])
), wave_relation_status AS (
    SELECT COUNT(*) = 4
           AND bool_and(relation.relkind = 'r')
           AND bool_and(owner_role.rolname = 'research_market_movement_owner')
           AND bool_and(NOT relation.relrowsecurity
                        AND NOT relation.relforcerowsecurity) AS ready
      FROM pg_catalog.pg_class relation
      JOIN pg_catalog.pg_roles owner_role ON owner_role.oid = relation.relowner
     WHERE relation.oid = ANY(ARRAY[
          pg_catalog.to_regclass('public.research_price_collection_attempts'),
          pg_catalog.to_regclass('public.research_neutral_price_anchors'),
          pg_catalog.to_regclass('public.research_market_movement_transitions'),
          pg_catalog.to_regclass('public.research_market_movement_memberships')
     ]::oid[])
), wave_trigger_status AS (
    SELECT COUNT(*) = 17
           AND bool_and(
                trigger_row.tgenabled = 'A'
                AND trigger_row.tgqual IS NULL
                AND trigger_row.tgtype::integer = CASE
                    WHEN trigger_row.tgname LIKE '%_append_only' THEN 27
                    WHEN trigger_row.tgname LIKE '%_no_truncate' THEN 34
                    WHEN trigger_row.tgname = ANY(ARRAY[
                        'trg_neutral_price_attempt_complete',
                        'trg_neutral_price_anchor_complete',
                        'trg_market_movement_transition_complete',
                        'trg_market_movement_membership_complete'
                    ]::name[]) THEN 5
                    ELSE 7
                END
                AND trigger_row.tgdeferrable = (
                    trigger_row.tgname = ANY(ARRAY[
                        'trg_neutral_price_attempt_complete',
                        'trg_neutral_price_anchor_complete',
                        'trg_market_movement_transition_complete',
                        'trg_market_movement_membership_complete'
                    ]::name[])
                )
                AND trigger_row.tginitdeferred = trigger_row.tgdeferrable
                AND (trigger_row.tgconstraint <> 0) = trigger_row.tgdeferrable
                AND function_namespace.nspname = 'public'
                AND owner_role.rolname = 'research_market_movement_owner'
                AND function_row.proowner = owner_role.oid
                AND function_row.proconfig IS NOT DISTINCT FROM
                    ARRAY['search_path=pg_catalog']::text[]
                AND function_row.proname = CASE
                    WHEN trigger_row.tgname LIKE '%_append_only'
                      OR trigger_row.tgname LIKE '%_no_truncate'
                        THEN 'prevent_market_movement_archive_mutation'
                    WHEN trigger_row.tgname LIKE '%_writer'
                        THEN 'assert_market_movement_writer_v5'
                    WHEN trigger_row.tgname = ANY(ARRAY[
                        'trg_neutral_price_attempt_complete',
                        'trg_neutral_price_anchor_complete'
                    ]::name[]) THEN 'assert_neutral_price_attempt_anchor_complete'
                    WHEN trigger_row.tgname =
                        'trg_validate_market_movement_transition_insert'
                        THEN 'validate_market_movement_transition_insert'
                    ELSE 'assert_market_movement_receipt_complete'
                END
                AND function_row.prosecdef = (
                    function_row.proname = ANY(ARRAY[
                        'assert_neutral_price_attempt_anchor_complete',
                        'validate_market_movement_transition_insert',
                        'assert_market_movement_receipt_complete'
                    ]::name[])
                )
           ) AS ready
      FROM pg_catalog.pg_trigger trigger_row
      JOIN pg_catalog.pg_proc function_row ON function_row.oid = trigger_row.tgfoid
      JOIN pg_catalog.pg_namespace function_namespace
        ON function_namespace.oid = function_row.pronamespace
      JOIN pg_catalog.pg_roles owner_role ON owner_role.oid = function_row.proowner
     WHERE trigger_row.tgrelid = ANY(ARRAY[
          pg_catalog.to_regclass('public.research_price_collection_attempts'),
          pg_catalog.to_regclass('public.research_neutral_price_anchors'),
          pg_catalog.to_regclass('public.research_market_movement_transitions'),
          pg_catalog.to_regclass('public.research_market_movement_memberships')
     ]::oid[])
       AND NOT trigger_row.tgisinternal
       AND trigger_row.tgname = ANY(ARRAY[
          'trg_market_movement_attempt_writer',
          'trg_neutral_price_attempt_complete',
          'trg_research_price_collection_attempts_append_only',
          'trg_research_price_collection_attempts_no_truncate',
          'trg_market_movement_anchor_writer',
          'trg_neutral_price_anchor_complete',
          'trg_research_neutral_price_anchors_append_only',
          'trg_research_neutral_price_anchors_no_truncate',
          'trg_market_movement_transition_complete',
          'trg_market_movement_transition_writer',
          'trg_research_market_movement_transitions_append_only',
          'trg_research_market_movement_transitions_no_truncate',
          'trg_validate_market_movement_transition_insert',
          'trg_market_movement_membership_complete',
          'trg_market_movement_membership_writer',
          'trg_research_market_movement_memberships_append_only',
          'trg_research_market_movement_memberships_no_truncate'
       ]::name[])
), wave_function_status AS (
    SELECT COUNT(*) = 5
           AND bool_and(
                owner_role.rolname = 'research_market_movement_owner'
                AND function_row.proconfig IS NOT DISTINCT FROM
                    ARRAY['search_path=pg_catalog']::text[]
                AND function_row.prosecdef = (
                    function_row.proname = ANY(ARRAY[
                        'assert_neutral_price_attempt_anchor_complete',
                        'validate_market_movement_transition_insert',
                        'assert_market_movement_receipt_complete'
                    ]::name[])
                )
                AND pg_catalog.pg_get_functiondef(function_row.oid) IS NOT NULL
           ) AS ready
      FROM pg_catalog.pg_proc function_row
      JOIN pg_catalog.pg_namespace function_namespace
        ON function_namespace.oid = function_row.pronamespace
      JOIN pg_catalog.pg_roles owner_role ON owner_role.oid = function_row.proowner
     WHERE function_namespace.nspname = 'public'
       AND function_row.proname = ANY(ARRAY[
            'assert_market_movement_writer_v5',
            'prevent_market_movement_archive_mutation',
            'assert_neutral_price_attempt_anchor_complete',
            'validate_market_movement_transition_insert',
            'assert_market_movement_receipt_complete'
       ]::name[])
), expected_wave_constraint_counts(
    relation_name, expected_checks, expected_identity_constraints
) AS (
    VALUES
      ('research_price_collection_attempts'::text, 13::bigint, 2::bigint),
      ('research_neutral_price_anchors'::text, 27::bigint, 4::bigint),
      ('research_market_movement_transitions'::text, 22::bigint, 5::bigint),
      ('research_market_movement_memberships'::text, 10::bigint, 6::bigint)
), wave_constraint_status AS (
    SELECT COUNT(*) = 4
           AND bool_and(
               actual.check_count = expected.expected_checks
               AND actual.identity_count =
                   expected.expected_identity_constraints
               AND actual.total_count = expected.expected_checks +
                   expected.expected_identity_constraints
               AND actual.all_exact
           ) AS ready
      FROM expected_wave_constraint_counts expected
      CROSS JOIN LATERAL (
          SELECT COUNT(*) AS total_count,
                 COUNT(*) FILTER (
                     WHERE constraint_row.contype = 'c'
                 ) AS check_count,
                 COUNT(*) FILTER (
                     WHERE constraint_row.contype IN ('p', 'u', 'f')
                 ) AS identity_count,
                 COALESCE(bool_and(
                     constraint_row.convalidated
                     AND constraint_row.conislocal
                     AND constraint_row.coninhcount = 0
                     AND pg_catalog.pg_get_constraintdef(
                         constraint_row.oid, false
                     ) IS NOT NULL
                 ), false) AS all_exact
            FROM pg_catalog.pg_constraint constraint_row
           WHERE constraint_row.conrelid = pg_catalog.to_regclass(
               'public.' || expected.relation_name
           )
             AND constraint_row.contype IN ('c', 'p', 'u', 'f')
      ) actual
), stage4_constraint_status AS (
    SELECT COUNT(*) = 4
           AND bool_and(actual.constraint_count > 0 AND actual.all_exact)
               AS ready
      FROM (VALUES
          ('research_events'::text),
          ('research_max_pain_snapshot_sets'::text),
          ('research_max_pain_snapshot_symbols'::text),
          ('research_max_pain_snapshot_rows'::text)
      ) expected(relation_name)
      CROSS JOIN LATERAL (
          SELECT COUNT(*) AS constraint_count,
                 COALESCE(bool_and(
                     constraint_row.convalidated
                     AND constraint_row.conislocal
                     AND constraint_row.coninhcount = 0
                     AND pg_catalog.pg_get_constraintdef(
                         constraint_row.oid, false
                     ) IS NOT NULL
                 ), false) AS all_exact
            FROM pg_catalog.pg_constraint constraint_row
           WHERE constraint_row.conrelid = pg_catalog.to_regclass(
               'public.' || expected.relation_name
           )
      ) actual
), source_visibility AS (
    SELECT NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class relation
         WHERE relation.oid = ANY(ARRAY[
              pg_catalog.to_regclass('public.research_events'),
              pg_catalog.to_regclass('public.research_max_pain_snapshot_sets'),
              pg_catalog.to_regclass('public.research_max_pain_snapshot_symbols'),
              pg_catalog.to_regclass('public.research_max_pain_snapshot_rows'),
              pg_catalog.to_regclass('public.research_price_collection_attempts'),
              pg_catalog.to_regclass('public.research_neutral_price_anchors'),
              pg_catalog.to_regclass('public.research_market_movement_transitions'),
              pg_catalog.to_regclass('public.research_market_movement_memberships')
         ]::oid[])
           AND (relation.relrowsecurity OR relation.relforcerowsecurity)
    ) AND NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_policy policy_row
         WHERE policy_row.polrelid = ANY(ARRAY[
              pg_catalog.to_regclass('public.research_events'),
              pg_catalog.to_regclass('public.research_max_pain_snapshot_sets'),
              pg_catalog.to_regclass('public.research_max_pain_snapshot_symbols'),
              pg_catalog.to_regclass('public.research_max_pain_snapshot_rows'),
              pg_catalog.to_regclass('public.research_price_collection_attempts'),
              pg_catalog.to_regclass('public.research_neutral_price_anchors'),
              pg_catalog.to_regclass('public.research_market_movement_transitions'),
              pg_catalog.to_regclass('public.research_market_movement_memberships')
         ]::oid[])
    ) AND NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_rewrite rule_row
         WHERE rule_row.ev_class = ANY(ARRAY[
              pg_catalog.to_regclass('public.research_events'),
              pg_catalog.to_regclass('public.research_max_pain_snapshot_sets'),
              pg_catalog.to_regclass('public.research_max_pain_snapshot_symbols'),
              pg_catalog.to_regclass('public.research_max_pain_snapshot_rows'),
              pg_catalog.to_regclass('public.research_price_collection_attempts'),
              pg_catalog.to_regclass('public.research_neutral_price_anchors'),
              pg_catalog.to_regclass('public.research_market_movement_transitions'),
              pg_catalog.to_regclass('public.research_market_movement_memberships')
         ]::oid[])
           AND rule_row.rulename <> '_RETURN'
    ) AS ready
), reader_role_status AS (
    SELECT role_row.rolcanlogin
           AND NOT role_row.rolinherit
           AND NOT role_row.rolsuper
           AND NOT role_row.rolcreatedb
           AND NOT role_row.rolcreaterole
           AND NOT role_row.rolreplication
           AND NOT role_row.rolbypassrls
           AND NOT pg_catalog.has_database_privilege(
                   role_row.rolname, pg_catalog.current_database(), 'CREATE')
           AND NOT pg_catalog.has_schema_privilege(
                   role_row.rolname, 'public', 'CREATE')
           AND pg_catalog.has_schema_privilege(
                   role_row.rolname, 'public', 'USAGE')
           AND NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_database database_row
                 WHERE database_row.datname = pg_catalog.current_database()
                   AND database_row.datdba = role_row.oid
           )
           AND NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_namespace namespace_row
                 WHERE namespace_row.nspname = 'public'
                   AND namespace_row.nspowner = role_row.oid
           )
           AND NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_class relation
                 WHERE relation.oid = ANY(ARRAY[
                      pg_catalog.to_regclass('public.research_events'),
                      pg_catalog.to_regclass(
                          'public.research_max_pain_snapshot_sets'),
                      pg_catalog.to_regclass(
                          'public.research_max_pain_snapshot_symbols'),
                      pg_catalog.to_regclass(
                          'public.research_max_pain_snapshot_rows'),
                      pg_catalog.to_regclass(
                          'public.research_price_collection_attempts'),
                      pg_catalog.to_regclass(
                          'public.research_neutral_price_anchors'),
                      pg_catalog.to_regclass(
                          'public.research_market_movement_transitions'),
                      pg_catalog.to_regclass(
                          'public.research_market_movement_memberships'),
                      pg_catalog.to_regclass(
                          'public.research_events_event_id_seq'),
                      pg_catalog.to_regclass(
                          'public.research_max_pain_snapshot_sets_snapshot_set_id_seq'),
                      pg_catalog.to_regclass(
                          'public.research_max_pain_snapshot_rows_snapshot_row_id_seq'),
                      pg_catalog.to_regclass(
                          'public.research_formula_exploration_stage4_v1'),
                      pg_catalog.to_regclass(
                          'public.research_formula_exploration_wave_v5_v1')
                 ]::oid[])
                   AND relation.relowner = role_row.oid
           )
           AND NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_auth_members membership
                 WHERE membership.member = role_row.oid
                    OR membership.roleid = role_row.oid
           ) AS ready
      FROM pg_catalog.pg_roles role_row
     WHERE role_row.rolname = '{TRUSTED_READER_ROLE}'
), raw_access_status AS (
    SELECT NOT EXISTS (
        SELECT 1
          FROM unnest(ARRAY[
              'public.research_events',
              'public.research_max_pain_snapshot_sets',
              'public.research_max_pain_snapshot_symbols',
              'public.research_max_pain_snapshot_rows',
              'public.research_price_collection_attempts',
              'public.research_neutral_price_anchors',
              'public.research_market_movement_transitions',
              'public.research_market_movement_memberships'
          ]::text[]) relation_name
         WHERE pg_catalog.has_table_privilege(
             '{TRUSTED_READER_ROLE}', relation_name,
             'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
         )
            OR pg_catalog.has_any_column_privilege(
             '{TRUSTED_READER_ROLE}', relation_name,
             'SELECT,INSERT,UPDATE,REFERENCES'
         )
    ) AND NOT EXISTS (
        SELECT 1
          FROM unnest(ARRAY[
              'public.research_events_event_id_seq',
              'public.research_max_pain_snapshot_sets_snapshot_set_id_seq',
              'public.research_max_pain_snapshot_rows_snapshot_row_id_seq'
          ]::text[]) sequence_name
         WHERE pg_catalog.to_regclass(sequence_name) IS NULL
            OR pg_catalog.has_sequence_privilege(
                 '{TRUSTED_READER_ROLE}', sequence_name,
                 'USAGE,SELECT,UPDATE'
            )
    ) AS ready
),
{_CATALOG_DEPARSE_GUCS_CTE},
{_CATALOG_ROLES_CTE},
{_CATALOG_SCHEMA_CTE},
{_CATALOG_RELATIONS_CTE},
{_CATALOG_SEQUENCES_CTE},
{_CATALOG_CONSTRAINTS_CTE},
{_CATALOG_INDEXES_CTE},
{_CATALOG_TRIGGERS_CTE},
{_CATALOG_FUNCTIONS_CTE},
{_CATALOG_VIEWS_CTE},
{_CATALOG_RECEIPT_CTES}
SELECT COALESCE((SELECT ready FROM reader_role_status), false)
           AS reader_role_ready,
       COALESCE((SELECT ready FROM wave_relation_status), false)
       AND COALESCE((SELECT ready FROM wave_trigger_status), false)
       AND COALESCE((SELECT ready FROM wave_function_status), false)
       AND COALESCE((SELECT ready FROM wave_constraint_status), false)
       AND COALESCE((SELECT wave_ready FROM source_relation_status), false)
       AND COALESCE((SELECT ready FROM catalog_receipt_status), false)
           AS migration_022_attested,
       pg_catalog.to_regclass('public.research_events') IS NOT NULL
       AND pg_catalog.to_regclass(
               'public.research_max_pain_snapshot_sets') IS NOT NULL
       AND COALESCE((SELECT ready FROM stage4_trigger_status), false)
       AND COALESCE((SELECT ready FROM stage4_index_status), false)
       AND COALESCE((SELECT ready FROM stage4_function_status), false)
       AND COALESCE((SELECT ready FROM stage4_constraint_status), false)
       AND COALESCE((SELECT stage4_ready FROM source_relation_status), false)
       AND COALESCE((SELECT ready FROM catalog_receipt_status), false)
       AND pg_catalog.to_regprocedure(
               'public.assert_research_signal_snapshot_v1_set_complete(text)')
               IS NOT NULL
       AND pg_catalog.to_regprocedure(
               'public.research_signal_snapshot_v1_expected_fingerprint(public.research_events)')
               IS NOT NULL
           AS migration_023_attested,
       COALESCE((SELECT ready FROM view_status
                  WHERE view_name = 'research_formula_exploration_stage4_v1'),
                false)
       AND COALESCE((SELECT ready FROM source_visibility), false)
       AND COALESCE((SELECT ready FROM catalog_receipt_status), false)
           AS stage4_view_attested,
       COALESCE((SELECT ready FROM view_status
                  WHERE view_name = 'research_formula_exploration_wave_v5_v1'),
                false)
       AND COALESCE((SELECT ready FROM source_visibility), false)
       AND COALESCE((SELECT ready FROM catalog_receipt_status), false)
           AS wave_view_attested,
       COALESCE((SELECT ready FROM raw_access_status), false)
           AS raw_access_absent,
       (SELECT source_catalog_sha256 FROM catalog_receipt_status)
           AS source_catalog_sha256
"""

_LOAD_STAGE4_SQL = f"""
/* formula_exploration_reader:load_stage4 */
SELECT {', '.join(STAGE4_VIEW_COLUMNS)}
  FROM {STAGE4_VIEW}
 WHERE claimed_snapshot_key = %s
 ORDER BY CASE WHEN event_type = 'SIGNAL_SNAPSHOT_PROJECTION' THEN 0 ELSE 1 END,
          event_fingerprint, event_id
 LIMIT %s
"""

_LOAD_WAVE_SQL = f"""
/* formula_exploration_reader:load_wave */
SELECT {', '.join(WAVE_VIEW_COLUMNS)}
  FROM {WAVE_VIEW}
 WHERE membership_eligible_at_utc = %s
 ORDER BY membership_receipt_sha256, transition_receipt_sha256, anchor_id
 LIMIT %s
"""

# Migration 025 intentionally leaves method, quality and timestamps exposed
# rather than filtering them in SQL.  This runtime check binds the view to its
# exact catalog surface before any outcome row can be consumed.
_OUTCOMES_ATTESTATION_SQL = f"""
/* formula_exploration_reader:outcomes_attestation */
WITH outcome_view AS (
    SELECT relation.oid, relation.relowner, relation.relkind,
           relation.reloptions, relation.relacl,
           pg_catalog.obj_description(relation.oid, 'pg_class') AS comment,
           (
               SELECT pg_catalog.encode(pg_catalog.sha256(
                   pg_catalog.convert_to(pg_catalog.pg_get_viewdef(
                       rewrite_rule.ev_class, false
                   ), 'UTF8')
               ), 'hex')
                 FROM pg_catalog.pg_rewrite rewrite_rule
                WHERE rewrite_rule.ev_class = relation.oid
                  AND rewrite_rule.rulename = '_RETURN'
           ) AS actual_definition_sha256
      FROM pg_catalog.pg_class relation
     WHERE relation.oid = pg_catalog.to_regclass('{OUTCOME_VIEW}')
), outcome_columns AS (
    SELECT COALESCE(pg_catalog.array_agg(
               attribute.attname::text || '|' || pg_catalog.format_type(
                   attribute.atttypid, attribute.atttypmod
               ) ORDER BY attribute.attnum
           ), ARRAY[]::text[]) AS shape,
           COALESCE(pg_catalog.bool_and(
               COALESCE(pg_catalog.cardinality(attribute.attacl), 0) = 0
           ), true) AS column_acl_absent
      FROM outcome_view target
      JOIN pg_catalog.pg_attribute attribute
        ON attribute.attrelid = target.oid
       AND attribute.attnum > 0
       AND NOT attribute.attisdropped
), outcome_dependencies AS (
    SELECT COALESCE(pg_catalog.array_agg(
               dependency_name ORDER BY dependency_name
           ), ARRAY[]::text[]) AS relation_names
      FROM (
          SELECT DISTINCT
                 referenced_namespace.nspname::text || '.' ||
                 referenced.relname::text AS dependency_name
            FROM outcome_view target
            JOIN pg_catalog.pg_rewrite rewrite_rule
              ON rewrite_rule.ev_class = target.oid
             AND rewrite_rule.rulename = '_RETURN'
            JOIN pg_catalog.pg_depend dependency
              ON dependency.classid = 'pg_catalog.pg_rewrite'::regclass
             AND dependency.objid = rewrite_rule.oid
             AND dependency.refclassid = 'pg_catalog.pg_class'::regclass
             AND dependency.deptype = 'n'
            JOIN pg_catalog.pg_class referenced
              ON referenced.oid = dependency.refobjid
            JOIN pg_catalog.pg_namespace referenced_namespace
              ON referenced_namespace.oid = referenced.relnamespace
           WHERE referenced.oid <> target.oid
             AND referenced.relkind IN ('r', 'p', 'v', 'm')
      ) exact_dependencies
), outcome_acl AS (
    SELECT NOT EXISTS (
               SELECT 1
                 FROM outcome_view target
                 CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(
                     target.relacl,
                     pg_catalog.acldefault('r', target.relowner)
                 )) acl
                WHERE acl.grantee <> target.relowner
                  AND NOT (
                      acl.grantee = (
                          SELECT role_row.oid
                            FROM pg_catalog.pg_roles role_row
                           WHERE role_row.rolname = '{TRUSTED_READER_ROLE}'
                      )
                      AND acl.privilege_type = 'SELECT'
                      AND NOT acl.is_grantable
                  )
           ) AS exact_acl
), comment_receipts AS (
    SELECT pg_catalog.substring(
               target.comment,
               'view_definition_sha256=([0-9a-f]{{64}})'
           ) AS comment_definition_sha256,
           pg_catalog.substring(
               target.comment,
               'stage4_source_catalog_sha256=([0-9a-f]{{64}})$'
           ) AS stage4_source_catalog_sha256
      FROM outcome_view target
)
SELECT COALESCE((
           SELECT target.relkind = 'v'
              AND target.relowner = (
                  SELECT raw.relowner FROM pg_catalog.pg_class raw
                   WHERE raw.oid = pg_catalog.to_regclass(
                       'public.research_alert_outcomes'
                   )
              )
              AND target.relowner = (
                  SELECT stage4.relowner FROM pg_catalog.pg_class stage4
                   WHERE stage4.oid = pg_catalog.to_regclass('{STAGE4_VIEW}')
              )
              AND COALESCE(target.reloptions, ARRAY[]::text[]) @>
                  ARRAY['security_barrier=true',
                        'security_invoker=false']::text[]
              AND pg_catalog.cardinality(target.reloptions) = 2
              AND target.comment LIKE
                  '{OUTCOME_VIEW_CONTRACT_VERSION};%'
              AND receipt.comment_definition_sha256 =
                  target.actual_definition_sha256
              AND receipt.stage4_source_catalog_sha256 IS NOT NULL
              AND columns.shape =
                  ARRAY[{','.join(repr(value) for value in OUTCOME_VIEW_COLUMN_TYPES)}]::text[]
              AND columns.column_acl_absent
              AND dependencies.relation_names = ARRAY[
                  'public.research_alert_outcomes',
                  'public.research_formula_exploration_stage4_v1'
              ]::text[]
              AND acl.exact_acl
              AND pg_catalog.has_table_privilege(
                  '{TRUSTED_READER_ROLE}', target.oid, 'SELECT'
              )
              AND NOT pg_catalog.has_table_privilege(
                  '{TRUSTED_READER_ROLE}', target.oid,
                  'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
              )
              AND NOT EXISTS (
                  SELECT 1 FROM pg_catalog.pg_policy policy_row
                   WHERE policy_row.polrelid = target.oid
              )
              AND NOT EXISTS (
                  SELECT 1 FROM pg_catalog.pg_rewrite rewrite_row
                   WHERE rewrite_row.ev_class = target.oid
                     AND rewrite_row.rulename <> '_RETURN'
              )
             FROM outcome_view target
             CROSS JOIN outcome_columns columns
             CROSS JOIN outcome_dependencies dependencies
             CROSS JOIN outcome_acl acl
             CROSS JOIN comment_receipts receipt
       ), false) AS outcomes_view_attested,
       NOT pg_catalog.has_table_privilege(
           '{TRUSTED_READER_ROLE}', 'public.research_alert_outcomes',
           'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
       )
       AND NOT pg_catalog.has_any_column_privilege(
           '{TRUSTED_READER_ROLE}', 'public.research_alert_outcomes',
           'SELECT,INSERT,UPDATE,REFERENCES'
       ) AS raw_outcomes_access_absent,
       (SELECT receipt.stage4_source_catalog_sha256
          FROM comment_receipts receipt)
           AS stage4_source_catalog_sha256,
       (SELECT target.actual_definition_sha256 FROM outcome_view target)
           AS outcomes_view_definition_sha256
"""

_NO_SIGNAL_OUTCOMES_ATTESTATION_SQL = f"""
/* formula_exploration_reader:no_signal_outcomes_attestation */
WITH carrier_view AS (
    SELECT relation.oid, relation.relowner, relation.relkind,
           relation.relpersistence, relation.relispartition,
           relation.relrowsecurity, relation.relforcerowsecurity,
           relation.reloptions, relation.relacl,
           pg_catalog.obj_description(relation.oid, 'pg_class') AS comment,
           (
               SELECT pg_catalog.encode(pg_catalog.sha256(
                   pg_catalog.convert_to(pg_catalog.pg_get_viewdef(
                       rewrite_rule.ev_class, false
                   ), 'UTF8')
               ), 'hex')
                 FROM pg_catalog.pg_rewrite rewrite_rule
                WHERE rewrite_rule.ev_class = relation.oid
                  AND rewrite_rule.rulename = '_RETURN'
           ) AS actual_definition_sha256
      FROM pg_catalog.pg_class relation
     WHERE relation.oid = pg_catalog.to_regclass('{NO_SIGNAL_OUTCOME_VIEW}')
), carrier_columns AS (
    SELECT COALESCE(pg_catalog.array_agg(
               attribute.attname::text || '|' || pg_catalog.format_type(
                   attribute.atttypid, attribute.atttypmod
               ) ORDER BY attribute.attnum
           ), ARRAY[]::text[]) AS shape,
           COALESCE(pg_catalog.bool_and(
               COALESCE(pg_catalog.cardinality(attribute.attacl), 0) = 0
           ), true) AS column_acl_absent
      FROM carrier_view target
      JOIN pg_catalog.pg_attribute attribute
        ON attribute.attrelid = target.oid
       AND attribute.attnum > 0
       AND NOT attribute.attisdropped
), raw_carrier AS (
    SELECT relation.oid, relation.relowner, relation.relkind,
           relation.relpersistence, relation.relispartition,
           relation.relrowsecurity, relation.relforcerowsecurity,
           relation.relacl,
           pg_catalog.obj_description(relation.oid, 'pg_class') AS comment
      FROM pg_catalog.pg_class relation
     WHERE relation.oid = pg_catalog.to_regclass(
         'public.research_stage4_no_signal_outcomes_v1'
     )
), raw_columns AS (
    SELECT COALESCE(pg_catalog.bool_and(
               COALESCE(pg_catalog.cardinality(attribute.attacl), 0) = 0
           ), true) AS column_acl_absent
      FROM raw_carrier target
      JOIN pg_catalog.pg_attribute attribute
        ON attribute.attrelid = target.oid
       AND attribute.attnum > 0
       AND NOT attribute.attisdropped
), raw_catalog_payload AS (
    SELECT pg_catalog.jsonb_build_object(
        'columns', COALESCE((
            SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
                       'ordinal', attribute.attnum,
                       'name', attribute.attname,
                       'type', pg_catalog.format_type(
                           attribute.atttypid, attribute.atttypmod
                       ),
                       'not_null', attribute.attnotnull,
                       'identity', attribute.attidentity,
                       'generated', attribute.attgenerated,
                       'collation',
                           attribute.attcollation::regcollation::text,
                       'default', pg_catalog.pg_get_expr(
                           default_row.adbin, default_row.adrelid, false
                       )
                   ) ORDER BY attribute.attnum)
              FROM pg_catalog.pg_attribute attribute
              LEFT JOIN pg_catalog.pg_attrdef default_row
                ON default_row.adrelid = attribute.attrelid
               AND default_row.adnum = attribute.attnum
             WHERE attribute.attrelid = raw.oid
               AND attribute.attnum > 0
               AND NOT attribute.attisdropped
        ), '[]'::jsonb),
        'constraints', COALESCE((
            SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
                       'name', constraint_row.conname,
                       'type', constraint_row.contype,
                       'deferrable', constraint_row.condeferrable,
                       'deferred', constraint_row.condeferred,
                       'validated', constraint_row.convalidated,
                       'no_inherit', constraint_row.connoinherit,
                       'definition', pg_catalog.pg_get_constraintdef(
                           constraint_row.oid, false
                       )
                   ) ORDER BY constraint_row.conname)
              FROM pg_catalog.pg_constraint constraint_row
             WHERE constraint_row.conrelid = raw.oid
        ), '[]'::jsonb),
        'indexes', COALESCE((
            SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
                       'name', index_relation.relname,
                       'access_method', access_method.amname,
                       'unique', index_row.indisunique,
                       'primary', index_row.indisprimary,
                       'exclusion', index_row.indisexclusion,
                       'immediate', index_row.indimmediate,
                       'valid', index_row.indisvalid,
                       'ready', index_row.indisready,
                       'live', index_row.indislive,
                       'definition', pg_catalog.pg_get_indexdef(
                           index_row.indexrelid
                       )
                   ) ORDER BY index_relation.relname)
              FROM pg_catalog.pg_index index_row
              JOIN pg_catalog.pg_class index_relation
                ON index_relation.oid = index_row.indexrelid
              JOIN pg_catalog.pg_am access_method
                ON access_method.oid = index_relation.relam
             WHERE index_row.indrelid = raw.oid
        ), '[]'::jsonb)
    ) AS payload
      FROM raw_carrier raw
), raw_catalog_digest AS (
    SELECT pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
               payload::text, 'UTF8'
           )), 'hex') AS raw_catalog_sha256
      FROM raw_catalog_payload
), trigger_catalog_payload AS (
    SELECT pg_catalog.jsonb_build_object(
        'triggers', COALESCE((
            SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
                       'name', trigger_row.tgname,
                       'type', trigger_row.tgtype,
                       'enabled', trigger_row.tgenabled,
                       'function', function_namespace.nspname || '.' ||
                           function_row.proname || '()',
                       'definition', pg_catalog.pg_get_triggerdef(
                           trigger_row.oid, false
                       )
                   ) ORDER BY trigger_row.tgname)
              FROM pg_catalog.pg_trigger trigger_row
              JOIN pg_catalog.pg_proc function_row
                ON function_row.oid = trigger_row.tgfoid
              JOIN pg_catalog.pg_namespace function_namespace
                ON function_namespace.oid = function_row.pronamespace
             WHERE trigger_row.tgrelid = raw.oid
               AND NOT trigger_row.tgisinternal
        ), '[]'::jsonb),
        'functions', COALESCE((
            SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
                       'name', function_row.proname,
                       'owner', function_row.proowner,
                       'security_definer', function_row.prosecdef,
                       'leakproof', function_row.proleakproof,
                       'volatile', function_row.provolatile,
                       'parallel', function_row.proparallel,
                       'language', function_language.lanname,
                       'acl', COALESCE(
                           pg_catalog.to_jsonb(function_row.proacl),
                           'null'::jsonb
                       ),
                       'config', COALESCE(
                           pg_catalog.to_jsonb(function_row.proconfig),
                           '[]'::jsonb
                       ),
                       'body_sha256', pg_catalog.encode(pg_catalog.sha256(
                           pg_catalog.convert_to(function_row.prosrc, 'UTF8')
                       ), 'hex')
                   ) ORDER BY function_row.proname)
              FROM pg_catalog.pg_proc function_row
              JOIN pg_catalog.pg_namespace function_namespace
                ON function_namespace.oid = function_row.pronamespace
              JOIN pg_catalog.pg_language function_language
                ON function_language.oid = function_row.prolang
             WHERE function_namespace.nspname = 'public'
               AND function_row.proname IN (
                   'validate_research_stage4_no_signal_outcome_v1',
                   'prevent_research_stage4_no_signal_outcome_v1_mutation',
                   'prevent_research_stage4_no_signal_outcome_v1_truncate'
               )
               AND function_row.pronargs = 0
        ), '[]'::jsonb)
    ) AS payload
      FROM raw_carrier raw
), trigger_catalog_digest AS (
    SELECT pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
               payload::text, 'UTF8'
           )), 'hex') AS trigger_catalog_sha256
      FROM trigger_catalog_payload
), carrier_dependencies AS (
    SELECT COALESCE(pg_catalog.array_agg(
               dependency_name ORDER BY dependency_name
           ), ARRAY[]::text[]) AS relation_names
      FROM (
          SELECT DISTINCT
                 referenced_namespace.nspname::text || '.' ||
                 referenced.relname::text AS dependency_name
            FROM carrier_view target
            JOIN pg_catalog.pg_rewrite rewrite_rule
              ON rewrite_rule.ev_class = target.oid
             AND rewrite_rule.rulename = '_RETURN'
            JOIN pg_catalog.pg_depend dependency
              ON dependency.classid = 'pg_catalog.pg_rewrite'::regclass
             AND dependency.objid = rewrite_rule.oid
             AND dependency.refclassid = 'pg_catalog.pg_class'::regclass
             AND dependency.deptype = 'n'
            JOIN pg_catalog.pg_class referenced
              ON referenced.oid = dependency.refobjid
            JOIN pg_catalog.pg_namespace referenced_namespace
              ON referenced_namespace.oid = referenced.relnamespace
           WHERE referenced.oid <> target.oid
             AND referenced.relkind IN ('r', 'p', 'v', 'm')
      ) exact_dependencies
), carrier_view_acl AS (
    SELECT NOT EXISTS (
               SELECT 1
                 FROM carrier_view target
                 CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(
                     target.relacl,
                     pg_catalog.acldefault('r', target.relowner)
                 )) acl
                WHERE acl.grantee <> target.relowner
                  AND NOT (
                      acl.grantee = (
                          SELECT role_row.oid
                            FROM pg_catalog.pg_roles role_row
                           WHERE role_row.rolname = '{TRUSTED_READER_ROLE}'
                      )
                      AND acl.privilege_type = 'SELECT'
                      AND NOT acl.is_grantable
                  )
           ) AS exact_acl
), raw_carrier_acl AS (
    SELECT NOT EXISTS (
               SELECT 1
                 FROM raw_carrier target
                 CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(
                     target.relacl,
                     pg_catalog.acldefault('r', target.relowner)
                 )) acl
                WHERE acl.grantee <> target.relowner
                  AND NOT (
                      acl.grantee = (
                          SELECT role_row.oid
                            FROM pg_catalog.pg_roles role_row
                           WHERE role_row.rolname =
                                 'research_stage4_no_signal_outcome_writer_v1'
                      )
                      AND acl.privilege_type IN ('SELECT', 'INSERT')
                      AND NOT acl.is_grantable
                  )
           ) AS exact_acl
), carrier_triggers AS (
    SELECT pg_catalog.count(*) = 3
           AND pg_catalog.bool_and(
               NOT trigger_row.tgisinternal
               AND trigger_row.tgenabled = 'A'
               AND trigger_row.tgqual IS NULL
               AND trigger_row.tgconstraint = 0
               AND NOT trigger_row.tgdeferrable
               AND NOT trigger_row.tginitdeferred
               AND trigger_row.tgname IN (
                   'trg_research_stage4_no_signal_outcome_v1_validate',
                   'trg_research_stage4_no_signal_outcome_v1_immutable',
                   'trg_research_stage4_no_signal_outcome_v1_no_truncate'
               )
               AND trigger_row.tgtype::integer = CASE trigger_row.tgname
                   WHEN 'trg_research_stage4_no_signal_outcome_v1_validate'
                       THEN 7
                   WHEN 'trg_research_stage4_no_signal_outcome_v1_immutable'
                       THEN 27
                   WHEN 'trg_research_stage4_no_signal_outcome_v1_no_truncate'
                       THEN 34
               END
               AND function_namespace.nspname = 'public'
               AND function_row.proowner = raw.relowner
               AND function_row.prorettype = 'pg_catalog.trigger'::regtype
               AND NOT function_row.prosecdef
               AND NOT function_row.proleakproof
               AND function_row.provolatile = 'v'
               AND function_row.proparallel = 'u'
               AND function_row.pronargs = 0
               AND pg_catalog.pg_get_function_identity_arguments(
                   function_row.oid
               ) = ''
               AND function_row.proname = CASE trigger_row.tgname
                   WHEN 'trg_research_stage4_no_signal_outcome_v1_validate'
                       THEN 'validate_research_stage4_no_signal_outcome_v1'
                   WHEN 'trg_research_stage4_no_signal_outcome_v1_immutable'
                       THEN 'prevent_research_stage4_no_signal_outcome_v1_mutation'
                   WHEN 'trg_research_stage4_no_signal_outcome_v1_no_truncate'
                       THEN 'prevent_research_stage4_no_signal_outcome_v1_truncate'
               END
               AND function_language.lanname = 'plpgsql'
               AND (
                   (
                       trigger_row.tgname =
                           'trg_research_stage4_no_signal_outcome_v1_validate'
                       AND pg_catalog.cardinality(function_row.proconfig) = 2
                       AND EXISTS (
                           SELECT 1
                             FROM pg_catalog.unnest(function_row.proconfig)
                                  AS config(setting)
                            WHERE pg_catalog.lower(config.setting) =
                                  'search_path=pg_catalog, public'
                       )
                       AND EXISTS (
                           SELECT 1
                             FROM pg_catalog.unnest(function_row.proconfig)
                                  AS config(setting)
                            WHERE pg_catalog.lower(config.setting) =
                                  'timezone=utc'
                       )
                   ) OR (
                       trigger_row.tgname IN (
                           'trg_research_stage4_no_signal_outcome_v1_immutable',
                           'trg_research_stage4_no_signal_outcome_v1_no_truncate'
                       )
                       AND pg_catalog.cardinality(function_row.proconfig) = 1
                       AND EXISTS (
                           SELECT 1
                             FROM pg_catalog.unnest(function_row.proconfig)
                                  AS config(setting)
                            WHERE pg_catalog.lower(config.setting) =
                                  'search_path=pg_catalog, public'
                       )
                   )
               )
               AND NOT EXISTS (
                   SELECT 1
                     FROM pg_catalog.aclexplode(COALESCE(
                         function_row.proacl,
                         pg_catalog.acldefault('f', function_row.proowner)
                     )) function_acl
                    WHERE function_acl.grantee <> function_row.proowner
               )
           ) AS ready
      FROM raw_carrier raw
      JOIN pg_catalog.pg_trigger trigger_row
        ON trigger_row.tgrelid = raw.oid
       AND NOT trigger_row.tgisinternal
      JOIN pg_catalog.pg_proc function_row
        ON function_row.oid = trigger_row.tgfoid
      JOIN pg_catalog.pg_namespace function_namespace
        ON function_namespace.oid = function_row.pronamespace
      JOIN pg_catalog.pg_language function_language
        ON function_language.oid = function_row.prolang
), carrier_function_inventory AS (
    SELECT pg_catalog.count(*) = 3 AS ready
      FROM pg_catalog.pg_proc function_row
      JOIN pg_catalog.pg_namespace function_namespace
        ON function_namespace.oid = function_row.pronamespace
     WHERE function_namespace.nspname = 'public'
       AND function_row.proname IN (
           'validate_research_stage4_no_signal_outcome_v1',
           'prevent_research_stage4_no_signal_outcome_v1_mutation',
           'prevent_research_stage4_no_signal_outcome_v1_truncate'
       )
), writer_role AS (
    SELECT role_row.oid,
           role_row.rolcanlogin
           AND NOT role_row.rolinherit
           AND NOT role_row.rolsuper
           AND NOT role_row.rolcreatedb
           AND NOT role_row.rolcreaterole
           AND NOT role_row.rolreplication
           AND NOT role_row.rolbypassrls
           AND NOT pg_catalog.has_database_privilege(
               role_row.rolname, pg_catalog.current_database(), 'CREATE'
           )
           AND pg_catalog.has_schema_privilege(
               role_row.rolname, 'public', 'USAGE'
           )
           AND NOT pg_catalog.has_schema_privilege(
               role_row.rolname, 'public', 'CREATE'
           )
           AND NOT EXISTS (
               SELECT 1 FROM pg_catalog.pg_auth_members membership
                WHERE membership.member = role_row.oid
                   OR membership.roleid = role_row.oid
           )
           AND NOT EXISTS (
               SELECT 1 FROM pg_catalog.pg_database database_row
                WHERE database_row.datname = pg_catalog.current_database()
                  AND database_row.datdba = role_row.oid
           )
           AND NOT EXISTS (
               SELECT 1 FROM pg_catalog.pg_namespace namespace_row
                WHERE namespace_row.nspname = 'public'
                  AND namespace_row.nspowner = role_row.oid
           ) AS ready
      FROM pg_catalog.pg_roles role_row
     WHERE role_row.rolname =
           'research_stage4_no_signal_outcome_writer_v1'
), writer_source_authority AS (
    SELECT pg_catalog.count(*) = 4
           AND COALESCE(pg_catalog.bool_and(
               pg_catalog.has_table_privilege(
                   'research_stage4_no_signal_outcome_writer_v1',
                   source_relation.oid, 'SELECT'
               )
               AND NOT pg_catalog.has_table_privilege(
                   'research_stage4_no_signal_outcome_writer_v1',
                   source_relation.oid,
                   'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
               )
           ), false)
           AND NOT EXISTS (
               SELECT 1
                 FROM pg_catalog.pg_attribute attribute
                CROSS JOIN LATERAL pg_catalog.aclexplode(
                     attribute.attacl
                 ) column_acl
                WHERE attribute.attrelid = ANY(ARRAY[
                          pg_catalog.to_regclass('public.research_events'),
                          pg_catalog.to_regclass(
                              'public.research_max_pain_snapshot_sets'
                          ),
                          pg_catalog.to_regclass(
                              'public.research_max_pain_snapshot_symbols'
                          ),
                          pg_catalog.to_regclass(
                              'public.research_max_pain_snapshot_rows'
                          )
                      ]::oid[])
                  AND attribute.attnum > 0
                  AND NOT attribute.attisdropped
                  AND column_acl.grantee = (
                      SELECT role_row.oid FROM pg_catalog.pg_roles role_row
                       WHERE role_row.rolname =
                           'research_stage4_no_signal_outcome_writer_v1'
                  )
           ) AS ready
      FROM pg_catalog.pg_class source_relation
     WHERE source_relation.oid = ANY(ARRAY[
         pg_catalog.to_regclass('public.research_events'),
         pg_catalog.to_regclass('public.research_max_pain_snapshot_sets'),
         pg_catalog.to_regclass('public.research_max_pain_snapshot_symbols'),
         pg_catalog.to_regclass('public.research_max_pain_snapshot_rows')
     ]::oid[])
), writer_unexpected_authority AS (
    SELECT NOT EXISTS (
               SELECT 1
                 FROM pg_catalog.pg_class relation
                JOIN pg_catalog.pg_namespace namespace_row
                   ON namespace_row.oid = relation.relnamespace
                WHERE namespace_row.nspname = 'public'
                  AND relation.relname NOT IN (
                      'research_events',
                      'research_max_pain_snapshot_sets',
                      'research_max_pain_snapshot_symbols',
                      'research_max_pain_snapshot_rows',
                      'research_stage4_no_signal_outcomes_v1'
                  )
                  AND (
                      (
                          relation.relkind = 'S'
                          AND pg_catalog.has_sequence_privilege(
                              'research_stage4_no_signal_outcome_writer_v1',
                              relation.oid, 'USAGE,SELECT,UPDATE'
                          )
                      ) OR (
                          relation.relkind IN ('r', 'p', 'v', 'm', 'f')
                          AND (
                              pg_catalog.has_table_privilege(
                                  'research_stage4_no_signal_outcome_writer_v1',
                                  relation.oid,
                                  'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
                              )
                              OR pg_catalog.has_any_column_privilege(
                                  'research_stage4_no_signal_outcome_writer_v1',
                                  relation.oid,
                                  'SELECT,INSERT,UPDATE,REFERENCES'
                              )
                          )
                      )
                  )
           ) AS ready
), source_hardening AS (
    SELECT NOT EXISTS (
               SELECT 1 FROM raw_carrier raw
                WHERE raw.relrowsecurity OR raw.relforcerowsecurity
           )
           AND NOT EXISTS (
               SELECT 1 FROM carrier_view target
                WHERE target.relrowsecurity OR target.relforcerowsecurity
           )
           AND NOT EXISTS (
               SELECT 1 FROM pg_catalog.pg_policy policy_row
                WHERE policy_row.polrelid IN (
                    SELECT raw.oid FROM raw_carrier raw
                    UNION ALL
                    SELECT target.oid FROM carrier_view target
                )
           )
           AND NOT EXISTS (
               SELECT 1 FROM pg_catalog.pg_rewrite rewrite_row
                WHERE rewrite_row.ev_class = (
                    SELECT raw.oid FROM raw_carrier raw
                )
           )
           AND (
               SELECT pg_catalog.count(*) = 1
                      AND pg_catalog.bool_and(
                          rewrite_row.rulename = '_RETURN'
                      )
                 FROM pg_catalog.pg_rewrite rewrite_row
                WHERE rewrite_row.ev_class = (
                    SELECT target.oid FROM carrier_view target
                )
           )
           AND NOT EXISTS (
               SELECT 1 FROM pg_catalog.pg_inherits inheritance
                WHERE inheritance.inhrelid = (
                          SELECT raw.oid FROM raw_carrier raw
                      )
                   OR inheritance.inhparent = (
                          SELECT raw.oid FROM raw_carrier raw
                      )
           ) AS ready
), comment_receipts AS (
    SELECT pg_catalog.substring(
               target.comment,
               'reference_hash_contract=([^;]+)'
           ) AS view_reference_hash_contract,
           pg_catalog.substring(
               target.comment,
               'outcome_hash_contract=([^;]+)'
           ) AS view_outcome_hash_contract,
           pg_catalog.substring(
               raw.comment,
               'reference_hash_contract=([^;]+)'
           ) AS table_reference_hash_contract,
           pg_catalog.substring(
               raw.comment,
               'outcome_hash_contract=([^;]+)'
           ) AS table_outcome_hash_contract,
           pg_catalog.substring(
               target.comment,
               'view_definition_sha256=([0-9a-f]{{64}})'
           ) AS comment_definition_sha256,
           pg_catalog.substring(
               target.comment,
               'raw_catalog_sha256=([0-9a-f]{{64}})'
           ) AS view_raw_catalog_sha256,
           pg_catalog.substring(
               target.comment,
               'trigger_catalog_sha256=([0-9a-f]{{64}})'
           ) AS view_trigger_catalog_sha256,
           pg_catalog.substring(
               raw.comment,
               'raw_catalog_sha256=([0-9a-f]{{64}})'
           ) AS table_raw_catalog_sha256,
           pg_catalog.substring(
               raw.comment,
               'trigger_catalog_sha256=([0-9a-f]{{64}})$'
           ) AS table_trigger_catalog_sha256,
           pg_catalog.substring(
               target.comment,
               'stage4_source_catalog_sha256=([0-9a-f]{{64}})$'
           ) AS stage4_source_catalog_sha256
      FROM carrier_view target
      CROSS JOIN raw_carrier raw
)
SELECT COALESCE((
           SELECT target.relkind = 'v'
              AND target.relowner = raw.relowner
              AND target.relowner = stage4.relowner
              AND target.relpersistence = 'p'
              AND NOT target.relispartition
              AND COALESCE(target.reloptions, ARRAY[]::text[]) @>
                  ARRAY['security_barrier=true',
                        'security_invoker=false']::text[]
              AND pg_catalog.cardinality(target.reloptions) = 2
              AND target.comment =
                  '{NO_SIGNAL_OUTCOME_VIEW_CONTRACT_VERSION}; '
                  || 'append-only explicit no-signal closed-path labels; '
                  || 'no Formula, delivery, LIVE, Telegram or trading authority; '
                  || 'reference_hash_contract='
                  || '{NO_SIGNAL_REFERENCE_HASH_CONTRACT_VERSION}; '
                  || 'outcome_hash_contract='
                  || '{NO_SIGNAL_OUTCOME_HASH_CONTRACT_VERSION}; '
                  || 'view_definition_sha256='
                  || target.actual_definition_sha256
                  || '; raw_catalog_sha256='
                  || raw_digest.raw_catalog_sha256
                  || '; trigger_catalog_sha256='
                  || trigger_digest.trigger_catalog_sha256
                  || '; stage4_source_catalog_sha256='
                  || receipt.stage4_source_catalog_sha256
              AND receipt.view_reference_hash_contract =
                  '{NO_SIGNAL_REFERENCE_HASH_CONTRACT_VERSION}'
              AND receipt.view_outcome_hash_contract =
                  '{NO_SIGNAL_OUTCOME_HASH_CONTRACT_VERSION}'
              AND receipt.table_reference_hash_contract =
                  '{NO_SIGNAL_REFERENCE_HASH_CONTRACT_VERSION}'
              AND receipt.table_outcome_hash_contract =
                  '{NO_SIGNAL_OUTCOME_HASH_CONTRACT_VERSION}'
              AND receipt.comment_definition_sha256 =
                  target.actual_definition_sha256
              AND receipt.view_raw_catalog_sha256 =
                  raw_digest.raw_catalog_sha256
              AND receipt.table_raw_catalog_sha256 =
                  raw_digest.raw_catalog_sha256
              AND receipt.view_trigger_catalog_sha256 =
                  trigger_digest.trigger_catalog_sha256
              AND receipt.table_trigger_catalog_sha256 =
                  trigger_digest.trigger_catalog_sha256
              AND receipt.stage4_source_catalog_sha256 IS NOT NULL
              AND columns.shape = ARRAY[
                  {','.join(repr(value) for value in NO_SIGNAL_OUTCOME_VIEW_COLUMN_TYPES)}
              ]::text[]
              AND columns.column_acl_absent
              AND dependencies.relation_names = ARRAY[
                  'public.research_formula_exploration_stage4_v1',
                  'public.research_stage4_no_signal_outcomes_v1'
              ]::text[]
              AND acl.exact_acl
              AND pg_catalog.has_table_privilege(
                  '{TRUSTED_READER_ROLE}', target.oid, 'SELECT'
              )
              AND NOT pg_catalog.has_table_privilege(
                  '{TRUSTED_READER_ROLE}', target.oid,
                  'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
              )
             FROM carrier_view target
             CROSS JOIN raw_carrier raw
             JOIN pg_catalog.pg_class stage4
               ON stage4.oid = pg_catalog.to_regclass('{STAGE4_VIEW}')
             CROSS JOIN carrier_columns columns
             CROSS JOIN carrier_dependencies dependencies
             CROSS JOIN carrier_view_acl acl
             CROSS JOIN comment_receipts receipt
             CROSS JOIN raw_catalog_digest raw_digest
             CROSS JOIN trigger_catalog_digest trigger_digest
             CROSS JOIN source_hardening hardening
            WHERE hardening.ready
       ), false) AS no_signal_outcomes_view_attested,
       COALESCE((
           SELECT raw.relkind = 'r'
              AND raw.relpersistence = 'p'
              AND NOT raw.relispartition
              AND raw.comment =
                  'stage4-explicit-no-signal-outcomes-raw-v1; '
                  || 'append-only cell carrier; reference_hash_contract='
                  || '{NO_SIGNAL_REFERENCE_HASH_CONTRACT_VERSION}; '
                  || 'outcome_hash_contract='
                  || '{NO_SIGNAL_OUTCOME_HASH_CONTRACT_VERSION}; '
                  || 'raw_catalog_sha256='
                  || raw_digest.raw_catalog_sha256
                  || '; trigger_catalog_sha256='
                  || trigger_digest.trigger_catalog_sha256
              AND acl.exact_acl
              AND columns.column_acl_absent
              AND trigger_status.ready
              AND function_inventory.ready
              AND writer.ready
              AND source_authority.ready
              AND unexpected_authority.ready
              AND hardening.ready
              AND raw.relowner = stage4.relowner
              AND pg_catalog.has_table_privilege(
                  'research_stage4_no_signal_outcome_writer_v1', raw.oid,
                  'SELECT,INSERT'
              )
              AND NOT pg_catalog.has_table_privilege(
                  'research_stage4_no_signal_outcome_writer_v1', raw.oid,
                  'UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
              )
             FROM raw_carrier raw
             JOIN pg_catalog.pg_class stage4
               ON stage4.oid = pg_catalog.to_regclass('{STAGE4_VIEW}')
             CROSS JOIN raw_carrier_acl acl
             CROSS JOIN raw_columns columns
             CROSS JOIN carrier_triggers trigger_status
             CROSS JOIN carrier_function_inventory function_inventory
             CROSS JOIN writer_role writer
             CROSS JOIN writer_source_authority source_authority
             CROSS JOIN writer_unexpected_authority unexpected_authority
             CROSS JOIN source_hardening hardening
             CROSS JOIN raw_catalog_digest raw_digest
             CROSS JOIN trigger_catalog_digest trigger_digest
       ), false) AS no_signal_outcomes_table_attested,
       COALESCE((
           SELECT writer.ready
              AND source_authority.ready
              AND unexpected_authority.ready
             FROM writer_role writer
             CROSS JOIN writer_source_authority source_authority
             CROSS JOIN writer_unexpected_authority unexpected_authority
       ), false) AS no_signal_writer_authority_attested,
       NOT pg_catalog.has_table_privilege(
           '{TRUSTED_READER_ROLE}',
           'public.research_stage4_no_signal_outcomes_v1',
           'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
       )
       AND NOT pg_catalog.has_any_column_privilege(
           '{TRUSTED_READER_ROLE}',
           'public.research_stage4_no_signal_outcomes_v1',
           'SELECT,INSERT,UPDATE,REFERENCES'
       ) AS raw_no_signal_outcomes_access_absent,
       (SELECT receipt.stage4_source_catalog_sha256
          FROM comment_receipts receipt)
           AS stage4_source_catalog_sha256,
       (SELECT target.actual_definition_sha256 FROM carrier_view target)
           AS no_signal_outcomes_view_definition_sha256,
       (SELECT digest.raw_catalog_sha256 FROM raw_catalog_digest digest)
           AS raw_catalog_sha256,
       (SELECT digest.trigger_catalog_sha256
          FROM trigger_catalog_digest digest)
           AS trigger_catalog_sha256,
       (SELECT receipt.view_raw_catalog_sha256 FROM comment_receipts receipt)
           AS view_raw_catalog_sha256,
       (SELECT receipt.table_raw_catalog_sha256 FROM comment_receipts receipt)
           AS table_raw_catalog_sha256,
       (SELECT receipt.view_trigger_catalog_sha256
          FROM comment_receipts receipt)
           AS view_trigger_catalog_sha256,
       (SELECT receipt.table_trigger_catalog_sha256
          FROM comment_receipts receipt)
           AS table_trigger_catalog_sha256,
       (SELECT receipt.view_reference_hash_contract
          FROM comment_receipts receipt)
           AS view_reference_hash_contract,
       (SELECT receipt.table_reference_hash_contract
          FROM comment_receipts receipt)
           AS table_reference_hash_contract,
       (SELECT receipt.view_outcome_hash_contract
          FROM comment_receipts receipt)
           AS view_outcome_hash_contract,
       (SELECT receipt.table_outcome_hash_contract
          FROM comment_receipts receipt)
           AS table_outcome_hash_contract
"""

_LOAD_PROJECTION_KEYS_SQL = f"""
/* formula_exploration_reader:load_projection_keys */
SELECT event_id AS projection_event_id,
       claimed_snapshot_key AS snapshot_key,
       alert_time_utc AS projection_decision_time_utc,
       event_created_at AS projection_created_at_utc,
       event_type
  FROM {STAGE4_VIEW}
 WHERE event_type = '{exploration.PROJECTION_EVENT_TYPE}'
   AND alert_time_utc >= %s
   AND alert_time_utc <= %s
   AND (
       %s::timestamptz IS NULL
       OR (alert_time_utc, event_id) < (%s::timestamptz, %s::bigint)
   )
 ORDER BY alert_time_utc DESC, event_id DESC
 LIMIT %s
"""

_LOAD_CORPUS_STAGE4_SQL = f"""
/* formula_exploration_reader:load_corpus_stage4 */
WITH requested(snapshot_key) AS (
    SELECT requested_key
      FROM pg_catalog.unnest(%s::text[]) AS supplied(requested_key)
), bounded AS (
    SELECT source.*
      FROM requested
      CROSS JOIN LATERAL (
          SELECT {', '.join(STAGE4_VIEW_COLUMNS)}
            FROM {STAGE4_VIEW}
           WHERE claimed_snapshot_key = requested.snapshot_key
           ORDER BY CASE
                        WHEN event_type = '{exploration.PROJECTION_EVENT_TYPE}'
                            THEN 0 ELSE 1
                    END,
                    event_fingerprint, event_id
           LIMIT %s
      ) source
)
SELECT {', '.join(STAGE4_VIEW_COLUMNS)}
  FROM bounded
 ORDER BY claimed_snapshot_key,
          CASE WHEN event_type = '{exploration.PROJECTION_EVENT_TYPE}'
                   THEN 0 ELSE 1 END,
          event_fingerprint, event_id
 LIMIT %s
"""

_LOAD_CORPUS_WAVE_SQL = f"""
/* formula_exploration_reader:load_corpus_wave */
WITH requested(expected_slot) AS (
    SELECT requested_slot
      FROM pg_catalog.unnest(%s::timestamptz[]) AS supplied(requested_slot)
), bounded AS (
    SELECT source.*
      FROM requested
      CROSS JOIN LATERAL (
          SELECT {', '.join(WAVE_VIEW_COLUMNS)}
            FROM {WAVE_VIEW}
           WHERE membership_eligible_at_utc = requested.expected_slot
           ORDER BY membership_receipt_sha256,
                    transition_receipt_sha256, anchor_id
           LIMIT %s
      ) source
)
SELECT {', '.join(WAVE_VIEW_COLUMNS)}
  FROM bounded
 ORDER BY membership_eligible_at_utc,
          membership_receipt_sha256, transition_receipt_sha256, anchor_id
 LIMIT %s
"""

_LOAD_CORPUS_OUTCOMES_SQL = f"""
/* formula_exploration_reader:load_corpus_outcomes */
SELECT {', '.join(OUTCOME_VIEW_COLUMNS)}
  FROM {OUTCOME_VIEW}
 WHERE event_id = ANY(%s::bigint[])
   AND horizon_minutes = %s
 ORDER BY event_id
LIMIT %s
"""

_LOAD_CORPUS_NO_SIGNAL_OUTCOMES_SQL = f"""
/* formula_exploration_reader:load_corpus_no_signal_outcomes */
SELECT {', '.join(NO_SIGNAL_OUTCOME_VIEW_COLUMNS)}
  FROM {NO_SIGNAL_OUTCOME_VIEW}
 WHERE projection_event_id = ANY(%s::bigint[])
   AND horizon_minutes = %s
 ORDER BY projection_event_id, symbol, direction
 LIMIT %s
"""


class AuthoritativeReaderError(RuntimeError):
    """Base failure for the authoritative Stage-4/Wave source boundary."""


class ReaderConfigurationError(AuthoritativeReaderError):
    """The dedicated database reader is unavailable or misconfigured."""


class ReaderAttestationError(AuthoritativeReaderError):
    """The reader identity or installed schema failed closed."""


class CohortIntegrityError(AuthoritativeReaderError):
    """Rows returned by an attested interface are incomplete or inconsistent."""


@dataclass(frozen=True)
class AuthoritativeStage4WaveResult:
    """Immutable, content-addressed result from one authoritative DB snapshot."""

    attestation_receipt_sha256: str
    _payload_json: str

    def __post_init__(self) -> None:
        try:
            body = json.loads(self._payload_json)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError("authoritative source payload is invalid JSON") from exc
        if _canonical_json(body) != self._payload_json:
            raise ValueError("authoritative source payload is not canonical")
        if body.get("source_contract_version") != SOURCE_CONTRACT_VERSION:
            raise ValueError("authoritative source contract mismatch")
        expected = _attestation_receipt(body)
        if self.attestation_receipt_sha256 != expected:
            raise ValueError("authoritative source attestation receipt mismatch")

    @classmethod
    def _from_payload(cls, payload: Mapping[str, Any]) -> "AuthoritativeStage4WaveResult":
        body = _json_value(payload, field="authoritative source payload")
        if not isinstance(body, Mapping):
            raise ValueError("authoritative source payload must be an object")
        normalized = dict(body)
        normalized.pop("attestation_receipt_sha256", None)
        receipt = _attestation_receipt(normalized)
        return cls(receipt, _canonical_json(normalized))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "attestation_receipt_sha256": self.attestation_receipt_sha256,
            **json.loads(self._payload_json),
        }

    @property
    def analysis_as_of_utc(self) -> str:
        return str(self.to_dict()["analysis_as_of_utc"])

    @property
    def database_snapshot_id(self) -> str:
        return str(self.to_dict()["database_snapshot_id"])

    @property
    def source_attestation(self) -> Dict[str, Any]:
        return dict(self.to_dict()["source_attestation"])

    @property
    def projection_event(self) -> Dict[str, Any]:
        return dict(self.to_dict()["projection_event"])

    @property
    def archive_set(self) -> Dict[str, Any]:
        return dict(self.to_dict()["archive_set"])

    @property
    def signal_events(self) -> tuple[Dict[str, Any], ...]:
        return tuple(dict(row) for row in self.to_dict()["signal_events"])

    @property
    def memberships(self) -> tuple[Dict[str, Any], ...]:
        return tuple(dict(row) for row in self.to_dict()["memberships"])

    @property
    def transitions(self) -> tuple[Dict[str, Any], ...]:
        return tuple(dict(row) for row in self.to_dict()["transitions"])

    @property
    def observations(self) -> tuple[exploration.ExplorationObservation, ...]:
        return tuple(
            exploration.ExplorationObservation.from_dict(row)
            for row in self.to_dict()["observations"]
        )


@dataclass(frozen=True)
class AuthoritativeStage4CorpusResult:
    """Immutable bounded corpus page from one authoritative DB snapshot."""

    attestation_receipt_sha256: str
    _payload_json: str

    def __post_init__(self) -> None:
        try:
            body = json.loads(self._payload_json)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError("authoritative corpus payload is invalid JSON") from exc
        if _canonical_json(body) != self._payload_json:
            raise ValueError("authoritative corpus payload is not canonical")
        if body.get("source_contract_version") != CORPUS_SOURCE_CONTRACT_VERSION:
            raise ValueError("authoritative corpus source contract mismatch")
        expected = _corpus_attestation_receipt(body)
        if self.attestation_receipt_sha256 != expected:
            raise ValueError("authoritative corpus attestation receipt mismatch")

    @classmethod
    def _from_payload(
        cls, payload: Mapping[str, Any]
    ) -> "AuthoritativeStage4CorpusResult":
        body = _json_value(payload, field="authoritative corpus payload")
        if not isinstance(body, Mapping):
            raise ValueError("authoritative corpus payload must be an object")
        normalized = dict(body)
        normalized.pop("attestation_receipt_sha256", None)
        return cls(
            _corpus_attestation_receipt(normalized),
            _canonical_json(normalized),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "attestation_receipt_sha256": self.attestation_receipt_sha256,
            **json.loads(self._payload_json),
        }

    @property
    def analysis_as_of_utc(self) -> str:
        return str(self.to_dict()["analysis_as_of_utc"])

    @property
    def database_snapshot_id(self) -> str:
        return str(self.to_dict()["database_snapshot_id"])

    @property
    def source_attestation(self) -> Dict[str, Any]:
        return dict(self.to_dict()["source_attestation"])

    @property
    def counts(self) -> Dict[str, int]:
        return {
            str(key): int(value)
            for key, value in self.to_dict()["counts"].items()
        }

    @property
    def cursor(self) -> Dict[str, Any]:
        return dict(self.to_dict()["cursor"])

    @property
    def next_cursor(self) -> Optional[Dict[str, Any]]:
        value = self.to_dict()["cursor"].get("next")
        return None if value is None else dict(value)

    @property
    def observations(self) -> tuple[exploration.ExplorationObservation, ...]:
        return tuple(
            exploration.ExplorationObservation.from_dict(row)
            for row in self.to_dict()["observations"]
        )


def _attestation_receipt(body: Mapping[str, Any]) -> str:
    envelope = {
        "kind": "authoritative-stage4-wave-source",
        "source_contract_version": SOURCE_CONTRACT_VERSION,
        "payload": dict(body),
    }
    return hashlib.sha256(_canonical_json(envelope).encode("utf-8")).hexdigest()


def _corpus_attestation_receipt(body: Mapping[str, Any]) -> str:
    envelope = {
        "kind": "authoritative-stage4-wave-closed-outcome-corpus",
        "source_contract_version": CORPUS_SOURCE_CONTRACT_VERSION,
        "payload": dict(body),
    }
    return hashlib.sha256(_canonical_json(envelope).encode("utf-8")).hexdigest()


def _json_value(value: Any, *, field: str) -> Any:
    if value is None or type(value) in (str, bool, int):
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise CohortIntegrityError(f"{field} contains a non-finite number")
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CohortIntegrityError(f"{field} contains a non-finite number")
        return value
    if isinstance(value, datetime):
        return _iso(value, field=field)
    if isinstance(value, Mapping):
        return {
            str(key): _json_value(item, field=f"{field}.{key}")
            for key, item in value.items()
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [
            _json_value(item, field=f"{field}[{index}]")
            for index, item in enumerate(value)
        ]
    raise CohortIntegrityError(f"{field} contains unsupported data")


def _canonical_json(value: Any) -> str:
    return json.dumps(
        _json_value(value, field="payload"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _utc(value: Any, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise CohortIntegrityError(f"{field} is not an ISO timestamp") from exc
    else:
        raise CohortIntegrityError(f"{field} must be a timezone-aware timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CohortIntegrityError(f"{field} must include a timezone")
    return parsed.astimezone(_UTC)


def _iso(value: Any, *, field: str) -> str:
    return _utc(value, field=field).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _hash(value: Any, *, field: str) -> str:
    if not isinstance(value, str):
        raise CohortIntegrityError(f"{field} is not a SHA-256 digest")
    normalized = value.rstrip()
    if _HEX_64.fullmatch(normalized) is None:
        raise CohortIntegrityError(f"{field} is not a lowercase SHA-256 digest")
    return normalized


def _tagged_sha256(
    tags: Sequence[str], values: Sequence[Optional[str]], *, field: str
) -> str:
    if len(tags) != len(values):  # pragma: no cover - programmer invariant
        raise CohortIntegrityError(f"{field} hash field count is invalid")
    encoded_fields: list[str] = []
    for tag, value in zip(tags, values):
        if value is None:
            encoded_fields.append(f"{tag}=-1:")
            continue
        if type(value) is not str:
            raise CohortIntegrityError(f"{field}.{tag} is not canonical text")
        byte_length = len(value.encode("utf-8"))
        encoded_fields.append(f"{tag}={byte_length}:{value}")
    payload = "\x1f".join(encoded_fields).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _float8_hex(value: Any, *, field: str) -> str:
    if isinstance(value, bool):
        raise CohortIntegrityError(f"{field} must be a finite float8")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise CohortIntegrityError(f"{field} must be a finite float8") from exc
    if not math.isfinite(number):
        raise CohortIntegrityError(f"{field} must be a finite float8")
    return struct.pack("!d", number).hex()


def _nonnegative_int_text(
    value: Any, *, field: str, nullable: bool = False
) -> Optional[str]:
    if value is None and nullable:
        return None
    if type(value) is not int or value < 0 or value > 2**31 - 1:
        qualifier = "nullable " if nullable else ""
        raise CohortIntegrityError(
            f"{field} must be a {qualifier}nonnegative signed-32-bit integer"
        )
    return str(value)


def _positive_int(value: Any, *, field: str) -> int:
    if type(value) is not int or value <= 0 or value > 2**63 - 1:
        raise CohortIntegrityError(f"{field} must be a positive signed-64-bit integer")
    return value


def _bounded_configuration_int(
    value: Any, *, field: str, maximum: int
) -> int:
    if type(value) is not int or value <= 0 or value > maximum:
        raise ReaderConfigurationError(
            f"{field} must be an integer from 1 through {maximum}"
        )
    return value


def _normalized_before_cursor(
    value: Optional[Mapping[str, Any]], *, analysis_as_of_utc: Optional[datetime] = None
) -> Optional[Dict[str, Any]]:
    if value is None:
        return None
    if not isinstance(value, Mapping) or set(value) != {
        "projection_decision_time_utc",
        "projection_event_id",
    }:
        raise ReaderConfigurationError(
            "before_cursor must contain projection_decision_time_utc and "
            "projection_event_id only"
        )
    try:
        decision = _utc(
            value.get("projection_decision_time_utc"),
            field="before_cursor projection_decision_time_utc",
        )
        event_id = _positive_int(
            value.get("projection_event_id"),
            field="before_cursor projection_event_id",
        )
    except CohortIntegrityError as exc:
        raise ReaderConfigurationError(str(exc)) from exc
    if analysis_as_of_utc is not None and decision > analysis_as_of_utc:
        raise ReaderConfigurationError("before_cursor is after analysis_as_of_utc")
    return {
        "projection_decision_time_utc": _iso(
            decision, field="before_cursor projection_decision_time_utc"
        ),
        "projection_event_id": event_id,
    }


def _decimal_text(value: Any, *, field: str) -> str:
    if isinstance(value, bool):
        raise CohortIntegrityError(f"{field} must be a finite decimal")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise CohortIntegrityError(f"{field} must be a finite decimal") from exc
    if not number.is_finite():
        raise CohortIntegrityError(f"{field} must be a finite decimal")
    text = format(number, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _json_object(value: Any, *, field: str) -> Dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise CohortIntegrityError(f"{field} is not valid JSON") from exc
    if not isinstance(value, Mapping):
        raise CohortIntegrityError(f"{field} must be a JSON object")
    normalized = _json_value(value, field=field)
    if not isinstance(normalized, Mapping):  # pragma: no cover - guarded above
        raise CohortIntegrityError(f"{field} must be a JSON object")
    return dict(normalized)


def _json_array(value: Any, *, field: str) -> list[Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise CohortIntegrityError(f"{field} is not valid JSON") from exc
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise CohortIntegrityError(f"{field} must be a JSON array")
    normalized = _json_value(value, field=field)
    return list(normalized)


def _fetchone(cursor: Any) -> Optional[Dict[str, Any]]:
    row = cursor.fetchone()
    return dict(row) if row is not None else None


def _fetchall(cursor: Any) -> list[Dict[str, Any]]:
    return [dict(row) for row in cursor.fetchall()]


def _database_target(url: str) -> tuple[str, str, int, str]:
    try:
        parsed = urlsplit(str(url or "").strip())
        hostname = parsed.hostname
        port = int(parsed.port or 5432)
        target_overrides = {
            key.lower()
            for key, _value in parse_qsl(parsed.query, keep_blank_values=True)
            if key.lower() in {
                "host", "hostaddr", "port", "dbname", "service", "servicefile"
            }
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


def _configured_database_url(database_url: Optional[str]) -> str:
    url = str(database_url or os.getenv(DATABASE_URL_ENV, "")).strip()
    target = _database_target(url)
    if not url:
        raise ReaderConfigurationError(
            "authoritative Formula exploration reader database is not configured"
        )
    if not all(target):
        raise ReaderConfigurationError(
            "authoritative Formula exploration database target is invalid"
        )
    alignment_urls = [
        os.getenv("RESEARCH_DATABASE_URL", "").strip(),
        os.getenv("RESEARCH_SIGNAL_SNAPSHOT_DATABASE_URL", "").strip(),
        os.getenv("RESEARCH_MARKET_MOVEMENT_DATABASE_URL", "").strip(),
    ]
    if os.getenv("RESEARCH_USE_PRIMARY_DATABASE", "").strip().lower() in {
        "1", "true", "yes", "on"
    }:
        alignment_urls.append(os.getenv("DATABASE_URL", "").strip())
    for aligned_url in alignment_urls:
        if aligned_url and _database_target(aligned_url) != target:
            raise ReaderConfigurationError(
                "authoritative reader must target the same research database"
            )
    if psycopg is None:
        raise ReaderConfigurationError("psycopg is unavailable")
    return url


def _validate_session(
    row: Optional[Mapping[str, Any]], *, expected_database_name: str
) -> tuple[datetime, Dict[str, Any]]:
    if not row:
        raise ReaderAttestationError("database session attestation returned no row")
    if (
        row.get("session_user") != TRUSTED_READER_ROLE
        or row.get("current_user") != TRUSTED_READER_ROLE
        or str(row.get("transaction_isolation") or "").lower() != "repeatable read"
        or str(row.get("transaction_read_only") or "").lower() != "on"
        or row.get("in_recovery") is not False
    ):
        raise ReaderAttestationError(
            "database session is not the dedicated read-only repeatable-read reader"
        )
    as_of = _utc(row.get("analysis_as_of_utc"), field="analysis_as_of_utc")
    snapshot_id = str(row.get("database_snapshot_id") or "").strip()
    database_name = str(row.get("database_name") or "").strip()
    if not snapshot_id or not database_name:
        raise ReaderAttestationError("database snapshot identity is incomplete")
    if database_name != expected_database_name:
        raise ReaderAttestationError(
            "database session target differs from the configured research database"
        )
    return as_of, {
        "analysis_as_of_utc": _iso(as_of, field="analysis_as_of_utc"),
        "database_snapshot_id": snapshot_id,
        "database_name": database_name,
        "reader_role": TRUSTED_READER_ROLE,
        "transaction_isolation": "repeatable read",
        "transaction_read_only": True,
        "writable_primary_verified": True,
    }


_ATTESTATION_FLAGS = (
    "reader_role_ready",
    "migration_022_attested",
    "migration_023_attested",
    "stage4_view_attested",
    "wave_view_attested",
    "raw_access_absent",
)


def _validate_attestation(row: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    if not row:
        raise ReaderAttestationError("schema attestation returned no row")
    missing = [name for name in _ATTESTATION_FLAGS if row.get(name) is not True]
    if missing:
        raise ReaderAttestationError(
            "authoritative source schema attestation failed: " + ", ".join(missing)
        )
    try:
        source_catalog_sha256 = _hash(
            row.get("source_catalog_sha256"), field="source_catalog_sha256"
        )
    except CohortIntegrityError as exc:
        raise ReaderAttestationError(
            "authoritative source catalog receipt is missing or malformed"
        ) from exc
    return {
        **{name: True for name in _ATTESTATION_FLAGS},
        "source_catalog_sha256": source_catalog_sha256,
    }


def _event_from_stage4_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    event = {key: row.get(key) for key in EVENT_COLUMNS}
    event["event_id"] = _positive_int(event.get("event_id"), field="event_id")
    event["setup_key"] = _hash(event.get("setup_key"), field="setup_key")
    event["event_fingerprint"] = _hash(
        event.get("event_fingerprint"), field="event_fingerprint"
    )
    event["categories"] = _json_array(event.get("categories"), field="categories")
    event["engine_snapshot"] = _json_object(
        event.get("engine_snapshot"), field="engine_snapshot"
    )
    for field in (
        "alert_time_utc", "delivery_attempted_at_utc", "delivered_at_utc"
    ):
        if event.get(field) is not None:
            event[field] = _iso(event[field], field=field)
    return event


def _validate_stage4_rows(
    rows: Sequence[Mapping[str, Any]], *, snapshot_key: str, as_of: datetime
) -> tuple[Dict[str, Any], Dict[str, Any], tuple[Dict[str, Any], ...]]:
    if not rows:
        raise CohortIntegrityError("authoritative Stage-4 projection is missing")
    if len(rows) > MAX_STAGE4_ROWS:
        raise CohortIntegrityError("authoritative Stage-4 read exceeded its bound")
    normalized_events: list[Dict[str, Any]] = []
    archive_identity: Optional[Dict[str, Any]] = None
    seen_ids: set[int] = set()
    seen_fingerprints: set[str] = set()
    for raw in rows:
        row = dict(raw)
        absent = set(STAGE4_VIEW_COLUMNS) - set(row)
        if absent:
            raise CohortIntegrityError(
                "Stage-4 view row omits columns: " + ", ".join(sorted(absent))
            )
        claimed_id = _positive_int(
            row.get("claimed_snapshot_set_id"), field="claimed_snapshot_set_id"
        )
        claimed_key = _hash(
            row.get("claimed_snapshot_key"), field="claimed_snapshot_key"
        )
        archive_id = _positive_int(
            row.get("archive_snapshot_set_id"), field="archive_snapshot_set_id"
        )
        archive_key = _hash(
            row.get("archive_snapshot_key"), field="archive_snapshot_key"
        )
        if claimed_key != snapshot_key or claimed_id != archive_id or claimed_key != archive_key:
            raise CohortIntegrityError("Stage-4 claim/archive join identity mismatch")
        event_time = _utc(row.get("alert_time_utc"), field="alert_time_utc")
        event_created = _utc(row.get("event_created_at"), field="event_created_at")
        archive_available = _utc(
            row.get("archive_available_at_utc"), field="archive_available_at_utc"
        )
        archive_created = _utc(
            row.get("archive_created_at_utc"), field="archive_created_at_utc"
        )
        if (
            event_created < event_time
            or archive_created < archive_available
            or archive_created > event_created
            or event_created > as_of
            or archive_created > as_of
        ):
            raise CohortIntegrityError("Stage-4 creation timestamps are not causal")
        archive = {
            "snapshot_set_id": archive_id,
            "snapshot_key": archive_key,
            "payload_sha256": _hash(
                row.get("archive_payload_sha256"), field="archive_payload_sha256"
            ),
            "cycle_time_utc": _iso(
                row.get("archive_cycle_time_utc"), field="archive_cycle_time_utc"
            ),
            "available_at_utc": _iso(
                archive_available, field="archive_available_at_utc"
            ),
            "source": row.get("archive_source"),
            "research_eligible": row.get("archive_research_eligible"),
        }
        if archive_identity is None:
            archive_identity = archive
        elif _canonical_json(archive_identity) != _canonical_json(archive):
            raise CohortIntegrityError("Stage-4 rows do not share one archive set")
        event = _event_from_stage4_row(row)
        if event["event_id"] in seen_ids or event["event_fingerprint"] in seen_fingerprints:
            raise CohortIntegrityError("Stage-4 view returned duplicate event identity")
        seen_ids.add(event["event_id"])
        seen_fingerprints.add(event["event_fingerprint"])
        normalized_events.append(event)
    projections = [
        row for row in normalized_events
        if row.get("event_type") == exploration.PROJECTION_EVENT_TYPE
    ]
    signals = [
        row for row in normalized_events
        if row.get("event_type") in exploration.SIGNAL_EVENT_TYPES
    ]
    if len(projections) != 1 or len(projections) + len(signals) != len(normalized_events):
        raise CohortIntegrityError(
            "Stage-4 view must contain exactly one projection and only its signals"
        )
    if archive_identity is None:  # pragma: no cover - rows guarantee this
        raise CohortIntegrityError("Stage-4 archive set is missing")
    return projections[0], archive_identity, tuple(signals)


def _typed_anchor_payload(row: Mapping[str, Any]) -> Dict[str, Any]:
    nullable_times = {
        "source_price_candle_open_utc", "source_price_candle_close_utc",
        "source_record_created_at_utc",
    }
    result: Dict[str, Any] = {}
    for field in ("anchor_id", "anchor_receipt_sha256"):
        result[field] = _hash(row.get(field), field=field)
    source_input_fingerprint = row.get("anchor_source_input_fingerprint")
    result["source_input_fingerprint"] = (
        None
        if source_input_fingerprint is None
        else _hash(
            source_input_fingerprint, field="anchor_source_input_fingerprint"
        )
    )
    result.update(
        {
            "contract_version": row.get("anchor_contract_version"),
            "symbol": row.get("anchor_symbol"),
            "origin": row.get("anchor_origin"),
            "sampler_version": row.get("anchor_sampler_version"),
            "price": _decimal_text(row.get("anchor_price"), field="anchor_price"),
            "source": row.get("anchor_source"),
            "upstream_source": row.get("anchor_upstream_source"),
            "price_exchange": row.get("anchor_price_exchange"),
            "price_market": row.get("anchor_price_market"),
            "price_pair": row.get("anchor_price_pair"),
            "price_instrument_id": row.get("anchor_price_instrument_id"),
            "price_timeframe": row.get("anchor_price_timeframe"),
            "quality_status": row.get("anchor_quality_status"),
            "fallback_used": row.get("anchor_fallback_used"),
            "fallback_policy": row.get("anchor_fallback_policy"),
            "price_candle_identity_basis": row.get(
                "anchor_price_candle_identity_basis"
            ),
        }
    )
    for field in (
        "eligible_at_utc", "decision_time_utc", "source_price_candle_open_utc",
        "source_price_candle_close_utc", "observed_at_utc",
        "refresh_completed_at_utc", "source_record_created_at_utc",
    ):
        value = row.get("anchor_" + field)
        result[field] = (
            None if value is None and field in nullable_times else _iso(value, field="anchor_" + field)
        )
    return result


def _typed_membership_payload(row: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "contract_version": row.get("membership_contract_version"),
        "membership_receipt_sha256": _hash(
            row.get("membership_receipt_sha256"),
            field="membership_receipt_sha256",
        ),
        "stream_id": _hash(row.get("membership_stream_id"), field="membership_stream_id"),
        "movement_id": _hash(
            row.get("membership_movement_id"), field="membership_movement_id"
        ),
        "anchor_id": _hash(row.get("membership_anchor_id"), field="membership_anchor_id"),
        "anchor_receipt_sha256": _hash(
            row.get("membership_anchor_receipt_sha256"),
            field="membership_anchor_receipt_sha256",
        ),
        "ordinal": _positive_int(row.get("membership_ordinal"), field="membership_ordinal"),
        "classification": row.get("membership_classification"),
        "eligible_at_utc": _iso(
            row.get("membership_eligible_at_utc"), field="membership_eligible_at_utc"
        ),
        "decision_time_utc": _iso(
            row.get("membership_decision_time_utc"), field="membership_decision_time_utc"
        ),
        "price": _decimal_text(row.get("membership_price"), field="membership_price"),
    }


def _typed_transition_payload(row: Mapping[str, Any]) -> Dict[str, Any]:
    previous = row.get("previous_transition_receipt_sha256")
    pre_state = row.get("transition_pre_state_sha256")
    return {
        "contract_version": row.get("transition_contract_version"),
        "transition_receipt_sha256": _hash(
            row.get("transition_receipt_sha256"), field="transition_receipt_sha256"
        ),
        "previous_transition_receipt_sha256": (
            None if previous is None else _hash(previous, field="previous_transition_receipt_sha256")
        ),
        "transition_type": row.get("transition_type"),
        "stream_id": _hash(row.get("transition_stream_id"), field="transition_stream_id"),
        "movement_id": _hash(
            row.get("transition_movement_id"), field="transition_movement_id"
        ),
        "trigger_anchor_id": _hash(
            row.get("transition_trigger_anchor_id"), field="transition_trigger_anchor_id"
        ),
        "trigger_eligible_at_utc": _iso(
            row.get("transition_trigger_eligible_at_utc"),
            field="transition_trigger_eligible_at_utc",
        ),
        "trigger_decision_time_utc": _iso(
            row.get("transition_trigger_decision_time_utc"),
            field="transition_trigger_decision_time_utc",
        ),
        "pre_state_sha256": (
            None if pre_state is None else _hash(pre_state, field="transition_pre_state_sha256")
        ),
        "post_state": _json_object(
            row.get("transition_post_state"), field="transition_post_state"
        ),
    }


def _validate_transition_state_semantics(
    transition: movement.MovementTransition, *, chain_ordinal: int
) -> None:
    """Check the transition semantics recoverable from one projected row.

    Exact predecessor receipt, state, and ordinal continuity cannot be replayed
    from this bounded view because it deliberately does not project the prior
    transition row.  Those cross-row invariants remain fail-closed through the
    attested migration-022 constraints and trigger/function bindings sealed by
    ``source_catalog_sha256``.
    """

    transition_type = transition.transition_type
    state = transition.post_state
    opening_types = {
        movement.OPENED,
        movement.OPENED_AFTER_DATA_GAP,
        movement.OPENED_AFTER_DIRECTION_END,
    }
    directional = {movement.UP_DIRECTION, movement.DOWN_DIRECTION}
    invalid = (
        (transition_type == movement.OPENED and chain_ordinal != 1)
        or (transition_type != movement.OPENED and chain_ordinal <= 1)
    )
    if transition_type in opening_types:
        invalid = invalid or (
            state.direction != movement.PENDING_DIRECTION
            or state.member_count != 1
            or state.consecutive_non_extremes != 0
        )
    elif transition_type == movement.DIRECTION_ESTABLISHED:
        invalid = invalid or (
            state.direction not in directional
            or state.member_count not in (2, 3)
            or state.consecutive_non_extremes != 0
        )
    elif transition_type == movement.EXTREME_EXTENDED:
        invalid = invalid or (
            state.direction not in directional
            or state.member_count < 3
            or state.consecutive_non_extremes != 0
        )
    elif transition_type == movement.NON_EXTREME_OBSERVED:
        invalid = invalid or (
            state.member_count < 2
            or state.consecutive_non_extremes != 1
            or state.last_member_anchor_id == state.extreme_anchor_id
            or state.last_member_eligible_at_utc <= state.extreme_eligible_at_utc
            or (
                state.direction == movement.PENDING_DIRECTION
                and (
                    state.member_count != 2
                    or state.last_member_price != state.start_price
                )
            )
            or (
                state.direction == movement.UP_DIRECTION
                and (
                    state.member_count < 3
                    or state.last_member_price > state.extreme_price
                )
            )
            or (
                state.direction == movement.DOWN_DIRECTION
                and (
                    state.member_count < 3
                    or state.last_member_price < state.extreme_price
                )
            )
        )
    if invalid:
        raise CohortIntegrityError(
            "Wave transition/state semantics conflict with canonical receipts"
        )


def _validate_wave_rows(
    rows: Sequence[Mapping[str, Any]], *, expected_slot: datetime, as_of: datetime
) -> tuple[tuple[Dict[str, Any], ...], tuple[Dict[str, Any], ...]]:
    if len(rows) > MAX_WAVE_ROWS:
        raise CohortIntegrityError("authoritative Wave read exceeded its bound")
    memberships: list[Dict[str, Any]] = []
    transitions: list[Dict[str, Any]] = []
    seen_memberships: set[str] = set()
    seen_transitions: set[str] = set()
    for raw in rows:
        row = dict(raw)
        absent = set(WAVE_VIEW_COLUMNS) - set(row)
        if absent:
            raise CohortIntegrityError(
                "Wave view row omits columns: " + ", ".join(sorted(absent))
            )
        membership_receipt_key = _hash(
            row.get("membership_receipt_sha256"),
            field="membership_receipt_sha256",
        )
        transition_receipt_key = _hash(
            row.get("transition_receipt_sha256"),
            field="transition_receipt_sha256",
        )
        if membership_receipt_key in seen_memberships:
            raise CohortIntegrityError(
                "Wave view returned duplicate membership receipt"
            )
        if transition_receipt_key in seen_transitions:
            raise CohortIntegrityError(
                "Wave view returned duplicate transition receipt"
            )
        seen_memberships.add(membership_receipt_key)
        seen_transitions.add(transition_receipt_key)
        membership_created = _utc(
            row.get("membership_created_at_utc"), field="membership_created_at_utc"
        )
        transition_created = _utc(
            row.get("transition_created_at_utc"), field="transition_created_at_utc"
        )
        anchor_created = _utc(
            row.get("anchor_created_at_utc"), field="anchor_created_at_utc"
        )
        source_record_created_raw = row.get(
            "anchor_source_record_created_at_utc"
        )
        source_record_created = (
            None
            if source_record_created_raw is None
            else _utc(
                source_record_created_raw,
                field="anchor_source_record_created_at_utc",
            )
        )
        membership_decision = _utc(
            row.get("membership_decision_time_utc"), field="membership_decision_time_utc"
        )
        transition_decision = _utc(
            row.get("transition_trigger_decision_time_utc"),
            field="transition_trigger_decision_time_utc",
        )
        anchor_decision = _utc(
            row.get("anchor_decision_time_utc"), field="anchor_decision_time_utc"
        )
        if (
            membership_created < membership_decision
            or transition_created < transition_decision
            or anchor_created < anchor_decision
            or anchor_created > transition_created
            or transition_created > membership_created
            or (
                source_record_created is not None
                and (
                    source_record_created > anchor_created
                    or source_record_created > as_of
                )
            )
            or membership_created > as_of
            or transition_created > as_of
            or anchor_created > as_of
        ):
            raise CohortIntegrityError("Wave creation timestamps are not causal")
        if _utc(
            row.get("membership_eligible_at_utc"), field="membership_eligible_at_utc"
        ) != expected_slot:
            raise CohortIntegrityError("Wave view returned a row outside the exact slot")

        typed_anchor = _typed_anchor_payload(row)
        typed_membership = _typed_membership_payload(row)
        typed_transition = _typed_transition_payload(row)
        transition_chain_ordinal = _positive_int(
            row.get("transition_chain_ordinal"),
            field="transition_chain_ordinal",
        )
        try:
            anchor = movement.NeutralPriceAnchor.from_dict(
                _json_object(row.get("anchor_receipt"), field="anchor_receipt")
            )
            member = movement.MovementMembership.from_dict(
                _json_object(row.get("membership_receipt"), field="membership_receipt")
            )
            transition = movement.MovementTransition.from_dict(
                _json_object(row.get("transition_receipt"), field="transition_receipt")
            )
        except (TypeError, ValueError) as exc:
            raise CohortIntegrityError("Wave receipt is forged or non-canonical") from exc
        membership_classification_by_transition = {
            movement.OPENED: movement.START_MEMBER,
            movement.OPENED_AFTER_DATA_GAP: movement.START_MEMBER,
            movement.OPENED_AFTER_DIRECTION_END: movement.START_MEMBER,
            movement.DIRECTION_ESTABLISHED:
                movement.DIRECTIONAL_EXTREME_MEMBER,
            movement.EXTREME_EXTENDED: movement.EXTREME_EXTENSION_MEMBER,
            movement.NON_EXTREME_OBSERVED: movement.NON_EXTREME_MEMBER,
        }
        if transition.transition_type == movement.MOVEMENT_CLOSED:
            raise CohortIntegrityError(
                "MOVEMENT_CLOSED cannot emit a Wave membership"
            )
        expected_classification = membership_classification_by_transition.get(
            transition.transition_type
        )
        if expected_classification is None:  # pragma: no cover - domain guard
            raise CohortIntegrityError(
                "Wave transition cannot emit a membership"
            )
        post_state = transition.post_state
        if (
            _canonical_json(anchor.to_dict()) != _canonical_json(typed_anchor)
            or _canonical_json(member.to_dict()) != _canonical_json(typed_membership)
            or _canonical_json(transition.to_dict()) != _canonical_json(typed_transition)
            or _hash(row.get("transition_post_state_sha256"), field="transition_post_state_sha256")
                != post_state.state_sha256
            or row.get("transition_namespace") != post_state.namespace
            or row.get("transition_symbol") != post_state.symbol
            or _hash(
                row.get("emitted_by_transition_receipt_sha256"),
                field="emitted_by_transition_receipt_sha256",
            ) != transition.transition_receipt_sha256
            or member.anchor_id != anchor.anchor_id
            or member.anchor_receipt_sha256 != anchor.anchor_receipt_sha256
            or member.eligible_at_utc != anchor.eligible_at_utc
            or member.decision_time_utc != anchor.decision_time_utc
            or member.price != anchor.price
            or transition.trigger_anchor_id != member.anchor_id
            or transition.trigger_eligible_at_utc != member.eligible_at_utc
            or transition.trigger_decision_time_utc != member.decision_time_utc
            or anchor.symbol != post_state.symbol
            or member.stream_id != transition.stream_id
            or member.stream_id != post_state.stream_id
            or member.movement_id != transition.movement_id
            or member.movement_id != post_state.movement_id
            or member.ordinal != post_state.member_count
            or member.classification != expected_classification
            or (
                expected_classification == movement.START_MEMBER
                and member.ordinal != 1
            )
            or (
                expected_classification != movement.START_MEMBER
                and member.ordinal <= 1
            )
            or post_state.last_member_anchor_id != member.anchor_id
            or post_state.last_member_anchor_id != anchor.anchor_id
            or post_state.last_member_eligible_at_utc != member.eligible_at_utc
            or post_state.last_member_eligible_at_utc != anchor.eligible_at_utc
            or post_state.last_member_decision_time_utc != member.decision_time_utc
            or post_state.last_member_decision_time_utc != anchor.decision_time_utc
            or post_state.last_member_price != member.price
            or post_state.last_member_price != anchor.price
        ):
            raise CohortIntegrityError("Wave typed columns conflict with canonical receipts")
        _validate_transition_state_semantics(
            transition,
            chain_ordinal=transition_chain_ordinal,
        )
        membership_row = {
            **member.to_dict(),
            "emitted_by_transition_receipt_sha256": transition.transition_receipt_sha256,
        }
        transition_row = {
            "contract_version": transition.contract_version,
            "transition_receipt_sha256": transition.transition_receipt_sha256,
            "stream_id": transition.stream_id,
            "movement_id": transition.movement_id,
            "namespace": transition.post_state.namespace,
            "symbol": transition.post_state.symbol,
            "trigger_anchor_id": transition.trigger_anchor_id,
            "trigger_eligible_at_utc": _iso(
                transition.trigger_eligible_at_utc, field="trigger_eligible_at_utc"
            ),
            "trigger_decision_time_utc": _iso(
                transition.trigger_decision_time_utc, field="trigger_decision_time_utc"
            ),
        }
        memberships.append(membership_row)
        transitions.append(transition_row)
    memberships.sort(key=lambda item: item["membership_receipt_sha256"])
    transitions.sort(key=lambda item: item["transition_receipt_sha256"])
    return tuple(memberships), tuple(transitions)


def _validate_outcomes_attestation(
    row: Optional[Mapping[str, Any]], *, expected_source_catalog_sha256: str
) -> Dict[str, Any]:
    if not row:
        raise ReaderAttestationError("outcome view attestation returned no row")
    missing = [
        name
        for name in ("outcomes_view_attested", "raw_outcomes_access_absent")
        if row.get(name) is not True
    ]
    if missing:
        raise ReaderAttestationError(
            "authoritative outcome schema attestation failed: "
            + ", ".join(missing)
        )
    try:
        stage4_source_catalog_sha256 = _hash(
            row.get("stage4_source_catalog_sha256"),
            field="stage4_source_catalog_sha256",
        )
        outcomes_view_definition_sha256 = _hash(
            row.get("outcomes_view_definition_sha256"),
            field="outcomes_view_definition_sha256",
        )
    except CohortIntegrityError as exc:
        raise ReaderAttestationError(
            "authoritative outcome view receipt is missing or malformed"
        ) from exc
    if stage4_source_catalog_sha256 != expected_source_catalog_sha256:
        raise ReaderAttestationError(
            "outcome view is not bound to the attested Stage-4 source catalog"
        )
    return {
        "outcomes_view_attested": True,
        "raw_outcomes_access_absent": True,
        "outcomes_view_contract_version": OUTCOME_VIEW_CONTRACT_VERSION,
        "outcomes_view_definition_sha256": outcomes_view_definition_sha256,
        "outcomes_stage4_source_catalog_sha256": stage4_source_catalog_sha256,
    }


def _validate_no_signal_outcomes_attestation(
    row: Optional[Mapping[str, Any]], *, expected_source_catalog_sha256: str
) -> Dict[str, Any]:
    if not row:
        raise ReaderAttestationError(
            "no-signal outcome view attestation returned no row"
        )
    missing = [
        name
        for name in (
            "no_signal_outcomes_view_attested",
            "no_signal_outcomes_table_attested",
            "no_signal_writer_authority_attested",
            "raw_no_signal_outcomes_access_absent",
        )
        if row.get(name) is not True
    ]
    if missing:
        raise ReaderAttestationError(
            "authoritative no-signal outcome schema attestation failed: "
            + ", ".join(missing)
        )
    try:
        stage4_source_catalog_sha256 = _hash(
            row.get("stage4_source_catalog_sha256"),
            field="no-signal stage4_source_catalog_sha256",
        )
        definition_sha256 = _hash(
            row.get("no_signal_outcomes_view_definition_sha256"),
            field="no_signal_outcomes_view_definition_sha256",
        )
        raw_catalog_sha256 = _hash(
            row.get("raw_catalog_sha256"),
            field="no-signal raw_catalog_sha256",
        )
        trigger_catalog_sha256 = _hash(
            row.get("trigger_catalog_sha256"),
            field="no-signal trigger_catalog_sha256",
        )
        view_raw_catalog_sha256 = _hash(
            row.get("view_raw_catalog_sha256"),
            field="no-signal view_raw_catalog_sha256",
        )
        table_raw_catalog_sha256 = _hash(
            row.get("table_raw_catalog_sha256"),
            field="no-signal table_raw_catalog_sha256",
        )
        view_trigger_catalog_sha256 = _hash(
            row.get("view_trigger_catalog_sha256"),
            field="no-signal view_trigger_catalog_sha256",
        )
        table_trigger_catalog_sha256 = _hash(
            row.get("table_trigger_catalog_sha256"),
            field="no-signal table_trigger_catalog_sha256",
        )
    except CohortIntegrityError as exc:
        raise ReaderAttestationError(
            "authoritative no-signal outcome receipt is missing or malformed"
        ) from exc
    if stage4_source_catalog_sha256 != expected_source_catalog_sha256:
        raise ReaderAttestationError(
            "no-signal outcome view is not bound to the attested Stage-4 source catalog"
        )
    if not (
        raw_catalog_sha256
        == view_raw_catalog_sha256
        == table_raw_catalog_sha256
        and trigger_catalog_sha256
        == view_trigger_catalog_sha256
        == table_trigger_catalog_sha256
    ):
        raise ReaderAttestationError(
            "no-signal outcome catalog receipts do not match the live catalogs"
        )
    for field_name, expected_value in (
        (
            "view_reference_hash_contract",
            NO_SIGNAL_REFERENCE_HASH_CONTRACT_VERSION,
        ),
        (
            "table_reference_hash_contract",
            NO_SIGNAL_REFERENCE_HASH_CONTRACT_VERSION,
        ),
        (
            "view_outcome_hash_contract",
            NO_SIGNAL_OUTCOME_HASH_CONTRACT_VERSION,
        ),
        (
            "table_outcome_hash_contract",
            NO_SIGNAL_OUTCOME_HASH_CONTRACT_VERSION,
        ),
    ):
        if row.get(field_name) != expected_value:
            raise ReaderAttestationError(
                "no-signal outcome payload hash contract receipt mismatch: "
                + field_name
            )
    return {
        "no_signal_outcomes_view_attested": True,
        "no_signal_outcomes_table_attested": True,
        "no_signal_writer_authority_attested": True,
        "raw_no_signal_outcomes_access_absent": True,
        "no_signal_outcomes_view_contract_version": (
            NO_SIGNAL_OUTCOME_VIEW_CONTRACT_VERSION
        ),
        "no_signal_outcomes_view_definition_sha256": definition_sha256,
        "no_signal_outcomes_stage4_source_catalog_sha256": (
            stage4_source_catalog_sha256
        ),
        "no_signal_outcomes_raw_catalog_sha256": raw_catalog_sha256,
        "no_signal_outcomes_trigger_catalog_sha256": (
            trigger_catalog_sha256
        ),
        "no_signal_reference_hash_contract_version": (
            NO_SIGNAL_REFERENCE_HASH_CONTRACT_VERSION
        ),
        "no_signal_outcome_hash_contract_version": (
            NO_SIGNAL_OUTCOME_HASH_CONTRACT_VERSION
        ),
    }


def _validate_projection_key_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    analysis_as_of_utc: datetime,
    lower_bound_utc: datetime,
    maturity_cutoff_utc: datetime,
    projection_limit: int,
    before_cursor: Optional[Mapping[str, Any]],
) -> tuple[tuple[Dict[str, Any], ...], bool]:
    if len(rows) > projection_limit + 1:
        raise CohortIntegrityError("projection keyset read exceeded its bound")
    required = {
        "projection_event_id",
        "snapshot_key",
        "projection_decision_time_utc",
        "projection_created_at_utc",
        "event_type",
    }
    normalized: list[Dict[str, Any]] = []
    seen_ids: set[int] = set()
    seen_keys: set[str] = set()
    previous_order: Optional[tuple[datetime, int]] = None
    cursor_order = None
    if before_cursor is not None:
        cursor_order = (
            _utc(
                before_cursor["projection_decision_time_utc"],
                field="before_cursor projection_decision_time_utc",
            ),
            _positive_int(
                before_cursor["projection_event_id"],
                field="before_cursor projection_event_id",
            ),
        )
    for raw in rows:
        row = dict(raw)
        absent = required - set(row)
        if absent:
            raise CohortIntegrityError(
                "projection keyset row omits columns: "
                + ", ".join(sorted(absent))
            )
        event_id = _positive_int(
            row.get("projection_event_id"), field="projection_event_id"
        )
        snapshot_key = _hash(row.get("snapshot_key"), field="snapshot_key")
        decision = _utc(
            row.get("projection_decision_time_utc"),
            field="projection_decision_time_utc",
        )
        created = _utc(
            row.get("projection_created_at_utc"),
            field="projection_created_at_utc",
        )
        if row.get("event_type") != exploration.PROJECTION_EVENT_TYPE:
            raise CohortIntegrityError("projection keyset returned a non-projection")
        if (
            decision < lower_bound_utc
            or decision > maturity_cutoff_utc
            or decision > analysis_as_of_utc
            or created < decision
            or created > analysis_as_of_utc
        ):
            raise CohortIntegrityError(
                "projection keyset timestamps are outside the causal corpus cut"
            )
        order = (decision, event_id)
        if cursor_order is not None and not order < cursor_order:
            raise CohortIntegrityError(
                "projection keyset row is outside the requested before_cursor"
            )
        if event_id in seen_ids or snapshot_key in seen_keys:
            raise CohortIntegrityError(
                "projection keyset returned duplicate projection identity"
            )
        if previous_order is not None and not order < previous_order:
            raise CohortIntegrityError(
                "projection keyset order is not strictly descending"
            )
        seen_ids.add(event_id)
        seen_keys.add(snapshot_key)
        previous_order = order
        normalized.append(
            {
                "projection_event_id": event_id,
                "snapshot_key": snapshot_key,
                "projection_decision_time_utc": _iso(
                    decision, field="projection_decision_time_utc"
                ),
                "projection_created_at_utc": _iso(
                    created, field="projection_created_at_utc"
                ),
            }
        )
    has_more = len(normalized) > projection_limit
    return tuple(normalized[:projection_limit]), has_more


def _validate_outcome_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    source_event_ids: Sequence[int],
    horizon_minutes: int,
    analysis_as_of_utc: datetime,
) -> tuple[Dict[str, Any], ...]:
    allowed_ids = set(source_event_ids)
    if len(rows) > len(allowed_ids):
        raise CohortIntegrityError("authoritative outcome read exceeded its bound")
    normalized: list[Dict[str, Any]] = []
    seen_ids: set[int] = set()
    previous_id = 0
    for raw in rows:
        row = dict(raw)
        absent = set(OUTCOME_VIEW_COLUMNS) - set(row)
        if absent:
            raise CohortIntegrityError(
                "outcome view row omits columns: " + ", ".join(sorted(absent))
            )
        event_id = _positive_int(row.get("event_id"), field="outcome event_id")
        if event_id not in allowed_ids:
            raise CohortIntegrityError(
                "outcome view returned an event outside the bounded source set"
            )
        if event_id in seen_ids:
            raise CohortIntegrityError("outcome view returned duplicate event identity")
        if previous_id and event_id <= previous_id:
            raise CohortIntegrityError("outcome view order is not strictly ascending")
        if (
            type(row.get("horizon_minutes")) is not int
            or row.get("horizon_minutes") != horizon_minutes
        ):
            raise CohortIntegrityError("outcome view returned the wrong horizon")
        measured = _utc(
            row.get("measured_at_utc"), field="outcome measured_at_utc"
        )
        created = _utc(
            row.get("outcome_created_at"), field="outcome_created_at"
        )
        if measured > analysis_as_of_utc or created > analysis_as_of_utc:
            raise CohortIntegrityError("outcome view returned a future row")
        if created < measured:
            raise CohortIntegrityError("outcome creation precedes path measurement")
        row["event_id"] = event_id
        row["measured_at_utc"] = _iso(
            measured, field="outcome measured_at_utc"
        )
        row["outcome_created_at"] = _iso(
            created, field="outcome_created_at"
        )
        normalized.append(row)
        seen_ids.add(event_id)
        previous_id = event_id
    return tuple(normalized)


def _canonical_receipt_timestamp(
    value: Any, *, field: str, nullable: bool = False
) -> Optional[str]:
    if value is None and nullable:
        return None
    if type(value) is not str:
        raise CohortIntegrityError(f"{field} is not canonical UTC text")
    canonical = _iso(value, field=field)
    if value != canonical:
        raise CohortIntegrityError(f"{field} is not canonical UTC text")
    return canonical


def _required_receipt_text(
    value: Any, *, field: str, allow_empty: bool = False
) -> str:
    if type(value) is not str or (not allow_empty and not value):
        raise CohortIntegrityError(f"{field} is not canonical text")
    return value


def _no_signal_reference_receipt_hash(
    reference: Mapping[str, Any],
    *,
    projection_event_id: int,
    projection_event_fingerprint: str,
    snapshot_set_id: int,
    snapshot_key: str,
    symbol: str,
    reference_price: Any,
) -> str:
    expected_keys = {
        "contract_version",
        "projection_event_id",
        "projection_event_fingerprint",
        "snapshot_set_id",
        "snapshot_key",
        "set_payload_sha256",
        "symbol",
        "symbol_manifest_payload_sha256",
        "source_timeframe",
        "snapshot_row_id",
        "snapshot_row_payload_sha256",
        "official_price",
    }
    if set(reference) != expected_keys:
        raise CohortIntegrityError(
            "no-signal outcome reference receipt shape mismatch"
        )
    official_price = reference.get("official_price")
    if not isinstance(official_price, Mapping) or set(official_price) != {
        "price",
        "source",
        "exchange",
        "market",
        "pair",
        "instrument",
        "interval",
        "fetched_at_utc",
        "observed_at_utc",
        "candle_open_time_utc",
        "candle_close_time_utc",
        "policy_status",
    }:
        raise CohortIntegrityError(
            "no-signal outcome official-price receipt shape mismatch"
        )
    reference_contract = _required_receipt_text(
        reference.get("contract_version"),
        field="no-signal reference contract_version",
    )
    if (
        reference_contract
        != exploration.STAGE4_NO_SIGNAL_OUTCOME_REFERENCE_POLICY_VERSION
        or reference.get("projection_event_id") != projection_event_id
        or reference.get("snapshot_set_id") != snapshot_set_id
        or reference.get("symbol") != symbol
        or reference.get("source_timeframe") != "12h"
    ):
        raise CohortIntegrityError(
            "no-signal outcome reference receipt identity mismatch"
        )
    reference_fingerprint = _hash(
        reference.get("projection_event_fingerprint"),
        field="no-signal reference projection_event_fingerprint",
    )
    reference_snapshot_key = _hash(
        reference.get("snapshot_key"),
        field="no-signal reference snapshot_key",
    )
    if (
        reference_fingerprint != projection_event_fingerprint
        or reference_snapshot_key != snapshot_key
    ):
        raise CohortIntegrityError(
            "no-signal outcome reference receipt identity mismatch"
        )
    set_payload_sha256 = _hash(
        reference.get("set_payload_sha256"),
        field="no-signal reference set_payload_sha256",
    )
    manifest_sha256 = _hash(
        reference.get("symbol_manifest_payload_sha256"),
        field="no-signal reference symbol_manifest_payload_sha256",
    )
    snapshot_row_id = _positive_int(
        reference.get("snapshot_row_id"),
        field="no-signal reference snapshot_row_id",
    )
    snapshot_row_sha256 = _hash(
        reference.get("snapshot_row_payload_sha256"),
        field="no-signal reference snapshot_row_payload_sha256",
    )
    official_price_hex = _float8_hex(
        official_price.get("price"),
        field="no-signal reference official_price.price",
    )
    if official_price_hex != _float8_hex(
        reference_price, field="no-signal reference_price"
    ):
        raise CohortIntegrityError(
            "no-signal outcome official price conflicts with reference_price"
        )
    fetched_at = _canonical_receipt_timestamp(
        official_price.get("fetched_at_utc"),
        field="no-signal reference official_price.fetched_at_utc",
    )
    observed_at = _canonical_receipt_timestamp(
        official_price.get("observed_at_utc"),
        field="no-signal reference official_price.observed_at_utc",
    )
    candle_open = _canonical_receipt_timestamp(
        official_price.get("candle_open_time_utc"),
        field="no-signal reference official_price.candle_open_time_utc",
        nullable=True,
    )
    candle_close = _canonical_receipt_timestamp(
        official_price.get("candle_close_time_utc"),
        field="no-signal reference official_price.candle_close_time_utc",
        nullable=True,
    )
    values = (
        NO_SIGNAL_REFERENCE_HASH_CONTRACT_VERSION,
        reference_contract,
        str(projection_event_id),
        reference_fingerprint,
        str(snapshot_set_id),
        reference_snapshot_key,
        set_payload_sha256,
        symbol,
        manifest_sha256,
        "12h",
        str(snapshot_row_id),
        snapshot_row_sha256,
        official_price_hex,
        _required_receipt_text(
            official_price.get("source"),
            field="no-signal reference official_price.source",
        ),
        _required_receipt_text(
            official_price.get("exchange"),
            field="no-signal reference official_price.exchange",
        ),
        _required_receipt_text(
            official_price.get("market"),
            field="no-signal reference official_price.market",
        ),
        _required_receipt_text(
            official_price.get("pair"),
            field="no-signal reference official_price.pair",
        ),
        _required_receipt_text(
            official_price.get("instrument"),
            field="no-signal reference official_price.instrument",
            allow_empty=True,
        ),
        _required_receipt_text(
            official_price.get("interval"),
            field="no-signal reference official_price.interval",
        ),
        fetched_at,
        observed_at,
        candle_open,
        candle_close,
        _required_receipt_text(
            official_price.get("policy_status"),
            field="no-signal reference official_price.policy_status",
        ),
    )
    return _tagged_sha256(
        NO_SIGNAL_REFERENCE_HASH_TAGS,
        values,
        field="no-signal reference receipt",
    )


def _no_signal_outcome_payload_hash(
    row: Mapping[str, Any],
    *,
    projection_event_id: int,
    projection_event_fingerprint: str,
    snapshot_set_id: int,
    snapshot_key: str,
    symbol: str,
    direction: str,
    horizon_minutes: int,
    decision_time_utc: datetime,
    measured_at_utc: datetime,
    cell_identity_sha256: str,
    reference_receipt_sha256: str,
) -> str:
    values = (
        NO_SIGNAL_OUTCOME_HASH_CONTRACT_VERSION,
        NO_SIGNAL_CARRIER_CONTRACT_VERSION,
        str(projection_event_id),
        projection_event_fingerprint,
        str(snapshot_set_id),
        snapshot_key,
        symbol,
        direction,
        str(horizon_minutes),
        _iso(decision_time_utc, field="no-signal decision_time_utc"),
        _required_receipt_text(
            row.get("absence_basis"), field="no-signal absence_basis"
        ),
        cell_identity_sha256,
        reference_receipt_sha256,
        _iso(measured_at_utc, field="no-signal measured_at_utc"),
        _float8_hex(row.get("reference_price"), field="no-signal reference_price"),
        _float8_hex(
            row.get("price_at_horizon"), field="no-signal price_at_horizon"
        ),
        _float8_hex(row.get("raw_return_pct"), field="no-signal raw_return_pct"),
        _float8_hex(
            row.get("directional_return_pct"),
            field="no-signal directional_return_pct",
        ),
        _float8_hex(
            row.get("max_favorable_price"),
            field="no-signal max_favorable_price",
        ),
        _float8_hex(
            row.get("max_adverse_price"), field="no-signal max_adverse_price"
        ),
        _float8_hex(row.get("mfe_pct"), field="no-signal mfe_pct"),
        _float8_hex(row.get("mae_pct"), field="no-signal mae_pct"),
        _nonnegative_int_text(
            row.get("time_to_first_progress_seconds"),
            field="no-signal time_to_first_progress_seconds",
            nullable=True,
        ),
        _nonnegative_int_text(
            row.get("time_to_mfe_seconds"),
            field="no-signal time_to_mfe_seconds",
        ),
        _nonnegative_int_text(
            row.get("path_resolution_seconds"),
            field="no-signal path_resolution_seconds",
        ),
        _nonnegative_int_text(
            row.get("path_samples"), field="no-signal path_samples"
        ),
        _required_receipt_text(
            row.get("outcome_method_version"),
            field="no-signal outcome_method_version",
        ),
        _required_receipt_text(
            row.get("price_source"), field="no-signal price_source"
        ),
        _required_receipt_text(
            row.get("data_quality_status"),
            field="no-signal data_quality_status",
        ),
    )
    return _tagged_sha256(
        NO_SIGNAL_OUTCOME_HASH_TAGS,
        values,
        field="no-signal outcome payload",
    )


def _validate_no_signal_outcome_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    no_signal_cells: Mapping[tuple[int, str, str], Mapping[str, Any]],
    horizon_minutes: int,
    analysis_as_of_utc: datetime,
) -> tuple[Dict[str, Any], ...]:
    allowed_cells = set(no_signal_cells)
    if len(rows) > len(allowed_cells):
        raise CohortIntegrityError(
            "authoritative no-signal outcome read exceeded its bound"
        )
    normalized: list[Dict[str, Any]] = []
    seen: set[tuple[int, str, str]] = set()
    previous: Optional[tuple[int, str, str]] = None
    for raw in rows:
        row = dict(raw)
        absent = set(NO_SIGNAL_OUTCOME_VIEW_COLUMNS) - set(row)
        if absent:
            raise CohortIntegrityError(
                "no-signal outcome view row omits columns: "
                + ", ".join(sorted(absent))
            )
        projection_event_id = _positive_int(
            row.get("projection_event_id"),
            field="no-signal projection_event_id",
        )
        symbol = str(row.get("symbol") or "").strip().upper()
        direction = str(row.get("direction") or "").strip().upper()
        if row.get("symbol") != symbol or row.get("direction") != direction:
            raise CohortIntegrityError(
                "no-signal outcome cell identity is not canonical"
            )
        cell = (projection_event_id, symbol, direction)
        expected = no_signal_cells.get(cell)
        if expected is None:
            raise CohortIntegrityError(
                "no-signal outcome view returned a cell outside the bounded cohort"
            )
        if cell in seen:
            raise CohortIntegrityError(
                "no-signal outcome view returned a duplicate cell"
            )
        if previous is not None and cell <= previous:
            raise CohortIntegrityError(
                "no-signal outcome view order is not strictly ascending"
            )
        if (
            type(row.get("horizon_minutes")) is not int
            or row.get("horizon_minutes") != horizon_minutes
        ):
            raise CohortIntegrityError(
                "no-signal outcome view returned the wrong horizon"
            )
        projection_fingerprint = _hash(
            row.get("projection_event_fingerprint"),
            field="no-signal projection_event_fingerprint",
        )
        snapshot_set_id = _positive_int(
            row.get("snapshot_set_id"),
            field="no-signal snapshot_set_id",
        )
        snapshot_key = _hash(
            row.get("snapshot_key"), field="no-signal snapshot_key"
        )
        decision = _utc(
            row.get("decision_time_utc"),
            field="no-signal decision_time_utc",
        )
        if (
            projection_fingerprint
            != expected.get("projection_event_fingerprint")
            or snapshot_set_id != expected.get("snapshot_set_id")
            or snapshot_key != expected.get("snapshot_key")
            or decision
            != _utc(
                expected.get("projection_decision_time_utc"),
                field="expected projection_decision_time_utc",
            )
            or row.get("absence_basis")
            != exploration.NO_SIGNAL_ABSENCE_BASIS
        ):
            raise CohortIntegrityError(
                "no-signal outcome view returned a mismatched cell identity"
            )
        expected_cell_identity = hashlib.sha256(
            (
                "stage4-explicit-no-signal-outcome-carrier-v1|"
                f"{projection_fingerprint}|{symbol}|{direction}|"
                f"{horizon_minutes}"
            ).encode("utf-8")
        ).hexdigest()
        cell_identity_sha256 = _hash(
            row.get("cell_identity_sha256"),
            field="no-signal cell_identity_sha256",
        )
        if cell_identity_sha256 != expected_cell_identity:
            raise CohortIntegrityError(
                "no-signal outcome cell identity receipt mismatch"
            )
        reference_receipt_sha256 = _hash(
            row.get("reference_receipt_sha256"),
            field="no-signal reference_receipt_sha256",
        )
        outcome_payload_sha256 = _hash(
            row.get("outcome_payload_sha256"),
            field="no-signal outcome_payload_sha256",
        )
        reference = row.get("reference_receipt")
        if not isinstance(reference, Mapping):
            raise CohortIntegrityError(
                "no-signal outcome reference receipt is malformed"
            )
        expected_reference_receipt_sha256 = _no_signal_reference_receipt_hash(
            reference,
            projection_event_id=projection_event_id,
            projection_event_fingerprint=projection_fingerprint,
            snapshot_set_id=snapshot_set_id,
            snapshot_key=snapshot_key,
            symbol=symbol,
            reference_price=row.get("reference_price"),
        )
        if reference_receipt_sha256 != expected_reference_receipt_sha256:
            raise CohortIntegrityError(
                "no-signal outcome reference receipt hash mismatch"
            )
        measured = _utc(
            row.get("measured_at_utc"),
            field="no-signal measured_at_utc",
        )
        created = _utc(
            row.get("outcome_created_at"),
            field="no-signal outcome_created_at",
        )
        if (
            measured > analysis_as_of_utc
            or created > analysis_as_of_utc
            or created < measured
        ):
            raise CohortIntegrityError(
                "no-signal outcome view returned a non-causal row"
            )
        expected_outcome_payload_sha256 = _no_signal_outcome_payload_hash(
            row,
            projection_event_id=projection_event_id,
            projection_event_fingerprint=projection_fingerprint,
            snapshot_set_id=snapshot_set_id,
            snapshot_key=snapshot_key,
            symbol=symbol,
            direction=direction,
            horizon_minutes=horizon_minutes,
            decision_time_utc=decision,
            measured_at_utc=measured,
            cell_identity_sha256=cell_identity_sha256,
            reference_receipt_sha256=reference_receipt_sha256,
        )
        if outcome_payload_sha256 != expected_outcome_payload_sha256:
            raise CohortIntegrityError(
                "no-signal outcome payload hash mismatch"
            )
        row.update(
            {
                "projection_event_id": projection_event_id,
                "projection_event_fingerprint": projection_fingerprint,
                "snapshot_set_id": snapshot_set_id,
                "snapshot_key": snapshot_key,
                "symbol": symbol,
                "direction": direction,
                "decision_time_utc": _iso(
                    decision, field="no-signal decision_time_utc"
                ),
                "measured_at_utc": _iso(
                    measured, field="no-signal measured_at_utc"
                ),
                "outcome_created_at": _iso(
                    created, field="no-signal outcome_created_at"
                ),
            }
        )
        normalized.append(row)
        seen.add(cell)
        previous = cell
    return tuple(normalized)


def _assert_attached_outcomes_authoritative(
    observations: Sequence[exploration.ExplorationObservation],
) -> None:
    for observation in observations:
        outcome = observation.to_dict().get("outcome") or {}
        reason = str(outcome.get("reason") or "")
        if reason.startswith((
            "INVALID_STAGE4_SIGNAL_OUTCOME:",
            "INVALID_STAGE4_NO_SIGNAL_OUTCOME:",
        )):
            raise CohortIntegrityError(
                "authoritative outcome failed closed validation: "
                + reason.partition(":")[2]
            )


def load_authoritative_stage4_wave(
    snapshot_key: str,
    database_url: Optional[str] = None,
) -> AuthoritativeStage4WaveResult:
    """Load and validate one Stage-4 projection and its exact Wave-v5 slot.

    The analysis cut is always PostgreSQL's transaction timestamp.  Historical
    caller-selected cuts are intentionally unsupported because the source
    tables do not provide a complete bitemporal update history.
    """

    normalized_key = _hash(snapshot_key, field="snapshot_key")
    url = _configured_database_url(database_url)
    conn: Any = None
    result: Optional[AuthoritativeStage4WaveResult] = None
    primary_error: Optional[BaseException] = None
    primary_traceback: Any = None
    try:
        conn = psycopg.connect(
            url,
            row_factory=dict_row,
            connect_timeout=5,
            autocommit=True,
            options=CONNECTION_OPTIONS,
        )
        conn.execute(_BEGIN_SQL)
        conn.execute(_SCHEMA_LOCK_SQL, (SCHEMA_LOCK_ID,))
        as_of, session_attestation = _validate_session(
            _fetchone(conn.execute(_SESSION_SQL)),
            expected_database_name=_database_target(url)[3],
        )
        # These probes acquire relation/source AccessShare locks before the
        # catalog check, closing the approved migration/DDL TOCTOU window.
        conn.execute(_PROBE_STAGE4_SQL)
        conn.execute(_PROBE_WAVE_SQL)
        schema_attestation = _validate_attestation(
            _fetchone(conn.execute(_ATTESTATION_SQL))
        )

        stage4_rows = _fetchall(
            conn.execute(_LOAD_STAGE4_SQL, (normalized_key, MAX_STAGE4_ROWS + 1))
        )
        projection, archive_set, signals = _validate_stage4_rows(
            stage4_rows, snapshot_key=normalized_key, as_of=as_of
        )
        frames = exploration.build_stage4_frames(
            projection,
            archive_set,
            signals,
            analysis_as_of_utc=as_of,
        )
        expected_slot = _utc(
            archive_set["cycle_time_utc"], field="archive cycle_time_utc"
        ) + timedelta(minutes=2)
        wave_rows = _fetchall(
            conn.execute(_LOAD_WAVE_SQL, (expected_slot, MAX_WAVE_ROWS + 1))
        )
        memberships, transitions = _validate_wave_rows(
            wave_rows, expected_slot=expected_slot, as_of=as_of
        )
        observations = exploration.bind_wave_v5(
            frames,
            memberships,
            transitions,
            analysis_as_of_utc=as_of,
        )
        source_attestation = {
            "source_contract_version": SOURCE_CONTRACT_VERSION,
            **session_attestation,
            **schema_attestation,
            "stage4_view": STAGE4_VIEW,
            "wave_view": WAVE_VIEW,
            "source_authority_attested": True,
            "formula_registry_effect": "NONE",
            "authority_effect": "NONE",
            "delivery_channel": "NONE",
            "live_eligible": False,
            "telegram_delivery_allowed": False,
            "trade_execution_allowed": False,
        }
        payload = {
            "source_contract_version": SOURCE_CONTRACT_VERSION,
            "analysis_as_of_utc": _iso(as_of, field="analysis_as_of_utc"),
            "database_snapshot_id": session_attestation["database_snapshot_id"],
            "source_attestation": source_attestation,
            "projection_event": projection,
            "archive_set": archive_set,
            "signal_events": list(signals),
            "memberships": list(memberships),
            "transitions": list(transitions),
            "observations": [item.to_dict() for item in observations],
            "formula_registry_effect": "NONE",
            "authority_effect": "NONE",
            "delivery_channel": "NONE",
            "live_eligible": False,
            "telegram_delivery_allowed": False,
            "trade_execution_allowed": False,
        }
        result = AuthoritativeStage4WaveResult._from_payload(payload)
    except BaseException as exc:
        primary_error = exc
        primary_traceback = exc.__traceback__

    cleanup_errors: list[BaseException] = []
    if conn is not None:
        try:
            conn.rollback()
        except BaseException as exc:
            cleanup_errors.append(exc)
        try:
            conn.close()
        except BaseException as exc:
            cleanup_errors.append(exc)

    if primary_error is not None:
        if isinstance(primary_error, AuthoritativeReaderError):
            raise primary_error.with_traceback(primary_traceback)
        if isinstance(primary_error, Exception):
            raise AuthoritativeReaderError(
                "authoritative Stage-4/Wave read failed: "
                f"{type(primary_error).__name__}: {primary_error}"
            ) from primary_error
        raise primary_error.with_traceback(primary_traceback)

    if cleanup_errors:
        cleanup_error = cleanup_errors[0]
        if not isinstance(cleanup_error, Exception):
            raise cleanup_error.with_traceback(cleanup_error.__traceback__)
        kinds = ", ".join(type(item).__name__ for item in cleanup_errors)
        raise AuthoritativeReaderError(
            f"authoritative reader cleanup failed: {kinds}"
        ) from cleanup_error

    if result is None:  # pragma: no cover - every path assigns or raises
        raise AuthoritativeReaderError(
            "authoritative Stage-4/Wave read produced no result"
        )
    return result


def load_authoritative_stage4_corpus(
    *,
    horizon_minutes: int,
    lookback_days: int = 120,
    projection_limit: int = 128,
    before_cursor: Optional[Mapping[str, Any]] = None,
    database_url: Optional[str] = None,
) -> AuthoritativeStage4CorpusResult:
    """Load one bounded, causally closed Stage-4 exploration corpus page.

    Projection selection, complete sibling cohorts, exact ``cycle + 2m`` Wave
    rows, and closed outcome labels are all read under the same read-only
    repeatable-read transaction.  Pagination is strict keyset pagination over
    ``(projection decision time, projection event id) DESC``.
    """

    if type(horizon_minutes) is not int or horizon_minutes not in {
        60, 240, 720, 1440
    }:
        raise ReaderConfigurationError(
            "horizon_minutes must be 60, 240, 720 or 1440"
        )
    normalized_lookback = _bounded_configuration_int(
        lookback_days, field="lookback_days", maximum=MAX_LOOKBACK_DAYS
    )
    normalized_limit = _bounded_configuration_int(
        projection_limit,
        field="projection_limit",
        maximum=MAX_PROJECTION_LIMIT,
    )
    initial_cursor = _normalized_before_cursor(before_cursor)
    url = _configured_database_url(database_url)
    conn: Any = None
    result: Optional[AuthoritativeStage4CorpusResult] = None
    primary_error: Optional[BaseException] = None
    primary_traceback: Any = None
    try:
        conn = psycopg.connect(
            url,
            row_factory=dict_row,
            connect_timeout=5,
            autocommit=True,
            options=CONNECTION_OPTIONS,
        )
        conn.execute(_BEGIN_SQL)
        conn.execute(_SCHEMA_LOCK_SQL, (SCHEMA_LOCK_ID,))
        as_of, session_attestation = _validate_session(
            _fetchone(conn.execute(_SESSION_SQL)),
            expected_database_name=_database_target(url)[3],
        )
        normalized_cursor = _normalized_before_cursor(
            initial_cursor, analysis_as_of_utc=as_of
        )

        # Probe all four relations first so their AccessShare locks close the
        # migration/DDL TOCTOU window before either catalog attestation.
        conn.execute(_PROBE_STAGE4_SQL)
        conn.execute(_PROBE_WAVE_SQL)
        conn.execute(_PROBE_OUTCOMES_SQL)
        conn.execute(_PROBE_NO_SIGNAL_OUTCOMES_SQL)
        schema_attestation = _validate_attestation(
            _fetchone(conn.execute(_ATTESTATION_SQL))
        )
        outcome_attestation = _validate_outcomes_attestation(
            _fetchone(conn.execute(_OUTCOMES_ATTESTATION_SQL)),
            expected_source_catalog_sha256=schema_attestation[
                "source_catalog_sha256"
            ],
        )
        no_signal_outcome_attestation = (
            _validate_no_signal_outcomes_attestation(
                _fetchone(conn.execute(_NO_SIGNAL_OUTCOMES_ATTESTATION_SQL)),
                expected_source_catalog_sha256=schema_attestation[
                    "source_catalog_sha256"
                ],
            )
        )

        lower_bound = as_of - timedelta(days=normalized_lookback)
        maturity_cutoff = as_of - timedelta(minutes=horizon_minutes)
        cursor_time = (
            None
            if normalized_cursor is None
            else normalized_cursor["projection_decision_time_utc"]
        )
        cursor_event_id = (
            None
            if normalized_cursor is None
            else normalized_cursor["projection_event_id"]
        )
        projection_key_rows = _fetchall(
            conn.execute(
                _LOAD_PROJECTION_KEYS_SQL,
                (
                    lower_bound,
                    maturity_cutoff,
                    cursor_time,
                    cursor_time,
                    cursor_event_id,
                    normalized_limit + 1,
                ),
            )
        )
        projection_keys, has_more = _validate_projection_key_rows(
            projection_key_rows,
            analysis_as_of_utc=as_of,
            lower_bound_utc=lower_bound,
            maturity_cutoff_utc=maturity_cutoff,
            projection_limit=normalized_limit,
            before_cursor=normalized_cursor,
        )

        observations: tuple[exploration.ExplorationObservation, ...] = ()
        stage4_event_count = 0
        signal_event_ids: set[int] = set()
        wave_row_count = 0
        outcome_rows: tuple[Dict[str, Any], ...] = ()
        no_signal_outcome_rows: tuple[Dict[str, Any], ...] = ()
        if projection_keys:
            snapshot_keys = [row["snapshot_key"] for row in projection_keys]
            raw_stage4_rows = _fetchall(
                conn.execute(
                    _LOAD_CORPUS_STAGE4_SQL,
                    (
                        snapshot_keys,
                        MAX_STAGE4_ROWS + 1,
                        MAX_CORPUS_STAGE4_ROWS + 1,
                    ),
                )
            )
            if len(raw_stage4_rows) > MAX_CORPUS_STAGE4_ROWS:
                raise CohortIntegrityError(
                    "authoritative corpus Stage-4 read exceeded its global bound"
                )
            stage4_by_key: Dict[str, list[Mapping[str, Any]]] = {
                key: [] for key in snapshot_keys
            }
            for raw in raw_stage4_rows:
                if "claimed_snapshot_key" not in raw:
                    raise CohortIntegrityError(
                        "Stage-4 corpus row omits claimed_snapshot_key"
                    )
                claimed_key = _hash(
                    raw.get("claimed_snapshot_key"),
                    field="claimed_snapshot_key",
                )
                if claimed_key not in stage4_by_key:
                    raise CohortIntegrityError(
                        "Stage-4 corpus returned an unrequested cohort"
                    )
                stage4_by_key[claimed_key].append(raw)

            frames: list[exploration.ExplorationObservation] = []
            expected_slots: set[datetime] = set()
            seen_event_ids: set[int] = set()
            seen_event_fingerprints: set[str] = set()
            for selected in projection_keys:
                snapshot_key = selected["snapshot_key"]
                cohort_rows = stage4_by_key[snapshot_key]
                projection, archive_set, signals = _validate_stage4_rows(
                    cohort_rows, snapshot_key=snapshot_key, as_of=as_of
                )
                if (
                    projection["event_id"] != selected["projection_event_id"]
                    or projection["alert_time_utc"]
                    != selected["projection_decision_time_utc"]
                ):
                    raise CohortIntegrityError(
                        "Stage-4 cohort projection differs from its keyset identity"
                    )
                cohort_events = (projection, *signals)
                for event in cohort_events:
                    event_id = int(event["event_id"])
                    fingerprint = str(event["event_fingerprint"])
                    if (
                        event_id in seen_event_ids
                        or fingerprint in seen_event_fingerprints
                    ):
                        raise CohortIntegrityError(
                            "Stage-4 corpus duplicated an event across cohorts"
                        )
                    seen_event_ids.add(event_id)
                    seen_event_fingerprints.add(fingerprint)
                signal_event_ids.update(int(row["event_id"]) for row in signals)
                stage4_event_count += len(cohort_events)
                cohort_frames = exploration.build_stage4_frames(
                    projection,
                    archive_set,
                    signals,
                    analysis_as_of_utc=as_of,
                )
                frames.extend(cohort_frames)
                expected_slot = _utc(
                    archive_set["cycle_time_utc"],
                    field="archive cycle_time_utc",
                ) + timedelta(minutes=2)
                if expected_slot > _utc(
                    selected["projection_decision_time_utc"],
                    field="projection_decision_time_utc",
                ):
                    raise CohortIntegrityError(
                        "Wave eligibility slot is after the Stage-4 decision"
                    )
                expected_slots.add(expected_slot)

            ordered_slots = sorted(expected_slots)
            raw_wave_rows = _fetchall(
                conn.execute(
                    _LOAD_CORPUS_WAVE_SQL,
                    (
                        ordered_slots,
                        MAX_WAVE_ROWS + 1,
                        MAX_CORPUS_WAVE_ROWS + 1,
                    ),
                )
            )
            if len(raw_wave_rows) > MAX_CORPUS_WAVE_ROWS:
                raise CohortIntegrityError(
                    "authoritative corpus Wave read exceeded its global bound"
                )
            wave_by_slot: Dict[datetime, list[Mapping[str, Any]]] = {
                slot: [] for slot in ordered_slots
            }
            for raw in raw_wave_rows:
                if "membership_eligible_at_utc" not in raw:
                    raise CohortIntegrityError(
                        "Wave corpus row omits membership_eligible_at_utc"
                    )
                slot = _utc(
                    raw.get("membership_eligible_at_utc"),
                    field="membership_eligible_at_utc",
                )
                if slot not in wave_by_slot:
                    raise CohortIntegrityError(
                        "Wave corpus returned a row outside requested exact slots"
                    )
                wave_by_slot[slot].append(raw)

            memberships: list[Dict[str, Any]] = []
            transitions: list[Dict[str, Any]] = []
            seen_membership_receipts: set[str] = set()
            seen_transition_receipts: set[str] = set()
            for expected_slot in ordered_slots:
                slot_memberships, slot_transitions = _validate_wave_rows(
                    wave_by_slot[expected_slot],
                    expected_slot=expected_slot,
                    as_of=as_of,
                )
                for membership in slot_memberships:
                    receipt = membership["membership_receipt_sha256"]
                    if receipt in seen_membership_receipts:
                        raise CohortIntegrityError(
                            "Wave corpus duplicated a membership across slots"
                        )
                    seen_membership_receipts.add(receipt)
                for transition in slot_transitions:
                    receipt = transition["transition_receipt_sha256"]
                    if receipt in seen_transition_receipts:
                        raise CohortIntegrityError(
                            "Wave corpus duplicated a transition across slots"
                        )
                    seen_transition_receipts.add(receipt)
                memberships.extend(slot_memberships)
                transitions.extend(slot_transitions)
            wave_row_count = len(raw_wave_rows)
            bound = exploration.bind_wave_v5(
                frames,
                memberships,
                transitions,
                analysis_as_of_utc=as_of,
            )

            bounded_source_ids = sorted(
                {
                    int(event_id)
                    for observation in bound
                    for event_id in observation.to_dict()["source_event_ids"]
                }
            )
            if set(bounded_source_ids) != signal_event_ids:
                raise CohortIntegrityError(
                    "Stage-4 observation source IDs do not cover the signal cohort"
                )
            no_signal_cells = {
                (
                    int(body["projection_event_id"]),
                    str(body["symbol"]),
                    str(body["direction"]),
                ): body
                for item in bound
                for body in (item.to_dict(),)
                if body.get("explicit_no_signal") is True
            }
            if bounded_source_ids:
                raw_outcomes = _fetchall(
                    conn.execute(
                        _LOAD_CORPUS_OUTCOMES_SQL,
                        (
                            bounded_source_ids,
                            horizon_minutes,
                            len(bounded_source_ids) + 1,
                        ),
                    )
                )
                outcome_rows = _validate_outcome_rows(
                    raw_outcomes,
                    source_event_ids=bounded_source_ids,
                    horizon_minutes=horizon_minutes,
                    analysis_as_of_utc=as_of,
                )
            if no_signal_cells:
                projection_event_ids = sorted(
                    {key[0] for key in no_signal_cells}
                )
                raw_no_signal_outcomes = _fetchall(
                    conn.execute(
                        _LOAD_CORPUS_NO_SIGNAL_OUTCOMES_SQL,
                        (
                            projection_event_ids,
                            horizon_minutes,
                            len(no_signal_cells) + 1,
                        ),
                    )
                )
                no_signal_outcome_rows = _validate_no_signal_outcome_rows(
                    raw_no_signal_outcomes,
                    no_signal_cells=no_signal_cells,
                    horizon_minutes=horizon_minutes,
                    analysis_as_of_utc=as_of,
                )
            observations = exploration.attach_closed_outcomes(
                bound,
                outcome_rows,
                no_signal_outcomes=no_signal_outcome_rows,
                horizon_minutes=horizon_minutes,
                analysis_as_of_utc=as_of,
            )
            _assert_attached_outcomes_authoritative(observations)

        available_outcomes = sum(
            item.to_dict()["outcome"].get("status") == "AVAILABLE"
            for item in observations
        )
        no_signal_observations = sum(
            item.to_dict().get("explicit_no_signal") is True
            for item in observations
        )
        distinct_btc_parent_movements = len(
            {
                str(item.to_dict()["wave_binding"][
                    "btc_parent_movement_id"
                ])
                for item in observations
                if item.to_dict()["wave_binding"].get("status") == "BOUND"
                and item.to_dict()["wave_binding"].get(
                    "btc_parent_movement_id"
                )
            }
        )
        readiness = exploration.dataset_readiness(
            observations,
            source_authority_attested=True,
            statistical_label_contract_implemented=True,
            wave_identity_candidate_search_implemented=True,
        )
        next_cursor = None
        if has_more and projection_keys:
            last = projection_keys[-1]
            next_cursor = {
                "projection_decision_time_utc": last[
                    "projection_decision_time_utc"
                ],
                "projection_event_id": last["projection_event_id"],
            }
        source_attestation = {
            "source_contract_version": CORPUS_SOURCE_CONTRACT_VERSION,
            **session_attestation,
            **schema_attestation,
            **outcome_attestation,
            **no_signal_outcome_attestation,
            "stage4_view": STAGE4_VIEW,
            "wave_view": WAVE_VIEW,
            "outcome_view": OUTCOME_VIEW,
            "no_signal_outcome_view": NO_SIGNAL_OUTCOME_VIEW,
            "source_authority_attested": True,
            "formula_registry_effect": "NONE",
            "authority_effect": "NONE",
            "delivery_channel": "NONE",
            "live_eligible": False,
            "telegram_delivery_allowed": False,
            "trade_execution_allowed": False,
        }
        payload = {
            "source_contract_version": CORPUS_SOURCE_CONTRACT_VERSION,
            "analysis_as_of_utc": _iso(as_of, field="analysis_as_of_utc"),
            "database_snapshot_id": session_attestation["database_snapshot_id"],
            "request": {
                "horizon_minutes": horizon_minutes,
                "lookback_days": normalized_lookback,
                "projection_limit": normalized_limit,
            },
            "cursor": {
                "order": (
                    "projection_decision_time_utc DESC, projection_event_id DESC"
                ),
                "before": normalized_cursor,
                "next": next_cursor,
                "has_more": has_more,
            },
            "counts": {
                "projections": len(projection_keys),
                "stage4_events": stage4_event_count,
                "signal_events": len(signal_event_ids),
                "wave_rows": wave_row_count,
                "outcome_rows": len(outcome_rows) + len(no_signal_outcome_rows),
                "signal_outcome_rows": len(outcome_rows),
                "no_signal_outcome_rows": len(no_signal_outcome_rows),
                "observations": len(observations),
                "available_outcomes": available_outcomes,
                "unavailable_outcomes": len(observations) - available_outcomes,
                "explicit_no_signal_observations": no_signal_observations,
                "distinct_btc_parent_movements": (
                    distinct_btc_parent_movements
                ),
            },
            "source_attestation": source_attestation,
            "observations": [item.to_dict() for item in observations],
            "dataset_readiness": readiness,
            "ready_for_candidate_search": readiness[
                "ready_for_formula_effect_research"
            ],
            "blockers": list(readiness["blockers"]),
            "formula_registry_effect": "NONE",
            "authority_effect": "NONE",
            "delivery_channel": "NONE",
            "live_eligible": False,
            "telegram_delivery_allowed": False,
            "trade_execution_allowed": False,
        }
        result = AuthoritativeStage4CorpusResult._from_payload(payload)
    except BaseException as exc:
        primary_error = exc
        primary_traceback = exc.__traceback__

    cleanup_errors: list[BaseException] = []
    if conn is not None:
        try:
            conn.rollback()
        except BaseException as exc:
            cleanup_errors.append(exc)
        try:
            conn.close()
        except BaseException as exc:
            cleanup_errors.append(exc)

    if primary_error is not None:
        if isinstance(primary_error, AuthoritativeReaderError):
            raise primary_error.with_traceback(primary_traceback)
        if isinstance(primary_error, Exception):
            raise AuthoritativeReaderError(
                "authoritative Stage-4 corpus read failed: "
                f"{type(primary_error).__name__}: {primary_error}"
            ) from primary_error
        raise primary_error.with_traceback(primary_traceback)

    if cleanup_errors:
        cleanup_error = cleanup_errors[0]
        if not isinstance(cleanup_error, Exception):
            raise cleanup_error.with_traceback(cleanup_error.__traceback__)
        kinds = ", ".join(type(item).__name__ for item in cleanup_errors)
        raise AuthoritativeReaderError(
            f"authoritative corpus reader cleanup failed: {kinds}"
        ) from cleanup_error

    if result is None:  # pragma: no cover - every path assigns or raises
        raise AuthoritativeReaderError(
            "authoritative Stage-4 corpus read produced no result"
        )
    return result


def descriptor() -> Dict[str, Any]:
    return {
        "source_contract_version": SOURCE_CONTRACT_VERSION,
        "corpus_source_contract_version": CORPUS_SOURCE_CONTRACT_VERSION,
        "database_url_env": DATABASE_URL_ENV,
        "trusted_reader_role": TRUSTED_READER_ROLE,
        "stage4_view": STAGE4_VIEW,
        "wave_view": WAVE_VIEW,
        "outcome_view": OUTCOME_VIEW,
        "no_signal_outcome_view": NO_SIGNAL_OUTCOME_VIEW,
        "transaction_isolation": "REPEATABLE READ",
        "transaction_read_only": True,
        "analysis_as_of_source": "POSTGRES_TRANSACTION_TIMESTAMP",
        "database_snapshot_source": "PG_CURRENT_SNAPSHOT",
        "schema_lock_mode": "SHARED_TRANSACTION_ADVISORY_LOCK",
        "schema_lock_id": SCHEMA_LOCK_ID,
        "schema_auto_create": False,
        "outcomes_loaded": True,
        "outcomes_loaded_capability": True,
        "runtime_wired": True,
        "runtime_wiring_scope": "DISCOVERY_INGESTION_OBSERVABILITY_ONLY",
        "candidate_search_runtime_wired": True,
        "candidate_search_readiness_evaluated_per_corpus": True,
        "ready_for_candidate_search": False,
        "blockers": list(CANDIDATE_SEARCH_BLOCKERS),
        "formula_registry_effect": "NONE",
        "authority_effect": "NONE",
        "delivery_channel": "NONE",
        "live_eligible": False,
        "telegram_delivery_allowed": False,
        "trade_execution_allowed": False,
    }
