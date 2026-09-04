"""Append-only PostgreSQL adapter for Market Movement Wave v5.

Runtime scheduling lives in :mod:`research_market_movement_worker`; this
adapter remains independently testable.  It accepts an explicit provider
callback, acquires the stable slot lock, reads an existing frozen anchor
*before* invoking that callback, and persists only canonical objects built by
:mod:`research_market_movement`.

Normal BTC processing advances its SYMBOL and BTC_PARENT streams in one
transaction.  Every non-BTC SYMBOL stream is independent: it advances from
its own frozen anchors even when the same-slot BTC_PARENT projection is
absent, and reports that optional parent projection when it already exists.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import os
from typing import Any, Callable, Mapping, Optional, Sequence

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - optional in pure/self-test environments
    psycopg = None
    dict_row = None

import research_market_movement as movement


ATTEMPT_TABLE = "public.research_price_collection_attempts"
ANCHOR_TABLE = "public.research_neutral_price_anchors"
TRANSITION_TABLE = "public.research_market_movement_transitions"
MEMBERSHIP_TABLE = "public.research_market_movement_memberships"
TRUSTED_WRITER_ROLE = "research_market_movement_writer_v5"
CONNECTION_OPTIONS = (
    "-c statement_timeout=15000 -c lock_timeout=3000 "
    "-c search_path=pg_catalog"
)


_LOCK_SQL = """
/* market_movement:lock */
SELECT pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(%(lock_key)s, 0)
)
"""

_LOAD_ANCHOR_SLOT_SQL = f"""
/* market_movement:load_anchor_slot */
SELECT * FROM {ANCHOR_TABLE}
 WHERE contract_version=%(contract_version)s
   AND symbol=%(symbol)s
   AND eligible_at_utc=%(eligible_at_utc)s
"""

_INSERT_ATTEMPT_SQL = f"""
/* market_movement:insert_attempt */
INSERT INTO {ATTEMPT_TABLE} (
    attempt_receipt_sha256, contract_version, symbol, eligible_at_utc,
    decision_time_utc, evaluation_status, evaluation_reason, anchor_id,
    anchor_receipt_sha256, attempt_receipt
) VALUES (
    %(attempt_receipt_sha256)s, %(contract_version)s, %(symbol)s,
    %(eligible_at_utc)s, %(decision_time_utc)s, %(evaluation_status)s,
    %(evaluation_reason)s, %(anchor_id)s, %(anchor_receipt_sha256)s,
    %(attempt_receipt)s::jsonb
)
ON CONFLICT (attempt_receipt_sha256) DO NOTHING
RETURNING attempt_receipt_sha256
"""

_LOAD_ATTEMPT_SQL = f"""
/* market_movement:load_attempt */
SELECT * FROM {ATTEMPT_TABLE}
 WHERE attempt_receipt_sha256=%(attempt_receipt_sha256)s
"""

_INSERT_ANCHOR_SQL = f"""
/* market_movement:insert_anchor */
INSERT INTO {ANCHOR_TABLE} (
    anchor_id, anchor_receipt_sha256, contract_version, symbol, origin,
    sampler_version, eligible_at_utc, decision_time_utc,
    source_price_candle_open_utc, source_price_candle_close_utc,
    observed_at_utc, refresh_completed_at_utc, price, source,
    upstream_source, price_exchange, price_market, price_pair,
    price_instrument_id, price_timeframe, quality_status, fallback_used,
    fallback_policy, price_candle_identity_basis, source_input_fingerprint,
    source_record_created_at_utc, anchor_receipt
) VALUES (
    %(anchor_id)s, %(anchor_receipt_sha256)s, %(contract_version)s,
    %(symbol)s, %(origin)s, %(sampler_version)s, %(eligible_at_utc)s,
    %(decision_time_utc)s, %(source_price_candle_open_utc)s,
    %(source_price_candle_close_utc)s, %(observed_at_utc)s,
    %(refresh_completed_at_utc)s, %(price)s, %(source)s,
    %(upstream_source)s, %(price_exchange)s, %(price_market)s,
    %(price_pair)s, %(price_instrument_id)s, %(price_timeframe)s,
    %(quality_status)s, %(fallback_used)s, %(fallback_policy)s,
    %(price_candle_identity_basis)s, %(source_input_fingerprint)s,
    %(source_record_created_at_utc)s, %(anchor_receipt)s::jsonb
)
ON CONFLICT (contract_version, symbol, eligible_at_utc) DO NOTHING
RETURNING anchor_id
"""

_LOAD_ANCHOR_ID_SQL = f"""
/* market_movement:load_anchor_id */
SELECT * FROM {ANCHOR_TABLE}
 WHERE anchor_id=%(anchor_id)s
"""

_LOAD_SYMBOL_ANCHORS_SQL = f"""
/* market_movement:load_symbol_anchors */
SELECT * FROM {ANCHOR_TABLE}
 WHERE contract_version=%(contract_version)s
   AND symbol=%(symbol)s
 ORDER BY eligible_at_utc ASC, anchor_id ASC
"""

_LOAD_CHAIN_SQL = f"""
/* market_movement:load_chain */
SELECT * FROM {TRANSITION_TABLE}
 WHERE stream_id=%(stream_id)s
 ORDER BY chain_ordinal ASC
"""

_INSERT_TRANSITION_SQL = f"""
/* market_movement:insert_transition */
INSERT INTO {TRANSITION_TABLE} (
    transition_receipt_sha256, contract_version,
    previous_transition_receipt_sha256, chain_ordinal, transition_type,
    stream_id, namespace, symbol, movement_id, trigger_anchor_id,
    trigger_eligible_at_utc, trigger_decision_time_utc, pre_state_sha256,
    post_state_sha256, post_state, transition_receipt
) VALUES (
    %(transition_receipt_sha256)s, %(contract_version)s,
    %(previous_transition_receipt_sha256)s, %(chain_ordinal)s,
    %(transition_type)s, %(stream_id)s, %(namespace)s, %(symbol)s,
    %(movement_id)s, %(trigger_anchor_id)s, %(trigger_eligible_at_utc)s,
    %(trigger_decision_time_utc)s, %(pre_state_sha256)s,
    %(post_state_sha256)s, %(post_state)s::jsonb,
    %(transition_receipt)s::jsonb
)
ON CONFLICT (stream_id, chain_ordinal) DO NOTHING
RETURNING transition_receipt_sha256
"""

_LOAD_TRANSITION_ORDINAL_SQL = f"""
/* market_movement:load_transition_ordinal */
SELECT * FROM {TRANSITION_TABLE}
 WHERE stream_id=%(stream_id)s
   AND chain_ordinal=%(chain_ordinal)s
"""

_INSERT_MEMBERSHIP_SQL = f"""
/* market_movement:insert_membership */
INSERT INTO {MEMBERSHIP_TABLE} (
    membership_receipt_sha256, emitted_by_transition_receipt_sha256,
    contract_version, stream_id, movement_id, anchor_id,
    anchor_receipt_sha256, ordinal, classification, eligible_at_utc,
    decision_time_utc, price, membership_receipt
) VALUES (
    %(membership_receipt_sha256)s,
    %(emitted_by_transition_receipt_sha256)s, %(contract_version)s,
    %(stream_id)s, %(movement_id)s, %(anchor_id)s,
    %(anchor_receipt_sha256)s, %(ordinal)s, %(classification)s,
    %(eligible_at_utc)s, %(decision_time_utc)s, %(price)s,
    %(membership_receipt)s::jsonb
)
ON CONFLICT (stream_id, anchor_id) DO NOTHING
RETURNING membership_receipt_sha256
"""

_LOAD_MEMBERSHIP_SQL = f"""
/* market_movement:load_membership */
SELECT * FROM {MEMBERSHIP_TABLE}
 WHERE stream_id=%(stream_id)s
   AND anchor_id=%(anchor_id)s
"""

_LOAD_STREAM_MEMBERSHIPS_SQL = f"""
/* market_movement:load_stream_memberships */
SELECT * FROM {MEMBERSHIP_TABLE}
 WHERE stream_id=%(stream_id)s
 ORDER BY eligible_at_utc ASC, anchor_id ASC
"""

_EARLIEST_PENDING_SQL = f"""
/* market_movement:earliest_pending */
SELECT anchor.*
  FROM {ANCHOR_TABLE} anchor
 WHERE anchor.contract_version=%(contract_version)s
   AND anchor.symbol=%(symbol)s
   AND NOT EXISTS (
       SELECT 1 FROM {MEMBERSHIP_TABLE} member
        WHERE member.stream_id=%(stream_id)s
          AND member.anchor_id=anchor.anchor_id
   )
 ORDER BY anchor.eligible_at_utc ASC, anchor.anchor_id ASC
 LIMIT 1
"""

_PARENT_MEMBERSHIP_AT_SLOT_SQL = f"""
/* market_movement:parent_membership_at_slot */
SELECT member.*
  FROM {MEMBERSHIP_TABLE} member
 WHERE member.stream_id=%(stream_id)s
   AND member.eligible_at_utc=%(eligible_at_utc)s
 ORDER BY member.membership_receipt_sha256
"""

_BTC_LOCAL_MEMBERSHIP_SQL = f"""
/* market_movement:btc_local_membership */
SELECT * FROM {MEMBERSHIP_TABLE}
 WHERE stream_id=%(stream_id)s
   AND anchor_id=%(anchor_id)s
"""

_VERIFY_WRITER_SQL = """
/* market_movement:verify_writer */
SELECT session_user::text AS session_user,
       current_user::text AS current_user
"""

_RUNTIME_PREFLIGHT_SQL = """
/* market_movement:runtime_preflight */
SELECT relation_name,
       pg_catalog.to_regclass('public.' || relation_name)::text AS relation,
       pg_catalog.has_schema_privilege(
           session_user, 'public', 'USAGE'
       ) AS schema_usage,
       pg_catalog.has_schema_privilege(
           session_user, 'public', 'CREATE'
       ) AS schema_create,
       (
           SELECT class.relkind::text
             FROM pg_catalog.pg_class class
            WHERE class.oid =
                  pg_catalog.to_regclass('public.' || relation_name)
       ) AS relation_kind,
       (
           SELECT pg_catalog.pg_get_userbyid(class.relowner)
             FROM pg_catalog.pg_class class
            WHERE class.oid =
                  pg_catalog.to_regclass('public.' || relation_name)
       ) AS relation_owner,
       CASE
           WHEN pg_catalog.to_regclass('public.' || relation_name) IS NULL
               THEN FALSE
           ELSE pg_catalog.has_table_privilege(
               session_user,
               pg_catalog.to_regclass('public.' || relation_name),
               'SELECT'
           )
       END AS writer_select,
       CASE
           WHEN pg_catalog.to_regclass('public.' || relation_name) IS NULL
               THEN FALSE
           ELSE pg_catalog.has_table_privilege(
               session_user,
               pg_catalog.to_regclass('public.' || relation_name),
               'INSERT'
           )
       END AS writer_insert,
       CASE
           WHEN pg_catalog.to_regclass('public.' || relation_name) IS NULL
               THEN FALSE
           ELSE pg_catalog.has_table_privilege(
                    session_user,
                    pg_catalog.to_regclass('public.' || relation_name),
                    'UPDATE'
                )
                OR pg_catalog.has_any_column_privilege(
                    session_user,
                    pg_catalog.to_regclass('public.' || relation_name),
                    'UPDATE'
                )
       END AS writer_update,
       CASE
           WHEN pg_catalog.to_regclass('public.' || relation_name) IS NULL
               THEN FALSE
           ELSE pg_catalog.has_table_privilege(
               session_user,
               pg_catalog.to_regclass('public.' || relation_name),
               'DELETE'
           )
       END AS writer_delete,
       CASE
           WHEN pg_catalog.to_regclass('public.' || relation_name) IS NULL
               THEN FALSE
           ELSE pg_catalog.has_table_privilege(
               session_user,
               pg_catalog.to_regclass('public.' || relation_name),
               'TRUNCATE'
           )
       END AS writer_truncate,
       CASE
           WHEN pg_catalog.to_regclass('public.' || relation_name) IS NULL
               THEN FALSE
           ELSE pg_catalog.has_table_privilege(
                    session_user,
                    pg_catalog.to_regclass('public.' || relation_name),
                    'REFERENCES'
                )
                OR pg_catalog.has_any_column_privilege(
                    session_user,
                    pg_catalog.to_regclass('public.' || relation_name),
                    'REFERENCES'
                )
       END AS writer_references,
       CASE
           WHEN pg_catalog.to_regclass('public.' || relation_name) IS NULL
               THEN FALSE
           ELSE pg_catalog.has_table_privilege(
               session_user,
               pg_catalog.to_regclass('public.' || relation_name),
               'TRIGGER'
           )
       END AS writer_trigger,
       NOT EXISTS (
           SELECT 1
             FROM pg_catalog.aclexplode(
                  COALESCE(
                      (
                          SELECT class.relacl
                            FROM pg_catalog.pg_class class
                           WHERE class.oid = pg_catalog.to_regclass(
                               'public.' || relation_name
                           )
                      ),
                      pg_catalog.acldefault(
                          'r',
                          (
                              SELECT class.relowner
                                FROM pg_catalog.pg_class class
                               WHERE class.oid = pg_catalog.to_regclass(
                                   'public.' || relation_name
                               )
                          )
                      )
                  )
             ) acl
            WHERE acl.grantee <> (
                      SELECT class.relowner
                        FROM pg_catalog.pg_class class
                       WHERE class.oid = pg_catalog.to_regclass(
                           'public.' || relation_name
                       )
                  )
              AND NOT (
                  acl.grantee = (
                      SELECT role.oid
                        FROM pg_catalog.pg_roles role
                       WHERE role.rolname =
                             'research_market_movement_writer_v5'
                  )
                  AND acl.privilege_type IN ('SELECT', 'INSERT')
                  AND NOT acl.is_grantable
              )
       ) AS relation_acl_safe,
       NOT EXISTS (
           SELECT 1
             FROM pg_catalog.pg_attribute attribute
            WHERE attribute.attrelid = pg_catalog.to_regclass(
                      'public.' || relation_name
                  )
              AND attribute.attnum > 0
              AND NOT attribute.attisdropped
              AND COALESCE(pg_catalog.cardinality(attribute.attacl), 0) <> 0
       ) AS column_acl_absent,
       COALESCE(
           (
               SELECT NOT (class.relrowsecurity OR class.relforcerowsecurity)
                 FROM pg_catalog.pg_class class
                WHERE class.oid = pg_catalog.to_regclass(
                          'public.' || relation_name
                      )
           ),
           FALSE
       ) AS rls_disabled,
       NOT EXISTS (
           SELECT 1
             FROM pg_catalog.pg_policy policy
            WHERE policy.polrelid = pg_catalog.to_regclass(
                      'public.' || relation_name
                  )
       ) AS policies_absent,
       NOT EXISTS (
           SELECT 1
             FROM pg_catalog.pg_rewrite rewrite_rule
            WHERE rewrite_rule.ev_class = pg_catalog.to_regclass(
                      'public.' || relation_name
                  )
       ) AS rewrite_rules_absent,
       COALESCE(
           (
               SELECT pg_catalog.array_agg(
                          trigger.tgname::text
                          ORDER BY trigger.tgname::text COLLATE pg_catalog."C"
                      )
                 FROM pg_catalog.pg_trigger trigger
                WHERE trigger.tgrelid = pg_catalog.to_regclass(
                          'public.' || relation_name
                      )
                  AND NOT trigger.tgisinternal
           ),
           ARRAY[]::text[]
       ) = expected_trigger_names
       AND NOT EXISTS (
           SELECT 1
             FROM pg_catalog.pg_trigger trigger
            WHERE trigger.tgrelid = pg_catalog.to_regclass(
                      'public.' || relation_name
                  )
              AND NOT trigger.tgisinternal
              AND trigger.tgenabled <> 'A'
       ) AS trigger_inventory_safe
  FROM (VALUES
       (
           'research_price_collection_attempts',
           ARRAY[
               'trg_market_movement_attempt_writer',
               'trg_neutral_price_attempt_complete',
               'trg_research_price_collection_attempts_append_only',
               'trg_research_price_collection_attempts_no_truncate'
           ]::text[]
       ),
       (
           'research_neutral_price_anchors',
           ARRAY[
               'trg_market_movement_anchor_writer',
               'trg_neutral_price_anchor_complete',
               'trg_research_neutral_price_anchors_append_only',
               'trg_research_neutral_price_anchors_no_truncate'
           ]::text[]
       ),
       (
           'research_market_movement_transitions',
           ARRAY[
               'trg_market_movement_transition_complete',
               'trg_market_movement_transition_writer',
               'trg_research_market_movement_transitions_append_only',
               'trg_research_market_movement_transitions_no_truncate',
               'trg_validate_market_movement_transition_insert'
           ]::text[]
       ),
       (
           'research_market_movement_memberships',
           ARRAY[
               'trg_market_movement_membership_complete',
               'trg_market_movement_membership_writer',
               'trg_research_market_movement_memberships_append_only',
               'trg_research_market_movement_memberships_no_truncate'
           ]::text[]
       )
  ) AS expected(relation_name, expected_trigger_names)
 ORDER BY relation_name
"""

_RUNTIME_ROLE_PREFLIGHT_SQL = """
/* market_movement:runtime_role_preflight */
SELECT role.rolname,
       role.rolcanlogin,
       role.rolinherit,
       role.rolsuper,
       role.rolcreatedb,
       role.rolcreaterole,
       role.rolreplication,
       role.rolbypassrls,
       owner_role.rolname AS owner_rolname,
       owner_role.rolcanlogin AS owner_rolcanlogin,
       owner_role.rolinherit AS owner_rolinherit,
       owner_role.rolsuper AS owner_rolsuper,
       owner_role.rolcreatedb AS owner_rolcreatedb,
       owner_role.rolcreaterole AS owner_rolcreaterole,
       owner_role.rolreplication AS owner_rolreplication,
       owner_role.rolbypassrls AS owner_rolbypassrls,
       (
           SELECT COUNT(*)::bigint
             FROM pg_catalog.pg_auth_members membership
            WHERE membership.member = role.oid
       ) AS memberships_held,
       (
           SELECT COUNT(*)::bigint
             FROM pg_catalog.pg_auth_members membership
            WHERE membership.roleid = role.oid
       ) AS members_granted_writer,
       (
           SELECT pg_catalog.pg_get_userbyid(database.datdba)
             FROM pg_catalog.pg_database database
            WHERE database.datname = pg_catalog.current_database()
       ) AS database_owner,
       (
           SELECT pg_catalog.pg_get_userbyid(namespace.nspowner)
             FROM pg_catalog.pg_namespace namespace
            WHERE namespace.nspname = 'public'
       ) AS public_schema_owner,
       pg_catalog.current_setting('transaction_read_only') AS transaction_read_only,
       pg_catalog.pg_is_in_recovery() AS in_recovery
  FROM pg_catalog.pg_roles role
  LEFT JOIN pg_catalog.pg_roles owner_role
    ON owner_role.rolname = 'research_market_movement_owner'
 WHERE role.rolname = session_user
"""

_SET_CONSTRAINTS_SQL = """
/* market_movement:set_constraints */
SET CONSTRAINTS ALL IMMEDIATE
"""


class MarketMovementStoreError(RuntimeError):
    """Base error for append-only Wave v5 storage."""


class MarketMovementConflictError(MarketMovementStoreError):
    """A stable natural key exists with different immutable content."""


class MarketMovementForkError(MarketMovementStoreError):
    """A stored stream is not one canonical transition chain."""


class ParentProjectionError(MarketMovementStoreError):
    """BTC parent/local atomicity or same-slot parent authority failed."""


@dataclass(frozen=True)
class ProcessResult:
    symbol: str
    status: str
    anchor_id: Optional[str]
    eligible_at_utc: Optional[datetime]
    local_movement_id: Optional[str]
    btc_parent_movement_id: Optional[str]
    transition_receipts: tuple[str, ...]
    membership_receipts: tuple[str, ...]


@dataclass(frozen=True)
class CaptureResult:
    symbol: str
    provider_called: bool
    idempotent_anchor: bool
    attempt_receipt_sha256: Optional[str]
    anchor: Optional[movement.NeutralPriceAnchor]
    processing: ProcessResult


@dataclass(frozen=True)
class HistoricalImportResult:
    anchor: movement.NeutralPriceAnchor
    idempotent_anchor: bool
    processing: ProcessResult


def _utc(value: Any, *, field: str = "timestamp") -> datetime:
    return movement._utc(value, field=field)


def _iso(value: Any) -> str:
    return movement._iso(value)


def _symbol(value: Any) -> str:
    return movement._symbol(value)


def _json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _json_value(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError as exc:
            raise MarketMovementConflictError("stored receipt is not JSON") from exc
    return value


def _fetchone(cursor: Any) -> Optional[Mapping[str, Any]]:
    row = cursor.fetchone()
    return dict(row) if row is not None else None


def _fetchall(cursor: Any) -> list[Mapping[str, Any]]:
    return [dict(row) for row in cursor.fetchall()]


def _same_time(left: Any, right: Any) -> bool:
    try:
        return _utc(left) == _utc(right)
    except (TypeError, ValueError):
        return False


def _same_decimal(left: Any, right: Any) -> bool:
    if isinstance(left, bool) or isinstance(right, bool):
        return False
    try:
        return Decimal(str(left)) == Decimal(str(right))
    except Exception:
        return False


def _assert_equal(field: str, actual: Any, expected: Any) -> None:
    if actual != expected:
        raise MarketMovementConflictError(
            f"stored {field} conflicts with canonical receipt"
        )


def _assert_char(field: str, actual: Any, expected: Any) -> None:
    normalized = actual.rstrip() if isinstance(actual, str) else actual
    if normalized != expected:
        raise MarketMovementConflictError(
            f"stored {field} conflicts with canonical receipt"
        )


def _assert_time(field: str, actual: Any, expected: Any) -> None:
    if not _same_time(actual, expected):
        raise MarketMovementConflictError(
            f"stored {field} conflicts with canonical receipt"
        )


def _attempt_params(attempt: movement.NeutralPriceAnchorAttempt) -> dict[str, Any]:
    payload = attempt.to_dict()
    return {
        **payload,
        "eligible_at_utc": attempt.eligible_at_utc,
        "decision_time_utc": attempt.decision_time_utc,
        "attempt_receipt": _json(payload),
    }


def _anchor_params(anchor: movement.NeutralPriceAnchor) -> dict[str, Any]:
    payload = anchor.to_dict()
    return {
        **payload,
        "eligible_at_utc": anchor.eligible_at_utc,
        "decision_time_utc": anchor.decision_time_utc,
        "source_price_candle_open_utc": anchor.source_price_candle_open_utc,
        "source_price_candle_close_utc": anchor.source_price_candle_close_utc,
        "observed_at_utc": anchor.observed_at_utc,
        "refresh_completed_at_utc": anchor.refresh_completed_at_utc,
        "price": anchor.price,
        "source_record_created_at_utc": anchor.source_record_created_at_utc,
        "anchor_receipt": _json(payload),
    }


def _transition_params(
    transition: movement.MovementTransition, *, chain_ordinal: int
) -> dict[str, Any]:
    payload = transition.to_dict()
    return {
        **payload,
        "chain_ordinal": chain_ordinal,
        "namespace": transition.post_state.namespace,
        "symbol": transition.post_state.symbol,
        "trigger_eligible_at_utc": transition.trigger_eligible_at_utc,
        "trigger_decision_time_utc": transition.trigger_decision_time_utc,
        "post_state_sha256": transition.post_state.state_sha256,
        "post_state": _json(transition.post_state.to_dict()),
        "transition_receipt": _json(payload),
    }


def _membership_params(
    member: movement.MovementMembership,
    *,
    emitted_by_transition_receipt_sha256: str,
) -> dict[str, Any]:
    payload = member.to_dict()
    return {
        **payload,
        "emitted_by_transition_receipt_sha256": (
            emitted_by_transition_receipt_sha256
        ),
        "eligible_at_utc": member.eligible_at_utc,
        "decision_time_utc": member.decision_time_utc,
        "price": member.price,
        "membership_receipt": _json(payload),
    }


def _attempt_from_row(
    row: Mapping[str, Any], expected: movement.NeutralPriceAnchorAttempt
) -> movement.NeutralPriceAnchorAttempt:
    payload = _json_value(row.get("attempt_receipt"))
    if not isinstance(payload, Mapping) or _json(payload) != _json(expected.to_dict()):
        raise MarketMovementConflictError("stored attempt receipt conflicts")
    for field in (
        "attempt_receipt_sha256", "anchor_id", "anchor_receipt_sha256"
    ):
        _assert_char(field, row.get(field), expected.to_dict()[field])
    for field in (
        "contract_version", "symbol", "evaluation_status", "evaluation_reason"
    ):
        _assert_equal(field, row.get(field), expected.to_dict()[field])
    _assert_time("eligible_at_utc", row.get("eligible_at_utc"), expected.eligible_at_utc)
    _assert_time("decision_time_utc", row.get("decision_time_utc"), expected.decision_time_utc)
    return expected


def _anchor_from_row(row: Mapping[str, Any]) -> movement.NeutralPriceAnchor:
    payload = _json_value(row.get("anchor_receipt"))
    if not isinstance(payload, Mapping):
        raise MarketMovementConflictError("stored anchor receipt is missing")
    try:
        anchor = movement.NeutralPriceAnchor.from_dict(payload)
    except (TypeError, ValueError) as exc:
        raise MarketMovementConflictError("stored anchor receipt is invalid") from exc
    for field in (
        "anchor_id", "anchor_receipt_sha256", "source_input_fingerprint"
    ):
        _assert_char(field, row.get(field), anchor.to_dict()[field])
    for field in (
        "contract_version", "symbol", "origin", "sampler_version", "source",
        "upstream_source", "price_exchange", "price_market", "price_pair",
        "price_instrument_id", "price_timeframe", "quality_status",
        "fallback_used", "fallback_policy", "price_candle_identity_basis",
    ):
        _assert_equal(field, row.get(field), anchor.to_dict()[field])
    for field in (
        "eligible_at_utc", "decision_time_utc", "source_price_candle_open_utc",
        "source_price_candle_close_utc", "observed_at_utc",
        "refresh_completed_at_utc", "source_record_created_at_utc",
    ):
        actual = row.get(field)
        expected = getattr(anchor, field)
        if actual is None or expected is None:
            if actual is not None or expected is not None:
                raise MarketMovementConflictError(f"stored {field} conflicts")
        else:
            _assert_time(field, actual, expected)
    if not _same_decimal(row.get("price"), anchor.price):
        raise MarketMovementConflictError("stored price conflicts with anchor receipt")
    return anchor


def _transition_from_row(
    row: Mapping[str, Any], *, expected_ordinal: Optional[int] = None
) -> movement.MovementTransition:
    payload = _json_value(row.get("transition_receipt"))
    if not isinstance(payload, Mapping):
        raise MarketMovementConflictError("stored transition receipt is missing")
    try:
        transition = movement.MovementTransition.from_dict(payload)
    except (TypeError, ValueError) as exc:
        raise MarketMovementConflictError("stored transition receipt is invalid") from exc
    if expected_ordinal is not None:
        _assert_equal("chain_ordinal", row.get("chain_ordinal"), expected_ordinal)
    for field in (
        "transition_receipt_sha256", "previous_transition_receipt_sha256",
        "stream_id", "movement_id", "trigger_anchor_id", "pre_state_sha256",
    ):
        _assert_char(field, row.get(field), transition.to_dict()[field])
    for field in ("contract_version", "transition_type"):
        _assert_equal(field, row.get(field), transition.to_dict()[field])
    _assert_equal("namespace", row.get("namespace"), transition.post_state.namespace)
    _assert_equal("symbol", row.get("symbol"), transition.post_state.symbol)
    _assert_char(
        "post_state_sha256",
        row.get("post_state_sha256"),
        transition.post_state.state_sha256,
    )
    _assert_time(
        "trigger_eligible_at_utc",
        row.get("trigger_eligible_at_utc"),
        transition.trigger_eligible_at_utc,
    )
    _assert_time(
        "trigger_decision_time_utc",
        row.get("trigger_decision_time_utc"),
        transition.trigger_decision_time_utc,
    )
    if _json(_json_value(row.get("post_state"))) != _json(
        transition.post_state.to_dict()
    ):
        raise MarketMovementConflictError("stored post_state conflicts")
    return transition


def _membership_from_row(
    row: Mapping[str, Any],
    *,
    expected_emitter: Optional[str] = None,
) -> movement.MovementMembership:
    payload = _json_value(row.get("membership_receipt"))
    if not isinstance(payload, Mapping):
        raise MarketMovementConflictError("stored membership receipt is missing")
    try:
        member = movement.MovementMembership.from_dict(payload)
    except (TypeError, ValueError) as exc:
        raise MarketMovementConflictError("stored membership receipt is invalid") from exc
    for field in (
        "membership_receipt_sha256", "stream_id", "movement_id", "anchor_id",
        "anchor_receipt_sha256",
    ):
        _assert_char(field, row.get(field), member.to_dict()[field])
    for field in ("contract_version", "ordinal", "classification"):
        _assert_equal(field, row.get(field), member.to_dict()[field])
    _assert_time("eligible_at_utc", row.get("eligible_at_utc"), member.eligible_at_utc)
    _assert_time("decision_time_utc", row.get("decision_time_utc"), member.decision_time_utc)
    if not _same_decimal(row.get("price"), member.price):
        raise MarketMovementConflictError("stored membership price conflicts")
    if expected_emitter is not None:
        _assert_char(
            "emitted_by_transition_receipt_sha256",
            row.get("emitted_by_transition_receipt_sha256"),
            expected_emitter,
        )
    return member


class MarketMovementStore:
    """One trusted-writer adapter; schema creation remains an offline action."""

    def __init__(
        self,
        *,
        database_url: Optional[str] = None,
        connection_factory: Optional[Callable[[], Any]] = None,
        clock: Optional[Callable[[], datetime]] = None,
    ) -> None:
        self.database_url = str(
            database_url
            if database_url is not None
            else os.getenv("RESEARCH_MARKET_MOVEMENT_DATABASE_URL", "")
        ).strip()
        self._connection_factory = connection_factory
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def _connect(self) -> Any:
        if self._connection_factory is not None:
            return self._connection_factory()
        if not self.database_url:
            raise MarketMovementStoreError(
                "Wave v5 requires an explicit trusted-writer database URL"
            )
        if psycopg is None:
            raise MarketMovementStoreError("psycopg is unavailable")
        conn = psycopg.connect(
            self.database_url,
            row_factory=dict_row,
            connect_timeout=5,
            autocommit=True,
            options=CONNECTION_OPTIONS,
        )
        try:
            row = _fetchone(conn.execute(_VERIFY_WRITER_SQL))
            if row is None or row.get("session_user") != TRUSTED_WRITER_ROLE or (
                row.get("current_user") != TRUSTED_WRITER_ROLE
            ):
                raise MarketMovementStoreError(
                    "Wave v5 database session is not the dedicated trusted writer"
                )
        except Exception:
            conn.close()
            raise
        return conn

    def status(self) -> dict[str, Any]:
        return {
            "configured": bool(self.database_url or self._connection_factory),
            "contract_version": movement.POLICY_VERSION,
            "schema_auto_create": False,
            "trusted_writer_required": True,
            "trusted_writer_role": TRUSTED_WRITER_ROLE,
            "read_before_provider": True,
            "btc_local_parent_atomic": True,
            "non_btc_parent_optional": True,
            "live_delivery_allowed": False,
            "runtime_wired": True,
            "runtime_owner": "research_market_movement_worker",
            "runtime_preflight_read_only": True,
        }

    def runtime_preflight(self) -> dict[str, Any]:
        """Verify the dedicated writer identity and required tables read-only."""

        with self._connect() as conn:
            rows = _fetchall(conn.execute(_RUNTIME_PREFLIGHT_SQL, {}))
            role = _fetchone(conn.execute(_RUNTIME_ROLE_PREFLIGHT_SQL, {}))
        missing = sorted(
            str(row.get("relation_name") or "")
            for row in rows
            if not row.get("relation")
        )
        denied = sorted(
            str(row.get("relation_name") or "")
            for row in rows
            if row.get("relation")
            and (
                row.get("schema_usage") is not True
                or row.get("writer_select") is not True
                or row.get("writer_insert") is not True
            )
        )
        unsafe = sorted(
            str(row.get("relation_name") or "")
            for row in rows
            if row.get("relation")
            and (
                row.get("schema_create") is not False
                or row.get("relation_kind") != "r"
                or row.get("relation_owner")
                != "research_market_movement_owner"
                or row.get("writer_update") is not False
                or row.get("writer_delete") is not False
                or row.get("writer_truncate") is not False
                or row.get("writer_references") is not False
                or row.get("writer_trigger") is not False
                or row.get("relation_acl_safe") is not True
                or row.get("column_acl_absent") is not True
                or row.get("rls_disabled") is not True
                or row.get("policies_absent") is not True
                or row.get("rewrite_rules_absent") is not True
                or row.get("trigger_inventory_safe") is not True
            )
        )
        role_unsafe = bool(
            role is None
            or role.get("rolname") != TRUSTED_WRITER_ROLE
            or role.get("rolcanlogin") is not True
            or role.get("rolinherit") is not False
            or role.get("rolsuper") is not False
            or role.get("rolcreatedb") is not False
            or role.get("rolcreaterole") is not False
            or role.get("rolreplication") is not False
            or role.get("rolbypassrls") is not False
            or role.get("owner_rolname") != "research_market_movement_owner"
            or role.get("owner_rolcanlogin") is not False
            or role.get("owner_rolinherit") is not False
            or role.get("owner_rolsuper") is not False
            or role.get("owner_rolcreatedb") is not False
            or role.get("owner_rolcreaterole") is not False
            or role.get("owner_rolreplication") is not False
            or role.get("owner_rolbypassrls") is not False
            or role.get("memberships_held") != 0
            or role.get("members_granted_writer") != 0
            or role.get("database_owner") == TRUSTED_WRITER_ROLE
            or role.get("public_schema_owner") == TRUSTED_WRITER_ROLE
            or role.get("transaction_read_only") != "off"
            or role.get("in_recovery") is not False
        )
        if len(rows) != 4 or missing or denied or unsafe or role_unsafe:
            raise MarketMovementStoreError(
                "Wave v5 runtime preflight failed: "
                f"relations={len(rows)}/4 missing={missing!r} "
                f"denied={denied!r} unsafe={unsafe!r} "
                f"writer_role_unsafe={role_unsafe}"
            )
        return {
            "ready": True,
            "relations_verified": 4,
            "trusted_writer_role": TRUSTED_WRITER_ROLE,
            "schema_auto_create": False,
            "writer_role_verified": True,
            "owner_role_verified": True,
            "wave_table_acl_verified": True,
            "rls_policies_rules_verified": True,
            "user_triggers_verified": 17,
            "writable_primary_verified": True,
        }

    @staticmethod
    def _lock(conn: Any, keys: Sequence[str]) -> None:
        for key in sorted(set(keys)):
            conn.execute(_LOCK_SQL, {"lock_key": key})

    @staticmethod
    def _slot_lock_key(symbol: str, eligible_at_utc: datetime) -> str:
        return (
            f"{movement.POLICY_VERSION}:ANCHOR:{symbol}:"
            f"{_iso(eligible_at_utc)}"
        )

    @staticmethod
    def _stream_lock_key(identity: movement.MovementIdentity) -> str:
        return f"{movement.POLICY_VERSION}:STREAM:{identity.stream_id}"

    def _load_anchor_slot(
        self, conn: Any, *, symbol: str, eligible_at_utc: datetime
    ) -> Optional[movement.NeutralPriceAnchor]:
        row = _fetchone(
            conn.execute(
                _LOAD_ANCHOR_SLOT_SQL,
                {
                    "contract_version": movement.POLICY_VERSION,
                    "symbol": symbol,
                    "eligible_at_utc": eligible_at_utc,
                },
            )
        )
        return _anchor_from_row(row) if row is not None else None

    def _load_symbol_anchors(
        self, conn: Any, *, symbol: str
    ) -> tuple[movement.NeutralPriceAnchor, ...]:
        rows = _fetchall(
            conn.execute(
                _LOAD_SYMBOL_ANCHORS_SQL,
                {
                    "contract_version": movement.POLICY_VERSION,
                    "symbol": symbol,
                },
            )
        )
        return tuple(_anchor_from_row(row) for row in rows)

    def _persist_attempt(
        self, conn: Any, attempt: movement.NeutralPriceAnchorAttempt
    ) -> bool:
        params = _attempt_params(attempt)
        inserted = _fetchone(conn.execute(_INSERT_ATTEMPT_SQL, params))
        if inserted is not None:
            return False
        existing = _fetchone(
            conn.execute(
                _LOAD_ATTEMPT_SQL,
                {"attempt_receipt_sha256": attempt.attempt_receipt_sha256},
            )
        )
        if existing is None:
            raise MarketMovementConflictError(
                "attempt conflict returned no authoritative row"
            )
        _attempt_from_row(existing, attempt)
        return True

    def _persist_anchor(
        self, conn: Any, anchor: movement.NeutralPriceAnchor
    ) -> bool:
        canonical = movement.NeutralPriceAnchor.from_dict(anchor.to_dict())
        params = _anchor_params(canonical)
        inserted = _fetchone(conn.execute(_INSERT_ANCHOR_SQL, params))
        if inserted is not None:
            return False
        existing = self._load_anchor_slot(
            conn,
            symbol=canonical.symbol,
            eligible_at_utc=canonical.eligible_at_utc,
        )
        if existing is None:
            raise MarketMovementConflictError(
                "anchor conflict returned no authoritative row"
            )
        if existing != canonical:
            raise MarketMovementConflictError(
                "neutral-price anchor natural key has different frozen content"
            )
        return True

    def _load_chain(
        self, conn: Any, identity: movement.MovementIdentity
    ) -> tuple[Optional[movement.MovementCursor], int]:
        rows = _fetchall(
            conn.execute(_LOAD_CHAIN_SQL, {"stream_id": identity.stream_id})
        )
        previous: Optional[movement.MovementTransition] = None
        for ordinal, row in enumerate(rows, 1):
            transition = _transition_from_row(row, expected_ordinal=ordinal)
            if transition.stream_id != identity.stream_id:
                raise MarketMovementForkError("transition stream projection changed")
            expected_previous = (
                previous.transition_receipt_sha256 if previous is not None else None
            )
            if transition.previous_transition_receipt_sha256 != expected_previous:
                raise MarketMovementForkError("transition chain has a fork or gap")
            if previous is None:
                if transition.transition_type != movement.OPENED:
                    raise MarketMovementForkError("transition chain root is not OPENED")
            elif previous.post_state.status == movement.CLOSED_STATUS:
                expected_reopen = {
                    movement.DATA_GAP_CENSORED: movement.OPENED_AFTER_DATA_GAP,
                    movement.TWO_CONSECUTIVE_NON_EXTREMES: (
                        movement.OPENED_AFTER_DIRECTION_END
                    ),
                }.get(previous.post_state.close_reason)
                if (
                    transition.transition_type != expected_reopen
                    or transition.trigger_anchor_id != previous.trigger_anchor_id
                ):
                    raise MarketMovementForkError(
                        "movement reopen type conflicts with close reason"
                    )
            elif transition.transition_type in {
                movement.OPENED_AFTER_DATA_GAP,
                movement.OPENED_AFTER_DIRECTION_END,
            }:
                raise MarketMovementForkError(
                    "movement reopen is not paired to close"
                )
            elif transition.pre_state_sha256 != previous.post_state.state_sha256:
                raise MarketMovementForkError("transition pre-state does not match chain")
            previous = transition
        if previous is None:
            return None, 0
        if previous.post_state.status != movement.OPEN_STATUS:
            raise MarketMovementForkError("transition chain ends in an unpaired close")
        cursor = movement.MovementCursor(
            identity=identity,
            state=previous.post_state,
            last_transition_receipt_sha256=(
                previous.transition_receipt_sha256
            ),
        )
        return movement.MovementCursor.from_dict(cursor.to_dict()), len(rows)

    def _assert_canonical_projection_prefix(
        self,
        conn: Any,
        identity: movement.MovementIdentity,
        history: movement.MovementHistory,
    ) -> tuple[movement.MovementMembership, ...]:
        """Verify a stored stream ends on a complete canonical anchor boundary."""

        if history.identity != identity:
            raise ParentProjectionError("canonical replay identity differs from stream")
        transition_rows = _fetchall(
            conn.execute(_LOAD_CHAIN_SQL, {"stream_id": identity.stream_id})
        )
        membership_rows = _fetchall(
            conn.execute(
                _LOAD_STREAM_MEMBERSHIPS_SQL,
                {"stream_id": identity.stream_id},
            )
        )
        if len(transition_rows) > len(history.transitions):
            raise ParentProjectionError(
                "stored projection extends beyond canonical frozen-anchor replay"
            )
        if len(membership_rows) > len(history.memberships):
            raise ParentProjectionError(
                "stored memberships extend beyond canonical frozen-anchor replay"
            )
        stored_transitions: list[movement.MovementTransition] = []
        for ordinal, row in enumerate(transition_rows, 1):
            stored = _transition_from_row(row, expected_ordinal=ordinal)
            expected = history.transitions[ordinal - 1]
            if stored != expected:
                raise ParentProjectionError(
                    "stored transition prefix differs from canonical replay"
                )
            stored_transitions.append(stored)

        emitter_by_anchor: dict[str, str] = {}
        transition_index_by_receipt: dict[str, int] = {}
        for index, transition in enumerate(history.transitions, 1):
            emitter_by_anchor[transition.trigger_anchor_id] = (
                transition.transition_receipt_sha256
            )
            transition_index_by_receipt[
                transition.transition_receipt_sha256
            ] = index
        stored_memberships: list[movement.MovementMembership] = []
        for index, row in enumerate(membership_rows):
            expected = history.memberships[index]
            expected_emitter = emitter_by_anchor.get(expected.anchor_id)
            if expected_emitter is None:
                raise ParentProjectionError(
                    "canonical membership has no emitting transition"
                )
            stored = _membership_from_row(
                row, expected_emitter=expected_emitter
            )
            if stored != expected:
                raise ParentProjectionError(
                    "stored membership prefix differs from canonical replay"
                )
            stored_memberships.append(stored)

        complete_transition_count = 0
        if stored_memberships:
            last_emitter = emitter_by_anchor[stored_memberships[-1].anchor_id]
            complete_transition_count = transition_index_by_receipt[last_emitter]
        if len(stored_transitions) != complete_transition_count:
            raise ParentProjectionError(
                "stored transition exists without its canonical membership"
            )
        return tuple(stored_memberships)

    def _earliest_pending(
        self, conn: Any, identity: movement.MovementIdentity
    ) -> Optional[movement.NeutralPriceAnchor]:
        row = _fetchone(
            conn.execute(
                _EARLIEST_PENDING_SQL,
                {
                    "contract_version": movement.POLICY_VERSION,
                    "symbol": identity.symbol,
                    "stream_id": identity.stream_id,
                },
            )
        )
        return _anchor_from_row(row) if row is not None else None

    def _persist_advance(
        self,
        conn: Any,
        advanced: movement.MovementAdvance,
        *,
        chain_length: int,
    ) -> None:
        if len(advanced.memberships) != 1 or not advanced.transitions:
            raise MarketMovementStoreError(
                "one anchor advance must emit transitions and one membership"
            )
        for offset, transition in enumerate(advanced.transitions, 1):
            ordinal = chain_length + offset
            params = _transition_params(transition, chain_ordinal=ordinal)
            inserted = _fetchone(conn.execute(_INSERT_TRANSITION_SQL, params))
            if inserted is None:
                existing = _fetchone(
                    conn.execute(
                        _LOAD_TRANSITION_ORDINAL_SQL,
                        {
                            "stream_id": transition.stream_id,
                            "chain_ordinal": ordinal,
                        },
                    )
                )
                if existing is None:
                    raise MarketMovementConflictError(
                        "transition conflict returned no authoritative row"
                    )
                stored = _transition_from_row(existing, expected_ordinal=ordinal)
                if stored != transition:
                    raise MarketMovementConflictError(
                        "transition chain ordinal has different content"
                    )
        member = advanced.memberships[0]
        emitter = advanced.transitions[-1].transition_receipt_sha256
        params = _membership_params(
            member,
            emitted_by_transition_receipt_sha256=emitter,
        )
        inserted = _fetchone(conn.execute(_INSERT_MEMBERSHIP_SQL, params))
        if inserted is None:
            existing = _fetchone(
                conn.execute(
                    _LOAD_MEMBERSHIP_SQL,
                    {"stream_id": member.stream_id, "anchor_id": member.anchor_id},
                )
            )
            if existing is None:
                raise MarketMovementConflictError(
                    "membership conflict returned no authoritative row"
                )
            stored = _membership_from_row(existing, expected_emitter=emitter)
            if stored != member:
                raise MarketMovementConflictError(
                    "movement membership natural key has different content"
                )

    @staticmethod
    def _state_semantics(state: movement.MovementState) -> tuple[Any, ...]:
        return (
            state.status,
            state.direction,
            state.started_anchor_id,
            state.started_eligible_at_utc,
            state.started_decision_time_utc,
            state.start_price,
            state.extreme_anchor_id,
            state.extreme_eligible_at_utc,
            state.extreme_price,
            state.last_member_anchor_id,
            state.last_member_eligible_at_utc,
            state.last_member_decision_time_utc,
            state.last_member_price,
            state.member_count,
            state.consecutive_non_extremes,
            state.closed_at_utc,
            state.close_boundary_eligible_at_utc,
            state.close_reason,
        )

    def _parent_membership_at_slot(
        self, conn: Any, eligible_at_utc: datetime
    ) -> Optional[movement.MovementMembership]:
        parent = movement.MovementIdentity.btc_parent()
        rows = _fetchall(
            conn.execute(
                _PARENT_MEMBERSHIP_AT_SLOT_SQL,
                {
                    "stream_id": parent.stream_id,
                    "eligible_at_utc": eligible_at_utc,
                },
            )
        )
        if len(rows) > 1:
            raise ParentProjectionError("multiple BTC parent memberships at one slot")
        return _membership_from_row(rows[0]) if rows else None

    def _existing_anchor_result(
        self, conn: Any, anchor: movement.NeutralPriceAnchor
    ) -> ProcessResult:
        """Describe one frozen anchor without advancing any stream."""

        local_identity = movement.MovementIdentity.for_symbol(anchor.symbol)
        parent_identity = movement.MovementIdentity.btc_parent()
        self._lock(
            conn,
            [
                self._stream_lock_key(local_identity),
                self._stream_lock_key(parent_identity),
            ],
        )
        local_row = _fetchone(
            conn.execute(
                _LOAD_MEMBERSHIP_SQL,
                {
                    "stream_id": local_identity.stream_id,
                    "anchor_id": anchor.anchor_id,
                },
            )
        )
        local_member = (
            _membership_from_row(local_row) if local_row is not None else None
        )
        parent_member = self._parent_membership_at_slot(
            conn, anchor.eligible_at_utc
        )
        if anchor.symbol == "BTC":
            if (local_member is None) != (parent_member is None):
                raise ParentProjectionError(
                    "BTC local and parent projections diverged; use "
                    "repair_missing_btc_parent"
                )
            if parent_member is not None:
                if parent_member.anchor_id != anchor.anchor_id:
                    raise ParentProjectionError(
                        "same-slot BTC parent refers to a different frozen anchor"
                    )
                if (
                    local_member.anchor_receipt_sha256
                    != parent_member.anchor_receipt_sha256
                    or local_member.ordinal != parent_member.ordinal
                    or local_member.classification
                    != parent_member.classification
                    or local_member.eligible_at_utc
                    != parent_member.eligible_at_utc
                    or local_member.decision_time_utc
                    != parent_member.decision_time_utc
                    or local_member.price != parent_member.price
                ):
                    raise ParentProjectionError(
                        "BTC local and parent membership semantics diverged"
                    )
        return ProcessResult(
            symbol=anchor.symbol,
            status=(
                "ALREADY_PROCESSED"
                if local_member is not None
                else "ANCHOR_PENDING"
            ),
            anchor_id=anchor.anchor_id,
            eligible_at_utc=anchor.eligible_at_utc,
            local_movement_id=(
                local_member.movement_id if local_member is not None else None
            ),
            btc_parent_movement_id=(
                parent_member.movement_id if parent_member is not None else None
            ),
            transition_receipts=(),
            membership_receipts=(),
        )

    def _process_btc_locked(self, conn: Any) -> ProcessResult:
        local_identity = movement.MovementIdentity.for_symbol("BTC")
        parent_identity = movement.MovementIdentity.btc_parent()
        self._lock(
            conn,
            [
                self._stream_lock_key(local_identity),
                self._stream_lock_key(parent_identity),
            ],
        )
        local_pending = self._earliest_pending(conn, local_identity)
        parent_pending = self._earliest_pending(conn, parent_identity)
        if local_pending is None and parent_pending is None:
            return ProcessResult("BTC", "NO_PENDING", None, None, None, None, (), ())
        if (
            local_pending is None
            or parent_pending is None
            or local_pending.anchor_receipt_sha256
            != parent_pending.anchor_receipt_sha256
        ):
            raise ParentProjectionError(
                "BTC local and parent earliest-pending projections diverged; use repair"
            )
        local_cursor, local_length = self._load_chain(conn, local_identity)
        parent_cursor, parent_length = self._load_chain(conn, parent_identity)
        local_advance = movement.advance(
            local_cursor, local_pending, identity=local_identity
        )
        parent_advance = movement.advance(
            parent_cursor, parent_pending, identity=parent_identity
        )
        if self._state_semantics(local_advance.cursor.state) != self._state_semantics(
            parent_advance.cursor.state
        ):
            raise ParentProjectionError("BTC local and parent semantic states diverged")
        if [item.transition_type for item in local_advance.transitions] != [
            item.transition_type for item in parent_advance.transitions
        ]:
            raise ParentProjectionError("BTC local and parent transitions diverged")
        # Parent first lets the deferred same-slot membership validator resolve
        # BTC's own local membership in this same transaction.
        self._persist_advance(
            conn, parent_advance, chain_length=parent_length
        )
        self._persist_advance(conn, local_advance, chain_length=local_length)
        return ProcessResult(
            symbol="BTC",
            status="PROCESSED",
            anchor_id=local_pending.anchor_id,
            eligible_at_utc=local_pending.eligible_at_utc,
            local_movement_id=local_advance.cursor.state.movement_id,
            btc_parent_movement_id=parent_advance.cursor.state.movement_id,
            transition_receipts=tuple(
                item.transition_receipt_sha256
                for item in (*parent_advance.transitions, *local_advance.transitions)
            ),
            membership_receipts=tuple(
                item.membership_receipt_sha256
                for item in (*parent_advance.memberships, *local_advance.memberships)
            ),
        )

    def _process_asset_locked(self, conn: Any, symbol: str) -> ProcessResult:
        identity = movement.MovementIdentity.for_symbol(symbol)
        self._lock(conn, [self._stream_lock_key(identity)])
        pending = self._earliest_pending(conn, identity)
        if pending is None:
            return ProcessResult(symbol, "NO_PENDING", None, None, None, None, (), ())
        cursor, chain_length = self._load_chain(conn, identity)
        advanced = movement.advance(cursor, pending, identity=identity)
        self._persist_advance(conn, advanced, chain_length=chain_length)
        parent_member = self._parent_membership_at_slot(
            conn, pending.eligible_at_utc
        )
        return ProcessResult(
            symbol=symbol,
            status="PROCESSED",
            anchor_id=pending.anchor_id,
            eligible_at_utc=pending.eligible_at_utc,
            local_movement_id=advanced.cursor.state.movement_id,
            btc_parent_movement_id=(
                parent_member.movement_id if parent_member is not None else None
            ),
            transition_receipts=tuple(
                item.transition_receipt_sha256 for item in advanced.transitions
            ),
            membership_receipts=tuple(
                item.membership_receipt_sha256 for item in advanced.memberships
            ),
        )

    def _process_earliest_locked(self, conn: Any, symbol: str) -> ProcessResult:
        return (
            self._process_btc_locked(conn)
            if symbol == "BTC"
            else self._process_asset_locked(conn, symbol)
        )

    def capture_prospective(
        self,
        *,
        symbol: Any,
        eligible_at_utc: Any,
        provider: Callable[[], Mapping[str, Any]],
    ) -> CaptureResult:
        """Capture one slot with authoritative read-before-provider behavior."""

        normalized_symbol = _symbol(symbol)
        eligible = movement._eligibility(eligible_at_utc)
        if not callable(provider):
            raise ValueError("provider must be callable")
        with self._connect() as conn:
            with conn.transaction():
                self._lock(
                    conn,
                    [self._slot_lock_key(normalized_symbol, eligible)],
                )
                existing = self._load_anchor_slot(
                    conn,
                    symbol=normalized_symbol,
                    eligible_at_utc=eligible,
                )
                if existing is not None:
                    processing = self._existing_anchor_result(conn, existing)
                    return CaptureResult(
                        symbol=normalized_symbol,
                        provider_called=False,
                        idempotent_anchor=True,
                        attempt_receipt_sha256=None,
                        anchor=existing,
                        processing=processing,
                    )
                before_provider = _utc(
                    self._clock(), field="pre_provider_time_utc"
                )
                if not (
                    eligible
                    <= before_provider
                    < eligible
                    + timedelta(minutes=movement.CAPTURE_WINDOW_MINUTES)
                ):
                    raise MarketMovementStoreError(
                        "capture request is outside the eligible 30-minute "
                        "capture window"
                    )
                try:
                    supplied = provider()
                except Exception:
                    supplied = {}
                decision_time = _utc(
                    self._clock(), field="decision_time_utc"
                )
                price_candle = (
                    supplied.get("price_candle")
                    if isinstance(supplied, Mapping)
                    else None
                )
                provenance = (
                    supplied.get("source_provenance")
                    if isinstance(supplied, Mapping)
                    else None
                )
                decision = movement.evaluate_prospective_anchor(
                    symbol=normalized_symbol,
                    eligible_at_utc=eligible,
                    decision_time_utc=decision_time,
                    price_candle=price_candle,
                    source_provenance=provenance,
                    source_input_fingerprint=(
                        supplied.get("source_input_fingerprint")
                        if isinstance(supplied, Mapping)
                        else None
                    ),
                )
                self._persist_attempt(conn, decision.attempt)
                if decision.anchor is None:
                    conn.execute(_SET_CONSTRAINTS_SQL, {})
                    return CaptureResult(
                        symbol=normalized_symbol,
                        provider_called=True,
                        idempotent_anchor=False,
                        attempt_receipt_sha256=(
                            decision.attempt.attempt_receipt_sha256
                        ),
                        anchor=None,
                        processing=ProcessResult(
                            normalized_symbol,
                            "UNEVALUABLE",
                            None,
                            eligible,
                            None,
                            None,
                            (),
                            (),
                        ),
                    )
                idempotent = self._persist_anchor(conn, decision.anchor)
                processing = self._process_earliest_locked(
                    conn, normalized_symbol
                )
                conn.execute(_SET_CONSTRAINTS_SQL, {})
                return CaptureResult(
                    symbol=normalized_symbol,
                    provider_called=True,
                    idempotent_anchor=idempotent,
                    attempt_receipt_sha256=(
                        decision.attempt.attempt_receipt_sha256
                    ),
                    anchor=decision.anchor,
                    processing=processing,
                )

    def process_earliest_pending(self, symbol: Any) -> ProcessResult:
        """Advance one earliest unassigned anchor; never bypass an older row."""

        normalized_symbol = _symbol(symbol)
        with self._connect() as conn:
            with conn.transaction():
                result = self._process_earliest_locked(conn, normalized_symbol)
                conn.execute(_SET_CONSTRAINTS_SQL, {})
                return result

    def import_verified_historical_slot(
        self, record: Mapping[str, Any]
    ) -> HistoricalImportResult:
        """Import one frozen v3/v4 slot without a provider lookup.

        Singular calls must be supplied chronologically per symbol.  A late
        slot cannot be appended behind the canonical cursor and fails closed.
        """

        anchor = movement.NeutralPriceAnchor.from_authorized_legacy_slot(record)
        with self._connect() as conn:
            with conn.transaction():
                self._lock(
                    conn,
                    [self._slot_lock_key(anchor.symbol, anchor.eligible_at_utc)],
                )
                existing = self._load_anchor_slot(
                    conn,
                    symbol=anchor.symbol,
                    eligible_at_utc=anchor.eligible_at_utc,
                )
                if existing is not None:
                    if existing != anchor:
                        raise MarketMovementConflictError(
                            "historical/live overlap has different frozen content"
                        )
                    idempotent = True
                    processing = self._existing_anchor_result(conn, existing)
                    return HistoricalImportResult(
                        anchor, idempotent, processing
                    )
                else:
                    idempotent = self._persist_anchor(conn, anchor)
                processing = self._process_earliest_locked(conn, anchor.symbol)
                conn.execute(_SET_CONSTRAINTS_SQL, {})
                return HistoricalImportResult(anchor, idempotent, processing)

    def repair_missing_btc_parent(self) -> ProcessResult:
        """Repair one wholly absent canonical BTC-parent suffix projection.

        Both stored streams must be exact canonical prefixes replayed from the
        frozen BTC anchors.  The target parent anchor must have no transition
        and no membership, while its exact BTC SYMBOL membership must already
        exist.  One call advances only that one target anchor.
        """

        local_identity = movement.MovementIdentity.for_symbol("BTC")
        parent_identity = movement.MovementIdentity.btc_parent()
        with self._connect() as conn:
            with conn.transaction():
                self._lock(
                    conn,
                    [
                        self._stream_lock_key(local_identity),
                        self._stream_lock_key(parent_identity),
                    ],
                )
                anchors = self._load_symbol_anchors(conn, symbol="BTC")
                local_history = movement.replay(
                    anchors, identity=local_identity
                )
                parent_history = movement.replay(
                    anchors, identity=parent_identity
                )
                local_members = self._assert_canonical_projection_prefix(
                    conn, local_identity, local_history
                )
                parent_members = self._assert_canonical_projection_prefix(
                    conn, parent_identity, parent_history
                )
                target_index = len(parent_members)
                if target_index > len(local_members):
                    raise ParentProjectionError(
                        "BTC parent projection is ahead of its local identity"
                    )
                if target_index == len(local_members):
                    conn.execute(_SET_CONSTRAINTS_SQL, {})
                    return ProcessResult(
                        "BTC", "NO_REPAIR_NEEDED", None, None, None, None, (), ()
                    )
                pending = anchors[target_index]
                expected_local = local_history.memberships[target_index]
                local_row = _fetchone(
                    conn.execute(
                        _BTC_LOCAL_MEMBERSHIP_SQL,
                        {
                            "stream_id": local_identity.stream_id,
                            "anchor_id": pending.anchor_id,
                        },
                    )
                )
                if local_row is None:
                    raise ParentProjectionError(
                        "parent repair requires the exact same-anchor BTC local membership"
                    )
                local_member = _membership_from_row(local_row)
                if local_member != expected_local:
                    raise ParentProjectionError(
                        "BTC local repair target differs from canonical replay"
                    )
                cursor, chain_length = self._load_chain(conn, parent_identity)
                advanced = movement.advance(
                    cursor, pending, identity=parent_identity
                )
                expected_transition_receipts = tuple(
                    item.transition_receipt_sha256
                    for item in parent_history.transitions[
                        chain_length : chain_length + len(advanced.transitions)
                    ]
                )
                expected_membership = parent_history.memberships[target_index]
                if (
                    tuple(
                        item.transition_receipt_sha256
                        for item in advanced.transitions
                    )
                    != expected_transition_receipts
                    or advanced.memberships != (expected_membership,)
                ):
                    raise ParentProjectionError(
                        "parent repair advance differs from canonical replay"
                    )
                self._persist_advance(
                    conn, advanced, chain_length=chain_length
                )
                conn.execute(_SET_CONSTRAINTS_SQL, {})
                return ProcessResult(
                    symbol="BTC",
                    status="REPAIRED_BTC_PARENT",
                    anchor_id=pending.anchor_id,
                    eligible_at_utc=pending.eligible_at_utc,
                    local_movement_id=local_member.movement_id,
                    btc_parent_movement_id=advanced.cursor.state.movement_id,
                    transition_receipts=tuple(
                        item.transition_receipt_sha256
                        for item in advanced.transitions
                    ),
                    membership_receipts=tuple(
                        item.membership_receipt_sha256
                        for item in advanced.memberships
                    ),
                )
