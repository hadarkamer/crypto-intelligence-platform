"""Static contract checks for migration 028's experimental Telegram boundary."""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parent
MIGRATION = ROOT / "migrations" / "028_stage4_experimental_telegram_v1.sql"
ROLE = "research_formula_experimental_dispatcher_v1"
TABLES = (
    "research_formula_experimental_search_runs_v1",
    "research_formula_experimental_alerts_v1",
    "research_formula_experimental_subscriptions_v1",
    "research_formula_experimental_deliveries_v1",
    "research_formula_experimental_delivery_attempt_events_v1",
)
DISCLAIMER = "ניסיוני, לא מאושר למסחר"


def _compact(value: str) -> str:
    return " ".join(value.lower().split())


def _table_body(sql: str, table: str) -> str:
    match = re.search(
        rf"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(?:\n\s*)?"
        rf"public\.{re.escape(table)}\s*\((.*?)\n\s*\);",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    assert match, f"missing table {table}"
    return match.group(1)


def _assert_isolated_authority(sql: str) -> None:
    lowered = sql.lower()
    compact = _compact(sql)

    assert ROLE in sql
    for option in (
        "rolcanlogin",
        "rolinherit",
        "rolsuper",
        "rolcreatedb",
        "rolcreaterole",
        "rolreplication",
        "rolbypassrls",
    ):
        assert option in lowered
    for endpoint in ("membership.member", "membership.roleid", "membership.grantor"):
        assert endpoint in lowered
    assert "must be an unprivileged noinherit login role" in lowered
    assert "cannot create or own database/schema objects" in lowered
    assert "already has forbidden existing authority" in lowered
    assert "already has forbidden column authority" in lowered
    assert "escaped its isolated authority boundary" in lowered
    assert lowered.count("pg_catalog.has_any_column_privilege") >= 2
    assert "revoke create on schema public" in lowered
    assert "grant usage on schema public" in lowered

    # Existing Formula/LIVE/source relations are inspected only to prove zero
    # authority.  They are never targets of INSERT, UPDATE, DELETE or grants.
    for protected in (
        "research_events",
        "research_formulas",
        "research_formula_live_approvals",
        "research_formula_live_deliveries",
        "research_formula_alert_subscriptions",
        "research_formula_exploration_stage4_v1",
    ):
        assert f"('public.{protected}')" in lowered
        assert not re.search(
            rf"(?:insert\s+into|update|delete\s+from)\s+"
            rf"(?:public\.)?{re.escape(protected)}\b",
            lowered,
        )
        assert not re.search(
            rf"grant\s+[^;]*\s+on\s+(?:table\s+)?(?:public\.)?"
            rf"{re.escape(protected)}\b",
            lowered,
            flags=re.DOTALL,
        )

    assert "formula_registry_effect = 'none'" in compact
    assert "human_formula_approval_required = false" in compact
    assert "live_eligible = false" in compact
    assert "trade_execution_allowed = false" in compact
    assert "telegram_delivery_allowed = true" in compact
    assert "delivery_channel = 'telegram_experimental_only'" in compact
    assert DISCLAIMER in sql
    assert "formula_live_alerts_enabled" not in lowered


def _assert_tables(sql: str) -> None:
    lowered = sql.lower()
    created = re.findall(
        r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+"
        r"(?:\n\s*)?public\.([a-z0-9_]+)",
        sql,
        flags=re.IGNORECASE,
    )
    assert tuple(created) == TABLES
    assert len(created) == 5

    search = _compact(_table_body(sql, TABLES[0]))
    for token in (
        "search_run_id char(64) not null",
        "search_receipt_sha256 char(64) not null",
        "source_corpus_receipt_sha256 char(64) not null",
        "horizon_minutes integer not null",
        "analysis_as_of_utc timestamptz not null",
        "input_observation_count integer not null",
        "input_observation_count between 0 and 131072",
        "eligible_candidate_count integer not null",
        "eligible_candidate_count between 0 and 4096",
        "search_status = 'empty_corpus'",
        "search_status = 'no_eligible_experimental_candidates'",
        "search_status = 'eligible_experimental_candidates_found'",
        "horizon_minutes, schedule_slot_utc",
        "search_payload ->> 'status' is not distinct from search_status",
        "search_payload #>> '{counts,eligible_candidate_variants}'",
        "is not distinct from eligible_candidate_count",
        "search_payload jsonb not null",
        "search_payload_sha256 char(64) not null",
        "search_payload ->> 'formula_registry_effect' is not distinct from 'none'",
        "search_payload -> 'live_eligible' is not distinct from 'false'::jsonb",
        "search_payload -> 'telegram_delivery_allowed' is not distinct from 'false'::jsonb",
        "search_payload -> 'trade_execution_allowed' is not distinct from 'false'::jsonb",
    ):
        assert token in search, token
    for exact_status_case in (
        "search_status = 'empty_corpus' and input_observation_count = 0 "
        "and eligible_candidate_count = 0",
        "search_status = 'no_eligible_experimental_candidates' and "
        "input_observation_count > 0 and eligible_candidate_count = 0",
        "search_status = 'eligible_experimental_candidates_found' and "
        "input_observation_count > 0 and eligible_candidate_count > 0",
    ):
        assert exact_status_case in search, exact_status_case
    assert "input_observation_count between 1" not in search
    assert "eligible_candidate_count between 0 and 256" not in search

    alert = _compact(_table_body(sql, TABLES[1]))
    for token in (
        "search_receipt_sha256 char(64) not null",
        "candidate_key char(64) not null",
        "candidate_snapshot jsonb not null",
        "trigger_key char(64) not null",
        "projection_event_id bigint not null",
        "btc_parent_movement_id char(64) not null",
        "symbol text not null",
        "direction text not null",
        "horizon_minutes integer not null",
        "decision_time_utc timestamptz not null",
        "expires_at_utc timestamptz not null",
        "trigger_snapshot jsonb not null",
        "rendered_message text not null",
        "rendered_message_sha256 char(64) not null",
        "independent_movement_count >= 5",
        "and not trigger_snapshot ? 'outcome'",
        "and not trigger_snapshot ? 'path'",
        "trigger_snapshot ->> 'status' is not distinct from 'frozen_bound_fresh'",
        "trigger_snapshot ->> 'contract_version' is not distinct from 'stage4-experimental-current-snapshot-no-outcome-v1'",
        "trigger_snapshot ->> 'observation_id' is not distinct from trigger_observation_id",
        "trigger_snapshot ->> 'projection_event_fingerprint' is not distinct from projection_event_fingerprint",
        "trigger_snapshot ->> 'btc_parent_movement_id' is not distinct from btc_parent_movement_id",
        "trigger_snapshot ->> 'trigger_snapshot_sha256' is not distinct from trigger_snapshot_sha256",
        "candidate_snapshot -> 'experimental_formula_eligible' is not distinct from 'true'::jsonb",
        "candidate_snapshot ->> 'formula_text' is not distinct from formula_text",
        "candidate_snapshot -> 'metrics' is not distinct from metrics",
        "current_trigger_policy_version = 'stage4-experimental-current-trigger-v1'",
        "renderer_version = 'stage4-experimental-telegram-renderer-v1'",
        "disclaimer = 'ניסיוני, לא מאושר למסחר'",
        "candidate_key, trigger_key",
    ):
        assert token in alert, token

    subscription = _compact(_table_body(sql, TABLES[2]))
    for token in (
        "chat_id bigint not null",
        "requested_by_user_id bigint not null",
        "consent_source = 'explicit_telegram_command'",
        "delivery_scope = 'telegram_experimental_only'",
        "disclaimer_acknowledged = 'ניסיוני, לא מאושר למסחר'",
    ):
        assert token in subscription, token

    delivery = _compact(_table_body(sql, TABLES[3]))
    for status in (
        "PENDING",
        "IN_FLIGHT",
        "RETRYABLE",
        "SENT",
        "FAILED_FINAL",
        "AMBIGUOUS",
        "EXPIRED",
    ):
        assert f"'{status.lower()}'" in delivery
    for token in (
        "delivery_key char(64) not null",
        "attempt_count integer not null default 0",
        "claim_token char(64)",
        "claim_expires_at_utc timestamptz",
        "telegram_message_id bigint",
        "alert_occurrence_id, chat_id",
    ):
        assert token in delivery, token

    attempt = _compact(_table_body(sql, TABLES[4]))
    for token in (
        "attempt_event_key char(64) not null",
        "event_phase = 'claimed'",
        "event_phase = 'terminal'",
        "terminal_result = 'sent'",
        "terminal_result in ('definite_failure', 'ambiguous')",
        "delivery_key, attempt_number, event_phase",
        "event_payload jsonb not null",
        "event_time_utc = created_at_utc",
        "event_payload ->> 'error_text' is not distinct from error_text",
    ):
        assert token in attempt, token

    # There is no migration-time opt-in, and legacy/LIVE subscriptions cannot
    # silently become experimental subscriptions.
    assert not re.search(
        rf"INSERT\s+INTO\s+(?:public\.)?{re.escape(TABLES[2])}",
        sql,
        flags=re.IGNORECASE,
    )
    assert "live-subscription-backfill=false" in lowered


def _assert_state_machine_and_audit(sql: str) -> None:
    lowered = sql.lower()
    compact = _compact(sql)
    for transition in (
        "old.status in ('pending', 'retryable') and new.status = 'in_flight'",
        "old.status = 'in_flight' and new.status = 'sent'",
        "old.status = 'in_flight' and new.status = 'retryable'",
        "old.status = 'in_flight' and new.status = 'failed_final'",
        "old.status = 'in_flight' and new.status = 'ambiguous'",
        "new.status = 'expired'",
    ):
        assert transition in compact, transition
    assert "stale in_flight delivery must become ambiguous, never retry" in lowered
    assert "new.status not in ('sent', 'ambiguous')" in compact
    assert "old.claim_expires_at_utc <= now_utc" in compact
    assert "old.available_at_utc > now_utc" in compact
    assert "new.attempt_count is distinct from old.attempt_count + 1" in compact
    assert "terminal experimental delivery cannot change" in lowered
    assert "experimental delivery status transition is forbidden" in lowered
    assert "new experimental delivery requires a current explicit opt-in" in lowered
    assert "subscription_active is not true" in compact
    assert "subscription_updated_at > occurrence_decision_time" in compact
    assert "for share of subscription" in compact
    assert "for share of occurrence" not in compact
    assert "deferrable initially deferred" in compact
    assert "same-transaction attempt audit" in lowered
    assert "new.status in ('retryable', 'failed_final')" in compact
    assert "event_row.event_phase = expected_phase" in compact
    assert "event_row.event_time_utc = pg_catalog.transaction_timestamp()" in compact
    assert "event_row.created_at_utc = pg_catalog.transaction_timestamp()" in compact

    for table, trigger_stem in (
        (TABLES[0], "search_runs"),
        (TABLES[1], "alerts"),
    ):
        assert re.search(
            rf"BEFORE\s+UPDATE\s+OR\s+DELETE\s+ON\s+public\."
            rf"{re.escape(table)}.*?prevent_research_stage4_experimental_immutable_v1",
            sql,
            flags=re.IGNORECASE | re.DOTALL,
        ), trigger_stem
        assert re.search(
            rf"BEFORE\s+TRUNCATE\s+ON\s+public\.{re.escape(table)}.*?"
            r"prevent_research_stage4_experimental_immutable_v1",
            sql,
            flags=re.IGNORECASE | re.DOTALL,
        ), trigger_stem
    assert re.search(
        rf"BEFORE\s+INSERT\s+OR\s+UPDATE\s+OR\s+DELETE\s+ON\s+public\."
        rf"{re.escape(TABLES[4])}.*?"
        r"validate_research_stage4_experimental_attempt_v1",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    assert re.search(
        rf"BEFORE\s+TRUNCATE\s+ON\s+public\.{re.escape(TABLES[4])}.*?"
        r"prevent_research_stage4_experimental_immutable_v1",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    assert "attempt audit timestamps must bind its transaction" in lowered
    assert "attempt audit does not bind the current delivery claim" in lowered
    assert lowered.count("enable always trigger") == 11
    assert "two-phase=claimed-terminal" in lowered
    assert "for update skip locked" not in lowered


def _assert_acl_catalog_and_rollback(sql: str) -> None:
    lowered = sql.lower()
    compact = _compact(sql)
    assert "pg_catalog.aclexplode" in lowered
    assert "revoke all privileges on table public.%i from %s cascade" in lowered
    assert "revoke %s (%i) on table public.%i from %s cascade" in lowered
    assert "grant select, insert on table" in lowered
    assert "grant update (active) on table" in compact
    assert "grant update ( status, attempt_count, available_at_utc" in compact
    assert "acl.privilege_type in ('select', 'insert')" in compact
    assert "acl.privilege_type = 'update'" in compact
    assert "acl.is_grantable" in lowered
    assert "experimental relation acl is not exact" in lowered
    assert "experimental column update acl is not exact" in lowered
    assert "function_row.prosecdef" in lowered
    assert "search_path=pg_catalog, public" in lowered
    assert "function_row.proowner <> trusted_owner" in compact
    assert "relation_row.relrowsecurity" in lowered
    assert "relation_row.relforcerowsecurity" in lowered
    assert "from pg_catalog.pg_constraint" in lowered
    assert "constraint_row.contype in ('c', 'p', 'u', 'f')" in compact
    assert "experimental table public.% has a weak constraint" in lowered
    assert "from pg_catalog.pg_index" in lowered
    assert "from pg_catalog.pg_trigger" in lowered
    assert "from pg_catalog.pg_policy" in lowered
    assert "from pg_catalog.pg_rewrite" in lowered
    for table in TABLES:
        assert f"comment on table public.{table}" in compact

    rollback_marker = lowered.index("-- explicit manual rollback")
    rollback = lowered[rollback_marker:]
    ordered = (
        TABLES[4],
        TABLES[3],
        TABLES[2],
        TABLES[1],
        TABLES[0],
    )
    positions = [rollback.index(table) for table in ordered]
    assert positions == sorted(positions)
    assert "research_formula_live_approvals" not in rollback
    assert "research_formula_live_deliveries" not in rollback
    assert "research_formula_alert_subscriptions" not in rollback


def run() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")
    assert "SET LOCAL lock_timeout = '3s'" in sql
    assert "SET LOCAL statement_timeout = '30s'" in sql
    _assert_tables(sql)
    _assert_isolated_authority(sql)
    _assert_state_machine_and_audit(sql)
    _assert_acl_catalog_and_rollback(sql)

    try:
        from pglast import parse_sql
    except ImportError:
        pass
    else:
        assert parse_sql(sql)

    print("research_stage4_experimental_schema_selftest: PASS")


if __name__ == "__main__":
    run()
