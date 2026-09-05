"""Static contract checks for migration 027's bounded scan state."""

from __future__ import annotations

import re
from pathlib import Path

import research_formula_schema_admin
import research_outcome_worker


ROOT = Path(__file__).resolve().parent
MIGRATION = (
    ROOT / "migrations" / "027_stage4_signal_outcome_scan_state_v1.sql"
)
TABLE = "public.research_stage4_signal_scan_state_v1"
SCAN_KEY = "STAGE4_SIGNAL_DUE_V1"
STATE_VERSION = "stage4-signal-due-scan-state-v1"


def _table_definition(sql: str) -> str:
    match = re.search(
        r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+"
        r"public\.research_stage4_signal_scan_state_v1\s*\("
        r"(.*?)\n\s*\);",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    assert match, "missing Stage-4 signal scan state table"
    return match.group(1)


def run() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")
    lowered = sql.lower()
    definition = _table_definition(sql).lower()
    formula_store_source = (ROOT / "research_formula_store.py").read_text(
        encoding="utf-8"
    )

    assert MIGRATION.resolve() in research_formula_schema_admin.MIGRATION_PATHS
    migration_position = research_formula_schema_admin.MIGRATION_PATHS.index(
        MIGRATION.resolve()
    )
    assert migration_position > 0
    assert (
        research_formula_schema_admin.MIGRATION_PATHS[migration_position + 1].name
        == "028_stage4_experimental_telegram_v1.sql"
    )
    assert research_outcome_worker._STAGE4_DUE_SCAN_STATE_KEY == SCAN_KEY
    assert research_outcome_worker._STAGE4_DUE_SCAN_STATE_VERSION == STATE_VERSION
    assert formula_store_source.count(
        '"research_stage4_signal_scan_state_v1"'
    ) >= 2
    assert "SET LOCAL lock_timeout = '3s'" in sql
    assert "SET LOCAL statement_timeout = '30s'" in sql

    expected_columns = (
        "scan_key text primary key",
        "state_version text not null",
        "cursor_alert_time_utc timestamptz",
        "cursor_event_id bigint",
        "lap_upper_alert_time_utc timestamptz",
        "lap_upper_event_id bigint",
        "completed_laps bigint not null default 0",
        "pages_scanned bigint not null default 0",
        "candidates_scanned bigint not null default 0",
        "updated_at_utc timestamptz not null default now()",
    )
    for column in expected_columns:
        assert column in " ".join(definition.split()), column

    # A cursor and its frozen lap upper bound are typed keyset tuples.  Partial
    # tuples, non-positive ids and advancement beyond the frozen upper key are
    # rejected by PostgreSQL rather than repaired by runtime code.
    for constraint in (
        "research_stage4_signal_scan_key_ck",
        "research_stage4_signal_scan_state_version_ck",
        "research_stage4_signal_scan_cursor_pair_ck",
        "research_stage4_signal_scan_upper_pair_ck",
        "research_stage4_signal_scan_cursor_requires_upper_ck",
        "research_stage4_signal_scan_positive_ids_ck",
        "research_stage4_signal_scan_cursor_within_lap_ck",
        "research_stage4_signal_scan_counters_ck",
    ):
        assert f"constraint {constraint} check" in " ".join(definition.split())
    assert (
        "(cursor_alert_time_utc is null) = (cursor_event_id is null)"
        in " ".join(definition.split())
    )
    assert (
        "(lap_upper_alert_time_utc is null) = (lap_upper_event_id is null)"
        in " ".join(definition.split())
    )
    assert "cursor_alert_time_utc is null or lap_upper_alert_time_utc is not null" in (
        " ".join(definition.split())
    )
    assert (
        "row(cursor_alert_time_utc, cursor_event_id) <= "
        "row(lap_upper_alert_time_utc, lap_upper_event_id)"
        in " ".join(definition.split())
    )
    assert f"scan_key = '{SCAN_KEY.lower()}'" in definition
    assert f"state_version = '{STATE_VERSION}'" in definition
    for counter in ("completed_laps", "pages_scanned", "candidates_scanned"):
        assert f"{counter} >= 0" in definition

    # Reapplying the migration preserves live progress; only the first apply
    # seeds the exact singleton contract row.
    assert re.search(
        rf"INSERT\s+INTO\s+{re.escape(TABLE)}\s*\(\s*"
        r"scan_key\s*,\s*state_version\s*\).*?"
        rf"'{re.escape(SCAN_KEY)}'\s*,\s*'{re.escape(STATE_VERSION)}'.*?"
        r"ON\s+CONFLICT\s*\(\s*scan_key\s*\)\s*DO\s+NOTHING",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    assert "do update" not in lowered

    # The state is owner-only.  The cleanup covers both relation and column
    # ACLs, and this migration grants no role any privilege.
    assert "pg_catalog.aclexplode" in lowered
    assert "revoke all privileges on table public.%i from %s cascade" in lowered
    assert "revoke %s (%i) on table public.%i from %s cascade" in lowered
    assert not re.search(r"\bGRANT\b", sql, flags=re.IGNORECASE)
    assert "acl.grantee <> owner_oid" in lowered
    for guard in (
        "relation_row.relpersistence = 'p'",
        "not relation_row.relrowsecurity",
        "not relation_row.relforcerowsecurity",
        "constraint_count <> pg_catalog.cardinality(expected_constraints)",
        "constraint_row.contype not in ('c', 'p', 'n')",
        "postgresql 18 materializes not null constraints as contype='n'",
        "server_version_num')::integer >= 180000",
        "->> 'conenforced'",
        "constraint_row.convalidated",
        "constraint_row.conislocal",
        "constraint_row.coninhcount <> 0",
        "constraint_row.conparentid <> 0",
        "'not null %i'",
        "pg_catalog.pg_get_constraintdef",
        "from pg_catalog.pg_trigger",
        "from pg_catalog.pg_policy",
        "from pg_catalog.pg_rewrite",
        "from pg_catalog.pg_inherits inheritance",
        "attribute.attcollation::regcollation::text",
    ):
        assert guard in lowered, guard
    assert "scan_key:text:not-null:\"default\"" in lowered
    assert "check (((completed_laps >= 0) and (pages_scanned >= 0)" in lowered
    assert "constraint_row.contype::text || '|'" in lowered
    assert "constraint_row.connoinherit::text" in lowered
    assert "rewrite_row.rulename <> '_return'" not in lowered
    assert "set local quote_all_identifiers = off" in lowered
    assert "pg18-generated/truncated not null names are deliberately" in lowered
    assert (
        "lock table public.research_stage4_signal_scan_state_v1\n"
        "    in share row exclusive mode"
    ) in lowered

    # The state is explicitly non-authoritative and rollback can only discard
    # pagination progress, never source events or outcome rows.
    assert "authority=operational_cursor_only" in lowered
    assert "lap_upper=frozen" in lowered
    assert "stop the research outcome worker first" in lowered
    assert re.search(
        r"--\s+DROP\s+TABLE\s+IF\s+EXISTS\s*\n"
        r"--\s+public\.research_stage4_signal_scan_state_v1\s*;",
        sql,
        flags=re.IGNORECASE,
    )
    assert "drop table if exists public.research_events" not in lowered
    assert "drop table if exists public.research_alert_outcomes" not in lowered

    try:
        from pglast import parse_sql
    except ImportError:
        pass
    else:
        assert parse_sql(sql)

    print("research_stage4_signal_outcome_scan_state_selftest: PASS")


if __name__ == "__main__":
    run()
