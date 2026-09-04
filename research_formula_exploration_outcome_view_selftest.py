"""Static checks for migration 025's Stage-4 outcome-label boundary."""
from __future__ import annotations

import re
from pathlib import Path

import research_formula_schema_admin
import research_signal_formula_exploration as exploration


ROOT = Path(__file__).resolve().parent
MIGRATION = ROOT / "migrations" / "025_formula_exploration_outcomes_v1.sql"
VIEW = "public.research_formula_exploration_outcomes_v1"
READER_ROLE = "research_formula_exploration_reader_v1"


def _view_definition(sql: str) -> str:
    match = re.search(
        r"CREATE\s+OR\s+REPLACE\s+VIEW\s+"
        r"public\.research_formula_exploration_outcomes_v1\s+"
        r"WITH\s*\(\s*security_barrier\s*=\s*true\s*,\s*"
        r"security_invoker\s*=\s*false\s*\)\s+AS\s+"
        r"(.*?)\s*;\s*\n\s*-- Normalize stale view grants",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    assert match, "missing exact security-barrier outcome view"
    return match.group(1)


def run() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")
    assert MIGRATION.resolve() in research_formula_schema_admin.MIGRATION_PATHS
    migration_index = research_formula_schema_admin.MIGRATION_PATHS.index(
        MIGRATION.resolve()
    )
    assert (
        research_formula_schema_admin.MIGRATION_PATHS[migration_index + 1].name
        == "026_stage4_no_signal_outcomes_v1.sql"
    )

    definition = _view_definition(sql)
    lowered_definition = definition.lower()
    select_sql, where_sql = re.split(
        r"\bwhere\b", definition, maxsplit=1, flags=re.IGNORECASE
    )
    lowered_where = where_sql.lower()
    assert "from public.research_alert_outcomes outcome_row" in lowered_definition
    assert (
        "join public.research_formula_exploration_stage4_v1 stage4_row"
        in lowered_definition
    )
    assert "stage4_row.event_id = outcome_row.event_id" in lowered_definition

    expected_columns = (
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
    assert len(expected_columns) == 19
    assert set(exploration._PATH_LABEL_FIELDS) <= set(expected_columns)
    positions = []
    for column in expected_columns:
        expression = (
            "outcome_row.created_at as outcome_created_at"
            if column == "outcome_created_at"
            else f"outcome_row.{column}"
        )
        positions.append(select_sql.lower().index(expression))
    assert positions == sorted(positions)

    # Admission is the Stage-4 signal envelope.  Projection rows and generic
    # historical/prospective rows cannot enter this view.
    for token in (
        "stage4_row.schema_version = 'research-event-v1'",
        "stage4_row.event_kind = 'decision_sample'",
        "stage4_row.capture_stage = 'silent_signal_snapshot'",
        "stage4_row.strategy_version = 'signal-snapshot-v1'",
        "stage4_row.delivery_status = 'not_applicable'",
        "stage4_row.delivery_attempted_at_utc is null",
        "stage4_row.delivered_at_utc is null",
        "'max_pain_confirmation_state'",
        "'magnet_confirmation_state'",
        "'silent_combined_confirmation_snapshot'",
        "'research-signal-snapshot-v1'",
    ):
        assert token in lowered_where, token
    assert "signal_snapshot_projection" not in lowered_where
    assert lowered_where.count("= 'false'::jsonb") == 4
    for authority_key in (
        "formula_authorized",
        "outcome_authorized",
        "telegram_delivery_allowed",
        "trade_execution_allowed",
    ):
        assert authority_key in lowered_where

    # These remain label-validation inputs.  The SQL boundary must not bless
    # a method, quality status, or timestamp merely by including the row.
    for reader_validated_field in (
        "outcome_method_version",
        "data_quality_status",
        "measured_at_utc",
        "outcome_created_at",
    ):
        assert reader_validated_field not in lowered_where

    # Owner, dependencies, view options and receipt are all asserted in the
    # migration, and the raw outcome carrier is never exposed to the reader.
    for catalog_token in (
        "outcome_owner_oid <> event_owner_oid",
        "outcome_owner_oid <> stage4_owner_oid",
        "pg_catalog.pg_get_userbyid(outcome_owner_oid) <> session_user",
        "security_barrier=true",
        "security_invoker=false",
        "cardinality(view_row.reloptions) = 2",
        "public.research_alert_outcomes",
        "public.research_formula_exploration_stage4_v1",
        "view_definition_sha256",
        "stage4_source_catalog_sha256",
        "stage4-formula-exploration-outcomes-v1",
        "pg_catalog.has_any_column_privilege",
        "pg_catalog.pg_auth_members",
    ):
        assert catalog_token in sql.lower(), catalog_token
    assert re.search(
        r"REVOKE\s+ALL\s+ON\s+TABLE\s+"
        r"public\.research_alert_outcomes\s+FROM\s+PUBLIC\s*,\s*"
        r"research_formula_exploration_reader_v1",
        sql,
        flags=re.IGNORECASE,
    )
    assert re.search(
        r"GRANT\s+SELECT\s+ON\s+TABLE\s+"
        r"public\.research_formula_exploration_outcomes_v1\s+TO\s+"
        r"research_formula_exploration_reader_v1",
        sql,
        flags=re.IGNORECASE,
    )
    assert not re.search(
        r"GRANT\s+(?:INSERT|UPDATE|DELETE|TRUNCATE|REFERENCES|TRIGGER|ALL)"
        r"[^;]*\bTO\s+research_formula_exploration_reader_v1",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    assert "drop view if exists public.research_formula_exploration_outcomes_v1" in (
        sql.lower()
    )
    assert "create table" not in sql.lower()
    assert "insert into" not in sql.lower()

    # Parse the complete migration when pglast is available in the runtime.
    try:
        from pglast import parse_sql
    except ImportError:
        pass
    else:
        assert parse_sql(sql)

    print("research_formula_exploration_outcome_view_selftest: PASS")


if __name__ == "__main__":
    run()
