"""Static contract checks for additive ordered First Touch migration 020."""

from pathlib import Path
import os
from types import SimpleNamespace
from unittest.mock import patch

import research_formula_schema_admin
import research_formula_store


ROOT = Path(__file__).resolve().parent
MIGRATION = ROOT / "migrations" / "020_ordered_first_touch_v7.sql"


class _Connection:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False


def _check_targeted_schema_apply() -> None:
    calls = []
    connection_calls = []

    class MigrationConnection(_Connection):
        def execute(self, query, params=()):
            calls.append((query, params))

        def commit(self):
            calls.append(("COMMIT", ()))

    def connect(*args, **kwargs):
        connection_calls.append((args, kwargs))
        return MigrationConnection()

    with patch.dict(os.environ, {
        "FORMULA_SCHEMA_APPLY": "1",
        "FORMULA_SCHEMA_APPLY_ONLY": MIGRATION.name,
        "RESEARCH_DATABASE_URL": "postgresql://selftest",
    }), patch.object(
        research_formula_schema_admin,
        "psycopg",
        SimpleNamespace(connect=connect),
    ):
        research_formula_schema_admin.apply_schema()
        assert len(connection_calls) == 1
        assert connection_calls[0][1]["options"] == (
            "-c statement_timeout=15000 -c lock_timeout=1000"
        )
        assert len(calls) == 3
        assert "pg_advisory_xact_lock" in calls[0][0]
        assert calls[1][0] == MIGRATION.read_text(encoding="utf-8")
        assert calls[2][0] == "COMMIT"

        for invalid in (
            "missing.sql",
            "../migrations/" + MIGRATION.name,
            MIGRATION.name + "," + MIGRATION.name,
            MIGRATION.name + ",",
            "*",
        ):
            os.environ["FORMULA_SCHEMA_APPLY_ONLY"] = invalid
            try:
                research_formula_schema_admin.apply_schema()
            except ValueError:
                pass
            else:
                raise AssertionError("unsafe migration selector was accepted")
            assert len(connection_calls) == 1

        previous = next(path for path in research_formula_schema_admin.MIGRATION_PATHS
                        if path.name == "019_outcome_worker_queue_indexes_v1.sql")
        os.environ["FORMULA_SCHEMA_APPLY_ONLY"] = (
            MIGRATION.name + ", " + previous.name
        )
        assert research_formula_schema_admin._selected_migration_paths() == (
            previous, MIGRATION,
        )
        os.environ["FORMULA_SCHEMA_APPLY_ONLY"] = ""
        assert research_formula_schema_admin._selected_migration_paths() == (
            research_formula_schema_admin.MIGRATION_PATHS
        )


def run() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")
    normalized = " ".join(sql.split())

    paths = research_formula_schema_admin.MIGRATION_PATHS
    assert paths.count(MIGRATION.resolve()) == 1
    position = paths.index(MIGRATION.resolve())
    assert paths[position - 1].name == "019_outcome_worker_queue_indexes_v1.sql"

    assert "CREATE UNIQUE INDEX IF NOT EXISTS idx_research_events_event_direction" in sql
    assert "CREATE TABLE IF NOT EXISTS research_ordered_first_touch_outcomes" in sql
    assert (
        "PRIMARY KEY ( event_id, window_minutes, threshold_bps, method_version )"
        in normalized
    )
    assert "method_version = 'ordered-first-touch-v7'" in sql
    assert "threshold_bps IN (25, 50, 75, 100, 125, 150, 175, 200)" in normalized
    for field in (
        "first_touch_side",
        "terminal_reason",
        "measurement_start_utc",
        "first_observed_open_utc",
        "observed_through_utc",
        "decision_time_utc",
        "time_to_decision_seconds",
        "initial_gap_unobserved",
        "favorable_barrier_price",
        "adverse_barrier_price",
        "favorable_touch_price",
        "adverse_touch_price",
        "max_favorable_price",
        "max_adverse_price",
        "mfe_pct",
        "mae_pct",
        "input_path_complete",
        "path_complete",
        "observation_closed",
        "data_quality_status",
        "data_quality_note",
        "threshold_policy",
        "calculation_audit",
    ):
        assert field in sql

    for status in (
        "'OPEN'",
        "'SUCCESS'",
        "'FAILURE'",
        "'UNRESOLVED'",
        "'DATA_MISSING'",
    ):
        assert status in sql
    for reason in (
        "'FAVORABLE_FIRST'",
        "'ADVERSE_FIRST'",
        "'SAME_CANDLE_BOTH'",
        "'OBSERVATION_WINDOW_CLOSED_NO_TOUCH'",
        "'INCOMPLETE_PATH'",
    ):
        assert reason in sql
    for quality in (
        "'VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES'",
        "'PARTIAL_BINANCE_SPOT_1M_CLOSED_CANDLES'",
        "'VERIFIED_HYPERLIQUID_SPOT_1M_CLOSED_CANDLES'",
        "'PARTIAL_HYPERLIQUID_SPOT_1M_CLOSED_CANDLES'",
    ):
        assert quality in sql

    assert "CREATE TABLE IF NOT EXISTS research_ordered_first_touch_sync_outbox" in sql
    assert "remote_row_key TEXT NOT NULL" in sql
    assert "payload_sha256 CHAR(64) NOT NULL" in sql
    assert "'IN_FLIGHT'" in sql
    assert "claim_token UUID" in sql
    assert "claimed_payload_sha256 = payload_sha256" in sql
    assert "idx_ordered_first_touch_sync_queue" in sql
    assert "WHERE sync_status IN ('PENDING', 'RETRY')" in sql
    assert "idx_ordered_first_touch_expired_lease" in sql
    assert "WHERE sync_status = 'IN_FLIGHT'" in sql
    assert "idx_ordered_first_touch_remote_row" in sql

    # High-value integrity checks: fixed 1m path resolution, finite floating
    # values, state/path equivalence, and explicit closed-no-touch semantics.
    assert "candle_interval_seconds = 60" in normalized
    assert "path_samples >= 0 AND path_samples <= window_minutes" in normalized
    assert "path_complete = (status <> 'DATA_MISSING')" in normalized
    assert "NOT path_complete OR input_path_complete" in normalized
    assert "path_complete AND data_quality_status IN" in normalized
    assert "NOT path_complete AND data_quality_status IN" in normalized
    assert "'Infinity'::DOUBLE PRECISION" in sql
    assert "'NaN'::DOUBLE PRECISION" in sql
    assert "favorable_touch_price = favorable_barrier_price" in normalized
    assert "adverse_touch_price = adverse_barrier_price" in normalized
    assert "observation_closed IS TRUE" in normalized
    assert "decision_time_utc IS NULL" in normalized
    assert "initial_gap_unobserved AND initial_gap_seconds > 0" in normalized

    # Every DDL creation is replay-safe because the installer deliberately
    # replays the complete ordered migration list.
    upper = sql.upper()
    assert upper.count("CREATE TABLE ") == upper.count(
        "CREATE TABLE IF NOT EXISTS "
    )
    assert upper.count("CREATE INDEX ") == upper.count(
        "CREATE INDEX IF NOT EXISTS "
    )
    assert upper.count("CREATE UNIQUE INDEX ") == upper.count(
        "CREATE UNIQUE INDEX IF NOT EXISTS "
    )

    # Migration 020 must not rewrite or reinterpret the retained v6 evidence.
    assert "ALTER TABLE research_first_touch_outcomes" not in sql
    assert "UPDATE research_first_touch_outcomes" not in sql
    assert "DROP TABLE" not in sql.upper()
    assert "DROP COLUMN" not in sql.upper()

    # Runtime readiness must fail closed when migration 020 did not install,
    # even if every older Formula Research relation is still present.
    original_connect = research_formula_store._connect
    original_database_url = research_formula_store._database_url
    original_table_exists = research_formula_store._table_exists
    original_missing_columns = research_formula_store._missing_columns
    try:
        research_formula_store._database_url = lambda: "postgresql://selftest"
        research_formula_store._connect = lambda *, read_only=False: _Connection()
        research_formula_store._table_exists = (
            lambda conn, table: table
            != "research_ordered_first_touch_outcomes"
        )
        missing_relation = research_formula_store.schema_status()
        assert missing_relation["schema_present"] is False
        assert "research_ordered_first_touch_outcomes" in (
            missing_relation["missing_tables"]
        )

        captured_required = {}

        def missing_v7_column(conn, required):
            captured_required.update(required)
            return [
                "research_ordered_first_touch_sync_outbox.claim_token"
            ]

        research_formula_store._table_exists = lambda conn, table: True
        research_formula_store._missing_columns = missing_v7_column
        missing_column = research_formula_store.schema_status()
        assert missing_column["schema_present"] is False
        assert missing_column["missing_columns"] == [
            "research_ordered_first_touch_sync_outbox.claim_token"
        ]
        assert "observation_closed" in captured_required[
            "research_ordered_first_touch_outcomes"
        ]
        assert "adverse_touch_price" in captured_required[
            "research_ordered_first_touch_outcomes"
        ]
        assert "claim_token" in captured_required[
            "research_ordered_first_touch_sync_outbox"
        ]
        assert "claimed_payload_sha256" in captured_required[
            "research_ordered_first_touch_sync_outbox"
        ]
    finally:
        research_formula_store._connect = original_connect
        research_formula_store._database_url = original_database_url
        research_formula_store._table_exists = original_table_exists
        research_formula_store._missing_columns = original_missing_columns

    _check_targeted_schema_apply()
    print("ordered First Touch v7 schema self-test: PASS")


if __name__ == "__main__":
    run()
