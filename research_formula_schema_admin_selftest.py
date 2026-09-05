"""Deterministic fail-closed checks for the Formula Research schema installer."""

from __future__ import annotations

from contextlib import contextmanager, redirect_stdout
from io import StringIO
import os

import research_formula_schema_admin as schema_admin


_ENV_NAMES = (
    "FORMULA_SCHEMA_APPLY",
    "RESEARCH_DATABASE_URL",
    "RESEARCH_USE_PRIMARY_DATABASE",
    "DATABASE_URL",
)
_CANONICAL_MIGRATION_NAMES = (
    "001_research_archive_v1.sql",
    "002_formula_research_v1.sql",
    "003_formula_autonomous_alerts_v1.sql",
    "004_historical_opportunity_replay_v1.sql",
    "005_formula_shadow_safety_v1.sql",
    "006_no_dwell_first_touch_outcomes_v6.sql",
    "007_max_pain_watch_archive_v1.sql",
    "008_prospective_neutral_anchors_v1.sql",
    "009_formula_owner_live_approval_v1.sql",
    "010_prospective_max_pain_freeze_v1.sql",
    "011_formula_owner_live_engine_binding_v2.sql",
    "012_historical_replay_v2_streaming_index.sql",
    "013_prospective_decision_feature_freeze_v1.sql",
    "014_outcome_rejection_audit_v1.sql",
    "015_formula_evidence_snapshots_v1.sql",
    "016_formula_relevance_hysteresis_v1.sql",
    "017_formula_discovery_scheduler_v1.sql",
    "018_prospective_shadow_view_indexed_union_v1.sql",
    "022_neutral_price_market_movements_v5.sql",
    "023_signal_snapshot_freeze_v1.sql",
    "024_formula_exploration_authoritative_reader_v1.sql",
    "025_formula_exploration_outcomes_v1.sql",
    "026_stage4_no_signal_outcomes_v1.sql",
    "027_stage4_signal_outcome_scan_state_v1.sql",
    "028_stage4_experimental_telegram_v1.sql",
)
_EXPECTED_OPTIONS = (
    "-c lock_timeout=3s "
    "-c statement_timeout=300s "
    "-c idle_in_transaction_session_timeout=360s"
)


@contextmanager
def _environment(**values: str):
    original = {name: os.environ.get(name) for name in _ENV_NAMES}
    try:
        for name in _ENV_NAMES:
            os.environ.pop(name, None)
        os.environ.update(values)
        yield
    finally:
        for name in _ENV_NAMES:
            os.environ.pop(name, None)
        for name, value in original.items():
            if value is not None:
                os.environ[name] = value


class _Connection:
    def __init__(self, *, fail_on_execution: int | None = None) -> None:
        self.executions: list[tuple[str, tuple | None]] = []
        self.commit_count = 0
        self.rollback_count = 0
        self.enter_count = 0
        self.exit_count = 0
        self.exit_exception_type = None
        self.fail_on_execution = fail_on_execution

    def __enter__(self):
        self.enter_count += 1
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.exit_count += 1
        self.exit_exception_type = exc_type
        # Mirror the psycopg connection-context contract closely enough to
        # prove that apply_schema lets a migration failure escape through the
        # rollback path instead of committing or swallowing it.
        if exc_type is not None:
            self.rollback_count += 1

    def execute(self, sql: str, params: tuple | None = None) -> None:
        self.executions.append((sql, params))
        if len(self.executions) == self.fail_on_execution:
            raise RuntimeError("injected migration failure")

    def commit(self) -> None:
        self.commit_count += 1


class _Psycopg:
    def __init__(self, *, fail_on_execution: int | None = None) -> None:
        self.connection = _Connection(fail_on_execution=fail_on_execution)
        self.connect_calls: list[tuple[str, dict]] = []

    def connect(self, database_url: str, **kwargs):
        self.connect_calls.append((database_url, kwargs))
        return self.connection


def _assert_runtime_error(message: str, action) -> None:
    try:
        action()
    except RuntimeError as exc:
        assert message in str(exc)
    else:
        raise AssertionError(f"Expected RuntimeError containing {message!r}")


def _check_canonical_order_and_bounded_apply() -> None:
    assert tuple(path.name for path in schema_admin.MIGRATION_PATHS) == (
        _CANONICAL_MIGRATION_NAMES
    )
    assert schema_admin.SCHEMA_CONNECTION_OPTIONS == _EXPECTED_OPTIONS

    fake_psycopg = _Psycopg()
    original_psycopg = schema_admin.psycopg
    try:
        schema_admin.psycopg = fake_psycopg
        with _environment(
            FORMULA_SCHEMA_APPLY="1",
            RESEARCH_DATABASE_URL="postgresql://dedicated/research",
            RESEARCH_USE_PRIMARY_DATABASE="1",
            DATABASE_URL="postgresql://primary/application",
        ):
            with redirect_stdout(StringIO()):
                schema_admin.apply_schema()
            status = schema_admin.status()
    finally:
        schema_admin.psycopg = original_psycopg

    assert fake_psycopg.connect_calls == [
        (
            "postgresql://dedicated/research",
            {
                "connect_timeout": 5,
                "options": _EXPECTED_OPTIONS,
            },
        )
    ]
    connection = fake_psycopg.connection
    assert connection.enter_count == 1
    assert connection.exit_count == 1
    assert connection.commit_count == 1
    assert connection.executions[0] == (
        "SELECT pg_advisory_xact_lock(%s)",
        (schema_admin.SCHEMA_LOCK_ID,),
    )
    assert [sql for sql, params in connection.executions[1:]] == [
        path.read_text(encoding="utf-8") for path in schema_admin.MIGRATION_PATHS
    ]
    assert all(params is None for _, params in connection.executions[1:])
    assert len(connection.executions) == len(schema_admin.MIGRATION_PATHS) + 1
    assert status["database_source"] == "RESEARCH_DATABASE_URL"
    assert status["database_timeout_policy"] == {
        "connect_timeout_seconds": 5,
        "lock_timeout": "3s",
        "statement_timeout": "300s",
        "idle_in_transaction_session_timeout": "360s",
    }


def _check_fail_closed_environment_selection() -> None:
    fake_psycopg = _Psycopg()
    original_psycopg = schema_admin.psycopg
    try:
        schema_admin.psycopg = fake_psycopg
        with _environment(RESEARCH_DATABASE_URL="postgresql://dedicated/research"):
            _assert_runtime_error(
                "set FORMULA_SCHEMA_APPLY=1 explicitly",
                schema_admin.apply_schema,
            )
        with _environment(
            FORMULA_SCHEMA_APPLY="1",
            DATABASE_URL="postgresql://primary/application",
        ):
            _assert_runtime_error(
                "configure RESEARCH_DATABASE_URL",
                schema_admin.apply_schema,
            )
            assert schema_admin._database_url() == ("", None)
        with _environment(
            FORMULA_SCHEMA_APPLY="1",
            RESEARCH_USE_PRIMARY_DATABASE="1",
            DATABASE_URL="postgresql://primary/application",
        ):
            assert schema_admin._database_url() == (
                "postgresql://primary/application",
                "DATABASE_URL_EXPLICIT_PRIMARY",
            )
        with _environment(
            FORMULA_SCHEMA_APPLY="unexpected",
            RESEARCH_DATABASE_URL="postgresql://dedicated/research",
        ):
            _assert_runtime_error(
                "set FORMULA_SCHEMA_APPLY=1 explicitly",
                schema_admin.apply_schema,
            )
    finally:
        schema_admin.psycopg = original_psycopg

    assert fake_psycopg.connect_calls == []


def _check_atomic_rollback_on_migration_failure() -> None:
    # Execution 1 is the advisory lock; execution 4 is the third migration.
    fake_psycopg = _Psycopg(fail_on_execution=4)
    original_psycopg = schema_admin.psycopg
    try:
        schema_admin.psycopg = fake_psycopg
        with _environment(
            FORMULA_SCHEMA_APPLY="1",
            RESEARCH_DATABASE_URL="postgresql://dedicated/research",
        ):
            _assert_runtime_error(
                "injected migration failure",
                schema_admin.apply_schema,
            )
    finally:
        schema_admin.psycopg = original_psycopg

    connection = fake_psycopg.connection
    assert connection.enter_count == 1
    assert connection.exit_count == 1
    assert connection.exit_exception_type is RuntimeError
    assert connection.rollback_count == 1
    assert connection.commit_count == 0
    assert len(connection.executions) == 4
    assert connection.executions[0] == (
        "SELECT pg_advisory_xact_lock(%s)",
        (schema_admin.SCHEMA_LOCK_ID,),
    )


def run() -> None:
    _check_canonical_order_and_bounded_apply()
    _check_fail_closed_environment_selection()
    _check_atomic_rollback_on_migration_failure()
    print("research_formula_schema_admin_selftest: PASS")


if __name__ == "__main__":
    run()
