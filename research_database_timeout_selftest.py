"""Focused checks for the shared heavy-research PostgreSQL timeout."""

from __future__ import annotations

import research_database_timeout as timeout
import research_feature_matrix as feature_matrix
import research_formula_store as formula_store
import research_outcome_worker as outcome_worker
import research_prospective_anchor_store as anchor_store


class _Psycopg:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    def connect(self, url: str, **kwargs: object) -> object:
        self.calls.append((url, kwargs))
        return object()


class _StatementTimeoutConnection:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, _query, _params=()):
        raise RuntimeError("canceling statement due to statement timeout")


def _options(fake: _Psycopg) -> str:
    return str(fake.calls[-1][1]["options"])


def run() -> None:
    env = timeout.ENV_HEAVY_STATEMENT_TIMEOUT_MS
    assert timeout.heavy_statement_timeout_ms({}) == 120_000
    assert timeout.heavy_statement_timeout_ms({env: ""}) == 120_000
    assert timeout.heavy_statement_timeout_ms({env: "invalid"}) == 120_000
    assert timeout.heavy_statement_timeout_ms({env: "0"}) == 30_000
    assert timeout.heavy_statement_timeout_ms({env: "29999"}) == 30_000
    assert timeout.heavy_statement_timeout_ms({env: "90000"}) == 90_000
    assert timeout.heavy_statement_timeout_ms({env: "300001"}) == 300_000

    status = timeout.heavy_timeout_status({env: "0"})
    assert status == {
        "environment_variable": env,
        "statement_timeout_ms": 30_000,
        "default_ms": 120_000,
        "minimum_ms": 30_000,
        "maximum_ms": 300_000,
        "finite": True,
    }

    original_feature_psycopg = feature_matrix.psycopg
    original_formula_psycopg = formula_store.psycopg
    original_formula_database_url = formula_store._database_url
    original_anchor_psycopg = anchor_store.psycopg
    original_timeout = timeout.heavy_statement_timeout_ms
    try:
        timeout.heavy_statement_timeout_ms = lambda environ=None: 90_000

        feature_psycopg = _Psycopg()
        feature_matrix.psycopg = feature_psycopg
        feature_matrix._connect("postgresql://feature-selftest")
        feature_options = _options(feature_psycopg)
        assert "statement_timeout=90000" in feature_options
        assert "default_transaction_read_only=on" in feature_options

        formula_psycopg = _Psycopg()
        formula_store.psycopg = formula_psycopg
        formula_store._database_url = lambda: "postgresql://formula-selftest"
        formula_store._connect(read_only=True)
        operational_formula_options = _options(formula_psycopg)
        assert "statement_timeout=20000" in operational_formula_options
        assert "default_transaction_read_only=on" in operational_formula_options
        assert "lock_timeout" not in operational_formula_options

        formula_store._connect(read_only=True, heavy=True)
        heavy_formula_read_options = _options(formula_psycopg)
        assert "statement_timeout=90000" in heavy_formula_read_options
        assert "default_transaction_read_only=on" in heavy_formula_read_options
        assert "lock_timeout" not in heavy_formula_read_options

        formula_store._connect(read_only=False, heavy=True)
        heavy_formula_write_options = _options(formula_psycopg)
        assert "statement_timeout=90000" in heavy_formula_write_options
        assert "default_transaction_read_only" not in heavy_formula_write_options
        assert "lock_timeout=3000" in heavy_formula_write_options

        anchor_psycopg = _Psycopg()
        anchor_store.psycopg = anchor_psycopg
        store = anchor_store.ProspectiveAnchorStore(
            database_url="postgresql://anchor-selftest"
        )
        store._connect()
        anchor_options = _options(anchor_psycopg)
        assert "statement_timeout=90000" in anchor_options
        assert "lock_timeout=3000" in anchor_options
        assert "default_transaction_read_only" not in anchor_options

        outcome_options = outcome_worker._database_connection_options()
        assert "statement_timeout=90000" in outcome_options
        assert "lock_timeout=1000" in outcome_options
        assert "default_transaction_read_only" not in outcome_options
    finally:
        timeout.heavy_statement_timeout_ms = original_timeout
        feature_matrix.psycopg = original_feature_psycopg
        formula_store.psycopg = original_formula_psycopg
        formula_store._database_url = original_formula_database_url
        anchor_store.psycopg = original_anchor_psycopg

    effective = timeout.heavy_statement_timeout_ms()
    assert feature_matrix.runtime_status()["heavy_query_timeout"][
        "statement_timeout_ms"
    ] == effective
    custom_anchor = anchor_store.ProspectiveAnchorStore(
        connection_factory=lambda: object()
    ).status()
    assert custom_anchor["heavy_query_timeout"]["statement_timeout_ms"] == effective
    assert custom_anchor["database_connection_options_enforced"] is False
    timed_out_anchor = anchor_store.ProspectiveAnchorStore(
        connection_factory=_StatementTimeoutConnection
    )
    try:
        timed_out_anchor.existing_captured_symbols(
            symbols=("BTC",),
            slot_open_utc="2026-09-04T12:00:00Z",
        )
    except RuntimeError:
        pass
    else:
        raise AssertionError("anchor timeout probe must preserve the failure")
    operation = timed_out_anchor.status()["database_operation"]
    assert operation["current_operation"] == "IDLE"
    assert operation["last_operation"] == "LOAD_EXISTING_SLOTS"
    assert operation["last_error_operation"] == "LOAD_EXISTING_SLOTS"
    assert operation["last_timeout_operation"] == "LOAD_EXISTING_SLOTS"
    assert operation["last_timeout_at_utc"] is not None
    assert operation["last_operation_duration_ms"] is not None
    assert formula_store.schema_status()["heavy_query_timeout"][
        "statement_timeout_ms"
    ] == effective
    formula_policy = formula_store.database_timeout_status()
    assert formula_policy["operational_statement_timeout_ms"] == 20_000
    assert formula_policy["heavy_write_lock_timeout_ms"] == 3_000
    assert formula_policy["heavy_query"]["statement_timeout_ms"] == effective
    print("research_database_timeout_selftest: PASS")


if __name__ == "__main__":
    run()
