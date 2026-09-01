"""Regressions for the one-shot PREVIEW staging read-only preflight."""

from __future__ import annotations

from pathlib import Path

import research_preview_staging_readonly_preflight as preflight


ROOT = Path(__file__).resolve().parent
INTERNAL_URL = (
    "postgresql://staging_user:staging_password@"
    f"{preflight.EXPECTED_INTERNAL_HOST}/{preflight.EXPECTED_DATABASE_NAME}"
)


def _environment(**overrides) -> dict:
    environment = {
        preflight.PREFLIGHT_ENABLED_ENV: "1",
        preflight.DATABASE_URL_ENV: INTERNAL_URL,
    }
    environment.update(overrides)
    return environment


def _row(**overrides):
    values = {
        "database_name": preflight.EXPECTED_DATABASE_NAME,
        "postgres_version": "18.4",
        "current_schema": "public",
        "transaction_read_only": "on",
        "public_schema_usage": True,
        "public_schema_create": True,
        "plpgsql_available": True,
        "reservation_table_exists": False,
        "consumption_table_exists": False,
        "validation_function_exists": False,
        "append_only_function_exists": False,
        "migration_trigger_count": 0,
    }
    values.update(overrides)
    return tuple(values[name] for name in preflight._ROW_FIELDS)


class _Cursor:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _Connection:
    def __init__(self, row, *, fail_query=False):
        self.row = row
        self.fail_query = fail_query
        self.calls = []
        self.closed = False

    def execute(self, sql):
        self.calls.append(sql)
        if sql == preflight.PREFLIGHT_SQL:
            if self.fail_query:
                raise RuntimeError("synthetic query failure")
            return _Cursor(self.row)
        return _Cursor(None)

    def close(self):
        self.closed = True


class _Connector:
    def __init__(self, connection):
        self.connection = connection
        self.calls = []

    def __call__(self, database_url, **kwargs):
        self.calls.append((database_url, dict(kwargs)))
        return self.connection


def _raises(error_type, fragment: str, callback) -> None:
    try:
        callback()
    except error_type as exc:
        assert fragment in str(exc), exc
    else:
        raise AssertionError(
            f"expected {error_type.__name__} containing {fragment!r}"
        )


def run() -> None:
    _raises(RuntimeError, "set", lambda: preflight.resolve_configuration({}))
    _raises(
        RuntimeError,
        "is required",
        lambda: preflight.resolve_configuration(
            {preflight.PREFLIGHT_ENABLED_ENV: "1"}
        ),
    )
    for mutation_flag in (
        "FORMULA_SCHEMA_APPLY",
        "RESEARCH_SCHEMA_APPLY",
        "RESEARCH_USE_PRIMARY_DATABASE",
    ):
        _raises(
            RuntimeError,
            mutation_flag,
            lambda flag=mutation_flag: preflight.resolve_configuration(
                _environment(**{flag: "1"})
            ),
        )
    _raises(
        ValueError,
        "expected internal host",
        lambda: preflight.resolve_configuration(
            _environment(
                **{
                    preflight.DATABASE_URL_ENV: (
                        "postgresql://user:password@"
                        f"{preflight.EXPECTED_INTERNAL_HOST}.oregon-postgres."
                        f"render.com/{preflight.EXPECTED_DATABASE_NAME}"
                    )
                }
            )
        ),
    )
    _raises(
        ValueError,
        "unexpected database",
        lambda: preflight.resolve_configuration(
            _environment(
                **{
                    preflight.DATABASE_URL_ENV: (
                        "postgresql://user:password@"
                        f"{preflight.EXPECTED_INTERNAL_HOST}/production"
                    )
                }
            )
        ),
    )
    _raises(
        ValueError,
        "dedicated credentials",
        lambda: preflight.resolve_configuration(
            _environment(
                **{
                    preflight.DATABASE_URL_ENV: (
                        f"postgresql://{preflight.EXPECTED_INTERNAL_HOST}/"
                        f"{preflight.EXPECTED_DATABASE_NAME}"
                    )
                }
            )
        ),
    )

    connection = _Connection(_row())
    connector = _Connector(connection)
    result = preflight.run_preflight(_environment(), connect=connector)
    assert result["status"] == "READY_FOR_SEPARATE_MIGRATION_019_DECISION"
    assert result["mode"] == preflight.MODE
    assert result["render_postgres_id"] == preflight.EXPECTED_RENDER_POSTGRES_ID
    assert result["database_name"] == preflight.EXPECTED_DATABASE_NAME
    assert result["transaction_read_only"] == "on"
    assert result["schema_object_count"] == 0
    assert result["migration_019_applied"] is False
    assert result["ready_for_separate_migration_019_decision"] is True
    assert result["transaction_rolled_back"] is True
    assert result["schema_mutation_allowed"] is False
    assert result["migration_apply_allowed"] is False
    assert result["database_writes"] == 0
    assert result["candidate_service_connected"] is False
    assert result["runtime_registered"] is False
    assert result["delivery_allowed"] is False
    assert result["telegram_api_calls"] == 0
    assert connection.calls == [
        preflight.BEGIN_SQL,
        preflight.PREFLIGHT_SQL,
        preflight.ROLLBACK_SQL,
    ]
    assert connection.closed is True
    assert len(connector.calls) == 1
    connected_url, connect_options = connector.calls[0]
    assert connected_url == INTERNAL_URL
    assert connect_options["autocommit"] is True
    assert connect_options["connect_timeout"] == 5
    assert "default_transaction_read_only=on" in connect_options["options"]
    assert INTERNAL_URL not in repr(result)
    assert "staging_password" not in repr(result)

    partial_connection = _Connection(
        _row(reservation_table_exists=True, migration_trigger_count=1)
    )
    partial = preflight.run_preflight(
        _environment(),
        connect=_Connector(partial_connection),
    )
    assert partial["status"] == "PREFLIGHT_BLOCKED"
    assert partial["schema_object_count"] == 2
    assert partial["migration_019_objects_present"] is True
    assert partial["migration_019_applied"] is False
    assert partial["ready_for_separate_migration_019_decision"] is False
    assert partial_connection.calls[-1] == preflight.ROLLBACK_SQL

    applied_connection = _Connection(
        _row(
            reservation_table_exists=True,
            consumption_table_exists=True,
            validation_function_exists=True,
            append_only_function_exists=True,
            migration_trigger_count=5,
        )
    )
    applied = preflight.run_preflight(
        _environment(),
        connect=_Connector(applied_connection),
    )
    assert applied["status"] == "PREFLIGHT_BLOCKED"
    assert applied["schema_object_count"] == 9
    assert applied["migration_019_objects_present"] is True
    assert applied["migration_019_applied"] is True
    assert applied_connection.calls[-1] == preflight.ROLLBACK_SQL

    failed_connection = _Connection(_row(), fail_query=True)
    _raises(
        RuntimeError,
        "synthetic query failure",
        lambda: preflight.run_preflight(
            _environment(),
            connect=_Connector(failed_connection),
        ),
    )
    assert failed_connection.calls == [
        preflight.BEGIN_SQL,
        preflight.PREFLIGHT_SQL,
        preflight.ROLLBACK_SQL,
    ]
    assert failed_connection.closed is True

    unsafe_row = _Connection(_row(transaction_read_only="off"))
    _raises(
        RuntimeError,
        "not read-only",
        lambda: preflight.run_preflight(
            _environment(),
            connect=_Connector(unsafe_row),
        ),
    )
    assert unsafe_row.calls[-1] == preflight.ROLLBACK_SQL
    assert unsafe_row.closed is True

    upper_sql = preflight.PREFLIGHT_SQL.upper()
    assert upper_sql.startswith("SELECT")
    for forbidden in (
        "INSERT ",
        "UPDATE ",
        "DELETE ",
        "CREATE ",
        "ALTER ",
        "DROP ",
        "TRUNCATE ",
        "GRANT ",
        "REVOKE ",
        "CALL ",
        "DO $$",
    ):
        assert forbidden not in upper_sql

    source = (
        ROOT / "research_preview_staging_readonly_preflight.py"
    ).read_text(encoding="utf-8")
    lower_source = source.lower()
    for forbidden in (
        "import main",
        "import ai_candidate_main",
        "import telegram",
        "from telegram",
        "send_message(",
        "reply_text(",
        ".commit(",
        "research_formula_schema_admin",
    ):
        assert forbidden not in lower_source
    assert "DATABASE_URL_ENV" in source
    assert '"DATABASE_URL"' not in source

    print("research_preview_staging_readonly_preflight_selftest: ok")


if __name__ == "__main__":
    run()
