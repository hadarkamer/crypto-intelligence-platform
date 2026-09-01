"""Regressions for the isolated PREVIEW staging migration-019 installer."""

from __future__ import annotations

from pathlib import Path

import research_preview_staging_migration_019_admin as installer


ROOT = Path(__file__).resolve().parent


def _database_url(
    *,
    user=installer.EXPECTED_DATABASE_USER,
    password="synthetic_password",
    host=installer.EXPECTED_INTERNAL_HOST,
    port=None,
    database=installer.EXPECTED_DATABASE_NAME,
    suffix="",
) -> str:
    target = host if port is None else f"{host}:{port}"
    return (
        "postgresql"
        + "://"
        + f"{user}:{password}"
        + "@"
        + f"{target}/{database}{suffix}"
    )


INTERNAL_URL = _database_url()


def _environment(**overrides) -> dict:
    environment = {
        installer.APPLY_ENABLED_ENV: "1",
        installer.DATABASE_URL_ENV: INTERNAL_URL,
    }
    environment.update(overrides)
    return environment


def _precondition_row(**overrides):
    values = {
        "database_name": installer.EXPECTED_DATABASE_NAME,
        "database_user": installer.EXPECTED_DATABASE_USER,
        "postgres_version_num": 180004,
        "current_schema": "public",
        "transaction_read_only": "off",
        "public_schema_usage": True,
        "public_schema_create": True,
        "plpgsql_available": True,
        "migration_relation_count": 0,
        "migration_function_count": 0,
        "migration_trigger_count": 0,
    }
    values.update(overrides)
    return tuple(values[name] for name in installer._PRECONDITION_FIELDS)


def _verification_row(**overrides):
    values = dict(installer._EXPECTED_VERIFICATION)
    values.update(overrides)
    return tuple(values[name] for name in installer._VERIFICATION_FIELDS)


class _Cursor:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _Connection:
    def __init__(
        self,
        *,
        precondition_row=None,
        verification_row=None,
        fail_sql=None,
        fail_close=False,
    ):
        self.precondition_row = (
            _precondition_row() if precondition_row is None else precondition_row
        )
        self.verification_row = (
            _verification_row() if verification_row is None else verification_row
        )
        self.fail_sql = fail_sql
        self.fail_close = fail_close
        self.calls = []
        self.closed = False

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        if sql == self.fail_sql:
            raise RuntimeError("synthetic database failure")
        if sql == installer.PRECONDITION_SQL:
            return _Cursor(self.precondition_row)
        if sql == installer.VERIFICATION_SQL:
            return _Cursor(self.verification_row)
        return _Cursor(None)

    def close(self):
        self.closed = True
        if self.fail_close:
            raise RuntimeError("synthetic close failure")


class _Connector:
    def __init__(self, connection):
        self.connection = connection
        self.calls = []

    def __call__(self, database_url, **kwargs):
        self.calls.append((database_url, dict(kwargs)))
        return self.connection


def _raises(error_type, fragment: str, callback) -> BaseException:
    try:
        callback()
    except error_type as exc:
        assert fragment in str(exc), exc
        return exc
    raise AssertionError(f"expected {error_type.__name__} containing {fragment!r}")


def run() -> None:
    assert (
        installer.EXPECTED_DATABASE_USER
        == "crypto_intelligence_staging_migration_019"
    )
    _raises(RuntimeError, "set", lambda: installer.resolve_configuration({}))
    _raises(
        RuntimeError,
        "is required",
        lambda: installer.resolve_configuration(
            {installer.APPLY_ENABLED_ENV: "1"}
        ),
    )
    for flag in installer._FORBIDDEN_ENABLED_FLAGS:
        _raises(
            RuntimeError,
            flag,
            lambda flag=flag: installer.resolve_configuration(
                _environment(**{flag: "1"})
            ),
        )
    for name in installer._FORBIDDEN_DATABASE_URLS:
        _raises(
            RuntimeError,
            name,
            lambda name=name: installer.resolve_configuration(
                _environment(**{name: "postgresql://ambiguous"})
            ),
        )
    _raises(
        ValueError,
        "expected internal host",
        lambda: installer.resolve_configuration(
            _environment(
                **{
                    installer.DATABASE_URL_ENV: _database_url(
                        host=(
                            f"{installer.EXPECTED_INTERNAL_HOST}."
                            "oregon-postgres.render.com"
                        )
                    )
                }
            )
        ),
    )
    _raises(
        ValueError,
        "unexpected database",
        lambda: installer.resolve_configuration(
            _environment(
                **{
                    installer.DATABASE_URL_ENV: _database_url(database="production")
                }
            )
        ),
    )
    _raises(
        ValueError,
        "exact dedicated credentials",
        lambda: installer.resolve_configuration(
            _environment(
                **{
                    installer.DATABASE_URL_ENV: _database_url(user="wrong_user")
                }
            )
        ),
    )
    _raises(
        ValueError,
        "unexpected port",
        lambda: installer.resolve_configuration(
            _environment(
                **{
                    installer.DATABASE_URL_ENV: _database_url(port=6432)
                }
            )
        ),
    )
    _raises(
        ValueError,
        "query or fragment",
        lambda: installer.resolve_configuration(
            _environment(
                **{
                    installer.DATABASE_URL_ENV: _database_url(
                        suffix="?sslmode=require"
                    )
                }
            )
        ),
    )

    assert installer.load_migration() == installer.MIGRATION_PATH.read_text(
        encoding="utf-8"
    )
    original_checksum = installer.MIGRATION_SHA256
    connector = _Connector(_Connection())
    try:
        installer.MIGRATION_SHA256 = "0" * 64
        _raises(
            RuntimeError,
            "checksum mismatch",
            lambda: installer.run_installer(
                _environment(),
                connect=connector,
            ),
        )
    finally:
        installer.MIGRATION_SHA256 = original_checksum
    assert connector.calls == []

    migration_sql = installer.load_migration()
    connection = _Connection()
    connector = _Connector(connection)
    result = installer.run_installer(_environment(), connect=connector)
    assert result["status"] == "MIGRATION_019_APPLIED_AND_VERIFIED"
    assert result["mode"] == installer.MODE
    assert result["render_postgres_id"] == installer.EXPECTED_RENDER_POSTGRES_ID
    assert result["database_name"] == installer.EXPECTED_DATABASE_NAME
    assert result["internal_target_verified"] is True
    assert result["migration_sha256"] == installer.MIGRATION_SHA256
    assert result["precondition_clean"] is True
    assert result["preexisting_object_count"] == 0
    assert result["catalog_verification_passed"] is True
    assert result["verified_table_count"] == 2
    assert result["verified_function_count"] == 2
    assert result["verified_trigger_count"] == 5
    assert result["verified_constraint_bindings"] is True
    assert installer._EXPECTED_VERIFICATION["reservation_row_count"] == 0
    assert installer._EXPECTED_VERIFICATION["consumption_row_count"] == 0
    assert result["migration_files_executed"] == 1
    assert result["commit_attempts"] == 1
    assert result["transaction_committed"] is True
    assert result["schema_mutation_committed"] is True
    assert result["migration_019_applied"] is True
    assert result["application_rows_written"] == 0
    assert result["automatic_retry_allowed"] is False
    assert result["candidate_service_connected"] is False
    assert result["runtime_database_registered"] is False
    assert result["delivery_allowed"] is False
    assert result["telegram_api_calls"] == 0
    assert INTERNAL_URL not in repr(result)
    assert "synthetic_password" not in repr(result)

    expected_calls = [
        (installer.BEGIN_SQL, None),
        (installer.SET_LOCK_TIMEOUT_SQL, None),
        (installer.SET_STATEMENT_TIMEOUT_SQL, None),
        (installer.SET_SEARCH_PATH_SQL, None),
        (installer.LOCK_SQL, (installer.SCHEMA_LOCK_ID,)),
        (installer.PRECONDITION_SQL, None),
        (migration_sql, None),
        (installer.VERIFICATION_SQL, None),
        (installer.COMMIT_SQL, None),
    ]
    assert connection.calls == expected_calls
    assert connection.closed is True
    assert len(connector.calls) == 1
    connected_url, connect_options = connector.calls[0]
    assert connected_url == INTERNAL_URL
    assert connect_options == {
        "connect_timeout": 5,
        "autocommit": True,
        "options": "-c application_name=preview_staging_migration_019_once",
    }

    close_failure = _Connection(fail_close=True)
    close_failure_result = installer.run_installer(
        _environment(),
        connect=_Connector(close_failure),
    )
    assert close_failure_result["transaction_committed"] is True
    assert close_failure.closed is True

    dirty_connection = _Connection(
        precondition_row=_precondition_row(migration_relation_count=1)
    )
    _raises(
        RuntimeError,
        "not clean",
        lambda: installer.run_installer(
            _environment(),
            connect=_Connector(dirty_connection),
        ),
    )
    assert (migration_sql, None) not in dirty_connection.calls
    assert dirty_connection.calls[-1] == (installer.ROLLBACK_SQL, None)
    assert (installer.COMMIT_SQL, None) not in dirty_connection.calls
    assert dirty_connection.closed is True

    wrong_target = _Connection(
        precondition_row=_precondition_row(database_name="production")
    )
    _raises(
        RuntimeError,
        "identity",
        lambda: installer.run_installer(
            _environment(),
            connect=_Connector(wrong_target),
        ),
    )
    assert wrong_target.calls[-1] == (installer.ROLLBACK_SQL, None)

    for overrides, fragment in (
        ({"database_user": "production_user"}, "user"),
        ({"postgres_version_num": 170009}, "major version"),
        ({"current_schema": "other"}, "search path"),
        ({"transaction_read_only": "on"}, "read-write"),
        ({"public_schema_usage": False}, "usage privilege"),
        ({"public_schema_create": False}, "create privilege"),
        ({"plpgsql_available": False}, "plpgsql"),
        ({"migration_function_count": 1}, "not clean"),
        ({"migration_trigger_count": 1}, "not clean"),
    ):
        blocked_connection = _Connection(
            precondition_row=_precondition_row(**overrides)
        )
        _raises(
            RuntimeError,
            fragment,
            lambda connection=blocked_connection: installer.run_installer(
                _environment(),
                connect=_Connector(connection),
            ),
        )
        assert (migration_sql, None) not in blocked_connection.calls
        assert blocked_connection.calls[-1] == (installer.ROLLBACK_SQL, None)
        assert (installer.COMMIT_SQL, None) not in blocked_connection.calls

    bad_verification = _Connection(
        verification_row=_verification_row(exact_trigger_mapping_count=4)
    )
    _raises(
        RuntimeError,
        "catalog verification mismatch",
        lambda: installer.run_installer(
            _environment(),
            connect=_Connector(bad_verification),
        ),
    )
    assert (migration_sql, None) in bad_verification.calls
    assert bad_verification.calls[-1] == (installer.ROLLBACK_SQL, None)
    assert (installer.COMMIT_SQL, None) not in bad_verification.calls
    assert bad_verification.closed is True

    unexpected_row = _Connection(
        verification_row=_verification_row(reservation_row_count=1)
    )
    _raises(
        RuntimeError,
        "catalog verification mismatch",
        lambda: installer.run_installer(
            _environment(),
            connect=_Connector(unexpected_row),
        ),
    )
    assert unexpected_row.calls[-1] == (installer.ROLLBACK_SQL, None)
    assert (installer.COMMIT_SQL, None) not in unexpected_row.calls

    precommit_failure = _Connection(fail_sql=installer.VERIFICATION_SQL)
    _raises(
        RuntimeError,
        "synthetic database failure",
        lambda: installer.run_installer(
            _environment(),
            connect=_Connector(precommit_failure),
        ),
    )
    assert precommit_failure.calls[-1] == (installer.ROLLBACK_SQL, None)
    assert precommit_failure.closed is True

    uncertain_connection = _Connection(fail_sql=installer.COMMIT_SQL)
    uncertain = _raises(
        installer.Migration019CommitOutcomeUncertain,
        "read-only reconciliation",
        lambda: installer.run_installer(
            _environment(),
            connect=_Connector(uncertain_connection),
        ),
    )
    assert uncertain_connection.calls[-1] == (installer.COMMIT_SQL, None)
    assert (installer.ROLLBACK_SQL, None) not in uncertain_connection.calls
    assert uncertain_connection.closed is True
    uncertain_result = installer._failed_closed(uncertain)
    assert uncertain_result["status"] == "MIGRATION_019_COMMIT_OUTCOME_UNCERTAIN"
    assert uncertain_result["migration_019_applied"] is None
    assert uncertain_result["manual_read_only_reconciliation_required"] is True
    assert uncertain_result["automatic_retry_allowed"] is False
    assert uncertain_result["database_url_exposed"] is False

    failed = installer._failed_closed(RuntimeError("secret synthetic_password"))
    assert failed["status"] == "MIGRATION_019_FAILED_CLOSED"
    assert failed["migration_019_applied"] is False
    assert failed["manual_read_only_reconciliation_required"] is False
    assert failed["database_url_exposed"] is False
    assert "synthetic_password" not in repr(failed)

    assert installer.PRECONDITION_SQL.upper().startswith("SELECT")
    assert installer.VERIFICATION_SQL.upper().startswith("WITH")
    for query in (installer.PRECONDITION_SQL, installer.VERIFICATION_SQL):
        upper_query = query.upper()
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
            assert forbidden not in upper_query

    source = (
        ROOT / "research_preview_staging_migration_019_admin.py"
    ).read_text(encoding="utf-8")
    lower_source = source.lower()
    for forbidden in (
        "import main",
        "import ai_candidate_main",
        "import telegram",
        "from telegram",
        "send_message(",
        "reply_text(",
        "research_formula_schema_admin",
        "migration_paths",
        ".commit(",
    ):
        assert forbidden not in lower_source
    assert "MIGRATION_PATH = ROOT / \"migrations\" / MIGRATION_FILENAME" in source
    assert "connector(" in source
    assert "migration_sql = load_migration()" in source
    assert "connection.execute(migration_sql)" in source
    assert "os.getenv(" not in source

    for runtime_path in ("ai_candidate_main.py", "main.py"):
        runtime_source = (ROOT / runtime_path).read_text(encoding="utf-8")
        assert "research_preview_staging_migration_019_admin" not in runtime_source

    print("research_preview_staging_migration_019_admin_selftest: ok")


if __name__ == "__main__":
    run()
