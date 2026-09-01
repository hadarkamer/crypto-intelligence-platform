"""Regressions for the migration-019 read-only post-commit verifier."""

from __future__ import annotations

import hashlib
from pathlib import Path

import research_preview_staging_migration_019_admin as installer
import research_preview_staging_migration_019_readonly_verifier as verifier


ROOT = Path(__file__).resolve().parent
INTERNAL_URL = (
    f"postgresql://{verifier.EXPECTED_DATABASE_USER}:staging_password@"
    f"{verifier.EXPECTED_INTERNAL_HOST}/{verifier.EXPECTED_DATABASE_NAME}"
)


def _environment(**overrides) -> dict:
    environment = {
        verifier.VERIFY_ENABLED_ENV: "1",
        verifier.DATABASE_URL_ENV: INTERNAL_URL,
    }
    environment.update(overrides)
    return environment


def _tuple(values, fields):
    return tuple(values[name] for name in fields)


def _identity(**overrides):
    values = {
        "database_name": verifier.EXPECTED_DATABASE_NAME,
        "database_user": verifier.EXPECTED_DATABASE_USER,
        "postgres_version_num": 180004,
        "current_schema": "public",
        "transaction_read_only": "on",
        "public_schema_usage": True,
        "plpgsql_available": True,
    }
    values.update(overrides)
    return _tuple(values, verifier._IDENTITY_FIELDS)


def _catalog(base=None, **overrides):
    values = dict(
        verifier._EXPECTED_APPLIED_CATALOG
        if base is None
        else base
    )
    values.update(overrides)
    return _tuple(values, verifier._CATALOG_FIELDS)


def _rows(reservations=0, consumptions=0):
    values = {
        "reservation_row_count": reservations,
        "consumption_row_count": consumptions,
    }
    return _tuple(values, verifier._ROW_COUNT_FIELDS)


class _Cursor:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _Connection:
    def __init__(
        self,
        *,
        identity=None,
        catalog=None,
        rows=None,
        fail_sql=None,
    ):
        self.identity = _identity() if identity is None else identity
        self.catalog = _catalog() if catalog is None else catalog
        self.rows = _rows() if rows is None else rows
        self.fail_sql = fail_sql
        self.calls = []
        self.closed = False

    def execute(self, sql):
        self.calls.append(sql)
        if sql == self.fail_sql:
            raise RuntimeError("synthetic query failure")
        if sql == verifier.IDENTITY_SQL:
            return _Cursor(self.identity)
        if sql == verifier.CATALOG_SQL:
            return _Cursor(self.catalog)
        if sql == verifier.ROW_COUNT_SQL:
            return _Cursor(self.rows)
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


def _run(connection):
    connector = _Connector(connection)
    result = verifier.run_verifier(_environment(), connect=connector)
    return result, connector


def run() -> None:
    assert (
        verifier.EXPECTED_DATABASE_USER
        == "crypto_intelligence_staging_migration_019"
    )
    assert verifier.MIGRATION_FILENAME == installer.MIGRATION_FILENAME
    assert verifier.MIGRATION_SHA256 == installer.MIGRATION_SHA256
    assert verifier.EXPECTED_RENDER_POSTGRES_ID == installer.EXPECTED_RENDER_POSTGRES_ID
    assert verifier.EXPECTED_DATABASE_NAME == installer.EXPECTED_DATABASE_NAME
    assert verifier.EXPECTED_DATABASE_USER == installer.EXPECTED_DATABASE_USER
    migration_bytes = (ROOT / "migrations" / verifier.MIGRATION_FILENAME).read_bytes()
    assert hashlib.sha256(migration_bytes).hexdigest() == verifier.MIGRATION_SHA256
    installer_contract = {
        key: value
        for key, value in installer._EXPECTED_VERIFICATION.items()
        if key not in {"reservation_row_count", "consumption_row_count"}
    }
    assert verifier._EXPECTED_APPLIED_CATALOG == installer_contract

    _raises(RuntimeError, "set", lambda: verifier.resolve_configuration({}))
    _raises(
        RuntimeError,
        "is required",
        lambda: verifier.resolve_configuration(
            {verifier.VERIFY_ENABLED_ENV: "1"}
        ),
    )
    for flag in verifier._FORBIDDEN_ENABLED_FLAGS:
        _raises(
            RuntimeError,
            flag,
            lambda blocked=flag: verifier.resolve_configuration(
                _environment(**{blocked: "1"})
            ),
        )
    for name in verifier._FORBIDDEN_DATABASE_URLS:
        _raises(
            RuntimeError,
            name,
            lambda blocked=name: verifier.resolve_configuration(
                _environment(**{blocked: "postgresql://ambiguous"})
            ),
        )
    _raises(
        ValueError,
        "expected internal host",
        lambda: verifier.resolve_configuration(
            _environment(
                **{
                    verifier.DATABASE_URL_ENV: (
                        f"postgresql://{verifier.EXPECTED_DATABASE_USER}:password@"
                        f"{verifier.EXPECTED_INTERNAL_HOST}.oregon-postgres.render.com/"
                        f"{verifier.EXPECTED_DATABASE_NAME}"
                    )
                }
            )
        ),
    )
    _raises(
        ValueError,
        "unexpected database",
        lambda: verifier.resolve_configuration(
            _environment(
                **{
                    verifier.DATABASE_URL_ENV: (
                        f"postgresql://{verifier.EXPECTED_DATABASE_USER}:password@"
                        f"{verifier.EXPECTED_INTERNAL_HOST}/production"
                    )
                }
            )
        ),
    )
    _raises(
        ValueError,
        "exact dedicated credentials",
        lambda: verifier.resolve_configuration(
            _environment(
                **{
                    verifier.DATABASE_URL_ENV: (
                        "postgresql://wrong_user:password@"
                        f"{verifier.EXPECTED_INTERNAL_HOST}/"
                        f"{verifier.EXPECTED_DATABASE_NAME}"
                    )
                }
            )
        ),
    )
    _raises(
        ValueError,
        "query or fragment",
        lambda: verifier.resolve_configuration(
            _environment(
                **{verifier.DATABASE_URL_ENV: INTERNAL_URL + "?sslmode=require"}
            )
        ),
    )

    applied_connection = _Connection()
    applied, connector = _run(applied_connection)
    assert applied["status"] == "APPLIED_VERIFIED"
    assert applied["migration_019_applied"] is True
    assert applied["catalog_matches_applied_contract"] is True
    assert applied["catalog_matches_absent_contract"] is False
    assert applied["zero_application_rows_verified"] is True
    assert applied["reservation_row_count"] == 0
    assert applied["consumption_row_count"] == 0
    assert applied["catalog_mismatch_fields"] == []
    assert applied["manual_reconciliation_resolved"] is True
    assert applied["manual_intervention_required"] is False
    assert applied["row_count_query_executed"] is True
    assert applied["read_only_queries_executed"] == 3
    assert applied["transaction_rolled_back"] is True
    assert applied["schema_mutation_allowed"] is False
    assert applied["migration_apply_allowed"] is False
    assert applied["automatic_retry_allowed"] is False
    assert applied["second_migration_trigger_allowed"] is False
    assert applied["database_writes"] == 0
    assert applied["candidate_service_connected"] is False
    assert applied["telegram_api_calls"] == 0
    assert applied_connection.calls == [
        verifier.BEGIN_SQL,
        verifier.SET_SEARCH_PATH_SQL,
        verifier.IDENTITY_SQL,
        verifier.CATALOG_SQL,
        verifier.ROW_COUNT_SQL,
        verifier.ROLLBACK_SQL,
    ]
    assert applied_connection.closed is True
    assert len(connector.calls) == 1
    connected_url, options = connector.calls[0]
    assert connected_url == INTERNAL_URL
    assert options["autocommit"] is True
    assert options["connect_timeout"] == 5
    assert "default_transaction_read_only=on" in options["options"]
    assert INTERNAL_URL not in repr(applied)
    assert "staging_password" not in repr(applied)

    absent_connection = _Connection(
        catalog=_catalog(verifier._EXPECTED_ABSENT_CATALOG)
    )
    absent, _ = _run(absent_connection)
    assert absent["status"] == "NOT_APPLIED"
    assert absent["migration_019_applied"] is False
    assert absent["catalog_matches_absent_contract"] is True
    assert absent["catalog_matches_applied_contract"] is False
    assert absent["zero_application_rows_verified"] is False
    assert absent["reservation_row_count"] is None
    assert absent["consumption_row_count"] is None
    assert absent["manual_reconciliation_resolved"] is True
    assert absent["manual_intervention_required"] is False
    assert absent["row_count_query_executed"] is False
    assert absent["read_only_queries_executed"] == 2
    assert verifier.ROW_COUNT_SQL not in absent_connection.calls
    assert absent_connection.calls[-1] == verifier.ROLLBACK_SQL

    partial_connection = _Connection(
        catalog=_catalog(
            verifier._EXPECTED_ABSENT_CATALOG,
            reservation_table_exists=True,
            named_relation_count=1,
            reservation_column_count=16,
            reservation_not_null_column_count=16,
        )
    )
    partial, _ = _run(partial_connection)
    assert partial["status"] == "PARTIAL_OR_CONFLICTING"
    assert partial["migration_019_applied"] is None
    assert partial["manual_reconciliation_resolved"] is False
    assert partial["manual_intervention_required"] is True
    assert partial["row_count_query_executed"] is False
    assert "consumption_table_exists" in partial["catalog_mismatch_fields"]
    assert verifier.ROW_COUNT_SQL not in partial_connection.calls

    rows_connection = _Connection(rows=_rows(reservations=1))
    rows_present, _ = _run(rows_connection)
    assert rows_present["status"] == "PARTIAL_OR_CONFLICTING"
    assert rows_present["migration_019_applied"] is None
    assert rows_present["catalog_matches_applied_contract"] is True
    assert rows_present["zero_application_rows_verified"] is False
    assert rows_present["reservation_row_count"] == 1
    assert rows_present["catalog_mismatch_fields"] == ["reservation_row_count"]
    assert rows_present["automatic_retry_allowed"] is False

    wrong_trigger_connection = _Connection(
        catalog=_catalog(exact_trigger_mapping_count=4)
    )
    wrong_trigger, _ = _run(wrong_trigger_connection)
    assert wrong_trigger["status"] == "PARTIAL_OR_CONFLICTING"
    assert wrong_trigger["catalog_mismatch_fields"] == [
        "exact_trigger_mapping_count"
    ]
    assert verifier.ROW_COUNT_SQL not in wrong_trigger_connection.calls

    wrong_type_connection = _Connection(
        catalog=_catalog(reservation_table_exists=1)
    )
    wrong_type, _ = _run(wrong_type_connection)
    assert wrong_type["status"] == "PARTIAL_OR_CONFLICTING"
    assert "reservation_table_exists" in wrong_type["catalog_mismatch_fields"]

    unsafe_connection = _Connection(
        identity=_identity(transaction_read_only="off")
    )
    _raises(
        RuntimeError,
        "not read-only",
        lambda: _run(unsafe_connection),
    )
    assert unsafe_connection.calls[-1] == verifier.ROLLBACK_SQL
    assert unsafe_connection.closed is True

    failed_connection = _Connection(fail_sql=verifier.CATALOG_SQL)
    _raises(
        RuntimeError,
        "synthetic query failure",
        lambda: _run(failed_connection),
    )
    assert failed_connection.calls[-1] == verifier.ROLLBACK_SQL
    assert failed_connection.closed is True

    for sql in (verifier.IDENTITY_SQL, verifier.CATALOG_SQL, verifier.ROW_COUNT_SQL):
        upper_sql = sql.upper()
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
        ROOT / "research_preview_staging_migration_019_readonly_verifier.py"
    ).read_text(encoding="utf-8")
    lower_source = source.lower()
    for forbidden in (
        "import ai_candidate_main",
        "import main",
        "import telegram",
        "from telegram",
        "send_message(",
        "reply_text(",
        ".commit(",
        "research_formula_schema_admin",
        "research_preview_staging_migration_019_admin",
        "formulapreview",
    ):
        assert forbidden not in lower_source
    assert '"DATABASE_URL"' in source
    assert "configuration[\"database_url\"]" in source
    for runtime_path in ("ai_candidate_main.py", "main.py"):
        runtime_source = (ROOT / runtime_path).read_text(encoding="utf-8")
        assert "migration_019_readonly_verifier" not in runtime_source

    print("research_preview_staging_migration_019_readonly_verifier_selftest: ok")


if __name__ == "__main__":
    run()
