"""Network-free checks for the immutable Stage-4 no-signal carrier."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import os
from pathlib import Path
import re
from types import SimpleNamespace

import research_outcome_worker as worker


BASE = datetime(2026, 9, 4, 12, 0, tzinfo=timezone.utc)
REFERENCE_HASH_TAGS = (
    "hash_contract_version",
    "reference_contract_version",
    "projection_event_id",
    "projection_event_fingerprint",
    "snapshot_set_id",
    "snapshot_key",
    "set_payload_sha256",
    "symbol",
    "symbol_manifest_payload_sha256",
    "source_timeframe",
    "snapshot_row_id",
    "snapshot_row_payload_sha256",
    "official_price_float8_hex",
    "official_price_source",
    "official_price_exchange",
    "official_price_market",
    "official_price_pair",
    "official_price_instrument",
    "official_price_interval",
    "official_price_fetched_at_utc",
    "official_price_observed_at_utc",
    "official_price_candle_open_time_utc",
    "official_price_candle_close_time_utc",
    "official_price_policy_status",
)
OUTCOME_HASH_TAGS = (
    "hash_contract_version",
    "carrier_contract_version",
    "projection_event_id",
    "projection_event_fingerprint",
    "snapshot_set_id",
    "snapshot_key",
    "symbol",
    "direction",
    "horizon_minutes",
    "decision_time_utc",
    "absence_basis",
    "cell_identity_sha256",
    "reference_receipt_sha256",
    "measured_at_utc",
    "reference_price_float8_hex",
    "price_at_horizon_float8_hex",
    "raw_return_pct_float8_hex",
    "directional_return_pct_float8_hex",
    "max_favorable_price_float8_hex",
    "max_adverse_price_float8_hex",
    "mfe_pct_float8_hex",
    "mae_pct_float8_hex",
    "time_to_first_progress_seconds",
    "time_to_mfe_seconds",
    "path_resolution_seconds",
    "path_samples",
    "outcome_method_version",
    "price_source",
    "data_quality_status",
)


def _canonical_hash_payload(fields: tuple[tuple[str, str | None], ...]) -> str:
    encoded = []
    for tag, value in fields:
        if value is None:
            encoded.append(f"{tag}=-1:")
        else:
            encoded.append(f"{tag}={len(value.encode('utf-8'))}:{value}")
    return "\x1f".join(encoded)


def _cell(direction: str) -> dict:
    return {
        "projection_event_id": 901,
        "projection_event_fingerprint": "a" * 64,
        "decision_time_utc": BASE,
        "snapshot_set_id": 81,
        "snapshot_key": "b" * 64,
        "set_payload_sha256": "c" * 64,
        "symbol": "BTC",
        "direction": direction,
        "manifest_payload_sha256": "d" * 64,
        "reference_snapshot_row_id": 701,
        "reference_row_payload_sha256": "e" * 64,
        "reference_price": 100.0,
        "price_source": "binance_spot",
        "price_exchange": "binance",
        "price_market": "spot",
        "price_pair": "BTCUSDT",
        "price_instrument": "BTC",
        "price_fetched_at_utc": BASE - timedelta(minutes=2),
        "price_source_policy_status": "PASS",
        "raw_provenance": {
            "price_interval": "1m",
            "price_observed_at_utc": (
                BASE - timedelta(minutes=3)
            ).isoformat(),
            "price_candle_open_time_utc": (
                BASE - timedelta(minutes=4)
            ).isoformat(),
            "price_candle_close_time_utc": (
                BASE - timedelta(minutes=3, milliseconds=1)
            ).isoformat(),
        },
        "archive_row_count": 7,
        "archive_price_signature_count": 1,
        "outcome_versions": {},
        "due_horizons": [60],
    }


def _candles() -> list[SimpleNamespace]:
    result = []
    for minute in range(60):
        opened = BASE + timedelta(minutes=minute)
        result.append(
            SimpleNamespace(
                open_time_utc=opened,
                close_time_utc=opened + timedelta(seconds=59, milliseconds=999),
                open=100.0,
                high=101.0 + minute / 100.0,
                low=99.0 - minute / 200.0,
                close=100.0 + minute / 100.0,
                volume=1.0,
            )
        )
    return result


def _path() -> dict:
    return {
        "symbol": "BTC",
        "exchange": "binance",
        "market": "spot",
        "pair": "BTCUSDT",
        "interval": "1m",
        "interval_seconds": 60,
        "complete": True,
        "provenance": "EXCHANGE_API_HISTORICAL_CANDLES_IMPORTED",
        "candles": _candles(),
    }


class _EmptyResult:
    def __init__(self) -> None:
        self.query = ""
        self.params = ()

    def execute(self, query, params=()):
        self.query = str(query)
        self.params = tuple(params)
        return self

    def fetchall(self):
        return []


def _check_reference() -> None:
    price, source, receipt = worker._stage4_no_signal_frozen_price_reference(
        _cell("LONG")
    )
    assert price == 100.0
    assert receipt["contract_version"] == (
        "stage4-no-signal-frozen-archive-price-reference-v1"
    )
    assert receipt["official_price"]["observed_at_utc"].endswith("Z")
    assert source.startswith(
        "reference_policy=stage4-no-signal-frozen-archive-price-reference-v1|"
    )
    assert (
        "admission_policy="
        "stage4-no-signal-completed-projection-evaluable-cell-admission-v1"
    ) in source
    invalid = _cell("LONG")
    invalid["raw_provenance"] = dict(invalid["raw_provenance"])
    invalid["raw_provenance"]["price_observed_at_utc"] = (
        BASE + timedelta(seconds=1)
    ).isoformat()
    try:
        worker._stage4_no_signal_frozen_price_reference(invalid)
    except ValueError:
        pass
    else:
        raise AssertionError("post-decision no-signal reference was accepted")


def _check_bounded_queries() -> None:
    capture = _EmptyResult()
    assert worker.ResearchOutcomeWorker._load_due_stage4_no_signal_projection_page(
        capture, cursor=(BASE, 7), page_size=9999
    ) == []
    assert "due_stage4_no_signal_projection_page" in capture.query
    assert "jsonb_array_elements" not in capture.query.lower()
    assert "(e.alert_time_utc, e.event_id) > (%s, %s)" in capture.query
    assert capture.params[-1] == worker._STAGE4_NO_SIGNAL_PROJECTION_PAGE_SIZE
    assert "FROM public.research_events e" in capture.query

    loader = (
        worker.ResearchOutcomeWorker
        ._load_stage4_no_signal_cells_for_projection_page
    )
    assert loader(capture, []) == []
    hydrated = _EmptyResult()
    assert loader(hydrated, [1]) == []
    for relation in (
        "research_events",
        "research_max_pain_snapshot_sets",
        "research_max_pain_snapshot_symbols",
        "research_max_pain_snapshot_rows",
        "research_stage4_no_signal_outcomes_v1",
    ):
        assert f"public.{relation}" in hydrated.query
        assert re.search(rf"(?<!public\.)\b{relation}\b", hydrated.query) is None


def _check_network_outside_database_and_direction_pair() -> None:
    active_connections = 0
    connect_calls = 0
    fetch_calls = 0
    writes: list[dict] = []

    class _Connection:
        def __enter__(self):
            nonlocal active_connections
            active_connections += 1
            return self

        def __exit__(self, exc_type, exc, traceback):
            nonlocal active_connections
            active_connections -= 1
            return False

    class _Psycopg:
        @staticmethod
        def connect(*args, **kwargs):
            nonlocal connect_calls
            connect_calls += 1
            return _Connection()

    def fetch(symbol, start, end):
        nonlocal fetch_calls
        assert active_connections == 0
        assert symbol == "BTC"
        fetch_calls += 1
        return _path()

    original_psycopg = worker.psycopg
    original_fetch = worker.canonical_price_path.fetch_closed_candles
    original_database_url = worker._database_url
    original_url = os.environ.get(worker._STAGE4_NO_SIGNAL_DATABASE_URL_ENV)
    instance = worker.ResearchOutcomeWorker()
    instance._load_due_stage4_no_signal_cells = lambda conn, limit, now: [
        _cell("LONG"),
        _cell("SHORT"),
    ]
    instance._write_stage4_no_signal_outcome = (
        lambda conn, **value: writes.append(value) or True
    )
    try:
        os.environ[worker._STAGE4_NO_SIGNAL_DATABASE_URL_ENV] = (
            "postgresql://writer@research.example/research"
        )
        worker._database_url = lambda: (
            "postgresql://owner@research.example/research"
        )
        worker.psycopg = _Psycopg
        worker.canonical_price_path.fetch_closed_candles = fetch
        result = instance._run_stage4_no_signal_once(
            limit=10, now=BASE + timedelta(minutes=61)
        )
    finally:
        worker.psycopg = original_psycopg
        worker.canonical_price_path.fetch_closed_candles = original_fetch
        worker._database_url = original_database_url
        if original_url is None:
            os.environ.pop(worker._STAGE4_NO_SIGNAL_DATABASE_URL_ENV, None)
        else:
            os.environ[worker._STAGE4_NO_SIGNAL_DATABASE_URL_ENV] = original_url
    assert active_connections == 0
    assert connect_calls == 2
    assert fetch_calls == 1
    assert result == {
        "configured": True,
        "checked": 2,
        "inserted": 2,
        "missing_price_paths": 0,
    }
    assert {item["cell"]["direction"] for item in writes} == {"LONG", "SHORT"}
    by_direction = {
        item["cell"]["direction"]: item["path_metrics"] for item in writes
    }
    assert by_direction["LONG"]["raw_return_pct"] == (
        by_direction["SHORT"]["raw_return_pct"]
    )
    assert by_direction["LONG"]["directional_return_pct"] == -(
        by_direction["SHORT"]["directional_return_pct"]
    )


def _check_projection_symbol_pair_is_atomic() -> None:
    instance = worker.ResearchOutcomeWorker()
    instance._load_due_stage4_no_signal_projection_page = (
        lambda conn, cursor=None: [
            {"projection_event_id": 901, "decision_time_utc": BASE}
        ]
    )
    instance._load_stage4_no_signal_cells_for_projection_page = (
        lambda conn, projection_event_ids: [_cell("LONG"), _cell("SHORT")]
    )
    cells = instance._load_due_stage4_no_signal_cells(
        object(), limit=1, now=BASE + timedelta(minutes=61)
    )
    assert len(cells) == 2
    assert {cell["direction"] for cell in cells} == {"LONG", "SHORT"}


def _check_connection_guardrails() -> None:
    assert worker._database_target_identity(
        "postgresql://writer:secret@research.example:5432/research"
    ) == worker._database_target_identity(
        "postgresql://owner:different@research.example/research"
    )
    original_database_url = worker._database_url
    try:
        worker._database_url = lambda: (
            "postgresql://owner@research.example/research"
        )
        try:
            worker._assert_stage4_no_signal_database_target(
                "postgresql://writer@other.example/research"
            )
        except RuntimeError as exc:
            assert "differs" in str(exc)
        else:
            raise AssertionError("a mismatched no-signal database was accepted")
    finally:
        worker._database_url = original_database_url
    options = worker._stage4_no_signal_connection_options()
    for required in (
        "statement_timeout=",
        "lock_timeout=1000",
        "search_path=pg_catalog,public",
        "timezone=UTC",
        "DateStyle=ISO,YMD",
        "IntervalStyle=postgres",
        "extra_float_digits=3",
        "row_security=on",
    ):
        assert required in options


def _check_canonical_hash_contract() -> None:
    payload = _canonical_hash_payload(
        (
            ("alpha", "x"),
            ("empty", ""),
            ("missing", None),
            ("utf8", "₪"),
        )
    )
    assert payload == "alpha=1:x\x1fempty=0:\x1fmissing=-1:\x1futf8=3:₪"
    assert hashlib.sha256(payload.encode("utf-8")).hexdigest() == (
        "bf849e4b3e6319eb5ec9805be1cdf934606bb0cfd627d04a0f4c9470deee746d"
    )


def _check_carrier_failure_isolated_from_legacy_run() -> None:
    transaction_commands = []

    class _Connection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def execute(self, query, params=()):
            assert not params
            command = " ".join(str(query).split())
            assert command in {
                "SAVEPOINT research_open_first_touch_load",
                "RELEASE SAVEPOINT research_open_first_touch_load",
            }
            transaction_commands.append(command)
            return self

    class _Psycopg:
        @staticmethod
        def connect(*args, **kwargs):
            return _Connection()

    original_enabled = worker._ENABLED
    original_psycopg = worker.psycopg
    original_database_url = worker._database_url
    instance = worker.ResearchOutcomeWorker()
    instance._load_open_first_touch_events = lambda conn, limit: []
    instance._load_due_events = lambda conn, limit: []
    instance._load_frozen_threshold_references = lambda conn, event_ids: []
    instance._load_current_slot_threshold_references = lambda events, now: {}
    instance._run_stage4_no_signal_once = lambda **kwargs: (_ for _ in ()).throw(
        RuntimeError("isolated carrier failure")
    )
    try:
        worker._ENABLED = True
        worker.psycopg = _Psycopg
        worker._database_url = lambda: "postgresql://legacy"
        result = instance.run_once(limit_per_horizon=1)
    finally:
        worker._ENABLED = original_enabled
        worker.psycopg = original_psycopg
        worker._database_url = original_database_url
    assert result["enabled"] is True
    assert result["checked"] == 0
    assert result["inserted"] == 0
    assert instance.metrics.runs == 1
    assert instance.metrics.stage4_no_signal_failures == 1
    assert "isolated carrier failure" in (
        instance.metrics.stage4_no_signal_last_error or ""
    )
    assert transaction_commands == [
        "SAVEPOINT research_open_first_touch_load",
        "RELEASE SAVEPOINT research_open_first_touch_load",
    ]


def _check_legacy_failure_cannot_starve_carrier() -> None:
    carrier_calls = 0

    class _Psycopg:
        @staticmethod
        def connect(*args, **kwargs):
            raise RuntimeError("legacy load failed")

    original_enabled = worker._ENABLED
    original_psycopg = worker.psycopg
    original_database_url = worker._database_url
    instance = worker.ResearchOutcomeWorker()

    def run_carrier(**kwargs):
        nonlocal carrier_calls
        carrier_calls += 1
        return {
            "configured": True,
            "checked": 2,
            "inserted": 1,
            "missing_price_paths": 0,
        }

    instance._run_stage4_no_signal_once = run_carrier
    try:
        worker._ENABLED = True
        worker.psycopg = _Psycopg
        worker._database_url = lambda: (
            "postgresql://owner@research.example/research"
        )
        try:
            instance.run_once(limit_per_horizon=1)
        except RuntimeError as exc:
            assert "legacy load failed" in str(exc)
        else:
            raise AssertionError("the synthetic legacy failure was not raised")
    finally:
        worker._ENABLED = original_enabled
        worker.psycopg = original_psycopg
        worker._database_url = original_database_url
    assert carrier_calls == 1
    assert instance.metrics.stage4_no_signal_cells_checked == 2
    assert instance.metrics.stage4_no_signal_outcomes_inserted == 1


def _check_migration_contract() -> None:
    migration = (
        Path(__file__).resolve().parent
        / "migrations"
        / "026_stage4_no_signal_outcomes_v1.sql"
    ).read_text(encoding="utf-8")
    required = (
        "research_stage4_no_signal_outcomes_v1",
        "research_formula_exploration_no_signal_outcomes_v1",
        "research_stage4_no_signal_outcome_writer_v1",
        "COMPLETED_PROJECTION_EVALUABLE_SYMBOL_WITHOUT_SIGNAL",
        "canonical-spot-1m-ohlc-path-v3+stage4-no-signal-frozen-archive-input-v1",
        "stage4-no-signal-completed-projection-evaluable-cell-admission-v1",
        "outcome_payload_sha256",
        "stage4_source_catalog_sha256=",
        "raw_catalog_sha256=",
        "trigger_catalog_sha256=",
        "WITH (security_barrier = true, security_invoker = false)",
        "BEFORE UPDATE OR DELETE",
        "BEFORE TRUNCATE",
        "ENABLE ALWAYS TRIGGER",
        "ON DELETE RESTRICT",
        "Basic manual rollback",
    )
    for fragment in required:
        assert fragment in migration, fragment
    assert "GRANT SELECT, INSERT ON TABLE" in migration
    assert "GRANT SELECT ON TABLE\n    public.research_formula_exploration" in migration
    assert "telegram_delivery_allowed" in migration
    assert "trade_execution_allowed" in migration
    assert "DROP VIEW IF EXISTS" in migration
    assert "NEW.created_at_utc := pg_catalog.transaction_timestamp()" in migration
    assert "source_token_count IS DISTINCT FROM 16" in migration
    assert "NEW.path_samples IS DISTINCT FROM expected_path_samples" in migration
    assert "NOT (projection.categories @>" in migration
    assert "categories NOT @>" not in migration
    assert "idx_research_stage4_completed_projection_due_v1" not in migration
    assert "REVOKE USAGE ON SCHEMA public" in migration
    assert "public.nspacl has no direct grantee" in migration
    assert "Effective USAGE may\n-- remain inherited from PUBLIC" in migration
    assert "do not\n-- revoke schema USAGE from PUBLIC" in migration
    assert "Effective SELECT on every source relation" in migration
    assert "pg_catalog.pg_get_constraintdef" in migration
    assert "pg_catalog.pg_get_indexdef" in migration
    assert "function_row.prosrc" in migration
    assert migration.count("relation.relrowsecurity") >= 2
    assert migration.count("relation.relforcerowsecurity") >= 2
    assert migration.count("policy_row.polrelid=raw_oid") == 2
    assert migration.count("rewrite_row.ev_class=raw_oid") == 2
    assert migration.count("pg_catalog.has_database_privilege(") == 4
    assert "FROM research_stage4_no_signal_outcome_writer_v1 RESTRICT" in migration
    assert migration.count(
        "function_row.pronamespace='public'::REGNAMESPACE"
    ) == 3
    assert "acl.privilege_type<>'SELECT' OR acl.is_grantable" in migration
    assert "'research_formula_exploration_reader_v1', 'public', 'USAGE'" in migration
    assert "pg_catalog.cardinality(relation.reloptions), 0\n                )<>2" in migration
    assert "'security_barrier=true',\n                    'security_invoker=false'" in migration
    assert "stage4-no-signal-reference-receipt-hash-v1" in migration
    assert "stage4-no-signal-outcome-payload-hash-v1" in migration
    assert "NEW.reference_receipt::TEXT" not in migration
    tag_blocks = re.findall(
        r"hash_tags\s*:=\s*ARRAY\[(.*?)\]::TEXT\[\];",
        migration,
        flags=re.DOTALL,
    )
    assert len(tag_blocks) == 2
    parsed_tags = [tuple(re.findall(r"'([^']+)'", block)) for block in tag_blocks]
    assert parsed_tags == [REFERENCE_HASH_TAGS, OUTCOME_HASH_TAGS]

    # Parse the complete migration when PostgreSQL's grammar is available in
    # the runtime.  Production migration validation remains mandatory before
    # apply; this keeps the local test useful in the lean worker image too.
    try:
        from pglast import parse_sql
    except ImportError:
        pass
    else:
        assert parse_sql(migration)


def main() -> None:
    _check_reference()
    _check_bounded_queries()
    _check_network_outside_database_and_direction_pair()
    _check_projection_symbol_pair_is_atomic()
    _check_connection_guardrails()
    _check_canonical_hash_contract()
    _check_carrier_failure_isolated_from_legacy_run()
    _check_legacy_failure_cannot_starve_carrier()
    _check_migration_contract()
    print("Stage-4 no-signal outcome carrier self-test: PASS")


if __name__ == "__main__":
    main()
