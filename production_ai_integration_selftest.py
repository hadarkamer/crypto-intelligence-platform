"""Deterministic checks for the production-only analytical AI integration."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
from pathlib import Path

# The self-test must never connect to or write a real Research database.
os.environ["RESEARCH_PERSISTENCE_ENABLED"] = "0"
os.environ["RESEARCH_OUTCOME_ENRICHMENT_ENABLED"] = "0"
os.environ["RESEARCH_USE_PRIMARY_DATABASE"] = "0"
os.environ.pop("RESEARCH_DATABASE_URL", None)

import ai_agent
import ai_alert_research
import ai_telegram
import ai_tools
import binance_spot_price_path
import research_event_capture
import research_event_runtime
import research_event_store
import research_outcome_worker


EXPECTED_TOOLS = [
    "get_oi_state",
    "get_cvd_state",
    "get_market_state",
    "research_market_history",
    "get_market_context_at_time",
    "research_alert_history",
    "get_alert_context",
    "research_formula_groups",
    "get_alert_price_path",
    "get_ai_capabilities",
]


def run() -> None:
    assert ai_tools.tool_names() == EXPECTED_TOOLS
    assert all(spec.get("name") not in {"web_search", "scan_coinglass_market"} for spec in ai_tools.TOOL_SPECS)
    payload = ai_agent.AGENT._base_payload([{"role": "user", "content": "test"}])
    assert payload["tools"] == ai_tools.TOOL_SPECS
    assert not any(tool.get("type") in {"web_search", "code_interpreter"} for tool in payload["tools"])

    archive = ai_alert_research.archive_status()
    assert archive["configured"] is False
    assert archive["schema_present"] is False

    # Formula-group SQL stays parameterized for every supported grouping and
    # never needs a live database during deterministic verification.
    class _Rows:
        def fetchall(self):
            return []

    class _Connection:
        def execute(self, query, params):
            assert query.count("%s") == len(params)
            return _Rows()

    class _ConnectionContext:
        def __enter__(self):
            return _Connection()

        def __exit__(self, exc_type, exc, traceback):
            return False

    original_archive_status = ai_alert_research.archive_status
    original_connect = ai_alert_research._connect
    try:
        ai_alert_research.archive_status = lambda: {"schema_present": True}
        ai_alert_research._connect = lambda: _ConnectionContext()
        for grouping in ("signal_combination", "event_type", "symbol", "score_band"):
            formula_result = ai_alert_research.research_formula_groups(
                symbol="BTC",
                lookback_days=30,
                horizon_minutes=240,
                group_by=grouping,
                minimum_samples=3,
                limit=25,
            )
            assert formula_result["available"] is True
            assert formula_result["groups"] == []
    finally:
        ai_alert_research.archive_status = original_archive_status
        ai_alert_research._connect = original_connect

    event = research_event_capture.build_generic_alert_event(
        symbol="BTC",
        event_type="SELFTEST_ALERT",
        direction="LONG",
        event_time="2026-08-28T10:00:00Z",
        current_price=100.0,
        score=80.0,
    )
    delivered_at = datetime(2026, 8, 28, 10, 0, 2, tzinfo=timezone.utc)
    row = research_event_store.serialize_event(
        event,
        capture_stage="TELEGRAM_ALERT",
        delivery_status="DELIVERED",
        delivery_attempted_at_utc="2026-08-28T10:00:01Z",
        delivered_at_utc=delivered_at,
    )
    assert row["delivery_status"] == "DELIVERED"
    assert row["capture_stage"] == "TELEGRAM_ALERT"
    assert research_event_store.WRITER.enabled is False

    # Explicit live hooks enqueue delivery metadata; default dry-run hooks do not.
    original_enabled = research_event_store._ENABLED
    original_writer = research_event_store.WRITER
    try:
        research_event_store._ENABLED = True
        research_event_store.WRITER = research_event_store.AsyncResearchEventWriter(capacity=4)
        item = {
            "symbol": "BTC",
            "timeframe": "24h",
            "side": "SHORT",
            "types": ["NEAR_MAX_PAIN"],
            "score": 82.0,
            "current_price": 100.0,
            "target_price": 105.0,
        }
        research_event_runtime.capture_sent_maxpain(
            item,
            event_time="2026-08-28T10:05:00Z",
        )
        assert research_event_store.WRITER.queue.qsize() == 0
        research_event_runtime.capture_sent_maxpain(
            item,
            event_time="2026-08-28T10:06:00Z",
            persist=True,
            delivery_status="DELIVERED",
            delivered_at_utc="2026-08-28T10:06:02Z",
        )
        queued = research_event_store.WRITER.queue.get_nowait()
        assert queued["delivery_status"] == "DELIVERED"
        assert queued["capture_stage"] == "TELEGRAM_ALERT"

        research_event_runtime.capture_manual_maxpain_sample(
            item,
            event_time="2026-08-28T10:07:00Z",
            persist=True,
        )
        manual_sample = research_event_store.WRITER.queue.get_nowait()
        assert manual_sample["event_kind"] == "DECISION_SAMPLE"
        assert manual_sample["event_type"] == "MANUAL_MAX_PAIN_SCAN"
        assert manual_sample["capture_stage"] == "TELEGRAM_MANUAL_SCAN"
        assert manual_sample["delivery_status"] == "NOT_APPLICABLE"
    finally:
        research_event_store.WRITER = original_writer
        research_event_store._ENABLED = original_enabled

    raw_long, adjusted_long = research_outcome_worker.calculate_returns(100.0, 105.0, "LONG")
    raw_short, adjusted_short = research_outcome_worker.calculate_returns(100.0, 95.0, "SHORT")
    assert round(raw_long, 8) == 5.0 and round(adjusted_long or 0.0, 8) == 5.0
    assert round(raw_short, 8) == -5.0 and round(adjusted_short or 0.0, 8) == 5.0
    assert research_outcome_worker.WORKER.enabled is False
    outcome_status = research_outcome_worker.WORKER.status()
    assert outcome_status["method"] == "binance-spot-1m-ohlc-path-v2"
    assert outcome_status["price_path"] == {
        "exchange": "binance",
        "market": "spot",
        "interval": "1m",
        "first_partial_minute": "excluded_to_prevent_pre_alert_leakage",
    }

    # One Binance fetch covers the maximum due horizon. Existing v1 rows are
    # upgraded while already-current v2 horizons are skipped.
    event_time = datetime(2026, 8, 26, 0, 0, tzinfo=timezone.utc)
    candles = []
    for minute in range(1440):
        opened = event_time + timedelta(minutes=minute)
        candles.append(
            binance_spot_price_path.SpotCandle(
                open_time_utc=opened,
                close_time_utc=opened + timedelta(seconds=59, milliseconds=999),
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.5,
                volume=1.0,
            )
        )
    due_event = {
        "event_id": 99,
        "alert_time_utc": event_time,
        "symbol": "BTC",
        "direction": "LONG",
        "current_price": 100.0,
        "target_price": 101.0,
        "engine_snapshot": {"price_source": "binance_spot", "price_pair": "BTCUSDT"},
        "outcome_versions": {
            "60": "fixed-horizon-30m-close-v1",
            "240": "binance-spot-1m-ohlc-path-v2",
        },
    }
    fetch_calls = []
    writes = []

    class _DbContext:
        def __enter__(self):
            return object()

        def __exit__(self, exc_type, exc, traceback):
            return False

    class _Psycopg:
        @staticmethod
        def connect(*args, **kwargs):
            return _DbContext()

    original_enabled = research_outcome_worker._ENABLED
    original_database_url = research_outcome_worker._database_url
    original_psycopg = research_outcome_worker.psycopg
    original_fetch_path = binance_spot_price_path.fetch_closed_candles
    try:
        research_outcome_worker._ENABLED = True
        research_outcome_worker._database_url = lambda: "postgresql://selftest"
        research_outcome_worker.psycopg = _Psycopg()

        def _fake_path(symbol, start, end):
            fetch_calls.append((symbol, start, end))
            count = int((end - start).total_seconds() // 60)
            selected_candles = candles[:count]
            return {
                "symbol": "BTC",
                "pair": "BTCUSDT",
                "exchange": "binance",
                "market": "spot",
                "interval": "1m",
                "interval_seconds": 60,
                "candles": selected_candles,
                "expected_candles": count,
                "complete": True,
            }

        binance_spot_price_path.fetch_closed_candles = _fake_path
        worker = research_outcome_worker.ResearchOutcomeWorker()
        worker._load_due_events = lambda conn, limit: [due_event]

        def _fake_write(conn, **kwargs):
            writes.append(kwargs)
            return True

        worker._write_outcome = _fake_write
        worker_result = worker.run_once()
        assert len(fetch_calls) == 1
        assert [row["horizon"] for row in writes] == [60, 720, 1440]
        assert worker_result["inserted"] == 2
        assert worker_result["upgraded"] == 1

        class _EventRows:
            def fetchone(self):
                return due_event

        class _EventConnection:
            def execute(self, query, params):
                assert query.count("%s") == len(params)
                return _EventRows()

        class _EventConnectionContext:
            def __enter__(self):
                return _EventConnection()

            def __exit__(self, exc_type, exc, traceback):
                return False

        ai_alert_research.archive_status = lambda: {"schema_present": True}
        ai_alert_research._connect = lambda: _EventConnectionContext()
        direct_path = ai_alert_research.alert_price_path(99, 240, 20)
        assert direct_path["available"] is True
        assert direct_path["path"]["pair"] == "BTCUSDT"
        assert direct_path["path"]["full_candle_samples"] == 240
        assert direct_path["path"]["returned_points"] <= 21
    finally:
        research_outcome_worker._ENABLED = original_enabled
        research_outcome_worker._database_url = original_database_url
        research_outcome_worker.psycopg = original_psycopg
        binance_spot_price_path.fetch_closed_candles = original_fetch_path
        ai_alert_research.archive_status = original_archive_status
        ai_alert_research._connect = original_connect

    cleaned = ai_telegram._plain_telegram_text("## כותרת\n**מודגש** ו-`קוד`")
    assert cleaned == "כותרת\nמודגש ו-קוד"

    root = Path(__file__).resolve().parent
    main_text = (root / "main.py").read_text(encoding="utf-8")
    assert "ai_telegram.register_ai_handlers(bot_app)" in main_text
    assert "research_event_store.WRITER.start()" in main_text
    assert "research_outcome_worker.WORKER.start()" in main_text
    assert "special_transitions_precomputed=True" in main_text
    assert "scan_coinglass_market" not in main_text
    manual_helper = main_text.split("async def _reply_alert_with_archive", 1)[1].split(
        "def _alert_card", 1
    )[0]
    assert "capture_manual_maxpain_sample" in manual_helper
    assert "capture_sent_maxpain" not in manual_helper
    assert "capture_special_transitions" not in manual_helper

    migration = (root / "migrations" / "001_research_archive_v1.sql").read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS research_events" in migration
    assert "CREATE TABLE IF NOT EXISTS research_alert_outcomes" in migration
    assert "event_fingerprint CHAR(64) NOT NULL UNIQUE" in migration
    assert "PRIMARY KEY (event_id, horizon_minutes)" in migration

    assert "Formula-discovery researcher" in ai_agent.SYSTEM_INSTRUCTIONS
    assert "research_formula_groups" in ai_agent.SYSTEM_INSTRUCTIONS

    print("Production AI analytical integration self-test: PASS")


if __name__ == "__main__":
    run()
