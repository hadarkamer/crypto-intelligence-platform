"""Deterministic checks for the production-only analytical AI integration."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
from pathlib import Path

# The self-test must never connect to or write a real Research database.
os.environ["RESEARCH_PERSISTENCE_ENABLED"] = "0"
os.environ["RESEARCH_OUTCOME_ENRICHMENT_ENABLED"] = "0"
os.environ["RESEARCH_USE_PRIMARY_DATABASE"] = "0"
os.environ["FORMULA_DISCOVERY_ENABLED"] = "0"
os.environ["FORMULA_SHADOW_ENABLED"] = "0"
os.environ["FORMULA_LIVE_ALERTS_ENABLED"] = "0"
os.environ.pop("RESEARCH_DATABASE_URL", None)

import ai_agent
import ai_alert_research
import ai_telegram
import ai_tools
import binance_spot_price_path
import canonical_price_path
import research_event_capture
import research_event_runtime
import research_event_store
import research_outcome_worker
import research_formula_worker
import research_formula_store


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
    "research_feature_matrix",
    "research_historical_replay_status",
    "research_formula_registry",
    "research_formula_shadow",
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
    assert research_formula_worker.WORKER.status()["live_alerts_enabled"] is False
    assert (
        research_formula_worker.WORKER.status()["automatic_stage_ceiling"]
        == "SHADOW_PENDING_EXPLICIT_APPROVAL"
    )
    capped_stage, capped_notes = research_formula_store._requested_stage_for_dataset(
        {"recommended_stage": "SHADOW", "gate_notes": []},
        replacement_ready=False,
    )
    assert capped_stage == "BACKTESTED"
    assert capped_notes
    ready_stage, ready_notes = research_formula_store._requested_stage_for_dataset(
        {"recommended_stage": "SHADOW", "gate_notes": ["strict gates passed"]},
        replacement_ready=True,
    )
    assert ready_stage == "SHADOW"
    assert ready_notes == ["strict gates passed"]
    outcome_status = research_outcome_worker.WORKER.status()
    assert outcome_status["method"] == "canonical-spot-1m-ohlc-path-v3"
    assert outcome_status["first_touch_method"] == "no-dwell-first-touch-v6"
    assert outcome_status["first_touch_policy"]["post_hit_reversal"] == (
        "does not cancel success"
    )
    closed_event_time = datetime.now(timezone.utc) - timedelta(days=2)
    all_legacy_current = {
        horizon: canonical_price_path.METHOD_VERSION
        for horizon in (60, 240, 720, 1440)
    }
    assert research_outcome_worker._due_horizons(
        closed_event_time,
        all_legacy_current,
        {},
        now=datetime.now(timezone.utc),
        first_touch_enabled=False,
    ) == [], "NEUTRAL alerts must retain legacy enrichment without FT retries"
    assert outcome_status["price_paths"] == {
        "default": "Binance Spot USDT",
        "HYPE": "Hyperliquid HYPE/USDT spot (@107)",
        "market": "spot",
        "interval": "1m",
        "first_partial_minute": "excluded_to_prevent_pre_alert_leakage",
        "historical_imports": "allowed_with_source_and_quality_provenance",
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
    first_touch_writes = []

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
    original_fetch_path = canonical_price_path.fetch_closed_candles
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
                "provenance": "EXCHANGE_API_HISTORICAL_CANDLES_IMPORTED",
            }

        canonical_price_path.fetch_closed_candles = _fake_path
        worker = research_outcome_worker.ResearchOutcomeWorker()
        worker._load_open_first_touch_events = lambda conn, limit: []
        worker._load_due_events = lambda conn, limit: [due_event]

        def _fake_write(conn, **kwargs):
            writes.append(kwargs)
            return True

        worker._write_outcome = _fake_write
        worker._write_first_touch_outcome = lambda conn, **kwargs: (
            first_touch_writes.append(kwargs) or True
        )
        worker_result = worker.run_once()
        assert len(fetch_calls) == 1
        assert [row["horizon"] for row in writes] == [60, 240, 720, 1440]
        assert [row["horizon"] for row in first_touch_writes] == [
            60,
            240,
            720,
            1440,
        ]
        assert all(row["first_touch"]["dwell_required_seconds"] == 0 for row in first_touch_writes)
        assert worker_result["inserted"] == 2
        assert worker_result["upgraded"] == 2
        assert worker_result["first_touch_rows_written"] == 4

        # An unavailable canonical symbol is requested only once per run,
        # even when several archived alerts need outcomes.  It remains
        # eligible for a retry on the next worker cycle.
        unavailable_fetch_calls = []
        unavailable_events = [
            {**due_event, "event_id": 100, "symbol": "NOPE"},
            {**due_event, "event_id": 101, "symbol": "NOPE"},
        ]

        def _fake_path_with_unavailable_symbol(symbol, start, end):
            unavailable_fetch_calls.append(symbol)
            if symbol == "NOPE":
                raise RuntimeError("symbol is unavailable on its canonical route")
            return _fake_path(symbol, start, end)

        canonical_price_path.fetch_closed_candles = _fake_path_with_unavailable_symbol
        unavailable_worker = research_outcome_worker.ResearchOutcomeWorker()
        unavailable_worker._load_open_first_touch_events = lambda conn, limit: []
        unavailable_worker._load_due_events = lambda conn, limit: [
            *unavailable_events,
            due_event,
        ]
        unavailable_worker._write_outcome = _fake_write
        unavailable_worker._write_first_touch_outcome = lambda conn, **kwargs: True
        unavailable_result = unavailable_worker.run_once()
        assert unavailable_fetch_calls.count("NOPE") == 1
        assert unavailable_result["missing_price_paths"] == 2
        assert unavailable_result["unavailable_symbols"] == {"NOPE": 2}

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
        canonical_price_path.fetch_closed_candles = original_fetch_path
        ai_alert_research.archive_status = original_archive_status
        ai_alert_research._connect = original_connect

    cleaned = ai_telegram._plain_telegram_text("## כותרת\n**מודגש** ו-`קוד`")
    assert cleaned == "כותרת\nמודגש ו-קוד"
    table = ai_telegram._plain_telegram_text(
        "| מדד | ערך |\n|---|---|\n| MFE | 2.5% |"
    )
    assert "|---|" not in table and "• מדד: MFE" in table

    root = Path(__file__).resolve().parent
    main_text = (root / "main.py").read_text(encoding="utf-8")
    assert "ai_telegram.register_ai_handlers(bot_app)" in main_text
    assert "research_event_store.WRITER.start()" in main_text
    assert "research_outcome_worker.WORKER.start()" in main_text
    assert "research_formula_worker.WORKER.start()" in main_text
    assert "research_formula_worker.WORKER.bind_telegram(bot_app.bot)" in main_text
    assert "research_formula_schema_admin.apply_schema" in main_text
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

    formula_migration = (root / "migrations" / "002_formula_research_v1.sql").read_text(
        encoding="utf-8"
    )
    assert "CREATE TABLE IF NOT EXISTS research_formulas" in formula_migration
    assert "CREATE TABLE IF NOT EXISTS research_formula_evaluations" in formula_migration
    assert "CREATE TABLE IF NOT EXISTS research_formula_shadow_hits" in formula_migration
    assert "delivery_status = 'NOT_SENT'" in formula_migration
    assert "current_stage NOT IN ('APPROVED', 'LIVE') OR live_alert_approved = TRUE" in formula_migration
    autonomous_migration = (
        root / "migrations" / "003_formula_autonomous_alerts_v1.sql"
    ).read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS research_formula_alert_subscriptions" in autonomous_migration
    assert "CREATE TABLE IF NOT EXISTS research_formula_live_deliveries" in autonomous_migration
    replay_migration = (
        root / "migrations" / "004_historical_opportunity_replay_v1.sql"
    ).read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS research_historical_replay_runs" in replay_migration
    assert "CREATE TABLE IF NOT EXISTS research_historical_opportunity_outcomes" in replay_migration
    shadow_safety_migration = (
        root / "migrations" / "005_formula_shadow_safety_v1.sql"
    ).read_text(encoding="utf-8")
    for required_shadow_column in (
        "evaluation_status",
        "evaluation_reason",
        "input_snapshot",
        "condition_results",
        "decision_cohort_key",
        "decision_anchor_time_utc",
    ):
        assert f"ADD COLUMN IF NOT EXISTS {required_shadow_column}" in shadow_safety_migration
    assert "CREATE TABLE IF NOT EXISTS research_formula_live_approvals" in shadow_safety_migration
    assert "approved_by TEXT NOT NULL" in shadow_safety_migration
    assert "approval_reason TEXT NOT NULL" in shadow_safety_migration
    assert "review_kind TEXT NOT NULL" in shadow_safety_migration
    assert "validation_cutoff_event_id BIGINT NOT NULL" in shadow_safety_migration
    assert "validated_future_matches INTEGER NOT NULL" in shadow_safety_migration
    assert "validated_future_controls INTEGER NOT NULL" in shadow_safety_migration
    assert "thresholds_met BOOLEAN NOT NULL" in shadow_safety_migration
    assert "UNIQUE (formula_id, formula_version)" in shadow_safety_migration
    assert "BEFORE UPDATE OR DELETE ON research_formula_live_approvals" in shadow_safety_migration
    assert "research_formula_live_approvals is append-only" in shadow_safety_migration
    first_touch_migration = (
        root / "migrations" / "006_no_dwell_first_touch_outcomes_v6.sql"
    ).read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS research_first_touch_outcomes" in first_touch_migration
    assert "dwell_required_seconds = 0" in first_touch_migration
    assert "status IN ('PENDING', 'HIT', 'MISS')" in first_touch_migration
    assert "ADD COLUMN IF NOT EXISTS long_first_touch_metrics" in first_touch_migration
    assert "ADD COLUMN IF NOT EXISTS first_touch_replay_run_id" in first_touch_migration
    assert "ADD COLUMN IF NOT EXISTS first_touch_method_version" in first_touch_migration

    assert "Formula-discovery researcher" in ai_agent.SYSTEM_INSTRUCTIONS
    assert "research_formula_groups" in ai_agent.SYSTEM_INSTRUCTIONS
    assert "research_formula_registry" in ai_agent.SYSTEM_INSTRUCTIONS
    assert "Market session is a first-class analytical variable" in ai_agent.SYSTEM_INSTRUCTIONS
    assert "MAE p75, p90 and p95 on three separate" in ai_agent.SYSTEM_INSTRUCTIONS
    formula_store_text = (root / "research_formula_store.py").read_text(encoding="utf-8")
    assert "superseded by newer same-horizon discovery cohort" in formula_store_text
    assert "hierarchical evidence-family formula schema v6" in formula_store_text
    assert "replacement_ready" in formula_store_text
    assert "latest_evaluation_run_id IS DISTINCT FROM %s" in formula_store_text
    readiness_body = formula_store_text.split(
        "def evaluate_shadow_readiness", 1
    )[1].split("def promote_eligible_shadow_formulas", 1)[0]
    assert "ready_for_explicit_review" in readiness_body
    assert '"promoted": []' in readiness_body
    assert "SET current_stage='LIVE'" not in readiness_body
    assert "live_alert_approved=TRUE" not in readiness_body
    assert "FROM research_formula_live_approvals a" in formula_store_text
    assert "f.live_alert_approved=TRUE" in formula_store_text
    assert "and current[\"current_stage\"] == \"LIVE\"" in formula_store_text
    assert "_DELIVERY_MAX_AGE_MINUTES" in formula_store_text
    telegram_text = (root / "ai_telegram.py").read_text(encoding="utf-8")
    assert "נוסחאות LIVE פעילות" in telegram_text
    assert "MAE p75: \\1" in telegram_text
    matrix_text = (root / "research_feature_matrix.py").read_text(encoding="utf-8")
    assert "live.symbol<>'HYPE' OR live.price_source='hyperliquid'" in matrix_text

    print("Production AI analytical integration self-test: PASS")


if __name__ == "__main__":
    run()
