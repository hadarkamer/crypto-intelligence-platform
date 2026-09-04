"""Network-free checks for guarded Research background task wiring."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import inspect
import os

import main


async def _archive_only_check() -> None:
    original_dom = main.collect_coinglass_dom_snapshot
    original_official = main.live_price_provider.enrich_research_snapshot_rows
    original_operational = main.live_price_provider.enrich_snapshot_rows
    original_archive = main._archive_max_pain_collection_attempt
    archive_calls = []

    async def fake_dom(**kwargs):
        return {
            "ok": True,
            "missing_timeframes": [],
            "rows": [
                {
                    "symbol": "BTC",
                    "rank": rank,
                    "timeframe": timeframe,
                    "price": 100.0,
                    "max_short_price": 110.0,
                    "max_long_price": 95.0,
                    "short_amount_usd": 200.0,
                    "long_amount_usd": 100.0,
                    "collected_at_utc": "2026-08-29T12:00:00Z",
                }
                for rank, timeframe in enumerate(main.TIMEFRAMES, start=1)
            ],
        }

    def fake_official(rows, excluded):
        return {
            "rows": list(rows),
            "skipped_symbols": [],
            "price_result": {"source": "official_closed_spot_1m_no_fallback"},
        }

    def forbidden_operational(*args, **kwargs):
        raise AssertionError("archive-only scheduler entered bot fallback pricing")

    async def fake_archive(**kwargs):
        archive_calls.append(kwargs)

    main.collect_coinglass_dom_snapshot = fake_dom
    main.live_price_provider.enrich_research_snapshot_rows = fake_official
    main.live_price_provider.enrich_snapshot_rows = forbidden_operational
    main._archive_max_pain_collection_attempt = fake_archive
    try:
        rows, result = await main.collect_live_rows_for_watch(
            archive_context={
                "cycle_id": "selftest",
                "cycle_time_utc": "2026-08-29T12:00:00Z",
                "source": "RESEARCH_PASSIVE",
            },
            archive_only=True,
        )
    finally:
        main.collect_coinglass_dom_snapshot = original_dom
        main.live_price_provider.enrich_research_snapshot_rows = original_official
        main.live_price_provider.enrich_snapshot_rows = original_operational
        main._archive_max_pain_collection_attempt = original_archive
    assert len(rows) == 7
    assert result["price_result"]["source"] == "official_closed_spot_1m_no_fallback"
    assert len(archive_calls) == 1


async def _archive_enrichment_failure_capture_handoff_check() -> None:
    archive_capture = {"selftest": "failed-enrichment-capture"}
    archive_calls = []
    original_dom = main.collect_coinglass_dom_snapshot
    original_official = main.live_price_provider.enrich_research_snapshot_rows
    original_archive = main._archive_max_pain_collection_attempt

    async def fake_dom(**kwargs):
        return {
            "ok": True,
            "missing_timeframes": [],
            "rows": [
                {
                    "symbol": "BTC",
                    "rank": 1,
                    "timeframe": timeframe,
                    "price": 100.0,
                    "max_short_price": 110.0,
                    "max_long_price": 95.0,
                    "short_amount_usd": 200.0,
                    "long_amount_usd": 100.0,
                    "collected_at_utc": "2026-08-29T12:00:00Z",
                }
                for timeframe in main.TIMEFRAMES
            ],
        }

    def failed_official(*_args, **_kwargs):
        raise RuntimeError("official enrichment unavailable")

    async def fake_archive(**kwargs):
        archive_calls.append(kwargs)
        return archive_capture

    main.collect_coinglass_dom_snapshot = fake_dom
    main.live_price_provider.enrich_research_snapshot_rows = failed_official
    main._archive_max_pain_collection_attempt = fake_archive
    try:
        rows, result = await main.collect_live_rows_for_watch(
            archive_context={
                "cycle_id": "failure-selftest",
                "cycle_time_utc": "2026-08-29T12:00:00Z",
                "source": "RESEARCH_PASSIVE",
            },
            archive_only=True,
        )
    finally:
        main.collect_coinglass_dom_snapshot = original_dom
        main.live_price_provider.enrich_research_snapshot_rows = original_official
        main._archive_max_pain_collection_attempt = original_archive

    assert rows == []
    assert result["rows"] == []
    assert result["_research_archive_capture"] is archive_capture
    assert result["timeframe_integrity"]["counts"] == {
        timeframe: 0 for timeframe in main.TIMEFRAMES
    }
    assert "official enrichment unavailable" in result["price_result"]["error"]
    assert len(archive_calls) == 1
    assert archive_calls[0]["enriched_rows"] == []
    assert "OfficialResearchPriceError" in archive_calls[0]["failure_reason"]


async def _projection_capture_handoff_check() -> None:
    archive_capture = {"selftest": "archive-capture-sentinel"}
    submitted_captures = []
    reconciliation_calls = []
    enabled_results = iter((True, True, False))

    class FakeProjector:
        def submit_reconciliation(self):
            reconciliation_calls.append(True)
            return {"submitted": True}

        def submit(self, capture):
            submitted_captures.append(capture)
            return {"submitted": True}

    async def fake_collect(**kwargs):
        assert kwargs["archive_only"] is True
        return [], {"_research_archive_capture": archive_capture}

    original_projector = main.research_signal_snapshot_runtime.PROJECTOR
    original_enabled = main.research_max_pain_archive.archive_enabled
    original_schedule = main._next_max_pain_archive_schedule
    original_collect = main.collect_live_rows_for_watch
    original_scrape_lock = main.SCRAPE_LOCK
    original_archive_runtime = dict(main.MAX_PAIN_ARCHIVE_RUNTIME)
    original_scan_owner = main.WATCH_RUNTIME.get("scan_owner")
    main.research_signal_snapshot_runtime.PROJECTOR = FakeProjector()
    main.research_max_pain_archive.archive_enabled = lambda: next(
        enabled_results, False
    )
    main._next_max_pain_archive_schedule = lambda: (
        datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 1, tzinfo=timezone.utc),
    )
    main.collect_live_rows_for_watch = fake_collect
    main.SCRAPE_LOCK = asyncio.Lock()
    try:
        await main._max_pain_archive_loop()
    finally:
        main.research_signal_snapshot_runtime.PROJECTOR = original_projector
        main.research_max_pain_archive.archive_enabled = original_enabled
        main._next_max_pain_archive_schedule = original_schedule
        main.collect_live_rows_for_watch = original_collect
        main.SCRAPE_LOCK = original_scrape_lock
        main.MAX_PAIN_ARCHIVE_RUNTIME.clear()
        main.MAX_PAIN_ARCHIVE_RUNTIME.update(original_archive_runtime)
        main.WATCH_RUNTIME["scan_owner"] = original_scan_owner
    assert len(reconciliation_calls) == 2
    assert submitted_captures == [archive_capture]
    assert submitted_captures[0] is archive_capture


def run() -> None:
    formula_worker = main.research_formula_worker
    formula_now = datetime(2026, 8, 29, 12, 4, 59, tzinfo=timezone.utc)
    slot, due_at = formula_worker._discovery_schedule(60, now=formula_now)
    assert slot == datetime(2026, 8, 29, 11, 0, tzinfo=timezone.utc)
    assert due_at == datetime(2026, 8, 29, 11, 5, tzinfo=timezone.utc)
    slot, due_at = formula_worker._discovery_schedule(
        60, now=formula_now + timedelta(seconds=1)
    )
    assert slot == datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
    assert due_at == datetime(2026, 8, 29, 12, 5, tzinfo=timezone.utc)
    slot, due_at = formula_worker._discovery_schedule(
        240, now=datetime(2026, 8, 29, 12, 5, tzinfo=timezone.utc)
    )
    assert slot == datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
    assert due_at == datetime(2026, 8, 29, 12, 5, tzinfo=timezone.utc)
    next_due = formula_worker._next_discovery_due_at(
        now=datetime(2026, 8, 29, 12, 5, tzinfo=timezone.utc)
    )
    assert next_due == datetime(2026, 8, 29, 13, 5, tzinfo=timezone.utc)

    watermark_dataset = {
        "available": True,
        "feature_schema_version": "selftest-features",
        "outcome_method_version": "selftest-outcomes",
        "sample_size": 2,
        "first_alert_time_utc": "2026-08-28T00:00:00Z",
        "last_alert_time_utc": "2026-08-28T01:00:00Z",
        "coverage": {
            "dataset_kind": "selftest",
            "analysis_as_of_utc": "2026-08-29T12:05:00Z",
        },
        "rows": [{"event": {"event_id": 1}}, {"event": {"event_id": 2}}],
    }
    watermark_a, digest_a = formula_worker._dataset_watermark(
        watermark_dataset, horizon_minutes=60
    )
    later_as_of = dict(watermark_dataset)
    later_as_of["coverage"] = dict(watermark_dataset["coverage"])
    later_as_of["coverage"]["analysis_as_of_utc"] = "2026-08-29T13:05:00Z"
    watermark_b, digest_b = formula_worker._dataset_watermark(
        later_as_of, horizon_minutes=60
    )
    assert watermark_a == watermark_b
    assert digest_a == digest_b
    changed = dict(watermark_dataset)
    changed["rows"] = [*watermark_dataset["rows"], {"event": {"event_id": 3}}]
    _, digest_c = formula_worker._dataset_watermark(changed, horizon_minutes=60)
    assert digest_c != digest_a

    worker = formula_worker.FormulaResearchWorker()
    feature_timeout = formula_worker.research_feature_matrix.runtime_status()[
        "heavy_query_timeout"
    ]["statement_timeout_ms"]
    assert (
        worker.status()["heavy_query_timeout"]["statement_timeout_ms"]
        == feature_timeout
    )
    telemetry_worker = formula_worker.FormulaResearchWorker()
    try:
        with telemetry_worker._phase("shadow", "load_features"):
            raise RuntimeError("canceling statement due to statement timeout")
    except RuntimeError:
        pass
    else:
        raise AssertionError("formula phase probe must preserve failures")
    assert telemetry_worker.metrics.shadow_phase == "IDLE"
    assert telemetry_worker.metrics.last_shadow_phase == "LOAD_FEATURES"
    assert telemetry_worker.metrics.last_shadow_error_phase == "LOAD_FEATURES"
    assert telemetry_worker.metrics.last_shadow_timeout_phase == "LOAD_FEATURES"
    assert telemetry_worker.metrics.last_shadow_timeout_at_utc is not None
    assert telemetry_worker.metrics.last_timeout_phase == "SHADOW:LOAD_FEATURES"
    assert telemetry_worker.metrics.last_shadow_phase_duration_ms is not None
    slot = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
    due_at = slot + timedelta(
        seconds=formula_worker._DISCOVERY_SLOT_GRACE_SECONDS
    )
    store = formula_worker.research_formula_store
    original_lock = store.discovery_horizon_lock
    original_state = store.load_discovery_schedule_state
    original_recovered = store.load_scheduled_discovery_run
    original_record = store.record_discovery_schedule_state
    original_dataset = formula_worker.research_feature_matrix.load_formula_dataset
    original_discover = formula_worker.research_formula_engine.discover_formulas
    original_persist = store.persist_discovery_run

    @contextmanager
    def locked_elsewhere(_horizon):
        yield False

    store.discovery_horizon_lock = locked_elsewhere
    try:
        locked = worker.run_discovery_horizon_once(
            60, schedule_slot_utc=slot, due_at_utc=due_at
        )
        assert locked["status"] == "SKIPPED_LOCKED"
        assert worker.metrics.discovery_locked_skips == 1
    finally:
        store.discovery_horizon_lock = original_lock

    @contextmanager
    def acquired(_horizon):
        yield True

    records = []
    store.discovery_horizon_lock = acquired
    store.load_discovery_schedule_state = lambda _horizon: {
        "last_slot_utc": slot - timedelta(hours=1),
        "last_source_watermark_sha256": digest_a,
    }
    store.load_scheduled_discovery_run = lambda *_args, **_kwargs: None
    store.record_discovery_schedule_state = lambda **kwargs: records.append(kwargs)
    formula_worker.research_feature_matrix.load_formula_dataset = (
        lambda **_kwargs: watermark_dataset
    )
    store.persist_discovery_run = lambda **_kwargs: (_ for _ in ()).throw(
        AssertionError("unchanged watermark entered persistence")
    )
    try:
        unchanged = worker.run_discovery_horizon_once(
            60, schedule_slot_utc=slot, due_at_utc=due_at
        )
        assert unchanged["status"] == "SKIPPED_UNCHANGED"
        assert records[-1]["status"] == "SKIPPED_UNCHANGED"
        assert records[-1]["slot_utc"] == slot

        captured_persistence = []
        formula_worker.research_feature_matrix.load_formula_dataset = (
            lambda **_kwargs: changed
        )
        formula_worker.research_formula_engine.discover_formulas = (
            lambda *_args, **_kwargs: {
                "available": True,
                "sample_size": 3,
                "discovery_sample_size": 1,
                "selection_sample_size": 1,
                "holdout_sample_size": 1,
                "candidates_evaluated": 1,
            }
        )
        store.persist_discovery_run = lambda **kwargs: (
            captured_persistence.append(kwargs)
            or {"run_id": 77, "formulas_persisted": 0}
        )
        completed = worker.run_discovery_horizon_once(
            60,
            schedule_slot_utc=slot + timedelta(hours=1),
            due_at_utc=due_at + timedelta(hours=1),
        )
        assert completed["status"] == "COMPLETED"
        assert records[-1]["status"] == "COMPLETED"
        scheduler_metadata = captured_persistence[-1]["scheduler_metadata"]
        assert scheduler_metadata["scheduler_version"] == (
            store.DISCOVERY_SCHEDULER_VERSION
        )
        assert scheduler_metadata["walk_forward_policy_version"] == (
            formula_worker.research_formula_engine.WALK_FORWARD_POLICY_VERSION
        )
    finally:
        store.discovery_horizon_lock = original_lock
        store.load_discovery_schedule_state = original_state
        store.load_scheduled_discovery_run = original_recovered
        store.record_discovery_schedule_state = original_record
        formula_worker.research_feature_matrix.load_formula_dataset = original_dataset
        formula_worker.research_formula_engine.discover_formulas = original_discover
        store.persist_discovery_run = original_persist

    original_grace = main.MAX_PAIN_ARCHIVE_SYNC_GRACE_SECONDS
    main.MAX_PAIN_ARCHIVE_SYNC_GRACE_SECONDS = 180
    try:
        slot, run_at = main._next_max_pain_archive_schedule(
            datetime(2026, 8, 29, 12, 1, tzinfo=timezone.utc)
        )
        assert slot == datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
        assert run_at == datetime(2026, 8, 29, 12, 3, tzinfo=timezone.utc)
        slot, run_at = main._next_max_pain_archive_schedule(
            datetime(2026, 8, 29, 12, 3, 1, tzinfo=timezone.utc)
        )
        assert slot == datetime(2026, 8, 29, 12, 30, tzinfo=timezone.utc)
        assert run_at == datetime(2026, 8, 29, 12, 33, tzinfo=timezone.utc)
    finally:
        main.MAX_PAIN_ARCHIVE_SYNC_GRACE_SECONDS = original_grace

    previous_archive = os.environ.get("MAX_PAIN_ARCHIVE_ENABLED")
    previous_first_touch = os.environ.get("FIRST_TOUCH_BACKFILL_ENABLED")
    previous_replay = os.environ.get("HISTORICAL_REPLAY_BACKFILL")
    try:
        os.environ["MAX_PAIN_ARCHIVE_ENABLED"] = "1"
        assert main._start_max_pain_archive_task(schema_ready=False) is False
        assert main.MAX_PAIN_ARCHIVE_RUNTIME["status"] == "blocked_schema"
        asyncio.run(_archive_only_check())
        asyncio.run(_archive_enrichment_failure_capture_handoff_check())
        asyncio.run(_projection_capture_handoff_check())

        os.environ["FIRST_TOUCH_BACKFILL_ENABLED"] = "1"
        os.environ["HISTORICAL_REPLAY_BACKFILL"] = "0"
        assert main._start_first_touch_backfill_task(schema_ready=True) is False
        assert (
            main.FIRST_TOUCH_BACKFILL_RUNTIME["status"]
            == "blocked_replay_write_guard"
        )
    finally:
        for name, value in (
            ("MAX_PAIN_ARCHIVE_ENABLED", previous_archive),
            ("FIRST_TOUCH_BACKFILL_ENABLED", previous_first_touch),
            ("HISTORICAL_REPLAY_BACKFILL", previous_replay),
        ):
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    passive_source = inspect.getsource(main._max_pain_archive_loop)
    assert "send_message" not in passive_source
    assert '"source": "RESEARCH_PASSIVE"' in passive_source
    assert "archive_only=True" in passive_source
    assert "async with scrape_lock" in passive_source
    assert passive_source.count("submit_reconciliation") == 2
    assert passive_source.index("submit_reconciliation") < passive_source.index(
        "while research_max_pain_archive.archive_enabled()"
    )
    assert passive_source.rindex("submit_reconciliation") < passive_source.index(
        "collect_live_rows_for_watch("
    )

    discovery_source = inspect.getsource(formula_worker.FormulaResearchWorker)
    assert "discovery_horizon_lock" in discovery_source
    assert "load_discovery_schedule_state" in discovery_source
    assert "record_discovery_schedule_state" in discovery_source
    assert "scheduler_metadata" in discovery_source
    assert "send_message" not in inspect.getsource(
        formula_worker.FormulaResearchWorker._discovery_loop
    )
    assert "FORMULA_DISCOVERY_INTERVAL_SECONDS" not in inspect.getsource(
        formula_worker.FormulaResearchWorker._discovery_loop
    )

    migration = open(
        "migrations/017_formula_discovery_scheduler_v1.sql", encoding="utf-8"
    ).read()
    assert "research_formula_discovery_schedule_state" in migration
    assert "idx_formula_runs_scheduler_slot" in migration
    assert "walk_forward_policy_version" in migration
    assert "CREATE TABLE" not in inspect.getsource(
        formula_worker.FormulaResearchWorker._discovery_loop
    )

    startup_source = inspect.getsource(main.main)
    schema_index = startup_source.index("await _prepare_research_schema()")
    assert schema_index < startup_source.index("research_event_store.WRITER.start()")
    assert schema_index < startup_source.index("research_outcome_worker.WORKER.start()")
    assert schema_index < startup_source.index("research_formula_worker.WORKER.start()")
    assert schema_index < startup_source.index("_start_max_pain_archive_task")
    assert schema_index < startup_source.index("_start_first_touch_backfill_task")

    # Production health must identify the exact Render build before any
    # runtime/schema receipt is treated as evidence for the current code.
    assert 'os.getenv("RENDER_GIT_COMMIT")' in inspect.getsource(main.health)

    print("Research background scheduler self-test: PASS")


if __name__ == "__main__":
    run()
