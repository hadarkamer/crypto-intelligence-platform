"""Network-free checks for guarded Research background task wiring."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
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


def run() -> None:
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

    startup_source = inspect.getsource(main.main)
    schema_index = startup_source.index("await _prepare_research_schema()")
    assert schema_index < startup_source.index("research_event_store.WRITER.start()")
    assert schema_index < startup_source.index("research_outcome_worker.WORKER.start()")
    assert schema_index < startup_source.index("research_formula_worker.WORKER.start()")
    assert schema_index < startup_source.index("_start_max_pain_archive_task")
    assert schema_index < startup_source.index("_start_first_touch_backfill_task")

    print("Research background scheduler self-test: PASS")


if __name__ == "__main__":
    run()
