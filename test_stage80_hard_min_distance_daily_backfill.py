from pathlib import Path

import alert_engine


def test_distance_below_08_gets_zero_points():
    assert alert_engine._target_proximity_points(0.79, 2.5) == 0.0
    assert alert_engine._target_proximity_points(0.50, 4.0) == 0.0


def test_distance_at_08_keeps_full_preferred_score():
    assert alert_engine._target_proximity_points(0.80, 2.5) == 25.0


def test_dynamic_upper_thresholds_unchanged():
    assert alert_engine._allowed_distance_pct("BTC", 1) == 2.5
    assert alert_engine._allowed_distance_pct("ETH", 2) == 2.7
    assert alert_engine._allowed_distance_pct("SOL", 5) == 3.0
    assert alert_engine._allowed_distance_pct("DOGE", 15) == 3.5
    assert alert_engine._allowed_distance_pct("OTHER", 99) == 4.0


def test_freshness_checked_backfill_loop_is_wired_into_startup():
    source = Path("main.py").read_text()
    assert "HISTORY_BACKFILL_INTERVAL_HOURS" in source
    assert "async def _history_backfill_loop" in source
    assert "HISTORY_BACKFILL_TASK = asyncio.create_task(_history_backfill_loop())" in source
    assert "last_backfill_run" in source
    assert "_run_history_backfill_once(\"automatic_due\")" in source
    assert "HISTORY_BACKFILL_CHECK_INTERVAL_MINUTES" in source
