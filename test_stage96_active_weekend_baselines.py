from datetime import datetime, timezone

import market_session_baseline as msb
import coinglass_history_backfill as hist


def dt(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def test_active_weekend_boundaries_new_york_summer():
    # Friday 19:30 ET is active; Friday 20:00 ET is weekend.
    assert msb.is_active_market(dt("2026-08-07T23:30:00+00:00")) is True
    assert msb.is_active_market(dt("2026-08-08T00:00:00+00:00")) is False
    # Sunday 17:30 ET is weekend; Sunday 18:00 ET is active.
    assert msb.is_active_market(dt("2026-08-09T21:30:00+00:00")) is False
    assert msb.is_active_market(dt("2026-08-09T22:00:00+00:00")) is True


def test_crossing_window_ratios_are_continuous_and_exact():
    # Friday 19:00-21:00 ET: one active hour and one weekend hour.
    active, weekend, segments = msb.session_ratios(
        dt("2026-08-07T23:00:00+00:00"),
        dt("2026-08-08T01:00:00+00:00"),
    )
    assert segments == 4
    assert round(active, 6) == 0.5
    assert round(weekend, 6) == 0.5


def test_long_window_ratios_sum_to_one():
    active, weekend, segments = msb.session_ratios(
        dt("2026-08-07T12:00:00+00:00"),
        dt("2026-08-10T12:00:00+00:00"),
    )
    assert segments == 144
    assert 0.0 < active < 1.0
    assert 0.0 < weekend < 1.0
    assert abs((active + weekend) - 1.0) < 1e-12


def test_distribution_blending_uses_window_composition():
    active = {"effective_samples": 100, "p25": 10, "median": 20, "p75": 30, "p90": 40, "p95": 50}
    weekend = {"effective_samples": 100, "p25": 2, "median": 4, "p75": 6, "p90": 8, "p95": 10}
    global_dist = {"count": 200, "p25": 6, "median": 12, "p75": 18, "p90": 24, "p95": 30}
    blended = hist._blend_distribution(active, weekend, global_dist, 0.75, 0.25)
    assert blended["p25"] == 8.0
    assert blended["p90"] == 32.0
    assert blended["baseline_mode"] == "ACTIVE_WEEKEND_CONTINUOUS"
