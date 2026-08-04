from datetime import datetime, timezone

import market_session_baseline as msb
import coinglass_history_backfill as hist


def dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def test_non_aligned_friday_boundary_is_split_exactly():
    # 2026-08-07 is EDT (UTC-4): 23:50 UTC = 19:50 ET, 00:20 UTC = 20:20 ET.
    active, weekend, _ = msb.session_ratios(
        dt("2026-08-07T23:50:00Z"), dt("2026-08-08T00:20:00Z")
    )
    assert abs(active - (10 / 30)) < 1e-12
    assert abs(weekend - (20 / 30)) < 1e-12


def test_non_aligned_sunday_boundary_is_split_exactly():
    # Sunday 17:50-18:20 ET = 10 weekend minutes + 20 active minutes.
    active, weekend, _ = msb.session_ratios(
        dt("2026-08-09T21:50:00Z"), dt("2026-08-09T22:20:00Z")
    )
    assert abs(active - (20 / 30)) < 1e-12
    assert abs(weekend - (10 / 30)) < 1e-12


def test_dst_is_resolved_by_new_york_timezone():
    # Before US DST starts, Friday 20:00 ET is Saturday 01:00 UTC.
    active, weekend, _ = msb.session_ratios(
        dt("2026-03-07T00:50:00Z"), dt("2026-03-07T01:20:00Z")
    )
    assert abs(active - (10 / 30)) < 1e-12
    assert abs(weekend - (20 / 30)) < 1e-12


def test_weighted_percentile_matches_ordinary_linear_for_equal_weights():
    values = [(1.0, 1.0), (2.0, 1.0), (3.0, 1.0), (4.0, 1.0)]
    assert msb.weighted_percentile(values, 0.25) == 1.75
    assert msb.weighted_percentile(values, 0.50) == 2.5
    assert msb.weighted_percentile(values, 0.75) == 3.25


def test_composition_matching_prefers_similar_windows():
    samples = [(10.0, 1.0), (12.0, 0.95), (2.0, 0.0), (3.0, 0.05)]
    active_weights = msb.composition_weighted_values(samples, 1.0, tolerance=0.25)
    weekend_weights = msb.composition_weighted_values(samples, 0.0, tolerance=0.25)
    assert {v for v, _ in active_weights} == {10.0, 12.0}
    assert {v for v, _ in weekend_weights} == {2.0, 3.0}


def test_composition_distribution_falls_back_when_matched_sample_is_sparse():
    global_dist = {"count": 100, "p25": 1.0, "median": 2.0, "p75": 3.0, "p90": 4.0, "p95": 5.0}
    result = hist._composition_distribution([(10.0, 1.0)], 1.0, global_dist, min_effective_samples=30)
    assert result["baseline_mode"] == "GLOBAL_FALLBACK"
    assert result["p75"] == 3.0


def test_open_candle_is_rejected_until_close_plus_grace():
    candle = dt("2026-08-04T12:00:00Z")
    assert not msb.is_closed_candle(candle, dt("2026-08-04T12:31:59Z"), 30, 2)
    assert msb.is_closed_candle(candle, dt("2026-08-04T12:32:00Z"), 30, 2)


def test_price_oi_reference_skips_windows_without_near_time_match(monkeypatch):
    rows = [
        {"candle_time": "2026-08-03T00:00:00+00:00", "price_close": 100.0, "oi_close_usd": 1000.0},
        {"candle_time": "2026-08-03T00:30:00+00:00", "price_close": 101.0, "oi_close_usd": 1010.0},
        # Large gap: index distance must not masquerade as a 30m/1h window.
        {"candle_time": "2026-08-03T04:00:00+00:00", "price_close": 120.0, "oi_close_usd": 1200.0},
    ]
    monkeypatch.setattr(hist, "_history_rows", lambda symbol: rows)
    result = hist.calculate_reference_ranges("BTC")
    assert result["windows"]["30m"]["samples"] == 1
    assert result["windows"]["1h"]["samples"] == 0
