"""Network-free checks for the no-dwell first-touch v6 outcome contract."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

from binance_spot_price_path import SpotCandle, calculate_path_metrics
import research_no_dwell_outcome as outcome


START = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)


def _candle(
    minute: int,
    *,
    open_: float = 100.0,
    high: float = 100.0,
    low: float = 100.0,
    close: float = 100.0,
) -> SpotCandle:
    opened = START + timedelta(minutes=minute)
    return SpotCandle(
        open_time_utc=opened,
        close_time_utc=opened + timedelta(seconds=59, milliseconds=999),
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=1.0,
    )


def run() -> None:
    assert dict(outcome.BASE_FAVORABLE_WIDTH_PCT_BY_HORIZON) == {
        60: 0.50,
        240: 1.00,
        720: 1.50,
        1440: 2.00,
    }

    # A favorable wick qualifies with zero dwell even though the candle closes
    # below the reference.  The later -10% reversal cannot cancel that HIT.
    long_path = [
        _candle(0, high=100.4, low=99.8, close=100.1),
        _candle(1, high=100.5, low=99.0, close=99.5),
        _candle(2, high=100.0, low=90.0, close=91.0),
    ]
    long_hit = outcome.calculate_first_touch_outcome(
        reference_price=100.0,
        direction="LONG",
        event_time=START,
        candles=long_path,
        horizon_minutes=60,
        horizon_closed=False,
    )
    assert long_hit["status"] == "HIT" and long_hit["success"] is True
    assert long_hit["dwell_required_seconds"] == 0
    assert long_hit["first_qualifying_move_time_utc"] == long_path[1].close_time_utc
    assert long_hit["time_to_first_qualifying_move_seconds"] == 119
    assert round(long_hit["pre_qualifying_mae_pct"], 8) == 1.0
    assert long_hit["qualifying_candle_order_ambiguous"] is True
    assert round(long_hit["qualifying_candle_adverse_excursion_pct"], 8) == 1.0
    legacy = calculate_path_metrics(
        reference_price=100.0,
        direction="LONG",
        event_time=START,
        candles=long_path,
    )
    assert round(legacy["mae_pct"], 8) == 10.0
    assert legacy["directional_return_pct"] < 0.0
    assert long_hit["status"] == "HIT", "diagnostic reversal must not cancel touch"

    # A decision in the middle of a minute must not inherit either side of the
    # already-open candle.  Even if that overlapping candle crossed the target,
    # only the first full post-decision minute is eligible.
    mid_minute_decision = START + timedelta(seconds=30)
    overlapping = SpotCandle(
        open_time_utc=START,
        close_time_utc=START + timedelta(seconds=59, milliseconds=999),
        open=100.0,
        high=110.0,
        low=90.0,
        close=100.0,
        volume=1.0,
    )
    first_full_post_decision = _candle(
        1, high=100.2, low=99.9, close=100.1
    )
    overlapping_ignored = outcome.calculate_first_touch_outcome(
        reference_price=100.0,
        direction="LONG",
        event_time=mid_minute_decision,
        candles=[overlapping, first_full_post_decision],
        horizon_minutes=60,
        horizon_closed=False,
    )
    assert overlapping_ignored["status"] == "PENDING"
    assert overlapping_ignored["first_qualifying_move_time_utc"] is None
    assert round(overlapping_ignored["pre_qualifying_mae_pct"], 8) == 0.1

    # Conservative ordering: the qualifying candle's opposite-side high is
    # included for SHORT because 1m OHLC cannot reveal whether it came first.
    short_path = [
        _candle(0, high=100.2, low=99.5, close=99.7),
        _candle(1, high=101.5, low=99.0, close=100.5),
    ]
    short_hit = outcome.calculate_first_touch_outcome(
        reference_price=100.0,
        direction="SHORT",
        event_time=START,
        candles=short_path,
        horizon_minutes=240,
        horizon_closed=False,
    )
    assert short_hit["status"] == "HIT"
    assert round(short_hit["pre_qualifying_mae_pct"], 8) == 1.5
    assert short_hit["qualifying_candle_order_ambiguous"] is True

    no_hit_path = [_candle(0, high=100.2, low=99.9, close=100.1)]
    pending = outcome.calculate_first_touch_outcome(
        reference_price=100.0,
        direction="LONG",
        event_time=START,
        candles=no_hit_path,
        horizon_minutes=60,
        horizon_closed=False,
    )
    assert pending["status"] == "PENDING"
    assert pending["success"] is None and pending["failure_final"] is False
    missed = outcome.calculate_first_touch_outcome(
        reference_price=100.0,
        direction="LONG",
        event_time=START,
        candles=no_hit_path,
        horizon_minutes=60,
        horizon_closed=True,
    )
    assert missed["status"] == "MISS"
    assert missed["success"] is False and missed["failure_final"] is True

    # A closed candle that began before the decision cannot qualify the label,
    # even if its OHLC range crossed the favorable threshold.
    pre_event = SpotCandle(
        open_time_utc=START - timedelta(minutes=1),
        close_time_utc=START - timedelta(milliseconds=1),
        open=100.0,
        high=110.0,
        low=90.0,
        close=100.0,
        volume=1.0,
    )
    post_event_pending = outcome.calculate_first_touch_outcome(
        reference_price=100.0,
        direction="LONG",
        event_time=START,
        candles=[pre_event, *no_hit_path],
        horizon_minutes=60,
        horizon_closed=False,
    )
    assert post_event_pending["status"] == "PENDING"
    assert round(post_event_pending["pre_qualifying_mae_pct"], 8) == 0.1

    prior_policy = outcome.freeze_threshold_policy(
        horizon_minutes=240,
        decision_time=START,
        prior_only_reference={
            "source_kind": "PRIOR_ONLY_SESSION_CALIBRATION",
            "as_of_utc": START - timedelta(minutes=1),
            "threshold_scale_factor": 0.60,
            "session_weekend_ratio": 1.0,
            "source": "prior raw-price weekend calibration",
        },
    )
    assert prior_policy["qualifying_move_threshold_pct"] == 0.60
    assert prior_policy["threshold_scale_factor"] == 0.60

    weekend_scaled_touch = [_candle(0, high=100.7, low=100.0, close=100.1)]
    base_width_pending = outcome.calculate_first_touch_outcome(
        reference_price=100.0,
        direction="LONG",
        event_time=START,
        candles=weekend_scaled_touch,
        horizon_minutes=240,
        horizon_closed=False,
    )
    scaled_width_hit = outcome.calculate_first_touch_outcome(
        reference_price=100.0,
        direction="LONG",
        event_time=START,
        candles=weekend_scaled_touch,
        horizon_minutes=240,
        horizon_closed=False,
        threshold_policy=prior_policy,
    )
    assert base_width_pending["status"] == "PENDING"
    assert scaled_width_hit["status"] == "HIT"
    assert scaled_width_hit["qualifying_candle_order_ambiguous"] is False
    assert scaled_width_hit["pre_qualifying_mae_pct"] == 0.0

    try:
        outcome.freeze_threshold_policy(
            horizon_minutes=240,
            decision_time=START,
            prior_only_reference={
                "source_kind": "PRIOR_ONLY_SESSION_CALIBRATION",
                "as_of_utc": START + timedelta(seconds=1),
                "threshold_scale_factor": 0.60,
            },
        )
    except ValueError as exc:
        assert "newer than decision time" in str(exc)
    else:
        raise AssertionError("future-derived threshold calibration was accepted")

    try:
        outcome.freeze_threshold_policy(
            horizon_minutes=240,
            decision_time=START,
            prior_only_reference={
                "source_kind": "PRIOR_ONLY_SESSION_CALIBRATION",
                "as_of_utc": START - timedelta(minutes=1),
                "threshold_scale_factor": 0.60,
                "session_weekend_ratio": 0.0,
            },
        )
    except ValueError as exc:
        assert "weekend/mixed horizon" in str(exc)
    else:
        raise AssertionError("weekday threshold relaxation was accepted")

    tampered_static = outcome.freeze_threshold_policy(
        horizon_minutes=240,
        decision_time=START,
    )
    tampered_static["threshold_scale_factor"] = 0.60
    tampered_static["qualifying_move_threshold_pct"] = 0.60
    try:
        outcome.calculate_first_touch_outcome(
            reference_price=100.0,
            direction="LONG",
            event_time=START,
            candles=no_hit_path,
            horizon_minutes=240,
            horizon_closed=False,
            threshold_policy=tampered_static,
        )
    except ValueError as exc:
        assert "static first-touch threshold" in str(exc)
    else:
        raise AssertionError("unproven static threshold scaling was accepted")

    # A missing immutable decision price must never be replaced by the first
    # post-decision candle open; that would move the reference into the future.
    worker_source = Path("research_outcome_worker.py").read_text(encoding="utf-8")
    assert "reference_price = float(full_path[0].open)" not in worker_source

    print("no-dwell first-touch outcome self-test: PASS")


if __name__ == "__main__":
    run()
