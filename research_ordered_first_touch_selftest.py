"""Network-free checks for the ordered two-barrier First Touch v7 contract."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import research_ordered_first_touch as outcome


START = datetime(2026, 9, 6, 12, 0, tzinfo=timezone.utc)


def _candle(
    minute: int,
    *,
    open_: float = 100.0,
    high: float = 100.0,
    low: float = 100.0,
    close: float = 100.0,
    start: datetime = START,
) -> SimpleNamespace:
    opened = start + timedelta(minutes=minute)
    return SimpleNamespace(
        open_time_utc=opened,
        close_time_utc=opened + timedelta(seconds=59, milliseconds=999),
        open=open_,
        high=high,
        low=low,
        close=close,
    )


def _one(
    direction: str,
    candles,
    *,
    threshold: float = 0.50,
    closed: bool = True,
    complete: bool = True,
    event_time: datetime = START,
):
    return outcome.calculate_ordered_first_touch_outcome(
        reference_price=100.0,
        direction=direction,
        event_time=event_time,
        candles=candles,
        threshold_pct=threshold,
        observation_closed=closed,
        path_complete=complete,
    )


def run() -> None:
    assert outcome.METHOD_VERSION == "ordered-first-touch-v7"
    assert outcome.SUPPORTED_THRESHOLDS_PCT == (
        0.25,
        0.50,
        0.75,
        1.00,
        1.25,
        1.50,
        1.75,
        2.00,
    )

    # Bullish boundary first: LONG succeeds exactly when SHORT fails.
    bullish_first_path = [
        _candle(0, high=100.6, low=99.9, close=100.5),
        _candle(1, high=100.5, low=99.4, close=99.5),
    ]
    long_bullish = _one("LONG", bullish_first_path)
    short_bullish = _one("SHORT", bullish_first_path)
    assert long_bullish["status"] == "SUCCESS"
    assert long_bullish["reference_price"] == 100.0
    assert long_bullish["first_touch_side"] == "FAVORABLE"
    assert round(long_bullish["favorable_touch_price"], 8) == 100.5
    assert long_bullish["adverse_touch_price"] is None
    assert long_bullish["decision_time_utc"] == bullish_first_path[0].close_time_utc
    assert long_bullish["time_to_decision_seconds"] == 59
    assert long_bullish["path_samples"] == 1
    assert round(long_bullish["mfe_pct"], 8) == 0.6
    assert round(long_bullish["mae_pct"], 8) == 0.1
    assert short_bullish["status"] == "FAILURE"
    assert short_bullish["first_touch_side"] == "ADVERSE"
    assert short_bullish["favorable_touch_price"] is None
    assert round(short_bullish["adverse_touch_price"], 8) == 100.5
    assert short_bullish["decision_time_utc"] == bullish_first_path[0].close_time_utc
    assert short_bullish["time_to_decision_seconds"] == 59

    # Bearish boundary first is the exact mirror.
    bearish_first_path = [
        _candle(0, high=100.1, low=99.4, close=99.5),
        _candle(1, high=100.6, low=99.5, close=100.5),
    ]
    long_bearish = _one("LONG", bearish_first_path)
    short_bearish = _one("SHORT", bearish_first_path)
    assert long_bearish["status"] == "FAILURE"
    assert long_bearish["first_touch_side"] == "ADVERSE"
    assert long_bearish["adverse_touch_price"] == 99.5
    assert short_bearish["status"] == "SUCCESS"
    assert short_bearish["first_touch_side"] == "FAVORABLE"
    assert short_bearish["favorable_touch_price"] == 99.5

    # Both barriers inside one 1m candle have unknowable intrabar order.
    both_same_candle = [_candle(0, high=100.6, low=99.4, close=100.0)]
    ambiguous_long = _one("LONG", both_same_candle)
    ambiguous_short = _one("SHORT", both_same_candle)
    for label in (ambiguous_long, ambiguous_short):
        assert label["status"] == "UNRESOLVED"
        assert label["first_touch_side"] == "AMBIGUOUS"
        assert label["terminal_reason"] == "SAME_CANDLE_BOTH"
        assert label["success"] is None
        assert label["favorable_touch_price"] is not None
        assert label["adverse_touch_price"] is not None

    # A small opposite wick is not ambiguity and does not cancel a clean touch.
    subthreshold_wick = [_candle(0, high=100.6, low=99.8, close=100.4)]
    clean = _one("LONG", subthreshold_wick)
    assert clean["status"] == "SUCCESS"
    assert clean["first_touch_side"] == "FAVORABLE"
    assert clean["terminal_reason"] == "FAVORABLE_FIRST"

    # No touch stays OPEN while observed and becomes unresolved, not a fake
    # adverse failure, when its observation window closes.
    no_touch_path = [_candle(0, high=100.2, low=99.8, close=100.0)]
    still_open = _one("LONG", no_touch_path, closed=False)
    closed_no_touch = _one("LONG", no_touch_path, closed=True)
    assert still_open["status"] == "OPEN"
    assert still_open["first_touch_side"] == "NONE"
    assert still_open["decision_time_utc"] is None
    assert closed_no_touch["status"] == "UNRESOLVED"
    assert closed_no_touch["first_touch_side"] == "NONE"
    assert closed_no_touch["terminal_reason"] == "OBSERVATION_WINDOW_CLOSED_NO_TOUCH"

    # Explicit or inferred incompleteness fails closed, even when a later
    # candle appears to cross a barrier.
    explicit_missing = _one("LONG", bullish_first_path, complete=False)
    assert explicit_missing["status"] == "DATA_MISSING"
    assert explicit_missing["first_touch_side"] == "NONE"
    assert explicit_missing["decision_time_utc"] is None
    assert explicit_missing["path_samples"] == 2
    assert round(explicit_missing["mae_pct"], 8) == 0.6
    gapped_path = [
        _candle(0, high=100.2, low=99.9),
        _candle(2, high=100.6, low=99.9),
    ]
    inferred_missing = _one("LONG", gapped_path)
    assert inferred_missing["status"] == "DATA_MISSING"
    assert inferred_missing["path_complete"] is False

    # The discarded partial decision minute is explicitly disclosed, while the
    # label applies to the fully observed path beginning at the next minute.
    mid_minute = START + timedelta(seconds=30)
    overlapping = _candle(0, high=110.0, low=90.0)
    first_full = _candle(1, high=100.6, low=99.9)
    initial_gap = _one(
        "LONG",
        [overlapping, first_full],
        event_time=mid_minute,
    )
    assert initial_gap["status"] == "SUCCESS"
    assert initial_gap["initial_gap_seconds"] == 30
    assert initial_gap["initial_gap_unobserved"] is True
    assert initial_gap["first_observed_open_utc"] == first_full.open_time_utc
    assert initial_gap["observed_from_utc"] == first_full.open_time_utc
    assert initial_gap["data_quality_note"] == "COMPLETE_POST_GAP_CLOSED_1M_PATH"
    subsecond_gap = _one(
        "LONG",
        [first_full],
        event_time=START + timedelta(seconds=59, milliseconds=500),
    )
    assert subsecond_gap["initial_gap_seconds"] == 1
    assert subsecond_gap["initial_gap_unobserved"] is True

    # All-threshold mode is stable, ordered, and uses integer basis points for
    # storage keys instead of floating-point identity.
    all_thresholds = outcome.calculate_all_ordered_first_touch_outcomes(
        reference_price=100.0,
        direction="LONG",
        event_time=START,
        candles=[_candle(0, high=102.1, low=100.0, close=102.0)],
        observation_closed=False,
    )
    assert [item["threshold_pct"] for item in all_thresholds] == list(
        outcome.SUPPORTED_THRESHOLDS_PCT
    )
    assert [item["threshold_bps"] for item in all_thresholds] == [
        25,
        50,
        75,
        100,
        125,
        150,
        175,
        200,
    ]
    assert all(item["status"] == "SUCCESS" for item in all_thresholds)

    try:
        _one("LONG", no_touch_path, threshold=0.30)
    except ValueError as exc:
        assert "threshold_pct must be one of" in str(exc)
    else:
        raise AssertionError("unsupported threshold was accepted")

    print("ordered First Touch v7 self-test: PASS")


if __name__ == "__main__":
    run()
