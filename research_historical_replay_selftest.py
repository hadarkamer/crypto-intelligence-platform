"""Pure checks for historical replay time and path safety."""

from datetime import datetime, timedelta, timezone

from binance_spot_price_path import SpotCandle
import research_historical_replay as replay
import research_no_dwell_outcome as no_dwell


def _candle(open_time: datetime, close: float = 100.0) -> SpotCandle:
    return SpotCandle(
        open_time_utc=open_time,
        close_time_utc=open_time + timedelta(seconds=59, milliseconds=999),
        open=close,
        high=close + 1.0,
        low=close - 1.0,
        close=close,
        volume=1.0,
    )


def run() -> None:
    observation = datetime(2026, 8, 29, 12, 32, tzinfo=timezone.utc)
    assert replay._hype_one_minute_observation_floor(
        datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
    ) == datetime(2026, 8, 26, 0, 42, tzinfo=timezone.utc)
    candles = [
        _candle(observation + timedelta(minutes=index), 100.0 + index / 10)
        for index in range(-2, 61)
    ]
    reference = replay._reference_candle(candles, observation)
    assert reference is not None
    assert reference.open_time_utc == observation - timedelta(minutes=1)
    future = replay._outcome_candles(candles, observation, 60)
    assert len(future) == replay._expected_outcome_candles(observation, 60) == 60
    assert future[0].open_time_utc == observation
    assert future[-1].open_time_utc == observation + timedelta(minutes=59)

    partial = observation + timedelta(seconds=10)
    partial_future = replay._outcome_candles(candles, partial, 60)
    assert len(partial_future) == replay._expected_outcome_candles(partial, 60) == 59
    assert partial_future[0].open_time_utc == observation + timedelta(minutes=1)

    long_metrics = replay.binance_spot_price_path.calculate_path_metrics(
        reference_price=float(reference.close),
        direction="LONG",
        event_time=observation,
        candles=future,
    )
    short_metrics = replay.binance_spot_price_path.calculate_path_metrics(
        reference_price=float(reference.close),
        direction="SHORT",
        event_time=observation,
        candles=future,
    )
    assert long_metrics["mfe_pct"] > short_metrics["mfe_pct"]
    assert long_metrics["mae_pct"] < short_metrics["mae_pct"]
    assert long_metrics["raw_return_pct"] == short_metrics["raw_return_pct"]
    policy = no_dwell.freeze_threshold_policy(
        horizon_minutes=60,
        decision_time=observation,
    )
    first_touch = no_dwell.calculate_first_touch_outcome(
        reference_price=float(reference.close),
        direction="LONG",
        event_time=observation,
        candles=future,
        horizon_minutes=60,
        horizon_closed=True,
        threshold_policy=policy,
    )
    assert first_touch["status"] == "HIT"
    assert first_touch["method_version"] == "no-dwell-first-touch-v6"
    assert first_touch["threshold_scale_factor"] == 1.0
    print("historical replay self-test: OK")


if __name__ == "__main__":
    run()
