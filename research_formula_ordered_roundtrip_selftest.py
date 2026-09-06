"""Real v7 calculator -> storage JSON -> evidence audit boundary checks."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace

import research_formula_ordered_v7 as evaluator
import research_ordered_first_touch as calculator
import research_outcome_worker as worker


class _StorageCapture:
    def __init__(self):
        self.calls = []

    def execute(self, query, params):
        self.calls.append((query, params))
        return self

    def fetchone(self):
        return {"event_id": 7}


def run() -> None:
    base = datetime(2026, 9, 6, 10, 0, tzinfo=timezone.utc)
    checked = 0
    for offset in (0, 1, 500_000, 59_999_999):
        start = base + timedelta(microseconds=offset)
        first_open = base if offset == 0 else base + timedelta(minutes=1)
        candle_close = first_open + timedelta(seconds=59, milliseconds=999)
        for direction in ("LONG", "SHORT"):
            for expected_status in ("SUCCESS", "FAILURE"):
                rises = (direction == "LONG") == (expected_status == "SUCCESS")
                candle = SimpleNamespace(
                    open_time_utc=first_open, close_time_utc=candle_close,
                    open=100.0, high=103.0 if rises else 100.01,
                    low=99.99 if rises else 97.0,
                    close=103.0 if rises else 97.0, volume=1.0,
                )
                event = {
                    "event_id": 7, "event_fingerprint": "roundtrip-7",
                    "alert_time_utc": start, "symbol": "BTC",
                    "direction": direction, "event_type": "SELFTEST",
                    "setup_key": "SELFTEST", "event_kind": "ALERT",
                    "delivery_status": "DELIVERED", "current_price": 100.0,
                    "target_price": None,
                    "engine_snapshot": {"price_source": "binance_spot"},
                }
                path = {
                    "symbol": "BTC", "pair": "BTCUSDT",
                    "exchange": "binance", "market": "spot",
                    "interval": "1m", "interval_seconds": 60,
                    "complete": True, "expected_candles": 1,
                    "provenance": "SELFTEST", "candles": [candle],
                }
                for threshold in calculator.SUPPORTED_THRESHOLDS_PCT:
                    outcome = calculator.calculate_ordered_first_touch_outcome(
                        reference_price=100.0, direction=direction,
                        event_time=start, candles=[candle],
                        threshold_pct=threshold, observation_closed=False,
                    )
                    assert outcome["status"] == expected_status
                    capture = _StorageCapture()
                    assert worker.ResearchOutcomeWorker._write_ordered_first_touch_outcome(
                        capture, event=event, window_minutes=60,
                        reference_source="binance_spot", path_result=path,
                        outcome=outcome, expected_candles=1,
                    )
                    # Use the exact production jsonb_to_record input, with its
                    # serialized UTC timestamps and persisted field types.
                    stored = json.loads(capture.calls[0][1][0])
                    row = {"event": event, "ordered_outcome": stored}
                    result = evaluator.ordered_outcome_evidence(
                        row, analysis_as_of_utc=base + timedelta(hours=1)
                    )
                    assert result["eligible"], (offset, direction, stored, result)
                    assert result["success"] == (expected_status == "SUCCESS")
                    assert stored["time_to_decision_seconds"] == int(
                        (candle_close - start).total_seconds()
                    )
                    assert stored["initial_gap_unobserved"] == (offset != 0)
                    if offset == 59_999_999:
                        # A real positive microsecond gap remains disclosed;
                        # storage intentionally reserves zero for aligned starts.
                        assert stored["initial_gap_seconds"] == 1
                    changed = {**stored, "time_to_decision_seconds": (
                        stored["time_to_decision_seconds"] + 1
                    )}
                    rejected = evaluator.ordered_outcome_evidence(
                        {"event": event, "ordered_outcome": changed},
                        analysis_as_of_utc=base + timedelta(hours=1),
                    )
                    assert not rejected["eligible"]
                    assert "DECISION_DURATION_MISMATCH" in rejected["exclusion_reasons"]
                    checked += 1
    assert checked == 128
    print(f"ordered formula calculator/storage roundtrip: {checked} cases PASS")


if __name__ == "__main__":
    run()
