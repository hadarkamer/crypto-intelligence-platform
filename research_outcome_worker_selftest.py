"""Network-free checks for prospective open-horizon first-touch polling."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import canonical_price_path
import research_no_dwell_outcome
import research_outcome_worker as worker


START = datetime(2026, 8, 29, 18, 41, tzinfo=timezone.utc)


class _CaptureResult:
    def __init__(self) -> None:
        self.query = ""
        self.params = []

    def execute(self, query, params):
        self.query = str(query)
        self.params = list(params)
        return self

    def fetchall(self):
        return []

    def fetchone(self):
        return {"event_id": 1}


class _ConnectionContext:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False


class _PsycopgStub:
    @staticmethod
    def connect(*args, **kwargs):
        return _ConnectionContext()


def _candle(open_time, *, high=100.0, low=100.0, close=100.0):
    return SimpleNamespace(
        open_time_utc=open_time,
        close_time_utc=open_time + timedelta(seconds=59, milliseconds=999),
        open=100.0,
        high=high,
        low=low,
        close=close,
        volume=1.0,
    )


def _run_once_with_path(candles):
    now = datetime.now(timezone.utc)
    event_time = now.replace(second=0, microsecond=0) - timedelta(minutes=3)
    event = {
        "event_id": 9001,
        "alert_time_utc": event_time,
        "symbol": "BTC",
        "direction": "LONG",
        "current_price": 100.0,
        "target_price": None,
        "engine_snapshot": {},
        "outcome_versions": {},
        "first_touch_versions": {},
        "open_first_touch_horizons": [60],
    }
    captured_writes = []
    service = worker.ResearchOutcomeWorker()
    service._load_open_first_touch_events = lambda conn, limit: [event]
    service._load_due_events = lambda conn, limit: []
    service._write_first_touch_outcome = (
        lambda conn, **kwargs: captured_writes.append(kwargs) or True
    )
    original_psycopg = worker.psycopg
    original_enabled = worker._ENABLED
    original_database_url = worker._database_url
    original_fetch = worker.canonical_price_path.fetch_closed_candles
    worker.psycopg = _PsycopgStub()
    worker._ENABLED = True
    worker._database_url = lambda: "postgresql://selftest"
    worker.canonical_price_path.fetch_closed_candles = lambda *args: {
        "symbol": "BTC",
        "pair": "BTCUSDT",
        "exchange": "binance",
        "market": "spot",
        "interval": "1m",
        "provenance": "SELFTEST",
        "candles": list(candles(event_time)),
    }
    try:
        result = service.run_once(limit_per_horizon=10)
    finally:
        worker.psycopg = original_psycopg
        worker._ENABLED = original_enabled
        worker._database_url = original_database_url
        worker.canonical_price_path.fetch_closed_candles = original_fetch
    return result, captured_writes


def run() -> None:
    # An open horizon is never polled merely because an event exists.  It must
    # be explicitly supplied by the read-side authorization query, which only
    # yields horizons of active Shadow formulas matched by an authorized
    # prospective event.
    ten_minutes_later = START + timedelta(minutes=10)
    assert worker._due_horizons(START, {}, {}, now=ten_minutes_later) == []
    assert worker._due_horizons(
        START,
        {},
        {},
        now=ten_minutes_later,
        open_first_touch_horizons=[240],
    ) == [240]
    assert worker._due_horizons(
        START,
        {},
        {240: research_no_dwell_outcome.METHOD_VERSION},
        now=ten_minutes_later,
        open_first_touch_horizons=[240],
    ) == []
    assert worker._due_horizons(
        START,
        {},
        {240: f"{research_no_dwell_outcome.METHOD_VERSION}:PENDING"},
        now=ten_minutes_later,
        open_first_touch_horizons=[240],
    ) == [240]

    # Closed-horizon enrichment remains unchanged and does not require open
    # authorization.  A complete first-touch label suppresses only its own
    # rewrite; a missing legacy endpoint row is still due independently.
    after_one_hour = START + timedelta(minutes=61)
    assert worker._due_horizons(
        START,
        {},
        {60: research_no_dwell_outcome.METHOD_VERSION},
        now=after_one_hour,
    ) == [60]
    assert worker._due_horizons(
        START,
        {60: canonical_price_path.METHOD_VERSION},
        {60: research_no_dwell_outcome.METHOD_VERSION},
        now=after_one_hour,
    ) == []

    assert worker._latest_closed_candle_cutoff(
        datetime(2026, 8, 29, 18, 51, 0, tzinfo=timezone.utc)
    ) == datetime(2026, 8, 29, 18, 50, 59, 999000, tzinfo=timezone.utc)
    assert worker._first_touch_write_is_safe(
        {"status": "PENDING"}, observed_prefix_complete=False
    )
    assert not worker._first_touch_write_is_safe(
        {"status": "HIT"}, observed_prefix_complete=False
    )
    assert worker._first_touch_write_is_safe(
        {"status": "HIT"}, observed_prefix_complete=True
    )

    closed_capture = _CaptureResult()
    assert worker.ResearchOutcomeWorker._load_due_events(
        closed_capture, 200
    ) == []
    assert closed_capture.query.count("%s") == len(closed_capture.params)
    assert "ARRAY[]::integer[] AS open_first_touch_horizons" in closed_capture.query
    assert "research_formula_shadow_checks open_check" not in closed_capture.query

    captured = _CaptureResult()
    assert worker.ResearchOutcomeWorker._load_open_first_touch_events(
        captured, 200
    ) == []
    assert captured.query.count("%s") == len(captured.params)
    for required in (
        "research_prospective_shadow_events authorized",
        "research_formula_shadow_checks open_check",
        "open_check.evaluation_status='MATCHED'",
        "open_formula.current_stage='SHADOW'",
        "open_ft.status='HIT'",
        "open_first_touch_horizons",
        "DISTINCT open_formula.horizon_minutes",
        "date_trunc('minute', NOW())",
        "INTERVAL '1 millisecond'",
    ):
        assert required in captured.query
    assert "research_formula_live_deliveries" not in captured.query

    # A favorable wick on a gapped prefix is not allowed to freeze a terminal
    # HIT.  Once the complete prefix is available, the same wick qualifies
    # immediately even though a later candle reverses deeply and closes down.
    incomplete, incomplete_writes = _run_once_with_path(
        lambda event_time: [
            _candle(event_time, high=100.6, low=99.8, close=99.9),
        ]
    )
    assert incomplete_writes == []
    assert (
        incomplete["first_touch_terminal_rows_deferred_for_incomplete_prefix"]
        == 1
    )
    complete, complete_writes = _run_once_with_path(
        lambda event_time: [
            _candle(event_time, high=100.6, low=99.8, close=99.9),
            _candle(
                event_time + timedelta(minutes=1),
                high=100.0,
                low=90.0,
                close=91.0,
            ),
            _candle(
                event_time + timedelta(minutes=2),
                high=92.0,
                low=89.0,
                close=90.0,
            ),
        ]
    )
    assert complete["first_touch_hits"] == 1
    assert len(complete_writes) == 1
    frozen = complete_writes[0]["first_touch"]
    assert frozen["status"] == "HIT"
    assert frozen["dwell_required_seconds"] == 0
    assert frozen["first_qualifying_move_time_utc"] == (
        complete_writes[0]["event"]["alert_time_utc"]
        + timedelta(seconds=59, milliseconds=999)
    )

    # A verified terminal row is absorbing at the application upsert.  A
    # legacy partial terminal row, if one ever existed, is deliberately not
    # protected and can be replaced by a later complete recalculation instead
    # of having its old semantic fields quality-laundered.
    conflict_capture = _CaptureResult()
    write = complete_writes[0]
    assert worker.ResearchOutcomeWorker._write_first_touch_outcome(
        conflict_capture,
        event=write["event"],
        horizon=write["horizon"],
        reference_price=write["reference_price"],
        reference_source=write["reference_source"],
        path_result=write["path_result"],
        first_touch=write["first_touch"],
        complete=True,
    )
    assert conflict_capture.query.count("%s") == len(conflict_capture.params)
    assert "status IN ('HIT', 'MISS')" in conflict_capture.query
    assert "data_quality_status=ANY(%s)" in conflict_capture.query
    assert conflict_capture.params[-1] == list(
        canonical_price_path.COMPLETE_QUALITIES
    )
    assert "direction=EXCLUDED.direction" in conflict_capture.query
    assert "WHEN research_first_touch_outcomes.status='HIT'" not in (
        conflict_capture.query
    )

    status = worker.ResearchOutcomeWorker().status()
    policy = status["first_touch_policy"]
    assert policy["success"] == "first favorable width touch; zero dwell"
    assert policy["post_hit_reversal"] == "does not cancel success"
    assert "every minute" in policy["worker_evaluation"]
    assert worker._POLL_SECONDS >= 60

    print("prospective first-touch outcome worker self-test: PASS")


if __name__ == "__main__":
    run()
