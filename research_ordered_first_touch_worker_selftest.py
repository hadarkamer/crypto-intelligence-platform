"""Network-free integration checks for the ordered First Touch v7 worker."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import inspect
import json
import re
import sqlite3
from types import SimpleNamespace

import research_ordered_first_touch as ordered
import research_outcome_worker as worker


class _Result:
    def __init__(self, *, one=None, many=None) -> None:
        self._one = one
        self._many = list(many or [])

    def fetchone(self):
        return self._one

    def fetchall(self):
        return list(self._many)


class _CaptureConnection:
    def __init__(self, *, first_write=True, rows=None) -> None:
        self.calls = []
        self.first_write = first_write
        self.rows = list(rows or [])

    def execute(self, query, params=()):
        call = (str(query), list(params))
        self.calls.append(call)
        if "INSERT INTO research_ordered_first_touch_outcomes" in call[0]:
            return _Result(one={"event_id": 7} if self.first_write else None)
        if "RETURNING queued.event_id" in call[0]:
            return _Result(many=self.rows)
        return _Result(many=self.rows)


class _ConnectionContext:
    def execute(self, query, params=()):
        if "pg_try_advisory_lock" in str(query):
            return _Result(one={"acquired": True})
        return _Result()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False


class _PsycopgStub:
    @staticmethod
    def connect(*args, **kwargs):
        return _ConnectionContext()


class _AdvisoryState:
    def __init__(self) -> None:
        self.locked = False
        self.connect_kwargs = []


class _AdvisoryConnection:
    def __init__(self, state: _AdvisoryState, kwargs) -> None:
        self.state = state
        self.kwargs = dict(kwargs)
        self.owns_lock = False

    def execute(self, query, params=()):
        assert "pg_try_advisory_lock" in str(query)
        assert list(params) == [worker._ORDERED_FIRST_TOUCH_PASS_LOCK_ID]
        acquired = not self.state.locked
        if acquired:
            self.state.locked = True
            self.owns_lock = True
        return _Result(one={"acquired": acquired})

    def __enter__(self):
        self.state.connect_kwargs.append(self.kwargs)
        return self

    def __exit__(self, exc_type, exc, traceback):
        if self.owns_lock:
            self.state.locked = False
        return False


class _AdvisoryPsycopgStub:
    def __init__(self, state: _AdvisoryState) -> None:
        self.state = state

    def connect(self, *args, **kwargs):
        return _AdvisoryConnection(self.state, kwargs)


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


def _event(event_time):
    return {
        "event_id": 7,
        "event_fingerprint": "event-seven",
        "alert_time_utc": event_time,
        "symbol": "BTC",
        "direction": "LONG",
        "event_type": "SELFTEST",
        "setup_key": "SELFTEST",
        "event_kind": "ALERT",
        "delivery_status": "DELIVERED",
        "current_price": 100.0,
        "target_price": None,
        "engine_snapshot": {
            "price_source": "binance_spot",
            "price_pair": "BTCUSDT",
        },
    }


def run() -> None:
    query_capture = _CaptureConnection()
    assert worker.ResearchOutcomeWorker._load_ordered_first_touch_due_events(
        query_capture, 999
    ) == []
    query, params = query_capture.calls[0]
    assert query.count("%s") == len(params)
    assert "research_prospective_shadow_events authorized" in query
    assert "e.event_kind='ALERT'" in query
    assert "e.event_kind='DECISION_SAMPLE'" in query
    assert "research_ordered_first_touch_outcomes ordered" in query
    assert "ordered.status='OPEN'" in query
    assert "ordered.status='DATA_MISSING'" in query
    assert "MIN(ordered.observed_through_utc) AS queue_time" in query
    assert "new_alerts AS MATERIALIZED" in query
    assert "new_samples AS MATERIALIZED" in query
    assert "picked AS MATERIALIZED" in query
    assert "ORDER BY queue_round, lane, event_id" in query
    assert "LIMIT 1 OFFSET 0" in query
    assert "_alert_reference_queue_priority_sql" not in query
    assert query.count("LIMIT (SELECT batch_limit FROM settings)") == 5
    assert params[-1] == worker._ORDERED_FIRST_TOUCH_EVENT_LIMIT
    assert worker._ORDERED_FIRST_TOUCH_ROW_COUNT == 32

    event_time = datetime(2026, 9, 6, 10, 0, tzinfo=timezone.utc)
    event = _event(event_time)
    path_result = {
        "symbol": "BTC",
        "pair": "BTCUSDT",
        "exchange": "binance",
        "market": "spot",
        "interval": "1m",
        "interval_seconds": 60,
        "complete": True,
        "expected_candles": 1,
        "provenance": "SELFTEST",
        "candles": [
            _candle(event_time, high=100.1, low=99.7, close=99.8)
        ],
    }
    failure = ordered.calculate_ordered_first_touch_outcome(
        reference_price=100.0,
        direction="LONG",
        event_time=event_time,
        candles=path_result["candles"],
        threshold_pct=0.25,
        observation_closed=False,
    )
    storage = _CaptureConnection()
    assert worker.ResearchOutcomeWorker._write_ordered_first_touch_outcome(
        storage,
        event=event,
        window_minutes=60,
        reference_source="binance_spot",
        path_result=path_result,
        outcome=failure,
        expected_candles=1,
    )
    assert len(storage.calls) == 2
    outcome_sql, outcome_params = storage.calls[0]
    stored = json.loads(outcome_params[0])
    assert "research_ordered_first_touch_outcomes" in outcome_sql
    assert stored["status"] == "FAILURE"
    assert stored["first_touch_side"] == "ADVERSE"
    assert stored["observation_closed"] is False
    assert stored["decision_time_utc"]
    assert stored["adverse_touch_price"] == 99.75
    assert stored["favorable_touch_price"] is None

    # A gap suppresses an early real touch and leaves diagnostic observation
    # through the final available candle.  Filling the gap must replace that
    # DATA_MISSING row even though the decisive candle precedes the old end.
    repaired_path = [
        _candle(event_time, high=100.1, low=99.7, close=99.8),
        _candle(event_time + timedelta(minutes=1)),
        _candle(event_time + timedelta(minutes=2)),
    ]
    missing = ordered.calculate_ordered_first_touch_outcome(
        reference_price=100.0,
        direction="LONG",
        event_time=event_time,
        candles=[repaired_path[0], repaired_path[2]],
        threshold_pct=0.25,
        observation_closed=False,
    )
    repaired = ordered.calculate_ordered_first_touch_outcome(
        reference_price=100.0,
        direction="LONG",
        event_time=event_time,
        candles=repaired_path,
        threshold_pct=0.25,
        observation_closed=False,
    )
    assert missing["status"] == "DATA_MISSING"
    assert repaired["status"] == "FAILURE"
    assert repaired["observed_through_utc"] < missing["observed_through_utc"]

    # Execute the actual portable SQL conflict predicate, not a reimplemented
    # Python approximation.  SQLite supplies SQL boolean/NULL semantics here;
    # the surrounding PostgreSQL JSON/insert machinery is captured above.
    predicate = outcome_sql.split(
        "WHERE research_ordered_first_touch_outcomes.status", 1
    )[1].split("RETURNING event_id", 1)[0]
    predicate = "research_ordered_first_touch_outcomes.status" + predicate
    predicate = re.sub(
        r"\b(research_ordered_first_touch_outcomes|EXCLUDED)\.(\w+)",
        lambda match: ":" + (
            "old" if match[1] == "research_ordered_first_touch_outcomes"
            else "new"
        ) + "_" + match[2],
        predicate,
    )
    with sqlite3.connect(":memory:") as sql_check:
        def permits(old, new):
            bindings = {}
            for prefix, record in (("old", old), ("new", new)):
                for key, value in record.items():
                    bindings[f"{prefix}_{key}"] = (
                        value.isoformat() if isinstance(value, datetime)
                        else value
                    )
            return bool(sql_check.execute(
                "SELECT (" + predicate + ")", bindings
            ).fetchone()[0])

        assert permits(missing, repaired)
        assert not permits(missing, {**repaired, "path_complete": False})
        assert permits(missing, {
            **missing,
            "observed_through_utc": missing["observed_through_utc"]
            + timedelta(minutes=1),
        })
        assert not permits(repaired, missing)
        assert not permits({**missing, "status": "OPEN"}, repaired)

    outbox_sql, outbox_params = storage.calls[1]
    assert "research_ordered_first_touch_sync_outbox" in outbox_sql
    assert "claim_token=CASE" in outbox_sql
    assert "claimed_payload_sha256=CASE" in outbox_sql
    sheet_payload = json.loads(outbox_params[5])
    sheet_row = sheet_payload["row"]
    assert sheet_payload["key"] == "outcome_id"
    assert sheet_row["outcome_id"] == (
        "7|60|25|ordered-first-touch-v7"
    )
    assert sheet_row["status"] == "FAILURE"
    assert sheet_row["decision_time_utc"]
    assert sheet_row["adverse_touch_price"] == 99.75
    assert len(outbox_params[6]) == 64

    no_write = _CaptureConnection(first_write=False)
    assert not worker.ResearchOutcomeWorker._write_ordered_first_touch_outcome(
        no_write,
        event=event,
        window_minutes=60,
        reference_source="binance_spot",
        path_result=path_result,
        outcome=failure,
        expected_candles=1,
    )
    assert len(no_write.calls) == 1

    claim_capture = _CaptureConnection()
    assert worker.ResearchOutcomeWorker._claim_ordered_first_touch_outbox(
        claim_capture, 999
    ) == []
    claim_sql, claim_params = claim_capture.calls[0]
    assert "FOR UPDATE SKIP LOCKED" in claim_sql
    assert "attempts=queued.attempts + 1" in claim_sql
    assert "sync_status='IN_FLIGHT'" in claim_sql
    assert "lease_expires_at_utc=NOW() + INTERVAL '2 minutes'" in claim_sql
    assert "claimed_payload_sha256=queued.payload_sha256" in claim_sql
    assert "sync_status='IN_FLIGHT'" in claim_sql
    assert claim_params == [worker._ORDERED_FIRST_TOUCH_OUTBOX_LIMIT]

    keys = [{
        "event_id": 7,
        "window_minutes": 60,
        "threshold_bps": 25,
        "method_version": ordered.METHOD_VERSION,
        "destination": "GOOGLE_SHEETS",
        "claim_token": "12345678-1234-5678-1234-567812345678",
        "claimed_payload_sha256": "a" * 64,
    }]
    finish_capture = _CaptureConnection(rows=[{"event_id": 7}])
    assert worker.ResearchOutcomeWorker._finish_ordered_first_touch_outbox(
        finish_capture, keys, delivered=True
    ) == 1
    assert "sync_status='SYNCED'" in finish_capture.calls[0][0]
    assert "queued.claim_token=keys.claim_token" in finish_capture.calls[0][0]
    assert "queued.payload_sha256=keys.claimed_payload_sha256" in (
        finish_capture.calls[0][0]
    )
    assert "claim_token=NULL" in finish_capture.calls[0][0]
    retry_capture = _CaptureConnection(rows=[{"event_id": 7}])
    assert worker.ResearchOutcomeWorker._finish_ordered_first_touch_outbox(
        retry_capture, keys, delivered=False, error="offline"
    ) == 1
    assert "THEN 'DEAD_LETTER'" in retry_capture.calls[0][0]
    assert "queued.claim_token=keys.claim_token" in retry_capture.calls[0][0]
    assert retry_capture.calls[0][1][1] == "offline"

    # The bounded worker fetches one shared route and derives 4 x 8 rows.
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    live_event = _event(now - timedelta(minutes=61))
    written = []
    fetch_calls = []

    def fetch_path(symbol, start, end):
        fetch_calls.append((symbol, start, end))
        count = worker._expected_candles(start, end)
        first_open = start.replace(second=0, microsecond=0)
        if start != first_open:
            first_open += timedelta(minutes=1)
        candles = [
            _candle(
                first_open + timedelta(minutes=index),
                high=100.3 if index == 0 else 100.1,
                low=99.9,
                close=100.1,
            )
            for index in range(count)
        ]
        return {
            "symbol": "BTC",
            "pair": "BTCUSDT",
            "exchange": "binance",
            "market": "spot",
            "interval": "1m",
            "interval_seconds": 60,
            "complete": True,
            "expected_candles": count,
            "provenance": "SELFTEST",
            "candles": candles,
        }

    service = worker.ResearchOutcomeWorker()
    service._load_ordered_first_touch_due_events = (
        lambda conn, limit: [live_event]
    )
    service._write_ordered_first_touch_outcome = (
        lambda conn, **kwargs: written.append(kwargs) or True
    )
    service._drain_ordered_first_touch_outbox = (
        lambda url: {"claimed": 0, "synced": 0, "failed": 0}
    )
    original_psycopg = worker.psycopg
    original_fetch = worker.canonical_price_path.fetch_closed_candles
    worker.psycopg = _PsycopgStub()
    worker.canonical_price_path.fetch_closed_candles = fetch_path
    try:
        summary = service._run_ordered_first_touch_once(
            "postgresql://selftest", event_limit=1
        )
    finally:
        worker.psycopg = original_psycopg
        worker.canonical_price_path.fetch_closed_candles = original_fetch
    assert summary["checked"] == 1
    assert summary["written"] == 32
    assert summary["path_failures"] == 0
    assert len(fetch_calls) == 1
    assert len({id(call["path_result"]) for call in written}) == 1
    assert {call["window_minutes"] for call in written} == {60, 240, 720, 1440}
    assert {
        call["outcome"]["threshold_bps"] for call in written
    } == {25, 50, 75, 100, 125, 150, 175, 200}
    sixty = [call for call in written if call["window_minutes"] == 60]
    assert next(
        call for call in sixty if call["outcome"]["threshold_bps"] == 25
    )["outcome"]["status"] == "SUCCESS"
    assert next(
        call for call in sixty if call["outcome"]["threshold_bps"] == 50
    )["outcome"]["terminal_reason"] == (
        "OBSERVATION_WINDOW_CLOSED_NO_TOUCH"
    )

    # Model the exact stale-overwrite interleaving: generation two attempts to
    # start while generation one's remote HTTP request is still outstanding.
    # The dedicated session lock must skip it, and permit it only after the
    # first pass (including HTTP) has returned and closed the lock session.
    advisory_state = _AdvisoryState()
    advisory_stub = _AdvisoryPsycopgStub(advisory_state)
    first_service = worker.ResearchOutcomeWorker()
    second_service = worker.ResearchOutcomeWorker()
    remote_order = []
    nested = {}

    def second_generation(url, *, event_limit):
        remote_order.append("new-generation-http-finished")
        return {
            "checked": 1,
            "written": 1,
            "path_failures": 0,
            "synced": 1,
            "sync_failures": 0,
            "lock_skipped": 0,
        }

    def first_generation(url, *, event_limit):
        remote_order.append("old-generation-http-started")
        nested["summary"] = second_service._run_ordered_first_touch_once(
            url, event_limit=event_limit
        )
        remote_order.append("old-generation-http-finished")
        return {
            "checked": 1,
            "written": 1,
            "path_failures": 0,
            "synced": 1,
            "sync_failures": 0,
            "lock_skipped": 0,
        }

    first_service._run_ordered_first_touch_locked = first_generation
    second_service._run_ordered_first_touch_locked = second_generation
    original_psycopg = worker.psycopg
    worker.psycopg = advisory_stub
    try:
        first_summary = first_service._run_ordered_first_touch_once(
            "postgresql://selftest", event_limit=1
        )
        second_summary = second_service._run_ordered_first_touch_once(
            "postgresql://selftest", event_limit=1
        )
    finally:
        worker.psycopg = original_psycopg
    assert first_summary["lock_skipped"] == 0
    assert nested["summary"] == {
        "checked": 0,
        "written": 0,
        "path_failures": 0,
        "synced": 0,
        "sync_failures": 0,
        "lock_skipped": 1,
    }
    assert second_summary["lock_skipped"] == 0
    assert remote_order == [
        "old-generation-http-started",
        "old-generation-http-finished",
        "new-generation-http-finished",
    ]
    assert all(
        call.get("autocommit") is True
        for call in advisory_state.connect_kwargs
    )

    # The retained v6 writer is database-audit-only.
    v6_writer = inspect.getsource(
        worker.ResearchOutcomeWorker._write_first_touch_outcome
    )
    assert "enqueue_first_touch_outcome" not in v6_writer

    print("ordered First Touch v7 worker self-test: PASS")


if __name__ == "__main__":
    run()
