from __future__ import annotations

from datetime import datetime, timezone
import asyncio
from unittest.mock import patch

import research_event_capture
import research_event_store


async def check_committed_release(event):
    sink = research_event_capture.DryRunResearchCapture(max_events=2)
    sink.emit(event)
    writer = research_event_store.AsyncResearchEventWriter(capacity=1, flush_interval_seconds=0.01)
    committed = []
    attempts = []
    class Connection:
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc, tb):
            if exc_type is None:
                if len(attempts) == 1:
                    raise RuntimeError("isolated commit failure")
                committed.extend(attempts[-1])
        def cursor(self):
            class Cursor:
                def __enter__(self):
                    return self
                def __exit__(self, *args):
                    return False
                def executemany(self, sql, rows):
                    assert sink.events() == [event.to_dict()], "retain body until commit"
                    attempts.append(rows.copy())
            return Cursor()
    def release(rows):
        assert committed == rows
        sink.release_persisted(row["event_fingerprint"] for row in rows)
    writer.on_persisted = release
    with patch.object(research_event_store, "_ENABLED", True), \
         patch.object(research_event_store, "_DATABASE_URL", "isolated://fake"), \
         patch.object(research_event_store.psycopg, "connect", return_value=Connection()), \
         patch.object(research_event_store, "MAX_RETRY_DELAY_SECONDS", 0.001):
        assert writer.enqueue(event)
        assert not writer.enqueue(event), "full queue cannot trigger release"
        assert sink.events() == [event.to_dict()]
        await writer.start()
        await asyncio.wait_for(writer.queue.join(), 2)
        await writer.stop()
    assert len(attempts) == 2 and attempts[0] == attempts[1]
    assert sink.events() == []
    assert sink.emit(event) is False, "commit must retain dedupe"
    assert writer.metrics.batches == 1 and writer.metrics.batch_retries == 1
    assert committed[0]["engine_snapshot"] == research_event_store.serialize_event(event)["engine_snapshot"]

    # A cleanup error cannot re-submit or lose a successfully committed batch.
    writer = research_event_store.AsyncResearchEventWriter(flush_interval_seconds=0.01)
    writer.on_persisted = lambda rows: (_ for _ in ()).throw(RuntimeError("cleanup only"))
    with patch.object(research_event_store, "_ENABLED", True), \
         patch.object(research_event_store, "_DATABASE_URL", "isolated://fake"), \
         patch.object(writer, "_write_batch") as write:
        writer.enqueue(event)
        await writer.start()
        await asyncio.wait_for(writer.queue.join(), 2)
        await writer.stop()
    assert write.call_count == 1 and writer.metrics.batch_retries == 0


def main() -> None:
    decision_time = datetime(2026, 8, 20, 10, 0, tzinfo=timezone.utc)
    delivered_time = datetime(2026, 8, 20, 10, 0, 2, tzinfo=timezone.utc)
    event = research_event_capture.build_generic_alert_event(
        symbol="BTC",
        event_type="FUTURES_CVD_HIGH",
        direction="LONG",
        source_side="BULLISH",
        event_time=decision_time,
        score=67.5,
        current_price=65000,
        categories=["FUTURES_CVD", "QUALITY_65"],
        engine_snapshot={"module": {"score": 67.5}},
        strategy_version="selftest",
        code_version="selftest",
    )
    row = research_event_store.serialize_event(
        event,
        capture_stage="TRANSITION_APPROVED",
        delivery_status="DELIVERED",
        delivery_attempted_at_utc=decision_time,
        delivered_at_utc=delivered_time,
    )
    assert row["symbol"] == "BTC"
    assert row["event_type"] == "FUTURES_CVD_HIGH"
    assert row["source_side"] == "BULLISH"
    assert row["delivery_status"] == "DELIVERED"
    assert row["alert_time_utc"] == event.alert_time_utc
    assert row["delivered_at_utc"] == delivered_time
    assert row["runtime_session_id"] == research_event_store.RUNTIME_SESSION_ID
    assert len(row["event_fingerprint"]) == 64
    status = research_event_store.WRITER.status()
    assert status["schema_auto_create"] is False
    assert status["transient_failure_policy"] == "retry_current_batch_with_backoff"

    # CI intentionally supplies no enable flag / research DB URL. Importing and
    # serializing must therefore remain incapable of writing anything.
    assert research_event_store.persistence_status()["enabled"] is False
    assert research_event_store.WRITER.enqueue(event) is False
    assert research_event_store.WRITER.status()["queue_size"] == 0

    sample = research_event_capture.build_decision_sample(
        symbol="BTC",
        sample_type="NEAR_THRESHOLD",
        direction="LONG",
        score=64.9,
        event_time=decision_time,
    )
    sample_row = research_event_store.serialize_event(sample)
    assert sample_row["delivery_status"] == "NOT_APPLICABLE"

    asyncio.run(check_committed_release(event))

    print("Research Event persistence self-test: PASS")
    print(research_event_store.persistence_status())


if __name__ == "__main__":
    main()
