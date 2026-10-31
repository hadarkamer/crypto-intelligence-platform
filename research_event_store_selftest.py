from __future__ import annotations

from datetime import datetime, timezone
import asyncio
from unittest.mock import patch
import os

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


def check_postgres_operation():
    """Exercise real Watch -> writer -> commit -> archive in a disposable DB."""
    if not os.environ.get("TEST_DATABASE_URL"):
        print("Memory operation PostgreSQL test: SKIP (explicit test DB required)")
        return
    from contextlib import ExitStack
    from datetime import timedelta
    from pathlib import Path
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from uuid import uuid4
    import psycopg
    from psycopg import sql
    from psycopg.conninfo import conninfo_to_dict, make_conninfo
    from psycopg.rows import dict_row

    test_url = os.environ["TEST_DATABASE_URL"]
    info = conninfo_to_dict(test_url)
    if info.get("host") not in {"localhost", "127.0.0.1", "::1", "postgres"} or not (
            info.get("dbname", "").startswith("test_") or info.get("dbname", "").endswith("_test")):
        raise ValueError("Only an explicit local/CI test database is allowed")
    name = "test_memory_operation_" + uuid4().hex
    admin = psycopg.connect(test_url, autocommit=True)
    admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    dsn = make_conninfo(test_url, dbname=name)
    try:
        with psycopg.connect(dsn) as conn:
            conn.execute((Path(__file__).parent / "migrations/001_research_archive_v1.sql").read_text(), prepare=False)
        with ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, {
                "DATABASE_URL": dsn, "RESEARCH_DATABASE_URL": dsn,
                "ALERT_DELIVERY_PROFILE": "ALL",
            }))
            import main as app
            import ai_alert_research as archive
            import research_event_runtime as runtime
            import watch_transition_delivery as delivery
            import watch_transition_store as transitions
            from maxpain_cvd_short_delivery_selftest import _item
            import requests
            import httpx
            stack.enter_context(patch.object(requests.sessions.Session, "request", side_effect=AssertionError("external HTTP forbidden")))
            stack.enter_context(patch.object(httpx.AsyncClient, "send", side_effect=AssertionError("external HTTP forbidden")))
            stack.enter_context(patch.object(research_event_store, "_DATABASE_URL", dsn))
            stack.enter_context(patch.object(research_event_store, "_ENABLED", True))
            stack.enter_context(patch.object(research_event_store, "MAX_RETRY_DELAY_SECONDS", 0.01))
            # Exercise the existing legacy path only in the test subscription.
            stack.enter_context(patch.object(delivery.delivery_policy, "other_experimental_alerts_enabled", return_value=True))
            stack.enter_context(patch.object(delivery.delivery_policy, "ordinary_alerts_enabled", return_value=True))
            sheets = stack.enter_context(patch.object(runtime.google_sheets_sync, "enqueue_delivered_event", return_value=True))
            stack.enter_context(patch.object(archive.ai_history_research, "context_at_time", return_value={"fixture_market_context": True}))
            writer = research_event_store.WRITER  # Production callback, not a test replacement.
            stack.enter_context(patch.object(writer, "flush_interval_seconds", 0.01))
            runtime.reset()
            transitions.init_schema(dsn)

            def rows():
                with psycopg.connect(dsn, row_factory=dict_row) as conn:
                    return conn.execute("SELECT * FROM research_events ORDER BY event_id").fetchall()

            async def exercise():
                now = datetime.now(timezone.utc) - timedelta(minutes=1)
                bot = SimpleNamespace(send_message=AsyncMock())
                await writer.start()
                try:
                    # Actual PostgreSQL lock timeout, not a mocked write failure.
                    with psycopg.connect(dsn) as blocker:
                        blocker.execute("LOCK TABLE research_events IN ACCESS EXCLUSIVE MODE")
                        token = runtime.set_watch_context(watch_scan_id="memory-operation-watch")
                        try:
                            render = lambda item: app._score_confirmation_transition_message(item, transition_confirmed=True)
                            await delivery.record_watch(918208, [{**_item(), "score": 59}],
                                watch_scan_id="memory-baseline", decision_time=now-timedelta(minutes=30), render_score65=render)
                            result = await delivery.record_watch(918208, [_item()],
                                watch_scan_id="memory-operation-watch", decision_time=now, render_score65=render)
                            assert result is not None and len(result["intents"]) == 2
                            await delivery.drain(bot, 918208)
                            async def failed_write():
                                while not writer.metrics.write_failures:
                                    await asyncio.sleep(0.01)
                            await asyncio.wait_for(failed_write(), 8)
                            assert bot.send_message.await_count == 2, "research failure must not block delivery"
                            assert runtime.status()["retained_event_bodies"] == 2
                            assert sheets.call_count == 2
                            assert delivery.status()["evidence_gaps"] == 0
                        finally:
                            runtime.reset_watch_context(token)
                        blocker.rollback()
                    await asyncio.wait_for(writer.queue.join(), 10)
                    saved = rows()
                    assert len(saved) == 2 and runtime.status()["retained_event_bodies"] == 0
                    for row in saved:
                        event = next(call.args[0] for call in sheets.call_args_list
                                     if call.args[0].event_fingerprint == row["event_fingerprint"])
                        assert row["engine_snapshot"] == event.to_dict()["engine_snapshot"]
                        assert row["alert_time_utc"] == now and row["delivery_status"] == "DELIVERED"
                        assert row["engine_snapshot"]["watch_scan_id"] == "memory-operation-watch"
                        assert row["direction"] == event.direction
                    event_id, fingerprint, body = saved[0]["event_id"], saved[0]["event_fingerprint"], saved[0]["engine_snapshot"]
                    context = archive.alert_context(event_id)
                    assert context["available"] and context["event"]["engine_snapshot"] == body
                    assert len(archive.research_alert_history(lookback_days=1)["recent_alerts"]) == 2
                    # Replay after memory release must not resend either message.
                    await delivery.record_watch(918208, [_item()], watch_scan_id="memory-operation-watch",
                        decision_time=now, render_score65=render)
                    await delivery.drain(bot, 918208)
                    assert bot.send_message.await_count == 2 and len(rows()) == 2

                    # More events than the original 4,000-ID capacity, across batches.
                    async def producer(offset):
                        for i in range(25):
                            index = offset+i
                            event = research_event_capture.build_generic_alert_event(
                                symbol="BTC", event_type="MEMORY_OPERATION_FIXTURE", direction="LONG",
                                event_time=now+timedelta(microseconds=index+1),
                                engine_snapshot={"ordinal": index, "payload": "x"*2048})
                            assert runtime._emit(event, persist=True)
                            await asyncio.sleep(0)
                    for cycle in range(41):
                        await asyncio.gather(*(producer(cycle*100+p*25) for p in range(4)))
                        await asyncio.wait_for(writer.queue.join(), 10)
                        assert runtime.status()["retained_event_bodies"] == 0
                    assert len(rows()) == 4102
                    assert runtime.status()["events"] == 4000
                    assert archive.alert_context(event_id)["event"]["engine_snapshot"] == body
                    assert len(archive.research_alert_history(lookback_days=1)["recent_alerts"]) == 2

                    # Graceful writer stop/start and loss of runtime cache preserve history.
                    duplicate = sheets.call_args_list[0].args[0]
                    await writer.stop()
                    runtime.reset()
                    await writer.start()
                    assert runtime._emit(duplicate, persist=True)
                    await asyncio.wait_for(writer.queue.join(), 10)
                    assert len(rows()) == 4102 and runtime.status()["retained_event_bodies"] == 0
                    assert archive.alert_context(event_id)["event"]["event_fingerprint"] == fingerprint
                    assert archive.alert_context(event_id)["event"]["engine_snapshot"] == body
                    print("Memory operation PostgreSQL: PASS; Watch/delivery/lock failure/retry/full archive/4100 concurrent events/restart")
                finally:
                    await writer.stop(flush_timeout_seconds=2)
                    runtime.reset()
            asyncio.run(exercise())
    finally:
        try:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        finally:
            admin.close()


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
    check_postgres_operation()

    print("Research Event persistence self-test: PASS")
    print(research_event_store.persistence_status())


if __name__ == "__main__":
    main()
