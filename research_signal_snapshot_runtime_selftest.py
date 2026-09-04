"""Database-free checks for the passive Stage-4 projection seam."""
from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import timedelta
import inspect
import os
import threading
from unittest.mock import patch

import research_event_store
import research_signal_snapshot as snapshots
import research_signal_snapshot_runtime as runtime
from research_max_pain_archive_selftest import BASE, _payload
from research_signal_snapshot_selftest import _derivatives, _strong_payload


class _Clock:
    def __init__(self):
        self._offset = 5

    def __call__(self):
        value = BASE + timedelta(minutes=5, seconds=self._offset)
        self._offset += 10
        return value


class _FixedClock:
    def __init__(self, value):
        self._value = value

    def __call__(self):
        return self._value


def _capture(*, idempotent=False, payload=None):
    return {
        "payload": payload or _payload(
            source="RESEARCH_PASSIVE", short_amount=100.0
        ),
        "persistence": {
            "persisted": True,
            "snapshot_set_id": 41,
            "idempotent_existing": idempotent,
        },
        "error": None,
    }


def run() -> None:
    previous = os.environ.get(runtime.ENV_ENABLED)
    previous_persistence = research_event_store._ENABLED
    previous_store_status = runtime.research_signal_snapshot_store.status
    store_gate_status = {
        "configured": True,
        "persistence_enabled": True,
        "archive_database_aligned": True,
        "driver_available": True,
    }

    def ready_store_status():
        return dict(store_gate_status)

    os.environ[runtime.ENV_ENABLED] = "true"
    research_event_store._ENABLED = True
    runtime.research_signal_snapshot_store.status = ready_store_status
    try:
        normalized_row = snapshots._scoring_rows_from_archive(
            {"BTC": [dict(_capture()["payload"]["rows"][0])]}
        )[0]
        assert normalized_row["distance_short_pct"] == 10.0
        assert normalized_row["distance_long_pct"] == -5.0
        fetch_calls = []
        persisted = []

        def fetch(symbols):
            fetch_calls.append(tuple(symbols))
            return _derivatives()

        def persist(events):
            event_list = list(events)
            persisted.append(event_list)
            assert event_list
            assert all(event.event_kind == "DECISION_SAMPLE" for event in event_list)
            assert all(event.event_type != "ALERT" for event in event_list)
            return {
                "persisted": True,
                "inserted": len(event_list),
                "idempotent_existing": 0,
                "event_ids": list(range(1, len(event_list) + 1)),
            }

        projector = runtime.SignalSnapshotProjector(
            snapshot_fetcher=fetch,
            persist=persist,
            now_provider=_Clock(),
        )
        result = asyncio.run(projector.project(_capture()))
        assert result["projected"] is True
        assert fetch_calls == [("BTC",)]
        assert len(persisted) == 1
        assert result["counts"]["magnet"] == 1
        assert result["counts"]["combined"] == 1
        combined = next(
            event
            for event in persisted[0]
            if event.event_type == snapshots.COMBINED_EVENT_TYPE
        )
        assert combined.direction == "SHORT"
        assert combined.engine_snapshot["source_families"] == [
            "COINGLASS_MAX_PAIN",
            "FUTURES_CVD",
            "PRICE_OI",
        ]
        status = projector.status()
        assert status["status"] == "completed"
        assert status["telegram_delivery_path"] is False
        assert status["formula_consumption_path"] is False
        assert status["outcome_consumption_path"] is False
        assert status["trading_path"] is False

        strong_events = []

        def persist_strong(events):
            values = tuple(events)
            strong_events.extend(values)
            return {
                "persisted": True,
                "inserted": len(values),
                "idempotent_existing": 0,
                "event_ids": list(range(1, len(values) + 1)),
            }

        strong_projector = runtime.SignalSnapshotProjector(
            snapshot_fetcher=lambda symbols: _derivatives(),
            persist=persist_strong,
            now_provider=_Clock(),
            retry_delay_seconds=0,
        )
        strong_payload = _strong_payload()
        strong_result = asyncio.run(
            strong_projector.project(_capture(payload=strong_payload))
        )
        assert strong_result["projected"] is True
        assert strong_result["counts"] == {
            "max_pain": 7,
            "magnet": 1,
            "combined": 1,
        }
        maxpain_events = [
            event
            for event in strong_events
            if event.event_type == snapshots.MAX_PAIN_EVENT_TYPE
        ]
        assert len(maxpain_events) == 7
        assert all("STRONG_CONFIRMED" in event.categories for event in maxpain_events)
        archived_targets = {
            row["timeframe"]: row["long_max_pain"]
            for row in strong_payload["rows"]
        }
        assert all(
            event.target_price == archived_targets[event.timeframe]
            for event in maxpain_events
        )

        barrier = threading.Barrier(2)
        parallel_calls = []

        def parallel_fetch(symbols):
            assert len(symbols) == 1
            symbol = symbols[0]
            parallel_calls.append(symbol)
            barrier.wait(timeout=1.0)
            value = deepcopy(_derivatives()["BTC"])
            value["regime"]["symbol"] = symbol
            return {symbol: value}

        parallel_projector = runtime.SignalSnapshotProjector(
            snapshot_fetcher=parallel_fetch,
            persist=lambda events: {},
            source_read_timeout_seconds=1.0,
            source_read_max_concurrency=2,
        )
        parallel_snapshot = asyncio.run(
            parallel_projector._fetch_derivatives(
                ["BTC", "ETH"], timeout_seconds=1.0
            )
        )
        assert sorted(parallel_calls) == ["BTC", "ETH"]
        assert sorted(parallel_snapshot) == ["BTC", "ETH"]

        release_slow_read = threading.Event()

        def slow_fetch(symbols):
            release_slow_read.wait(timeout=1.0)
            return {symbols[0]: deepcopy(_derivatives()["BTC"])}

        timeout_projector = runtime.SignalSnapshotProjector(
            snapshot_fetcher=slow_fetch,
            persist=lambda events: {},
            source_read_timeout_seconds=0.01,
        )

        async def assert_source_timeout():
            result = await timeout_projector._fetch_derivatives(
                ["BTC"], timeout_seconds=0.01
            )
            assert result == {}
            assert timeout_projector.status()["pending_source_read_tasks"] == 1
            asyncio.get_running_loop().call_later(
                0.01, release_slow_read.set
            )
            await timeout_projector.stop()
            assert timeout_projector.status()["pending_source_read_tasks"] == 0

        asyncio.run(assert_source_timeout())

        blocked_read_release = threading.Event()
        blocked_read_starts = []

        def blocked_fetch(symbols):
            blocked_read_starts.append(symbols[0])
            blocked_read_release.wait(timeout=1.0)
            return {symbols[0]: deepcopy(_derivatives()["BTC"])}

        bounded_timeout = runtime.SignalSnapshotProjector(
            snapshot_fetcher=blocked_fetch,
            persist=lambda events: {},
            source_read_timeout_seconds=0.01,
            source_read_max_concurrency=2,
        )

        async def assert_timed_out_reads_remain_bounded():
            first = await bounded_timeout._fetch_derivatives(
                ["BTC", "ETH", "SOL"], timeout_seconds=0.02
            )
            assert first == {}
            assert sorted(blocked_read_starts) == ["BTC", "ETH"]
            assert bounded_timeout.status()["pending_source_read_tasks"] == 2

            second = await bounded_timeout._fetch_derivatives(
                ["XRP"], timeout_seconds=0.01
            )
            assert second == {}
            assert sorted(blocked_read_starts) == ["BTC", "ETH"]
            assert bounded_timeout.status()["pending_source_read_tasks"] == 2

            blocked_read_release.set()
            await bounded_timeout.stop()
            assert bounded_timeout.status()["pending_source_read_tasks"] == 0

        asyncio.run(assert_timed_out_reads_remain_bounded())

        blocked_default = runtime.SignalSnapshotProjector(
            persist=lambda events: {},
            source_read_timeout_seconds=60.0,
        )
        assert blocked_default.status()["source_read_execution"] == (
            "killable_subprocess"
        )
        assert blocked_default.status()["bounded_source_shutdown"] is True

        async def assert_blocked_default_shutdown_is_bounded():
            fetch_task = asyncio.create_task(
                blocked_default._fetch_derivatives(
                    ["BTC"], timeout_seconds=60.0
                )
            )
            for _ in range(1000):
                if blocked_default._source_processes:
                    break
                await asyncio.sleep(0.001)
            assert len(blocked_default._source_processes) == 1
            child = next(iter(blocked_default._source_processes))
            started_at = asyncio.get_running_loop().time()
            await asyncio.wait_for(blocked_default.stop(), timeout=1.0)
            elapsed = asyncio.get_running_loop().time() - started_at
            assert elapsed < 1.0
            assert await asyncio.wait_for(fetch_task, timeout=1.0) == {}
            assert child.returncode is not None
            assert blocked_default.status()["pending_source_read_tasks"] == 0
            assert blocked_default.status()["pending_source_processes"] == 0

        with patch.object(
            blocked_default,
            "_default_source_subprocess_args",
            return_value=(
                runtime.sys.executable,
                "-c",
                "import time; time.sleep(60)",
            ),
        ):
            asyncio.run(assert_blocked_default_shutdown_is_bounded())

        timed_default = runtime.SignalSnapshotProjector(
            persist=lambda events: {},
            source_read_timeout_seconds=0.01,
        )

        async def assert_default_timeout_reaps_child():
            result = await timed_default._fetch_derivatives(
                ["BTC"], timeout_seconds=0.02
            )
            assert result == {}
            assert timed_default.status()["pending_source_read_tasks"] == 0
            assert timed_default.status()["pending_source_processes"] == 0

        with patch.object(
            timed_default,
            "_default_source_subprocess_args",
            return_value=(
                runtime.sys.executable,
                "-c",
                "import time; time.sleep(60)",
            ),
        ):
            asyncio.run(assert_default_timeout_reaps_child())

        mixed_fetches = []
        mixed_writes = []

        def mixed_fetch(symbols):
            assert len(symbols) == 1
            symbol = symbols[0]
            mixed_fetches.append(symbol)
            if symbol == "ETH":
                raise RuntimeError("unsupported derivatives symbol")
            value = deepcopy(_derivatives()["BTC"])
            value["regime"]["symbol"] = symbol
            return {symbol: value}

        mixed = runtime.SignalSnapshotProjector(
            snapshot_fetcher=mixed_fetch,
            persist=lambda events: (
                mixed_writes.append(tuple(events))
                or {
                    "persisted": True,
                    "inserted": len(mixed_writes[-1]),
                    "idempotent_existing": 0,
                    "event_ids": list(range(1, len(mixed_writes[-1]) + 1)),
                }
            ),
            now_provider=_Clock(),
            retry_delay_seconds=0,
        )
        mixed_result = asyncio.run(
            mixed.project(
                _capture(
                    payload=_payload(
                        symbols=("BTC", "ETH"), source="RESEARCH_PASSIVE"
                    )
                )
            )
        )
        assert mixed_result["projected"] is True
        assert mixed_result["evaluation_status"] == "PARTIAL"
        assert mixed_result["evaluated_symbols"] == ["BTC"]
        assert mixed_result["unevaluable_symbols"] == ["ETH"]
        assert sorted(mixed_fetches) == ["BTC", "ETH"]
        assert len(mixed_writes) == 1
        mixed_projection = next(
            event
            for event in mixed_writes[0]
            if event.event_type == snapshots.PROJECTION_EVENT_TYPE
        )
        assert mixed_projection.engine_snapshot["projection"][
            "symbol_evaluations"
        ] == [
            {"symbol": "BTC", "status": "EVALUABLE", "reason": None},
            {
                "symbol": "ETH",
                "status": "UNEVALUABLE",
                "reason": "DERIVATIVES_SNAPSHOT_MISSING",
            },
        ]

        terminal_fetches = []
        terminal = runtime.SignalSnapshotProjector(
            snapshot_fetcher=lambda symbols: terminal_fetches.append(symbols),
            persist=lambda events: {},
            projection_loader=lambda snapshot_key: {
                "terminal": True,
                "event_id": 77,
                "snapshot_key": snapshot_key,
                "status": "COMPLETED",
                "decision_time_utc": (BASE + timedelta(minutes=6)).isoformat(),
                "counts": {"max_pain": 1, "magnet": 1, "combined": 1},
                "signal_event_count": 3,
            },
            now_provider=_Clock(),
        )
        terminal_result = asyncio.run(terminal.project(_capture(idempotent=True)))
        assert terminal_result["terminal"] is True
        assert terminal_fetches == []

        transient_factory_calls = []
        transient_load_calls = []
        transient_closes = []
        transient_fetches = []
        transient_writes = []

        class _TransientLease:
            def __init__(self, snapshot_key):
                self.snapshot_key = snapshot_key

            def load(self):
                transient_load_calls.append(self.snapshot_key)
                if len(transient_load_calls) == 1:
                    raise TimeoutError("transient projection lookup")
                return {"terminal": False, "snapshot_key": self.snapshot_key}

            def persist(self, events):
                values = tuple(events)
                transient_writes.append(values)
                return {
                    "persisted": True,
                    "inserted": len(values),
                    "idempotent_existing": 0,
                    "event_ids": list(range(1, len(values) + 1)),
                }

            def close(self):
                transient_closes.append(self.snapshot_key)

        def transient_lease_factory(snapshot_key):
            transient_factory_calls.append(snapshot_key)
            if len(transient_factory_calls) == 1:
                raise ConnectionError("transient lease acquisition")
            return _TransientLease(snapshot_key)

        transient = runtime.SignalSnapshotProjector(
            snapshot_fetcher=lambda symbols: (
                transient_fetches.append(tuple(symbols)) or _derivatives()
            ),
            lease_factory=transient_lease_factory,
            now_provider=_Clock(),
            retry_delay_seconds=0,
            submission_retry_interval_seconds=0,
        )
        transient_result = asyncio.run(
            transient._project_with_causal_retries(_capture())
        )
        assert transient_result["projected"] is True
        assert transient_result["submission_attempts"] == 3
        assert len(transient_factory_calls) == 3
        assert len(transient_load_calls) == 2
        assert len(transient_closes) == 2
        assert transient_fetches == [("BTC",)]
        assert len(transient_writes) == 1

        conflict_fetches = []
        conflict = runtime.SignalSnapshotProjector(
            snapshot_fetcher=lambda symbols: conflict_fetches.append(symbols),
            lease_factory=lambda snapshot_key: (_ for _ in ()).throw(
                runtime.research_signal_snapshot_store.SignalSnapshotConflictError(
                    snapshot_key
                )
            ),
            now_provider=_Clock(),
            submission_retry_interval_seconds=0,
        )
        conflict_result = asyncio.run(
            conflict._project_with_causal_retries(_capture())
        )
        assert conflict_result["projected"] is False
        assert conflict_result["submission_attempts"] == 1
        assert conflict_fetches == []

        bounded_retry_calls = []
        bounded_retry = runtime.SignalSnapshotProjector(
            snapshot_fetcher=lambda symbols: (_ for _ in ()).throw(
                AssertionError("bounded lease retries must not read sources")
            ),
            lease_factory=lambda snapshot_key: (
                bounded_retry_calls.append(snapshot_key)
                or (_ for _ in ()).throw(ConnectionError("lease unavailable"))
            ),
            now_provider=_FixedClock(BASE + timedelta(minutes=5)),
            submission_retry_interval_seconds=0,
        )
        with patch.object(runtime, "CAUSAL_RETRY_MAX_ATTEMPTS", 2):
            bounded_retry_result = asyncio.run(
                bounded_retry._project_with_causal_retries(_capture())
            )
        assert bounded_retry_result["submission_attempts"] == 2
        assert len(bounded_retry_calls) == 2

        retry_fetches = []
        retry_payloads = []

        def retry_persist(events):
            fingerprints = tuple(event.event_fingerprint for event in events)
            retry_payloads.append(fingerprints)
            if len(retry_payloads) < 3:
                raise RuntimeError("transient write")
            return {
                "persisted": True,
                "inserted": len(fingerprints),
                "idempotent_existing": 0,
                "event_ids": list(range(1, len(fingerprints) + 1)),
            }

        retry = runtime.SignalSnapshotProjector(
            snapshot_fetcher=lambda symbols: (
                retry_fetches.append(tuple(symbols)) or _derivatives()
            ),
            persist=retry_persist,
            now_provider=_Clock(),
            retry_delay_seconds=0,
        )
        retry_result = asyncio.run(retry.project(_capture(idempotent=True)))
        assert retry_result["projected"] is True
        assert retry_fetches == [("BTC",)]
        assert len(retry_payloads) == 3
        assert retry_payloads[0] == retry_payloads[1] == retry_payloads[2]

        failed_attempts = []
        failed_fetches = []
        failed = runtime.SignalSnapshotProjector(
            snapshot_fetcher=lambda symbols: (
                failed_fetches.append(tuple(symbols)) or _derivatives()
            ),
            persist=lambda events: (
                failed_attempts.append(tuple(events))
                or (_ for _ in ()).throw(RuntimeError("write failed"))
            ),
            now_provider=_Clock(),
            retry_delay_seconds=0,
        )
        failed_result = asyncio.run(
            failed._project_with_causal_retries(_capture())
        )
        assert failed_result["projected"] is False
        assert "write failed" in failed_result["reason"]
        assert failed_result["submission_attempts"] == 1
        assert failed.status()["cycles_failed"] == 1
        assert failed_fetches == [("BTC",)]
        assert len(failed_attempts) == runtime.PERSIST_RETRY_ATTEMPTS
        assert all(
            attempt == failed_attempts[0] for attempt in failed_attempts[1:]
        )

        unavailable_writes = []
        unavailable_derivatives = _derivatives()
        unavailable_derivatives["BTC"]["regime"]["available"] = False
        unavailable = runtime.SignalSnapshotProjector(
            snapshot_fetcher=lambda symbols: unavailable_derivatives,
            persist=lambda events: (
                unavailable_writes.append(tuple(events))
                or {
                    "persisted": True,
                    "inserted": len(unavailable_writes[-1]),
                    "idempotent_existing": 0,
                    "event_ids": [1],
                }
            ),
            now_provider=_Clock(),
            retry_delay_seconds=0,
        )
        unavailable_result = asyncio.run(unavailable.project(_capture()))
        assert unavailable_result["projected"] is True
        assert unavailable_result["evaluation_status"] == "UNEVALUABLE"
        assert unavailable_result["evaluated_symbols"] == []
        assert unavailable_result["unevaluable_symbols"] == ["BTC"]
        assert unavailable_result["counts"] == {
            "max_pain": 0,
            "magnet": 0,
            "combined": 0,
        }
        assert len(unavailable_writes) == 1
        assert len(unavailable_writes[0]) == 1
        unavailable_projection = unavailable_writes[0][0]
        assert unavailable_projection.event_type == snapshots.PROJECTION_EVENT_TYPE
        assert unavailable_projection.engine_snapshot["projection"][
            "symbol_evaluations"
        ] == [
            {
                "symbol": "BTC",
                "status": "UNEVALUABLE",
                "reason": "PRICE_OI_UNAVAILABLE",
            }
        ]

        recovering_reads = []
        recovering_writes = []

        def recovering_fetch(symbols):
            recovering_reads.append(tuple(symbols))
            if len(recovering_reads) == 1:
                value = _derivatives()
                value["BTC"]["regime"]["available"] = False
                return value
            return _derivatives()

        recovering = runtime.SignalSnapshotProjector(
            snapshot_fetcher=recovering_fetch,
            persist=lambda events: (
                recovering_writes.append(tuple(events))
                or {
                    "persisted": True,
                    "inserted": len(tuple(events)),
                    "idempotent_existing": 0,
                    "event_ids": list(range(1, len(tuple(events)) + 1)),
                }
            ),
            now_provider=_Clock(),
            retry_delay_seconds=0,
            submission_retry_interval_seconds=0,
        )
        terminalized = asyncio.run(
            recovering._project_with_causal_retries(_capture())
        )
        assert terminalized["projected"] is True
        assert terminalized["evaluation_status"] == "UNEVALUABLE"
        assert terminalized["submission_attempts"] == 1
        assert recovering_reads == [("BTC",)]
        assert len(recovering_writes) == 1

        missed_events = []
        missed_fetches = []

        def persist_missed(events):
            values = list(events)
            missed_events.extend(values)
            return {
                "persisted": True,
                "inserted": len(values),
                "idempotent_existing": 0,
                "event_ids": [901],
            }

        missed = runtime.SignalSnapshotProjector(
            snapshot_fetcher=lambda symbols: missed_fetches.append(symbols),
            persist=persist_missed,
            now_provider=_FixedClock(BASE + timedelta(minutes=21)),
            retry_delay_seconds=0,
        )
        missed_result = asyncio.run(missed.project(_capture(idempotent=True)))
        assert missed_result["status"] == "MISSED_CAUSAL_WINDOW"
        assert missed_result["evaluation_status"] == "UNEVALUABLE"
        assert missed_fetches == []
        assert len(missed_events) == 1
        assert missed_events[0].event_type == snapshots.PROJECTION_EVENT_TYPE
        assert missed_events[0].engine_snapshot["projection"][
            "evaluation_status"
        ] == "UNEVALUABLE"

        archive_calls = []
        reconciliation_events = []

        def archive_loader(**kwargs):
            archive_calls.append(kwargs)
            return [_capture(idempotent=True)]

        def reconciliation_persist(events):
            values = list(events)
            reconciliation_events.extend(values)
            return {
                "persisted": True,
                "inserted": len(values),
                "idempotent_existing": 0,
                "event_ids": list(range(1, len(values) + 1)),
            }

        reconciliation_fetches = []

        def reconciliation_fetch(symbols):
            reconciliation_fetches.append(tuple(symbols))
            value = _derivatives()
            if len(reconciliation_fetches) == 1:
                value["BTC"]["regime"]["available"] = False
            return value

        reconciliation = runtime.SignalSnapshotProjector(
            snapshot_fetcher=reconciliation_fetch,
            persist=reconciliation_persist,
            archive_loader=archive_loader,
            now_provider=_Clock(),
            retry_delay_seconds=0,
            submission_retry_interval_seconds=0,
        )
        reconciled = asyncio.run(reconciliation.reconcile_recent())
        assert reconciled["reconciled"] is True
        assert reconciled["counts"]["archives_loaded"] == 1
        assert reconciled["counts"]["completed_now"] == 1
        assert reconciliation_fetches == [("BTC",)]
        assert len(archive_calls) == 1
        assert archive_calls[0]["limit"] == runtime.RECONCILIATION_LIMIT
        assert archive_calls[0][
            "available_since_utc"
        ] == runtime.RECONCILIATION_ARCHIVE_FLOOR_UTC
        assert archive_calls[0][
            "only_without_signal_snapshot_projection"
        ] is True
        assert any(
            event.event_type == snapshots.PROJECTION_EVENT_TYPE
            for event in reconciliation_events
        )

        malformed_fetches = []
        malformed_writes = []
        malformed = runtime.SignalSnapshotProjector(
            snapshot_fetcher=lambda symbols: malformed_fetches.append(symbols),
            persist=lambda events: malformed_writes.append(tuple(events)),
            now_provider=_Clock(),
        )
        malformed_result = asyncio.run(
            malformed.project(
                {
                    "payload": object(),
                    "persistence": {"persisted": True},
                }
            )
        )
        assert malformed_result["projected"] is False
        assert malformed.status()["status"] == "failed"
        assert malformed.status()["cycles_failed"] == 1
        assert "TypeError" in malformed_result["reason"]
        assert malformed_fetches == []
        assert malformed_writes == []

        submitted = runtime.SignalSnapshotProjector(
            snapshot_fetcher=lambda symbols: _derivatives(),
            persist=reconciliation_persist,
            now_provider=_Clock(),
            retry_delay_seconds=0,
        )

        async def submit_and_wait():
            result = submitted.submit(_capture())
            assert result["submitted"] is True
            await asyncio.gather(*tuple(submitted._background_tasks))

        asyncio.run(submit_and_wait())
        assert submitted.status()["background_projection_tasks"] == 0

        lease_attempted = threading.Event()

        def busy_lease(_snapshot_key):
            lease_attempted.set()
            return None

        stoppable = runtime.SignalSnapshotProjector(
            lease_factory=busy_lease,
            now_provider=_Clock(),
            submission_retry_interval_seconds=60,
        )

        async def submit_and_stop():
            submission = stoppable.submit(_capture())
            assert submission["submitted"] is True
            owned_task = next(iter(stoppable._background_tasks))
            for _ in range(1000):
                if lease_attempted.is_set():
                    break
                await asyncio.sleep(0)
            assert lease_attempted.is_set()
            await stoppable.stop()
            assert owned_task.done()
            assert owned_task.cancelled()

        asyncio.run(submit_and_stop())
        assert stoppable.status()["status"] == "stopped"
        assert stoppable.status()["background_projection_tasks"] == 0

        archive_loader_source = inspect.getsource(
            runtime.research_max_pain_archive.load_recent_passive_snapshot_payloads
        )
        assert "FROM public.research_events projection_event" in archive_loader_source
        assert "projection_event.engine_snapshot" in archive_loader_source
        assert (
            "FROM public.research_max_pain_snapshot_sets archive_set"
            in archive_loader_source
        )
        assert "BTRIM(archive_set.snapshot_key)" in archive_loader_source
        assert "BTRIM(research_max_pain_snapshot_sets.snapshot_key)" not in (
            archive_loader_source
        )

        os.environ[runtime.ENV_ENABLED] = "false"
        disabled_fetches = []
        disabled = runtime.SignalSnapshotProjector(
            snapshot_fetcher=lambda symbols: disabled_fetches.append(symbols),
            persist=lambda events: {},
            now_provider=_Clock(),
        )
        disabled_result = asyncio.run(disabled.project(_capture()))
        assert disabled_result["projected"] is False
        assert disabled_fetches == []
        assert disabled.status()["status"] == "off"

        os.environ[runtime.ENV_ENABLED] = "true"
        with patch.object(
            runtime.research_signal_snapshot_store,
            "status",
            side_effect=ValueError("malformed database URL"),
        ):
            assert runtime.enabled() is False

        os.environ[runtime.ENV_ENABLED] = "true"
        for gate in (
            "configured",
            "persistence_enabled",
            "archive_database_aligned",
            "driver_available",
        ):
            store_gate_status[gate] = False
            gated = runtime.SignalSnapshotProjector(
                snapshot_fetcher=lambda symbols: disabled_fetches.append(symbols),
                persist=lambda events: {},
                now_provider=_Clock(),
            )
            gated_result = asyncio.run(gated.project(_capture()))
            assert gated_result["projected"] is False
            assert gated.status()["enabled"] is False
            store_gate_status[gate] = True
        assert disabled_fetches == []
    finally:
        runtime.research_signal_snapshot_store.status = previous_store_status
        research_event_store._ENABLED = previous_persistence
        if previous is None:
            os.environ.pop(runtime.ENV_ENABLED, None)
        else:
            os.environ[runtime.ENV_ENABLED] = previous

    print("Research signal snapshot runtime self-test: PASS")
    print("One derivatives read per passive cycle: PASS")
    print("No Watch/Telegram dependency: PASS")


if __name__ == "__main__":
    run()
