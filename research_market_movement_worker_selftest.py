"""Deterministic, network-free self-test for the Wave v5 anchor worker."""

from __future__ import annotations

import ast
import asyncio
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import threading
from types import SimpleNamespace

import research_market_movement as movement
import research_market_movement_worker as worker_module


UTC = timezone.utc


def _raises(expected: str, callback) -> None:
    try:
        callback()
    except Exception as exc:
        assert expected.lower() in str(exc).lower(), (expected, str(exc))
    else:
        raise AssertionError(f"expected failure containing {expected!r}")


def _price_result(
    symbol: str,
    eligible: datetime,
    _fetched_at: datetime,
    *,
    price: object = 100.0,
) -> dict:
    normalized = symbol.upper()
    hype = normalized == "HYPE"
    result = {
        "symbol": normalized,
        "fallback_used": False,
        "pair": "HYPE/USDT" if hype else f"{normalized}USDT",
        "exchange": "hyperliquid" if hype else "binance",
        "market": "spot",
        "interval": "1m",
        "interval_seconds": 60,
        "candles": [
            {
                "open_time_utc": eligible - timedelta(minutes=1),
                "close_time_utc": eligible - timedelta(milliseconds=1),
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 1.0,
            }
        ],
        "expected_candles": 1,
        "complete": True,
    }
    if hype:
        result["api_coin"] = "@107"
        result["provenance"] = "EXCHANGE_API_HISTORICAL_CANDLES_IMPORTED"
    else:
        result["multiplier"] = 1.0
    return result


class _FakeStore:
    def __init__(
        self,
        clock,
        *,
        configured: bool = True,
        preflight_error: Exception | None = None,
        store_errors=(),
        projection_status: dict[str, str] | None = None,
        processing_anchor_override: dict[str, str] | None = None,
    ) -> None:
        self.clock = clock
        self.configured = configured
        self.preflight_error = preflight_error
        self.store_errors = {str(item).upper() for item in store_errors}
        self.projection_status = dict(projection_status or {})
        self.processing_anchor_override = dict(processing_anchor_override or {})
        self.anchors: dict[tuple[str, datetime], movement.NeutralPriceAnchor] = {}
        self.capture_calls: list[tuple[str, datetime]] = []
        self.provider_calls = 0
        self.attempts = 0
        self.attempt_records = []
        self.preflight_calls = 0

    def status(self):
        return {
            "configured": self.configured,
            "schema_auto_create": False,
            "runtime_wired": True,
            "trusted_writer_required": True,
            "live_delivery_allowed": False,
        }

    def runtime_preflight(self):
        self.preflight_calls += 1
        if self.preflight_error is not None:
            raise self.preflight_error
        return {
            "ready": True,
            "relations_verified": 4,
            "schema_auto_create": False,
            "writer_role_verified": True,
            "owner_role_verified": True,
            "wave_table_acl_verified": True,
            "rls_policies_rules_verified": True,
            "user_triggers_verified": 17,
        }

    def capture_prospective(self, *, symbol, eligible_at_utc, provider):
        normalized = movement._symbol(symbol)
        eligible = movement._eligibility(eligible_at_utc)
        self.capture_calls.append((normalized, eligible))
        if normalized in self.store_errors:
            raise RuntimeError(f"simulated store failure for {normalized}")
        key = (normalized, eligible)
        existing = self.anchors.get(key)
        if existing is not None:
            status = self.projection_status.get(normalized, "ALREADY_PROCESSED")
            processing_id = self.processing_anchor_override.get(
                normalized, existing.anchor_id
            )
            return SimpleNamespace(
                symbol=normalized,
                provider_called=False,
                idempotent_anchor=True,
                attempt_receipt_sha256=None,
                anchor=existing,
                processing=SimpleNamespace(
                    status=status,
                    anchor_id=processing_id,
                ),
            )
        if not worker_module.capture_window_open(self.clock(), eligible):
            raise RuntimeError("capture request is outside the eligible window")
        self.provider_calls += 1
        try:
            supplied = provider()
        except Exception:
            supplied = {}
        decision = movement.evaluate_prospective_anchor(
            symbol=normalized,
            eligible_at_utc=eligible,
            decision_time_utc=self.clock(),
            price_candle=supplied.get("price_candle"),
            source_provenance=supplied.get("source_provenance"),
            source_input_fingerprint=supplied.get("source_input_fingerprint"),
        )
        self.attempts += 1
        self.attempt_records.append(decision.attempt)
        if decision.anchor is not None:
            self.anchors[key] = decision.anchor
        status = (
            self.projection_status.get(normalized, "PROCESSED")
            if decision.anchor is not None
            else "UNEVALUABLE"
        )
        processing_id = (
            self.processing_anchor_override.get(
                normalized,
                decision.anchor.anchor_id if decision.anchor is not None else None,
            )
        )
        return SimpleNamespace(
            symbol=normalized,
            provider_called=True,
            idempotent_anchor=False,
            attempt_receipt_sha256=decision.attempt.attempt_receipt_sha256,
            anchor=decision.anchor,
            processing=SimpleNamespace(status=status, anchor_id=processing_id),
        )


class _IdleWorker(worker_module.MarketMovementAnchorWorker):
    async def _run_loop(self) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self._runtime["status"] = "cancelled"
            self._runtime["scheduler_state"] = "cancelled"
            raise


class _BlockingStore(_FakeStore):
    def __init__(self, clock) -> None:
        super().__init__(clock)
        self.started = threading.Event()
        self.release = threading.Event()
        self.finished = threading.Event()

    def capture_prospective(self, **kwargs):
        self.started.set()
        try:
            if not self.release.wait(timeout=3):
                raise RuntimeError("blocking capture was not released")
            return super().capture_prospective(**kwargs)
        finally:
            self.finished.set()


class _BlockingPreflightStore(_FakeStore):
    def __init__(self, clock) -> None:
        super().__init__(clock)
        self.preflight_started = threading.Event()
        self.preflight_release = threading.Event()
        self.preflight_finished = threading.Event()

    def runtime_preflight(self):
        self.preflight_started.set()
        try:
            if not self.preflight_release.wait(timeout=3):
                raise RuntimeError("blocking preflight was not released")
            return super().runtime_preflight()
        finally:
            self.preflight_finished.set()


def _test_schedule_and_adapter() -> None:
    assert worker_module.next_minute_boundary(
        datetime(2026, 9, 3, 12, 1, 59, 999999, tzinfo=UTC)
    ) == datetime(2026, 9, 3, 12, 2, tzinfo=UTC)
    assert worker_module.next_minute_boundary(
        datetime(2026, 9, 3, 12, 2, tzinfo=UTC)
    ) == datetime(2026, 9, 3, 12, 3, tzinfo=UTC)
    assert worker_module.next_minute_boundary(
        datetime(2026, 9, 3, 12, 59, 59, tzinfo=UTC)
    ) == datetime(2026, 9, 3, 13, 0, tzinfo=UTC)
    assert worker_module.latest_eligible_at(
        datetime(2026, 9, 3, 12, 1, 59, tzinfo=UTC)
    ) == datetime(2026, 9, 3, 11, 32, tzinfo=UTC)
    assert worker_module.latest_eligible_at(
        datetime(2026, 9, 3, 12, 2, tzinfo=UTC)
    ) == datetime(2026, 9, 3, 12, 2, tzinfo=UTC)
    assert worker_module.latest_eligible_at(
        datetime(2026, 9, 3, 12, 31, 59, tzinfo=UTC)
    ) == datetime(2026, 9, 3, 12, 2, tzinfo=UTC)
    assert worker_module.latest_eligible_at(
        datetime(2026, 9, 3, 12, 32, tzinfo=UTC)
    ) == datetime(2026, 9, 3, 12, 32, tzinfo=UTC)
    eligibility = datetime(2026, 9, 3, 12, 2, tzinfo=UTC)
    assert worker_module.capture_window_open(eligibility, eligibility)
    assert worker_module.capture_window_open(
        eligibility + timedelta(minutes=29, seconds=59), eligibility
    )
    assert not worker_module.capture_window_open(
        eligibility + timedelta(minutes=30), eligibility
    )
    _raises(
        "timezone",
        lambda: worker_module.latest_eligible_at(datetime(2026, 9, 3, 12, 2)),
    )

    fetched = eligibility + timedelta(seconds=7)
    btc = worker_module.official_provider_payload(
        "BTC",
        _price_result("BTC", eligibility, fetched, price=63_500.25),
        eligible_at_utc=eligibility,
        refresh_completed_at_utc=fetched,
    )
    assert btc["price_candle"]["open_time_utc"].endswith("12:01:00.000000Z")
    assert btc["price_candle"]["close_time_utc"].endswith("12:01:59.999000Z")
    assert btc["price_candle"]["observed_at_utc"] == btc["price_candle"][
        "close_time_utc"
    ]
    assert btc["source_provenance"] == {
        "source": "binance_spot",
        "upstream_source": "binance_spot",
        "quality_status": "PASS",
        "price_exchange": "binance",
        "price_market": "spot",
        "price_pair": "BTCUSDT",
        "price_instrument_id": "BTCUSDT",
        "price_timeframe": "1m",
        "fallback_used": False,
        "fallback_policy": "PROVIDER_ATTESTED_NO_FALLBACK",
    }
    hype = worker_module.official_provider_payload(
        "HYPE",
        _price_result("HYPE", eligibility, fetched, price=47.5),
        eligible_at_utc=eligibility,
        refresh_completed_at_utc=fetched,
    )
    assert hype["source_provenance"]["source"] == "hyperliquid_spot_@107"
    assert hype["source_provenance"]["price_pair"] == "HYPE/USDT"
    assert hype["source_provenance"]["price_instrument_id"] == "@107"

    invalid_cases: list[tuple] = []
    fallback = _price_result("BTC", eligibility, fetched)
    fallback["fallback_used"] = True
    invalid_cases.append(("fallback", fallback))
    wrong_route = _price_result("BTC", eligibility, fetched)
    wrong_route["exchange"] = "bybit"
    invalid_cases.append(("canonical path", wrong_route))
    wrong_open = _price_result("BTC", eligibility, fetched)
    wrong_open["candles"][0]["open_time_utc"] = eligibility
    invalid_cases.append(("open", wrong_open))
    wrong_close = _price_result("BTC", eligibility, fetched)
    wrong_close["candles"][0]["close_time_utc"] = eligibility - timedelta(
        seconds=2
    )
    invalid_cases.append(("close", wrong_close))
    nonfinite = _price_result("BTC", eligibility, fetched, price=float("nan"))
    invalid_cases.append(("finite", nonfinite))
    boolean = _price_result("BTC", eligibility, fetched, price=True)
    invalid_cases.append(("numeric", boolean))
    naive = _price_result("BTC", eligibility, fetched)
    invalid_cases.append(("timezone", naive, fetched.replace(tzinfo=None)))
    expired = _price_result("BTC", eligibility, fetched)
    invalid_cases.append(
        ("capture window", expired, eligibility + timedelta(minutes=30))
    )
    for item in invalid_cases:
        expected, result = item[:2]
        refresh = item[2] if len(item) == 3 else fetched
        _raises(
            expected,
            lambda value=deepcopy(result), completed=refresh: (
                worker_module.official_provider_payload(
                    "BTC",
                    value,
                    eligible_at_utc=eligibility,
                    refresh_completed_at_utc=completed,
                )
            ),
        )
    wrong_hype = _price_result("HYPE", eligibility, fetched)
    wrong_hype["api_coin"] = "@999"
    _raises(
        "HYPE",
        lambda: worker_module.official_provider_payload(
            "HYPE",
            wrong_hype,
            eligible_at_utc=eligibility,
            refresh_completed_at_utc=fetched,
        ),
    )
    for field, value in (("exchange", "binance"), ("pair", "HYPEUSDT")):
        wrong_hype_route = _price_result("HYPE", eligibility, fetched)
        wrong_hype_route[field] = value
        _raises(
            "HYPE",
            lambda result=wrong_hype_route: worker_module.official_provider_payload(
                "HYPE",
                result,
                eligible_at_utc=eligibility,
                refresh_completed_at_utc=fetched,
            ),
        )


async def _test_worker_collection() -> None:
    current = [datetime(2026, 9, 3, 12, 5, 7, tzinfo=UTC)]
    eligibility = datetime(2026, 9, 3, 12, 2, tzinfo=UTC)
    fail_eth = [True]
    fetch_calls: list[tuple[str, datetime, datetime]] = []

    def fetcher(symbol, start_time, end_time):
        fetch_calls.append((symbol, start_time, end_time))
        if symbol == "ETH" and fail_eth[0]:
            result = _price_result(symbol, end_time, current[0])
            result.update({"complete": False, "candles": []})
            return result
        return _price_result(symbol, end_time, current[0])

    store = _FakeStore(lambda: current[0])
    worker = worker_module.MarketMovementAnchorWorker(
        symbols=("ETH", "BTC", "HYPE"),
        candle_fetcher=fetcher,
        store_factory=lambda: store,
        now_provider=lambda: current[0],
    )
    worker._store = store
    first = await worker.run_once(scheduled_at_utc=current[0])
    assert first["eligible_at_utc"].endswith("12:02:00.000000Z")
    assert first["run_symbols"] == ["BTC", "ETH", "HYPE"]
    assert first["captured_symbols"] == ["BTC", "HYPE"]
    assert first["unevaluable_symbols"] == ["ETH"]
    assert first["pending_symbols"] == ["ETH"]
    assert first["results"]["ETH"]["attempt_receipt_sha256"]
    assert store.attempts == 3
    assert len(store.attempt_records) == 3
    assert all(
        attempt.decision_time_utc == current[0]
        for attempt in store.attempt_records
    )
    eth_attempt = next(
        attempt for attempt in store.attempt_records if attempt.symbol == "ETH"
    )
    assert eth_attempt.evaluation_status == movement.UNEVALUABLE
    assert ("ETH", eligibility) not in store.anchors
    assert all(
        call[1] == eligibility - timedelta(minutes=1)
        and call[2] == eligibility
        for call in fetch_calls
    )

    fail_eth[0] = False
    current[0] = datetime(2026, 9, 3, 12, 6, 2, tzinfo=UTC)
    second = await worker.run_once(scheduled_at_utc=current[0])
    assert second["run_symbols"] == ["ETH"]
    assert second["captured_symbols"] == ["ETH"]
    assert second["pending_symbols"] == []
    assert fetch_calls[-1] == (
        "ETH",
        eligibility - timedelta(minutes=1),
        eligibility,
    )
    call_count = len(fetch_calls)
    current[0] = datetime(2026, 9, 3, 12, 7, tzinfo=UTC)
    assert await worker._run_due_slot(current[0]) is False
    assert len(fetch_calls) == call_count

    frozen_ids = {
        key: value.anchor_id for key, value in store.anchors.items()
    }
    frozen_counts = (
        store.attempts,
        len(store.anchors),
        store.provider_calls,
        len(store.capture_calls),
    )

    # A fresh process still enters the store.  Existing anchors are read under
    # the slot lock before the lazy callback, so restart retries perform no I/O.
    restarted = worker_module.MarketMovementAnchorWorker(
        symbols=("BTC", "ETH", "HYPE"),
        candle_fetcher=fetcher,
        store_factory=lambda: store,
        now_provider=lambda: current[0],
    )
    restarted._store = store
    restart_summary = await restarted.run_once(scheduled_at_utc=current[0])
    assert restart_summary["existing_symbols"] == ["BTC", "ETH", "HYPE"]
    assert len(fetch_calls) == call_count
    assert all(
        result["idempotent_anchor"] is True
        and result["provider_called"] is False
        and result["attempt_receipt_sha256"] is None
        for result in restart_summary["results"].values()
    )
    assert (
        store.attempts,
        len(store.anchors),
        store.provider_calls,
        len(store.capture_calls),
    ) == (
        frozen_counts[0],
        frozen_counts[1],
        frozen_counts[2],
        frozen_counts[3] + 3,
    )
    assert {
        key: value.anchor_id for key, value in store.anchors.items()
    } == frozen_ids

    # An expired exact retry still reads the frozen authority before the
    # store's window gate; no provider or attempt is allowed on that path.
    late_counts = (store.attempts, store.provider_calls, len(fetch_calls))
    current[0] = eligibility + timedelta(minutes=31)
    late_worker = worker_module.MarketMovementAnchorWorker(
        symbols=("BTC",),
        candle_fetcher=fetcher,
        store_factory=lambda: store,
        now_provider=lambda: current[0],
    )
    late_worker._store = store
    late = await late_worker.run_once(
        scheduled_at_utc=current[0], eligible_at_utc=eligibility
    )
    assert late["existing_symbols"] == ["BTC"]
    assert late["results"]["BTC"]["idempotent_anchor"] is True
    assert (store.attempts, store.provider_calls, len(fetch_calls)) == late_counts

    absent_store = _FakeStore(lambda: current[0])
    absent_fetches: list[str] = []
    absent_worker = worker_module.MarketMovementAnchorWorker(
        symbols=("BTC",),
        candle_fetcher=lambda symbol, start, end: absent_fetches.append(symbol),
        store_factory=lambda: absent_store,
        now_provider=lambda: current[0],
    )
    absent_worker._store = absent_store
    absent = await absent_worker.run_once(
        scheduled_at_utc=current[0], eligible_at_utc=eligibility
    )
    assert set(absent["errors"]) == {"BTC"}
    assert absent["pending_symbols"] == ["BTC"]
    assert absent_fetches == []
    assert absent_store.attempts == 0

    # An unresolved old slot is never carried across the next eligibility.
    rollover_now = [datetime(2026, 9, 3, 12, 31, 30, tzinfo=UTC)]
    rollover_calls: list[datetime] = []

    def missing_fetcher(symbol, start_time, end_time):
        rollover_calls.append(end_time)
        result = _price_result(symbol, end_time, rollover_now[0])
        result.update({"complete": False, "candles": []})
        return result

    rollover_store = _FakeStore(lambda: rollover_now[0])
    rollover = worker_module.MarketMovementAnchorWorker(
        symbols=("ETH",),
        candle_fetcher=missing_fetcher,
        store_factory=lambda: rollover_store,
        now_provider=lambda: rollover_now[0],
    )
    rollover._store = rollover_store
    old = await rollover.run_once(scheduled_at_utc=rollover_now[0])
    assert old["pending_symbols"] == ["ETH"]
    rollover_now[0] = datetime(2026, 9, 3, 12, 32, 1, tzinfo=UTC)
    new = await rollover.run_once(scheduled_at_utc=rollover_now[0])
    assert new["eligible_at_utc"].endswith("12:32:00.000000Z")
    assert rollover_calls == [
        datetime(2026, 9, 3, 12, 2, tzinfo=UTC),
        datetime(2026, 9, 3, 12, 32, tzinfo=UTC),
    ]

    # One store failure remains local; later symbols still capture.
    error_now = [datetime(2026, 9, 3, 13, 3, tzinfo=UTC)]
    error_store = _FakeStore(lambda: error_now[0], store_errors=("BTC",))
    error_calls: list[str] = []

    def error_fetcher(symbol, start_time, end_time):
        error_calls.append(symbol)
        return _price_result(symbol, end_time, error_now[0])

    error_worker = worker_module.MarketMovementAnchorWorker(
        symbols=("BTC", "HYPE"),
        candle_fetcher=error_fetcher,
        store_factory=lambda: error_store,
        now_provider=lambda: error_now[0],
    )
    error_worker._store = error_store
    error_summary = await error_worker.run_once(scheduled_at_utc=error_now[0])
    assert set(error_summary["errors"]) == {"BTC"}
    assert error_summary["captured_symbols"] == ["HYPE"]
    assert error_calls == ["HYPE"]
    error_status = error_worker.status()
    assert error_status["status"] == "retry_pending_with_errors"
    assert error_status["pending_symbols"] == ["BTC"]
    error_store.store_errors.clear()
    error_now[0] = datetime(2026, 9, 3, 13, 4, tzinfo=UTC)
    assert await error_worker._run_due_slot(error_now[0]) is True
    assert error_worker.status()["last_run_summary"]["run_symbols"] == ["BTC"]
    assert error_worker.status()["pending_symbols"] == []
    assert error_calls == ["HYPE", "BTC"]
    assert [symbol for symbol, _eligible in error_store.capture_calls] == [
        "BTC",
        "HYPE",
        "BTC",
    ]

    # A store may process an older backlog anchor while inserting the current
    # one.  The worker must surface the mismatch and must not auto-drain it.
    backlog_now = [datetime(2026, 9, 3, 13, 4, tzinfo=UTC)]
    backlog_store = _FakeStore(
        lambda: backlog_now[0],
        processing_anchor_override={"BTC": "0" * 64},
    )
    backlog_worker = worker_module.MarketMovementAnchorWorker(
        symbols=("BTC",),
        candle_fetcher=lambda symbol, start_time, end_time: _price_result(
            "BTC", end_time, backlog_now[0]
        ),
        store_factory=lambda: backlog_store,
        now_provider=lambda: backlog_now[0],
    )
    backlog_worker._store = backlog_store
    backlog = await backlog_worker.run_once(scheduled_at_utc=backlog_now[0])
    assert backlog["projection_pending_symbols"] == ["BTC"]
    assert backlog["pending_symbols"] == []
    assert backlog_worker.status()["status"] == "blocked_projection"
    assert await backlog_worker._run_due_slot(backlog_now[0]) is False

    # A projection block must survive later retries of unrelated source gaps.
    mixed_now = [datetime(2026, 9, 3, 13, 5, tzinfo=UTC)]
    mixed_eth_missing = [True]
    mixed_store = _FakeStore(
        lambda: mixed_now[0],
        processing_anchor_override={"BTC": "1" * 64},
    )

    def mixed_fetcher(symbol, start_time, end_time):
        if symbol == "ETH" and mixed_eth_missing[0]:
            result = _price_result(symbol, end_time, mixed_now[0])
            result.update({"complete": False, "candles": []})
            return result
        return _price_result(symbol, end_time, mixed_now[0])

    mixed_worker = worker_module.MarketMovementAnchorWorker(
        symbols=("BTC", "ETH"),
        candle_fetcher=mixed_fetcher,
        store_factory=lambda: mixed_store,
        now_provider=lambda: mixed_now[0],
    )
    mixed_worker._store = mixed_store
    mixed_first = await mixed_worker.run_once(scheduled_at_utc=mixed_now[0])
    assert mixed_first["projection_pending_symbols"] == ["BTC"]
    assert mixed_first["pending_symbols"] == ["ETH"]
    mixed_eth_missing[0] = False
    mixed_now[0] = datetime(2026, 9, 3, 13, 6, tzinfo=UTC)
    mixed_second = await mixed_worker.run_once(scheduled_at_utc=mixed_now[0])
    assert mixed_second["run_symbols"] == ["ETH"]
    assert mixed_second["projection_pending_symbols"] == ["BTC"]
    assert mixed_second["pending_symbols"] == []
    mixed_status = mixed_worker.status()
    assert mixed_status["status"] == "blocked_projection"
    assert mixed_status["last_completed_eligible_at_utc"] is None

    # Scheduler shutdown must join a DB/network thread that already started;
    # cancellation of asyncio.to_thread alone would otherwise return early.
    stop_now = [datetime(2026, 9, 3, 14, 5, tzinfo=UTC)]
    blocking_store = _BlockingStore(lambda: stop_now[0])
    blocking_worker = worker_module.MarketMovementAnchorWorker(
        symbols=("BTC",),
        candle_fetcher=lambda symbol, start_time, end_time: _price_result(
            "BTC", end_time, stop_now[0]
        ),
        store_factory=lambda: blocking_store,
        now_provider=lambda: stop_now[0],
    )
    blocking_worker._store = blocking_store
    blocking_worker._runtime["running"] = True
    blocking_worker._task = asyncio.create_task(
        blocking_worker.run_once(scheduled_at_utc=stop_now[0])
    )
    for _ in range(100):
        if blocking_store.started.is_set():
            break
        await asyncio.sleep(0.001)
    assert blocking_store.started.is_set()
    stop_task = asyncio.create_task(blocking_worker.stop())
    await asyncio.sleep(0.02)
    assert not stop_task.done()
    assert not blocking_store.finished.is_set()
    blocking_store.release.set()
    await asyncio.wait_for(stop_task, timeout=2)
    assert blocking_store.finished.is_set()
    assert blocking_worker.status()["capture_inflight"] is False
    assert blocking_worker._store is None


async def _test_real_scheduler_loop() -> None:
    current = [datetime(2026, 9, 3, 15, 2, 5, tzinfo=UTC)]
    missing = [True]
    fetch_calls: list[tuple[str, datetime, datetime]] = []
    waiting_again = asyncio.Event()
    sleep_delays: list[float] = []

    def fetcher(symbol, start_time, end_time):
        fetch_calls.append((symbol, start_time, end_time))
        result = _price_result(symbol, end_time, current[0])
        if missing[0]:
            result.update({"complete": False, "candles": []})
        return result

    async def controlled_sleep(delay):
        sleep_delays.append(delay)
        if len(sleep_delays) == 1:
            missing[0] = False
            current[0] = worker_module.next_minute_boundary(current[0])
            return
        waiting_again.set()
        await asyncio.Event().wait()

    store = _FakeStore(lambda: current[0])
    worker = worker_module.MarketMovementAnchorWorker(
        symbols=("BTC",),
        candle_fetcher=fetcher,
        store_factory=lambda: store,
        now_provider=lambda: current[0],
        sleep=controlled_sleep,
    )
    worker._store = store
    worker._runtime["running"] = True
    task = asyncio.create_task(worker._run_loop())
    await asyncio.wait_for(waiting_again.wait(), timeout=2)
    status = worker.status()
    assert sleep_delays == [55.0, 60.0]
    assert len(fetch_calls) == 2
    assert fetch_calls[0][1:] == fetch_calls[1][1:]
    assert fetch_calls[0][1] == datetime(2026, 9, 3, 15, 1, tzinfo=UTC)
    assert fetch_calls[0][2] == datetime(2026, 9, 3, 15, 2, tzinfo=UTC)
    assert status["status"] == "completed"
    assert status["scheduler_state"] == "waiting"
    assert status["pending_symbols"] == []
    assert status["next_run_utc"].endswith("15:04:00.000000Z")
    worker._runtime["status"] = "starting"
    assert await worker._run_due_slot(current[0]) is False
    assert worker.status()["status"] == "completed"
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Waiting for the next minute must not hide a projection block in health.
    blocked_now = [datetime(2026, 9, 3, 16, 2, 5, tzinfo=UTC)]
    blocked_waiting = asyncio.Event()

    async def blocked_sleep(_delay):
        blocked_waiting.set()
        await asyncio.Event().wait()

    blocked_store = _FakeStore(
        lambda: blocked_now[0],
        processing_anchor_override={"BTC": "f" * 64},
    )
    blocked = worker_module.MarketMovementAnchorWorker(
        symbols=("BTC",),
        candle_fetcher=lambda symbol, start, end: _price_result(
            symbol, end, blocked_now[0]
        ),
        store_factory=lambda: blocked_store,
        now_provider=lambda: blocked_now[0],
        sleep=blocked_sleep,
    )
    blocked._store = blocked_store
    blocked._runtime["running"] = True
    blocked_task = asyncio.create_task(blocked._run_loop())
    await asyncio.wait_for(blocked_waiting.wait(), timeout=2)
    blocked_status = blocked.status()
    assert blocked_status["status"] == "blocked_projection"
    assert blocked_status["scheduler_state"] == "waiting"
    assert blocked_status["projection_pending_symbols"] == ["BTC"]
    blocked._runtime["status"] = "starting"
    assert await blocked._run_due_slot(blocked_now[0]) is False
    assert blocked.status()["status"] == "blocked_projection"
    blocked_task.cancel()
    try:
        await blocked_task
    except asyncio.CancelledError:
        pass

    # A long event-loop stall must use the real wake time and collect the
    # still-active slot, not replay the expired boundary computed before sleep.
    delayed_now = [datetime(2026, 9, 3, 18, 1, 50, tzinfo=UTC)]
    delayed_waiting = asyncio.Event()
    delayed_sleeps = [0]
    delayed_eligibilities: list[datetime] = []

    async def delayed_sleep(_delay):
        delayed_sleeps[0] += 1
        if delayed_sleeps[0] == 1:
            delayed_now[0] = datetime(2026, 9, 3, 19, 1, tzinfo=UTC)
            return
        delayed_waiting.set()
        await asyncio.Event().wait()

    def delayed_fetcher(symbol, start_time, end_time):
        delayed_eligibilities.append(end_time)
        return _price_result(symbol, end_time, delayed_now[0])

    delayed_store = _FakeStore(lambda: delayed_now[0])
    delayed = worker_module.MarketMovementAnchorWorker(
        symbols=("BTC",),
        candle_fetcher=delayed_fetcher,
        store_factory=lambda: delayed_store,
        now_provider=lambda: delayed_now[0],
        sleep=delayed_sleep,
    )
    delayed._store = delayed_store
    delayed._runtime["running"] = True
    delayed_task = asyncio.create_task(delayed._run_loop())
    await asyncio.wait_for(delayed_waiting.wait(), timeout=2)
    assert delayed_eligibilities == [
        datetime(2026, 9, 3, 17, 32, tzinfo=UTC),
        datetime(2026, 9, 3, 18, 32, tzinfo=UTC),
    ]
    assert delayed.status()["active_eligible_at_utc"].endswith(
        "18:32:00.000000Z"
    )
    delayed_task.cancel()
    try:
        await delayed_task
    except asyncio.CancelledError:
        pass


async def _test_default_canonical_path() -> None:
    current = [datetime(2026, 9, 3, 17, 2, 7, tzinfo=UTC)]
    direct_calls: list[tuple[str, datetime, datetime]] = []
    canonical = worker_module.canonical_price_path
    original_binance_fetch = canonical.binance_spot_price_path.fetch_closed_candles
    original_hype_fetch = canonical.hyperliquid_spot_price_path.fetch_closed_candles

    def direct_binance(symbol, start_time, end_time):
        direct_calls.append((symbol, start_time, end_time))
        return _price_result(symbol, end_time, current[0])

    def direct_hype(symbol, start_time, end_time):
        direct_calls.append((symbol, start_time, end_time))
        return _price_result(symbol, end_time, current[0])

    canonical.binance_spot_price_path.fetch_closed_candles = direct_binance
    canonical.hyperliquid_spot_price_path.fetch_closed_candles = direct_hype
    try:
        store = _FakeStore(lambda: current[0])
        worker = worker_module.MarketMovementAnchorWorker(
            symbols=("HYPE", "BTC"),
            store_factory=lambda: store,
            now_provider=lambda: current[0],
        )
        assert worker._candle_fetcher is canonical.fetch_closed_candles
        assert worker_module.WORKER._candle_fetcher is canonical.fetch_closed_candles
        worker._store = store
        summary = await worker.run_once(scheduled_at_utc=current[0])
        assert summary["captured_symbols"] == ["BTC", "HYPE"]
        assert direct_calls == [
            (
                "BTC",
                datetime(2026, 9, 3, 17, 1, tzinfo=UTC),
                datetime(2026, 9, 3, 17, 2, tzinfo=UTC),
            ),
            (
                "HYPE",
                datetime(2026, 9, 3, 17, 1, tzinfo=UTC),
                datetime(2026, 9, 3, 17, 2, tzinfo=UTC),
            ),
        ]
        assert all(
            result["evaluation_status"] == movement.EVALUABLE
            for result in summary["results"].values()
        )
    finally:
        canonical.binance_spot_price_path.fetch_closed_candles = (
            original_binance_fetch
        )
        canonical.hyperliquid_spot_price_path.fetch_closed_candles = (
            original_hype_fetch
        )


async def _test_start_guards() -> None:
    previous = os.environ.get(worker_module.ENV_ENABLED)
    try:
        os.environ.pop(worker_module.ENV_ENABLED, None)
        factory_calls = [0]

        def forbidden_factory():
            factory_calls[0] += 1
            raise AssertionError("disabled worker constructed a store")

        disabled = worker_module.MarketMovementAnchorWorker(
            store_factory=forbidden_factory
        )
        assert await disabled.start() is False
        assert factory_calls == [0]
        assert disabled.status()["status"] == "off"

        os.environ[worker_module.ENV_ENABLED] = "1"
        clock = lambda: datetime(2026, 9, 3, 12, 5, tzinfo=UTC)
        unconfigured = worker_module.MarketMovementAnchorWorker(
            store_factory=lambda: _FakeStore(clock, configured=False),
            now_provider=clock,
        )
        assert await unconfigured.start() is False
        assert unconfigured.status()["status"] == "blocked_database"
        assert unconfigured._task is None

        blocked = worker_module.MarketMovementAnchorWorker(
            store_factory=lambda: _FakeStore(
                clock, preflight_error=RuntimeError("wrong writer")
            ),
            now_provider=clock,
        )
        assert await blocked.start() is False
        assert blocked.status()["status"] == "blocked_writer"
        assert blocked._task is None

        ready_store = _FakeStore(clock)
        ready = _IdleWorker(
            store_factory=lambda: ready_store,
            now_provider=clock,
        )
        starts = await asyncio.gather(ready.start(), ready.start())
        assert starts == [True, True]
        assert ready_store.preflight_calls == 1
        first_task = ready._task
        assert await ready.start() is True
        assert ready._task is first_task
        status = ready.status()
        assert status["preflight"]["relations_verified"] == 4
        assert status["formula_consumption_path"] is False
        assert status["telegram_delivery_path"] is False
        assert status["trading_path"] is False
        assert status["automatic_repair"] is False
        assert status["historical_import_runtime"] is False
        await ready.stop()
        assert ready.status()["task_running"] is False
        assert ready._store is None
        assert ready.status()["preflight"] is None

        ready._store_factory = lambda: _FakeStore(clock, configured=False)
        assert await ready.start() is False
        assert ready.status()["status"] == "blocked_database"
        assert ready._store is None
        assert ready.status()["preflight"] is None
        try:
            await ready.run_once(scheduled_at_utc=clock())
        except RuntimeError as exc:
            assert "not initialized" in str(exc)
        else:
            raise AssertionError("failed restart retained the prior store")

        race_store = _BlockingPreflightStore(clock)
        race = _IdleWorker(
            store_factory=lambda: race_store,
            now_provider=clock,
        )
        race_start = asyncio.create_task(race.start())
        for _ in range(100):
            if race_store.preflight_started.is_set():
                break
            await asyncio.sleep(0.001)
        assert race_store.preflight_started.is_set()
        race_stop = asyncio.create_task(race.stop())
        await asyncio.sleep(0.02)
        assert not race_stop.done()
        race_store.preflight_release.set()
        assert await asyncio.wait_for(race_start, timeout=2) is True
        await asyncio.wait_for(race_stop, timeout=2)
        assert race.status()["task_running"] is False
        assert race._store is None

        cancelled_store = _BlockingPreflightStore(clock)
        cancelled = _IdleWorker(
            store_factory=lambda: cancelled_store,
            now_provider=clock,
        )
        cancelled_start = asyncio.create_task(cancelled.start())
        for _ in range(100):
            if cancelled_store.preflight_started.is_set():
                break
            await asyncio.sleep(0.001)
        assert cancelled_store.preflight_started.is_set()
        cancelled_start.cancel()
        cancelled_stop = asyncio.create_task(cancelled.stop())
        await asyncio.sleep(0.02)
        assert not cancelled_stop.done()
        assert not cancelled_store.preflight_finished.is_set()
        cancelled_store.preflight_release.set()
        try:
            await asyncio.wait_for(cancelled_start, timeout=2)
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("cancelled start unexpectedly completed")
        await asyncio.wait_for(cancelled_stop, timeout=2)
        assert cancelled_store.preflight_finished.is_set()
        assert cancelled.status()["preflight_inflight"] is False
    finally:
        if previous is None:
            os.environ.pop(worker_module.ENV_ENABLED, None)
        else:
            os.environ[worker_module.ENV_ENABLED] = previous


def _test_static_isolation() -> None:
    root = Path(__file__).resolve().parent
    module_path = root / "research_market_movement_worker.py"
    module_text = module_path.read_text(encoding="utf-8")
    imports = set()
    for node in ast.walk(ast.parse(module_text)):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.add(node.module or "")
    assert not any(name.startswith("research_prospective") for name in imports)
    assert not any("telegram" in name for name in imports)
    assert not any("formula" in name for name in imports)
    assert not any("alert" in name for name in imports)
    assert "canonical_price_path.fetch_closed_candles" in module_text
    assert "selected_eligibility - timedelta(minutes=1)" in module_text
    assert "fetch_research_spot_1m_prices" not in module_text
    assert "schema_ready" not in module_text
    assert "CREATE TABLE" not in module_text
    assert "process_earliest_pending" not in module_text
    assert "import_verified_historical_slot" not in module_text
    assert "repair_missing_btc_parent" not in module_text
    assert "send_message" not in module_text

    main_text = (root / "main.py").read_text(encoding="utf-8")
    assert "import research_market_movement_worker" in main_text
    assert '"market_movements": research_market_movement_worker.WORKER.status()' in main_text
    assert "await research_market_movement_worker.WORKER.start()" in main_text
    assert "await research_market_movement_worker.WORKER.stop()" in main_text
    startup = main_text.index("await _prepare_research_schema()")
    wave_start = main_text.index(
        "await research_market_movement_worker.WORKER.start()"
    )
    assert startup < wave_start


def run() -> None:
    _test_schedule_and_adapter()
    asyncio.run(_test_worker_collection())
    asyncio.run(_test_real_scheduler_loop())
    asyncio.run(_test_default_canonical_path())
    asyncio.run(_test_start_guards())
    _test_static_isolation()
    print("research_market_movement_worker_selftest: PASS")


if __name__ == "__main__":
    run()
