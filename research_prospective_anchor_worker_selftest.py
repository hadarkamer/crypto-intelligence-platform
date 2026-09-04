"""Deterministic self-test for the prospective-anchor minute worker."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import os
from pathlib import Path
from types import SimpleNamespace

import research_prospective_anchor_worker as worker_module


UTC = timezone.utc


class _FakeStore:
    def status(self):
        return {
            "configured": True,
            "telegram_alerts": 0,
            "live_delivery_allowed": False,
        }


class _FakeSampling:
    slot_open_utc = datetime(2026, 8, 29, 12, 0, tzinfo=UTC)

    def __init__(self):
        checked = datetime(2026, 8, 29, 12, 34, 5, tzinfo=UTC)
        expires = datetime(2026, 8, 29, 13, 2, tzinfo=UTC)
        self.batch = SimpleNamespace(
            decisions=tuple(
                SimpleNamespace(
                    symbol=symbol,
                    evaluation_status=(
                        worker_module.research_prospective_anchors.EVALUABLE
                    ),
                    checked_at_utc=checked,
                    expires_at_utc=expires,
                )
                for symbol in ("BTC", "HYPE")
            )
        )
        self.existing_symbols = ()
        self.persisted = tuple(
            SimpleNamespace(symbol=symbol, anchor_slot_id=index + 1)
            for index, symbol in enumerate(("BTC", "HYPE"))
        )
        self.conflicts = ()

    def summary(self):
        return {
            "directional_events": 4,
            "persisted_slots": 2,
            "telegram_alerts": 0,
            "live_delivery_allowed": False,
        }


class _FakeService:
    def __init__(self, store, **kwargs):
        self.store = store
        self.kwargs = kwargs
        self.calls = []

    async def run_once_async(self, **kwargs):
        self.calls.append(kwargs)
        sampling = _FakeSampling()
        sampling.slot_open_utc = kwargs["slot_open_utc"]
        return sampling


class _TimeoutService(_FakeService):
    async def run_once_async(self, **kwargs):
        self.calls.append(kwargs)
        raise RuntimeError("canceling statement due to statement timeout")


class _SequenceService(_FakeService):
    def __init__(self, store, *, samplings, **kwargs):
        super().__init__(store, **kwargs)
        self.samplings = list(samplings)

    async def run_once_async(self, **kwargs):
        self.calls.append(kwargs)
        sampling = self.samplings.pop(0)
        if not getattr(sampling, "preserve_slot_open", False):
            sampling.slot_open_utc = kwargs["slot_open_utc"]
        return sampling


class _StateSampling:
    slot_open_utc = datetime(2026, 8, 29, 12, 0, tzinfo=UTC)

    def __init__(
        self,
        decisions,
        *,
        captured=(),
        conflicts=(),
        existing=(),
        preserve_slot_open=False,
    ):
        captured_symbols = {str(symbol).upper() for symbol in captured}
        conflicted_symbols = {
            str(item).split(":", 1)[0].strip().upper()
            for item in conflicts
            if str(item).strip()
        }
        self.existing_symbols = tuple(existing)
        self.preserve_slot_open = bool(preserve_slot_open)
        self.batch = SimpleNamespace(
            decisions=tuple(SimpleNamespace(**decision) for decision in decisions)
        )
        self.persisted = tuple(
            SimpleNamespace(
                symbol=decision["symbol"],
                anchor_slot_id=(
                    index + 1
                    if str(decision["symbol"]).upper() in captured_symbols
                    else None
                ),
            )
            for index, decision in enumerate(decisions)
            if decision["evaluation_status"]
            != worker_module.research_prospective_anchors.NOT_DUE
            and str(decision["symbol"]).upper() not in conflicted_symbols
        )
        self.conflicts = tuple(conflicts)

    def summary(self):
        persisted_slots = sum(
            1 for item in self.persisted if item.anchor_slot_id is not None
        )
        return {
            "decisions": len(self.batch.decisions),
            "directional_events": persisted_slots * 2,
            "persisted_attempts": len(self.persisted),
            "persisted_slots": persisted_slots,
            "conflicts": list(self.conflicts),
            "telegram_alerts": 0,
            "live_delivery_allowed": False,
        }


def _price_result():
    candle_open = "2026-08-29T12:33:00.000000Z"
    candle_close = "2026-08-29T12:33:59.999000Z"
    fetched = "2026-08-29T12:34:01.000000Z"
    return {
        "ok": True,
        "fallback_used": False,
        "prices": {
            "BTC": {
                "symbol": "BTC",
                "pair": "BTCUSDT",
                "price": 63_500.0,
                "source": "binance_spot",
                "exchange": "binance",
                "market": "spot",
                "instrument": "BTCUSDT",
                "interval": "1m",
                "candle_open_time_utc": candle_open,
                "candle_close_time_utc": candle_close,
                "fetched_at_utc": fetched,
            },
            "HYPE": {
                "symbol": "HYPE",
                "pair": "HYPE/USDT",
                "price": 42.5,
                "source": "hyperliquid",
                "exchange": "hyperliquid",
                "market": "spot",
                "instrument": "@107",
                "interval": "1m",
                "candle_open_time_utc": candle_open,
                "candle_close_time_utc": candle_close,
                "fetched_at_utc": fetched,
            },
        },
        "errors": {"ETH": "Binance official route unavailable"},
    }


def _eth_price_result():
    result = _price_result()
    result["prices"] = {
        "ETH": {
            "symbol": "ETH",
            "pair": "ETHUSDT",
            "price": 3_250.0,
            "source": "binance_spot",
            "exchange": "binance",
            "market": "spot",
            "instrument": "ETHUSDT",
            "interval": "1m",
            "candle_open_time_utc": "2026-08-29T12:34:00.000000Z",
            "candle_close_time_utc": "2026-08-29T12:34:59.999000Z",
            "fetched_at_utc": "2026-08-29T12:35:01.000000Z",
        }
    }
    result["errors"] = {}
    return result


async def _exercise_worker() -> None:
    fixed_now = datetime(2026, 8, 29, 12, 34, 5, tzinfo=UTC)
    observed_calls = []

    def price_fetcher(symbols, *, observed_at_utc):
        observed_calls.append((tuple(symbols), observed_at_utc))
        return _price_result()

    worker = worker_module.ProspectiveAnchorWorker(
        symbols=("BTC", "HYPE"),
        price_fetcher=price_fetcher,
        store_factory=_FakeStore,
        service_factory=_FakeService,
        now_provider=lambda: fixed_now,
    )
    started = await worker.start(schema_ready=True)
    assert started is True
    summary = await worker.run_once(
        scheduled_at_utc=datetime(2026, 8, 29, 12, 34, tzinfo=UTC)
    )
    assert summary["persisted_slots"] == 2
    assert observed_calls == [
        (
            ("BTC", "HYPE"),
            datetime(2026, 8, 29, 12, 34, tzinfo=UTC),
        )
    ]
    assert len(worker._service.calls) == 1
    assert (
        worker._service.kwargs["strategy_version"]
        == "formula-prospective-neutral-v4"
    )
    official = worker._service.calls[0]["official_prices_by_symbol"]
    assert set(official) == {"BTC", "HYPE"}
    assert official["BTC"]["source"] == "binance_spot"
    assert official["BTC"]["price_exchange"] == "binance"
    assert official["BTC"]["price_market"] == "spot"
    assert official["BTC"]["price_pair"] == "BTCUSDT"
    assert official["BTC"]["price_instrument_id"] == "BTCUSDT"
    assert official["BTC"]["price_timeframe"] == "1m"
    assert official["BTC"]["quality_status"] == "PASS"
    assert official["BTC"]["fallback_used"] is False
    assert (
        official["BTC"]["fallback_policy"]
        == "PROVIDER_ATTESTED_NO_FALLBACK"
    )
    assert official["BTC"]["observed_at_utc"].endswith("12:33:59.999000Z")
    assert official["BTC"]["refresh_completed_at_utc"].endswith(
        "12:34:01.000000Z"
    )
    assert official["HYPE"]["source"] == "hyperliquid_spot_@107"
    assert official["HYPE"]["price_instrument_id"] == "@107"
    assert official["HYPE"]["fallback_used"] is False
    assert worker._service.calls[0]["now"] == fixed_now
    assert worker._service.calls[0]["slot_open_utc"] == datetime(
        2026, 8, 29, 12, 0, tzinfo=UTC
    )
    assert worker._service.calls[0]["symbols"] == ("BTC", "HYPE")
    status = worker.status()
    assert status["task_running"] is True
    assert status["last_official_found_symbols"] == ["BTC", "HYPE"]
    assert status["last_official_missing_symbols"] == []
    assert status["last_official_errors"] == {}
    assert status["last_completed_slot_utc"].endswith("12:00:00.000000Z")
    assert status["telegram_delivery_path"] is False
    assert status["live_delivery_allowed"] is False
    await worker.stop()
    assert worker.status()["task_running"] is False


async def _exercise_retry_contract() -> None:
    current_now = [datetime(2026, 8, 29, 12, 34, 5, tzinfo=UTC)]
    expires = datetime(2026, 8, 29, 13, 2, tzinfo=UTC)
    retryable = _StateSampling(
        (
            {
                "symbol": "BTC",
                "evaluation_status": (
                    worker_module.research_prospective_anchors.EVALUABLE
                ),
                "checked_at_utc": current_now[0],
                "expires_at_utc": expires,
            },
            {
                "symbol": "ETH",
                "evaluation_status": (
                    worker_module.research_prospective_anchors.UNEVALUABLE
                ),
                "checked_at_utc": current_now[0],
                "expires_at_utc": expires,
            },
            {
                "symbol": "HYPE",
                "evaluation_status": (
                    worker_module.research_prospective_anchors.COVERAGE_EXCLUDED
                ),
                "checked_at_utc": current_now[0],
                "expires_at_utc": expires,
            },
        ),
        captured=("BTC",),
    )
    recovered = _StateSampling(
        (
            {
                "symbol": "ETH",
                "evaluation_status": (
                    worker_module.research_prospective_anchors.EVALUABLE
                ),
                "checked_at_utc": datetime(
                    2026, 8, 29, 12, 35, 5, tzinfo=UTC
                ),
                "expires_at_utc": expires,
            },
        ),
        captured=("ETH",),
    )

    def service_factory(store, **kwargs):
        return _SequenceService(
            store,
            samplings=(retryable, recovered),
            **kwargs,
        )

    price_calls = []

    def price_fetcher(symbols, *, observed_at_utc):
        price_calls.append((tuple(symbols), observed_at_utc))
        return _price_result() if len(price_calls) == 1 else _eth_price_result()

    worker = worker_module.ProspectiveAnchorWorker(
        symbols=("BTC", "ETH", "HYPE"),
        price_fetcher=price_fetcher,
        store_factory=_FakeStore,
        service_factory=service_factory,
        now_provider=lambda: current_now[0],
    )
    assert await worker.start(schema_ready=True) is True

    assert await worker._run_due_slot(
        datetime(2026, 8, 29, 12, 34, tzinfo=UTC)
    ) is True
    first = worker.status()["last_sampling_summary"]
    assert first["run_symbols"] == ["BTC", "ETH", "HYPE"]
    assert first["persisted_slots"] == 1
    assert first["retry_pending_symbols"] == ["ETH"]
    status = worker.status()
    assert status["status"] == "retry_pending"
    assert status["last_attempted_slot_utc"].endswith("12:00:00.000000Z")
    assert status["last_completed_slot_utc"] is None
    assert status["retry_pending_symbols"] == ["ETH"]

    current_now[0] = datetime(2026, 8, 29, 12, 35, 5, tzinfo=UTC)
    assert await worker._run_due_slot(
        datetime(2026, 8, 29, 12, 35, tzinfo=UTC)
    ) is True
    second = worker.status()["last_sampling_summary"]
    assert second["run_symbols"] == ["ETH"]
    assert second["retry_pending_symbols"] == []
    status = worker.status()
    assert status["status"] == "completed"
    assert status["last_completed_slot_utc"].endswith("12:00:00.000000Z")
    assert status["retry_pending_symbols"] == []

    current_now[0] = datetime(2026, 8, 29, 12, 36, 5, tzinfo=UTC)
    assert await worker._run_due_slot(
        datetime(2026, 8, 29, 12, 36, tzinfo=UTC)
    ) is False
    assert price_calls == [
        (
            ("BTC", "ETH", "HYPE"),
            datetime(2026, 8, 29, 12, 34, tzinfo=UTC),
        ),
        (
            ("ETH",),
            datetime(2026, 8, 29, 12, 35, tzinfo=UTC),
        ),
    ]
    assert [call["symbols"] for call in worker._service.calls] == [
        ("BTC", "ETH", "HYPE"),
        ("ETH",),
    ]
    assert [call["slot_open_utc"] for call in worker._service.calls] == [
        datetime(2026, 8, 29, 12, 0, tzinfo=UTC),
        datetime(2026, 8, 29, 12, 0, tzinfo=UTC),
    ]
    assert set(worker._service.calls[0]["official_prices_by_symbol"]) == {
        "BTC",
        "HYPE",
    }
    assert set(worker._service.calls[1]["official_prices_by_symbol"]) == {
        "ETH"
    }
    await worker.stop()


async def _exercise_slot_rollover() -> None:
    current_now = [datetime(2026, 8, 29, 13, 1, 5, tzinfo=UTC)]
    expires = datetime(2026, 8, 29, 13, 2, tzinfo=UTC)
    first = _StateSampling(
        (
            {
                "symbol": "BTC",
                "evaluation_status": (
                    worker_module.research_prospective_anchors.EVALUABLE
                ),
                "checked_at_utc": current_now[0],
                "expires_at_utc": expires,
            },
            {
                "symbol": "ETH",
                "evaluation_status": (
                    worker_module.research_prospective_anchors.UNEVALUABLE
                ),
                "checked_at_utc": current_now[0],
                "expires_at_utc": expires,
            },
        ),
        captured=("BTC",),
    )
    second = _StateSampling(
        (
            {
                "symbol": "ETH",
                "evaluation_status": (
                    worker_module.research_prospective_anchors.UNEVALUABLE
                ),
                "checked_at_utc": expires,
                "expires_at_utc": expires,
            },
        )
    )
    third = _StateSampling(
        (
            {
                "symbol": symbol,
                "evaluation_status": (
                    worker_module.research_prospective_anchors.EVALUABLE
                ),
                "checked_at_utc": datetime(
                    2026, 8, 29, 13, 3, 5, tzinfo=UTC
                ),
                "expires_at_utc": datetime(
                    2026, 8, 29, 13, 32, tzinfo=UTC
                ),
            }
            for symbol in ("BTC", "ETH")
        ),
        captured=("BTC", "ETH"),
    )

    def service_factory(store, **kwargs):
        return _SequenceService(store, samplings=(first, second, third), **kwargs)

    def crossing_fetcher(symbols, *, observed_at_utc):
        current_now[0] = (
            datetime(2026, 8, 29, 13, 2, 5, tzinfo=UTC)
            if observed_at_utc.minute == 1
            else datetime(2026, 8, 29, 13, 3, 5, tzinfo=UTC)
        )
        return _price_result()

    worker = worker_module.ProspectiveAnchorWorker(
        symbols=("BTC", "ETH"),
        price_fetcher=crossing_fetcher,
        store_factory=_FakeStore,
        service_factory=service_factory,
        now_provider=lambda: current_now[0],
    )
    assert await worker.start(schema_ready=True) is True
    assert await worker._run_due_slot(
        datetime(2026, 8, 29, 13, 1, tzinfo=UTC)
    )
    assert worker.status()["retry_pending_symbols"] == ["ETH"]
    current_now[0] = datetime(2026, 8, 29, 13, 2, 5, tzinfo=UTC)
    assert await worker._run_due_slot(
        datetime(2026, 8, 29, 13, 2, tzinfo=UTC)
    )
    assert worker.status()["last_completed_slot_utc"].endswith(
        "12:00:00.000000Z"
    )
    current_now[0] = datetime(2026, 8, 29, 13, 3, 5, tzinfo=UTC)
    assert await worker._run_due_slot(
        datetime(2026, 8, 29, 13, 3, tzinfo=UTC)
    )
    assert [call["slot_open_utc"] for call in worker._service.calls] == [
        datetime(2026, 8, 29, 12, 0, tzinfo=UTC),
        datetime(2026, 8, 29, 12, 0, tzinfo=UTC),
        datetime(2026, 8, 29, 12, 30, tzinfo=UTC),
    ]
    assert [call["symbols"] for call in worker._service.calls] == [
        ("BTC", "ETH"),
        ("ETH",),
        ("BTC", "ETH"),
    ]
    await worker.stop()

    wrong_slot = _StateSampling(
        (
            {
                "symbol": "HYPE",
                "evaluation_status": (
                    worker_module.research_prospective_anchors.COVERAGE_EXCLUDED
                ),
                "checked_at_utc": current_now[0],
                "expires_at_utc": datetime(
                    2026, 8, 29, 13, 32, tzinfo=UTC
                ),
            },
        ),
        preserve_slot_open=True,
    )

    def wrong_service_factory(store, **kwargs):
        return _SequenceService(store, samplings=(wrong_slot,), **kwargs)

    wrong_worker = worker_module.ProspectiveAnchorWorker(
        symbols=("HYPE",),
        price_fetcher=lambda symbols, **kwargs: _price_result(),
        store_factory=_FakeStore,
        service_factory=wrong_service_factory,
        now_provider=lambda: current_now[0],
    )
    assert await wrong_worker.start(schema_ready=True) is True
    try:
        await wrong_worker.run_once(
            scheduled_at_utc=datetime(2026, 8, 29, 13, 3, tzinfo=UTC)
        )
    except RuntimeError as exc:
        assert "unexpected slot" in str(exc)
    else:
        raise AssertionError("wrong prospective slot was accepted")
    await wrong_worker.stop()


async def _exercise_conflict_block() -> None:
    current_now = [datetime(2026, 8, 29, 12, 34, 5, tzinfo=UTC)]
    checked = current_now[0]
    expires = datetime(2026, 8, 29, 13, 2, tzinfo=UTC)
    conflict = _StateSampling(
        (
            {
                "symbol": "BTC",
                "evaluation_status": (
                    worker_module.research_prospective_anchors.EVALUABLE
                ),
                "checked_at_utc": checked,
                "expires_at_utc": expires,
            },
            {
                "symbol": "ETH",
                "evaluation_status": (
                    worker_module.research_prospective_anchors.UNEVALUABLE
                ),
                "checked_at_utc": checked,
                "expires_at_utc": expires,
            },
        ),
        conflicts=("BTC:frozen slot conflict",),
    )
    recovered = _StateSampling(
        (
            {
                "symbol": "ETH",
                "evaluation_status": (
                    worker_module.research_prospective_anchors.EVALUABLE
                ),
                "checked_at_utc": datetime(
                    2026, 8, 29, 12, 35, 5, tzinfo=UTC
                ),
                "expires_at_utc": expires,
            },
        ),
        captured=("ETH",),
    )
    next_slot = _StateSampling(
        (
            {
                "symbol": symbol,
                "evaluation_status": (
                    worker_module.research_prospective_anchors.EVALUABLE
                ),
                "checked_at_utc": datetime(
                    2026, 8, 29, 13, 3, 5, tzinfo=UTC
                ),
                "expires_at_utc": datetime(
                    2026, 8, 29, 13, 32, tzinfo=UTC
                ),
            }
            for symbol in ("BTC", "ETH")
        ),
        captured=("BTC", "ETH"),
    )

    def service_factory(store, **kwargs):
        return _SequenceService(
            store,
            samplings=(conflict, recovered, next_slot),
            **kwargs,
        )

    worker = worker_module.ProspectiveAnchorWorker(
        symbols=("BTC", "ETH"),
        price_fetcher=lambda symbols, **kwargs: _price_result(),
        store_factory=_FakeStore,
        service_factory=service_factory,
        now_provider=lambda: current_now[0],
    )
    assert await worker.start(schema_ready=True) is True
    run_at = datetime(2026, 8, 29, 12, 34, tzinfo=UTC)
    assert await worker._run_due_slot(run_at) is True
    status = worker.status()
    assert status["status"] == "retry_pending_with_conflicts"
    assert status["last_completed_slot_utc"] is None
    assert status["last_blocked_slot_utc"] is None
    assert status["active_conflict_slot_utc"].endswith("12:00:00.000000Z")
    assert status["active_conflict_symbols"] == ["BTC"]
    assert status["retry_pending_symbols"] == ["ETH"]
    assert status["last_conflicts"] == ["BTC:frozen slot conflict"]
    assert status["cycles_failed"] == 1
    current_now[0] = datetime(2026, 8, 29, 12, 35, 5, tzinfo=UTC)
    assert await worker._run_due_slot(
        datetime(2026, 8, 29, 12, 35, tzinfo=UTC)
    ) is True
    status = worker.status()
    assert status["status"] == "blocked_conflict"
    assert status["last_completed_slot_utc"] is None
    assert status["last_blocked_slot_utc"].endswith("12:00:00.000000Z")
    assert status["active_conflict_slot_utc"] is None
    assert status["active_conflict_symbols"] == []
    assert status["retry_pending_symbols"] == []
    assert status["last_conflicts"] == ["BTC:frozen slot conflict"]
    assert status["cycles_started"] == 2
    assert status["cycles_completed"] == 1
    assert status["cycles_failed"] == 1
    assert await worker._run_due_slot(
        datetime(2026, 8, 29, 12, 36, tzinfo=UTC)
    ) is False
    current_now[0] = datetime(2026, 8, 29, 13, 3, 5, tzinfo=UTC)
    assert await worker._run_due_slot(
        datetime(2026, 8, 29, 13, 3, tzinfo=UTC)
    ) is True
    assert [call["symbols"] for call in worker._service.calls] == [
        ("BTC", "ETH"),
        ("ETH",),
        ("BTC", "ETH"),
    ]
    assert [call["slot_open_utc"] for call in worker._service.calls] == [
        datetime(2026, 8, 29, 12, 0, tzinfo=UTC),
        datetime(2026, 8, 29, 12, 0, tzinfo=UTC),
        datetime(2026, 8, 29, 12, 30, tzinfo=UTC),
    ]
    status = worker.status()
    assert status["cycles_started"] == 3
    assert status["cycles_completed"] == 2
    assert status["cycles_failed"] == 1
    await worker.stop()


async def _exercise_start_guards() -> None:
    worker = worker_module.ProspectiveAnchorWorker(
        store_factory=_FakeStore,
        service_factory=_FakeService,
    )
    assert await worker.start(schema_ready=False) is False
    assert worker.status()["status"] == "blocked_schema"
    await worker.stop()


async def _exercise_timeout_telemetry() -> None:
    fixed_now = datetime(2026, 8, 29, 12, 34, 5, tzinfo=UTC)
    worker = worker_module.ProspectiveAnchorWorker(
        symbols=("BTC", "HYPE"),
        price_fetcher=lambda symbols, **kwargs: _price_result(),
        store_factory=_FakeStore,
        service_factory=_TimeoutService,
        now_provider=lambda: fixed_now,
    )
    assert await worker.start(schema_ready=True) is True
    try:
        await worker.run_once(
            scheduled_at_utc=datetime(2026, 8, 29, 12, 34, tzinfo=UTC)
        )
    except RuntimeError as exc:
        assert "statement timeout" in str(exc)
    else:
        raise AssertionError("anchor timeout probe must preserve the failure")
    status = worker.status()
    assert status["last_error_phase"] == "PERSISTING"
    assert status["last_timeout_phase"] == "PERSISTING"
    assert status["last_phase_duration_ms"] is not None
    await worker.stop()


def run() -> None:
    previous = os.environ.get(worker_module.ENV_ENABLED)
    try:
        os.environ[worker_module.ENV_ENABLED] = "1"
        assert worker_module.next_minute_boundary(
            datetime(2026, 8, 29, 12, 34, 0, tzinfo=UTC)
        ) == datetime(2026, 8, 29, 12, 35, 0, tzinfo=UTC)

        rows, errors = worker_module.official_anchor_rows(
            _price_result(), symbols=("BTC", "ETH", "HYPE")
        )
        assert set(rows) == {"BTC", "HYPE"}
        assert set(errors) == {"ETH"}

        bad_hype = _price_result()
        bad_hype["prices"]["HYPE"]["instrument"] = "HYPE"
        rows, errors = worker_module.official_anchor_rows(
            bad_hype, symbols=("BTC", "HYPE")
        )
        assert set(rows) == {"BTC"}
        assert "HYPE" in errors

        fallback = _price_result()
        fallback["fallback_used"] = True
        try:
            worker_module.official_anchor_rows(
                fallback, symbols=("BTC", "HYPE")
            )
        except ValueError as exc:
            assert "fallback" in str(exc).lower()
        else:
            raise AssertionError("fallback result was accepted")

        expires = datetime(2026, 8, 29, 13, 2, tzinfo=UTC)
        expired = _StateSampling(
            (
                {
                    "symbol": "ETH",
                    "evaluation_status": (
                        worker_module.research_prospective_anchors.UNEVALUABLE
                    ),
                    "checked_at_utc": expires,
                    "expires_at_utc": expires,
                },
            )
        )
        excluded = _StateSampling(
            (
                {
                    "symbol": "HYPE",
                    "evaluation_status": (
                        worker_module.research_prospective_anchors.COVERAGE_EXCLUDED
                    ),
                    "checked_at_utc": datetime(
                        2026, 8, 29, 12, 34, tzinfo=UTC
                    ),
                    "expires_at_utc": expires,
                },
            )
        )
        conflict = _StateSampling(
            (
                {
                    "symbol": "BTC",
                    "evaluation_status": (
                        worker_module.research_prospective_anchors.EVALUABLE
                    ),
                    "checked_at_utc": datetime(
                        2026, 8, 29, 12, 34, tzinfo=UTC
                    ),
                    "expires_at_utc": expires,
                },
            ),
            conflicts=("BTC:frozen slot conflict",),
        )
        empty = SimpleNamespace(
            batch=SimpleNamespace(decisions=()),
            existing_symbols=(),
            persisted=(),
            conflicts=(),
        )
        assert worker_module._retry_pending_symbols(
            expired, expected_symbols=("ETH",)
        ) == []
        assert worker_module._retry_pending_symbols(
            excluded, expected_symbols=("HYPE",)
        ) == []
        assert worker_module._retry_pending_symbols(
            conflict, expected_symbols=("BTC",)
        ) == []
        assert worker_module._retry_pending_symbols(
            empty, expected_symbols=("BTC", "ETH")
        ) == ["BTC", "ETH"]

        asyncio.run(_exercise_worker())
        asyncio.run(_exercise_retry_contract())
        asyncio.run(_exercise_slot_rollover())
        asyncio.run(_exercise_conflict_block())
        asyncio.run(_exercise_start_guards())
        asyncio.run(_exercise_timeout_telemetry())

        os.environ[worker_module.ENV_ENABLED] = "0"
        disabled = worker_module.ProspectiveAnchorWorker(
            store_factory=_FakeStore,
            service_factory=_FakeService,
        )
        assert asyncio.run(disabled.start(schema_ready=True)) is False
        assert disabled.status()["status"] == "off"

        module_text = Path("research_prospective_anchor_worker.py").read_text()
        assert "import ai_telegram" not in module_text
        assert "from telegram" not in module_text
        assert "fetch_research_spot_1m_prices" in module_text
        main_text = Path("main.py").read_text()
        assert '"prospective_anchors":' in main_text
        assert "WORKER.start(" in main_text
        assert "WORKER.stop()" in main_text
    finally:
        if previous is None:
            os.environ.pop(worker_module.ENV_ENABLED, None)
        else:
            os.environ[worker_module.ENV_ENABLED] = previous
    print("research_prospective_anchor_worker_selftest: PASS")


if __name__ == "__main__":
    run()
