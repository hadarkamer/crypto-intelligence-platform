"""Deterministic self-test for the prospective-anchor minute worker."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import os
from pathlib import Path

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
        return _FakeSampling()


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


async def _exercise_worker() -> None:
    fixed_now = datetime(2026, 8, 29, 12, 34, 5, tzinfo=UTC)
    observed_calls = []

    def price_fetcher(symbols, *, observed_at_utc):
        observed_calls.append((tuple(symbols), observed_at_utc))
        return _price_result()

    worker = worker_module.ProspectiveAnchorWorker(
        symbols=("BTC", "ETH", "HYPE"),
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
            ("BTC", "ETH", "HYPE"),
            datetime(2026, 8, 29, 12, 34, tzinfo=UTC),
        )
    ]
    assert len(worker._service.calls) == 1
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
    status = worker.status()
    assert status["task_running"] is True
    assert status["last_official_found_symbols"] == ["BTC", "HYPE"]
    assert status["last_official_missing_symbols"] == ["ETH"]
    assert "ETH" in status["last_official_errors"]
    assert status["last_completed_slot_utc"].endswith("12:00:00.000000Z")
    assert status["telegram_delivery_path"] is False
    assert status["live_delivery_allowed"] is False
    await worker.stop()
    assert worker.status()["task_running"] is False


async def _exercise_start_guards() -> None:
    worker = worker_module.ProspectiveAnchorWorker(
        store_factory=_FakeStore,
        service_factory=_FakeService,
    )
    assert await worker.start(schema_ready=False) is False
    assert worker.status()["status"] == "blocked_schema"
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

        asyncio.run(_exercise_worker())
        asyncio.run(_exercise_start_guards())

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
