"""Opt-in production scheduler for silent prospective Formula anchors.

The worker wakes on UTC minute boundaries and delegates one idempotent 30m
slot pass to :class:`ProspectiveAnchorService`.  Its price input is restricted
to ``live_price_provider.fetch_research_spot_1m_prices``: Binance Spot USDT
closed 1m candles for every non-HYPE symbol and Hyperliquid HYPE/USDT Spot
``@107`` closed 1m candles for HYPE.  Failed routes remain missing.

This module has no Telegram import or delivery surface.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
import math
import os
from typing import Any, Callable, Dict, Mapping, Optional, Sequence

import live_price_provider
import research_prospective_anchor_store
import research_prospective_anchors


ENV_ENABLED = "PROSPECTIVE_ANCHORS_ENABLED"
SCHEDULER_VERSION = "prospective-anchor-minute-scheduler-v1"
_TRUE = {"1", "true", "yes", "on"}


def _utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(value: Any) -> str:
    return _utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _enabled() -> bool:
    return os.getenv(ENV_ENABLED, "").strip().lower() in _TRUE


def next_minute_boundary(now: Any) -> datetime:
    current = _utc(now)
    return current.replace(second=0, microsecond=0) + timedelta(minutes=1)


def _normalized_pair(value: Any) -> str:
    return "".join(
        character
        for character in str(value or "").strip().upper()
        if character.isalnum()
    )


def _official_anchor_row(
    symbol: str, raw: Mapping[str, Any]
) -> Dict[str, Any]:
    """Validate and translate one canonical closed-1m provider result."""
    normalized = str(symbol or "").strip().upper()
    row_symbol = str(raw.get("symbol") or "").strip().upper()
    if row_symbol != normalized:
        raise ValueError("official price symbol mismatch")
    price = float(raw.get("price"))
    if not math.isfinite(price) or price <= 0.0:
        raise ValueError("official price is non-positive")
    exchange = str(raw.get("exchange") or "").strip().lower()
    market = str(raw.get("market") or "").strip().lower()
    pair = _normalized_pair(raw.get("pair"))
    interval = str(raw.get("interval") or "").strip().lower()
    instrument = str(raw.get("instrument") or "").strip().upper()
    upstream_source = str(raw.get("source") or "").strip().lower()
    observed_at = _utc(raw.get("candle_close_time_utc"))
    fetched_at = _utc(raw.get("fetched_at_utc"))
    candle_open = _utc(raw.get("candle_open_time_utc"))
    if not candle_open < observed_at <= fetched_at:
        raise ValueError("official candle/fetch timestamps are incoherent")
    if market != "spot" or interval != "1m":
        raise ValueError("official price is not closed Spot 1m")
    if normalized == "HYPE":
        if (
            exchange != "hyperliquid"
            or pair != "HYPEUSDT"
            or instrument != "@107"
            or upstream_source != "hyperliquid"
        ):
            raise ValueError("HYPE official route is not Hyperliquid Spot @107")
        source = "hyperliquid_spot_@107"
        instrument_id = "@107"
    else:
        expected_pair = f"{normalized}USDT"
        if (
            exchange != "binance"
            or pair != expected_pair
            or instrument != expected_pair
            or upstream_source != "binance_spot"
        ):
            raise ValueError("official route is not Binance Spot USDT")
        source = "binance_spot"
        instrument_id = instrument
    return {
        "symbol": normalized,
        "observed_at_utc": _iso(observed_at),
        "refresh_completed_at_utc": _iso(fetched_at),
        "source": source,
        "upstream_source": upstream_source,
        "quality_status": "PASS",
        "price_exchange": exchange,
        "price_market": market,
        "price_pair": str(raw.get("pair") or "").strip().upper(),
        "price_instrument_id": instrument_id,
        "price_timeframe": "1m",
        "price": price,
    }


def official_anchor_rows(
    result: Mapping[str, Any], *, symbols: Sequence[str]
) -> tuple[Dict[str, Dict[str, Any]], Dict[str, str]]:
    """Return only verified official rows; never substitute a failed symbol."""
    requested = tuple(
        dict.fromkeys(str(item or "").strip().upper() for item in symbols if item)
    )
    if result.get("fallback_used") is not False:
        raise ValueError("Research official price provider reported a fallback")
    raw_prices = result.get("prices")
    if not isinstance(raw_prices, Mapping):
        raw_prices = {}
    rows: Dict[str, Dict[str, Any]] = {}
    raw_errors = result.get("errors")
    errors: Dict[str, str] = (
        {
            str(symbol).strip().upper(): str(error)
            for symbol, error in raw_errors.items()
            if str(symbol).strip().upper() in requested
        }
        if isinstance(raw_errors, Mapping)
        else {}
    )
    for symbol in requested:
        raw = raw_prices.get(symbol)
        if not isinstance(raw, Mapping):
            errors.setdefault(symbol, "official closed Spot 1m price missing")
            continue
        try:
            rows[symbol] = {
                **_official_anchor_row(symbol, raw),
                # ``fallback_used`` is accepted only after the provider-level
                # assertion above has proved that no route was substituted.
                "fallback_used": False,
                "fallback_policy": "PROVIDER_ATTESTED_NO_FALLBACK",
            }
            errors.pop(symbol, None)
        except Exception as exc:
            errors[symbol] = f"{type(exc).__name__}: {exc}"
    return rows, errors


class ProspectiveAnchorWorker:
    def __init__(
        self,
        *,
        symbols: Sequence[str] = research_prospective_anchor_store.DEFAULT_SYMBOLS,
        price_fetcher: Callable[..., Mapping[str, Any]] = (
            live_price_provider.fetch_research_spot_1m_prices
        ),
        store_factory: Callable[[], Any] = (
            research_prospective_anchor_store.ProspectiveAnchorStore
        ),
        service_factory: Callable[..., Any] = (
            research_prospective_anchor_store.ProspectiveAnchorService
        ),
        now_provider: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self.symbols = tuple(
            dict.fromkeys(str(item or "").strip().upper() for item in symbols if item)
        )
        self._price_fetcher = price_fetcher
        self._store_factory = store_factory
        self._service_factory = service_factory
        self._now = now_provider
        self._task: Optional[asyncio.Task] = None
        self._store: Any = None
        self._service: Any = None
        self._schema_ready = False
        self._runtime: Dict[str, Any] = {}
        self._reset_runtime()

    def _reset_runtime(self) -> None:
        self._runtime = {
            "enabled": _enabled(),
            "running": False,
            "status": "off",
            "schema_ready": False,
            "started_at_utc": None,
            "stopped_at_utc": None,
            "next_run_utc": None,
            "last_started_at_utc": None,
            "last_completed_at_utc": None,
            "last_completed_slot_utc": None,
            "last_error": None,
            "cycles_started": 0,
            "cycles_completed": 0,
            "cycles_failed": 0,
            "last_official_found_symbols": [],
            "last_official_missing_symbols": [],
            "last_official_errors": {},
            "last_sampling_summary": None,
        }

    def status(self) -> Dict[str, Any]:
        value = dict(self._runtime)
        value.update(
            {
                "enabled": _enabled(),
                "task_running": bool(
                    self._task is not None and not self._task.done()
                ),
                "scheduler_version": SCHEDULER_VERSION,
                "cadence_seconds": 60,
                "slot_interval_minutes": 30,
                "symbols": list(self.symbols),
                "price_policy": (
                    "Binance Spot USDT closed 1m; HYPE Hyperliquid "
                    "HYPE/USDT Spot @107 closed 1m; no fallback"
                ),
                "telegram_delivery_path": False,
                "live_delivery_allowed": False,
                "store": self._store.status() if self._store is not None else None,
            }
        )
        return json.loads(json.dumps(value, default=str))

    async def start(self, *, schema_ready: bool) -> bool:
        self._schema_ready = bool(schema_ready)
        self._runtime["enabled"] = _enabled()
        self._runtime["schema_ready"] = self._schema_ready
        if not _enabled():
            self._runtime["status"] = "off"
            return False
        if not self._schema_ready:
            self._runtime["status"] = "blocked_schema"
            return False
        if self._task is not None and not self._task.done():
            return True
        try:
            self._store = self._store_factory()
            store_status = self._store.status()
            if not bool(store_status.get("configured")):
                self._runtime["status"] = "blocked_database"
                self._runtime["last_error"] = (
                    "prospective anchor research database is not configured"
                )
                return False
            self._service = self._service_factory(
                self._store,
                symbols=self.symbols,
                strategy_version="formula-prospective-neutral-v4",
                code_version=(
                    os.getenv("RENDER_GIT_COMMIT")
                    or os.getenv("GITHUB_SHA")
                    or "candidate-unknown"
                ),
            )
        except Exception as exc:
            self._runtime["status"] = "startup_failed"
            self._runtime["last_error"] = repr(exc)
            return False
        now = self._now()
        self._runtime.update(
            {
                "running": True,
                "status": "starting",
                "started_at_utc": _iso(now),
                "stopped_at_utc": None,
                "last_error": None,
            }
        )
        self._task = asyncio.create_task(
            self._run_loop(), name="research-prospective-anchor-worker"
        )
        return True

    async def run_once(self, *, scheduled_at_utc: Any = None) -> Dict[str, Any]:
        if self._service is None:
            raise RuntimeError("prospective anchor service is not initialized")
        scheduled = _utc(scheduled_at_utc or self._now())
        self._runtime.update(
            {
                "status": "fetching_official_prices",
                "last_started_at_utc": _iso(self._now()),
                "last_error": None,
                "cycles_started": int(self._runtime["cycles_started"]) + 1,
            }
        )
        result = await asyncio.to_thread(
            self._price_fetcher,
            self.symbols,
            observed_at_utc=scheduled,
        )
        official_rows, official_errors = official_anchor_rows(
            result, symbols=self.symbols
        )
        checked_at = self._now()
        self._runtime.update(
            {
                "status": "persisting",
                "last_official_found_symbols": sorted(official_rows),
                "last_official_missing_symbols": sorted(
                    set(self.symbols) - set(official_rows)
                ),
                "last_official_errors": official_errors,
            }
        )
        sampling = await self._service.run_once_async(
            now=checked_at,
            official_prices_by_symbol=official_rows,
        )
        summary = sampling.summary()
        completed_slot = getattr(sampling, "slot_open_utc", None)
        if completed_slot is None:
            completed_slot = summary.get("slot_open_utc")
        self._runtime.update(
            {
                "status": "completed",
                "last_completed_at_utc": _iso(self._now()),
                "last_completed_slot_utc": (
                    _iso(completed_slot) if completed_slot is not None else None
                ),
                "cycles_completed": int(self._runtime["cycles_completed"]) + 1,
                "last_sampling_summary": summary,
                "last_error": None,
            }
        )
        return summary

    async def _run_loop(self) -> None:
        try:
            while True:
                run_at = next_minute_boundary(self._now())
                self._runtime.update(
                    {"status": "waiting", "next_run_utc": _iso(run_at)}
                )
                delay = max(0.0, (run_at - self._now()).total_seconds())
                await asyncio.sleep(delay)
                due_slot = research_prospective_anchors.latest_due_slot_open(run_at)
                if self._runtime.get("last_completed_slot_utc") == _iso(due_slot):
                    # One completed audit pass per 30m slot is enough.  The
                    # minute loop exists to retry an exceptional failed pass,
                    # not to append a fresh ineligible attempt every minute.
                    continue
                try:
                    await self.run_once(scheduled_at_utc=run_at)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._runtime.update(
                        {
                            "status": "failed",
                            "last_completed_at_utc": _iso(self._now()),
                            "last_error": repr(exc),
                            "cycles_failed": int(
                                self._runtime["cycles_failed"]
                            )
                            + 1,
                        }
                    )
                    print(
                        f"[prospective-anchors] minute cycle failed: {exc!r}",
                        flush=True,
                    )
        except asyncio.CancelledError:
            self._runtime["status"] = "cancelled"
            raise
        finally:
            self._runtime.update(
                {
                    "running": False,
                    "next_run_utc": None,
                    "stopped_at_utc": _iso(self._now()),
                }
            )

    async def stop(self) -> None:
        task = self._task
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._runtime["running"] = False
        self._runtime["next_run_utc"] = None
        if self._runtime.get("status") != "cancelled":
            self._runtime["status"] = "stopped"
            self._runtime["stopped_at_utc"] = _iso(self._now())


WORKER = ProspectiveAnchorWorker()
