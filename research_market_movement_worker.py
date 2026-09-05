"""Opt-in runtime collector for neutral-price Market Movement Wave v5.

The collector is deliberately independent from Formula, alert delivery and
trading.  It retries unresolved symbols on UTC minute boundaries within the
active ``:02/:32`` eligibility window.  Every price lookup remains a lazy,
single-symbol callback passed to :class:`MarketMovementStore`, preserving the
store's locked read-before-provider guarantee on exact retries.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
import math
import os
from typing import Any, Callable, Mapping, Optional, Sequence

import canonical_price_path
import research_market_movement as movement
import research_market_movement_store


ENV_ENABLED = "RESEARCH_MARKET_MOVEMENT_ENABLED"
SCHEDULER_VERSION = "market-movement-anchor-minute-scheduler-v1"
DEFAULT_SYMBOLS: tuple[str, ...] = (
    "BTC",
    "ETH",
    "SOL",
    "BNB",
    "DOGE",
    "XRP",
    "ZEC",
    "HYPE",
)
_TRUE = {"1", "true", "yes", "on"}


def _utc(value: Any, *, field: str = "timestamp") -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{field} must be an ISO-8601 timestamp") from exc
    else:
        raise ValueError(f"{field} must be a timezone-aware timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _iso(value: Any, *, field: str = "timestamp") -> str:
    return (
        _utc(value, field=field)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _enabled() -> bool:
    return os.getenv(ENV_ENABLED, "").strip().lower() in _TRUE


def next_minute_boundary(now: Any) -> datetime:
    current = _utc(now)
    return current.replace(second=0, microsecond=0) + timedelta(minutes=1)


def latest_eligible_at(now: Any) -> datetime:
    """Return the active ``:02/:32`` eligibility for one UTC instant."""

    current = _utc(now)
    shifted = current - timedelta(minutes=2)
    floored = shifted.replace(
        minute=(shifted.minute // movement.INTERVAL_MINUTES)
        * movement.INTERVAL_MINUTES,
        second=0,
        microsecond=0,
    )
    return floored + timedelta(minutes=2)


def capture_window_open(now: Any, eligible_at_utc: Any) -> bool:
    current = _utc(now)
    eligible = movement._eligibility(eligible_at_utc)
    return bool(
        eligible
        <= current
        < eligible + timedelta(minutes=movement.CAPTURE_WINDOW_MINUTES)
    )


def _normalized_pair(value: Any) -> str:
    return "".join(
        character
        for character in str(value or "").strip().upper()
        if character.isalnum()
    )


def _candle_value(candle: Any, name: str) -> Any:
    if isinstance(candle, Mapping):
        return candle.get(name)
    return getattr(candle, name, None)


def official_provider_payload(
    symbol: Any,
    result: Mapping[str, Any],
    *,
    eligible_at_utc: Any,
    refresh_completed_at_utc: Any,
) -> dict[str, Any]:
    """Translate one exact official provider row into the Wave v5 contract."""

    normalized = movement._symbol(symbol)
    eligible = movement._eligibility(eligible_at_utc)
    if not isinstance(result, Mapping):
        raise ValueError("official price provider returned no result")
    if "fallback_used" in result and result.get("fallback_used") is not False:
        raise ValueError("canonical price path reported a fallback")
    if str(result.get("symbol") or "").strip().upper() != normalized:
        raise ValueError("official price symbol mismatch")
    route = canonical_price_path.validated_route(
        normalized, dict(result), require_complete=True
    )
    candles = list(result.get("candles") or ())
    if type(result.get("expected_candles")) is not int:
        raise ValueError("canonical price path expected_candles is not an integer")
    if result.get("expected_candles") != 1 or len(candles) != 1:
        raise ValueError("canonical price path must contain exactly one target candle")
    raw = candles[0]

    raw_price = _candle_value(raw, "close")
    if isinstance(raw_price, bool):
        raise ValueError("official price is not numeric")
    try:
        price = float(raw_price)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("official price is not numeric") from exc
    if not math.isfinite(price) or price <= 0.0:
        raise ValueError("official price must be finite and positive")

    opened = _utc(
        _candle_value(raw, "open_time_utc"),
        field="open_time_utc",
    )
    closed = _utc(
        _candle_value(raw, "close_time_utc"),
        field="close_time_utc",
    )
    refreshed = _utc(
        refresh_completed_at_utc, field="refresh_completed_at_utc"
    )
    if opened != eligible - timedelta(minutes=1):
        raise ValueError("official candle open is not eligibility minus one minute")
    if not eligible - timedelta(seconds=1) <= closed < eligible:
        raise ValueError("official candle close is not in the final eligibility second")
    if not (
        eligible
        <= refreshed
        < eligible + timedelta(minutes=movement.CAPTURE_WINDOW_MINUTES)
    ):
        raise ValueError("official price refresh is outside the capture window")

    exchange = str(route.get("exchange") or "").strip().lower()
    market = str(route.get("market") or "").strip().lower()
    pair = str(route.get("pair") or "").strip().upper()
    interval = str(route.get("interval") or "").strip().lower()
    instrument = str(route.get("instrument") or "").strip().upper()
    if market != "spot" or interval != "1m":
        raise ValueError("official price is not closed Spot 1m")
    if normalized == "HYPE":
        if (
            exchange != "hyperliquid"
            or _normalized_pair(pair) != "HYPEUSDT"
            or instrument != "@107"
        ):
            raise ValueError("HYPE official route is not Hyperliquid Spot @107")
        source = "hyperliquid_spot_@107"
        upstream_source = "hyperliquid"
        instrument_id = "@107"
    else:
        expected_pair = f"{normalized}USDT"
        if (
            exchange != "binance"
            or _normalized_pair(pair) != expected_pair
            or instrument
        ):
            raise ValueError(
                f"{normalized} official route is not Binance Spot {expected_pair}"
            )
        source = "binance_spot"
        upstream_source = "binance_spot"
        instrument_id = expected_pair

    return {
        "price_candle": {
            "open_time_utc": _iso(opened),
            "close_time_utc": _iso(closed),
            "observed_at_utc": _iso(closed),
            "refresh_completed_at_utc": _iso(refreshed),
            "price": price,
        },
        "source_provenance": {
            "source": source,
            "upstream_source": upstream_source,
            "quality_status": "PASS",
            "price_exchange": exchange,
            "price_market": market,
            "price_pair": pair,
            "price_instrument_id": instrument_id,
            "price_timeframe": "1m",
            "fallback_used": False,
            "fallback_policy": "PROVIDER_ATTESTED_NO_FALLBACK",
        },
    }


class MarketMovementAnchorWorker:
    def __init__(
        self,
        *,
        symbols: Sequence[str] = DEFAULT_SYMBOLS,
        candle_fetcher: Callable[..., Mapping[str, Any]] = (
            canonical_price_path.fetch_closed_candles
        ),
        store_factory: Callable[[], Any] = (
            research_market_movement_store.MarketMovementStore
        ),
        now_provider: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        sleep: Callable[[float], Any] = asyncio.sleep,
    ) -> None:
        normalized = tuple(
            dict.fromkeys(movement._symbol(item) for item in symbols)
        )
        if not normalized:
            raise ValueError("market movement worker requires at least one symbol")
        # BTC first makes its local/parent projection available before optional
        # same-slot parent annotations for other assets.
        self.symbols = (
            (("BTC",) if "BTC" in normalized else ())
            + tuple(item for item in normalized if item != "BTC")
        )
        self._candle_fetcher = candle_fetcher
        self._store_factory = store_factory
        self._now = now_provider
        self._sleep = sleep
        self._lifecycle_lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        self._inflight_preflight: Optional[asyncio.Task] = None
        self._inflight_capture: Optional[asyncio.Task] = None
        self._inflight_symbol: Optional[str] = None
        self._store: Any = None
        self._runtime: dict[str, Any] = {}
        self._reset_runtime()

    def _reset_runtime(self) -> None:
        self._runtime = {
            "enabled": _enabled(),
            "running": False,
            "status": "off",
            "scheduler_state": "off",
            "started_at_utc": None,
            "stopped_at_utc": None,
            "next_run_utc": None,
            "active_eligible_at_utc": None,
            "last_attempted_eligible_at_utc": None,
            "last_completed_eligible_at_utc": None,
            "last_started_at_utc": None,
            "last_completed_at_utc": None,
            "pending_symbols": [],
            "projection_pending_symbols": [],
            "last_captured_symbols": [],
            "last_existing_symbols": [],
            "last_unevaluable_symbols": [],
            "last_errors": {},
            "last_run_summary": None,
            "last_error": None,
            "cycles_started": 0,
            "cycles_completed": 0,
            "cycles_failed": 0,
            "preflight": None,
        }

    def status(self) -> dict[str, Any]:
        value = dict(self._runtime)
        value.update(
            {
                "enabled": _enabled(),
                "task_running": bool(
                    self._task is not None and not self._task.done()
                ),
                "capture_inflight": bool(
                    self._inflight_capture is not None
                    and not self._inflight_capture.done()
                ),
                "capture_inflight_symbol": self._inflight_symbol,
                "preflight_inflight": bool(
                    self._inflight_preflight is not None
                    and not self._inflight_preflight.done()
                ),
                "scheduler_version": SCHEDULER_VERSION,
                "contract_version": movement.POLICY_VERSION,
                "sampler_version": movement.NEUTRAL_PRICE_SAMPLER_VERSION,
                "cadence_seconds": 60,
                "eligibility_minutes": list(movement.ELIGIBILITY_MINUTES),
                "capture_window_minutes": movement.CAPTURE_WINDOW_MINUTES,
                "symbols": list(self.symbols),
                "price_policy": (
                    "Binance Spot USDT exact closed 1m; HYPE Hyperliquid "
                    "HYPE/USDT Spot @107 exact closed 1m; no fallback"
                ),
                "schema_auto_create": False,
                "dedicated_writer_required": True,
                "automatic_repair": False,
                "historical_import_runtime": False,
                "formula_consumption_path": False,
                "telegram_delivery_path": False,
                "trading_path": False,
                "live_delivery_allowed": False,
                "runtime_wired": True,
                "store": (
                    self._store.status() if self._store is not None else None
                ),
            }
        )
        return json.loads(json.dumps(value, default=str))

    async def start(self) -> bool:
        async with self._lifecycle_lock:
            return await self._start_locked()

    async def _start_locked(self) -> bool:
        self._runtime["enabled"] = _enabled()
        if self._task is not None and not self._task.done():
            return True
        self._store = None
        self._runtime["preflight"] = None
        if not _enabled():
            self._runtime.update(
                {
                    "status": "off",
                    "scheduler_state": "off",
                    "last_error": None,
                }
            )
            return False
        try:
            store = self._store_factory()
            store_status = store.status()
            if not bool(store_status.get("configured")):
                self._runtime.update(
                    {
                        "status": "blocked_database",
                        "scheduler_state": "blocked",
                        "last_error": (
                            "Wave v5 dedicated writer database is not configured"
                        ),
                    }
                )
                return False
            preflight_task = asyncio.create_task(
                asyncio.to_thread(store.runtime_preflight),
                name="market-movement-runtime-preflight",
            )
            self._inflight_preflight = preflight_task
            try:
                preflight = await asyncio.shield(preflight_task)
            except asyncio.CancelledError:
                try:
                    await asyncio.shield(preflight_task)
                except Exception:
                    pass
                raise
            finally:
                if self._inflight_preflight is preflight_task:
                    self._inflight_preflight = None
            if preflight.get("ready") is not True:
                raise RuntimeError("Wave v5 runtime preflight is not ready")
            self._store = store
            self._runtime["preflight"] = dict(preflight)
        except asyncio.CancelledError:
            self._runtime.update(
                {"status": "cancelled", "scheduler_state": "cancelled"}
            )
            raise
        except Exception as exc:
            self._runtime.update(
                {
                    "status": "blocked_writer",
                    "scheduler_state": "blocked",
                    "last_error": f"{type(exc).__name__}: {exc}",
                }
            )
            return False
        now = _utc(self._now(), field="startup_time_utc")
        self._runtime.update(
            {
                "running": True,
                "status": "starting",
                "scheduler_state": "starting",
                "started_at_utc": _iso(now),
                "stopped_at_utc": None,
                "last_error": None,
            }
        )
        self._task = asyncio.create_task(
            self._run_loop(), name="research-market-movement-anchor-worker"
        )
        return True

    def _begin_slot(self, eligible_at_utc: datetime) -> None:
        slot_key = _iso(eligible_at_utc)
        if self._runtime.get("active_eligible_at_utc") == slot_key:
            return
        self._runtime.update(
            {
                "active_eligible_at_utc": slot_key,
                "pending_symbols": list(self.symbols),
                "projection_pending_symbols": [],
            }
        )

    async def _capture_one(
        self,
        *,
        symbol: str,
        eligible_at_utc: datetime,
        provider: Callable[[], Mapping[str, Any]],
    ) -> Any:
        """Await a blocking capture even when scheduler shutdown is requested."""

        if self._inflight_capture is not None and not self._inflight_capture.done():
            raise RuntimeError("a market movement capture is already in flight")
        capture_task = asyncio.create_task(
            asyncio.to_thread(
                self._store.capture_prospective,
                symbol=symbol,
                eligible_at_utc=eligible_at_utc,
                provider=provider,
            ),
            name=f"market-movement-capture-{symbol.lower()}",
        )
        self._inflight_capture = capture_task
        self._inflight_symbol = symbol
        try:
            return await asyncio.shield(capture_task)
        except asyncio.CancelledError:
            # Cancelling ``to_thread`` cannot stop its DB/network call.  Keep
            # shutdown honest by joining the bounded in-flight capture before
            # propagating scheduler cancellation.
            try:
                await asyncio.shield(capture_task)
            except Exception:
                pass
            raise
        finally:
            if self._inflight_capture is capture_task:
                self._inflight_capture = None
                self._inflight_symbol = None

    async def run_once(
        self,
        *,
        scheduled_at_utc: Any = None,
        eligible_at_utc: Any = None,
    ) -> dict[str, Any]:
        if self._store is None:
            raise RuntimeError("market movement store is not initialized")
        scheduled = _utc(
            scheduled_at_utc if scheduled_at_utc is not None else self._now(),
            field="scheduled_at_utc",
        )
        eligible = (
            movement._eligibility(eligible_at_utc)
            if eligible_at_utc is not None
            else latest_eligible_at(scheduled)
        )
        actual_start = _utc(self._now(), field="collection_start_utc")
        self._begin_slot(eligible)
        pending = set(self._runtime.get("pending_symbols") or ())
        run_symbols = tuple(symbol for symbol in self.symbols if symbol in pending)
        if not run_symbols:
            return {
                "eligible_at_utc": _iso(eligible),
                "run_symbols": [],
                "pending_symbols": [],
                "skipped_completed": True,
            }

        self._runtime.update(
            {
                "status": "collecting",
                "scheduler_state": "collecting",
                "last_started_at_utc": _iso(actual_start),
                "last_error": None,
                "cycles_started": int(self._runtime["cycles_started"]) + 1,
            }
        )
        captured: list[str] = []
        existing: list[str] = []
        unevaluable: list[str] = []
        projection_pending: list[str] = list(
            self._runtime.get("projection_pending_symbols") or ()
        )
        errors: dict[str, str] = {}
        results: dict[str, dict[str, Any]] = {}

        for symbol in run_symbols:
            def provider(
                selected_symbol: str = symbol,
                selected_eligibility: datetime = eligible,
            ) -> Mapping[str, Any]:
                raw_result = self._candle_fetcher(
                    selected_symbol,
                    selected_eligibility - timedelta(minutes=1),
                    selected_eligibility,
                )
                return official_provider_payload(
                    selected_symbol,
                    raw_result,
                    eligible_at_utc=selected_eligibility,
                    refresh_completed_at_utc=self._now(),
                )

            try:
                capture = await self._capture_one(
                    symbol=symbol,
                    eligible_at_utc=eligible,
                    provider=provider,
                )
                anchor = getattr(capture, "anchor", None)
                processing = getattr(capture, "processing", None)
                processing_status = str(
                    getattr(processing, "status", "") or ""
                ).upper()
                processing_anchor_id = getattr(processing, "anchor_id", None)
                provider_called = getattr(capture, "provider_called", None) is True
                if anchor is None:
                    unevaluable.append(symbol)
                else:
                    pending.discard(symbol)
                    if provider_called:
                        captured.append(symbol)
                    else:
                        existing.append(symbol)
                    if (
                        processing_status not in {"PROCESSED", "ALREADY_PROCESSED"}
                        or processing_anchor_id != getattr(anchor, "anchor_id", None)
                    ):
                        if symbol not in projection_pending:
                            projection_pending.append(symbol)
                results[symbol] = {
                    "provider_called": provider_called,
                    "idempotent_anchor": (
                        getattr(capture, "idempotent_anchor", None) is True
                    ),
                    "evaluation_status": (
                        movement.EVALUABLE if anchor is not None else movement.UNEVALUABLE
                    ),
                    "anchor_id": (
                        getattr(anchor, "anchor_id", None)
                        if anchor is not None
                        else None
                    ),
                    "attempt_receipt_sha256": getattr(
                        capture, "attempt_receipt_sha256", None
                    ),
                    "processing_status": processing_status or None,
                    "processing_anchor_id": processing_anchor_id,
                }
            except Exception as exc:
                errors[symbol] = f"{type(exc).__name__}: {exc}"

        pending_symbols = [symbol for symbol in self.symbols if symbol in pending]
        if errors:
            status = "retry_pending_with_errors"
        elif projection_pending:
            status = "blocked_projection"
        elif pending_symbols:
            status = "retry_pending"
        else:
            status = "completed"
        completed_at = _utc(self._now(), field="collection_completed_at_utc")
        summary = {
            "eligible_at_utc": _iso(eligible),
            "scheduled_at_utc": _iso(scheduled),
            "completed_at_utc": _iso(completed_at),
            "run_symbols": list(run_symbols),
            "captured_symbols": captured,
            "existing_symbols": existing,
            "unevaluable_symbols": unevaluable,
            "projection_pending_symbols": projection_pending,
            "pending_symbols": pending_symbols,
            "errors": errors,
            "results": results,
        }
        self._runtime.update(
            {
                "status": status,
                "scheduler_state": "cycle_complete",
                "last_attempted_eligible_at_utc": _iso(eligible),
                "last_completed_eligible_at_utc": (
                    _iso(eligible)
                    if not pending_symbols and not projection_pending and not errors
                    else self._runtime.get("last_completed_eligible_at_utc")
                ),
                "last_completed_at_utc": _iso(completed_at),
                "pending_symbols": pending_symbols,
                "projection_pending_symbols": projection_pending,
                "last_captured_symbols": captured,
                "last_existing_symbols": existing,
                "last_unevaluable_symbols": unevaluable,
                "last_errors": errors,
                "last_run_summary": summary,
                "last_error": (
                    "; ".join(f"{key}: {value}" for key, value in errors.items())
                    if errors
                    else None
                ),
                "cycles_completed": int(self._runtime["cycles_completed"]) + 1,
                "cycles_failed": int(self._runtime["cycles_failed"])
                + (1 if errors else 0),
            }
        )
        return summary

    async def _run_due_slot(self, run_at_utc: Any) -> bool:
        run_at = _utc(run_at_utc, field="run_at_utc")
        eligible = latest_eligible_at(run_at)
        slot_key = _iso(eligible)
        if (
            self._runtime.get("active_eligible_at_utc") == slot_key
            and not self._runtime.get("pending_symbols")
        ):
            self._runtime["status"] = (
                "blocked_projection"
                if self._runtime.get("projection_pending_symbols")
                else "completed"
            )
            return False
        await self.run_once(
            scheduled_at_utc=run_at,
            eligible_at_utc=eligible,
        )
        return True

    async def _run_loop(self) -> None:
        try:
            run_immediately = True
            while True:
                if run_immediately:
                    run_at = _utc(self._now(), field="run_at_utc")
                    run_immediately = False
                else:
                    run_at = next_minute_boundary(self._now())
                    self._runtime.update(
                        {
                            "scheduler_state": "waiting",
                            "next_run_utc": _iso(run_at),
                        }
                    )
                    delay = max(0.0, (run_at - _utc(self._now())).total_seconds())
                    await self._sleep(delay)
                    # The loop may wake much later than its planned boundary.
                    # Always select the eligibility that is active at the real
                    # wake time instead of replaying an expired scheduled slot.
                    run_at = _utc(self._now(), field="run_at_utc")
                try:
                    self._runtime.update(
                        {"scheduler_state": "running", "next_run_utc": None}
                    )
                    await self._run_due_slot(run_at)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._runtime.update(
                        {
                            "status": "failed",
                            "scheduler_state": "cycle_failed",
                            "last_completed_at_utc": _iso(self._now()),
                            "last_error": f"{type(exc).__name__}: {exc}",
                            "cycles_failed": int(self._runtime["cycles_failed"]) + 1,
                        }
                    )
                    print(
                        f"[market-movement] anchor cycle failed: {exc!r}",
                        flush=True,
                    )
        except asyncio.CancelledError:
            self._runtime["status"] = "cancelled"
            self._runtime["scheduler_state"] = "cancelled"
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
        async with self._lifecycle_lock:
            await self._stop_locked()

    async def _stop_locked(self) -> None:
        task = self._task
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        inflight = self._inflight_capture
        if inflight is not None and not inflight.done():
            try:
                await asyncio.shield(inflight)
            except Exception:
                pass
        preflight = self._inflight_preflight
        if preflight is not None and not preflight.done():
            try:
                await asyncio.shield(preflight)
            except Exception:
                pass
        self._task = None
        self._store = None
        self._runtime["preflight"] = None
        self._runtime["running"] = False
        self._runtime["next_run_utc"] = None
        if self._runtime.get("status") != "cancelled":
            self._runtime["status"] = "stopped"
            self._runtime["scheduler_state"] = "stopped"
            self._runtime["stopped_at_utc"] = _iso(self._now())


WORKER = MarketMovementAnchorWorker()
