"""Canonical Binance Spot OHLC path reader for Research outcomes.

The production bot already uses Binance Spot as its preferred live-price source.
This module adds the missing historical path: closed one-minute candles between
an alert and an outcome horizon.  It is deliberately independent from the bot's
scoring models so Research can measure price behaviour against either raw inputs
or the bot's existing scores.

Only Binance Spot data is accepted here.  Unsupported pairs fail explicitly;
the Research layer must never silently mix futures, Bybit or another fallback
into a result labelled as a Binance Spot path.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import os
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

import requests


BINANCE_SPOT_BASE_URL = os.getenv(
    "BINANCE_MARKET_DATA_BASE_URL",
    "https://data-api.binance.vision",
).rstrip("/")
BINANCE_SPOT_KLINES_ENDPOINT = os.getenv(
    "BINANCE_SPOT_KLINES_ENDPOINT",
    "/api/v3/klines",
)
REQUEST_TIMEOUT_SECONDS = max(
    3, int(os.getenv("BINANCE_PRICE_PATH_TIMEOUT_SECONDS", "15"))
)

# One minute is the canonical Research resolution.  A full 24-hour outcome
# requires only two Binance pages and preserves far more path detail than the
# bot's existing 30-minute Price/OI history.
INTERVAL = "1m"
INTERVAL_SECONDS = 60
INTERVAL_MS = INTERVAL_SECONDS * 1000
PAGE_LIMIT = 1000
MAX_CANDLES = 2000

# CoinGlass uses 1000PEPE as a display/instrument symbol while Binance Spot uses
# PEPEUSDT.  Prices are scaled back to the bot's instrument units.
SYMBOL_ALIASES: Dict[str, Tuple[str, float]] = {
    "1000PEPE": ("PEPE", 1000.0),
}


class BinanceSpotPathError(RuntimeError):
    """Raised when a canonical Binance Spot path cannot be obtained safely."""


@dataclass(frozen=True)
class SpotCandle:
    open_time_utc: datetime
    close_time_utc: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

    def to_dict(self) -> Dict[str, Any]:
        value = asdict(self)
        value["open_time_utc"] = self.open_time_utc.isoformat()
        value["close_time_utc"] = self.close_time_utc.isoformat()
        return value


def _utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _epoch_ms(value: Any) -> int:
    return int(_utc(value).timestamp() * 1000)


def _datetime_ms(value: int) -> datetime:
    return datetime.fromtimestamp(int(value) / 1000.0, tz=timezone.utc)


def _ceil_interval_ms(value: int) -> int:
    return ((int(value) + INTERVAL_MS - 1) // INTERVAL_MS) * INTERVAL_MS


def resolve_pair(symbol: str) -> Tuple[str, float]:
    normalized = str(symbol or "").strip().upper()
    if not normalized or len(normalized) > 20 or not normalized.replace("-", "").isalnum():
        raise ValueError("Invalid crypto symbol")
    base, multiplier = SYMBOL_ALIASES.get(normalized, (normalized, 1.0))
    return f"{base}USDT", float(multiplier)


def _parse_candle(raw: Any, multiplier: float) -> SpotCandle:
    if not isinstance(raw, (list, tuple)) or len(raw) < 7:
        raise BinanceSpotPathError("Binance Spot returned an invalid kline row")
    try:
        open_time_ms = int(raw[0])
        close_time_ms = int(raw[6])
        scale = float(multiplier)
        candle = SpotCandle(
            open_time_utc=_datetime_ms(open_time_ms),
            close_time_utc=_datetime_ms(close_time_ms),
            open=float(raw[1]) * scale,
            high=float(raw[2]) * scale,
            low=float(raw[3]) * scale,
            close=float(raw[4]) * scale,
            volume=float(raw[5]),
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise BinanceSpotPathError("Binance Spot returned a malformed kline") from exc
    if min(candle.open, candle.high, candle.low, candle.close) <= 0:
        raise BinanceSpotPathError("Binance Spot returned a non-positive price")
    if candle.high < max(candle.open, candle.close, candle.low):
        raise BinanceSpotPathError("Binance Spot kline high is inconsistent")
    if candle.low > min(candle.open, candle.close, candle.high):
        raise BinanceSpotPathError("Binance Spot kline low is inconsistent")
    return candle


def fetch_closed_candles(
    symbol: str,
    start_time: Any,
    end_time: Any,
    *,
    request_get: Callable[..., Any] = requests.get,
) -> Dict[str, Any]:
    """Fetch closed post-event one-minute candles from Binance Spot.

    The first partial minute is intentionally excluded.  Including its full
    high/low would leak price movement that occurred before the alert.  The
    immutable alert price remains the reference point, followed by candles that
    opened at or after the next minute boundary and closed by the horizon.
    """
    start = _utc(start_time)
    end = _utc(end_time)
    if end <= start:
        raise ValueError("end_time must be after start_time")

    pair, multiplier = resolve_pair(symbol)
    start_ms = _ceil_interval_ms(_epoch_ms(start))
    end_ms = _epoch_ms(end)
    if start_ms + INTERVAL_MS - 1 > end_ms:
        return {
            "symbol": str(symbol).strip().upper(),
            "pair": pair,
            "exchange": "binance",
            "market": "spot",
            "interval": INTERVAL,
            "interval_seconds": INTERVAL_SECONDS,
            "multiplier": multiplier,
            "candles": [],
            "expected_candles": 0,
            "complete": True,
        }

    url = BINANCE_SPOT_BASE_URL + BINANCE_SPOT_KLINES_ENDPOINT
    cursor = start_ms
    candles_by_open: Dict[int, SpotCandle] = {}

    while cursor <= end_ms and len(candles_by_open) < MAX_CANDLES:
        remaining = MAX_CANDLES - len(candles_by_open)
        response = request_get(
            url,
            params={
                "symbol": pair,
                "interval": INTERVAL,
                "startTime": cursor,
                "endTime": end_ms,
                "limit": min(PAGE_LIMIT, remaining),
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        try:
            response.raise_for_status()
        except Exception as exc:
            status = getattr(response, "status_code", "unknown")
            raise BinanceSpotPathError(
                f"Binance Spot kline request failed for {pair} (HTTP {status})"
            ) from exc
        payload = response.json()
        if not isinstance(payload, list):
            raise BinanceSpotPathError(
                f"Binance Spot returned a non-list kline payload for {pair}"
            )
        if not payload:
            break

        last_open_ms = None
        for raw in payload:
            candle = _parse_candle(raw, multiplier)
            open_ms = _epoch_ms(candle.open_time_utc)
            close_ms = _epoch_ms(candle.close_time_utc)
            last_open_ms = open_ms
            if open_ms < start_ms or close_ms > end_ms:
                continue
            candles_by_open[open_ms] = candle

        if last_open_ms is None or last_open_ms < cursor:
            break
        next_cursor = last_open_ms + INTERVAL_MS
        if next_cursor <= cursor:
            raise BinanceSpotPathError("Binance Spot kline pagination did not advance")
        cursor = next_cursor

    candles = [candles_by_open[key] for key in sorted(candles_by_open)]
    last_eligible_open = ((end_ms - (INTERVAL_MS - 1)) // INTERVAL_MS) * INTERVAL_MS
    expected = (
        max(0, ((last_eligible_open - start_ms) // INTERVAL_MS) + 1)
        if last_eligible_open >= start_ms
        else 0
    )
    complete = len(candles) == expected
    if expected > MAX_CANDLES:
        raise BinanceSpotPathError(
            f"Requested Binance Spot path exceeds {MAX_CANDLES} candles"
        )

    return {
        "symbol": str(symbol).strip().upper(),
        "pair": pair,
        "exchange": "binance",
        "market": "spot",
        "interval": INTERVAL,
        "interval_seconds": INTERVAL_SECONDS,
        "multiplier": multiplier,
        "candles": candles,
        "expected_candles": expected,
        "complete": complete,
    }


def _seconds_since(start: datetime, value: datetime) -> int:
    return max(0, int((_utc(value) - _utc(start)).total_seconds()))


def calculate_path_metrics(
    *,
    reference_price: float,
    direction: str,
    event_time: Any,
    candles: Iterable[SpotCandle],
    target_price: Optional[float] = None,
) -> Dict[str, Any]:
    """Calculate deterministic directional path and target-quality metrics."""
    reference = float(reference_price)
    if reference <= 0:
        raise ValueError("reference_price must be positive")
    normalized_direction = str(direction or "NEUTRAL").strip().upper()
    if normalized_direction not in {"LONG", "SHORT", "NEUTRAL"}:
        raise ValueError("direction must be LONG, SHORT or NEUTRAL")
    start = _utc(event_time)
    path = list(candles)
    if not path:
        raise ValueError("at least one closed candle is required")

    horizon_price = float(path[-1].close)
    raw_return_pct = (horizon_price - reference) / reference * 100.0
    directional_return_pct: Optional[float]
    if normalized_direction == "LONG":
        directional_return_pct = raw_return_pct
    elif normalized_direction == "SHORT":
        directional_return_pct = -raw_return_pct
    else:
        directional_return_pct = None

    result: Dict[str, Any] = {
        "measured_at_utc": path[-1].close_time_utc,
        "price_at_horizon": horizon_price,
        "raw_return_pct": raw_return_pct,
        "directional_return_pct": directional_return_pct,
        "max_favorable_price": None,
        "max_adverse_price": None,
        "mfe_pct": None,
        "mae_pct": None,
        "time_to_first_progress_seconds": None,
        "time_to_mfe_seconds": None,
        "time_to_closest_target_seconds": None,
        "time_to_target_seconds": None,
        "closest_target_price": None,
        "closest_target_distance_pct": None,
        "target_progress_ratio": None,
        "target_reached": None,
    }

    if normalized_direction in {"LONG", "SHORT"}:
        if normalized_direction == "LONG":
            favorable = max(path, key=lambda candle: candle.high)
            adverse = min(path, key=lambda candle: candle.low)
            favorable_price = max(reference, float(favorable.high))
            adverse_price = min(reference, float(adverse.low))
            mfe_pct = max(0.0, (favorable_price - reference) / reference * 100.0)
            mae_pct = max(0.0, (reference - adverse_price) / reference * 100.0)
            progress_candle = next(
                (candle for candle in path if candle.high > reference), None
            )
            mfe_time = favorable.close_time_utc if favorable_price > reference else start
        else:
            favorable = min(path, key=lambda candle: candle.low)
            adverse = max(path, key=lambda candle: candle.high)
            favorable_price = min(reference, float(favorable.low))
            adverse_price = max(reference, float(adverse.high))
            mfe_pct = max(0.0, (reference - favorable_price) / reference * 100.0)
            mae_pct = max(0.0, (adverse_price - reference) / reference * 100.0)
            progress_candle = next(
                (candle for candle in path if candle.low < reference), None
            )
            mfe_time = favorable.close_time_utc if favorable_price < reference else start

        result.update(
            {
                "max_favorable_price": favorable_price,
                "max_adverse_price": adverse_price,
                "mfe_pct": mfe_pct,
                "mae_pct": mae_pct,
                "time_to_first_progress_seconds": (
                    _seconds_since(start, progress_candle.close_time_utc)
                    if progress_candle
                    else None
                ),
                "time_to_mfe_seconds": _seconds_since(start, mfe_time),
            }
        )

    target = float(target_price) if target_price is not None else None
    if target is not None and target > 0 and target != reference:
        upward_target = target > reference
        initial_distance = abs(target - reference)
        best_distance = initial_distance
        best_price = reference
        best_time = start
        reached_time: Optional[datetime] = None

        for candle in path:
            if upward_target:
                observed = min(float(candle.high), target)
                reached = candle.high >= target
            else:
                observed = max(float(candle.low), target)
                reached = candle.low <= target
            distance = abs(target - observed)
            if distance < best_distance:
                best_distance = distance
                best_price = observed
                best_time = candle.close_time_utc
            if reached and reached_time is None:
                reached_time = candle.close_time_utc

        progress_ratio = max(0.0, min(1.0, 1.0 - best_distance / initial_distance))
        result.update(
            {
                "time_to_closest_target_seconds": _seconds_since(start, best_time),
                "time_to_target_seconds": (
                    _seconds_since(start, reached_time) if reached_time else None
                ),
                "closest_target_price": best_price,
                "closest_target_distance_pct": best_distance / target * 100.0,
                "target_progress_ratio": progress_ratio,
                "target_reached": reached_time is not None,
            }
        )

    return result
