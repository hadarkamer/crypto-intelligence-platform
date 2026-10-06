"""Full fixed-window excursions, separate from ordered First Touch v7 labels.

The immutable alert price is the reference. Only complete closed Spot 1m
candles inside the requested horizon are measured. Partial boundary minutes
are disclosed, never reconstructed. A first touch does not stop this path.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
from typing import Any, Iterable, Mapping

import canonical_price_path
from research_ordered_first_touch import _excursion_metrics, _field, _finite_number

METHOD_VERSION = "common-window-spot-1m-v1"
WINDOWS = (60, 240, 720, 1440)
ASYMMETRY_METHOD = "sum_mfe_pct_over_sum_mae_pct_same_full_window_v1"


def utc(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("common-window timestamps must include an explicit timezone")
    return parsed.astimezone(timezone.utc)


def calculate_common_window_metrics(*, symbol: str, reference_price: float,
        direction: str, event_time: Any, window_minutes: int, candles: Iterable[Any],
        observed_at: Any, path_result: Mapping[str, Any]) -> dict[str, Any]:
    if type(window_minutes) is not int or window_minutes not in WINDOWS:
        raise ValueError("unsupported common window")
    reference = _finite_number(reference_price, name="reference_price")
    if reference <= 0:
        raise ValueError("reference price must be positive")
    side = str(direction).upper()
    if side not in {"LONG", "SHORT"}:
        raise ValueError("direction must be LONG or SHORT")
    start, now = utc(event_time), utc(observed_at)
    if now < start:
        raise ValueError("observation cannot precede the immutable event time")
    end = start + timedelta(minutes=window_minutes)
    first = start.replace(second=0, microsecond=0)
    if first < start:
        first += timedelta(minutes=1)
    # Millisecond-inclusive exchange candle closes; no current minute leaks.
    cutoff = min(end, now.replace(second=0, microsecond=0) - timedelta(milliseconds=1))
    expected_opens = []
    cursor = first
    while cursor + timedelta(minutes=1, milliseconds=-1) <= cutoff:
        expected_opens.append(cursor)
        cursor += timedelta(minutes=1)
    route_error = None
    try:
        route = canonical_price_path.validated_route(symbol, dict(path_result), require_complete=False)
        if str(path_result.get("symbol") or "").upper() != str(symbol).upper():
            raise ValueError("returned price path symbol does not match the event")
    except (TypeError, ValueError) as exc:
        route, route_error = None, str(exc)
    rows, errors = [], []
    for raw in candles:
        try:
            opened, closed = utc(_field(raw, "open_time_utc")), utc(_field(raw, "close_time_utc"))
            if opened < first or opened > cutoff or closed > cutoff:
                continue
            prices = {key: _finite_number(_field(raw, key), name="candle." + key) for key in ("open", "high", "low", "close")}
            if min(prices.values()) <= 0 or prices["high"] < max(prices.values()) or prices["low"] > min(prices.values()):
                raise ValueError("invalid positive OHLC envelope")
            if opened.second or opened.microsecond or closed - opened != timedelta(minutes=1, milliseconds=-1):
                raise ValueError("candle must span the exact exchange closed 1m interval")
            rows.append({"open_time_utc": opened, "close_time_utc": closed, **prices})
        except (TypeError, ValueError, KeyError, AttributeError) as exc:
            errors.append(str(exc))
    rows.sort(key=lambda item: item["open_time_utc"])
    prefix_complete = not errors and [row["open_time_utc"] for row in rows] == expected_opens
    closed_window = now >= end
    complete = bool(prefix_complete and route is not None and closed_window)
    status = "READY" if complete else "DATA_MISSING" if not prefix_complete or route is None else "OPEN"
    result = {
        "method_version": METHOD_VERSION, "measurement_kind": "FIXED_WINDOW",
        "window_minutes": window_minutes, "symbol": str(symbol).upper(), "direction": side,
        "measurement_start_utc": start, "window_end_utc": end,
        "observed_from_utc": rows[0]["open_time_utc"] if rows else None,
        "observed_through_utc": rows[-1]["close_time_utc"] if rows else None,
        "observed_at_utc": now, "reference_price": reference, "status": status,
        "observation_closed": closed_window, "path_complete": complete,
        "observed_prefix_complete": prefix_complete,
        "expected_candles": len(expected_opens), "path_samples": len(rows),
        "initial_gap_seconds": (first - start).total_seconds(),
        "trailing_partial_minute_seconds": (end - (end.replace(second=0, microsecond=0))).total_seconds(),
        "boundary_policy": "EXCLUDE_PARTIAL_MINUTES_USE_IMMUTABLE_ALERT_PRICE",
        "candle_interval_seconds": 60, "source": route,
        "data_quality_status": canonical_price_path.quality_status(dict(path_result), complete=complete) if route else "INVALID_PRICE_PROVENANCE",
        "missing_reason": route_error or ("; ".join(errors) if errors else "INCOMPLETE_CLOSED_1M_PATH" if not prefix_complete else None),
        "mfe_pct": None, "mae_pct": None, "asymmetry_ratio": None,
        "asymmetry_method": ASYMMETRY_METHOD,
        "asymmetry_status": "WINDOW_NOT_READY",
    }
    if complete:
        maximum = max([reference] + [row["high"] for row in rows])
        minimum = min([reference] + [row["low"] for row in rows])
        result.update(_excursion_metrics(reference_price=reference, direction=side, maximum=maximum, minimum=minimum))
        result["asymmetry_ratio"] = result["mfe_pct"] / result["mae_pct"] if result["mae_pct"] > 0 else None
        result["asymmetry_status"] = "DEFINED" if result["mae_pct"] > 0 else "UNDEFINED_ZERO_MAE"
    digest_rows = [{**row, "open_time_utc": row["open_time_utc"].isoformat(), "close_time_utc": row["close_time_utc"].isoformat()} for row in rows]
    result["path_sha256"] = hashlib.sha256(json.dumps({"source": route, "candles": digest_rows}, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    return result


def aggregate_common_window_metrics(records: Iterable[Mapping[str, Any]], *, expected_count: int,
        window_minutes: int, threshold_bps: int) -> dict[str, Any]:
    """Aggregate one fixed representative per wave; caller selects cohort.

    Explicit expected_count makes missing representatives visible. Outcomes of
    every First Touch status must be supplied, never successes alone.
    """
    rows = list(records)
    if type(expected_count) is not int or expected_count < 0 or window_minutes not in WINDOWS or threshold_bps not in (25,50,75,100,125,150,175,200):
        raise ValueError("invalid exact metric cohort")
    valid = []
    for row in rows:
        try:
            mfe, mae = _finite_number(row.get("mfe_pct"), name="mfe"), _finite_number(row.get("mae_pct"), name="mae")
            source = dict(row.get("source") or {})
            canonical_price_path.validated_route(row.get("symbol"),
                {**source, "api_coin": source.get("instrument")}, require_complete=False)
            start, end = utc(row.get("measurement_start_utc")), utc(row.get("window_end_utc"))
            if end != start + timedelta(minutes=window_minutes) or utc(row.get("observed_at_utc")) < end:
                continue
        except (TypeError, ValueError):
            continue
        if (row.get("method_version") == METHOD_VERSION and row.get("measurement_kind") == "FIXED_WINDOW"
                and row.get("window_minutes") == window_minutes and row.get("status") == "READY"
                and row.get("observation_closed") is True and row.get("path_complete") is True
                and row.get("data_quality_status") in canonical_price_path.COMPLETE_QUALITIES and min(mfe, mae) >= 0):
            valid.append((mfe, mae))
    complete = len(rows) == len(valid) == expected_count and expected_count > 0
    total_mfe, total_mae = sum(item[0] for item in valid), sum(item[1] for item in valid)
    return {"method_version": METHOD_VERSION, "asymmetry_method": ASYMMETRY_METHOD,
        "window_minutes": window_minutes, "threshold_bps": threshold_bps,
        "expected_representatives": expected_count, "valid_representatives": len(valid),
        "coverage_complete": complete, "mfe_pct": total_mfe / expected_count if complete else None,
        "mae_pct": total_mae / expected_count if complete else None,
        "asymmetry_ratio": total_mfe / total_mae if complete and total_mae > 0 else None,
        "asymmetry_status": "INCOMPLETE_COHORT" if not complete else "UNDEFINED_ZERO_MAE" if total_mae == 0 else "DEFINED"}
