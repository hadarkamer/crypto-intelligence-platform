"""Versioned, no-lookahead feature rows for alert-formula research.

The matrix keeps three different kinds of information separate:

* raw market measurements that existed at or before the alert;
* compact model/score state captured inside the immutable Research Event;
* verified post-alert Binance Spot outcomes used only as labels.

Nothing in this module writes to PostgreSQL or changes a live alert rule.  Raw
time-series rows remain in their existing archive tables and are joined in
bounded batches when a research request is made.
"""

from __future__ import annotations

from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import os
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - validated at runtime
    psycopg = None
    dict_row = None


FEATURE_SCHEMA_VERSION = "research-feature-matrix-v1"
VERIFIED_OUTCOME_METHOD = "binance-spot-1m-ohlc-path-v2"
VERIFIED_OUTCOME_QUALITY = "VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES"

CORE_WINDOWS_MINUTES: tuple[int, ...] = (30, 60, 240, 720, 1440)
EXTENDED_WINDOWS_MINUTES: tuple[int, ...] = CORE_WINDOWS_MINUTES + (2880, 4320, 10080)
SEQUENCE_WINDOWS_MINUTES: tuple[int, ...] = (30, 120, 360)

# The bot stores 30-minute historical market rows.  A prior-only point older
# than this hard ceiling is reported missing instead of silently joining stale
# evidence to an alert.
MAX_POINT_AGE_MINUTES = 45
_TRUE = {"1", "true", "yes", "on"}


def _as_utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip().replace("Z", "+00:00")
        if not text:
            raise ValueError("timestamp is required")
        parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _float(value: Any) -> Optional[float]:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _round(value: Any, digits: int = 6) -> Optional[float]:
    number = _float(value)
    return round(number, digits) if number is not None else None


def _pct_change(current: Any, previous: Any) -> Optional[float]:
    current_number = _float(current)
    previous_number = _float(previous)
    if current_number is None or previous_number in (None, 0.0):
        return None
    return round((current_number - previous_number) / previous_number * 100.0, 6)


def _difference(current: Any, previous: Any) -> Optional[float]:
    current_number = _float(current)
    previous_number = _float(previous)
    if current_number is None or previous_number is None:
        return None
    return round(current_number - previous_number, 6)


def _json_safe(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str, ensure_ascii=False))


def _symbol(value: Any) -> Optional[str]:
    text = str(value or "").strip().upper()
    if not text:
        return None
    if len(text) > 20 or not text.replace("-", "").isalnum():
        raise ValueError("Invalid crypto symbol")
    return text


def _event_type(value: Any) -> Optional[str]:
    text = str(value or "").strip().upper()
    if not text:
        return None
    if len(text) > 100 or not text.replace("_", "").replace("-", "").isalnum():
        raise ValueError("Invalid event_type")
    return text


def _research_database_url() -> str:
    dedicated = os.getenv("RESEARCH_DATABASE_URL", "").strip()
    use_primary = os.getenv("RESEARCH_USE_PRIMARY_DATABASE", "").strip().lower() in _TRUE
    if dedicated:
        return dedicated
    if use_primary:
        return os.getenv("DATABASE_URL", "").strip()
    return ""


def _raw_database_url() -> str:
    return os.getenv("DATABASE_URL", "").strip()


def _connect(url: str):
    if not url:
        raise RuntimeError("Required PostgreSQL archive is not configured")
    if psycopg is None:
        raise RuntimeError("psycopg is unavailable")
    return psycopg.connect(
        url,
        row_factory=dict_row,
        connect_timeout=5,
        options="-c statement_timeout=12000 -c default_transaction_read_only=on",
    )


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT to_regclass(%s) AS relation", (f"public.{table_name}",)
    ).fetchone()
    return bool(row and row.get("relation"))


@dataclass(frozen=True)
class _Series:
    times: tuple[datetime, ...]
    rows: tuple[Dict[str, Any], ...]


def _prepare_series(
    rows: Iterable[Mapping[str, Any]], *, time_column: str
) -> Dict[str, _Series]:
    grouped: Dict[str, list[tuple[datetime, Dict[str, Any]]]] = defaultdict(list)
    for source_row in rows:
        row = dict(source_row)
        symbol = _symbol(row.get("symbol"))
        if symbol is None or row.get(time_column) is None:
            continue
        grouped[symbol].append((_as_utc(row[time_column]), row))

    prepared: Dict[str, _Series] = {}
    for symbol, values in grouped.items():
        values.sort(key=lambda item: item[0])
        prepared[symbol] = _Series(
            times=tuple(item[0] for item in values),
            rows=tuple(item[1] for item in values),
        )
    return prepared


def _prior_point(
    series: Optional[_Series], target_time: datetime, max_age_minutes: int = MAX_POINT_AGE_MINUTES
) -> tuple[Optional[Dict[str, Any]], Optional[float]]:
    """Return the newest row at or before target_time, never a future row."""
    if not series or not series.times:
        return None, None
    target = _as_utc(target_time)
    index = bisect_right(series.times, target) - 1
    if index < 0:
        return None, None
    row_time = series.times[index]
    age_minutes = (target - row_time).total_seconds() / 60.0
    if age_minutes < 0 or age_minutes > max_age_minutes:
        return None, None
    return dict(series.rows[index]), round(age_minutes, 3)


def _sign(value: Any, epsilon: float = 0.0) -> int:
    number = _float(value)
    if number is None or abs(number) <= epsilon:
        return 0
    return 1 if number > 0 else -1


def _alignment(left: Any, right: Any) -> str:
    left_sign = _sign(left)
    right_sign = _sign(right)
    if not left_sign or not right_sign:
        return "NEUTRAL_OR_MISSING"
    return "ALIGNED" if left_sign == right_sign else "DIVERGENT"


def _price_oi_state(price_change_pct: Any, oi_change_pct: Any) -> str:
    price_sign = _sign(price_change_pct, epsilon=0.000001)
    oi_sign = _sign(oi_change_pct, epsilon=0.000001)
    labels = {-1: "DOWN", 0: "FLAT_OR_MISSING", 1: "UP"}
    return f"PRICE_{labels[price_sign]}__OI_{labels[oi_sign]}"


def _latest_price_oi(row: Optional[Mapping[str, Any]], age: Optional[float]) -> Dict[str, Any]:
    if not row:
        return {"available": False, "age_minutes": None}
    return {
        "available": True,
        "timestamp_utc": row.get("candle_time"),
        "age_minutes": age,
        "price_close": _round(row.get("price_close")),
        "oi_close_usd": _round(row.get("oi_close_usd"), 2),
        "price_exchange": row.get("price_exchange"),
        "price_pair": row.get("price_pair"),
        "source": row.get("source"),
    }


def _latest_flow(row: Optional[Mapping[str, Any]], age: Optional[float]) -> Dict[str, Any]:
    if not row:
        return {"available": False, "age_minutes": None}
    buy = _float(row.get("buy_volume_usd"))
    sell = _float(row.get("sell_volume_usd"))
    ratio = None if sell in (None, 0.0) or buy is None else round(buy / sell, 6)
    return {
        "available": True,
        "timestamp_utc": row.get("candle_time"),
        "age_minutes": age,
        "buy_volume_usd": _round(buy, 2),
        "sell_volume_usd": _round(sell, 2),
        "net_taker_flow_usd": _difference(buy, sell),
        "buy_sell_ratio": ratio,
        "continuous_cvd_usd": _round(row.get("continuous_cum_vol_delta_usd"), 2),
        "api_cvd_usd": _round(row.get("api_cum_vol_delta_usd"), 2),
        "exchange_list": row.get("exchange_list"),
        "source": row.get("source"),
    }


def _window_features(
    *,
    event_time: datetime,
    minutes: int,
    price_series: Optional[_Series],
    futures_series: Optional[_Series],
    spot_series: Optional[_Series],
) -> Dict[str, Any]:
    reference_time = event_time - timedelta(minutes=minutes)
    current_price, current_price_age = _prior_point(price_series, event_time)
    prior_price, prior_price_age = _prior_point(price_series, reference_time)
    current_futures, current_futures_age = _prior_point(futures_series, event_time)
    prior_futures, prior_futures_age = _prior_point(futures_series, reference_time)
    current_spot, current_spot_age = _prior_point(spot_series, event_time)
    prior_spot, prior_spot_age = _prior_point(spot_series, reference_time)

    price_change = _pct_change(
        current_price.get("price_close") if current_price else None,
        prior_price.get("price_close") if prior_price else None,
    )
    oi_change = _pct_change(
        current_price.get("oi_close_usd") if current_price else None,
        prior_price.get("oi_close_usd") if prior_price else None,
    )
    futures_cvd_change = _difference(
        current_futures.get("continuous_cum_vol_delta_usd") if current_futures else None,
        prior_futures.get("continuous_cum_vol_delta_usd") if prior_futures else None,
    )
    spot_cvd_change = _difference(
        current_spot.get("continuous_cum_vol_delta_usd") if current_spot else None,
        prior_spot.get("continuous_cum_vol_delta_usd") if prior_spot else None,
    )
    futures_api_change = _difference(
        current_futures.get("api_cum_vol_delta_usd") if current_futures else None,
        prior_futures.get("api_cum_vol_delta_usd") if prior_futures else None,
    )
    spot_api_change = _difference(
        current_spot.get("api_cum_vol_delta_usd") if current_spot else None,
        prior_spot.get("api_cum_vol_delta_usd") if prior_spot else None,
    )

    absolute_futures = abs(futures_cvd_change) if futures_cvd_change is not None else None
    spot_to_futures_ratio = None
    if absolute_futures not in (None, 0.0) and spot_cvd_change is not None:
        spot_to_futures_ratio = round(abs(spot_cvd_change) / absolute_futures, 6)

    return {
        "window_minutes": minutes,
        "reference_time_utc": reference_time,
        "price_change_pct": price_change,
        "oi_change_pct": oi_change,
        "price_oi_state": _price_oi_state(price_change, oi_change),
        "futures_continuous_cvd_change_usd": futures_cvd_change,
        "spot_continuous_cvd_change_usd": spot_cvd_change,
        "futures_api_cvd_change_usd": futures_api_change,
        "spot_api_cvd_change_usd": spot_api_change,
        "spot_futures_alignment": _alignment(spot_cvd_change, futures_cvd_change),
        "spot_to_futures_abs_cvd_ratio": spot_to_futures_ratio,
        "price_spot_alignment": _alignment(price_change, spot_cvd_change),
        "price_futures_alignment": _alignment(price_change, futures_cvd_change),
        "source_ages_minutes": {
            "price_current": current_price_age,
            "price_reference": prior_price_age,
            "futures_current": current_futures_age,
            "futures_reference": prior_futures_age,
            "spot_current": current_spot_age,
            "spot_reference": prior_spot_age,
        },
        "complete": all(
            value is not None
            for value in (price_change, oi_change, futures_cvd_change, spot_cvd_change)
        ),
    }


def _flatten_snapshot(
    value: Any,
    *,
    prefix: str = "snapshot",
    output: Optional[Dict[str, Any]] = None,
    depth: int = 0,
    max_depth: int = 6,
    max_features: int = 160,
) -> Dict[str, Any]:
    """Flatten compact decision-time state into stable dotted feature names."""
    if output is None:
        output = {}
    if len(output) >= max_features:
        return output
    if isinstance(value, Mapping) and depth < max_depth:
        for key in sorted(value, key=lambda item: str(item)):
            if len(output) >= max_features:
                break
            child = value[key]
            child_prefix = f"{prefix}.{key}"
            _flatten_snapshot(
                child,
                prefix=child_prefix,
                output=output,
                depth=depth + 1,
                max_depth=max_depth,
                max_features=max_features,
            )
        return output
    if isinstance(value, (list, tuple)):
        if len(value) <= 24 and all(
            item is None or isinstance(item, (str, int, float, bool)) for item in value
        ):
            output[prefix] = _json_safe(value)
        else:
            output[f"{prefix}.__count"] = len(value)
        return output
    if value is None or isinstance(value, (str, int, float, bool)):
        output[prefix] = value
    return output


def _model_features(event: Mapping[str, Any]) -> Dict[str, Any]:
    snapshot = event.get("engine_snapshot")
    if isinstance(snapshot, str):
        try:
            snapshot = json.loads(snapshot)
        except json.JSONDecodeError:
            snapshot = {}
    snapshot = snapshot if isinstance(snapshot, Mapping) else {}
    flattened = _flatten_snapshot(snapshot)
    return {
        "alert_score": _round(event.get("score")),
        "initial_target_distance_pct": _round(event.get("initial_target_distance_pct")),
        "categories": _json_safe(event.get("categories") or []),
        "snapshot_features": flattened,
        "snapshot_feature_count": len(flattened),
    }


def _nested_value(value: Mapping[str, Any], path: str) -> Any:
    current: Any = value
    for part in path.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current[part]
    return current


def _captured_event_inputs(event: Mapping[str, Any]) -> Dict[str, Any]:
    """Expose known non-score alert inputs separately from model features.

    These values were frozen into the event because they cannot always be
    reconstructed from the raw time-series tables.  They are still labelled as
    captured inputs, not as canonical raw history.
    """
    snapshot = event.get("engine_snapshot")
    if isinstance(snapshot, str):
        try:
            snapshot = json.loads(snapshot)
        except json.JSONDecodeError:
            snapshot = {}
    snapshot = snapshot if isinstance(snapshot, Mapping) else {}
    paths = (
        "distance_pct",
        "near_share_pct",
        "near_amount",
        "far_amount",
        "consensus_hits",
        "consensus_total",
        "gap_consensus_supporting",
        "gap_consensus_total",
        "cluster_candidate_count",
        "cluster_count",
        "cluster_same_direction_count",
        "cluster.members",
        "cluster.count",
        "cluster.spread_pct",
        "cluster.average_target",
        "gap.advantage",
        "gap.near_distance",
        "gap.far_distance",
        "balance.near_share_pct",
        "magnet.side",
        "magnet.count",
        "magnet.members",
        "magnet.min_target",
        "magnet.max_target",
        "magnet.average_target",
        "magnet.spread_pct",
        "magnet.gross_candidate_liquidity",
        "magnet.gross_opposite_liquidity",
        "magnet.gross_liquidity_timeframe",
        "price_source",
        "price_pair",
        "top_item_price_source",
        "top_item_price_pair",
    )
    captured = {
        path: _json_safe(_nested_value(snapshot, path))
        for path in paths
        if _nested_value(snapshot, path) is not None
    }
    return {
        "event_current_price": _round(event.get("current_price")),
        "event_target_price": _round(event.get("target_price")),
        "event_initial_target_distance_pct": _round(
            event.get("initial_target_distance_pct")
        ),
        "snapshot_inputs": captured,
    }


def _time_features(event_time: datetime) -> Dict[str, Any]:
    timestamp = _as_utc(event_time)
    hour = timestamp.hour
    if hour < 8:
        bucket = "UTC_00_07"
    elif hour < 13:
        bucket = "UTC_08_12"
    elif hour < 21:
        bucket = "UTC_13_20"
    else:
        bucket = "UTC_21_23"
    return {
        "utc_hour": hour,
        "utc_weekday": timestamp.weekday(),
        "utc_weekday_name": timestamp.strftime("%A").upper(),
        "is_weekend_utc": timestamp.weekday() >= 5,
        "fixed_utc_session_bucket": bucket,
    }


def _sequence_features(
    event: Mapping[str, Any], prior_events: Sequence[Mapping[str, Any]]
) -> Dict[str, Any]:
    event_time = _as_utc(event["alert_time_utc"])
    symbol = str(event.get("symbol") or "")
    direction = str(event.get("direction") or "NEUTRAL")
    setup_key = str(event.get("setup_key") or "")
    result: Dict[str, Any] = {}

    for minutes in SEQUENCE_WINDOWS_MINUTES:
        start = event_time - timedelta(minutes=minutes)
        eligible = [
            row
            for row in prior_events
            if start <= _as_utc(row["alert_time_utc"]) < event_time
        ]
        same_symbol = [row for row in eligible if str(row.get("symbol") or "") == symbol]
        same_direction = [
            row for row in same_symbol if str(row.get("direction") or "NEUTRAL") == direction
        ]
        market_long = sum(1 for row in eligible if str(row.get("direction")) == "LONG")
        market_short = sum(1 for row in eligible if str(row.get("direction")) == "SHORT")
        directional_total = market_long + market_short
        result[f"{minutes}m"] = {
            "same_symbol_alerts": len(same_symbol),
            "same_symbol_same_direction": len(same_direction),
            "same_symbol_distinct_event_types": len(
                {str(row.get("event_type") or "") for row in same_symbol}
            ),
            "same_setup_repetitions": sum(
                1 for row in eligible if setup_key and str(row.get("setup_key") or "") == setup_key
            ),
            "market_alerts": len(eligible),
            "market_distinct_symbols": len(
                {str(row.get("symbol") or "") for row in eligible}
            ),
            "market_long_alerts": market_long,
            "market_short_alerts": market_short,
            "market_direction_balance_pct": (
                round((market_long - market_short) / directional_total * 100.0, 4)
                if directional_total
                else None
            ),
        }
    return result


def _outcome_label(event: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "horizon_minutes": event.get("horizon_minutes"),
        "measured_at_utc": event.get("measured_at_utc"),
        "reference_price": _round(event.get("reference_price")),
        "price_at_horizon": _round(event.get("price_at_horizon")),
        "raw_return_pct": _round(event.get("raw_return_pct")),
        "directional_return_pct": _round(event.get("directional_return_pct")),
        "mfe_pct": _round(event.get("mfe_pct")),
        "mae_pct": _round(event.get("mae_pct")),
        "time_to_first_progress_seconds": event.get("time_to_first_progress_seconds"),
        "time_to_mfe_seconds": event.get("time_to_mfe_seconds"),
        "time_to_closest_target_seconds": event.get("time_to_closest_target_seconds"),
        "time_to_target_seconds": event.get("time_to_target_seconds"),
        "target_progress_ratio": _round(event.get("target_progress_ratio")),
        "target_reached": event.get("target_reached"),
        "path_samples": event.get("path_samples"),
        "outcome_method_version": event.get("outcome_method_version"),
        "data_quality_status": event.get("data_quality_status"),
    }


def build_feature_rows(
    events: Sequence[Mapping[str, Any]],
    *,
    price_oi_rows: Iterable[Mapping[str, Any]],
    futures_rows: Iterable[Mapping[str, Any]],
    spot_rows: Iterable[Mapping[str, Any]],
    prior_events: Sequence[Mapping[str, Any]],
    windows_minutes: Sequence[int] = CORE_WINDOWS_MINUTES,
) -> list[Dict[str, Any]]:
    """Pure deterministic builder used by the DB wrapper and self-tests."""
    price_series = _prepare_series(price_oi_rows, time_column="candle_time")
    futures_series = _prepare_series(futures_rows, time_column="candle_time")
    spot_series = _prepare_series(spot_rows, time_column="candle_time")
    rows: list[Dict[str, Any]] = []

    for source_event in events:
        event = dict(source_event)
        event_time = _as_utc(event["alert_time_utc"])
        symbol = str(event.get("symbol") or "").upper()
        current_price, current_price_age = _prior_point(price_series.get(symbol), event_time)
        current_futures, current_futures_age = _prior_point(futures_series.get(symbol), event_time)
        current_spot, current_spot_age = _prior_point(spot_series.get(symbol), event_time)

        windows = {
            f"{minutes}m": _window_features(
                event_time=event_time,
                minutes=int(minutes),
                price_series=price_series.get(symbol),
                futures_series=futures_series.get(symbol),
                spot_series=spot_series.get(symbol),
            )
            for minutes in windows_minutes
        }
        rows.append(
            {
                "feature_schema_version": FEATURE_SCHEMA_VERSION,
                "event": {
                    "event_id": event.get("event_id"),
                    "alert_time_utc": event_time,
                    "symbol": symbol,
                    "direction": event.get("direction"),
                    "source_side": event.get("source_side"),
                    "timeframe": event.get("timeframe"),
                    "event_type": event.get("event_type"),
                    "strategy_version": event.get("strategy_version"),
                    "code_version": event.get("code_version"),
                },
                "time_features": _time_features(event_time),
                "raw_features": {
                    "captured_event_inputs": _captured_event_inputs(event),
                    "latest_at_or_before_alert": {
                        "price_oi": _latest_price_oi(current_price, current_price_age),
                        "futures_cvd": _latest_flow(current_futures, current_futures_age),
                        "spot_cvd": _latest_flow(current_spot, current_spot_age),
                    },
                    "windows": windows,
                },
                "model_features": _model_features(event),
                "sequence_features": _sequence_features(event, prior_events),
                "outcome_label": _outcome_label(event),
            }
        )
    return _json_safe(rows)


def _load_verified_events(
    conn,
    *,
    symbol: Optional[str],
    event_type: Optional[str],
    direction: Optional[str],
    lookback_days: int,
    horizon_minutes: int,
    limit: int,
) -> list[Dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = [
        lookback_days,
        horizon_minutes,
        VERIFIED_OUTCOME_METHOD,
        VERIFIED_OUTCOME_QUALITY,
    ]
    if symbol:
        clauses.append("AND e.symbol=%s")
        params.append(symbol)
    if event_type:
        clauses.append("AND e.event_type=%s")
        params.append(event_type)
    if direction:
        clauses.append("AND e.direction=%s")
        params.append(direction)
    params.append(limit)
    filters = "\n".join(clauses)
    return [
        dict(row)
        for row in conn.execute(
            f"""
            SELECT e.event_id, e.alert_time_utc, e.symbol, e.direction,
                   e.source_side, e.timeframe, e.event_type, e.score,
                   e.current_price, e.target_price,
                   e.initial_target_distance_pct, e.categories, e.setup_key,
                   e.strategy_version, e.code_version, e.engine_snapshot,
                   o.horizon_minutes, o.measured_at_utc, o.reference_price,
                   o.price_at_horizon, o.raw_return_pct,
                   o.directional_return_pct, o.mfe_pct, o.mae_pct,
                   o.time_to_first_progress_seconds, o.time_to_mfe_seconds,
                   o.time_to_closest_target_seconds, o.time_to_target_seconds,
                   o.target_progress_ratio, o.target_reached, o.path_samples,
                   o.outcome_method_version, o.data_quality_status
            FROM research_events e
            JOIN research_alert_outcomes o ON o.event_id=e.event_id
            WHERE e.event_kind='ALERT'
              AND e.delivery_status='DELIVERED'
              AND e.alert_time_utc >= NOW() - (%s * INTERVAL '1 day')
              AND o.horizon_minutes=%s
              AND o.outcome_method_version=%s
              AND o.data_quality_status=%s
              {filters}
            ORDER BY e.alert_time_utc DESC, e.event_id DESC
            LIMIT %s
            """,
            params,
        ).fetchall()
    ]


def _load_prior_events(conn, start: datetime, end: datetime) -> list[Dict[str, Any]]:
    return [
        dict(row)
        for row in conn.execute(
            """
            SELECT event_id, alert_time_utc, symbol, direction, event_type, setup_key
            FROM research_events
            WHERE event_kind='ALERT' AND delivery_status='DELIVERED'
              AND alert_time_utc >= %s AND alert_time_utc <= %s
            ORDER BY alert_time_utc ASC, event_id ASC
            """,
            (start, end),
        ).fetchall()
    ]


def _load_raw_rows(
    conn, *, symbols: Sequence[str], start: datetime, end: datetime
) -> tuple[list[Dict[str, Any]], list[Dict[str, Any]], list[Dict[str, Any]]]:
    price_rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT symbol, candle_time, price_close, oi_close_usd,
                   price_exchange, price_pair, source
            FROM oi_price_history
            WHERE symbol=ANY(%s) AND candle_time >= %s AND candle_time <= %s
            ORDER BY symbol, candle_time
            """,
            (list(symbols), start, end),
        ).fetchall()
    ]
    # ``oi_price_history`` is the historical/backfill archive.  The running
    # bot persists newer Price/OI observations in ``oi_regime_snapshots``.
    # Treat the live table as an additive source so research remains complete
    # after the backfill endpoint, while _prior_point continues to enforce a
    # strict at-or-before-alert join (no future leakage).
    if _table_exists(conn, "oi_regime_snapshots"):
        price_rows.extend(
            dict(row)
            for row in conn.execute(
                """
                SELECT symbol, collected_at AS candle_time,
                       price AS price_close,
                       open_interest_usd AS oi_close_usd,
                       price_source AS price_exchange,
                       (symbol || 'USDT') AS price_pair,
                       ('oi_regime_snapshots:' || COALESCE(oi_source, 'unknown')) AS source
                FROM oi_regime_snapshots
                WHERE symbol=ANY(%s)
                  AND collected_at >= %s AND collected_at <= %s
                  AND data_quality_status IN ('PASS', 'WARNING')
                ORDER BY symbol, collected_at
                """,
                (list(symbols), start, end),
            ).fetchall()
        )
        # Stable sorting means a live snapshot wins an exact-timestamp tie
        # with the older backfill row that was appended first.
        price_rows.sort(
            key=lambda row: (
                str(row.get("symbol") or ""),
                _as_utc(row["candle_time"]),
            )
        )
    flow_rows: list[list[Dict[str, Any]]] = []
    for table in ("futures_taker_history", "spot_taker_history"):
        flow_rows.append(
            [
                dict(row)
                for row in conn.execute(
                    f"""
                    SELECT symbol, candle_time, buy_volume_usd, sell_volume_usd,
                           api_cum_vol_delta_usd, continuous_cum_vol_delta_usd,
                           exchange_list, source
                    FROM {table}
                    WHERE symbol=ANY(%s) AND candle_time >= %s AND candle_time <= %s
                    ORDER BY symbol, candle_time
                    """,
                    (list(symbols), start, end),
                ).fetchall()
            ]
        )
    return price_rows, flow_rows[0], flow_rows[1]


def research_feature_matrix(
    *,
    symbol: Any = None,
    event_type: Any = None,
    direction: Any = None,
    lookback_days: int = 90,
    horizon_minutes: int = 240,
    window_profile: str = "core",
    limit: int = 15,
) -> Dict[str, Any]:
    """Return bounded raw/model/label rows for formula discovery."""
    normalized_symbol = _symbol(symbol)
    normalized_event_type = _event_type(event_type)
    normalized_direction = str(direction or "").strip().upper() or None
    if normalized_direction not in {None, "LONG", "SHORT"}:
        raise ValueError("direction must be LONG, SHORT or null")
    days = max(1, min(int(lookback_days), 3650))
    horizon = int(horizon_minutes)
    if horizon not in {60, 240, 720, 1440}:
        raise ValueError("horizon_minutes must be 60, 240, 720 or 1440")
    profile = str(window_profile or "core").strip().lower()
    if profile not in {"core", "extended"}:
        raise ValueError("window_profile must be core or extended")
    row_limit = max(1, min(int(limit), 25))
    windows = CORE_WINDOWS_MINUTES if profile == "core" else EXTENDED_WINDOWS_MINUTES

    research_url = _research_database_url()
    raw_url = _raw_database_url()
    if not research_url:
        return {"available": False, "reason": "research archive is not configured"}
    if not raw_url:
        return {"available": False, "reason": "raw market archive DATABASE_URL is not configured"}

    with _connect(research_url) as research_conn:
        required_research = ("research_events", "research_alert_outcomes")
        missing_research = [
            table for table in required_research if not _table_exists(research_conn, table)
        ]
        if missing_research:
            return {
                "available": False,
                "reason": "research archive schema is incomplete",
                "missing_tables": missing_research,
            }
        events = _load_verified_events(
            research_conn,
            symbol=normalized_symbol,
            event_type=normalized_event_type,
            direction=normalized_direction,
            lookback_days=days,
            horizon_minutes=horizon,
            limit=row_limit,
        )
        if not events:
            return {
                "available": True,
                "scope": {
                    "symbol": normalized_symbol or "ALL",
                    "event_type": normalized_event_type or "ALL",
                    "direction": normalized_direction or "BOTH",
                    "lookback_days": days,
                    "horizon_minutes": horizon,
                    "window_profile": profile,
                },
                "rows": [],
                "reason": "no verified delivered-alert outcomes matched the scope",
            }
        minimum_event_time = min(_as_utc(row["alert_time_utc"]) for row in events)
        maximum_event_time = max(_as_utc(row["alert_time_utc"]) for row in events)
        prior_events = _load_prior_events(
            research_conn,
            minimum_event_time - timedelta(minutes=max(SEQUENCE_WINDOWS_MINUTES)),
            maximum_event_time,
        )

    symbols = sorted({str(row["symbol"]).upper() for row in events})
    raw_start = minimum_event_time - timedelta(
        minutes=max(windows) + MAX_POINT_AGE_MINUTES
    )
    with _connect(raw_url) as raw_conn:
        required_raw = ("oi_price_history", "futures_taker_history", "spot_taker_history")
        missing_raw = [table for table in required_raw if not _table_exists(raw_conn, table)]
        if missing_raw:
            return {
                "available": False,
                "reason": "raw market archive schema is incomplete",
                "missing_tables": missing_raw,
            }
        price_rows, futures_rows, spot_rows = _load_raw_rows(
            raw_conn,
            symbols=symbols,
            start=raw_start,
            end=maximum_event_time,
        )

    matrix_rows = build_feature_rows(
        events,
        price_oi_rows=price_rows,
        futures_rows=futures_rows,
        spot_rows=spot_rows,
        prior_events=prior_events,
        windows_minutes=windows,
    )
    complete_counts = {
        f"{minutes}m": sum(
            1
            for row in matrix_rows
            if row["raw_features"]["windows"][f"{minutes}m"]["complete"]
        )
        for minutes in windows
    }

    return _json_safe(
        {
            "available": True,
            "feature_schema_version": FEATURE_SCHEMA_VERSION,
            "scope": {
                "symbol": normalized_symbol or "ALL",
                "event_type": normalized_event_type or "ALL",
                "direction": normalized_direction or "BOTH",
                "lookback_days": days,
                "horizon_minutes": horizon,
                "window_profile": profile,
                "windows_minutes": list(windows),
                "verified_outcomes_only": True,
            },
            "sample_size": len(matrix_rows),
            "complete_raw_windows": complete_counts,
            "rows": matrix_rows,
            "research_contract": [
                "Every raw feature uses the newest stored point at or before alert_time_utc; future points are never eligible.",
                f"A raw point more than {MAX_POINT_AGE_MINUTES} minutes old is returned as missing instead of being silently joined.",
                "model_features come only from the immutable decision-time Research Event snapshot.",
                "outcome_label is later Binance Spot evidence and must never be used as an input feature.",
                "Rows expose raw and existing-model features side by side; current bot scores are candidates for comparison, not assumed truth.",
                "The matrix is a discovery sample. Candidate formulas still require chronological holdout and out-of-sample validation.",
            ],
        }
    )
