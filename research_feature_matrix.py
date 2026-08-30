"""Versioned, no-lookahead feature rows for alert-formula research.

The matrix keeps three different kinds of information separate:

* raw market measurements that existed at or before the alert;
* compact model/score state captured inside the immutable Research Event;
* verified post-alert canonical spot outcomes used only as labels.

Nothing in this module writes to PostgreSQL or changes a live alert rule.  Raw
time-series rows remain in their existing archive tables and are joined in
bounded batches when a research request is made.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import os
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

import canonical_price_path
import market_session_baseline
import research_historical_replay
import research_max_pain_archive
import research_no_dwell_outcome
import research_prospective_anchors
import research_session_width

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - validated at runtime
    psycopg = None
    dict_row = None


FEATURE_SCHEMA_VERSION = (
    "research-feature-matrix-v8-prospective-max-pain-frozen"
)
PROSPECTIVE_FROZEN_INPUT_POLICY_VERSION = (
    "prospective-frozen-anchor-slot-series-v2-max-pain"
)
PROSPECTIVE_ANCHOR_SAMPLER_VERSION = (
    research_prospective_anchors.SAMPLER_VERSION
)
VERIFIED_OUTCOME_METHOD = research_no_dwell_outcome.METHOD_VERSION
VERIFIED_OUTCOME_QUALITIES = canonical_price_path.COMPLETE_QUALITIES
# Compatibility alias for callers that persist one textual dataset contract.
VERIFIED_OUTCOME_QUALITY = ",".join(VERIFIED_OUTCOME_QUALITIES)

CORE_WINDOWS_MINUTES: tuple[int, ...] = (30, 60, 240, 720, 1440)
EXTENDED_WINDOWS_MINUTES: tuple[int, ...] = CORE_WINDOWS_MINUTES + (2880, 4320, 10080)
SEQUENCE_WINDOWS_MINUTES: tuple[int, ...] = (30, 120, 360)

# The bot stores 30-minute historical market rows.  A prior-only point older
# than this hard ceiling is reported missing instead of silently joining stale
# evidence to an alert.
MAX_POINT_AGE_MINUTES = 45
# Historical comparisons use only observations that existed before the alert,
# from the same symbol and with an ACTIVE/WEEKEND composition similar to the
# current window.  The session contract is shared with the production bot and
# is evaluated in America/New_York, including DST transitions.
HISTORICAL_BASELINE_DAYS = research_session_width.LOOKBACK_DAYS
HISTORICAL_BASELINE_MIN_SAMPLES = research_session_width.MIN_EFFECTIVE_SAMPLES
HISTORICAL_BASELINE_COMPOSITION_TOLERANCE = (
    research_session_width.COMPOSITION_TOLERANCE
)
HISTORICAL_BASELINE_FEATURES: tuple[str, ...] = (
    "price_change_pct",
    "oi_change_pct",
    "futures_continuous_cvd_change_usd",
    "spot_continuous_cvd_change_usd",
)
REPLAY_MIN_ANCHORS_PER_SYMBOL = 250
REPLAY_MIN_UTC_DATES_PER_SYMBOL = 14
REPLAY_MIN_SPAN_HOURS_PER_SYMBOL = 336.0
REPLAY_MIN_ELIGIBLE_SYMBOLS = 4
REPLAY_COVERAGE_STREAM_BATCH_SIZE = 500
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


def _latest_price_oi(
    price_row: Optional[Mapping[str, Any]],
    price_age: Optional[float],
    oi_row: Optional[Mapping[str, Any]] = None,
    oi_age: Optional[float] = None,
) -> Dict[str, Any]:
    oi_row = oi_row if oi_row is not None else price_row
    oi_age = oi_age if oi_age is not None else price_age
    if not price_row and not oi_row:
        return {"available": False, "age_minutes": None}
    return {
        "available": bool(price_row and oi_row),
        "timestamp_utc": price_row.get("candle_time") if price_row else None,
        "age_minutes": price_age,
        "price_timestamp_utc": (
            price_row.get("candle_time") if price_row else None
        ),
        "price_age_minutes": price_age,
        "oi_timestamp_utc": oi_row.get("candle_time") if oi_row else None,
        "oi_age_minutes": oi_age,
        "price_close": _round(
            price_row.get("price_close") if price_row else None
        ),
        "oi_close_usd": _round(
            oi_row.get("oi_close_usd") if oi_row else None, 2
        ),
        "price_exchange": (
            price_row.get("price_exchange") if price_row else None
        ),
        "price_pair": price_row.get("price_pair") if price_row else None,
        "price_source": price_row.get("source") if price_row else None,
        "oi_source": oi_row.get("source") if oi_row else None,
        "source": price_row.get("source") if price_row else None,
        "price_market": price_row.get("price_market") if price_row else None,
        "price_instrument_id": (
            price_row.get("price_instrument_id") if price_row else None
        ),
        "price_timeframe": (
            price_row.get("price_timeframe") if price_row else None
        ),
        "price_interval_seconds": (
            price_row.get("price_interval_seconds") if price_row else None
        ),
        "canonical_price_method_version": (
            price_row.get("canonical_price_method_version")
            if price_row
            else None
        ),
        "canonical_price_provenance_version": (
            price_row.get("canonical_price_provenance_version")
            if price_row
            else None
        ),
        "canonical_price_provenance": (
            price_row.get("canonical_price_provenance") if price_row else None
        ),
        "prospective_anchor_slot_id": (
            price_row.get("prospective_anchor_slot_id") if price_row else None
        ),
        "prospective_input_fingerprint": (
            price_row.get("prospective_input_fingerprint")
            if price_row
            else None
        ),
        "prospective_slot_created_at_utc": (
            price_row.get("prospective_slot_created_at_utc")
            if price_row
            else None
        ),
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
        "prospective_anchor_slot_id": row.get("prospective_anchor_slot_id"),
        "prospective_input_fingerprint": row.get(
            "prospective_input_fingerprint"
        ),
        "prospective_slot_created_at_utc": row.get(
            "prospective_slot_created_at_utc"
        ),
    }


def _window_features(
    *,
    event_time: datetime,
    minutes: int,
    price_series: Optional[_Series],
    futures_series: Optional[_Series],
    spot_series: Optional[_Series],
    oi_series: Optional[_Series] = None,
) -> Dict[str, Any]:
    reference_time = event_time - timedelta(minutes=minutes)
    active_ratio, weekend_ratio, session_segments = (
        market_session_baseline.session_ratios(reference_time, event_time)
    )
    current_price, current_price_age = _prior_point(price_series, event_time)
    prior_price, prior_price_age = _prior_point(price_series, reference_time)
    effective_oi_series = oi_series if oi_series is not None else price_series
    current_oi, current_oi_age = _prior_point(effective_oi_series, event_time)
    prior_oi, prior_oi_age = _prior_point(effective_oi_series, reference_time)
    current_futures, current_futures_age = _prior_point(futures_series, event_time)
    prior_futures, prior_futures_age = _prior_point(futures_series, reference_time)
    current_spot, current_spot_age = _prior_point(spot_series, event_time)
    prior_spot, prior_spot_age = _prior_point(spot_series, reference_time)

    price_change = _pct_change(
        current_price.get("price_close") if current_price else None,
        prior_price.get("price_close") if prior_price else None,
    )
    oi_change = _pct_change(
        current_oi.get("oi_close_usd") if current_oi else None,
        prior_oi.get("oi_close_usd") if prior_oi else None,
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
        "session_active_ratio": round(active_ratio, 6),
        "session_weekend_ratio": round(weekend_ratio, 6),
        "session_segments": session_segments,
        "session_composition": _session_composition_label(active_ratio),
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
            "oi_current": current_oi_age,
            "oi_reference": prior_oi_age,
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
    # Prospective sampler metadata is an audit envelope, not a bot model
    # prediction. Exposing it as ``model.*`` would let formula discovery or
    # Shadow evaluation consume coverage/status/hash fields as market inputs.
    flattened = (
        {}
        if isinstance(snapshot.get("prospective_anchor"), Mapping)
        else _flatten_snapshot(snapshot)
    )
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
    market_session = (
        "ACTIVE"
        if market_session_baseline.is_active_market(timestamp)
        else "WEEKEND"
    )
    return {
        "utc_hour": hour,
        "utc_weekday": timestamp.weekday(),
        "utc_weekday_name": timestamp.strftime("%A").upper(),
        "is_calendar_weekend_utc": timestamp.weekday() >= 5,
        "is_market_weekend": market_session == "WEEKEND",
        "market_session": market_session,
        "market_regime": market_session,
        "market_session_timezone": "America/New_York",
        "market_session_definition": "SUN_18_ET__FRI_20_ET_ACTIVE",
        "fixed_utc_session_bucket": bucket,
        **market_session_baseline.market_time_features(timestamp),
    }


def _closed_archive_rows(
    rows: Iterable[Mapping[str, Any]],
) -> list[Dict[str, Any]]:
    """Expose CoinGlass interval-open rows only after close plus grace.

    Historical backfills make old rows visible in the database long after the
    fact.  Treating their opening timestamp as the decision-time availability
    would leak up to 30 minutes of future information into replay research.
    """
    output: list[Dict[str, Any]] = []
    for source in rows:
        row = dict(source)
        raw_time = _as_utc(row["candle_time"])
        row["source_candle_time"] = raw_time
        row["candle_time"] = market_session_baseline.closed_candle_available_at(
            raw_time
        )
        output.append(row)
    return output


@dataclass(frozen=True)
class _HistoricalWindowSeries:
    times: tuple[datetime, ...]
    values: tuple[Dict[str, float], ...]
    active_ratios: tuple[float, ...]


def _session_composition_label(active_ratio: float) -> str:
    return research_session_width.session_composition_label(active_ratio)


def _weighted_percentile_rank(
    value: Any, population: Sequence[tuple[float, float]]
) -> Optional[float]:
    number = _float(value)
    cleaned = [
        (float(item), float(weight))
        for item, weight in population
        if _float(item) is not None
        and _float(weight) is not None
        and float(weight) > 0.0
    ]
    if number is None or not cleaned:
        return None
    total = sum(weight for _, weight in cleaned)
    if total <= 0.0:
        return None
    below = sum(weight for item, weight in cleaned if item < number)
    equal = sum(weight for item, weight in cleaned if item == number)
    return round((below + 0.5 * equal) / total * 100.0, 4)


def _historical_window_index(
    *,
    price_series: Mapping[str, _Series],
    oi_series: Optional[Mapping[str, _Series]] = None,
    futures_series: Mapping[str, _Series],
    spot_series: Mapping[str, _Series],
    windows_minutes: Sequence[int],
) -> Dict[tuple[str, int], _HistoricalWindowSeries]:
    """Precompute raw window changes at archived points without outcomes.

    Each point is derived only from the point timestamp and older raw market
    rows.  Later, an alert receives only the prefix strictly before its current
    Price/OI observation, which keeps historical percentile features free of
    lookahead and excludes the alert's own observation from its baseline.
    """
    grouped: Dict[
        tuple[str, int], list[tuple[datetime, Dict[str, float], float]]
    ] = defaultdict(list)
    for symbol, prices in price_series.items():
        anchor_times = tuple(
            _as_utc(row.get("decision_time_utc"))
            if row.get("decision_time_utc") is not None
            else point_time
            for point_time, row in zip(prices.times, prices.rows)
        )
        for minutes in windows_minutes:
            for anchor_time in anchor_times:
                window = _window_features(
                    event_time=anchor_time,
                    minutes=int(minutes),
                    price_series=prices,
                    oi_series=(oi_series or price_series).get(symbol),
                    futures_series=futures_series.get(symbol),
                    spot_series=spot_series.get(symbol),
                )
                values = {
                    feature: number
                    for feature in HISTORICAL_BASELINE_FEATURES
                    if (number := _float(window.get(feature))) is not None
                }
                if values:
                    grouped[(symbol, int(minutes))].append(
                        (
                            anchor_time,
                            values,
                            float(window["session_active_ratio"]),
                        )
                    )
    return {
        key: _HistoricalWindowSeries(
            times=tuple(item[0] for item in points),
            values=tuple(item[1] for item in points),
            active_ratios=tuple(item[2] for item in points),
        )
        for key, points in grouped.items()
    }


def _historical_context(
    *,
    symbol: str,
    event_time: datetime,
    current_price_row: Optional[Mapping[str, Any]],
    windows: Mapping[str, Mapping[str, Any]],
    historical_index: Mapping[tuple[str, int], _HistoricalWindowSeries],
) -> Dict[str, Any]:
    cutoff = (
        _as_utc(current_price_row["candle_time"])
        if current_price_row and current_price_row.get("candle_time") is not None
        else _as_utc(event_time)
    )
    start = cutoff - timedelta(days=HISTORICAL_BASELINE_DAYS)
    window_context: Dict[str, Any] = {}
    for window_name, current in windows.items():
        minutes = int(str(window_name).removesuffix("m"))
        historical = historical_index.get((symbol, minutes))
        current_active_ratio = float(current.get("session_active_ratio") or 0.0)
        if historical is None:
            points: Sequence[Mapping[str, float]] = ()
            point_active_ratios: Sequence[float] = ()
        else:
            left = bisect_left(historical.times, start)
            right = bisect_left(historical.times, cutoff)
            points = historical.values[left:right]
            point_active_ratios = historical.active_ratios[left:right]

        stats: Dict[str, Any] = {
            "session_active_ratio": round(current_active_ratio, 6),
            "session_weekend_ratio": round(1.0 - current_active_ratio, 6),
            "session_composition": _session_composition_label(
                current_active_ratio
            ),
            "prior_points": len(points),
        }
        effective_counts: list[float] = []
        for feature in HISTORICAL_BASELINE_FEATURES:
            population = [
                (float(point[feature]), float(active_ratio))
                for point, active_ratio in zip(points, point_active_ratios)
                if feature in point
            ]
            weighted = market_session_baseline.composition_weighted_values(
                population,
                current_active_ratio,
                HISTORICAL_BASELINE_COMPOSITION_TOLERANCE,
            )
            effective_samples = sum(weight for _, weight in weighted)
            effective_counts.append(effective_samples)
            current_value = _float(current.get(feature))
            enough = effective_samples >= HISTORICAL_BASELINE_MIN_SAMPLES
            stats[f"{feature}_history_samples"] = len(population)
            stats[f"{feature}_session_matched_samples"] = len(weighted)
            stats[f"{feature}_session_matched_effective_samples"] = round(
                effective_samples, 4
            )
            stats[f"{feature}_percentile_session_matched"] = (
                _weighted_percentile_rank(current_value, weighted)
                if enough
                else None
            )
            stats[f"{feature}_abs_percentile_session_matched"] = (
                _weighted_percentile_rank(
                    abs(current_value) if current_value is not None else None,
                    [(abs(value), weight) for value, weight in weighted],
                )
                if enough
                else None
            )
            stats[f"{feature}_median_session_matched"] = (
                _round(
                    market_session_baseline.weighted_percentile(weighted, 0.5),
                    6,
                )
                if enough
                else None
            )
            stats[f"{feature}_abs_median_session_matched"] = (
                _round(
                    market_session_baseline.weighted_percentile(
                        [(abs(value), weight) for value, weight in weighted],
                        0.5,
                    ),
                    6,
                )
                if enough
                else None
            )
        stats["sufficient_history"] = bool(effective_counts) and all(
            count >= HISTORICAL_BASELINE_MIN_SAMPLES
            for count in effective_counts
        )
        window_context[window_name] = stats
    return {
        "event_market_session": (
            "ACTIVE"
            if market_session_baseline.is_active_market(event_time)
            else "WEEKEND"
        ),
        "policy": "same-symbol exact ACTIVE/WEEKEND composition matched prior-only",
        "session_timezone": "America/New_York",
        "session_definition": "SUN_18_ET__FRI_20_ET_ACTIVE",
        "composition_tolerance": HISTORICAL_BASELINE_COMPOSITION_TOLERANCE,
        "lookback_days": HISTORICAL_BASELINE_DAYS,
        "minimum_samples": HISTORICAL_BASELINE_MIN_SAMPLES,
        "cutoff_time_utc": cutoff,
        "windows": window_context,
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


def _movement_width_reference(
    *,
    event: Mapping[str, Any],
    symbol: str,
    event_time: datetime,
    current_price_row: Optional[Mapping[str, Any]],
    historical_index: Mapping[
        tuple[str, int], research_session_width.PriceWidthSeries
    ],
) -> Dict[str, Any]:
    """Delegate to the replay-shared prior-only movement-width policy."""
    horizon = int(event.get("horizon_minutes") or 0)
    cutoff = (
        _as_utc(current_price_row["candle_time"])
        if current_price_row and current_price_row.get("candle_time") is not None
        else event_time
    )
    return research_session_width.movement_width_reference(
        symbol=symbol,
        event_time=event_time,
        horizon_minutes=horizon,
        as_of_utc=cutoff,
        historical_index=historical_index,
        lookback_days=HISTORICAL_BASELINE_DAYS,
        minimum_effective_samples=HISTORICAL_BASELINE_MIN_SAMPLES,
        composition_tolerance=HISTORICAL_BASELINE_COMPOSITION_TOLERANCE,
    )


def _outcome_label(
    event: Mapping[str, Any],
    *,
    symbol: str,
    event_time: datetime,
    current_price_row: Optional[Mapping[str, Any]],
    historical_index: Mapping[
        tuple[str, int], research_session_width.PriceWidthSeries
    ],
) -> Dict[str, Any]:
    horizon = int(event.get("horizon_minutes") or 0)
    active_ratio, weekend_ratio, segments = market_session_baseline.session_ratios(
        event_time,
        event_time + timedelta(minutes=max(0, horizon)),
    )
    return {
        "horizon_minutes": horizon,
        "session_active_ratio": round(active_ratio, 6),
        "session_weekend_ratio": round(weekend_ratio, 6),
        "session_segments": segments,
        "session_composition": _session_composition_label(active_ratio),
        "movement_width_reference": _movement_width_reference(
            event=event,
            symbol=symbol,
            event_time=event_time,
            current_price_row=current_price_row,
            historical_index=historical_index,
        ),
        "measured_at_utc": event.get("measured_at_utc"),
        "reference_price": _round(event.get("reference_price")),
        "price_at_horizon": _round(event.get("price_at_horizon")),
        "raw_return_pct": _round(event.get("raw_return_pct")),
        "directional_return_pct": _round(event.get("directional_return_pct")),
        "path_success": event.get("path_success"),
        "first_touch_status": event.get("first_touch_status"),
        "first_qualifying_move_time_utc": event.get(
            "first_qualifying_move_time_utc"
        ),
        "time_to_first_qualifying_move_seconds": event.get(
            "time_to_first_qualifying_move_seconds"
        ),
        "qualifying_move_threshold_pct": _round(
            event.get("qualifying_move_threshold_pct")
        ),
        "threshold_scale_factor": _round(event.get("threshold_scale_factor")),
        "pre_qualifying_mae_pct": _round(event.get("pre_qualifying_mae_pct")),
        "qualifying_candle_order_ambiguous": event.get(
            "qualifying_candle_order_ambiguous"
        ),
        "mfe_pct": _round(event.get("mfe_pct")),
        # Formula risk uses adverse movement only through the qualifying first
        # touch (or through horizon close for a MISS).  The full-horizon value
        # remains visible as a diagnostic and never cancels a prior HIT.
        "mae_pct": _round(
            event.get("pre_qualifying_mae_pct", event.get("mae_pct"))
        ),
        "full_horizon_mae_pct": _round(event.get("mae_pct")),
        "time_to_first_progress_seconds": event.get(
            "time_to_first_qualifying_move_seconds",
            event.get("time_to_first_progress_seconds"),
        ),
        "time_to_mfe_seconds": event.get("time_to_mfe_seconds"),
        "time_to_closest_target_seconds": event.get("time_to_closest_target_seconds"),
        "time_to_target_seconds": event.get("time_to_target_seconds"),
        "target_progress_ratio": _round(event.get("target_progress_ratio")),
        "target_reached": event.get("target_reached"),
        "path_samples": event.get("path_samples"),
        "outcome_method_version": event.get("outcome_method_version"),
        "legacy_outcome_method_version": event.get(
            "legacy_outcome_method_version"
        ),
        "data_quality_status": event.get("data_quality_status"),
    }


def build_feature_rows(
    events: Sequence[Mapping[str, Any]],
    *,
    price_oi_rows: Iterable[Mapping[str, Any]],
    oi_rows: Optional[Iterable[Mapping[str, Any]]] = None,
    futures_rows: Iterable[Mapping[str, Any]],
    spot_rows: Iterable[Mapping[str, Any]],
    prior_events: Sequence[Mapping[str, Any]],
    windows_minutes: Sequence[int] = CORE_WINDOWS_MINUTES,
    max_pain_by_event_id: Optional[Mapping[int, Mapping[str, Any]]] = None,
) -> list[Dict[str, Any]]:
    """Pure deterministic builder used by the DB wrapper and self-tests."""
    shared_price_oi_rows = list(price_oi_rows)
    independent_oi_rows = (
        list(oi_rows) if oi_rows is not None else shared_price_oi_rows
    )
    price_series = _prepare_series(
        shared_price_oi_rows, time_column="candle_time"
    )
    oi_series = _prepare_series(independent_oi_rows, time_column="candle_time")
    futures_series = _prepare_series(futures_rows, time_column="candle_time")
    spot_series = _prepare_series(spot_rows, time_column="candle_time")
    historical_index = _historical_window_index(
        price_series=price_series,
        oi_series=oi_series,
        futures_series=futures_series,
        spot_series=spot_series,
        windows_minutes=windows_minutes,
    )
    width_horizons = sorted(
        {
            int(value)
            for value in windows_minutes
            if int(value) > 0
        }
        | {
            int(event.get("horizon_minutes") or 0)
            for event in events
            if int(event.get("horizon_minutes") or 0) > 0
        }
    )
    width_index = research_session_width.build_price_width_index(
        price_points={
            symbol: tuple(
                (point_time, row.get("price_close"))
                for point_time, row in zip(series.times, series.rows)
            )
            for symbol, series in price_series.items()
        },
        horizons_minutes=width_horizons,
        max_point_age_minutes=MAX_POINT_AGE_MINUTES,
    )
    rows: list[Dict[str, Any]] = []

    for source_event in events:
        event = dict(source_event)
        event_time = _as_utc(event["alert_time_utc"])
        symbol = str(event.get("symbol") or "").upper()
        current_price, current_price_age = _prior_point(price_series.get(symbol), event_time)
        current_oi, current_oi_age = _prior_point(oi_series.get(symbol), event_time)
        current_futures, current_futures_age = _prior_point(futures_series.get(symbol), event_time)
        current_spot, current_spot_age = _prior_point(spot_series.get(symbol), event_time)

        windows = {
            f"{minutes}m": _window_features(
                event_time=event_time,
                minutes=int(minutes),
                price_series=price_series.get(symbol),
                oi_series=oi_series.get(symbol),
                futures_series=futures_series.get(symbol),
                spot_series=spot_series.get(symbol),
            )
            for minutes in windows_minutes
        }
        historical_context = _historical_context(
            symbol=symbol,
            event_time=event_time,
            current_price_row=current_price,
            windows=windows,
            historical_index=historical_index,
        )
        event_id = int(event.get("event_id") or 0)
        max_pain = (
            dict((max_pain_by_event_id or {}).get(event_id) or {})
            if event_id
            else {}
        )
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
                        "price_oi": _latest_price_oi(
                            current_price,
                            current_price_age,
                            current_oi,
                            current_oi_age,
                        ),
                        "futures_cvd": _latest_flow(current_futures, current_futures_age),
                        "spot_cvd": _latest_flow(current_spot, current_spot_age),
                    },
                    "windows": windows,
                },
                "historical_context": historical_context,
                "model_features": _model_features(event),
                "sequence_features": _sequence_features(event, prior_events),
                "max_pain_features": max_pain,
                "outcome_label": _outcome_label(
                    event,
                    symbol=symbol,
                    event_time=event_time,
                    current_price_row=current_price,
                    historical_index=width_index,
                ),
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
        list(VERIFIED_OUTCOME_QUALITIES),
        canonical_price_path.METHOD_VERSION,
        list(VERIFIED_OUTCOME_QUALITIES),
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
                   ft.horizon_minutes, o.measured_at_utc, o.reference_price,
                   o.price_at_horizon, o.raw_return_pct,
                   o.directional_return_pct, o.mfe_pct, o.mae_pct,
                   ft.success AS path_success,
                   ft.status AS first_touch_status,
                   ft.first_qualifying_move_time_utc,
                   ft.time_to_first_qualifying_move_seconds,
                   ft.qualifying_move_threshold_pct,
                   ft.threshold_scale_factor,
                   ft.pre_qualifying_mae_pct,
                   ft.qualifying_candle_order_ambiguous,
                   ft.time_to_first_qualifying_move_seconds
                     AS time_to_first_progress_seconds,
                   o.time_to_mfe_seconds,
                   o.time_to_closest_target_seconds, o.time_to_target_seconds,
                   o.target_progress_ratio, o.target_reached, o.path_samples,
                   ft.method_version AS outcome_method_version,
                   o.outcome_method_version AS legacy_outcome_method_version,
                   ft.data_quality_status
            FROM research_events e
            JOIN research_first_touch_outcomes ft ON ft.event_id=e.event_id
            JOIN research_alert_outcomes o
              ON o.event_id=e.event_id
             AND o.horizon_minutes=ft.horizon_minutes
            WHERE e.event_kind='ALERT'
              AND e.delivery_status='DELIVERED'
              AND e.alert_time_utc >= NOW() - (%s * INTERVAL '1 day')
              AND ft.horizon_minutes=%s
              AND ft.method_version=%s
              AND ft.status IN ('HIT', 'MISS')
              AND ft.data_quality_status=ANY(%s)
              AND o.outcome_method_version=%s
              AND o.data_quality_status=ANY(%s)
              {filters}
            ORDER BY e.alert_time_utc DESC, e.event_id DESC
            LIMIT %s
            """,
            params,
        ).fetchall()
    ]


def _load_delivered_events_by_id(
    conn, event_ids: Sequence[int]
) -> list[Dict[str, Any]]:
    """Load immutable eligible decision rows without joining a later outcome.

    Silent samples are accepted only through the migration-008 authority view;
    an arbitrary ``DECISION_SAMPLE`` row is never enough.
    """
    normalized = sorted({int(event_id) for event_id in event_ids if int(event_id) > 0})
    if not normalized:
        return []
    return [
        dict(row)
        for row in conn.execute(
            """
            SELECT event_id, event_kind, alert_time_utc, symbol, direction,
                   source_side, timeframe, event_type, score,
                   current_price, target_price, initial_target_distance_pct,
                   categories, setup_key, strategy_version, code_version,
                   engine_snapshot
            FROM research_events
            WHERE event_id=ANY(%s)
              AND (
                (event_kind='ALERT' AND delivery_status='DELIVERED')
                OR (
                  event_kind='DECISION_SAMPLE'
                  AND delivery_status='NOT_APPLICABLE'
                  AND EXISTS (
                    SELECT 1 FROM research_prospective_shadow_events authorized
                    WHERE authorized.event_id=research_events.event_id
                  )
                )
              )
            ORDER BY alert_time_utc ASC, event_id ASC
            """,
            (normalized,),
        ).fetchall()
    ]


def _load_max_pain_features_batch(
    conn, events: Sequence[Mapping[str, Any]]
) -> Dict[int, Dict[str, Any]]:
    """Load at most two prior eligible Max-Pain sets per decision in batches.

    The migration-007 archive is the only source.  The legacy
    ``max_pain_snapshots`` table is intentionally never named or queried here.
    """
    event_requests: Dict[int, tuple[str, datetime]] = {}
    for event in events:
        event_id = int(event.get("event_id") or 0)
        symbol = str(event.get("symbol") or "").strip().upper()
        decision = event.get("alert_time_utc")
        if event_id and symbol and decision is not None:
            event_requests[event_id] = (symbol, _as_utc(decision))
    if not event_requests:
        return {}
    required = (
        "research_max_pain_snapshot_sets",
        "research_max_pain_snapshot_symbols",
        "research_max_pain_snapshot_rows",
    )
    if any(not _table_exists(conn, table) for table in required):
        return {
            event_id: {
                "evaluation_status": "UNEVALUABLE",
                "reason": "migration 007 Max-Pain archive schema is unavailable",
                "features": {},
            }
            for event_id in event_requests
        }

    unique_requests = sorted(set(event_requests.values()), key=lambda item: (item[1], item[0]))
    request_index = {request: index + 1 for index, request in enumerate(unique_requests)}
    symbols = [request[0] for request in unique_requests]
    decision_times = [request[1] for request in unique_requests]
    candidate_rows = conn.execute(
        """
        WITH requested AS (
          SELECT symbol, decision_time_utc, request_id
          FROM UNNEST(%s::text[], %s::timestamptz[])
               WITH ORDINALITY AS item(symbol, decision_time_utc, request_id)
        )
        SELECT requested.request_id, requested.symbol AS requested_symbol,
               requested.decision_time_utc, chosen.*
        FROM requested
        CROSS JOIN LATERAL (
          SELECT snapshot.*, ROW_NUMBER() OVER (
                   ORDER BY snapshot.available_at_utc DESC,
                            snapshot.snapshot_set_id DESC
                 ) AS candidate_rank
          FROM research_max_pain_snapshot_sets snapshot
          JOIN research_max_pain_snapshot_symbols manifest
            ON manifest.snapshot_set_id=snapshot.snapshot_set_id
           AND manifest.symbol=requested.symbol
           AND manifest.research_eligible=TRUE
          WHERE snapshot.research_eligible=TRUE
            AND snapshot.method_version=%s
            AND snapshot.cutover_marker=%s
            AND snapshot.available_at_utc<=requested.decision_time_utc
            AND snapshot.created_at_utc<=requested.decision_time_utc
          ORDER BY snapshot.available_at_utc DESC, snapshot.snapshot_set_id DESC
          LIMIT 2
        ) chosen
        ORDER BY requested.request_id, chosen.candidate_rank
        """,
        (
            symbols,
            decision_times,
            research_max_pain_archive.METHOD_VERSION,
            research_max_pain_archive.CUTOVER_MARKER,
        ),
    ).fetchall()
    candidates_by_request: Dict[int, list[Dict[str, Any]]] = defaultdict(list)
    set_symbol_pairs: set[tuple[int, str]] = set()
    for source in candidate_rows:
        row = dict(source)
        request_id = int(row.pop("request_id"))
        requested_symbol = str(row.pop("requested_symbol"))
        row.pop("decision_time_utc", None)
        row.pop("candidate_rank", None)
        candidates_by_request[request_id].append(row)
        set_symbol_pairs.add((int(row["snapshot_set_id"]), requested_symbol))

    manifests: Dict[tuple[int, str], Dict[str, Any]] = {}
    rows_by_pair: Dict[tuple[int, str], list[Dict[str, Any]]] = defaultdict(list)
    if set_symbol_pairs:
        set_ids = sorted({pair[0] for pair in set_symbol_pairs})
        requested_symbols = sorted({pair[1] for pair in set_symbol_pairs})
        for source in conn.execute(
            """
            SELECT * FROM research_max_pain_snapshot_symbols
            WHERE snapshot_set_id=ANY(%s) AND symbol=ANY(%s)
            """,
            (set_ids, requested_symbols),
        ).fetchall():
            row = dict(source)
            key = (int(row["snapshot_set_id"]), str(row["symbol"]).upper())
            if key in set_symbol_pairs:
                manifests[key] = row
        for source in conn.execute(
            """
            SELECT * FROM research_max_pain_snapshot_rows
            WHERE snapshot_set_id=ANY(%s) AND symbol=ANY(%s)
            ORDER BY snapshot_set_id, symbol, CASE timeframe
              WHEN '12h' THEN 1 WHEN '24h' THEN 2 WHEN '48h' THEN 3
              WHEN '3d' THEN 4 WHEN '1w' THEN 5 WHEN '2w' THEN 6
              WHEN '1m' THEN 7 ELSE 99 END
            """,
            (set_ids, requested_symbols),
        ).fetchall():
            row = dict(source)
            key = (int(row["snapshot_set_id"]), str(row["symbol"]).upper())
            if key in set_symbol_pairs:
                rows_by_pair[key].append(row)

    results_by_request: Dict[int, Dict[str, Any]] = {}
    for request, request_id in request_index.items():
        symbol, decision_time = request
        candidates = candidates_by_request.get(request_id, [])
        current = candidates[0] if candidates else None
        previous = candidates[1] if len(candidates) > 1 else None
        current_key = (
            (int(current["snapshot_set_id"]), symbol) if current else None
        )
        previous_key = (
            (int(previous["snapshot_set_id"]), symbol) if previous else None
        )
        results_by_request[request_id] = research_max_pain_archive.derive_prior_only_features(
            symbol=symbol,
            decision_time_utc=decision_time,
            current_set=current,
            current_rows=rows_by_pair.get(current_key, []) if current_key else (),
            current_symbol_manifest=manifests.get(current_key) if current_key else None,
            previous_set=previous,
            previous_rows=rows_by_pair.get(previous_key, []) if previous_key else (),
            previous_symbol_manifest=manifests.get(previous_key) if previous_key else None,
        )
    return {
        event_id: dict(results_by_request[request_index[request]])
        for event_id, request in event_requests.items()
    }


def _verified_coverage(
    conn, *, lookback_days: int, horizon_minutes: int
) -> Dict[str, Any]:
    rows = conn.execute(
        """
        SELECT e.symbol,
               COUNT(*)::bigint AS delivered_alerts,
               COUNT(ft.event_id)::bigint AS verified_outcomes
        FROM research_events e
        LEFT JOIN research_first_touch_outcomes ft
          ON ft.event_id=e.event_id
         AND ft.horizon_minutes=%s
         AND ft.method_version=%s
         AND ft.status IN ('HIT', 'MISS')
         AND ft.data_quality_status=ANY(%s)
        WHERE e.event_kind='ALERT' AND e.delivery_status='DELIVERED'
          AND e.alert_time_utc >= NOW() - (%s * INTERVAL '1 day')
        GROUP BY e.symbol
        ORDER BY e.symbol
        """,
        (
            horizon_minutes,
            VERIFIED_OUTCOME_METHOD,
            list(VERIFIED_OUTCOME_QUALITIES),
            lookback_days,
        ),
    ).fetchall()
    by_symbol: Dict[str, Any] = {}
    excluded: Dict[str, Any] = {}
    for source in rows:
        symbol = str(source["symbol"] or "").upper()
        delivered = int(source["delivered_alerts"] or 0)
        verified = int(source["verified_outcomes"] or 0)
        missing = max(0, delivered - verified)
        by_symbol[symbol] = {
            "delivered_alerts": delivered,
            "verified_outcomes": verified,
            "missing_verified_outcomes": missing,
        }
        if missing:
            reason = "VERIFIED_CANONICAL_SPOT_OUTCOME_NOT_AVAILABLE"
            excluded[symbol] = {"count": missing, "reason": reason}
    return {
        "dataset_kind": "delivered_alert_outcomes",
        "replacement_ready": False,
        "canonical_outcome_source": canonical_price_path.canonical_source_description(),
        "historical_price_import_policy": (
            "allowed when exchange, market, pair, resolution, retrieval method and quality are retained"
        ),
        "by_symbol": by_symbol,
        "excluded": excluded,
    }


def _completed_replay_owner_sql(alias: str) -> str:
    """Bind a stored outcome to the exact Replay run that completed it."""
    normalized = str(alias or "").strip()
    if not normalized.replace("_", "").isalnum():
        raise ValueError("invalid SQL alias for replay owner binding")
    return f"""
        {normalized}.first_touch_replay_run_id={normalized}.replay_run_id
        AND EXISTS (
            SELECT 1
            FROM research_historical_replay_runs owner_run
            WHERE owner_run.replay_run_id={normalized}.replay_run_id
              AND owner_run.replay_version={normalized}.replay_version
              AND owner_run.status='COMPLETED'
        )
    """


def _historical_replay_coverage(
    conn, *, lookback_days: int, horizon_minutes: int
) -> Dict[str, Any]:
    sibling_coherence = (
        research_historical_replay.sibling_reference_coherence_sql(
            "historical"
        )
    )
    completed_owner = _completed_replay_owner_sql("historical")
    exact_params = (
        horizon_minutes,
        lookback_days,
        VERIFIED_OUTCOME_METHOD,
        research_historical_replay.REPLAY_VERSION,
        list(VERIFIED_OUTCOME_QUALITIES),
        canonical_price_path.METHOD_VERSION,
        list(VERIFIED_OUTCOME_QUALITIES),
    )
    exact_filters = f"""
          historical.horizon_minutes=%s
      AND historical.observation_time_utc >=
            NOW() - (%s * INTERVAL '1 day')
      AND historical.first_touch_method_version=%s
      AND historical.replay_version=%s
      AND historical.first_touch_data_quality_status=ANY(%s)
      AND historical.outcome_method_version=%s
      AND historical.data_quality_status=ANY(%s)
      AND ({completed_owner})
    """
    # Return one lightweight aggregate row per exact current-contract symbol.
    # Legacy replay/method/quality rows never enter the validation stream.
    preflight_rows = conn.execute(
        f"""
        SELECT historical.symbol,
               COUNT(*)::bigint AS stored_candidates,
               MIN(historical.observation_time_utc) AS first_candidate_utc,
               MAX(historical.observation_time_utc) AS last_candidate_utc
        FROM research_historical_opportunity_outcomes historical
        WHERE {exact_filters}
        GROUP BY historical.symbol
        ORDER BY historical.symbol
        """,
        exact_params,
    ).fetchall()
    preflight_by_symbol: Dict[str, Dict[str, Any]] = {}
    for source in preflight_rows:
        symbol = str(source.get("symbol") or "").upper()
        if not symbol:
            continue
        preflight_by_symbol[symbol] = {
            "stored_candidates": int(source.get("stored_candidates") or 0),
            "first_candidate_utc": _as_utc(source["first_candidate_utc"]),
            "last_candidate_utc": _as_utc(source["last_candidate_utc"]),
        }
    candidate_symbols = sorted(preflight_by_symbol)
    if candidate_symbols:
        first_candidate = min(
            item["first_candidate_utc"]
            for item in preflight_by_symbol.values()
        )
        last_candidate = max(
            item["last_candidate_utc"]
            for item in preflight_by_symbol.values()
        )
        canonical_references = (
            research_historical_replay.load_canonical_reference_rows(
                conn,
                start=first_candidate
                - timedelta(
                    days=HISTORICAL_BASELINE_DAYS,
                    minutes=(
                        max(CORE_WINDOWS_MINUTES) + MAX_POINT_AGE_MINUTES
                    ),
                ),
                end=last_candidate + timedelta(minutes=1),
                symbols=candidate_symbols,
            )
        )
        width_index = research_historical_replay.build_canonical_width_index(
            canonical_references,
            horizons=(horizon_minutes,),
        )
    else:
        width_index = {}

    coherent_counts: Dict[str, int] = defaultdict(int)
    coherent_first: Dict[str, datetime] = {}
    coherent_last: Dict[str, datetime] = {}
    coherent_dates: Dict[str, set[Any]] = defaultdict(set)
    coherent_total = 0
    if candidate_symbols:
        candidate_query = f"""
            SELECT historical.opportunity_id, historical.symbol,
                   historical.observation_time_utc,
                   historical.source_observation_time_utc,
                   historical.horizon_minutes,
                   historical.reference_time_utc,
                   historical.reference_price,
                   historical.price_at_horizon, historical.raw_return_pct,
                   historical.long_metrics, historical.short_metrics,
                   historical.long_first_touch_metrics,
                   historical.short_first_touch_metrics,
                   historical.first_touch_method_version,
                   historical.first_touch_path_samples,
                   historical.path_samples,
                   historical.first_touch_data_quality_status,
                   historical.outcome_method_version,
                   historical.exchange, historical.market, historical.pair,
                   historical.interval_seconds, historical.provenance,
                   historical.data_quality_status, historical.replay_version,
                   historical.replay_run_id,
                   historical.first_touch_replay_run_id,
                   ({sibling_coherence}) AS sibling_reference_coherent
            FROM research_historical_opportunity_outcomes historical
            WHERE {exact_filters}
            ORDER BY historical.symbol, historical.observation_time_utc,
                     historical.opportunity_id
        """
        for source in research_historical_replay.iter_query_rows(
            conn,
            candidate_query,
            exact_params,
            batch_size=REPLAY_COVERAGE_STREAM_BATCH_SIZE,
        ):
            row = dict(source)
            symbol = str(row.get("symbol") or "").upper()
            if symbol not in preflight_by_symbol:
                continue
            if not research_historical_replay.replay_outcome_row_is_coherent(
                row, width_index=width_index
            ):
                continue
            observation_time = _as_utc(row["observation_time_utc"])
            coherent_counts[symbol] += 1
            coherent_total += 1
            coherent_dates[symbol].add(observation_time.date())
            if (
                symbol not in coherent_first
                or observation_time < coherent_first[symbol]
            ):
                coherent_first[symbol] = observation_time
            if (
                symbol not in coherent_last
                or observation_time > coherent_last[symbol]
            ):
                coherent_last[symbol] = observation_time

    by_symbol: Dict[str, Dict[str, Any]] = {}
    for symbol in candidate_symbols:
        first_observation = coherent_first.get(symbol)
        last_observation = coherent_last.get(symbol)
        span_hours = (
            (last_observation - first_observation).total_seconds() / 3600.0
            if first_observation and last_observation
            else 0.0
        )
        anchors = coherent_counts[symbol]
        utc_dates = len(coherent_dates[symbol])
        stored_candidates = preflight_by_symbol[symbol]["stored_candidates"]
        failures = []
        if anchors < REPLAY_MIN_ANCHORS_PER_SYMBOL:
            failures.append("minimum_anchors")
        if utc_dates < REPLAY_MIN_UTC_DATES_PER_SYMBOL:
            failures.append("minimum_utc_dates")
        if span_hours < REPLAY_MIN_SPAN_HOURS_PER_SYMBOL:
            failures.append("minimum_span_hours")
        by_symbol[symbol] = {
            "anchors": anchors,
            "directional_rows": anchors * 2,
            "first_observation_utc": first_observation,
            "last_observation_utc": last_observation,
            "utc_dates": utc_dates,
            "span_hours": round(span_hours, 3),
            "stored_candidates": stored_candidates,
            "recomputed_policy_rejections": stored_candidates - anchors,
            "eligible": not failures,
            "failed_gates": failures,
        }
    eligible_symbols = sorted(
        symbol for symbol, item in by_symbol.items() if item["eligible"]
    )
    excluded_symbols = {
        symbol: list(item["failed_gates"])
        for symbol, item in by_symbol.items()
        if not item["eligible"]
    }
    eligible_items = [by_symbol[symbol] for symbol in eligible_symbols]
    total_anchors = sum(item["anchors"] for item in eligible_items)
    stored_anchors = sum(
        item["stored_candidates"] for item in preflight_by_symbol.values()
    )
    first = min(
        (item["first_observation_utc"] for item in eligible_items),
        default=None,
    )
    last = max(
        (item["last_observation_utc"] for item in eligible_items),
        default=None,
    )
    distinct_dates = len(
        set().union(*(coherent_dates[symbol] for symbol in eligible_symbols))
        if eligible_symbols
        else set()
    )
    span_hours = (
        (last - first).total_seconds() / 3600.0 if first and last else 0.0
    )
    replacement_ready = (
        len(eligible_symbols) >= REPLAY_MIN_ELIGIBLE_SYMBOLS
        and distinct_dates >= REPLAY_MIN_UTC_DATES_PER_SYMBOL
        and span_hours >= REPLAY_MIN_SPAN_HOURS_PER_SYMBOL
    )
    return {
        "dataset_kind": "historical_raw_opportunity_replay",
        "replay_version": research_historical_replay.REPLAY_VERSION,
        "first_touch_method_version": VERIFIED_OUTCOME_METHOD,
        "movement_width_calibration_version": (
            research_session_width.CALIBRATION_VERSION
        ),
        "canonical_price_provenance_version": (
            canonical_price_path.PRICE_PROVENANCE_VERSION
        ),
        "replacement_ready": replacement_ready,
        "readiness_policy": {
            "minimum_anchors_per_symbol": REPLAY_MIN_ANCHORS_PER_SYMBOL,
            "minimum_eligible_symbols": REPLAY_MIN_ELIGIBLE_SYMBOLS,
            "minimum_utc_dates_per_symbol": REPLAY_MIN_UTC_DATES_PER_SYMBOL,
            "minimum_span_hours_per_symbol": REPLAY_MIN_SPAN_HOURS_PER_SYMBOL,
        },
        "anchors": total_anchors,
        "directional_rows": total_anchors * 2,
        "symbols": len(eligible_symbols),
        "eligible_symbols": eligible_symbols,
        "excluded_symbols": excluded_symbols,
        "stored_anchors": stored_anchors,
        "stored_symbols": len(candidate_symbols),
        "coherent_anchors": coherent_total,
        "recomputed_policy_rejections": stored_anchors - coherent_total,
        "coverage_validation": "full-row prior-only recomputation",
        "distinct_utc_dates": distinct_dates,
        "span_hours": round(span_hours, 3),
        "first_observation_utc": first,
        "last_observation_utc": last,
        "by_symbol": by_symbol,
        "canonical_outcome_source": canonical_price_path.canonical_source_description(),
        "historical_price_import_policy": (
            "exchange API candles may be imported for labels when provenance and quality are retained"
        ),
    }


def _even_sample(values: Sequence[Mapping[str, Any]], size: int) -> list[Dict[str, Any]]:
    rows = [dict(value) for value in values]
    target = max(0, min(int(size), len(rows)))
    if target == 0:
        return []
    if target == len(rows):
        return rows
    if target == 1:
        return [rows[len(rows) // 2]]
    indexes = {
        round(index * (len(rows) - 1) / (target - 1)) for index in range(target)
    }
    return [rows[index] for index in sorted(indexes)]


def _load_historical_opportunities(
    conn,
    *,
    lookback_days: int,
    horizon_minutes: int,
    anchor_limit: int,
    symbols: Sequence[str],
) -> list[Dict[str, Any]]:
    symbols = sorted({str(symbol).upper() for symbol in symbols if symbol})
    if not symbols:
        return []
    quota = max(1, (anchor_limit + len(symbols) - 1) // len(symbols))
    sibling_coherence = (
        research_historical_replay.sibling_reference_coherence_sql(
            "historical"
        )
    )
    completed_owner = _completed_replay_owner_sql("historical")
    rows = conn.execute(
        f"""
        WITH eligible AS (
            SELECT opportunity_id, symbol, observation_time_utc,
                   source_observation_time_utc, horizon_minutes,
                   reference_time_utc, reference_price,
                   price_at_horizon, raw_return_pct,
                   long_metrics, short_metrics, path_samples,
                   long_first_touch_metrics, short_first_touch_metrics,
                   first_touch_method_version, first_touch_path_samples,
                   first_touch_data_quality_status,
                   outcome_method_version AS legacy_outcome_method_version,
                   data_quality_status AS legacy_data_quality_status,
                   outcome_method_version, data_quality_status,
                   exchange, market, pair, interval_seconds, provenance,
                   replay_version, replay_run_id,
                   first_touch_replay_run_id,
                   ({sibling_coherence}) AS sibling_reference_coherent,
                   ROW_NUMBER() OVER (
                       PARTITION BY symbol ORDER BY observation_time_utc, opportunity_id
                   ) AS sequence_number,
                   COUNT(*) OVER (PARTITION BY symbol) AS symbol_total
            FROM research_historical_opportunity_outcomes historical
            WHERE horizon_minutes=%s
              AND observation_time_utc >= NOW() - (%s * INTERVAL '1 day')
              AND first_touch_method_version=%s
              AND replay_version=%s
              AND ({completed_owner})
              AND first_touch_data_quality_status=ANY(%s)
              AND outcome_method_version=%s
              AND data_quality_status=ANY(%s)
              AND long_first_touch_metrics->>'method_version'=%s
              AND short_first_touch_metrics->>'method_version'=%s
              AND long_first_touch_metrics->>'status' IN ('HIT', 'MISS')
              AND short_first_touch_metrics->>'status' IN ('HIT', 'MISS')
              AND long_first_touch_metrics->>'direction'='LONG'
              AND short_first_touch_metrics->>'direction'='SHORT'
              AND long_first_touch_metrics->>'horizon_minutes'=horizon_minutes::text
              AND short_first_touch_metrics->>'horizon_minutes'=horizon_minutes::text
              AND long_first_touch_metrics->>'dwell_required_seconds'='0'
              AND short_first_touch_metrics->>'dwell_required_seconds'='0'
              AND long_first_touch_metrics->'threshold_policy'
                  = short_first_touch_metrics->'threshold_policy'
              AND long_first_touch_metrics->'threshold_policy'
                    ->>'threshold_reference_version'=%s
              AND long_first_touch_metrics->'threshold_policy'
                    ->>'threshold_reference_hash' ~ '^[0-9a-f]{{64}}$'
              AND LOWER(market)='spot' AND interval_seconds=60
              AND (
                    (symbol='HYPE' AND LOWER(exchange)='hyperliquid'
                     AND UPPER(pair)='HYPE/USDT'
                     AND provenance LIKE '%%"instrument":"@107"%%')
                    OR
                    (symbol<>'HYPE' AND LOWER(exchange)='binance'
                     AND REPLACE(UPPER(pair), '/', '')=symbol || 'USDT')
                  )
              AND symbol=ANY(%s)
        )
        SELECT *
        FROM eligible
        WHERE symbol_total <= %s
           OR MOD(
                sequence_number - 1,
                GREATEST(1, CEIL(symbol_total::numeric / %s)::integer)
              ) = 0
           OR sequence_number = symbol_total
        ORDER BY symbol, observation_time_utc, opportunity_id
        """,
        (
            horizon_minutes,
            lookback_days,
            VERIFIED_OUTCOME_METHOD,
            research_historical_replay.REPLAY_VERSION,
            list(VERIFIED_OUTCOME_QUALITIES),
            canonical_price_path.METHOD_VERSION,
            list(VERIFIED_OUTCOME_QUALITIES),
            VERIFIED_OUTCOME_METHOD,
            VERIFIED_OUTCOME_METHOD,
            research_session_width.CALIBRATION_VERSION,
            symbols,
            quota,
            quota,
        ),
    ).fetchall()
    grouped: Dict[str, list[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["symbol"]).upper()].append(dict(row))
    base, remainder = divmod(anchor_limit, len(symbols))
    sampled: list[Dict[str, Any]] = []
    for index, symbol in enumerate(symbols):
        sampled.extend(
            _even_sample(grouped.get(symbol, []), base + (1 if index < remainder else 0))
        )
    return sorted(
        sampled,
        key=lambda row: (
            _as_utc(row["observation_time_utc"]),
            str(row["symbol"]),
            int(row["opportunity_id"]),
        ),
    )


def _opportunity_events(
    opportunities: Sequence[Mapping[str, Any]],
) -> list[Dict[str, Any]]:
    events: list[Dict[str, Any]] = []
    for source in opportunities:
        row = dict(source)
        for direction, metrics_key, first_touch_key, offset in (
            ("LONG", "long_metrics", "long_first_touch_metrics", 1),
            ("SHORT", "short_metrics", "short_first_touch_metrics", 2),
        ):
            metrics = row.get(metrics_key) or {}
            if isinstance(metrics, str):
                metrics = json.loads(metrics)
            first_touch = row.get(first_touch_key) or {}
            if isinstance(first_touch, str):
                first_touch = json.loads(first_touch)
            opportunity_id = int(row["opportunity_id"])
            events.append(
                {
                    "event_id": -(opportunity_id * 2 + offset),
                    "alert_time_utc": row["observation_time_utc"],
                    "symbol": row["symbol"],
                    "direction": direction,
                    "source_side": "RAW_REPLAY",
                    "timeframe": "30m",
                    "event_type": "HISTORICAL_RAW_OPPORTUNITY",
                    "score": None,
                    "current_price": row["reference_price"],
                    "target_price": None,
                    "initial_target_distance_pct": None,
                    "categories": [],
                    "setup_key": None,
                    "strategy_version": None,
                    "code_version": None,
                    "engine_snapshot": {},
                    "horizon_minutes": row["horizon_minutes"],
                    "measured_at_utc": metrics.get("measured_at_utc"),
                    "reference_price": row["reference_price"],
                    "price_at_horizon": metrics.get("price_at_horizon"),
                    "raw_return_pct": metrics.get("raw_return_pct"),
                    "directional_return_pct": metrics.get(
                        "directional_return_pct"
                    ),
                    "path_success": first_touch.get("success"),
                    "first_touch_status": first_touch.get("status"),
                    "first_qualifying_move_time_utc": first_touch.get(
                        "first_qualifying_move_time_utc"
                    ),
                    "time_to_first_qualifying_move_seconds": first_touch.get(
                        "time_to_first_qualifying_move_seconds"
                    ),
                    "qualifying_move_threshold_pct": first_touch.get(
                        "qualifying_move_threshold_pct"
                    ),
                    "threshold_scale_factor": first_touch.get(
                        "threshold_scale_factor"
                    ),
                    "pre_qualifying_mae_pct": first_touch.get(
                        "pre_qualifying_mae_pct"
                    ),
                    "qualifying_candle_order_ambiguous": first_touch.get(
                        "qualifying_candle_order_ambiguous"
                    ),
                    "mfe_pct": metrics.get("mfe_pct"),
                    "mae_pct": metrics.get("mae_pct"),
                    "time_to_first_progress_seconds": metrics.get(
                        "time_to_first_progress_seconds"
                    ),
                    "time_to_mfe_seconds": metrics.get("time_to_mfe_seconds"),
                    "time_to_closest_target_seconds": None,
                    "time_to_target_seconds": None,
                    "target_progress_ratio": None,
                    "target_reached": None,
                    "path_samples": row["path_samples"],
                    "outcome_method_version": row["first_touch_method_version"],
                    "legacy_outcome_method_version": row[
                        "legacy_outcome_method_version"
                    ],
                    "data_quality_status": row[
                        "first_touch_data_quality_status"
                    ],
                }
            )
    return events
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


def _mapping_value(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, Mapping) else {}
    return {}


def _load_prospective_frozen_rows(
    conn,
    *,
    symbols: Sequence[str],
    start: datetime,
    end: datetime,
) -> tuple[
    list[Dict[str, Any]],
    list[Dict[str, Any]],
    list[Dict[str, Any]],
    list[Dict[str, Any]],
    Dict[int, Dict[str, Any]],
]:
    """Rebuild decision series only from immutable prospective slot inputs.

    ``created_at_utc`` must be within five minutes of the recorded decision.
    This admits the normal atomic event/slot transaction while preventing a
    later insert with a backdated decision time from changing an old feature
    row. No mutable raw archive or candle-time-only fallback is consulted.
    """
    rows = conn.execute(
        """
        SELECT anchor_slot_id, sampler_version, coverage_policy_version,
               coverage_snapshot, symbol, source_candle_open_utc,
               source_candle_close_utc, base_eligible_at_utc, expires_at_utc,
               decision_time_utc, input_fingerprint, source_timestamps,
               source_provenance, frozen_inputs,
               long_event_id, short_event_id, created_at_utc
        FROM research_prospective_anchor_slots
        WHERE sampler_version=%s
          AND symbol=ANY(%s)
          AND decision_time_utc >= %s
          AND decision_time_utc <= %s
          AND created_at_utc <= decision_time_utc + INTERVAL '5 minutes'
        ORDER BY symbol, decision_time_utc, anchor_slot_id
        """,
        (PROSPECTIVE_ANCHOR_SAMPLER_VERSION, list(symbols), start, end),
    ).fetchall()
    price_rows: list[Dict[str, Any]] = []
    oi_rows: list[Dict[str, Any]] = []
    futures_rows: list[Dict[str, Any]] = []
    spot_rows: list[Dict[str, Any]] = []
    max_pain_by_event_id: Dict[int, Dict[str, Any]] = {}
    for source in rows:
        row = dict(source)
        symbol = str(row.get("symbol") or "").strip().upper()
        decision_time = _as_utc(row["decision_time_utc"])
        frozen = _mapping_value(row.get("frozen_inputs"))
        provenance = _mapping_value(row.get("source_provenance"))
        timestamps = _mapping_value(row.get("source_timestamps"))
        coverage = _mapping_value(row.get("coverage_snapshot"))
        try:
            expected_fingerprint = (
                research_prospective_anchors.compute_input_fingerprint(
                    sampler_version=row.get("sampler_version"),
                    coverage_policy_version=row.get("coverage_policy_version"),
                    coverage_snapshot=coverage,
                    symbol=symbol,
                    source_candle_open_utc=row.get("source_candle_open_utc"),
                    source_candle_close_utc=row.get("source_candle_close_utc"),
                    base_eligible_at_utc=row.get("base_eligible_at_utc"),
                    expires_at_utc=row.get("expires_at_utc"),
                    evaluation_status=research_prospective_anchors.EVALUABLE,
                    decision_time_utc=decision_time,
                    source_timestamps=timestamps,
                    source_provenance=provenance,
                    frozen_inputs=frozen,
                )
            )
        except (TypeError, ValueError, OverflowError):
            continue
        if expected_fingerprint != str(row.get("input_fingerprint") or "").strip():
            continue
        official = _mapping_value(frozen.get("official_price"))
        price_oi = _mapping_value(frozen.get("price_oi"))
        futures = _mapping_value(frozen.get("futures_cvd"))
        spot = _mapping_value(frozen.get("spot_cvd"))
        max_pain = _mapping_value(frozen.get("max_pain"))
        if not max_pain or not isinstance(max_pain.get("features"), Mapping):
            # Sampler v3 requires an explicit frozen Max-Pain result, including
            # the empty feature map of an UNEVALUABLE decision-time lookup.
            continue
        official_provenance = _mapping_value(provenance.get("official_price"))
        shared = {
            "symbol": symbol,
            "candle_time": decision_time,
            "prospective_anchor_slot_id": int(row["anchor_slot_id"]),
            "prospective_input_fingerprint": str(row["input_fingerprint"]),
            "prospective_slot_created_at_utc": row["created_at_utc"],
        }
        instrument = official_provenance.get("price_instrument_id")
        canonical_route = {
            "provenance_version": canonical_price_path.PRICE_PROVENANCE_VERSION,
            "method_version": canonical_price_path.METHOD_VERSION,
            "symbol": symbol,
            "exchange": str(
                official_provenance.get("price_exchange") or ""
            ).strip().lower(),
            "market": str(
                official_provenance.get("price_market") or ""
            ).strip().lower(),
            "pair": str(
                official_provenance.get("price_pair") or ""
            ).strip().upper(),
            "instrument": str(instrument).strip() if instrument else None,
            "interval": str(
                official_provenance.get("price_timeframe") or ""
            ).strip().lower(),
            "interval_seconds": 60,
            "provider_provenance": str(
                official_provenance.get("source") or ""
            ),
        }
        try:
            canonical_route = canonical_price_path.validated_route(
                symbol,
                {
                    "exchange": canonical_route["exchange"],
                    "market": canonical_route["market"],
                    "pair": canonical_route["pair"],
                    "interval": canonical_route["interval"],
                    "interval_seconds": canonical_route["interval_seconds"],
                    "api_coin": canonical_route["instrument"],
                    "complete": True,
                    "provenance": canonical_route["provider_provenance"],
                },
            )
        except (TypeError, ValueError, OverflowError):
            # One malformed immutable slot cannot be allowed to contaminate
            # later windows; omitting it makes affected features unavailable.
            continue
        price_rows.append(
            {
                **shared,
                "price_close": official.get("price"),
                "price_exchange": canonical_route["exchange"],
                "price_market": canonical_route["market"],
                "price_pair": canonical_route["pair"],
                "price_instrument_id": canonical_route["instrument"],
                "price_timeframe": canonical_route["interval"],
                "price_interval_seconds": 60,
                "source": official_provenance.get("source"),
                "canonical_price_method_version": (
                    canonical_price_path.METHOD_VERSION
                ),
                "canonical_price_provenance_version": (
                    canonical_price_path.PRICE_PROVENANCE_VERSION
                ),
                "canonical_price_provenance": canonical_route,
            }
        )
        oi_rows.append(
            {
                **shared,
                "oi_close_usd": price_oi.get("oi_close_usd"),
                "source": _mapping_value(provenance.get("price_oi")).get(
                    "source"
                ),
            }
        )
        for values, family_provenance, target in (
            (futures, provenance.get("futures_cvd"), futures_rows),
            (spot, provenance.get("spot_cvd"), spot_rows),
        ):
            flow_provenance = _mapping_value(family_provenance)
            target.append(
                {
                    **shared,
                    "buy_volume_usd": values.get("buy_volume_usd"),
                    "sell_volume_usd": values.get("sell_volume_usd"),
                    "api_cum_vol_delta_usd": values.get(
                        "api_cum_vol_delta_usd"
                    ),
                    "continuous_cum_vol_delta_usd": values.get(
                        "continuous_cum_vol_delta_usd"
                    ),
                    "exchange_list": flow_provenance.get("exchange_list"),
                    "source": flow_provenance.get("source"),
                }
            )
        for event_column in ("long_event_id", "short_event_id"):
            event_id = row.get(event_column)
            if event_id is not None:
                max_pain_by_event_id[int(event_id)] = dict(max_pain)
    return (
        price_rows,
        oi_rows,
        futures_rows,
        spot_rows,
        max_pain_by_event_id,
    )


def _load_raw_rows(
    conn, *, symbols: Sequence[str], start: datetime, end: datetime
) -> tuple[list[Dict[str, Any]], list[Dict[str, Any]], list[Dict[str, Any]]]:
    price_rows = _closed_archive_rows([
        dict(row)
        for row in conn.execute(
            """
            SELECT symbol, candle_time, price_close, oi_close_usd,
                   price_exchange, price_pair, source
            FROM oi_price_history
            WHERE symbol=ANY(%s)
              AND symbol<>'HYPE'
              AND candle_time >= %s AND candle_time <= %s
            ORDER BY symbol, candle_time
            """,
            (list(symbols), start, end),
        ).fetchall()
    ])
    # ``oi_price_history`` is the historical/backfill archive.  The running
    # bot persists newer Price/OI observations in ``oi_regime_snapshots``.
    # Use the live table only after each non-HYPE symbol's backfill endpoint so
    # the overlapping archives do not double-weight historical distributions.
    # HYPE is intentionally sourced only from the live archive because its
    # price provenance is Hyperliquid; the older backfill labels HYPE as
    # Binance and is therefore not eligible for canonical HYPE research.
    if _table_exists(conn, "oi_regime_snapshots"):
        price_rows.extend(
            dict(row)
            for row in conn.execute(
                """
                SELECT symbol, collected_at AS candle_time,
                       price AS price_close,
                       open_interest_usd AS oi_close_usd,
                       price_source AS price_exchange,
                       CASE WHEN symbol='HYPE' THEN 'HYPE/USDT'
                            ELSE (symbol || 'USDT') END AS price_pair,
                       ('oi_regime_snapshots:' || COALESCE(oi_source, 'unknown')) AS source
                FROM oi_regime_snapshots live
                WHERE live.symbol=ANY(%s)
                  AND live.collected_at >= %s AND live.collected_at <= %s
                  AND live.data_quality_status='PASS'
                  AND (live.symbol<>'HYPE' OR live.price_source='hyperliquid')
                  AND (
                    live.symbol='HYPE'
                    OR live.collected_at > COALESCE(
                      (SELECT MAX(backfill.candle_time)
                       FROM oi_price_history backfill
                       WHERE backfill.symbol=live.symbol),
                      '-infinity'::timestamptz
                    )
                  )
                ORDER BY live.symbol, live.collected_at
                """,
                (list(symbols), start, end),
            ).fetchall()
        )
        price_rows.sort(
            key=lambda row: (
                str(row.get("symbol") or ""),
                _as_utc(row["candle_time"]),
            )
        )
    flow_rows: list[list[Dict[str, Any]]] = []
    for table in ("futures_taker_history", "spot_taker_history"):
        flow_rows.append(
            _closed_archive_rows([
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
            ])
        )
    return price_rows, flow_rows[0], flow_rows[1]


def _load_independent_oi_rows(
    conn, *, symbols: Sequence[str], start: datetime, end: datetime
) -> list[Dict[str, Any]]:
    """Load OI only; archived CoinGlass prices are deliberately not selected."""
    rows = _closed_archive_rows(
        [
            dict(row)
            for row in conn.execute(
                """
                SELECT symbol, candle_time, oi_close_usd,
                       ('oi_price_history:' || source) AS source
                FROM oi_price_history
                WHERE symbol=ANY(%s) AND symbol<>'HYPE'
                  AND candle_time >= %s AND candle_time <= %s
                ORDER BY symbol, candle_time
                """,
                (list(symbols), start, end),
            ).fetchall()
        ]
    )
    if _table_exists(conn, "oi_regime_snapshots"):
        rows.extend(
            dict(row)
            for row in conn.execute(
                """
                SELECT symbol, collected_at AS candle_time,
                       open_interest_usd AS oi_close_usd,
                       ('oi_regime_snapshots:' || COALESCE(oi_source, 'unknown'))
                           AS source
                FROM oi_regime_snapshots live
                WHERE live.symbol=ANY(%s)
                  AND live.collected_at >= %s AND live.collected_at <= %s
                  AND live.data_quality_status='PASS'
                  AND (
                    live.symbol='HYPE'
                    OR live.collected_at > COALESCE(
                      (SELECT MAX(backfill.candle_time)
                       FROM oi_price_history backfill
                       WHERE backfill.symbol=live.symbol),
                      '-infinity'::timestamptz
                    )
                  )
                ORDER BY live.symbol, live.collected_at
                """,
                (list(symbols), start, end),
            ).fetchall()
        )
    return sorted(
        rows,
        key=lambda row: (
            str(row.get("symbol") or ""),
            _as_utc(row["candle_time"]),
        ),
    )


def _load_raw_flow_rows(
    conn, *, symbols: Sequence[str], start: datetime, end: datetime
) -> tuple[list[Dict[str, Any]], list[Dict[str, Any]]]:
    flows: list[list[Dict[str, Any]]] = []
    for table in ("futures_taker_history", "spot_taker_history"):
        flows.append(
            _closed_archive_rows(
                [
                    dict(row)
                    for row in conn.execute(
                        f"""
                        SELECT symbol, candle_time, buy_volume_usd,
                               sell_volume_usd, api_cum_vol_delta_usd,
                               continuous_cum_vol_delta_usd,
                               exchange_list, source
                        FROM {table}
                        WHERE symbol=ANY(%s)
                          AND candle_time >= %s AND candle_time <= %s
                        ORDER BY symbol, candle_time
                        """,
                        (list(symbols), start, end),
                    ).fetchall()
                ]
            )
        )
    return flows[0], flows[1]


def _canonical_price_feature_rows(
    references: Sequence[Mapping[str, Any]],
) -> list[Dict[str, Any]]:
    """Expose only proven canonical reference prices as raw Price inputs."""
    rows: list[Dict[str, Any]] = []
    for source in references:
        row = dict(source)
        symbol = str(row.get("symbol") or "").upper()
        rows.append(
            {
                "symbol": symbol,
                "candle_time": row["reference_time_utc"],
                "price_close": row["reference_price"],
                "price_exchange": row.get("exchange"),
                "price_pair": row.get("pair"),
                "source": (
                    "research_historical_opportunity_outcomes:"
                    "canonical_reference_price"
                ),
                "canonical_price_method_version": row.get(
                    "outcome_method_version"
                ),
                "canonical_price_replay_version": row.get("replay_version"),
                "canonical_price_provenance": row.get("provenance"),
                "source_observation_time_utc": row.get(
                    "source_observation_time_utc"
                ),
                "decision_time_utc": row.get("observation_time_utc"),
            }
        )
    return rows


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
        required_research = (
            "research_events",
            "research_alert_outcomes",
            "research_first_touch_outcomes",
        )
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
        max_pain_by_event_id = _load_max_pain_features_batch(
            research_conn, events
        )

    symbols = sorted({str(row["symbol"]).upper() for row in events})
    raw_start = minimum_event_time - timedelta(
        days=HISTORICAL_BASELINE_DAYS,
        minutes=max(windows) + MAX_POINT_AGE_MINUTES,
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
        max_pain_by_event_id=max_pain_by_event_id,
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
                "Every raw feature uses the newest closed, grace-cleared stored point at or before alert_time_utc; future points are never eligible.",
                f"A raw point more than {MAX_POINT_AGE_MINUTES} minutes old is returned as missing instead of being silently joined.",
                "Every input window carries its own exact America/New_York ACTIVE/WEEKEND composition; historical_context matches older same-symbol Price/OI/CVD observations by that composition.",
                "model_features come only from the immutable decision-time Research Event snapshot.",
                "outcome_label is later canonical spot evidence with a separately calculated future-session composition and must never be used as an input feature.",
                "Rows expose raw and existing-model features side by side; current bot scores are candidates for comparison, not assumed truth.",
                "The matrix is a discovery sample. Candidate formulas still require chronological holdout and out-of-sample validation.",
            ],
        }
    )


def load_historical_replay_dataset(
    *,
    lookback_days: int = 3650,
    horizon_minutes: int = 240,
    limit: int = 2000,
) -> Dict[str, Any]:
    """Load an evenly sampled, chronological raw-opportunity replay dataset."""
    days = max(1, min(int(lookback_days), 3650))
    horizon = int(horizon_minutes)
    if horizon not in {60, 240, 720, 1440}:
        raise ValueError("horizon_minutes must be 60, 240, 720 or 1440")
    row_limit = max(50, min(int(limit), 5000))
    anchor_limit = max(25, row_limit // 2)
    research_url = _research_database_url()
    raw_url = _raw_database_url()
    if not research_url or not raw_url:
        return {
            "available": False,
            "reason": "research and raw market archives must both be configured",
        }

    with _connect(research_url) as research_conn:
        table = "research_historical_opportunity_outcomes"
        if not _table_exists(research_conn, table):
            return {
                "available": False,
                "reason": "historical opportunity replay schema is not installed",
                "missing_tables": [table],
            }
        coverage = _historical_replay_coverage(
            research_conn,
            lookback_days=days,
            horizon_minutes=horizon,
        )
        opportunities = _load_historical_opportunities(
            research_conn,
            lookback_days=days,
            horizon_minutes=horizon,
            anchor_limit=anchor_limit,
            symbols=coverage.get("eligible_symbols") or [],
        )
    if not opportunities:
        if coverage.get("eligible_symbols"):
            return {
                "available": False,
                "feature_schema_version": FEATURE_SCHEMA_VERSION,
                "outcome_method_version": VERIFIED_OUTCOME_METHOD,
                "replay_version": research_historical_replay.REPLAY_VERSION,
                "horizon_minutes": horizon,
                "sample_size": 0,
                "coverage": coverage,
                "rows": [],
                "reason": (
                    "historical replay coverage/sample invariant failed: "
                    "eligible coverage produced no discovery sample"
                ),
            }
        return {
            "available": True,
            "feature_schema_version": FEATURE_SCHEMA_VERSION,
            "outcome_method_version": VERIFIED_OUTCOME_METHOD,
            "replay_version": research_historical_replay.REPLAY_VERSION,
            "first_touch_method_version": VERIFIED_OUTCOME_METHOD,
            "movement_width_calibration_version": (
                research_session_width.CALIBRATION_VERSION
            ),
            "canonical_price_provenance_version": (
                canonical_price_path.PRICE_PROVENANCE_VERSION
            ),
            "horizon_minutes": horizon,
            "sample_size": 0,
            "coverage": coverage,
            "rows": [],
            "reason": "no verified historical replay outcomes are available",
        }

    first_time = min(
        _as_utc(row["observation_time_utc"]) for row in opportunities
    )
    last_time = max(
        _as_utc(row["observation_time_utc"]) for row in opportunities
    )
    symbols = sorted({str(row["symbol"]).upper() for row in opportunities})
    raw_start = first_time - timedelta(
        days=HISTORICAL_BASELINE_DAYS,
        minutes=max(CORE_WINDOWS_MINUTES) + MAX_POINT_AGE_MINUTES,
    )
    with _connect(research_url) as research_conn:
        canonical_references = (
            research_historical_replay.load_canonical_reference_rows(
                research_conn,
                start=raw_start,
                end=last_time + timedelta(minutes=1),
                symbols=symbols,
            )
        )
        width_index = research_historical_replay.build_canonical_width_index(
            canonical_references,
            horizons=sorted(set(CORE_WINDOWS_MINUTES) | {horizon}),
        )
        candidate_count = len(opportunities)
        opportunities = [
            row
            for row in opportunities
            if research_historical_replay.replay_outcome_row_is_coherent(
                row, width_index=width_index
            )
        ]
        rejected = candidate_count - len(opportunities)
        if rejected:
            coverage = dict(coverage)
            coverage["replacement_ready"] = False
            coverage["recomputed_policy_rejections"] = rejected
            coverage["replacement_blocker"] = (
                "stored threshold/reference differs from prior-only recomputation"
            )
        events = _opportunity_events(opportunities)
        max_pain_by_event_id = (
            _load_max_pain_features_batch(research_conn, events)
            if events
            else {}
        )
    if not opportunities:
        return {
            "available": True,
            "feature_schema_version": FEATURE_SCHEMA_VERSION,
            "outcome_method_version": VERIFIED_OUTCOME_METHOD,
            "replay_version": research_historical_replay.REPLAY_VERSION,
            "first_touch_method_version": VERIFIED_OUTCOME_METHOD,
            "horizon_minutes": horizon,
            "sample_size": 0,
            "coverage": coverage,
            "rows": [],
            "reason": "all sampled replay rows failed coherent-policy recomputation",
        }
    canonical_price_rows = _canonical_price_feature_rows(
        canonical_references
    )
    with _connect(raw_url) as raw_conn:
        required_raw = (
            "oi_price_history",
            "futures_taker_history",
            "spot_taker_history",
        )
        missing_raw = [
            table for table in required_raw if not _table_exists(raw_conn, table)
        ]
        if missing_raw:
            return {
                "available": False,
                "reason": "raw market archive schema is incomplete",
                "missing_tables": missing_raw,
            }
        oi_rows = _load_independent_oi_rows(
            raw_conn,
            symbols=symbols,
            start=raw_start,
            end=last_time,
        )
        futures_rows, spot_rows = _load_raw_flow_rows(
            raw_conn,
            symbols=symbols,
            start=raw_start,
            end=last_time,
        )

    rows = build_feature_rows(
        events,
        price_oi_rows=canonical_price_rows,
        oi_rows=oi_rows,
        futures_rows=futures_rows,
        spot_rows=spot_rows,
        # Alert-sequence features are intentionally absent in the neutral raw
        # replay.  They remain available in the delivered-alert dataset.
        prior_events=[],
        windows_minutes=CORE_WINDOWS_MINUTES,
        max_pain_by_event_id=max_pain_by_event_id,
    )
    return _json_safe(
        {
            "available": True,
            "feature_schema_version": FEATURE_SCHEMA_VERSION,
            "outcome_method_version": VERIFIED_OUTCOME_METHOD,
            "replay_version": research_historical_replay.REPLAY_VERSION,
            "first_touch_method_version": VERIFIED_OUTCOME_METHOD,
            "movement_width_calibration_version": (
                research_session_width.CALIBRATION_VERSION
            ),
            "canonical_price_provenance_version": (
                canonical_price_path.PRICE_PROVENANCE_VERSION
            ),
            "outcome_quality": VERIFIED_OUTCOME_QUALITY,
            "horizon_minutes": horizon,
            "lookback_days": days,
            "sample_size": len(rows),
            "anchor_sample_size": len(opportunities),
            "first_alert_time_utc": first_time,
            "last_alert_time_utc": last_time,
            "chronological_order": "ascending",
            "sampling": (
                "deterministic even sampling by symbol and observation time; "
                "both LONG and SHORT labels per anchor"
            ),
            "historical_baseline": {
                "lookback_days": HISTORICAL_BASELINE_DAYS,
                "minimum_samples": HISTORICAL_BASELINE_MIN_SAMPLES,
                "session_policy": "same-symbol exact ACTIVE/WEEKEND composition matched prior-only",
                "session_timezone": "America/New_York",
                "session_definition": "SUN_18_ET__FRI_20_ET_ACTIVE",
                "composition_tolerance": HISTORICAL_BASELINE_COMPOSITION_TOLERANCE,
            },
            "coverage": coverage,
            "rows": rows,
        }
    )


def _load_alert_formula_dataset(
    *,
    lookback_days: int = 3650,
    horizon_minutes: int = 240,
    limit: int = 2000,
) -> Dict[str, Any]:
    """Load a chronological, bounded delivered-alert dataset.

    This internal research surface is intentionally larger than the GPT-facing
    ``research_feature_matrix`` tool.  It still accepts only verified delivered
    alerts and keeps every post-alert value inside ``outcome_label``.
    """
    days = max(1, min(int(lookback_days), 3650))
    horizon = int(horizon_minutes)
    if horizon not in {60, 240, 720, 1440}:
        raise ValueError("horizon_minutes must be 60, 240, 720 or 1440")
    row_limit = max(50, min(int(limit), 5000))
    research_url = _research_database_url()
    raw_url = _raw_database_url()
    if not research_url or not raw_url:
        return {
            "available": False,
            "reason": "research and raw market archives must both be configured",
        }

    with _connect(research_url) as research_conn:
        required = (
            "research_events",
            "research_alert_outcomes",
            "research_first_touch_outcomes",
        )
        missing = [table for table in required if not _table_exists(research_conn, table)]
        if missing:
            return {
                "available": False,
                "reason": "research archive schema is incomplete",
                "missing_tables": missing,
            }
        coverage = _verified_coverage(
            research_conn,
            lookback_days=days,
            horizon_minutes=horizon,
        )
        events = _load_verified_events(
            research_conn,
            symbol=None,
            event_type=None,
            direction=None,
            lookback_days=days,
            horizon_minutes=horizon,
            limit=row_limit,
        )
        events.sort(key=lambda row: (_as_utc(row["alert_time_utc"]), int(row["event_id"])))
        if not events:
            return {
                "available": True,
                "feature_schema_version": FEATURE_SCHEMA_VERSION,
                "horizon_minutes": horizon,
                "sample_size": 0,
                "rows": [],
                "coverage": coverage,
            }
        first_time = _as_utc(events[0]["alert_time_utc"])
        last_time = _as_utc(events[-1]["alert_time_utc"])
        prior_events = _load_prior_events(
            research_conn,
            first_time - timedelta(minutes=max(SEQUENCE_WINDOWS_MINUTES)),
            last_time,
        )
        max_pain_by_event_id = _load_max_pain_features_batch(
            research_conn, events
        )

    symbols = sorted({str(row["symbol"] or "").upper() for row in events})
    raw_start = first_time - timedelta(
        days=HISTORICAL_BASELINE_DAYS,
        minutes=max(CORE_WINDOWS_MINUTES) + MAX_POINT_AGE_MINUTES,
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
            end=last_time,
        )

    rows = build_feature_rows(
        events,
        price_oi_rows=price_rows,
        futures_rows=futures_rows,
        spot_rows=spot_rows,
        prior_events=prior_events,
        windows_minutes=CORE_WINDOWS_MINUTES,
        max_pain_by_event_id=max_pain_by_event_id,
    )
    return _json_safe(
        {
            "available": True,
            "feature_schema_version": FEATURE_SCHEMA_VERSION,
            "outcome_method_version": VERIFIED_OUTCOME_METHOD,
            "outcome_quality": VERIFIED_OUTCOME_QUALITY,
            "horizon_minutes": horizon,
            "lookback_days": days,
            "sample_size": len(rows),
            "first_alert_time_utc": first_time,
            "last_alert_time_utc": last_time,
            "chronological_order": "ascending",
            "historical_baseline": {
                "lookback_days": HISTORICAL_BASELINE_DAYS,
                "minimum_samples": HISTORICAL_BASELINE_MIN_SAMPLES,
                "session_policy": "same-symbol exact ACTIVE/WEEKEND composition matched prior-only",
                "session_timezone": "America/New_York",
                "session_definition": "SUN_18_ET__FRI_20_ET_ACTIVE",
                "composition_tolerance": HISTORICAL_BASELINE_COMPOSITION_TOLERANCE,
            },
            "coverage": coverage,
            "rows": rows,
        }
    )


def load_formula_dataset(
    *,
    lookback_days: int = 3650,
    horizon_minutes: int = 240,
    limit: int = 2000,
) -> Dict[str, Any]:
    """Select the safest ready dataset for automatic formula discovery.

    ``auto`` prefers the full raw-market replay only after its coverage gate is
    met.  Until then it preserves the delivered-alert dataset and reports the
    replay readiness reason.  An operator may explicitly request ``alerts`` or
    ``historical_replay`` for bounded research runs.
    """
    mode = os.getenv("FORMULA_DISCOVERY_DATASET_MODE", "auto").strip().lower()
    if mode not in {"auto", "alerts", "historical_replay"}:
        mode = "auto"
    if mode in {"auto", "historical_replay"}:
        replay = load_historical_replay_dataset(
            lookback_days=lookback_days,
            horizon_minutes=horizon_minutes,
            limit=limit,
        )
        ready = bool((replay.get("coverage") or {}).get("replacement_ready"))
        if mode == "historical_replay" or ready:
            return replay
    else:
        replay = None

    alerts = _load_alert_formula_dataset(
        lookback_days=lookback_days,
        horizon_minutes=horizon_minutes,
        limit=limit,
    )
    if replay is not None:
        alerts = dict(alerts)
        alerts["historical_replay_readiness"] = replay.get("coverage") or {
            "reason": replay.get("reason")
        }
    return alerts


def load_shadow_feature_rows_by_horizon(
    event_ids_by_horizon: Mapping[int, Sequence[int]],
) -> Dict[tuple[int, int], Dict[str, Any]]:
    """Build prior-only Shadow rows keyed by ``(event_id, horizon)``.

    The formula horizon is injected before the feature row is built so the
    decision-time weekend width reference uses the correct future window.  The
    reference itself is calculated exclusively from raw history that predates
    the event; realized return/MFE/MAE are never loaded here.
    """
    unsupported = sorted(
        {
            int(horizon)
            for horizon, event_ids in event_ids_by_horizon.items()
            if event_ids and int(horizon) not in {60, 240, 720, 1440}
        }
    )
    if unsupported:
        raise ValueError(f"unsupported Shadow horizons: {unsupported}")
    normalized_by_horizon = {
        int(horizon): sorted(
            {int(event_id) for event_id in event_ids if int(event_id) > 0}
        )
        for horizon, event_ids in event_ids_by_horizon.items()
        if int(horizon) in {60, 240, 720, 1440}
    }
    normalized_by_horizon = {
        horizon: event_ids
        for horizon, event_ids in normalized_by_horizon.items()
        if event_ids
    }
    normalized = sorted(
        {event_id for event_ids in normalized_by_horizon.values() for event_id in event_ids}
    )
    if not normalized:
        return {}
    if len(normalized) > 250:
        raise ValueError("shadow feature batch is limited to 250 distinct events")
    research_url = _research_database_url()
    raw_url = _raw_database_url()
    if not research_url or not raw_url:
        raise RuntimeError("research and raw market archives must both be configured")

    with _connect(research_url) as research_conn:
        events = _load_delivered_events_by_id(research_conn, normalized)
        if not events:
            return {}
        first_time = min(_as_utc(row["alert_time_utc"]) for row in events)
        last_time = max(_as_utc(row["alert_time_utc"]) for row in events)
        symbols = sorted({str(row["symbol"] or "").upper() for row in events})
        raw_start = first_time - timedelta(
            days=HISTORICAL_BASELINE_DAYS,
            minutes=max(CORE_WINDOWS_MINUTES) + MAX_POINT_AGE_MINUTES,
        )
        prior_events = _load_prior_events(
            research_conn,
            first_time - timedelta(minutes=max(SEQUENCE_WINDOWS_MINUTES)),
            last_time,
        )
        prospective_events = [
            row for row in events if str(row.get("event_kind") or "") == "DECISION_SAMPLE"
        ]
        alert_events = [
            row for row in events if str(row.get("event_kind") or "") == "ALERT"
        ]
        # Delivered ALERT compatibility keeps its old prior-only archive
        # reconstruction. Prospective v6 samples never consult Max-Pain here;
        # they consume only the wrapper frozen in their authoritative slot.
        max_pain_by_event_id = _load_max_pain_features_batch(
            research_conn, alert_events
        )
        if prospective_events:
            (
                frozen_price_rows,
                frozen_oi_rows,
                frozen_futures_rows,
                frozen_spot_rows,
                frozen_max_pain_by_event_id,
            ) = _load_prospective_frozen_rows(
                research_conn,
                symbols=sorted(
                    {str(row["symbol"] or "").upper() for row in prospective_events}
                ),
                start=raw_start,
                end=last_time,
            )
            max_pain_by_event_id.update(frozen_max_pain_by_event_id)
        else:
            frozen_price_rows = []
            frozen_oi_rows = []
            frozen_futures_rows = []
            frozen_spot_rows = []
            frozen_max_pain_by_event_id = {}

    if alert_events:
        with _connect(raw_url) as raw_conn:
            price_rows, futures_rows, spot_rows = _load_raw_rows(
                raw_conn,
                symbols=sorted(
                    {str(row["symbol"] or "").upper() for row in alert_events}
                ),
                start=raw_start,
                end=last_time,
            )
    else:
        price_rows = []
        futures_rows = []
        spot_rows = []
    events_by_id = {int(row["event_id"]): dict(row) for row in events}
    horizon_events = []
    for horizon, event_ids in sorted(normalized_by_horizon.items()):
        for event_id in event_ids:
            source = events_by_id.get(event_id)
            if source is None:
                continue
            event = dict(source)
            event["horizon_minutes"] = horizon
            horizon_events.append(event)
    prospective_horizon_events = [
        event
        for event in horizon_events
        if str(event.get("event_kind") or "") == "DECISION_SAMPLE"
    ]
    alert_horizon_events = [
        event
        for event in horizon_events
        if str(event.get("event_kind") or "") == "ALERT"
    ]
    rows: list[Dict[str, Any]] = []
    if prospective_horizon_events:
        frozen_rows = build_feature_rows(
            prospective_horizon_events,
            price_oi_rows=frozen_price_rows,
            oi_rows=frozen_oi_rows,
            futures_rows=frozen_futures_rows,
            spot_rows=frozen_spot_rows,
            prior_events=prior_events,
            windows_minutes=CORE_WINDOWS_MINUTES,
            max_pain_by_event_id=max_pain_by_event_id,
        )
        for row in frozen_rows:
            row["decision_input_policy_version"] = (
                PROSPECTIVE_FROZEN_INPUT_POLICY_VERSION
            )
        rows.extend(frozen_rows)
    if alert_horizon_events:
        legacy_rows = build_feature_rows(
            alert_horizon_events,
            price_oi_rows=price_rows,
            futures_rows=futures_rows,
            spot_rows=spot_rows,
            prior_events=prior_events,
            windows_minutes=CORE_WINDOWS_MINUTES,
            max_pain_by_event_id=max_pain_by_event_id,
        )
        for row in legacy_rows:
            row["decision_input_policy_version"] = (
                "legacy-mutable-raw-archive-reconstruction"
            )
        rows.extend(legacy_rows)
    result: Dict[tuple[int, int], Dict[str, Any]] = {}
    for row in rows:
        event_id = row.get("event", {}).get("event_id")
        horizon = row.get("outcome_label", {}).get("horizon_minutes")
        if event_id is not None and horizon is not None:
            result[(int(event_id), int(horizon))] = row
    return result


def load_shadow_feature_rows(event_ids: Sequence[int]) -> Dict[int, Dict[str, Any]]:
    """Backward-compatible decision-time rows without a formula horizon.

    Formula Shadow validation must call ``load_shadow_feature_rows_by_horizon``.
    This wrapper remains for read-only callers that do not need weekend width
    calibration and therefore receives no relaxation.
    """
    normalized = sorted({int(event_id) for event_id in event_ids if int(event_id) > 0})
    if not normalized:
        return {}
    if len(normalized) > 250:
        raise ValueError("shadow feature batch is limited to 250 events")
    research_url = _research_database_url()
    raw_url = _raw_database_url()
    if not research_url or not raw_url:
        raise RuntimeError("research and raw market archives must both be configured")
    with _connect(research_url) as research_conn:
        events = _load_delivered_events_by_id(research_conn, normalized)
        # Prospective samples require an explicit formula horizon so they can
        # be rebuilt exclusively from immutable slot series. This legacy
        # wrapper fails closed for them instead of touching mutable archives.
        events = [
            row for row in events if str(row.get("event_kind") or "") == "ALERT"
        ]
        if not events:
            return {}
        first_time = min(_as_utc(row["alert_time_utc"]) for row in events)
        last_time = max(_as_utc(row["alert_time_utc"]) for row in events)
        prior_events = _load_prior_events(
            research_conn,
            first_time - timedelta(minutes=max(SEQUENCE_WINDOWS_MINUTES)),
            last_time,
        )
        max_pain_by_event_id = _load_max_pain_features_batch(
            research_conn, events
        )
    symbols = sorted({str(row["symbol"] or "").upper() for row in events})
    raw_start = first_time - timedelta(
        days=HISTORICAL_BASELINE_DAYS,
        minutes=max(CORE_WINDOWS_MINUTES) + MAX_POINT_AGE_MINUTES,
    )
    with _connect(raw_url) as raw_conn:
        price_rows, futures_rows, spot_rows = _load_raw_rows(
            raw_conn, symbols=symbols, start=raw_start, end=last_time
        )
    rows = build_feature_rows(
        events,
        price_oi_rows=price_rows,
        futures_rows=futures_rows,
        spot_rows=spot_rows,
        prior_events=prior_events,
        windows_minutes=CORE_WINDOWS_MINUTES,
        max_pain_by_event_id=max_pain_by_event_id,
    )
    return {
        int(row["event"]["event_id"]): row
        for row in rows
        if row.get("event", {}).get("event_id") is not None
    }
