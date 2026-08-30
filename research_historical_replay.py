"""Resumable historical raw-market opportunity replay.

The replay evaluates every eligible archived Price/OI observation as a neutral
research opportunity.  It joins only information that was available at that
time, then labels both LONG and SHORT paths from canonical one-minute spot
candles.  Raw candles are processed in bounded chunks and discarded; only the
compact MFE/MAE/return summaries are persisted.

This is an offline research command.  It is never imported by the production
Watch loop and refuses to write unless ``HISTORICAL_REPLAY_BACKFILL=1``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import math
import os
import time
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - validated at runtime
    psycopg = None
    dict_row = None

import binance_spot_price_path
import canonical_price_path
import market_session_baseline
import research_no_dwell_outcome
import research_session_width


REPLAY_VERSION = "historical-raw-opportunity-replay-v2-prior-session-width"
COVERAGE_SCOPE_VERSION = "global-coherent-current-replay-all-horizons-v1"
_TRUE = {"1", "true", "yes", "on"}
_HORIZONS = (60, 240, 720, 1440)
_LOCK_ID = 94837243
_FETCH_SEGMENT_MINUTES = 1900
_HYPERLIQUID_RECENT_ONE_MINUTE_CANDLES = 5000
_HORIZON_CLOSE_GRACE_MINUTES = 5


def sibling_reference_coherence_sql(alias: str) -> str:
    """Return a fail-closed same-anchor route/reference consistency check."""
    normalized = str(alias or "").strip()
    if not normalized.replace("_", "").isalnum():
        raise ValueError("invalid SQL alias for sibling coherence")
    return f"""
        NOT EXISTS (
            SELECT 1
            FROM research_historical_opportunity_outcomes sibling
            WHERE sibling.symbol={normalized}.symbol
              AND sibling.observation_time_utc={normalized}.observation_time_utc
              AND sibling.replay_version={normalized}.replay_version
              AND sibling.first_touch_method_version=
                    {normalized}.first_touch_method_version
              AND (
                    sibling.source_observation_time_utc IS DISTINCT FROM
                        {normalized}.source_observation_time_utc
                    OR sibling.reference_time_utc IS DISTINCT FROM
                        {normalized}.reference_time_utc
                    OR sibling.reference_price IS DISTINCT FROM
                        {normalized}.reference_price
                    OR sibling.outcome_method_version IS DISTINCT FROM
                        {normalized}.outcome_method_version
                    OR LOWER(sibling.exchange) IS DISTINCT FROM
                        LOWER({normalized}.exchange)
                    OR LOWER(sibling.market) IS DISTINCT FROM
                        LOWER({normalized}.market)
                    OR UPPER(sibling.pair) IS DISTINCT FROM
                        UPPER({normalized}.pair)
                    OR sibling.interval_seconds IS DISTINCT FROM
                        {normalized}.interval_seconds
                    OR sibling.provenance IS DISTINCT FROM
                        {normalized}.provenance
                    OR sibling.data_quality_status IS DISTINCT FROM
                        {normalized}.data_quality_status
                    OR sibling.first_touch_data_quality_status IS DISTINCT FROM
                        {normalized}.first_touch_data_quality_status
                  )
        )
    """


def _utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))


def _hype_one_minute_observation_floor(
    now: Optional[datetime] = None,
) -> datetime:
    """Earliest HYPE anchor whose reference minute remains in official history."""
    current = _floor_minute(now or datetime.now(timezone.utc))
    # The replay asks for two minutes before an anchor so it can prove that the
    # reference candle closed before the decision time. Hyperliquid exposes
    # only its most recent 5000 candles, so reserve those two minutes here.
    return current - timedelta(
        minutes=_HYPERLIQUID_RECENT_ONE_MINUTE_CANDLES - 2
    )


def _research_database_url() -> str:
    dedicated = os.getenv("RESEARCH_DATABASE_URL", "").strip()
    use_primary = (
        os.getenv("RESEARCH_USE_PRIMARY_DATABASE", "").strip().lower() in _TRUE
    )
    if dedicated:
        return dedicated
    if use_primary:
        return os.getenv("DATABASE_URL", "").strip()
    return ""


def _raw_database_url() -> str:
    return os.getenv("DATABASE_URL", "").strip()


def _connect(url: str, *, read_only: bool):
    if not url:
        raise RuntimeError("Required PostgreSQL archive is not configured")
    if psycopg is None:
        raise RuntimeError("psycopg is unavailable")
    options = "-c statement_timeout=120000"
    if read_only:
        options += " -c default_transaction_read_only=on"
    return psycopg.connect(
        url,
        row_factory=dict_row,
        connect_timeout=8,
        options=options,
    )


def _table_exists(conn, name: str) -> bool:
    row = conn.execute(
        "SELECT to_regclass(%s) AS relation", (f"public.{name}",)
    ).fetchone()
    return bool(row and row.get("relation"))


def _symbols_from_env() -> tuple[str, ...]:
    values = []
    for raw in os.getenv("HISTORICAL_REPLAY_SYMBOLS", "").split(","):
        symbol = raw.strip().upper()
        if not symbol:
            continue
        if len(symbol) > 20 or not symbol.replace("-", "").isalnum():
            raise ValueError(f"Invalid replay symbol: {symbol}")
        if symbol not in values:
            values.append(symbol)
    return tuple(values)


def _horizons_from_env() -> tuple[int, ...]:
    values = []
    for raw in os.getenv(
        "HISTORICAL_REPLAY_HORIZONS", "60,240,720,1440"
    ).split(","):
        try:
            value = int(raw.strip())
        except ValueError:
            continue
        if value in _HORIZONS and value not in values:
            values.append(value)
    return tuple(values or _HORIZONS)


def _optional_time(name: str) -> Optional[datetime]:
    value = os.getenv(name, "").strip()
    return _utc(value) if value else None


def fully_closed_end(
    horizons: Sequence[int], *, now: Optional[datetime] = None
) -> datetime:
    """Freeze an end time whose longest requested outcome is fully closed."""
    normalized = [int(value) for value in horizons if int(value) in _HORIZONS]
    if not normalized:
        raise ValueError("at least one supported horizon is required")
    current = _utc(now or datetime.now(timezone.utc))
    return current - timedelta(
        minutes=max(normalized) + _HORIZON_CLOSE_GRACE_MINUTES
    )


@dataclass(frozen=True)
class Anchor:
    symbol: str
    observation_time_utc: datetime
    source_observation_time_utc: datetime


def _load_anchors(
    conn,
    *,
    start: Optional[datetime],
    end: Optional[datetime],
    symbols: Sequence[str],
) -> list[Anchor]:
    """Load canonical decision times without double-counting archive overlap."""
    params: list[Any] = []
    archive_filters = ["symbol<>'HYPE'"]
    live_filters = ["data_quality_status='PASS'"]
    if start is not None:
        # Historical CoinGlass rows become usable 32 minutes after candle open.
        archive_filters.append("candle_time >= %s")
        params.append(start - timedelta(minutes=32))
    if end is not None:
        archive_filters.append("candle_time <= %s")
        params.append(end - timedelta(minutes=32))
    if symbols:
        archive_filters.append("symbol=ANY(%s)")
        params.append(list(symbols))
    archive = conn.execute(
        f"""
        SELECT symbol, candle_time
        FROM oi_price_history
        WHERE {' AND '.join(archive_filters)}
        ORDER BY symbol, candle_time
        """,
        params,
    ).fetchall()

    anchors: list[Anchor] = []
    for row in archive:
        source_time = _utc(row["candle_time"])
        available = market_session_baseline.closed_candle_available_at(source_time)
        if start is not None and available < start:
            continue
        if end is not None and available > end:
            continue
        anchors.append(
            Anchor(str(row["symbol"]).upper(), available, source_time)
        )

    if _table_exists(conn, "oi_regime_snapshots"):
        live_params: list[Any] = []
        if start is not None:
            live_filters.append("live.collected_at >= %s")
            live_params.append(start)
        if end is not None:
            live_filters.append("live.collected_at <= %s")
            live_params.append(end)
        if symbols:
            live_filters.append("live.symbol=ANY(%s)")
            live_params.append(list(symbols))
        live_filters.append("(live.symbol<>'HYPE' OR live.collected_at >= %s)")
        live_params.append(_hype_one_minute_observation_floor())
        live_filters.extend(
            (
                """(
                    (live.symbol='HYPE' AND live.price_source='hyperliquid')
                    OR
                    (live.symbol<>'HYPE' AND live.price_source='binance_spot')
                )""",
                """(
                    live.symbol='HYPE'
                    OR live.collected_at > COALESCE(
                        (SELECT MAX(backfill.candle_time)
                         FROM oi_price_history backfill
                         WHERE backfill.symbol=live.symbol),
                        '-infinity'::timestamptz
                    )
                )""",
            )
        )
        live = conn.execute(
            f"""
            SELECT live.symbol, live.collected_at
            FROM oi_regime_snapshots live
            WHERE {' AND '.join(live_filters)}
            ORDER BY live.symbol, live.collected_at
            """,
            live_params,
        ).fetchall()
        anchors.extend(
            Anchor(
                str(row["symbol"]).upper(),
                _utc(row["collected_at"]),
                _utc(row["collected_at"]),
            )
            for row in live
        )

    deduplicated = {
        (anchor.symbol, anchor.observation_time_utc): anchor for anchor in anchors
    }
    return sorted(
        deduplicated.values(),
        key=lambda item: (item.symbol, item.observation_time_utc),
    )


def _floor_minute(value: datetime) -> datetime:
    moment = _utc(value)
    return moment.replace(second=0, microsecond=0)


def _ceil_minute(value: datetime) -> datetime:
    moment = _utc(value)
    floor = _floor_minute(moment)
    return floor if moment == floor else floor + timedelta(minutes=1)


def _fetch_range(
    symbol: str, start: datetime, end: datetime, *, pause_seconds: float
) -> Dict[str, Any]:
    """Fetch an arbitrary range through bounded canonical-provider calls."""
    cursor = _floor_minute(start)
    finish = _utc(end)
    candles_by_open: Dict[datetime, binance_spot_price_path.SpotCandle] = {}
    metadata: Optional[Dict[str, Any]] = None
    validated_route: Optional[Dict[str, Any]] = None
    expected_total = 0
    while cursor < finish:
        segment_end = min(
            finish, cursor + timedelta(minutes=_FETCH_SEGMENT_MINUTES)
        )
        result = canonical_price_path.fetch_closed_candles(
            symbol, cursor, segment_end
        )
        segment_route = canonical_price_path.validated_route(
            symbol, result, require_complete=True
        )
        if validated_route is None:
            validated_route = segment_route
            metadata = dict(result)
        elif segment_route != validated_route:
            raise ValueError("canonical price route changed between replay segments")
        expected_total += int(result.get("expected_candles") or 0)
        for candle in result.get("candles") or []:
            candles_by_open[_utc(candle.open_time_utc)] = candle
        if segment_end >= finish:
            break
        cursor = segment_end
        if pause_seconds > 0:
            time.sleep(pause_seconds)
    if metadata is None:
        raise RuntimeError("canonical price provider returned no metadata")
    candles = [candles_by_open[key] for key in sorted(candles_by_open)]
    metadata["candles"] = candles
    metadata["expected_candles"] = expected_total
    metadata["complete"] = len(candles) == expected_total
    return metadata


def _reference_candle(
    candles: Sequence[binance_spot_price_path.SpotCandle],
    observation_time: datetime,
) -> Optional[binance_spot_price_path.SpotCandle]:
    eligible = [
        candle
        for candle in candles
        if _utc(candle.close_time_utc) < _utc(observation_time)
    ]
    if not eligible:
        return None
    candle = eligible[-1]
    if _utc(observation_time) - _utc(candle.close_time_utc) > timedelta(minutes=2):
        return None
    return candle


def _outcome_candles(
    candles: Sequence[binance_spot_price_path.SpotCandle],
    observation_time: datetime,
    horizon_minutes: int,
) -> list[binance_spot_price_path.SpotCandle]:
    first_open = _ceil_minute(observation_time)
    horizon_time = _utc(observation_time) + timedelta(minutes=horizon_minutes)
    return [
        candle
        for candle in candles
        if _utc(candle.open_time_utc) >= first_open
        and _utc(candle.close_time_utc) <= horizon_time
    ]


def _expected_outcome_candles(observation_time: datetime, horizon_minutes: int) -> int:
    first_open = _ceil_minute(observation_time)
    horizon_time = _utc(observation_time) + timedelta(minutes=horizon_minutes)
    last_open = _floor_minute(horizon_time - timedelta(seconds=59, milliseconds=999))
    if last_open < first_open:
        return 0
    return int((last_open - first_open).total_seconds() // 60) + 1


def _expected_last_outcome_close(
    observation_time: datetime, horizon_minutes: int
) -> datetime:
    first_open = _ceil_minute(observation_time)
    samples = _expected_outcome_candles(observation_time, horizon_minutes)
    if samples <= 0:
        raise ValueError("outcome horizon contains no fully closed candle")
    return first_open + timedelta(minutes=samples) - timedelta(milliseconds=1)


def _expected_reference_close(observation_time: datetime) -> datetime:
    """Return the latest canonical 1m candle close strictly before a decision."""
    observation = _utc(observation_time)
    minute_open = _floor_minute(observation)
    current_close = minute_open + timedelta(minutes=1, milliseconds=-1)
    if current_close < observation:
        return current_close
    return minute_open - timedelta(milliseconds=1)


def _source_anchor_is_canonical(anchor: Anchor) -> bool:
    """Bind a replay anchor to one of the two source-time writer contracts."""
    observation = _utc(anchor.observation_time_utc)
    source = _utc(anchor.source_observation_time_utc)
    if source == observation:
        return True
    if str(anchor.symbol).upper() == "HYPE":
        return False
    archive_source_aligned = (
        source.second == 0
        and source.microsecond == 0
        and source.minute % 30 == 0
    )
    return bool(
        archive_source_aligned
        and market_session_baseline.closed_candle_available_at(source)
        == observation
    )


def _is_expected_outcome_close(
    value: datetime, observation_time: datetime, horizon_minutes: int
) -> bool:
    first_close = _ceil_minute(observation_time) + timedelta(
        minutes=1, milliseconds=-1
    )
    last_close = _expected_last_outcome_close(
        observation_time, horizon_minutes
    )
    candidate = _utc(value)
    if candidate < first_close or candidate > last_close:
        return False
    offset = candidate - first_close
    offset_seconds = offset.days * 86400 + offset.seconds
    return offset.microseconds == 0 and offset_seconds % 60 == 0


def _is_expected_outcome_elapsed(
    value: int, observation_time: datetime, horizon_minutes: int
) -> bool:
    if type(value) is not int or value <= 0:
        return False
    start = _utc(observation_time)
    first_close = _ceil_minute(start) + timedelta(
        minutes=1, milliseconds=-1
    )
    last_close = _expected_last_outcome_close(start, horizon_minutes)
    first_elapsed = int((first_close - start).total_seconds())
    last_elapsed = int((last_close - start).total_seconds())
    elapsed = value
    return (
        first_elapsed <= elapsed <= last_elapsed
        and (elapsed - first_elapsed) % 60 == 0
    )


def _metric_payload(metrics: Dict[str, Any]) -> Dict[str, Any]:
    keys = (
        "measured_at_utc",
        "price_at_horizon",
        "raw_return_pct",
        "directional_return_pct",
        "max_favorable_price",
        "max_adverse_price",
        "mfe_pct",
        "mae_pct",
        "time_to_first_progress_seconds",
        "time_to_mfe_seconds",
    )
    return {key: metrics.get(key) for key in keys}


def _first_touch_payload(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """Compact no-dwell label; legacy full-horizon metrics stay separate."""
    return dict(metrics)


def _mapping(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return dict(value) if hasattr(value, "items") else {}


def load_canonical_reference_rows(
    conn,
    *,
    start: datetime,
    end: datetime,
    symbols: Sequence[str],
) -> list[Dict[str, Any]]:
    """Load proven canonical decision-time prices for calibration/features.

    Legacy non-HYPE rows are accepted only when their relational metadata
    proves Binance Spot USDT under the current canonical path method.  Legacy
    HYPE rows cannot prove instrument ``@107`` and are intentionally excluded.
    """
    normalized_symbols = sorted(
        {str(symbol or "").strip().upper() for symbol in symbols if symbol}
    )
    if not normalized_symbols or _utc(end) <= _utc(start):
        return []
    sibling_coherence = sibling_reference_coherence_sql("price_ref")
    rows = conn.execute(
        f"""
        SELECT DISTINCT ON (
                   price_ref.symbol, price_ref.observation_time_utc
               )
               price_ref.symbol, price_ref.observation_time_utc,
               price_ref.source_observation_time_utc,
               price_ref.reference_time_utc, price_ref.reference_price,
               price_ref.exchange, price_ref.market, price_ref.pair,
               price_ref.interval_seconds, price_ref.provenance,
               price_ref.data_quality_status,
               price_ref.outcome_method_version, price_ref.replay_version
        FROM research_historical_opportunity_outcomes price_ref
        WHERE price_ref.observation_time_utc >= %s
          AND price_ref.observation_time_utc < %s
          AND price_ref.symbol=ANY(%s)
          AND price_ref.outcome_method_version=%s
          AND price_ref.data_quality_status=ANY(%s)
          AND ({sibling_coherence})
        ORDER BY price_ref.symbol, price_ref.observation_time_utc,
                 (price_ref.replay_version=%s) DESC,
                 price_ref.horizon_minutes
        """,
        (
            _utc(start),
            _utc(end),
            normalized_symbols,
            canonical_price_path.METHOD_VERSION,
            list(canonical_price_path.COMPLETE_QUALITIES),
            REPLAY_VERSION,
        ),
    ).fetchall()
    deduplicated: Dict[tuple[str, datetime], Dict[str, Any]] = {}
    for source in rows:
        row = dict(source)
        if not canonical_price_path.persisted_reference_is_canonical(
            row, required_hype_replay_version=REPLAY_VERSION
        ):
            continue
        if (
            str(row.get("replay_version") or "") == REPLAY_VERSION
            and not _new_route_provenance_is_coherent(row)
        ):
            continue
        observation_time = _utc(row["observation_time_utc"])
        source_observation_time = _utc(row["source_observation_time_utc"])
        symbol = str(row.get("symbol") or "").upper()
        if not _source_anchor_is_canonical(
            Anchor(symbol, observation_time, source_observation_time)
        ):
            continue
        reference_time = _utc(row["reference_time_utc"])
        if reference_time != _expected_reference_close(observation_time):
            continue
        try:
            reference_price = _strict_finite_number(row["reference_price"])
        except (TypeError, ValueError, OverflowError):
            continue
        expected_quality = (
            canonical_price_path.HYPERLIQUID_COMPLETE
            if symbol == "HYPE"
            else canonical_price_path.BINANCE_COMPLETE
        )
        if (
            reference_price <= 0.0
            or str(row.get("data_quality_status") or "")
            != expected_quality
        ):
            continue
        row["symbol"] = symbol
        row["observation_time_utc"] = observation_time
        row["source_observation_time_utc"] = source_observation_time
        row["reference_time_utc"] = reference_time
        row["reference_price"] = reference_price
        deduplicated.setdefault((row["symbol"], observation_time), row)
    return sorted(
        deduplicated.values(),
        key=lambda row: (row["symbol"], row["observation_time_utc"]),
    )


def build_canonical_width_index(
    rows: Sequence[Dict[str, Any]], *, horizons: Sequence[int]
) -> Dict[tuple[str, int], research_session_width.PriceWidthSeries]:
    grouped: Dict[str, list[tuple[datetime, float]]] = {}
    for row in rows:
        grouped.setdefault(str(row["symbol"]).upper(), []).append(
            (_utc(row["reference_time_utc"]), float(row["reference_price"]))
        )
    return research_session_width.build_price_width_index(
        price_points=grouped,
        horizons_minutes=horizons,
        max_point_age_minutes=research_session_width.MAX_POINT_AGE_MINUTES,
    )


def _calibration_reference(
    *,
    anchor: Anchor,
    horizon: int,
    reference_time_utc: datetime,
    width_index: Dict[
        tuple[str, int], research_session_width.PriceWidthSeries
    ],
) -> Dict[str, Any]:
    return research_session_width.movement_width_reference(
        symbol=anchor.symbol,
        event_time=anchor.observation_time_utc,
        horizon_minutes=horizon,
        as_of_utc=reference_time_utc,
        historical_index=width_index,
    )


def _new_route_provenance_is_coherent(row: Dict[str, Any]) -> bool:
    try:
        stored = json.loads(str(row.get("provenance") or ""))
    except (TypeError, json.JSONDecodeError):
        return False
    if not isinstance(stored, dict):
        return False
    expected = {
        "provenance_version": canonical_price_path.PRICE_PROVENANCE_VERSION,
        "method_version": canonical_price_path.METHOD_VERSION,
        "symbol": str(row.get("symbol") or "").upper(),
        "exchange": str(row.get("exchange") or "").lower(),
        "market": str(row.get("market") or "").lower(),
        "pair": str(row.get("pair") or "").upper(),
        "instrument": "@107" if str(row.get("symbol") or "").upper() == "HYPE" else None,
        "interval": "1m",
        "interval_seconds": 60,
        "provider_provenance": "EXCHANGE_API_HISTORICAL_CANDLES_IMPORTED",
    }
    return _type_strict_json_equal(stored, expected)


def _strict_finite_number(value: Any) -> float:
    """Parse one persisted numeric without admitting JSON booleans/strings."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("persisted replay metric is not a JSON number")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("persisted replay metric is not finite")
    return result


def _strict_json_int(value: Any, *, minimum: int = 0) -> int:
    """Accept only an actual JSON integer, never bool/float/string coercions."""
    if type(value) is not int:
        raise TypeError("persisted replay field is not a JSON integer")
    if value < minimum:
        raise ValueError("persisted replay integer is below its minimum")
    return value


def _type_strict_json_equal(left: Any, right: Any) -> bool:
    """Compare canonical JSON without Python's ``True == 1`` shortcut."""
    try:
        return json.dumps(
            left,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ) == json.dumps(
            right,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError, OverflowError):
        return False


def _coherent_path_metrics(
    source: Dict[str, Any],
    *,
    reference_price: float,
    observation_time_utc: datetime,
    horizon_minutes: int,
) -> Optional[Dict[str, Dict[str, Any]]]:
    """Validate internally checkable MFE/MAE and endpoint identities."""
    try:
        stored_price = _strict_finite_number(source["price_at_horizon"])
        stored_return = _strict_finite_number(source["raw_return_pct"])
        expected_close = _expected_last_outcome_close(
            observation_time_utc, horizon_minutes
        )
        expected_return = (
            (stored_price - reference_price) / reference_price * 100.0
        )
        if not (
            math.isfinite(stored_price)
            and stored_price > 0.0
            and math.isfinite(stored_return)
            and math.isclose(
                stored_return, expected_return, rel_tol=1e-12, abs_tol=1e-9
            )
        ):
            return None
        metrics_by_direction: Dict[str, Dict[str, Any]] = {}
        for direction, key, sign in (
            ("LONG", "long_metrics", 1.0),
            ("SHORT", "short_metrics", -1.0),
        ):
            metrics = _mapping(source.get(key))
            measured = _utc(metrics["measured_at_utc"])
            price_at_horizon = _strict_finite_number(
                metrics["price_at_horizon"]
            )
            raw_return = _strict_finite_number(metrics["raw_return_pct"])
            directional_return = _strict_finite_number(
                metrics["directional_return_pct"]
            )
            favorable_price = _strict_finite_number(
                metrics["max_favorable_price"]
            )
            adverse_price = _strict_finite_number(
                metrics["max_adverse_price"]
            )
            mfe = _strict_finite_number(metrics["mfe_pct"])
            mae = _strict_finite_number(metrics["mae_pct"])
            time_to_mfe = _strict_json_int(
                metrics["time_to_mfe_seconds"]
            )
            progress = metrics.get("time_to_first_progress_seconds")
            progress_seconds = (
                _strict_json_int(progress, minimum=1)
                if progress is not None
                else None
            )
            numeric = (
                price_at_horizon,
                raw_return,
                directional_return,
                favorable_price,
                adverse_price,
                mfe,
                mae,
                time_to_mfe,
            )
            if not all(math.isfinite(value) for value in numeric):
                return None
            if progress_seconds is not None and not math.isfinite(
                progress_seconds
            ):
                return None
            if not (
                measured == expected_close
                and math.isclose(
                    price_at_horizon,
                    stored_price,
                    rel_tol=1e-12,
                    abs_tol=1e-10,
                )
                and math.isclose(
                    raw_return,
                    stored_return,
                    rel_tol=1e-12,
                    abs_tol=1e-9,
                )
                and math.isclose(
                    directional_return,
                    sign * stored_return,
                    rel_tol=1e-12,
                    abs_tol=1e-9,
                )
                and favorable_price > 0.0
                and adverse_price > 0.0
                and mfe >= 0.0
                and mae >= 0.0
                and (
                    direction != "LONG"
                    or (
                        favorable_price >= reference_price
                        and adverse_price <= reference_price
                    )
                )
                and (
                    direction != "SHORT"
                    or (
                        favorable_price <= reference_price
                        and adverse_price >= reference_price
                    )
                )
                and 0 <= time_to_mfe <= horizon_minutes * 60
                and (
                    progress_seconds is None
                    or 0
                    <= progress_seconds
                    <= horizon_minutes * 60
                )
            ):
                return None
            if direction == "LONG":
                expected_mfe = max(
                    0.0,
                    (favorable_price - reference_price)
                    / reference_price
                    * 100.0,
                )
                expected_mae = max(
                    0.0,
                    (reference_price - adverse_price)
                    / reference_price
                    * 100.0,
                )
            else:
                expected_mfe = max(
                    0.0,
                    (reference_price - favorable_price)
                    / reference_price
                    * 100.0,
                )
                expected_mae = max(
                    0.0,
                    (adverse_price - reference_price)
                    / reference_price
                    * 100.0,
                )
            if not (
                math.isclose(mfe, expected_mfe, rel_tol=1e-12, abs_tol=1e-9)
                and math.isclose(
                    mae, expected_mae, rel_tol=1e-12, abs_tol=1e-9
                )
            ):
                return None
            if math.isclose(mfe, 0.0, rel_tol=0.0, abs_tol=1e-12):
                if not (
                    math.isclose(
                        favorable_price,
                        reference_price,
                        rel_tol=1e-12,
                        abs_tol=1e-10,
                    )
                    and math.isclose(
                        float(time_to_mfe), 0.0, rel_tol=0.0, abs_tol=0.0
                    )
                    and progress_seconds is None
                ):
                    return None
            elif not (
                _is_expected_outcome_elapsed(
                    time_to_mfe, observation_time_utc, horizon_minutes
                )
                and progress_seconds is not None
                and _is_expected_outcome_elapsed(
                    progress_seconds,
                    observation_time_utc,
                    horizon_minutes,
                )
                and progress_seconds <= time_to_mfe
            ):
                return None
            metrics_by_direction[direction] = metrics
        long_metrics = metrics_by_direction["LONG"]
        short_metrics = metrics_by_direction["SHORT"]
        paired_values = (
            (long_metrics["max_favorable_price"], short_metrics["max_adverse_price"]),
            (long_metrics["max_adverse_price"], short_metrics["max_favorable_price"]),
            (long_metrics["mfe_pct"], short_metrics["mae_pct"]),
            (long_metrics["mae_pct"], short_metrics["mfe_pct"]),
        )
        if not all(
            math.isclose(
                _strict_finite_number(left),
                _strict_finite_number(right),
                rel_tol=1e-12,
                abs_tol=1e-9,
            )
            for left, right in paired_values
        ):
            return None
        if not (
            _strict_finite_number(long_metrics["max_adverse_price"])
            <= stored_price
            <= _strict_finite_number(long_metrics["max_favorable_price"])
        ):
            return None
        return metrics_by_direction
    except (
        KeyError,
        TypeError,
        ValueError,
        OverflowError,
        json.JSONDecodeError,
    ):
        return None


def replay_outcome_row_is_coherent(
    row: Dict[str, Any],
    *,
    width_index: Dict[
        tuple[str, int], research_session_width.PriceWidthSeries
    ],
) -> bool:
    """Recompute and validate one v2 first-touch row before reuse."""
    source = dict(row)
    try:
        if source.get("sibling_reference_coherent") is not True:
            return False
        if str(source.get("replay_version") or "") != REPLAY_VERSION:
            return False
        if str(source.get("first_touch_method_version") or "") != (
            research_no_dwell_outcome.METHOD_VERSION
        ):
            return False
        if str(source.get("first_touch_data_quality_status") or "") not in (
            canonical_price_path.COMPLETE_QUALITIES
        ):
            return False
        if not canonical_price_path.persisted_reference_is_canonical(
            source, required_hype_replay_version=REPLAY_VERSION
        ):
            return False
        if not _new_route_provenance_is_coherent(source):
            return False
        symbol = str(source["symbol"]).upper()
        expected_quality = (
            canonical_price_path.HYPERLIQUID_COMPLETE
            if symbol == "HYPE"
            else canonical_price_path.BINANCE_COMPLETE
        )
        if (
            str(source.get("first_touch_data_quality_status") or "")
            != expected_quality
            or str(source.get("data_quality_status") or "")
            != expected_quality
        ):
            return False
        anchor = Anchor(
            symbol=symbol,
            observation_time_utc=_utc(source["observation_time_utc"]),
            source_observation_time_utc=_utc(
                source["source_observation_time_utc"]
            ),
        )
        horizon = _strict_json_int(source["horizon_minutes"], minimum=1)
        if horizon not in _HORIZONS:
            return False
        if not _source_anchor_is_canonical(anchor):
            return False
        reference_time = _utc(source["reference_time_utc"])
        reference_age = anchor.observation_time_utc - reference_time
        if (
            reference_time
            != _expected_reference_close(anchor.observation_time_utc)
            or not timedelta(0) < reference_age <= timedelta(minutes=2)
        ):
            return False
        reference_price = _strict_finite_number(source["reference_price"])
        if not math.isfinite(reference_price) or reference_price <= 0.0:
            return False
        path_metrics = _coherent_path_metrics(
            source,
            reference_price=reference_price,
            observation_time_utc=anchor.observation_time_utc,
            horizon_minutes=horizon,
        )
        if path_metrics is None:
            return False
        expected_path_samples = _expected_outcome_candles(
            anchor.observation_time_utc, horizon
        )
        if (
            expected_path_samples <= 0
            or _strict_json_int(
                source.get("first_touch_path_samples"), minimum=1
            )
            != expected_path_samples
            or _strict_json_int(source.get("path_samples"), minimum=1)
            != expected_path_samples
        ):
            return False
        expected_reference = _calibration_reference(
            anchor=anchor,
            horizon=horizon,
            reference_time_utc=reference_time,
            width_index=width_index,
        )
        expected_policy = research_no_dwell_outcome.freeze_threshold_policy(
            horizon_minutes=horizon,
            decision_time=anchor.observation_time_utc,
            prior_only_reference=expected_reference,
        )
        expected_policy_json = json.loads(_json(expected_policy))
        expected_reference_snapshot = (
            research_no_dwell_outcome.threshold_reference_snapshot(
                expected_reference
            )
        )
        expected_reference_hash = (
            research_no_dwell_outcome.threshold_reference_hash(
                expected_reference_snapshot
            )
        )
        policies = []
        for direction, key in (
            ("LONG", "long_first_touch_metrics"),
            ("SHORT", "short_first_touch_metrics"),
        ):
            metrics = _mapping(source.get(key))
            if (
                metrics.get("method_version")
                != research_no_dwell_outcome.METHOD_VERSION
                or metrics.get("status") not in {"HIT", "MISS"}
                or not isinstance(metrics.get("success"), bool)
                or not isinstance(metrics.get("failure_final"), bool)
                or str(metrics.get("direction") or "") != direction
                or _strict_json_int(
                    metrics.get("horizon_minutes"), minimum=1
                )
                != horizon
                or type(metrics.get("dwell_required_seconds")) is not int
                or metrics.get("dwell_required_seconds") != 0
            ):
                return False
            status = str(metrics.get("status"))
            success = bool(metrics.get("success"))
            if (status == "HIT") != success:
                return False
            if bool(metrics.get("failure_final")) != (status == "MISS"):
                return False
            if metrics.get("post_hit_reversal_policy") != (
                "ignored_for_success"
            ):
                return False
            policy = _mapping(metrics.get("threshold_policy"))
            if not _type_strict_json_equal(policy, expected_policy_json):
                return False
            if (
                str(metrics.get("threshold_source_kind") or "")
                != str(policy.get("threshold_source_kind") or "")
                or str(metrics.get("threshold_source") or "")
                != str(policy.get("threshold_source") or "")
            ):
                return False
            metric_scale = _strict_finite_number(
                metrics.get("threshold_scale_factor")
            )
            metric_threshold = _strict_finite_number(
                metrics.get("qualifying_move_threshold_pct")
            )
            qualifying_price = _strict_finite_number(
                metrics.get("qualifying_move_price")
            )
            pre_qualifying_mae = _strict_finite_number(
                metrics.get("pre_qualifying_mae_pct")
            )
            full_mae = _strict_finite_number(
                path_metrics[direction]["mae_pct"]
            )
            expected_qualifying_price = reference_price * (
                1.0 + metric_threshold / 100.0
                if direction == "LONG"
                else 1.0 - metric_threshold / 100.0
            )
            if not (
                math.isfinite(metric_scale)
                and math.isfinite(metric_threshold)
                and metric_threshold > 0.0
                and math.isfinite(qualifying_price)
                and qualifying_price > 0.0
                and math.isfinite(pre_qualifying_mae)
                and pre_qualifying_mae >= 0.0
                and pre_qualifying_mae <= full_mae + 1e-9
                and math.isclose(
                    metric_scale,
                    _strict_finite_number(policy["threshold_scale_factor"]),
                    rel_tol=0.0,
                    abs_tol=1e-8,
                )
                and math.isclose(
                    metric_threshold,
                    _strict_finite_number(
                        policy["qualifying_move_threshold_pct"]
                    ),
                    rel_tol=0.0,
                    abs_tol=1e-8,
                )
                and math.isclose(
                    qualifying_price,
                    expected_qualifying_price,
                    rel_tol=1e-12,
                    abs_tol=1e-10,
                )
            ):
                return False
            max_favorable_price = _strict_finite_number(
                path_metrics[direction]["max_favorable_price"]
            )
            did_touch = (
                max_favorable_price >= qualifying_price
                if direction == "LONG"
                else max_favorable_price <= qualifying_price
            )
            if (status == "HIT") != did_touch:
                return False
            observed_through = _utc(metrics["observed_through_utc"])
            horizon_end = anchor.observation_time_utc + timedelta(
                minutes=horizon
            )
            if not (
                anchor.observation_time_utc < observed_through <= horizon_end
                and _is_expected_outcome_close(
                    observed_through,
                    anchor.observation_time_utc,
                    horizon,
                )
            ):
                return False
            first_touch_time = metrics.get("first_qualifying_move_time_utc")
            time_to_touch = metrics.get(
                "time_to_first_qualifying_move_seconds"
            )
            if status == "HIT":
                if first_touch_time is None or time_to_touch is None:
                    return False
                first_touch_time = _utc(first_touch_time)
                time_to_touch = _strict_json_int(time_to_touch, minimum=1)
                if not (
                    first_touch_time == observed_through
                    and anchor.observation_time_utc
                    < first_touch_time
                    <= horizon_end
                    and _strict_json_int(
                        path_metrics[direction][
                            "time_to_first_progress_seconds"
                        ],
                        minimum=1,
                    )
                    <= time_to_touch
                    and time_to_touch
                    <= _strict_json_int(
                        path_metrics[direction]["time_to_mfe_seconds"]
                    )
                    and time_to_touch
                    == max(
                        0,
                        int(
                            (
                                first_touch_time
                                - anchor.observation_time_utc
                            ).total_seconds()
                        ),
                    )
                ):
                    return False
            else:
                if first_touch_time is not None or time_to_touch is not None:
                    return False
                if observed_through != _expected_last_outcome_close(
                    anchor.observation_time_utc, horizon
                ):
                    return False
                if not math.isclose(
                    pre_qualifying_mae,
                    full_mae,
                    rel_tol=1e-12,
                    abs_tol=1e-9,
                ):
                    return False
            adverse = metrics.get(
                "qualifying_candle_adverse_excursion_pct"
            )
            ambiguous = metrics.get("qualifying_candle_order_ambiguous")
            if not isinstance(ambiguous, bool):
                return False
            if status == "HIT":
                adverse = _strict_finite_number(adverse)
                if (
                    not math.isfinite(adverse)
                    or adverse < 0.0
                    or adverse > pre_qualifying_mae + 1e-9
                    or ambiguous != (adverse > 0.0)
                ):
                    return False
            elif adverse is not None or ambiguous:
                return False
            stored_reference = policy.get("threshold_reference")
            if not isinstance(stored_reference, Mapping):
                return False
            if not _type_strict_json_equal(
                stored_reference, expected_reference_snapshot
            ):
                return False
            if policy.get("threshold_reference_hash") != expected_reference_hash:
                return False
            if research_no_dwell_outcome.threshold_reference_hash(
                stored_reference
            ) != policy.get("threshold_reference_hash"):
                return False
            if policy.get("threshold_reference_version") != (
                research_session_width.CALIBRATION_VERSION
            ):
                return False
            policies.append(policy)
        return policies[0] == policies[1]
    except (
        KeyError,
        TypeError,
        ValueError,
        OverflowError,
        json.JSONDecodeError,
    ):
        return False


def _existing_keys(
    conn,
    *,
    symbol: str,
    start: datetime,
    end: datetime,
    horizons: Sequence[int],
    width_index: Dict[
        tuple[str, int], research_session_width.PriceWidthSeries
    ],
) -> set[tuple[datetime, int]]:
    sibling_coherence = sibling_reference_coherence_sql("stored")
    rows = conn.execute(
        f"""
        SELECT stored.symbol, stored.observation_time_utc,
               stored.source_observation_time_utc,
               stored.horizon_minutes, stored.reference_time_utc,
               stored.reference_price, stored.price_at_horizon,
               stored.raw_return_pct, stored.long_metrics,
               stored.short_metrics, stored.long_first_touch_metrics,
               stored.short_first_touch_metrics,
               stored.first_touch_method_version,
               stored.first_touch_path_samples, stored.path_samples,
               stored.first_touch_data_quality_status,
               stored.outcome_method_version, stored.exchange, stored.market,
               stored.pair, stored.interval_seconds, stored.provenance,
               stored.data_quality_status, stored.replay_version,
               ({sibling_coherence}) AS sibling_reference_coherent
        FROM research_historical_opportunity_outcomes stored
        WHERE stored.symbol=%s
          AND stored.observation_time_utc >= %s
          AND stored.observation_time_utc < %s
          AND stored.horizon_minutes=ANY(%s)
          AND stored.first_touch_method_version=%s
          AND stored.replay_version=%s
          AND stored.first_touch_data_quality_status=ANY(%s)
        """,
        (
            symbol,
            start,
            end,
            list(horizons),
            research_no_dwell_outcome.METHOD_VERSION,
            REPLAY_VERSION,
            list(canonical_price_path.COMPLETE_QUALITIES),
        ),
    ).fetchall()
    requested = {int(value) for value in horizons}
    coherent_by_anchor: Dict[datetime, set[int]] = {}
    for row in rows:
        if not replay_outcome_row_is_coherent(
            dict(row), width_index=width_index
        ):
            continue
        anchor_time = _utc(row["observation_time_utc"])
        coherent_by_anchor.setdefault(anchor_time, set()).add(
            int(row["horizon_minutes"])
        )
    # A partial anchor is deliberately replayed as one complete cohort.  This
    # prevents a later retry from mixing a newly fetched reference price/route
    # with sibling horizons committed by an earlier partial attempt.
    return {
        (anchor_time, horizon)
        for anchor_time, stored_horizons in coherent_by_anchor.items()
        if requested.issubset(stored_horizons)
        for horizon in requested
    }


def _select_pending_anchors(
    anchors: Sequence[Anchor],
    *,
    horizons: Sequence[int],
    existing_by_symbol: Dict[str, set[tuple[datetime, int]]],
    max_anchors: Optional[int],
) -> list[Anchor]:
    """Apply the bounded-canary limit only after coherent v2 completion."""
    pending = [
        anchor
        for anchor in anchors
        if any(
            (anchor.observation_time_utc, int(horizon))
            not in existing_by_symbol.get(anchor.symbol, set())
            for horizon in horizons
        )
    ]
    if max_anchors is not None:
        pending = pending[: max(1, int(max_anchors))]
    return pending


def _write_outcome(
    conn,
    *,
    run_id: int,
    anchor: Anchor,
    horizon: int,
    reference_candle: binance_spot_price_path.SpotCandle,
    path_result: Dict[str, Any],
    future_candles: Sequence[binance_spot_price_path.SpotCandle],
    width_index: Dict[
        tuple[str, int], research_session_width.PriceWidthSeries
    ],
) -> None:
    canonical_price_path.validated_route(
        anchor.symbol, path_result, require_complete=True
    )
    reference_price = float(reference_candle.close)
    long_metrics = binance_spot_price_path.calculate_path_metrics(
        reference_price=reference_price,
        direction="LONG",
        event_time=anchor.observation_time_utc,
        candles=future_candles,
    )
    short_metrics = binance_spot_price_path.calculate_path_metrics(
        reference_price=reference_price,
        direction="SHORT",
        event_time=anchor.observation_time_utc,
        candles=future_candles,
    )
    calibration_reference = _calibration_reference(
        anchor=anchor,
        horizon=horizon,
        reference_time_utc=_utc(reference_candle.close_time_utc),
        width_index=width_index,
    )
    threshold_policy = research_no_dwell_outcome.freeze_threshold_policy(
        horizon_minutes=horizon,
        decision_time=anchor.observation_time_utc,
        prior_only_reference=calibration_reference,
    )
    long_first_touch = research_no_dwell_outcome.calculate_first_touch_outcome(
        reference_price=reference_price,
        direction="LONG",
        event_time=anchor.observation_time_utc,
        candles=future_candles,
        horizon_minutes=horizon,
        horizon_closed=True,
        threshold_policy=threshold_policy,
    )
    short_first_touch = research_no_dwell_outcome.calculate_first_touch_outcome(
        reference_price=reference_price,
        direction="SHORT",
        event_time=anchor.observation_time_utc,
        candles=future_candles,
        horizon_minutes=horizon,
        horizon_closed=True,
        threshold_policy=threshold_policy,
    )
    quality = canonical_price_path.quality_status(path_result, complete=True)
    conn.execute(
        """
        INSERT INTO research_historical_opportunity_outcomes (
            symbol, observation_time_utc, source_observation_time_utc,
            horizon_minutes, reference_time_utc, reference_price,
            price_at_horizon, raw_return_pct, long_metrics, short_metrics,
            long_first_touch_metrics, short_first_touch_metrics,
            first_touch_method_version, first_touch_path_samples,
            first_touch_data_quality_status, first_touch_replay_run_id,
            path_samples, outcome_method_version, exchange, market, pair,
            interval_seconds, provenance, data_quality_status, replay_version,
            replay_run_id
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb,
            %s::jsonb, %s::jsonb, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (symbol, observation_time_utc, horizon_minutes) DO UPDATE SET
            source_observation_time_utc=EXCLUDED.source_observation_time_utc,
            reference_time_utc=EXCLUDED.reference_time_utc,
            reference_price=EXCLUDED.reference_price,
            price_at_horizon=EXCLUDED.price_at_horizon,
            raw_return_pct=EXCLUDED.raw_return_pct,
            long_metrics=EXCLUDED.long_metrics,
            short_metrics=EXCLUDED.short_metrics,
            path_samples=EXCLUDED.path_samples,
            outcome_method_version=EXCLUDED.outcome_method_version,
            exchange=EXCLUDED.exchange,
            market=EXCLUDED.market,
            pair=EXCLUDED.pair,
            interval_seconds=EXCLUDED.interval_seconds,
            provenance=EXCLUDED.provenance,
            data_quality_status=EXCLUDED.data_quality_status,
            replay_version=EXCLUDED.replay_version,
            replay_run_id=EXCLUDED.replay_run_id,
            long_first_touch_metrics=EXCLUDED.long_first_touch_metrics,
            short_first_touch_metrics=EXCLUDED.short_first_touch_metrics,
            first_touch_method_version=EXCLUDED.first_touch_method_version,
            first_touch_path_samples=EXCLUDED.first_touch_path_samples,
            first_touch_data_quality_status=EXCLUDED.first_touch_data_quality_status,
            first_touch_replay_run_id=EXCLUDED.first_touch_replay_run_id,
            updated_at_utc=NOW()
        """,
        (
            anchor.symbol,
            anchor.observation_time_utc,
            anchor.source_observation_time_utc,
            horizon,
            reference_candle.close_time_utc,
            reference_price,
            float(long_metrics["price_at_horizon"]),
            float(long_metrics["raw_return_pct"]),
            _json(_metric_payload(long_metrics)),
            _json(_metric_payload(short_metrics)),
            _json(_first_touch_payload(long_first_touch)),
            _json(_first_touch_payload(short_first_touch)),
            research_no_dwell_outcome.METHOD_VERSION,
            len(future_candles),
            quality,
            run_id,
            len(future_candles),
            canonical_price_path.METHOD_VERSION,
            path_result["exchange"],
            path_result.get("market") or "spot",
            path_result["pair"],
            int(path_result.get("interval_seconds") or 60),
            canonical_price_path.canonical_provenance_text(
                anchor.symbol, path_result
            ),
            quality,
            REPLAY_VERSION,
            run_id,
        ),
    )


def _coverage(conn) -> Dict[str, Any]:
    sibling_coherence = sibling_reference_coherence_sql("stored")
    stored_rows = [
        dict(row)
        for row in conn.execute(
            f"""
            SELECT stored.symbol, stored.observation_time_utc,
                   stored.source_observation_time_utc,
                   stored.horizon_minutes, stored.reference_time_utc,
                   stored.reference_price, stored.price_at_horizon,
                   stored.raw_return_pct, stored.long_metrics,
                   stored.short_metrics, stored.long_first_touch_metrics,
                   stored.short_first_touch_metrics,
                   stored.first_touch_method_version,
                   stored.first_touch_path_samples, stored.path_samples,
                   stored.first_touch_data_quality_status,
                   stored.outcome_method_version, stored.exchange,
                   stored.market, stored.pair, stored.interval_seconds,
                   stored.provenance, stored.data_quality_status,
                   stored.replay_version,
                   ({sibling_coherence}) AS sibling_reference_coherent
            FROM research_historical_opportunity_outcomes stored
            WHERE stored.first_touch_method_version=%s
              AND stored.replay_version=%s
            ORDER BY stored.symbol, stored.observation_time_utc,
                     stored.horizon_minutes
            """,
            (research_no_dwell_outcome.METHOD_VERSION, REPLAY_VERSION),
        ).fetchall()
    ]
    if not stored_rows:
        return {}
    first = min(_utc(row["observation_time_utc"]) for row in stored_rows)
    last = max(_utc(row["observation_time_utc"]) for row in stored_rows)
    symbols = sorted({str(row["symbol"]).upper() for row in stored_rows})
    references = load_canonical_reference_rows(
        conn,
        start=first
        - timedelta(
            days=research_session_width.LOOKBACK_DAYS,
            minutes=(
                max(_HORIZONS)
                + research_session_width.MAX_POINT_AGE_MINUTES
                + 2
            ),
        ),
        end=last + timedelta(minutes=1),
        symbols=symbols,
    )
    width_index = build_canonical_width_index(references, horizons=_HORIZONS)
    grouped: Dict[tuple[str, int], list[datetime]] = {}
    for row in stored_rows:
        if not replay_outcome_row_is_coherent(row, width_index=width_index):
            continue
        key = (str(row["symbol"]).upper(), int(row["horizon_minutes"]))
        grouped.setdefault(key, []).append(_utc(row["observation_time_utc"]))
    return {
        f"{symbol}:{horizon}": {
            "outcomes": len(times),
            "first_observation_utc": min(times),
            "last_observation_utc": max(times),
            "utc_dates": len({value.date() for value in times}),
        }
        for (symbol, horizon), times in sorted(grouped.items())
    }


def status() -> Dict[str, Any]:
    """Return read-only replay coverage for diagnostics and the AI tool surface."""
    url = _research_database_url()
    base: Dict[str, Any] = {
        "configured": bool(url),
        "schema_present": False,
        "replay_version": REPLAY_VERSION,
        "outcome_method_version": canonical_price_path.METHOD_VERSION,
        "first_touch_method_version": research_no_dwell_outcome.METHOD_VERSION,
        "canonical_source": canonical_price_path.canonical_source_description(),
        "hype_one_minute_history_policy": {
            "provider_limit_candles": _HYPERLIQUID_RECENT_ONE_MINUTE_CANDLES,
            "eligible_observation_floor_utc": _hype_one_minute_observation_floor(),
            "older_hype_anchors": "excluded rather than approximated or mislabeled",
        },
        "stores_one_minute_candles": False,
        "storage_contract": (
            "legacy endpoint/MFE/MAE diagnostics plus compact no-dwell "
            "first-touch labels; no candle storage"
        ),
    }
    if not url or psycopg is None:
        return base
    with _connect(url, read_only=True) as conn:
        required = (
            "research_historical_replay_runs",
            "research_historical_opportunity_outcomes",
        )
        missing = [table for table in required if not _table_exists(conn, table)]
        base["missing_tables"] = missing
        if missing:
            return base
        base["schema_present"] = True
        base["coverage"] = _coverage(conn)
        latest = conn.execute(
            """
            SELECT replay_run_id, status, anchors_seen, outcomes_written,
                   outcomes_skipped, failures, started_at_utc,
                   completed_at_utc, error_text
            FROM research_historical_replay_runs
            ORDER BY replay_run_id DESC
            LIMIT 1
            """
        ).fetchone()
        base["latest_run"] = dict(latest) if latest else None
    return json.loads(json.dumps(base, ensure_ascii=False, default=str))


def run_backfill(
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    symbols: Sequence[str] = (),
    horizons: Sequence[int] = _HORIZONS,
    chunk_days: int = 2,
    max_anchors: Optional[int] = None,
    pause_seconds: float = 0.05,
) -> Dict[str, Any]:
    """Run a resumable replay with a single-writer advisory lock."""
    if os.getenv("HISTORICAL_REPLAY_BACKFILL", "").strip().lower() not in _TRUE:
        raise RuntimeError(
            "Refusing replay writes: set HISTORICAL_REPLAY_BACKFILL=1 explicitly"
        )
    research_url = _research_database_url()
    raw_url = _raw_database_url()
    if not research_url or not raw_url:
        raise RuntimeError("research and raw database URLs must both be configured")
    normalized_horizons = tuple(
        value for value in sorted({int(item) for item in horizons}) if value in _HORIZONS
    )
    if not normalized_horizons:
        raise ValueError("at least one supported horizon is required")
    normalized_symbols = tuple(sorted({str(item).upper() for item in symbols if item}))
    chunk_span = timedelta(days=max(1, min(int(chunk_days), 14)))
    frozen_now = datetime.now(timezone.utc)
    closed_end = fully_closed_end(normalized_horizons, now=frozen_now)
    effective_end = min(_utc(end), closed_end) if end is not None else closed_end
    effective_start = _utc(start) if start is not None else None
    if effective_start is not None and effective_start >= effective_end:
        return {
            "available": True,
            "anchors": 0,
            "outcomes_written": 0,
            "reason": "requested cohort has no fully closed horizons",
            "frozen_end_utc": effective_end,
        }

    with _connect(raw_url, read_only=True) as raw_conn:
        for table in ("oi_price_history", "futures_taker_history", "spot_taker_history"):
            if not _table_exists(raw_conn, table):
                raise RuntimeError(f"raw archive is missing {table}")
        anchors = _load_anchors(
            raw_conn,
            start=effective_start,
            end=effective_end,
            symbols=normalized_symbols,
        )
    if not anchors:
        return {
            "available": True,
            "anchors": 0,
            "outcomes_written": 0,
            "frozen_end_utc": effective_end,
        }

    with _connect(research_url, read_only=False) as research_conn:
        for table in (
            "research_historical_replay_runs",
            "research_historical_opportunity_outcomes",
        ):
            if not _table_exists(research_conn, table):
                raise RuntimeError(f"historical replay schema is missing {table}")
        lock = research_conn.execute(
            "SELECT pg_try_advisory_lock(%s) AS acquired", (_LOCK_ID,)
        ).fetchone()
        if not lock or not lock["acquired"]:
            raise RuntimeError("another historical replay is already running")
        calibration_start = min(
            anchor.observation_time_utc for anchor in anchors
        ) - timedelta(
            days=research_session_width.LOOKBACK_DAYS,
            minutes=(
                max(normalized_horizons)
                + research_session_width.MAX_POINT_AGE_MINUTES
                + 2
            ),
        )
        calibration_end = max(
            anchor.observation_time_utc for anchor in anchors
        ) + timedelta(minutes=1)
        canonical_references = load_canonical_reference_rows(
            research_conn,
            start=calibration_start,
            end=calibration_end,
            symbols=sorted({anchor.symbol for anchor in anchors}),
        )
        width_index = build_canonical_width_index(
            canonical_references, horizons=normalized_horizons
        )
        known_reference_keys = {
            (
                str(row["symbol"]).upper(),
                _utc(row["reference_time_utc"]),
            )
            for row in canonical_references
        }
        existing_by_symbol: Dict[str, set[tuple[datetime, int]]] = {}
        for symbol in sorted({anchor.symbol for anchor in anchors}):
            symbol_anchors = [
                anchor for anchor in anchors if anchor.symbol == symbol
            ]
            existing_by_symbol[symbol] = _existing_keys(
                research_conn,
                symbol=symbol,
                start=symbol_anchors[0].observation_time_utc,
                end=symbol_anchors[-1].observation_time_utc
                + timedelta(minutes=1),
                horizons=normalized_horizons,
                width_index=width_index,
            )
        pending_anchors = _select_pending_anchors(
            anchors,
            horizons=normalized_horizons,
            existing_by_symbol=existing_by_symbol,
            max_anchors=max_anchors,
        )
        run_config = {
            "symbols": list(normalized_symbols) or "ALL",
            "horizons_minutes": list(normalized_horizons),
            "chunk_days": int(chunk_span.total_seconds() // 86400),
            "max_anchors": max_anchors,
            "first_touch_method_version": (
                research_no_dwell_outcome.METHOD_VERSION
            ),
            "movement_width_calibration_version": (
                research_session_width.CALIBRATION_VERSION
            ),
            "canonical_price_provenance_version": (
                canonical_price_path.PRICE_PROVENANCE_VERSION
            ),
            "coverage_scope_version": COVERAGE_SCOPE_VERSION,
            "frozen_fully_closed_end_utc": effective_end,
        }
        if not pending_anchors:
            try:
                coverage = _coverage(research_conn)
                requested_keys = {
                    f"{symbol}:{horizon}"
                    for symbol in sorted({anchor.symbol for anchor in anchors})
                    for horizon in normalized_horizons
                }
                missing_keys = sorted(requested_keys - set(coverage))
                if missing_keys:
                    raise RuntimeError(
                        "coherent replay coverage is missing requested keys: "
                        + ",".join(missing_keys)
                    )
                recovery_config = {
                    **run_config,
                    "completion_mode": "METADATA_ONLY_ALREADY_COHERENT",
                }
                run = research_conn.execute(
                    """
                    INSERT INTO research_historical_replay_runs (
                        replay_version, outcome_method_version, status,
                        requested_start_utc, requested_end_utc, config,
                        coverage, anchors_seen, outcomes_written,
                        outcomes_skipped, failures, completed_at_utc
                    ) VALUES (
                        %s, %s, 'COMPLETED', %s, %s, %s::jsonb,
                        %s::jsonb, 0, 0, %s, 0, NOW()
                    )
                    RETURNING replay_run_id
                    """,
                    (
                        REPLAY_VERSION,
                        canonical_price_path.METHOD_VERSION,
                        effective_start,
                        effective_end,
                        _json(recovery_config),
                        _json(coverage),
                        len(anchors) * len(normalized_horizons),
                    ),
                ).fetchone()
                run_id = int(run["replay_run_id"])
                research_conn.commit()
            except Exception:
                research_conn.rollback()
                raise
            finally:
                research_conn.execute(
                    "SELECT pg_advisory_unlock(%s)", (_LOCK_ID,)
                )
                research_conn.commit()
            return {
                "available": True,
                "replay_run_id": run_id,
                "anchors": 0,
                "outcomes_written": 0,
                "outcomes_skipped": len(anchors) * len(normalized_horizons),
                "already_complete": True,
                "replay_version": REPLAY_VERSION,
                "first_touch_method_version": (
                    research_no_dwell_outcome.METHOD_VERSION
                ),
                "movement_width_calibration_version": (
                    research_session_width.CALIBRATION_VERSION
                ),
                "frozen_end_utc": effective_end,
                "coverage": coverage,
            }
        grouped: Dict[str, list[Anchor]] = {}
        for anchor in pending_anchors:
            grouped.setdefault(anchor.symbol, []).append(anchor)
        run = research_conn.execute(
            """
            INSERT INTO research_historical_replay_runs (
                replay_version, outcome_method_version, status,
                requested_start_utc, requested_end_utc, config
            ) VALUES (%s, %s, 'RUNNING', %s, %s, %s::jsonb)
            RETURNING replay_run_id
            """,
            (
                REPLAY_VERSION,
                canonical_price_path.METHOD_VERSION,
                effective_start,
                effective_end,
                _json(run_config),
            ),
        ).fetchone()
        run_id = int(run["replay_run_id"])
        research_conn.commit()

        seen = written = skipped = failures = 0
        try:
            for symbol, symbol_anchors in grouped.items():
                chunk_start = _floor_minute(symbol_anchors[0].observation_time_utc)
                symbol_end = symbol_anchors[-1].observation_time_utc + timedelta(minutes=1)
                while chunk_start < symbol_end:
                    chunk_end = min(symbol_end, chunk_start + chunk_span)
                    chunk = [
                        anchor
                        for anchor in symbol_anchors
                        if chunk_start <= anchor.observation_time_utc < chunk_end
                    ]
                    if not chunk:
                        chunk_start = chunk_end
                        continue
                    existing = existing_by_symbol.get(symbol, set())
                    pending = [
                        anchor
                        for anchor in chunk
                        if any(
                            (anchor.observation_time_utc, horizon) not in existing
                            for horizon in normalized_horizons
                        )
                    ]
                    seen += len(chunk)
                    if not pending:
                        skipped += len(chunk) * len(normalized_horizons)
                        chunk_start = chunk_end
                        continue
                    fetch_start = min(
                        anchor.observation_time_utc for anchor in pending
                    ) - timedelta(minutes=2)
                    fetch_end = (
                        max(anchor.observation_time_utc for anchor in pending)
                        + timedelta(minutes=max(normalized_horizons))
                    )
                    path_result = _fetch_range(
                        symbol,
                        fetch_start,
                        fetch_end,
                        pause_seconds=max(0.0, float(pause_seconds)),
                    )
                    canonical_price_path.validated_route(
                        symbol, path_result, require_complete=True
                    )
                    candles = list(path_result.get("candles") or [])
                    chunk_references: Dict[datetime, Any] = {}
                    new_calibration_points = 0
                    for anchor in pending:
                        reference = _reference_candle(
                            candles, anchor.observation_time_utc
                        )
                        if reference is not None:
                            chunk_references[
                                anchor.observation_time_utc
                            ] = reference
                            reference_key = (
                                anchor.symbol,
                                _utc(reference.close_time_utc),
                            )
                            if reference_key not in known_reference_keys:
                                known_reference_keys.add(reference_key)
                                new_calibration_points += 1
                                canonical_references.append(
                                    {
                                        "symbol": anchor.symbol,
                                        "reference_time_utc": (
                                            reference.close_time_utc
                                        ),
                                        "reference_price": float(
                                            reference.close
                                        ),
                                    }
                                )
                    # Include every newly fetched decision-time reference before
                    # labelling this chunk.  Strict ``< as_of`` slicing means
                    # later points remain invisible, while HYPE rows written in
                    # the same run receive the same prior @107 series that a
                    # future loader will reconstruct.
                    if new_calibration_points:
                        width_index = build_canonical_width_index(
                            canonical_references,
                            horizons=normalized_horizons,
                        )
                    for anchor in pending:
                        reference = chunk_references.get(
                            anchor.observation_time_utc
                        )
                        if reference is None:
                            failures += len(normalized_horizons)
                            continue
                        for horizon in normalized_horizons:
                            key = (anchor.observation_time_utc, horizon)
                            if key in existing:
                                skipped += 1
                                continue
                            if anchor.observation_time_utc + timedelta(minutes=horizon) > fetch_end:
                                skipped += 1
                                continue
                            future = _outcome_candles(
                                candles, anchor.observation_time_utc, horizon
                            )
                            expected = _expected_outcome_candles(
                                anchor.observation_time_utc, horizon
                            )
                            if expected <= 0 or len(future) != expected:
                                failures += 1
                                continue
                            _write_outcome(
                                research_conn,
                                run_id=run_id,
                                anchor=anchor,
                                horizon=horizon,
                                reference_candle=reference,
                                path_result=path_result,
                                future_candles=future,
                                width_index=width_index,
                            )
                            written += 1
                    research_conn.execute(
                        """
                        UPDATE research_historical_replay_runs
                        SET anchors_seen=%s, outcomes_written=%s,
                            outcomes_skipped=%s, failures=%s
                        WHERE replay_run_id=%s
                        """,
                        (seen, written, skipped, failures, run_id),
                    )
                    research_conn.commit()
                    print(
                        f"[historical-replay] {symbol} {chunk_start.isoformat()} "
                        f"anchors={len(chunk)} written={written} skipped={skipped} "
                        f"failures={failures}",
                        flush=True,
                    )
                    chunk_start = chunk_end

            coverage = _coverage(research_conn)
            research_conn.execute(
                """
                UPDATE research_historical_replay_runs
                SET status='COMPLETED', coverage=%s::jsonb,
                    anchors_seen=%s, outcomes_written=%s,
                    outcomes_skipped=%s, failures=%s,
                    completed_at_utc=NOW()
                WHERE replay_run_id=%s
                """,
                (_json(coverage), seen, written, skipped, failures, run_id),
            )
            research_conn.commit()
        except Exception as exc:
            research_conn.rollback()
            research_conn.execute(
                """
                UPDATE research_historical_replay_runs
                SET status='FAILED', anchors_seen=%s, outcomes_written=%s,
                    outcomes_skipped=%s, failures=%s, error_text=%s,
                    completed_at_utc=NOW()
                WHERE replay_run_id=%s
                """,
                (seen, written, skipped, failures + 1, f"{type(exc).__name__}: {exc}", run_id),
            )
            research_conn.commit()
            raise
        finally:
            research_conn.execute("SELECT pg_advisory_unlock(%s)", (_LOCK_ID,))
            research_conn.commit()

    return {
        "available": True,
        "replay_run_id": run_id,
        "replay_version": REPLAY_VERSION,
        "first_touch_method_version": research_no_dwell_outcome.METHOD_VERSION,
        "movement_width_calibration_version": (
            research_session_width.CALIBRATION_VERSION
        ),
        "frozen_end_utc": effective_end,
        "anchors": seen,
        "outcomes_written": written,
        "outcomes_skipped": skipped,
        "failures": failures,
        "coverage": coverage,
    }


def main() -> None:
    result = run_backfill(
        start=_optional_time("HISTORICAL_REPLAY_START_UTC"),
        end=_optional_time("HISTORICAL_REPLAY_END_UTC"),
        symbols=_symbols_from_env(),
        horizons=_horizons_from_env(),
        chunk_days=int(os.getenv("HISTORICAL_REPLAY_CHUNK_DAYS", "2")),
        max_anchors=(
            int(os.getenv("HISTORICAL_REPLAY_MAX_ANCHORS", "0")) or None
        ),
        pause_seconds=float(os.getenv("HISTORICAL_REPLAY_API_PAUSE_SECONDS", "0.05")),
    )
    print(_json(result), flush=True)


if __name__ == "__main__":
    main()
