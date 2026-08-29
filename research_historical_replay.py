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
import os
import time
from typing import Any, Dict, Iterable, Optional, Sequence

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - validated at runtime
    psycopg = None
    dict_row = None

import binance_spot_price_path
import canonical_price_path
import market_session_baseline


REPLAY_VERSION = "historical-raw-opportunity-replay-v1"
_TRUE = {"1", "true", "yes", "on"}
_HORIZONS = (60, 240, 720, 1440)
_LOCK_ID = 94837243
_FETCH_SEGMENT_MINUTES = 1900
_HYPERLIQUID_RECENT_ONE_MINUTE_CANDLES = 5000


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
                "(live.symbol<>'HYPE' OR live.price_source='hyperliquid')",
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
    expected_total = 0
    while cursor < finish:
        segment_end = min(
            finish, cursor + timedelta(minutes=_FETCH_SEGMENT_MINUTES)
        )
        result = canonical_price_path.fetch_closed_candles(
            symbol, cursor, segment_end
        )
        metadata = dict(result)
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


def _existing_keys(
    conn,
    *,
    symbol: str,
    start: datetime,
    end: datetime,
    horizons: Sequence[int],
) -> set[tuple[datetime, int]]:
    rows = conn.execute(
        """
        SELECT observation_time_utc, horizon_minutes
        FROM research_historical_opportunity_outcomes
        WHERE symbol=%s
          AND observation_time_utc >= %s AND observation_time_utc < %s
          AND horizon_minutes=ANY(%s)
          AND outcome_method_version=%s
          AND replay_version=%s
          AND data_quality_status=ANY(%s)
        """,
        (
            symbol,
            start,
            end,
            list(horizons),
            canonical_price_path.METHOD_VERSION,
            REPLAY_VERSION,
            list(canonical_price_path.COMPLETE_QUALITIES),
        ),
    ).fetchall()
    return {
        (_utc(row["observation_time_utc"]), int(row["horizon_minutes"]))
        for row in rows
    }


def _write_outcome(
    conn,
    *,
    run_id: int,
    anchor: Anchor,
    horizon: int,
    reference_candle: binance_spot_price_path.SpotCandle,
    path_result: Dict[str, Any],
    future_candles: Sequence[binance_spot_price_path.SpotCandle],
) -> None:
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
    quality = canonical_price_path.quality_status(path_result, complete=True)
    conn.execute(
        """
        INSERT INTO research_historical_opportunity_outcomes (
            symbol, observation_time_utc, source_observation_time_utc,
            horizon_minutes, reference_time_utc, reference_price,
            price_at_horizon, raw_return_pct, long_metrics, short_metrics,
            path_samples, outcome_method_version, exchange, market, pair,
            interval_seconds, provenance, data_quality_status, replay_version,
            replay_run_id
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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
            len(future_candles),
            canonical_price_path.METHOD_VERSION,
            path_result["exchange"],
            path_result.get("market") or "spot",
            path_result["pair"],
            int(path_result.get("interval_seconds") or 60),
            path_result.get("provenance")
            or "EXCHANGE_API_HISTORICAL_CANDLES_IMPORTED",
            quality,
            REPLAY_VERSION,
            run_id,
        ),
    )


def _coverage(conn) -> Dict[str, Any]:
    rows = conn.execute(
        """
        SELECT symbol, horizon_minutes, COUNT(*)::bigint AS outcomes,
               MIN(observation_time_utc) AS first_observation_utc,
               MAX(observation_time_utc) AS last_observation_utc,
               COUNT(DISTINCT observation_time_utc::date)::bigint AS utc_dates
        FROM research_historical_opportunity_outcomes
        WHERE outcome_method_version=%s AND replay_version=%s
          AND data_quality_status=ANY(%s)
        GROUP BY symbol, horizon_minutes
        ORDER BY symbol, horizon_minutes
        """,
        (
            canonical_price_path.METHOD_VERSION,
            REPLAY_VERSION,
            list(canonical_price_path.COMPLETE_QUALITIES),
        ),
    ).fetchall()
    return {
        f"{row['symbol']}:{int(row['horizon_minutes'])}": {
            "outcomes": int(row["outcomes"]),
            "first_observation_utc": row["first_observation_utc"],
            "last_observation_utc": row["last_observation_utc"],
            "utc_dates": int(row["utc_dates"]),
        }
        for row in rows
    }


def status() -> Dict[str, Any]:
    """Return read-only replay coverage for diagnostics and the AI tool surface."""
    url = _research_database_url()
    base: Dict[str, Any] = {
        "configured": bool(url),
        "schema_present": False,
        "replay_version": REPLAY_VERSION,
        "outcome_method_version": canonical_price_path.METHOD_VERSION,
        "canonical_source": canonical_price_path.canonical_source_description(),
        "hype_one_minute_history_policy": {
            "provider_limit_candles": _HYPERLIQUID_RECENT_ONE_MINUTE_CANDLES,
            "eligible_observation_floor_utc": _hype_one_minute_observation_floor(),
            "older_hype_anchors": "excluded rather than approximated or mislabeled",
        },
        "stores_one_minute_candles": False,
        "storage_contract": "compact per-anchor MFE/MAE/return summaries only",
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

    with _connect(raw_url, read_only=True) as raw_conn:
        for table in ("oi_price_history", "futures_taker_history", "spot_taker_history"):
            if not _table_exists(raw_conn, table):
                raise RuntimeError(f"raw archive is missing {table}")
        anchors = _load_anchors(
            raw_conn,
            start=start,
            end=end,
            symbols=normalized_symbols,
        )
    if max_anchors is not None:
        anchors = anchors[: max(1, int(max_anchors))]
    if not anchors:
        return {"available": True, "anchors": 0, "outcomes_written": 0}

    grouped: Dict[str, list[Anchor]] = {}
    for anchor in anchors:
        grouped.setdefault(anchor.symbol, []).append(anchor)

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
                start,
                end,
                _json(
                    {
                        "symbols": list(normalized_symbols) or "ALL",
                        "horizons_minutes": list(normalized_horizons),
                        "chunk_days": int(chunk_span.total_seconds() // 86400),
                        "max_anchors": max_anchors,
                    }
                ),
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
                    existing = _existing_keys(
                        research_conn,
                        symbol=symbol,
                        start=chunk_start,
                        end=chunk_end,
                        horizons=normalized_horizons,
                    )
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
                    fetch_end = min(
                        datetime.now(timezone.utc) - timedelta(seconds=1),
                        max(anchor.observation_time_utc for anchor in pending)
                        + timedelta(minutes=max(normalized_horizons)),
                    )
                    path_result = _fetch_range(
                        symbol,
                        fetch_start,
                        fetch_end,
                        pause_seconds=max(0.0, float(pause_seconds)),
                    )
                    candles = list(path_result.get("candles") or [])
                    for anchor in pending:
                        reference = _reference_candle(
                            candles, anchor.observation_time_utc
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
