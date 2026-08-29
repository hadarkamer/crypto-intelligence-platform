"""Read-only alert archive queries for the production AI layer.

The functions in this module never create tables or write outcomes. They expose
only bounded summaries and exact event context to the GPT tool layer.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - validated at runtime
    psycopg = None
    dict_row = None

import ai_history_research
import binance_spot_price_path

_TRUE = {"1", "true", "yes", "on"}
_SUPPORTED_HORIZONS = {60, 240, 720, 1440}
_PATH_V2_METHOD = "binance-spot-1m-ohlc-path-v2"
_PATH_V2_COMPLETE = "VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES"


def _database_url() -> str:
    dedicated = os.getenv("RESEARCH_DATABASE_URL", "").strip()
    use_primary = os.getenv("RESEARCH_USE_PRIMARY_DATABASE", "").strip().lower() in _TRUE
    if dedicated:
        return dedicated
    if use_primary:
        return os.getenv("DATABASE_URL", "").strip()
    return ""


def _require_db() -> str:
    url = _database_url()
    if not url:
        raise RuntimeError(
            "Research archive is not configured. Set RESEARCH_DATABASE_URL or "
            "explicitly enable RESEARCH_USE_PRIMARY_DATABASE."
        )
    if psycopg is None:
        raise RuntimeError("psycopg is unavailable")
    return url


def _connect():
    return psycopg.connect(
        _require_db(),
        row_factory=dict_row,
        connect_timeout=5,
        options="-c statement_timeout=8000 -c default_transaction_read_only=on",
    )


def _json_safe(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str, ensure_ascii=False))


def _symbol(value: Any) -> Optional[str]:
    text = str(value or "").strip().upper()
    if not text:
        return None
    if len(text) > 20 or not text.replace("-", "").isalnum():
        raise ValueError("Invalid crypto symbol")
    return text


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute("SELECT to_regclass(%s) AS relation", (f"public.{table_name}",)).fetchone()
    return bool(row and row.get("relation"))


def archive_status() -> Dict[str, Any]:
    """Return safe archive coverage without exposing connection details."""
    configured = bool(_database_url())
    base: Dict[str, Any] = {
        "configured": configured,
        "schema_present": False,
        "research_events": 0,
        "delivered_alerts": 0,
        "outcomes": 0,
        "legacy_alert_history_rows": 0,
        "first_event_utc": None,
        "latest_event_utc": None,
    }
    if not configured or psycopg is None:
        return base

    with _connect() as conn:
        base["schema_present"] = _table_exists(conn, "research_events") and _table_exists(
            conn, "research_alert_outcomes"
        )
        if _table_exists(conn, "alert_history"):
            row = conn.execute("SELECT COUNT(*)::bigint AS count FROM alert_history").fetchone()
            base["legacy_alert_history_rows"] = int(row["count"] or 0)
        if not base["schema_present"]:
            return base

        row = conn.execute(
            """
            SELECT COUNT(*)::bigint AS events,
                   COUNT(*) FILTER (
                       WHERE event_kind='ALERT' AND delivery_status='DELIVERED'
                   )::bigint AS delivered,
                   MIN(alert_time_utc) AS first_event,
                   MAX(alert_time_utc) AS latest_event
            FROM research_events
            """
        ).fetchone()
        outcome_row = conn.execute(
            "SELECT COUNT(*)::bigint AS count FROM research_alert_outcomes"
        ).fetchone()
        base.update(
            {
                "research_events": int(row["events"] or 0),
                "delivered_alerts": int(row["delivered"] or 0),
                "outcomes": int(outcome_row["count"] or 0),
                "first_event_utc": row["first_event"],
                "latest_event_utc": row["latest_event"],
            }
        )
    return _json_safe(base)


def research_alert_history(
    *,
    symbol: Any = None,
    lookback_days: int = 30,
    horizon_minutes: int = 240,
    limit: int = 20,
) -> Dict[str, Any]:
    """Summarize delivered alerts and their measured fixed-horizon outcomes."""
    normalized_symbol = _symbol(symbol)
    days = max(1, min(int(lookback_days), 3650))
    horizon = int(horizon_minutes)
    if horizon not in _SUPPORTED_HORIZONS:
        raise ValueError("horizon_minutes must be 60, 240, 720 or 1440")
    row_limit = max(1, min(int(limit), 50))

    status = archive_status()
    if not status.get("schema_present"):
        return {
            "available": False,
            "reason": "research archive schema is not installed",
            "archive": status,
            "historical_truth": (
                "The existing alert_history table is not a usable historical archive. "
                "Market history can be reconstructed separately but must not be called a delivered alert."
            ),
        }

    symbol_clause = "AND e.symbol = %s" if normalized_symbol else ""
    symbol_params: list[Any] = [normalized_symbol] if normalized_symbol else []
    with _connect() as conn:
        coverage = conn.execute(
            f"""
            SELECT e.delivery_status, COUNT(*)::bigint AS count
            FROM research_events e
            WHERE e.event_kind='ALERT'
              AND e.alert_time_utc >= NOW() - (%s * INTERVAL '1 day')
              {symbol_clause}
            GROUP BY e.delivery_status
            ORDER BY e.delivery_status
            """,
            [days, *symbol_params],
        ).fetchall()

        summary = conn.execute(
            f"""
            SELECT e.event_type,
                   e.direction,
                   o.horizon_minutes,
                   COUNT(*)::bigint AS sample_size,
                   ROUND(AVG(o.directional_return_pct)::numeric, 6) AS avg_directional_return_pct,
                   ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                       ORDER BY o.directional_return_pct
                   )::numeric, 6) AS median_directional_return_pct,
                   ROUND((100.0 * COUNT(*) FILTER (
                       WHERE o.directional_return_pct > 0
                   ) / NULLIF(COUNT(*), 0))::numeric, 2) AS positive_rate_pct,
                   ROUND(AVG(o.mfe_pct)::numeric, 6) AS avg_mfe_pct,
                   ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                       ORDER BY o.mfe_pct
                   )::numeric, 6) AS median_mfe_pct,
                   ROUND(AVG(o.mae_pct)::numeric, 6) AS avg_mae_pct,
                   ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                       ORDER BY o.mae_pct
                   )::numeric, 6) AS median_mae_pct,
                   ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (
                       ORDER BY o.mae_pct
                   )::numeric, 6) AS p90_mae_pct,
                   ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (
                       ORDER BY o.mae_pct
                   )::numeric, 6) AS p95_mae_pct,
                   ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                       ORDER BY o.time_to_first_progress_seconds
                   )::numeric, 2) AS median_time_to_first_progress_seconds,
                   ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                       ORDER BY o.time_to_mfe_seconds
                   )::numeric, 2) AS median_time_to_mfe_seconds,
                   ROUND((100.0 * COUNT(*) FILTER (
                       WHERE o.target_reached IS TRUE
                   ) / NULLIF(COUNT(*) FILTER (
                       WHERE o.target_reached IS NOT NULL
                   ), 0))::numeric, 2) AS target_reach_rate_pct,
                   ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                       ORDER BY o.target_progress_ratio
                   )::numeric, 6) AS median_target_progress_ratio,
                   COUNT(*) FILTER (
                       WHERE o.outcome_method_version=%s
                         AND o.data_quality_status=%s
                   )::bigint AS verified_path_samples
            FROM research_events e
            JOIN research_alert_outcomes o ON o.event_id=e.event_id
            WHERE e.event_kind='ALERT'
              AND e.delivery_status='DELIVERED'
              AND e.alert_time_utc >= NOW() - (%s * INTERVAL '1 day')
              AND o.horizon_minutes=%s
              {symbol_clause}
            GROUP BY e.event_type, e.direction, o.horizon_minutes
            ORDER BY sample_size DESC, e.event_type, e.direction
            LIMIT 100
            """,
            [_PATH_V2_METHOD, _PATH_V2_COMPLETE, days, horizon, *symbol_params],
        ).fetchall()

        recent = conn.execute(
            f"""
            SELECT e.event_id, e.alert_time_utc, e.symbol, e.direction,
                   e.source_side, e.timeframe, e.event_type, e.score,
                   e.current_price, e.target_price, e.categories,
                   e.strategy_version, e.code_version, e.delivery_status,
                   o.horizon_minutes, o.measured_at_utc, o.reference_price,
                   o.price_at_horizon, o.raw_return_pct,
                   o.directional_return_pct, o.max_favorable_price,
                   o.max_adverse_price, o.mfe_pct, o.mae_pct,
                   o.time_to_first_progress_seconds, o.time_to_mfe_seconds,
                   o.time_to_closest_target_seconds, o.time_to_target_seconds,
                   o.closest_target_price, o.closest_target_distance_pct,
                   o.target_progress_ratio, o.target_reached,
                   o.path_resolution_seconds, o.path_samples, o.price_source,
                   o.data_quality_status, o.outcome_method_version
            FROM research_events e
            LEFT JOIN research_alert_outcomes o
              ON o.event_id=e.event_id AND o.horizon_minutes=%s
            WHERE e.event_kind='ALERT'
              AND e.delivery_status='DELIVERED'
              AND e.alert_time_utc >= NOW() - (%s * INTERVAL '1 day')
              {symbol_clause}
            ORDER BY e.alert_time_utc DESC
            LIMIT %s
            """,
            [horizon, days, *symbol_params, row_limit],
        ).fetchall()

    return _json_safe(
        {
            "available": True,
            "scope": {
                "symbol": normalized_symbol or "ALL",
                "lookback_days": days,
                "horizon_minutes": horizon,
                "delivered_alerts_only": True,
            },
            "archive": status,
            "delivery_coverage": coverage,
            "performance_by_type": summary,
            "recent_alerts": recent,
            "interpretation_rules": [
                "Always report sample_size; small samples are descriptive, not proof.",
                "Positive rate means direction-adjusted return above zero at the selected horizon.",
                "Verified v2 outcomes use closed Binance Spot 1-minute candles; the first partial minute after an alert is excluded to avoid pre-alert leakage.",
                "MFE is favorable excursion, MAE is adverse excursion, and p90/p95 MAE help estimate stop distances that historically survived 90%/95% of the sampled paths; they are research evidence, not a live stop recommendation.",
                "Compare verified_path_samples with sample_size before using path-quality metrics; legacy v1 rows may still be awaiting upgrade.",
                "Historical market reconstructions are not delivered Telegram alerts.",
            ],
        }
    )


def research_formula_groups(
    *,
    symbol: Any = None,
    lookback_days: int = 90,
    horizon_minutes: int = 240,
    group_by: str = "signal_combination",
    minimum_samples: int = 3,
    limit: int = 50,
) -> Dict[str, Any]:
    """Return reproducible formula-building aggregates from verified paths.

    This is a discovery surface, not a formula validator.  It intentionally
    exposes both favorable and adverse path distributions, speed, target
    progress and exact sample IDs so the model can examine counterexamples with
    ``get_alert_context`` instead of ranking candidates by hit rate alone.
    """
    normalized_symbol = _symbol(symbol)
    days = max(1, min(int(lookback_days), 3650))
    horizon = int(horizon_minutes)
    if horizon not in _SUPPORTED_HORIZONS:
        raise ValueError("horizon_minutes must be 60, 240, 720 or 1440")
    grouping = str(group_by or "").strip().lower()
    minimum = max(1, min(int(minimum_samples), 1000))
    row_limit = max(1, min(int(limit), 100))

    grouping_sql = {
        "signal_combination": (
            "v.event_type, v.direction, v.categories, v.strategy_version",
            "v.event_type, v.direction, v.categories, v.strategy_version",
        ),
        "event_type": (
            "v.event_type, v.direction, v.strategy_version",
            "v.event_type, v.direction, v.strategy_version",
        ),
        "symbol": (
            "v.symbol, v.direction, v.strategy_version",
            "v.symbol, v.direction, v.strategy_version",
        ),
        "score_band": (
            "v.direction, v.strategy_version, "
            "(FLOOR(v.score / 10.0) * 10.0) AS score_from, "
            "(FLOOR(v.score / 10.0) * 10.0 + 10.0) AS score_to_exclusive",
            "v.direction, v.strategy_version, FLOOR(v.score / 10.0)",
        ),
    }
    if grouping not in grouping_sql:
        raise ValueError(
            "group_by must be signal_combination, event_type, symbol or score_band"
        )
    select_group, group_clause = grouping_sql[grouping]

    status = archive_status()
    if not status.get("schema_present"):
        return {
            "available": False,
            "reason": "research archive schema is not installed",
            "archive": status,
        }

    symbol_clause = "AND e.symbol = %s" if normalized_symbol else ""
    symbol_params: list[Any] = [normalized_symbol] if normalized_symbol else []
    with _connect() as conn:
        rows = conn.execute(
            f"""
            WITH verified AS (
                SELECT e.event_id, e.alert_time_utc, e.symbol, e.direction,
                       e.event_type, e.categories, e.strategy_version, e.score,
                       o.horizon_minutes, o.directional_return_pct,
                       o.mfe_pct, o.mae_pct,
                       o.time_to_first_progress_seconds, o.time_to_mfe_seconds,
                       o.target_reached, o.target_progress_ratio
                FROM research_events e
                JOIN research_alert_outcomes o ON o.event_id=e.event_id
                WHERE e.event_kind='ALERT'
                  AND e.delivery_status='DELIVERED'
                  AND e.alert_time_utc >= NOW() - (%s * INTERVAL '1 day')
                  AND o.horizon_minutes=%s
                  AND o.outcome_method_version=%s
                  AND o.data_quality_status=%s
                  {symbol_clause}
            ),
            baseline AS (
                SELECT COUNT(*)::bigint AS baseline_samples,
                       ROUND((100.0 * COUNT(*) FILTER (
                           WHERE directional_return_pct > 0
                       ) / NULLIF(COUNT(*), 0))::numeric, 2) AS baseline_positive_rate_pct,
                       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                           ORDER BY mfe_pct
                       )::numeric, 6) AS baseline_median_mfe_pct,
                       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                           ORDER BY mae_pct
                       )::numeric, 6) AS baseline_median_mae_pct
                FROM verified
            ),
            grouped AS (
                SELECT {select_group},
                       COUNT(*)::bigint AS sample_size,
                       ROUND((100.0 * COUNT(*) FILTER (
                           WHERE v.directional_return_pct > 0
                       ) / NULLIF(COUNT(*), 0))::numeric, 2) AS positive_rate_pct,
                       ROUND(AVG(v.directional_return_pct)::numeric, 6) AS avg_directional_return_pct,
                       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                           ORDER BY v.directional_return_pct
                       )::numeric, 6) AS median_directional_return_pct,
                       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                           ORDER BY v.mfe_pct
                       )::numeric, 6) AS median_mfe_pct,
                       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                           ORDER BY v.mae_pct
                       )::numeric, 6) AS median_mae_pct,
                       ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (
                           ORDER BY v.mae_pct
                       )::numeric, 6) AS p75_mae_pct,
                       ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (
                           ORDER BY v.mae_pct
                       )::numeric, 6) AS p90_mae_pct,
                       ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (
                           ORDER BY v.mae_pct
                       )::numeric, 6) AS p95_mae_pct,
                       ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (
                           ORDER BY v.mfe_pct
                       ) / NULLIF(PERCENTILE_CONT(0.5) WITHIN GROUP (
                           ORDER BY v.mae_pct
                       ), 0))::numeric, 4) AS median_mfe_to_mae_ratio,
                       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                           ORDER BY v.time_to_first_progress_seconds
                       )::numeric, 2) AS median_time_to_first_progress_seconds,
                       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                           ORDER BY v.time_to_mfe_seconds
                       )::numeric, 2) AS median_time_to_mfe_seconds,
                       ROUND((100.0 * COUNT(*) FILTER (
                           WHERE v.target_reached IS TRUE
                       ) / NULLIF(COUNT(*) FILTER (
                           WHERE v.target_reached IS NOT NULL
                       ), 0))::numeric, 2) AS target_reach_rate_pct,
                       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                           ORDER BY v.target_progress_ratio
                       )::numeric, 6) AS median_target_progress_ratio,
                       (ARRAY_AGG(v.event_id ORDER BY v.alert_time_utc DESC))[1:10]
                           AS recent_event_ids
                FROM verified v
                GROUP BY {group_clause}
                HAVING COUNT(*) >= %s
            )
            SELECT grouped.*,
                   baseline.baseline_samples,
                   baseline.baseline_positive_rate_pct,
                   baseline.baseline_median_mfe_pct,
                   baseline.baseline_median_mae_pct,
                   ROUND((100.0 * grouped.sample_size /
                       NULLIF(baseline.baseline_samples, 0))::numeric, 4)
                       AS sample_share_pct
            FROM grouped CROSS JOIN baseline
            ORDER BY grouped.sample_size DESC,
                     grouped.positive_rate_pct DESC,
                     grouped.median_mae_pct ASC
            LIMIT %s
            """,
            [
                days,
                horizon,
                _PATH_V2_METHOD,
                _PATH_V2_COMPLETE,
                *symbol_params,
                minimum,
                row_limit,
            ],
        ).fetchall()

    return _json_safe(
        {
            "available": True,
            "scope": {
                "symbol": normalized_symbol or "ALL",
                "lookback_days": days,
                "horizon_minutes": horizon,
                "group_by": grouping,
                "minimum_samples": minimum,
                "outcome_method": _PATH_V2_METHOD,
                "verified_paths_only": True,
            },
            "groups": rows,
            "research_contract": [
                "These are candidate-discovery aggregates, not validated live formulas.",
                "Prefer high MFE, low MAE, fast progress and improvement over baseline together; hit rate alone is insufficient.",
                "p75/p90/p95 MAE describe historical adverse-path distributions and can inform candidate stop-distance tests.",
                "sample_share_pct shows rarity inside the selected archive scope; always report it together with absolute sample_size.",
                "Use recent_event_ids with get_alert_context to inspect failures and counterexamples.",
                "A candidate still requires chronological holdout or later out-of-sample validation before live activation.",
            ],
        }
    )


def alert_price_path(
    event_id: int,
    horizon_minutes: int = 240,
    max_points: int = 80,
) -> Dict[str, Any]:
    """Fetch one bounded canonical Binance Spot path for an archived alert."""
    identifier = int(event_id)
    if identifier < 1:
        raise ValueError("event_id must be positive")
    horizon = int(horizon_minutes)
    if horizon not in _SUPPORTED_HORIZONS:
        raise ValueError("horizon_minutes must be 60, 240, 720 or 1440")
    point_limit = max(10, min(int(max_points), 120))

    status = archive_status()
    if not status.get("schema_present"):
        return {"available": False, "reason": "research archive schema is not installed"}

    with _connect() as conn:
        event = conn.execute(
            """
            SELECT event_id, alert_time_utc, symbol, direction, current_price,
                   target_price, engine_snapshot, strategy_version, code_version
            FROM research_events
            WHERE event_id=%s AND event_kind='ALERT' AND delivery_status='DELIVERED'
            """,
            (identifier,),
        ).fetchone()
    if not event:
        return {"available": False, "reason": "delivered alert event was not found"}

    event_time = event["alert_time_utc"]
    if not isinstance(event_time, datetime):
        event_time = datetime.fromisoformat(str(event_time).replace("Z", "+00:00"))
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=timezone.utc)
    event_time = event_time.astimezone(timezone.utc)
    horizon_time = event_time + timedelta(minutes=horizon)
    now = datetime.now(timezone.utc)
    if horizon_time > now:
        return _json_safe(
            {
                "available": False,
                "reason": "requested outcome horizon is not complete yet",
                "event_id": identifier,
                "eligible_at_utc": horizon_time,
            }
        )

    try:
        path_result = binance_spot_price_path.fetch_closed_candles(
            str(event["symbol"]), event_time, horizon_time
        )
    except Exception as exc:
        return {
            "available": False,
            "reason": f"Binance Spot path unavailable: {type(exc).__name__}: {exc}",
            "event_id": identifier,
            "fallback_used": False,
        }
    candles = list(path_result.get("candles") or [])
    if not candles:
        return {
            "available": False,
            "reason": "Binance Spot returned no closed post-alert candles",
            "event_id": identifier,
            "fallback_used": False,
        }

    try:
        reference_price = float(event.get("current_price"))
        if reference_price <= 0:
            raise ValueError
        snapshot = event.get("engine_snapshot")
        if isinstance(snapshot, str):
            try:
                snapshot = json.loads(snapshot)
            except json.JSONDecodeError:
                snapshot = {}
        snapshot = snapshot if isinstance(snapshot, dict) else {}
        reference_source = str(
            snapshot.get("price_source")
            or snapshot.get("top_item_price_source")
            or "research_event_current_price"
        )
        reference_pair = str(
            snapshot.get("price_pair")
            or snapshot.get("top_item_price_pair")
            or ""
        )
        if reference_pair:
            reference_source = f"{reference_source}:{reference_pair}"
    except (TypeError, ValueError):
        reference_price = float(candles[0].open)
        reference_source = "first_full_binance_spot_minute_open"
    metrics = binance_spot_price_path.calculate_path_metrics(
        reference_price=reference_price,
        direction=str(event.get("direction") or "NEUTRAL"),
        event_time=event_time,
        candles=candles,
        target_price=event.get("target_price"),
    )

    stride = max(1, (len(candles) + point_limit - 1) // point_limit)
    selected = candles[::stride]
    if selected[-1] is not candles[-1]:
        selected.append(candles[-1])

    return _json_safe(
        {
            "available": True,
            "event": {
                "event_id": identifier,
                "alert_time_utc": event_time,
                "symbol": event["symbol"],
                "direction": event["direction"],
                "target_price": event.get("target_price"),
                "strategy_version": event.get("strategy_version"),
                "code_version": event.get("code_version"),
            },
            "path": {
                "exchange": "binance",
                "market": "spot",
                "pair": path_result["pair"],
                "interval": path_result["interval"],
                "horizon_minutes": horizon,
                "reference_price": reference_price,
                "reference_source": reference_source,
                "complete": bool(path_result.get("complete")),
                "full_candle_samples": len(candles),
                "returned_points": len(selected),
                "downsample_stride": stride,
                "first_partial_minute_excluded": True,
            },
            "metrics": metrics,
            "sampled_candles": [candle.to_dict() for candle in selected],
            "interpretation": (
                "Metrics use the full one-minute path. sampled_candles are bounded for model context "
                "and must not be used to recalculate extremes when downsample_stride is above 1."
            ),
        }
    )


def alert_context(event_id: int, window_minutes: int = 90) -> Dict[str, Any]:
    """Return one archived alert plus surrounding stored market evidence."""
    identifier = int(event_id)
    if identifier < 1:
        raise ValueError("event_id must be positive")
    window = max(1, min(int(window_minutes), 1440))

    status = archive_status()
    if not status.get("schema_present"):
        return {"available": False, "reason": "research archive schema is not installed"}

    with _connect() as conn:
        event = conn.execute(
            """
            SELECT * FROM research_events
            WHERE event_id=%s AND event_kind='ALERT'
            """,
            (identifier,),
        ).fetchone()
        if not event:
            return {"available": False, "reason": "alert event was not found", "event_id": identifier}
        outcomes = conn.execute(
            """
            SELECT * FROM research_alert_outcomes
            WHERE event_id=%s ORDER BY horizon_minutes
            """,
            (identifier,),
        ).fetchall()

    market_context = ai_history_research.context_at_time(
        str(event["symbol"]), event["alert_time_utc"], window
    )
    return _json_safe(
        {
            "available": True,
            "event": event,
            "outcomes": outcomes,
            "market_context": market_context,
            "warning": (
                "Use only evidence timestamped at or before alert_time_utc when assessing the decision. "
                "Outcome rows are later evidence and must remain separate."
            ),
        }
    )
