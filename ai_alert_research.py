"""Read-only alert archive queries for the production AI layer.

The functions in this module never create tables or write outcomes. They expose
only bounded summaries and exact event context to the GPT tool layer.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - validated at runtime
    psycopg = None
    dict_row = None

import ai_history_research

_TRUE = {"1", "true", "yes", "on"}
_SUPPORTED_HORIZONS = {60, 240, 720, 1440}


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
                   ) / NULLIF(COUNT(*), 0))::numeric, 2) AS positive_rate_pct
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
            [days, horizon, *symbol_params],
        ).fetchall()

        recent = conn.execute(
            f"""
            SELECT e.event_id, e.alert_time_utc, e.symbol, e.direction,
                   e.source_side, e.timeframe, e.event_type, e.score,
                   e.current_price, e.target_price, e.categories,
                   e.strategy_version, e.code_version, e.delivery_status,
                   o.horizon_minutes, o.measured_at_utc, o.reference_price,
                   o.price_at_horizon, o.raw_return_pct,
                   o.directional_return_pct, o.price_source,
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
                "Current outcome v1 uses a nearest 30-minute close and does not claim exact MFE/MAE.",
                "Historical market reconstructions are not delivered Telegram alerts.",
            ],
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
