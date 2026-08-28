"""Fail-open fixed-horizon outcome enrichment for delivered Research Events.

Version 1 intentionally uses the existing 30-minute Price/OI close history only
for fixed-horizon returns. It leaves MFE, MAE and exact target timing empty,
because that precision requires a finer verified price path.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import os
from typing import Any, Dict, Optional

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    dict_row = None

_TRUE = {"1", "true", "yes", "on"}
_ENABLED = os.getenv("RESEARCH_OUTCOME_ENRICHMENT_ENABLED", "").strip().lower() in _TRUE
_HORIZONS = (60, 240, 720, 1440)
_POLL_SECONDS = max(60, int(os.getenv("RESEARCH_OUTCOME_POLL_SECONDS", "900")))
_METHOD_VERSION = "fixed-horizon-30m-close-v1"
_QUALITY = "APPROXIMATE_FIXED_HORIZON_ONLY_NO_MFE_MAE"


def _database_url() -> str:
    dedicated = os.getenv("RESEARCH_DATABASE_URL", "").strip()
    use_primary = os.getenv("RESEARCH_USE_PRIMARY_DATABASE", "").strip().lower() in _TRUE
    if dedicated:
        return dedicated
    if use_primary:
        return os.getenv("DATABASE_URL", "").strip()
    return ""


def _utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def calculate_returns(
    reference_price: float,
    horizon_price: float,
    direction: str,
) -> tuple[float, Optional[float]]:
    """Return raw and direction-adjusted percentages for deterministic tests."""
    reference = float(reference_price)
    horizon = float(horizon_price)
    if reference <= 0:
        raise ValueError("reference_price must be positive")
    raw = (horizon - reference) / reference * 100.0
    normalized = str(direction or "NEUTRAL").upper()
    directional = raw if normalized == "LONG" else -raw if normalized == "SHORT" else None
    return raw, directional


@dataclass
class OutcomeMetrics:
    runs: int = 0
    events_checked: int = 0
    outcomes_inserted: int = 0
    missing_price_rows: int = 0
    failures: int = 0
    last_run_utc: Optional[str] = None
    last_error: Optional[str] = None


class ResearchOutcomeWorker:
    def __init__(self) -> None:
        self.metrics = OutcomeMetrics()
        self._task: Optional[asyncio.Task] = None
        self._stopping = False

    @property
    def enabled(self) -> bool:
        return _ENABLED

    def status(self) -> Dict[str, Any]:
        return {
            "enabled": _ENABLED,
            "configured": bool(_database_url()),
            "running": bool(self._task and not self._task.done()),
            "horizons_minutes": list(_HORIZONS),
            "poll_seconds": _POLL_SECONDS,
            "method": _METHOD_VERSION,
            "precision": _QUALITY,
            "metrics": self.metrics.__dict__.copy(),
        }

    async def start(self) -> bool:
        if not _ENABLED:
            return False
        if not _database_url():
            raise RuntimeError("Research outcome worker database is not configured")
        if psycopg is None:
            raise RuntimeError("psycopg is unavailable")
        if self._task and not self._task.done():
            return True
        self._stopping = False
        self._task = asyncio.create_task(self._run(), name="research-outcome-worker")
        return True

    async def stop(self) -> None:
        if not self._task:
            return
        self._stopping = True
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None

    async def _run(self) -> None:
        while not self._stopping:
            try:
                await asyncio.to_thread(self.run_once)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.metrics.failures += 1
                self.metrics.last_error = f"{type(exc).__name__}: {exc}"
                print(f"[research-outcomes] run failed: {exc!r}", flush=True)
            await asyncio.sleep(_POLL_SECONDS)

    @staticmethod
    def _nearest_price(conn, symbol: str, target_time: datetime) -> Optional[Dict[str, Any]]:
        return conn.execute(
            """
            SELECT candle_time, price_close, source, price_exchange, price_pair
            FROM oi_price_history
            WHERE symbol=%s
              AND candle_time BETWEEN %s - INTERVAL '45 minutes'
                                  AND %s + INTERVAL '45 minutes'
            ORDER BY ABS(EXTRACT(EPOCH FROM (candle_time - %s)))
            LIMIT 1
            """,
            (symbol, target_time, target_time, target_time),
        ).fetchone()

    def run_once(self, *, limit_per_horizon: int = 200) -> Dict[str, Any]:
        url = _database_url()
        if not _ENABLED:
            return {"enabled": False, "inserted": 0}
        if not url or psycopg is None:
            raise RuntimeError("Research outcome worker database is not configured")

        inserted = 0
        checked = 0
        with psycopg.connect(
            url,
            row_factory=dict_row,
            connect_timeout=5,
            options="-c statement_timeout=15000 -c lock_timeout=1000",
        ) as conn:
            for horizon in _HORIZONS:
                events = conn.execute(
                    """
                    SELECT e.event_id, e.alert_time_utc, e.symbol, e.direction,
                           e.current_price, e.target_price
                    FROM research_events e
                    LEFT JOIN research_alert_outcomes o
                      ON o.event_id=e.event_id AND o.horizon_minutes=%s
                    WHERE e.event_kind='ALERT'
                      AND e.delivery_status='DELIVERED'
                      AND o.event_id IS NULL
                      AND e.alert_time_utc <= NOW() - (%s * INTERVAL '1 minute')
                    ORDER BY e.alert_time_utc ASC
                    LIMIT %s
                    """,
                    (horizon, horizon, max(1, min(int(limit_per_horizon), 1000))),
                ).fetchall()
                for event in events:
                    checked += 1
                    event_time = _utc(event["alert_time_utc"])
                    reference = None
                    if event.get("current_price") is not None:
                        reference = {
                            "candle_time": event_time,
                            "price_close": float(event["current_price"]),
                            "source": "research_event_decision_price",
                            "price_exchange": None,
                            "price_pair": None,
                        }
                    else:
                        reference = self._nearest_price(conn, event["symbol"], event_time)
                    horizon_row = self._nearest_price(
                        conn,
                        event["symbol"],
                        event_time + timedelta(minutes=horizon),
                    )
                    if not reference or not horizon_row:
                        self.metrics.missing_price_rows += 1
                        continue

                    reference_price = float(reference["price_close"])
                    horizon_price = float(horizon_row["price_close"])
                    if reference_price <= 0:
                        self.metrics.missing_price_rows += 1
                        continue
                    direction = str(event.get("direction") or "NEUTRAL").upper()
                    raw_return, directional_return = calculate_returns(
                        reference_price, horizon_price, direction
                    )
                    source_parts = [
                        str(horizon_row.get("source") or "oi_price_history"),
                        str(horizon_row.get("price_exchange") or ""),
                        str(horizon_row.get("price_pair") or ""),
                    ]
                    price_source = ":".join(part for part in source_parts if part)
                    result = conn.execute(
                        """
                        INSERT INTO research_alert_outcomes (
                            event_id, horizon_minutes, measured_at_utc,
                            reference_price, price_at_horizon, raw_return_pct,
                            directional_return_pct, path_resolution_seconds,
                            path_samples, outcome_method_version, price_source,
                            data_quality_status
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, 1800, 1, %s, %s, %s
                        )
                        ON CONFLICT (event_id, horizon_minutes) DO NOTHING
                        RETURNING event_id
                        """,
                        (
                            event["event_id"],
                            horizon,
                            horizon_row["candle_time"],
                            reference_price,
                            horizon_price,
                            raw_return,
                            directional_return,
                            _METHOD_VERSION,
                            price_source,
                            _QUALITY,
                        ),
                    ).fetchone()
                    inserted += int(bool(result))
            conn.commit()

        self.metrics.runs += 1
        self.metrics.events_checked += checked
        self.metrics.outcomes_inserted += inserted
        self.metrics.last_run_utc = datetime.now(timezone.utc).isoformat()
        self.metrics.last_error = None
        return {"enabled": True, "checked": checked, "inserted": inserted}


WORKER = ResearchOutcomeWorker()
