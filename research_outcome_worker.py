"""Fail-open canonical spot-path enrichment for delivered Research Events.

The worker is observational: it never changes alert logic. Once an alert has
aged into a configured horizon, one canonical spot one-minute path is
fetched and converted into fixed-horizon return, MFE, MAE, speed and optional
target-progress measurements. Existing 30-minute v1 rows are upgraded in
place, preserving the ``(event_id, horizon_minutes)`` identity.

Binance Spot USDT is the default route. HYPE is explicitly routed to the
Hyperliquid HYPE/USDT spot market. Historical candles may be imported from
those exchange APIs as long as their provenance and quality remain attached.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import os
from typing import Any, Dict, Iterable, Optional

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    dict_row = None

import binance_spot_price_path
import canonical_price_path


_TRUE = {"1", "true", "yes", "on"}
_ENABLED = os.getenv("RESEARCH_OUTCOME_ENRICHMENT_ENABLED", "").strip().lower() in _TRUE
_HORIZONS = (60, 240, 720, 1440)
_POLL_SECONDS = max(60, int(os.getenv("RESEARCH_OUTCOME_POLL_SECONDS", "900")))
_METHOD_VERSION = canonical_price_path.METHOD_VERSION


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


def _snapshot_price_source(value: Any) -> str:
    snapshot = value
    if isinstance(snapshot, str):
        try:
            snapshot = json.loads(snapshot)
        except json.JSONDecodeError:
            snapshot = {}
    if not isinstance(snapshot, dict):
        snapshot = {}
    market_evidence = snapshot.get("market_evidence")
    if not isinstance(market_evidence, dict):
        market_evidence = {}
    source = str(
        snapshot.get("price_source")
        or snapshot.get("top_item_price_source")
        or market_evidence.get("price_source")
        or "research_event_current_price"
    ).strip()
    pair = str(
        snapshot.get("price_pair")
        or snapshot.get("top_item_price_pair")
        or market_evidence.get("price_pair")
        or ""
    ).strip()
    return ":".join(part for part in (source, pair) if part)


def _due_horizons(
    event_time: datetime,
    existing_versions: Dict[int, str],
    *,
    now: datetime,
) -> list[int]:
    due = []
    for horizon in _HORIZONS:
        if event_time > now - timedelta(minutes=horizon):
            continue
        if existing_versions.get(horizon) == _METHOD_VERSION:
            continue
        due.append(horizon)
    return due


def _versions(value: Any) -> Dict[int, str]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            value = {}
    if not isinstance(value, dict):
        return {}
    result: Dict[int, str] = {}
    for key, version in value.items():
        try:
            result[int(key)] = str(version or "")
        except (TypeError, ValueError):
            continue
    return result


def _candles_for_horizon(
    candles: Iterable[binance_spot_price_path.SpotCandle],
    horizon_time: datetime,
) -> list[binance_spot_price_path.SpotCandle]:
    cutoff = _utc(horizon_time)
    return [candle for candle in candles if candle.close_time_utc <= cutoff]


def _expected_candles(event_time: datetime, horizon_time: datetime) -> int:
    start_ms = int(_utc(event_time).timestamp() * 1000)
    end_ms = int(_utc(horizon_time).timestamp() * 1000)
    interval_ms = canonical_price_path.INTERVAL_MS
    first_open = ((start_ms + interval_ms - 1) // interval_ms) * interval_ms
    last_open = ((end_ms - (interval_ms - 1)) // interval_ms) * interval_ms
    if last_open < first_open:
        return 0
    return int((last_open - first_open) // interval_ms) + 1


@dataclass
class OutcomeMetrics:
    runs: int = 0
    events_checked: int = 0
    outcomes_inserted: int = 0
    outcomes_upgraded: int = 0
    missing_price_paths: int = 0
    partial_price_paths: int = 0
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
            "price_paths": {
                "default": "Binance Spot USDT",
                "HYPE": "Hyperliquid HYPE/USDT spot (@107)",
                "market": "spot",
                "interval": canonical_price_path.INTERVAL,
                "first_partial_minute": "excluded_to_prevent_pre_alert_leakage",
                "historical_imports": "allowed_with_source_and_quality_provenance",
            },
            "complete_quality_statuses": list(canonical_price_path.COMPLETE_QUALITIES),
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
    def _load_due_events(conn, limit: int) -> list[Dict[str, Any]]:
        clauses = []
        condition_params: list[Any] = []
        for horizon in _HORIZONS:
            clauses.append(
                """
                (
                    e.alert_time_utc <= NOW() - (%s * INTERVAL '1 minute')
                    AND NOT EXISTS (
                        SELECT 1 FROM research_alert_outcomes current_o
                        WHERE current_o.event_id=e.event_id
                          AND current_o.horizon_minutes=%s
                          AND current_o.outcome_method_version=%s
                          AND current_o.data_quality_status=ANY(%s)
                    )
                )
                """
            )
            condition_params.extend(
                (
                    horizon,
                    horizon,
                    _METHOD_VERSION,
                    list(canonical_price_path.COMPLETE_QUALITIES),
                )
            )
        query = f"""
            SELECT e.event_id, e.alert_time_utc, e.symbol, e.direction,
                   e.current_price, e.target_price, e.engine_snapshot,
                   COALESCE(
                       jsonb_object_agg(
                           o.horizon_minutes,
                           CASE
                               WHEN o.outcome_method_version=%s
                                AND o.data_quality_status=ANY(%s)
                               THEN o.outcome_method_version
                               ELSE COALESCE(o.outcome_method_version, '') || ':incomplete'
                           END
                       )
                           FILTER (WHERE o.event_id IS NOT NULL),
                       '{{}}'::jsonb
                   ) AS outcome_versions
            FROM research_events e
            LEFT JOIN research_alert_outcomes o ON o.event_id=e.event_id
            WHERE e.event_kind='ALERT'
              AND e.delivery_status='DELIVERED'
              AND ({' OR '.join(clauses)})
            GROUP BY e.event_id
            ORDER BY e.alert_time_utc ASC
            LIMIT %s
        """
        params: list[Any] = [
            _METHOD_VERSION,
            list(canonical_price_path.COMPLETE_QUALITIES),
            *condition_params,
            max(1, min(int(limit), 1000)),
        ]
        return conn.execute(query, params).fetchall()

    @staticmethod
    def _write_outcome(
        conn,
        *,
        event: Dict[str, Any],
        horizon: int,
        reference_price: float,
        reference_source: str,
        path_result: Dict[str, Any],
        path_metrics: Dict[str, Any],
        complete: bool,
    ) -> bool:
        exchange = str(path_result.get("exchange") or "unknown").lower()
        market = str(path_result.get("market") or "spot").lower()
        source = (
            f"reference={reference_source}|path={exchange}_{market}:"
            f"{path_result['pair']}:{path_result['interval']}|"
            f"provenance={path_result.get('provenance') or 'exchange_api'}"
        )
        quality = canonical_price_path.quality_status(path_result, complete=complete)
        row = conn.execute(
            """
            INSERT INTO research_alert_outcomes (
                event_id, horizon_minutes, measured_at_utc,
                reference_price, price_at_horizon, raw_return_pct,
                directional_return_pct, max_favorable_price,
                max_adverse_price, mfe_pct, mae_pct,
                time_to_first_progress_seconds, time_to_mfe_seconds,
                time_to_closest_target_seconds, time_to_target_seconds,
                closest_target_price, closest_target_distance_pct,
                target_progress_ratio, target_reached,
                path_resolution_seconds, path_samples,
                outcome_method_version, price_source, data_quality_status
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (event_id, horizon_minutes) DO UPDATE SET
                measured_at_utc=EXCLUDED.measured_at_utc,
                reference_price=EXCLUDED.reference_price,
                price_at_horizon=EXCLUDED.price_at_horizon,
                raw_return_pct=EXCLUDED.raw_return_pct,
                directional_return_pct=EXCLUDED.directional_return_pct,
                max_favorable_price=EXCLUDED.max_favorable_price,
                max_adverse_price=EXCLUDED.max_adverse_price,
                mfe_pct=EXCLUDED.mfe_pct,
                mae_pct=EXCLUDED.mae_pct,
                time_to_first_progress_seconds=EXCLUDED.time_to_first_progress_seconds,
                time_to_mfe_seconds=EXCLUDED.time_to_mfe_seconds,
                time_to_closest_target_seconds=EXCLUDED.time_to_closest_target_seconds,
                time_to_target_seconds=EXCLUDED.time_to_target_seconds,
                closest_target_price=EXCLUDED.closest_target_price,
                closest_target_distance_pct=EXCLUDED.closest_target_distance_pct,
                target_progress_ratio=EXCLUDED.target_progress_ratio,
                target_reached=EXCLUDED.target_reached,
                path_resolution_seconds=EXCLUDED.path_resolution_seconds,
                path_samples=EXCLUDED.path_samples,
                outcome_method_version=EXCLUDED.outcome_method_version,
                price_source=EXCLUDED.price_source,
                data_quality_status=EXCLUDED.data_quality_status,
                created_at=NOW()
            WHERE research_alert_outcomes.outcome_method_version
                  IS DISTINCT FROM EXCLUDED.outcome_method_version
               OR research_alert_outcomes.data_quality_status
                  IS DISTINCT FROM EXCLUDED.data_quality_status
               OR research_alert_outcomes.path_samples < EXCLUDED.path_samples
            RETURNING event_id
            """,
            (
                event["event_id"],
                horizon,
                path_metrics["measured_at_utc"],
                reference_price,
                path_metrics["price_at_horizon"],
                path_metrics["raw_return_pct"],
                path_metrics["directional_return_pct"],
                path_metrics["max_favorable_price"],
                path_metrics["max_adverse_price"],
                path_metrics["mfe_pct"],
                path_metrics["mae_pct"],
                path_metrics["time_to_first_progress_seconds"],
                path_metrics["time_to_mfe_seconds"],
                path_metrics["time_to_closest_target_seconds"],
                path_metrics["time_to_target_seconds"],
                path_metrics["closest_target_price"],
                path_metrics["closest_target_distance_pct"],
                path_metrics["target_progress_ratio"],
                path_metrics["target_reached"],
                canonical_price_path.INTERVAL_SECONDS,
                len(path_result["candles"]),
                _METHOD_VERSION,
                source,
                quality,
            ),
        ).fetchone()
        return bool(row)

    def run_once(self, *, limit_per_horizon: int = 200) -> Dict[str, Any]:
        url = _database_url()
        if not _ENABLED:
            return {"enabled": False, "inserted": 0, "upgraded": 0}
        if not url or psycopg is None:
            raise RuntimeError("Research outcome worker database is not configured")

        inserted = 0
        upgraded = 0
        checked = 0
        path_failures = 0
        partial_paths = 0
        now = datetime.now(timezone.utc)
        prepared: list[Dict[str, Any]] = []
        unavailable_symbols: Dict[str, str] = {}
        unavailable_event_counts: Dict[str, int] = {}

        with psycopg.connect(
            url,
            row_factory=dict_row,
            connect_timeout=5,
            options="-c statement_timeout=15000 -c lock_timeout=1000",
        ) as conn:
            events = self._load_due_events(conn, limit_per_horizon)
        # Do not hold a PostgreSQL connection or transaction while waiting for
        # Binance. This keeps outcome research isolated from the live bot load.
        for event in events:
            checked += 1
            symbol = str(event["symbol"]).strip().upper()
            event_time = _utc(event["alert_time_utc"])
            versions = _versions(event.get("outcome_versions"))
            horizons = _due_horizons(event_time, versions, now=now)
            if not horizons:
                continue

            # A symbol whose canonical provider rejected it earlier in this run will be
            # retried on the next scheduled run, but never once per event in
            # the same batch.  Missing metrics still count every affected
            # event so health reporting remains honest.
            if symbol in unavailable_symbols:
                path_failures += 1
                unavailable_event_counts[symbol] += 1
                continue

            max_horizon = max(horizons)
            horizon_time = event_time + timedelta(minutes=max_horizon)
            try:
                path_result = canonical_price_path.fetch_closed_candles(
                    symbol, event_time, horizon_time
                )
            except Exception as exc:
                path_failures += 1
                unavailable_symbols[symbol] = repr(exc)
                unavailable_event_counts[symbol] = 1
                print(
                    f"[research-outcomes] canonical {canonical_price_path.provider_for_symbol(symbol)} "
                    f"spot path unavailable event={event['event_id']} "
                    f"symbol={symbol}: {exc!r}",
                    flush=True,
                )
                continue

            full_path = list(path_result.get("candles") or [])
            if not full_path:
                path_failures += 1
                unavailable_symbols[symbol] = "empty closed-candle path"
                unavailable_event_counts[symbol] = 1
                continue

            reference_value = event.get("current_price")
            reference_source = _snapshot_price_source(event.get("engine_snapshot"))
            try:
                reference_price = float(reference_value)
                if reference_price <= 0:
                    raise ValueError
            except (TypeError, ValueError):
                reference_price = float(full_path[0].open)
                reference_source = (
                    f"{path_result['exchange']}_spot:"
                    f"{path_result['pair']}:first_full_minute_open"
                )

            for horizon in horizons:
                cutoff = event_time + timedelta(minutes=horizon)
                candles = _candles_for_horizon(full_path, cutoff)
                if not candles:
                    path_failures += 1
                    continue
                expected = _expected_candles(event_time, cutoff)
                complete = len(candles) == expected
                partial_paths += int(not complete)
                metrics = binance_spot_price_path.calculate_path_metrics(
                    reference_price=reference_price,
                    direction=str(event.get("direction") or "NEUTRAL"),
                    event_time=event_time,
                    candles=candles,
                    target_price=event.get("target_price"),
                )
                outcome_path = dict(path_result)
                outcome_path["candles"] = candles
                prepared.append(
                    {
                        "event": event,
                        "horizon": horizon,
                        "reference_price": reference_price,
                        "reference_source": reference_source,
                        "path_result": outcome_path,
                        "path_metrics": metrics,
                        "complete": complete,
                        "upgrade": horizon in versions,
                    }
                )

        if unavailable_symbols:
            summary = ", ".join(
                f"{symbol} events={unavailable_event_counts[symbol]}"
                for symbol in sorted(unavailable_symbols)
            )
            print(
                f"[research-outcomes] unavailable canonical spot symbols this run: {summary}",
                flush=True,
            )

        if prepared:
            with psycopg.connect(
                url,
                row_factory=dict_row,
                connect_timeout=5,
                options="-c statement_timeout=15000 -c lock_timeout=1000",
            ) as conn:
                for outcome in prepared:
                    written = self._write_outcome(
                        conn,
                        event=outcome["event"],
                        horizon=outcome["horizon"],
                        reference_price=outcome["reference_price"],
                        reference_source=outcome["reference_source"],
                        path_result=outcome["path_result"],
                        path_metrics=outcome["path_metrics"],
                        complete=outcome["complete"],
                    )
                    if not written:
                        continue
                    if outcome["upgrade"]:
                        upgraded += 1
                    else:
                        inserted += 1

        self.metrics.runs += 1
        self.metrics.events_checked += checked
        self.metrics.outcomes_inserted += inserted
        self.metrics.outcomes_upgraded += upgraded
        self.metrics.missing_price_paths += path_failures
        self.metrics.partial_price_paths += partial_paths
        self.metrics.last_run_utc = datetime.now(timezone.utc).isoformat()
        self.metrics.last_error = None
        return {
            "enabled": True,
            "checked": checked,
            "inserted": inserted,
            "upgraded": upgraded,
            "missing_price_paths": path_failures,
            "partial_price_paths": partial_paths,
            "unavailable_symbols": {
                symbol: unavailable_event_counts[symbol]
                for symbol in sorted(unavailable_symbols)
            },
        }


WORKER = ResearchOutcomeWorker()
