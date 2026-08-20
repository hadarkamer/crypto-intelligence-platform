"""Candidate persistence layer for Research Events.

Safety defaults:
- persistence is OFF unless RESEARCH_PERSISTENCE_ENABLED=1;
- a dedicated RESEARCH_DATABASE_URL is required when enabled;
- this module NEVER creates schema;
- Watch/Telegram callers enqueue with put_nowait and never wait for PostgreSQL;
- the background writer batches idempotent inserts using event_fingerprint;
- transient database failures keep the current batch in memory and retry with
  bounded exponential backoff instead of immediately discarding research data.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
import os
import uuid
from typing import Any, Dict, Iterable, Mapping, Optional

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None

import research_event_capture

_ENABLED = os.getenv("RESEARCH_PERSISTENCE_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"}
_DATABASE_URL = os.getenv("RESEARCH_DATABASE_URL", "").strip()
RUNTIME_SESSION_ID = os.getenv("RESEARCH_RUNTIME_SESSION_ID", "").strip() or uuid.uuid4().hex
DEFAULT_QUEUE_CAPACITY = 2000
DEFAULT_BATCH_SIZE = 50
DEFAULT_FLUSH_INTERVAL_SECONDS = 0.5
MAX_RETRY_DELAY_SECONDS = 30.0

_INSERT_SQL = """
INSERT INTO research_events (
    schema_version, event_kind, event_type, alert_time_utc, symbol, direction,
    source_side, timeframe, score, current_price, target_price,
    initial_target_distance_pct, categories, setup_key, event_fingerprint,
    strategy_version, code_version, runtime_session_id, capture_stage,
    delivery_status, delivery_attempted_at_utc, delivered_at_utc, engine_snapshot
) VALUES (
    %(schema_version)s, %(event_kind)s, %(event_type)s, %(alert_time_utc)s,
    %(symbol)s, %(direction)s, %(source_side)s, %(timeframe)s, %(score)s,
    %(current_price)s, %(target_price)s, %(initial_target_distance_pct)s,
    %(categories)s::jsonb, %(setup_key)s, %(event_fingerprint)s,
    %(strategy_version)s, %(code_version)s, %(runtime_session_id)s,
    %(capture_stage)s, %(delivery_status)s, %(delivery_attempted_at_utc)s,
    %(delivered_at_utc)s, %(engine_snapshot)s::jsonb
)
ON CONFLICT (event_fingerprint) DO NOTHING
"""

_ALLOWED_DELIVERY = {
    "UNKNOWN", "NOT_APPLICABLE", "APPROVED_FOR_DELIVERY", "DELIVERED", "DELIVERY_FAILED"
}


@dataclass
class WriterMetrics:
    enqueued: int = 0
    inserted_or_deduped: int = 0
    queue_full_drops: int = 0
    write_failures: int = 0
    batch_retries: int = 0
    batches: int = 0


def persistence_status() -> Dict[str, Any]:
    return {
        "enabled": _ENABLED,
        "configured": bool(_DATABASE_URL),
        "database_source": "RESEARCH_DATABASE_URL" if _DATABASE_URL else None,
        "schema_auto_create": False,
        "watch_blocking_writes": False,
        "idempotency": "event_fingerprint",
        "runtime_session_id": RUNTIME_SESSION_ID,
        "transient_failure_policy": "retry_current_batch_with_backoff",
        "default_queue_capacity": DEFAULT_QUEUE_CAPACITY,
        "default_batch_size": DEFAULT_BATCH_SIZE,
    }


def _delivery(value: str, event_kind: str) -> str:
    normalized = str(value or "").strip().upper()
    if not normalized:
        normalized = "NOT_APPLICABLE" if event_kind != "ALERT" else "UNKNOWN"
    if normalized not in _ALLOWED_DELIVERY:
        raise ValueError(f"invalid research delivery status: {value!r}")
    return normalized


def serialize_event(
    event: research_event_capture.ResearchEvent,
    *, capture_stage: str = "OBSERVED",
    delivery_status: str = "",
    delivery_attempted_at_utc: Any = None,
    delivered_at_utc: Any = None,
) -> Dict[str, Any]:
    """Serialize without changing the event's decision-time timestamp.

    alert_time_utc is always the signal decision/observation anchor. Telegram
    lifecycle times, when known, are separate metadata and never replace it.
    """
    research_event_capture.validate_event(event)
    data = event.to_dict()
    normalized_delivery = _delivery(delivery_status, data["event_kind"])
    if normalized_delivery == "DELIVERED" and delivered_at_utc is None:
        raise ValueError("DELIVERED research event requires delivered_at_utc")
    return {
        "schema_version": data["schema_version"],
        "event_kind": data["event_kind"],
        "event_type": data["event_type"],
        "alert_time_utc": data["alert_time_utc"],
        "symbol": data["symbol"],
        "direction": data["direction"],
        "source_side": data.get("source_side"),
        "timeframe": data.get("timeframe"),
        "score": data.get("score"),
        "current_price": data.get("current_price"),
        "target_price": data.get("target_price"),
        "initial_target_distance_pct": data.get("initial_target_distance_pct"),
        "categories": json.dumps(data.get("categories") or [], ensure_ascii=False, separators=(",", ":")),
        "setup_key": data["setup_key"],
        "event_fingerprint": data["event_fingerprint"],
        "strategy_version": data["strategy_version"],
        "code_version": data["code_version"],
        "runtime_session_id": RUNTIME_SESSION_ID,
        "capture_stage": str(capture_stage or "OBSERVED").strip().upper(),
        "delivery_status": normalized_delivery,
        "delivery_attempted_at_utc": delivery_attempted_at_utc,
        "delivered_at_utc": delivered_at_utc,
        "engine_snapshot": json.dumps(data.get("engine_snapshot") or {}, ensure_ascii=False, separators=(",", ":")),
    }


class AsyncResearchEventWriter:
    """Bounded non-blocking queue with a separate PostgreSQL batch writer."""

    def __init__(
        self,
        *,
        capacity: int = DEFAULT_QUEUE_CAPACITY,
        batch_size: int = DEFAULT_BATCH_SIZE,
        flush_interval_seconds: float = DEFAULT_FLUSH_INTERVAL_SECONDS,
    ) -> None:
        if capacity < 1 or capacity > 10000:
            raise ValueError("capacity must be between 1 and 10000")
        if batch_size < 1 or batch_size > 500:
            raise ValueError("batch_size must be between 1 and 500")
        self.queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue(maxsize=capacity)
        self.batch_size = int(batch_size)
        self.flush_interval_seconds = float(flush_interval_seconds)
        self.metrics = WriterMetrics()
        self._task: Optional[asyncio.Task] = None
        self._stopping = False

    @property
    def enabled(self) -> bool:
        return _ENABLED

    def status(self) -> Dict[str, Any]:
        return {
            **persistence_status(),
            "running": bool(self._task and not self._task.done()),
            "queue_size": self.queue.qsize(),
            "queue_capacity": self.queue.maxsize,
            "metrics": self.metrics.__dict__.copy(),
        }

    async def start(self) -> bool:
        if not _ENABLED:
            return False
        if not _DATABASE_URL:
            raise RuntimeError("RESEARCH_PERSISTENCE_ENABLED=1 requires RESEARCH_DATABASE_URL")
        if psycopg is None:
            raise RuntimeError("psycopg is unavailable")
        if self._task and not self._task.done():
            return True
        self._stopping = False
        self._task = asyncio.create_task(self._run(), name="research-event-writer")
        return True

    def enqueue(
        self,
        event: research_event_capture.ResearchEvent,
        *,
        capture_stage: str = "OBSERVED",
        delivery_status: str = "",
        delivery_attempted_at_utc: Any = None,
        delivered_at_utc: Any = None,
    ) -> bool:
        """Never blocks Watch/Telegram. Returns False if disabled or saturated."""
        if not _ENABLED:
            return False
        row = serialize_event(
            event,
            capture_stage=capture_stage,
            delivery_status=delivery_status,
            delivery_attempted_at_utc=delivery_attempted_at_utc,
            delivered_at_utc=delivered_at_utc,
        )
        try:
            self.queue.put_nowait(row)
            self.metrics.enqueued += 1
            return True
        except asyncio.QueueFull:
            self.metrics.queue_full_drops += 1
            print("[research-store] queue full; research event dropped", flush=True)
            return False

    async def stop(self, *, flush_timeout_seconds: float = 5.0) -> None:
        if not self._task:
            return
        self._stopping = True
        try:
            await asyncio.wait_for(self._task, timeout=flush_timeout_seconds)
        except asyncio.TimeoutError:
            self._task.cancel()
        finally:
            self._task = None

    async def _run(self) -> None:
        while not self._stopping or not self.queue.empty():
            batch: list[Dict[str, Any]] = []
            try:
                first = await asyncio.wait_for(self.queue.get(), timeout=self.flush_interval_seconds)
                batch.append(first)
            except asyncio.TimeoutError:
                continue
            while len(batch) < self.batch_size and not self.queue.empty():
                batch.append(self.queue.get_nowait())

            attempt = 0
            while True:
                try:
                    await asyncio.to_thread(self._write_batch, batch)
                    self.metrics.inserted_or_deduped += len(batch)
                    self.metrics.batches += 1
                    for _ in batch:
                        self.queue.task_done()
                    break
                except Exception as exc:
                    attempt += 1
                    self.metrics.write_failures += len(batch)
                    self.metrics.batch_retries += 1
                    delay = min(MAX_RETRY_DELAY_SECONDS, 0.5 * (2 ** min(attempt - 1, 6)))
                    print(
                        f"[research-store] batch write failed attempt={attempt}; "
                        f"retry_in={delay:.1f}s; queued={self.queue.qsize()}; error={exc!r}",
                        flush=True,
                    )
                    # Keep the current batch in memory. Watch/Telegram never wait;
                    # new events can continue entering the bounded queue. If a long
                    # DB outage fills the queue, queue_full_drops exposes the loss.
                    await asyncio.sleep(delay)

    def _write_batch(self, rows: Iterable[Mapping[str, Any]]) -> None:
        if not _DATABASE_URL or psycopg is None:
            raise RuntimeError("research database is not configured")
        with psycopg.connect(
            _DATABASE_URL,
            connect_timeout=3,
            options="-c statement_timeout=3000 -c lock_timeout=1000",
        ) as conn:
            with conn.cursor() as cur:
                cur.executemany(_INSERT_SQL, list(rows))


# Importing the module is safe: no task starts and no write can happen until
# both the explicit enable flag and dedicated research DB URL are configured.
WRITER = AsyncResearchEventWriter()
