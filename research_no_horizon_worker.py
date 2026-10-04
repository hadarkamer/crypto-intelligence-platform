"""Opt-in PostgreSQL executor for admitted, immutable no-horizon cohorts.

Executes bounded queued work only. It neither invents research populations nor
collects provider data, promotes formulas, delivers messages or places trades.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import os
from uuid import uuid4

import research_no_horizon_postgres_store as store

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:
    psycopg = None
    dict_row = None

_TRUE = {"1", "true", "yes", "on"}
_AUTHORITY = {"runtime_authorized": False, "telegram_authorized": False,
              "trading_authorized": False}


def enabled():
    # No inheritance from legacy outcome/Formula/Watch flags.
    return os.getenv("RESEARCH_NO_HORIZON_ENABLED", "").strip().lower() in _TRUE


def _database_url():
    return os.getenv("RESEARCH_NO_HORIZON_DATABASE_URL", "").strip()


def _settings():
    fields = {
        "poll_seconds": ("POLL_SECONDS", 30, 5, 3600),
        "candle_budget": ("CANDLE_BUDGET", 4096, 1, 100000),
        "entry_budget": ("ENTRY_BUDGET", 64, 1, 2048),
        "batch_size": ("BATCH_SIZE", 128, 1, 4096),
        "lease_seconds": ("LEASE_SECONDS", 120, 30, 600),
    }
    result = {}
    for key, (suffix, default, minimum, maximum) in fields.items():
        name = "RESEARCH_NO_HORIZON_" + suffix
        try:
            value = int(os.getenv(name, str(default)))
        except ValueError as exc:
            raise ValueError("Invalid integer setting: " + name) from exc
        if not minimum <= value <= maximum:
            raise ValueError("Out-of-range setting: " + name)
        result[key] = value
    return result


def _connect(url):
    if psycopg is None:
        raise RuntimeError("No-horizon PostgreSQL driver unavailable")
    return psycopg.connect(url, row_factory=dict_row, connect_timeout=5,
        options="-c statement_timeout=15000 -c lock_timeout=1000 "
                "-c idle_in_transaction_session_timeout=20000 "
                "-c application_name=no_horizon_research")


class ResearchNoHorizonWorker:
    def __init__(self):
        self._task = None
        self._stop_event = asyncio.Event()
        self._lifecycle_lock = asyncio.Lock()
        self._schema_ready = False
        self._config = None
        self.worker_id = "no-horizon-" + uuid4().hex
        self.metrics = {"runs": 0, "claimed_passes": 0, "failures": 0,
            "last_run_utc": None, "last_error_type": None, "last_work": None}

    def status(self):
        return {"enabled": enabled(), "configured": bool(_database_url()),
            "running": bool(self._task and not self._task.done()),
            "schema_ready": self._schema_ready, "store_version": store.VERSION,
            "settings": dict(self._config) if self._config else None,
            "execution_scope": "ALREADY_ADMITTED_FROZEN_GLOBAL_COHORTS",
            "automatic_source_admission": False, "provider_requests": False,
            "live_effect": "NONE", "metrics": dict(self.metrics), **_AUTHORITY}

    def _check_schema(self):
        if not _database_url():
            raise RuntimeError("Explicit no-horizon research database required")
        with _connect(_database_url()) as conn:
            result = store.schema_status(conn)
        if not result["schema_present"]:
            raise RuntimeError("No-horizon research migration is missing or incompatible")
        return True

    async def start(self):
        async with self._lifecycle_lock:
            if self._task and not self._task.done():
                return True
            if not enabled():
                return False
            self._config = _settings()
            self._schema_ready = await asyncio.to_thread(self._check_schema)
            self._stop_event.clear()
            self._task = asyncio.create_task(self._run(), name="no-horizon-research")
            return True

    async def stop(self):
        async with self._lifecycle_lock:
            self._stop_event.set()
            task = self._task
            if task is not None:
                # Never detach an in-flight DB thread: the bounded pass must
                # finish or roll back before lifecycle replacement is allowed.
                await asyncio.shield(task)
                self._task = None

    async def _run(self):
        while not self._stop_event.is_set():
            pending = asyncio.create_task(asyncio.to_thread(self.run_once))
            try:
                await asyncio.shield(pending)
            except asyncio.CancelledError:
                self._stop_event.set()
                try:
                    await asyncio.shield(pending)
                finally:
                    raise
            except Exception:
                # run_once records a sanitized diagnostic; subsequent passes
                # can recover after transient DB failures/expired leases.
                pass
            if self._stop_event.is_set():
                break
            try:
                await asyncio.wait_for(self._stop_event.wait(),
                    timeout=self._config["poll_seconds"])
            except asyncio.TimeoutError:
                pass

    def run_once(self):
        if not enabled():
            return {"claimed": False, "reason": "DISABLED", **_AUTHORITY}
        try:
            config = self._config or _settings()
            if not _database_url():
                raise RuntimeError("Explicit no-horizon research database required")
            with _connect(_database_url()) as conn:
                result = store.PostgresCohortStore(conn).run_once(self.worker_id,
                    **{key: config[key] for key in
                       ("lease_seconds", "candle_budget", "entry_budget", "batch_size")})
            work = None if result is None else {key: result[key] for key in
                ("plan_id", "scope_ordinal", "candle_evaluations_this_run")}
            self.metrics["runs"] += 1
            self.metrics["claimed_passes"] += int(result is not None)
            self.metrics["last_run_utc"] = datetime.now(timezone.utc).isoformat()
            self.metrics["last_error_type"] = None
            self.metrics["last_work"] = work
            return {"claimed": result is not None, "work": work, **_AUTHORITY}
        except Exception as exc:
            self.metrics["failures"] += 1
            # Do not expose DSNs or query/payload contents in public health.
            self.metrics["last_error_type"] = type(exc).__name__
            raise


WORKER = ResearchNoHorizonWorker()
