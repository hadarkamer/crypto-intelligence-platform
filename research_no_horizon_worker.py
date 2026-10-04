"""Opt-in PostgreSQL execution and acquisition of frozen no-horizon cohorts.

Acquisition requires its own opt-in and a separate read-only source connection.
Neither path invents populations, contacts providers, promotes formulas,
delivers messages or places trades.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import os
from uuid import uuid4

import research_no_horizon_postgres_store as store
import research_no_horizon_acquisition_store as acquisition

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


def acquisition_enabled():
    return enabled() and os.getenv("RESEARCH_NO_HORIZON_ACQUISITION_ENABLED", "").strip().lower() in _TRUE


def _read_database_url():
    return os.getenv("RESEARCH_NO_HORIZON_READ_DATABASE_URL", "").strip()


def _acquisition_settings():
    name = "RESEARCH_NO_HORIZON_ACQUISITION_LEAF_BUDGET"
    try:
        value = int(os.getenv(name, "4"))
    except ValueError as exc:
        raise ValueError("Invalid integer setting: " + name) from exc
    if not 1 <= value <= 32:
        raise ValueError("Out-of-range setting: " + name)
    return {"leaf_budget": value}


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


def _connect_source(url):
    if psycopg is None:
        raise RuntimeError("No-horizon PostgreSQL driver unavailable")
    return psycopg.connect(url, row_factory=dict_row, connect_timeout=5,
        options="-c default_transaction_read_only=on -c timezone=UTC "
                "-c statement_timeout=15000 -c lock_timeout=1000 "
                "-c idle_in_transaction_session_timeout=20000 "
                "-c application_name=no_horizon_acquisition_source")


class ResearchNoHorizonWorker:
    def __init__(self):
        self._task = None
        self._stop_event = asyncio.Event()
        self._lifecycle_lock = asyncio.Lock()
        self._schema_ready = False
        self._config = None
        self._acquisition_schema_ready = False
        self.acquisition_metrics = {"runs": 0, "claimed_passes": 0,
            "failures": 0, "last_error_type": None, "last_work": None}
        self.worker_id = "no-horizon-" + uuid4().hex
        self.metrics = {"runs": 0, "claimed_passes": 0, "failures": 0,
            "last_run_utc": None, "last_error_type": None, "last_work": None}

    def status(self):
        return {"enabled": enabled(), "configured": bool(_database_url()),
            "running": bool(self._task and not self._task.done()),
            "schema_ready": self._schema_ready, "store_version": store.VERSION,
            "settings": dict(self._config) if self._config else None,
            "execution_scope": "EXPLICITLY_REGISTERED_FROZEN_GLOBAL_COHORTS",
            "automatic_source_admission": acquisition_enabled(),
            "acquisition": {"enabled": acquisition_enabled(),
                "source_configured": bool(_read_database_url()),
                "schema_ready": self._acquisition_schema_ready,
                "metrics": dict(self.acquisition_metrics)},
            "provider_requests": False,
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
                intake_work = self._acquire_once(conn, config)
            work = None if result is None else {key: result[key] for key in
                ("plan_id", "scope_ordinal", "candle_evaluations_this_run")}
            self.metrics["runs"] += 1
            self.metrics["claimed_passes"] += int(result is not None)
            self.metrics["last_run_utc"] = datetime.now(timezone.utc).isoformat()
            self.metrics["last_error_type"] = None
            self.metrics["last_work"] = work
            return {"claimed": result is not None, "work": work,
                    "acquisition": intake_work, **_AUTHORITY}
        except Exception as exc:
            self.metrics["failures"] += 1
            # Do not expose DSNs or query/payload contents in public health.
            self.metrics["last_error_type"] = type(exc).__name__
            raise

    def _acquire_once(self, conn, config):
        if not acquisition_enabled():
            return {"claimed": False, "reason": "DISABLED"}
        try:
            settings = _acquisition_settings()
            read_url = _read_database_url()
            if not read_url:
                raise RuntimeError("Explicit no-horizon read database required")
            if not self._acquisition_schema_ready:
                if not acquisition.schema_status(conn)["schema_present"]:
                    raise RuntimeError("No-horizon acquisition migration is missing or incompatible")
                self._acquisition_schema_ready = True
            result = acquisition.AcquisitionStore(conn).run_once(self.worker_id,
                source_connection_factory=lambda: _connect_source(read_url),
                lease_seconds=config["lease_seconds"], **settings)
            work = None if result is None else {key: result.get(key) for key in
                ("request_id", "status", "plan_id", "proof_count")}
            self.acquisition_metrics["runs"] += 1
            self.acquisition_metrics["claimed_passes"] += int(result is not None)
            self.acquisition_metrics["last_error_type"] = None
            self.acquisition_metrics["last_work"] = work
            return {"claimed": result is not None, "work": work}
        except Exception as exc:
            # Source availability or an intake configuration failure must not
            # prevent already admitted cohorts from executing.
            self._acquisition_schema_ready = False
            self.acquisition_metrics["failures"] += 1
            self.acquisition_metrics["last_error_type"] = type(exc).__name__
            return {"claimed": False, "error_type": type(exc).__name__}


WORKER = ResearchNoHorizonWorker()
