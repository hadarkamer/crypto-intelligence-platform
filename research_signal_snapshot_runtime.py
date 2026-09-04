"""Opt-in passive projector for Stage-4 silent signal snapshots.

The existing Max-Pain passive scheduler owns cadence and source collection.
This projector runs only after that raw archive committed successfully.  It has
no Telegram, Formula, outcome or trading path.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sys
from typing import Any, Callable, Dict, Mapping, Optional

import market_confidence_engine
import research_max_pain_archive
import research_signal_snapshot
import research_signal_snapshot_store


ENV_ENABLED = "RESEARCH_SIGNAL_SNAPSHOT_ENABLED"
# Migration 007's immutable archive begins at this cutover.  Reconciliation
# selects only rows without a terminal projection, so a bounded batch drains
# arbitrarily long downtime without repeatedly scanning completed receipts.
RECONCILIATION_ARCHIVE_FLOOR_UTC = datetime(
    2026, 8, 29, tzinfo=timezone.utc
)
RECONCILIATION_LIMIT = 96
PERSIST_RETRY_ATTEMPTS = 3
# Initial attempt plus at most one retry per minute across the decision window;
# the wall-clock deadline remains the primary bound.
CAUSAL_RETRY_MAX_ATTEMPTS = (
    research_signal_snapshot.MAX_DECISION_LAG_MINUTES + 1
)
MAX_PENDING_PROJECTION_TASKS = 4
SUBMISSION_RETRY_INTERVAL_SECONDS = 60.0
SOURCE_READ_TIMEOUT_SECONDS = 120.0
SOURCE_READ_MAX_CONCURRENCY = 8
_TRUE = {"1", "true", "yes", "on"}

_DEFAULT_SOURCE_WORKER_CODE = """
import contextlib
import json
import sys

symbols = json.loads(sys.argv[1])
sys.path.insert(0, sys.argv[2])
with contextlib.redirect_stdout(sys.stderr):
    import market_confidence_engine
    captured = market_confidence_engine.capture_snapshot(symbols)
sys.stdout.write(json.dumps(captured, default=str, allow_nan=False))
"""


def enabled() -> bool:
    try:
        store_status = research_signal_snapshot_store.status()
    except Exception:
        # Stage 4 is optional and must never take down the passive archive
        # scheduler because of malformed or temporarily unavailable config.
        return False
    return os.getenv(ENV_ENABLED, "").strip().lower() in _TRUE and all(
        store_status.get(key) is True
        for key in (
            "configured",
            "persistence_enabled",
            "archive_database_aligned",
            "driver_available",
        )
    )


def _utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("signal snapshot timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


class _CallbackLease:
    """Test adapter; production uses a cross-process PostgreSQL lease."""

    def __init__(self, snapshot_key, persist, projection_loader):
        self.snapshot_key = snapshot_key
        self._persist = persist
        self._projection_loader = projection_loader

    def load(self):
        if self._projection_loader is None:
            return {"terminal": False, "snapshot_key": self.snapshot_key}
        return self._projection_loader(self.snapshot_key)

    def persist(self, events):
        return self._persist(events)

    def close(self):
        return None


def _eligible_symbols(payload: Mapping[str, Any]) -> list[str]:
    return sorted(
        {
            str(item.get("symbol") or "").strip().upper()
            for item in payload.get("symbols") or []
            if isinstance(item, Mapping)
            and item.get("research_eligible") is True
            and str(item.get("symbol") or "").strip()
        }
    )


class SignalSnapshotProjector:
    def __init__(
        self,
        *,
        snapshot_fetcher: Callable[[list[str]], Dict[str, Dict[str, Any]]] = (
            market_confidence_engine.capture_snapshot
        ),
        persist: Optional[Callable[..., Dict[str, Any]]] = None,
        projection_loader: Optional[Callable[[str], Dict[str, Any]]] = None,
        lease_factory: Optional[Callable[[str], Any]] = None,
        archive_loader: Callable[..., list[Dict[str, Any]]] = (
            research_max_pain_archive.load_recent_passive_snapshot_payloads
        ),
        now_provider: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        retry_delay_seconds: float = 0.25,
        submission_retry_interval_seconds: float = (
            SUBMISSION_RETRY_INTERVAL_SECONDS
        ),
        source_read_timeout_seconds: float = SOURCE_READ_TIMEOUT_SECONDS,
        source_read_max_concurrency: int = SOURCE_READ_MAX_CONCURRENCY,
    ) -> None:
        self._snapshot_fetcher = snapshot_fetcher
        # The production provider runs in a child process so a causal timeout
        # or shutdown can actually terminate the blocking operation.  Python
        # cannot kill a running thread; injected callables retain the thread
        # adapter solely for deterministic tests and finite custom providers.
        self._default_source_subprocess = (
            snapshot_fetcher is market_confidence_engine.capture_snapshot
        )
        if lease_factory is not None and (
            persist is not None or projection_loader is not None
        ):
            raise ValueError(
                "lease_factory cannot be combined with persist/projection_loader"
            )
        if lease_factory is not None:
            self._lease_factory = lease_factory
        elif persist is None and projection_loader is None:
            self._lease_factory = (
                research_signal_snapshot_store.acquire_projection_lease
            )
        else:
            callback_persist = persist or research_signal_snapshot_store.persist_events
            self._lease_factory = lambda snapshot_key: _CallbackLease(
                snapshot_key, callback_persist, projection_loader
            )
        self._archive_loader = archive_loader
        self._now = now_provider
        self._retry_delay_seconds = max(0.0, float(retry_delay_seconds))
        self._submission_retry_interval_seconds = max(
            0.0, float(submission_retry_interval_seconds)
        )
        self._source_read_timeout_seconds = max(
            0.01, float(source_read_timeout_seconds)
        )
        self._source_read_max_concurrency = max(
            1, int(source_read_max_concurrency)
        )
        self._source_read_semaphore = asyncio.Semaphore(
            self._source_read_max_concurrency
        )
        self._lock = asyncio.Lock()
        self._background_tasks: set[asyncio.Task] = set()
        self._source_read_tasks: set[asyncio.Task] = set()
        self._source_processes: set[asyncio.subprocess.Process] = set()
        self._runtime: Dict[str, Any] = {
            "status": "off",
            "last_snapshot_key": None,
            "last_snapshot_set_id": None,
            "last_started_at_utc": None,
            "last_completed_at_utc": None,
            "last_decision_time_utc": None,
            "last_counts": None,
            "last_persistence": None,
            "last_error": None,
            "cycles_started": 0,
            "cycles_completed": 0,
            "cycles_failed": 0,
            "events_persisted": 0,
            "last_reconciliation_started_at_utc": None,
            "last_reconciliation_completed_at_utc": None,
            "last_reconciliation_counts": None,
            "last_reconciliation_error": None,
        }

    def status(self) -> Dict[str, Any]:
        value = dict(self._runtime)
        value.update(
            {
                "enabled": enabled(),
                "projection_in_progress": self._lock.locked(),
                "background_projection_tasks": len(self._background_tasks),
                "pending_source_read_tasks": sum(
                    int(not task.done()) for task in self._source_read_tasks
                ),
                "pending_source_processes": sum(
                    int(process.returncode is None)
                    for process in self._source_processes
                ),
                "contract_version": research_signal_snapshot.CONTRACT_VERSION,
                "capture_stage": research_signal_snapshot.CAPTURE_STAGE,
                "event_kind": "DECISION_SAMPLE",
                "formula_consumption_path": False,
                "outcome_consumption_path": False,
                "telegram_delivery_path": False,
                "trading_path": False,
                "source_scheduler": "RESEARCH_PASSIVE_MAX_PAIN_30M",
                "submission_retry_interval_seconds": (
                    self._submission_retry_interval_seconds
                ),
                "source_read_timeout_seconds": self._source_read_timeout_seconds,
                "source_read_max_concurrency": self._source_read_max_concurrency,
                "source_read_execution": (
                    "killable_subprocess"
                    if self._default_source_subprocess
                    else "injected_thread_adapter"
                ),
                "bounded_source_shutdown": self._default_source_subprocess,
                "store": research_signal_snapshot_store.status(),
            }
        )
        return json.loads(json.dumps(value, default=str, allow_nan=False))

    def _track_background(self, awaitable: Any, *, name: str) -> Dict[str, Any]:
        task = asyncio.create_task(awaitable, name=name)
        self._background_tasks.add(task)

        def _finished(done: asyncio.Task) -> None:
            self._background_tasks.discard(done)
            if done.cancelled():
                return
            try:
                done.result()
            except Exception as exc:
                # project() normally fails open; this guard keeps task errors
                # observable if an exception escapes its outer boundary.
                print(
                    f"[signal-snapshot] background task failed: {exc!r}",
                    flush=True,
                )

        task.add_done_callback(_finished)
        return {"submitted": True, "task_name": task.get_name()}

    def submit(self, archive_capture: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
        """Schedule projection without holding up the 30-minute collector."""

        if not enabled():
            return {"submitted": False, "reason": "signal snapshots are disabled"}
        pending = sum(
            int(
                not task.done()
                and task.get_name() == "research-signal-snapshot-projection"
            )
            for task in self._background_tasks
        )
        if pending >= MAX_PENDING_PROJECTION_TASKS:
            return {
                "submitted": False,
                "reason": "projection backlog limit reached; reconciliation will retry",
                "pending": pending,
            }
        return self._track_background(
            self._project_with_causal_retries(archive_capture),
            name="research-signal-snapshot-projection",
        )

    def submit_reconciliation(self) -> Dict[str, Any]:
        """Schedule bounded restart recovery without delaying raw collection."""

        if not enabled():
            return {"submitted": False, "reason": "signal snapshots are disabled"}
        if any(
            not task.done()
            and task.get_name() == "research-signal-snapshot-reconciliation"
            for task in self._background_tasks
        ):
            return {
                "submitted": False,
                "reason": "reconciliation is already pending",
            }
        return self._track_background(
            self.reconcile_recent(),
            name="research-signal-snapshot-reconciliation",
        )

    async def stop(self) -> None:
        """Cancel and drain projector-owned tasks during application shutdown."""

        tasks = tuple(task for task in self._background_tasks if not task.done())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        source_tasks = tuple(
            task for task in self._source_read_tasks if not task.done()
        )
        for task in source_tasks:
            task.cancel()
        if source_tasks:
            # Production subprocess wrappers kill and reap their child;
            # injected thread adapters drain their finite worker because a
            # running Python thread cannot be terminated safely.
            await asyncio.gather(*source_tasks, return_exceptions=True)
        processes = tuple(
            process
            for process in self._source_processes
            if process.returncode is None
        )
        for process in processes:
            try:
                process.kill()
            except ProcessLookupError:
                pass
        if processes:
            await asyncio.gather(
                *(process.wait() for process in processes),
                return_exceptions=True,
            )
        self._background_tasks.clear()
        self._source_read_tasks.clear()
        self._source_processes.clear()
        self._runtime["status"] = "stopped"

    async def _project_with_causal_retries(
        self, archive_capture: Optional[Mapping[str, Any]]
    ) -> Dict[str, Any]:
        """Retry transient/busy projection attempts through the causal window."""

        capture = dict(archive_capture or {})
        try:
            available_at = _utc(
                ((capture.get("payload") or {}).get("set") or {}).get(
                    "available_at_utc"
                )
            )
        except (TypeError, ValueError):
            return await self.project(capture)
        deadline = available_at + timedelta(
            minutes=research_signal_snapshot.MAX_DECISION_LAG_MINUTES
        )
        attempts = 0
        while True:
            attempt_started = self._now()
            result = await self.project(capture)
            attempts += 1
            retry_without_source_read = bool(
                result.pop("_retry_without_source_read", False)
            )
            result = {**result, "submission_attempts": attempts}
            if result.get("projected") is True or result.get("terminal") is True:
                return result
            # Persistence already retries the exact frozen event tuple while
            # its lease remains open.  Once a projection has reached source
            # collection, another outer attempt could observe a new derivative
            # generation for the same immutable archive.  Only lease-busy
            # attempts, which happen before any source read or computation, are
            # therefore eligible for causal-window resubmission.
            if (
                not retry_without_source_read
                or not enabled()
                or attempt_started > deadline
                or attempts >= CAUSAL_RETRY_MAX_ATTEMPTS
            ):
                return result
            remaining = (deadline - self._now()).total_seconds()
            delay = min(
                self._submission_retry_interval_seconds,
                max(0.01, remaining + 0.01),
            )
            await asyncio.sleep(delay)

    async def _blocking(
        self, function: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> Any:
        """Do not close a leased connection while its worker thread still runs."""

        task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            try:
                await task
            finally:
                raise

    def _default_source_subprocess_args(self, symbol: str) -> tuple[str, ...]:
        return (
            sys.executable,
            "-c",
            _DEFAULT_SOURCE_WORKER_CODE,
            json.dumps([symbol], separators=(",", ":")),
            str(Path(__file__).resolve().parent),
        )

    async def _read_default_source_subprocess(
        self, symbol: str
    ) -> Dict[str, Any]:
        """Run the production provider behind a killable process boundary."""

        creation = asyncio.create_task(
            asyncio.create_subprocess_exec(
                *self._default_source_subprocess_args(symbol),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                cwd=str(Path(__file__).resolve().parent),
            )
        )
        try:
            process = await asyncio.shield(creation)
        except asyncio.CancelledError:
            # Resolve the shielded spawn before propagating cancellation so a
            # child created in the cancellation race can still be reaped.
            process = await creation
            if process.returncode is None:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
            await process.wait()
            raise
        self._source_processes.add(process)
        try:
            try:
                stdout, _stderr = await process.communicate()
            except asyncio.CancelledError:
                if process.returncode is None:
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
                await process.wait()
                raise
            if process.returncode != 0:
                raise RuntimeError("derivatives source worker failed")
            parsed = json.loads(stdout.decode("utf-8"))
            if not isinstance(parsed, Mapping):
                raise ValueError("derivatives source worker returned a non-object")
            return dict(parsed)
        finally:
            self._source_processes.discard(process)

    async def _fetch_derivatives(
        self, symbols: list[str], *, timeout_seconds: float
    ) -> Dict[str, Dict[str, Any]]:
        """Read symbols independently within one bounded causal deadline."""

        async def read_one(
            symbol: str, state: Dict[str, bool]
        ) -> tuple[str, Optional[Dict[str, Any]]]:
            try:
                async with self._source_read_semaphore:
                    state["worker_started"] = True
                    if state["killable"]:
                        captured = await self._read_default_source_subprocess(
                            symbol
                        )
                    else:
                        worker = asyncio.create_task(
                            asyncio.to_thread(self._snapshot_fetcher, [symbol])
                        )
                        try:
                            captured = await asyncio.shield(worker)
                        except asyncio.CancelledError:
                            # A running thread cannot be terminated safely.
                            # Drain finite injected providers so their call
                            # remains tracked and retains its concurrency slot.
                            try:
                                await worker
                            finally:
                                raise
                if not isinstance(captured, Mapping):
                    return symbol, None
                value = captured.get(symbol)
                return (
                    (symbol, dict(value))
                    if isinstance(value, Mapping)
                    else (symbol, None)
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                # Provider details can contain credentials or unstable text;
                # the pure contract records a bounded missing-source code.
                return symbol, None

        entries = []
        for symbol in symbols:
            state = {
                "worker_started": False,
                "killable": self._default_source_subprocess,
            }
            task = asyncio.create_task(
                read_one(symbol, state),
                name=f"research-signal-snapshot-source-{symbol}",
            )
            self._source_read_tasks.add(task)

            def _finished(done: asyncio.Task) -> None:
                self._source_read_tasks.discard(done)
                if done.cancelled():
                    return
                # Retrieve unexpected exceptions so detached, timed-out reads
                # never produce an unobserved-task warning.
                done.exception()

            task.add_done_callback(_finished)
            entries.append((task, state))
        tasks = [task for task, _state in entries]
        try:
            done, pending = await asyncio.wait(
                tasks, timeout=max(0.01, timeout_seconds)
            )
        except asyncio.CancelledError:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        # Calls already running in threads cannot be cancelled.  Leave their
        # wrappers tracked so they continue holding the instance-wide
        # concurrency slot and are drained by stop().  Reads that have not
        # started are cancelled, as are killable production subprocesses;
        # neither can accumulate behind a slow provider after the deadline.
        unstarted = [
            task
            for task, state in entries
            if task in pending
            and (not state["worker_started"] or state["killable"])
        ]
        for task in unstarted:
            task.cancel()
        if unstarted:
            await asyncio.gather(*unstarted, return_exceptions=True)
        completed = [task.result() for task in done if not task.cancelled()]
        return {
            symbol: value
            for symbol, value in completed
            if value is not None
        }

    async def _persist_with_retry(self, lease: Any, events: Any) -> Dict[str, Any]:
        frozen_events = tuple(events)
        for attempt in range(1, PERSIST_RETRY_ATTEMPTS + 1):
            try:
                return await self._blocking(lease.persist, frozen_events)
            except research_signal_snapshot_store.SignalSnapshotConflictError:
                raise
            except asyncio.CancelledError:
                raise
            except Exception:
                if attempt >= PERSIST_RETRY_ATTEMPTS:
                    raise
                await asyncio.sleep(self._retry_delay_seconds * attempt)
        raise RuntimeError("unreachable signal snapshot persistence retry state")

    def _completed_runtime(
        self,
        *,
        status: str,
        snapshot_key: str,
        snapshot_set_id: int,
        decision_time_utc: Any,
        counts: Mapping[str, Any],
        persistence: Mapping[str, Any],
    ) -> None:
        completed = self._now()
        persisted_count = int(persistence.get("inserted") or 0) + int(
            persistence.get("idempotent_existing") or 0
        )
        self._runtime.update(
            {
                "status": status,
                "last_snapshot_key": snapshot_key,
                "last_snapshot_set_id": snapshot_set_id,
                "last_completed_at_utc": completed.isoformat(),
                "last_decision_time_utc": decision_time_utc,
                "last_counts": dict(counts),
                "last_persistence": dict(persistence),
                "last_error": None,
                "cycles_completed": int(self._runtime["cycles_completed"]) + 1,
                "events_persisted": int(self._runtime["events_persisted"])
                + persisted_count,
            }
        )

    async def project(
        self, archive_capture: Optional[Mapping[str, Any]]
    ) -> Dict[str, Any]:
        """Project one newly persisted passive archive, failing open to scheduler."""

        if not enabled():
            self._runtime.update({"status": "off", "last_error": None})
            return {"projected": False, "reason": "signal snapshots are disabled"}
        try:
            capture = dict(archive_capture or {})
            payload = dict(capture.get("payload") or {})
            persistence = dict(capture.get("persistence") or {})
            if persistence.get("persisted") is not True:
                self._runtime.update(
                    {
                        "status": "skipped_archive_not_persisted",
                        "last_error": capture.get("error")
                        or persistence.get("reason")
                        or "Max-Pain archive was not persisted",
                    }
                )
                return {"projected": False, "reason": self._runtime["last_error"]}
            set_record = dict(payload.get("set") or {})
            if set_record.get("research_eligible") is not True:
                self._runtime.update(
                    {"status": "skipped_ineligible_archive", "last_error": None}
                )
                return {
                    "projected": False,
                    "reason": "archive is not research eligible",
                }
            snapshot_key = str(set_record.get("snapshot_key") or "").strip()
            research_signal_snapshot.projection_event_fingerprint(snapshot_key)
        except Exception as exc:
            self._runtime.update(
                {
                    "status": "failed",
                    "last_completed_at_utc": self._now().isoformat(),
                    "last_error": f"{type(exc).__name__}: {exc}",
                    "cycles_failed": int(self._runtime["cycles_failed"]) + 1,
                }
            )
            print(
                f"[signal-snapshot] projection failed open: {exc!r}",
                flush=True,
            )
            return {"projected": False, "reason": self._runtime["last_error"]}

        async with self._lock:
            started = self._now()
            lease = None
            retry_without_source_read = True
            self._runtime.update(
                {
                    "status": "projecting",
                    "last_started_at_utc": started.isoformat(),
                    "last_error": None,
                    "cycles_started": int(self._runtime["cycles_started"]) + 1,
                }
            )
            try:
                lease = await self._blocking(self._lease_factory, snapshot_key)
                if lease is None:
                    self._runtime.update(
                        {"status": "skipped_projection_lease_busy", "last_error": None}
                    )
                    return {
                        "projected": False,
                        "snapshot_key": snapshot_key,
                        "reason": "projection lease is held by another process",
                        "_retry_without_source_read": True,
                    }

                existing = dict(await self._blocking(lease.load) or {})
                # A completed load is not retried: a terminal receipt must be
                # consumed exactly once, while a nonterminal result may now
                # proceed into source collection and batch computation.
                retry_without_source_read = False
                if existing.get("terminal") is True:
                    self._runtime.update(
                        {
                            "status": "terminal_projection_exists",
                            "last_snapshot_key": snapshot_key,
                            "last_snapshot_set_id": persistence.get("snapshot_set_id"),
                            "last_completed_at_utc": self._now().isoformat(),
                            "last_decision_time_utc": existing.get(
                                "decision_time_utc"
                            ),
                            "last_counts": dict(existing.get("counts") or {}),
                            "last_persistence": {
                                "persisted": True,
                                "terminal_existing": True,
                                "event_id": existing.get("event_id"),
                            },
                            "last_error": None,
                        }
                    )
                    return {
                        "projected": False,
                        "terminal": True,
                        "snapshot_key": snapshot_key,
                        "projection": existing,
                        "reason": "terminal projection receipt already exists",
                    }

                available_at = _utc(set_record.get("available_at_utc"))
                if started - available_at > timedelta(
                    minutes=research_signal_snapshot.MAX_DECISION_LAG_MINUTES
                ):
                    missed = research_signal_snapshot.build_missed_projection_event(
                        archive_payload=payload,
                        archive_persistence=persistence,
                        observed_at_utc=started,
                    )
                    stored = await self._persist_with_retry(lease, (missed,))
                    projection = dict(
                        missed.engine_snapshot.get("projection") or {}
                    )
                    counts = dict(projection.get("counts") or {})
                    self._completed_runtime(
                        status="missed_causal_window",
                        snapshot_key=snapshot_key,
                        snapshot_set_id=int(persistence["snapshot_set_id"]),
                        decision_time_utc=projection.get("decision_time_utc"),
                        counts=counts,
                        persistence=stored,
                    )
                    return {
                        "projected": False,
                        "terminal": True,
                        "status": "MISSED_CAUSAL_WINDOW",
                        "evaluation_status": "UNEVALUABLE",
                        "snapshot_key": snapshot_key,
                        "snapshot_set_id": int(persistence["snapshot_set_id"]),
                        "counts": counts,
                        "persistence": stored,
                    }

                symbols = _eligible_symbols(payload)
                if not symbols:
                    raise ValueError("passive archive has no eligible symbol manifests")
                derivatives_read_started = self._now()
                causal_seconds_remaining = (
                    available_at
                    + timedelta(
                        minutes=research_signal_snapshot.MAX_DECISION_LAG_MINUTES
                    )
                    - derivatives_read_started
                ).total_seconds()
                derivatives = await self._fetch_derivatives(
                    symbols,
                    timeout_seconds=min(
                        self._source_read_timeout_seconds,
                        max(0.01, causal_seconds_remaining),
                    ),
                )
                derivatives_read_completed = self._now()

                decision_time = self._now()
                batch = research_signal_snapshot.build_signal_snapshot_batch(
                    archive_payload=payload,
                    archive_persistence=persistence,
                    opportunities=(),
                    magnet_observations=(),
                    derivatives_snapshot=derivatives,
                    directional_market_evidence=None,
                    derivatives_read_started_at_utc=derivatives_read_started,
                    derivatives_read_completed_at_utc=derivatives_read_completed,
                    decision_time_utc=decision_time,
                )
                stored = await self._persist_with_retry(lease, batch.events)
                self._completed_runtime(
                    status="completed",
                    snapshot_key=batch.snapshot_key,
                    snapshot_set_id=batch.snapshot_set_id,
                    decision_time_utc=batch.decision_time_utc,
                    counts=batch.counts,
                    persistence=stored,
                )
                return {
                    "projected": True,
                    "snapshot_key": batch.snapshot_key,
                    "snapshot_set_id": batch.snapshot_set_id,
                    "decision_time_utc": batch.decision_time_utc,
                    "counts": dict(batch.counts),
                    "evaluation_status": batch.evaluation_status,
                    "evaluated_symbols": list(batch.evaluated_symbols),
                    "unevaluable_symbols": list(batch.unevaluable_symbols),
                    "persistence": stored,
                }
            except asyncio.CancelledError:
                self._runtime["status"] = "cancelled"
                raise
            except Exception as exc:
                completed = self._now()
                retry_safe = (
                    retry_without_source_read
                    and not isinstance(
                        exc,
                        research_signal_snapshot_store.SignalSnapshotConflictError,
                    )
                )
                self._runtime.update(
                    {
                        "status": "failed",
                        "last_completed_at_utc": completed.isoformat(),
                        "last_error": f"{type(exc).__name__}: {exc}",
                        "cycles_failed": int(self._runtime["cycles_failed"]) + 1,
                    }
                )
                print(
                    f"[signal-snapshot] projection failed open: {exc!r}",
                    flush=True,
                )
                result = {
                    "projected": False,
                    "reason": self._runtime["last_error"],
                }
                if retry_safe:
                    result["_retry_without_source_read"] = True
                return result
            finally:
                if lease is not None:
                    try:
                        await self._blocking(lease.close)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        print(
                            f"[signal-snapshot] projection lease close failed: {exc!r}",
                            flush=True,
                        )

    async def reconcile_recent(self) -> Dict[str, Any]:
        """Revisit a bounded archive window without inventing old derivatives."""

        if not enabled():
            return {"reconciled": False, "reason": "signal snapshots are disabled"}
        started = self._now()
        self._runtime.update(
            {
                "last_reconciliation_started_at_utc": started.isoformat(),
                "last_reconciliation_error": None,
            }
        )
        try:
            captures = await self._blocking(
                self._archive_loader,
                available_since_utc=RECONCILIATION_ARCHIVE_FLOOR_UTC,
                available_before_utc=started,
                limit=RECONCILIATION_LIMIT,
                only_without_signal_snapshot_projection=True,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._runtime.update(
                {
                    "last_reconciliation_completed_at_utc": self._now().isoformat(),
                    "last_reconciliation_error": f"{type(exc).__name__}: {exc}",
                }
            )
            print(f"[signal-snapshot] reconciliation failed open: {exc!r}", flush=True)
            return {
                "reconciled": False,
                "reason": self._runtime["last_reconciliation_error"],
            }
        captures = list(captures or [])
        results = []
        for capture in captures:
            results.append(await self._project_with_causal_retries(capture))
        counts = {
            "archives_loaded": len(captures),
            "completed_now": sum(
                int(result.get("projected") is True) for result in results
            ),
            "missed_now": sum(
                int(result.get("status") == "MISSED_CAUSAL_WINDOW")
                for result in results
            ),
            "terminal_existing": sum(
                int(
                    result.get("terminal") is True
                    and result.get("status") != "MISSED_CAUSAL_WINDOW"
                    and result.get("projected") is not True
                )
                for result in results
            ),
            "failed_or_busy": sum(
                int(
                    result.get("projected") is not True
                    and result.get("terminal") is not True
                )
                for result in results
            ),
        }
        completed = self._now()
        self._runtime.update(
            {
                "last_reconciliation_completed_at_utc": completed.isoformat(),
                "last_reconciliation_counts": counts,
                "last_reconciliation_error": None,
            }
        )
        return {"reconciled": True, "counts": counts, "results": results}


PROJECTOR = SignalSnapshotProjector()
