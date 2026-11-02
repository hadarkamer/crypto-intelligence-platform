"""One distinct natural-sync window: bounded scalars, never graph traversal.

Existing source hooks route through legacy._OWNER. No old installer is called.
All deltas are observer-perturbed process endpoints, never exclusive owners.
"""
from collections import Counter, OrderedDict
from datetime import datetime, timezone
import functools
import hashlib
import importlib
import inspect
import json
from pathlib import Path
import sys
import threading
import time

import research_process_memory
import research_sheets_delivery_memory_investigation as previous

legacy = previous.legacy
VERSION = "scalar-sync-memory-investigation-v3"
EXPIRES_AT_UTC = "2026-11-02T18:00:00+00:00"
ARM_SECONDS = 90
MAX_DURATION_SECONDS = 900
MAX_RECORDS = 96
MAX_RECORD_BYTES = 8192
MAX_TOTAL_CACHE_BYTES = 600 * 1024
SELECTION_RULE = "FIRST_NATURAL_SYNC_DELIVERY_OR_AUDIT_FUNCTION_ENTRY_AFTER_90S_SCALAR_ONLY_NO_SIZE_SUCCESS_OUTCOME_FILTER_NO_REPLACEMENT"
SCOPE = "SEQUENTIAL_OBSERVER_PERTURBED_PROCESS_ENDPOINTS_AND_CONTAINER_HEADERS_NOT_PEAK_OR_EXCLUSIVE_CAUSAL_OWNER"
TARGET_SPECS = (
    ("watch_scan_measurement", "research_watch_scan_measurement_worker", "run_once"),
    ("watch_scan_formula", "research_watch_scan_formula_worker", "run_once"),
    ("watch_scan_timeframe", "research_watch_scan_formula_timeframe_worker", "run_once"),
    ("formula_catalog", "research_watch_scan_formula_worker", "register_catalog"),
    ("timeframe_catalog", "research_watch_scan_formula_timeframe_worker", "register_catalog"),
    ("prospective_prices", "research_prospective_anchor_worker", "WORKER._price_fetcher"),
    ("prospective_sampling", "research_prospective_anchor_store", "ProspectiveAnchorService.run_once"),
)
SOURCE_NAMES = (
    "main.py", "research_snapshot_sync_worker.py", "research_sheet_outbox.py",
    "research_sheet_reconciliation.py", "google_sheets_sync.py",
    "research_sheets_memory.py", "research_sheets_memory_investigation.py",
    "research_sheets_delivery_memory_investigation.py", "research_process_memory.py",
    "research_scalar_sync_memory_investigation.py",
    "research_watch_scan_measurement_worker.py", "research_watch_scan_formula_worker.py",
    "research_watch_scan_formula_timeframe_worker.py",
    "research_prospective_anchor_worker.py", "research_prospective_anchor_store.py",
)
_OWNER = None
_EXTRA_LABELS = {"TRANSPORT_ENTRY", "TRANSPORT_RETURN", "BACKGROUND_START", "BACKGROUND_END"}
_PAYLOAD_KINDS = frozenset(("research_sheet_upserts", "research_outcomes", "research_formula_results", "snapshot", "snapshots"))


def remaining_seconds(now=None):
    return max(0., min(MAX_DURATION_SECONDS, (datetime.fromisoformat(EXPIRES_AT_UTC)
                     - (now or datetime.now(timezone.utc))).total_seconds()))


def _copy(value, cap=MAX_RECORD_BYTES):
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    size = len(raw.encode("utf-8"))
    if size > cap:
        raise ValueError("SCALAR_RECEIPT_BOUND")
    return json.loads(raw), size


def _emit(value):
    try:
        print("[SCALAR_SYNC_MEMORY_V3] " + json.dumps(value, sort_keys=True,
              separators=(",", ":"), allow_nan=False), flush=True)
    except BaseException:
        pass


def _header(value):
    kind = type(value)
    if kind is bytes:
        return dict(kind="bytes", length=bytes.__len__(value), shallow_bytes=sys.getsizeof(value))
    if kind is dict:
        return dict(kind="dict", length=dict.__len__(value), shallow_bytes=sys.getsizeof(value))
    if kind is list:
        return dict(kind="list", length=list.__len__(value), shallow_bytes=sys.getsizeof(value))
    if kind is Counter:
        return dict(kind="counter", length=dict.__len__(value),
                    shallow_bytes=dict.__sizeof__(value) + sys.getsizeof({}) - dict.__sizeof__({}))
    if kind is OrderedDict:
        return dict(kind="ordered_dict", length=OrderedDict.__len__(value), shallow_bytes=sys.getsizeof(value))
    if value is None:
        return dict(kind="none", length=None, shallow_bytes=sys.getsizeof(None))
    return dict(kind="unsupported", length=None, shallow_bytes=None)


class Investigation(previous.Investigation):
    def __init__(self, worker, *, targets=(), **kwargs):
        kwargs.setdefault("process", lambda: research_process_memory.sample(include_mallinfo2=True))
        super().__init__(worker, **kwargs)
        self.arm = self.start + ARM_SECONDS
        self.state = "WAITING_FIRST_SCALAR_TRANSPORT_ENTRY"
        self.targets = tuple(targets)
        self.background_wrappers = []
        self.background = {label: dict(started=0, completed=0, active=0, revision=0)
                           for label, _, _ in TARGET_SPECS}
        self.installed_labels = set()
        self.missing_labels = set(self.background)
        self.activity_revision = 0
        self.selected_run_transport_entries = 0
        self.phase_calls = Counter()
        self.phase_retained = Counter()
        self.record_attempts = 0
        self.reserved_records = 0
        self.in_flight_records = 0
        self.cache_bytes = 0
        self.dropped_record_cap = 0
        self.dropped_byte_cap = 0
        self.dropped_record_size = 0
        self.diagnostic_failures = 0
        self.warmup_status = "NOT_RUN"
        self.stop_event = threading.Event()
        self.restored = False

    def _window(self):
        try:
            return (self.selected_call is not None and not self.restored
                    and self.state not in ("COMPLETED", "EXPIRED_SCALAR_WINDOW")
                    and self.clock() < self.deadline)
        except BaseException:
            return False

    def _counts(self):
        with self.lock:
            return dict(activity_revision=self.activity_revision,
                        registered={name: dict(row) for name, row in self.background.items()},
                        coverage_complete=False, other_workers_unknown=True,
                        scope="REGISTERED_NATIVE_FUNCTION_BOUNDARIES_NOT_GLOBAL_IDLE_OR_ALLOCATION_OWNERSHIP")

    def _background_boundary(self, label, entering):
        with self.lock:
            row = self.background[label]
            row["started" if entering else "completed"] += 1
            row["active"] += 1 if entering else -1
            row["revision"] += 1
            self.activity_revision += 1
        if self._window():
            self.safe_point("BACKGROUND_START" if entering else "BACKGROUND_END", background_label=label)

    def _attach_background(self):
        for label, owner, attribute in self.targets:
            if label not in self.background or label in self.installed_labels:
                continue
            try:
                original = getattr(owner, attribute)
                if not callable(original) or inspect.iscoroutinefunction(original):
                    continue
                def make_wrapper(fn, static_label):
                    @functools.wraps(fn)
                    def wrapped(*args, **kwargs):
                        entered = False
                        try:
                            self._background_boundary(static_label, True)
                            entered = True
                        except BaseException:
                            self.errors += 1
                        try:
                            return fn(*args, **kwargs)
                        finally:
                            if entered:
                                try:
                                    self._background_boundary(static_label, False)
                                except BaseException:
                                    self.errors += 1
                    return wrapped
                wrapped = make_wrapper(original, label)
                setattr(owner, attribute, wrapped)
                self.background_wrappers.append((owner, attribute, original, wrapped))
                self.installed_labels.add(label)
                self.missing_labels.discard(label)
            except BaseException:
                self.errors += 1

    def attach(self):
        original = self.worker.run_once
        self.original = original
        self.delivery_original = self.google._deliver_envelope
        self.audit_original = self.google.read_telegram_audit_page

        @functools.wraps(original)
        def sync_wrapped(*args, **kwargs):
            try:
                expired = self.clock() >= self.deadline
                if expired:
                    self.expire()
                else:
                    prior = (getattr(self.local, "in_sync", False), getattr(self.local, "run_call", None),
                             getattr(self.local, "selected", False))
                    with self.lock:
                        self.total_calls += 1
                        call = self.total_calls
                    self.local.in_sync, self.local.run_call, self.local.selected = True, call, False
            except BaseException:
                self.errors += 1
                return original(*args, **kwargs)
            if expired:
                return original(*args, **kwargs)
            failed = False
            try:
                return original(*args, **kwargs)
            except BaseException as exc:
                failed = True
                if getattr(self.local, "selected", False):
                    self.exception_type = type(exc).__name__
                raise
            finally:
                try:
                    selected = (getattr(self.local, "selected", False) and self.selected_call == call
                                and threading.get_ident() == self.selected_thread_id)
                    self.local.in_sync, self.local.run_call, self.local.selected = prior
                    if selected:
                        self.safe_point("RETURN", persistent=True)
                        if self.clock() >= self.deadline:
                            self.expire()
                        else:
                            with self.lock:
                                if not self.restored:
                                    self.state = "COMPLETED_EXCEPTION_NO_OUTER_TAILS" if failed else "AWAITING_OUTER_RETURN"
                            if failed:
                                self.restore()
                except BaseException:
                    self.errors += 1

        def transport(fn, kind):
            @functools.wraps(fn)
            def wrapped(*args, **kwargs):
                previous_context = getattr(self.local, "transport", None)
                tracked = False
                try:
                    if getattr(self.local, "in_sync", False):
                        now = self.clock()
                        with self.lock:
                            self.sync_transport_entries += 1
                            if kind == "ENVELOPE_DELIVERY":
                                self.sync_delivery_entries += 1
                            selected = self.selected_call is None and self.arm <= now < self.deadline and not self.restored
                            if selected:
                                self.selected_call = self.local.run_call
                                self.selected_thread_id = threading.get_ident()
                                self.selected_transport_entry = self.sync_transport_entries
                                self.selected_delivery_entry = self.sync_delivery_entries if kind == "ENVELOPE_DELIVERY" else None
                                self.target_kind = kind
                                self.state = "MEASURING_SCALAR_TRANSPORT_WINDOW"
                            tracked = (self.selected_call == self.local.run_call
                                       and self.selected_thread_id == threading.get_ident()
                                       and now < self.deadline and not self.restored)
                            if tracked:
                                self.selected_run_transport_entries += 1
                                context = dict(index=self.selected_run_transport_entries,
                                               process_sync_index=self.sync_transport_entries, kind=kind)
                        if tracked:
                            self.local.selected = True
                            self.local.transport = context
                            roots = {"request_envelope": args[0] if args else kwargs.get("envelope")} if kind == "ENVELOPE_DELIVERY" else None
                            self.safe_point("BEGIN" if selected else "TRANSPORT_ENTRY", roots)
                except BaseException:
                    self.errors += 1
                try:
                    return fn(*args, **kwargs)
                finally:
                    if tracked:
                        self.safe_point("TRANSPORT_RETURN")
                    self.local.transport = previous_context
            return wrapped

        self.wrapper = sync_wrapped
        self.delivery_wrapper = transport(self.delivery_original, "ENVELOPE_DELIVERY")
        self.audit_wrapper = transport(self.audit_original, "AUDIT_PAGE_READ")
        try:
            self.worker.run_once = sync_wrapped
            self.google._deliver_envelope = self.delivery_wrapper
            self.google.read_telegram_audit_page = self.audit_wrapper
            self._attach_background()
            self.spawn(self._expiry_watch)
        except BaseException:
            self.restore()
            raise

    def _scalar_roots(self, roots, persistent):
        result = {}
        if persistent:
            audit = self.worker._sheet_reconciler
            live = dict(audit_expected=audit.expected, audit_seen=audit.seen,
                        audit_unexpected=audit.unexpected,
                        audit_last_complete=audit.runtime.get("last_complete"),
                        snapshot_cache=self.google._SNAPSHOT_CACHE,
                        worker_result=self.worker._runtime.get("last_result"))
        else:
            live = roots if type(roots) is dict else {}
        for name in ("request_bytes", "ack_raw", "request_envelope", "audit_page", "drain_rows",
                     "audit_expected", "audit_seen", "audit_unexpected", "audit_last_complete",
                     "snapshot_cache", "worker_result"):
            if name not in live:
                continue
            value = dict.__getitem__(live, name)
            row = _header(value)
            if name == "request_envelope" and type(value) is dict:
                kind = dict.get(value, "kind")
                row["payload_kind"] = kind if type(kind) is str and kind in _PAYLOAD_KINDS else "OTHER_OR_ABSENT"
                upserts = dict.get(value, "upserts")
                row["upserts_count"] = list.__len__(upserts) if type(upserts) is list else None
            if name == "audit_page" and type(value) is dict:
                rows = dict.get(value, "rows")
                row["rows_count"] = list.__len__(rows) if type(rows) is list else None
                row["top_key_count"] = dict.__len__(value)
            result[name] = row
        return result

    def safe_point(self, label, roots=None, *, persistent=False, background_label=None, forced_status=None):
        try:
            self._point(label, roots, persistent=persistent,
                        background_label=background_label, forced_status=forced_status)
        except BaseException:
            with self.lock:
                self.errors += 1
                self.diagnostic_failures += 1

    def _point(self, label, roots=None, *, persistent=False, background_label=None, forced_status=None):
        if label not in legacy.LABELS and label not in _EXTRA_LABELS:
            raise ValueError("UNDECLARED_SCALAR_LABEL")
        with self.lock:
            self.phase_calls[label] += 1
            self.record_attempts += 1
            attempt = self.record_attempts
            if self.reserved_records >= MAX_RECORDS:
                self.dropped_record_cap += 1
                return
            if self.cache_bytes + MAX_RECORD_BYTES > MAX_TOTAL_CACHE_BYTES:
                self.dropped_byte_cap += 1
                return
            self.reserved_records += 1
            self.in_flight_records += 1
            self.cache_bytes += MAX_RECORD_BYTES
            self.labels.add(label)
            context = getattr(self.local, "transport", None)
            context = dict(context) if context is not None else None
        try:
            began = self.clock()
            record = dict(version=VERSION, session=self.session, runtime_commit=self.runtime,
                          attempt=attempt, label=label, at_utc=self.utc(), selected_call=self.selected_call,
                          target_kind=self.target_kind, transport=context, observer_perturbed=True,
                          selected_thread=threading.get_ident() == self.selected_thread_id,
                          sample_started_at_utc=None, sample_completed_at_utc=None,
                          graph_walk_performed=False, scope=SCOPE)
            if background_label is not None:
                record["background_label"] = background_label
            record["activity_before"] = self._counts()
            if forced_status is not None:
                record["status"] = forced_status
            elif self.clock() >= self.deadline:
                record["status"] = "MISSED_DEADLINE"
            else:
                try:
                    record["sample_started_at_utc"] = self.utc()
                    record["process"] = self.process()
                    record["scalar_roots"] = self._scalar_roots(roots, persistent)
                    record["status"] = "SCALAR_MEASURED"
                except BaseException:
                    record["status"] = "DIAGNOSTIC_FAILED_NO_RETRY"
                    with self.lock:
                        self.diagnostic_failures += 1
                finally:
                    record["sample_completed_at_utc"] = self.utc()
            record["activity_after"] = self._counts()
            record["diagnostic_elapsed_ms"] = round((self.clock() - began) * 1000, 3)
            # Append order is the durable cached prefix; sample clocks describe
            # concurrent read order. Never insert a slow earlier sample ahead
            # of an already published prefix.
            with self.lock:
                record["seq"] = len(self.records) + 1
                bounded, size = _copy(record)
                self.cache_bytes += size - MAX_RECORD_BYTES
                self.records.append(bounded)
                self.phase_retained[label] += 1
                self.in_flight_records -= 1
        except BaseException:
            with self.lock:
                self.cache_bytes -= MAX_RECORD_BYTES
                self.dropped_record_size += 1
                self.in_flight_records -= 1
            return
        _emit(bounded)

    def outer_return(self, worker):
        if worker is not self.worker or self.state != "AWAITING_OUTER_RETURN":
            return
        try:
            self.outer_at = self.clock()
            self.safe_point("POST_AWAIT_0", persistent=True)
            with self.lock:
                if self.restored:
                    return
                self.state = "OBSERVING_POST_RETURN"
            self.spawn(self._tails)
        except BaseException:
            self.tail_start_failed = True
            self.state = "COMPLETED_TAIL_SETUP_FAILED"
            self.restore()

    def tail_tick(self):
        if self.clock() >= self.deadline:
            self.expire()
            return
        if self.state != "OBSERVING_POST_RETURN":
            return
        for offset in (1, 3):
            if offset in self.post_done or self.clock() < self.outer_at + offset:
                continue
            self.post_done.add(offset)
            self.safe_point("POST_AWAIT_" + str(offset), persistent=True,
                            forced_status="MISSED_TIMER_WINDOW" if self.clock() > self.outer_at + offset + .75 else None)
        if self.post_done == {1, 3}:
            with self.lock:
                if not self.restored:
                    self.state = "COMPLETED"
            self.restore()

    def _expiry_watch(self):
        try:
            while not self.stop_event.is_set():
                left = self.deadline - self.clock()
                if left <= 0:
                    self.expire()
                    return
                self.stop_event.wait(min(left, 1.))
        except BaseException:
            self.errors += 1
            self.state = "COMPLETED_EXPIRY_WATCH_FAILED"
            self.restore()

    def expire(self):
        with self.lock:
            if self.restored:
                return
            self.state = "EXPIRED_SCALAR_WINDOW" if self.selected_call is not None else "EXPIRED_NO_SCALAR_TRANSPORT_ENTRY"
        self.restore()

    def restore(self):
        self.restored = True
        self.stop_event.set()
        super().restore()
        for owner, attribute, original, wrapped in self.background_wrappers:
            try:
                if getattr(owner, attribute) is wrapped:
                    setattr(owner, attribute, original)
            except BaseException:
                self.errors += 1

    def status(self):
        with self.lock:
            value = dict(version=VERSION, protocol_version=3, state=self.state,
                         session=self.session, runtime_commit=self.runtime,
                         expires_at_utc=EXPIRES_AT_UTC, selection_rule=SELECTION_RULE,
                         source_code_sha256=self.source_pins, records=list(self.records),
                         selected_call=self.selected_call, selected_transport_entry=self.selected_transport_entry,
                         selected_delivery_entry=self.selected_delivery_entry, target_kind=self.target_kind,
                         total_calls=self.total_calls, sync_transport_entries=self.sync_transport_entries,
                         sync_delivery_entries=self.sync_delivery_entries,
                         selected_run_transport_entries=self.selected_run_transport_entries,
                         phase_calls=dict(self.phase_calls), phase_retained=dict(self.phase_retained),
                         record_attempts=self.record_attempts, reserved_records=self.reserved_records,
                         in_flight_records=self.in_flight_records,
                         window_finalized=self.restored and self.in_flight_records == 0,
                         retained_record_bytes_includes_inflight_reservations=True,
                         retained_record_bytes=self.cache_bytes, dropped_record_cap=self.dropped_record_cap,
                         dropped_byte_cap=self.dropped_byte_cap, dropped_record_size=self.dropped_record_size,
                         diagnostic_failures=self.diagnostic_failures, errors=self.errors,
                         selected_exception_type=self.exception_type,
                         tail_start_failed=self.tail_start_failed, restored=self.restored,
                         background_installed=sorted(self.installed_labels), background_missing=sorted(self.missing_labels),
                         background_counts={name: dict(row) for name, row in self.background.items()},
                         background_counts_as_of_health_read=True,
                         background_work_may_still_be_running=True,
                         window_finalized_scope="RECORD_WRITES_DRAINED_AND_WRAPPERS_RESTORED_NOT_BACKGROUND_BUSINESS_COMPLETION",
                         window_analysis_context="PER_RECORD_COPIED_ACTIVITY_COUNTS_HEALTH_COUNTERS_MAY_ADVANCE_AFTER_RESTORE",
                         activity_revision=self.activity_revision, background_coverage_complete=False,
                         other_workers_unknown=True, preinstallation_calls_unknown=True,
                         background_labels_may_be_nested=True,
                         warmup_status=self.warmup_status, graph_walk_performed=False,
                         all_gauges_observer_perturbed=True, original_experiments_rearmed=False,
                         transport_function_entry_does_not_guarantee_network_admission=True,
                         repeated_source_hooks_retained=True, scope=SCOPE,
                         limits=dict(records=MAX_RECORDS, record_bytes=MAX_RECORD_BYTES,
                                     total_cache_bytes=MAX_TOTAL_CACHE_BYTES, duration_seconds=MAX_DURATION_SECONDS))
        return _copy(value, MAX_TOTAL_CACHE_BYTES + 32768)[0]


def install(*, worker=None, google=None, targets=None, process=None, clock=time.monotonic,
            utc=legacy.utc_now, spawn=None, now=None, pin_paths=None):
    global _OWNER
    if _OWNER is not None:
        return _OWNER.status()
    duration = remaining_seconds(now)
    if duration <= 0:
        return dict(version=VERSION, state="EXPIRED_NO_PROBES", expires_at_utc=EXPIRES_AT_UTC)
    owner = None
    try:
        if google is None:
            google = importlib.import_module("google_sheets_sync")
        if worker is None:
            worker = importlib.import_module("research_snapshot_sync_worker").WORKER
        if targets is None:
            targets = []
            for label, module, attribute in TARGET_SPECS:
                try:
                    # Paths are fixed source constants, never business input.
                    target_owner = importlib.import_module(module)
                    parts = attribute.split(".")
                    for part in parts[:-1]:
                        target_owner = getattr(target_owner, part)
                    targets.append((label, target_owner, parts[-1]))
                except BaseException:
                    pass
        owner = Investigation(worker, google=google, targets=targets, duration=duration,
                              process=process or (lambda: research_process_memory.sample(include_mallinfo2=True)),
                              clock=clock, utc=utc, spawn=spawn)
        # One unretained warmup before attachment/selection resolves native loading.
        try:
            warmed = owner.process()
            quality = warmed.get("status") if type(warmed) is dict else None
            owner.warmup_status = quality if quality in ("COMPLETE", "PARTIAL", "UNAVAILABLE") else "UNAVAILABLE"
            del warmed
        except BaseException:
            owner.warmup_status = "UNAVAILABLE"
        paths = pin_paths if pin_paths is not None else {name: Path(__file__).with_name(name) for name in SOURCE_NAMES}
        owner.source_pins = {name: hashlib.sha256(Path(path).read_bytes()).hexdigest() for name, path in paths.items()}
        owner.attach()
        _OWNER = owner
        legacy._OWNER = owner
        return owner.status()
    except BaseException:
        if owner is not None:
            owner.restore()
        return dict(version=VERSION, state="INSTALL_UNAVAILABLE_NO_PROBES")


def status():
    return _OWNER.status() if _OWNER is not None else dict(version=VERSION, state="NOT_INSTALLED", expires_at_utc=EXPIRES_AT_UTC)
