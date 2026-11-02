"""Distinct v2 case: first natural transport entry on the snapshot sync thread.

The unchanged v1 module supplies source-hook routing and bounded measurement
helpers. This module never installs or rearms the v1 experiment.
"""
from datetime import datetime, timezone
import functools
import hashlib
from pathlib import Path
import sys
import threading
import time

import research_sheets_memory_investigation as legacy

VERSION = "sheets-delivery-memory-investigation-v2"
EXPIRES_AT_UTC = "2026-11-02T16:00:00+00:00"
ARM_SECONDS = 90
MAX_DURATION_SECONDS = 900
SELECTION_RULE = "FIRST_DELIVER_ENVELOPE_OR_AUDIT_PAGE_READ_FUNCTION_ENTRY_ON_NATURAL_SYNC_THREAD_AFTER_90S_NO_SIZE_SUCCESS_OR_OUTCOME_FILTER_NO_REPLACEMENT"
_OWNER = None
_EXCLUDED_GRAPH_ROOTS = frozenset(("audit_expected", "audit_rows"))


def remaining_seconds(now=None):
    return max(0., min(MAX_DURATION_SECONDS, (
        datetime.fromisoformat(EXPIRES_AT_UTC)
        - (now or datetime.now(timezone.utc))).total_seconds()))


def _emit(record):
    try:
        print("[SHEETS_DELIVERY_MEMORY_V2] " + legacy.json.dumps(
            record, sort_keys=True, separators=(",", ":")), flush=True)
    except Exception:
        pass


class Investigation(legacy.Investigation):
    def __init__(self, worker, *, google=None, **kwargs):
        super().__init__(worker, **kwargs)
        self.google = google if google is not None else sys.modules.get("google_sheets_sync")
        self.roots = kwargs.get("roots") or self._small_roots
        self.state = "WAITING_FIRST_ACTUAL_TRANSPORT_ENTRY"
        self.delivery_wrapper = self.delivery_original = None
        self.audit_wrapper = self.audit_original = None
        self.sync_delivery_entries = 0
        self.sync_transport_entries = 0
        self.selected_delivery_entry = None
        self.selected_transport_entry = None
        self.target_kind = None
        self.expected_headers = {}
        self.tail_start_failed = False

    def _small_roots(self, worker):
        audit = worker._sheet_reconciler
        return dict(snapshot_cache=self.google._SNAPSHOT_CACHE,
                    worker_result=worker._runtime.get("last_result"),
                    audit_seen=audit.seen, audit_unexpected=audit.unexpected,
                    audit_last_complete=audit.runtime.get("last_complete"))

    def _expected_header(self):
        try:
            value = self.worker._sheet_reconciler.expected
            if type(value) is dict:
                return dict(status="COMPLETE", kind="dict", entries=dict.__len__(value),
                            shallow_bytes=sys.getsizeof(value), iterated=False,
                            deep_bytes=None)
            if value is None:
                return dict(status="ABSENT", kind="none", entries=None,
                            shallow_bytes=None, iterated=False, deep_bytes=None)
            return dict(status="UNSUPPORTED", kind="unsupported", entries=None,
                        shallow_bytes=None, iterated=False, deep_bytes=None)
        except Exception:
            return dict(status="UNAVAILABLE", entries=None, shallow_bytes=None,
                        iterated=False, deep_bytes=None)

    def attach(self):
        original = self.worker.run_once
        delivery = self.google._deliver_envelope
        audit_read = self.google.read_telegram_audit_page
        self.original, self.delivery_original = original, delivery
        self.audit_original = audit_read

        @functools.wraps(original)
        def sync_wrapped(*args, **kwargs):
            try:
                previous = (getattr(self.local, "in_sync", False),
                            getattr(self.local, "run_call", None),
                            getattr(self.local, "selected", False))
                with self.lock:
                    self.total_calls += 1
                    call = self.total_calls
                    if self.selected_call is None and self.clock() >= self.deadline:
                        self.state = "EXPIRED_NO_ACTUAL_TRANSPORT_ENTRY"
                self.local.in_sync = True
                self.local.run_call = call
                self.local.selected = False
            except Exception:
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
                    selected = (getattr(self.local, "selected", False)
                                and self.selected_call == call
                                and self.selected_thread_id == threading.get_ident())
                    self.local.in_sync, self.local.run_call, self.local.selected = previous
                    if selected:
                        self.finished_at = self.clock()
                        self.safe_point("RETURN", persistent=True)
                        with self.lock:
                            self.state = ("COMPLETED_EXCEPTION_NO_OUTER_TAILS" if failed
                                          else "AWAITING_OUTER_RETURN")
                        if failed:
                            self.restore()
                    elif self.state == "EXPIRED_NO_ACTUAL_TRANSPORT_ENTRY":
                        self.restore()
                except Exception:
                    self.errors += 1

        def transport_wrapper(frozen_original, kind):
            @functools.wraps(frozen_original)
            def transport_wrapped(*args, **kwargs):
                try:
                    if getattr(self.local, "in_sync", False):
                        with self.lock:
                            self.sync_transport_entries += 1
                            if kind == "ENVELOPE_DELIVERY":
                                self.sync_delivery_entries += 1
                            now = self.clock()
                            selected = (self.selected_call is None
                                        and self.arm <= now < self.deadline)
                            if selected:
                                self.selected_call = self.local.run_call
                                self.selected_transport_entry = self.sync_transport_entries
                                self.selected_delivery_entry = (self.sync_delivery_entries
                                                                if kind == "ENVELOPE_DELIVERY" else None)
                                self.target_kind = kind
                                self.selected_thread_id = threading.get_ident()
                                self.state = "MEASURING_ACTUAL_TRANSPORT_ENTRY"
                        if selected:
                            self.local.selected = True
                            if kind == "ENVELOPE_DELIVERY":
                                envelope = args[0] if args else kwargs.get("envelope")
                                self.safe_point("BEGIN", {"request_envelope": envelope})
                            else:
                                self.safe_point("BEGIN", persistent=True)
                except Exception:
                    self.errors += 1
                return frozen_original(*args, **kwargs)
            return transport_wrapped

        self.wrapper = sync_wrapped
        self.delivery_wrapper = transport_wrapper(delivery, "ENVELOPE_DELIVERY")
        self.audit_wrapper = transport_wrapper(audit_read, "AUDIT_PAGE_READ")
        self.worker.run_once = sync_wrapped
        try:
            self.google._deliver_envelope = self.delivery_wrapper
            self.google.read_telegram_audit_page = self.audit_wrapper
        except Exception:
            self.restore()
            raise

    def safe_point(self, label, roots=None, *, persistent=False):
        try:
            self._point(label, roots, persistent=persistent)
        except Exception:
            self.errors += 1
            if label in legacy.LABELS:
                failed = dict(version=VERSION, session=self.session,
                              runtime_commit=self.runtime, label=label,
                              status="DIAGNOSTIC_FAILED_NO_RETRY", observer_perturbed=True)
                with self.lock:
                    if len(self.records) < legacy.MAX_POINTS:
                        self.records.append(failed)
                _emit(failed)

    def _point(self, label, roots=None, *, persistent=False):
        if label not in legacy.LABELS:
            raise ValueError("UNDECLARED_LABEL")
        with self.lock:
            if label in self.labels or len(self.records) >= legacy.MAX_POINTS:
                return
            self.labels.add(label)
        began = self.clock()
        record = dict(version=VERSION, session=self.session, runtime_commit=self.runtime,
                      label=label, at_utc=self.utc(), selected_call=self.selected_call,
                      selected_delivery_entry=self.selected_delivery_entry,
                      selected_transport_entry=self.selected_transport_entry,
                      target_kind=self.target_kind,
                      total_calls=self.total_calls,
                      selected_thread=threading.get_ident() == self.selected_thread_id,
                      persistent_roots=persistent, observer_perturbed=True,
                      scope="SEQUENTIAL_PROCESS_ENDPOINT_AND_SMALL_LOGICAL_ROOTS_NOT_TRUE_PEAK_OR_CAUSAL_OWNER")
        if self.clock() >= self.deadline:
            record["status"] = "MISSED_DEADLINE"
        else:
            record["process_before_graph"] = self.process()
            header = self._expected_header()
            record["audit_expected_header"] = header
            with self.lock:
                self.expected_headers[label] = header
            live = self.roots(self.worker) if persistent else (roots or {})
            try:
                # Shallow fixed-label map only. Do not copy business containers.
                small = {name: value for name, value in live.items()
                         if name not in _EXCLUDED_GRAPH_ROOTS}
                try:
                    if not small:
                        record.update(status="GAUGE_ONLY", graph=None)
                    else:
                        record["admission"] = self.admission()
                        rss = record["process_before_graph"].get("rss_bytes")
                        if (type(rss) is not int or not 0 <= rss <= 1024 * 1024 * 1024
                                or record["admission"].get("admitted") is not True):
                            record.update(status="GRAPH_NOT_ADMITTED_RESOURCE", graph=None)
                        else:
                            record["graph"] = self.measure(small)
                            record["status"] = "MEASURED"
                finally:
                    del small
            finally:
                del live
        record["diagnostic_elapsed_ms"] = round((self.clock() - began) * 1000, 3)
        bounded = legacy._copy(record)
        with self.lock:
            self.records.append(bounded)
        _emit(bounded)

    def outer_return(self, worker):
        try:
            super().outer_return(worker)
        except Exception:
            self.errors += 1
            self.tail_start_failed = True
            self.state = "COMPLETED_TAIL_SETUP_FAILED"
            self.restore()

    def tail_tick(self):
        if self.state != "OBSERVING_POST_RETURN":
            return
        for offset in (1, 3):
            if offset in self.post_done or self.clock() < self.outer_at + offset:
                continue
            self.post_done.add(offset)
            label = "POST_AWAIT_" + str(offset)
            if self.clock() > self.outer_at + offset + .75:
                late = dict(version=VERSION, session=self.session, runtime_commit=self.runtime,
                            label=label, status="MISSED_TIMER_WINDOW", observer_perturbed=True,
                            at_utc=self.utc(), selected_call=self.selected_call,
                            selected_delivery_entry=self.selected_delivery_entry,
                            selected_transport_entry=self.selected_transport_entry,
                            target_kind=self.target_kind,
                            total_calls=self.total_calls)
                with self.lock:
                    self.labels.add(label)
                    if len(self.records) < legacy.MAX_POINTS:
                        self.records.append(late)
                _emit(late)
            else:
                self.safe_point(label, persistent=True)
        if self.post_done == {1, 3}:
            with self.lock:
                self.state = "COMPLETED"
            self.restore()

    def _tails(self):
        try:
            super()._tails()
        except Exception:
            self.errors += 1
            self.state = "COMPLETED_TAIL_THREAD_FAILED"
            self.restore()

    def restore(self):
        if self.worker.run_once is self.wrapper:
            self.worker.run_once = self.original
        if self.google is not None and self.google._deliver_envelope is self.delivery_wrapper:
            self.google._deliver_envelope = self.delivery_original
        if self.google is not None and self.google.read_telegram_audit_page is self.audit_wrapper:
            self.google.read_telegram_audit_page = self.audit_original

    def status(self):
        cached = super().status()
        with self.lock:
            cached.update(version=VERSION, protocol_version=2,
                          selection_rule=SELECTION_RULE,
                          selected_delivery_entry=self.selected_delivery_entry,
                          selected_transport_entry=self.selected_transport_entry,
                          target_kind=self.target_kind,
                          sync_delivery_entries=self.sync_delivery_entries,
                          sync_transport_entries=self.sync_transport_entries,
                          audit_expected_headers=legacy._copy(self.expected_headers),
                          excluded_graph_root_names=sorted(_EXCLUDED_GRAPH_ROOTS),
                          tail_start_failed=self.tail_start_failed,
                          separate_experiment=True,
                          transport_function_entry_does_not_guarantee_network_admission=True,
                          original_first_poll_experiment_rearmed=False)
        return cached


def install():
    global _OWNER
    if _OWNER is not None:
        return _OWNER.status()
    duration = remaining_seconds()
    if duration <= 0:
        return dict(version=VERSION, state="EXPIRED_NO_PROBES", expires_at_utc=EXPIRES_AT_UTC)
    try:
        import google_sheets_sync
        import research_snapshot_sync_worker
        owner = Investigation(research_snapshot_sync_worker.WORKER,
                              google=google_sheets_sync, duration=duration)
        names = ("main.py", "research_snapshot_sync_worker.py", "research_sheet_outbox.py",
                 "research_sheet_reconciliation.py", "google_sheets_sync.py",
                 "research_sheets_memory.py", "research_sheets_memory_investigation.py",
                 "research_sheets_delivery_memory_investigation.py", "research_process_memory.py")
        owner.source_pins = {name: hashlib.sha256(
            Path(__file__).with_name(name).read_bytes()).hexdigest() for name in names}
        owner.attach()
        _OWNER = owner
        # Existing source hook calls route through this field only. No call to
        # legacy.install(), no old observer binding, no old experiment rearm.
        legacy._OWNER = owner
        return owner.status()
    except Exception:
        return dict(version=VERSION, state="INSTALL_UNAVAILABLE_NO_PROBES")


def status():
    return (_OWNER.status() if _OWNER is not None else
            dict(version=VERSION, state="NOT_INSTALLED", expires_at_utc=EXPIRES_AT_UTC))
