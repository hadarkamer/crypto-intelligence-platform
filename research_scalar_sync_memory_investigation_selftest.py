"""Offline fakes only: no native/procfs/network reads or benchmark workloads."""
from collections import Counter
import contextlib
import io
import json
import threading
from types import SimpleNamespace
import unittest

import research_scalar_sync_memory_investigation as m


class Worker:
    def __init__(self):
        self._runtime = {}
        self._sheet_reconciler = SimpleNamespace(expected={"SECRET_EXPECTED": {}},
                                                seen=Counter(), unexpected=Counter(), runtime={})
        self.calls = []
        self.body = lambda *args, **kwargs: 17

    def run_once(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.body(*args, **kwargs)


class Tests(unittest.TestCase):
    def setUp(self):
        self.clock = [0.]
        self.worker = Worker()
        self.spawned = []
        self.samples = 0
        self.stdout = io.StringIO()
        self.quiet = contextlib.redirect_stdout(self.stdout)
        self.quiet.__enter__()
        self.old_owner, self.old_installed = m.legacy._OWNER, m._OWNER
        self.delivery_calls, self.audit_calls = [], []
        def delivery(*args, **kwargs):
            self.delivery_calls.append((args, kwargs))
            m.legacy.point("REQUEST_READY", {"request_bytes": b"SECRET_REQUEST"})
            m.legacy.point("ACK_READ", {"ack_raw": b"SECRET_ACK"})
            return False
        def audit(*args, **kwargs):
            self.audit_calls.append((args, kwargs))
            m.legacy.point("REQUEST_READY", {"request_bytes": b"SECRET_REQUEST"})
            m.legacy.point("ACK_READ", {"ack_raw": b"SECRET_ACK"})
            return {"rows": [{"SECRET_CELL": 3}]}
        self.google = SimpleNamespace(_deliver_envelope=delivery, read_telegram_audit_page=audit,
                                      _SNAPSHOT_CACHE={"SECRET_CACHE": 7})
        self.original_delivery, self.original_audit = delivery, audit
        self.original_worker = self.worker.run_once
        self.background_calls = []
        def background(*args, **kwargs):
            self.background_calls.append((args, kwargs))
            return self.background_body(*args, **kwargs)
        self.background_body = lambda *args, **kwargs: "SECRET_RESULT"
        self.bg = SimpleNamespace(run_once=background)
        self.original_background = background
        def process():
            self.samples += 1
            # Acquiring the state lock here detects accidental sampling under it.
            self.assertTrue(self.owner.lock.acquire(blocking=False))
            self.owner.lock.release()
            return dict(status="COMPLETE", rss_bytes=123456,
                        allocator=dict(status="COMPLETE", fields=dict(uordblks=100, fordblks=200)))
        self.process = process
        self.owner = m.Investigation(
            self.worker, google=self.google, targets=[("watch_scan_formula", self.bg, "run_once")],
            clock=lambda: self.clock[0], utc=lambda: "2026-11-02T10:00:00+00:00",
            process=process, spawn=self.spawned.append,
            measure=lambda _: self.fail("graph traversal invoked"))
        self.owner.attach()
        m.legacy._OWNER = self.owner

    def tearDown(self):
        self.owner.restore()
        m.legacy._OWNER, m._OWNER = self.old_owner, self.old_installed
        self.quiet.__exit__(None, None, None)

    def run_delivery(self):
        self.clock[0] = 90.
        self.worker.body = lambda *a, **kw: self.google._deliver_envelope(
            {"kind": "research_sheet_upserts", "upserts": [{"SECRET_CELL": 2}]}, attempts=1)
        return self.worker.run_once()

    def finish(self):
        m.legacy.after_await(self.worker)
        self.clock[0] = 91.
        self.owner.tail_tick()
        self.clock[0] = 93.
        self.owner.tail_tick()

    def test_warmup_and_nontransport_calls_never_select_or_sample(self):
        self.clock[0] = 89.
        self.worker.body = lambda: self.google._deliver_envelope({})
        self.worker.run_once()
        self.clock[0] = 90.
        self.worker.body = lambda: 17
        self.worker.run_once()
        self.google._deliver_envelope({})
        self.assertIsNone(self.owner.selected_call)
        self.assertEqual(self.samples, 0)

    def test_every_selected_request_ack_and_batch_has_its_transport_index(self):
        self.clock[0] = 90.
        def body():
            m.legacy.point("DRAIN_BATCH", {"drain_rows": []})
            self.google._deliver_envelope({"kind": "research_sheet_upserts", "upserts": [{}]})
            m.legacy.point("DRAIN_BATCH", {"drain_rows": [{}, {}]})
            self.google._deliver_envelope({"kind": "SECRET_KIND", "upserts": []})
            page = self.google.read_telegram_audit_page(start_row=2)
            m.legacy.point("AUDIT_PAGE", {"audit_page": page})
            m.legacy.point("DRAIN_DONE")
            return page
        self.worker.body = body
        self.worker.run_once()
        requests = [r for r in self.owner.records if r["label"] == "REQUEST_READY"]
        acks = [r for r in self.owner.records if r["label"] == "ACK_READ"]
        self.assertEqual([r["transport"]["index"] for r in requests], [1, 2, 3])
        self.assertEqual([r["transport"]["kind"] for r in requests],
                         ["ENVELOPE_DELIVERY", "ENVELOPE_DELIVERY", "AUDIT_PAGE_READ"])
        self.assertEqual(len(acks), 3)
        # The batch preceding selection is explicitly outside this target window.
        self.assertEqual(self.owner.phase_calls["DRAIN_BATCH"], 1)
        self.assertEqual(self.owner.selected_run_transport_entries, 3)
        self.assertEqual(len([r for r in self.owner.records if r["label"] == "TRANSPORT_RETURN"]), 3)
        self.assertEqual([r["seq"] for r in self.owner.records], list(range(1, len(self.owner.records) + 1)))
        count = self.samples
        self.worker.run_once()
        self.assertEqual(self.samples, count)

    def test_exact_scalars_never_leak_values_or_graphs(self):
        self.run_delivery()
        self.finish()
        encoded = json.dumps(self.owner.status()) + self.stdout.getvalue()
        self.assertNotIn("SECRET", encoded)
        first = self.owner.records[0]
        self.assertEqual(first["scalar_roots"]["request_envelope"]["upserts_count"], 1)
        request = next(r for r in self.owner.records if r["label"] == "REQUEST_READY")
        self.assertEqual(request["scalar_roots"]["request_bytes"]["length"], len(b"SECRET_REQUEST"))
        self.assertTrue(all(r["graph_walk_performed"] is False for r in self.owner.records))
        tail = self.owner.records[-1]
        self.assertEqual(tail["scalar_roots"]["audit_expected"]["length"], 1)
        self.assertEqual(tail["version"], m.VERSION)

    def test_other_threads_and_direct_calls_do_not_select(self):
        self.clock[0] = 90.
        self.google.read_telegram_audit_page()
        def body():
            thread = threading.Thread(target=lambda: self.google._deliver_envelope({}))
            thread.start()
            thread.join()
        self.worker.body = body
        self.worker.run_once()
        self.assertIsNone(self.owner.selected_call)
        self.assertEqual(self.samples, 0)

    def test_business_arguments_identity_result_and_exception_are_preserved(self):
        self.clock[0] = 90.
        envelope = {"SECRET": 1}
        self.worker.body = lambda *a, **kw: self.google._deliver_envelope(envelope=envelope, attempts=7)
        self.assertFalse(self.worker.run_once(2, token=3))
        self.assertEqual(self.worker.calls, [((2,), {"token": 3})])
        self.assertIs(self.delivery_calls[0][1]["envelope"], envelope)
        self.assertEqual(self.delivery_calls[0][1]["attempts"], 7)
        failure = RuntimeError("SECRET_FAILURE")
        self.owner.restore()
        self.owner = m.Investigation(self.worker, google=self.google, clock=lambda: self.clock[0],
                                     process=self.process, spawn=self.spawned.append)
        self.owner.attach()
        m.legacy._OWNER = self.owner
        self.clock[0] += 90.
        def body():
            self.google._deliver_envelope({})
            raise failure
        self.worker.body = body
        with self.assertRaises(RuntimeError) as caught:
            self.worker.run_once()
        self.assertIs(caught.exception, failure)
        self.assertEqual(self.owner.state, "COMPLETED_EXCEPTION_NO_OUTER_TAILS")
        self.assertTrue(self.owner.restored)

    def test_background_counters_precede_window_but_endpoints_do_not(self):
        self.assertEqual(self.bg.run_once(1, a=2), "SECRET_RESULT")
        self.assertEqual(self.samples, 0)
        self.assertEqual(self.owner.background["watch_scan_formula"]["revision"], 2)
        self.clock[0] = 90.
        self.worker.body = lambda: (self.google._deliver_envelope({}), self.bg.run_once(5, a=6))
        self.worker.run_once()
        labels = [r["label"] for r in self.owner.records]
        self.assertEqual(labels.count("BACKGROUND_START"), 1)
        self.assertEqual(labels.count("BACKGROUND_END"), 1)
        row = self.owner.background["watch_scan_formula"]
        self.assertEqual(row, dict(started=2, completed=2, active=0, revision=4))
        self.assertEqual(self.background_calls, [((1,), {"a": 2}), ((5,), {"a": 6})])
        self.assertFalse(self.owner.status()["background_coverage_complete"])

    def test_background_exceptions_propagate_exactly_once(self):
        failure = ValueError("SECRET_BACKGROUND")
        self.background_body = lambda *a, **kw: (_ for _ in ()).throw(failure)
        with self.assertRaises(ValueError) as caught:
            self.bg.run_once(9)
        self.assertIs(caught.exception, failure)
        self.assertEqual(len(self.background_calls), 1)
        self.assertEqual(self.owner.background["watch_scan_formula"]["active"], 0)
        self.assertEqual(self.samples, 0)

    def test_cross_thread_sampling_occurs_outside_lock_and_counts_overlap(self):
        self.clock[0] = 90.
        entered, release = threading.Event(), threading.Event()
        self.background_body = lambda *a, **kw: (entered.set(), release.wait(2))
        def body():
            self.google._deliver_envelope({})
            thread = threading.Thread(target=self.bg.run_once)
            thread.start()
            self.assertTrue(entered.wait(2))
            m.legacy.point("DRAIN_DONE")
            release.set()
            thread.join(2)
            self.assertFalse(thread.is_alive())
        self.worker.body = body
        self.worker.run_once()
        point = next(r for r in self.owner.records if r["label"] == "DRAIN_DONE")
        self.assertEqual(point["activity_before"]["registered"]["watch_scan_formula"]["active"], 1)
        self.assertEqual(self.owner.background["watch_scan_formula"]["active"], 0)

    def test_restore_all_wrappers_after_tails_or_expiry(self):
        self.run_delivery()
        self.finish()
        self.assertEqual(self.owner.state, "COMPLETED")
        self.assertEqual([r["label"] for r in self.owner.records][-3:],
                         ["POST_AWAIT_0", "POST_AWAIT_1", "POST_AWAIT_3"])
        self.assertIs(self.bg.run_once, self.original_background)
        self.assertIs(self.google._deliver_envelope, self.original_delivery)
        self.assertIs(self.google.read_telegram_audit_page, self.original_audit)
        self.assertEqual(self.worker.run_once, self.original_worker)

    def test_expiry_without_business_activity_restores_without_sampling(self):
        self.clock[0] = 901.
        self.owner._expiry_watch()
        self.assertTrue(self.owner.restored)
        self.assertEqual(self.samples, 0)
        self.assertIs(self.bg.run_once, self.original_background)
        self.assertEqual(self.worker.run_once, self.original_worker)

    def test_process_faults_and_oversized_receipts_never_retry_business(self):
        self.owner.process = lambda: (_ for _ in ()).throw(RuntimeError("SECRET_PROBE"))
        self.assertFalse(self.run_delivery())
        self.assertEqual(len(self.delivery_calls), 1)
        self.assertTrue(all(r["status"] == "DIAGNOSTIC_FAILED_NO_RETRY" for r in self.owner.records))
        self.owner.process = lambda: {"large": "x" * (m.MAX_RECORD_BYTES + 1)}
        self.owner.safe_point("DRAIN_DONE")
        self.assertEqual(self.owner.dropped_record_size, 1)
        self.assertNotIn("SECRET_PROBE", json.dumps(self.owner.status()))

    def test_every_label_counts_after_caps_and_status_is_cached(self):
        self.run_delivery()
        for _ in range(m.MAX_RECORDS + 10):
            self.owner.safe_point("DRAIN_BATCH", {"drain_rows": []})
        status = self.owner.status()
        self.assertEqual(len(status["records"]), m.MAX_RECORDS)
        self.assertGreater(status["dropped_record_cap"], 0)
        self.assertEqual(status["phase_calls"]["DRAIN_BATCH"], m.MAX_RECORDS + 10)
        self.assertLessEqual(status["retained_record_bytes"], m.MAX_TOTAL_CACHE_BYTES)
        samples = self.samples
        self.owner.status()
        self.assertEqual(samples, self.samples)
        self.assertTrue(all(len(json.dumps(r, separators=(",", ":")).encode()) <= m.MAX_RECORD_BYTES
                            for r in status["records"]))

    def test_subclass_hooks_are_never_called_by_scalar_headers(self):
        class Hostile(dict):
            def __len__(self):
                raise AssertionError("length hook")
            def __sizeof__(self):
                raise AssertionError("size hook")
        self.worker._sheet_reconciler.expected = Hostile()
        self.run_delivery()
        self.finish()
        self.assertEqual(self.owner.records[-1]["scalar_roots"]["audit_expected"]["kind"], "unsupported")

    def test_audit_can_be_original_target_and_timers_have_v3_receipts(self):
        self.clock[0] = 90.
        self.worker.body = lambda: self.google.read_telegram_audit_page(start_row=2)
        self.worker.run_once()
        self.assertEqual(self.owner.target_kind, "AUDIT_PAGE_READ")
        self.assertIsNone(self.owner.selected_delivery_entry)
        m.legacy.after_await(self.worker)
        self.clock[0] = 94.
        self.owner.tail_tick()
        self.assertEqual(self.owner.state, "COMPLETED")
        self.assertEqual(self.owner.records[-1]["status"], "MISSED_TIMER_WINDOW")
        self.assertTrue(all(r["version"] == m.VERSION for r in self.owner.records))

    def test_install_warms_once_and_never_calls_old_installers(self):
        self.owner.restore()
        m._OWNER = None
        calls = []
        def process():
            calls.append(1)
            return {"status": "COMPLETE", "SECRET_WARMUP": 2}
        old_v1, old_v2 = m.legacy.install, m.previous.install
        m.legacy.install = m.previous.install = lambda: self.fail("old installer called")
        try:
            from datetime import datetime, timezone
            result = m.install(worker=self.worker, google=self.google, targets=[], process=process,
                               clock=lambda: self.clock[0], spawn=self.spawned.append,
                               now=datetime(2026, 11, 2, 10, tzinfo=timezone.utc), pin_paths={})
            self.assertEqual(calls, [1])
            self.assertEqual(result["warmup_status"], "COMPLETE")
            self.assertNotIn("SECRET", json.dumps(result))
            self.assertIs(m.legacy._OWNER, m._OWNER)
            m._OWNER.restore()
        finally:
            m.legacy.install, m.previous.install = old_v1, old_v2

    def test_absolute_expiry_and_duration_cap(self):
        from datetime import datetime, timezone
        self.assertEqual(m.remaining_seconds(datetime(2026, 11, 2, 18, tzinfo=timezone.utc)), 0.)
        self.assertEqual(m.remaining_seconds(datetime(2026, 11, 2, 17, tzinfo=timezone.utc)), 900)

    def test_out_of_order_sample_completion_preserves_append_only_prefix(self):
        self.run_delivery()
        blocked, release = threading.Event(), threading.Event()
        slow_thread = [None]
        def process():
            if threading.get_ident() == slow_thread[0]:
                blocked.set()
                self.assertTrue(release.wait(2))
            return dict(status="COMPLETE", rss_bytes=123)
        self.owner.process = process
        def slow():
            slow_thread[0] = threading.get_ident()
            self.owner.safe_point("DRAIN_DONE")
        thread = threading.Thread(target=slow)
        thread.start()
        self.assertTrue(blocked.wait(2))
        self.owner.safe_point("AUDIT_DONE")
        prefix = self.owner.status()["records"]
        release.set()
        thread.join(2)
        self.assertFalse(thread.is_alive())
        final = self.owner.status()["records"]
        self.assertEqual(final[:len(prefix)], prefix)
        self.assertEqual([r["label"] for r in final[-2:]], ["AUDIT_DONE", "DRAIN_DONE"])
        self.assertEqual([r["seq"] for r in final], list(range(1, len(final) + 1)))
        self.assertGreater(final[-2]["attempt"], final[-1]["attempt"])

    def test_expired_or_broken_clock_business_exception_is_never_retried(self):
        failure = ValueError("SECRET_ONCE")
        def body():
            raise failure
        self.worker.body = body
        self.clock[0] = 901.
        with self.assertRaises(ValueError) as caught:
            self.worker.run_once()
        self.assertIs(caught.exception, failure)
        self.assertEqual(len(self.worker.calls), 1)
        # Call a separately captured wrapper after a diagnostic clock failure.
        self.owner.restored = False
        self.owner.clock = lambda: (_ for _ in ()).throw(RuntimeError("clock"))
        with self.assertRaises(ValueError) as caught:
            self.owner.wrapper()
        self.assertIs(caught.exception, failure)
        self.assertEqual(len(self.worker.calls), 2)

    def test_prospective_class_unbound_method_and_instance_callable_preserve_binding(self):
        self.owner.restore()
        calls = []
        answer = object()
        failure = RuntimeError("SECRET_SAMPLING")
        class Service:
            def run_once(service_self, *args, **kwargs):
                calls.append((service_self, args, kwargs))
                if kwargs.get("fail"):
                    raise failure
                return answer
        def fetcher(*args, **kwargs):
            calls.append(("fetch", args, kwargs))
            return answer
        prospective_worker = SimpleNamespace(_price_fetcher=fetcher)
        original_method = Service.__dict__["run_once"]
        service = Service()
        self.owner = m.Investigation(
            self.worker, google=self.google, clock=lambda: self.clock[0], process=self.process,
            spawn=self.spawned.append,
            targets=[("prospective_prices", prospective_worker, "_price_fetcher"),
                     ("prospective_sampling", Service, "run_once")])
        self.owner.attach()
        m.legacy._OWNER = self.owner
        self.assertIs(service.run_once(1, key=answer), answer)
        self.assertIs(Service.run_once(service, 2), answer)
        self.assertIs(prospective_worker._price_fetcher(3, key=answer), answer)
        with self.assertRaises(RuntimeError) as caught:
            service.run_once(fail=True)
        self.assertIs(caught.exception, failure)
        self.assertEqual(calls[0], (service, (1,), {"key": answer}))
        self.assertEqual(calls[1], (service, (2,), {}))
        self.assertEqual(calls[2], ("fetch", (3,), {"key": answer}))
        self.assertEqual(len(calls), 4)
        self.assertEqual(self.owner.background["prospective_sampling"],
                         dict(started=3, completed=3, active=0, revision=6))
        self.owner.restore()
        self.assertIs(Service.__dict__["run_once"], original_method)
        self.assertIs(prospective_worker._price_fetcher, fetcher)
        self.assertEqual(self.samples, 0)

    def test_default_seven_fixed_targets_resolve_dotted_owners_without_async_worker(self):
        self.owner.restore()
        m._OWNER = None
        async def async_worker():
            return 5
        def sync(*args, **kwargs):
            return 8
        class Service:
            def run_once(self, *args, **kwargs):
                return 9
        prospective_worker = SimpleNamespace(_price_fetcher=sync, run_once=async_worker)
        fake_modules = {
            "research_watch_scan_measurement_worker": SimpleNamespace(run_once=sync),
            "research_watch_scan_formula_worker": SimpleNamespace(run_once=sync, register_catalog=sync),
            "research_watch_scan_formula_timeframe_worker": SimpleNamespace(run_once=sync, register_catalog=sync),
            "research_prospective_anchor_worker": SimpleNamespace(WORKER=prospective_worker),
            "research_prospective_anchor_store": SimpleNamespace(ProspectiveAnchorService=Service),
        }
        old_import = m.importlib.import_module
        m.importlib.import_module = fake_modules.__getitem__
        try:
            from datetime import datetime, timezone
            result = m.install(worker=self.worker, google=self.google,
                               process=lambda: {"status": "COMPLETE"},
                               clock=lambda: self.clock[0], spawn=self.spawned.append,
                               now=datetime(2026, 11, 2, 10, tzinfo=timezone.utc), pin_paths={})
            self.assertEqual(len(result["background_installed"]), 7)
            self.assertEqual(result["background_missing"], [])
            self.assertIs(prospective_worker.run_once, async_worker)
            self.assertEqual(Service().run_once(), 9)
            self.assertEqual(prospective_worker._price_fetcher(), 8)
            self.assertTrue(result["background_counts_as_of_health_read"])
            self.assertTrue(result["background_work_may_still_be_running"])
            self.assertEqual(len(m.SOURCE_NAMES), 15)
            m._OWNER.restore()
            self.assertIs(prospective_worker._price_fetcher, sync)
            self.assertIs(prospective_worker.run_once, async_worker)
        finally:
            if m._OWNER is not None:
                m._OWNER.restore()
            m.importlib.import_module = old_import

    def test_async_callable_is_refused_by_sync_background_wrapper(self):
        self.owner.restore()
        async def async_business():
            return 4
        owner = SimpleNamespace(run_once=async_business)
        self.owner = m.Investigation(
            self.worker, google=self.google, clock=lambda: self.clock[0],
            process=self.process, spawn=self.spawned.append,
            targets=[("prospective_sampling", owner, "run_once")])
        self.owner.attach()
        self.assertIs(owner.run_once, async_business)
        self.assertIn("prospective_sampling", self.owner.missing_labels)


if __name__ == "__main__":
    unittest.main()
