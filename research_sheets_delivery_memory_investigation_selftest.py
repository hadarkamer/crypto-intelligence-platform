"""Offline fake-worker/fake-delivery tests; all readers/timers are injected."""
import contextlib
import io
import json
import threading
from types import SimpleNamespace
import unittest

import research_sheets_delivery_memory_investigation as m


class Worker:
    def __init__(self):
        self._runtime = {}
        self._sheet_reconciler = SimpleNamespace(expected={"SECRET_EXPECTED": {"value": 7}},
                                                seen={}, unexpected={}, runtime={})
        self.calls = []
        self.body = lambda *args, **kwargs: 17

    def run_once(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.body(*args, **kwargs)


class Tests(unittest.TestCase):
    def setUp(self):
        self.clock = [0.]
        self.worker = Worker()
        self.delivery_calls = []
        self.audit_calls = []
        self.samples = 0
        self.measured_names = []
        self.spawned = []
        self.stdout = io.StringIO()
        self.quiet = contextlib.redirect_stdout(self.stdout)
        self.quiet.__enter__()
        self.old_owner = m.legacy._OWNER
        def delivery(*args, **kwargs):
            self.delivery_calls.append((args, kwargs))
            m.legacy.point("REQUEST_READY", {"request_bytes": b"SECRET_REQUEST"})
            m.legacy.point("ACK_READ", {"ack_raw": b"SECRET_ACK"})
            return False
        def audit_read(*args, **kwargs):
            self.audit_calls.append((args, kwargs))
            m.legacy.point("REQUEST_READY", {"request_bytes": b"SECRET_AUDIT_REQUEST"})
            m.legacy.point("ACK_READ", {"ack_raw": b"SECRET_AUDIT_ACK"})
            return {"rows": []}
        self.google = SimpleNamespace(_deliver_envelope=delivery,
                                      read_telegram_audit_page=audit_read,
                                      _SNAPSHOT_CACHE={})
        self.original_delivery = delivery
        self.original_audit = audit_read
        self.original_worker = self.worker.run_once
        def process():
            self.samples += 1
            return dict(rss_bytes=123456)
        def measure(roots):
            self.measured_names.append(tuple(roots))
            return dict(complete=True, union_bytes=42)
        self.owner = m.Investigation(
            self.worker, google=self.google, clock=lambda: self.clock[0],
            utc=lambda: "2026-11-02T10:00:00+00:00", process=process,
            measure=measure, spawn=self.spawned.append,
            admission=lambda: dict(admitted=True))
        self.owner.attach()
        m.legacy._OWNER = self.owner

    def tearDown(self):
        self.owner.restore()
        m.legacy._OWNER = self.old_owner
        self.quiet.__exit__(None, None, None)

    def run_delivery(self, result=False):
        self.clock[0] = 90.
        self.worker.body = lambda: self.google._deliver_envelope({"SECRET_ENVELOPE": 3}, attempts=1)
        return self.worker.run_once()

    def test_warmup_and_sync_calls_without_delivery_are_noops(self):
        self.worker.run_once()
        self.clock[0] = 89.9
        self.worker.body = lambda: self.google._deliver_envelope({})
        self.worker.run_once()
        self.clock[0] = 90.
        self.worker.body = lambda: 4
        self.worker.run_once()
        self.assertIsNone(self.owner.selected_call)
        self.assertEqual(self.samples, 0)

    def test_selection_occurs_before_original_even_if_false(self):
        events = []
        def delivery(*args, **kwargs):
            events.append(self.owner.records[0]["label"])
            self.assertTrue(m.legacy.active())
            return False
        self.google._deliver_envelope = delivery
        self.owner.restore()
        self.google._deliver_envelope = delivery
        self.owner.attach()
        self.assertFalse(self.run_delivery())
        self.assertEqual(events, ["BEGIN"])
        self.assertEqual(self.owner.selected_call, 1)
        self.assertEqual(self.owner.state, "AWAITING_OUTER_RETURN")

    def test_first_entry_only_and_existing_hooks_after_delivery(self):
        self.clock[0] = 90.
        def body():
            m.legacy.point("DRAIN_BATCH", {"drain_rows": []})
            self.google._deliver_envelope({"which": 1})
            self.google._deliver_envelope({"which": 2})
            m.legacy.point("DRAIN_DONE")
            return 8
        self.worker.body = body
        self.assertEqual(self.worker.run_once(), 8)
        labels = [r["label"] for r in self.owner.records]
        self.assertEqual(labels, ["BEGIN", "REQUEST_READY", "ACK_READ", "DRAIN_DONE", "RETURN"])
        self.assertEqual(self.owner.selected_delivery_entry, 1)
        self.assertEqual(self.owner.sync_delivery_entries, 2)
        self.worker.run_once()
        self.assertEqual(self.owner.selected_call, 1)
        self.assertEqual(len(self.owner.records), 5)

    def test_other_thread_or_direct_sender_never_selects(self):
        self.clock[0] = 90.
        self.google._deliver_envelope({})
        def body():
            thread = threading.Thread(target=lambda: self.google._deliver_envelope({}))
            thread.start()
            thread.join()
            return 1
        self.worker.body = body
        self.worker.run_once()
        self.assertIsNone(self.owner.selected_call)
        self.assertEqual(self.samples, 0)

    def test_preserves_business_args_kwargs_return_and_envelope_identity(self):
        self.clock[0] = 90.
        envelope = {"SECRET": 12}
        self.worker.body = lambda *a, **kw: self.google._deliver_envelope(envelope=envelope, attempts=4)
        self.assertFalse(self.worker.run_once(2, token=3))
        self.assertEqual(self.worker.calls, [((2,), {"token": 3})])
        args, kwargs = self.delivery_calls[0]
        self.assertEqual(args, ())
        self.assertIs(kwargs["envelope"], envelope)
        self.assertEqual(kwargs["attempts"], 4)

    def test_business_exception_propagates_and_restores_both(self):
        self.clock[0] = 90.
        failure = ValueError("SECRET_BUSINESS_ERROR")
        def body():
            self.google._deliver_envelope({})
            raise failure
        self.worker.body = body
        with self.assertRaises(ValueError) as caught:
            self.worker.run_once()
        self.assertIs(caught.exception, failure)
        self.assertEqual(self.owner.state, "COMPLETED_EXCEPTION_NO_OUTER_TAILS")
        self.assertIs(self.google._deliver_envelope, self.original_delivery)
        self.assertEqual(self.worker.run_once, self.original_worker)
        self.assertNotIn("SECRET_BUSINESS_ERROR", json.dumps(self.owner.status()))

    def test_post_await_assignment_and_restore_both(self):
        result = self.run_delivery()
        self.worker._runtime["last_result"] = result
        m.legacy.after_await(self.worker)
        self.assertEqual(len(self.spawned), 1)
        self.clock[0] = 91.
        self.owner.tail_tick()
        self.clock[0] = 93.
        self.owner.tail_tick()
        self.assertEqual(self.owner.state, "COMPLETED")
        self.assertEqual([r["label"] for r in self.owner.records][-3:],
                         ["POST_AWAIT_0", "POST_AWAIT_1", "POST_AWAIT_3"])
        self.assertIs(self.google._deliver_envelope, self.original_delivery)
        self.assertEqual(self.worker.run_once, self.original_worker)
        self.assertTrue(all(r["version"] == m.VERSION for r in self.owner.records))

    def test_expected_is_header_only_all_maps_and_health_is_cached(self):
        self.clock[0] = 90.
        def body():
            self.google._deliver_envelope({})
            m.legacy.point("AUDIT_BEGIN", {"audit_expected": self.worker._sheet_reconciler.expected,
                                          "audit_rows": [{"SECRET_AUDIT_ROW": 3}]})
            return 3
        self.worker.body = body
        self.worker.run_once()
        self.assertFalse(any("audit_expected" in names or "audit_rows" in names
                             for names in self.measured_names))
        count = self.samples
        status = self.owner.status()
        self.owner.status()
        self.assertEqual(self.samples, count)
        self.assertEqual(status["audit_expected_headers"]["BEGIN"]["entries"], 1)
        self.assertNotIn("SECRET", json.dumps(status))
        self.assertNotIn("SECRET", self.stdout.getvalue())

    def test_expiry_without_delivery_restores_both(self):
        self.clock[0] = 901.
        self.worker.run_once()
        self.assertEqual(self.owner.state, "EXPIRED_NO_ACTUAL_TRANSPORT_ENTRY")
        self.assertEqual(self.samples, 0)
        self.assertIs(self.google._deliver_envelope, self.original_delivery)
        self.assertEqual(self.worker.run_once, self.original_worker)

    def test_diagnostic_process_failure_preserves_business(self):
        self.owner.process = lambda: (_ for _ in ()).throw(ValueError("SECRET_PROBE"))
        self.assertFalse(self.run_delivery())
        self.assertTrue(all(r["status"] == "DIAGNOSTIC_FAILED_NO_RETRY" for r in self.owner.records))
        self.assertNotIn("SECRET", json.dumps(self.owner.status()))

    def test_tail_setup_failure_restores_both(self):
        self.run_delivery()
        self.owner.spawn = lambda _: (_ for _ in ()).throw(RuntimeError("thread"))
        m.legacy.after_await(self.worker)
        self.assertEqual(self.owner.state, "COMPLETED_TAIL_SETUP_FAILED")
        self.assertIs(self.google._deliver_envelope, self.original_delivery)
        self.assertEqual(self.worker.run_once, self.original_worker)

    def test_absolute_expiry_is_fixed(self):
        from datetime import datetime, timezone
        self.assertEqual(m.remaining_seconds(datetime(2026, 11, 3, tzinfo=timezone.utc)), 0.)
        self.assertTrue(issubclass(m.Investigation, m.legacy.Investigation))

    def test_audit_read_selects_before_body_and_keeps_following_hooks(self):
        self.clock[0] = 90.
        def body():
            result = self.google.read_telegram_audit_page(start_row=2, last_row=17)
            self.assertEqual(self.owner.records[0]["label"], "BEGIN")
            self.assertEqual(self.owner.target_kind, "AUDIT_PAGE_READ")
            m.legacy.point("AUDIT_PAGE", {"audit_page": result,
                                         "audit_expected": self.worker._sheet_reconciler.expected})
            m.legacy.point("CONSUME_DONE", {"audit_page": result})
            self.google._deliver_envelope({})
            return result
        self.worker.body = body
        self.assertEqual(self.worker.run_once(), {"rows": []})
        self.assertEqual(self.audit_calls, [((), {"start_row": 2, "last_row": 17})])
        self.assertEqual(self.owner.selected_transport_entry, 1)
        self.assertEqual(self.owner.sync_transport_entries, 2)
        self.assertEqual(self.owner.target_kind, "AUDIT_PAGE_READ")
        self.assertIsNone(self.owner.selected_delivery_entry)
        labels = [r["label"] for r in self.owner.records]
        self.assertEqual(labels, ["BEGIN", "REQUEST_READY", "ACK_READ", "AUDIT_PAGE", "CONSUME_DONE", "RETURN"])
        m.legacy.after_await(self.worker)
        self.clock[0] = 91.
        self.owner.tail_tick()
        self.clock[0] = 93.
        self.owner.tail_tick()
        self.assertIs(self.google.read_telegram_audit_page, self.original_audit)
        self.assertIs(self.google._deliver_envelope, self.original_delivery)
        self.assertEqual(self.worker.run_once, self.original_worker)

    def test_audit_calls_outside_sync_or_other_thread_do_not_select(self):
        self.clock[0] = 90.
        self.google.read_telegram_audit_page(start_row=2)
        def body():
            thread = threading.Thread(target=lambda: self.google.read_telegram_audit_page(start_row=2))
            thread.start()
            thread.join()
        self.worker.body = body
        self.worker.run_once()
        self.assertIsNone(self.owner.selected_call)
        self.assertEqual(self.samples, 0)

    def test_expected_header_refuses_subclass_hooks(self):
        class Hostile(dict):
            def __len__(self):
                raise AssertionError("unexpected length hook")
            def __sizeof__(self):
                raise AssertionError("unexpected size hook")
        self.worker._sheet_reconciler.expected = Hostile()
        self.run_delivery()
        header = self.owner.status()["audit_expected_headers"]["BEGIN"]
        self.assertEqual(header["status"], "UNSUPPORTED")

    def test_graph_resource_guard_preserves_all_original_calls(self):
        self.owner.admission = lambda: dict(admitted=False)
        self.assertFalse(self.run_delivery())
        self.assertEqual(self.measured_names, [])
        self.assertEqual(len(self.delivery_calls), 1)
        self.assertTrue(all(r["status"] == "GRAPH_NOT_ADMITTED_RESOURCE"
                            for r in self.owner.records))


if __name__ == "__main__":
    unittest.main()
