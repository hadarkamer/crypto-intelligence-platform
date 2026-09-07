"""Local-only concurrency checks for pre-lease Sheet sender admission."""
from __future__ import annotations

import threading
import unittest
from unittest.mock import patch

import google_sheets_sync as sync


class DeliveryFairnessTests(unittest.TestCase):
    def setUp(self):
        self.failures = []
        self.threads = []
        self.assertFalse(sync._DELIVERY_WAITERS)

    def tearDown(self):
        for thread in self.threads:
            thread.join(3)
            self.assertFalse(thread.is_alive(), "sender did not finish")
        self.assertFalse(self.failures)
        self.assertFalse(sync._DELIVERY_WAITERS, "stale turn would block senders")

    def spawn(self, operation):
        def guarded():
            try:
                operation()
            except BaseException as exc:
                self.failures.append(exc)
        thread = threading.Thread(target=guarded, daemon=True)
        self.threads.append(thread)
        thread.start()
        return thread

    def wait_for_tickets(self, count):
        with sync._DELIVERY_TURN:
            self.assertTrue(sync._DELIVERY_TURN.wait_for(
                lambda: len(sync._DELIVERY_WAITERS) == count, timeout=2
            ))

    def test_queued_outcomes_cannot_be_overtaken_by_snapshot_passes(self):
        order = []

        def sender(name):
            with sync.delivery_slot(wait_seconds=2) as acquired:
                self.assertTrue(acquired)
                # This point represents DB claiming: only after our turn.
                order.append(name)

        with sync.delivery_slot() as acquired:
            self.assertTrue(acquired)
            self.spawn(lambda: sender("outcomes"))
            self.wait_for_tickets(2)
            self.spawn(lambda: sender("snapshots-2"))
            self.wait_for_tickets(3)
            self.spawn(lambda: sender("snapshots-3"))
            self.wait_for_tickets(4)
            self.assertEqual(order, [])
        for thread in self.threads:
            thread.join(2)
        self.assertEqual(order, ["outcomes", "snapshots-2", "snapshots-3"])

    def test_timeout_does_not_claim_and_does_not_leave_stale_ticket(self):
        operations = []
        timed_out = threading.Event()

        def impatient():
            with sync.delivery_slot(wait_seconds=0.05) as acquired:
                if acquired:
                    operations.append("claim")
                else:
                    operations.append("defer")
            timed_out.set()

        def following():
            with sync.delivery_slot(wait_seconds=2) as acquired:
                self.assertTrue(acquired)
                operations.append("following-claim")

        with sync.delivery_slot():
            self.spawn(impatient)
            self.assertTrue(timed_out.wait(2))
            self.assertEqual(operations, ["defer"])
            self.assertEqual(len(sync._DELIVERY_WAITERS), 1)
            self.spawn(following)
            self.wait_for_tickets(2)
        for thread in self.threads:
            thread.join(2)
        self.assertEqual(operations, ["defer", "following-claim"])

    def test_legacy_http_lock_is_still_exclusive_and_precedes_claim(self):
        legacy_entered = threading.Event()
        release_legacy = threading.Event()
        claimed = threading.Event()

        def legacy():
            with sync._DELIVERY_LOCK:
                legacy_entered.set()
                self.assertTrue(release_legacy.wait(2))

        def sender():
            with sync.delivery_slot(wait_seconds=2) as acquired:
                self.assertTrue(acquired)
                claimed.set()

        self.spawn(legacy)
        self.assertTrue(legacy_entered.wait(2))
        self.spawn(sender)
        self.wait_for_tickets(1)
        self.assertFalse(claimed.is_set())
        release_legacy.set()
        self.assertTrue(claimed.wait(2))

    def test_legacy_timeout_and_exception_release_turn(self):
        legacy_entered = threading.Event()
        release_legacy = threading.Event()

        def legacy():
            with sync._DELIVERY_LOCK:
                legacy_entered.set()
                self.assertTrue(release_legacy.wait(2))

        self.spawn(legacy)
        self.assertTrue(legacy_entered.wait(2))
        with sync.delivery_slot(wait_seconds=0.01) as acquired:
            self.assertFalse(acquired)
        self.assertFalse(sync._DELIVERY_WAITERS)
        release_legacy.set()
        for thread in self.threads:
            thread.join(2)
        with self.assertRaisesRegex(RuntimeError, "ack failed"):
            with sync.delivery_slot() as acquired:
                self.assertTrue(acquired)
                raise RuntimeError("ack failed")
        with sync.delivery_slot(wait_seconds=0) as acquired:
            self.assertTrue(acquired)

    def test_nested_delivery_and_actual_http_are_reentrant(self):
        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return b'{"ok": true}'

        with patch.object(sync, "enabled", return_value=True), patch.object(
            sync, "_WEBHOOK_URL", "https://example.invalid/local-only"
        ), patch.object(sync, "urlopen", return_value=Response()) as http:
            with sync.delivery_slot() as acquired:
                self.assertTrue(acquired)
                with sync.delivery_slot(wait_seconds=0) as nested:
                    self.assertTrue(nested)
                    self.assertEqual(len(sync._DELIVERY_WAITERS), 1)
                    self.assertTrue(sync.deliver_now({"kind": "local-selftest"}))
                self.assertTrue(sync._DELIVERY_SLOT_LOCAL.active)
            self.assertFalse(sync._DELIVERY_SLOT_LOCAL.active)
            self.assertEqual(http.call_count, 1)


if __name__ == "__main__":
    unittest.main()
