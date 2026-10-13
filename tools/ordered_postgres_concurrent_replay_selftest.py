#!/usr/bin/env python3
"""Tiny offline checks that concurrent replay gate failures unblock workers."""
from __future__ import annotations

import os
from pathlib import Path
import sys
import threading
import time
import unittest

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))
from ordered_postgres_concurrent_replay import RoundGate


class RoundGateFailureTests(unittest.TestCase):
    def start_worker(self, gate):
        entered = threading.Event()
        finished = threading.Event()
        released = threading.Event()
        errors = []

        def work():
            entered.set()
            try:
                gate.worker_ready('ordered', {
                    'pid': os.getpid(),
                    'thread_id': threading.get_ident(),
                    'backend_pid': 999,  # Synthetic identity; no database is used.
                })
                released.set()
            except Exception as error:
                errors.append(str(error))
            finally:
                finished.set()

        thread = threading.Thread(target=work, name='gate-selftest', daemon=True)
        thread.start()
        self.addCleanup(self.cleanup_worker, gate, thread)
        self.assertTrue(entered.wait(0.5), 'Worker did not enter the gate')
        deadline = time.monotonic() + 0.5
        while 'ordered' not in gate.snapshot()['ready']:
            self.assertFalse(finished.is_set(), 'Worker exited before registering')
            self.assertLess(time.monotonic(), deadline, 'Worker did not register')
            finished.wait(0.001)
        return thread, released, errors

    @staticmethod
    def cleanup_worker(gate, thread):
        # Safety net only: each test checks automatic exit before cleanup runs.
        gate.abort('self-test cleanup')
        thread.join(0.5)

    def assert_failed_round(self, gate, thread, released, errors):
        next_round_started = threading.Event()
        with self.assertRaises(Exception):
            gate.wait_loaded()
            next_round_started.set()
        thread.join(0.5)
        self.assertFalse(thread.is_alive(), 'Failure left a worker blocked')
        self.assertFalse(released.is_set(), 'Aborted worker resumed successfully')
        self.assertEqual(len(errors), 1, 'Worker did not receive the failure')
        self.assertFalse(next_round_started.is_set(), 'Started a round after failure')
        # Abort is sticky: later waits must fail as well.
        with self.assertRaises(Exception):
            gate.wait_loaded()

    def test_peer_error_unblocks_waiting_worker(self):
        gate = RoundGate(0, gate_timeout=0.2, round_timeout=0.2)
        thread, released, errors = self.start_worker(gate)
        self.assertTrue(thread.is_alive(), 'Worker was not waiting for its peer')
        gate.abort(RuntimeError('injected peer failure'))
        self.assert_failed_round(gate, thread, released, errors)

    def test_timeout_broadcasts_abort_without_cleanup(self):
        gate = RoundGate(0, gate_timeout=0.05, round_timeout=0.2)
        thread, released, errors = self.start_worker(gate)
        self.assert_failed_round(gate, thread, released, errors)


if __name__ == '__main__':
    unittest.main(verbosity=2)
