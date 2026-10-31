"""Offline invariants for passive worker memory observations."""
import asyncio
import gc
from datetime import datetime, timezone
import importlib
import json
from pathlib import Path
import socket
import tempfile
import threading
import unittest
from unittest.mock import patch
import weakref

import research_background_memory as monitor


class ObserverTests(unittest.TestCase):
    def observer(self, **kw):
        return monitor.Observer(measure=lambda: {'rss_bytes': 100, 'python_allocated_blocks': 2,
                                                'gc_counts': [1, 2, 3]}, **kw)

    def events(self, observer):
        result = []
        while not observer.queue.empty():
            result.append(json.loads(observer.queue.get_nowait()))
        return result

    def test_result_identity_exception_and_no_argument_logging(self):
        observer = self.observer()
        secret = object()
        wrapped = observer.wrap('test', lambda *a, **k: secret)
        self.assertIs(wrapped('PASSWORD', recipient='TOKEN'), secret)
        error = RuntimeError('SECRET_DATABASE_URL')
        def failing():
            raise error
        with self.assertRaises(RuntimeError) as caught:
            observer.wrap('failure', failing)()
        self.assertIs(caught.exception, error)
        events = self.events(observer)
        self.assertEqual([e['outcome'] for e in events if e['event'] == 'FINISH'], ['RETURNED', 'RAISED'])
        self.assertNotIn('SECRET', json.dumps(events))
        self.assertNotIn('TOKEN', json.dumps(events))
        self.assertFalse(observer.active)

    def test_diagnostic_baseexception_never_changes_call(self):
        observer = self.observer()
        value = object()
        wrapped = observer.wrap('test', lambda: value)
        with patch.object(observer, 'begin', side_effect=KeyboardInterrupt):
            self.assertIs(wrapped(), value)
        with patch.object(observer, 'end', side_effect=SystemExit):
            self.assertIs(wrapped(), value)
        self.assertEqual(monitor._CURRENT.get(), ())

    def test_async_cancellation_and_result_preserved(self):
        observer = self.observer()
        async def run():
            entered = asyncio.Event()
            async def work():
                entered.set()
                await asyncio.Event().wait()
            task = asyncio.create_task(observer.wrap('async', work)())
            await entered.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertFalse(observer.active)
            async def success():
                return entered
            self.assertIs(await observer.wrap('async', success)(), entered)
        asyncio.run(run())
        self.assertEqual([e['outcome'] for e in self.events(observer) if e['event'] == 'FINISH'],
                         ['CANCELLED', 'RETURNED'])

    def test_thread_overlap_and_recursion(self):
        observer = self.observer()
        gate = threading.Barrier(2)
        def work():
            gate.wait(timeout=3)
            gate.wait(timeout=3)
        thread = threading.Thread(target=observer.wrap('thread', work))
        thread.start()
        gate.wait(timeout=3)
        observer.wrap('other', lambda: None)()
        gate.wait(timeout=3)
        thread.join(timeout=3)
        events = self.events(observer)
        ends = {e['task']: e for e in events if e['event'] == 'FINISH'}
        self.assertEqual(ends['thread']['overlap'], ['other'])
        self.assertEqual(ends['other']['overlap'], ['thread'])
        def recurse(n):
            return wrapped(n - 1) if n else 123
        wrapped = observer.wrap('recursive', recurse)
        self.assertEqual(wrapped(5), 123)
        self.assertEqual(len(self.events(observer)), 2)
        self.assertFalse(observer.active)

    def test_queue_active_and_lifetime_bounds(self):
        clock = [0]
        observer = self.observer(clock=lambda: clock[0], duration=10)
        for _ in range(200):
            observer.wrap('bounded', lambda: None)()
        self.assertEqual(observer.queue.qsize(), monitor.MAX_QUEUE)
        self.assertGreater(observer.dropped, 0)
        self.assertLessEqual(len(observer.recent), monitor.MAX_ACTIVE)
        self.events(observer)
        keys = [observer.begin('test') for _ in range(100)]
        self.assertEqual(len(observer.active), monitor.MAX_ACTIVE)
        for key in keys:
            observer.end(key, 'RETURNED')
        self.events(observer)
        clock[0] = 11
        self.assertIsNone(observer.begin('expired'))
        self.assertFalse(observer.active)

    def test_inputs_and_results_not_retained(self):
        observer = self.observer()
        class Payload:
            pass
        payload = Payload()
        reference = weakref.ref(payload)
        wrapped = observer.wrap('payload', lambda item: item)
        self.assertIs(wrapped(payload), payload)
        del payload
        gc.collect()
        self.assertIsNone(reference())

    def test_absolute_expiry_prevents_future_startup_and_shortens_restart(self):
        with patch.object(monitor, 'datetime') as clock:
            clock.fromisoformat.side_effect = datetime.fromisoformat
            clock.now.return_value = datetime(2026, 10, 31, 17, 5, tzinfo=timezone.utc)
            self.assertEqual(monitor._remaining_seconds(), 600)
            clock.now.return_value = datetime(2026, 10, 31, 18, tzinfo=timezone.utc)
            self.assertEqual(monitor._remaining_seconds(), 0)
            with patch.object(monitor, '_OBSERVER', None), patch.object(monitor, 'Observer') as factory:
                namespace = {}
                monitor.install(namespace)
                factory.assert_not_called()
                self.assertEqual(namespace, {})

    def test_real_installation_has_no_missing_targets_and_is_idempotent(self):
        # No main() call, network, scheduler, DB credentials or external messages.
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(socket.socket, 'connect', side_effect=AssertionError('OFFLINE_ONLY')), \
             patch.dict('os.environ', {'DB_PATH': str(Path(directory) / 'test.sqlite')}), \
             patch.object(monitor.Observer, 'start'), patch.object(monitor, '_OBSERVER', None), \
             patch.object(monitor, '_remaining_seconds', return_value=7200):
            app = importlib.import_module('main')
            originals = []
            for module, path, _ in monitor.TARGETS:
                owner = vars(app)[module]
                parts = path.split('.')
                for part in parts[:-1]:
                    owner = getattr(owner, part)
                originals.append((owner, parts[-1], getattr(owner, parts[-1])))
            originals += [(app, name, getattr(app, name)) for name, _ in monitor.MAIN_TARGETS]
            originals += [(worker, 'tick', worker.tick) for worker in app.sol_proximity_experimental_worker.ADDITIONAL_WORKERS.values()]
            try:
                monitor.install(vars(app))
                observer = monitor._OBSERVER
                events = self.events(observer)
                self.assertEqual(events[0]['event'], 'INSTALLED')
                self.assertEqual(events[0]['missing'], [])
                self.assertGreaterEqual(len(events[0]['tasks']), 35)
                monitor.install(vars(app))
                self.assertIs(monitor._OBSERVER, observer)
                self.assertEqual(self.events(observer), [])
            finally:
                for owner, name, value in originals:
                    setattr(owner, name, value)


if __name__ == '__main__':
    unittest.main()
