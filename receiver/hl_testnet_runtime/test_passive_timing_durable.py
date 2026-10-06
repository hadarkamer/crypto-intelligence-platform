"""Compare actual PostgreSQL controller outcomes with diagnostics on and off.

Only the exchange is a deterministic double. No keys or external network.
The same scenarios compare complete wire requests, public reads, store calls,
durable requests/events, positions and proof projections (including restart).
"""
from collections import Counter
from copy import deepcopy
import os
import threading
import unittest
from unittest.mock import patch

from . import passive_timing as timing
from . import test_long_stream_runtime as fixtures
from . import filled_quantity_dispatch as dispatch
from . import long_stream_runtime as stream
from .filled_dispatch_store import SCHEMA


def capture_scenario(mode='disabled', *, side='LONG', lost_reply=False):
    fixture = fixtures.DurableLongStreamTests()
    recorder = None
    released = threading.Event()
    entered = threading.Event()
    locked = False
    try:
        fixture.setUp()
        if mode != 'disabled':
            def hung_sink(line):
                entered.set()
                released.wait(30)
            recorder = timing.Recorder(capacity=2 if mode == 'full' else 256,
                                       sink=hung_sink if mode == 'hung' else None)
            assert timing.set_recorder(recorder)
            if mode == 'full':
                for _ in range(recorder.capacity):
                    recorder.record('fixture')
            elif mode == 'contended':
                recorder._lock.acquire()
                locked = True
            elif mode == 'hung':
                recorder.record('fixture')
                recorder.start()
                assert entered.wait(2), 'WRITER_DID_NOT_START'
            else:
                recorder.start()
        else:
            assert timing.set_recorder(None)

        role = 'long_account' if side == 'LONG' else 'short_account'
        route = fixtures.ROUTES2[role]
        card = fixture.card(601 if side == 'LONG' else 602, side=side)
        if side == 'SHORT':
            fixture.v.env = {'HL_TESTNET_SHORT_TRIAL_CARD_ID': card['card_id']}
        fixture.c.register(card['card_id'])
        trace = []
        store_calls = Counter()
        for method in ('read', 'lookup', 'sample', 'metadata', 'send'):
            original = getattr(fixture.v, method)
            def traced(*args, _name=method, _fn=original, **kwargs):
                trace.append((_name, deepcopy(args), deepcopy(kwargs)))
                return _fn(*args, **kwargs)
            setattr(fixture.v, method, traced)
        for method in ('load', 'for_account', 'change', 'request', 'prepare_and_begin', 'reply'):
            original = getattr(fixture.store, method)
            def counted(*args, _name=method, _fn=original, **kwargs):
                store_calls[_name] += 1
                return _fn(*args, **kwargs)
            setattr(fixture.store, method, counted)

        results, trades = [], []
        def tick(entries=False):
            results.append(stream.tick(fixture.c, route, fixture.start,
                                       new_entries=entries, role=role))
        def observe():
            trades.append(stream.observed_trades(fixture.c, route, role=role))
        with patch('hl_testnet_runtime.card_sync_evidence.PublicReader', return_value=fixture.v), \
             patch.object(stream.selection, 'page', return_value=([], None)), \
             patch.object(stream, '_account_owned', return_value=True):
            tick(True)
            fixture.v.fill('1000', '40')
            fixture.v.lose_reply = lost_reply
            tick()
            if lost_reply:
                fixture.c = dispatch.Controller(fixture.store, fixture.v, fixtures.ROUTES2,
                                               after_exit_policy=dispatch.AFTER_EXIT)
                tick()
            observe()
            fixture.v.fill('1000', '30')
            tick()
            observe()
            fixture.v.fill('1000', '30')
            tick()
            observe()
            take = next(oid for oid, row in fixture.v.orders.items()
                        if row['status'] == 'open' and
                        row['order']['orderType'] == 'Take Profit Limit')
            fixture.v.fill(take, '100')
            for _ in range(3):
                fixture.v.t += 1
                tick()
            observe()
            fixture.c = dispatch.Controller(fixture.store, fixture.v, fixtures.ROUTES2,
                                           after_exit_policy=dispatch.AFTER_EXIT)
            observe()

        state = fixture.store.for_account(route['account'])
        with fixture.j._transaction() as conn:
            requests = conn.execute(f'SELECT value FROM {SCHEMA}.requests ORDER BY request_id').fetchall()
            events = conn.execute(f'SELECT * FROM {SCHEMA}.events ORDER BY bucket,revision').fetchall()
            nonces = conn.execute(f'SELECT * FROM {SCHEMA}.nonces ORDER BY agent').fetchall()
        diagnostic_health = recorder.health() if recorder else {}
        return dict(trace=trace, store_calls=dict(store_calls), state=state,
                    requests=requests, events=events, nonces=nonces,
                    results=results, trades=trades, sent=fixture.v.sent,
                    reads=fixture.v.calls), diagnostic_health
    finally:
        if locked:
            recorder._lock.release()
        released.set()
        if recorder is not None:
            recorder.stop(timeout=0.25)
        timing.set_recorder(None)
        fixture.doCleanups()


@unittest.skipUnless(os.environ.get('HL_JOURNAL_CI_URL'),
                     'Disposable loopback PostgreSQL required')
class PassiveTimingDurableTests(unittest.TestCase):
    def test_exact_partial_fill_resize_closure_restart_trace_both_accounts(self):
        for side in ('LONG', 'SHORT'):
            baseline, _ = capture_scenario(side=side)
            self.assertEqual(baseline['sent'], 8)
            for quantity, projection in zip(('40', '70', '100'), baseline['trades']):
                self.assertEqual(projection[0]['entry_quantity'], quantity)
                self.assertTrue(projection[0]['protection_verified'])
            self.assertTrue(baseline['trades'][-1][0]['closure_verified'])
            for mode in ('enabled', 'full', 'contended', 'hung'):
                with self.subTest(side=side, mode=mode):
                    actual, health = capture_scenario(mode, side=side)
                    self.assertEqual(actual, baseline)
                    self.assertEqual(health['dropped_invalid'], 0)
                    if mode in ('full', 'contended'):
                        counter = 'dropped_full' if mode == 'full' else 'dropped_busy'
                        self.assertGreater(health[counter], 0)

    def test_unknown_stop_reply_after_restart_never_duplicates_send(self):
        for side in ('LONG', 'SHORT'):
            baseline, _ = capture_scenario(side=side, lost_reply=True)
            actual, health = capture_scenario('enabled', side=side, lost_reply=True)
            self.assertEqual(actual, baseline)
            self.assertEqual(actual['sent'], 8)
            self.assertTrue(actual['trades'][-1][0]['closure_verified'])
            self.assertEqual(health['dropped_invalid'], 0)


if __name__ == '__main__':
    unittest.main()
