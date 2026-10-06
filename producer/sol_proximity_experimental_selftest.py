"""Offline causal/lifecycle/persistence/notification fixtures; no market or bot calls."""
from copy import deepcopy
import asyncio
import unittest
from unittest.mock import patch, AsyncMock
from types import SimpleNamespace

import sol_proximity_experimental_signal as s
import sol_proximity_experimental_store as store
import sol_proximity_experimental_worker as worker
from hype_row71205_experimental_store_selftest import MemoryDatabase

M = s.MINUTE
BASE = 1791111600000//M*M


def bundle(now, target=101., *, score=25., source=100., complete=True, cycle=None):
    rows, slots = [], []
    for tf in s.TIMEFRAMES if complete else s.TIMEFRAMES[:1]:
        rows.append(dict(timeframe=tf, current_price=source, short_max_pain=target, long_max_pain=source*.9,
                         source_observed_at_utc=worker.dt(now).isoformat(), price_fetched_at_utc=worker.dt(now).isoformat()))
        for side, t in [('SHORT', target), ('LONG', source*.9)]:
            slots.append(dict(timeframe=tf, source_side=side, target_price=t, status='SCORED', score=70.,
                              components={'target_proximity': score if side == 'SHORT' else 0.}))
    return dict(cycle_id=cycle or str(now), computed_at_utc=worker.dt(now).isoformat(),
                coins={'SOL': {'maxpain': slots, 'sources': {'maxpain_operational_rows': rows}}})


def bar(t, o=100, h=100.1, l=99.9, c=100):
    return [t, o, h, l, c]


def ingest(state, now, target, **kw):
    return s.ingest(state, s.decode_bundle(bundle(now, target, **kw), now), now, [bar(now//M*M)])


def boot():
    state = s.initial(BASE+10_000)
    ingest(state, BASE+10_000, 101.)
    s.advance(state, [bar(BASE)], BASE+M+10_000)
    return state


def pending(target=102.):
    state = boot()
    ingest(state, BASE+M+10_000, target)
    return state


class PureTests(unittest.TestCase):
    def test_live_timeframes_strict_rounded_score(self):
        for score, count in ((15, 0), (15.004, 0), (15.01, 1), (25, 1)):
            state = boot()
            ingest(state, BASE+M+10_000, 102., score=score)
            self.assertEqual(len(state['active']), count)
        self.assertIn('48h', s.TIMEFRAMES)

    def test_source_slot_integrity_and_future(self):
        b = bundle(BASE+M+10_000, 102.)
        b['coins']['SOL']['maxpain'][0]['target_price'] = 999
        d = s.decode_bundle(b, BASE+M+10_000)
        self.assertFalse(d['rows'][0]['eligible'])
        with self.assertRaises(ValueError):
            s.decode_bundle(b, BASE)
        b['coins']['SOL']['maxpain'].append(deepcopy(b['coins']['SOL']['maxpain'][0]))
        with self.assertRaises(ValueError):
            s.decode_bundle(b, BASE+M+10_000)

    def test_older_second_timeframe_guard_precedes_all_admission(self):
        state = boot()
        now = BASE+M+10_000
        b = bundle(now, 102.)
        row = b['coins']['SOL']['sources']['maxpain_operational_rows'][1]
        row['source_observed_at_utc'] = worker.dt(BASE+10_000).isoformat()
        d = s.decode_bundle(b, now)
        s.ingest(state, d, now, [bar(BASE, 100, 103, 99, 100), bar(BASE+M)])
        self.assertTrue(state['episodes']['102']['consumed'])
        self.assertEqual(state['active'], [])

    def test_wrong_side_does_not_reverse_initial_episode_direction(self):
        state = boot()
        now = BASE+M+10_000
        b = bundle(now, 102.)
        b['coins']['SOL']['sources']['maxpain_operational_rows'][1]['long_max_pain'] = 102.
        d = s.decode_bundle(b, now)
        s.ingest(state, d, now, [bar(BASE+M)])
        self.assertEqual(state['episodes']['102']['directions'], [1])
        self.assertFalse(state['episodes']['102']['consumed'])
        self.assertEqual(len(state['active']), 1)

    def test_rollover_guard_fails_closed(self):
        state = boot()
        now = BASE+M+10_000
        d = s.decode_bundle(bundle(now, 102.), now)
        with self.assertRaises(ValueError):
            s.ingest(state, d, now, [bar(BASE)])
        self.assertEqual(state['active'], [])

    def test_geometry_both_directions_and_deadline(self):
        long, short = s.levels(100, 102), s.levels(100, 98)
        self.assertEqual([long[k] for k in ('entry_price', 'stop_price', 'take_price')], [96, 90, 102])
        self.assertEqual([short[k] for k in ('entry_price', 'stop_price', 'take_price')], [104, 110, 98])
        p = pending()['active'][0]
        self.assertEqual(p['arm_ms'], BASE+2*M)
        self.assertEqual(p['expires_ms'], p['arm_ms']+24*60*M)

    def test_bootstrap_partial_does_not_prove_absence(self):
        state = s.initial(BASE+10_000)
        ingest(state, BASE+10_000, 101., complete=False)
        self.assertFalse(state['initialized_universe'])
        s.advance(state, [bar(BASE)], BASE+M+10_000)
        ingest(state, BASE+M+10_000, 102., complete=False)
        self.assertEqual(state['active'], [])
        s.advance(state, [bar(BASE+M)], BASE+2*M+10_000)
        ingest(state, BASE+2*M+10_000, 103.)
        self.assertEqual(state['active'], [])
        s.advance(state, [bar(BASE+2*M)], BASE+3*M+10_000)
        ingest(state, BASE+3*M+10_000, 104.)
        self.assertEqual(len(state['active']), 1)

    def test_all_timeframes_same_price_single_plan(self):
        state = pending()
        self.assertEqual(len(state['active']), 1)
        self.assertEqual(state['counts']['PENDING_CREATED'], 1)

    def test_near_targets_include_boundary_and_both_directions(self):
        self.assertTrue(s.near(100, 100.2))
        self.assertFalse(s.near(100, 100.200001))
        state = pending()
        s.advance(state, [bar(BASE+M)], BASE+2*M+10_000)
        ingest(state, BASE+2*M+10_000, 102.1)
        self.assertEqual(len(state['active']), 1)
        s.advance(state, [bar(BASE+2*M)], BASE+3*M+10_000)
        ingest(state, BASE+3*M+10_000, 103)
        self.assertEqual(len(state['active']), 2)

    def test_partial_absence_keeps_consumed_and_complete_absence_renews(self):
        state = boot()
        # First target is already cold-start consumed; partial absence cannot clear it.
        ingest(state, BASE+M+10_000, 102., complete=False)
        self.assertIn('101', state['episodes'])
        s.advance(state, [bar(BASE+M)], BASE+2*M+10_000)
        ingest(state, BASE+2*M+10_000, 101.)
        self.assertFalse(any(p['target_price'] == 101 for p in state['active']))
        s.advance(state, [bar(BASE+2*M)], BASE+3*M+10_000)
        ingest(state, BASE+3*M+10_000, 103.)
        s.advance(state, [bar(BASE+3*M)], BASE+4*M+10_000)
        ingest(state, BASE+4*M+10_000, 101.)
        self.assertTrue(any(p['target_price'] == 101 for p in state['active']))

    def test_source_guard_even_ineligible_and_gap_beyond_target(self):
        state = boot()
        d = s.decode_bundle(bundle(BASE+M+10_000, 102, score=0), BASE+M+10_000)
        s.ingest(state, d, BASE+M+10_000, [bar(BASE+M, 103, 104, 103, 104)])
        self.assertTrue(state['episodes']['102']['consumed'])
        s.advance(state, [bar(BASE+M)], BASE+2*M+10_000)
        ingest(state, BASE+2*M+10_000, 102.)
        self.assertEqual(state['active'], [])

    def test_target_before_entry_consumed_gap(self):
        state = pending()
        s.advance(state, [bar(BASE+M), bar(BASE+2*M, 103, 104, 103, 104)], BASE+3*M)
        self.assertEqual(state['active'], [])
        self.assertTrue(state['episodes']['102']['consumed'])

    def test_live_target_guard_waits_closed_bar_without_permanent_block(self):
        state = pending()
        now = BASE+2*M+10_000
        s.advance(state, [bar(BASE+M)], now)
        d = s.decode_bundle(bundle(now, 102.), now)
        s.ingest(state, d, now, [bar(BASE+2*M, 100, 103, 99, 102)])
        self.assertEqual(state['active'][0]['status'], 'PENDING')
        self.assertTrue(state['episodes']['102']['consumed'])
        s.advance(state, [bar(BASE+2*M, 100, 103, 99, 102)], BASE+3*M)
        self.assertEqual(state['active'], [])
        self.assertEqual(state['history'][-1]['status'], 'TARGET_BEFORE_ENTRY')

    def test_ambiguous_entry_retains_capacity(self):
        state = pending()
        s.advance(state, [bar(BASE+M), bar(BASE+2*M, 100, 102, 95, 101)], BASE+3*M)
        self.assertEqual(state['active'][0]['status'], 'UNKNOWN')
        self.assertEqual(state['intents'], [])

    def test_fill_then_stop_permanent_consumption(self):
        state = pending()
        s.advance(state, [bar(BASE+M), bar(BASE+2*M, 100, 100, 95, 96)], BASE+3*M)
        self.assertEqual(state['active'][0]['status'], 'OPEN')
        self.assertEqual(state['active'][0]['fill_price'], 96)
        self.assertEqual(len(state['intents']), 1)
        s.advance(state, [bar(BASE+3*M, 96, 97, 89, 91)], BASE+4*M)
        self.assertEqual(state['active'], [])
        self.assertTrue(state['episodes']['102']['consumed'])
        ingest(state, BASE+4*M, 102.)
        self.assertEqual(state['active'], [])

    def test_ambiguous_barrier_and_no_outage_alert(self):
        state = pending()
        s.advance(state, [bar(BASE+M), bar(BASE+2*M, 95, 103, 89, 99)], BASE+30*M)
        self.assertEqual(state['active'][0]['status'], 'UNKNOWN')
        self.assertEqual(state['intents'], [])

    def test_no_gap_no_future_minute(self):
        state = pending()
        with self.assertRaises(ValueError):
            s.advance(state, [bar(BASE+2*M)], BASE+3*M)
        with self.assertRaises(ValueError):
            s.advance(state, [bar(BASE+M)], BASE+M+1)

    def test_expiry_allows_rearm_next_observation(self):
        state = pending()
        deadline = state['active'][0]['expires_ms']
        bars = [bar(t) for t in range(BASE+M, deadline+M, M)]
        s.advance(state, bars, deadline+M)
        self.assertEqual(state['active'], [])
        self.assertFalse(state['episodes']['102']['consumed'])
        ingest(state, deadline+M, 102.)
        self.assertEqual(len(state['active']), 1)

    def test_disappearance_does_not_cancel_active_and_same_return_blocked(self):
        state = pending()
        old_id = state['active'][0]['position_id']
        s.advance(state, [bar(BASE+M)], BASE+2*M+10_000)
        ingest(state, BASE+2*M+10_000, 104.)
        self.assertTrue(any(p['position_id'] == old_id for p in state['active']))
        s.advance(state, [bar(BASE+2*M)], BASE+3*M+10_000)
        ingest(state, BASE+3*M+10_000, 102.)
        self.assertEqual(sum(p['target_price'] == 102 for p in state['active']), 1)


class PersistenceTests(unittest.TestCase):
    def setUp(self):
        self.db = MemoryDatabase()
        self.patcher = patch.object(store, '_connect', self.db.connect)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)
        self.kw = dict(config_sha256='frozen')
        store.initialize_scope('chat', BASE+10_000, **self.kw)

    def test_restart_claim_unknown_no_retry(self):
        state = pending()
        s.advance(state, [bar(BASE+M), bar(BASE+2*M, 100, 100, 95, 96)], BASE+3*M)
        store.transact('chat', BASE+3*M, 'frozen', lambda st: st.update(deepcopy(state)))
        first = store.claim_pending('chat', BASE+3*M, **self.kw)
        self.assertIsNotNone(first)
        self.assertIsNone(store.claim_pending('chat', BASE+3*M, **self.kw))
        store.initialize_scope('chat', BASE+6*M, **self.kw)
        read = store.snapshot('chat')
        self.assertEqual(read['intents'][0]['status'], 'UNKNOWN')
        self.assertEqual(read['active'][0]['status'], 'OPEN')
        self.assertIsNone(store.claim_pending('chat', BASE+6*M, **self.kw))

    def test_transaction_rollback_and_config_fence(self):
        before = store.snapshot('chat')
        def fail(st):
            st['active'].append({'bad': True})
            raise ValueError('abort')
        with self.assertRaises(ValueError):
            store.transact('chat', BASE+M, 'frozen', fail)
        self.assertEqual(store.snapshot('chat'), before)
        with self.assertRaises(ValueError):
            store.initialize_scope('chat', BASE+M, config_sha256='changed')

    def test_unknown_ack_and_stale_claim(self):
        state = pending()
        s.advance(state, [bar(BASE+M), bar(BASE+2*M, 100, 100, 95, 96)], BASE+3*M)
        store.transact('chat', BASE+3*M, 'frozen', lambda st: st.update(deepcopy(state)))
        i = store.claim_pending('chat', BASE+3*M, **self.kw)
        self.assertTrue(store.finish_attempt('chat', i['intent_id'], i['attempt_token'], 'UNKNOWN', BASE+3*M, **self.kw))
        self.assertFalse(store.finish_attempt('chat', i['intent_id'], i['attempt_token'], 'DELIVERED', BASE+3*M, **self.kw))
        self.assertIsNone(store.claim_pending('chat', BASE+3*M, **self.kw))


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_source_only_poll_limit_stale_guard_and_no_orders(self):
        db = MemoryDatabase()
        clock = [BASE+10_000]
        calls = []
        def fetch(symbol, start, end):
            calls.append((symbol, start, end))
            return [bar(t) for t in range(start, end, M)]
        w = worker.SolProximityWorker(cache=worker.SolPriceCache(fetch), clock=lambda: clock[0])
        w.bot, w.subscription = SimpleNamespace(), lambda: (True, 123)
        with patch.object(store, '_connect', db.connect), patch.object(worker.policy, 'sol_proximity_experimental_enabled', lambda: True):
            await w.observe(bundle(clock[0]), 123)
            self.assertEqual(w.status()['state'], 'BOOTSTRAP_UNVERIFIED')
            clock[0] += M
            await w.tick()
            count = len(calls)
            await w.tick()
            self.assertEqual(len(calls), count)
            self.assertEqual(set(c[0] for c in calls), {'SOL'})
            self.assertTrue(w.status()['notification_only'])
            self.assertFalse(w.status()['live_order_execution'])

    async def test_fresh_fill_delivery_once_then_restart(self):
        db = MemoryDatabase()
        now = BASE+3*M
        state = pending()
        s.advance(state, [bar(BASE+M), bar(BASE+2*M, 100, 100, 95, 96)], now)
        def fetch(symbol, start, end):
            return [bar(t, 96, 97, 95, 96) for t in range(start, end, M)]
        w = worker.SolProximityWorker(cache=worker.SolPriceCache(fetch), clock=lambda: now)
        w.bot = SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(message_id=55)))
        w.subscription = lambda: (True, 123)
        scope = worker.subscription_scope(123)
        with patch.object(store, '_connect', db.connect), patch.object(worker.policy, 'sol_proximity_experimental_enabled', lambda: True):
            await w.initialize(scope)
            store.transact(scope, now, worker.CONFIG_SHA256, lambda st: st.update(deepcopy(state)))
            self.assertTrue(await w.deliver(scope, 123))
            self.assertFalse(await w.deliver(scope, 123))
            restarted = worker.SolProximityWorker(cache=worker.SolPriceCache(fetch), clock=lambda: now)
            restarted.bot, restarted.subscription = w.bot, w.subscription
            await restarted.initialize(scope)
            self.assertFalse(await restarted.deliver(scope, 123))
            w.bot.send_message.assert_awaited_once()
            self.assertEqual(store.snapshot(scope)['intents'][0]['status'], 'DELIVERED')

    async def test_pending_barrier_veto_sends_nothing(self):
        db = MemoryDatabase()
        now = BASE+3*M
        state = pending()
        s.advance(state, [bar(BASE+M), bar(BASE+2*M, 100, 100, 95, 96)], now)
        def fetch(symbol, start, end):
            return [bar(t, 96, 103, 95, 96) for t in range(start, end, M)]
        w = worker.SolProximityWorker(cache=worker.SolPriceCache(fetch), clock=lambda: now)
        w.bot, w.subscription = SimpleNamespace(send_message=AsyncMock()), lambda: (True, 123)
        scope = worker.subscription_scope(123)
        with patch.object(store, '_connect', db.connect), patch.object(worker.policy, 'sol_proximity_experimental_enabled', lambda: True):
            await w.initialize(scope)
            store.transact(scope, now, worker.CONFIG_SHA256, lambda st: st.update(deepcopy(state)))
            await w.deliver(scope, 123)
            w.bot.send_message.assert_not_awaited()
            self.assertEqual(store.snapshot(scope)['intents'][0]['status'], 'CANCELLED_STALE_PRICE')

    async def test_failure_backoff_no_collecting_other_sources(self):
        db = MemoryDatabase()
        calls = []
        def fail(*args):
            calls.append(args)
            raise ValueError('unavailable')
        w = worker.SolProximityWorker(cache=worker.SolPriceCache(fail), clock=lambda: BASE+10_000)
        w.bot, w.subscription = SimpleNamespace(), lambda: (True, 123)
        with patch.object(store, '_connect', db.connect), patch.object(worker.policy, 'sol_proximity_experimental_enabled', lambda: True):
            await w.observe(bundle(BASE+10_000), 123)
            await w.observe(bundle(BASE+10_000), 123)
            self.assertEqual(len(calls), 1)
            self.assertEqual(w.retry_ms, BASE+10_000+5*M)


if __name__ == '__main__':
    unittest.main()
