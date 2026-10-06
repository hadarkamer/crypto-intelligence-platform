"""Offline price-source isolation, causal quote and migration regressions."""
from copy import deepcopy
from types import SimpleNamespace
import unittest
from unittest.mock import patch, AsyncMock

import sol_proximity_experimental_signal as signal
import sol_proximity_experimental_store as store
import sol_proximity_experimental_worker as worker
from maxpain_experimental_specs import SPECS, SOL_RANGE24, ETH_LONG, DOGE_LONG_TF
from maxpain_experimental_refresh_selftest import fixture, decoded
from sol_proximity_experimental_selftest import BASE, M, bar, pending
from hype_row71205_experimental_store_selftest import MemoryDatabase


def predecessor(coin='ETH'):
    state = pending()
    template = deepcopy(state['active'][0])
    template.update(coin=coin, rule_id=SPECS[coin].rule_id)
    opened = dict(template, status='OPEN', fill_ms=BASE+2*M, fill_price=96,
                  position_id='filled-old', timeframe='1m')
    ambiguous = dict(opened, status='UNKNOWN', position_id='unknown-old',
                     unknown_reason='BARRIER_ORDER_UNKNOWN')
    state['active'] = [dict(template, position_id='pending-old'), opened, ambiguous]
    state['intents'] = [dict(intent_id='old-intent', position_id='filled-old', status='PENDING',
                            expires_ms=BASE+20*M, payload=deepcopy(opened))]
    state.update(version=store.VERSION, config_sha256=store.PRE_HYPERLIQUID_CONFIGS[coin])
    return state


class QuoteTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_five_source_quotes_use_closed_hl_candle_and_do_not_mutate_watch(self):
        for coin, spec in SPECS.items():
            with self.subTest(coin=coin):
                now = BASE+10000
                incoming = fixture(spec, now, 102, source=80)
                before = deepcopy(incoming)
                calls = []
                def fetch(symbol, start, end):
                    calls.append((symbol, start, end))
                    return [bar(t, 99, 104, 98, 100) for t in range(start, end, M)]
                w = worker.SolProximityWorker(spec=spec, cache=worker.SolPriceCache(fetch, coin),
                                              clock=lambda: now)
                adjusted = await w.reprice_bundle(incoming, now)
                self.assertEqual(incoming, before)
                self.assertEqual(calls, [(coin, BASE-M, BASE)])
                row = adjusted['coins'][coin]['sources']['maxpain_operational_rows'][0]
                self.assertEqual(row['current_price'], 100)
                self.assertEqual(row['experimental_original_quote']['current_price'], 80)
                self.assertEqual(row['source_observed_at_utc'],
                                 before['coins'][coin]['sources']['maxpain_operational_rows'][0]['source_observed_at_utc'])
                self.assertEqual(row['price_source'], f'HYPERLIQUID_{coin}_PERPETUAL_TRADE_1M')
                self.assertEqual(signal.milliseconds(row['price_fetched_at_utc']), BASE-1)
                self.assertEqual(adjusted['coins'][coin]['maxpain'], before['coins'][coin]['maxpain'])

    async def test_geometry_uses_new_quote_while_target_liquidity_remain_provider_data(self):
        now = BASE+10000
        w = worker.SolProximityWorker(spec=ETH_LONG,
            cache=worker.SolPriceCache(lambda coin, start, end: [bar(t) for t in range(start, end, M)], 'ETH'),
            clock=lambda: now)
        b = fixture(ETH_LONG, now, 102, source=80)
        d = signal.decode_bundle(await w.reprice_bundle(b, now), now, ETH_LONG)
        row = next(r for r in d['rows'] if r['eligible'])
        self.assertEqual(row['levels'], dict(source_price=100., target_price=102., direction=1,
                                             entry_price=96., stop_price=90., take_price=101.))
        self.assertEqual(row['distance_pct'], 2.)
        self.assertEqual(row['liquidation_amount'], 100)

    async def test_stale_or_future_decision_refuses_before_fetch(self):
        def fail(*args):
            raise AssertionError('must not fetch an invalid generation')
        w = worker.SolProximityWorker(cache=worker.SolPriceCache(fail), clock=lambda: BASE)
        for decision in (BASE+1, BASE-5*M-1):
            with self.assertRaises(ValueError):
                await w.reprice_bundle(fixture(SOL_RANGE24, decision, 102), BASE)

    async def test_complete_sol_range_and_reference_share_only_hl_closed_cache(self):
        now = BASE+10000
        calls = []
        def fetch(coin, start, end):
            calls.append((coin, start, end))
            return [bar(t, 100, 104, 96, 100) for t in range(start, end, M)]
        w = worker.SolProximityWorker(cache=worker.SolPriceCache(fetch), clock=lambda: now)
        w.subscription = lambda: (True, 123)
        db = MemoryDatabase()
        with patch.object(store, '_connect', db.connect), patch.object(worker.policy, 'sol_proximity_experimental_enabled', return_value=True):
            await w.observe(fixture(SOL_RANGE24, now, 102, source=80), 123)
            state = store.snapshot(w.scope_for(123))
        self.assertTrue(state['initialized_universe'])
        self.assertEqual(w.status()['state'], 'BOOTSTRAP_UNVERIFIED')
        self.assertTrue(all(coin == 'SOL' for coin, _, _ in calls))
        closed_requests = [(a, b) for _, a, b in calls if b <= BASE]
        self.assertEqual(sum((b-a)//M for a, b in closed_requests), 1440)
        self.assertFalse(any(t >= BASE for t in w.cache.rows['SOL']))


class MigrationTests(unittest.TestCase):
    def test_legacy_hype_keeps_same_market_with_shared_archive_capable_adapter(self):
        with patch.object(worker.hyperliquid, 'fetch_rows', return_value=[bar(BASE)]) as fetch:
            with patch.object(worker.requests, 'get', side_effect=AssertionError('No Binance HYPE route')):
                self.assertEqual(worker.legacy_fetch_rows('HYPE', BASE, BASE+M), [bar(BASE)])
        fetch.assert_called_once_with('HYPE', BASE, BASE+M)

    def test_five_whitelisted_predecessors_preserve_frozen_filled_and_cancel_pending(self):
        for coin, spec in SPECS.items():
            with self.subTest(coin=coin):
                state = predecessor(coin)
                before = deepcopy(state)
                new_hash = worker.config_hash(spec)
                store.migrate_price_source(state, BASE+3*M, new_hash, coin)
                self.assertEqual(state['active'], [])
                self.assertEqual(state['legacy_source_state']['active'], before['active'][1:])
                self.assertEqual(state['legacy_source_state']['bar_cursor_ms'], before['bar_cursor_ms'])
                self.assertEqual(state['history'][-1]['status'], 'CANCELLED_SOURCE_REPLACED')
                self.assertNotEqual(state['intents'][0]['status'], 'PENDING')
                self.assertFalse(state['initialized_universe'])
                self.assertTrue(state['episodes']['102']['consumed'])
                self.assertTrue(state['episodes']['102']['bootstrap_unverified'])
                self.assertEqual(state['source_migration']['preserved_filled_or_unknown'], 2)
                self.assertEqual(state['bar_cursor_ms'], BASE+2*M)
                self.assertEqual(state['source_migration']['cancelled_pending'], 1)
                expected = 'HYPERLIQUID_HYPE_PERPETUAL_TRADE_1M' if coin=='HYPE' else f'BINANCE_SPOT_{coin}USDT_TRADE_1M'
                self.assertEqual(state['legacy_source_state']['source_route'], expected)

    def test_unknown_predecessor_wrong_coin_or_inflight_refused(self):
        for change, coin in [({'config_sha256':'unrecognized'}, 'ETH'), ({}, 'DOGE'),
                             ({'intents':[dict(status='IN_FLIGHT', attempt_ms=BASE+3*M)]}, 'ETH')]:
            state = predecessor('ETH')
            state.update(change)
            with self.assertRaises(ValueError):
                store.migrate_price_source(state, BASE+3*M, worker.config_hash(ETH_LONG), coin)

    def test_stale_inflight_becomes_unknown_and_can_never_be_replayed(self):
        state = predecessor()
        state['intents'][0].update(status='IN_FLIGHT', attempt_ms=BASE,
                                   attempt_token='expired-attempt')
        store.migrate_price_source(state, BASE+3*M, worker.config_hash(ETH_LONG), 'ETH')
        self.assertEqual(state['intents'][0]['status'], 'UNKNOWN')

    def test_restart_is_idempotent_and_old_config_cannot_mutate_new_state(self):
        db = MemoryDatabase()
        now = BASE+3*M
        old_hash, new_hash = store.PRE_HYPERLIQUID_CONFIGS['ETH'], worker.config_hash(ETH_LONG)
        with patch.object(store, '_connect', db.connect):
            store.initialize_scope('eth', now, config_sha256=old_hash)
            store.transact('eth', now, old_hash, lambda s: s.update(predecessor()))
            first = store.initialize_scope('eth', now, config_sha256=new_hash, migrate_source_coin='ETH')
            second = store.initialize_scope('eth', now+M, config_sha256=new_hash, migrate_source_coin='ETH')
            self.assertEqual(first, second)
            self.assertIsNone(store.claim_pending('eth', now+M, config_sha256=new_hash))
            with self.assertRaises(ValueError):
                store.transact('eth', now+M, old_hash, lambda s: s.update(wrong=True))

    def test_bootstrap_cannot_reuse_existing_target_but_complete_absence_then_return_can(self):
        state = predecessor('DOGE')
        state['active'] = []
        state['intents'] = []
        store.migrate_price_source(state, BASE+3*M, worker.config_hash(DOGE_LONG_TF), 'DOGE')
        def intake(t, target):
            signal.ingest(state, decoded(DOGE_LONG_TF, t, target), t, [bar(t//M*M)])
        intake(BASE+3*M, 102)
        self.assertFalse(state['active'])
        signal.advance(state, [bar(BASE+3*M)], BASE+4*M)
        intake(BASE+4*M, 103)
        self.assertFalse(state['active'])
        signal.advance(state, [bar(BASE+4*M)], BASE+5*M)
        intake(BASE+5*M, 102)
        self.assertEqual(len(state['active']), 1)
        self.assertEqual(state['active'][0]['target_price'], 102)

    def test_legacy_filled_target_still_blocks_nearby_new_target(self):
        state = predecessor('DOGE')
        store.migrate_price_source(state, BASE+3*M, worker.config_hash(DOGE_LONG_TF), 'DOGE')
        signal.ingest(state, decoded(DOGE_LONG_TF, BASE+3*M, 103), BASE+3*M, [bar(BASE+3*M)])
        signal.advance(state, [bar(BASE+3*M)], BASE+4*M)
        signal.ingest(state, decoded(DOGE_LONG_TF, BASE+4*M, 102.1), BASE+4*M, [bar(BASE+4*M)])
        self.assertFalse(state['active'])
        self.assertGreater(state['counts']['NEAR_TARGET_BLOCKED'], 0)


class LegacyWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_old_positions_close_on_legacy_path_only_and_unknown_is_retained(self):
        now = BASE+3*M
        calls = {'new': [], 'old': []}
        def new_fetch(coin, start, end):
            calls['new'].append((coin, start, end))
            return [bar(t, 96, 99, 94, 96) for t in range(start, end, M)]
        def old_fetch(coin, start, end):
            calls['old'].append((coin, start, end))
            return [bar(t, 96, 103, 94, 96) for t in range(start, end, M)]
        w = worker.SolProximityWorker(spec=ETH_LONG, clock=lambda: now,
            cache=worker.SolPriceCache(new_fetch, 'ETH'), legacy_cache=worker.SolPriceCache(old_fetch, 'ETH'))
        w.subscription = lambda: (True, 123)
        w.bot = SimpleNamespace(send_message=AsyncMock())
        scope, db = w.scope_for(123), MemoryDatabase()
        with patch.object(store, '_connect', db.connect):
            old_hash = store.PRE_HYPERLIQUID_CONFIGS['ETH']
            store.initialize_scope(scope, now, config_sha256=old_hash)
            store.transact(scope, now, old_hash, lambda s: s.update(predecessor()))
            state = await w.initialize(scope)
            state = await w.monitor(scope, state)
            self.assertTrue(w.caught_up(state))
            self.assertFalse(await w.deliver(scope, 123))
        self.assertEqual(calls['new'], [])
        self.assertEqual(calls['old'], [('ETH', BASE+M, BASE+3*M)])
        old = state['legacy_source_state']
        self.assertEqual(old['history'][-1]['status'], 'TAKE_TOUCHED')
        self.assertEqual(old['active'][0]['status'], 'UNKNOWN')
        self.assertEqual(w.status()['legacy_unknown'], 1)
        w.bot.send_message.assert_not_awaited()

    async def test_unrecovered_legacy_cursor_blocks_new_intake(self):
        state = predecessor()
        now = BASE+2001*M
        store.migrate_price_source(state, now, worker.config_hash(ETH_LONG), 'ETH')
        w = worker.SolProximityWorker(spec=ETH_LONG, clock=lambda: now)
        self.assertFalse(w.caught_up(state))
        state['legacy_source_state']['bar_cursor_ms'] = now-M
        self.assertTrue(w.caught_up(state))

    async def test_legacy_failure_does_not_fall_back_to_hl_or_reprice(self):
        def fail(*args):
            raise ValueError('old source unavailable')
        def forbidden(*args):
            raise AssertionError('new source must not replace legacy evidence')
        state = predecessor()
        now = BASE+3*M
        store.migrate_price_source(state, now, worker.config_hash(ETH_LONG), 'ETH')
        w = worker.SolProximityWorker(spec=ETH_LONG, clock=lambda: now,
            cache=worker.SolPriceCache(forbidden, 'ETH'), legacy_cache=worker.SolPriceCache(fail, 'ETH'))
        with self.assertRaisesRegex(ValueError, 'old source unavailable'):
            await w.monitor('unused', state)
        self.assertEqual(state['legacy_source_state']['active'][0]['entry_price'], 96)


if __name__ == '__main__':
    unittest.main()
