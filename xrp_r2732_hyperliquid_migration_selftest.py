"""Source migration regressions for all three indicator alert workers; no network."""
import asyncio
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import xrp_r2732_experimental_store as xrp_store
import sol_g65_experimental_store as sol_store
import hype_row71205_experimental_store as hype_store
import xrp_r2732_experimental_worker as xrp_worker
import sol_g65_experimental_worker as sol_worker
import hype_row71205_experimental_worker as hype_worker
from xrp_r2732_experimental_store_selftest import MemoryDatabase

BASE = datetime(2026, 10, 6, 8, tzinfo=timezone.utc)
DECISION = BASE+timedelta(minutes=30)
ENTRY = DECISION+timedelta(minutes=1)
NOW = ENTRY+timedelta(seconds=10)
MODULES = [(xrp_store, xrp_worker), (sol_store, sol_worker), (hype_store, hype_worker)]


def seed_old(db, store, *, open_position=True):
    old = store.LEGACY_MIXED_SOURCE_CONFIG_SHA256
    store.initialize_scope('scope', BASE, config_sha256=old)
    store.reserve_signal('scope', DECISION, ENTRY, 100, {'valid': True, 'signal': True},
                         NOW, config_sha256=old)
    if store is sol_store and open_position:
        store.advance_position('scope', [dict(open_at=ENTRY, open=100.6, high=100.7, low=100, close=100.5)],
                               ENTRY+timedelta(minutes=1), config_sha256=old)
    state = store.snapshot('scope')
    # Fixtures reproduce the exact pre-migration shape: there was no source on positions.
    for key in ('price_source', 'source_contract_version', 'config_sha256'):
        state['active'].pop(key, None)
    db.values[store.key_for('scope')] = json.dumps(state)
    return state


class MigrationTests(unittest.TestCase):
    def test_open_levels_cursor_and_history_preserved_then_restart_idempotent(self):
        for store, worker in MODULES:
            with self.subTest(store=store.__name__):
                db = MemoryDatabase()
                with patch.object(store, '_connect', db.connect):
                    before = seed_old(db, store)
                    at = ENTRY+timedelta(minutes=2)
                    migrated = store.initialize_scope('scope', at, config_sha256=worker.CONFIG_SHA256,
                                                      migrate_all_prices=True)
                    for key, value in before['active'].items():
                        self.assertEqual(migrated['active'][key], value, key)
                    self.assertEqual(migrated['active']['price_source'], store.LEGACY_POSITION_PRICE_SOURCE)
                    self.assertEqual(migrated['active']['config_sha256'], store.LEGACY_MIXED_SOURCE_CONFIG_SHA256)
                    self.assertEqual(migrated['history'], before['history'])
                    self.assertEqual(migrated['decision_cursor'], before['decision_cursor'])
                    self.assertEqual(migrated['activated_at'], store.iso(at))
                    self.assertFalse(any(i['status']=='PENDING' for i in migrated['intents']))
                    restarted = store.initialize_scope('scope', at+timedelta(days=1),
                                config_sha256=worker.CONFIG_SHA256, migrate_all_prices=True)
                    self.assertEqual(restarted['activated_at'], migrated['activated_at'])
                    self.assertEqual(restarted['all_prices_source_migration'], migrated['all_prices_source_migration'])
                    self.assertEqual(restarted['active'], migrated['active'])
                    self.assertIsNone(store.claim_pending('scope', at, config_sha256=worker.CONFIG_SHA256))

    def test_sol_unfilled_limit_cancelled_and_ambiguous_capacity_retained(self):
        for ambiguous in (False, True):
            db=MemoryDatabase()
            with patch.object(sol_store, '_connect', db.connect):
                before=seed_old(db, sol_store, open_position=False)
                if ambiguous:
                    before['active']['monitor_status']='AMBIGUOUS'
                    db.values[sol_store.key_for('scope')]=json.dumps(before)
                result=sol_store.initialize_scope('scope', NOW, config_sha256=sol_worker.CONFIG_SHA256,
                                                  migrate_all_prices=True)
                if ambiguous:
                    self.assertEqual(result['active']['monitor_status'],'AMBIGUOUS')
                    self.assertEqual(result['active']['price_source'],sol_store.LEGACY_POSITION_PRICE_SOURCE)
                else:
                    self.assertIsNone(result['active'])
                    self.assertEqual(result['history'][-1]['outcome'],'CANCELLED_SOURCE_CHANGE')
                    self.assertEqual(result['history'][-1]['entry_price'],before['active']['entry_price'])

    def test_inflight_settles_normally_before_migration_and_stale_is_unknown(self):
        for store, worker in MODULES:
            with self.subTest(store=store.__name__):
                db=MemoryDatabase()
                with patch.object(store, '_connect', db.connect):
                    before=seed_old(db,store)
                    # SOL's notification exists only after the closed fill minute.
                    at=ENTRY+timedelta(minutes=1,seconds=10) if store is sol_store else NOW
                    intent=store.claim_pending('scope',at,config_sha256=store.LEGACY_MIXED_SOURCE_CONFIG_SHA256)
                    self.assertIsNotNone(intent)
                    frozen=deepcopy(db.values)
                    with self.assertRaisesRegex(ValueError,'in-flight'):
                        store.initialize_scope('scope',at+timedelta(seconds=30),config_sha256=worker.CONFIG_SHA256,
                                               migrate_all_prices=True)
                    self.assertEqual(db.values,frozen)
                    later=at+timedelta(minutes=2)
                    result=store.initialize_scope('scope',later,config_sha256=worker.CONFIG_SHA256,migrate_all_prices=True)
                    self.assertEqual(result['intents'][-1]['status'],'UNKNOWN')
                    self.assertIsNotNone(result['active'])
                    self.assertIsNone(store.claim_pending('scope',later,config_sha256=worker.CONFIG_SHA256))

    def test_arbitrary_origin_rejected_without_mutation(self):
        for store,worker in MODULES:
            db=MemoryDatabase()
            with self.subTest(store=store.__name__),patch.object(store,'_connect',db.connect):
                store.initialize_scope('scope',BASE,config_sha256='unreviewed')
                frozen=deepcopy(db.values)
                with self.assertRaisesRegex(ValueError,'hash mismatch'):
                    store.initialize_scope('scope',NOW,config_sha256=worker.CONFIG_SHA256,migrate_all_prices=True)
                self.assertEqual(db.values,frozen)

    def test_no_pre_migration_decision_can_be_replayed(self):
        for store,worker in MODULES:
            db=MemoryDatabase()
            with self.subTest(store=store.__name__),patch.object(store,'_connect',db.connect):
                store.initialize_scope('scope',BASE,config_sha256=store.LEGACY_MIXED_SOURCE_CONFIG_SHA256)
                store.initialize_scope('scope',NOW,config_sha256=worker.CONFIG_SHA256,migrate_all_prices=True)
                result=store.record_no_signal('scope',DECISION,NOW,config_sha256=worker.CONFIG_SHA256)
                self.assertEqual(result['status'],'BEFORE_ACTIVATION')


class WorkerSourceTests(unittest.IsolatedAsyncioTestCase):
    async def test_legacy_xrp_and_sol_open_monitor_uses_original_venue_only(self):
        for store,worker,cls,symbol in [(xrp_store,xrp_worker,xrp_worker.R2732Worker,'XRP'),
                                      (sol_store,sol_worker,sol_worker.SolG65Worker,'SOL')]:
            db=MemoryDatabase();calls=[]
            with self.subTest(symbol=symbol),patch.object(store,'_connect',db.connect):
                seed_old(db,store)
                current=ENTRY+timedelta(minutes=2)
                state=store.initialize_scope('scope',current,config_sha256=worker.CONFIG_SHA256,migrate_all_prices=True)
                def old_rows(s,start,end):
                    calls.append((s,start,end))
                    return [[t,100.3,100.4,100.2,100.3] for t in range(start,end,60000)]
                def reject(*_):
                    raise AssertionError('An existing Binance open must not consume Hyperliquid prices')
                w=cls(cache=worker.PriceCache(reject),legacy_cache=worker.PriceCache(old_rows),clock=lambda:worker.ms(current))
                if store is xrp_store:
                    await w.monitor('scope',state,worker.ms(current))
                else:
                    await w.monitor('scope',state)
                self.assertTrue(calls)
                self.assertEqual({call[0] for call in calls},{symbol})
                self.assertEqual(store.snapshot('scope')['active']['position_id'],state['active']['position_id'])

    async def test_unknown_position_source_never_silently_changes_market(self):
        for cls in (xrp_worker.R2732Worker,sol_worker.SolG65Worker):
            w=cls()
            for active in ({},{'price_source':'UNREVIEWED_EXCHANGE'}):
                with self.assertRaisesRegex(ValueError,'provenance'):
                    w.position_cache(active)

    async def test_xrp_fetches_only_prior_session_and_last_closed_minute(self):
        calls=[]
        decision=xrp_worker.ms(datetime(2026,10,6,8,tzinfo=timezone.utc))
        def fetch(symbol,start,end):
            calls.append((symbol,start,end))
            return [[t,100,101,99,100] for t in range(start,end,60000)]
        w=xrp_worker.R2732Worker(cache=xrp_worker.PriceCache(fetch))
        rows=await w.signal_rows(decision)
        start,end=xrp_worker.signal.previous_ny_regular_session(decision)
        self.assertEqual(calls,[('XRP',start,end),('XRP',decision-60000,decision)])
        self.assertEqual(len(rows),(end-start)//60000+1)
        self.assertTrue(xrp_worker.signal.evaluate_signal(rows,decision)['valid'])

    async def test_new_signal_source_metadata_is_explicit(self):
        for store,worker in MODULES:
            db=MemoryDatabase()
            with self.subTest(store=store.__name__),patch.object(store,'_connect',db.connect):
                store.initialize_scope('scope',BASE,config_sha256=worker.CONFIG_SHA256)
                result=store.reserve_signal('scope',DECISION,ENTRY,100,{'valid':True,'signal':True},NOW,
                                            config_sha256=worker.CONFIG_SHA256)
                self.assertEqual(result['position']['price_source'],store.PRICE_SOURCE)
                self.assertEqual(result['position']['source_contract_version'],store.SOURCE_CONTRACT_VERSION)
                self.assertEqual(result['position']['config_sha256'],worker.CONFIG_SHA256)


if __name__=='__main__':
    unittest.main()
