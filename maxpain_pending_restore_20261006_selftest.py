"""Local-only regression suite for the explicit incident restoration."""
import asyncio
from copy import deepcopy
import os
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import maxpain_pending_restore_20261006 as r
import sol_proximity_experimental_store as store
import sol_proximity_experimental_signal as signal
import sol_proximity_experimental_worker as worker_module
from hype_row71205_experimental_store_selftest import MemoryDatabase

M=r.MINUTE
NOW=1791284400000
COIN='ETH'
SCOPE='explicit-local-fixture'
ROUTE='HYPERLIQUID_ETH_PERPETUAL_TRADE_1M'
OLD='BINANCE_SPOT_ETHUSDT_TRADE_1M'


def fixture():
    p=dict(position_id='case1',coin=COIN,rule_id='ETH_MAXPAIN_LONG_DIST1_3',status=r.TERMINAL,
           source_price=100.,target_price=102.,entry_price=96.,stop_price=90.,take_price=101.,direction=1,
           arm_ms=NOW-6*M,expires_ms=NOW+20*M,decision_ms=NOW-6*M-10000,terminal_ms=NOW-3*M+5000,
           source_quote={'price_source':OLD},timeframe='12h',episode_key='102',episode_generation=1,
           liquidity_growth=True)
    state=dict(signal.initial(NOW-10*M), version=store.VERSION,config_sha256='f'*64,
               bar_cursor_ms=NOW-M,history=[p],active=[],intents=[],counts={},
               episodes={'102':dict(consumed=True,consume_reason='SOURCE_REBASE_UNVERIFIED',directions=[1],generation=1,present=True)},
               source_migration=dict(coin=COIN,migrated_ms=p['terminal_ms'],to_config_sha256='f'*64,
                                     legacy_price_source=OLD,new_price_source=ROUTE),
               legacy_source_state={'active':[],'history':[]})
    rows=[[t,100.,100.2,99.8,100.] for t in range(NOW-6*M,NOW,M)]
    prices={s:deepcopy(rows) for s in (OLD,ROUTE)}
    live={s:[NOW,100.,100.2,99.8,100.] for s in (OLD,ROUTE)}
    approved={COIN:dict(config_sha256='f'*64,source_migration=deepcopy(state['source_migration']),
                       plan_hashes={'case1':r.digest(p)},bot_settings_key=store.key_for(SCOPE))}
    return state,prices,live,approved


class PureProductionTests(unittest.TestCase):
    def setUp(self):
        self.state,self.prices,self.live,self.approved=fixture()
        self.env=patch.dict(os.environ,{r.ENABLE_ENV:r.ENABLE_NONCE});self.env.start()
        self.whitelist=patch.object(r,'APPROVED',self.approved);self.whitelist.start()
    def tearDown(self):self.whitelist.stop();self.env.stop()
    def apply(self):return r.apply_fresh(self.state,self.prices,self.live,NOW+5000,NOW+6000)

    def test_default_disabled_and_wrong_nonce_rejected(self):
        for value in ('','1','true','RESTORE'):
            with patch.dict(os.environ,{r.ENABLE_ENV:value}):
                with self.assertRaisesRegex(ValueError,'disabled'):self.apply()
        self.assertEqual(self.state['active'],[])

    def test_exact_candidate_only_and_immutable_cancel_record(self):
        self.state['history'][0]['entry_price']=97
        with self.assertRaisesRegex(ValueError,'changed'):self.apply()

    def test_unknown_config_and_migration_refused(self):
        self.state['config_sha256']='a'*64
        with self.assertRaisesRegex(ValueError,'configuration'):self.apply()

    def test_restore_levels_ttl_audit_and_idempotence(self):
        before=deepcopy(self.state);result=self.apply()
        self.assertEqual(result['restored'],1)
        p=self.state['active'][0]
        for key in ('position_id','source_price','target_price','entry_price','take_price','stop_price','expires_ms','source_quote'):
            self.assertEqual(p[key],before['history'][0][key])
        self.assertEqual(p['arm_ms'],NOW+M)
        self.assertEqual(p['manual_source_restore']['original_arm_ms'],NOW-6*M)
        self.assertEqual(self.state['history'],before['history'])
        self.assertEqual(self.state['episodes'],before['episodes'])
        self.assertEqual(self.state['intents'],[])
        self.assertEqual(self.apply()['status'],'ALREADY_PROCESSED')
        self.assertEqual(len(self.state['active']),1)

    def test_no_backdated_partial_minute_fill_then_future_entry(self):
        self.apply()
        # Current minute hits entry after restoration: effective resume is next minute.
        signal.advance(self.state,[[NOW,100.,100.2,95.9,96.2]],NOW+M)
        self.assertEqual(self.state['active'][0]['status'],'PENDING')
        self.assertEqual(self.state['intents'],[])
        signal.advance(self.state,[[NOW+M,96.,96.1,95.8,95.9]],NOW+2*M)
        self.assertEqual(self.state['active'][0]['status'],'OPEN')
        self.assertEqual(len(self.state['intents']),1)
        self.assertEqual(self.state['intents'][0]['payload']['fill_ms'],NOW+M)

    def test_target_before_effective_arm_cancels_without_intent(self):
        self.apply()
        signal.advance(self.state,[[NOW,100.,102.1,99.9,101.9]],NOW+M)
        self.assertEqual(self.state['active'],[])
        self.assertEqual(self.state['history'][-1]['status'],'TARGET_BEFORE_ARM')
        self.assertEqual(self.state['intents'],[])

    def test_entry_current_live_bar_excluded(self):
        self.live[ROUTE][3]=95.9
        self.assertEqual(self.apply()['restored'],0)

    def test_target_current_live_bar_excluded(self):
        self.live[OLD][2]=102.
        self.assertEqual(self.apply()['restored'],0)

    def test_missing_current_and_stale_capture_refused(self):
        del self.live[OLD]
        with self.assertRaisesRegex(ValueError,'Current minute'):self.apply()
        with self.assertRaisesRegex(ValueError,'stale'):
            r.apply_fresh(self.state,self.prices,self.live,NOW,NOW+20000)

    def test_minute_rollover_refused(self):
        with self.assertRaisesRegex(ValueError,'Minute rolled'):
            r.apply_fresh(self.state,self.prices,self.live,NOW+59000,NOW+M+1000)

    def test_full_arm_path_missing_and_prior_entry_rejected(self):
        self.prices[OLD][0][3]=95.9
        self.assertEqual(self.apply()['restored'],0)

    def test_new_neighbor_blocks_and_is_receipted(self):
        self.state['active']=[dict(position_id='new',target_price=102.1,status='PENDING')]
        self.assertEqual(self.apply()['restored'],0)
        self.assertIn('NEARBY_ACTIVE',self.state['manual_pending_restore_incidents'][r.BATCH]['decisions'][0]['reasons'][0])

    def test_expiry_not_extended_including_resume_boundary(self):
        p=self.state['history'][0];p['expires_ms']=NOW+M
        self.approved[COIN]['plan_hashes']['case1']=r.digest(p)
        self.assertEqual(self.apply()['restored'],0)

    def test_atomic_store_transaction_keeps_current_config_on_restart(self):
        memory=MemoryDatabase()
        with patch.object(store,'_connect',memory.connect):
            store.initialize_scope(SCOPE,NOW,config_sha256='f'*64)
            store.transact(SCOPE,NOW,'f'*64,lambda s:(s.clear(),s.update(deepcopy(self.state))))
            result=store.transact(SCOPE,NOW+6000,'f'*64,lambda s:r.apply_fresh(s,self.prices,self.live,NOW+5000,NOW+6000))
            self.assertEqual(result['restored'],1)
            again=store.initialize_scope(SCOPE,NOW+7000,config_sha256='f'*64,migrate_source_coin='ETH')
            self.assertEqual(len(again['active']),1)
            self.assertEqual(again['source_migration'],self.state['source_migration'])
            self.assertIsNone(store.claim_pending(SCOPE,NOW+7000,config_sha256='f'*64))


class BackgroundTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        r._RECOVERY_LOCK=asyncio.Lock()
        self.state,self.prices,self.live,self.approved=fixture()
        self.env=patch.dict(os.environ,{r.ENABLE_ENV:r.ENABLE_NONCE});self.env.start()
        self.whitelist=patch.object(r,'APPROVED',self.approved);self.whitelist.start()
    async def asyncTearDown(self):self.whitelist.stop();self.env.stop()

    async def test_disabled_scheduler_creates_nothing(self):
        w=SimpleNamespace(source_restore_task=None)
        with patch.dict(os.environ,{r.ENABLE_ENV:''}):r.schedule(w,SCOPE)
        self.assertIsNone(w.source_restore_task)

    async def test_failure_does_not_pause_regular_worker(self):
        def fail(*args):raise RuntimeError('scoped fixture failure')
        w=SimpleNamespace(spec=SimpleNamespace(coin='ETH'),clock=lambda:NOW+5000,
              runtime={'ready':True},retry_ms=0,cache=SimpleNamespace(fetch=fail),legacy_cache=SimpleNamespace(fetch=fail))
        with patch.object(store,'snapshot',return_value=self.state):await r.run_once(w,SCOPE)
        self.assertTrue(w.runtime['ready']);self.assertEqual(w.retry_ms,0)
        self.assertEqual(w.runtime['pending_source_restore']['status'],'DEFERRED_NO_MUTATION')

    async def test_bad_scope_prevents_even_snapshot_read(self):
        w=SimpleNamespace(spec=SimpleNamespace(coin='ETH'),runtime={})
        with patch.object(store,'snapshot',side_effect=AssertionError('forbidden')) as read:
            await r.run_once(w,'other-scope')
            read.assert_not_called()
        self.assertEqual(w.runtime['pending_source_restore']['status'],'DEFERRED_NO_MUTATION')

    async def test_background_scheduler_one_attempt_only(self):
        w=SimpleNamespace(spec=SimpleNamespace(coin='ETH'),source_restore_task=None,runtime={})
        with patch.object(r,'run_once',return_value=None) as run:
            r.schedule(w,SCOPE);first=w.source_restore_task;r.schedule(w,SCOPE)
            self.assertIs(first,w.source_restore_task);await first
            run.assert_called_once()


class AdditionalSafetyTests(unittest.TestCase):
    def test_formula_hashes_unchanged_for_all_five(self):
        from maxpain_experimental_specs import SPECS
        for coin,spec in SPECS.items():
            self.assertEqual(worker_module.config_hash(spec),r.APPROVED[coin]['config_sha256'])

    def test_operational_capacity_blocks_restore(self):
        state,prices,live,approved=fixture()
        state['active']=[dict(position_id='existing'+str(i),target_price=110+i,status='PENDING') for i in range(signal.MAX_ACTIVE)]
        with patch.dict(os.environ,{r.ENABLE_ENV:r.ENABLE_NONCE}),patch.object(r,'APPROVED',approved):
            result=r.apply_fresh(state,prices,live,NOW+5000,NOW+6000)
        self.assertEqual(result['restored'],0)
        self.assertIn('OPERATIONAL_CAPACITY_REACHED',result['receipt']['decisions'][0]['reasons'])


class IntegrationSchedulingTests(unittest.IsolatedAsyncioTestCase):
    async def test_hook_only_after_monitor_and_normal_delivery(self):
        from unittest.mock import AsyncMock
        trace=[]
        w=worker_module.SolProximityWorker(clock=lambda:NOW+5000)
        w.subscription=lambda:(True,123)
        w.allowed=lambda chat:True
        state=dict(signal.initial(NOW),bar_cursor_ms=NOW-M)
        async def init(scope):trace.append('initialize');return state
        async def monitor(scope,state):trace.append('monitor');return state
        async def deliver(scope,chat):trace.append('deliver');return False
        w.initialize=init;w.monitor=monitor;w.deliver=deliver
        with patch.object(r,'schedule',side_effect=lambda w,scope:trace.append('schedule')):
            await w.tick()
        self.assertEqual(trace,['initialize','monitor','deliver','schedule'])

    async def test_enabled_background_happy_path_one_atomic_apply(self):
        state,prices,live,approved=fixture()
        memory=MemoryDatabase();r._RECOVERY_LOCK=asyncio.Lock()
        calls=[]
        def fetch_for(route):
            def fetch(coin,start,end):
                calls.append((route,start,end))
                allrows=prices[route]+[live[route]]
                return [row for row in allrows if start<=row[0]<end]
            return fetch
        w=SimpleNamespace(spec=SimpleNamespace(coin='ETH'),clock=lambda:NOW+5000,runtime={'ready':True},retry_ms=0,
            cache=SimpleNamespace(fetch=fetch_for(ROUTE)),legacy_cache=SimpleNamespace(fetch=fetch_for(OLD)))
        async def db(func,*args,**kwargs):return func(*args,config_sha256='f'*64,**kwargs)
        w.db=db
        with patch.dict(os.environ,{r.ENABLE_ENV:r.ENABLE_NONCE}),patch.object(r,'APPROVED',approved),patch.object(store,'_connect',memory.connect):
            store.initialize_scope(SCOPE,NOW,config_sha256='f'*64)
            store.transact(SCOPE,NOW,'f'*64,lambda s:(s.clear(),s.update(deepcopy(state))))
            await r.run_once(w,SCOPE)
            result=store.snapshot(SCOPE)
            self.assertEqual(w.runtime['pending_source_restore']['restored'],1)
            self.assertEqual(len(result['active']),1)
            self.assertEqual(result['intents'],[])
            before_calls=len(calls)
            await r.run_once(w,SCOPE)
            self.assertEqual(len(calls),before_calls)
        self.assertEqual(len(calls),4) # two source histories and two current-minute vetoes
        self.assertTrue(w.runtime['ready']);self.assertEqual(w.retry_ms,0)


if __name__=='__main__':unittest.main(verbosity=2)
