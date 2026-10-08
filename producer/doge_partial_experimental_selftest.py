"""Offline fixtures for the requested DOGE alert; all I/O isolated."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json
import unittest
from unittest.mock import patch,AsyncMock
from types import SimpleNamespace
import doge_partial_experimental_signal as s
import doge_partial_experimental_worker as w
import sol_proximity_experimental_signal as old
import sol_proximity_experimental_store as store
import sol_proximity_experimental_worker as parent
import alert_delivery_policy as policy
from maxpain_experimental_specs import DOGE_LONG_TF
from sol_proximity_experimental_selftest import bundle,bar,BASE,M
from hype_row71205_experimental_store_selftest import MemoryDatabase

def decoded(now,target=102,source=100,spec=w.SPEC):
    b=bundle(now,target,source=source)
    b['coins']['DOGE']=b['coins'].pop('SOL')
    if target<source:
        for r in b['coins']['DOGE']['sources']['maxpain_operational_rows']:
            r['long_max_pain']=target;r['short_max_pain']=source*1.1
        for r in b['coins']['DOGE']['maxpain']:
            r['target_price']=target if r['source_side']=='LONG'else source*1.1
    return s.decode_bundle(b,now,spec)

def bootstrap():
    x=s.initial(BASE+10000)
    s.ingest(x,decoded(BASE+10000,101),BASE+10000,[bar(BASE)])
    s.advance(x,[bar(BASE)],BASE+M+10000)
    return x

def pending(target=102):
    x=bootstrap();s.ingest(x,decoded(BASE+M+10000,target),BASE+M+10000,[bar(BASE+M)])
    s.advance(x,[bar(BASE+M)],BASE+2*M)
    return x

def filled(target=102):
    x=pending(target);p=x['active'][0];e=p['entry_price'];t=BASE+2*M
    s.advance(x,[bar(t,e,e+.1,e-.1,e)],t+M+10000)
    return x

class ReducerTests(unittest.TestCase):
    def test_definition_and_both_directions(self):
        for target,expected in [(102,(99,98,101.6)),(98,(101,102,98.4))]:
            x=pending(target);p=x['active'][0]
            self.assertEqual((p['entry_price'],p['stop_price'],p['take_price']),expected)
            self.assertEqual(len(x['active']),1)
    def test_distance_boundaries(self):
        for target,ok in [(100.99,False),(101,True),(102.999,True),(103,False)]:
            d=decoded(BASE,target);self.assertEqual(any(r['eligible']for r in d['rows']),ok)
    def test_bootstrap_consumed_and_new_target(self):
        x=bootstrap();self.assertFalse(x['active']);self.assertEqual(len(pending()['active']),1)
    def test_cap_pending_and_open(self):
        for x in [pending(),filled()]:
            t=x['bar_cursor_ms']+M+10000
            s.ingest(x,decoded(t,102.5),t,[bar(t//M*M)])
            self.assertEqual(len(x['active']),1)
    def test_long_partial_then_original_stop(self):
        x=filled();p=x['active'][0];self.assertAlmostEqual(p['partial_take_price'],100.95)
        t=BASE+3*M;s.advance(x,[bar(t,100,101,99.9,100.8)],t+M)
        self.assertEqual(p['remaining_fraction'],.25);self.assertEqual(p['stop_price'],98)
        s.advance(x,[bar(t+M,99,99.5,97.9,98)],t+2*M)
        self.assertEqual(x['history'][-1]['status'],'STOP_TOUCHED')
        self.assertAlmostEqual(x['history'][-1]['realized_price_move'],1.2125)
    def test_short_partial_then_take(self):
        x=filled(98);p=x['active'][0];self.assertAlmostEqual(p['partial_take_price'],99.05)
        t=BASE+3*M;s.advance(x,[bar(t,100,100.2,98.3,98.4)],t+M)
        self.assertEqual(x['history'][-1]['status'],'TAKE_TOUCHED')
        self.assertAlmostEqual(x['history'][-1]['realized_price_move'],2.1125)
    def test_partial_stop_ambiguous(self):
        x=filled();t=BASE+3*M;s.advance(x,[bar(t,100,101,97.9,100)],t+M)
        self.assertEqual(x['active'][0]['status'],'UNKNOWN')
        self.assertEqual(x['intents'][0]['status'],'CANCELLED_AMBIGUOUS')
    def test_partial_gap_open_then_stop_is_ordered(self):
        x=filled();t=BASE+3*M;s.advance(x,[bar(t,101,101.2,97.9,98)],t+M)
        self.assertEqual(x['history'][-1]['status'],'STOP_TOUCHED')
        self.assertAlmostEqual(x['history'][-1]['realized_price_move'],1.25)
    def test_entry_partial_intrabar_is_unknown(self):
        x=pending();t=BASE+2*M;s.advance(x,[bar(t,100,101,98.9,100)],t+M)
        self.assertEqual(x['active'][0]['status'],'UNKNOWN');self.assertFalse(x['intents'])
    def test_target_before_entry(self):
        x=pending();t=BASE+2*M;s.advance(x,[bar(t,101,102.1,100,102)],t+M)
        self.assertEqual(x['history'][-1]['status'],'TARGET_BEFORE_ENTRY')
    def test_pending_expiry_and_holding_expiry(self):
        x=pending();p=x['active'][0];t=p['expires_ms'];x['bar_cursor_ms']=t-M
        s.advance(x,[bar(t)],t+M);self.assertEqual(x['history'][-1]['status'],'EXPIRED_PENDING')
        x=filled();p=x['active'][0];t=p['hold_until_ms'];x['bar_cursor_ms']=t-M
        s.advance(x,[bar(t,100,103,97,100)],t+M)
        self.assertEqual(x['history'][-1]['status'],'TIME_EXIT_24H');self.assertEqual(x['history'][-1]['exit_price'],100)
    def test_partial_holding_expiry_and_restart(self):
        x=filled();t=BASE+3*M;s.advance(x,[bar(t,100,101,99.5,100)],t+M)
        x=json.loads(json.dumps(x));p=x['active'][0];t=p['hold_until_ms'];x['bar_cursor_ms']=t-M
        s.advance(x,[bar(t,100,100.1,99.9,100)],t+M)
        self.assertAlmostEqual(x['history'][-1]['realized_price_move'],1.7125)
    def test_duplicate_bar_and_gap_rejected(self):
        x=filled();before=deepcopy(x);t=x['bar_cursor_ms'];s.advance(x,[bar(t)],t+M);self.assertEqual(x,before)
        with self.assertRaises(ValueError):s.advance(x,[bar(t+2*M)],t+3*M)
    def test_stale_fill_no_alert(self):
        x=pending();t=BASE+2*M;s.advance(x,[bar(t,99,99.1,98.9,99)],t+4*M)
        self.assertFalse(x['intents'])
    def test_notification_renders_full_contract(self):
        p=filled()['active'][0];text=w.render_alert(p)
        for value in ['100.95','101.6','75%','25%','24','0.2%','Hyperliquid']:self.assertIn(value,text)
        self.assertTrue(w.WORKER.policy_enabled())
        self.assertEqual(w.WORKER.status()['operational_state_capacity'],1)
    def test_old_workers_keep_same_config_and_api(self):
        for worker in [parent.WORKER,*parent.ADDITIONAL_WORKERS.values()]:
            self.assertIs(worker.signal,old);self.assertIs(worker.store,store)
            self.assertEqual(worker.config_sha256,parent.config_hash(worker.spec))

class StoreTests(unittest.TestCase):
    def setUp(self):
        self.db=MemoryDatabase();self.mock=patch.object(store,'_connect',self.db.connect);self.mock.start();self.addCleanup(self.mock.stop)
        self.oldscope='chat:'+DOGE_LONG_TF.rule_id;self.newscope='chat:'+w.RULE_ID
        for scope in [self.oldscope,self.newscope]:
            store.initialize_scope(scope,BASE+10000,config_sha256='test')
            store.transact(scope,BASE+M+10000,'test',lambda x:x.update(bootstrap()))
    def intake(self,scope,target):
        d=decoded(BASE+M+10000,target,spec=w.SPEC if scope==self.newscope else DOGE_LONG_TF)
        fn=w.StoreAPI().ingest if scope==self.newscope else store.ingest
        return fn(scope,d,[bar(BASE+M)],BASE+M+10000,config_sha256='test')
    def test_shared_guard_both_arrival_orders(self):
        for first,second in [(self.oldscope,self.newscope),(self.newscope,self.oldscope)]:
            for scope in [first,second]:store.transact(scope,BASE+M+10000,'test',lambda x:x.update(bootstrap()))
            self.intake(first,102);self.intake(second,102.1)
            self.assertEqual(len(store.snapshot(first)['active']),1);self.assertEqual(len(store.snapshot(second)['active']),0)
            self.assertNotIn('_admission_peers',store.snapshot(second))
    def test_far_target_can_coexist(self):
        self.intake(self.oldscope,102);self.intake(self.newscope,102.5)
        self.assertEqual(len(store.snapshot(self.oldscope)['active']),1);self.assertEqual(len(store.snapshot(self.newscope)['active']),1)
    def test_concurrent_requests_serialize(self):
        with ThreadPoolExecutor(2)as pool:list(pool.map(lambda x:self.intake(x,102),[self.oldscope,self.newscope]))
        self.assertEqual(sum(len(store.snapshot(x)['active'])for x in [self.oldscope,self.newscope]),1)
        locks=[p[0]for q,p in self.db.calls if q.startswith('SELECT pg_advisory_xact_lock')]
        self.assertGreaterEqual(len(locks),4)
    def test_restart_does_not_reset_pending(self):
        self.intake(self.newscope,102);a=store.snapshot(self.newscope)
        b=store.initialize_scope(self.newscope,BASE+3*M,config_sha256='test');self.assertEqual(a['active'],b['active'])
    def test_notification_only_skips_execution_sync(self):
        self.intake(self.newscope,102)
        with patch('experimental_execution_bridge.approved_enabled',return_value=True),patch('approved_alert_outbox.synchronize',side_effect=AssertionError('execution forbidden')):
            store.transact(self.newscope,BASE+2*M,'test',lambda x:None)
    def test_rule_not_in_execution_contract(self):
        import approved_alert_contract,experimental_execution_contract
        self.assertNotIn(w.RULE_ID,approved_alert_contract.SPECS)
        self.assertNotIn(w.RULE_ID,experimental_execution_contract.SPECS)

class WorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_delivery_uses_new_renderer_once_and_vetoes_partial(self):
        for high,expected in [(99.5,'DELIVERED'),(101,'CANCELLED_STALE_PRICE')]:
            db=MemoryDatabase();now=BASE+3*M+10000
            def fetch(symbol,start,end):return [bar(t,99,high,98.9,99) for t in range(start,end,M)]
            worker=w.DogePartialWorker(cache=parent.SolPriceCache(fetch),clock=lambda:now)
            worker.bot=SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(message_id=55)))
            worker.subscription=lambda:(True,123);scope=worker.scope_for(123)
            with patch.object(store,'_connect',db.connect):
                await worker.initialize(scope)
                store.transact(scope,now,worker.config_sha256,lambda x:x.update(filled()))
                await worker.deliver(scope,123);await worker.deliver(scope,123)
                self.assertEqual(store.snapshot(scope)['intents'][0]['status'],expected)
                if expected=='DELIVERED':
                    worker.bot.send_message.assert_awaited_once()
                    self.assertIn('75%',worker.bot.send_message.call_args.kwargs['text'])
                else:worker.bot.send_message.assert_not_awaited()

if __name__=='__main__':unittest.main()
