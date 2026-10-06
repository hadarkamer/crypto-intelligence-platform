"""Technical rehearsal tests: no user key and no exchange network."""
from datetime import datetime,timedelta,timezone
from decimal import Decimal
import json,os,unittest
from unittest.mock import patch
from . import cancel_rehearsal as r,price_precision as p,half_threshold_cancel as policy
from .test_half_threshold_cancel import MemoryOps,FakeExchange,rule,record,observation


def environment():
    now=datetime.now(timezone.utc)
    return {'RENDER_SERVICE_ID':r.SERVICE,'HL_TESTNET_RUNTIME_MODE':r.MODE,
        'HL_TESTNET_JOURNAL_BACKEND':'staging_postgres_v1','HL_TESTNET_EXIT_TYPE':'tp_limit_sl_market',
        'HL_TESTNET_PRICE_ROUNDING':p.POLICY,'HL_TESTNET_ACCOUNT_ADDRESS':'0x'+'1'*40,
        'HL_TESTNET_AGENT_ADDRESS':'0x'+'2'*40,'HL_TESTNET_REHEARSAL_TICKET':json.dumps({
            'run_id':r.RUN_ID,'technical_fixture':True,'issued_at':now.isoformat(),
            'expires_at':(now+timedelta(seconds=180)).isoformat()})}


class AuthorizationTests(unittest.TestCase):
    def test_exact_ticket(self):self.assertEqual(r.authorize(environment())[0],'0x'+'1'*40)
    def test_each_gate_required(self):
        for k in ('RENDER_SERVICE_ID','HL_TESTNET_RUNTIME_MODE','HL_TESTNET_JOURNAL_BACKEND','HL_TESTNET_EXIT_TYPE','HL_TESTNET_PRICE_ROUNDING'):
            e=environment();e[k]='wrong'
            with self.assertRaises(r.RehearsalError):r.authorize(e)
    def test_expired_cannot_create(self):
        with self.assertRaises(r.RehearsalError):r.authorize(environment(),datetime.now(timezone.utc)+timedelta(minutes=4))
    def test_future_cannot_create(self):
        with self.assertRaises(r.RehearsalError):r.authorize(environment(),datetime.now(timezone.utc)-timedelta(minutes=1))
    def test_same_account_agent_rejected(self):
        e=environment();e['HL_TESTNET_AGENT_ADDRESS']=e['HL_TESTNET_ACCOUNT_ADDRESS']
        with self.assertRaises(r.RehearsalError):r.authorize(e)
    def test_inspection_expiry_does_not_authorize_writes(self):
        e=environment();e['HL_TESTNET_RUNTIME_MODE']=r.INSPECT
        r.authorize(e,datetime.now(timezone.utc)+timedelta(days=1))
    def test_false_technical_label_or_foreign_run_refused(self):
        for field,value in [('technical_fixture',False),('run_id','another')]:
            e=environment();t=json.loads(e['HL_TESTNET_REHEARSAL_TICKET']);t[field]=value;e['HL_TESTNET_REHEARSAL_TICKET']=json.dumps(t)
            with self.assertRaises(r.RehearsalError):r.authorize(e)
    def test_default_inert(self):
        with patch('http.client.HTTPSConnection',side_effect=AssertionError('no network')):
            self.assertEqual(r.run({})['status'],'DISABLED')
    def test_no_parameter_can_change_symbol_prices_or_endpoint(self):
        e=environment();t=json.loads(e['HL_TESTNET_REHEARSAL_TICKET']);t['symbol']='DOGE';e['HL_TESTNET_REHEARSAL_TICKET']=json.dumps(t)
        with self.assertRaises(r.RehearsalError):r.authorize(e)
    def test_fixture_is_not_bot_alert(self):
        s=r.fixture_source('75000');self.assertEqual(s['event_id'],r.RUN_ID);self.assertEqual(s['symbol'],'BTC')
    def test_fixture_actual_half_range_and_risk_unchanged(self):
        import hyperliquid_testnet_executor as sender
        meta={'universe':[{'name':'BTC','szDecimals':5}]}
        prepared=p.prepare_signal(r.fixture_source('75000'),meta)
        a=sender.build_action(prepared['execution'],meta,'0x'+'1'*40,exit_type='tp_limit_sl_market')
        entry,qty=Decimal(a['orders'][0]['p']),Decimal(a['orders'][0]['s'])
        self.assertLess(entry*Decimal('1.0075'),Decimal(75000));self.assertGreater(Decimal(prepared['execution']['take_profit']),Decimal(75000))
        self.assertLessEqual(qty*(entry-Decimal(prepared['execution']['stop'])),Decimal(20))
        self.assertLessEqual(entry*qty,Decimal(1600))
    def test_invalid_mark(self):
        for value in ('0','1','NaN','Infinity','-1'):
            with self.assertRaises(ValueError):r.fixture_source(value)


class CancellationTests(unittest.TestCase):
    def setUp(self):
        self.ops=MemoryOps();self.ex=FakeExchange(self.ops)
        self.patchers=[patch('hl_testnet_runtime.pending_cancel_executor.Operations',return_value=self.ops),
            patch.object(policy,'remember_crossing'),patch('hl_testnet_runtime.pending_cancel_executor.time.sleep')]
        for x in self.patchers:x.start();self.addCleanup(x.stop)
    def finish(self,allowed=True):return r.finish_fixture('key',record(),rule(),object(),self.ex,allow_cancel=allowed)
    def test_real_engine_pending_to_cancel(self):
        result=self.finish();self.assertTrue(result['test_passed']);self.assertTrue(result['entry_observed_unfilled']);self.assertTrue(result['actual_half_threshold_observed']);self.assertEqual(self.ex.cancel_requests_attempted,1)
    def test_full_fill_is_not_cancel_success(self):
        self.ex.default=observation(status='filled',remaining='0',position='10')
        result=self.finish();self.assertFalse(result['test_passed']);self.assertFalse(self.ex.calls)
    def test_partial_fill_is_not_cancelled(self):
        self.ex.default=observation(remaining='9',position='1');result=self.finish();self.assertFalse(result['test_passed']);self.assertFalse(self.ex.calls)
    def test_inspection_never_cancels(self):
        self.assertFalse(self.finish(False)['test_passed']);self.assertFalse(self.ex.calls)
    def test_below_threshold_cleanup_is_not_a_strategy_pass(self):
        self.ex.default=observation(mark='100.1');result=self.finish()
        self.assertTrue(result['cleanup_only']);self.assertFalse(result['test_passed']);self.assertEqual(self.ex.cancel_requests_attempted,1)
    def test_unknown_status_never_assumed_unfilled(self):
        self.ex.default['order']={'status':'unknownOid'};result=self.finish();self.assertFalse(result['entry_observed_unfilled']);self.assertFalse(self.ex.calls)
    def test_fill_race_preserves_protection_not_false_success(self):
        self.ex.race='partial';result=self.finish();self.assertFalse(result['test_passed']);self.assertTrue(self.ex.repaired)
    def test_lost_cancel_reply_is_not_resent(self):
        self.ex.timeout=True;self.assertTrue(self.finish()['test_passed']);self.assertEqual(self.ex.cancel_requests_attempted,1)


@unittest.skipUnless(os.environ.get('HL_JOURNAL_CI_URL'),'Disposable PostgreSQL required')
class StorageTests(unittest.TestCase):
    def setUp(self):
        from .test_postgres_journal import PostgresTests
        PostgresTests.setUp(self)
        self.store=r.TrialStore(self.journal);self.store.initialize()
        self.account='0x'+'1'*40
        self.key,_=self.journal.save_prepared(self.account,self.prepared)
        self.manifest={'record':{'account':self.account},'rule':{},'before':{}}
    def test_commit_visible_on_new_connection(self):
        from .postgres_journal import PostgresJournal
        n=self.store.reserve(self.account,self.key,self.manifest)
        other=r.TrialStore(PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL']))
        self.assertEqual(other.load(self.account)['nonce'],n)
    def test_no_repeat_reservation(self):
        self.store.reserve(self.account,self.key,self.manifest)
        with self.assertRaises(ValueError):self.store.reserve(self.account,self.key,self.manifest)
    def test_original_attempt_lock_not_removed_or_modified(self):
        import hyperliquid_testnet_executor as sender
        a=sender.build_action(self.prepared['execution'],self.meta,self.account,exit_type='tp_limit_sl_market')
        self.journal.reserve(self.key,a);before=self.journal.load(self.key)
        self.store.reserve(self.account,self.key,self.manifest)
        self.assertEqual(before,self.journal.load(self.key))
    def test_parallel_starters_get_one_reservation(self):
        from concurrent.futures import ThreadPoolExecutor
        def one(_):
            try:self.store.reserve(self.account,self.key,self.manifest);return True
            except ValueError:return False
        with ThreadPoolExecutor(max_workers=4) as pool:out=list(pool.map(one,range(4)))
        self.assertEqual(sum(out),1)


if __name__=='__main__':unittest.main()
