"""Persistent lifecycle tests use only the existing disposable localhost CI DB."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from copy import deepcopy
import json
import os
import subprocess
import sys
import unittest
from unittest.mock import patch
from . import card_lifecycle as life
from . import card_lifecycle_store as mod
from .test_card_lifecycle import A, B, T, binding, opened, closed

CI=os.environ.get('HL_JOURNAL_CI_URL')


@unittest.skipUnless(CI, 'Requires disposable localhost PostgreSQL')
class LifecyclePostgresTests(unittest.TestCase):
    def setUp(self):
        from .test_trade_cards_phase1 import CardPostgresTests
        CardPostgresTests.setUp(self)  # for_ci validates localhost/disposable DB.
        with self.journal._transaction() as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS {mod.SCHEMA} CASCADE')
        self.lifecycle=mod.LifecycleStore(self.journal)
        self.lifecycle.initialize()

    def save(self,b,s,revision=0):
        return self.lifecycle.save([b] if isinstance(b,dict) else b,s,
            expected_revision=revision,now_ms=s['at_ms'])

    def history_count(self):
        with self.journal._transaction() as conn:
            return conn.execute(f'SELECT count(*) FROM {mod.SCHEMA}.history').fetchone()[0]

    def test_same_delivery_only_one_snapshot_even_after_concurrent_retries(self):
        b=binding();s=opened(b)
        with ThreadPoolExecutor(max_workers=6) as pool:
            rows=list(pool.map(lambda _:self.save(b,s),range(6)))
        self.assertEqual(sum(not r['duplicate'] for r in rows),1)
        self.assertEqual(self.history_count(),1)

    def test_close_updates_projection_and_keeps_old_observation(self):
        b=binding();s=opened(b);self.save(b,s)
        end=closed(b);end['at_ms']=T+1
        out=self.save(b,end,1)
        self.assertTrue(out['report']['cards'][0]['closure_verified']);self.assertEqual(out['revision'],2)
        self.assertEqual(self.history_count(),2)
        with self.journal._transaction() as conn:
            old=conn.execute(f'SELECT evidence FROM {mod.SCHEMA}.history WHERE revision=1').fetchone()[0]
        self.assertEqual(old['snapshot'],s)

    def test_lost_commit_ack_is_safe_to_replay(self):
        original=self.journal._transaction
        @contextmanager
        def lost_ack():
            with original() as conn:yield conn
            raise RuntimeError('simulated lost response after commit')
        b=binding();s=opened(b)
        with patch.object(self.journal,'_transaction',lost_ack):
            with self.assertRaises(RuntimeError):self.save(b,s)
        self.assertTrue(self.save(b,s)['duplicate']);self.assertEqual(self.history_count(),1)

    def test_compare_and_swap_rejects_stale_worker_without_overwrite(self):
        b=binding();self.save(b,opened(b))
        end=closed(b);end['at_ms']=T+2;self.save(b,end,1)
        old=opened(b);old['at_ms']=T+1
        with self.assertRaises(life.LifecycleError):self.save(b,old,1)
        self.assertTrue(self.lifecycle.load(A,'DOGE',now_ms=T+2)['report']['cards'][0]['closure_verified'])
        self.assertEqual(self.history_count(),2)

    def test_two_different_concurrent_updates_only_one_wins(self):
        b=binding();self.save(b,opened(b))
        def send(n):
            s=opened(b);s['at_ms']=T+n
            try:self.save(b,s,1);return 'saved'
            except life.LifecycleError:return 'reload'
        with ThreadPoolExecutor(max_workers=2) as pool:out=list(pool.map(send,[1,2]))
        self.assertEqual(sorted(out),['reload','saved']);self.assertEqual(self.history_count(),2)

    def test_reassigned_card_or_order_is_rejected(self):
        b=binding();self.save(b,opened(b));s=opened(b);s['at_ms']=T+1
        for field,value in [('prices',dict(entry='100',stop='97',take_profit='104')),
                            ('orders',dict(ENTRY=['999'],STOP=['12'],TAKE_PROFIT=['11']))]:
            changed=deepcopy(b);changed[field]=value
            with self.assertRaises(life.LifecycleError):self.save(changed,s,1)
        self.assertEqual(self.history_count(),1)

    def test_recorded_fill_cannot_disappear_or_change(self):
        b=binding();self.save(b,opened(b))
        for mode in ('missing','changed'):
            s=opened(b);s['at_ms']=T+1
            if mode=='missing':s['fills']=[]
            else:s['fills'][0]['fee']='0.2'
            with self.assertRaises(life.LifecycleError):self.save(b,s,1)
        self.assertEqual(self.history_count(),1)

    def test_new_card_is_append_only_not_overwrite(self):
        a,b=binding(),binding(2,qty='60');self.save(a,opened(a));s=opened(a);ob=opened(b)
        s['at_ms']=T+1;s['position_quantity']='160'
        for key in ('fills','open_orders','terminal_orders'):s[key]+=ob[key]
        r=self.save([a,b],s,1)
        self.assertFalse(r['report']['needs_review']);self.assertEqual(len(r['report']['cards']),2)
        with self.assertRaises(life.LifecycleError):
            removed=opened(b);removed['at_ms']=T+2;self.save(b,removed,2)

    def test_corruption_is_not_loaded_as_a_valid_state(self):
        b=binding();self.save(b,opened(b))
        with self.journal._transaction() as conn:
            conn.execute(f"UPDATE {mod.SCHEMA}.heads SET evidence=jsonb_set(evidence,'{{snapshot,position_quantity}}','\"0\"')")
        with self.assertRaises(life.LifecycleError):self.lifecycle.load(A,'DOGE',now_ms=T)

    def test_saved_green_report_is_stale_after_restart_delay(self):
        b=binding();self.save(b,closed(b));r=self.lifecycle.load(A,'DOGE',now_ms=T+20000)
        self.assertIn('STALE_OR_FUTURE_SNAPSHOT',r['report']['bucket_issues'])
        self.assertFalse(r['report']['cards'][0]['closure_verified'])

    def test_readback_in_independent_process(self):
        self.save(binding(),closed(binding()))
        script='''import sys,json,os
from hl_testnet_runtime.postgres_journal import PostgresJournal
from hl_testnet_runtime.card_lifecycle_store import LifecycleStore
r=LifecycleStore(PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL'])).load(sys.argv[1],'DOGE',now_ms=int(sys.argv[2]))
assert r['report']['cards'][0]['closure_verified']
assert r['report']['order_requests_sent']==0
print('RESTART_READBACK_PASSED')'''
        r=subprocess.run([sys.executable,'-c',script,A,str(T)],capture_output=True,text=True,timeout=15)
        self.assertEqual(r.returncode,0,r.stderr);self.assertIn('RESTART_READBACK_PASSED',r.stdout)

    def test_original_card_bridge_and_legacy_records_untouched(self):
        from . import trade_cards
        from .test_trade_cards_phase1 import source, META
        src=trade_cards.prepare_card(source(),META,rule_id='C1',threshold_pct='2',record_kind='received_alert')
        self.store.record(src);before=self.store.load(src['card_id'])
        b=life.binding_from_card(src,A,{'long_account':{'account':A}},binding()['orders'])
        self.assertEqual(b['planned_quantity'],'5')
        self.assertEqual(b['prices'],dict(entry='100',stop='98',take_profit='104'))
        self.save(b,closed(b))
        self.assertEqual(before,self.store.load(src['card_id']))
        with self.journal._transaction() as conn:
            self.assertEqual(conn.execute('SELECT count(*) FROM hl_testnet_execution_v1.attempts').fetchone()[0],0)
        with self.assertRaises(life.LifecycleError):life.binding_from_card(src,B,{'long_account':{'account':A}},binding()['orders'])

    def test_archived_or_unavailable_cards_are_not_promoted(self):
        from . import trade_cards
        from .test_trade_cards_phase1 import source, META
        src=trade_cards.prepare_card(source(),META,rule_id='C1',threshold_pct='2',record_kind='historical_review')
        with self.assertRaises(life.LifecycleError):life.binding_from_card(src,A,{'long_account':{'account':A}},binding()['orders'])

    def test_migration_repeatable_and_no_new_trade_attempts(self):
        self.save(binding(),opened(binding()));self.lifecycle.initialize()
        self.assertEqual(self.history_count(),1)
        with self.journal._transaction() as conn:
            self.assertEqual(conn.execute('SELECT count(*) FROM hl_testnet_execution_v1.attempts').fetchone()[0],0)


if __name__=='__main__':unittest.main(verbosity=2)
