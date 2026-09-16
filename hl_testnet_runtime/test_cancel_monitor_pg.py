"""Real disposable PostgreSQL, real cancellation engine, simulated exchange only."""
from copy import deepcopy
import os
import threading
import unittest
from unittest.mock import patch
from . import pending_cancel_monitor as m
from . import pending_cancel_executor as executor
from . import test_half_threshold_cancel as fixtures
A=fixtures.A

B='0x'+'2'*40


class ExchangeFixture:
    def __init__(self,case):
        self.case=case;self.status='open';self.mark='100';self.remaining=case.r['size'];self.position='0'
        self.requests_attempted=self.cancel_requests_attempted=0
    def observe(self):
        r=self.case.r
        return dict(order={'status':'order','order':{'status':self.status,'order':{
            'cloid':r['entry_cloid'],'coin':r['symbol'],'side':'B','reduceOnly':False,
            'isTrigger':False,'orderType':'Limit','limitPx':r['entry'],
            'origSz':r['size'],'sz':self.remaining}}},
            position_size=self.position,mark=self.mark,sample_age_seconds=0)
    def write(self,action,nonce,*,phase,position=None):
        self.case.assertEqual(phase,'cancel')
        saved=executor.Operations(self.case.journal).get(self.case.key,phase)
        self.case.assertEqual(saved['action'],action)
        self.case.assertEqual(action,executor.cancel_action(self.case.rec))
        self.requests_attempted+=1;self.cancel_requests_attempted+=1;self.status='canceled'
    def child_states(self):return ['canceled','canceled']
    def verify_original(self):return {'verified':True}


@unittest.skipUnless(os.environ.get('HL_JOURNAL_CI_URL'),'Requires disposable local PostgreSQL')
class MonitorPostgresTests(unittest.TestCase):
    def setUp(self):
        fixtures.PostgresCancelTests.setUp(self)
        self.store=m.Store(self.journal,A);self.store.initialize()
        self.exchange=ExchangeFixture(self)
        self.patch=patch.object(executor,'Exchange',return_value=self.exchange)
        self.patch.start();self.addCleanup(self.patch.stop)
        self.network=patch('http.client.HTTPSConnection',side_effect=AssertionError('No network'))
        self.network.start();self.addCleanup(self.network.stop)
        self.stop=threading.Event()
        self.mon=self.make_monitor()
    def make_monitor(self):
        return m.Monitor(m.Config(A,B),m.Store(self.journal,A),executor.cancel_registered_once,
                         lambda a:{'open_entry_orders':int(self.exchange.status=='open'),'open_exit_orders':0})
    def test_pending_below_then_cross_then_verified_cancel_then_restart(self):
        first=self.mon.tick(self.stop)
        self.assertEqual(first['decisions'],{'WAITING_BELOW_CANCEL_THRESHOLD':1})
        self.assertEqual(self.exchange.requests_attempted,0)
        self.exchange.mark=self.r['cancel_price']
        second=self.mon.tick(self.stop)
        self.assertEqual(second['decisions'],{'CANCELLATION_VERIFIED_FLAT':1})
        self.assertEqual(second['cancel_requests_sent_total'],1)
        third=self.make_monitor().tick(self.stop)
        self.assertEqual(third['active_registered_rules'],0)
        self.assertEqual(self.exchange.requests_attempted,1)
    def test_filled_parent_excluded_and_no_write(self):
        self.exchange.status='filled';self.exchange.remaining='0';self.exchange.position=self.r['size']
        out=self.mon.tick(self.stop)
        self.assertEqual(out['decisions'],{'ENTRY_ALREADY_FILLED_NO_CANCEL':1})
        self.assertEqual(self.make_monitor().tick(self.stop)['active_registered_rules'],0)
        self.assertEqual(self.exchange.requests_attempted,0)
    def test_partial_parent_excluded_and_no_write(self):
        self.exchange.remaining='1';self.exchange.position='1'
        out=self.mon.tick(self.stop)
        self.assertEqual(out['decisions'],{'FILL_OBSERVED_DO_NOT_CANCEL':1})
        self.assertEqual(self.exchange.requests_attempted,0)
    def test_other_account_never_selected(self):
        self.assertEqual(m.Store(self.journal,B).active_keys(),[])
        with self.assertRaises(m.MonitorError):m.Store(self.journal,B).save(self.key,'CANCELLATION_VERIFIED_FLAT')
    def test_old_pass_cannot_reactivate_terminal_record(self):
        self.store.save(self.key,'CANCELLATION_VERIFIED_FLAT')
        self.store.save(self.key,'WAITING_BELOW_CANCEL_THRESHOLD')
        self.assertEqual(self.store.active_keys(),[])
        with self.journal._transaction() as c:
            row=c.execute(f'SELECT status,terminal FROM {m.STATE} WHERE plan_key=%s',(self.key,)).fetchone()
        self.assertEqual(row,('CANCELLATION_VERIFIED_FLAT',True))
    def test_monitor_does_not_modify_source_action_or_receipt(self):
        before=deepcopy(self.journal.load(self.key))
        self.mon.tick(self.stop)
        self.assertEqual(before,self.journal.load(self.key))
    def test_reinitialization_keeps_monitor_state(self):
        self.store.save(self.key,'ENTRY_ALREADY_FILLED_NO_CANCEL')
        self.store.initialize()
        self.assertEqual(self.store.active_keys(),[])


if __name__=='__main__':unittest.main()
