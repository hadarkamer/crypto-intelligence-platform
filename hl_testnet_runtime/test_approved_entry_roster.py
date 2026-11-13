"""New-entry withdrawal must not retire historical ownership or protection."""
from copy import deepcopy
import os
import unittest
from unittest.mock import patch
import approved_alert_contract as contract
from experimental_execution_fixtures import r2732_message,hype_row71205_message,sol_g65_message
from . import test_experimental_live_runtime as support
from .test_approved_alert_lifecycle import alert
from .test_maxpain_execution import ARM,REPORT_FORMULAS,report_plan
from . import experimental_execution_dispatch as boundary
from .test_experimental_execution_dispatch import approved_fixture

class RosterContract:
    def setUp(self):
        support.RuntimeContract.setUp(self)
        self.venue.t=ARM+10000
        self.make_worker(not_before=ARM-120000)
    make_worker=support.RuntimeContract.make_worker
    cycle=support.RuntimeContract.cycle
    restart=support.RuntimeContract.restart
    real_pg=False

    def test_only_implemented_approved_identities_enter(self):
        withdrawn=[sol_g65_message(reference=100,decision_ms=ARM-60000)]
        withdrawn += [alert(symbol=symbol,side=side,approved_ms=ARM,cycle='withdrawn-'+symbol)
                      for symbol,side in [('SOL','SHORT'),('HYPE','LONG'),('ETH','LONG')]]
        for spec in REPORT_FORMULAS:
            self.assertFalse(contract.new_entry_allowed(report_plan(spec,(spec[2]+spec[3])/2)))
        for msg in withdrawn:
            self.worker.receive([msg]);self.cycle()
        self.assertFalse(self.venue.requests);self.assertFalse(self.store.load()['trades'])
        for factory in (r2732_message,hype_row71205_message):
            msg=factory(entry=2.3 if factory==r2732_message else 100,decision_ms=ARM-60000)
            self.worker.receive([msg]);self.cycle()
            for _ in range(3):self.cycle(False)
            self.assertIn(msg['occurrence_id'],self.store.load()['trades'])
        self.assertEqual(sum(r['proposal']['operation']=='ENTRY' for r in self.venue.requests),2)

    def test_existing_withdrawn_trade_keeps_exits_accounting_and_unknown(self):
        msg=sol_g65_message(reference=100,decision_ms=ARM-60000)
        # Represent an entry accepted BEFORE withdrawal; all subsequent cycles
        # run the real current policy. No policy bypass exists in production.
        with patch.object(contract,'new_entry_allowed',return_value=True):
            self.worker.receive([msg]);self.cycle()
        cid=msg['occurrence_id'];before=deepcopy(self.store.load()['requests'])
        entry=self.venue.oid('ENTRY');self.venue.fill(entry,self.venue.orders[entry]['view']['wire_order']['s'])
        self.venue.mark[self.venue._key(self.store.load()['routes']['short_account'],'SOL')]=msg['entry']
        self.restart()
        for _ in range(3):self.cycle(False)
        state=self.store.load();self.assertIn(cid,state['trades'])
        self.assertEqual({r['proposal']['leg'] for r in state['requests'].values()},{'ENTRY','STOP','TAKE_PROFIT'})
        for key,row in before.items():self.assertEqual(state['requests'][key]['attempts'],row['attempts'])
        self.assertEqual(len(self.venue.requests),3)
        stop=self.venue.oid('STOP');self.venue.fill(stop,self.venue.orders[stop]['view']['wire_order']['s'])
        for _ in range(4):self.cycle(False)
        self.assertEqual(sum(r['proposal']['operation']=='ENTRY' for r in self.venue.requests),1)
        self.assertEqual(self.store.load()['trades'][cid]['phase'],'CLOSED')

    def test_pending_unknown_withdrawn_entry_is_not_replayed_after_restart(self):
        msg=sol_g65_message(reference=100,decision_ms=ARM-60000)
        self.venue.lose_reply=True
        with patch.object(contract,'new_entry_allowed',return_value=True):
            self.worker.receive([msg]);self.cycle()
        before=deepcopy(self.store.load()['requests'])
        self.assertTrue(any(r['phase']=='OUTCOME_UNKNOWN' for r in before.values()))
        self.restart()
        for _ in range(4):self.cycle(False)
        self.assertEqual(sum(r['proposal']['operation']=='ENTRY' for r in self.venue.requests),1)
        for key,row in before.items():
            self.assertIn(key,self.store.load()['requests'])
            self.assertEqual(self.store.load()['requests'][key]['attempts'],row['attempts'])

class RosterTests(RosterContract,unittest.TestCase):pass
@unittest.skipUnless(os.environ.get('HL_JOURNAL_CI_URL'),'Disposable PostgreSQL required')
class RosterPostgresTests(RosterContract,unittest.TestCase):real_pg=True

class FinalBoundaryTests(unittest.TestCase):
    def test_prepared_old_formula_entry_refused_but_maintenance_allowed(self):
        request,context,now=approved_fixture()
        with self.assertRaisesRegex(boundary.BoundaryError,'FORMULA_NEW_ENTRIES_DISABLED'):
            boundary._source(context['source'],context['current_source'],now,entry=True)
        boundary._source(context['source'],context['current_source'],now,entry=False)

from . import test_experimental_live_service as connection, position_recovery as recovery
class StaleCheckpointRecoveryTests(unittest.TestCase):
    setUp=connection.FullConnectionTests.setUp
    build=connection.FullConnectionTests.build
    reconnect=connection.FullConnectionTests.reconnect
    ingest=connection.FullConnectionTests.ingest
    seed=connection.FullConnectionTests.seed

    def test_choice_after_minute_proves_complete_gap_without_resending(self):
        msg=r2732_message(entry=2.3,decision_ms=support.T-60000);self.seed(msg)
        for _ in range(4):self.service.tick()
        self.assertEqual(len(self.http),3);cid=msg['occurrence_id']
        read=self.raw.read
        def lost(kind,*args,**kwargs):
            if kind=='orderStatus':raise ValueError('PUBLIC_READ_UNAVAILABLE')
            value=read(kind,*args,**kwargs)
            return [o for o in value if o['orderType']!='Stop Market'] if kind=='frontendOpenOrders' else value
        with patch.object(self.raw,'read',side_effect=lost):
            for _ in range(5):self.oracle.t+=5001;self.service.tick()
        row=self.store.load()['position_recovery'][cid]
        self.assertEqual(row['phase'],'WAITING');self.assertFalse(row['facts']['complete'])
        self.oracle.t+=65000
        answer=self.store.mutate(lambda s:recovery.choose(s,token=row['event_id'],round_number=row['round'],
            choice='auto',actor='7011666653',allowed_users={'7011666653'},now=self.oracle.now()))
        self.assertEqual(answer['status'],'RECORDED');self.reconnect()
        for _ in range(4):self.oracle.t+=1001;self.service.tick()
        row=self.store.load()['position_recovery'][cid]
        self.assertEqual(row['phase'],'RESOLVED');self.assertTrue(row['facts']['complete'])
        self.assertEqual(row['facts']['exits'],{'STOP':'PRESENT','TAKE_PROFIT':'PRESENT'})
        self.assertLessEqual(row['read_calls'],24);self.assertLessEqual(row['read_weight'],720)
        self.assertEqual(len(self.http),3)
