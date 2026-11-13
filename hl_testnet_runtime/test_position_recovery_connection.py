"""Full production adapters against synthetic raw exchange endpoints."""
from copy import deepcopy
from decimal import Decimal
import unittest
from unittest.mock import patch
from . import test_experimental_live_service as connection
from .test_experimental_execution_runtime import T
from .test_maxpain_execution import ARM
from experimental_execution_fixtures import hype_row71205_message, sol_g65_message, r2732_message
from . import position_recovery as recovery, experimental_execution_runtime as core


class RecoveryConnectionTests(unittest.TestCase):
    setUp=connection.FullConnectionTests.setUp
    build=connection.FullConnectionTests.build
    reconnect=connection.FullConnectionTests.reconnect
    ingest=connection.FullConnectionTests.ingest
    seed=connection.FullConnectionTests.seed

    def test_incomplete_hype_history_keeps_actual_exposure_but_allows_xrp(self):
        self.oracle.t=ARM+10000;self.build(ARM-120000)
        self.oracle.instant_fraction=Decimal('.75')
        from .test_approved_alert_lifecycle import alert
        sol=r2732_message(entry=2.3,decision_ms=ARM-60000)
        self.oracle.mark[core._lane(self.release['routes']['short_account'],'XRP')]=sol['entry']
        hype=hype_row71205_message(entry=100,decision_ms=ARM-60000)
        self.seed(hype);self.assertEqual(self.service.tick().get('operation'),'ENTRY')
        oid=self.oracle.oid('ENTRY');order=self.oracle.orders[oid]['view']
        self.assertLess(sum(Decimal(f['quantity']) for f in order['fills']),Decimal(order['wire_order']['s']))
        order['status']='FILLED' # Venue filled status contradicts incomplete per-order fills.
        self.oracle.t+=1001;self.service.tick()
        value=self.store.load();account=value['routes']['short_account'];lane=core._lane(account,'HYPE')
        self.assertEqual(value['blocked_lanes'][lane],'FILLED_ORDER_HISTORY_INCOMPLETE')
        self.assertTrue(any(r['phase']=='OUTCOME_UNKNOWN' for r in value['requests'].values()))
        observed=value['external_activity']['accounts'][account]['positions']['HYPE']
        self.assertNotEqual(Decimal(observed),0)
        self.seed(sol);self.oracle.mark[core._lane(account,'XRP')]=sol['entry'];result=self.service.tick()
        self.assertEqual(result.get('operation'),'ENTRY',result)
        self.assertEqual(self.oracle.requests[-1]['proposal']['symbol'],'XRP')
        self.assertEqual(self.store.load()['external_activity']['accounts'][account]['positions']['HYPE'],observed)
        self.assertEqual(len([r for r in self.http if r['action']['type']=='order']),2)

    def test_persistent_unknown_inquiry_stops_but_shared_inventory_continues(self):
        msg=r2732_message(entry=2.3,decision_ms=T-60000);self.seed(msg);self.service.tick()
        self.drop_stop=True;self.service.tick();self.oracle.t+=5001
        self.service.tick();cid=msg['occurrence_id']
        self.assertEqual(self.store.load()['position_recovery'][cid]['phase'],'RECOVERING')
        for _ in range(6):self.oracle.t+=1001;self.service.tick()
        row=self.store.load()['position_recovery'][cid]
        self.assertEqual(row['phase'],'WAITING');self.assertLessEqual(row['read_weight'],720)
        self.assertEqual(sum(r['proposal']['operation']=='CREATE_EXIT' for r in self.store.load()['requests'].values()),1)
        before_http=len(self.http) # Existing bounded emergency-close policy is unchanged.
        before=len(self.raw.calls)
        for _ in range(4):self.oracle.t+=1001;self.service.tick()
        extra=self.raw.calls[before:]
        self.assertFalse(any(c[0] in ('lookup','orderStatus') for c in extra),extra)
        self.assertTrue(any(c[0]=='clearinghouseState' for c in extra))
        self.assertEqual(len(self.http),before_http)

    def test_lost_response_for_existing_exit_reconciles_without_duplicate(self):
        msg=r2732_message(entry=2.3,decision_ms=T-60000);self.seed(msg);self.service.tick()
        self.fail_reply=True;self.service.tick();self.oracle.t+=5001
        for _ in range(4):self.oracle.t+=1001;self.service.tick()
        self.assertEqual(len(self.http),3)
        self.assertEqual({o['leg'] for o in self.oracle.orders.values()},{'ENTRY','STOP','TAKE_PROFIT'})

    def test_shared_inventory_error_still_blocks_new_entry(self):
        self.oracle.t=ARM+10000;self.build(ARM-120000)
        msg=r2732_message(entry=2.3,decision_ms=ARM-60000);self.seed(msg)
        original=self.raw.read
        def read(kind,*args,**kwargs):
            if kind=='clearinghouseState':raise ValueError('COMPLETE_RAW_ACCOUNT_INVENTORY_REQUIRED')
            return original(kind,*args,**kwargs)
        with patch.object(self.raw,'read',side_effect=read):self.service.tick()
        self.assertFalse(self.http)
