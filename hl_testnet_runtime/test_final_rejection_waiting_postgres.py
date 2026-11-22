"""Product-adjudicated no-order rejection must not strand later exit proof.

Real PostgreSQL, authenticated intake and product provider/runtime/dispatch.
Only raw venue HTTPS/read/signature boundaries are controlled. No terminal
request flag or rejection evidence is hand-written by these acceptance tests.
"""
from copy import deepcopy
from decimal import Decimal
import json
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from . import experimental_live_dispatch as dispatch
from . import position_recovery as recovery
from . import test_recovery_isolation_postgres as support


@unittest.skipUnless(support.CI, 'Disposable loopback PostgreSQL required')
class FinalRejectionWaitingPostgresTests(unittest.TestCase):
    metrics = []
    setUp = support.RecoveryIsolationPostgresTests.setUp
    tearDown = support.RecoveryIsolationPostgresTests.tearDown
    tick = support.RecoveryIsolationPostgresTests.tick
    restart = support.RecoveryIsolationPostgresTests.restart

    @classmethod
    def tearDownClass(cls):
        value = dict(version='settled-no-oid-waiting-postgres-v1',
            production_requests=0, tests=cls.metrics)
        target = os.environ.get('FINAL_REJECTION_ACCEPTANCE_METRICS')
        if target:
            Path(target).write_text(json.dumps(value, indent=2)+'\n')
        print('FINAL_REJECTION_ACCEPTANCE_METRICS='+json.dumps(value,separators=(',',':')))

    def reject_take_at_http_boundary(self):
        original = dispatch.http.client.HTTPSConnection
        fx = self.fx
        class RejectTake:
            def __init__(self, host, timeout):
                self.inner = original(host, timeout)
                self.rejected = False
            def request(self, method, path, body, headers):
                data = json.loads(body)
                request = next(r for r in fx.store.load()['requests'].values()
                    if r['nonce'] == data['nonce'] and r['proposal']['action'] == data['action'])
                if request['proposal']['operation'] == 'CREATE_EXIT' and request['proposal']['leg'] == 'TAKE_PROFIT':
                    fx.http.append(data)
                    self.rejected = True
                    self.reply = dict(status='ok',response=dict(type='order',data=dict(
                        statuses=[dict(error='Insufficient margin to place order.') ])))
                    return
                self.inner.request(method,path,body,headers)
            def getresponse(self):
                if not self.rejected:
                    return self.inner.getresponse()
                raw = json.dumps(self.reply).encode()
                return SimpleNamespace(status=200,read=lambda n:raw[:n])
            def close(self):
                self.inner.close()
        guard = patch.object(dispatch.http.client,'HTTPSConnection',RejectTake)
        guard.start(); self.addCleanup(guard.stop)

    def rejected_tp(self):
        self.reject_take_at_http_boundary()
        msg = deepcopy(support.FROZEN['sol_source']); self.cid = msg['occurrence_id']
        self.fx.seed(msg)
        self.assertEqual(self.tick().get('operation'),'ENTRY')
        self.fx.oracle.t = support.FROZEN['sol_fills'][0]['at_ms']-1
        self.fx.oracle.fill(support.SOL_OIDS[0],'3.05','65.469')
        self.assertEqual(self.tick(250).get('operation'),'CREATE_EXIT')
        self.assertIn(support.SOL_OIDS[1],self.fx.oracle.orders)
        self.tick(250)
        state = self.fx.store.load()
        stop = next(r for r in state['requests'].values() if r['proposal']['leg']=='STOP')
        self.assertEqual((stop['phase'],stop['observed_oid']),('OBSERVED',support.SOL_OIDS[1]))
        self.assertEqual(state['trades'][self.cid]['orders'][support.SOL_OIDS[1]]['status'],'OPEN')
        request = next(r for r in state['requests'].values() if r['proposal']['leg']=='TAKE_PROFIT')
        self.assertEqual(request['phase'],'OUTCOME_UNKNOWN')
        self.assertEqual(request['reply']['state'],'REJECTED')
        self.assertIsNone(request['observed_oid'])
        self.assertNotIn(support.SOL_OIDS[2],self.fx.oracle.orders)
        self.assertEqual(len(self.fx.http),3)
        return request['request_id']

    def fill_stop(self):
        fill = support.FROZEN['sol_fills'][1]
        self.fx.oracle.t = max(self.fx.oracle.t,fill['at_ms']-1)
        self.fx.oracle.fill(fill['oid'],fill['quantity'],fill['price'])

    def test_product_rejected_tp_allows_waiting_stop_completion_once(self):
        rid = self.rejected_tp()
        self.tick(1000)
        state = self.fx.store.load(); request = state['requests'][rid]
        self.assertEqual((request['phase'],request['terminal_state'],request['observed_oid']),
            ('OBSERVED','REJECTED_NO_ORDER',None))
        self.assertTrue(any(e.get('kind')=='REJECTED_NO_ORDER' and e.get('request_id')==rid
            for e in state['events']))
        before = deepcopy(state['position_recovery'][self.cid])
        self.assertEqual(before['phase'],'WAITING')
        self.assertNotIn('choice',before)
        self.assertEqual(len(self.fx.http),3)
        frozen_request = deepcopy(request)
        reads = len(self.read_metrics)
        # Restart before observation proves that only durable adjudication is used.
        self.restart(); self.tick(5000)
        state = self.fx.store.load()
        self.assertEqual(state['position_recovery'][self.cid]['phase'],'WAITING')
        self.assertNotEqual(state['trades'][self.cid]['phase'],'CLOSED')
        self.assertEqual(len(self.fx.http),3)
        self.fill_stop(); self.tick(5000)
        state = self.fx.store.load(); trade = state['trades'][self.cid]
        self.assertEqual(trade['phase'],'CLOSED',state.get('blocked_lanes'))
        row = state['position_recovery'][self.cid]
        self.assertEqual((row['phase'],row['reason']),('RESOLVED','POSITION_CLOSED'))
        for key in ('round','read_calls','read_weight','sends','started_ms','deadline_ms'):
            self.assertEqual(row[key],before[key])
        self.assertNotIn('choice',row)
        self.assertEqual(state['requests'][rid],frozen_request)
        self.assertEqual(Decimal(trade['settlement']['fees_usd']),Decimal('.120795'))
        accounting = deepcopy(state['formula_accounting'])
        facts = deepcopy(trade['fee_facts'])
        self.restart()
        for _ in range(3):self.tick(5000)
        state = self.fx.store.load()
        self.assertEqual(state['formula_accounting'],accounting)
        self.assertEqual(state['trades'][self.cid]['fee_facts'],facts)
        self.assertEqual(state['requests'][rid],frozen_request)
        self.assertEqual(sum(e.get('kind')=='REJECTED_NO_ORDER' and e.get('request_id')==rid
            for e in state['events']),1)
        self.assertEqual(len(self.fx.http),3)
        self.assertFalse(any(r['kind']=='lookup' for r in self.read_metrics[reads:]))

    def test_raw_rejection_without_adjudication_remains_waiting_after_stop(self):
        rid = self.rejected_tp()
        # No verification tick: raw rejection remains OUTCOME_UNKNOWN. The
        # existing round deadline moves it to WAITING before another inquiry.
        self.fx.oracle.t += recovery.WINDOW_MS+1
        self.tick()
        state = self.fx.store.load(); before = deepcopy(state['position_recovery'][self.cid])
        self.assertEqual(before['phase'],'WAITING')
        self.assertEqual(state['requests'][rid]['phase'],'OUTCOME_UNKNOWN')
        self.assertNotIn('terminal_state',state['requests'][rid])
        self.assertFalse(any(e.get('kind')=='REJECTED_NO_ORDER' and e.get('request_id')==rid
            for e in state['events']))
        reads = len(self.read_metrics)
        self.fill_stop(); self.restart()
        for _ in range(3):self.tick(5000)
        state = self.fx.store.load()
        self.assertEqual(state['position_recovery'][self.cid]['phase'],'WAITING')
        self.assertNotEqual(state['trades'][self.cid]['phase'],'CLOSED')
        self.assertEqual(state['requests'][rid]['phase'],'OUTCOME_UNKNOWN')
        self.assertNotIn('settlement',state['trades'][self.cid])
        for key in ('round','read_calls','read_weight','sends'):
            self.assertEqual(state['position_recovery'][self.cid][key],before[key])
        self.assertEqual(len(self.fx.http),3)
        self.assertFalse(any(r['kind']=='lookup' for r in self.read_metrics[reads:]))


if __name__ == '__main__':
    unittest.main()
