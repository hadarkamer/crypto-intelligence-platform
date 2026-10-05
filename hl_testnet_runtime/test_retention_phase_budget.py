"""Retained recovery under live load, with phase faults and durable fences."""
from contextlib import contextmanager
from copy import deepcopy
import json
import os
import unittest
from unittest.mock import patch

from . import card_sync_evidence as evidence, history_gap_recovery as gap
from . import request_budget as budget
from . import test_history_gap_recovery as recovery_tests
from . import test_history_gap_budget as budget_tests
from .test_card_sync import Reader
from .test_filled_quantity_dispatch import NoExternal


class PhaseFailureTests(NoExternal):
    setup_case=recovery_tests.RecoveryPureTests.setup_case
    def test_each_denied_phase_preserves_durable_history_and_does_not_send(self):
        for denied in (1,2,3):
            with self.subTest(phase=denied):
                state,store,venue,controller,reader=self.setup_case()
                phases=[]
                @contextmanager
                def funded(bodies):
                    phases.append(deepcopy(bodies))
                    if len(phases)==denied:
                        raise budget.BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED')
                    yield
                reader.observation_batch=funded
                with self.assertRaisesRegex(budget.BudgetError,'EXHAUSTED'):
                    gap.step(controller,state,reader=reader)
                self.assertEqual(reader.calls,(0,1,3)[denied-1])
                self.assertEqual(store.load(state['bucket']),state)
                self.assertEqual(venue.sent,0)
                self.assertTrue(gap.needed(state,venue.now()))


@unittest.skipUnless(os.environ.get('HL_JOURNAL_CI_URL'), 'Requires disposable PostgreSQL CI service')
class RetainedPhasePostgresTests(unittest.TestCase):
    setUp=budget_tests.HistoryGapBudgetPostgresTests.setUp
    seed=budget_tests.HistoryGapBudgetPostgresTests.seed
    total=budget_tests.HistoryGapBudgetPostgresTests.total

    def fake_http(self, ev):
        fixture=Reader(ev)
        calls=[];weights=[];owner=self
        class Connection:
            status=200
            def __init__(self,*args,**kwargs):pass
            def request(self,method,path,raw,headers):
                self.body=json.loads(raw)
                calls.append(deepcopy(self.body))
                weights.append(owner.total())
            def getresponse(self):return self
            def read(self,*args):
                body=self.body
                return json.dumps(fixture.read(body['type'],body['user'],
                    start=body.get('startTime'),end=body.get('endTime'),
                    oid=str(body['oid']) if 'oid' in body else None)).encode()
            def close(self):pass
        return Connection,calls,weights

    def test_actual_gap_step_progresses_at_observed_460_load_and_keeps_400_reserve(self):
        pure=recovery_tests.RecoveryPureTests()
        state,store,venue,controller,_=pure.setup_case()
        original=deepcopy(state['evidence'])
        self.seed(460)
        transport,calls,weights=self.fake_http(original)
        reader=evidence.PublicReader(budget=self.budget,priority='background')
        with patch('http.client.HTTPSConnection',transport):
            result=gap.step(controller,state,reader=reader)
        self.assertEqual(result['status'],'HISTORY_GAP_RECOVERY_PROGRESS')
        self.assertEqual(result['cursor_ms'],state[gap.KEY]['cursor_ms']+gap.WINDOW_MS)
        self.assertEqual(result['state']['evidence'],original)
        self.assertEqual(len(calls),4)
        self.assertEqual(calls[0],calls[-1])
        self.assertLessEqual(max(weights),800)
        self.assertEqual(self.total(),544) # anchors and original overlap: 21 per response
        for _ in range(20):
            self.budget.acquire('/info',{'type':'meta'},priority='protection').check()
        self.assertEqual(self.total(),944)
        self.assertEqual(venue.sent,0)

    def test_final_real_collection_can_fit_460_load_without_fabricating_clocks(self):
        pure=recovery_tests.RecoveryPureTests()
        state,store,venue,controller,offline=pure.setup_case()
        while venue.now()-state[gap.KEY]['cursor_ms']>gap.WINDOW_MS:
            state=gap.step(controller,state,reader=offline)['state']
        original=deepcopy(state['evidence'])
        self.seed(460)
        transport,calls,weights=self.fake_http(original)
        reader=evidence.PublicReader(budget=self.budget,priority='background')
        with patch('http.client.HTTPSConnection',transport):
            result=gap.step(controller,state,reader=reader)
        self.assertEqual(result['status'],'HISTORY_GAP_RECOVERY_COMPLETE')
        self.assertNotIn(gap.KEY,result['state'])
        self.assertEqual(result['state']['evidence']['snapshot']['at_ms'],venue.now())
        self.assertEqual(evidence.facts(result['state']['evidence']['snapshot']),
                         evidence.facts(original['snapshot']))
        self.assertEqual(len(calls),8)
        self.assertLessEqual(max(weights),800)
        self.assertEqual(self.total(),586)
        self.assertEqual(venue.sent,0)

    def test_failed_size_refund_does_not_erase_charge_or_release_history_fence(self):
        pure=recovery_tests.RecoveryPureTests()
        state,store,venue,controller,_=pure.setup_case()
        self.seed(460)
        transport,calls,weights=self.fake_http(state['evidence'])
        reader=evidence.PublicReader(budget=self.budget,priority='background')
        with patch('http.client.HTTPSConnection',transport), \
                patch.object(self.budget,'_settle',return_value=False):
            with self.assertRaisesRegex(budget.BudgetError,'EXHAUSTED'):
                gap.step(controller,state,reader=reader)
        self.assertEqual(len(calls),1)
        self.assertEqual(self.total(),580)
        self.assertEqual(store.load(state['bucket']),state)
        self.assertEqual(venue.sent,0)
        self.assertTrue(gap.needed(state,venue.now()))

    def test_capacity_wait_does_not_spend_retention_reads_or_restart_history(self):
        pure=recovery_tests.RecoveryPureTests()
        state,store,venue,controller,_=pure.setup_case()
        self.seed(600)
        reader=evidence.PublicReader(budget=self.budget,priority='background')
        for _ in range(3):
            with self.assertRaises(budget.BudgetError) as caught:
                gap.step(controller,state,reader=reader)
            self.assertEqual(caught.exception.requested_weight,261)
            self.assertEqual(caught.exception.used_weight,600)
            self.assertEqual(self.total(),600)
            self.assertEqual(reader.calls,0)
            self.assertEqual(store.load(state['bucket']),state)
        self.connection.assert_not_called()
        self.assertEqual(venue.sent,0)
