"""Offline evidence-pass reuse; no network, signatures or live account state."""
from collections import Counter
from contextlib import contextmanager
from copy import deepcopy
import unittest
from unittest.mock import patch

from . import card_sync_evidence as sync
from .test_card_sync import evidence, raw_state
from .test_card_lifecycle import binding, T


class Reader:
    def __init__(self, values):
        self.facts=[raw_state(ev) for ev in values]
        self.calls=[];self.plans=[];self.fail_plan=False;self.changed_second=False

    @contextmanager
    def observation_batch(self, bodies):
        self.plans.append(deepcopy(bodies))
        if self.fail_plan:raise sync.SyncError('TESTNET_REQUEST_BUDGET_EXHAUSTED')
        yield

    def read(self, kind, account, *, oid=None, start=None, end=None):
        self.calls.append((kind, account, oid, start, end))
        if kind=='userFillsByTime':
            return [deepcopy(f) for facts in self.facts for f in facts['fills'] if start<=f['time']<=end]
        if kind=='frontendOpenOrders':return []
        if kind=='clearinghouseState':
            n=sum(c[0]==kind for c in self.calls)
            return dict(assetPositions=[dict(position=dict(coin=self.facts[0]['fills'][0]['coin'],
                        szi='1'))] if self.changed_second and n==2 else [])
        if kind=='orderStatus':
            return next(deepcopy(v['statuses'][oid]) for v in self.facts if oid in v['statuses'])
        raise AssertionError(kind)


class ObservationCoalescingTests(unittest.TestCase):
    def setUp(self):
        p=patch('http.client.HTTPSConnection',side_effect=AssertionError('NETWORK_FORBIDDEN'))
        p.start();self.addCleanup(p.stop)
        self.one=evidence();b=binding(2);b['symbol']='SOL';self.two=evidence(b)
        self.reader=Reader([self.one,self.two])
        self.cache=sync.ObservationReadCache(self.reader)

    def collect(self,value,cache=None):
        return sync.collect(value,self.reader,clock=lambda:T+10000,
            reuse_verified_terminals=True,observation_cache=cache or self.cache,
            observation_end_ms=T+10000)

    def test_multiple_coins_share_account_fills_and_inventory_only_within_each_pass(self):
        first=self.collect(self.one);second=self.collect(self.two)
        self.assertFalse(first['report']['needs_review']);self.assertFalse(second['report']['needs_review'])
        self.assertEqual(Counter(c[0] for c in self.reader.calls),
            Counter(userFillsByTime=2,frontendOpenOrders=2,clearinghouseState=2))
        self.assertEqual(len(self.reader.plans),1)
        self.assertEqual(len(self.reader.plans[0]),6)
        self.assertEqual(second['snapshot']['at_ms'],T+10000)
        self.assertEqual(self.one,evidence())

    def test_precollected_first_inventory_is_not_funded_or_read_twice(self):
        account=self.one['snapshot']['account'];first=self.cache.pass_reader(0)
        first.read('frontendOpenOrders',account);first.read('clearinghouseState',account)
        self.collect(self.one)
        counts=Counter(body['type'] for body in self.reader.plans[0])
        self.assertEqual(counts,Counter(userFillsByTime=2,frontendOpenOrders=1,clearinghouseState=1))
        self.assertEqual(len(self.reader.calls),6)

    def test_second_verification_pass_is_an_independent_transport_observation(self):
        self.reader.changed_second=True
        with self.assertRaisesRegex(sync.SyncError,'OBSERVATION_CHANGED_RETRY'):
            self.collect(self.one)
        self.assertEqual(sum(c[0]=='clearinghouseState' for c in self.reader.calls),2)

    def test_complete_missing_plan_is_funded_before_any_uncached_transport(self):
        self.reader.fail_plan=True
        with self.assertRaisesRegex(sync.SyncError,'REQUEST_BUDGET_EXHAUSTED'):
            self.collect(self.one)
        self.assertEqual(self.reader.calls,[])
        self.assertEqual(self.cache.samples,({},{}))

    def test_next_collection_has_two_fresh_passes_and_original_time_is_fixed(self):
        self.collect(self.one)
        self.collect(self.one,sync.ObservationReadCache(self.reader))
        self.assertEqual(len(self.reader.calls),12)
        self.assertEqual(len(self.reader.plans),2)


if __name__=='__main__':unittest.main()
