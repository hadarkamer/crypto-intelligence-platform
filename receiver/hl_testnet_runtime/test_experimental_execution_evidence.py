"""Trusted collector normalization with raw fixtures, no network capability."""
from copy import deepcopy
import unittest
from . import experimental_execution_evidence as evidence
from . import test_experimental_dispatch_integration as integration
from . import card_lifecycle as life

class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.h=integration.DispatchIntegrationTests();self.h.setUp();self.addCleanup(self.h.doCleanups)
        self.h.worker.receive([self.h.msg]);self.h.worker.run_once();self.h.worker.run_once()
        self.state=self.h.store.load();self.trade=self.state['trades'][self.h.msg['occurrence_id']]
        self.rawsnap=next(s for s in self.h.venue.collect(self.state)['snapshots'] if s['account']==self.trade['account'])
        self.owner,self.snapshot=integration.legacy_view(self.trade,self.rawsnap)
        self.lookups={}
        for row in self.rawsnap['orders']:
            o=row['wire_order'];trigger=o['t'].get('trigger')
            self.lookups[row['cloid']]=dict(status='order',order=dict(status={'OPEN':'open','FILLED':'filled','CANCELED':'canceled','REJECTED':'rejected'}[row['status']],statusTimestamp=row['at_ms'],order=dict(oid=int(row['oid']),cloid=row['cloid'],coin=self.trade['symbol'],side='B' if o['b'] else 'A',reduceOnly=o['r'],origSz=o['s'],limitPx=o['p'],orderType='Limit' if not trigger else 'Stop Market' if trigger['tpsl']=='sl' else 'Take Profit Limit',isPositionTpsl=False,isTrigger=trigger is not None,triggerPx=None if not trigger else trigger['triggerPx'])))
    def convert(self):return evidence.normalize_snapshot(self.state,self.snapshot,self.lookups,now_ms=self.h.venue.t)
    def test_exact_lookup_and_complete_fill_snapshot_normalize_worker_shape(self):
        actual=self.convert();self.assertEqual(actual,self.rawsnap)
    def test_unknown_oid_cannot_erase_previously_observed_order(self):
        request=next(r for r in self.state['requests'].values() if r['observed_oid'])
        cloid=request['proposal']['action']['orders'][0]['c'];self.lookups[cloid]={'status':'unknownOid'}
        with self.assertRaisesRegex(evidence.EvidenceError,'PREVIOUSLY_OWNED'):self.convert()
    def test_changed_raw_wire_terms_do_not_become_owned(self):
        next(iter(self.lookups.values()))['order']['order']['origSz']='1'
        with self.assertRaisesRegex(ValueError,'TERMS_NOT_BOUND'):self.convert()
    def test_terminal_fill_quantity_mismatch_is_rejected(self):
        self.snapshot['terminal_orders'][0]['filled_quantity']='1'
        with self.assertRaisesRegex(evidence.EvidenceError,'FILL_HISTORY_INCOMPLETE'):self.convert()
    def test_unowned_order_in_complete_snapshot_is_rejected(self):
        row=deepcopy(self.snapshot['terminal_orders'][0]);row['oid']='999';self.snapshot['terminal_orders'].append(row)
        with self.assertRaisesRegex(evidence.EvidenceError,'UNOWNED_OR_UNPROVEN'):self.convert()
    def test_live_mark_preserves_original_time_and_cannot_be_mainnet_trade(self):
        at=self.h.venue.t-100
        raw=[dict(universe=[dict(name=self.trade['symbol'],szDecimals=2,maxLeverage=10)]),[dict(markPx='2.3')]]
        mark=evidence.mark_sample(raw,account=self.trade['account'],symbol=self.trade['symbol'],observed_at_ms=at,now_ms=self.h.venue.t)
        self.assertEqual(mark['at_ms'],at)
        with self.assertRaises(evidence.EvidenceError):evidence.mark_sample(raw,account=self.trade['account'],symbol=self.trade['symbol'],observed_at_ms=at-16000,now_ms=self.h.venue.t)
        with self.assertRaises((ValueError,KeyError)):evidence.require_mark_window(mark,account=self.trade['account'],symbol=self.trade['symbol'],reference_at_ms=at,now_ms=self.h.venue.t)
    def test_reference_mark_window_requires_independent_full_history_proof(self):
        now=self.h.venue.t;window=dict(environment='testnet',account=self.trade['account'],symbol='XRP',price_kind='MARK',reference_at_ms=now-10000,at_ms=now,history_complete=False,low='2.29',high='2.31',price='2.3')
        with self.assertRaisesRegex(evidence.EvidenceError,'CONTINUOUS_REFERENCE'):evidence.require_mark_window(window,account=self.trade['account'],symbol='XRP',reference_at_ms=now-10000,now_ms=now)

if __name__=='__main__':unittest.main()
