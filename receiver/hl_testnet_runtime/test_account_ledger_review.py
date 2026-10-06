"""Offline public ledger observations: no private account or network needed."""
from copy import deepcopy
import json
from pathlib import Path
import unittest
from unittest.mock import Mock, patch
from . import account_ledger_review as mod, checks

A='0x'+'1'*40
B='0x'+'2'*40
END=1800000000000


class Reader:
    def __init__(self):
        self.calls=0; self.seen=[]
        self.responses={k:[] for k in mod.KINDS}
    def read(self,kind,account,start,end):
        self.calls+=1;self.seen.append((kind,account,start,end))
        return deepcopy(self.responses[kind])


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.reader=Reader()
        p=patch('http.client.HTTPSConnection',side_effect=AssertionError('No live network'))
        p.start();self.addCleanup(p.stop)
    def run_case(self):
        return mod.review_history(A,client=self.reader,end_ms=END)
    def event(self):
        return dict(time=END-10000,hash='0x'+'3'*64,
            delta=dict(type='internalTransfer',usdc='1000.0',fee='1.0',user=B,destination=A))
    def test_reports_fields_not_guessed_net(self):
        self.reader.responses[mod.KINDS[0]]=[self.event()]
        r=self.run_case();e=r['ledger_entries'][0]
        self.assertEqual(e['amounts'],{'usdc':'1000.0','fee':'1.0'})
        self.assertTrue(e['destination_is_checked_account'])
        self.assertFalse(e['user_is_checked_account'])
        self.assertFalse(r['fee_cause_inferred'])
        self.assertNotIn(A,json.dumps(r));self.assertNotIn(B,json.dumps(r))
    def test_empty_history_not_invented(self):
        r=self.run_case()
        self.assertEqual(r['ledger_entries'],[])
        self.assertEqual(r['reads']['userFillsByTime']['returned_count'],0)
        self.assertEqual(r['public_reads'],3)
    def test_exact_bounded_window_and_actual_user(self):
        self.run_case()
        self.assertEqual(self.reader.seen,[(k,A,END-mod.WEEK_MS,END) for k in mod.KINDS])
    def test_missing_fee_not_imputed(self):
        e=self.event();del e['delta']['fee']
        self.reader.responses[mod.KINDS[0]]=[e]
        self.assertNotIn('fee',self.run_case()['ledger_entries'][0]['amounts'])
    def test_malformed_amount_does_not_suppress_other_reads(self):
        e=self.event();e['delta']['fee']='NaN'
        self.reader.responses[mod.KINDS[0]]=[e]
        r=self.run_case();self.assertEqual(r['status'],'PUBLIC_HISTORY_PARTIAL')
        self.assertEqual(r['reads']['userFunding']['status'],'OBSERVED')
    def test_excerpt_truncation_explicit(self):
        self.reader.responses[mod.KINDS[0]]=[self.event() for _ in range(33)]
        r=self.run_case();self.assertTrue(r['ledger_excerpt_truncated'])
        self.assertEqual(len(r['ledger_entries']),32)
        self.assertEqual(r['reads'][mod.KINDS[0]]['returned_count'],33)
    def test_history_outside_window_rejected(self):
        e=self.event();e['time']=END+1
        self.reader.responses[mod.KINDS[0]]=[e]
        self.assertEqual(self.run_case()['status'],'PUBLIC_HISTORY_PARTIAL')
    def test_invalid_address_no_io(self):
        r=mod.review_history('0x'+'1'*64,client=self.reader,end_ms=END)
        self.assertEqual(self.reader.calls,0);self.assertEqual(r['public_reads'],0)
    def test_non_read_type_cannot_reach_network(self):
        with self.assertRaises(checks.Blocked):mod.HistoryReader().read('order',A,END-1,END)
    def test_changed_host_refused(self):
        with patch.object(mod,'HOST','api.hyperliquid.xyz'):
            with self.assertRaises(checks.Blocked):mod.HistoryReader().read(mod.KINDS[0],A,END-1,END)
    def test_fixed_info_transport_no_secret_headers(self):
        response=Mock(status=200);response.read.return_value=b'[]'
        conn=Mock();conn.getresponse.return_value=response
        with patch.object(mod.http.client,'HTTPSConnection',return_value=conn) as ctor:
            mod.HistoryReader().read(mod.KINDS[0],A,END-1,END)
        ctor.assert_called_once_with('api.hyperliquid-testnet.xyz',timeout=4)
        args=conn.request.call_args.args
        self.assertEqual(args[:2],('POST','/info'))
        self.assertEqual(json.loads(args[2]),dict(type=mod.KINDS[0],user=A,startTime=END-1,endTime=END))
        self.assertNotIn('Authorization',args[3])
    def test_redirect_no_retry(self):
        response=Mock(status=302);conn=Mock();conn.getresponse.return_value=response
        with patch.object(mod.http.client,'HTTPSConnection',return_value=conn):
            with self.assertRaises(checks.Blocked):mod.HistoryReader().read(mod.KINDS[0],A,END-1,END)
        self.assertEqual(conn.request.call_count,1)
    def test_no_state_mutation_or_signing(self):
        before=deepcopy(self.reader.responses);r=self.run_case()
        self.assertEqual(self.reader.responses,before)
        self.assertEqual(r['order_requests_sent'],0);self.assertEqual(r['transfers_sent'],0)
        self.assertFalse(r['signing_tested'])
        text=Path(mod.__file__).read_text()
        for value in ('import os','_wallet','sign_l1_action','/exchange'):
            self.assertNotIn(value,text)


if __name__=='__main__':unittest.main(verbosity=2)
