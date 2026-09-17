"""Tests of the historical DOGE adapter; all exchange responses are substitutes."""
from copy import deepcopy
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch

from . import doge_lifecycle_review as m
from . import card_lifecycle as life

A = '0x'+'1'*36+'2b88'
AGENT = '0x'+'2'*40
T = 1789640000000
SOURCE_AT = datetime.fromtimestamp((T-100000)/1000,timezone.utc).isoformat()
CI = os.environ.get('HL_JOURNAL_CI_URL')


def record():
    source = dict(kind='SIGNAL',event_id=m.SOURCE_ID,symbol='DOGE',side='LONG',
        entry='100',stop='98',take_profit='104',at=SOURCE_AT)
    orders = [dict(c='0x'+str(i)*32,s='5',p=px,b=i==1,r=i!=1,
        t={'limit':{'tif':'Gtc'}} if i==1 else {'trigger':{'triggerPx':px,'isMarket':i==3,'tpsl':'tp' if i==2 else 'sl'}})
        for i,px in enumerate(('100','104','98'),1)]
    return dict(plan_key='f'*64,prepared={'source':source,'execution':source,'audit':{}},
        action={'orders':orders},result={'status':'VERIFIED_OPEN_PROTECTED','verified':True})


def env():
    return dict(HL_TESTNET_CLOSED_CARD_REVIEW=m.MODE,RENDER_SERVICE_ID=m.SERVICE,
        HL_TESTNET_RUNTIME_MODE='read_only',HL_TESTNET_JOURNAL_BACKEND='staging_postgres_v1',
        HL_TESTNET_LONG_ACCOUNT_ADDRESS=A,HL_TESTNET_LONG_AGENT_ADDRESS=AGENT)


class Reader:
    def __init__(self, original=None):
        original = original or record()
        self.calls = self.status_calls = self.history_calls = 0
        self.statuses = {}
        for i,(leg,sent) in enumerate(zip(life.LEGS,original['action']['orders']),1):
            self.statuses[sent['c']] = dict(status='order',order=dict(
                status='siblingFilledCanceled' if leg=='STOP' else 'filled',statusTimestamp=T-1000,
                order=dict(coin='DOGE',oid=i,cloid=sent['c'],side='B' if leg=='ENTRY' else 'A',
                    reduceOnly=leg!='ENTRY',origSz=sent['s'],limitPx=sent['p'],
                    triggerPx='0' if leg=='ENTRY' else sent['t']['trigger']['triggerPx'])))
        self.fills = [dict(coin='DOGE',tid=i,oid=i,dir='Open Long' if i==1 else 'Close Long',
            side='B' if i==1 else 'A',sz=original['action']['orders'][0]['s'],px=px,
            fee='0.1',feeToken='USDC',closedPnl='0' if i==1 else '20',time=T-2000)
            for i,px in enumerate(('100','104'),1)]
        self.position = {'assetPositions':[]}
        self.opens = []
        self.change_on_second = False

    def read(self, kind, account, **kw):
        assert account == A
        self.calls += 1
        if kind=='orderStatus':
            self.status_calls += 1
            return deepcopy(self.statuses[kw['oid']])
        if kind=='userFillsByTime':
            self.history_calls += 1
            data=deepcopy(self.fills)
            if self.change_on_second and self.history_calls==2:
                data[0]['fee']='0.2'
            return data
        if kind=='clearinghouseState':return deepcopy(self.position)
        if kind=='frontendOpenOrders':return deepcopy(self.opens)
        raise AssertionError('Unexpected read')


class AdapterTests(unittest.TestCase):
    def setUp(self):
        p=patch('http.client.HTTPSConnection',side_effect=AssertionError('No network'))
        p.start();self.addCleanup(p.stop)

    def collect(self,r=None,original=None):
        return m.collect(original or record(),A,r or Reader(),clock=lambda:T)

    def test_closed_card_and_cash_flow_result(self):
        reader=Reader();b,s,view,comparison=self.collect(reader)
        c=view['cards'][0]
        self.assertTrue(c['closure_verified']);self.assertEqual(c['state'],'CLOSED')
        self.assertEqual(c['remaining_quantity'],'0');self.assertEqual(c['net_before_funding_usdc'],'19.8')
        self.assertFalse(comparison['accounting_review_required']);self.assertEqual(reader.calls,12)
        self.assertFalse(view['dispatch_enabled']);self.assertFalse(view['app_delivery_enabled'])
        self.assertIsNone(c['funding_usdc']);self.assertIsNone(c['final_net_usdc'])

    def test_binding_preserves_original_quantity_and_source(self):
        old=record();old['action']['orders'][0]['s']='10'
        for o in old['action']['orders']:o['s']='10'
        before=deepcopy(old);b,_,_,_=self.collect(Reader(old),old)
        self.assertEqual(b['planned_quantity'],'10');self.assertEqual(old,before)
        self.assertEqual(b['prices'],{'entry':'100','stop':'98','take_profit':'104'})

    def test_same_fill_twice_is_not_double_counted(self):
        r=Reader();r.fills.append(deepcopy(r.fills[0]))
        self.assertEqual(self.collect(r)[2]['cards'][0]['entry_quantity'],'5')

    def test_conflicting_duplicate_is_rejected(self):
        r=Reader();r.fills.append({**r.fills[0],'fee':'1'})
        with self.assertRaisesRegex(m.ReviewError,'CONFLICTING_RAW_FILL'):self.collect(r)

    def test_missing_fill_does_not_claim_history_complete(self):
        r=Reader();r.fills.pop()
        with self.assertRaisesRegex(m.ReviewError,'TERMINAL_QUANTITIES'):self.collect(r)

    def test_partial_fill_is_not_closed(self):
        r=Reader();r.fills[1]['sz']='2'
        with self.assertRaises(m.ReviewError):self.collect(r)

    def test_full_page_refuses_unproven_completeness(self):
        r=Reader();r.fills=[r.fills[0]]*2000
        with self.assertRaisesRegex(m.ReviewError,'TRUNCATED'):self.collect(r)

    def test_other_doge_activity_requires_other_bindings(self):
        r=Reader();r.fills.append({**r.fills[0],'tid':90,'oid':90})
        with self.assertRaisesRegex(m.ReviewError,'OTHER_DOGE'):self.collect(r)

    def test_other_symbol_is_not_assigned_to_doge(self):
        r=Reader();r.fills.append({**r.fills[0],'coin':'ETH','tid':90,'oid':90})
        self.assertTrue(self.collect(r)[2]['cards'][0]['closure_verified'])

    def test_fill_direction_is_checked(self):
        for field,value in [('side','B'),('dir','Open Long'),('feeToken','OTHER')]:
            r=Reader();r.fills[1][field]=value
            with self.assertRaises(m.ReviewError):self.collect(r)

    def test_identity_required_for_each_order(self):
        for field,value in [('oid',True),('cloid','0x'+'a'*32),('coin','ETH'),('reduceOnly',True)]:
            r=Reader();next(iter(r.statuses.values()))['order']['order'][field]=value
            with self.assertRaises(m.ReviewError):self.collect(r)

    def test_original_quantity_and_prices_checked(self):
        for field,value in [('origSz','50'),('limitPx','101')]:
            r=Reader();next(iter(r.statuses.values()))['order']['order'][field]=value
            with self.assertRaises(m.ReviewError):self.collect(r)
        r=Reader();list(r.statuses.values())[1]['order']['order']['triggerPx']='105'
        with self.assertRaises(m.ReviewError):self.collect(r)

    def test_nonterminal_parent_or_stop_rejected(self):
        for leg in (0,2):
            r=Reader();list(r.statuses.values())[leg]['order']['status']='open'
            with self.assertRaisesRegex(m.ReviewError,'NOT_FULLY_TP_CLOSED'):self.collect(r)

    def test_future_status_or_fill_rejected(self):
        r=Reader();r.fills[0]['time']=T+1
        with self.assertRaises(m.ReviewError):self.collect(r)
        r=Reader();list(r.statuses.values())[0]['order']['statusTimestamp']=T+1
        with self.assertRaises(m.ReviewError):self.collect(r)

    def test_fill_after_terminal_rejected(self):
        r=Reader();r.fills[0]['time']=T-10
        with self.assertRaisesRegex(m.ReviewError,'AFTER_TERMINAL'):self.collect(r)

    def test_stop_fill_cannot_be_hidden(self):
        r=Reader();r.fills.append({**r.fills[1],'tid':3,'oid':3})
        with self.assertRaises(m.ReviewError):self.collect(r)

    def test_nonflat_position_and_orphan_order_refused(self):
        r=Reader();r.position={'assetPositions':[{'position':{'coin':'DOGE','szi':'1'}}]}
        with self.assertRaisesRegex(m.ReviewError,'NOT_FLAT'):self.collect(r)
        r=Reader();r.opens=[{'coin':'DOGE','oid':3}]
        with self.assertRaisesRegex(m.ReviewError,'WORKING_ORDERS'):self.collect(r)

    def test_read_failure_not_empty_position(self):
        for pos,opens in [(None,[]),({'assetPositions':[]},None)]:
            r=Reader();r.position=pos;r.opens=opens
            with self.assertRaises(m.ReviewError):self.collect(r)

    def test_changed_observations_are_not_committed(self):
        r=Reader();r.change_on_second=True
        with self.assertRaisesRegex(m.ReviewError,'OBSERVATIONS_CHANGED'):self.collect(r)

    def test_slow_or_expired_read_rejected(self):
        with self.assertRaisesRegex(m.ReviewError,'TOO_SLOW'):
            m.collect(record(),A,Reader(),clock=lambda:T,elapsed=iter([0,16]).__next__)
        with self.assertRaisesRegex(m.ReviewError,'WINDOW_EXCEEDED'):
            m.collect(record(),A,Reader(),clock=lambda:T+m.WEEK_MS)

    def test_exchange_pnl_difference_is_not_erased(self):
        r=Reader();r.fills[1]['closedPnl']='20.009173'
        b,s,view,c=self.collect(r)
        self.assertEqual(c['exchange_minus_cash_flow_usdc'],'0.009173')
        self.assertTrue(c['accounting_review_required'])
        self.assertEqual(view['cards'][0]['net_before_funding_usdc'],'19.8')

    def test_disabled_and_execution_modes_never_connect(self):
        self.assertEqual(m.run({},journal=object())['status'],'DISABLED')
        for mode in ('single_testnet_attempt_v1','mainnet','cancel_rehearsal_testnet_v1'):
            self.assertEqual(m.run({**env(),'HL_TESTNET_RUNTIME_MODE':mode},journal=object())['status'],'READ_ONLY_MODE_REQUIRED')
        self.assertEqual(m.run({**env(),'HL_TESTNET_TWO_ACCOUNT_EXECUTION':'approved_single_attempt_v1'})['status'],'READ_ONLY_MODE_REQUIRED')

    def test_connection_reader_rejects_nonpublic_calls(self):
        for kind in ('order','cancel','exchange','approveAgent'):
            with self.assertRaises(m.ReviewError):m.PublicReader().read(kind,A)
        r=m.PublicReader();r.calls=12
        with self.assertRaises(m.ReviewError):r.read('clearinghouseState',A)
        with patch.object(m,'HOST','api.hyperliquid.xyz'):
            with self.assertRaises(m.ReviewError):m.PublicReader().read('clearinghouseState',A)

    def test_no_key_or_sender_or_app_in_adapter(self):
        text=Path(m.__file__).read_text()
        for forbidden in ('AGENT_KEY','sign_l1_action','submit_persisted(',"'/exchange'",'os.environ'):
            self.assertNotIn(forbidden,text)


@unittest.skipUnless(CI,'Requires disposable loopback PostgreSQL')
class PostgresAdapterTests(unittest.TestCase):
    def setUp(self):
        from .postgres_journal import PostgresJournal
        from .price_precision import prepare_signal
        import hyperliquid_testnet_executor as sender
        self.j=PostgresJournal.for_ci(CI)
        with self.j._transaction() as conn:
            conn.execute('DROP SCHEMA IF EXISTS hl_testnet_card_lifecycle_v1 CASCADE')
            conn.execute('DROP SCHEMA IF EXISTS hl_testnet_execution_v1 CASCADE')
        self.j.bootstrap()
        meta={'universe':[{'name':'DOGE','szDecimals':2}]}
        prepared=prepare_signal(record()['prepared']['source'],meta)
        self.key,_=self.j.save_prepared(A,prepared)
        action=sender.build_action(prepared['execution'],meta,A,exit_type='tp_limit_sl_market')
        self.j.reserve(self.key,action)
        self.j.save_result(self.key,{'status':'VERIFIED_OPEN_PROTECTED','verified':True,'protection_active':True})
        self.original=m.load_original(self.j,A)
        self.reader=Reader(self.original)
        p=patch('http.client.HTTPSConnection',side_effect=AssertionError('No exchange calls'))
        p.start();self.addCleanup(p.stop)

    def run_review(self, at=T):
        return m.run(env(),journal=self.j,reader=Reader(self.original),clock=lambda:at)

    def test_end_to_end_saved_shadow_keeps_original(self):
        out=self.run_review()
        self.assertEqual(out['status'],'CLOSED_DOGE_SHADOW_SAVED_AND_VERIFIED',out)
        self.assertEqual(out['card_state'],'CLOSED');self.assertTrue(out['legacy_record_unchanged'])
        self.assertTrue(out['replay_duplicate_verified']);self.assertTrue(out['separate_connection_readback_verified'])
        self.assertEqual(out['shadow_revision'],1)
        self.assertEqual(m.load_original(self.j,A),self.original)
        with self.j._transaction() as conn:
            self.assertEqual(conn.execute('SELECT count(*) FROM hl_testnet_execution_v1.attempts').fetchone()[0],1)
            self.assertEqual(conn.execute('SELECT count(*) FROM hl_testnet_card_lifecycle_v1.history').fetchone()[0],1)

    def test_next_observation_preserves_card_and_history(self):
        first=self.run_review();second=self.run_review(T+100)
        self.assertEqual(second['status'],'CLOSED_DOGE_SHADOW_SAVED_AND_VERIFIED',second)
        self.assertEqual(first['card_id'],second['card_id']);self.assertEqual(second['shadow_revision'],2)
        self.assertTrue(second['previous_shadow_seen'])
        with self.j._transaction() as conn:
            self.assertEqual(conn.execute('SELECT count(*) FROM hl_testnet_card_lifecycle_v1.heads').fetchone()[0],1)
            self.assertEqual(conn.execute('SELECT count(*) FROM hl_testnet_card_lifecycle_v1.history').fetchone()[0],2)

    def test_incomplete_history_does_not_initialize_shadow(self):
        self.reader.fills.pop()
        out=m.run(env(),journal=self.j,reader=self.reader,clock=lambda:T)
        self.assertIn('TERMINAL_QUANTITIES',out['status'])
        with self.j._transaction() as conn:
            self.assertIsNone(conn.execute("SELECT to_regnamespace('hl_testnet_card_lifecycle_v1')").fetchone()[0])
        self.assertEqual(m.load_original(self.j,A),self.original)

    def test_missing_saved_trade_does_not_read_exchange(self):
        with patch.object(m,'SOURCE_ID','missing'):
            out=m.run(env(),journal=self.j,reader=self.reader,clock=lambda:T)
        self.assertEqual(out['status'],'EXACT_SAVED_DOGE_REQUIRED');self.assertEqual(self.reader.calls,0)

    def test_original_changed_during_read_is_not_silently_accepted(self):
        changed={**self.original,'result':{'status':'OTHER','verified':False}}
        with patch.object(m,'load_original',side_effect=[self.original,changed]):
            out=self.run_review()
        self.assertEqual(out['status'],'ORIGINAL_CHANGED_DURING_OBSERVATION')
        self.assertEqual(m.load_original(self.j,A),self.original)


if __name__=='__main__':unittest.main(verbosity=2)
