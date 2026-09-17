"""No real HTTP, keys or trades. Account routing, scoped balance and fill audit."""
from copy import deepcopy
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
import json
import os
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from . import two_account_execution as r, checks, saved_trade_review as trade
from . import persistent_execution as p

A='0x'+'1'*40; B='0x'+'2'*40; C=r.PHANTOM; D='0x'+'4'*40
ENV=dict(HL_TESTNET_LONG_ACCOUNT_ADDRESS=A,HL_TESTNET_LONG_AGENT_ADDRESS=B,
    HL_TESTNET_SHORT_ACCOUNT_ADDRESS=C,HL_TESTNET_SHORT_AGENT_ADDRESS=D,
    RENDER_SERVICE_ID=r.SERVICE,HL_TESTNET_RUNTIME_MODE='cancel_monitor_testnet_v1',
    HL_TESTNET_CONNECTION_REVIEW='two_account_no_orders_v1')
PLAN=dict(symbol='BTC',side='SHORT',entry='100',stop='102',take_profit='98')

class Reader:
    def __init__(self):
        self.calls=0;self.mode='default';self.switch=False;self.mode_calls=0
        self.perp=dict(assetPositions=[],withdrawable='999',marginSummary=dict(
            accountValue='999',totalRawUsd='999',totalMarginUsed='0',totalNtlPos='0'))
        self.spot={'balances':[]}
        self.active=dict(user=C,coin='BTC',availableToTrade=['999','999'],
            maxTradeSzs=['50','50'],markPx='100',leverage=dict(type='cross',value=1))
    def read(self,kind,*,user=None,coin=None):
        self.calls+=1
        if kind=='userAbstraction':
            self.mode_calls+=1
            return 'unifiedAccount' if self.switch and self.mode_calls>1 else self.mode
        if kind=='userRole':return {'role':'user'} if user==C else {'role':'agent','data':{'user':C}}
        if kind=='clearinghouseState':return deepcopy(self.perp)
        if kind=='spotClearinghouseState':return deepcopy(self.spot)
        if kind=='activeAssetData':return deepcopy(self.active)
        if kind=='meta':return {'universe':[dict(name='BTC',szDecimals=3,maxLeverage=40)]}
        raise AssertionError('Unexpected read')

class ConnectionTests(unittest.TestCase):
    def setUp(self):
        self.net=patch('http.client.HTTPSConnection',side_effect=AssertionError('Real network prohibited'))
        self.net.start();self.addCleanup(self.net.stop)
        self.reader=Reader();self.route=r.route_for(ENV,'short_account')
    def test_direction_and_account_must_both_match(self):
        for account,agent,side in [(A,D,'SHORT'),(C,B,'SHORT'),(C,D,'LONG')]:
            with self.assertRaises(checks.Blocked):r.route_for(ENV,'short_account',account,agent,side)
    def test_unknown_role_never_chooses_a_default(self):
        with self.assertRaises(checks.Blocked):r.route_for(ENV,'unknown')
    def test_short_key_never_falls_back_to_first_account(self):
        env={**ENV,'HL_TESTNET_AGENT_KEY':'1'*64}
        with self.assertRaisesRegex(checks.Blocked,'NOT_CONFIGURED'):
            r.wallet_for_role(env,'short_account',C,D)
    def test_master_key_or_wrong_derived_agent_rejected(self):
        fake=SimpleNamespace(Account=SimpleNamespace(from_key=lambda _:SimpleNamespace(address=C)))
        with patch.dict('sys.modules',{'eth_account':fake}),patch.object(r.importlib.metadata,'version',return_value='0.24.0'):
            with self.assertRaisesRegex(checks.Blocked,'ADDRESS_MISMATCH'):
                r.wallet_for_role({**ENV,'HL_TESTNET_SHORT_AGENT_KEY':'1'*64},'short_account',C,D)
    def test_local_correct_key_has_no_signature_or_mutation(self):
        env={**ENV,'HL_TESTNET_SHORT_AGENT_KEY':'1'*64};before=dict(env)
        fake=SimpleNamespace(Account=SimpleNamespace(from_key=lambda _:SimpleNamespace(address=D)))
        with patch.dict('sys.modules',{'eth_account':fake}),patch.object(r.importlib.metadata,'version',return_value='0.24.0'):
            self.assertEqual(r.wallet_for_role(env,'short_account',C,D).address,D)
        self.assertEqual(env,before)
    def test_readiness_missing_key_is_explicit_not_connected(self):
        out=r.inspect_second(ENV,reader=self.reader)
        self.assertEqual(out['status'],'PUBLIC_CAPACITY_CHECKED_WAITING_FOR_PRIVATE_KEY')
        self.assertFalse(out['key_address_verified']);self.assertFalse(out['signing_tested'])
        self.assertFalse(out['specific_order_checked']);self.assertEqual(out['order_requests_sent'],0)
    def test_default_evidence_not_a_mode_rename(self):
        out=r.default_native_snapshot(self.route,self.reader,'BTC')
        self.assertEqual(out['account_mode'],'default');self.assertFalse(out['mode_was_renamed'])
        self.assertEqual(Decimal(out['balance_usd']),999)
        self.assertFalse(out['trade_authorized'])
    def test_other_default_account_not_silently_enabled(self):
        with self.assertRaisesRegex(checks.Blocked,'SCOPE_NOT_APPROVED'):
            r.default_native_snapshot({'account':A,'agent':B},self.reader,'BTC')
    def test_exact_plan_uses_same_ten_dollar_quantity(self):
        out=r.budget_for_role(ENV,'short_account',C,D,PLAN,self.reader)
        self.assertTrue(out['test_plan_checked'])
        self.assertEqual(out['budget_diagnostics']['planned_risk_usd'],'10')
        self.assertEqual(out['budget_diagnostics']['plan_sha256'],p.digest(PLAN))
    def test_insufficient_money_blocks_exact_plan(self):
        self.reader.active['availableToTrade']=['1','1']
        with self.assertRaises(checks.Blocked):r.budget_for_role(ENV,'short_account',C,D,PLAN,self.reader)
    def test_missing_leverage_is_not_invented(self):
        self.reader.active.pop('leverage')
        with self.assertRaises(checks.Blocked):r.budget_for_role(ENV,'short_account',C,D,PLAN,self.reader)
    def test_spot_funds_do_not_get_added_or_used_to_infer_mode(self):
        self.reader.spot={'balances':[dict(coin='USDC',token=0,total='999',hold='0')]}
        with self.assertRaisesRegex(checks.Blocked,'OTHER_BALANCES'):
            r.default_native_snapshot(self.route,self.reader,'BTC')
    def test_balance_disagreement_blocks(self):
        self.reader.perp['marginSummary']['totalRawUsd']='1000'
        with self.assertRaisesRegex(checks.Blocked,'NOT_RECONCILED'):
            r.default_native_snapshot(self.route,self.reader,'BTC')
    def test_mode_change_during_sample_blocks(self):
        self.reader.switch=True
        with self.assertRaisesRegex(checks.Blocked,'MODE_CHANGED'):
            r.default_native_snapshot(self.route,self.reader,'BTC')
    def test_nonempty_account_stays_for_later_multi_card_stage(self):
        self.reader.perp['assetPositions']=[{'position':{'szi':'1'}}]
        with self.assertRaisesRegex(checks.Blocked,'NOT_EMPTY'):
            r.default_native_snapshot(self.route,self.reader,'BTC')
    def test_no_role_submission_without_separate_approval(self):
        with patch.dict(os.environ,ENV,clear=True),patch.object(p,'prepare_and_store',side_effect=AssertionError('No DB')):
            out=p.submit_persisted({},account=C,agent=D,account_role='short_account',enable_testnet=True)
        self.assertEqual(out['status'],'ROLE_EXECUTION_LOCKED');self.assertEqual(out['order_requests_sent'],0)
    def test_mode_and_service_cannot_be_bypassed_by_flag(self):
        for k,v in [('RENDER_SERVICE_ID','other'),('HL_TESTNET_RUNTIME_MODE','read_only')]:
            env={**ENV,'HL_TESTNET_TWO_ACCOUNT_EXECUTION':'approved_single_attempt_v1',k:v}
            self.assertFalse(r.execution_unlocked(env))
    def test_disabled_public_review_no_io(self):
        self.assertEqual(r.inspect_second({},reader=Mock(side_effect=AssertionError()))['status'],'DISABLED')
    def test_review_cannot_run_in_submission_mode(self):
        self.assertEqual(r.inspect_second({**ENV,'HL_TESTNET_RUNTIME_MODE':'single_testnet_attempt_v1'},reader=self.reader)['status'],'REVIEW_MODE_NOT_ALLOWED')
        self.assertEqual(self.reader.calls,0)
    def test_persistent_role_path_checks_identity_before_storage(self):
        env={**ENV,'HL_TESTNET_RUNTIME_MODE':'single_testnet_attempt_v1',
             'HL_TESTNET_TWO_ACCOUNT_EXECUTION':'approved_single_attempt_v1'}
        src=dict(kind='SIGNAL',event_id='fixture',at=datetime.now(timezone.utc).isoformat(),**PLAN)
        with patch.dict(os.environ,env,clear=True),patch.object(p,'prepare_and_store',side_effect=AssertionError('No storage')) as store:
            out=p.submit_persisted(src,account=A,agent=D,account_role='short_account',enable_testnet=True)
        store.assert_not_called();self.assertEqual(out['order_requests_sent'],0)


def fill(tid,oid,side,price,size,pnl,fee,t):
    return dict(tid=tid,oid=oid,coin='DOGE',side=side,dir='Open Long' if side=='B' else 'Close Long',
                px=price,sz=size,closedPnl=pnl,fee=fee,feeToken='USDC',time=t)

class FillAuditTests(unittest.TestCase):
    def setUp(self):
        self.fills=[fill(1,11,'B','100','5','0','0.45',1000),fill(2,12,'A','104','5','20','0.46',2000)]
        self.ids={'ENTRY':11,'TAKE_PROFIT':12,'STOP':13}
    def summary(self,fs=None):return trade.summarize_fills(self.fills if fs is None else fs,self.ids,'5')
    def test_fees_subtracted_once_not_ui_and_api_twice(self):
        out=self.summary();self.assertEqual(Decimal(out['gross_pnl_usdc']),20)
        self.assertEqual(Decimal(out['net_before_funding_usdc']),Decimal('19.09'))
        self.assertTrue(out['complete_expected_quantity'])
    def test_duplicate_fills_do_not_double_pnl(self):
        self.assertEqual(self.summary(self.fills+self.fills),self.summary())
    def test_conflicting_fill_id_blocks(self):
        with self.assertRaises(checks.Blocked):self.summary(self.fills+[{**self.fills[0],'fee':'2'}])
    def test_partial_exit_not_closed(self):
        self.fills[1]['sz']='2';self.assertFalse(self.summary()['complete_expected_quantity'])
    def test_unrelated_close_not_attributed(self):
        self.fills[1]['oid']=99;out=self.summary()
        self.assertEqual(out['unmatched_doge_fills'],1);self.assertFalse(out['complete_expected_quantity'])
    def test_wrong_fee_currency_blocks_usd_total(self):
        self.fills[1]['feeToken']='HYPE'
        with self.assertRaises(checks.Blocked):self.summary()
    def test_role_direction_mismatch_blocks(self):
        self.fills[1]['dir']='Open Short'
        with self.assertRaises(checks.Blocked):self.summary()
    def test_source_is_not_mutated(self):
        before=deepcopy(self.fills);self.summary();self.assertEqual(before,self.fills)
    def test_empty_history_not_success(self):
        self.assertFalse(self.summary([])['complete_expected_quantity'])
    def test_trade_review_disabled_never_reads_storage(self):
        self.assertEqual(trade.review_saved_trade({},journal=object())['status'],'DISABLED')
    def test_duplicate_order_ids_cannot_mix_roles(self):
        with self.assertRaises(checks.Blocked):trade.summarize_fills(self.fills,{'ENTRY':11,'STOP':11},'5')

if __name__=='__main__':unittest.main()
