"""Read-only balance fixtures. No real addresses, keys, exchange writes or DB."""
from copy import deepcopy
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from . import account_balance_review as balance
from .test_runtime import Reader, A, B


class BalanceReviewTests(unittest.TestCase):
    def setUp(self):
        self.reader = Reader()
        self.reader.mode = 'default'
        self.reader.perp = dict(marginSummary=dict(accountValue='1000.0',totalRawUsd='1000.0',
            totalMarginUsed='0.0',totalNtlPos='0.0'),withdrawable='1000.0',assetPositions=[])
        self.reader.spot = {'balances': []}
        self.reader.capacity['availableToTrade'] = ['1000','1000']
        p = patch('http.client.HTTPSConnection',side_effect=AssertionError('No real network'))
        p.start();self.addCleanup(p.stop)

    def run_case(self, account=A, expected='1000'):
        return balance.review_balance(account, expected_amount=expected, client=self.reader)

    def test_default_balance_found_without_mode_reclassification(self):
        r = self.run_case()
        self.assertEqual(r['status'],'EXPECTED_AMOUNT_OBSERVED_READ_ONLY')
        self.assertEqual(r['perp']['account_value_usd'],'1000.0')
        self.assertEqual(r['perp']['withdrawable_usd'],'1000.0')
        self.assertEqual(r['spot']['usdc_total'],'0')
        self.assertEqual(r['account_mode'],'default')
        self.assertFalse(r['account_mode_resolved'])
        self.assertTrue(r['default_mode_not_coerced'])
        self.assertEqual(r['perp']['nonzero_positions'],0)

    def test_six_public_calls_all_balance_queries_target_user(self):
        r = self.run_case()
        self.assertEqual(r['public_reads'],6)
        self.assertEqual(self.reader.seen,['userRole','userAbstraction','clearinghouseState',
            'spotClearinghouseState','activeAssetData','userAbstraction'])

    def test_amount_difference_not_fabricated_or_rounded(self):
        self.reader.perp['marginSummary']['accountValue']='999.999999'
        r=self.run_case()
        self.assertFalse(r['expected_amount_observed'])
        self.assertEqual(r['perp']['account_value_usd'],'999.999999')

    def test_unified_uses_spot_not_misleading_perp_value(self):
        self.reader.mode='unifiedAccount'
        self.assertFalse(self.run_case()['expected_amount_observed'])
        self.reader.spot={'balances':[{'coin':'USDC','token':0,'total':'1000','hold':'20'}]}
        r=self.run_case()
        self.assertTrue(r['expected_amount_observed'])
        self.assertEqual(r['spot']['usdc_unheld'],'980')
        self.assertEqual(r['observed_balance_source'],'spotClearinghouseState')

    def test_standard_reports_perp_without_summing_spot(self):
        self.reader.mode='disabled'
        self.reader.spot={'balances':[{'coin':'USDC','token':0,'total':'1000','hold':'0'}]}
        r=self.run_case()
        self.assertTrue(r['expected_amount_observed'])
        self.assertFalse(r['balances_added_together'])
        self.assertEqual(r['observed_balance_source'],'clearinghouseState')

    def test_portfolio_mode_not_marked_execution_ready(self):
        self.reader.mode='portfolioMargin'
        r=self.run_case()
        self.assertFalse(r['expected_amount_observed'])
        self.assertFalse(r['account_mode_resolved'])
        self.assertFalse(r['entry_sending_enabled'])

    def test_missing_balance_fields_do_not_become_zero(self):
        for key in ('accountValue','totalRawUsd','totalMarginUsed','totalNtlPos'):
            with self.subTest(key=key):
                saved=self.reader.perp['marginSummary'].pop(key)
                self.assertFalse(self.run_case()['balance_observed'])
                self.reader.perp['marginSummary'][key]=saved

    def test_nonfinite_values_are_rejected(self):
        for amount in ('NaN','Infinity',1000,True):
            self.reader.perp['marginSummary']['accountValue']=amount
            self.assertFalse(self.run_case()['expected_amount_observed'])

    def test_account_not_agent_and_invalid_input_no_network(self):
        self.assertFalse(self.run_case(account=B)['balance_observed'])
        self.reader.calls=0
        self.assertFalse(self.run_case(account='0x'+'1'*64)['balance_observed'])
        self.assertEqual(self.reader.calls,0)

    def test_missing_expected_value_is_not_assumed(self):
        for value in ('0','-1','NaN',None):
            self.assertFalse(self.run_case(expected=value)['balance_observed'])
        self.assertEqual(self.reader.calls,0)

    def test_capacity_failure_does_not_erase_balance(self):
        self.reader.capacity['user']=B
        r=self.run_case()
        self.assertTrue(r['expected_amount_observed'])
        self.assertEqual(r['btc_capacity_observation']['status'],'UNAVAILABLE')
        self.assertFalse(r['specific_trade_capacity_checked'])

    def test_nonzero_positions_are_not_hidden(self):
        self.reader.perp['assetPositions']=[{'position':{'coin':'BTC','szi':'0.1'}}]
        r=self.run_case()
        self.assertEqual(r['perp']['nonzero_positions'],1)
        self.assertFalse(r['entry_sending_enabled'])

    def test_duplicate_positions_fail_closed(self):
        self.reader.perp['assetPositions']=[{'position':{'coin':'BTC','szi':'0.1'}}]*2
        self.assertEqual(self.run_case()['status'],'DUPLICATE_POSITION_RESPONSE')

    def test_malformed_spot_not_ignored_because_perp_matches(self):
        self.reader.spot={'balances':[{'coin':'USDC','token':0,'total':'10','hold':'11'}]}
        self.assertFalse(self.run_case()['expected_amount_observed'])

    def test_account_mode_changed_during_read_blocks_combined_sample(self):
        original=self.reader.read;seen=[]
        def read(kind,**kw):
            value=original(kind,**kw)
            if kind=='userAbstraction':
                seen.append(kind)
                if len(seen)==2:return 'unifiedAccount'
            return value
        self.reader.read=read
        r=self.run_case()
        self.assertTrue(r['account_mode_changed_during_read'])
        self.assertFalse(r['expected_amount_observed'])

    def test_expired_combined_sample_not_confirmed(self):
        with patch.object(balance.time,'monotonic',side_effect=[0,21]):r=self.run_case()
        self.assertEqual(r['status'],'BALANCE_SAMPLE_EXPIRED')
        self.assertFalse(r['balance_observed'])

    def test_read_does_not_mutate_inputs_or_output_private_addresses(self):
        before=deepcopy(self.reader.perp)
        r=self.run_case()
        self.assertEqual(self.reader.perp,before)
        self.assertNotIn(A,json.dumps(r))
        self.assertNotIn(B,json.dumps(r))
        self.assertEqual(r['order_requests_sent'],0)
        self.assertEqual(r['account_settings_changes'],0)
        self.assertEqual(r['transfers_sent'],0)
        self.assertFalse(r['signing_tested'])

    def test_no_credential_or_exchange_write_imports(self):
        text=Path(balance.__file__).read_text()
        for forbidden in ('import os','_wallet','sign_l1_action','submit_persisted','/exchange'):
            self.assertNotIn(forbidden,text)


if __name__=='__main__':unittest.main(verbosity=2)
