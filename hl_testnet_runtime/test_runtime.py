"""Offline fixtures only. No account, key, or network is needed."""
from copy import deepcopy
from io import StringIO
import json
import unittest
from unittest.mock import Mock, patch
from . import checks, app

A, B = '0x' + '1'*40, '0x' + '2'*40
ENV = {'HL_TESTNET_ACCOUNT_ADDRESS': A, 'HL_TESTNET_AGENT_ADDRESS': B}
PLAN = {'symbol':'BTC','side':'LONG','entry':'100','stop':'90','take_profit':'110'}


class Reader:
    def __init__(self):
        self.calls = 0
        self.seen = []
        self.mode = 'unifiedAccount'
        self.spot = {'balances':[{'coin':'USDC','token':0,'total':'1000','hold':'100'}]}
        self.perp = {'marginSummary':{'accountValue':'0'},'withdrawable':'0'}
        self.capacity = {'user':A,'coin':'BTC','availableToTrade':['500','500'],
                         'maxTradeSzs':['5','5'],'markPx':'100','leverage':{'type':'cross','value':1}}
        self.meta = {'universe':[{'name':'BTC','szDecimals':2,'maxLeverage':10}]}
        self.role = {'role':'agent','data':{'user':A}}
    def read(self, kind, *, user=None, coin=None):
        self.calls += 1
        self.seen.append(kind)
        if kind == 'userRole': return {'role':'user'} if user == A else deepcopy(self.role)
        return deepcopy({'userAbstraction':self.mode,'spotClearinghouseState':self.spot,
                         'clearinghouseState':self.perp,'activeAssetData':self.capacity,'meta':self.meta}[kind])


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.reader = Reader()
        self.p = patch('http.client.HTTPSConnection', side_effect=AssertionError('No network in unit tests'))
        self.p.start()
    def tearDown(self): self.p.stop()
    def run_case(self, env=None, **kw):
        return checks.run_check(ENV if env is None else env, client=self.reader, **kw)
    def plan_case(self, plan=None, **kw):
        return self.run_case({**ENV,'HL_TESTNET_CHECK_PLAN':json.dumps(PLAN if plan is None else plan)}, **kw)
    def test_without_addresses_no_io(self):
        self.assertEqual(self.run_case({})['status'],'WAITING_FOR_ADDRESSES')
        self.assertEqual(self.reader.calls,0)
    def test_disabled_other_modes_no_io(self):
        for mode in ('live','testnet','send','true','',True):
            self.assertEqual(self.run_case({**ENV,'HL_TESTNET_RUNTIME_MODE':mode})['status'],'READ_ONLY_RUNTIME_ONLY')
        self.assertEqual(self.reader.calls,0)
    def test_key_in_address_field_rejected_before_network(self):
        self.assertEqual(self.run_case({'HL_TESTNET_ACCOUNT_ADDRESS':'0x'+'a'*64})['status'],'PUBLIC_ADDRESS_REQUIRED')
        self.assertEqual(self.reader.calls,0)
    def test_same_agent_account_rejected(self):
        self.assertEqual(self.run_case({**ENV,'HL_TESTNET_AGENT_ADDRESS':A})['status'],'ACCOUNT_AND_AGENT_MUST_DIFFER')
    def test_wrong_agent_mapping(self):
        self.reader.role['data']['user'] = B
        self.assertEqual(self.run_case()['status'],'AGENT_ACCOUNT_MISMATCH')
    def test_unified_ignores_zero_perp_equity(self):
        result = self.run_case()
        self.assertTrue(result['positive_usdc_observed'])
        self.assertEqual(result['balance_source'],'unified_spot_state')
        self.assertNotIn('clearinghouseState',self.reader.seen)
        self.assertFalse(result['test_plan_checked'])
    def test_standard_uses_perp_state(self):
        self.reader.mode = 'disabled'
        self.reader.perp = {'marginSummary':{'accountValue':'1000'},'withdrawable':'900'}
        self.assertTrue(self.run_case()['positive_usdc_observed'])
        self.assertNotIn('spotClearinghouseState',self.reader.seen)
    def test_unknown_modes_fail_closed(self):
        for mode in ('portfolioMargin','default','dexAbstraction','future'):
            self.reader.mode = mode
            self.assertEqual(self.run_case()['status'],'ACCOUNT_MODE_REQUIRES_REVIEW')
    def test_total_balance_not_used_as_buying_power(self):
        self.reader.capacity['availableToTrade'] = ['0','0']
        self.assertTrue(self.run_case()['positive_usdc_observed'])
        self.assertFalse(self.run_case()['exchange_capacity_observed'])
        self.assertFalse(self.plan_case()['test_plan_checked'])
    def test_full_hold_blocks_budget(self):
        self.reader.spot['balances'][0]['hold'] = '1000'
        self.assertFalse(self.plan_case()['test_plan_checked'])
    def test_hold_greater_than_total(self):
        self.reader.spot['balances'][0]['hold'] = '1001'
        self.assertEqual(self.run_case()['status'],'INVALID_HOLD')
    def test_hold_is_mandatory(self):
        del self.reader.spot['balances'][0]['hold']
        self.assertEqual(self.run_case()['status'],'INVALID_NUMBER')
    def test_wrong_or_duplicate_usdc_token(self):
        self.reader.spot['balances'][0]['token'] = 1
        self.assertEqual(self.run_case()['status'],'WRONG_USDC_TOKEN')
        self.reader.spot['balances'][0]['token'] = 0
        self.reader.spot['balances'] *= 2
        self.assertEqual(self.run_case()['status'],'DUPLICATE_USDC')
    def test_other_tokens_not_usdc(self):
        self.reader.spot = {'balances':[{'coin':'OTHER','token':10,'total':'1000','hold':'0'}]}
        self.assertFalse(self.run_case()['positive_usdc_observed'])
    def test_zero_usdc_not_fabricated(self):
        self.reader.spot = {'balances':[]}
        self.assertFalse(self.run_case()['positive_usdc_observed'])
    def test_plan_pass_without_signing_or_orders(self):
        result = self.plan_case()
        self.assertTrue(result['test_plan_checked'])
        self.assertEqual(result['status'],'PRECHECK_PASSED_NOT_ORDER_AUTHORIZATION')
        self.assertFalse(result['signing_tested'])
        self.assertEqual(result['order_requests_sent'],0)
    def test_short_plan_pass(self):
        self.assertTrue(self.plan_case({**PLAN,'side':'SHORT','stop':'110','take_profit':'90'})['test_plan_checked'])
    def test_quantity_cap_not_bypassed(self):
        # $10 / $10 gap = 1 unit; the smaller-side cap deliberately excludes it.
        self.reader.capacity['maxTradeSzs'] = ['0.5','5']
        self.assertEqual(self.plan_case()['status'],'QUANTITY_EXCEEDS_EXCHANGE_CAP')
    def test_available_budget_uses_conservative_side(self):
        # 1 unit * 100 at 1x plus 1 reserve = 101, not 202 as under $20 risk.
        self.reader.capacity['availableToTrade'] = ['500','100.5']
        self.assertFalse(self.plan_case()['test_plan_checked'])
    def test_capacity_identity_and_symbol(self):
        for key, value in [('user',B),('coin','ETH')]:
            previous = self.reader.capacity[key]
            self.reader.capacity[key] = value
            self.assertEqual(self.run_case()['status'],'CAPACITY_ACCOUNT_OR_ASSET_MISMATCH')
            self.reader.capacity[key] = previous
    def test_capacity_schema_must_be_complete(self):
        for value in (None,[],['5'],['5',5],['NaN','1']):
            self.reader.capacity['maxTradeSzs'] = value
            self.assertFalse(self.plan_case()['test_plan_checked'])
    def test_no_prices_are_rounded(self):
        self.assertEqual(self.plan_case({**PLAN,'entry':'100.123456'})['status'],'PRICE_PRECISION_NO_ROUNDING')
    def test_plan_input_is_unchanged(self):
        original = deepcopy(PLAN)
        self.plan_case(PLAN)
        self.assertEqual(PLAN, original)
    def test_delisted_asset_blocks_plan(self):
        self.reader.meta['universe'][0]['isDelisted'] = True
        self.assertEqual(self.plan_case()['status'],'ASSET_UNAVAILABLE')
    def test_invalid_plan_cannot_pass(self):
        for plan in ({}, {**PLAN,'side':'BUY'}, {**PLAN,'entry':100.0}, {**PLAN,'stop':'100'}, {**PLAN,'secret':'unexpected'}):
            self.assertFalse(self.plan_case(plan)['test_plan_checked'])
    def test_nonfinite_excessive_numbers_rejected(self):
        for number in ('NaN','Infinity','1e5000','1e-5000','-1',1,True,'9'*81):
            with self.assertRaises(checks.Blocked): checks.number(number)
    def test_duplicate_json_keys_and_nan_rejected(self):
        for value in ('{"a":1,"a":2}','{"x":NaN}'):
            with self.assertRaises(checks.Blocked): checks.decode(value)
    def test_key_not_required_for_public_check(self):
        verifier = Mock(side_effect=AssertionError('must not check absent key'))
        self.assertFalse(self.run_case(local_key_check=verifier)['local_key_address_checked'])
        verifier.assert_not_called()
    def test_optional_key_check_is_local_and_redacted(self):
        verifier = Mock(return_value=True)
        result = self.run_case({**ENV,'HL_TESTNET_AGENT_KEY':'FAKE_NOT_A_KEY'}, local_key_check=verifier)
        verifier.assert_called_once_with('FAKE_NOT_A_KEY',B)
        self.assertTrue(result['local_key_address_checked'])
        self.assertNotIn('FAKE_NOT_A_KEY',json.dumps(result))
        self.assertFalse(result['signing_tested'])
    def test_wrong_key_blocks_readiness(self):
        verifier = Mock(side_effect=checks.Blocked('KEY_DOES_NOT_MATCH_AGENT'))
        result = self.run_case({**ENV,'HL_TESTNET_AGENT_KEY':'FAKE'}, local_key_check=verifier)
        self.assertEqual(result['status'],'KEY_DOES_NOT_MATCH_AGENT')
    def test_error_does_not_echo_private_info(self):
        self.reader.read = Mock(side_effect=RuntimeError('PRIVATE_TEST_VALUE'))
        result = self.run_case()
        self.assertEqual(result['status'],'CHECK_UNAVAILABLE')
        self.assertNotIn('PRIVATE_TEST_VALUE',json.dumps(result))
    def test_report_never_contains_addresses_balances_or_plan(self):
        report = json.dumps(self.plan_case())
        for text in (A,B,'1000','"entry"','"stop"','"take_profit"'):
            self.assertNotIn(text,report)
    def test_stale_sample_cannot_pass(self):
        with patch.object(checks.time,'monotonic',side_effect=[0,21]):
            result = self.plan_case()
        self.assertEqual(result['status'],'SAMPLE_EXPIRED')
        self.assertFalse(result['test_plan_checked'])
    def test_forbidden_calls_have_no_network(self):
        reader = checks.InfoReader()
        for kind in ('order','withdraw3','sendAsset','updateLeverage','unknown'):
            with self.assertRaises(checks.Blocked): reader.read(kind,user=A)
        self.assertEqual(reader.calls,0)
    def test_changed_testnet_host_rejected(self):
        with patch.object(checks,'HOST','api.hyperliquid.xyz'):
            with self.assertRaises(checks.Blocked): checks.InfoReader().read('meta')
    def test_transport_fixed_url_no_secret_headers(self):
        response = Mock(status=200)
        response.read.return_value=b'{"universe":[]}'
        connection = Mock(); connection.getresponse.return_value=response
        with patch.object(checks.http.client,'HTTPSConnection',return_value=connection) as ctor:
            checks.InfoReader().read('meta')
        ctor.assert_called_once_with('api.hyperliquid-testnet.xyz',timeout=4)
        args = connection.request.call_args.args
        self.assertEqual(args[:2],('POST','/info'))
        self.assertEqual(json.loads(args[2]),{'type':'meta'})
        self.assertNotIn('Authorization',args[3])
    def test_redirect_is_never_followed(self):
        response = Mock(status=302)
        connection=Mock();connection.getresponse.return_value=response
        with patch.object(checks.http.client,'HTTPSConnection',return_value=connection):
            with self.assertRaises(checks.Blocked): checks.InfoReader().read('meta')
        self.assertEqual(connection.request.call_count,1)
    def test_wsgi_is_static_and_rejects_post_query(self):
        with patch.object(app,'run_check',side_effect=AssertionError('no check via HTTP')):
            for method,path,query,status in [('GET','/','',200),('HEAD','/healthz','',200),('POST','/','',405),('GET','/','key=PRIVATE',404),('GET','/account','',404)]:
                response=[]
                body=b''.join(app.application({'REQUEST_METHOD':method,'PATH_INFO':path,'QUERY_STRING':query},lambda s,h:response.append((s,h))))
                self.assertTrue(response[0][0].startswith(str(status)))
                self.assertNotIn(b'PRIVATE',body)
                if method=='HEAD':self.assertEqual(body,b'')
    def test_startup_logs_redacted_report_only(self):
        fake_report = {'status':'WAITING_FOR_ADDRESSES','order_requests_sent':0}
        with patch.object(app,'run_check',return_value=fake_report),patch('sys.stdout',new_callable=StringIO) as output:
            app.startup_check()
        self.assertEqual(json.loads(output.getvalue()),{'testnet_runtime':fake_report})
    def test_invalid_secret_never_imports_sdk(self):
        with patch.object(checks.importlib.metadata,'version',side_effect=AssertionError('No SDK for invalid input')):
            with self.assertRaises(checks.Blocked):checks.key_matches('not-a-secret',B)


    def test_legacy_budget_components_are_reported_separately(self):
        self.reader.capacity['maxTradeSzs'] = ['0.5','5']
        self.reader.capacity['availableToTrade'] = ['100','100']
        self.reader.spot['balances'][0]['hold'] = '900'
        report = self.plan_case()['budget_diagnostics']
        self.assertEqual(report['legacy_checks_same_sample'], {
            'quantity_within_exchange_cap': False,
            'full_cash_within_unheld_balance': False,
            'full_cash_within_exchange_available': False})
        self.assertEqual(len(report['failed_checks']),3)
    def test_existing_leverage_resolves_cash_only_false_rejection(self):
        self.reader.capacity['leverage']['value'] = 5
        self.reader.capacity['availableToTrade'] = ['100','100']
        result = self.plan_case()
        self.assertTrue(result['test_plan_checked'])
        report = result['budget_diagnostics']
        self.assertFalse(report['legacy_passed'])
        self.assertTrue(report['current_settings_passed'])
        self.assertEqual(report['existing_leverage_used'],5)
        self.assertFalse(report['account_settings_changed'])
        self.assertFalse(report['order_authorization'])
    def test_one_x_does_not_bypass_cash_shortage(self):
        self.reader.capacity['availableToTrade'] = ['100','100']
        self.assertEqual(self.plan_case()['status'],'ESTIMATED_MARGIN_EXCEEDS_EXCHANGE_AVAILABLE')
    def test_unheld_usdc_cap_remains_enforced(self):
        self.reader.capacity['leverage']['value'] = 5
        self.reader.spot['balances'][0]['hold'] = '985'
        self.assertEqual(self.plan_case()['status'],'ESTIMATED_MARGIN_EXCEEDS_UNHELD_USDC')
    def test_reserve_is_not_divided_by_leverage(self):
        self.reader.capacity['leverage']['value'] = 5
        # Required = 20 margin + 1 reserve, NOT 20 + 1/5.
        self.reader.capacity['availableToTrade'] = ['20.5','20.5']
        self.assertFalse(self.plan_case()['test_plan_checked'])
        self.reader.capacity['availableToTrade'] = ['21','21']
        self.assertTrue(self.plan_case()['test_plan_checked'])
    def test_all_current_budget_failures_are_preserved(self):
        self.reader.capacity['availableToTrade'] = ['1','1']
        self.reader.spot['balances'][0]['hold'] = '999'
        result = self.plan_case()
        self.assertEqual(result['budget_diagnostics']['failed_checks'],[
            'estimated_margin_within_unheld_balance','estimated_margin_within_exchange_available'])
    def test_missing_or_invalid_leverage_blocks_no_default(self):
        for value in (None,{}, {'type':'cross','value':0}, {'type':'cross','value':True},
                      {'type':'cross','value':1.5}, {'type':'other','value':5},
                      {'type':'cross','value':11}):
            self.reader.capacity['leverage'] = value
            result=self.plan_case()
            self.assertEqual(result['status'],'CURRENT_LEVERAGE_NOT_VERIFIED')
            self.assertFalse(result['test_plan_checked'])
    def test_metadata_leverage_cap_required(self):
        del self.reader.meta['universe'][0]['maxLeverage']
        self.assertEqual(self.plan_case()['status'],'CURRENT_LEVERAGE_NOT_VERIFIED')
    def test_existing_isolated_setting_is_only_read(self):
        self.reader.capacity['leverage'] = {'type':'isolated','value':5,'rawUsd':'0'}
        result = self.plan_case()
        self.assertTrue(result['test_plan_checked'])
        self.assertEqual(result['budget_diagnostics']['margin_mode'],'isolated')
        self.assertFalse(result['budget_diagnostics']['account_settings_changed'])
    def test_adverse_entry_price_is_not_ignored_long(self):
        self.reader.capacity['markPx']='80'
        self.reader.capacity['leverage']['value']=5
        # Margin + reserve = 17, but adding adverse loss requires 37.
        self.reader.capacity['availableToTrade']=['20','20']
        result=self.plan_case()
        self.assertFalse(result['test_plan_checked'])
        self.assertTrue(result['budget_diagnostics']['adverse_entry_mark_loss_included'])
    def test_adverse_entry_price_is_not_ignored_short(self):
        self.reader.capacity['markPx']='120'
        self.reader.capacity['leverage']['value']=5
        # Margin + reserve = 25.2, but adding adverse loss requires 45.2.
        self.reader.capacity['availableToTrade']=['30','30']
        result=self.plan_case({**PLAN,'side':'SHORT','stop':'110','take_profit':'90'})
        self.assertFalse(result['test_plan_checked'])
        self.assertTrue(result['budget_diagnostics']['adverse_entry_mark_loss_included'])
    def test_quantity_cap_still_applies_with_leverage(self):
        self.reader.capacity['leverage']['value']=5
        self.reader.capacity['maxTradeSzs']=['0.5','5']
        self.assertEqual(self.plan_case()['status'],'QUANTITY_EXCEEDS_EXCHANGE_CAP')
    def test_capacity_array_order_is_not_guessed(self):
        self.reader.capacity['leverage']['value']=5
        for pair in (['20.5','500'],['500','20.5']):
            self.reader.capacity['availableToTrade']=pair
            self.assertFalse(self.plan_case()['test_plan_checked'])
    def test_key_match_is_independent_of_budget_failure(self):
        self.reader.capacity['availableToTrade']=['1','1']
        result=self.run_case({**ENV,'HL_TESTNET_CHECK_PLAN':json.dumps(PLAN),
                             'HL_TESTNET_AGENT_KEY':'FAKE'}, local_key_check=Mock(return_value=True))
        self.assertFalse(result['test_plan_checked'])
        self.assertTrue(result['local_key_address_checked'])
        self.assertFalse(result['signing_tested'])
    def test_account_state_and_plan_are_not_mutated(self):
        original = deepcopy(self.reader.__dict__)
        plan = deepcopy(PLAN)
        self.plan_case(plan)
        for name in ('spot','perp','capacity','meta','role'):
            self.assertEqual(self.reader.__dict__[name],original[name])
        self.assertEqual(plan,PLAN)
    def test_missing_plan_never_manufactures_budget(self):
        self.assertIsNone(self.run_case()['budget_diagnostics'])
    def test_plan_digest_bound_to_actual_input(self):
        one=self.plan_case()['budget_diagnostics']['plan_sha256']
        two=self.plan_case(dict(reversed(list(PLAN.items()))))['budget_diagnostics']['plan_sha256']
        three=self.plan_case({**PLAN,'take_profit':'111'})['budget_diagnostics']['plan_sha256']
        self.assertEqual(one,two)
        self.assertNotEqual(one,three)
    def test_mark_value_lab_bound_not_raised(self):
        self.reader.capacity['markPx']='6000'
        self.reader.capacity['availableToTrade']=['100000','100000']
        self.reader.spot['balances'][0]['total']='100000'
        result=self.plan_case()
        self.assertFalse(result['test_plan_checked'])
        self.assertIn('mark_notional_within_lab_cap',result['budget_diagnostics']['failed_checks'])
    def test_ten_dollar_risk_does_not_shrink_to_make_budget_pass(self):
        self.reader.capacity['leverage']['value']=5
        # 10/(100-99) = 10 units, not 1; cap deliberately excludes it.
        result=self.plan_case({**PLAN,'stop':'99'})
        self.assertEqual(result['status'],'QUANTITY_EXCEEDS_EXCHANGE_CAP')
        self.assertFalse(result['budget_diagnostics']['risk_rule_changed'])
    def test_legacy_wrapper_remains_conservative_without_context(self):
        from decimal import Decimal as D
        with self.assertRaises(checks.Blocked):
            checks.plan_check(PLAN,'BTC',2,D('100'),D('100'),D('5'))


if __name__ == '__main__':
    unittest.main(verbosity=2)
