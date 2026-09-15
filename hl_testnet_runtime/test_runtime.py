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
                         'maxTradeSzs':['5','5'],'markPx':'100'}
        self.meta = {'universe':[{'name':'BTC','szDecimals':2}]}
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
        self.reader.capacity['maxTradeSzs'] = ['1','5']
        self.assertEqual(self.plan_case()['status'],'OUTSIDE_CONSERVATIVE_LAB_BUDGET')
    def test_available_budget_uses_conservative_side(self):
        self.reader.capacity['availableToTrade'] = ['500','201']
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


if __name__ == '__main__':
    unittest.main(verbosity=2)
