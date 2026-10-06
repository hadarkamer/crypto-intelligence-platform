"""Official market schema, capacity join and budgeted Testnet-only transport."""
from copy import deepcopy
from decimal import Decimal
import json
import unittest
from unittest.mock import Mock, patch

from . import checks, experimental_execution_evidence as evidence
from .experimental_market_context import MarketSnapshot, MarketContextError

T = 1791288960000
A = '0x' + '1' * 40
B = '0x' + '2' * 40


def response():
    return [dict(universe=[dict(name='SOL',szDecimals=2,maxLeverage=20),
        dict(name='XRP',szDecimals=2,maxLeverage=10),
        dict(name='kPEPE',szDecimals=1,maxLeverage=10)]),
        [dict(markPx='100',oraclePx='101',midPx='102'),
         dict(markPx='2.3',oraclePx='2.4',midPx='2.5'),dict(markPx='0.01')]]


def capacity():
    # Official activeAssetData has account capacity, leverage and no markPx.
    return dict(user=A,coin='XRP',availableToTrade=['5000','4000'],
        maxTradeSzs=['10000','9000'],leverage=dict(type='cross',value=10))


class MarketContextTests(unittest.TestCase):
    def setUp(self):
        self.raw=response()
        self.market=MarketSnapshot(self.raw,observed_at_ms=T)

    def mark(self,**kwargs):
        args=dict(account=A,symbol='XRP',now_ms=T+5)
        args.update(kwargs)
        return self.market.mark(**args)

    def test_mark_is_index_bound_and_not_oracle_or_mid(self):
        self.assertEqual(self.mark(),dict(environment='testnet',account=A,symbol='XRP',
            mark_price='2.3',at_ms=T))
        self.assertEqual(self.mark(symbol='SOL')['mark_price'],'100')

    def test_public_symbol_mark_shared_across_separate_account_bindings(self):
        first=self.mark();second=self.mark(account=B)
        self.assertEqual(first['mark_price'],second['mark_price'])
        self.assertEqual(first['at_ms'],second['at_ms'])
        self.assertNotEqual(first['account'],second['account'])

    def test_original_response_or_returned_metadata_mutation_cannot_rebind_mark(self):
        self.raw[0]['universe'].reverse();self.raw[1][1]['markPx']='999'
        metadata=self.market.metadata;metadata['universe'].reverse()
        self.assertEqual(self.mark()['mark_price'],'2.3')
        self.assertEqual(self.market.metadata['universe'][1]['name'],'XRP')

    def test_freshness_boundary_and_future_are_enforced_without_refresh(self):
        self.assertEqual(self.mark(now_ms=T+15000)['at_ms'],T)
        for now in (T-1,T+15001):
            with self.subTest(now=now),self.assertRaisesRegex(MarketContextError,'FRESH'):
                self.mark(now_ms=now)

    def test_no_metadata_context_length_mismatch_or_invented_active_shape(self):
        bad=[{},capacity(),[{}],self.raw+[{}],[self.raw[0],self.raw[1][:-1]],
             [dict(universe=[]),[]]]
        for raw in bad:
            with self.subTest(raw=raw),self.assertRaises(MarketContextError):
                MarketSnapshot(raw,observed_at_ms=T)

    def test_universe_duplicates_or_nonobject_context_refuse_ambiguous_index(self):
        for mutation in ('duplicate','context','missing_name'):
            raw=response()
            if mutation=='duplicate':raw[0]['universe'][0]['name']='XRP'
            elif mutation=='context':raw[1][0]=None
            else:del raw[0]['universe'][0]['name']
            with self.subTest(mutation=mutation),self.assertRaisesRegex(MarketContextError,'UNIQUE'):
                MarketSnapshot(raw,observed_at_ms=T)

    def test_unknown_delisted_or_invalid_selected_metadata_cannot_create_mark(self):
        with self.assertRaisesRegex(MarketContextError,'TRADABLE'):
            self.mark(symbol='BTC')
        for field,value in (('isDelisted',True),('szDecimals',True),('szDecimals',7),
                            ('maxLeverage',0),('maxLeverage','10')):
            raw=response();raw[0]['universe'][1][field]=value
            with self.subTest(field=field,value=value),self.assertRaisesRegex(MarketContextError,'TRADABLE'):
                MarketSnapshot(raw,observed_at_ms=T).mark(account=A,symbol='XRP',now_ms=T)

    def test_selected_context_explicit_wrong_symbol_never_silently_ignored(self):
        for field in ('coin','name'):
            raw=response();raw[1][1][field]='SOL'
            with self.subTest(field=field),self.assertRaisesRegex(MarketContextError,'SYMBOL_MISMATCH'):
                MarketSnapshot(raw,observed_at_ms=T).mark(account=A,symbol='XRP',now_ms=T)

    def test_invalid_or_absent_mark_has_no_oracle_fallback(self):
        for value in (None,'0','-1','NaN','Infinity',2.3):
            raw=response();raw[1][1]['markPx']=value
            with self.subTest(value=value),self.assertRaisesRegex(MarketContextError,'VALID_TESTNET_MARK'):
                MarketSnapshot(raw,observed_at_ms=T).mark(account=A,symbol='XRP',now_ms=T)

    def test_official_capacity_is_joined_purely_and_checks_use_verified_mark(self):
        raw=capacity();original=deepcopy(raw)
        joined=self.market.active_asset_data(raw,account=A,symbol='XRP',now_ms=T)
        self.assertEqual(raw,original);self.assertNotIn('markPx',raw)
        self.assertEqual(joined,dict(raw,markPx='2.3'))
        self.assertEqual(checks.capacity(joined,A,'XRP'),(Decimal('4000'),Decimal('9000')))
        self.assertTrue(checks.plan_check(dict(symbol='XRP',side='LONG',entry='2.3',
            stop='2.2',take_profit='2.5'),'XRP',2,Decimal('5000'),Decimal('4000'),
            Decimal('9000'),active=joined,metadata_max_leverage=10))

    def test_capacity_join_rejects_wrong_account_symbol_and_stale_mark(self):
        for kwargs in (dict(account=B),dict(symbol='SOL'),dict(now_ms=T+15001)):
            args=dict(account=A,symbol='XRP',now_ms=T);args.update(kwargs)
            with self.subTest(kwargs=kwargs),self.assertRaises(ValueError):
                self.market.active_asset_data(capacity(),**args)

    def test_fabricated_raw_capacity_mark_cannot_override_real_market(self):
        raw=capacity();raw['markPx']='999'
        with self.assertRaisesRegex(MarketContextError,'MUST_NOT_SUPPLY_MARK'):
            self.market.active_asset_data(raw,account=A,symbol='XRP',now_ms=T)

    def test_official_sample_helper_rejects_old_fabricated_active_response(self):
        raw=capacity();raw['markPx']='2.3'
        with self.assertRaises(evidence.EvidenceError):
            evidence.mark_sample(raw,account=A,symbol='XRP',observed_at_ms=T,now_ms=T)
        sample=evidence.mark_sample(response(),account=A,symbol='XRP',observed_at_ms=T,now_ms=T+1)
        self.assertEqual(sample['mark_price'],'2.3');self.assertEqual(sample['at_ms'],T)

    def test_info_read_uses_fixed_testnet_host_shared_budget_and_exact_body(self):
        raw=response();budget=Mock();permit=budget.acquire.return_value
        response_mock=Mock(status=200);response_mock.read.return_value=json.dumps(raw).encode()
        connection=Mock();connection.getresponse.return_value=response_mock
        with patch.object(checks.http.client,'HTTPSConnection',return_value=connection) as ctor:
            result=checks.InfoReader(budget=budget,priority='protection').read('metaAndAssetCtxs')
        self.assertEqual(result,raw)
        budget.acquire.assert_called_once_with('/info',{'type':'metaAndAssetCtxs'},
            priority='protection',host='api.hyperliquid-testnet.xyz')
        ctor.assert_called_once_with('api.hyperliquid-testnet.xyz',timeout=4)
        self.assertEqual(json.loads(connection.request.call_args.args[2]),dict(type='metaAndAssetCtxs'))
        permit.check.assert_called_once();permit.finish.assert_called_once_with(raw)

    def test_budget_denial_and_wrong_scope_never_start_transport(self):
        budget=Mock();budget.acquire.side_effect=ValueError('TEST_BUDGET_DENIED')
        with patch.object(checks.http.client,'HTTPSConnection') as ctor:
            with self.assertRaisesRegex(ValueError,'DENIED'):
                checks.InfoReader(budget=budget).read('metaAndAssetCtxs')
            for kwargs in (dict(user=A),dict(coin='XRP'),dict(user=A,coin='XRP')):
                with self.assertRaisesRegex(checks.Blocked,'NOT_ALLOWED'):
                    checks.InfoReader(budget=budget).read('metaAndAssetCtxs',**kwargs)
        ctor.assert_not_called();self.assertEqual(budget.acquire.call_count,1)

    def test_mark_context_is_not_reused_as_static_metadata(self):
        reader=checks.InfoReader(budget=object(),reuse=True)
        with patch.object(reader,'_transport',return_value=response()) as fetch:
            reader.read('metaAndAssetCtxs');reader.read('metaAndAssetCtxs')
        self.assertEqual(fetch.call_count,2)


if __name__=='__main__':
    unittest.main()
