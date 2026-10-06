"""Offline rounding and TP/SL request tests. No real account, key or order."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP, localcontext
import json
import unittest
from unittest.mock import Mock, patch

from . import price_precision as pp


def sample():
    return {'kind': 'SIGNAL', 'event_id': 'rounding-fixture', 'symbol': 'DEMO',
            'side': 'LONG', 'entry': '96.88', 'stop': '94.9424', 'take_profit': '98.8176',
            'at': (datetime.now(timezone.utc)-timedelta(seconds=1)).isoformat()}


def metadata(decimals=2):
    return {'universe': [{'name': 'DEMO', 'szDecimals': decimals, 'maxLeverage': 10}]}


class PureTests(unittest.TestCase):
    def test_five_significant_figures(self):
        self.assertEqual(pp.round_price('94.9424', 2), '94.942')
        self.assertEqual(pp.round_price('98.8176', 2), '98.818')
    def test_rounds_up_and_down(self):
        self.assertEqual(pp.round_price('1234.56', 2), '1234.6')
        self.assertEqual(pp.round_price('1234.54', 2), '1234.5')
    def test_halfway_goes_up(self):
        self.assertEqual(pp.round_price('1234.55', 2), '1234.6')
        self.assertEqual(pp.round_price('1.23445', 0), '1.2345')
    def test_asset_fractional_limit(self):
        self.assertEqual(pp.round_price('0.012345', 1), '0.01235')
        self.assertEqual(pp.round_price('0.0012345', 0), '0.001235')
        self.assertEqual(pp.round_price('0.012345', 4), '0.01')
    def test_all_integer_prices_stay_exact(self):
        for value in ('123456', '123456789', '1000000000000000'):
            for decimals in range(7):
                self.assertEqual(pp.round_price(value, decimals), value)
    def test_integer_exception_does_not_overround(self):
        self.assertEqual(pp.round_price('123456.789', 2), '123457')
        self.assertEqual(pp.round_price('75807.67', 5), '75808')
    def test_carry_at_magnitude_boundary(self):
        self.assertEqual(pp.round_price('9999.95', 2), '10000')
        self.assertEqual(pp.round_price('9.99996', 0), '10')
    def test_zero_is_rejected_not_raised_to_minimum(self):
        with self.assertRaisesRegex(pp.PrecisionError, 'ZERO'):
            pp.round_price('0.0000001', 0)
    def test_invalid_prices(self):
        for x in (True, 1.0, 1, None, '', 'NaN', 'Infinity', '0', '-1', '1e999', '1e-999', '9'*81):
            with self.subTest(value=x), self.assertRaises(pp.PrecisionError):
                pp.round_price(x, 2)
    def test_invalid_metadata_precision(self):
        for n in (None, True, '2', -1, 7, 2.0):
            with self.assertRaises(pp.PrecisionError): pp.round_price('10', n)
    def test_valid_prices_have_same_value(self):
        for price, n in [('100.00',2), ('1234.5',2), ('0.01234',1), ('0.001234',0)]:
            self.assertEqual(Decimal(pp.round_price(price, n)), Decimal(price))
    def test_idempotent_over_many_magnitudes_and_precisions(self):
        for precision in range(7):
            for power in range(-7, 12):
                for coefficient in ('1.23456789','4.99995001','9.99999999'):
                    p = Decimal(coefficient).scaleb(power)
                    try: rounded = pp.round_price(str(p), precision)
                    except pp.PrecisionError as exc:
                        self.assertEqual(str(exc),'ROUNDING_REACHED_ZERO'); continue
                    self.assertEqual(rounded, pp.round_price(rounded, precision))
                    q = Decimal(rounded).normalize()
                    if q != q.to_integral_value():
                        self.assertLessEqual(len(q.as_tuple().digits),5)
                        self.assertGreaterEqual(q.as_tuple().exponent, -(6-precision))
    def test_decimal_context_does_not_change_answer(self):
        with localcontext() as ctx:
            ctx.prec=3
            self.assertEqual(pp.round_price('1234.56',2),'1234.6')
    def test_original_and_metadata_are_not_mutated(self):
        raw, meta = sample(), metadata(); before, before_meta=deepcopy(raw),deepcopy(meta)
        result=pp.prepare_signal(raw,meta)
        self.assertEqual(raw,before); self.assertEqual(meta,before_meta)
        self.assertEqual(result['source'],before)
        result['execution']['entry']='999'
        self.assertEqual(raw,before)
    def test_identity_time_side_preserved(self):
        raw=sample(); prepared=pp.prepare_signal(raw,metadata())
        for key in ('kind','event_id','symbol','side','at'):
            self.assertEqual(raw[key],prepared['execution'][key])
    def test_audit_has_original_and_rounded_values(self):
        p=pp.prepare_signal(sample(),metadata())
        self.assertEqual(p['audit']['changes'],{
            'stop':{'before':'94.9424','after':'94.942'},
            'take_profit':{'before':'98.8176','after':'98.818'}})
        self.assertEqual(p['audit']['source_digest'],pp.digest(p['source']))
        self.assertEqual(p['audit']['execution_digest'],pp.digest(p['execution']))
    def test_short_order_preserved(self):
        raw=sample();raw.update(side='SHORT',stop='98.8176',take_profit='94.9424')
        p=pp.prepare_signal(raw,metadata())
        self.assertLess(Decimal(p['execution']['take_profit']), Decimal(p['execution']['entry']))
        self.assertGreater(Decimal(p['execution']['stop']), Decimal(p['execution']['entry']))
    def test_original_invalid_order_not_repaired(self):
        raw=sample();raw['stop']='100'
        with self.assertRaisesRegex(pp.PrecisionError,'SOURCE_PRICE_ORDER'):pp.prepare_signal(raw,metadata())
    def test_coalesced_prices_stop_instead_of_zero_division(self):
        raw=sample();raw.update(entry='100.004',stop='100.003',take_profit='100.005')
        with self.assertRaisesRegex(pp.PrecisionError,'COLLAPSED'):pp.prepare_signal(raw,metadata())
    def test_symbol_missing_duplicate_delisted(self):
        for meta in ({'universe':[]}, {'universe':metadata()['universe']*2},
                     {'universe':[{'name':'OTHER','szDecimals':2}]},
                     {'universe':[{'name':'DEMO','szDecimals':2,'isDelisted':True}]}):
            with self.assertRaises(pp.PrecisionError):pp.prepare_signal(sample(),meta)
    def test_secret_or_missing_field_rejected(self):
        for raw in ({**sample(),'key':'NEVER_PUBLISH'}, {k:v for k,v in sample().items() if k!='stop'}):
            with self.assertRaises(pp.PrecisionError):pp.prepare_signal(raw,metadata())
    def test_time_and_identity_validation(self):
        for k,v in [('at','2026-01-01T00:00:00'),('event_id',''),('symbol',None),('side','BUY')]:
            with self.assertRaises(pp.PrecisionError):pp.prepare_signal({**sample(),k:v},metadata())
    def test_unknown_policy_is_not_accepted(self):
        with self.assertRaises(pp.PrecisionError):pp.prepare_signal(sample(),metadata(),policy='floor')
    def test_different_originals_can_have_same_execution_but_distinct_audit(self):
        a=sample();b=deepcopy(a);b['stop']='94.94241'
        pa,pb=pp.prepare_signal(a,metadata()),pp.prepare_signal(b,metadata())
        self.assertEqual(pa['execution'],pb['execution'])
        self.assertNotEqual(pa['audit']['source_digest'],pb['audit']['source_digest'])


class IntegrationTests(unittest.TestCase):
    def setUp(self):
        from .test_guarded_execution import Reader, A, B
        self.reader=Reader();self.reader.mark='96.88'
        self.a,self.b=A,B
        self.network=patch('http.client.HTTPSConnection',side_effect=AssertionError('No network'))
        self.network.start()
    def tearDown(self): self.network.stop()
    def review(self, raw=None, **kw):
        return pp.review_rounded_signal(sample() if raw is None else raw, account=self.a,
            agent=self.b,client=self.reader,**kw)
    def test_rounding_resolves_precision_failure_in_real_guard(self):
        r=self.review()
        self.assertTrue(r['budget_passed'])
        self.assertTrue(r['budget_plan_bound'])
        self.assertEqual(r['price_precision_fields'],[])
        self.assertEqual(r['rounded_fields'],['stop','take_profit'])
        self.assertEqual(r['order_requests_sent'],0)
    def test_missing_storage_and_exit_mode_still_block(self):
        r=self.review()
        self.assertIn('PERSISTENT_JOURNAL_NOT_CONFIGURED',r['blockers'])
        self.assertIn('EXPLICIT_EXIT_TYPE_REQUIRED',r['blockers'])
        self.assertFalse(r['eligible_for_controlled_attempt'])
    def test_stale_original_not_retimestamped(self):
        raw=sample();raw['at']=(datetime.now(timezone.utc)-timedelta(hours=1)).isoformat()
        before=deepcopy(raw);r=self.review(raw)
        self.assertEqual(raw,before)
        self.assertIn('SOURCE_NOT_FRESH_FOR_ONE_SHOT_TEST',r['blockers'])
    def test_market_context_and_cash_guards_still_apply(self):
        self.reader.mark='150';r=self.review()
        self.assertIn('TESTNET_PRICE_OUTSIDE_SUPPLIED_EXIT_RANGE',r['blockers'])
        self.reader.mark='96.88';self.reader.available='0.01'
        self.assertFalse(self.review()['budget_passed'])
    def test_public_report_has_no_prices_addresses_or_key(self):
        text=json.dumps(self.review())
        for value in (self.a,self.b,'94.9424','94.942','98.818','96.88'):
            self.assertNotIn(value,text)
    def test_unknown_and_zero_rounding_never_call_budget(self):
        from . import guarded_execution as guard
        with patch.object(guard,'review_only',side_effect=AssertionError('Must not review')):
            raw=sample();raw.update(entry='0.00000003',stop='0.00000002',take_profit='0.00000004')
            r=self.review(raw)
        self.assertIn('ROUNDING_REACHED_ZERO',r['blockers'])
    def test_metadata_is_reused_once(self):
        original=self.reader.read;seen=[]
        def recorded(kind,**kw):seen.append(kind);return original(kind,**kw)
        self.reader.read=recorded
        self.review()
        self.assertEqual(seen.count('meta'),1)
    def test_preconfigured_tp_and_sl_in_same_three_order_group(self):
        import hyperliquid_testnet_executor as sender
        with patch.object(sender,'submit_once',side_effect=AssertionError('No submission')):
            prepared=pp.build_prepared_orders(sample(),metadata(),self.a,exit_type='limit')
        orders=prepared['action']['orders']
        self.assertEqual(prepared['action']['grouping'],'normalTpsl')
        self.assertEqual(len(orders),3)
        self.assertEqual([o['p'] for o in orders],['96.88','98.818','94.942'])
        self.assertEqual(orders[1]['t']['trigger']['tpsl'],'tp')
        self.assertEqual(orders[2]['t']['trigger']['tpsl'],'sl')
        self.assertEqual([o['r'] for o in orders],[False,True,True])
    def test_quantity_recomputed_after_rounding_and_floored(self):
        p=pp.build_prepared_orders(sample(),metadata(),self.a,exit_type='limit')
        size=Decimal(p['action']['orders'][0]['s']);gap=Decimal('96.88')-Decimal('94.942')
        self.assertLessEqual(size*gap,Decimal('10'))
        self.assertGreater((size+Decimal('0.01'))*gap,Decimal('10'))
    def test_both_exit_types_preserve_trigger_and_no_new_policy(self):
        import hyperliquid_testnet_executor as sender
        for mode in ('limit','market'):
            orders=pp.build_prepared_orders(sample(),metadata(),self.a,exit_type=mode)['action']['orders']
            self.assertEqual(orders[1]['t']['trigger']['triggerPx'],'98.818')
            self.assertEqual(orders[2]['t']['trigger']['triggerPx'],'94.942')
            self.assertEqual(orders[1]['t']['trigger']['isMarket'],mode=='market')
        with self.assertRaises(sender.TestnetError):
            pp.build_prepared_orders(sample(),metadata(),self.a,exit_type=None)
    def test_read_only_path_never_uses_key_or_sender(self):
        import hyperliquid_testnet_executor as sender
        with patch.object(sender,'_wallet',side_effect=AssertionError('No keys')), \
             patch.object(sender,'submit_once',side_effect=AssertionError('No orders')):
            self.assertTrue(self.review()['budget_passed'])

    def test_app_uses_approved_rounding_only_in_read_only_review(self):
        from . import app
        from io import StringIO
        values={'HL_TESTNET_REVIEW_SIGNAL':json.dumps(sample()),
                'HL_TESTNET_PRICE_ROUNDING':pp.POLICY,'HL_TESTNET_RUNTIME_MODE':'read_only'}
        with patch.dict(app.os.environ,values,clear=True), \
             patch.object(pp,'review_rounded_signal',return_value={'order_requests_sent':0}) as review, \
             patch('sys.stdout',new_callable=StringIO):
            app.review_configured_signal()
        review.assert_called_once()
    def test_app_rejects_unknown_rounding_or_send_mode(self):
        from . import app
        from io import StringIO
        for policy,mode in [('floor','read_only'),(pp.POLICY,'send')]:
            values={'HL_TESTNET_REVIEW_SIGNAL':json.dumps(sample()),
                    'HL_TESTNET_PRICE_ROUNDING':policy,'HL_TESTNET_RUNTIME_MODE':mode}
            with patch.dict(app.os.environ,values,clear=True), \
                 patch.object(pp,'review_rounded_signal',side_effect=AssertionError('No call')) as review, \
                 patch('sys.stdout',new_callable=StringIO) as output:
                app.review_configured_signal()
            review.assert_not_called()
            self.assertIn('REVIEW_CONFIGURATION_INVALID',output.getvalue())


if __name__ == '__main__':
    unittest.main(verbosity=2)
