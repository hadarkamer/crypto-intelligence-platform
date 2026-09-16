"""New $10 entries and immutable old $20 records. No real exchange or keys."""
from copy import deepcopy
from decimal import Decimal
import os
import unittest
from unittest.mock import patch

import hyperliquid_testnet_executor as sender
from . import risk_policy as risk, checks, price_precision as precision
from . import postgres_journal as pg, half_threshold_cancel as cancellation
from . import pending_cancel_executor as canceller
from . import test_postgres_journal as fixtures

ACCOUNT = '0x' + '1'*40
META = {'universe':[{'name':'BTC','szDecimals':3,'maxLeverage':40}]}


def source(side='LONG'):
    return dict(kind='SIGNAL',event_id='risk-fixture-1',symbol='BTC',side=side,
        entry='100',stop='98' if side=='LONG' else '102',
        take_profit='104' if side=='LONG' else '96',at='2026-09-16T00:00:00+00:00')


def action(src=None, historical=None):
    return sender.build_action(source() if src is None else src,META,ACCOUNT,
        exit_type='tp_limit_sl_market',historical_risk_usd=historical)


class RiskPolicyTests(unittest.TestCase):
    def setUp(self):
        self.net = patch('http.client.HTTPSConnection', side_effect=AssertionError('No live network'))
        self.net.start(); self.addCleanup(self.net.stop)
    def test_current_budget_is_ten(self):
        self.assertEqual(risk.budget(),Decimal(10))
        self.assertEqual(risk.CURRENT_RISK_USD,'10')
    def test_environment_cannot_increase_risk(self):
        with patch.dict(os.environ, {'HL_TESTNET_RISK_USD':'20','RISK_USD':'1000'}):
            self.assertEqual(action()['orders'][0]['s'],'5')
    def test_both_directions_use_ten_dollars(self):
        for side in ('LONG','SHORT'):
            src=source(side); built=action(src)
            self.assertEqual([o['s'] for o in built['orders']],['5']*3)
            self.assertEqual(Decimal(built['orders'][0]['s'])*abs(Decimal(src['entry'])-Decimal(src['stop'])),Decimal(10))
            risk.assert_new_entry_budget(built)
    def test_legacy_pure_rebuild_has_original_size_and_same_ids(self):
        new,old=action(),action(historical='20')
        self.assertEqual([o['s'] for o in old['orders']],['10']*3)
        for a,b in zip(new['orders'],old['orders']):
            a=deepcopy(a);b=deepcopy(b);a.pop('s');b.pop('s');self.assertEqual(a,b)
    def test_legacy_action_cannot_be_signed(self):
        with self.assertRaisesRegex(risk.RiskError,'TEN_DOLLAR'):
            sender._signed_body(None,action(historical='20'),0)
    def test_legacy_action_cannot_reach_transport(self):
        http=sender.TestnetHTTP(allow_orders=True)
        with self.assertRaisesRegex(risk.RiskError,'TEN_DOLLAR'):
            http._post('/exchange',{'action':action(historical='20')})
        self.assertEqual(http.order_attempts,0)
    def test_price_source_direction_and_timestamp_unchanged(self):
        src=source();before=deepcopy(src);built=action(src)
        self.assertEqual(src,before)
        self.assertEqual([o['p'] for o in built['orders']],['100','104','98'])
    def test_rounded_prices_use_ten_not_old_quantity(self):
        src=source();src.update(entry='96.88',stop='94.9424',take_profit='98.8176')
        prepared=precision.build_prepared_orders(src,META,ACCOUNT,exit_type='tp_limit_sl_market')
        self.assertEqual(prepared['source'],src)
        entry,_,stop=prepared['action']['orders']
        loss=Decimal(entry['s'])*abs(Decimal(entry['p'])-Decimal(stop['p']))
        self.assertLessEqual(loss,Decimal(10));self.assertGreater(loss,Decimal('9.99'))
    def test_quantity_rounds_down(self):
        src=source();src['stop']='97'
        self.assertEqual(action(src)['orders'][0]['s'],'3.333')
        risk.assert_new_entry_budget(action(src))
    def test_minimum_notional_not_increased_to_force_acceptance(self):
        src=source('SHORT');src.update(entry='1',stop='3',take_profit='0.5')
        with self.assertRaisesRegex(sender.TestnetError,'SIZE_BOUNDS'):action(src)
    def test_unknown_historical_profiles_rejected(self):
        for profile in ('15','100','NaN',20,True):
            with self.assertRaises(risk.RiskError):action(historical=profile)
    def test_current_budget_check_uses_same_quantity_as_sender(self):
        plan={k:source()[k] for k in ('symbol','side','entry','stop','take_profit')}
        diag={}
        self.assertTrue(checks.plan_check(plan,'BTC',3,Decimal('56'),Decimal('56'),Decimal('5'),
            active={'markPx':'100','leverage':{'type':'cross','value':10}},metadata_max_leverage=40,diagnostics=diag))
        self.assertEqual(diag['planned_risk_usd'],'10')
        self.assertFalse(diag['fees_funding_slippage_included_in_risk'])
        with self.assertRaises(checks.Blocked):
            checks.plan_check(plan,'BTC',3,Decimal('54'),Decimal('54'),Decimal('5'),
                active={'markPx':'100','leverage':{'type':'cross','value':10}},metadata_max_leverage=40)
    def test_current_and_legacy_records_validate_without_rewrite(self):
        prepared=precision.prepare_signal(source(),META)
        for built in (action(),action(historical='20')):
            before=deepcopy(built)
            self.assertEqual(pg.validate_action(built,prepared,ACCOUNT),before)
            self.assertEqual(built,before)
    def test_arbitrary_fifteen_dollar_record_rejected(self):
        built=action()
        for o in built['orders']:o['s']='7.5'
        with self.assertRaises(pg.JournalError):pg.validate_action(built,precision.prepare_signal(source(),META),ACCOUNT)
        with self.assertRaises(risk.RiskError):risk.assert_new_entry_budget(built)
    def test_legacy_cancel_rule_and_repairs_preserve_old_quantity(self):
        src=source();prepared=precision.prepare_signal(src,META)
        rec={'account':ACCOUNT,'prepared':prepared,'action':action(historical='20')}
        before=deepcopy(rec)
        rule=cancellation.rule_from_record(rec,dict(event_id=src['event_id'],rule_id='FIXTURE',threshold_pct='1.5',source_digest=pg.digest(src)))
        self.assertEqual(rule['size'],'10');self.assertEqual(rule['cancel_price'],'100.75')
        repaired=canceller.repair_action(rec,'8')
        self.assertEqual([o['s'] for o in repaired['orders']],['8','8'])
        risk.assert_new_entry_budget(repaired)
        self.assertEqual(rec,before)
    def test_missing_stop_and_wrong_bracket_fail_closed(self):
        for bad in ({'type':'order'},{'type':'order','orders':[]}, {'type':'order','orders':action()['orders'][:1]}):
            with self.assertRaises(risk.RiskError):risk.assert_new_entry_budget(bad)
    def test_nan_negative_and_over_budget_quantities_refused(self):
        for qty in ('NaN','-1','5.001','Infinity'):
            built=action()
            for o in built['orders']:o['s']=qty
            with self.assertRaises(risk.RiskError):risk.assert_new_entry_budget(built)
    def test_mismatched_exit_quantity_side_and_asset_refused(self):
        for k,v in [('s','4'),('b',True),('a',1)]:
            built=action();built['orders'][1][k]=v
            with self.assertRaises(risk.RiskError):risk.assert_new_entry_budget(built)
    def test_disabled_entry_still_cannot_read_key(self):
        with patch.object(sender,'_wallet',side_effect=AssertionError('No key')):
            self.assertEqual(sender.submit_once(source(),account=ACCOUNT,journal='/no-file',exit_type='tp_limit_sl_market')['status'],'DISABLED')
    def test_testnet_host_stays_fixed(self):
        self.assertEqual(sender.TESTNET_HOST,'api.hyperliquid-testnet.xyz')


@unittest.skipUnless(os.environ.get('HL_JOURNAL_CI_URL'),'Requires disposable PostgreSQL')
class RiskMigrationPostgresTests(unittest.TestCase):
    setUp=fixtures.PostgresTests.setUp
    def pair(self):
        key,_=self.journal.save_prepared(ACCOUNT,self.prepared)
        return key,sender.build_action(self.prepared['execution'],self.meta,ACCOUNT,exit_type='tp_limit_sl_market'),sender.build_action(self.prepared['execution'],self.meta,ACCOUNT,exit_type='tp_limit_sl_market',historical_risk_usd='20')
    def test_new_twenty_reservation_refused(self):
        key,new,old=self.pair()
        with self.assertRaises(pg.JournalError):self.journal.reserve(key,old)
        self.assertIsNone(self.journal.load(key)['action'])
        self.assertTrue(self.journal.reserve(key,new)[0])
    def test_old_twenty_record_survives_restart_without_resend(self):
        key,new,old=self.pair();self.journal.reserve(key,new)
        # Simulate a pre-upgrade stored action in DISPOSABLE localhost CI only.
        with self.journal._transaction() as conn:
            conn.execute('UPDATE hl_testnet_execution_v1.attempts SET action=%s::jsonb WHERE plan_key=%s',(pg.canonical(old),key))
        other=pg.PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL'])
        before=other.load(key)
        self.assertEqual(pg.validate_action(before['action'],before['prepared'],ACCOUNT),old)
        self.assertFalse(other.reserve(key,old)[0])
        with self.assertRaises(pg.JournalError):other.reserve(key,new)
        self.assertEqual(other.load(key),before)
    def test_new_ten_action_survives_restart(self):
        key,new,_=self.pair();self.assertTrue(self.journal.reserve(key,new)[0])
        other=pg.PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL'])
        self.assertEqual(other.load(key)['action'],new)
        self.assertFalse(other.reserve(key,new)[0])


if __name__=='__main__':unittest.main(verbosity=2)
