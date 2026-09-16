"""Owner-approved TP limit + SL market. No user account or exchange requests.

PostgreSQL cases use the existing disposable localhost CI database only.
The HTTP/signing fixture is replaced before any simulated dispatch.
"""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import os
import unittest
from unittest.mock import patch

import hyperliquid_testnet_executor as sender
from hyperliquid_testnet_executor_selftest import signal, META, ACCOUNT, AGENT
from . import price_precision as precision, postgres_journal as pg
from . import guarded_execution as guard, persistent_execution as dispatch
from . import test_postgres_journal as pgfixtures
from .test_guarded_execution import Reader

MIXED = 'tp_limit_sl_market'


class MixedBuildTests(unittest.TestCase):
    def setUp(self):
        self.patcher = patch('http.client.HTTPSConnection', side_effect=AssertionError('No real network'))
        self.patcher.start()
        self.addCleanup(self.patcher.stop)
    def build(self, side='LONG', mode=MIXED):
        return sender.build_action(signal(side=side), META, ACCOUNT, exit_type=mode)
    def test_long_uses_limit_tp_and_market_stop(self):
        entry, tp, sl = self.build()['orders']
        self.assertEqual(entry['t'], {'limit': {'tif': 'Gtc'}})
        self.assertEqual(tp['t']['trigger'], {'tpsl':'tp','triggerPx':'104','isMarket':False})
        self.assertEqual(sl['t']['trigger'], {'tpsl':'sl','triggerPx':'98','isMarket':True})
        self.assertEqual([entry['b'],tp['b'],sl['b']], [True,False,False])
    def test_short_reverses_sides_not_exit_types(self):
        entry,tp,sl = self.build('SHORT')['orders']
        self.assertEqual([entry['b'],tp['b'],sl['b']], [False,True,True])
        self.assertFalse(tp['t']['trigger']['isMarket'])
        self.assertTrue(sl['t']['trigger']['isMarket'])
    def test_three_orders_same_group_and_reduce_only_children(self):
        action = self.build()
        self.assertEqual(action['grouping'], 'normalTpsl')
        self.assertEqual(len(action['orders']), 3)
        self.assertEqual([o['r'] for o in action['orders']], [False,True,True])
        self.assertEqual(len({o['s'] for o in action['orders']}), 1)
    def test_mixed_only_changes_stop_flag_from_limit(self):
        msg=signal()
        old=sender.build_action(msg,META,ACCOUNT,exit_type='limit')
        new=sender.build_action(msg,META,ACCOUNT,exit_type=MIXED)
        old['orders'][2]['t']['trigger']['isMarket']=True
        self.assertEqual(old,new)
    def test_old_profiles_are_unchanged(self):
        for mode,flag in [('market',True),('limit',False)]:
            self.assertEqual([o['t']['trigger']['isMarket'] for o in self.build(mode=mode)['orders'][1:]], [flag,flag])
    def test_no_default_or_reverse_profile(self):
        for value in (None,'auto','tp_market_sl_limit','',True):
            with self.assertRaises(sender.TestnetError): self.build(mode=value)
    def test_rounded_prices_quantity_and_source_preserved(self):
        src=signal();src.update(entry='96.88',stop='94.9424',take_profit='98.8176')
        before=deepcopy(src)
        prepared=precision.build_prepared_orders(src,META,ACCOUNT,exit_type=MIXED)
        entry,tp,sl=prepared['action']['orders']
        self.assertEqual([entry['p'],tp['p'],sl['p']], ['96.88','98.818','94.942'])
        self.assertEqual(src,before)
        self.assertEqual(prepared['source'],before)
        self.assertEqual(prepared['execution']['at'],src['at'])
        self.assertLessEqual(Decimal(entry['s'])*abs(Decimal(entry['p'])-Decimal(sl['t']['trigger']['triggerPx'])),Decimal(20))
    def test_durable_validator_accepts_mixed(self):
        prepared=precision.prepare_signal(signal(),META)
        action=sender.build_action(prepared['execution'],META,ACCOUNT,exit_type=MIXED)
        self.assertEqual(pg.validate_action(action,prepared,ACCOUNT),action)
    def test_durable_validator_rejects_reverse_and_numeric_flags(self):
        prepared=precision.prepare_signal(signal(),META)
        for flags in ((True,False),(0,True),(False,1)):
            action=sender.build_action(prepared['execution'],META,ACCOUNT,exit_type=MIXED)
            for order,flag in zip(action['orders'][1:],flags):order['t']['trigger']['isMarket']=flag
            with self.assertRaises(pg.JournalError):pg.validate_action(action,prepared,ACCOUNT)
    def test_durable_validator_still_rejects_price_or_reduce_only_changes(self):
        prepared=precision.prepare_signal(signal(),META)
        for field,value in [('r',False),('p','1')]:
            action=sender.build_action(prepared['execution'],META,ACCOUNT,exit_type=MIXED)
            action['orders'][1][field]=value
            with self.assertRaises(pg.JournalError):pg.validate_action(action,prepared,ACCOUNT)
    def test_review_recognizes_profile_but_keeps_other_guards(self):
        src=signal();src['at']=(datetime.now(timezone.utc)-timedelta(hours=1)).isoformat()
        result=guard.review_only(src,account=ACCOUNT,agent=AGENT,exit_type=MIXED,client=Reader(),env={})
        self.assertNotIn('EXPLICIT_EXIT_TYPE_REQUIRED',result['blockers'])
        self.assertIn('PERSISTENT_JOURNAL_NOT_CONFIGURED',result['blockers'])
        self.assertIn('SOURCE_NOT_FRESH_FOR_ONE_SHOT_TEST',result['blockers'])
        self.assertEqual(result['take_profit_type'],'limit')
        self.assertEqual(result['stop_loss_type'],'market')
        self.assertFalse(result['eligible_for_controlled_attempt'])
    def test_profile_does_not_enable_disabled_sender(self):
        with patch.object(sender,'_wallet',side_effect=AssertionError('No key')):
            result=dispatch.submit_persisted(signal(),account=ACCOUNT,agent=AGENT,exit_type=MIXED)
        self.assertEqual(result['status'],'DISABLED')
        self.assertEqual(result['order_requests_sent'],0)


@unittest.skipUnless(pgfixtures.CI_URL, 'Requires disposable PostgreSQL CI service')
class MixedPostgresTests(unittest.TestCase):
    setUp = pgfixtures.PostgresTests.setUp
    _dispatch_fixture = pgfixtures.PostgresTests._dispatch_fixture
    def test_mixed_action_survives_restart_and_cannot_change_mode(self):
        key,_=self.journal.save_prepared(ACCOUNT,self.prepared)
        action=sender.build_action(self.prepared['execution'],self.meta,ACCOUNT,exit_type=MIXED)
        self.assertTrue(self.journal.reserve(key,action)[0])
        other=pg.PostgresJournal.for_ci(pgfixtures.CI_URL)
        self.assertEqual(other.load(key)['action'],action)
        self.assertFalse(other.reserve(key,action)[0])
        changed=sender.build_action(self.prepared['execution'],self.meta,ACCOUNT,exit_type='limit')
        with self.assertRaisesRegex(pg.JournalError,'NO_RESEND'):other.reserve(key,changed)
    def test_real_pg_mixed_dispatch_readback_and_no_duplicate(self):
        exchange,signer=self._dispatch_fixture()
        result=dispatch.submit_persisted(self.source,account=ACCOUNT,agent=AGENT,journal=self.journal,exit_type=MIXED,enable_testnet=True)
        self.assertEqual(result['status'],'VERIFIED_OPEN_PROTECTED',result)
        sent=exchange.action
        self.assertFalse(sent['orders'][1]['t']['trigger']['isMarket'])
        self.assertTrue(sent['orders'][2]['t']['trigger']['isMarket'])
        signer.assert_called_once()
        repeated=dispatch.submit_persisted(self.source,account=ACCOUNT,agent=AGENT,journal=pg.PostgresJournal.for_ci(pgfixtures.CI_URL),exit_type=MIXED,enable_testnet=True)
        self.assertTrue(repeated['replayed'])
        self.assertEqual(len([c for c in exchange.calls if c[0]=='/exchange']),1)
    def test_mixed_timeout_preserved_across_connections(self):
        exchange,signer=self._dispatch_fixture();exchange.timeout=True
        first=dispatch.submit_persisted(self.source,account=ACCOUNT,agent=AGENT,journal=self.journal,exit_type=MIXED,enable_testnet=True)
        self.assertEqual(first['status'],'UNCERTAIN_REQUIRES_REVIEW')
        repeated=dispatch.submit_persisted(self.source,account=ACCOUNT,agent=AGENT,journal=pg.PostgresJournal.for_ci(pgfixtures.CI_URL),exit_type=MIXED,enable_testnet=True)
        self.assertTrue(repeated['replayed'])
        self.assertEqual(repeated['order_requests_sent'],0)
        signer.assert_called_once()


if __name__ == '__main__':
    unittest.main(verbosity=2)
