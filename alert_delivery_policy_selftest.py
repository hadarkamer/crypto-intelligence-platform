"""Offline owner roster, retirement and existing-queue boundaries."""
from datetime import timedelta
import os
import unittest
from unittest.mock import patch

import alert_delivery_policy as policy
import manual_formula_alert_delivery as delivery
import manual_formula_alert_store as store
import manual_formula_alert_delivery_selftest as delivery_tests
from manual_formula_alert_store_selftest import BASE, c1274_bundle, c1274_references, observation_pair, pair

_STRICT_MANUAL_RULE_ENABLED = policy.manual_rule_enabled
_EXPECTED = {
    'R2732_XRP_SHORT_NY_WEEKDAYS_LOCK', 'HYPE_ROW71205_SHORT',
    'SOL_MAXPAIN_DIST1_3_RANGE24', 'SOL_G65_K49_PROFIT_LOCK',
    'HYPE_MAXPAIN_DIST05_15_LONG_TF', 'DOGE_MAXPAIN_DIST15_25_LONG_TF',
    'XRP_MAXPAIN_LONG_DIST2_4_SHORT_TF',
}


class PolicyTests(unittest.TestCase):
    def test_all_profiles_preserve_exact_seven_rule_roster(self):
        for profile in ('ALL', 'SELECTED_EXPERIMENTAL_ONLY', 'ORDINARY_AND_SELECTED_EXPERIMENTAL'):
            with self.subTest(profile=profile), patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': profile}):
                self.assertEqual(policy.ordinary_alerts_enabled(), profile != 'SELECTED_EXPERIMENTAL_ONLY')
                self.assertFalse(policy.other_experimental_alerts_enabled())
                self.assertFalse(policy.u21_experimental_enabled())
                self.assertTrue(policy.xrp_r2732_experimental_enabled())
                self.assertTrue(policy.hype_row71205_experimental_enabled())
                self.assertTrue(policy.sol_proximity_experimental_enabled())
                self.assertTrue(policy.sol_g65_experimental_enabled())
                self.assertEqual(set(policy.status()['selected_experimental_rule_ids']), _EXPECTED)
                self.assertEqual(policy.status()['manual_rule_allowlist'], [])
                self.assertTrue(policy.status()['configuration_valid'])
                for rule in policy.MAXPAIN_COMPONENT_RULES:
                    self.assertTrue(policy.maxpain_component_experimental_enabled(rule))
                for rule in policy.RETIRED_EXPERIMENTAL_RULES | {'unknown'}:
                    self.assertFalse(policy.selected_rule_enabled(rule))
                    self.assertFalse(policy.maxpain_component_experimental_enabled(rule))
                    self.assertFalse(policy.manual_rule_enabled(rule))
                self.assertFalse(policy.maxpain_component_experimental_enabled('HYPE_ROW71205_SHORT'))
                self.assertEqual(delivery.status()['active_rule_ids'], [])
                self.assertEqual(set(delivery.status()['paused_rule_ids']), set(store.rules.RULE_IDS))

    def test_legacy_all_and_environment_cannot_reenable_retired_sources(self):
        with patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': 'ALL', 'ENABLE_U21': 'true',
                                     'RESEARCH_EXPERIMENTAL_ENABLED': 'true'}):
            self.assertFalse(policy.u21_experimental_enabled())
            self.assertFalse(policy.other_experimental_alerts_enabled())
            self.assertFalse(any(policy.manual_rule_enabled(rule) for rule in store.rules.RULE_IDS))
            self.assertEqual(policy.status()['roster_version'], 'owner-r2732-and-later-20261005-v1')
            self.assertTrue(policy.ordinary_alerts_enabled())

    def test_unknown_profile_fails_closed(self):
        for profile in ('', 'SELECTED', 'OFF'):
            with self.subTest(profile=profile), patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': profile}):
                self.assertFalse(policy.ordinary_alerts_enabled())
                self.assertFalse(policy.other_experimental_alerts_enabled())
                self.assertFalse(policy.u21_experimental_enabled())
                self.assertFalse(policy.xrp_r2732_experimental_enabled())
                self.assertFalse(policy.hype_row71205_experimental_enabled())
                self.assertFalse(policy.sol_proximity_experimental_enabled())
                self.assertFalse(policy.sol_g65_experimental_enabled())
                self.assertFalse(any(policy.maxpain_component_experimental_enabled(r) for r in _EXPECTED))
                self.assertFalse(any(policy.manual_rule_enabled(rule) for rule in store.rules.RULE_IDS))
                self.assertFalse(policy.status()['configuration_valid'])
                self.assertEqual(policy.status()['selected_experimental_rule_ids'], [])

    def test_retired_matching_events_get_receipts_without_new_intents(self):
        state = store._initial(BASE)
        with patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': 'ALL'}):
            self.assertEqual(store.record_events(state, [pair()], BASE + timedelta(minutes=2)), 0)
            self.assertIn('1', state['receipts'])
            self.assertEqual(state['intents'], [])
            self.assertEqual(store.record_events(state, [observation_pair(2)],
                                                 BASE + timedelta(minutes=2)), 0)
            self.assertIn('2', state['receipts'])
            self.assertEqual(store.record_c1274_scan(state, c1274_bundle(), c1274_references(),
                                                    BASE + timedelta(minutes=2)),
                             (0, 'ALERT_POLICY_DISABLED'))
            self.assertEqual(state['intents'], [])
        # Historical receipts prevent replay even in a test-only old contract.
        with patch.object(policy, 'manual_rule_enabled', return_value=True):
            self.assertEqual(store.record_events(state, [pair()], BASE + timedelta(minutes=3)), 0)


class RetiredDeliveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        delivery_tests.DeliveryTests.setUp(self)
        # The archived-delivery fixture may emulate old permissions. These are
        # production-roster tests and deliberately restore the real strict gate.
        p = patch.object(policy, 'manual_rule_enabled', _STRICT_MANUAL_RULE_ENABLED)
        p.start(); self.addCleanup(p.stop)

    seed = delivery_tests.DeliveryTests.seed
    rows = delivery_tests.DeliveryTests.rows
    bot = delivery_tests.DeliveryTests.bot
    run_delivery = delivery_tests.DeliveryTests.run_delivery

    async def test_existing_manual_pending_cancel_without_resetting_history(self):
        with patch.object(policy, 'manual_rule_enabled', return_value=True):
            self.seed(3)
            state = self.db.read(1)
            self.assertEqual(store.record_events(state, [observation_pair(20)], self.clock[0]), 1)
            self.assertEqual(store.record_c1274_scan(state, c1274_bundle(), c1274_references(), self.clock[0]),
                             (1, 'MATCH'))
            self.db.write(1, state)
        before_receipts = self.db.read(1)['receipts'].copy()
        with patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': 'ALL'}):
            bot = self.bot()
            self.assertEqual(await self.run_delivery(bot), 0)
            self.assertEqual(await self.run_delivery(bot), 0)
            rows = self.rows()
            self.assertEqual({row['payload']['rule_id'] for row in rows if row['status'] == 'DELIVERED'}, set())
            muted = [row for row in rows if row['status'] == 'CANCELLED']
            self.assertEqual(len(muted), 5)
            self.assertEqual({row['payload']['rule_id'] for row in muted},
                             {'CONSENSUS_FULL', 'PRICE_OI_ENTRY2', 'PRICE_OI_SPOT65',
                              'C1274', 'MAGNET_OBSERVATION_DOGE_SHORT'})
            self.assertTrue(all(row['cancellation_reason'] == 'ALERT_POLICY_DISABLED' for row in muted))
            self.assertEqual(self.db.read(1)['receipts'], before_receipts)
            bot.send_message.assert_not_awaited()

    async def test_retirement_is_rechecked_after_awaited_claim(self):
        legacy = [True]
        with patch.object(policy, 'manual_rule_enabled', side_effect=lambda _rule: legacy[0]):
            self.seed(1)
            original_claim = store.claim

            def retire_after_claim(*args, **kwargs):
                claimed = original_claim(*args, **kwargs)
                legacy[0] = False
                return claimed

            bot = self.bot()
            with patch.object(store, 'claim', side_effect=retire_after_claim):
                self.assertEqual(await self.run_delivery(bot), 0)
            bot.send_message.assert_not_awaited()
            self.assertEqual(self.rows()[0]['status'], 'CANCELLED')


if __name__ == '__main__':
    unittest.main()
