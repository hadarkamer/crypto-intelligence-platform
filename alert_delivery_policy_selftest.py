"""Offline notification selection and existing-queue boundaries."""
from datetime import timedelta
import os
import unittest
from unittest.mock import patch

import alert_delivery_policy as policy
import manual_formula_alert_delivery as delivery
import manual_formula_alert_store as store
import manual_formula_alert_delivery_selftest as delivery_tests
from manual_formula_alert_store_selftest import BASE, c1274_bundle, c1274_references, observation_pair, pair


class PolicyTests(unittest.TestCase):
    def test_all_profile_preserves_existing_behavior(self):
        with patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': 'ALL'}):
            self.assertTrue(policy.ordinary_alerts_enabled())
            self.assertTrue(all(policy.manual_rule_enabled(rule) for rule in store.rules.RULE_IDS))
            self.assertTrue(policy.status()['configuration_valid'])

    def test_selected_profile_allows_exactly_two_manual_rules(self):
        with patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': 'SELECTED_EXPERIMENTAL_ONLY'}):
            self.assertFalse(policy.ordinary_alerts_enabled())
            self.assertEqual([rule for rule in store.rules.RULE_IDS if policy.manual_rule_enabled(rule)],
                             ['C1274', 'MAGNET_OBSERVATION_DOGE_SHORT'])
            self.assertFalse(policy.manual_rule_enabled('unknown'))
            self.assertEqual(delivery.status()['active_rule_ids'],
                             ['C1274', 'MAGNET_OBSERVATION_DOGE_SHORT'])
            self.assertEqual(set(delivery.status()['paused_rule_ids']),
                             {'PRICE_OI_ENTRY2', 'PRICE_OI_SPOT65', 'CONSENSUS_FULL', 'C0964'})

    def test_unknown_profile_fails_closed(self):
        for profile in ('', 'SELECTED', 'OFF'):
            with self.subTest(profile=profile), patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': profile}):
                self.assertFalse(policy.ordinary_alerts_enabled())
                self.assertFalse(any(policy.manual_rule_enabled(rule) for rule in store.rules.RULE_IDS))
                self.assertFalse(policy.status()['configuration_valid'])

    def test_muted_matching_events_get_receipts_without_creating_pending_alerts(self):
        state = store._initial(BASE)
        with patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': 'SELECTED_EXPERIMENTAL_ONLY'}):
            self.assertEqual(store.record_events(state, [pair()], BASE + timedelta(minutes=2)), 0)
            self.assertIn('1', state['receipts'])
            self.assertEqual(state['intents'], [])
            self.assertEqual(store.record_events(state, [observation_pair(2)],
                                                 BASE + timedelta(minutes=2)), 1)
        with patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': 'ALL'}):
            self.assertEqual(store.record_events(state, [pair()], BASE + timedelta(minutes=3)), 0)


class SelectedDeliveryTests(unittest.IsolatedAsyncioTestCase):
    setUp = delivery_tests.DeliveryTests.setUp
    seed = delivery_tests.DeliveryTests.seed
    rows = delivery_tests.DeliveryTests.rows
    bot = delivery_tests.DeliveryTests.bot
    run_delivery = delivery_tests.DeliveryTests.run_delivery

    async def test_only_selected_alerts_send_and_existing_other_pending_alerts_cancel(self):
        with patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': 'ALL'}):
            self.seed(3)
        with patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': 'SELECTED_EXPERIMENTAL_ONLY'}):
            state = self.db.read(1)
            self.assertEqual(store.record_events(state, [observation_pair(20)], self.clock[0]), 1)
            self.assertEqual(store.record_c1274_scan(state, c1274_bundle(), c1274_references(), self.clock[0]),
                             (1, 'MATCH'))
            self.db.write(1, state)
            bot = self.bot()
            self.assertEqual(await self.run_delivery(bot), 2)
            self.assertEqual(await self.run_delivery(bot), 0)
            rows = self.rows()
            self.assertEqual({row['payload']['rule_id'] for row in rows if row['status'] == 'DELIVERED'},
                             {'C1274', 'MAGNET_OBSERVATION_DOGE_SHORT'})
            muted = [row for row in rows if row['status'] == 'CANCELLED']
            self.assertEqual(len(muted), 3)
            self.assertTrue(all(row['cancellation_reason'] == 'ALERT_POLICY_DISABLED' for row in muted))
            self.assertEqual(bot.send_message.await_count, 2)


    async def test_policy_is_rechecked_after_awaited_claim(self):
        with patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': 'ALL'}):
            self.seed(1)
            original_claim = store.claim

            def change_after_claim(*args, **kwargs):
                claimed = original_claim(*args, **kwargs)
                os.environ['ALERT_DELIVERY_PROFILE'] = 'SELECTED_EXPERIMENTAL_ONLY'
                return claimed

            bot = self.bot()
            with patch.object(store, 'claim', side_effect=change_after_claim):
                self.assertEqual(await self.run_delivery(bot), 0)
            bot.send_message.assert_not_awaited()
            self.assertEqual(self.rows()[0]['status'], 'CANCELLED')



if __name__ == '__main__':
    unittest.main()
