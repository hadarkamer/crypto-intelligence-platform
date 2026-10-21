"""A venue-unrepresentable DOGE approval must not stall other formulas."""
import unittest

import approved_alert_contract as contract
from approved_alert_fixtures import doge_partial_alert
from experimental_execution_fixtures import r2732_message
from . import test_experimental_live_runtime as support


class DogeEntryRefusalTests(unittest.TestCase):
    def setUp(self):
        self.h = support.RuntimeUnitTests()
        self.h.setUp()
        self.addCleanup(self.h.doCleanups)

    def check_refusal(self, msg, reason):
        msg['occurrence_id'] = contract.occurrence_id(msg)
        contract.validate(msg)
        other = r2732_message(entry=2.3, decision_ms=support.T-60000)
        self.h.worker.receive([msg, other])
        result = self.h.cycle()
        self.assertEqual(result.get('operation'), 'ENTRY', result)
        state = self.h.store.load()
        self.assertEqual(state['entry_blocked'][msg['occurrence_id']], reason)
        self.assertNotIn(msg['occurrence_id'], state['trades'])
        self.assertIn(other['occurrence_id'], state['trades'])
        self.assertEqual(len(self.h.venue.requests), 1)
        self.assertEqual(self.h.venue.requests[0]['proposal']['card_id'], other['occurrence_id'])

    def test_collapsed_partial_price_defers_only_its_occurrence(self):
        msg = doge_partial_alert(approved_ms=support.T-60000)
        msg['policy']['partial_take_price'] = '99.00001'
        self.check_refusal(msg, 'DOGE_PARTIAL_TARGET_COLLAPSED_AFTER_ROUNDING')

    def test_unrepresentable_split_defers_only_its_occurrence(self):
        msg = doge_partial_alert(approved_ms=support.T-60000)
        msg.update(entry='1000', stop='800', take_profit='1200')
        msg['policy']['partial_take_price'] = '1150'
        self.check_refusal(msg, 'DOGE_SPLIT_QUANTITY_UNREPRESENTABLE')


if __name__ == '__main__':
    unittest.main()
