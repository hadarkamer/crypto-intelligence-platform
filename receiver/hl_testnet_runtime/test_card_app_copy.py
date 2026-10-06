"""The journal copy distinguishes receipt from venue-verified execution."""
import json
import unittest

from . import card_app_copy as copy, card_display_projection as display, trade_cards
from .test_card_lifecycle import binding, closed, T
from .test_filled_quantity_exits import original, META


class AppCopyTests(unittest.TestCase):
    def test_received_alert_has_no_implied_action_or_pnl(self):
        _, row = original()
        card = trade_cards.prepare_card(row['card']['prepared']['source'], META,
            rule_id='SOFTWARE_TEST', threshold_pct='1.5', record_kind='received_alert')
        out = copy.received(card, observed_at_ms=T)
        self.assertEqual(set(out['payload']), copy.FIELDS)
        self.assertEqual(out['payload']['status'], 'RECORDED_ONLY')
        self.assertIsNone(out['payload']['quantity_entered'])
        self.assertIsNone(out['payload']['realized_pnl'])
        self.assertFalse(out['payload']['pnl_verified'])
        self.assertNotIn('account', json.dumps(out))

    def test_verified_lifecycle_keeps_unknown_funding_and_pnl_unknown(self):
        b = binding()
        projected = display.project([b], closed(b), revision=2, now_ms=T)
        out = copy.lifecycle(projected, b['card_id'])
        self.assertEqual(set(out['payload']), copy.FIELDS)
        self.assertTrue(out['payload']['closure_verified'])
        self.assertFalse(out['payload']['pnl_verified'])
        self.assertIsNone(out['payload']['realized_pnl'])
        self.assertEqual(out['payload']['side'], 'long')
        for secret in (b['account'], 'orders', 'oid', 'agent_key'):
            self.assertNotIn(secret, json.dumps(out))

    def test_rejects_unverified_or_unrelated_display(self):
        b = binding()
        projected = display.project([b], closed(b), revision=1, now_ms=T)
        projected['read_only'] = False
        with self.assertRaisesRegex(Exception, 'VERIFIED_DISPLAY_COPY_REQUIRED'):
            copy.lifecycle(projected, b['card_id'])


if __name__ == '__main__':
    unittest.main()
