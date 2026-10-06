"""Formula selection stays with the alert producer, not the Testnet worker."""
from datetime import datetime, timezone
import unittest

from . import approved_alert_selection as selection, trade_cards
from .test_filled_quantity_exits import original, META
from .test_card_lifecycle import T


class SelectionTests(unittest.TestCase):
    def card(self, side, rule):
        _, record = original(9 if side == 'LONG' else 10, side)
        source = record['card']['prepared']['source']
        expiry = datetime.fromtimestamp((T + 30000) / 1000, timezone.utc).isoformat()
        return trade_cards.prepare_card(source, META, rule_id=rule,
            threshold_pct='1.5', record_kind='received_alert',
            source_expires_at=expiry)

    def test_distinct_rules_and_both_roles_use_the_same_read_gate(self):
        since = datetime.fromtimestamp((T - 30000) / 1000, timezone.utc)
        now = datetime.fromtimestamp(T / 1000, timezone.utc)
        for side, rule in (('LONG', 'FORMULA_A'), ('SHORT', 'FORMULA_B')):
            card = self.card(side, rule)
            self.assertTrue(selection.eligible(card, not_before=since, now=now))
            self.assertEqual(card['rule']['id'], rule)

    def test_expiry_and_start_window_block_old_alert(self):
        card = self.card('LONG', 'FORMULA_A')
        self.assertFalse(selection.eligible(card,
            not_before=datetime.fromtimestamp(T / 1000, timezone.utc),
            now=datetime.fromtimestamp(T / 1000, timezone.utc)))
        self.assertFalse(selection.eligible(card,
            not_before=datetime.fromtimestamp((T - 30000) / 1000, timezone.utc),
            now=datetime.fromtimestamp((T + 31000) / 1000, timezone.utc)))


if __name__ == '__main__':
    unittest.main()
