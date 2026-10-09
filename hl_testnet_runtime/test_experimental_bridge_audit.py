"""Offline audit of why new experimental families are NOT ready to execute.

Passing these tests confirms current fail-closed boundaries and reproduces
missing semantics. It is not a certification of a new trading integration.
No production code is modified and no exchange or database is contacted.
"""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import patch

import alert_cards_wire as wire
from alert_cards_forwarder_selftest import delivery
from . import approved_alert_selection as selection
from . import execution_occurrence as occurrence
from . import price_precision, trade_cards, unavailable_asset_cards
from . import card_exit_recovery, card_lifecycle
from .test_filled_quantity_exits import case


SOURCE_AT = '2026-10-06T10:00:00+00:00'
EXPIRES_AT = '2026-10-06T10:01:30+00:00'


def signal(symbol='XRP', side='SHORT', identity='audit-event'):
    return dict(kind='SIGNAL', event_id=identity, symbol=symbol, side=side,
                entry='100', stop='102' if side == 'SHORT' else '98',
                take_profit='98' if side == 'SHORT' else '102', at=SOURCE_AT)


def draft_card(family='xrp_r2732', scope='1', identity='audit-event'):
    """Deliberately bypass only wire intake to expose the next compatibility gap.

    This synthetic 0.5 threshold is NOT a proposed strategy setting or producer
    adapter. An actual source must supply its own verified cancellation policy.
    """
    return trade_cards.prepare_card(signal(identity=identity),
        {'universe': [{'name': 'XRP', 'szDecimals': 2}]},
        rule_id='R2732', threshold_pct='0.5', record_kind='received_alert',
        source_stream=family + ':' + scope * 64, source_expires_at=EXPIRES_AT)


class ExperimentalBridgeAuditTests(unittest.TestCase):
    def setUp(self):
        for target in ('http.client.HTTPSConnection', 'socket.create_connection',
                       'socket.socket.connect', 'socket.socket.connect_ex',
                       'hyperliquid_testnet_executor._wallet',
                       'hyperliquid_testnet_executor._signed_body'):
            guard = patch(target, side_effect=AssertionError('OFFLINE_AUDIT_ONLY'))
            guard.start()
            self.addCleanup(guard.stop)

    def test_new_families_are_rejected_before_metadata_or_orders(self):
        for family in ('xrp_r2732', 'maxpain_proximity'):
            value = delivery()
            value['family'] = family
            with self.subTest(family=family):
                with self.assertRaisesRegex(wire.WireError, 'UNKNOWN_DELIVERY_SOURCE'):
                    wire.normalize(value)

    def test_renaming_as_u21_does_not_bypass_specific_formula_validation(self):
        value = delivery()
        value.update(version=wire.U21_VERSION, family='u21_xrp_short', rule_id='R2732')
        with self.assertRaisesRegex(wire.WireError, 'UNSUPPORTED_U21_CONTRACT'):
            wire.normalize(value)

    def test_missing_formula_threshold_cannot_borrow_u21_cancel_policy(self):
        for family, rule in (('xrp_r2732', 'R2732'), ('maxpain_proximity', 'HYPE_LONG_TF')):
            with self.subTest(family=family):
                with self.assertRaisesRegex(trade_cards.CardError,
                                           'SOURCE_CANCEL_POLICY_MISSING_OR_UNEXPECTED'):
                    trade_cards.prepare_card(signal(),
                        {'universe': [{'name': 'XRP', 'szDecimals': 2}]},
                        rule_id=rule, threshold_pct=None, record_kind='received_alert',
                        source_stream=family + ':' + '1' * 64,
                        source_expires_at=EXPIRES_AT)

    def test_conditional_stop_and_cluster_terms_have_no_signal_contract(self):
        for terms in (dict(lock_trigger_price='99', lock_stop_price='99.75'),
                      dict(target_price='102', range_hours=24, liquidity='1000000')):
            value = signal()
            value.update(terms)
            with self.subTest(terms=list(terms)):
                with self.assertRaisesRegex(price_precision.PrecisionError,
                                           'COMPLETE_SOURCE_SIGNAL_REQUIRED'):
                    price_precision.prepare_signal(value,
                        {'universe': [{'name': 'XRP', 'szDecimals': 2}]})

    def test_metadata_present_allows_exact_symbol_and_preserves_direction(self):
        for symbol, side, role in (('XRP', 'SHORT', 'short_account'),
                                   ('HYPE', 'LONG', 'long_account')):
            source = signal(symbol, side)
            card = trade_cards.prepare_card(source,
                {'universe': [{'name': symbol, 'szDecimals': 2}]},
                rule_id='AUDIT_ONLY', threshold_pct='2', record_kind='synthetic_test')
            self.assertEqual(card['prepared']['source'], source)
            self.assertEqual(card['account_role'], role)
            self.assertFalse(card['dispatch_enabled'])

    def test_absent_delisted_or_alias_market_cannot_use_guessed_precision(self):
        for symbol in ('XRP', 'HYPE'):
            for entry in ({'name': 'OTHER', 'szDecimals': 2},
                          {'name': symbol + '-PERP', 'szDecimals': 2},
                          {'name': symbol, 'szDecimals': 2, 'isDelisted': True}):
                with self.subTest(symbol=symbol, entry=entry):
                    with self.assertRaisesRegex(price_precision.PrecisionError,
                                               'ASSET_UNAVAILABLE'):
                        price_precision.prepare_signal(signal(symbol), {'universe': [entry]})

    def test_unavailable_record_can_never_be_selected_for_execution(self):
        value = delivery()
        card = unavailable_asset_cards.prepare(value)
        now = datetime.fromisoformat(value['source_at']) + timedelta(seconds=1)
        self.assertIsNone(card['prepared']['execution'])
        self.assertIsNone(card['planning']['quantity'])
        self.assertFalse(selection.eligible(card,
            not_before=now - timedelta(minutes=1), now=now))

    def test_old_historical_entry_is_not_revived_by_recording_it_now(self):
        card = draft_card()
        source = datetime.fromisoformat(SOURCE_AT)
        self.assertTrue(selection.eligible(card, not_before=source, now=source + timedelta(seconds=1)))
        self.assertFalse(selection.eligible(card, not_before=source,
                                           now=source + timedelta(minutes=30)))
        self.assertFalse(selection.eligible(card, not_before=source + timedelta(seconds=1),
                                           now=source + timedelta(seconds=2)))

    def test_extended_expiry_above_ten_minutes_is_rejected(self):
        with self.assertRaisesRegex(trade_cards.CardError, 'SOURCE_EXPIRY_INVALID'):
            trade_cards.prepare_card(signal(), {'universe': [{'name': 'XRP', 'szDecimals': 2}]},
                rule_id='R2732', threshold_pct='0.5', record_kind='received_alert',
                source_expires_at='2026-10-06T10:10:01+00:00')

    def test_new_family_has_no_recipient_independent_occurrence_identity(self):
        for family in ('xrp_r2732', 'maxpain_proximity'):
            first = draft_card(family, '1', 'recipient-one')
            second = draft_card(family, '2', 'recipient-two')
            state = dict(originals={first['card_id']: dict(card=first),
                                    second['card_id']: dict(card=second)},
                         bindings=[dict(card_id=first['card_id'])])
            self.assertNotEqual(first['card_id'], second['card_id'])
            self.assertIsNone(occurrence.identity(first))
            self.assertIsNone(occurrence.duplicate_attempt(deepcopy(state), second['card_id']))

    def test_existing_manual_occurrence_remains_deduplicated_after_restart(self):
        first = draft_card('manual', '1', 'recipient-one')
        second = draft_card('manual', '2', 'recipient-two')
        state = dict(originals={first['card_id']: dict(card=first),
                                second['card_id']: dict(card=second)},
                     bindings=[dict(card_id=first['card_id'])])
        self.assertEqual(occurrence.identity(first), occurrence.identity(second))
        self.assertEqual(occurrence.duplicate_attempt(deepcopy(state), second['card_id']),
                         first['card_id'])

    def test_favorable_mark_does_not_move_existing_fixed_stop(self):
        bindings, snapshot, context, _ = case(q='40', side='SHORT', stop='40', take='40')
        context['mark_price'] = '9.89'  # more than 1% favorable from entry 10
        before = deepcopy(bindings)
        report = card_exit_recovery.plan(bindings, snapshot, context, now_ms=snapshot['at_ms'])
        self.assertEqual(report['desired_exits'][0]['stop_price'], '10.1')
        self.assertIsNone(report['next_step'])
        self.assertEqual(bindings, before)

    def test_adding_conditional_stop_terms_to_recovery_context_is_rejected(self):
        bindings, snapshot, context, _ = case(q='40', side='SHORT', stop='40', take='40')
        context.update(lock_trigger_price='9.9', lock_stop_price='9.975')
        with self.assertRaisesRegex(card_lifecycle.LifecycleError, 'UNEXPECTED_FIELDS'):
            card_exit_recovery.plan(bindings, snapshot, context, now_ms=snapshot['at_ms'])

    def test_one_active_market_blocks_another_entry_regardless_of_target_distance(self):
        from .long_stream_runtime import _entry_market_blocked
        bindings, snapshot, _, originals = case(q='40', stop='40', take='40')
        state = dict(bindings=bindings, originals=originals, pending=None,
                     evidence=dict(bindings=bindings, snapshot=snapshot))
        self.assertTrue(_entry_market_blocked(state))


if __name__ == '__main__':
    unittest.main()
