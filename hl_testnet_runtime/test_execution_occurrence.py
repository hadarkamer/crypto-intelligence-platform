"""Economic alert copies remain records but cannot repeat an attempted entry."""
from copy import deepcopy
from datetime import datetime
import unittest
from unittest.mock import patch

import alert_cards_wire as wire
from alert_cards_forwarder_selftest import delivery
from alert_cards_u21_selftest import delivered, FIRST, SECOND
from . import execution_occurrence as occurrence, trade_cards


def card(value=None, *, kind='received_alert', stream=None):
    spec = wire.normalize(delivery() if value is None else value)
    return trade_cards.prepare_card(spec['signal'],
        {'universe': [{'name': spec['signal']['symbol'], 'szDecimals': 2}]},
        rule_id=spec['rule_id'], threshold_pct=spec['threshold_pct'],
        record_kind=kind, source_stream=stream or spec['source_stream'],
        source_expires_at=spec['source_expires_at'])


def two_cards(side='LONG', family='manual'):
    one = delivery(side, identity='first', family=family)
    two = delivery(side, identity='second', family=family)
    two['scope_hash'] = '2' * 64
    return card(one), card(two)


def state(first, second):
    return dict(originals={first['card_id']: dict(card=first),
                           second['card_id']: dict(card=second)}, bindings=[])


class ExecutionOccurrenceTests(unittest.TestCase):
    def setUp(self):
        network = patch('http.client.HTTPSConnection', side_effect=AssertionError('No network'))
        network.start()
        self.addCleanup(network.stop)

    def test_two_recipients_keep_distinct_cards_and_one_exact_occurrence(self):
        for side in ('LONG', 'SHORT'):
            for family in ('manual', 'dual_cvd65'):
                first, second = two_cards(side, family)
                self.assertNotEqual(first['card_id'], second['card_id'])
                self.assertNotEqual(first['event_id'], second['event_id'])
                self.assertEqual(occurrence.identity(first), occurrence.identity(second))
                saved = state(first, second)
                before = deepcopy(saved)
                self.assertIsNone(occurrence.duplicate_attempt(saved, second['card_id']))
                self.assertEqual(saved, before)

    def test_bound_closed_peer_and_unknown_attempt_block_after_restart(self):
        first, second = two_cards()
        for evidence in ('bound', 'attempt', 'rejected', 'unsent'):
            saved = state(first, second)
            if evidence == 'bound':
                saved['bindings'] = [dict(card_id=first['card_id'])]
            elif evidence == 'attempt':
                saved['entry_timing_armed'] = {first['card_id']: 1790000000000}
            else:
                saved['originals'][first['card_id']]['entry_' + evidence + '_no_retry'] = True
            restarted = deepcopy(saved)
            self.assertEqual(occurrence.duplicate_attempt(restarted, second['card_id']), first['card_id'])
            self.assertEqual(restarted, saved)

    def test_source_time_prices_rule_and_family_are_never_approximately_merged(self):
        first, second = two_cards()
        values = []
        for key, value in (('source_at', '2026-09-16T12:05:00.001+00:00'),
                           ('rule_id', 'C2')):
            changed = delivery(identity='second'); changed[key] = value
            values.append(changed)
        for label, old, new in (('שער בסיס', '100', '100.0001'),
                                ('סטופלוס', '98', '98.0001'),
                                ('טייק פרופיט', '102', '102.0001')):
            changed = delivery(identity='second')
            changed['text'] = changed['text'].replace(label + ':</b> ' + old,
                                                      label + ':</b> ' + new)
            if label == 'שער בסיס':
                changed['text'] = changed['text'].replace('שער בסיס — תחילת נתוני הבסיס:</b> 100',
                                                          'שער בסיס — תחילת נתוני הבסיס:</b> 100.0001')
                changed['reference']['price'] = new
            values.append(changed)
        values.extend((delivery('SHORT', identity='second'),
                       delivery(identity='second', family='dual_cvd65')))
        for value in values:
            other = card(value)
            saved = state(first, other)
            saved['entry_timing_armed'] = {first['card_id']: 1790000000000}
            with self.subTest(value=value['source_at'], rule=value['rule_id'], family=value['family']):
                self.assertNotEqual(occurrence.identity(first), occurrence.identity(other))
                self.assertIsNone(occurrence.duplicate_attempt(saved, other['card_id']))

    def test_u21_copy_uses_precise_frozen_terms_and_keeps_half_percent_stop(self):
        first = wire.u21_delivery(delivered(FIRST), '1' * 64, wire.U21_CONFIG)
        second = wire.u21_delivery(delivered(SECOND), '2' * 64, wire.U21_CONFIG)
        one, two = card(first), card(second)
        self.assertEqual(occurrence.identity(one), occurrence.identity(two))
        self.assertEqual(one['prepared']['source']['stop'], first['stop'])
        saved = state(one, two)
        saved['entry_timing_armed'] = {one['card_id']: 1790000000000}
        self.assertEqual(occurrence.duplicate_attempt(saved, two['card_id']), one['card_id'])

    def test_synthetic_legacy_or_unknown_stream_has_no_occurrence_claim(self):
        for item in (card(kind='synthetic_test'), card(kind='historical_review'),
                     card(stream='legacy_testnet_review'), card(stream='custom:' + '1' * 64)):
            self.assertIsNone(occurrence.identity(item))

    def test_source_string_precision_is_preserved_without_merging_rounded_prices(self):
        first, _ = two_cards()
        source = deepcopy(first['prepared']['source'])
        source['event_id'] = 'second'
        source['entry'] = '100.00000001'
        other = trade_cards.prepare_card(source,
            {'universe': [{'name': source['symbol'], 'szDecimals': 2}]},
            rule_id=first['rule']['id'], threshold_pct=first['rule']['threshold_pct'],
            record_kind='received_alert', source_stream='manual:' + '2' * 64,
            source_expires_at=first['source_expires_at'])
        self.assertEqual(first['prepared']['execution']['entry'], other['prepared']['execution']['entry'])
        self.assertNotEqual(occurrence.identity(first), occurrence.identity(other))

    def test_false_no_retry_marker_does_not_invent_an_attempt(self):
        first, second = two_cards()
        saved = state(first, second)
        saved['originals'][first['card_id']].update(entry_rejected_no_retry=False,
                                                  entry_unsent_no_retry=False)
        self.assertIsNone(occurrence.duplicate_attempt(saved, second['card_id']))

    def test_real_choice_skips_recipient_copy_after_terminal_attempt_and_keeps_new_event(self):
        from . import filled_quantity_dispatch as dispatch, filled_quantity_exits as exits
        from . import card_lifecycle as life
        account = '0x' + '1' * 40
        routes = {'long_account': dict(account=account)}
        meta = {'universe': [{'name': 'DEMO', 'szDecimals': 2}]}
        first, second = two_cards()
        at = int(datetime.fromisoformat(first['prepared']['source']['at']).timestamp() * 1000)
        now = at + 1000
        saved = dict(bucket=life.digest(['testnet', account, 'DEMO']),
            account=account, symbol='DEMO', revision=1, bindings=[], pending=None,
            originals={}, entry_timing_armed={first['card_id']: at},
            evidence=dict(bindings=[], snapshot=dict(environment='testnet', account=account,
                symbol='DEMO', at_ms=now, history_complete=True, orders_complete=True,
                position_quantity='0', fills=[], open_orders=[], terminal_orders=[])))
        for item in (first, second):
            saved['originals'][item['card_id']] = dict(card=item,
                draft=exits.prepare_entry(item, meta, account, routes))
        saved['originals'][first['card_id']]['entry_rejected_no_retry'] = True
        sample = dict(mark_price='100', at_ms=now)
        self.assertIsNone(dispatch.choose(saved, routes, meta, sample, now_ms=now))
        changed = delivery(identity='genuine-next-event')
        changed['source_at'] = '2026-09-16T12:05:00.001+00:00'
        next_event = card(changed)
        saved['originals'][next_event['card_id']] = dict(card=next_event,
            draft=exits.prepare_entry(next_event, meta, account, routes))
        proposal = dispatch.choose(saved, routes, meta, sample, now_ms=now)
        self.assertEqual(proposal['operation'], 'ENTRY')
        self.assertEqual(proposal['card_id'], next_event['card_id'])

    def test_real_choice_does_not_reenter_closed_occurrence_from_another_recipient(self):
        from . import filled_quantity_dispatch as dispatch, filled_quantity_exits as exits
        from . import card_lifecycle as life
        from .test_card_lifecycle import fill, terminal
        for side in ('LONG', 'SHORT'):
            first, second = two_cards(side)
            account = '0x' + ('1' if side == 'LONG' else '2') * 40
            routes = {first['account_role']: dict(account=account)}
            meta = {'universe': [{'name': 'DEMO', 'szDecimals': 2}]}
            at = int(datetime.fromisoformat(first['prepared']['source']['at']).timestamp() * 1000)
            now = at + 1000
            bound = life.binding_from_card(first, account, routes,
                dict(ENTRY=['10'], STOP=['11'], TAKE_PROFIT=['12']))
            quantity = first['planning']['quantity']
            fills = [fill(bound, qty=quantity), fill(bound, 'TAKE_PROFIT', qty=quantity)]
            for index, item in enumerate(fills):
                item['at_ms'] = at + 100 + index * 100
            terms = [terminal(bound, 'ENTRY', quantity), terminal(bound, 'STOP', '0'),
                     terminal(bound, 'TAKE_PROFIT', quantity)]
            for item in terms:
                item['at_ms'] = at + 500
            saved = dict(bucket=life.digest(['testnet', account, 'DEMO']), account=account,
                symbol='DEMO', revision=1, pending=None, bindings=[bound], originals={},
                evidence=dict(bindings=[bound], snapshot=dict(environment='testnet',
                    account=account, symbol='DEMO', at_ms=now, history_complete=True,
                    orders_complete=True, position_quantity='0', fills=fills,
                    open_orders=[], terminal_orders=terms)))
            for item in (first, second):
                saved['originals'][item['card_id']] = dict(card=item,
                    draft=exits.prepare_entry(item, meta, account, routes))
            report = life.review(saved['bindings'], saved['evidence']['snapshot'], now_ms=now)
            self.assertTrue(report['cards'][0]['closure_verified'])
            self.assertIsNone(dispatch.choose(saved, routes, meta,
                dict(mark_price='100', at_ms=now), now_ms=now))


if __name__ == '__main__':
    unittest.main()
