"""Offline R2732 atomic amendment protocol; no HTTP, keys or venue orders."""
from copy import deepcopy
import json
import unittest
from unittest.mock import patch

from . import card_lifecycle as life
from . import r2732_conditional_stop as condition
from . import r2732_stop_dispatch as stop
from experimental_execution_fixtures import r2732_message

T = 1791288960000
NOW = T + 60000
ACCOUNT = '0x' + '2' * 40
CID = 'a' * 64
EVENT = 'b' * 64
META = {'universe': [{'name': 'XRP', 'szDecimals': 1}]}


def envelope(entry='2'):
    return r2732_message(entry=float(entry), decision_ms=T-60000)


def binding(contract):
    execution = stop.prepare_execution(contract, META)['prepared']['execution']
    return dict(card_id=CID, card_digest=life.digest(contract), account=ACCOUNT,
        role='short_account', symbol='XRP', side='SHORT', planned_quantity='100',
        prices={k: execution[k] for k in ('entry', 'stop', 'take_profit')},
        orders=dict(ENTRY=['10'], STOP=['11'], TAKE_PROFIT=['12']), environment='testnet')


def order(binding, leg, quantity):
    price = binding['prices']['entry' if leg == 'ENTRY' else
                              'stop' if leg == 'STOP' else 'take_profit']
    return dict(account=ACCOUNT, symbol='XRP', oid=binding['orders'][leg][0],
        quantity=quantity, price=price, trigger_price=None if leg == 'ENTRY' else price,
        side='A' if leg == 'ENTRY' else 'B', reduce_only=leg != 'ENTRY', state='ACTIVE',
        order_type='LIMIT' if leg == 'ENTRY' else 'SL_MARKET' if leg == 'STOP' else 'TP_LIMIT')


def fill(binding, quantity, *, leg='ENTRY', fid='entry', at=T+1000):
    price = binding['prices']['entry' if leg == 'ENTRY' else
                              'stop' if leg == 'STOP' else 'take_profit']
    return dict(account=ACCOUNT, symbol='XRP', oid=binding['orders'][leg][0],
        fill_id=fid, quantity=quantity, price=price, fee='0.001', fee_token='USDC',
        side='A' if leg == 'ENTRY' else 'B', at_ms=at)


def terminal(oid, quantity='0', *, at=NOW+1):
    return dict(account=ACCOUNT, symbol='XRP', oid=oid, state='CANCELED',
                filled_quantity=quantity, at_ms=at)


def initial(quantity='40', entry='2'):
    contract = envelope(entry)
    owner = binding(contract)
    opens = [order(owner, 'STOP', quantity), order(owner, 'TAKE_PROFIT', quantity)]
    endings = []
    if quantity != '100':
        opens.append(order(owner, 'ENTRY', life.text(100-life.number(quantity))))
    else:
        endings.append({**terminal('10', '100', at=T+1000), 'state': 'FILLED'})
    snapshot = dict(environment='testnet', account=ACCOUNT, symbol='XRP', at_ms=NOW,
        history_complete=True, orders_complete=True, position_quantity='-'+quantity,
        fills=[fill(owner, quantity)], open_orders=opens, terminal_orders=endings)
    return stop.initialize(contract, owner, snapshot, metadata=META, now_ms=NOW)


def due(quantity='40', entry='2'):
    state = initial(quantity, entry)
    e = life.number(state['contract']['entry'])
    candle = dict(open_at_ms=T, open=life.text(e), high=life.text(e),
                  low=life.text(e*life.number('.989')), close=life.text(e*life.number('.99')))
    return stop.advance(state, [candle], now_ms=NOW)


def sample(at=NOW, mark='1.98'):
    return dict(mark_price=mark, at_ms=at)


def reserved(state=None):
    return stop.reserve(due() if state is None else state, META, sample(), now_ms=NOW)


def attempted(state=None):
    state = reserved() if state is None else state
    return stop.begin(state, state['pending'], META, sample(), now_ms=NOW)


def public_result(state, *, oid='13', at=NOW+2):
    req = state['requests'][-1]
    p = req['proposal']; order = stop.wire.requested_order(p['action'])
    snapshot = deepcopy(state['snapshot']); snapshot['at_ms'] = at
    snapshot['open_orders'] = [row for row in snapshot['open_orders'] if row['oid'] != p['old_oid']]
    snapshot['terminal_orders'].append(terminal(p['old_oid'], at=at))
    snapshot['open_orders'].append(dict(account=ACCOUNT, symbol='XRP', oid=oid,
        quantity=p['quantity'], price=order['p'], trigger_price=order['p'], side='B',
        reduce_only=True, state='ACTIVE', order_type='SL_MARKET'))
    raw = dict(status='order', order=dict(status='open', statusTimestamp=at,
        order=dict(oid=int(oid), cloid=order['c'], coin='XRP', side='B',
            limitPx=order['p'], origSz=order['s'], sz=order['s'], reduceOnly=True,
            isTrigger=True, triggerPx=order['p'], isPositionTpsl=False,
            orderType='Stop Market')))
    return snapshot, raw


class R2732StopDispatchTests(unittest.TestCase):
    def setUp(self):
        for target in ('http.client.HTTPSConnection', 'socket.create_connection',
                       'hyperliquid_testnet_executor._wallet',
                       'hyperliquid_testnet_executor._signed_body'):
            guard = patch(target, side_effect=AssertionError('NO_NETWORK_OR_SIGNER'))
            guard.start(); self.addCleanup(guard.stop)

    def test_lock_amendment_pins_exact_owner_quantity_and_original_prices(self):
        state = due(); before = deepcopy(state)
        reserved_state = reserved(state); request = reserved_state['requests'][-1]
        action = request['proposal']['action']; item = action['modifies'][0]
        self.assertEqual(action['type'], 'batchModify')
        self.assertEqual(item['oid'], 11)
        self.assertEqual(item['order'], dict(a=0, b=True, p='1.995', s='40', r=True,
            t=dict(trigger=dict(isMarket=True, triggerPx='1.995', tpsl='sl')),
            c=item['order']['c']))
        self.assertEqual(state, before)
        self.assertEqual(reserved_state['original_binding'], before['original_binding'])
        self.assertEqual(reserved_state['stop_terms'], {'11': '2.01'})
        self.assertFalse(stop.review(reserved_state, now_ms=NOW)['needs_review'])

    def test_no_trigger_or_open_trigger_candle_cannot_reserve(self):
        for state in (initial(), stop.advance(initial(), [dict(open_at_ms=T,
                open='2', high='2', low='1.978', close='1.98')], now_ms=NOW-1)):
            result = stop.reserve(state, META, sample(), now_ms=NOW)
            self.assertIsNone(result['pending'])
            self.assertEqual(result['requests'], [])

    def test_begin_is_durable_once_across_restart(self):
        state = attempted()
        restarted = json.loads(json.dumps(state))
        with self.assertRaisesRegex(life.LifecycleError, 'ALREADY_CONSUMED'):
            stop.begin(restarted, restarted['pending'], META, sample(), now_ms=NOW)
        with self.assertRaisesRegex(life.LifecycleError, 'UNRESOLVED'):
            reserved(restarted)
        with self.assertRaisesRegex(life.LifecycleError, 'CANNOT_BE_ABANDONED'):
            stop.abandon_unsent(restarted, restarted['pending'])

    def test_acknowledgement_never_changes_stop_or_retires_old_order(self):
        state = attempted()
        reply = dict(status='ok', response=dict(type='order', data=dict(statuses=[dict(resting=dict(oid=13))])))
        result = stop.record_reply(state, state['pending'], reply)
        self.assertEqual(result['requests'][-1]['phase'], 'ACKNOWLEDGED')
        self.assertEqual(result['stop_terms'], {'11': '2.01'})
        self.assertIsNotNone(result['pending'])
        self.assertEqual(result['snapshot'], state['snapshot'])

    def test_lost_reply_reconciles_same_cloid_without_second_attempt(self):
        state = attempted()
        state = stop.record_reply(state, state['pending'], None)
        state = json.loads(json.dumps(state))
        snapshot, lookup = public_result(state)
        result = stop.observe(state, snapshot, now_ms=NOW+2, lookup=lookup)
        self.assertIsNone(result['pending'])
        self.assertEqual(result['requests'][-1]['phase'], 'OBSERVED')
        self.assertEqual(result['stop_terms'], {'11': '2.01', '13': '1.995'})
        self.assertEqual(result['original_binding'], state['original_binding'])
        self.assertFalse(stop.review(result, now_ms=NOW+2)['needs_review'])
        self.assertEqual(stop.reserve(result, META, sample(NOW+2), now_ms=NOW+2), result)

    def test_generic_lifecycle_remains_strict_for_promoted_stop(self):
        state = attempted(); snapshot, lookup = public_result(state)
        observed = stop.observe(state, snapshot, now_ms=NOW+2, lookup=lookup)
        binding = stop._binding(observed)
        self.assertIn('ORDER_TERMS_MISMATCH', life.review([binding], snapshot,
                        now_ms=NOW+2)['cards'][0]['issues'])
        self.assertFalse(stop.review(observed, now_ms=NOW+2)['needs_review'])

    def test_unknown_lookup_never_frees_pending_even_with_old_stop_present(self):
        state = stop.record_reply(attempted(), attempted()['pending'], None)
        snapshot = deepcopy(state['snapshot']); snapshot['at_ms'] = NOW+2
        with self.assertRaisesRegex(life.LifecycleError, 'UNRESOLVED'):
            stop.observe(state, snapshot, now_ms=NOW+2, lookup={'status': 'unknownOid'})
        self.assertEqual(state['requests'][-1]['phase'], 'UNKNOWN')

    def test_rejection_keeps_old_stop_and_latches_review_only_after_public_proof(self):
        state = attempted()
        rejected = stop.record_reply(state, state['pending'], dict(status='err', response='Invalid TP/SL price.'))
        self.assertEqual(rejected['requests'][-1]['phase'], 'REJECTED')
        self.assertIsNotNone(rejected['pending'])
        snapshot = deepcopy(state['snapshot']); snapshot['at_ms'] = NOW+2
        result = stop.observe(rejected, snapshot, now_ms=NOW+2, lookup={'status': 'unknownOid'})
        self.assertEqual(result['halted_reason'], 'STOP_AMENDMENT_REJECTED')
        self.assertEqual(result['stop_terms'], {'11': '2.01'})
        self.assertFalse(stop.review(result, now_ms=NOW+2)['needs_review'])
        with self.assertRaisesRegex(life.LifecycleError, 'REJECTED_REVIEW'):
            stop.reserve(result, META, sample(NOW+2), now_ms=NOW+2)

    def test_rejected_receipt_conflicting_public_order_stays_blocked(self):
        state = attempted()
        rejected = stop.record_reply(state, state['pending'], dict(status='err', response='Invalid TP/SL price.'))
        snapshot, lookup = public_result(rejected)
        with self.assertRaisesRegex(life.LifecycleError, 'REJECTION_CONFLICTS'):
            stop.observe(rejected, snapshot, now_ms=NOW+2, lookup=lookup)

    def test_new_public_order_requires_old_stop_terminal(self):
        state = attempted(); snapshot, lookup = public_result(state)
        for case in ('missing', 'still-open', 'filled', 'pre-attempt'):
            with self.subTest(case=case):
                bad = deepcopy(snapshot)
                if case == 'missing': bad['terminal_orders'] = []
                elif case == 'still-open': bad['open_orders'].append(order(state['original_binding'], 'STOP', '40'))
                elif case == 'filled': bad['terminal_orders'][-1]['state'] = 'FILLED'
                else: bad['terminal_orders'][-1]['at_ms'] = NOW-1
                with self.assertRaises(life.LifecycleError):
                    stop.observe(state, bad, now_ms=NOW+2, lookup=lookup)

    def test_public_lookup_must_match_exact_reserved_terms(self):
        state = attempted(); snapshot, lookup = public_result(state)
        for field, value in (('cloid', '0x'+'c'*32), ('coin', 'BTC'), ('side', 'A'),
                             ('reduceOnly', False), ('limitPx', '1.994'),
                             ('triggerPx', '1.994'), ('origSz', '50'), ('oid', 11)):
            with self.subTest(field=field):
                bad = deepcopy(lookup); bad['order']['order'][field] = value
                with self.assertRaises(life.LifecycleError):
                    stop.observe(state, snapshot, now_ms=NOW+2, lookup=bad)

    def test_public_lookup_cannot_postdate_snapshot(self):
        state = attempted(); snapshot, lookup = public_result(state)
        lookup['order']['statusTimestamp'] = NOW+3
        with self.assertRaisesRegex(life.LifecycleError, 'NEWER_THAN_SNAPSHOT'):
            stop.observe(state, snapshot, now_ms=NOW+3, lookup=lookup)

    def test_replacement_remaining_plus_fills_must_equal_requested_quantity(self):
        state = attempted(); snapshot, lookup = public_result(state)
        for quantity in ('30', '50'):
            with self.subTest(quantity=quantity):
                bad = deepcopy(snapshot); bad['open_orders'][-1]['quantity'] = quantity
                with self.assertRaisesRegex(life.LifecycleError, 'QUANTITY_NOT_PROVEN'):
                    stop.observe(state, bad, now_ms=NOW+2, lookup=lookup)

    def test_changed_market_index_or_precision_requires_review(self):
        state = reserved()
        for meta in ({'universe': [{'name': 'BTC', 'szDecimals': 1}, {'name': 'XRP', 'szDecimals': 1}]},
                     {'universe': [{'name': 'XRP', 'szDecimals': 2}]}):
            with self.subTest(meta=meta):
                with self.assertRaisesRegex(life.LifecycleError, 'MARKET_CHANGED'):
                    stop.begin(state, state['pending'], meta, sample(), now_ms=NOW)

    def test_observed_inventory_must_match_exact_owned_stop_terms(self):
        state = attempted(); snapshot, lookup = public_result(state)
        for field, value in (('trigger_price', '1.994'), ('price', '1.994'),
                             ('reduce_only', False), ('side', 'A'), ('state', 'WAITING_PARENT')):
            with self.subTest(field=field):
                bad = deepcopy(snapshot); bad['open_orders'][-1][field] = value
                with self.assertRaises(life.LifecycleError):
                    stop.observe(state, bad, now_ms=NOW+2, lookup=lookup)

    def test_during_pending_fill_changes_require_public_reconciliation_then_resize(self):
        state = attempted(); snapshot, lookup = public_result(state)
        snapshot['fills'].append(fill(state['original_binding'], '20', fid='late-entry', at=NOW+1))
        snapshot['position_quantity'] = '-60'
        next(o for o in snapshot['open_orders'] if o['oid'] == '10')['quantity'] = '40'
        observed = stop.observe(state, snapshot, now_ms=NOW+2, lookup=lookup)
        result = stop.reserve(observed, META, sample(NOW+2), now_ms=NOW+2)
        proposal = result['requests'][-1]['proposal']
        self.assertEqual((proposal['quantity'], proposal['old_oid']), ('60', '13'))
        self.assertEqual(stop.wire.requested_order(proposal['action'])['p'], '1.995')
        self.assertEqual(result['condition']['levels'], state['condition']['levels'])

    def test_partial_take_before_amend_observation_records_overcoverage_and_resizes_stop(self):
        state = attempted(reserved(due('100'))); snapshot, lookup = public_result(state)
        snapshot['fills'].append(fill(state['original_binding'], '20', leg='TAKE_PROFIT', fid='partial-take', at=NOW+1))
        snapshot['position_quantity'] = '-80'
        next(o for o in snapshot['open_orders'] if o['oid'] == '12')['quantity'] = '80'
        observed = stop.observe(state, snapshot, now_ms=NOW+2, lookup=lookup)
        self.assertIn('STOP_EXCEEDS_CARD_REMAINDER', stop.review(observed, now_ms=NOW+2)['cards'][0]['issues'])
        result = stop.reserve(observed, META, sample(NOW+2), now_ms=NOW+2)
        self.assertEqual(result['requests'][-1]['proposal']['quantity'], '80')

    def test_old_stop_partial_fill_during_atomic_amend_does_not_invent_quantity(self):
        state = attempted(reserved(due('100'))); snapshot, lookup = public_result(state)
        snapshot['fills'].append(fill(state['original_binding'], '10', leg='STOP',
                                     fid='old-stop-race', at=NOW+1))
        snapshot['position_quantity'] = '-90'
        next(o for o in snapshot['terminal_orders'] if o['oid'] == '11')['filled_quantity'] = '10'
        next(o for o in snapshot['open_orders'] if o['oid'] == '12')['quantity'] = '90'
        observed = stop.observe(state, snapshot, now_ms=NOW+2, lookup=lookup)
        result = stop.reserve(observed, META, sample(NOW+2), now_ms=NOW+2)
        self.assertEqual(result['requests'][-1]['proposal']['quantity'], '90')
        self.assertEqual(result['requests'][-1]['proposal']['old_oid'], '13')
        self.assertEqual(result['original_binding']['planned_quantity'], '100')

    def test_unapproved_formula_or_wrong_actual_initial_prices_never_become_stop_owner(self):
        from experimental_execution_contract import ContractError
        state = initial()
        for case in ('changed-policy', 'wrong-source-price', 'wrong-binding-price'):
            with self.subTest(case=case):
                contract = deepcopy(state['contract']); owner = deepcopy(state['original_binding'])
                if case == 'changed-policy': contract['policy']['profit_R'] = '1'
                elif case == 'wrong-source-price': contract['entry'] = '2.01'
                else: owner['prices']['stop'] = '2.02'
                with self.assertRaises((life.LifecycleError, ContractError)):
                    stop.initialize(contract, owner, state['snapshot'], metadata=META, now_ms=NOW)

    def test_fill_between_reservation_and_begin_invalidates_original_intent(self):
        state = reserved(); snapshot = deepcopy(state['snapshot']); snapshot['at_ms'] = NOW+1
        snapshot['fills'].append(fill(state['original_binding'], '20', fid='late-entry', at=NOW+1))
        snapshot['position_quantity'] = '-60'
        next(o for o in snapshot['open_orders'] if o['oid'] == '10')['quantity'] = '40'
        changed = stop.observe(state, snapshot, now_ms=NOW+1)
        with self.assertRaisesRegex(life.LifecycleError, 'CHANGED_REPLAN'):
            stop.begin(changed, changed['pending'], META, sample(NOW+1), now_ms=NOW+1)
        retired = stop.abandon_unsent(changed, changed['pending'])
        replanned = stop.reserve(retired, META, sample(NOW+1), now_ms=NOW+1)
        self.assertEqual(replanned['requests'][-1]['proposal']['quantity'], '60')
        self.assertEqual(replanned['requests'][0]['phase'], 'ABANDONED_UNSENT')

    def test_unavailable_asset_stale_source_or_crossed_stop_block(self):
        for case in ('missing', 'delisted', 'stale', 'crossed'):
            with self.subTest(case=case):
                state = due()
                meta = deepcopy(META); now = NOW; mark = '1.98'
                if case == 'missing': meta['universe'][0]['name'] = 'BTC'
                elif case == 'delisted': meta['universe'][0]['isDelisted'] = True
                elif case == 'stale': now = NOW+15001
                elif case == 'crossed': mark = '1.995'
                with self.assertRaises(life.LifecycleError):
                    stop.reserve(state, meta, sample(now, mark), now_ms=now)

    def test_real_producer_prices_use_existing_rounding_without_changing_source_rule(self):
        for entry in ('2.3', '100', '2.0', '1.234'):
            with self.subTest(entry=entry):
                state = due(entry=entry); before = deepcopy(state)
                mark = life.text(life.number(entry)*life.number('.99'))
                result = stop.reserve(state, META, sample(mark=mark), now_ms=NOW)
                request = result['requests'][-1]
                actual = stop.wire.requested_order(request['proposal']['action'])
                expected = stop.price_precision.round_price(state['condition']['levels']['locked_stop'], 1)
                self.assertEqual(actual['p'], expected)
                self.assertEqual(state, before)
                self.assertEqual(result['condition']['levels'], condition.frozen_levels(entry))
                self.assertEqual(result['contract'], envelope(entry))
                self.assertEqual(result['execution']['prepared']['audit']['policy'], stop.price_precision.POLICY)
                begun = stop.begin(result, result['pending'], META, sample(mark=mark), now_ms=NOW)
                snapshot, lookup = public_result(begun)
                observed = stop.observe(begun, snapshot, now_ms=NOW+2, lookup=lookup)
                self.assertFalse(stop.review(observed, now_ms=NOW+2)['needs_review'])
                self.assertEqual(observed['stop_terms']['13'], expected)

    def test_rounding_cannot_collapse_initial_risk_or_profit_lock(self):
        from .price_precision import PrecisionError
        for entry, decimals in (('2', 6), ('1', 4)):
            with self.subTest(entry=entry, decimals=decimals):
                with self.assertRaises((life.LifecycleError, PrecisionError)):
                    stop.prepare_execution(envelope(entry),
                        {'universe': [{'name': 'XRP', 'szDecimals': decimals}]})

    def test_rounded_lock_crossing_blocks_even_when_source_lock_not_crossed(self):
        state = due(entry='1.234')
        source_lock = life.number(state['condition']['levels']['locked_stop'])
        rounded_lock = life.number(state['execution']['locked_stop'])
        self.assertLess(rounded_lock, source_lock)
        mark = life.text((source_lock+rounded_lock)/2)
        with self.assertRaisesRegex(life.LifecycleError, 'ROUNDED_LOCK_ALREADY_CROSSED'):
            stop.reserve(state, META, sample(mark=mark), now_ms=NOW)

    def test_per_order_stop_terms_cannot_be_forged_or_swapped(self):
        state = attempted(); snapshot, lookup = public_result(state)
        observed = stop.observe(state, snapshot, now_ms=NOW+2, lookup=lookup)
        for case in ('original-price', 'effective-price', 'new-oid', 'binding-price', 'request-price'):
            with self.subTest(case=case):
                bad = deepcopy(observed)
                if case == 'original-price': bad['stop_terms']['11'] = '1.995'
                elif case == 'effective-price': bad['stop_terms']['13'] = '2.01'
                elif case == 'new-oid': bad['stop_terms']['14'] = '1.995'
                elif case == 'binding-price': bad['original_binding']['prices']['stop'] = '1.995'
                else: bad['requests'][0]['proposal']['action']['modifies'][0]['order']['p'] = '1.994'
                with self.assertRaises(life.LifecycleError): stop.validate(bad)

    def test_immutable_fill_and_terminal_history_cannot_disappear_after_restart(self):
        state = attempted(); snapshot, lookup = public_result(state)
        observed = stop.observe(state, snapshot, now_ms=NOW+2, lookup=lookup)
        restarted = json.loads(json.dumps(observed))
        for field in ('fills', 'terminal_orders'):
            with self.subTest(field=field):
                bad = deepcopy(snapshot); bad['at_ms'] = NOW+3; bad[field] = []
                with self.assertRaisesRegex(life.LifecycleError, 'EVIDENCE_REGRESSION'):
                    stop.observe(restarted, bad, now_ms=NOW+3)

    def test_complete_new_stop_fill_records_actual_closure_without_inventing_entry_fill(self):
        state = attempted(reserved(due('100'))); snapshot, lookup = public_result(state)
        snapshot['open_orders'] = []
        snapshot['position_quantity'] = '0'
        stop_fill = fill(state['original_binding'], '100', leg='STOP', fid='closed-stop', at=NOW+2)
        stop_fill.update(oid='13', price='1.995')
        snapshot['fills'].append(stop_fill)
        snapshot['terminal_orders'] += [{**terminal('13', '100', at=NOW+2), 'state': 'FILLED'}, terminal('12')]
        lookup['order']['status'] = 'filled'
        observed = stop.observe(state, snapshot, now_ms=NOW+2, lookup=lookup)
        view = stop.review(observed, now_ms=NOW+2)['cards'][0]
        self.assertTrue(view['closure_verified'])
        self.assertEqual((view['entry_quantity'], view['exit_quantity']), ('100', '100'))


if __name__ == '__main__':
    unittest.main()
