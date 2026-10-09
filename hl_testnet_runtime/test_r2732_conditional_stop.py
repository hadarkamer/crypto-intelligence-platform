"""R2732 source causality and restart contracts; no I/O, signing or settings."""
from copy import deepcopy
import json
import math
import unittest
from unittest.mock import patch

from . import r2732_conditional_stop as stop
from . import card_lifecycle as life

ENTRY_AT = 1791288960000
CID = 'a'*64
EVENT = 'b'*64


def initial(entry='100'):
    return stop.initialize(card_id=CID, source_event_id=EVENT,
                           reference_entry=entry, entry_at_ms=ENTRY_AT)


def bar(at=ENTRY_AT, **changes):
    return {**dict(open_at_ms=at, open='100', high='100.1', low='99.5', close='100'),
            **changes}


def triggered():
    return stop.advance(initial(), [bar(low='99', close='99.5')],
                        now_ms=ENTRY_AT+stop.MINUTE_MS)


def proposal(state=None, **changes):
    value = triggered() if state is None else state
    args = dict(now_ms=ENTRY_AT+stop.MINUTE_MS,
        remaining_quantity='40', observed_quantity='40', old_oid='1001',
        observed_stop=value['levels']['initial_stop'], mark_price='99.5',
        snapshot_at_ms=ENTRY_AT+stop.MINUTE_MS, pending=None)
    args.update(changes)
    return stop.promotion(value, **args)


class R2732ConditionalStopTests(unittest.TestCase):
    def setUp(self):
        for target in ('http.client.HTTPSConnection', 'socket.create_connection'):
            guard = patch(target, side_effect=AssertionError('NO_NETWORK'))
            guard.start(); self.addCleanup(guard.stop)

    def test_frozen_levels_match_source_floating_arithmetic(self):
        for entry in (100, 2.71818, .00012, 100001.12):
            with self.subTest(entry=entry):
                state = initial(str(entry)); levels = state['levels']
                risk = abs(float(entry)-float(entry)*1.005)
                self.assertEqual(float(levels['original_risk_distance']), risk)
                self.assertEqual(float(levels['locked_stop']), entry-.5*risk)
                self.assertEqual(float(levels['lock_trigger_price']), entry-2*risk)
                self.assertEqual(float(levels['take_profit']), entry*.92)

    def test_initializer_uses_real_shared_contract_occurrence_and_reference(self):
        from experimental_execution_fixtures import r2732_message
        value = r2732_message(entry=2, decision_ms=ENTRY_AT-stop.MINUTE_MS)
        state = stop.initialize_from_contract(CID, value)
        self.assertEqual(state['card_id'], CID)
        self.assertEqual(state['source_event_id'], value['occurrence_id'])
        self.assertEqual(state['entry_at_ms'], ENTRY_AT)
        self.assertEqual(state['levels']['original_risk_distance'],
                         value['policy']['original_risk_distance'])
        self.assertEqual(state['levels']['locked_stop'], value['policy']['locked_stop'])

    def test_initializer_rejects_forged_original_risk_and_other_formula(self):
        from experimental_execution_fixtures import r2732_message, maxpain_message
        value = r2732_message(entry=100, decision_ms=ENTRY_AT-stop.MINUTE_MS)
        value['policy']['original_risk_distance'] = '0.5'
        with self.assertRaisesRegex(ValueError, 'FROZEN_PRICE_MISMATCH'):
            stop.initialize_from_contract(CID, value)
        with self.assertRaisesRegex(stop.ConditionalStopError, 'R2732_CONTRACT_REQUIRED'):
            stop.initialize_from_contract(CID, maxpain_message())

    def test_initial_risk_is_not_decimal_rewrite(self):
        levels = initial()['levels']
        self.assertNotEqual(levels['initial_stop'], '100.5')
        self.assertNotEqual(levels['original_risk_distance'], '.5')

    def test_incomplete_trigger_bar_does_not_lock_or_advance(self):
        state = stop.advance(initial(), [bar(low='98')],
                             now_ms=ENTRY_AT+stop.MINUTE_MS-1)
        self.assertIsNone(state['lock_effective_at_ms'])
        self.assertEqual(state['cursor_ms'], ENTRY_AT-stop.MINUTE_MS)
        self.assertEqual(state['status'], 'AWAITING_CLOSED_BAR')
        self.assertIsNone(proposal(state))

    def test_open_tail_never_hides_already_completed_lock(self):
        state = stop.advance(initial(), [bar(low='99'),
            bar(ENTRY_AT+stop.MINUTE_MS, open='99.5', high='99.6', low='99.2', close='99.5')],
            now_ms=ENTRY_AT+stop.MINUTE_MS)
        self.assertEqual(state['status'], 'AWAITING_CLOSED_BAR')
        self.assertEqual(state['cursor_ms'], ENTRY_AT)
        self.assertEqual(state['lock_effective_at_ms'], ENTRY_AT+stop.MINUTE_MS)
        self.assertIsNotNone(proposal(state))

    def test_processing_batch_and_restarting_each_minute_agree(self):
        bars = [bar(), bar(ENTRY_AT+stop.MINUTE_MS, low='99', close='99.5'),
                bar(ENTRY_AT+2*stop.MINUTE_MS, open='99.5', high='99.6', low='99.2', close='99.4'),
                bar(ENTRY_AT+3*stop.MINUTE_MS, open='99.4', high='99.8', low='99.2', close='99.7')]
        batch = stop.advance(initial(), bars, now_ms=ENTRY_AT+4*stop.MINUTE_MS)
        sequential = initial()
        for row in bars:
            sequential = stop.advance(json.loads(json.dumps(sequential)), [row],
                                      now_ms=row['open_at_ms']+stop.MINUTE_MS)
        self.assertEqual(batch, sequential)
        self.assertEqual(batch['outcome']['reason'], 'PROFIT_LOCK_STOP')

    def test_trigger_bar_uses_previous_stop_and_next_minute_start(self):
        state = triggered()
        self.assertIsNone(state['outcome'])
        self.assertEqual(state['formula_stop'], state['levels']['locked_stop'])
        self.assertEqual(state['trigger_bar_at_ms'], ENTRY_AT)
        self.assertEqual(state['lock_effective_at_ms'], ENTRY_AT+stop.MINUTE_MS)
        self.assertEqual(state['cursor_ms'], ENTRY_AT)
        self.assertFalse(proposal(state)['exchange_protection_confirmed'])

    def test_trigger_bar_original_stop_wins(self):
        state = stop.advance(initial(), [bar(high='100.6', low='98')],
                             now_ms=ENTRY_AT+stop.MINUTE_MS)
        self.assertEqual(state['outcome']['kind'], 'SL')
        self.assertEqual(state['outcome']['reason'], 'INITIAL_STOP')
        self.assertIsNone(state['lock_effective_at_ms'])
        self.assertEqual(state['status'], 'EXIT_RECONCILIATION_REQUIRED')
        self.assertIsNone(proposal(state))

    def test_trigger_bar_take_wins(self):
        state = stop.advance(initial(), [bar(low='91')], now_ms=ENTRY_AT+stop.MINUTE_MS)
        self.assertEqual(state['outcome']['kind'], 'TP')
        self.assertIsNone(state['lock_effective_at_ms'])

    def test_dual_touch_is_sticky_ambiguity_and_retains_exposure(self):
        state = stop.advance(initial(), [bar(high='101', low='91')],
                             now_ms=ENTRY_AT+stop.MINUTE_MS)
        self.assertEqual(state['status'], 'AMBIGUOUS')
        self.assertIsNone(state['outcome']['price'])
        replay = stop.advance(state, [bar(ENTRY_AT+stop.MINUTE_MS)],
                              now_ms=ENTRY_AT+2*stop.MINUTE_MS)
        self.assertEqual(replay, state)
        self.assertIsNone(proposal(state))
        self.assertNotIn('remaining_quantity', state)
        self.assertNotIn('exchange_protection_confirmed', state)

    def test_open_resolves_dual_touch_before_range(self):
        for opening, kind, price in (('101', 'SL', '101'), ('90', 'TP', '92.0')):
            with self.subTest(opening=opening):
                state = stop.advance(initial(), [bar(open=opening, high='102', low='89')],
                                     now_ms=ENTRY_AT+stop.MINUTE_MS)
                self.assertEqual(state['outcome']['kind'], kind)
                self.assertEqual(state['outcome']['price'], price)

    def test_locked_stop_applies_only_to_following_closed_bar(self):
        state = stop.advance(triggered(), [bar(ENTRY_AT+stop.MINUTE_MS,
            open='99.7', high='99.8', low='99.6', close='99.7')],
            now_ms=ENTRY_AT+2*stop.MINUTE_MS)
        self.assertEqual(state['outcome']['reason'], 'PROFIT_LOCK_STOP')
        self.assertEqual(state['outcome']['price'], state['levels']['locked_stop'])

    def test_locked_stop_gap_uses_actual_source_open(self):
        state = stop.advance(triggered(), [bar(ENTRY_AT+stop.MINUTE_MS,
            open='100.2', high='100.3', low='99.7', close='100')],
            now_ms=ENTRY_AT+2*stop.MINUTE_MS)
        self.assertEqual(state['outcome']['price'], '100.2')
        self.assertEqual(state['outcome']['reason'], 'PROFIT_LOCK_STOP')

    def test_trigger_threshold_preserves_exact_ieee_comparison(self):
        value = initial(); levels = value['levels']; bound = float(levels['lock_trigger_price'])
        for low in (math.nextafter(bound, -math.inf), bound, math.nextafter(bound, math.inf)):
            expected = (100-float(low))/float(levels['original_risk_distance']) >= 2
            state = stop.advance(value, [bar(low=str(low))], now_ms=ENTRY_AT+stop.MINUTE_MS)
            self.assertEqual(state['lock_effective_at_ms'] is not None, expected)

    def test_gap_never_skips_a_possible_stop_and_can_be_healed(self):
        state = stop.advance(initial(), [bar(ENTRY_AT+stop.MINUTE_MS, low='98')],
                             now_ms=ENTRY_AT+2*stop.MINUTE_MS)
        self.assertEqual(state['status'], 'PRICE_GAP')
        self.assertEqual(state['next_expected_bar_ms'], ENTRY_AT)
        self.assertIsNone(state['lock_effective_at_ms'])
        healed = stop.advance(state, [bar(), bar(ENTRY_AT+stop.MINUTE_MS, low='98')],
                              now_ms=ENTRY_AT+2*stop.MINUTE_MS)
        self.assertEqual(healed['status'], 'OPEN')
        self.assertEqual(healed['trigger_bar_at_ms'], ENTRY_AT+stop.MINUTE_MS)

    def test_gap_backfill_checks_missed_initial_stop_first(self):
        state = stop.advance(initial(), [bar(ENTRY_AT+stop.MINUTE_MS, low='98')],
                             now_ms=ENTRY_AT+2*stop.MINUTE_MS)
        healed = stop.advance(state, [bar(high='101'), bar(ENTRY_AT+stop.MINUTE_MS, low='98')],
                              now_ms=ENTRY_AT+2*stop.MINUTE_MS)
        self.assertEqual(healed['outcome']['reason'], 'INITIAL_STOP')
        self.assertIsNone(healed['lock_effective_at_ms'])

    def test_no_price_data_cannot_remove_existing_source_intent(self):
        state = stop.advance(triggered(), [], now_ms=ENTRY_AT+stop.MINUTE_MS)
        self.assertEqual(state['status'], 'NO_PRICE_DATA')
        self.assertIsNotNone(state['lock_effective_at_ms'])
        self.assertIsNone(proposal(state))
        restored = stop.advance(state, [bar(low='99', close='99.5')],
                                now_ms=ENTRY_AT+stop.MINUTE_MS)
        self.assertEqual(restored['status'], 'OPEN')
        self.assertIsNotNone(proposal(restored))

    def test_identical_replay_is_idempotent(self):
        state = triggered()
        self.assertEqual(stop.advance(state, [bar(low='99', close='99.5')],
                         now_ms=ENTRY_AT+stop.MINUTE_MS), state)

    def test_historical_pre_entry_bars_are_ignored(self):
        state = stop.advance(initial(), [bar(ENTRY_AT-stop.MINUTE_MS), bar(low='99')],
                             now_ms=ENTRY_AT+stop.MINUTE_MS)
        self.assertIsNotNone(state['lock_effective_at_ms'])

    def test_last_processed_bar_correction_is_not_silently_accepted(self):
        with self.assertRaisesRegex(stop.ConditionalStopError, 'LAST_CLOSED_BAR_CHANGED'):
            stop.advance(triggered(), [bar(low='98')], now_ms=ENTRY_AT+stop.MINUTE_MS)

    def test_duplicate_or_reverse_candles_fail_atomically(self):
        original = initial()
        for bars in ([bar(), bar()], [bar(ENTRY_AT+stop.MINUTE_MS), bar()]):
            with self.assertRaisesRegex(stop.ConditionalStopError, 'UNIQUE_ASCENDING'):
                stop.advance(original, bars, now_ms=ENTRY_AT+2*stop.MINUTE_MS)
            self.assertEqual(original, initial())

    def test_malformed_ohlc_future_stamp_and_numeric_specials_fail(self):
        for changes in (dict(low='101'), dict(close='90'), dict(high='NaN'),
                        dict(low='Infinity'), dict(open=True), dict(open_at_ms=ENTRY_AT+1)):
            with self.subTest(changes=changes), self.assertRaises(life.LifecycleError):
                stop.advance(initial(), [bar(**changes)], now_ms=ENTRY_AT+stop.MINUTE_MS)

    def test_price_source_and_clock_cannot_change(self):
        with self.assertRaisesRegex(stop.ConditionalStopError, 'PRICE_SOURCE_MISMATCH'):
            stop.advance(initial(), [bar()], now_ms=ENTRY_AT+stop.MINUTE_MS,
                         price_source='TESTNET_MARK')
        with self.assertRaisesRegex(stop.ConditionalStopError, 'CLOCK_REGRESSION'):
            stop.advance(triggered(), [], now_ms=ENTRY_AT+stop.MINUTE_MS-1)

    def test_json_roundtrip_preserves_restart_and_partial_fill_plan(self):
        state = triggered()
        restarted = json.loads(json.dumps(state, sort_keys=True))
        self.assertEqual(proposal(restarted), proposal(state))
        more = proposal(restarted, remaining_quantity='70')
        self.assertEqual(more['quantity'], '70')
        self.assertEqual(more['desired_stop'], proposal(state)['desired_stop'])
        self.assertEqual(restarted, state)

    def test_partial_exit_reuses_frozen_stop_with_smaller_remainder(self):
        value = proposal(remaining_quantity='15', observed_quantity='40')
        self.assertEqual(value['quantity'], '15')
        self.assertEqual(value['desired_stop'], triggered()['levels']['locked_stop'])

    def test_confirmed_lock_can_resize_without_relaxing_stop(self):
        state = triggered()
        self.assertIsNone(proposal(state, observed_stop=state['levels']['locked_stop']))
        resize = proposal(state, observed_stop=state['levels']['locked_stop'], remaining_quantity='70')
        self.assertEqual(resize['quantity'], '70')
        self.assertEqual(resize['desired_stop'], state['levels']['locked_stop'])

    def test_pending_unknown_ack_or_prepared_never_creates_second_intent(self):
        for phase in ('PREPARED', 'ACK_UNVERIFIED', 'OUTCOME_UNKNOWN', 'REJECTED'):
            with self.subTest(phase=phase), self.assertRaisesRegex(stop.ConditionalStopError, 'UNRESOLVED'):
                proposal(pending=dict(phase=phase))

    def test_crossed_lock_or_take_requires_reconciliation(self):
        for mark, error in (('100', 'LOCK_ALREADY_CROSSED'), ('91', 'TAKE_ALREADY_CROSSED')):
            with self.assertRaisesRegex(stop.ConditionalStopError, error):
                proposal(mark_price=mark)

    def test_fresh_exchange_snapshot_does_not_make_old_source_fresh(self):
        later = ENTRY_AT+stop.MINUTE_MS+15001
        with self.assertRaisesRegex(stop.ConditionalStopError, 'FRESH_CLOSED_SOURCE'):
            proposal(now_ms=later, snapshot_at_ms=later)

    def test_fresh_source_does_not_make_exchange_evidence_fresh(self):
        with self.assertRaisesRegex(stop.ConditionalStopError, 'FRESH_EXCHANGE'):
            proposal(snapshot_at_ms=ENTRY_AT+stop.MINUTE_MS-15001)

    def test_pre_effective_now_is_not_authority_to_move_stop(self):
        with self.assertRaisesRegex(stop.ConditionalStopError, 'FRESH_CLOSED_SOURCE'):
            proposal(now_ms=ENTRY_AT+stop.MINUTE_MS-1,
                     snapshot_at_ms=ENTRY_AT+stop.MINUTE_MS-1)

    def test_foreign_stop_and_missing_owner_id_fail_closed(self):
        for changes in (dict(observed_stop='101'), dict(old_oid='0'), dict(old_oid='foreign')):
            with self.subTest(changes=changes), self.assertRaises(life.LifecycleError):
                proposal(**changes)

    def test_no_filled_quantity_never_generates_protection_order(self):
        for quantity in ('0', '-1', 'NaN'):
            with self.subTest(quantity=quantity), self.assertRaises(life.LifecycleError):
                proposal(remaining_quantity=quantity)

    def test_tampered_state_fails_instead_of_authorizing_new_stop(self):
        for key, replacement in (('formula_stop', '90'), ('lock_effective_at_ms', ENTRY_AT),
                                 ('cursor_ms', ENTRY_AT-stop.MINUTE_MS), ('price_source', 'OTHER')):
            state = triggered(); state[key] = replacement
            with self.subTest(key=key), self.assertRaises(life.LifecycleError):
                stop.validate(state)

    def test_amendment_plan_is_not_exchange_fill_or_send_authority(self):
        value = proposal()
        self.assertEqual(value['order_requests_sent'], 0)
        self.assertFalse(value['exchange_protection_confirmed'])
        self.assertTrue(value['reduce_only'])
        self.assertNotIn('action', value)
        self.assertNotIn('closed_quantity', value)

    def test_state_and_input_bars_are_never_mutated(self):
        state = initial(); bars = [bar(low='99')]
        before = deepcopy((state, bars))
        stop.advance(state, bars, now_ms=ENTRY_AT+stop.MINUTE_MS)
        self.assertEqual((state, bars), before)


if __name__ == '__main__':
    unittest.main()
