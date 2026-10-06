"""g65 execution preserves frozen source chronology without exchange I/O."""
from copy import deepcopy
import json
import unittest

from experimental_execution_fixtures import sol_g65_message, hype_row71205_message
from . import sol_g65_conditional_stop as stop
from . import experimental_plan_store as store
from experimental_execution_contract import iso_ms, moment_ms, LEASE_MS, RANK, validate


def initial():
    message = sol_g65_message()
    return stop.initialize_from_contract(message['occurrence_id'], message)


def bar(at=None, **kwargs):
    at = initial()['reference_at_ms'] if at is None else at
    return dict(open_at_ms=at, **{**dict(open='100', high='100.4', low='99.5', close='100'), **kwargs})


def advance(state, rows):
    return stop.advance(state, rows, now_ms=rows[-1]['open_at_ms']+60000)


class G65ConditionalTests(unittest.TestCase):
    def test_frozen_reference_is_not_actual_fill_price(self):
        state = initial()
        self.assertEqual(state['levels']['entry'], str(100*1.005))
        self.assertEqual(state['levels']['initial_stop'], str(100*1.0125))
        self.assertEqual(state['levels']['locked_stop'], str(100*1.005-.25*(100*1.005-100*.9825)))
        self.assertIsNone(state['contract']['expires_at'])

    def test_wait_has_no_formula_timeout(self):
        state = initial(); at = state['reference_at_ms']
        rows = [bar(at+i*60000) for i in range(1500)]
        state = advance(state, rows)
        self.assertEqual(state['phase'], 'PENDING')
        self.assertIsNone(state['outcome'])

    def test_entry_cancel_same_bar_ambiguous_without_intrabar_order(self):
        state = advance(initial(), [bar(high='100.6', low='98.9')])
        self.assertEqual(state['outcome']['reason'], 'ENTRY_CANCEL_ORDER_UNKNOWN')
        self.assertEqual(state['phase'], 'PENDING')

    def test_cancel_at_open_precedes_entry(self):
        state = advance(initial(), [bar(open='98.9', high='100.6', low='98.9')])
        self.assertEqual(state['status'], 'CANCELLED')
        self.assertIsNone(state['entry_at_ms'])

    def test_fill_bar_uses_close_and_later_bar_uses_low(self):
        state = initial(); at = state['reference_at_ms']
        state = advance(state, [bar(open='100.6', high='100.7', low='98.8', close='100')])
        self.assertEqual(state['phase'], 'OPEN')
        self.assertIsNone(state['lock_effective_at_ms'])
        state = advance(state, [bar(at+60000, open='100', high='100.1', low='98.8', close='100')])
        self.assertEqual(state['lock_effective_at_ms'], at+120000)
        self.assertIsNone(state['outcome'])

    def test_fill_bar_close_can_trigger_from_following_minute(self):
        state = advance(initial(), [bar(open='100.6', high='100.7', low='98.8', close='98.8')])
        self.assertEqual(state['lock_effective_at_ms'], state['reference_at_ms']+60000)
        self.assertIsNone(state['outcome'])

    def test_initial_stop_wins_over_promotion(self):
        state = advance(initial(), [bar(open='100.6', high='101.3', low='98.8', close='98.8')])
        self.assertEqual(state['outcome']['reason'], 'INITIAL_STOP')
        self.assertIsNone(state['lock_effective_at_ms'])

    def test_unordered_entry_take_touch_not_reported_as_win(self):
        # Source entry/cancel ambiguity already dominates this larger bar.
        state = advance(initial(), [bar(high='100.6', low='98.2')])
        self.assertEqual(state['status'], 'AMBIGUOUS')
        self.assertNotEqual(state['outcome']['kind'], 'TP')

    def test_open_bar_cannot_trigger(self):
        state = initial(); at = state['reference_at_ms']
        state = stop.advance(state, [bar(open='100.6', high='100.7', low='98.8', close='98.8')], now_ms=at+59999)
        self.assertEqual(state['phase'], 'PENDING')
        self.assertEqual(state['status'], 'AWAITING_CLOSED_BAR')

    def test_gap_repaired_without_resetting_reference(self):
        state = initial(); at = state['reference_at_ms']
        gap = advance(state, [bar(at+60000)])
        self.assertEqual(gap['status'], 'PRICE_GAP')
        repaired = stop.advance(gap, [bar(), bar(at+60000)], now_ms=at+120000)
        self.assertEqual(repaired, advance(state, [bar(), bar(at+60000)]))

    def test_restart_and_batch_are_identical(self):
        state = initial(); at = state['reference_at_ms']
        rows = [bar(), bar(at+60000, open='100.6', high='100.7', low='98.8', close='98.8'),
            bar(at+120000, open='99', high='99.95', low='98.8', close='99')]
        batch = advance(state, rows)
        for row in rows:
            state = advance(json.loads(json.dumps(state)), [row])
        self.assertEqual(state, batch)
        self.assertEqual(state['outcome']['reason'], 'PROFIT_LOCK_STOP')

    def test_corrected_last_candle_is_rejected(self):
        state = advance(initial(), [bar()])
        with self.assertRaisesRegex(ValueError, 'LAST_CLOSED_BAR_CHANGED'):
            advance(state, [bar(low='99.4')])

    def test_forged_state_and_source_refused(self):
        state = initial(); state['levels']['locked_stop'] = '90'
        with self.assertRaisesRegex(ValueError, 'STATE_INVALID'):
            stop.validate(state)
        with self.assertRaisesRegex(ValueError, 'SOURCE_MISMATCH'):
            stop.advance(initial(), [bar()], now_ms=initial()['reference_at_ms']+60000,
                price_source='BINANCE_SPOT_TRADE_1M')
        with self.assertRaisesRegex(ValueError, 'CONTRACT_REQUIRED'):
            stop.initialize_from_contract('a'*64, hype_row71205_message())

    def test_impossible_phase_and_outcome_cannot_survive_restart(self):
        state = initial(); state['status'] = 'OPEN'
        with self.assertRaisesRegex(ValueError, 'STATE_INVALID'):
            stop.validate(state)
        state = advance(initial(), [bar(open='98.9', high='100.6', low='98.9')])
        for changes in ({'kind': 'TP'}, {'price': '100'}, {'reason': 'anything'}):
            altered = deepcopy(state); altered['outcome'].update(changes)
            with self.assertRaisesRegex(ValueError, 'STATE_INVALID'):
                stop.validate(altered)

    def test_promotion_waits_for_owned_fresh_remaining_and_preserves_rule_levels(self):
        state = advance(initial(), [bar(open='100.6', high='100.7', low='98.8', close='98.8')])
        at = state['reference_at_ms']+60000
        args = dict(now_ms=at, remaining_quantity='2', observed_quantity='4', old_oid='100',
            observed_stop=state['levels']['initial_stop'], mark_price='99',
            snapshot_at_ms=at, pending=None)
        intent = stop.promotion(state, **args)
        self.assertEqual(intent['quantity'], '2')
        self.assertEqual(intent['desired_stop'], state['levels']['locked_stop'])
        self.assertFalse(intent['exchange_protection_confirmed'])
        with self.assertRaisesRegex(ValueError, 'UNRESOLVED'):
            stop.promotion(state, **{**args, 'pending': {'request': 'unknown'}})
        with self.assertRaisesRegex(ValueError, 'FRESH_SOURCE'):
            stop.promotion(state, **{**args, 'now_ms': at+15001})
        with self.assertRaisesRegex(ValueError, 'ALREADY_CROSSED'):
            stop.promotion(state, **{**args, 'mark_price': '100'})
        for oid in ('0', '001', str(2**64)):
            with self.subTest(oid=oid), self.assertRaises(ValueError):
                stop.promotion(state, **{**args, 'old_oid': oid})


class G65PlanLifetimeTests(unittest.TestCase):
    def test_null_expiry_does_not_disable_source_lease(self):
        source = sol_g65_message(); at = moment_ms(source['source_at'])
        state, _ = store.reduce_source(None, source, now=source['source_as_of'],
            not_before=iso_ms(at-60000), domain='software')
        for i in range(1, 1501):
            source = {**source, 'kind': 'HEARTBEAT', 'source_as_of': iso_ms(at+i*60000),
                'source_sequence': (at+i*60000)*10+RANK['HEARTBEAT'],
                'valid_until': iso_ms(at+i*60000+LEASE_MS)}
            validate(source)
            state, _ = store.reduce_source(state, source, now=source['source_as_of'],
                not_before=iso_ms(at-60000), domain='software')
        self.assertEqual(state['entry_permission'], 'WAITING')
        state, _ = store.reduce_source(state, source, now=source['valid_until'],
            not_before=iso_ms(at-60000), domain='software')
        self.assertEqual(state['terminal_reason'], 'SOURCE_LEASE_EXPIRED')

    def test_late_first_plan_cannot_recreate_old_pending(self):
        source = sol_g65_message(); at = moment_ms(source['source_at'])
        with self.assertRaisesRegex(ValueError, 'NOT_RECEIVED_BEFORE_ARM'):
            store.reduce_source(None, source, now=iso_ms(at+60000),
                not_before=iso_ms(at-60000), domain='software')

    def test_cancellation_tombstone_cannot_be_renewed(self):
        source = sol_g65_message(); at = moment_ms(source['source_at'])
        cancelled = {**source, 'kind': 'CANCEL', 'source_sequence': at*10+3,
            'source_as_of': iso_ms(at), 'valid_until': iso_ms(at+LEASE_MS),
            'source_state': 'CANCELLED', 'cancel_reason': 'CANCELLED_BEFORE_FILL'}
        state, _ = store.reduce_source(None, cancelled, now=iso_ms(at),
            not_before=iso_ms(at-60000), domain='software')
        replay, _ = store.reduce_source(state, source, now=source['source_as_of'],
            not_before=iso_ms(at-60000), domain='software')
        self.assertEqual(replay['entry_permission'], 'RETIRED')


if __name__ == '__main__':
    unittest.main()
