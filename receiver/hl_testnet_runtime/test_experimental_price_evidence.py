"""Price provenance, reference coverage and fail-closed MARK history tests."""
from copy import deepcopy
import json
import unittest
from unittest.mock import Mock, patch

import experimental_execution_contract as contract
from experimental_execution_fixtures import r2732_message, sol_g65_message
from . import experimental_price_evidence as evidence
from . import experimental_execution_evidence as execution
from . import experimental_execution_runtime as runtime
from . import r2732_conditional_stop as r2732
from . import sol_g65_conditional_stop as g65
from .test_maxpain_execution import plan as maxpain_message

T = 1791288960000
A = '0x' + '1' * 40
B = '0x' + '2' * 40


def candle(at=T, *, symbol='XRP', price='2.3'):
    return dict(t=at, T=at + 59999, s=symbol, i='1m',
        o=price, h=price, l=price, c=price, v='1', n=1)


def mark_context():
    return [dict(universe=[dict(name='XRP',szDecimals=2,maxLeverage=10)]),
            [dict(markPx='2.3')]]


class PriceEvidenceTests(unittest.TestCase):
    def setUp(self):
        for target in ('socket.create_connection', 'http.client.HTTPSConnection',
                       'hyperliquid_testnet_executor._wallet'):
            guard = patch(target, side_effect=AssertionError('LOCAL_PRICE_TEST_NO_NETWORK'))
            guard.start()
            self.addCleanup(guard.stop)
        self.message = r2732_message(entry=2.3, decision_ms=T - 60000)
        self.raw = [candle()]
        self.now = T + 60000

    def normalize(self, raw=None, **kw):
        args = dict(environment='mainnet', symbol='XRP', observed_at_ms=self.now,
            now_ms=self.now)
        args.update(kw)
        return evidence.normalize_closed_candles(self.raw if raw is None else raw, **args)

    def source(self, raw=None, **kw):
        args = dict(message=self.message, environment='mainnet',
            observed_at_ms=self.now, now_ms=self.now)
        args.update(kw)
        return evidence.source_range(self.raw if raw is None else raw, **args)

    def test_closed_range_has_last_close_timestamp_and_satisfies_runtime_contract(self):
        value = self.source()
        self.assertEqual(value['at_ms'], T + 59999)
        self.assertEqual(value['reference_at_ms'], T)
        self.assertTrue(value['history_complete'])
        self.assertEqual(runtime._range(value, self.message, self.now),
            tuple(contract.positive('2.3') for _ in range(3)))
        self.assertEqual(self.raw, [candle()])

    def test_exact_high_low_and_final_close_preserve_numeric_values(self):
        first = candle()
        first.update(h='2.5', l='2.1')
        second = candle(T + 60000)
        second.update(o='2.30', h='2.4', l='2.2', c='2.25')
        now = T + 120000
        result = self.source([second, first], observed_at_ms=now, now_ms=now)
        self.assertEqual((result['low'], result['high'], result['price']),
            ('2.1', '2.5', '2.25'))

    def test_open_candle_does_not_become_closed_after_cache_wait(self):
        at = T + 59999
        self.assertEqual(self.normalize(observed_at_ms=at, now_ms=T + 61000), [])
        with self.assertRaisesRegex(evidence.PriceEvidenceError, 'NO_CLOSED'):
            self.source(observed_at_ms=at, now_ms=T + 61000)

    def test_current_partial_candle_never_influences_closed_range_or_formula(self):
        current = candle(T + 60000, price='999')
        self.assertEqual(self.normalize([*self.raw, current]), self.normalize())
        self.assertEqual(self.source([*self.raw, current]), self.source())

    def test_future_observation_is_rejected(self):
        with self.assertRaisesRegex(evidence.PriceEvidenceError, 'FUTURE'):
            self.normalize(observed_at_ms=self.now + 1)

    def test_candle_opening_after_original_observation_is_rejected(self):
        with self.assertRaisesRegex(evidence.PriceEvidenceError, 'AFTER_OBSERVATION'):
            self.normalize([candle(T + 120000)])

    def test_source_cache_receipt_cannot_refresh_stale_closed_range(self):
        now = T + 80000
        with self.assertRaisesRegex(evidence.PriceEvidenceError, 'STALE'):
            self.source(observed_at_ms=now, now_ms=now)

    def test_range_at_existing_freshness_limit_is_accepted_next_ms_rejected(self):
        self.source(now_ms=T + 74999)
        with self.assertRaisesRegex(evidence.PriceEvidenceError, 'STALE'):
            self.source(now_ms=T + 75000)

    def test_missing_first_minute_fails_even_when_recent_candle_is_fresh(self):
        now = T + 120000
        with self.assertRaisesRegex(evidence.PriceEvidenceError, 'GAP'):
            self.source([candle(T + 60000)], observed_at_ms=now, now_ms=now)

    def test_missing_middle_minute_is_not_filled_with_previous_close(self):
        now = T + 180000
        with self.assertRaisesRegex(evidence.PriceEvidenceError, 'GAP'):
            self.source([candle(), candle(T + 120000)], observed_at_ms=now, now_ms=now)

    def test_intraminute_maxpain_reference_refused_without_strategy_approximation(self):
        msg = maxpain_message()
        observed = contract.moment_ms(msg['source_at']) + 1
        msg['source_at'] = contract.iso_ms(observed)
        msg['proof']['source_observed_ms'] = observed
        msg['occurrence_id'] = contract.occurrence_id(msg)
        msg = contract.validate(msg)
        self.assertNotEqual(contract.moment_ms(msg['source_at']) % 60000, 0)
        with self.assertRaisesRegex(evidence.PriceEvidenceError, 'NOT_RECOVERABLE'):
            self.source(message=msg)

    def test_pre_reference_closed_candles_are_excluded_from_exact_range(self):
        self.assertEqual(self.source([candle(T - 60000, price='999'), *self.raw]), self.source())

    def test_duplicate_equivalent_prices_are_idempotent(self):
        duplicate = candle(price='2.3000')
        self.assertEqual(self.normalize([*self.raw, duplicate]), self.normalize())

    def test_duplicate_price_revision_is_rejected(self):
        with self.assertRaisesRegex(evidence.PriceEvidenceError, 'CONFLICTING'):
            self.normalize([*self.raw, candle(price='2.4')])

    def test_wrong_market_symbol_interval_boundary_and_invalid_ohlc_fail(self):
        for changes in (dict(s='SOL'), dict(s='@107'), dict(i='5m'),
                        dict(t=T + 1), dict(T=T + 60000), dict(h='2.2'),
                        dict(l='NaN'), dict(c=True), dict(o='Infinity'), dict(t=True)):
            with self.subTest(changes=changes):
                row = {**candle(), **changes}
                with self.assertRaises(evidence.PriceEvidenceError):
                    self.normalize([row])
        with self.assertRaisesRegex(evidence.PriceEvidenceError, 'MAINNET'):
            self.normalize(environment='testnet')

    def test_official_response_bound_is_enforced(self):
        with self.assertRaisesRegex(evidence.PriceEvidenceError, 'BOUNDED'):
            self.normalize([candle()] * 5001)

    def test_normalized_closed_bars_feed_unchanged_r2732_logic(self):
        state = r2732.initialize_from_contract(self.message['occurrence_id'], self.message)
        result = r2732.advance(state, self.normalize(), now_ms=self.now)
        self.assertEqual(result['cursor_ms'], T)
        self.assertEqual(result['status'], 'OPEN')

    def test_normalized_closed_bars_feed_unchanged_g65_logic(self):
        msg = sol_g65_message()
        at = contract.moment_ms(msg['source_at'])
        now = at + 60000
        state = g65.initialize_from_contract(msg['occurrence_id'], msg)
        row = candle(at, symbol='SOL', price=msg['proof']['reference_price'])
        normalized = self.normalize([row], symbol='SOL', observed_at_ms=now, now_ms=now)
        result = g65.advance(state, normalized, now_ms=now)
        self.assertEqual(result['cursor_ms'], at)
        self.assertEqual(result, g65.advance(state, [dict(open_at_ms=at,
            open=row['o'], high=row['h'], low=row['l'], close=row['c'])], now_ms=now))

    def test_source_trade_range_cannot_pass_as_mark_history(self):
        value = self.source()
        with self.assertRaises(ValueError):
            execution.require_mark_window(value, account=A, symbol='XRP',
                reference_at_ms=T, now_ms=self.now)

    def test_sample_provider_preserves_account_symbol_and_original_timestamp(self):
        read = Mock(return_value=(mark_context(), self.now - 100))
        provider = evidence.SampledMarkProvider(read)
        sample = provider.mark(A, 'XRP', now_ms=self.now)
        self.assertEqual(sample['at_ms'], self.now - 100)
        self.assertEqual(sample['account'], A)
        other = provider.mark(B, 'XRP', now_ms=self.now)
        self.assertEqual(other['account'], B)
        self.assertEqual(other['mark_price'], sample['mark_price'])
        self.assertEqual(other['at_ms'], sample['at_ms'])
        with self.assertRaises(ValueError):
            provider.mark(A, 'SOL', now_ms=self.now)

    def test_stale_restored_sample_does_not_gain_new_timestamp(self):
        restored = json.loads(json.dumps(dict(raw=mark_context(),
            observed_at_ms=T)))
        provider = evidence.SampledMarkProvider(lambda *_: (restored['raw'], restored['observed_at_ms']))
        with self.assertRaises(ValueError):
            provider.mark(A, 'XRP', now_ms=self.now)

    def test_even_fresh_restored_sample_cannot_supply_continuous_mark_history(self):
        read = Mock(return_value=(mark_context(), self.now))
        for provider in (evidence.SampledMarkProvider(read), evidence.SampledMarkProvider(read)):
            provider.mark(A, 'XRP', now_ms=self.now)
            read.reset_mock()
            with self.assertRaisesRegex(evidence.PriceEvidenceError, evidence.MARK_HISTORY_UNAVAILABLE):
                provider.mark_window(A, 'XRP', T, now_ms=self.now)
            read.assert_not_called()
            diagnosis = json.loads(json.dumps(provider.diagnosis()))
            self.assertFalse(diagnosis['history_complete'])
            self.assertFalse(diagnosis['recoverable_by_sample_persistence'])

    def test_cached_provider_matches_live_provider_contract_and_has_no_fetcher(self):
        payload = dict(environment='mainnet', symbol='XRP', observed_at_ms=self.now,
            candles=deepcopy(self.raw))
        source = Mock(return_value=payload)
        sample = Mock(return_value=(mark_context(), self.now))
        provider = evidence.CachedPriceEvidence(source, sample)
        self.assertEqual(provider.source_range(self.message, self.now), self.source())
        self.assertEqual(provider.closed_bars(self.message, self.now), self.normalize())
        self.assertEqual(provider.mark(A, 'XRP', self.now)['mark_price'], '2.3')
        with self.assertRaisesRegex(evidence.PriceEvidenceError, evidence.MARK_HISTORY_UNAVAILABLE):
            provider.mark_window(A, 'XRP', T, self.now)
        self.assertEqual(source.call_count, 2)
        self.assertEqual(sample.call_count, 1)

    def test_cached_source_identity_cannot_be_relabelled_by_message(self):
        payload = dict(environment='mainnet', symbol='SOL', observed_at_ms=self.now,
            candles=self.raw)
        provider = evidence.CachedPriceEvidence(lambda *_: payload, lambda *_: None)
        with self.assertRaisesRegex(evidence.PriceEvidenceError, 'IDENTITY'):
            provider.source_range(self.message, self.now)


if __name__ == '__main__':
    unittest.main()
