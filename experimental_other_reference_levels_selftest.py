"""Offline reference-price integration checks for non-manual experiments."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import dual_cvd65_alert as dual
import dual_cvd65_store as dual_store
import maxpain_cvd_short_alert as legacy
import research_ordered_experimental as ordered
import watch_transition_delivery as transitions
import watch_transition_store as transition_store
from dual_cvd65_alert_selftest import fixture as dual_fixture, set_scores
from maxpain_cvd_short_alert_selftest import opportunity
from research_ordered_experimental_selftest import ExperimentalTests, fixture as ordered_fixture


def reference(component, price='100', when='2026-09-15T12:00:00Z', symbol='ZEC'):
    boundary = datetime.fromisoformat(when.replace('Z', '+00:00')).replace(second=0, microsecond=0).isoformat()
    return {'status': 'READY', 'component': component, 'symbol': symbol,
            'price': price, 'price_time_utc': boundary, 'anchor_time_utc': when,
            'source': 'BINANCE_SPOT_TRADE_1M', 'precision': 'CLOSED_1M'}


class ReferenceIntegrationTests(unittest.TestCase):
    def test_dual_direct_direction_and_threshold_reach_shared_renderer(self):
        for score, direction in ((70, 'LONG'), (-70, 'SHORT')):
            bundle, now = dual_fixture()
            set_scores(bundle, score, score, symbol='ZEC')
            result = dual.evaluate_bundle(bundle, now)
            observation = next(r for r in result['observations'] if r['symbol'] == 'ZEC')
            observation['price_reference'] = reference('SPOT_CVD')
            with patch.object(dual, 'render_reference_levels', return_value='FROZEN LEVELS') as render:
                text = dual.render_message(observation, result['source_at_utc'])
            render.assert_called_once_with(observation['price_reference'], 200, direction)
            self.assertLess(text.index('FROZEN LEVELS'), text.index('Futures CVD:'))

    def test_dual_display_reference_freezes_without_changing_receipt_or_capture(self):
        bundle, now = dual_fixture()
        set_scores(bundle, 70, 70, symbol='ZEC')
        evaluation = dual.evaluate_bundle(bundle, now)
        original_bundle, original_evaluation = deepcopy(bundle), deepcopy(evaluation)
        source = dual_store._utc(evaluation['source_at_utc'])
        refs = {'ZEC': {
            'FUTURES_CVD': reference('FUTURES_CVD', '101', source.isoformat()),
            'SPOT_CVD': reference('SPOT_CVD', '100', (source-timedelta(seconds=1)).isoformat()),
            'MAX_PAIN': reference('MAX_PAIN', '999', (source-timedelta(seconds=2)).isoformat()),
        }}

        class Connection:
            def __init__(self):
                self.receipt = None
                self.intents = []

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def execute(self, sql, params=None):
                if 'SELECT * FROM dual_cvd65_scopes' in sql:
                    return SimpleNamespace(fetchone=lambda: {
                        'activated_at_utc': source-timedelta(seconds=10),
                        'last_source_at_utc': None, 'state': {}})
                if 'SELECT input_sha256,result FROM dual_cvd65_receipts' in sql:
                    return SimpleNamespace(fetchone=lambda: self.receipt)
                if 'INSERT INTO dual_cvd65_intents' in sql:
                    self.intents.append(deepcopy(params[12].obj))
                    return SimpleNamespace(rowcount=1)
                if 'INSERT INTO dual_cvd65_receipts' in sql:
                    self.receipt = {'input_sha256': params[4], 'result': params[5].obj}
                return SimpleNamespace(rowcount=0)

        conn = Connection()
        with patch.object(dual_store, '_connect', return_value=conn):
            first = dual_store.record_cycle('price-fixture', evaluation, now, price_references=refs)
            frozen_text = conn.intents[0]['text']
            refs['ZEC']['SPOT_CVD']['price'] = '500'
            replay = dual_store.record_cycle('price-fixture', evaluation, now, price_references=refs)
        self.assertEqual(first['created_intents'], 1)
        self.assertEqual(replay['record_status'], 'REPLAY')
        self.assertEqual(replay['created_intents'], 0)
        self.assertEqual(len(conn.intents), 1)
        self.assertEqual(conn.intents[0]['text'], frozen_text)
        frozen = conn.intents[0]['observation']['price_reference']
        self.assertEqual(frozen['price'], '100')
        self.assertEqual(frozen['component'], 'SPOT_CVD')
        self.assertEqual(bundle, original_bundle)
        self.assertEqual(evaluation, original_evaluation)
        self.assertEqual(conn.receipt['input_sha256'], dual_store._digest(dual_store._evaluation(evaluation)[2]))

    def test_ordered_uses_only_formula_components_and_final_inverse_direction(self):
        data = ordered_fixture(inverse=True)
        event = data[3]
        event['engine_snapshot']['experimental_price_references'] = {
            'PRICE_OI': reference('PRICE_OI', '100', event['alert_time_utc'].isoformat(), 'BTC'),
            'MAX_PAIN': reference('MAX_PAIN', '900', '2026-01-01T00:00:00Z', 'BTC'),
        }
        before = deepcopy(event)
        payload = ExperimentalTests().notify(data)
        self.assertEqual(payload['price_reference']['component'], 'PRICE_OI')
        self.assertEqual(payload['price_reference']['price'], '100')
        self.assertEqual(payload['direction'], 'LONG')
        self.assertEqual(event, before)
        with patch.object(ordered, 'render_reference_levels', return_value='FROZEN LEVELS') as render:
            text = ordered.render(payload)
        render.assert_called_once_with(payload['price_reference'], payload['threshold_bps'], 'LONG', html=False)
        self.assertLess(text.index('FROZEN LEVELS'), text.index('גלים עתידיים'))

    def test_ordered_prices_survive_long_conditions_and_legacy_payloads_render(self):
        payload = ExperimentalTests().notify()
        payload['conditions'] = [{'feature': 'x' * 5000, 'operator': '>=', 'value': 65}]
        with patch.object(ordered, 'render_reference_levels', return_value='FROZEN LEVELS'):
            text = ordered.render(payload)
        self.assertIn('FROZEN LEVELS', text)
        self.assertLessEqual(len(text), 4000)
        payload.pop('price_reference', None)
        self.assertIn('ניסיוני, לא למסחר', ordered.render(payload))

    def test_ordered_wrong_coin_or_future_reference_does_not_fabricate_prices(self):
        for kind in ('wrong_coin', 'future'):
            data = ordered_fixture()
            event = data[3]
            when = event['alert_time_utc'] + (timedelta(minutes=1) if kind == 'future' else timedelta())
            symbol = 'ZEC' if kind == 'wrong_coin' else 'BTC'
            event['engine_snapshot']['experimental_price_references'] = {
                'PRICE_OI': reference('PRICE_OI', '100', when.isoformat(), symbol)}
            payload = ExperimentalTests().notify(data)
            self.assertEqual(payload['price_reference']['status'], 'UNAVAILABLE')
            self.assertIn('לא חושבו', ordered.render(payload))

    def test_legacy_preserves_scoring_price_and_has_no_invented_threshold(self):
        item = opportunity()
        item['experimental_price_references'] = {
            component: reference(component, '100', symbol='BTC')
            for component in ('MAX_PAIN', 'FUTURES_CVD', 'SPOT_CVD')}
        match, = legacy.select_matches([item])
        self.assertEqual(match.reference_price, 62000.5)
        with patch.object(legacy, 'render_reference_levels', return_value='FROZEN LEVELS') as render:
            text = legacy.render_message(match, datetime(2026, 9, 15, 12, 5, tzinfo=timezone.utc))
        self.assertEqual(render.call_args.args[1:], (None, 'LONG'))
        self.assertIn('FROZEN LEVELS', text)
        self.assertIn('מחיר שנשמר בסקירה', text)
        self.assertIn('62000.5', text)


class TransitionReferenceReplayTests(unittest.IsolatedAsyncioTestCase):
    async def test_changed_display_quote_replays_frozen_intent_but_changed_signal_conflicts(self):
        when = datetime(2026, 9, 15, 12, 5, tzinfo=timezone.utc)
        item = opportunity()
        original_item = deepcopy(item)
        key = transition_store.signal_key(item)
        references = {'BTC': {
            component: reference(component, '100', symbol='BTC')
            for component in ('MAX_PAIN', 'FUTURES_CVD', 'SPOT_CVD')}}

        class Connection:
            def __init__(self):
                self.state = {key: {'active': False, 'episode': 0}}
                self.batch = None
                self.intents = []

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def execute(self, sql, params=None):
                if 'SELECT state,last_observed_at FROM watch_transition_scopes' in sql:
                    return SimpleNamespace(fetchone=lambda: {
                        'state': self.state, 'last_observed_at': None})
                if 'SELECT input_sha256,result_state,result_metadata,crossing_keys,rejected_older' in sql:
                    return SimpleNamespace(fetchone=lambda: self.batch)
                if 'INSERT INTO watch_transition_intents' in sql:
                    self.intents.append({'kind': params[7], 'payload': deepcopy(params[9].obj)})
                if 'INSERT INTO watch_transition_batches' in sql:
                    self.batch = dict(input_sha256=params[4], result_state=params[5].obj,
                        result_metadata=params[6].obj, crossing_keys=params[7].obj, rejected_older=params[8])
                return SimpleNamespace(rowcount=0)

        conn = Connection()

        async def record(items):
            return await transitions.record_watch(1, items, watch_scan_id='frozen-reference-watch',
                decision_time=when, render_score65=lambda item: 'ordinary score65')

        with patch.object(transitions, '_READY', True), \
             patch.object(transitions, '_NEXT_INIT', 0), \
             patch.object(transitions, '_STATUS', deepcopy(transitions._STATUS)), \
             patch.object(transitions.research_event_runtime, 'watch_context_snapshot',
                side_effect=lambda: {'experimental_reference_prices_by_symbol': deepcopy(references)}), \
             patch.object(transition_store, '_connect', return_value=conn), \
             patch.object(transition_store, '_prune_history'), \
             patch.object(transition_store, '_intent_rows', side_effect=lambda *args: deepcopy(conn.intents)):
            first = await record([item])
            self.assertFalse(first['idempotent_existing'])
            legacy_intent = next(row for row in first['intents'] if row['kind'] == legacy.FORMULA_ID)
            self.assertEqual(legacy_intent['payload']['match']['item']['experimental_price_references']['SPOT_CVD']['price'], '100')
            frozen_text = legacy_intent['payload']['text']
            self.assertIn('100', frozen_text)
            references['BTC']['SPOT_CVD']['price'] = '500'
            replay = await record([item])
            self.assertTrue(replay['idempotent_existing'])
            self.assertEqual(replay['intents'], first['intents'])
            self.assertEqual(len(conn.intents), 2)
            ordinary = next(row for row in first['intents'] if row['kind'] == 'MAX_PAIN_SCORE_65')
            self.assertNotIn('experimental_price_references', ordinary['payload']['item'])
            self.assertEqual(item, original_item)
            expected = transition_store._digest({'observed_at': transition_store._iso(when), 'items': [item]})
            self.assertEqual(conn.batch['input_sha256'], expected)
            self.assertIsNone(await record([{**item, 'score': item['score'] + 1}]))
            self.assertEqual(transitions.status()['last_record_status'], 'EVIDENCE_GAP')


if __name__ == '__main__':
    unittest.main()
