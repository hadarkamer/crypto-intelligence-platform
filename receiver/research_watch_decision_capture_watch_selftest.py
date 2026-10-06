"""Offline integration for early Watch decision capture and unchanged delivery.

Actual main function bodies run with explicit in-memory market, capture and bot
boundaries. No bot startup, database, provider or network module is imported.
"""
from __future__ import annotations

import ast
import asyncio
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timezone
import html
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock


ROOT = Path(__file__).resolve().parent
TIMEFRAMES = ('12h', '24h', '48h', '3d', '1w', '2w', '1m')
HELPERS = {
    '_combined_group_key', '_magnet_alert_side', '_combined_magnet_confirmations',
    '_combined_confirmation_candidates', '_combined_confirmation_message',
    '_collect_combined_confirmation_messages', '_filter_top8_items',
    '_is_displayable_opportunity',
}


def load_main(names, scope):
    tree = ast.parse((ROOT / 'main.py').read_text())
    nodes = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
             and node.name in names]
    if {node.name for node in nodes} != names:
        raise AssertionError('A tested production entry point is missing')
    future = ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0)
    module = ast.fix_missing_locations(ast.Module(body=[future, *nodes], type_ignores=[]))
    exec(compile(module, str(ROOT / 'main.py'), 'exec'), scope)
    return scope


def item(symbol='BTC', timeframe='12h', *, score=81.0, share=60.0, distance=1.0):
    return {
        'symbol': symbol, 'side': 'SHORT', 'timeframe': timeframe, 'score': score,
        'distance_pct': distance, 'near_share_pct': share, 'types': [],
        'maxpain_confirmation': {'status': 'UNCONFIRMED'},
        'market_evidence': {'modules': {}},
        'market_regime': {'generation': 'same-oi'},
        'flow_context': {'generation': 'same-cvd'},
        'average_score_all_timeframes': 70.0,
    }


def helper_scope():
    return load_main(HELPERS, {
        'defaultdict': defaultdict, 'html': html, 'datetime': datetime, 'timezone': timezone,
        'TIMEFRAMES': TIMEFRAMES, 'TOP8_SYMBOLS': {'BTC', 'ETH', 'SOL', 'HYPE', 'DOGE', 'ZEC', 'BNB', 'XRP'},
        'COMBINED_MIN_SIGNALS': 2, 'COMBINED_HIGH_SCORE_THRESHOLD': 80.0,
        'DERIVATIVES_HIGH_THRESHOLD': 65.0, 'COMBINED_CONFIRMATION_STATE': {},
        'magnet_v1': SimpleNamespace(build_magnets=Mock(return_value=[]),
            expected_price_direction=Mock(return_value='BULLISH'), evaluate_confirmation=Mock()),
        'market_confidence_engine': SimpleNamespace(combine=Mock(return_value={'generation': 'frozen'})),
        'research_event_runtime': SimpleNamespace(capture_combined_state_changes=Mock()),
    })


class PureCombinedBoundaryTests(unittest.TestCase):
    def test_unqualified_group_and_exact_thresholds_keep_actual_top_item(self):
        scope = helper_scope()
        items = [item(timeframe='24h', score=80, share=59), item(score=80, share=60)]
        before = deepcopy(items)
        groups, magnets = [], []
        candidates = scope['_combined_confirmation_candidates'](
            items, [], capture_groups=groups, capture_magnet_evaluations=magnets)
        self.assertEqual(candidates, [])
        self.assertEqual(magnets, [])
        self.assertEqual(len(groups), 1)
        self.assertFalse(groups[0]['qualified'])
        self.assertEqual(groups[0]['signal_keys'], {'liquidity_60:12h'})
        self.assertEqual(groups[0]['high_scores'], [])
        self.assertIs(groups[0]['top_item'], items[1])
        self.assertEqual([x['timeframe'] for x in groups[0]['ordered_items']], ['12h', '24h'])
        self.assertEqual(items, before)
        self.assertEqual(scope['COMBINED_CONFIRMATION_STATE'], {})
        scope['research_event_runtime'].capture_combined_state_changes.assert_not_called()
        items[0]['score'] = 80.01
        groups = []
        candidates = scope['_combined_confirmation_candidates'](items, [], capture_groups=groups)
        self.assertEqual(len(candidates), 1)
        self.assertTrue(groups[0]['qualified'])
        self.assertEqual(candidates[0]['signal_count'], 2)
        self.assertIs(candidates[0]['top_item'], items[0])

    def test_magnet_sink_retains_negative_missing_error_and_unchanged_best_rank(self):
        scope = helper_scope()
        source = item()
        magnets = [
            {'symbol': 'BTC', 'side': 'UPPER', 'members': ['12h', '24h'], 'magnet_quality': 90,
             'liquidity_edge_pct': 30, 'result': 'NOT_CONFIRMED'},
            {'symbol': 'BTC', 'side': 'UPPER', 'members': ['24h', '48h'], 'magnet_quality': 90,
             'liquidity_edge_pct': 30, 'result': 'CONFIRMED'},
            {'symbol': 'BTC', 'side': 'UPPER', 'members': ['1w', '2w'], 'magnet_quality': 70,
             'liquidity_edge_pct': 20, 'result': 'STRONG_CONFIRMED'},
            {'symbol': 'BTC', 'side': 'UPPER', 'members': ['12h', '48h'], 'magnet_quality': 65,
             'liquidity_edge_pct': None, 'result': 'LIQUIDITY_UNAVAILABLE'},
            {'symbol': 'BTC', 'side': 'UPPER', 'members': ['48h', '3d'], 'magnet_quality': 80,
             'result': 'ERROR'},
            {'symbol': 'ETH', 'side': 'UPPER', 'members': ['12h', '24h'], 'magnet_quality': 80,
             'result': 'CONFIRMED'},
        ]
        def evaluate(magnet, evidence):
            if magnet['result'] == 'ERROR':
                raise ValueError('fixture')
            return {'status': magnet['result'], 'label': 'literal result'}
        scope['magnet_v1'].build_magnets.return_value = magnets
        scope['magnet_v1'].evaluate_confirmation.side_effect = evaluate
        sink = []
        confirmed = scope['_combined_magnet_confirmations']([], [source], capture_evaluations=sink)
        self.assertEqual(len(sink), 6)
        self.assertEqual([entry['evaluation_status'] for entry in sink],
                         ['EVALUATED'] * 4 + ['ERROR', 'NOT_EVALUATED'])
        self.assertEqual(sink[0]['confirmation']['status'], 'NOT_CONFIRMED')
        self.assertEqual(sink[3]['confirmation']['status'], 'LIQUIDITY_UNAVAILABLE')
        self.assertEqual(sink[4]['error_type'], 'ValueError')
        self.assertEqual(sink[5]['reason'], 'MISSING_SOURCE_ITEM')
        self.assertIsNone(sink[5]['source_item'])
        self.assertEqual(confirmed['BTC|SHORT']['members'], ['1w', '2w'])
        self.assertEqual(confirmed['BTC|SHORT']['status'], 'STRONG_CONFIRMED')
        self.assertEqual(scope['market_confidence_engine'].combine.call_count, 5)
        for call in scope['market_confidence_engine'].combine.call_args_list:
            self.assertIs(call.kwargs['regime'], source['market_regime'])
            self.assertIs(call.kwargs['flow'], source['flow_context'])

    def test_cached_candidates_preserve_messages_and_all_transition_states(self):
        ordinary, cached = helper_scope(), helper_scope()
        when = datetime(2026, 9, 14, 8, tzinfo=timezone.utc)
        entered = item()
        expanded = deepcopy(entered)
        expanded['maxpain_confirmation'] = {'status': 'CONFIRMED'}
        # Entry, identical continuation, new signal, weakening, deactivation.
        for items in ([entered], [entered], [expanded], [entered], []):
            expected = ordinary['_collect_combined_confirmation_messages'](items, [], event_time=when)
            candidates = cached['_combined_confirmation_candidates'](items, [])
            evaluate = cached['_combined_confirmation_candidates']
            cached['_combined_confirmation_candidates'] = Mock(side_effect=AssertionError('recomputed'))
            try:
                actual = cached['_collect_combined_confirmation_messages'](
                    items, [], event_time=when, precomputed_candidates=candidates)
            finally:
                cached['_combined_confirmation_candidates'] = evaluate
            self.assertEqual(actual, expected)
            self.assertEqual(cached['COMBINED_CONFIRMATION_STATE'], ordinary['COMBINED_CONFIRMATION_STATE'])
        self.assertEqual(cached['COMBINED_CONFIRMATION_STATE'], {})


class SharedWatchCaptureTests(unittest.IsolatedAsyncioTestCase):
    async def run_cycle(self, *, capture_error=False, precompute_error=False, general=True, empty=False):
        scope = helper_scope()
        order, archives, captures = [], [], []
        items = [] if empty else [item(), item(timeframe='24h', score=99, distance=None), item('ADA', score=99)]
        base_bundle = {'version': 'watch-operational-scores-v2', 'payload_sha256': 'a' * 64}
        rows = [{'symbol': 'BTC'}, {'symbol': 'ADA'}]
        def failed(cycle_id, reason):
            return {'status': 'FAILED', 'cycle_id': cycle_id, 'reason': reason}
        def decision_bundle(**kwargs):
            order.append('freeze_decisions')
            self.assertEqual(scope['COMBINED_CONFIRMATION_STATE'], {})
            self.assertIs(kwargs['score_bundle'], base_bundle)
            self.assertIs(kwargs['prepared_items'], items)
            self.assertEqual(kwargs['displayable_items'], [] if empty else [items[0]])
            captures.append(kwargs)
            if capture_error:
                raise ValueError('fixture serialization failure')
            return {'status': 'COMPLETE', 'cycle_id': kwargs['cycle_id']}
        async def collect(**kwargs):
            result = {}
            self.assertEqual(kwargs['archive_context']['metadata']['operational_decisions']['status'], 'FAILED')
            await kwargs['prepare_watch'](rows, result)
            order.append('archive')
            self.assertEqual(scope['COMBINED_CONFIRMATION_STATE'], {})
            archives.append(dict(kwargs['archive_context']['metadata']))
            return rows, result
        async def dual_record(chat_id, value, **kwargs):
            order.append('dual')
            self.assertIs(value, base_bundle)
        async def send(**kwargs):
            order.append('send')
            return SimpleNamespace(message_id=1)
        def lifecycle(candidates, **kwargs):
            order.append('combined_lifecycle')
            if captures:
                self.assertIs(candidates, captures[0]['combined_candidates'])
        def specials(*args, **kwargs):
            order.append('special_lifecycle')
            return []
        scope['market_confidence_engine'].capture_snapshot = Mock(return_value={})
        scope['research_event_runtime'] = SimpleNamespace(
            set_watch_context=Mock(return_value='context'), reset_watch_context=Mock(),
            capture_special_transitions=Mock(), capture_combined_confirmation=Mock(),
            capture_combined_state_changes=Mock(side_effect=lifecycle))
        scope.update({
            'asyncio': asyncio, 'json': json, 'WATCH_RUNTIME': {'chat_id': 1}, 'WATCH_GENERAL_ENABLED': True,
            'WATCH_PRIORITY_THRESHOLD': 70, '_get_scrape_lock': lambda: asyncio.Lock(),
            'collect_live_rows_for_watch': collect, '_ensure_watch_derivatives_ready': AsyncMock(return_value={}),
            'research_watch_score_capture': SimpleNamespace(failure=failed,
                prepare=Mock(return_value=(items, {}, {})), build_bundle=Mock(return_value=base_bundle)),
            'research_watch_decision_capture': SimpleNamespace(failure=failed,
                error_reason=lambda exc: type(exc).__name__, build_bundle=Mock(side_effect=decision_bundle)),
            'dual_cvd65_delivery': SimpleNamespace(record_watch=AsyncMock(side_effect=dual_record), drain=AsyncMock(return_value=0)),
            'watch_transition_delivery': SimpleNamespace(record_watch=AsyncMock(), drain=AsyncMock(return_value=0),
                cycle_result=Mock(return_value={'status': 'COMPLETE'})),
            '_collect_special_transition_messages': specials, '_watch_derivatives_line': lambda status: '',
            '_send_alert_with_confirmation': AsyncMock(), '_alert_card': lambda *args: 'ordinary card',
            'alert_summary': SimpleNamespace(format_alert_count_summary=lambda items: 'count'),
            '_send_magnet_watch_reports': AsyncMock(return_value=0),
        })
        original = scope['_combined_confirmation_candidates']
        def precompute(*args, **kwargs):
            order.append('precompute')
            if precompute_error:
                raise ValueError('fixture precompute failure')
            return original(*args, **kwargs)
        scope['_combined_confirmation_candidates'] = Mock(side_effect=precompute)
        load_main({'run_watch_cycle'}, scope)
        bot = SimpleNamespace(bot=SimpleNamespace(send_message=AsyncMock(side_effect=send)))
        result = await scope['run_watch_cycle'](bot, 1, top8_only=True, general_enabled=general)
        return scope, result, order, archives, captures, bot, base_bundle

    async def test_capture_precedes_archive_and_delivery_and_candidates_are_computed_once(self):
        scope, result, order, archives, captures, bot, base = await self.run_cycle()
        self.assertTrue(result['ok'], result)
        scope['_combined_confirmation_candidates'].assert_called_once()
        scope['magnet_v1'].build_magnets.assert_called_once()
        self.assertLess(order.index('precompute'), order.index('freeze_decisions'))
        self.assertLess(order.index('freeze_decisions'), order.index('archive'))
        self.assertLess(order.index('archive'), order.index('dual'))
        self.assertLess(order.index('archive'), order.index('special_lifecycle'))
        self.assertLess(order.index('archive'), order.index('combined_lifecycle'))
        self.assertLess(order.index('combined_lifecycle'), order.index('send'))
        self.assertEqual(archives[0]['operational_decisions']['status'], 'COMPLETE')
        self.assertIs(archives[0]['operational_scores'], base)
        self.assertEqual(captures[0]['combined_groups'][0]['top_item']['score'], 81)
        self.assertEqual(result['combined_sent'], 1)
        self.assertEqual(set(scope['COMBINED_CONFIRMATION_STATE']), {'BTC|SHORT'})

    async def test_capture_failure_keeps_base_dual_and_successful_precompute(self):
        scope, result, order, archives, captures, bot, base = await self.run_cycle(capture_error=True)
        self.assertTrue(result['ok'], result)
        self.assertEqual(archives[0]['operational_decisions']['status'], 'FAILED')
        self.assertIs(archives[0]['operational_scores'], base)
        scope['_combined_confirmation_candidates'].assert_called_once()
        scope['dual_cvd65_delivery'].record_watch.assert_awaited_once()
        self.assertEqual(result['combined_sent'], 1)
        self.assertTrue(bot.bot.send_message.await_count)

    async def test_successfully_empty_precompute_is_observed_and_never_recomputed(self):
        scope, result, order, archives, captures, bot, base = await self.run_cycle(empty=True)
        self.assertTrue(result['ok'], result)
        self.assertEqual(archives[0]['operational_decisions']['status'], 'COMPLETE')
        self.assertEqual(captures[0]['combined_candidates'], [])
        self.assertEqual(captures[0]['combined_groups'], [])
        scope['_combined_confirmation_candidates'].assert_called_once()

    async def test_precompute_error_is_deferred_to_original_general_watch_lifecycle(self):
        scope, result, order, archives, captures, bot, base = await self.run_cycle(precompute_error=True)
        self.assertFalse(result['ok'])
        self.assertEqual(archives[0]['operational_decisions']['reason'], 'PRECOMPUTE:ValueError')
        self.assertIs(archives[0]['operational_scores'], base)
        self.assertIn('special_lifecycle', order)
        self.assertNotIn('combined_lifecycle', order)
        self.assertEqual(scope['COMBINED_CONFIRMATION_STATE'], {})
        scope['_combined_confirmation_candidates'].assert_called_once()
        scope['research_watch_decision_capture'].build_bundle.assert_not_called()

    async def test_general_disabled_capture_has_no_delivery_state_or_error_rethrow(self):
        for broken in (False, True):
            with self.subTest(precompute_error=broken):
                scope, result, order, archives, captures, bot, base = await self.run_cycle(
                    precompute_error=broken, general=False)
                self.assertTrue(result['ok'], result)
                self.assertEqual(scope['COMBINED_CONFIRMATION_STATE'], {})
                self.assertNotIn('special_lifecycle', order)
                self.assertNotIn('combined_lifecycle', order)
                bot.bot.send_message.assert_not_awaited()
                scope['_combined_confirmation_candidates'].assert_called_once()


if __name__ == '__main__':
    unittest.main()
