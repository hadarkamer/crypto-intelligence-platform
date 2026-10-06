"""Pure frozen decision/source contracts; no main, providers, DB or sends."""
from copy import deepcopy
from datetime import timedelta
import unittest

import research_watch_decision_capture as capture
from research_watch_score_capture_selftest import BASE, bundle, inputs as score_inputs, derivatives
import live_price_provider


def groups_for(items):
    """Completed fixture sinks using the documented operational signal rules."""
    groups, candidates = [], []
    for symbol, side in sorted({(i['symbol'], i['side']) for i in items}):
        ordered = sorted((i for i in items if (i['symbol'], i['side']) == (symbol, side)),
                         key=lambda i: (-i['score'], capture.TIMEFRAMES.index(i['timeframe'])))
        group = {'key': symbol+'|'+side, 'symbol': symbol, 'side': side,
                 'top_item': ordered[0], 'magnet': None, 'signal_keys': set()}
        for key in capture.SIGNAL_LISTS:
            group[key] = []
        for item in ordered:
            tf, score = item['timeframe'], item['score']
            status = item.get('maxpain_confirmation', {}).get('status')
            types = sorted(set(item.get('types') or []))
            if status in ('CONFIRMED', 'STRONG_CONFIRMED'):
                strong = status == 'STRONG_CONFIRMED'
                group['strong_confirmations' if strong else 'normal_confirmations'].append({'timeframe': tf, 'score': score})
                group['signal_keys'].add(('strong_confirmation:' if strong else 'confirmation:')+tf)
            if score > 80:
                group['high_scores'].append({'timeframe': tf, 'score': score})
                group['signal_keys'].add('score_over_80:'+tf)
            if len(types) >= 3:
                group['anomaly_setups'].append({'timeframe': tf, 'count': len(types), 'types': types})
                group['signal_keys'].add('three_anomalies:'+tf)
            share = item.get('near_share_pct')
            if share is not None and share >= 60:
                group['liquidity_imbalances'].append({'timeframe': tf, 'share_pct': share})
                group['signal_keys'].add('liquidity_60:'+tf)
        for name, title in (('positioning', 'מחיר + OI'), ('futures_flow', 'Futures CVD')):
            module = ordered[0]['market_evidence']['modules'].get(name, {})
            if module.get('available') is not False and module.get('relation') == 'SUPPORT' and abs(module.get('score') or 0) >= 65:
                group['derivatives_high'].append({'title': title, 'score': module['score']})
                group['signal_keys'].add('derivatives_high:'+name)
        group['signal_count'] = len(group['signal_keys'])
        if group['signal_count'] >= 2:
            candidates.append(dict(group))
        groups.append({**group, 'qualified': group['signal_count'] >= 2, 'ordered_items': ordered})
    return groups, candidates


def inputs(cycle_id='decision-test', *, rows=None):
    rows = score_inputs() if rows is None else rows
    snapshot = derivatives()
    for captured in snapshot.values():
        captured['regime'].update(price_fetched_at=BASE.isoformat(), oi_fetched_at=BASE.isoformat())
        captured['flow']['spot']['quality']['candle_close'] = BASE.isoformat()
    score, items, *_ = bundle(rows=rows, snapshot=snapshot, cycle_id=cycle_id)
    groups, candidates = groups_for(items)
    return dict(cycle_id=cycle_id, score_bundle=score, prepared_items=items,
                displayable_items=items, combined_candidates=candidates, combined_groups=groups,
                magnet_evaluations=[], computed_at_utc=BASE+timedelta(minutes=5, seconds=30),
                top8_only=False, general_enabled=True)


def rehash(block):
    block['payload_sha256'] = capture.digest({k: v for k, v in block.items() if k != 'payload_sha256'})
    return block


def add_magnet(args, status='CONFIRMED', *, evaluation_status='EVALUATED', symbol='BTC', quality=70, members=None):
    source = next((i for i in args['displayable_items'] if i['symbol'] == symbol), None)
    source_side = source['side'] if source else 'SHORT'
    magnet = {'symbol': symbol, 'side': 'UPPER' if source_side == 'SHORT' else 'LOWER',
              'count': 3, 'members': members or ['12h', '24h', '48h'], 'min_target': 95,
              'max_target': 95.1, 'average_target': 95.05, 'spread_pct': .1,
              'magnet_quality': quality, 'liquidity_edge_pct': 15}
    record = {'symbol': symbol, 'alert_side': source_side, 'magnet': magnet,
              'source_item': source, 'evaluation_status': evaluation_status}
    if evaluation_status == 'EVALUATED':
        record['confirmation'] = {'status': status, 'label': 'literal '+status,
                                  'magnet_quality': quality, 'liquidity_edge_pct': 15,
                                  'derivatives': {'status': 'CONFIRMED', 'strong_core': True}}
    elif evaluation_status == 'ERROR':
        record['error_type'] = 'ValueError'
    else:
        record['reason'] = 'MISSING_SOURCE_ITEM'
    args['magnet_evaluations'].append(record)
    if evaluation_status == 'EVALUATED' and status in ('CONFIRMED', 'STRONG_CONFIRMED'):
        group = next(g for g in args['combined_groups'] if (g['symbol'], g['side']) == (symbol, source_side))
        existing = group['magnet']
        rank = lambda m: (m['status'] == 'STRONG_CONFIRMED', m['magnet_quality'], m['liquidity_edge_pct'], len(m['members']))
        selected = {'status': status, 'magnet_quality': quality, 'liquidity_edge_pct': 15,
                    'members': magnet['members'], 'magnet_side': magnet['side'], 'label': 'literal '+status}
        if existing is None or rank(selected) > rank(existing):
            group['magnet'] = selected
        group['signal_keys'] = {key for key in group['signal_keys'] if not key.startswith('magnet:')}
        group['signal_keys'].add('magnet:'+group['magnet']['status']+':'+','.join(group['magnet']['members']))
        group['signal_count'] = len(group['signal_keys'])
        group['qualified'] = group['signal_count'] >= 2
        args['combined_candidates'] = [{k: v for k, v in g.items() if k not in ('qualified', 'ordered_items')}
                                       for g in args['combined_groups'] if g['qualified']]
    return record


class DecisionCaptureTests(unittest.TestCase):
    def valid(self, args, block=None):
        block = capture.build_bundle(**args) if block is None else block
        self.assertIs(capture.validate_bundle(block, args['score_bundle'], cycle_id=args['cycle_id'],
                                             available_at_utc=BASE+timedelta(minutes=6)), block)
        return block

    def test_exact_copy_hash_roundtrip_whitelist_and_unchanged_sources(self):
        args = inputs()
        args['prepared_items'][0]['unsaved_transport_field'] = object()
        before = capture.canonical(args['score_bundle'])
        block = self.valid(args)
        self.assertEqual(block['status'], 'COMPLETE')
        self.assertEqual(set(block['coins']), set(capture.SYMBOLS))
        self.assertEqual(before, capture.canonical(args['score_bundle']))
        self.assertNotIn('unsaved_transport_field', capture.canonical(block))
        self.assertNotIn('event_type', capture.canonical(block))
        self.assertEqual(block['source_score_sha256'], args['score_bundle']['payload_sha256'])
        self.assertEqual(len(block['code_sha256']), 5)
        self.assertLess(len(capture.canonical(block).encode()), capture.MAX_BYTES)
        frozen = capture.canonical(block)
        args['prepared_items'][0]['components'].clear()
        self.assertEqual(capture.canonical(block), frozen)

    def test_top_item_ties_and_item_references_are_bound(self):
        args = inputs()
        block = self.valid(args)
        for coin in block['coins'].values():
            for group in coin['combined_groups']:
                self.assertEqual(group['top_item_id'], group['ordered_item_ids'][0])
                self.assertEqual(group['ordered_item_ids'][0].split('|')[1], '12h')
        args['combined_groups'][0]['top_item'] = args['combined_groups'][0]['ordered_items'][-1]
        with self.assertRaisesRegex(ValueError, 'TOP_SELECTION'):
            capture.build_bundle(**args)
        args = inputs()
        args['combined_groups'][0]['top_item'] = deepcopy(args['combined_groups'][0]['top_item'])
        args['combined_groups'][0]['top_item']['score'] += 1
        with self.assertRaisesRegex(ValueError, 'REFERENCE_MISMATCH'):
            capture.build_bundle(**args)

    def test_missing_raw_amount_does_not_become_observed_zero(self):
        results = []
        for amount in (None, 0):
            rows = score_inputs()
            next(row for row in rows if (row['symbol'], row['timeframe']) == ('BTC', '12h'))['long_liquidation_amount'] = amount
            args = inputs(rows=rows)
            block = self.valid(args)
            item = next(i for i in block['coins']['BTC']['prepared_items'] if i['timeframe'] == '12h')
            results.append(item)
            self.assertEqual(block['coins']['ETH']['status'], 'COMPLETE')
            self.assertEqual(item['source_liquidity']['raw_amounts']['long_liquidation_amount'], amount)
        self.assertNotIn('near_amount', results[0]['source_liquidity']['captured_amounts'])
        self.assertEqual(results[1]['source_liquidity']['captured_amounts']['near_amount'], 0)
        self.assertEqual(results[0]['operational_liquidity']['near_amount'], 0)

    def test_all_literal_maxpain_statuses_are_retained_without_aliases(self):
        for status in ('BELOW_SCORE', 'CONFLICT', 'UNCONFIRMED', 'CONFIRMED', 'STRONG_CONFIRMED', 'NOT_CONFIRMED', 'OBSERVATION'):
            args = inputs()
            for item in args['prepared_items']:
                item['maxpain_confirmation']['status'] = status
            args['combined_groups'], args['combined_candidates'] = groups_for(args['prepared_items'])
            block = self.valid(args)
            self.assertEqual({i['maxpain_confirmation']['status'] for c in block['coins'].values()
                              for i in c['prepared_items']}, {status})

    def test_magnet_literal_statuses_and_source_identity(self):
        for status in ('OBSERVATION', 'NOT_CONFIRMED', 'LIQUIDITY_UNAVAILABLE', 'LIQUIDITY_CONFLICT', 'CONFIRMED', 'STRONG_CONFIRMED'):
            args = inputs()
            add_magnet(args, status)
            block = self.valid(args)
            record = block['coins']['BTC']['magnet_evaluations'][0]
            self.assertEqual(record['confirmation']['status'], status)
            self.assertEqual(record['source_item_id'], block['coins']['BTC']['displayable_item_ids'][0])
            self.assertEqual(record['evaluation_status'], 'EVALUATED')

    def test_magnet_ranking_and_exact_tie_keep_first_captured_identity(self):
        args = inputs()
        add_magnet(args, 'CONFIRMED', quality=95)
        add_magnet(args, 'STRONG_CONFIRMED', quality=80, members=['3d', '1w', '2w'])
        add_magnet(args, 'STRONG_CONFIRMED', quality=80, members=['24h', '48h', '3d'])
        block = self.valid(args)
        self.assertEqual(block['coins']['BTC']['combined_groups'][0]['magnet']['members'], ['3d', '1w', '2w'])
        next(g for g in args['combined_groups'] if g['symbol'] == 'BTC')['magnet'] = None
        with self.assertRaises(ValueError):
            capture.build_bundle(**args)

    def test_error_and_unevaluated_are_partial_not_negative_and_isolated(self):
        for status in ('ERROR', 'NOT_EVALUATED'):
            args = inputs()
            add_magnet(args, evaluation_status=status)
            block = self.valid(args)
            self.assertEqual(block['status'], 'PARTIAL')
            self.assertEqual(block['coins']['BTC']['status'], 'PARTIAL')
            self.assertEqual(block['coins']['ETH']['status'], 'COMPLETE')
            self.assertEqual(block['coins']['BTC']['magnet_evaluations'][0]['confirmation'], {})
            self.assertTrue(block['coins']['BTC']['missing_evidence'])

    def test_inactive_and_population_cut_have_distinct_completeness(self):
        rows = score_inputs()
        for row in rows:
            row.update(short_max_pain=None, long_max_pain=None)
            distance = live_price_provider.recalculate_distances(row['current_price'], None, None)
            row.update(distance_short_pct=distance['short_signed_pct'], distance_long_pct=distance['long_signed_pct'])
        block = self.valid(inputs(rows=rows))
        self.assertEqual(block['status'], 'COMPLETE')
        self.assertTrue(all(not coin['combined_groups'] for coin in block['coins'].values()))
        args = inputs()
        args['prepared_items'] = args['displayable_items'] = []
        args['combined_candidates'] = args['combined_groups'] = []
        block = self.valid(args)
        self.assertEqual(block['status'], 'PARTIAL')
        self.assertTrue(all('SELECTED_ITEMS_OUTSIDE_PREPARED_POPULATION' in coin['missing_evidence'] for coin in block['coins'].values()))

    def test_changed_flags_do_not_suppress_observed_decisions(self):
        args = inputs()
        first = self.valid(args)
        args.update(top8_only=True, general_enabled=False)
        second = self.valid(args)
        self.assertEqual(first['coins'], second['coins'])
        self.assertNotEqual(first['payload_sha256'], second['payload_sha256'])

    def test_source_binding_score_average_selection_and_candidate_drift_rejected(self):
        for mutate, message in (
            (lambda a: a['prepared_items'][0].__setitem__('score', 0), 'SCORE_SLOT'),
            (lambda a: a['prepared_items'][0].__setitem__('average_score_all_timeframes', 99), 'AVERAGE'),
            (lambda a: a['combined_groups'][0].__setitem__('qualified', not a['combined_groups'][0]['qualified']), 'QUALIFICATION'),
            (lambda a: a['displayable_items'].pop(), 'DISPLAYABLE'),
        ):
            args = inputs()
            # Keep display and prepared arrays distinct while retaining item references.
            args['displayable_items'] = list(args['prepared_items'])
            mutate(args)
            with self.assertRaises(ValueError, msg=message):
                capture.build_bundle(**args)

    def test_hash_cycle_time_coverage_and_completeness_guards(self):
        args = inputs()
        good = self.valid(args)
        cases = []
        corrupt = deepcopy(good); corrupt['general_enabled'] = False; cases.append(corrupt)
        corrupt = deepcopy(good); corrupt['source_score_sha256'] = '0'*64; cases.append(rehash(corrupt))
        corrupt = deepcopy(good); corrupt['cycle_id'] = 'other'; cases.append(rehash(corrupt))
        corrupt = deepcopy(good); corrupt['coins'].pop('HYPE'); cases.append(rehash(corrupt))
        corrupt = deepcopy(good); corrupt['coins']['BTC']['status'] = 'PARTIAL'; cases.append(rehash(corrupt))
        corrupt = deepcopy(good); corrupt['computed_at_utc'] = (BASE+timedelta(minutes=7)).isoformat(); cases.append(rehash(corrupt))
        corrupt = deepcopy(good); corrupt['computed_at_utc'] = BASE.isoformat(); cases.append(rehash(corrupt))
        for corrupt in cases:
            with self.assertRaises(ValueError):
                self.valid(args, corrupt)
        source = deepcopy(args['score_bundle']); source['watch_display_threshold'] += 1
        with self.assertRaisesRegex(ValueError, 'SCORE_HASH'):
            capture.validate_bundle(good, source, cycle_id=args['cycle_id'], available_at_utc=BASE+timedelta(minutes=6))

    def test_bounded_failure_and_oversize_leave_score_bundle_untouched(self):
        args = inputs()
        before = capture.canonical(args['score_bundle'])
        args['prepared_items'][0]['maxpain_confirmation']['label'] = 'x' * capture.MAX_BYTES
        with self.assertRaisesRegex(ValueError, 'TOO_LARGE'):
            capture.build_bundle(**args)
        self.assertEqual(capture.canonical(args['score_bundle']), before)
        failed = capture.failure(args['cycle_id'], 'x'*1000)
        self.assertEqual(failed['status'], 'FAILED')
        self.assertEqual(len(failed['reason']), 500)
        with self.assertRaises(ValueError):
            self.valid(args, failed)
        self.assertEqual(capture.error_reason(ValueError('private source content')), 'ValueError')
        self.assertEqual(capture.error_reason(capture.CaptureValidationError('ITEM_HASH_MISMATCH')), 'ITEM_HASH_MISMATCH')

    def test_actual_main_sinks_and_real_magnet_builder_match_compact_capture(self):
        from research_watch_decision_capture_watch_selftest import helper_scope
        import magnet_v1
        import market_confidence_engine
        scope = helper_scope()
        scope.update(magnet_v1=magnet_v1, market_confidence_engine=market_confidence_engine)
        for bullish in (False, True):
            rows = score_inputs()
            if bullish:
                for row in rows:
                    row.update(short_max_pain=101, long_max_pain=95)
                    distance = live_price_provider.recalculate_distances(row['current_price'], 101, 95)
                    row.update(distance_short_pct=distance['short_signed_pct'], distance_long_pct=distance['long_signed_pct'])
            args = inputs(rows=rows)
            groups, magnets = [], []
            candidates = scope['_combined_confirmation_candidates'](args['displayable_items'], rows,
                capture_groups=groups, capture_magnet_evaluations=magnets)
            args.update(combined_groups=groups, magnet_evaluations=magnets, combined_candidates=candidates)
            block = self.valid(args)
            self.assertEqual(sum(len(c['magnet_evaluations']) for c in block['coins'].values()), len(magnets))
            self.assertEqual(sum(len(c['combined_candidates']) for c in block['coins'].values()), len(candidates))
            self.assertEqual(len(candidates), 8 if bullish else 0)


if __name__ == '__main__':
    unittest.main()
