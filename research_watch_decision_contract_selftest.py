"""Literal predicates, causal selection and inactive population contracts."""
from copy import deepcopy
from datetime import timedelta
import json
import unittest
from unittest.mock import patch

import research_watch_decision_contract as contract
import research_ordered_question_catalog as original
import live_price_provider
from research_watch_decision_capture_selftest import inputs, groups_for, add_magnet, rehash, BASE, score_inputs


def result(args, block=None):
    block = contract.capture.build_bundle(**args) if block is None else block
    return contract.evaluate_capture(block, args['score_bundle'], cycle_id=args['cycle_id'],
                                     available_at_utc=BASE+timedelta(minutes=6))


def units(value, population, symbol='BTC'):
    return [unit for unit in value['coins'][symbol]['units'] if unit['population'] == population]


def evaluations(unit, suffix, orientation='NORMAL'):
    return next(row for row in unit['evaluations'] if row['candidate_key'].endswith(':'+suffix)
                and row['orientation'] == orientation)


def refresh(args):
    args['combined_groups'], args['combined_candidates'] = groups_for(args['displayable_items'])


class ContractTests(unittest.TestCase):
    def test_original42_definitions_hashes_orientations_and_inactive_support(self):
        rows = contract.catalog_contracts()
        original_rows = {r['candidate_key']: r for r in contract.legacy.catalog_records()}
        self.assertEqual((len(rows), sum(r['evaluable'] for r in rows)), (42, 38))
        self.assertEqual(sum(r['structurally_unreachable'] for r in rows), 4)
        for row in rows:
            for key in ('definition', 'definition_sha256', 'orientation'):
                self.assertEqual(row[key], original_rows[row['candidate_key']][key])
            self.assertFalse(row['support_activation'])
            self.assertFalse(row['population_equivalent_to_event'])
            self.assertFalse(row['cohort_eligible'])
            if not row['evaluable']:
                self.assertEqual(row['block_reason'], 'COMBINED_EVENT_IDENTITY_NOT_CAPTURED')

    def test_all_known_maxpain_literals_exact_with_unreachable_negative_predicates(self):
        for status in contract.LITERALS[contract.MAXPAIN]:
            args = inputs()
            for item in args['prepared_items']:
                item['maxpain_confirmation']['status'] = status
            refresh(args)
            unit = units(result(args), contract.MAXPAIN)[0]
            for literal in ('CONFIRMED', 'STRONG_CONFIRMED', 'NOT_CONFIRMED', 'OBSERVATION'):
                for orientation in ('NORMAL', 'INVERSE'):
                    row = evaluations(unit, 'maxpain_'+literal, orientation)
                    self.assertEqual(row['match_status'], 'MATCH' if literal == status else 'NO_MATCH')
            self.assertEqual(unit['literal_status'], status)

    def test_missing_unknown_literal_and_producer_drift_remain_unknown(self):
        for value in (None, 'FUTURE_STATUS', 'NOT_CONFIRMED'):
            args = inputs()
            item = next(i for i in args['prepared_items'] if i['symbol'] == 'BTC')
            item['maxpain_confirmation'] = {} if value is None else {'status': value}
            refresh(args)
            unit = units(result(args), contract.MAXPAIN)[0]
            self.assertEqual({r['match_status'] for r in unit['evaluations']}, {'UNKNOWN'})
        args = inputs()
        block = contract.capture.build_bundle(**args)
        block['code_sha256']['market_confidence_engine.py'] = 'a'*64
        value = result(args, rehash(block))
        self.assertEqual({u['selection_status'] for u in units(value, contract.MAXPAIN)}, {'UNKNOWN'})

    def test_missing_failed_invalid_source_returns_no_fabricated_empty_population(self):
        args = inputs()
        block = contract.capture.build_bundle(**args)
        corrupt = deepcopy(block)
        corrupt['payload_sha256'] = '0'*64
        for bad in (None, {}, contract.capture.failure(args['cycle_id'], 'test'), corrupt):
            value = contract.evaluate_capture(bad, args['score_bundle'], cycle_id=args['cycle_id'],
                                              available_at_utc=BASE+timedelta(minutes=6))
            self.assertEqual(value['status'], 'UNKNOWN')
            self.assertEqual(value['source_gate']['status'], 'UNKNOWN')
            self.assertEqual(value['coins'], {})
            self.assertTrue(value['source_gate']['reasons'])
        wrong_cycle = contract.evaluate_capture(block, args['score_bundle'], cycle_id='another',
                                                 available_at_utc=BASE+timedelta(minutes=6))
        self.assertEqual(wrong_cycle['coins'], {})

    def test_magnet_all_literals_and_inverse_keep_base_proof(self):
        for status in contract.LITERALS[contract.MAGNET]:
            args = inputs()
            add_magnet(args, status)
            unit = units(result(args), contract.MAGNET)[0]
            for literal in ('CONFIRMED', 'STRONG_CONFIRMED', 'NOT_CONFIRMED', 'OBSERVATION'):
                normal = evaluations(unit, 'magnet_'+literal)
                inverse = evaluations(unit, 'magnet_'+literal, 'INVERSE')
                self.assertEqual(normal['match_status'], 'MATCH' if literal == status else 'NO_MATCH')
                self.assertEqual(normal['match_status'], inverse['match_status'])
                self.assertEqual(normal['base_direction'], inverse['base_direction'])
                self.assertEqual(normal['analysis_direction'], contract._opposite(inverse['analysis_direction']))

    def test_opposing_clusters_use_magnet_side_not_first_source_item_side(self):
        args = inputs()
        first = add_magnet(args, 'OBSERVATION')
        second = add_magnet(args, 'NOT_CONFIRMED', members=['3d', '1w', '2w'])
        second['magnet']['side'] = 'LOWER' if first['magnet']['side'] == 'UPPER' else 'UPPER'
        second['alert_side'] = 'LONG' if second['magnet']['side'] == 'LOWER' else 'SHORT'
        value = result(args)
        observed = units(value, contract.MAGNET)
        self.assertEqual(len(observed), 2)
        self.assertEqual({u['base_direction'] for u in observed}, {'LONG', 'SHORT'})
        self.assertEqual(observed[0]['source_item_id'], observed[1]['source_item_id'])
        self.assertNotEqual(observed[0]['magnet_id'], observed[1]['magnet_id'])
        self.assertEqual(evaluations(observed[1], 'magnet_NOT_CONFIRMED')['match_status'], 'MATCH')

    def test_magnet_errors_are_unknown_and_do_not_suppress_other_families(self):
        for status in ('ERROR', 'NOT_EVALUATED'):
            args = inputs()
            add_magnet(args, evaluation_status=status)
            value = result(args)
            unit = units(value, contract.MAGNET)[0]
            self.assertEqual({r['match_status'] for r in unit['evaluations']}, {'UNKNOWN'})
            self.assertTrue(all(u['selection_status'] == 'SELECTED' for u in units(value, contract.MAXPAIN)))
            self.assertEqual(value['coins']['BTC']['capture_status'], 'PARTIAL')

    def test_qualified_group_uses_exact_top_item_mean_no_event_identity(self):
        args = inputs()
        for item in args['prepared_items']:
            item['maxpain_confirmation']['status'] = 'CONFIRMED'
            item['types'] = ['A', 'B', 'C']
        refresh(args)
        value = result(args)
        for unit in units(value, contract.COMBINED):
            self.assertTrue(unit['qualified'])
            self.assertEqual(unit['selection_status'], 'SELECTED')
            self.assertEqual(len(unit['evaluations']), 22)
            self.assertNotIn('event.event_type', unit['features'])
            source = next(i for i in args['prepared_items']
                          if '|'.join((i['symbol'], i['timeframe'], i['side'])) == unit['top_item_id'])
            self.assertEqual(unit['features'][contract.TOP_MEAN], source['average_score_all_timeframes'])
            expected = original.extended_features({'event_type': 'COMBINED_CONFIRMATION',
                'direction': unit['base_direction'], 'source_side': source['side'],
                'current_price': source['current_price'], 'target_price': source['target_price'],
                'engine_snapshot': {'alert_side': source['side'],
                    'top_item_average_score_all_timeframes': source['average_score_all_timeframes']}})
            for row in unit['evaluations']:
                definition = next(r['definition'] for r in contract.catalog_contracts()
                                  if r['candidate_key'] == row['candidate_key'])
                self.assertEqual(row['match_status'], contract.legacy.evaluate_candidate(definition, expected)['match_status'])

    def test_unqualified_group_is_not_control_but_missing_qualification_is_unknown(self):
        args = inputs()
        group = next(g for g in args['combined_groups'] if g['symbol'] == 'BTC')
        group['qualified'] = False
        group['signal_keys'] = set()
        group['signal_count'] = 0
        args['combined_candidates'] = [g for g in args['combined_candidates'] if g['symbol'] != 'BTC']
        value = result(args)
        unit = units(value, contract.COMBINED)[0]
        self.assertEqual({r['match_status'] for r in unit['evaluations']}, {'NOT_APPLICABLE'})
        self.assertFalse(value['is_false_signal_control'])
        add_magnet(args, evaluation_status='ERROR')
        unit = units(result(args), contract.COMBINED)[0]
        self.assertEqual({r['match_status'] for r in unit['evaluations']}, {'UNKNOWN'})

    def test_missing_selected_items_invalidates_group_selection_only(self):
        args = inputs()
        removed = next(i for i in args['prepared_items'] if i['symbol'] == 'BTC')
        args['prepared_items'] = [i for i in args['prepared_items'] if i is not removed]
        args['displayable_items'] = args['prepared_items']
        refresh(args)
        value = result(args)
        self.assertEqual({u['selection_status'] for u in units(value, contract.COMBINED)}, {'UNKNOWN'})
        self.assertEqual({u['selection_status'] for u in units(value, contract.MAXPAIN)}, {'SELECTED'})
        self.assertEqual(value['coins']['BTC']['populations'][contract.MAXPAIN]['status'], 'PARTIAL')
        self.assertEqual(value['coins']['ETH']['populations'][contract.MAXPAIN]['status'], 'AVAILABLE')

    def test_source_clock_dependencies_and_unrelated_spot_are_scoped(self):
        for error, mp_unknown, magnet_unknown in (
                ('2w/price_fetched_at_utc:FUTURE_TIME', True, True),
                ('24h/price_fetched_at_utc:FUTURE_TIME', True, True),
                ('futures/candle_close:FUTURE_TIME', True, True),
                ('spot/candle_close:FUTURE_TIME', False, False)):
            args = inputs()
            args['score_bundle']['coins']['BTC']['source_time_errors'].append(error)
            rehash(args['score_bundle'])
            add_magnet(args, 'OBSERVATION')
            value = result(args)
            self.assertEqual(units(value, contract.MAXPAIN)[0]['selection_status'] == 'UNKNOWN', mp_unknown)
            self.assertEqual(units(value, contract.MAGNET)[0]['selection_status'] == 'UNKNOWN', magnet_unknown)
            self.assertEqual(units(value, contract.COMBINED)[0]['selection_status'] == 'UNKNOWN', mp_unknown)

    def test_unknown_magnet_literal_is_not_negative_control(self):
        args = inputs()
        add_magnet(args, 'NEW_STATUS')
        unit = units(result(args), contract.MAGNET)[0]
        self.assertEqual({r['match_status'] for r in unit['evaluations']}, {'UNKNOWN'})

    def test_empty_population_and_truncated_unknown_are_distinct(self):
        rows = score_inputs()
        for row in rows:
            row.update(short_max_pain=None, long_max_pain=None)
            distance = live_price_provider.recalculate_distances(row['current_price'], None, None)
            row.update(distance_short_pct=distance['short_signed_pct'], distance_long_pct=distance['long_signed_pct'])
        args = inputs(rows=rows)
        args['score_bundle']['coins']['BTC']['source_time_errors'].append('spot/candle_close:MISSING_TIME')
        rehash(args['score_bundle'])
        value = result(args)
        self.assertEqual(value['coins']['BTC']['units'], [])
        self.assertEqual({p['status'] for p in value['coins']['BTC']['populations'].values()}, {'EMPTY'})
        args = inputs()
        args['prepared_items'] = args['displayable_items'] = []
        args['combined_groups'] = args['combined_candidates'] = []
        value = result(args)
        self.assertEqual(value['coins']['BTC']['units'], [])
        self.assertEqual({p['status'] for p in value['coins']['BTC']['populations'].values()}, {'UNKNOWN'})

    def test_missing_liquidity_does_not_suppress_known_confirmation(self):
        rows = score_inputs()
        rows[0]['short_liquidation_amount'] = None
        args = inputs(rows=rows)
        add_magnet(args, 'OBSERVATION')
        value = result(args)
        self.assertEqual(value['capture_status'], 'PARTIAL')
        self.assertEqual({u['selection_status'] for u in units(value, contract.MAXPAIN)}, {'SELECTED'})
        self.assertEqual(units(value, contract.MAGNET)[0]['selection_status'], 'SELECTED')

    def test_probe_has_no_provider_or_outcome_side_effects_and_stable_jsonb_hash(self):
        args = inputs()
        block = contract.capture.build_bundle(**args)
        before = contract.canonical(block)
        with patch.object(contract.capture.scores, 'prepare', side_effect=AssertionError('rescoring')):
            first = result(args, block)
            second = result(args, json.loads(json.dumps(block)))
        self.assertEqual(first, second)
        self.assertEqual(before, contract.canonical(block))
        self.assertEqual(first['payload_sha256'], contract.digest({k: v for k, v in first.items() if k != 'payload_sha256'}))
        self.assertEqual(first['flags'], contract.FLAGS)
        self.assertFalse(first['flags']['outcome_reuse_authorized'])


if __name__ == '__main__':
    unittest.main()
