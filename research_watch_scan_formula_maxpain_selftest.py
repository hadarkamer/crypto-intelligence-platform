"""Pure checks of the exact source-side averages and their proof boundary."""
from copy import deepcopy
from datetime import timedelta
import unittest
from unittest.mock import patch

import research_watch_scan_formula as legacy
import research_watch_scan_formula_maxpain as adapter
from research_watch_scan_formula_selftest import observation as legacy_observation

PREFIX = 'captured-question-search-v3-experimental-binding:'


def observation(symbol='BTC'):
    obs = legacy_observation(symbol)
    stamp = obs['observed_at_utc'].isoformat()
    rows, slots = [], []
    for tf in adapter.TIMEFRAMES:
        rows.append({'timeframe': tf, 'current_price': 100, 'short_max_pain': 101,
            'long_max_pain': 98, 'price_source': 'hyperliquid' if symbol=='HYPE' else 'binance_spot',
            'price_pair': symbol+'USDT', 'source_observed_at_utc': stamp, 'price_fetched_at_utc': stamp})
        for side, score, target, hits in (('SHORT', 70, 101, 7), ('LONG', 40, 98, 0)):
            slots.append({'timeframe': tf, 'source_side': side, 'status': 'SCORED',
                'target_price': target, 'score': score, 'consensus_hits': hits, 'consensus_total': 7,
                'cluster_count': 0, 'selected': side=='SHORT',
                'components': {'directional_alignment': 20, 'target_proximity': 20,
                               'cluster_confidence': 0, 'relative_gap': score-40}})
    obs['sources']['maxpain_operational_rows'] = rows
    obs['maxpain_slots'] = slots
    return obs


def slot(obs, tf='12h', side='SHORT'):
    return next(s for s in obs['maxpain_slots'] if (s['timeframe'],s['source_side'])==(tf,side))


def decision(payload, suffix='average_score_all_timeframes_GE65', base='LONG'):
    return next(r for r in payload['evaluations'] if r['candidate_key']==PREFIX+suffix and r['base_direction']==base)


def features(payload, base='LONG'):
    return payload['features_by_direction'][base]['features']


class MaxPainTests(unittest.TestCase):
    def test_298_original_definitions_frozen_with_82_supported(self):
        old, new = legacy.catalog_records(), adapter.catalog_records()
        self.assertEqual(len(new), 298)
        self.assertEqual(sum(r['supported'] for r in new), 82)
        for before, after in zip(old, new):
            self.assertEqual({k:before[k] for k in ('candidate_key','definition','definition_sha256','orientation')},
                             {k:after[k] for k in ('candidate_key','definition','definition_sha256','orientation')})
        changed = deepcopy(legacy.existing.candidate_catalog(include_extended=True))
        changed.pop()
        with patch.object(legacy.existing, 'candidate_catalog', return_value=changed):
            with self.assertRaisesRegex(ValueError, 'CATALOG_DRIFT'):
                adapter.catalog_records()

    def test_exact_side_averages_and_consensus_164_decisions(self):
        result = adapter.evaluate_coin(observation())
        self.assertEqual(len(result['evaluations']), 164)
        self.assertEqual(features(result)[adapter.AVERAGE], 70)
        self.assertEqual(features(result)[adapter.OPPOSITE_AVERAGE], 40)
        self.assertEqual(features(result,'SHORT')[adapter.AVERAGE], 40)
        self.assertIs(features(result)[adapter.CONSENSUS], True)
        self.assertIs(features(result,'SHORT')[adapter.CONSENSUS], False)
        proof = result['maxpain_provenance_by_direction']['LONG']
        self.assertEqual((proof['source_side'], proof['denominator'], proof['score_sum']), ('SHORT',7,490))
        self.assertEqual(proof['validation_status'], 'VALID')

    def test_inactive_target_is_excluded_from_denominator_not_zero_or_missing(self):
        obs = observation()
        # LONG target at/above current is inactive, while the SHORT target stays active.
        obs['sources']['maxpain_operational_rows'][0]['long_max_pain'] = 100
        slot(obs, side='LONG').clear()
        obs['maxpain_slots'][1].update(timeframe='12h', source_side='LONG', status='INACTIVE_TARGET', score=None)
        result = adapter.evaluate_coin(obs)
        proof = result['maxpain_provenance_by_direction']['SHORT']
        self.assertEqual((proof['denominator'], proof['rounded_average']), (6,40))
        self.assertEqual(proof['inactive_timeframes'], ['12h'])
        self.assertEqual(proof['missing_timeframes'], [])
        self.assertEqual(proof['validation_status'], 'VALID')
        # A null absent target produces no candidate in the original scorer too.
        obs['sources']['maxpain_operational_rows'][0]['long_max_pain'] = None
        self.assertEqual(adapter.evaluate_coin(obs)['maxpain_provenance_by_direction']['SHORT']['validation_status'], 'VALID')

    def test_known_zero_and_rounding_match_original_ordered_active_mean(self):
        obs = observation()
        values = (0, 55.55, 65.01, 70.02, 80.03, 90.04, 99.99)
        for tf, value in zip(adapter.TIMEFRAMES, values):
            item = slot(obs, tf)
            item.update(score=value, components=dict.fromkeys(adapter.ADDITIVE_COMPONENTS,0))
            item['components']['directional_alignment'] = value
        result = adapter.evaluate_coin(obs)
        self.assertEqual(features(result)[adapter.AVERAGE], round(sum(values)/7,2))
        proof = result['maxpain_provenance_by_direction']['LONG']
        self.assertEqual(proof['denominator'],7)
        self.assertEqual(proof['timeframe_values']['12h'],0)

    def test_missing_row_or_missing_slot_never_means_partial_average_or_false_mapping(self):
        for mode in ('missing_row','missing_slot','missing_status'):
            obs = observation()
            if mode=='missing_row':
                obs['sources']['maxpain_operational_rows'].pop()
            elif mode=='missing_slot':
                obs['maxpain_slots'].pop()
            else:
                slot(obs).update(status='MISSING_INPUT', score=None)
            result = adapter.evaluate_coin(obs)
            self.assertNotIn(adapter.MAPPING, features(result))
            self.assertEqual(decision(result)['match_status'],'UNKNOWN')
            self.assertEqual(decision(result,'CONSENSUS_False')['match_status'],'UNKNOWN')

    def test_no_active_opposite_side_is_independent_of_valid_base_features(self):
        obs = observation()
        for row in obs['sources']['maxpain_operational_rows']:
            row['long_max_pain'] = None
        for item in obs['maxpain_slots']:
            if item['source_side']=='LONG':
                item.update(status='INACTIVE_TARGET', score=None)
        result = adapter.evaluate_coin(obs)
        self.assertEqual(decision(result)['match_status'],'MATCH')
        self.assertEqual(decision(result,'opposite_average_score_all_timeframes_GE65')['match_status'],'UNKNOWN')
        self.assertNotIn(adapter.MAPPING, features(result,'SHORT'))

    def test_consensus_uses_closest_active_target_with_short_tie_not_scorer_selected(self):
        obs = observation()
        for row in obs['sources']['maxpain_operational_rows']:
            row['long_max_pain'] = 99
        for item in obs['maxpain_slots']:
            item['selected'] = item['source_side']=='LONG'
            if item['source_side']=='LONG': item['target_price']=99
        result = adapter.evaluate_coin(obs)
        self.assertIs(features(result)[adapter.CONSENSUS], True)
        self.assertIs(features(result,'SHORT')[adapter.CONSENSUS], False)
        self.assertEqual(result['maxpain_provenance_by_direction']['LONG']['consensus_by_timeframe'],
                         dict.fromkeys(adapter.TIMEFRAMES,'SHORT'))

    def test_inverse_uses_same_base_features_and_only_flips_outcome(self):
        result = adapter.evaluate_coin(observation())
        key = PREFIX+'average_score_all_timeframes_GE65'
        inverse = next(r for r in result['evaluations'] if r['candidate_key']==PREFIX+'INVERSE:'+key and r['base_direction']=='LONG')
        self.assertEqual((inverse['match_status'],inverse['analysis_direction']),('MATCH','SHORT'))
        self.assertEqual(decision(result,base='SHORT')['match_status'],'NO_MATCH')

    def test_future_or_invalid_source_proof_is_unknown_while_legacy_is_unchanged(self):
        for field, value in (('current_price',True),('current_price',float('nan')),
                             ('short_max_pain',float('inf')),('price_source',''),
                             ('source_observed_at_utc','2027-01-01T00:00:00+00:00'),
                             ('price_fetched_at_utc',None),('price_fetched_at_utc','2026-09-13T12:00:00')):
            with self.subTest(field=field,value=value):
                obs=observation()
                obs['sources']['maxpain_operational_rows'][0][field]=value
                result=adapter.evaluate_coin(obs)
                self.assertEqual(decision(result)['match_status'],'UNKNOWN')
                legacy_rows=legacy.evaluate_coin(obs)['evaluations']
                self.assertEqual([r for r in result['evaluations'] if r['candidate_key'] in {x['candidate_key'] for x in legacy_rows}], legacy_rows)
        obs=observation()
        obs['source_time_errors']=['12h/price_fetched_at_utc:FUTURE_TIME']
        self.assertEqual(decision(adapter.evaluate_coin(obs))['match_status'],'UNKNOWN')

    def test_slot_direction_consensus_and_original_component_validation_fail_closed(self):
        for key, value in (('target_price',99),('consensus_hits',6),('consensus_total',True),
                           ('cluster_count',8),('cluster_count',None),('score',70.0105)):
            obs=observation()
            slot(obs)[key]=value
            self.assertEqual(decision(adapter.evaluate_coin(obs))['match_status'],'UNKNOWN')
        obs=observation()
        slot(obs)['components']['relative_gap']=float('nan')
        self.assertEqual(decision(adapter.evaluate_coin(obs))['match_status'],'UNKNOWN')

    def test_legacy_34_decisions_identical_for_valid_missing_and_zero_maxpain(self):
        for obs in (observation(),legacy_observation(),observation('HYPE')):
            old=legacy.evaluate_coin(obs)
            result=adapter.evaluate_coin(obs)
            keys={row['candidate_key'] for row in old['evaluations']}
            self.assertEqual([row for row in result['evaluations'] if row['candidate_key'] in keys],old['evaluations'])
            for base in adapter.DIRECTIONS:
                for feature in legacy.SUPPORTED_FEATURES:
                    self.assertEqual(features(result,base).get(feature),features(old,base).get(feature))

    def test_hype_operational_quote_retains_source_and_is_not_forced_to_spot(self):
        obs=observation('HYPE')
        for row in obs['sources']['maxpain_operational_rows']:
            row.update(price_source='binance_futures_mark',price_market='PERP',price_instrument='HYPEUSDT')
        result=adapter.evaluate_coin(obs)
        self.assertEqual(decision(result)['match_status'],'MATCH')
        quote=result['maxpain_provenance_by_direction']['LONG']['source_quotes_by_timeframe']['12h']
        self.assertEqual((quote['price_source'],quote['price_market']),('binance_futures_mark','PERP'))

    def test_payload_is_deterministic_immutable_and_outcome_blind(self):
        obs=observation()
        original=deepcopy(obs)
        first=adapter.evaluate_coin(obs)
        self.assertEqual(obs,original)
        obs['maxpain_slots'].reverse()
        obs['sources']['maxpain_operational_rows'].reverse()
        obs['future_outcomes']={'success':False}
        self.assertEqual(adapter.evaluate_coin(obs),first)
        self.assertEqual(first['feature_sha256'],adapter.digest({k:v for k,v in first.items() if k!='feature_sha256'}))
        for row in obs['maxpain_slots']:
            row['score']=float(row['score'])
        self.assertEqual(adapter.evaluate_coin(obs),first)


if __name__=='__main__':
    unittest.main()
