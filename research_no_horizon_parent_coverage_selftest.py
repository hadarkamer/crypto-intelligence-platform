"""Outcome-free parent coverage tests using rebuilt genuine capture fixtures.

The shifted populations below are synthetic test data, never research evidence.
Scores come from the existing capture fixture; capture hashes, archive identities,
intakes and causal parents are regenerated through their actual pure builders.
"""
from contextlib import ExitStack
from copy import deepcopy
from datetime import datetime, timedelta
from functools import lru_cache
import unittest
from unittest.mock import patch

import research_btc_parent_movement as btc
import research_max_pain_archive as archive
import research_no_horizon_contract as contracts
import research_no_horizon_parent_coverage as coverage
import research_no_horizon_source as source
import research_watch_scan_intake as intake
import research_watch_score_capture as capture
from research_max_pain_archive_selftest import _raw_rows, _enriched_rows
from research_no_horizon_source_selftest import fixture
from research_watch_scan_intake_selftest import rehash
from research_watch_score_capture_selftest import BASE


def scope(direction='SHORT', candidate='FUTURES_CVD_TOTAL_65', threshold=.25):
    return {'candidate_key':candidate, 'base_direction':direction, 'threshold_pct':threshold}


def _shift(value, delta):
    if isinstance(value, datetime):
        return value + delta
    if isinstance(value, dict):
        return {key:_shift(item, delta) for key,item in value.items()}
    if isinstance(value, list):
        return [_shift(item, delta) for item in value]
    if isinstance(value, str) and len(value) >= 20 and value[4:5] == '-' and value[10:11] == 'T':
        try:
            stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
        except ValueError:
            return value
        if stamp.tzinfo is not None:
            return (stamp+delta).isoformat()
    return value


@lru_cache(maxsize=2)
def _template(unavailable):
    return fixture(unavailable=unavailable)


def source_row(*, parent_offset=0, snapshot_set_id=1, unavailable=False, decision_delay_seconds=0):
    """One revalidated source; equal offsets identify the same causal parent."""
    delta = timedelta(hours=2*parent_offset)
    block = _shift(deepcopy(_template(unavailable)['source_rows'][0]['scores']), delta)
    cycle_id = 'parent-coverage-fixture-'+str(snapshot_set_id)
    block['cycle_id'] = cycle_id
    rehash(block)
    payload = archive.build_snapshot_payload(cycle_id=cycle_id,
        cycle_time_utc=BASE+delta, collection_started_at_utc=BASE+delta,
        collection_completed_at_utc=BASE+delta+timedelta(minutes=6),
        source='WATCH_SHARED', collector_version='parent-coverage-selftest-v1',
        snapshot={'ok':True, 'rows':_shift(_raw_rows(capture.SYMBOLS),delta),
                  'missing_timeframes':[], 'duplicate_pairs':[]},
        enriched_rows=_shift(_enriched_rows(capture.SYMBOLS),delta),
        live_result={'skipped_symbols':[]}, capture_metadata={'operational_scores':block})
    stored = payload['set']
    created = BASE+delta+timedelta(minutes=7, seconds=decision_delay_seconds)
    raw = {'snapshot_set_id':snapshot_set_id, 'snapshot_key':stored['snapshot_key'],
        'payload_sha256':stored['payload_sha256'], 'cycle_id':cycle_id, 'source':stored['source'],
        'available_at_utc':stored['available_at_utc'], 'created_at_utc':created, 'bundle':block}
    cutoff = BASE+delta+timedelta(minutes=11)
    accepted = intake.validate_source(raw, now=cutoff, activated_at=BASE+delta)
    if accepted['intake_status'] != 'ACCEPTED':
        raise AssertionError('Generated capture fixture rejected: '+str(accepted['rejection_reason']))
    accepted.update(consumer_version=intake.VERSION, ingested_at_utc=cutoff)
    prices = []
    for minute, price in enumerate((100.,103.,100.,100.,100.,100.,100.)):
        opened = BASE+delta+timedelta(minutes=minute)
        prices.append({'open_time_utc':opened, 'close_time_utc':opened+timedelta(minutes=1,milliseconds=-1),
            'open':price, 'high':price+.1, 'low':price-.1, 'close':price})
    parent = btc.advance_parents(prices, as_of_utc=cutoff)[-1]
    return {'intake':accepted, 'stored_source':{key:value for key,value in raw.items() if key!='bundle'},
        'scores':block, 'btc_parent':parent, 'btc_prior_bar':{**prices[-1], 'price_source':btc.SOURCE}}


def population(parent_count=1, *, copies_per_parent=1, unavailable=False):
    """Complete source population; no entry candles are needed for parent counts."""
    if type(parent_count) is not int or parent_count < 0 or copies_per_parent < 1:
        raise ValueError('Nonnegative parent count and positive copies required')
    rows = [source_row(parent_offset=parent, snapshot_set_id=parent*copies_per_parent+copy+1,
                       unavailable=unavailable)
            for parent in range(parent_count) for copy in range(copies_per_parent)]
    delta = timedelta(hours=2*max(parent_count-1,0))
    return {'export_version':source.EXPORT_VERSION, 'symbol':'BTC',
        'source_start_utc':BASE+timedelta(minutes=6),
        'source_end_utc':BASE+delta+timedelta(minutes=8),
        'cutoff_utc':BASE+delta+timedelta(minutes=11), 'source_rows':rows, 'candles':[],
        'source_receipt':{'expected_accepted_rows':len(rows), 'rows_complete':True,
            'truncated':False, 'transaction_mode':'REPEATABLE_READ_READ_ONLY'}}


class ParentCoverageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.single = population()
        cls.five = population(5)
        cls.unknown = population(unavailable=True)

    def setUp(self):
        self.data = deepcopy(self.single)

    def report(self, scopes=None, **kwargs):
        return coverage.preflight_parent_coverage(self.data, scopes or [scope()], **kwargs)

    def assert_status(self, result, expected, count, *, complete=True):
        row = result['scopes'][0]
        self.assertEqual(row['coverage_status'],expected)
        self.assertEqual(row['matched_parent_count'],count)
        self.assertEqual(row['parent_membership_complete'],complete)
        self.assertEqual(row['matched_parent_upper_bound'],count if complete and row['source_decisions_complete'] else None)
        self.assertEqual(row['minimum_required_parents'],5)
        self.assertEqual(len(row['decision_ledger']),len(self.data['source_rows']))
        return row

    def test_zero_matches_are_decidable_but_insufficient(self):
        result = self.report([scope('LONG')])
        row = self.assert_status(result,'INSUFFICIENT',0)
        self.assertEqual(row['source_counts'],{'MATCH':0,'NO_MATCH':1,'UNKNOWN':0,'UNKNOWN_SOURCE':0})
        self.assertTrue(row['source_decisions_complete'])
        self.assertEqual(row['representatives'],[])
        self.assertTrue(result['source_preflight']['ready_for_outcome_research'])
        self.assertFalse(result['ready_for_outcome_research'])

    def test_empty_complete_source_is_zero_not_unknown(self):
        self.data = population(0)
        row = self.assert_status(self.report(),'INSUFFICIENT',0)
        self.assertEqual(row['source_counts'],dict.fromkeys(('MATCH','NO_MATCH','UNKNOWN','UNKNOWN_SOURCE'),0))
        self.assertEqual(row['decision_ledger'],[])

    def test_many_valid_captures_of_one_parent_do_not_inflate_count(self):
        self.data = population(1,copies_per_parent=6)
        result = self.report()
        row = self.assert_status(result,'INSUFFICIENT',1)
        self.assertEqual(result['source_preflight']['validated_source_rows'],6)
        self.assertEqual(row['source_counts']['MATCH'],6)
        self.assertEqual(len(row['representatives']),1)
        self.assertFalse(result['any_scope_potentially_sufficient'])

    def test_five_distinct_verified_parents_are_possible_not_qualified(self):
        self.data = deepcopy(self.five)
        result = self.report()
        row = self.assert_status(result,'POSSIBLE',5)
        self.assertEqual(result['source_preflight']['validated_source_rows'],5)
        self.assertEqual(row['source_counts']['MATCH'],5)
        self.assertEqual(len(row['representatives']),5)
        self.assertTrue(result['ready_for_outcome_research'])
        self.assertTrue(result['all_scopes_potentially_sufficient'])
        self.assertTrue(result['any_scope_potentially_sufficient'])
        self.assertIsNone(result['gate'])
        self.assertFalse(result['outcomes_evaluated'])
        for representative in row['representatives']:
            self.assertNotIn('outcome',representative)
            self.assertNotIn('reference_price',representative)
            self.assertNotIn('success',representative)

    def test_earliest_decision_then_lexical_entry_id_is_input_order_independent(self):
        rows = [source_row(snapshot_set_id=2),source_row(snapshot_set_id=10),
                source_row(snapshot_set_id=1,decision_delay_seconds=30)]
        self.data['source_rows'] = rows
        self.data['source_receipt']['expected_accepted_rows'] = len(rows)
        expected = 'watch:10:BTC:SHORT'
        first = self.report()['scopes'][0]['representatives']
        self.data['source_rows'] = list(reversed(rows))
        second = self.report()['scopes'][0]['representatives']
        self.assertEqual([item['entry_id'] for item in first],[expected])
        self.assertEqual([item['entry_id'] for item in second],[expected])
        self.assertEqual(contracts.utc(first[0]['decision_time_utc']),BASE+timedelta(minutes=7))

    def test_missing_stale_future_and_wrong_parent_are_not_fewer_complete_parents(self):
        mutations = (
            lambda row:row.update(btc_parent=None),
            lambda row:row.update(btc_prior_bar=None),
            lambda row:row['btc_parent'].update(observed_through_utc=BASE+timedelta(minutes=3)),
            lambda row:row['btc_parent'].update(confirmed_at_utc=BASE+timedelta(minutes=8)),
            lambda row:row['btc_parent'].update(btc_parent_movement_id='a'*64),
            lambda row:row['btc_prior_bar'].update(close_time_utc=BASE+timedelta(minutes=8)),
            lambda row:row['btc_prior_bar'].update(high=1),
        )
        for index,mutate in enumerate(mutations):
            with self.subTest(mutation=index):
                self.data = deepcopy(self.single)
                mutate(self.data['source_rows'][0])
                result = self.report()
                row = self.assert_status(result,'BLOCKED',0,complete=False)
                self.assertEqual(row['source_counts']['MATCH'],1)
                self.assertTrue(row['source_decisions_complete'])
                self.assertEqual(row['representatives'],[])
                self.assertEqual(row['representative_selection'],'NOT_SELECTED_INCOMPLETE_POPULATION')
                self.assertIsNone(row['decision_ledger'][0].get('btc_parent_movement_id'))
                self.assertFalse(result['ready_for_outcome_research'])

    def test_invalid_earliest_match_never_replaced_by_a_later_valid_capture(self):
        early = source_row(snapshot_set_id=1)
        later = source_row(snapshot_set_id=2,decision_delay_seconds=30)
        early['btc_parent']['observed_through_utc'] = BASE+timedelta(minutes=3)
        self.data['source_rows'] = [later,early]
        self.data['source_receipt']['expected_accepted_rows'] = 2
        row = self.report()['scopes'][0]
        self.assertEqual(row['source_counts']['MATCH'],2)
        self.assertEqual(row['coverage_status'],'BLOCKED')
        self.assertIsNone(row['matched_parent_upper_bound'])
        self.assertEqual(row['representatives'],[])
        self.assertEqual(row['representative_selection'],'NOT_SELECTED_INCOMPLETE_POPULATION')

    def test_parent_metadata_is_not_required_for_decidable_no_match(self):
        self.data['source_rows'][0]['btc_parent'] = None
        self.data['source_rows'][0]['btc_prior_bar'] = None
        row = self.assert_status(self.report([scope('LONG')]),'INSUFFICIENT',0)
        self.assertEqual(row['decision_ledger'][0]['match_status'],'NO_MATCH')

    def test_unknown_feature_is_retained_and_cannot_become_zero_parent_upper_bound(self):
        self.data = deepcopy(self.unknown)
        result = self.report([scope(),scope('LONG')])
        self.assertEqual(result['source_preflight']['validated_source_rows'],1)
        self.assertEqual(result['source_preflight']['invalid_source_rows'],0)
        for row in result['scopes']:
            self.assertEqual(row['source_counts'],{'MATCH':0,'NO_MATCH':0,'UNKNOWN':1,'UNKNOWN_SOURCE':0})
            self.assertEqual(row['decision_ledger'][0]['match_status'],'UNKNOWN')
            self.assertFalse(row['source_decisions_complete'])
            self.assertEqual(row['coverage_status'],'BLOCKED')
            self.assertIsNone(row['matched_parent_upper_bound'])
            self.assertEqual(row['representatives'],[])
        self.assertFalse(result['ready_for_outcome_research'])

    def test_known_false_conjunct_is_not_blocked_by_unrelated_unknown_feature(self):
        result = self.report([scope('LONG','STRICT_TRIPLE_TOTAL_65')])
        row = self.assert_status(result,'INSUFFICIENT',0)
        self.assertTrue(row['source_decisions_complete'])
        self.assertEqual(row['source_counts']['NO_MATCH'],1)
        self.assertTrue(row['decision_ledger'][0]['missing_features'])

    def test_bad_source_and_duplicate_ordinal_are_never_silently_dropped(self):
        for kind in ('hash','missing','duplicate','malformed'):
            with self.subTest(kind=kind):
                self.data = deepcopy(self.five)
                if kind == 'hash':
                    self.data['source_rows'][0]['scores']['coins']['BTC']['models']['futures_flow']['score'] = 0
                elif kind == 'missing':
                    self.data['source_rows'][0]['stored_source'] = None
                elif kind == 'duplicate':
                    self.data['source_rows'].append(deepcopy(self.data['source_rows'][0]))
                else:
                    self.data['source_rows'].append(None)
                self.data['source_receipt']['expected_accepted_rows'] = len(self.data['source_rows'])
                result = self.report([scope(),scope('LONG')])
                self.assertFalse(result['ready_for_outcome_research'])
                self.assertEqual(result['source_preflight']['invalid_source_rows'],1)
                for row in result['scopes']:
                    self.assertEqual(row['coverage_status'],'BLOCKED')
                    self.assertEqual(row['source_counts']['UNKNOWN_SOURCE'],1)
                    self.assertEqual(len(row['decision_ledger']),len(self.data['source_rows']))
                    self.assertEqual([item['ordinal'] for item in row['decision_ledger']],list(range(len(self.data['source_rows']))))
                    self.assertIsNone(row['matched_parent_upper_bound'])
                    self.assertEqual(row['representatives'],[])

    def test_incomplete_extraction_blocks_even_five_known_matching_parents(self):
        for change in ({'expected_accepted_rows':6},{'rows_complete':False},{'truncated':True},
                       {'candle_truncated':True},{'transaction_mode':'SEPARATE_READ_ONLY_QUERIES'}):
            with self.subTest(change=change):
                self.data = deepcopy(self.five)
                self.data['source_receipt'].update(change)
                result = self.report()
                row = result['scopes'][0]
                self.assertEqual(row['source_counts']['MATCH'],5)
                self.assertEqual(row['coverage_status'],'BLOCKED')
                self.assertIsNone(row['matched_parent_upper_bound'])
                self.assertEqual(row['representatives'],[])
                self.assertFalse(result['ready_for_outcome_research'])

    def test_contradictory_causal_metadata_for_one_parent_blocks_selection(self):
        for field in ('direction','end_time_utc'):
            with self.subTest(field=field):
                self.data = population(1,copies_per_parent=2)
                first,second = [row['btc_parent'] for row in self.data['source_rows']]
                if field == 'direction':
                    second['direction'] = 'DOWN' if first['direction']=='UP' else 'UP'
                else:
                    first['end_time_utc'] = BASE+timedelta(minutes=8)
                    second['end_time_utc'] = BASE+timedelta(minutes=9)
                row = self.report()['scopes'][0]
                self.assertEqual(row['coverage_status'],'BLOCKED')
                self.assertFalse(row['parent_membership_complete'])
                self.assertIsNone(row['matched_parent_upper_bound'])
                self.assertEqual(row['representatives'],[])

    def test_mutable_observed_through_and_consistent_open_to_closed_are_allowed(self):
        self.data = population(1,copies_per_parent=2)
        parent = self.data['source_rows'][1]['btc_parent']
        parent['observed_through_utc'] = BASE+timedelta(minutes=9,milliseconds=-1)
        parent['end_time_utc'] = BASE+timedelta(minutes=8)
        row = self.assert_status(self.report(),'INSUFFICIENT',1)
        self.assertEqual(len(row['representatives']),1)

    def test_later_open_checkpoint_cannot_override_an_earlier_closed_boundary(self):
        early = source_row(snapshot_set_id=1)
        later = source_row(snapshot_set_id=2,decision_delay_seconds=30)
        early['btc_parent']['end_time_utc'] = BASE+timedelta(minutes=7,seconds=15)
        self.data['source_rows'] = [early,later]
        self.data['source_receipt']['expected_accepted_rows'] = 2
        row = self.report()['scopes'][0]
        self.assertEqual(row['source_counts']['MATCH'],2)
        self.assertEqual(row['coverage_status'],'BLOCKED')
        self.assertFalse(row['parent_membership_complete'])
        self.assertEqual(len(row['conflicting_parent_ids']),1)
        self.assertIsNone(row['matched_parent_upper_bound'])
        self.assertEqual(row['representatives'],[])

    def test_separate_scopes_do_not_pool_three_parents_into_six(self):
        self.data = population(3)
        result = self.report([scope(threshold=.25),scope(threshold=.5)])
        self.assertEqual(len(result['scopes']),2)
        for row in result['scopes']:
            self.assertEqual(row['matched_parent_count'],3)
            self.assertEqual(row['coverage_status'],'INSUFFICIENT')
        self.assertFalse(result['any_scope_potentially_sufficient'])
        self.assertFalse(result['all_scopes_potentially_sufficient'])
        self.assertFalse(result['ready_for_outcome_research'])

    def test_any_and_all_scope_readiness_remain_distinct(self):
        self.data = deepcopy(self.five)
        result = self.report([scope(),scope('LONG')])
        by_direction = {row['base_direction']:row for row in result['scopes']}
        self.assertEqual(by_direction['SHORT']['coverage_status'],'POSSIBLE')
        self.assertEqual(by_direction['LONG']['coverage_status'],'INSUFFICIENT')
        self.assertTrue(result['any_scope_potentially_sufficient'])
        self.assertFalse(result['all_scopes_potentially_sufficient'])
        self.assertFalse(result['ready_for_outcome_research'])

    def test_feature_validation_occurs_once_per_source_across_scopes(self):
        self.data = deepcopy(self.five)
        with patch.object(source,'_validate_row',wraps=source._validate_row) as validate:
            self.report([scope(),scope('LONG'),scope(threshold=.5)])
        self.assertEqual(validate.call_count,5)

    def test_never_builds_entries_reads_outcome_prices_or_touches_sqlite(self):
        self.data = deepcopy(self.five)
        self.data['candles'] = [None,{'not_an_entry_price':'must not inspect'}]
        targets = ('research_no_horizon_source.build_snapshot','research_no_horizon_source._entry_time',
            'research_no_horizon_source.touch._bar','research_no_horizon_contract.make_contract',
            'research_no_horizon_first_touch.initialize','research_no_horizon_first_touch.evaluate',
            'research_no_horizon_first_touch.advance','research_no_horizon_gate.evaluate_gate',
            'research_no_horizon_replay.replay_snapshot','research_no_horizon_store.LocalResearchStore.__init__',
            'research_no_horizon_experiment.LocalExperimentStore.__init__','sqlite3.connect',
            'research_watch_score_capture.prepare')
        with ExitStack() as stack:
            for target in targets:
                stack.enter_context(patch(target,side_effect=AssertionError('forbidden during parent coverage')))
            result = self.report()
        self.assertTrue(result['ready_for_outcome_research'])
        for key in ('entry_evaluation','outcome_evaluation','gate_evaluation'):
            self.assertEqual(result[key],'NOT_EVALUATED')
        self.assertIsNone(result['gate'])
        self.assertFalse(result['outcomes_evaluated'])
        for key in ('runtime_authorized','telegram_authorized','trading_authorized'):
            self.assertFalse(result[key])

    def test_receipt_is_deterministic_hash_bound_and_input_immutable(self):
        before = deepcopy(self.data)
        declarations = [scope(),scope('LONG')]
        scopes_before = deepcopy(declarations)
        result = self.report(declarations)
        again = self.report(list(reversed(declarations)))
        self.assertEqual(result['coverage_version'],'no-horizon-matched-parent-coverage-v1')
        self.assertEqual(result['source_export_sha256'],result['source_preflight']['source_export_sha256'])
        self.assertEqual(result['source_preflight_receipt_sha256'],result['source_preflight']['receipt_sha256'])
        self.assertFalse(result['scope_pooling'])
        self.assertEqual(result,again)
        self.assertEqual(self.data,before)
        self.assertEqual(declarations,scopes_before)
        self.assertEqual(result['receipt_sha256'],contracts.digest({k:v for k,v in result.items() if k!='receipt_sha256'}))
        source_receipt = result['source_preflight']
        self.assertEqual(source_receipt['receipt_sha256'],contracts.digest({k:v for k,v in source_receipt.items() if k!='receipt_sha256'}))
        changed = deepcopy(result)
        changed['scopes'][0]['matched_parent_count'] += 1
        self.assertNotEqual(changed['receipt_sha256'],contracts.digest({k:v for k,v in changed.items() if k!='receipt_sha256'}))

    def test_minimum_comes_from_validated_gate_policy_without_evaluating_gate(self):
        from research_no_horizon_gate import make_policy
        self.data = deepcopy(self.five)
        policy = make_policy(policy_version='coverage-selftest-six-parents-v1',minimum_waves=6)
        result = self.report(gate_policy=policy)
        self.assertEqual(result['scopes'][0]['minimum_required_parents'],6)
        self.assertEqual(result['scopes'][0]['coverage_status'],'INSUFFICIENT')
        altered = {**policy,'minimum_waves':True}
        with self.assertRaises(ValueError):
            self.report(gate_policy=altered)


if __name__ == '__main__':
    unittest.main()
