"""Pure common-anchor cohort coverage tests; no outcome, DB or network work.

All populations are synthetic fixtures rebuilt by the genuine capture/archive/
intake/parent pipeline. Child manifests share one explicitly sealed test anchor.
"""
from contextlib import ExitStack
from copy import deepcopy
from datetime import timedelta
import json
import unittest
from unittest.mock import Mock, patch

import research_no_horizon_cohort as cohort
import research_no_horizon_cohort_coverage as coverage
import research_no_horizon_contract as contracts
import research_no_horizon_manifest as manifest
import research_no_horizon_manifest_sql as manifest_sql
import research_no_horizon_source as source
from research_no_horizon_gate import make_policy
from research_no_horizon_manifest_selftest import manifest_fixture
from research_no_horizon_parent_coverage_selftest import scope, source_row
from research_watch_score_capture_selftest import BASE


def later_shared_row(snapshot_set_id=4):
    """Fresh causal prior for a later capture of the same 4-hour parent."""
    row = source_row(parent_offset=2, snapshot_set_id=snapshot_set_id,decision_delay_seconds=90)
    prior = row['btc_prior_bar']
    for field in ('open_time_utc','close_time_utc'):
        prior[field] = contracts.utc(prior[field])+timedelta(minutes=1)
    row['btc_parent']['observed_through_utc'] = prior['close_time_utc']
    return row


def cross_part_rows():
    first = [source_row(parent_offset=i,snapshot_set_id=i+1) for i in range(3)]
    shared = later_shared_row()
    # One anchor observes one exact parent row, even across different decisions.
    first[-1]['btc_parent'] = deepcopy(shared['btc_parent'])
    return [first,[shared,source_row(parent_offset=3,snapshot_set_id=5),
                         source_row(parent_offset=4,snapshot_set_id=6)]]


def cohort_fixture(rows=None, *, scopes=None, gate_policy=None, part_byte_limits=None):
    """Two complete contiguous parts, one source snapshot and one declaration."""
    rows = cross_part_rows() if rows is None else deepcopy(rows)
    if len(rows) != 2:
        raise ValueError('This focused fixture has exactly two required parts')
    bounds = [BASE+timedelta(minutes=6),BASE+timedelta(hours=4,minutes=8),BASE+timedelta(hours=9)]
    cutoff = BASE+timedelta(hours=10)
    declaration = {'cohort_version':cohort.VERSION,'cohort_key':'selftest-common-anchor',
        'declared_at_utc':(BASE-timedelta(days=1)).isoformat(),'prior_outcomes_observed':True,
        'symbol':'BTC','price_route':manifest.ROUTE,
        'source_start_utc':bounds[0].isoformat(),'source_end_utc':bounds[-1].isoformat(),
        'cutoff_utc':cutoff.isoformat(),'scopes':scopes or [scope()],
        'parts':[{'ordinal':ordinal,'source_start_utc':bounds[ordinal].isoformat(),
                  'source_end_utc':bounds[ordinal+1].isoformat()} for ordinal in range(2)]}
    if gate_policy is not None:
        declaration['gate_policy'] = gate_policy
    if part_byte_limits is not None:
        if len(part_byte_limits) != len(declaration['parts']):
            raise ValueError('One byte limit is required per fixture part')
        for part,limit in zip(declaration['parts'],part_byte_limits):
            part['source_byte_limit'] = limit
    declaration = cohort.normalize_declaration(declaration)
    raw_parts, payloads = [], []
    for ordinal,part in enumerate(declaration['parts']):
        ordered = sorted(rows[ordinal],key=lambda row:(contracts.utc(row['intake']['usable_from_utc']),row['intake']['snapshot_set_id']))
        export = {'export_version':source.EXPORT_VERSION,'symbol':'BTC',
            'source_start_utc':part['source_start_utc'],'source_end_utc':part['source_end_utc'],
            'cutoff_utc':declaration['cutoff_utc'],'source_rows':ordered,'candles':[],
            'source_receipt':{'expected_accepted_rows':len(ordered),'rows_complete':True,'truncated':False,
                'transaction_mode':'REPEATABLE_READ_READ_ONLY'}}
        child,sources,candles = manifest_fixture(export)
        child['receipt'].update(parent_snapshot_mode=manifest_sql.PARENT_SNAPSHOT_MODE,
            query_identity='no-horizon-pinned-parent-manifest-select-v2',
            serializer='POSTGRESQL_JSONB_TEXT_UTF8_V1',serializer_timezone='UTC',
            database_writes=False,mvcc_snapshot='123:125:',extracted_at_utc=cutoff.isoformat(),
            source_cap=part['source_row_limit'],candle_cap=part['max_candle_rows'],page_size=part['page_size'])
        child['receipt'].pop('query_sha256')
        for entry,chunk in zip(child['source_entries'],sources):
            entry['btc_parent_payload_text'] = contracts.canonical(json.loads(chunk['payload_text'])['btc_parent'])
        raw_parts.append({'ordinal':ordinal,'manifest':child})
        payloads.append((sources,candles))
    raw_anchor = {'anchor_version':cohort.ANCHOR_VERSION,'declaration_sha256':contracts.digest(declaration),
        'parts':raw_parts,'extraction_receipt':{'transaction_mode':'SINGLE_STATEMENT_READ_ONLY',
            'consistent_read':'ONE_STATEMENT_SNAPSHOT','read_only':'on','mvcc_snapshot':'123:125:',
            'extracted_at_utc':cutoff.isoformat(),'query_identity':'no-horizon-global-part-anchor-select-v1',
            'serializer':'POSTGRESQL_JSONB_TEXT_UTF8_V1','serializer_timezone':'UTC',
            'database_writes':False}}
    anchor = cohort.seal_anchor(declaration,raw_anchor)
    exports = [manifest.assemble_export(part['manifest'],*payloads[ordinal])
               for ordinal,part in enumerate(anchor['parts'])]
    return declaration,anchor,exports


def scope_rows(receipt):
    return {(row['candidate_key'],row['base_direction'],row['threshold_pct']):row for row in receipt['scopes']}


class CohortCoverageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original = cohort_fixture()

    def setUp(self):
        self.declaration,self.anchor,self.exports = deepcopy(self.original)

    def report(self, loader=None):
        return coverage.preflight_cohort(self.declaration,self.anchor,
            loader or (lambda ordinal:self.exports[ordinal]))

    def test_three_plus_three_rows_share_one_parent_and_yield_five_globally(self):
        loader = Mock(side_effect=lambda ordinal:self.exports[ordinal])
        result = self.report(loader)
        self.assertEqual([call.args for call in loader.call_args_list],[(0,),(1,)])
        self.assertEqual(result['part_count'],2)
        self.assertEqual(result['accepted_source_rows'],6)
        row = result['scopes'][0]
        self.assertEqual(row['source_counts'],{'MATCH':6,'NO_MATCH':0,'UNKNOWN':0,'UNKNOWN_SOURCE':0})
        self.assertEqual(row['matched_parent_count'],5)
        self.assertEqual(row['matched_parent_upper_bound'],5)
        self.assertEqual(row['coverage_status'],'POSSIBLE')
        self.assertEqual(row['minimum_required_parents'],5)
        self.assertEqual(len(row['representatives']),5)
        self.assertTrue(result['ready_for_outcome_research'])
        self.assertTrue(result['all_scopes_potentially_sufficient'])
        self.assertTrue(result['coverage_only'])
        self.assertFalse(result['outcome_runner_available'])

    def test_global_earliest_shared_parent_selected_before_any_outcome(self):
        row = self.report()['scopes'][0]
        parent_id = self.exports[0]['source_rows'][2]['btc_parent']['btc_parent_movement_id']
        representative = next(item for item in row['representatives'] if item['btc_parent_movement_id']==parent_id)
        self.assertEqual(representative['entry_id'],'watch:3:BTC:SHORT')
        self.assertEqual((representative['part_ordinal'],representative['ordinal'],representative['global_ordinal']),(0,2,2))
        self.assertEqual([item['global_ordinal'] for item in row['decision_ledger']],list(range(6)))
        self.assertEqual([item['part_ordinal'] for item in row['decision_ledger']],[0,0,0,1,1,1])
        self.assertEqual([item['ordinal'] for item in row['decision_ledger']],[0,1,2,0,1,2])
        self.assertEqual({item['snapshot_set_id'] for item in row['decision_ledger']},set(range(1,7)))
        self.assertNotIn('outcome',representative)
        self.assertNotIn('reference_price',representative)

    def test_different_exact_parent_pins_across_parts_reject_the_single_anchor(self):
        rows = cross_part_rows()
        parent = rows[1][0]['btc_parent']
        parent['direction'] = 'UP' if parent['direction']=='DOWN' else 'DOWN'
        with self.assertRaises(ValueError):
            cohort_fixture(rows)

    def test_shared_causally_invalid_parent_blocks_without_later_replacement(self):
        rows = cross_part_rows()
        invalid = deepcopy(rows[0][2]['btc_parent'])
        invalid['confirmed_at_utc'] = BASE+timedelta(hours=4,minutes=9)
        rows[0][2]['btc_parent'] = deepcopy(invalid)
        rows[1][0]['btc_parent'] = deepcopy(invalid)
        self.declaration,self.anchor,self.exports = cohort_fixture(rows)
        row = self.report()['scopes'][0]
        self.assertEqual(row['source_counts']['MATCH'],6)
        self.assertEqual(row['coverage_status'],'BLOCKED')
        self.assertFalse(row['parent_membership_complete'])
        self.assertIsNone(row['matched_parent_upper_bound'])
        self.assertEqual(row['representatives'],[])
        self.assertEqual(row['representative_selection'],'NOT_SELECTED_INCOMPLETE_POPULATION')
        self.assertEqual(row['unverified_matched_rows'],2)

    def test_unknown_feature_anywhere_blocks_whole_scope_without_dropping_known_matches(self):
        rows = cross_part_rows()
        rows[1][1] = source_row(parent_offset=3,snapshot_set_id=5,unavailable=True)
        self.declaration,self.anchor,self.exports = cohort_fixture(rows)
        result = self.report()
        row = result['scopes'][0]
        self.assertEqual(row['source_counts'],{'MATCH':5,'NO_MATCH':0,'UNKNOWN':1,'UNKNOWN_SOURCE':0})
        self.assertEqual(len(row['decision_ledger']),6)
        self.assertEqual(row['coverage_status'],'BLOCKED')
        self.assertFalse(row['source_decisions_complete'])
        self.assertIsNone(row['matched_parent_upper_bound'])
        self.assertEqual(row['representatives'],[])
        self.assertFalse(result['ready_for_outcome_research'])

    def test_invalid_source_inside_valid_manifest_is_unknown_source_not_transport_loss(self):
        rows = cross_part_rows()
        rows[1][1]['stored_source'] = None
        self.declaration,self.anchor,self.exports = cohort_fixture(rows)
        row = self.report()['scopes'][0]
        self.assertEqual(row['source_counts'],{'MATCH':5,'NO_MATCH':0,'UNKNOWN':0,'UNKNOWN_SOURCE':1})
        self.assertEqual(len(row['decision_ledger']),6)
        self.assertEqual(row['coverage_status'],'BLOCKED')
        self.assertIsNone(row['matched_parent_upper_bound'])
        self.assertEqual(row['representatives'],[])

    def test_missing_malformed_foreign_and_changed_exports_reject_no_successful_receipt(self):
        changed = deepcopy(self.exports[1])
        changed['source_rows'][0]['scores']['coins']['BTC']['models']['futures_flow']['score'] = 0
        variants = (None,{},[],self.exports[0],changed)
        for invalid in variants:
            with self.subTest(kind=type(invalid).__name__):
                loader = Mock(side_effect=lambda ordinal:self.exports[0] if ordinal==0 else invalid)
                with self.assertRaises(ValueError):
                    self.report(loader)
                self.assertEqual(loader.call_count,2)

    def test_anchor_mutation_fails_before_loading_any_part(self):
        self.anchor['parts'][1]['manifest']['receipt']['mvcc_snapshot'] = 'foreign-snapshot'
        loader = Mock(side_effect=AssertionError('invalid anchor must fail before loading'))
        with self.assertRaises(ValueError):
            self.report(loader)
        loader.assert_not_called()

    def test_loaded_parent_must_equal_exact_pin_even_with_rebound_compact_receipt(self):
        changed = deepcopy(self.exports[1])
        parent = changed['source_rows'][0]['btc_parent']
        parent['observed_through_utc'] = (contracts.utc(parent['observed_through_utc'])+timedelta(minutes=1)).isoformat()
        receipt = changed['source_receipt']
        identity = {key:changed[key] for key in ('export_version','symbol','route',
            'source_start_utc','source_end_utc','cutoff_utc')}
        receipt['canonical_payload_sha256'] = contracts.digest({'identity':identity,
            'source_rows':changed['source_rows'],'candles':changed['candles'],
            'anchor_manifest_sha256':receipt['anchor_manifest_sha256']})
        # The compact receipt alone is internally consistent; the sealed pin is not.
        self.assertIsNone(manifest.validate_export_binding(changed))
        self.exports[1] = changed
        with patch.object(source,'_validate_row',wraps=source._validate_row) as validate:
            with self.assertRaisesRegex(ValueError,'COHORT_PART_PARENT_PIN_MISMATCH'):
                self.report()
        self.assertEqual(validate.call_count,3,'Foreign parent must fail before second-part feature evaluation')

    def test_declared_part_byte_limit_includes_assembled_receipt_not_only_raw_leaves(self):
        part = self.anchor['parts'][0]['manifest']
        raw_bytes = sum(item['byte_length'] for item in part['source_entries']+part['candle_pages'])
        canonical_bytes = len(contracts.canonical(self.exports[0]).encode('utf-8'))
        self.assertGreater(canonical_bytes,raw_bytes+1)
        cap = raw_bytes+(canonical_bytes-raw_bytes)//2
        self.assertLess(raw_bytes,cap)
        self.assertLess(cap,canonical_bytes)
        self.declaration,self.anchor,self.exports = cohort_fixture(part_byte_limits=[cap,source.MAX_BYTES])
        self.assertEqual(cohort.validate_anchor(self.declaration,self.anchor),self.anchor)
        with patch.object(source,'_validate_row',side_effect=AssertionError('Overbudget part must fail before feature evaluation')):
            with self.assertRaisesRegex(ValueError,'COHORT_PART_DECLARED_CANONICAL_BYTE_LIMIT_EXCEEDED'):
                self.report()

    def test_empty_required_part_is_loaded_and_retained(self):
        rows = cross_part_rows();rows[1] = []
        self.declaration,self.anchor,self.exports = cohort_fixture(rows)
        loader = Mock(side_effect=lambda ordinal:self.exports[ordinal])
        result = self.report(loader)
        self.assertEqual(loader.call_count,2)
        self.assertEqual(result['part_count'],2)
        self.assertEqual(result['accepted_source_rows'],3)
        row = result['scopes'][0]
        self.assertEqual(row['matched_parent_upper_bound'],3)
        self.assertEqual(row['coverage_status'],'INSUFFICIENT')
        self.assertEqual(len(row['decision_ledger']),3)

    def test_two_threshold_scopes_cannot_pool_three_parents_into_six(self):
        rows = cross_part_rows();rows[1] = []
        self.declaration,self.anchor,self.exports = cohort_fixture(rows,scopes=[scope(threshold=.25),scope(threshold=.5)])
        result = self.report()
        self.assertEqual(len(result['scopes']),2)
        for row in result['scopes']:
            self.assertEqual(row['matched_parent_count'],3)
            self.assertEqual(row['matched_parent_upper_bound'],3)
            self.assertEqual(row['coverage_status'],'INSUFFICIENT')
        self.assertFalse(result['any_scope_potentially_sufficient'])
        self.assertFalse(result['ready_for_outcome_research'])
        self.assertFalse(result['scope_pooling'])

    def test_one_possible_scope_does_not_hide_an_empty_scope(self):
        self.declaration,self.anchor,self.exports = cohort_fixture(scopes=[scope(),scope('LONG')])
        result = self.report()
        rows = scope_rows(result)
        self.assertEqual(rows[('FUTURES_CVD_TOTAL_65','SHORT',.25)]['coverage_status'],'POSSIBLE')
        empty = rows[('FUTURES_CVD_TOTAL_65','LONG',.25)]
        self.assertEqual(empty['source_counts']['NO_MATCH'],6)
        self.assertEqual(empty['matched_parent_count'],0)
        self.assertEqual(empty['coverage_status'],'INSUFFICIENT')
        self.assertTrue(result['any_scope_potentially_sufficient'])
        self.assertFalse(result['all_scopes_potentially_sufficient'])
        self.assertFalse(result['ready_for_outcome_research'])

    def test_custom_versioned_policy_minimum_applies_globally(self):
        policy = make_policy(policy_version='cohort-selftest-six-parent-minimum-v1',minimum_waves=6)
        self.declaration,self.anchor,self.exports = cohort_fixture(gate_policy=policy)
        row = self.report()['scopes'][0]
        self.assertEqual(row['matched_parent_count'],5)
        self.assertEqual(row['minimum_required_parents'],6)
        self.assertEqual(row['coverage_status'],'INSUFFICIENT')

    def test_more_than_256_total_rows_are_admitted_with_each_part_bounded(self):
        original = source_row(parent_offset=2,snapshot_set_id=1)
        later = later_shared_row(snapshot_set_id=257)
        original['btc_parent'] = deepcopy(later['btc_parent'])
        repeated = []
        for sid in range(1,257):
            row = deepcopy(original)
            row['intake']['snapshot_set_id'] = row['stored_source']['snapshot_set_id'] = sid
            repeated.append(row)
        rows = [repeated,[later]]
        self.declaration,self.anchor,self.exports = cohort_fixture(rows)
        with patch.object(source,'_validate_row',wraps=source._validate_row) as validate:
            result = self.report()
        self.assertEqual(validate.call_count,257)
        self.assertEqual(result['accepted_source_rows'],257)
        row = result['scopes'][0]
        self.assertEqual(row['source_counts'],{'MATCH':257,'NO_MATCH':0,'UNKNOWN':0,'UNKNOWN_SOURCE':0})
        self.assertEqual(row['matched_parent_count'],1)
        self.assertEqual(row['matched_parent_upper_bound'],1)
        self.assertEqual(len(row['decision_ledger']),257)
        self.assertEqual(row['coverage_status'],'INSUFFICIENT')

    def test_deterministic_receipt_binding_and_no_input_mutation(self):
        before = deepcopy((self.declaration,self.anchor,self.exports))
        result = self.report()
        again = self.report()
        self.assertEqual(result,again)
        self.assertEqual((self.declaration,self.anchor,self.exports),before)
        self.assertEqual(result['receipt_sha256'],contracts.digest({k:v for k,v in result.items() if k!='receipt_sha256'}))
        self.assertEqual(result['declaration_sha256'],contracts.digest(self.declaration))
        self.assertEqual(result['anchor_sha256'],self.anchor['anchor_sha256'])
        changed = deepcopy(result)
        changed['scopes'][0]['matched_parent_count'] += 1
        self.assertNotEqual(changed['receipt_sha256'],contracts.digest({k:v for k,v in changed.items() if k!='receipt_sha256'}))

    def test_never_builds_entries_computes_outcomes_or_opens_sqlite(self):
        targets = ('research_no_horizon_source.build_snapshot','research_no_horizon_source._entry_time',
            'research_no_horizon_source.touch._bar','research_no_horizon_contract.make_contract',
            'research_no_horizon_first_touch.initialize','research_no_horizon_first_touch.evaluate',
            'research_no_horizon_first_touch.advance','research_no_horizon_gate.evaluate_gate',
            'research_no_horizon_replay.replay_snapshot','research_no_horizon_store.LocalResearchStore.__init__',
            'research_no_horizon_experiment.LocalExperimentStore.__init__','sqlite3.connect',
            'research_watch_score_capture.prepare')
        with ExitStack() as stack:
            for target in targets:
                stack.enter_context(patch(target,side_effect=AssertionError('forbidden during cohort coverage')))
            result = self.report()
        self.assertTrue(result['coverage_only'])
        self.assertFalse(result['outcome_runner_available'])
        self.assertFalse(result['outcomes_evaluated'])
        self.assertIsNone(result['gate'])
        for field in ('entry_evaluation','outcome_evaluation','gate_evaluation'):
            self.assertEqual(result[field],'NOT_EVALUATED')
        for field in ('runtime_authorized','telegram_authorized','trading_authorized'):
            self.assertFalse(result[field])


if __name__ == '__main__':
    unittest.main()
