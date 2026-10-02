"""Source feature preflight tests; no DB, jobs, or outcome computation."""
from copy import deepcopy
from contextlib import ExitStack
import json
import unittest
from unittest.mock import patch

import research_no_horizon_contract as contracts
import research_no_horizon_preflight as preflight
import research_no_horizon_source as source
from research_no_horizon_source_selftest import fixture


def scope(direction='SHORT', candidate='FUTURES_CVD_TOTAL_65', threshold=1.5):
    return {'candidate_key':candidate,'base_direction':direction,'threshold_pct':threshold}


class FeaturePreflight(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original = fixture()
        cls.unavailable = fixture(unavailable=True)

    def setUp(self):
        self.data = deepcopy(self.original)

    def report(self, scopes=None):
        return preflight.preflight_source_features(self.data,scopes or [scope()])

    def test_valid_intake_features_and_predicate_counts_are_separate(self):
        result = self.report([scope(),scope('LONG')])
        self.assertEqual(result['accepted_source_rows'],1)
        self.assertEqual(result['validated_source_rows'],1)
        self.assertEqual(result['invalid_source_rows'],0)
        self.assertTrue(result['ready_for_outcome_research'])
        rows = {row['base_direction']:row for row in result['scopes']}
        self.assertEqual(rows['SHORT']['counts']['MATCH'],1)
        self.assertEqual(rows['LONG']['counts']['NO_MATCH'],1)
        available = result['feature_availability']['futures_cvd.aligned_score']
        self.assertEqual(available['available_rows'],1)
        self.assertEqual(available['unavailable_rows'],0)
        self.assertEqual(rows['SHORT']['decision_ledger'][0]['ordinal'],0)

    def test_accepted_partial_capture_becomes_unknown_feature_not_unknown_source(self):
        self.data = deepcopy(self.unavailable)
        result = self.report([scope(),scope('LONG')])
        self.assertEqual(result['validated_source_rows'],1)
        self.assertEqual(result['invalid_source_rows'],0)
        self.assertFalse(result['ready_for_outcome_research'])
        for row in result['scopes']:
            self.assertEqual(row['counts'],{'MATCH':0,'NO_MATCH':0,'UNKNOWN':1,'UNKNOWN_SOURCE':0})
            self.assertEqual(row['decision_ledger'][0]['missing_feature_reasons'],
                {'futures_cvd.aligned_score':['MODEL_UNAVAILABLE']})
        feature = result['feature_availability']['futures_cvd.aligned_score']
        self.assertEqual(feature['unavailable_rows'],1)
        self.assertEqual(feature['reason_counts'],{'MODEL_UNAVAILABLE':1})
        self.assertEqual(feature['unavailable_source_ordinals'],[0])

    def test_known_false_conjunct_remains_decidable_despite_missing_features(self):
        result = self.report([scope('LONG','STRICT_TRIPLE_TOTAL_65')])
        row = result['scopes'][0]
        self.assertTrue(result['ready_for_outcome_research'])
        self.assertTrue(row['predicate_decisions_complete'])
        self.assertFalse(row['required_features_complete'])
        self.assertEqual(row['counts']['NO_MATCH'],1)
        self.assertTrue(row['decision_ledger'][0]['missing_features'])
        self.assertTrue(row['decision_ledger'][0]['missing_feature_reasons'])
        self.assertFalse(row['source_blockers'])

    def test_unrelated_unavailable_models_do_not_block_requested_scope(self):
        result = self.report()
        self.assertTrue(result['ready_for_outcome_research'])
        self.assertEqual(set(result['feature_availability']),{'futures_cvd.aligned_score'})
        self.assertTrue(result['scopes'][0]['required_features_complete'])

    def test_invalid_duplicate_and_malformed_rows_are_retained_with_ordinals(self):
        duplicate = deepcopy(self.data['source_rows'][0])
        bad_id = deepcopy(duplicate)
        bad_id['intake']['snapshot_set_id'] = 'invalid'
        self.data['source_rows'].extend([duplicate,None,{'intake':[]},bad_id])
        self.data['source_receipt']['expected_accepted_rows'] = 5
        result = self.report([scope(),scope('LONG')])
        self.assertEqual(result['accepted_source_rows'],5)
        self.assertEqual(result['validated_source_rows'],1)
        self.assertEqual(result['invalid_source_rows'],4)
        for row in result['scopes']:
            self.assertEqual(len(row['decision_ledger']),5)
            self.assertEqual([item['ordinal'] for item in row['decision_ledger']],list(range(5)))
            self.assertEqual([item['snapshot_set_id'] for item in row['decision_ledger']],[1,1,None,None,'invalid'])
            self.assertEqual(row['counts']['UNKNOWN_SOURCE'],4)
            self.assertEqual(sum(row['counts'].values()),5)
        self.assertEqual(result['feature_availability']['futures_cvd.aligned_score']['unknown_source_rows'],4)
        self.assertFalse(result['ready_for_outcome_research'])

    def test_tampered_capture_and_missing_raw_source_are_not_no_match(self):
        for kind in ('hash','missing'):
            with self.subTest(kind=kind):
                self.data = deepcopy(self.original)
                if kind == 'hash':
                    self.data['source_rows'][0]['scores']['coins']['BTC']['models']['futures_flow']['score']=0
                else:
                    self.data['source_rows'][0]['stored_source']=None
                result = self.report([scope('LONG')])
                self.assertEqual(result['scopes'][0]['counts']['UNKNOWN_SOURCE'],1)
                self.assertEqual(result['scopes'][0]['counts']['NO_MATCH'],0)
                self.assertTrue(result['scopes'][0]['decision_ledger'][0]['source_error'])
                self.assertFalse(result['ready_for_outcome_research'])

    def test_existing_validation_runs_once_per_row_across_all_scopes(self):
        declarations = [scope(),scope('LONG'),scope(threshold=3),scope('LONG','STRICT_TRIPLE_TOTAL_65')]
        with patch.object(source,'_validate_row',wraps=source._validate_row) as validate:
            result = self.report(declarations)
        self.assertEqual(validate.call_count,1)
        self.assertEqual(len(result['scopes']),4)
        self.assertEqual(result['feature_availability']['futures_cvd.aligned_score']['available_rows'],1)

    def test_never_constructs_entries_assigns_parents_or_evaluates_outcomes(self):
        targets = ('research_no_horizon_source.build_snapshot', 'research_no_horizon_source._parent_evidence',
            'research_no_horizon_source._entry_time','research_no_horizon_source.touch._bar',
            'research_no_horizon_contract.make_contract','research_no_horizon_first_touch.initialize',
            'research_no_horizon_first_touch.advance','research_no_horizon_gate.evaluate_gate',
            'research_no_horizon_store.LocalResearchStore.submit_snapshot')
        with ExitStack() as stack:
            for target in targets:
                stack.enter_context(patch(target,side_effect=AssertionError('not part of feature preflight')))
            result = self.report()
        for key in ('entry_evaluation','parent_evaluation','outcome_evaluation','gate_evaluation'):
            self.assertEqual(result[key],'NOT_EVALUATED')
        self.assertIsNone(result['gate'])
        self.assertFalse(result['outcomes_evaluated'])
        for key in ('runtime_authorized','telegram_authorized','trading_authorized'):
            self.assertFalse(result[key])

    def test_missing_entry_price_or_parent_is_not_misclassified_as_missing_features(self):
        self.data['candles'] = []
        self.data['source_rows'][0]['btc_parent'] = None
        self.data['source_rows'][0]['btc_prior_bar'] = None
        result = self.report()
        self.assertTrue(result['ready_for_outcome_research'])
        self.assertEqual(result['scopes'][0]['counts']['MATCH'],1)
        self.assertEqual(result['parent_evaluation'],'NOT_EVALUATED')
        self.assertEqual(result['entry_evaluation'],'NOT_EVALUATED')

    def test_shared_extraction_checks_block_every_scope_without_hiding_diagnostics(self):
        for change in ({'expected_accepted_rows':2},{'rows_complete':False},{'truncated':True},
                {'candle_truncated':True},{'transaction_mode':'UNRECOGNIZED'},
                {'transaction_mode':'MANIFEST_ATTESTED_MULTI_READ_V1'}):
            with self.subTest(change=change):
                self.data = deepcopy(self.original)
                self.data['source_receipt'].update(change)
                result = self.report([scope(),scope('LONG')])
                self.assertFalse(result['ready_for_outcome_research'])
                self.assertEqual(result['validated_source_rows'],1)
                self.assertTrue(result['extraction_blockers'])
                for row in result['scopes']:
                    self.assertFalse(row['ready_for_outcome_research'])
                    self.assertEqual(row['source_blockers'],result['extraction_blockers'])
                snapshot = source.build_snapshot(self.data,'FUTURES_CVD_TOTAL_65','SHORT','BTC',1.5)
                self.assertEqual(snapshot['source_receipt']['source_blockers'],result['extraction_blockers'])

    def test_every_scope_is_reported_when_another_scope_is_unknown(self):
        result = self.report([scope(),scope('LONG','PRICE_OI_TOTAL_65')])
        rows = {row['candidate_key']:row for row in result['scopes']}
        self.assertTrue(rows['FUTURES_CVD_TOTAL_65']['ready_for_outcome_research'])
        self.assertFalse(rows['PRICE_OI_TOTAL_65']['ready_for_outcome_research'])
        self.assertFalse(result['ready_for_outcome_research'])
        self.assertEqual(len(result['scopes']),2)

    def test_report_is_deterministic_bound_and_does_not_mutate_input(self):
        before = deepcopy(self.data)
        declarations = [scope(),scope('LONG')]
        result = self.report(declarations)
        again = self.report(list(reversed(declarations)))
        self.assertEqual(result,again)
        self.assertEqual(self.data,before)
        self.assertEqual(result['receipt_sha256'],contracts.digest({key:value for key,value in result.items() if key!='receipt_sha256'}))
        self.assertNotIn('generated_at',result)
        altered = deepcopy(result)
        altered['scopes'][0]['counts']['MATCH'] += 1
        self.assertNotEqual(altered['receipt_sha256'],contracts.digest({key:value for key,value in altered.items() if key!='receipt_sha256'}))

    def test_scope_errors_and_global_input_errors_reject_before_row_work(self):
        with patch.object(source,'_validate_row',side_effect=AssertionError('must not classify rows')):
            for declarations in ([scope(),scope(threshold='1.500')],[scope(candidate='unknown')],[]):
                with self.subTest(declarations=declarations):
                    with self.assertRaises(ValueError):
                        preflight.preflight_source_features(self.data,declarations)
            self.data['source_rows'] = None
            with self.assertRaises(ValueError):
                self.report()

    def test_empty_complete_population_is_decidable_but_has_no_gate_or_evidence(self):
        self.data['source_rows'] = []
        self.data['source_receipt']['expected_accepted_rows'] = 0
        result = self.report()
        self.assertTrue(result['ready_for_outcome_research'])
        self.assertEqual(result['scopes'][0]['counts'],dict.fromkeys(('MATCH','NO_MATCH','UNKNOWN','UNKNOWN_SOURCE'),0))
        self.assertIsNone(result['gate'])
        self.assertEqual(result['outcome_evaluation'],'NOT_EVALUATED')


if __name__=='__main__':
    unittest.main()
