"""Artificial retained-map fixtures; no production evidence or strategy tests."""
from __future__ import annotations
import copy
import json
import unittest
import ast
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

from research_forward_predicate_audit import audit_predicates, evaluate_condition
from research_forward_paper_engine import PaperStudy
from research_forward_source_audit_selftest import (
    policy, event, candidate, capture, predicate_envelope, digest)


def fixture(*, extra=False, bootstrap_positive=True):
    engine = PaperStudy(':memory:', policy(), resume=False)
    try:
        engine.observe_candidates(candidate('boot', 1, [event(1, at=-1)] if bootstrap_positive else [], True))
        engine.observe_candidates(candidate('forward-doge', 10, [event(2)]))
        engine.observe_candidates(candidate('forward-eth', 11, [event(3, 'ETH', at=6)]))
        if extra: engine.observe_candidates(candidate('forward-extra', 12, [event(4, at=7)]))
        s, source, baseline = capture(engine)
        return s, source, predicate_envelope(s, source), baseline
    finally:
        engine.close()


class PredicateTests(unittest.TestCase):
    def setUp(self): self.s, self.source, self.e, self.baseline = fixture()
    def audit(self): return audit_predicates(self.s, self.source, self.e)
    def failed(self, code=None):
        result = self.audit()
        self.assertEqual(result['status'], 'FAIL', result)
        if code: self.assertIn(code, {r['code'] for r in result['failures']})
        return result

    def test_all_recorded_positives_bootstrap_and_inverse(self):
        r = self.audit(); self.assertEqual(r['status'], 'PASS', r['failures'])
        self.assertEqual(r['summary']['expected_positive_keys'], 3)
        self.assertEqual(r['summary']['by_symbol'], {'DOGE': 2, 'ETH': 1})
        eth = next(c for c in r['cases'] if c['symbol'] == 'ETH')
        self.assertEqual((eth['source_direction'], eth['formula_direction']), ('SHORT', 'LONG'))
        self.assertFalse(r['coverage']['raw_market_features_reconstructed'])
        self.assertFalse(r['coverage']['feature_capture_at_original_reader_receipt'])

    def test_original_scalar_semantics(self):
        cases = [(2.0, '==', 2, True), ('2', '==', 2, False), (True, '==', 1, False),
                 (False, '==', False, True), ('70', '>=', 65, True), (True, '>=', 0, False),
                 ('n/a', '>=', 65, False), (float('nan'), '>=', 65, False),
                 (float('inf'), '==', float('inf'), False), (65, '>', 65, False),
                 (65, '<=', 65, True), (65, '<', 66, True)]
        for actual, op, expected, passed in cases:
            with self.subTest(actual=actual, op=op):
                self.assertEqual(evaluate_condition(actual, op, expected), passed)
        self.assertFalse(evaluate_condition(2, '==', 2, present=False))

    def test_matches_frozen_reference_predicates(self):
        source = Path(__file__).with_name('research_formula_ordered_v7.py').read_text(encoding='utf-8-sig')
        nodes = []
        for node in ast.parse(source).body:
            if isinstance(node, ast.FunctionDef) and node.name in {'_number','_conditions','_matches','matches'}:
                nodes.append(node)
            elif isinstance(node, ast.Assign) and any(isinstance(t,ast.Name) and t.id in {'DIRECTIONS','_FEATURE_PREFIXES','_LABEL_TOKENS'} for t in node.targets):
                nodes.append(node)
        namespace = {'math':math,'Any':Any,'Mapping':Mapping,'Sequence':Sequence}
        exec(compile(ast.Module(body=nodes,type_ignores=[]),'frozen_reference','exec'),namespace)
        for actual in [None,False,True,-1,0,2,2.0,'2','70','bad',65,65.0,float('inf'),float('nan')]:
            for op in ['==','>=','<=','>','<']:
                for expected in [False,True,2,65,65.0,'2']:
                    c={'feature':'sequence.test','operator':op,'value':expected}
                    with self.subTest(actual=actual,op=op,expected=expected):
                        self.assertEqual(evaluate_condition(actual,op,expected),namespace['_matches']({'sequence.test':actual},[c]))
        for row in self.e['matches']:
            c=next(d['definition']['candidate'] for d in self.e['definitions'] if d['candidate_key']==row['candidate_key'])
            self.assertTrue(namespace['matches'](c,row['decision_features'],row['source_direction']))

    def test_float_ordinal_and_numeric_score_strings_pass(self):
        for row in self.e['matches']:
            f = row['decision_features']
            if row['symbol'] == 'DOGE': f['sequence.30m.liquidity.entry_ordinal'] = 2.0
            else: f['price_oi.aligned_score'] = '70'
        self.assertEqual(self.audit()['status'], 'PASS')

    def test_missing_bad_or_boolean_condition_fails(self):
        for value in [None, True, '2', 3]:
            with self.subTest(value=value):
                s, source, e, _ = fixture()
                f = e['matches'][0]['decision_features']
                if value is None: del f['sequence.30m.liquidity.entry_ordinal']
                else: f['sequence.30m.liquidity.entry_ordinal'] = value
                r = audit_predicates(s, source, e)
                self.assertEqual(r['status'], 'FAIL')
                self.assertIn('RETAINED_CONDITION', {x['code'] for x in r['failures']})

    def test_wrong_original_direction_or_inverse_route_fails(self):
        row = next(r for r in self.e['matches'] if r['symbol'] == 'ETH')
        row['source_direction'] = 'LONG'
        self.failed('ORIENTATION_DIRECTION')
        self.setUp(); row = next(r for r in self.e['matches'] if r['symbol'] == 'ETH')
        row['direction'] = 'SHORT'; self.failed('ORIENTATION_DIRECTION')

    def test_definition_hash_and_frozen_pin(self):
        d = self.e['definitions'][0]
        d['definition']['candidate']['conditions'][0]['value'] = 3
        self.failed('DEFINITION_CANONICAL_DIGEST')
        d['definition_sha256'] = digest(d['definition'])
        self.failed('PINNED_DEFINITION_DIGEST')

    def test_metadata_created_source_time_snapshot_and_price(self):
        for field, value, code in [('created_at_utc','2026-01-01T00:00:59Z','CAPTURED_MATCH_METADATA'),
                                   ('source_symbol','BTC','MATCH_CANDIDATE_AND_SYMBOL'),
                                   ('snapshot_id','changed','CAPTURED_SNAPSHOT_ID'),
                                   ('entry_price',101,'CAPTURED_ENTRY_PRICE')]:
            with self.subTest(field=field):
                s, source, e, _ = fixture(); e['matches'][0][field] = value
                r = audit_predicates(s, source, e)
                self.assertEqual(r['status'], 'FAIL')
                self.assertIn(code, {x['code'] for x in r['failures']})

    def test_missing_evidence_rows_maps_or_definitions_are_pending(self):
        self.assertEqual(audit_predicates(self.s, self.source, None)['status'], 'PENDING_PREDICATE_EVIDENCE')
        for what in ('row', 'left-row', 'map', 'definition', 'left-definition', 'incomplete'):
            with self.subTest(what=what):
                s, source, e, _ = fixture()
                if what == 'row': e['matches'].pop(); e['counts']['matched_rows'] -= 1
                elif what == 'left-row': e['matches'][0]['match_present'] = False; e['counts']['matched_rows'] -= 1
                elif what == 'map': e['matches'][0]['decision_features'] = None
                elif what == 'definition': e['definitions'].pop(); e['counts']['definition_rows'] -= 1
                elif what == 'left-definition':
                    d=e['definitions'][0]; d.update({'definition_present':False,'definition':None,'definition_sha256':None,'formula_version':None})
                    e['counts']['definition_rows'] -= 1
                else: e['extraction_complete'] = False
                r = audit_predicates(s, source, e)
                self.assertEqual(r['status'], 'PENDING_PREDICATE_EVIDENCE', r['failures'])
                self.assertFalse(r['coverage']['retained_positive_match_features'])

    def test_current_read_metadata_requires_utc_and_cannot_predate_snapshot(self):
        self.e['read_utc']='2026-01-01T00:00:00Z'; self.failed('CURRENT_SNAPSHOT_READ_TIME')
        self.e['read_utc']='2026-01-01T00:02:00'; self.failed('MALFORMED_PREDICATE_EVIDENCE')

    def test_duplicate_extra_and_expected_coverage_fail(self):
        self.e['matches'].append(copy.deepcopy(self.e['matches'][0])); self.e['counts']['matched_rows'] += 1
        self.failed('UNIQUE_RETAINED_MATCH')
        self.setUp(); self.e['matches'][0]['event_id'] = 999; self.failed('KNOWN_RETAINED_MATCH')
        self.setUp(); self.e['expected_keys'].pop(); self.failed('EXACT_EXPECTED_POSITIVE_KEYS')
        self.setUp(); self.s['state_json']['first_seen']['doge:99'] = {}; self.failed('RECORDED_FIRST_SEEN_KEY_COVERAGE')

    def test_source_packet_digest_and_count_mismatch_fail(self):
        self.source['packets'][0]['payload_json']['rows'][0]['source_direction'] = 'LONG'
        self.failed('CANDIDATE_PACKET_DIGEST')
        self.setUp(); self.e['counts']['expected_positive_keys'] = 4; self.failed('EXPECTED_POSITIVE_COUNT')

    def test_supporting_identity_is_separate_and_no_extra_strategy_gate(self):
        eth = next(r for r in self.e['matches'] if r['symbol'] == 'ETH')
        eth['decision_features']['event.direction_mapping_valid'] = False
        eth['decision_features']['liquidity.capture_status'] = 'MISSING'
        self.assertEqual(self.audit()['status'], 'PASS')
        eth['decision_features']['price_oi.total_score'] = 75
        result = self.failed('ALIGNED_SCORE_IDENTITY')
        bad = next(r for r in result['failures'] if r['code'] == 'ALIGNED_SCORE_IDENTITY')
        self.assertEqual(bad['group'], 'supporting_feature_consistency')
        self.assertTrue(all(c['passed'] for case in result['cases'] for c in case['conditions']))

    def test_nonfinite_unused_feature_fails_with_finite_result(self):
        self.e['matches'][0]['decision_features']['unused'] = float('nan')
        r = self.failed('MALFORMED_PREDICATE_EVIDENCE')
        json.dumps(r, allow_nan=False)

    def test_full_core_integration_missing_and_corrupt_evidence(self):
        from research_forward_evaluation import assess, safe_summary
        r = assess(self.s, [], expected_baseline=self.baseline, source_evidence=self.source, predicate_evidence=self.e)
        self.assertEqual(r['status'], 'PASS', r['failures'])
        self.assertTrue(r['audit_scope']['predicates_recomputed_from_retained_match_features'])
        self.assertTrue(safe_summary(r)['formula_predicates_recomputed'])
        self.assertFalse(safe_summary(r)['raw_market_features_reconstructed'])
        pending = assess(self.s, [], expected_baseline=self.baseline, source_evidence=self.source)
        self.assertEqual(pending['status'], 'PENDING_PREDICATE_EVIDENCE')
        self.e['matches'][0]['decision_features']['sequence.30m.liquidity.entry_ordinal'] = 1
        bad = assess(self.s, [], expected_baseline=self.baseline, source_evidence=self.source, predicate_evidence=self.e)
        self.assertEqual(bad['status'], 'FAIL')
        self.assertFalse(bad['audit_scope']['formula_predicates_recomputed'])

    def test_prospective_pin_retries_drift_and_restoration(self):
        from research_forward_evaluation import assess
        def run(evidence, previous=None):
            return assess(self.s, [], expected_baseline=self.baseline, source_evidence=self.source,
                          predicate_evidence=evidence, previous_assessment=previous)
        first = run(self.e)
        self.assertEqual(first['status'], 'PASS', first['failures'])
        self.assertEqual(first['predicate_audit']['summary']['pins_established'], 3)
        changed = copy.deepcopy(self.e)
        eth = next(r for r in changed['matches'] if r['symbol'] == 'ETH')['decision_features']
        eth.update({'price_oi.aligned_score':71,'price_oi.total_score':71})
        # The changed values still pass both the predicate and extraction identity.
        self.assertEqual(audit_predicates(self.s, self.source, changed)['status'], 'PASS')
        for _ in range(2):
            bad = run(changed, first)
            self.assertEqual(bad['status'], 'FAIL')
            self.assertIn('FORMULA_PREDICATE_RETAINED_RECORD_UNCHANGED', {f['code'] for f in bad['failures']})
            self.assertEqual(bad['predicate_audit']['summary']['pins_established'], 0)
        restored = run(self.e, first)
        self.assertEqual(restored['status'], 'PASS', restored['failures'])
        self.assertEqual(restored['predicate_audit']['summary']['prior_keys_compared'], 3)
        self.assertTrue(restored['audit_scope']['unchanged_since_first_successful_v3_capture'])
        invalid_previous = copy.deepcopy(first)
        del invalid_previous['predicate_audit']['cases'][0]['retained_record_sha256']
        invalid = run(self.e, invalid_previous)
        self.assertIn(invalid['status'], {'FAIL','PENDING_PREDICATE_EVIDENCE'})
        self.assertEqual(invalid['predicate_audit']['summary']['pins_established'], 0)
        failed_previous = copy.deepcopy(first); failed_previous['status'] = 'FAIL'
        self.assertEqual(run(self.e, failed_previous)['status'], 'PENDING_PREDICATE_EVIDENCE')

    def test_prospective_pin_accepts_new_keys_and_rejects_removed_history(self):
        from research_forward_evaluation import assess
        first = assess(self.s, [], expected_baseline=self.baseline, source_evidence=self.source, predicate_evidence=self.e)
        s, source, e, baseline = fixture(extra=True)
        new = assess(s, [], expected_baseline=baseline, source_evidence=source, predicate_evidence=e, previous_assessment=first)
        self.assertEqual(new['status'], 'PASS', new['failures'])
        self.assertEqual(new['predicate_audit']['summary']['pins_established'], 1)
        self.assertEqual(new['predicate_audit']['summary']['prior_keys_compared'], 3)
        s, source, e, baseline = fixture(bootstrap_positive=False)
        lost = assess(s, [], expected_baseline=baseline, source_evidence=source, predicate_evidence=e, previous_assessment=first)
        self.assertEqual(lost['status'], 'FAIL')
        self.assertIn('FORMULA_PREDICATE_PREVIOUS_POSITIVE_REMAINS_PRESENT', {f['code'] for f in lost['failures']})

    def test_pin_ignores_later_read_clock_and_presence_annotations(self):
        from research_forward_predicate_audit import retained_record_digest
        row = copy.deepcopy(self.e['matches'][0]); definition = copy.deepcopy(self.e['definitions'][0])
        before = retained_record_digest(row, definition)
        row['read_utc']='later'; row['input_ordinal']=123; row['match_present']=False
        row['created_at_utc']=row['created_at_utc'].replace('Z','+00:00')
        definition['definition_present']=True
        self.assertEqual(before, retained_record_digest(row, definition))


if __name__ == '__main__': unittest.main()
