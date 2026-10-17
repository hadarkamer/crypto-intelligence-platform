"""Fail-closed declaration/common-anchor contract regressions; no DB or outcomes."""
from copy import deepcopy
from datetime import timedelta
import hashlib
import unittest
from unittest.mock import patch

import research_no_horizon_cohort as cohort
import research_no_horizon_contract as contracts
import research_no_horizon_gate as gate
from research_no_horizon_cohort_coverage_selftest import cohort_fixture


def unsealed(anchor):
    value = deepcopy(anchor)
    value.pop('anchor_sha256', None)
    value['extraction_receipt'].pop('query_sha256', None)
    for part in value['parts']:
        part['manifest']['receipt'].pop('query_sha256', None)
    return value


def rehash(anchor):
    anchor['anchor_sha256'] = contracts.digest({k:v for k,v in anchor.items() if k!='anchor_sha256'})
    return anchor


class CohortContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        declaration, anchor, _ = cohort_fixture()
        cls.original = declaration, anchor

    def setUp(self):
        self.declaration, self.anchor = deepcopy(self.original)

    def bare(self):
        value = deepcopy(self.declaration)
        value.pop('resource_budget')
        value.pop('version_bindings')
        return value

    def partition(self, count, byte_limit=None):
        value = self.bare()
        start,end = (contracts.utc(value[k]) for k in ('source_start_utc','source_end_utc'))
        step = (end-start)/count
        value['parts'] = [{'ordinal':i,'source_start_utc':(start+i*step).isoformat(),
            'source_end_utc':(end if i==count-1 else start+(i+1)*step).isoformat(),
            **({'source_byte_limit':byte_limit} if byte_limit is not None else {})} for i in range(count)]
        return value

    def test_normalization_is_idempotent_and_does_not_mutate_input(self):
        value = self.bare()
        original = deepcopy(value)
        normalized = cohort.normalize_declaration(value)
        self.assertEqual(value, original)
        self.assertEqual(normalized,self.declaration)
        self.assertEqual(cohort.normalize_declaration(normalized),normalized)
        self.assertEqual(normalized['gate_policy'],gate.make_policy())
        self.assertEqual(normalized['resource_budget']['max_parts'],8)

    def test_adjacent_intervals_cannot_hide_a_gap_overlap_or_missing_edge(self):
        for part,key,seconds in ((1,'source_start_utc',1),(1,'source_start_utc',-1),
                                 (0,'source_start_utc',1),(1,'source_end_utc',-1)):
            with self.subTest(part=part,key=key,seconds=seconds):
                value = self.bare()
                value['parts'][part][key] = (contracts.utc(value['parts'][part][key])+timedelta(seconds=seconds)).isoformat()
                with self.assertRaises(ValueError):
                    cohort.normalize_declaration(value)

    def test_order_ordinals_and_zero_length_parts_are_not_silently_repaired(self):
        variants = []
        reverse = self.bare();reverse['parts'].reverse();variants.append(reverse)
        duplicate = self.bare();duplicate['parts'][1]['ordinal']=0;variants.append(duplicate)
        boolean = self.bare();boolean['parts'][0]['ordinal']=False;variants.append(boolean)
        empty = self.bare();empty['parts'][0]['source_end_utc']=empty['parts'][0]['source_start_utc'];variants.append(empty)
        for value in variants:
            with self.subTest(parts=value['parts']):
                with self.assertRaises(ValueError):
                    cohort.normalize_declaration(value)

    def test_global_31day_limit_applies_before_any_part_query_is_built(self):
        value = self.bare()
        start = contracts.utc(value['source_start_utc'])
        value['cutoff_utc'] = (start+timedelta(days=31)).isoformat()
        self.assertEqual(cohort.normalize_declaration(value)['cutoff_utc'],value['cutoff_utc'])
        value['cutoff_utc'] = (start+timedelta(days=31,microseconds=1)).isoformat()
        with self.assertRaisesRegex(ValueError,'GLOBAL_COHORT_TIME'):
            cohort.anchor_sql(value)

    def test_naive_times_future_end_and_invalid_perpart_caps_reject(self):
        for change in ('naive','end_after_cutoff','rows','candles','page','bytes','bool_cap'):
            with self.subTest(change=change):
                value = self.bare()
                if change=='naive': value['declared_at_utc']='2026-09-01T00:00:00'
                elif change=='end_after_cutoff': value['cutoff_utc']=value['source_start_utc']
                else:
                    key,number = {'rows':('source_row_limit',257),'candles':('max_candle_rows',44641),
                        'page':('page_size',2049),'bytes':('source_byte_limit',64*1024*1024+1),
                        'bool_cap':('source_row_limit',True)}[change]
                    value['parts'][0][key]=number
                with self.assertRaises(ValueError): cohort.normalize_declaration(value)

    def test_eight_parts_fit_only_inside_aggregate_declared_byte_budget(self):
        self.assertEqual(len(cohort.normalize_declaration(self.partition(8,32*1024*1024))['parts']),8)
        with self.assertRaisesRegex(ValueError,'PART_LIMIT'):
            cohort.normalize_declaration(self.partition(9,1024))
        with self.assertRaisesRegex(ValueError,'DECLARED_COHORT_BYTE_BUDGET'):
            cohort.normalize_declaration(self.partition(5))

    def test_unknown_fields_and_forged_version_or_budget_bindings_reject(self):
        for change in ('top','part','version','bindings','budget','knowledge_missing','knowledge_not_bool'):
            with self.subTest(change=change):
                value = deepcopy(self.declaration)
                if change=='top': value['allow_prior_outcome_pooling']=True
                elif change=='part': value['parts'][0]['drop_unknown_rows']=True
                elif change=='version': value['cohort_version']='future-unsupported'
                elif change=='bindings': value['version_bindings']['parent_policy_version']='different'
                elif change=='budget': value['resource_budget']['max_parts']=9
                elif change=='knowledge_missing': value.pop('prior_outcomes_observed')
                else: value['prior_outcomes_observed']=1
                with self.assertRaises(ValueError): cohort.normalize_declaration(value)

    def test_scopes_and_policy_are_validated_before_freezing(self):
        for change in ('empty','duplicate','unsupported','lower_minimum','changed_same_policy','unknown_policy_field'):
            with self.subTest(change=change):
                value = self.bare()
                if change=='empty': value['scopes']=[]
                elif change=='duplicate': value['scopes']*=2
                elif change=='unsupported': value['scopes'][0]['candidate_key']='INVENTED_CANDIDATE'
                elif change=='lower_minimum': value['gate_policy']['minimum_waves']=3
                elif change=='changed_same_policy': value['gate_policy']['minimum_waves']=6
                else: value['gate_policy']['accept_unknown']=True
                with self.assertRaises(ValueError): cohort.normalize_declaration(value)
        value = self.bare();value['gate_policy']=gate.make_policy(policy_version='fixture-six-v1',minimum_waves=6)
        self.assertEqual(cohort.normalize_declaration(value)['gate_policy']['minimum_waves'],6)

    def test_seal_binds_exact_sql_everywhere_without_mutating_raw_anchor(self):
        raw = unsealed(self.anchor);original = deepcopy(raw)
        actual = cohort.seal_anchor(self.declaration,raw)
        query_sha = hashlib.sha256(cohort.anchor_sql(self.declaration).encode()).hexdigest()
        self.assertEqual(raw,original)
        self.assertEqual(actual,self.anchor)
        self.assertEqual(actual['extraction_receipt']['query_sha256'],query_sha)
        self.assertTrue(all(p['manifest']['receipt']['query_sha256']==query_sha for p in actual['parts']))
        self.assertEqual(cohort.validate_anchor(self.declaration,actual),actual)

    def test_seal_never_replaces_prior_query_hash_or_reseals_an_old_root(self):
        for where in ('root','child','sealed'):
            with self.subTest(where=where):
                value = self.anchor if where=='sealed' else unsealed(self.anchor)
                if where=='root': value['extraction_receipt']['query_sha256']='0'*64
                elif where=='child': value['parts'][0]['manifest']['receipt']['query_sha256']='0'*64
                before = deepcopy(value)
                with self.assertRaises(ValueError): cohort.seal_anchor(self.declaration,value)
                self.assertEqual(value,before)

    def test_validation_does_not_implicitly_seal_unhashed_evidence(self):
        with self.assertRaises(ValueError): cohort.validate_anchor(self.declaration,unsealed(self.anchor))
        value = deepcopy(self.anchor);value['parts'][0]['manifest']['receipt'].pop('query_sha256');rehash(value)
        with self.assertRaises(ValueError): cohort.validate_anchor(self.declaration,value)

    def test_changed_declaration_or_unrehashed_anchor_is_not_accepted(self):
        value = self.bare();value['cohort_key']='another-declaration'
        with self.assertRaises(ValueError): cohort.validate_anchor(value,self.anchor)
        changed = deepcopy(self.anchor);changed['parts'][0]['manifest']['source_entries'][0]['sha256']='0'*64
        with self.assertRaisesRegex(ValueError,'IDENTITY_OR_HASH'):
            cohort.validate_anchor(self.declaration,changed)

    def test_child_context_cannot_differ_even_with_a_recomputed_root_hash(self):
        for key,value in (('query_sha256','0'*64),('mvcc_snapshot','other-snapshot'),
                ('extracted_at_utc',(contracts.utc(self.declaration['cutoff_utc'])+timedelta(seconds=1)).isoformat()),
                ('serializer','OTHER'),('serializer_timezone','Asia/Jerusalem'),('read_only','off'),
                ('database_writes',True),('parent_snapshot_mode','UNPINNED'),('page_size',1)):
            with self.subTest(key=key):
                changed = deepcopy(self.anchor);changed['parts'][0]['manifest']['receipt'][key]=value
                with self.assertRaises(ValueError): cohort.validate_anchor(self.declaration,rehash(changed))

    def test_root_provenance_cannot_be_changed_by_rehashing(self):
        for key,value in (('query_sha256','0'*64),('read_only','off'),('database_writes',True),
                          ('query_identity','independent-part-select'),('consistent_read','SEPARATE_SNAPSHOTS')):
            with self.subTest(key=key):
                changed = deepcopy(self.anchor);changed['extraction_receipt'][key]=value
                with self.assertRaises(ValueError): cohort.validate_anchor(self.declaration,rehash(changed))

    def test_anchor_must_postdate_both_declaration_and_cutoff(self):
        raw = unsealed(self.anchor)
        stamp = (contracts.utc(self.declaration['cutoff_utc'])-timedelta(seconds=1)).isoformat()
        raw['extraction_receipt']['extracted_at_utc']=stamp
        for part in raw['parts']: part['manifest']['receipt']['extracted_at_utc']=stamp
        with self.assertRaises(ValueError): cohort.seal_anchor(self.declaration,raw)
        declared = self.bare()
        declared['declared_at_utc']=(contracts.utc(self.declaration['cutoff_utc'])+timedelta(seconds=1)).isoformat()
        raw=unsealed(self.anchor);raw['declaration_sha256']=contracts.digest(cohort.normalize_declaration(declared))
        with self.assertRaisesRegex(ValueError,'PRECEDES_DECLARATION'):
            cohort.seal_anchor(declared,raw)

    def test_required_part_cannot_be_missing_reordered_or_duplicated(self):
        for change in ('missing','reordered','duplicate','extra'):
            with self.subTest(change=change):
                value = unsealed(self.anchor)
                if change=='missing': value['parts'].pop()
                elif change=='reordered': value['parts'].reverse()
                elif change=='duplicate': value['parts'][1]=deepcopy(value['parts'][0])
                else: value['parts'].append(deepcopy(value['parts'][-1]))
                with self.assertRaises(ValueError): cohort.seal_anchor(self.declaration,value)

    def test_source_ids_cannot_repeat_across_otherwise_disjoint_intervals(self):
        raw = unsealed(self.anchor)
        raw['parts'][1]['manifest']['source_entries'][0]['snapshot_set_id']=1
        with self.assertRaisesRegex(ValueError,'DUPLICATE_SOURCE_ID_ACROSS'):
            cohort.seal_anchor(self.declaration,raw)

    def test_complete_source_parent_pins_are_required_and_byte_exact_globally(self):
        for change in ('missing','different_text','different_state'):
            with self.subTest(change=change):
                value=unsealed(self.anchor);entry=value['parts'][1]['manifest']['source_entries'][0]
                if change=='missing': entry.pop('btc_parent_payload_text')
                elif change=='different_text': entry['btc_parent_payload_text']=' '+entry['btc_parent_payload_text']
                else: entry['btc_parent_payload_text']=entry['btc_parent_payload_text'].replace('"evidence_eligible":true','"evidence_eligible":false')
                with self.assertRaises(ValueError): cohort.seal_anchor(self.declaration,value)

    def test_child_overflow_or_changed_interval_cannot_masquerade_as_complete(self):
        for change in ('source_overflow','candle_overflow','truncated','wrong_interval'):
            with self.subTest(change=change):
                value=unsealed(self.anchor);part=value['parts'][0]['manifest']
                if change=='wrong_interval': part['source_end_utc']=self.declaration['source_end_utc']
                else: part['receipt'][change]=True
                with self.assertRaises(ValueError): cohort.seal_anchor(self.declaration,value)

    def test_declared_part_raw_byte_limit_applies_before_loading_payloads(self):
        declaration=self.bare();declaration['parts'][0]['source_byte_limit']=1
        raw=unsealed(self.anchor);raw['declaration_sha256']=contracts.digest(cohort.normalize_declaration(declaration))
        with self.assertRaisesRegex(ValueError,'DECLARED_RAW_BYTE_LIMIT'):
            cohort.seal_anchor(declaration,raw)

    def test_real_root_8mib_limit_rejects_before_interpreting_extra_content(self):
        value=unsealed(self.anchor);value['padding']='x'*cohort.MAX_ANCHOR_BYTES
        with self.assertRaisesRegex(ValueError,'byte limit'):
            cohort.seal_anchor(self.declaration,value)

    def test_global_source_limit_is_checked_independently_of_part_caps(self):
        with patch.object(cohort,'MAX_SOURCE_ROWS',5):
            declaration=cohort.normalize_declaration(self.bare())
            raw=unsealed(self.anchor);raw['declaration_sha256']=contracts.digest(declaration)
            with self.assertRaisesRegex(ValueError,'GLOBAL_INPUT_OR_DECISION_BUDGET'):
                cohort.seal_anchor(declaration,raw)

    def test_actual_row_times_scope_limit_cannot_be_evaded_by_small_parts(self):
        declaration=self.bare()
        seed=declaration['scopes'][0]
        declaration['scopes']=[{**seed,'threshold_pct':i+.25} for i in range(64)]
        declaration=cohort.normalize_declaration(declaration)
        raw=unsealed(self.anchor);raw['declaration_sha256']=contracts.digest(declaration)
        for part,expected in zip(raw['parts'],declaration['parts']):
            entries=[]
            base=deepcopy(part['manifest']['source_entries'][0])
            for i in range(128):
                entries.append({**base,'ordinal':i,'snapshot_set_id':part['ordinal']*256+i+1,
                    'usable_from_utc':(contracts.utc(expected['source_start_utc'])+timedelta(seconds=i)).isoformat(),
                    'btc_parent_payload_text':'null'})
            part['manifest']['source_entries']=entries
            part['manifest']['receipt'].update(source_count=128,source_count_capped_plus_one=128)
        self.assertEqual(sum(len(p['manifest']['source_entries']) for p in cohort.seal_anchor(declaration,raw)['parts'])*64,16384)
        extra=deepcopy(raw['parts'][1]['manifest']['source_entries'][-1])
        extra.update(ordinal=128,snapshot_set_id=385,
            usable_from_utc=(contracts.utc(declaration['parts'][1]['source_start_utc'])+timedelta(seconds=128)).isoformat())
        raw['parts'][1]['manifest']['source_entries'].append(extra)
        raw['parts'][1]['manifest']['receipt'].update(source_count=129,source_count_capped_plus_one=129)
        with self.assertRaisesRegex(ValueError,'GLOBAL_INPUT_OR_DECISION_BUDGET'):
            cohort.seal_anchor(declaration,raw)


if __name__=='__main__':
    unittest.main()
