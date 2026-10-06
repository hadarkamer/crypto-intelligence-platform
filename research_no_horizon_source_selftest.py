"""Pure source-adapter regressions using genuine captured-score fixtures."""
from copy import deepcopy
from datetime import timedelta
from contextlib import redirect_stdout,redirect_stderr
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import research_btc_parent_movement as btc
import research_no_horizon_source as adapter
import research_no_horizon_replay as replay
import research_watch_scan_intake as intake
import research_watch_scan_formula as formulas
import research_watch_scan_formula_maxpain as maxpain
import research_watch_score_capture as capture
from research_watch_scan_intake_selftest import source_fixture
from research_watch_score_capture_selftest import BASE, bundle, derivatives, inputs

MAXPAIN_KEY = 'captured-question-search-v3-experimental-binding:average_score_all_timeframes_GE65'


def fixture(*, unavailable=False, rows=None):
    market = derivatives()
    for item in market.values():
        for window in item['flow']['futures']['windows'].values():
            window['continuous_strength'] = 1.
        if unavailable:
            item['flow']['futures'].update(available=False,windows={},quality={'status':'NO_DATA'})
    block,*_ = bundle(rows=rows,snapshot=market)
    raw = source_fixture(block)
    cutoff = BASE+timedelta(minutes=11)
    accepted = intake.validate_source(raw,now=cutoff,activated_at=BASE)
    accepted.update(consumer_version=intake.VERSION,ingested_at_utc=cutoff)
    bars=[]
    for i,price in enumerate((100.,103.,100.,100.,100.,100.,100.)):
        bars.append({'open_time_utc':BASE+timedelta(minutes=i),
            'close_time_utc':BASE+timedelta(minutes=i+1,milliseconds=-1),
            'open':price,'high':price+.1,'low':price-.1,'close':price})
    parent=btc.advance_parents(bars,as_of_utc=cutoff)[-1]
    prior={**bars[-1],'price_source':btc.SOURCE}
    prices=[{'route':adapter.ARCHIVE_ROUTE,'symbol':'BTC',
        'open_time_utc':BASE+timedelta(minutes=i),'close_time_utc':BASE+timedelta(minutes=i+1,milliseconds=-1),
        'open':100.,'high':100.1,'low':98. if i==8 else 99.9,'close':100.} for i in range(7,11)]
    return {'export_version':adapter.EXPORT_VERSION,'symbol':'BTC',
        'source_start_utc':BASE+timedelta(minutes=6),'source_end_utc':BASE+timedelta(minutes=8),
        'cutoff_utc':cutoff,'source_rows':[{'intake':accepted,
            'stored_source':{k:v for k,v in raw.items() if k!='bundle'},'scores':block,
            'btc_parent':parent,'btc_prior_bar':prior}], 'candles':prices,
        'source_receipt':{'expected_accepted_rows':1,'rows_complete':True,'truncated':False,
            'transaction_mode':'REPEATABLE_READ_READ_ONLY'}}


def maxpain_fixture(*, opposite_active=True):
    """Freeze actual scorer outputs; no hand-written research model scores."""
    rows = inputs()
    for row in rows:
        row.update(short_max_pain=101.,long_max_pain=90. if opposite_active else 100.,
            distance_short_pct=1.,distance_long_pct=-10. if opposite_active else 0.,closest_side='SHORT')
    return fixture(rows=rows)


def refresh_capture(export):
    """Keep valid source hashes/receipt while testing feature-level rejection."""
    row = export['source_rows'][0]
    block = row['scores']
    block['payload_sha256'] = capture.digest({k:v for k,v in block.items() if k!='payload_sha256'})
    raw = source_fixture(block)
    accepted = intake.validate_source(raw,now=export['cutoff_utc'],activated_at=BASE)
    if accepted['intake_status'] != 'ACCEPTED':
        raise AssertionError(accepted['rejection_reason'])
    accepted.update(consumer_version=intake.VERSION,ingested_at_utc=export['cutoff_utc'])
    row.update(intake=accepted,stored_source={k:v for k,v in raw.items() if k!='bundle'})


def build(export,**changes):
    return adapter.build_snapshot(export,**{'candidate_key':'FUTURES_CVD_TOTAL_65',
        'base_direction':'SHORT','symbol':'BTC','threshold_pct':1.5,**changes})


class SourceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original=fixture()
        cls.maxpain=maxpain_fixture()

    def setUp(self):
        self.data=deepcopy(self.original)

    def assert_blocked(self,snapshot):
        self.assertFalse(snapshot['source_coverage_complete'])
        self.assertFalse(replay.replay_snapshot(snapshot)['gate']['experimental_eligible'])
        self.assertEqual(sum(snapshot['source_receipt']['counts'].values()),len(self.data['source_rows']))

    def test_genuine_capture_revalidated_and_entry_is_exact_minute(self):
        before=deepcopy(self.data)
        with patch('research_watch_score_capture.prepare',side_effect=AssertionError('No score replay')):
            snapshot=build(self.data)
        self.assertEqual(self.data,before)
        self.assertTrue(snapshot['source_coverage_complete'])
        self.assertEqual(snapshot['source_receipt']['counts']['MATCH'],1)
        contract=snapshot['opportunities'][0]['contract']
        self.assertEqual(contract['decision_time_utc'],(BASE+timedelta(minutes=7)).isoformat())
        self.assertEqual(contract['reference_price'],100.)
        report=replay.replay_snapshot(snapshot)
        self.assertEqual(report['status_counts']['SUCCESS'],1)
        self.assertEqual(report['gate']['selected_parents'],1)
        self.assertFalse(report['gate']['experimental_eligible'])

    def test_no_match_is_complete_and_missing_features_are_unknown(self):
        clear=build(self.data,base_direction='LONG')
        self.assertTrue(clear['source_coverage_complete'])
        self.assertEqual(clear['source_receipt']['counts']['NO_MATCH'],1)
        self.assertFalse(clear['opportunities'])
        unknown=build(fixture(unavailable=True))
        self.assertFalse(unknown['source_coverage_complete'])
        self.assertEqual(unknown['source_receipt']['counts']['UNKNOWN'],1)

    def test_known_false_condition_stays_no_match_despite_other_unknowns(self):
        result=build(self.data,candidate_key='STRICT_TRIPLE_TOTAL_65',base_direction='LONG')
        self.assertTrue(result['source_coverage_complete'])
        self.assertEqual(result['source_receipt']['counts']['NO_MATCH'],1)
        self.assertTrue(result['source_receipt']['decision_ledger'][0]['missing_features'])

    def test_raw_hash_mutation_never_becomes_no_match(self):
        self.data['source_rows'][0]['scores']['coins']['BTC']['models']['futures_flow']['score']=0
        result=build(self.data)
        self.assert_blocked(result)
        self.assertEqual(result['source_receipt']['counts']['UNKNOWN_SOURCE'],1)
        self.assertEqual(result['source_receipt']['validated_source_rows'],0)

    def test_maxpain_capture_matches_exact_average_with_versioned_normal_and_inverse_outcomes(self):
        self.data = deepcopy(self.maxpain)
        before = deepcopy(self.data)
        inverse = next(row['candidate_key'] for row in maxpain.catalog_records()
            if row['definition'].get('base_candidate_key') == MAXPAIN_KEY)
        with patch('research_watch_score_capture.prepare',side_effect=AssertionError('No score replay')), \
                patch('alert_engine._score_details_for_side',side_effect=AssertionError('No rescoring')):
            normal = build(self.data,candidate_key=MAXPAIN_KEY,base_direction='LONG')
            flipped = build(self.data,candidate_key=inverse,base_direction='LONG')
            opposite = build(self.data,candidate_key=MAXPAIN_KEY,base_direction='SHORT')
        self.assertEqual(self.data,before)
        for result,direction in ((normal,'LONG'),(flipped,'SHORT')):
            self.assertTrue(result['source_coverage_complete'])
            self.assertEqual(result['source_receipt']['counts']['MATCH'],1)
            self.assertEqual(result['opportunities'][0]['contract']['direction'],direction)
            selection = result['source_receipt']['selection']
            self.assertEqual(selection['adapter_version'],'no-horizon-accepted-watch-source-v3-maxpain')
            self.assertEqual(selection['feature_version'],maxpain.FEATURE_VERSION)
            self.assertEqual(selection['catalog_sha256'],formulas.CATALOG_SHA256)
        self.assertEqual(replay.replay_snapshot(normal)['status_counts']['FAILURE'],1)
        self.assertEqual(replay.replay_snapshot(flipped)['status_counts']['SUCCESS'],1)
        self.assertTrue(opposite['source_coverage_complete'])
        self.assertEqual(opposite['source_receipt']['counts']['NO_MATCH'],1)

    def test_maxpain_future_quote_with_valid_capture_hash_is_unknown_feature(self):
        self.data = deepcopy(self.maxpain)
        row = self.data['source_rows'][0]
        row['scores']['coins']['BTC']['sources']['maxpain_operational_rows'][0][
            'price_fetched_at_utc'] = (BASE+timedelta(minutes=6)).isoformat()
        refresh_capture(self.data)
        result = build(self.data,candidate_key=MAXPAIN_KEY,base_direction='LONG')
        self.assert_blocked(result)
        self.assertEqual(result['source_receipt']['validated_source_rows'],1)
        self.assertEqual(result['source_receipt']['counts'],
            {'MATCH':0,'NO_MATCH':0,'UNKNOWN':1,'UNKNOWN_SOURCE':0})
        self.assertIn(maxpain.AVERAGE,result['source_receipt']['decision_ledger'][0]['missing_features'])

    def test_maxpain_slot_hash_tampering_is_unknown_source(self):
        self.data = deepcopy(self.maxpain)
        self.data['source_rows'][0]['scores']['coins']['BTC']['maxpain'][1]['score'] = 0
        result = build(self.data,candidate_key=MAXPAIN_KEY,base_direction='LONG')
        self.assert_blocked(result)
        self.assertEqual(result['source_receipt']['counts']['UNKNOWN_SOURCE'],1)
        self.assertIn('CAPTURE_HASH_MISMATCH',result['source_receipt']['decision_ledger'][0]['blocker'])

    def test_expanded_source_preserves_all_legacy_decisions_on_genuine_capture(self):
        row = self.maxpain['source_rows'][0]
        observation,expanded = adapter._validate_row(row,start=self.maxpain['source_start_utc'],
            end=self.maxpain['source_end_utc'],cutoff=self.maxpain['cutoff_utc'],symbol='BTC')
        old = formulas.evaluate_coin(observation)
        old_keys = {item['candidate_key'] for item in old['evaluations']}
        self.assertEqual(len(old_keys),34)
        self.assertEqual(len(expanded['evaluations']),164)
        self.assertEqual([item for item in expanded['evaluations'] if item['candidate_key'] in old_keys],
            old['evaluations'])
        proof = expanded['maxpain_provenance_by_direction']['LONG']
        self.assertEqual((proof['source_side'],proof['denominator'],proof['rounded_average']),('SHORT',7,65.5))

    def test_missing_raw_source_preserved_in_ledger(self):
        self.data['source_rows'][0]['stored_source']=None
        result=build(self.data)
        self.assert_blocked(result)
        self.assertEqual(result['source_receipt']['decision_ledger'][0]['snapshot_set_id'],1)

    def test_missing_parent_preserves_matched_opportunity(self):
        self.data['source_rows'][0]['btc_parent']=None
        result=build(self.data)
        self.assert_blocked(result)
        self.assertEqual(len(result['opportunities']),1)
        self.assertIsNone(result['opportunities'][0]['btc_parent_movement_id'])

    def test_parent_identity_staleness_and_future_confirmation_block(self):
        for change in ({'btc_parent_movement_id':'a'*64},
                       {'observed_through_utc':BASE+timedelta(minutes=3)},
                       {'confirmed_at_utc':BASE+timedelta(minutes=8)}):
            with self.subTest(change=change):
                raw=deepcopy(self.data)
                raw['source_rows'][0]['btc_parent'].update(change)
                self.assert_blocked(build(raw))

    def test_btc_prior_invalid_prices_or_naive_time_block(self):
        for change in ({'high':1}, {'close_time_utc':(BASE+timedelta(minutes=7,milliseconds=-1)).replace(tzinfo=None).isoformat()}):
            raw=deepcopy(self.data)
            raw['source_rows'][0]['btc_prior_bar'].update(change)
            self.assert_blocked(build(raw))

    def test_missing_entry_still_counted_and_blocks_population(self):
        self.data['candles']=self.data['candles'][1:]
        result=build(self.data)
        self.assert_blocked(result)
        self.assertEqual(result['source_receipt']['counts']['MATCH'],1)
        self.assertFalse(result['opportunities'])
        self.assertEqual(result['source_receipt']['decision_ledger'][0]['blocker'],'MISSING_MATCHED_ENTRY_OPEN')

    def test_source_time_receipt_mismatch_is_unknown(self):
        self.data['source_rows'][0]['intake']['usable_from_utc']=BASE+timedelta(minutes=6)
        self.assert_blocked(build(self.data))

    def test_export_count_truncation_and_separate_transactions_block(self):
        for change in ({'expected_accepted_rows':2},{'truncated':True},{'candle_truncated':True},
                       {'transaction_mode':'SEPARATE_READ_ONLY_QUERIES'}):
            raw=deepcopy(self.data);raw['source_receipt'].update(change)
            self.assert_blocked(build(raw))

    def test_unsupported_product_candidate_and_bad_threshold_rejected(self):
        for change in ({'symbol':'HYPE'},{'candidate_key':'invented'}, {'threshold_pct':True}, {'threshold_pct':100}):
            with self.assertRaises(ValueError): build(self.data,**change)

    def test_manifest_mode_without_verified_binding_is_blocked(self):
        self.data['source_receipt']['transaction_mode']='MANIFEST_ATTESTED_MULTI_READ_V1'
        result=build(self.data)
        self.assert_blocked(result)
        self.assertIn('INVALID_MANIFEST_ATTESTED_SOURCE_EXTRACTION',
                      result['source_receipt']['source_blockers'])

    def test_route_conflict_duplicate_and_future_prices_rejected(self):
        for mutation in (lambda x:x['candles'][0].update(route='FUTURES'),
                         lambda x:x['candles'].append(deepcopy(x['candles'][-1])),
                         lambda x:x['candles'][-1].update(close_time_utc=BASE+timedelta(hours=1))):
            raw=deepcopy(self.data);mutation(raw)
            with self.assertRaises(ValueError): build(raw)

    def test_duplicate_source_does_not_inflate_coverage(self):
        self.data['source_rows'].append(deepcopy(self.data['source_rows'][0]))
        self.data['source_receipt']['expected_accepted_rows']=2
        self.assert_blocked(build(self.data))

    def test_snapshot_identity_binds_raw_source_and_candidate_policy(self):
        one=build(self.data);two=build(self.data,threshold_pct=2)
        self.assertEqual(one['dataset_id'],two['dataset_id'])
        self.assertNotEqual(one['cohort_id'],two['cohort_id'])
        self.data['source_receipt']['extraction_note']='new extraction'
        self.assertNotEqual(one['dataset_id'],build(self.data)['dataset_id'])

    def test_cli_builds_replayable_snapshot_and_cannot_overwrite(self):
        with tempfile.TemporaryDirectory() as name:
            original=Path(name)/'export.json'; output=Path(name)/'snapshot.json'
            original.write_text(formulas.canonical(self.data),encoding='utf-8')
            argv=['research_no_horizon_source.py',str(original),'--candidate','FUTURES_CVD_TOTAL_65',
                  '--base-direction','SHORT','--symbol','BTC','--threshold-pct','1.5','--output',str(output)]
            with patch('sys.argv',argv),redirect_stdout(io.StringIO()):
                self.assertEqual(adapter.main(),0)
            saved=output.read_bytes()
            self.assertEqual(replay.replay_snapshot(json.loads(saved))['status_counts']['SUCCESS'],1)
            with patch('sys.argv',argv),redirect_stderr(io.StringIO()),self.assertRaises(SystemExit) as caught:
                adapter.main()
            self.assertEqual(caught.exception.code,2)
            self.assertEqual(output.read_bytes(),saved)


if __name__=='__main__':
    unittest.main()
