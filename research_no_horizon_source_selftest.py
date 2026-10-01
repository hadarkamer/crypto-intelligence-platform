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
from research_watch_scan_intake_selftest import source_fixture
from research_watch_score_capture_selftest import BASE, bundle, derivatives


def fixture(*, unavailable=False):
    market = derivatives()
    for item in market.values():
        for window in item['flow']['futures']['windows'].values():
            window['continuous_strength'] = 1.
        if unavailable:
            item['flow']['futures'].update(available=False,windows={},quality={'status':'NO_DATA'})
    block,*_ = bundle(snapshot=market)
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


def build(export,**changes):
    return adapter.build_snapshot(export,**{'candidate_key':'FUTURES_CVD_TOTAL_65',
        'base_direction':'SHORT','symbol':'BTC','threshold_pct':1.5,**changes})


class SourceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original=fixture()

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
