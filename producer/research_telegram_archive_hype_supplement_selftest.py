"""Behavioral HYPE MARK supplement, provenance, resume and Spot isolation gates."""
from copy import deepcopy
from datetime import timedelta
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import research_telegram_archive_hype_supplement as hype
import research_telegram_archive_runtime_importer as importer
from research_telegram_archive_backfill import calculate_event as calculate_spot, canonical
from research_telegram_archive_features import extract_message
from research_telegram_archive_reconstruction_selftest import source, MANIFEST, ENTRY, cache
from research_telegram_archive_runtime_importer_selftest import artifact, ArchivePGFixture

NOW = ENTRY + timedelta(days=2)


def original_event(index=1):
    raw = source('#1 HYPE / 24h | 🔴 SHORT | 78\nמחיר נוכחי: $88')
    raw.update(source_identity_key=f'hype-source-{index}', source_message_id=f'hype-{index}')
    event = extract_message(raw, MANIFEST)
    event, labels, metrics = calculate_spot(event, cache(), [], observed_at=NOW)
    assert event['calculation_status'] == 'UNSUPPORTED_SPOT_SYMBOL' and not labels and not metrics
    event.update(membership_status='BOUNDARY_UNVERIFIED', btc_parent_movement_id='retained-parent',
        parent_evidence_eligible=False, parent_start_time_utc=(ENTRY-timedelta(hours=2)).isoformat())
    return event


def mark_bars():
    return {bar['open_time_utc']: hype._validated_bar(bar) for bar in cache().bars['BTC']}


def write_cache(path, bars):
    path.write_text(canonical({'price_source_contract': hype.SOURCE})+'\n'+
        ''.join(canonical(bar)+'\n' for bar in bars.values()), encoding='utf-8')


class HypeSupplementTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.base = self.root/'original.sqlite'
        self.base_run = artifact(self.base)
        self.originals = [original_event(1), original_event(2)]
        with sqlite3.connect(self.base) as conn:
            conn.executemany('INSERT INTO archive_reconstructed_events VALUES (?,?,?,?,?,?,?)',
                [(self.base_run, event['archive_event_key'],event['source_message_time_utc'],'HYPE',
                  event['reconstruction_status'],event['calculation_status'],canonical(event)) for event in self.originals])
        self.sha = importer.file_sha256(self.base)
        self.output = self.root/'supplement'
        self.output.mkdir()
        self.bars = mark_bars()
        write_cache(self.output/'hype_futures_mark_1m.jsonl', self.bars)

    def tearDown(self):
        self.temp.cleanup()

    def run_supplement(self, limit=1):
        return hype.run(source_artifact=self.base, expected_source_sha256=self.sha,
            base_run_key=self.base_run, output_dir=self.output, observed_at=NOW,
            event_limit=limit, allow_network=False, max_fetches=0)

    def test_mark_open_replaces_only_measurement_and_retests_both_directions(self):
        event = hype.derive_event(self.originals[0], base_run_key=self.base_run)
        measured, labels, metrics = hype.calculate_event(event, self.bars, observed_at=NOW)
        for key in ('features','original_printed_price','analysis_direction','source_message_time_utc',
                    'membership_status','btc_parent_movement_id','parent_evidence_eligible','parent_start_time_utc'):
            self.assertEqual(measured[key],self.originals[0][key])
        self.assertEqual(measured['entry_price'],100)
        self.assertEqual(measured['original_printed_price'],88)
        self.assertEqual(measured['entry_price_source'],'BINANCE_FUTURES_MARK_1M_OPEN')
        self.assertEqual(measured['calculation_status'],'COMPLETE_64_LABELS')
        self.assertEqual(len({label['outcome_id'] for label in labels}),64)
        self.assertEqual(len(metrics),8)
        self.assertTrue(all(label['status']=='SUCCESS' for label in labels if label['signal_variant']=='NORMAL'))
        self.assertTrue(all(label['status']=='FAILURE' for label in labels if label['signal_variant']=='INVERSE'))
        self.assertTrue(all(label['source_price_market']=='futures' and label['source_price_kind']=='MARK' for label in labels))
        self.assertTrue(all(metric['status']=='READY' and metric['source']==hype.SOURCE for metric in metrics))

    def test_missing_tail_never_claims_full_window_and_wrong_entry_cannot_price(self):
        event = hype.derive_event(self.originals[0],base_run_key=self.base_run)
        partial = dict(self.bars)
        partial.pop(max(partial))
        measured, labels, metrics = hype.calculate_event(event,partial,observed_at=NOW)
        self.assertEqual(measured['calculation_status'],'RETRY_INCOMPLETE_PATH')
        self.assertTrue(all(row['status']=='DATA_MISSING' for row in labels if row['window_minutes']==1440))
        self.assertTrue(all(row['status']=='DATA_MISSING' and row['mfe_pct'] is None for row in metrics if row['window_minutes']==1440))
        partial[ENTRY] = self.bars[ENTRY+timedelta(minutes=1)]
        with self.assertRaisesRegex(ValueError,'entry candle time'):
            hype.calculate_event(event,partial,observed_at=NOW)

    def test_supplement_resumes_and_imports_as_new_run_preserving_original_spot_rows(self):
        first, second, third = self.run_supplement(),self.run_supplement(),self.run_supplement()
        self.assertEqual([first['processed_this_invocation'],second['processed_this_invocation'],third['processed_this_invocation']],[1,1,0])
        self.assertEqual(first['run_key'],third['run_key'])
        self.assertNotEqual(third['run_key'],self.base_run)
        self.assertEqual(third['calculation_status_counts'],{'COMPLETE_64_LABELS':2})
        self.assertEqual(importer.file_sha256(self.base),self.sha)
        pg = ArchivePGFixture()
        importer.import_artifact(pg,self.base,expected_sha256=self.sha,expected_run_key=self.base_run)
        old = deepcopy(pg.tables)
        path = Path(third['sqlite_artifact'])
        report = importer.import_artifact(pg,path,expected_sha256=third['sqlite_sha256'],expected_run_key=third['run_key'])
        self.assertEqual(report['inserted_by_table']['research_archive_reconstructed_events'],2)
        self.assertEqual(report['inserted_by_table']['research_archive_delayed_entry_outcomes'],128)
        self.assertEqual(report['inserted_by_table']['research_archive_common_window_metrics'],16)
        for table,records in old.items():
            for key,row in records.items():self.assertEqual(pg.tables[table][key],row)
        repeated = importer.import_artifact(pg,path,expected_sha256=third['sqlite_sha256'],expected_run_key=third['run_key'],trust_atomic_resume=True)
        self.assertTrue(all(count==0 for count in repeated['inserted_by_table'].values()))
        self.assertFalse(report['live_union_eligible'])
        self.assertEqual(report['live_formula_statuses_changed'],0)

    def test_importer_rejects_mixed_sources_wrong_entry_and_inverse_metric(self):
        report = self.run_supplement(limit=2)
        with sqlite3.connect(report['sqlite_artifact']) as conn:
            conn.row_factory=sqlite3.Row
            row=dict(conn.execute('SELECT * FROM archive_reconstructed_events LIMIT 1').fetchone())
            metric=dict(conn.execute("SELECT * FROM archive_common_window_metrics WHERE event_key=? AND signal_variant='INVERSE' LIMIT 1",(row['event_key'],)).fetchone())
            outcome=dict(conn.execute('SELECT * FROM archive_delayed_entry_outcomes WHERE event_key=? LIMIT 1',(row['event_key'],)).fetchone())
        contract={key:report[key] for key in hype.CONTRACT_KEYS}
        event=importer._event_record(row,contract=contract)
        with self.assertRaises(ValueError):importer._event_record(row)  # Futures is never accepted as original Spot.
        for field,value in (('entry_price_source','BINANCE_SPOT_1M_OPEN'),('entry_time_utc',(ENTRY+timedelta(minutes=1)).isoformat())):
            bad=deepcopy(row);payload=json.loads(bad['event_json']);payload[field]=value;bad['event_json']=canonical(payload)
            with self.assertRaises(ValueError):importer._event_record(bad,contract=contract)
        for field,value in (('source_price_market','spot'),('reference_price',88),('measurement_start_utc',(ENTRY+timedelta(minutes=1)).isoformat())):
            bad=deepcopy(outcome);payload=json.loads(bad['outcome_json']);payload[field]=value;bad['outcome_json']=canonical(payload)
            with self.assertRaises(ValueError):importer._outcome_record(bad,event)
        bad=deepcopy(metric);payload=json.loads(bad['metrics_json']);payload['direction']=event['event_payload']['analysis_direction'];bad['metrics_json']=canonical(payload)
        with self.assertRaises(ValueError):importer._metric_record(bad,event)
        changed={**contract,'price_source':{**hype.SOURCE,'market':'spot'}}
        with self.assertRaises(ValueError):importer.validate_contract(changed,hype.run_identity(changed))

    def test_partial_fetch_is_bounded_and_retry_fills_holes_without_replacing_prices(self):
        path=self.root/'cache.jsonl'
        end=ENTRY+timedelta(minutes=1441)
        calls=[]
        def fetch(symbol,start,stop):
            calls.append((symbol,start,stop))
            return {**hype.SOURCE,'candles':[self.bars[ENTRY]]}
        with patch.object(hype.provider,'fetch_closed_candles',side_effect=fetch):
            bars,count,failures=hype.fill_cache(path,start=ENTRY,end=end,allow_network=True,max_fetches=1)
        self.assertEqual(count,1)
        self.assertEqual(calls,[('HYPE',ENTRY,ENTRY+timedelta(days=1))])
        self.assertEqual(len(bars),1)
        self.assertEqual(failures,[])
        def resumed(symbol,start,stop):
            calls.append((symbol,start,stop))
            return {**hype.SOURCE,'candles':[bar for opened,bar in self.bars.items() if start<=opened<stop]}
        with patch.object(hype.provider,'fetch_closed_candles',side_effect=resumed):
            bars,count,failures=hype.fill_cache(path,start=ENTRY,end=end,allow_network=True,max_fetches=1)
        self.assertEqual(calls[-1][1],ENTRY+timedelta(minutes=1))
        self.assertEqual(len(bars),1440)
        self.assertEqual(bars[ENTRY]['open'],100)
        write_cache(path,{ENTRY:self.bars[ENTRY]})
        altered=path.read_text().replace('"market":"futures"','"market":"spot"')
        path.write_text(altered)
        with self.assertRaises(ValueError):hype.load_cache(path)


if __name__=='__main__':
    unittest.main()
