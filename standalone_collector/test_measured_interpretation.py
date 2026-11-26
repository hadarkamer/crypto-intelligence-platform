"""Offline replay of the measured BTC entry point with a transport-free analyst."""
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import modern_map_task as task
from model1_execution import StageFailure
from test_modern_map import FIXTURE, frozen_result


class MeasuredInterpretation(unittest.TestCase):
    def setUp(self):
        self.result = frozen_result()
        self.snapshot = {k:v for k,v in self.result['measure_snapshot'].items()
                         if k not in ('sha256', 'artifact_id')}
        self.overview = dict(source_time=self.snapshot['source_time'], raw_total='1.29B',
                             raw_above_pct='64%', raw_below_pct='36%',
                             internal_marker='must-remain-in-process')
        self.job = self.result['run_id']
        self.calls = 0

    def capture(self, symbol, model, timeframe, image_path, observe):
        self.assertEqual((symbol,model,timeframe),('BTC',1,'12H'))
        image_path.write_bytes(FIXTURE.read_bytes())
        observe('overview',self.overview)
        return self.snapshot

    def analyze(self, images, **options):
        self.calls += 1
        root = Path(images[0]['image']).parent
        self.assertEqual(json.loads((root/'measured-source.json').read_text())['snapshot'],self.snapshot)
        self.assertEqual(options['evidence_schema']['required'],['short_summary'])
        self.assertNotIn(self.overview['internal_marker'],options['evidence_instructions'])
        self.assertIn('1.29B',options['evidence_instructions'])
        return dict(model='mock',short_summary='ריכוז הנזילות מעל למחיר גבוה יותר.')

    def test_source_and_interpretation_replayed_after_write_failure(self):
        def fail_final(path,data):
            if path.name == 'result.json':
                raise OSError('simulated final write failure')
            return original_write(path,data)
        original_write = task._write_once
        with TemporaryDirectory() as directory, patch.object(task, 'capture', side_effect=self.capture) as capture:
            root = Path(directory)
            with patch.object(task,'_write_once',side_effect=fail_final), self.assertRaises(OSError):
                task._run_measured_btc(root,self.job,self.analyze)
            self.assertTrue((root/'measured-interpretation.json').is_file())
            for _ in range(2):
                task._run_measured_btc(root,self.job,self.analyze)
            self.assertEqual((capture.call_count,self.calls),(1,1))
            result = json.loads((root/'result.json').read_text())
            self.assertIn('1.29B',result['summary'])
            self.assertIn('פרשנות',result['summary'])
            self.assertEqual(result['measure_snapshot']['groups'],self.snapshot['groups'])

    def test_indeterminate_transport_is_not_repeated_and_source_is_kept(self):
        def disconnected(*args,**kwargs):
            self.calls += 1
            raise TimeoutError
        with TemporaryDirectory() as directory, patch.object(task,'capture',side_effect=self.capture) as cap:
            root=Path(directory)
            with self.assertRaises(TimeoutError):
                task._run_measured_btc(root,self.job,disconnected)
            with self.assertRaises(StageFailure):
                task._run_measured_btc(root,self.job,disconnected)
            self.assertEqual((cap.call_count,self.calls),(1,1))
            self.assertTrue((root/'measured-source.json').is_file())
            self.assertFalse((root/'result.json').exists())

    def test_model_cannot_supply_amounts(self):
        raw=dict(model='mock',short_summary='נוספו 150 מיליון.')
        with self.assertRaises(StageFailure):
            task.normalized_summary(raw,self.snapshot,self.job,FIXTURE.read_bytes())
        raw=dict(model='mock',short_summary='מעל המחיר.',prominent_levels=[])
        with self.assertRaises(StageFailure):
            task.normalized_summary(raw,self.snapshot,self.job,FIXTURE.read_bytes())


if __name__ == '__main__':
    unittest.main()
