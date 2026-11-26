"""The normal BTC numerical task never imports a model, even with no model credentials."""
from copy import deepcopy
from hashlib import sha256
import json
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from uuid import uuid4

import modern_map_task as task
from model1_execution import StageFailure
from symbol_jobs import valid_result
from test_modern_map import FIXTURE
from test_modern_map_basic_numbers import live_sample


class NumericPublication(unittest.TestCase):
    def setUp(self):
        self.job = str(uuid4())
        self.image = FIXTURE.read_bytes()
        self.snapshot = live_sample()
        self.snapshot['source_sha256'] = sha256(self.image).hexdigest()

    def capture(self,symbol,model,timeframe,image_path,observe):
        self.assertEqual((symbol,model,timeframe),('BTC',1,'12H'))
        image_path.write_bytes(self.image)
        return deepcopy(self.snapshot)

    def test_normal_entry_reuses_saved_source_without_model(self):
        env = {'COLLECTION_HEATMAP_SYMBOL':'BTC','COLLECTION_HEATMAP_MODEL':'1'}
        with (TemporaryDirectory() as tmp,patch.dict(os.environ,env,clear=True),
              patch.dict(sys.modules,{'market_vision.openai_heatmap_scanner':None}),
              patch.object(task,'capture',side_effect=self.capture) as capture):
            root = Path(tmp)
            original = task._write_once
            def fail_publish(path, data):
                if path.name=='result.json':
                    self.assertTrue((root/'measured-source.json').is_file())
                    raise OSError('interrupted publication')
                return original(path,data)
            with patch.object(task,'_write_once',side_effect=fail_publish), self.assertRaises(OSError):
                task.run_modern_task('12H',self.job,root)
            for _ in range(2):
                task.run_modern_task('12H',self.job,root)
            self.assertEqual(capture.call_count,1)
            result = json.loads((root/'result.json').read_text())
            self.assertEqual(result['basic_snapshot'],self.snapshot)
            self.assertTrue(valid_result(result,self.image,self.job,'12H',1,'BTC',True))
            self.assertFalse({'zones','provider_model','model','usage','summary','measure_snapshot'} & set(result))
            self.assertFalse((root/'measured-analysis-started.json').exists())
            self.assertFalse((root/'measured-interpretation.json').exists())

    def test_live_evidence_job_and_values_cannot_be_swapped(self):
        result = task.numeric_result(self.snapshot,self.job,self.image)
        self.assertFalse(valid_result(result,self.image,str(uuid4()),'12H',1,'BTC',True))
        self.assertFalse(valid_result(result,self.image,self.job,'24H',1,'BTC',True))
        self.assertFalse(valid_result(result,self.image,self.job,'12H',1,'BTC',False))
        self.assertFalse(valid_result(dict(result,model='fabricated'),self.image,self.job,'12H',1,'BTC',True))
        bad = deepcopy(result)
        bad['basic_snapshot']['levels'][0]['amount'] += 1
        self.assertFalse(valid_result(bad,self.image,self.job,'12H',1,'BTC',True))
        with self.assertRaises(StageFailure):
            task.numeric_result(self.snapshot,self.job,self.image+b'wrong')

    def test_reconstruction_cannot_become_live_even_with_valid_hash(self):
        from modern_map_basic_numbers import build_basic_snapshot
        from test_modern_map_basic_numbers import live_inputs
        market,args = live_inputs()
        end = args['end_observation']
        args.update(provenance='retained_reconstruction',column_time_after=None,source_sha256=self.snapshot['source_sha256'],
                    end_observation={k:end[k] for k in ('source_time','raw_total','raw_above_total','raw_below_total')})
        with self.assertRaises(StageFailure):
            task.numeric_result(build_basic_snapshot(market,**args),self.job,self.image)


if __name__ == '__main__':
    unittest.main()
