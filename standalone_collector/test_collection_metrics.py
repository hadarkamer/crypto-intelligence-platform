"""Exercise redacted metrics using the source reader and real provider request builder offline."""
import importlib.util
import io
import json
import os
from contextlib import redirect_stdout
from pathlib import Path
import shutil
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from uuid import uuid4

import collection_metrics as metrics
from install_modern_map import install
import modern_map_task as modern
import test_modern_map as fixtures


class MetricsTests(unittest.TestCase):
    def test_metrics_filter_rejects_strings_unknowns_booleans_and_oversize_files(self):
        self.assertEqual(metrics.clean_numbers({'cpu_ms': 55, 'provider_requests': True,
            'session': 'secret', 'map_read_ms': '12', 'queue_wait_ms': -1}), {'cpu_ms': 55})
        with tempfile.TemporaryDirectory() as tmp, redirect_stdout(io.StringIO()) as stream:
            (Path(tmp) / 'collection-metrics.json').write_text('x' * 2500)
            metrics.emit_child(uuid4(), tmp)
        self.assertEqual(stream.getvalue(), '')

    def test_failed_read_counts_time_without_a_provider_call(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(Exception):
                with metrics.task_metrics(tmp):
                    fixtures.ReaderReplay().replay(changed=True)
            data = json.loads((Path(tmp) / 'collection-metrics.json').read_text())
            self.assertGreater(data['map_read_ms'], 0)
            self.assertEqual(data['provider_requests'], 0)
            self.assertNotIn('analysis_ms', data)

    def test_measures_reader_request_boundary_resources_and_atomic_file_save(self):
        from PIL import Image
        with tempfile.TemporaryDirectory() as tmp:
            runtime = Path(tmp) / 'runtime'
            (runtime / 'market_vision').mkdir(parents=True)
            scanner_path = runtime / 'market_vision/openai_heatmap_scanner.py'
            shutil.copyfile(Path(__file__).resolve().parents[1] / 'market_vision/openai_heatmap_scanner.py', scanner_path)
            task_path = runtime / 'collection_model1_task.py'
            task_path.write_text('def main(timeframe,run_id,output):\n    pass\n')
            install(runtime)
            spec = importlib.util.spec_from_file_location('market_vision.openai_heatmap_scanner', scanner_path)
            scanner = importlib.util.module_from_spec(spec)
            with patch.dict('sys.modules', {'requests':SimpleNamespace(post=None)}):
                spec.loader.exec_module(scanner)
            def read_fixture(symbol, model, tf, path):
                snapshot = fixtures.ReaderReplay().replay()
                Image.new('RGB', (50,50)).save(path)
                return snapshot
            audit = {'observed_symbol':'BTC','observed_model':'Model 1','observed_timeframe':'12 hour',
                'observed_mode':'Coin','readable':True,'blocking_condition':'none','short_summary':'fixture',
                'prominent_levels':[{'candidate_id':'c'+str(i),'right_edge_visible':True,
                    'strength_score':0.9} for i in range(3)]}
            response = SimpleNamespace(ok=True, json=lambda:{'output_text':json.dumps(audit)})
            env = {'COLLECTION_HEATMAP_SYMBOL':'BTC','COLLECTION_HEATMAP_MODEL':'1','OPENAI_API_KEY':'offline-test'}
            with patch.object(modern,'capture',side_effect=read_fixture), patch.object(scanner.requests,'post',return_value=response) as post, patch.dict(
                    'sys.modules',{'market_vision.openai_heatmap_scanner':scanner}), patch.dict(os.environ,env):
                path=Path(tmp)/'output'
                modern.run_modern_task('12H',str(uuid4()),path)
                self.assertEqual(post.call_count,1)
            data=json.loads((path/'collection-metrics.json').read_text())
            for field in ('map_read_ms','analysis_ms','result_write_ms','sampled_peak_rss_kib','cpu_ms','sampled_peak_processes'):
                self.assertIn(field,data)
            self.assertGreater(data['map_read_ms'],0)
            self.assertGreater(data['sampled_peak_rss_kib'],0)
            self.assertEqual(data['provider_requests'],1)
            self.assertTrue((path/'result.json').is_file())
            if os.getenv('COINGLASS_FIXTURE_PROFILE')=='1':
                print('OFFLINE_READER_MOCK_HTTP',json.dumps(data,sort_keys=True))
