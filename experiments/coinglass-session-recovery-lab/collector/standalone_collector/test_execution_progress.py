"""Private stage breadcrumbs survive a deadline before any screenshot exists."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE/'runtime'))
import model1_execution as execution
import heatmap_models as models
from model1_evidence_format import evidence_payload


class ExecutionProgressTests(unittest.TestCase):
    def test_active_operation_is_available_before_completion_without_png(self):
        seen=[]
        def main(timeframe,job_id,output):
            execution.stage('capture')
            with execution.capture_operation('navigation'):
                image,diagnostic=evidence_payload(output,timeframe)
                seen.append(diagnostic['execution_progress'])
                self.assertIsNone(image);self.assertFalse(diagnostic['assessment_validated'])
            execution.stage('image_check')
        with tempfile.TemporaryDirectory() as directory,patch.object(models,'HEATMAP_MODEL',3):
            self.assertEqual(execution.run_task(main,'12H','synthetic',directory),0)
            self.assertEqual(seen[0]['stage'],'capture');self.assertEqual(seen[0]['capture_phase'],'navigation')
            image,diagnostic=evidence_payload(directory,'12H')
            progress=diagnostic['execution_progress']
            self.assertIsNone(image);self.assertFalse(diagnostic['image_retained'])
            self.assertEqual(progress['stage'],'image_check');self.assertIsNone(progress['capture_phase'])
            self.assertEqual(progress['heatmap_model'],3);self.assertEqual(progress['timeframe'],'12H')
            self.assertEqual(progress['capture_operations'][0]['phase'],'navigation')
            self.assertEqual(progress['capture_operations'][0]['status'],'completed')
            self.assertIsNone(execution._progress_path)

    def test_failed_operation_retains_original_error_and_fixed_progress(self):
        def main(timeframe,job_id,output):
            execution.stage('capture')
            with execution.capture_operation('navigation'):raise TimeoutError('SECRET URL COOKIE')
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(execution.run_task(main,'12H','synthetic',directory),1)
            report=json.loads((Path(directory)/'error.json').read_text())
            self.assertEqual(report['code'],'source_timeout')
            image,diagnostic=evidence_payload(directory,'12H')
            self.assertIsNone(image)
            self.assertEqual(diagnostic['execution_progress']['capture_operations'][0]['status'],'failed')
            self.assertNotIn('SECRET',json.dumps(diagnostic))

    def test_progress_metadata_discards_unknown_strings_and_bounds_events(self):
        event={'phase':'navigation','status':'completed','duration_seconds':1}
        value={'format_version':'execution-progress.v1','stage':'capture','capture_phase':'SECRET',
            'elapsed_seconds':2,'secret':'SECRET','capture_operations':[event]*30+[
                {'phase':'PRIVATE','status':'completed','duration_seconds':1},
                {'phase':'navigation','status':'SECRET','duration_seconds':1}]}
        cleaned=execution.safe_progress(value)
        self.assertIsNone(cleaned['capture_phase']);self.assertEqual(len(cleaned['capture_operations']),22)
        self.assertNotIn('SECRET',json.dumps(cleaned));self.assertNotIn('PRIVATE',json.dumps(cleaned))
        value['elapsed_seconds']=float('nan');self.assertIsNone(execution.safe_progress(value))
        value['stage']=[];self.assertIsNone(execution.safe_progress(value))

    def test_invalid_or_symlink_sidecar_cannot_be_retained(self):
        with tempfile.TemporaryDirectory() as directory:
            progress=Path(directory)/'execution_progress.json'
            progress.write_text(json.dumps({'stage':'SECRET'}))
            self.assertNotIn('execution_progress',evidence_payload(directory,'12H')[1])
            progress.write_text('x'*8193)
            self.assertNotIn('execution_progress',evidence_payload(directory,'12H')[1])
            progress.write_text('{}')
            with patch.object(Path,'is_symlink',return_value=True):
                self.assertNotIn('execution_progress',evidence_payload(directory,'12H')[1])

    def test_progress_write_failure_does_not_change_task_success(self):
        def main(*args):execution.stage('capture')
        with tempfile.TemporaryDirectory() as directory,patch.object(Path,'write_text',side_effect=OSError('SECRET')):
            self.assertEqual(execution.run_task(main,'12H','synthetic',directory),0)
        self.assertIsNone(execution._progress_path)


if __name__=='__main__':unittest.main()
