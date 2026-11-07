"""Offline response-shape failures retain useful safe evidence."""
import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE/'runtime'))
import model1_execution as execution
from model1_evidence_format import save_analysis_snapshot,evidence_payload

class ResponseShapeTests(unittest.TestCase):
    def test_multiple_scans_keep_count_without_untrusted_content(self):
        def failed(*args):
            execution.stage('validation')
            execution.remember_analysis({'scans':[{'user':'PRIVATE'},{'user':'PRIVATE'}]})
            raise execution.StageFailure('analysis_response_invalid')
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(execution.run_task(failed,'12H','job',directory),1)
            report=json.loads((Path(directory)/'error.json').read_text())
            self.assertEqual(report['scan_count'],2)
            with contextlib.redirect_stdout(io.StringIO()) as output:
                execution.read_failure(Path(directory)/'error.json','job',1)
            self.assertIn('"scan_count": 2',output.getvalue())
            self.assertNotIn('PRIVATE',json.dumps(report)+output.getvalue())
    def test_single_scan_reports_new_contract_presence(self):
        def failed(*args):
            execution.stage('validation')
            execution.remember_analysis({'scans':[{'readable':True,'prominent_levels':[]}]})
            raise execution.StageFailure('zones_invalid')
        with tempfile.TemporaryDirectory() as directory:
            execution.run_task(failed,'12H','job',directory)
            report=json.loads((Path(directory)/'error.json').read_text())
            self.assertEqual(report['scan_count'],1)
            self.assertTrue(report['prominent_levels_present'])
            self.assertTrue(report['readable'])
    def test_snapshot_preserves_multiple_scan_count(self):
        with tempfile.TemporaryDirectory() as directory,patch('price_detail_input.detail_provenance',return_value=None):
            save_analysis_snapshot({'scans':[{'user':'PRIVATE'},{'user':'PRIVATE'}]},directory,{})
            _,diagnostic=evidence_payload(directory,'12H')
            self.assertEqual(diagnostic['scan_count'],2)
            self.assertIsNone(diagnostic['analysis']['current_price_estimate'])
            self.assertNotIn('prominent_levels_present',diagnostic)
            self.assertNotIn('PRIVATE',json.dumps(diagnostic))
    def test_empty_scans_count_is_retained_and_not_accepted(self):
        with tempfile.TemporaryDirectory() as directory,patch('price_detail_input.detail_provenance',return_value=None):
            save_analysis_snapshot({'scans':[]},directory,{})
            _,diagnostic=evidence_payload(directory,'24H')
            self.assertEqual(diagnostic['scan_count'],0)
            self.assertFalse(diagnostic['assessment_validated'])

if __name__=='__main__':unittest.main()
