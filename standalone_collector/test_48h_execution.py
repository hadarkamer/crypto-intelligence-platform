"""Synthetic fixtures only, no external services or real credentials."""
import contextlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch
from uuid import uuid4

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE/'runtime'))
import collection_bridge as bridge
import collection_model1_task as task
import model1_execution as execution
from market_vision import coinglass_heatmap_capture as capture
from market_vision import openai_heatmap_scanner as scanner
from test_original_flow import sample


def sample48():
    value=sample()
    value['scans'][0].update(timeframe='48h',observed_timeframe='48h')
    return value

class TimeframeTests(unittest.TestCase):
    def test_api_accepts48h_not_unknown(self):
        rid=str(uuid4())
        self.assertEqual(bridge.request_fields({'request_id':rid,'timeframe':'48H'}),(rid,'48H'))
        with self.assertRaises(bridge.BridgeError):bridge.request_fields({'request_id':rid,'timeframe':'72H'})
    def test_selector_clicks_actual48hour(self):
        page=Mock();trigger=Mock();option=Mock();selected=Mock()
        with patch.object(capture,'_first_visible',side_effect=[None,trigger,option,selected]):
            capture._select_timeframe(page,'48h')
        pattern=page.get_by_role.call_args.kwargs['name']
        self.assertIsNotNone(pattern.fullmatch('48 hour'));self.assertIsNone(pattern.fullmatch('24 hour'))
        trigger.click.assert_called_once();option.click.assert_called_once()
    def test_unknown_selector_does_not_fall_back24(self):
        page=Mock()
        with self.assertRaises(ValueError):capture._select_timeframe(page,'72h')
        page.locator.assert_not_called()
    def test_missing48_option_fails_instead_of_relabel(self):
        page=Mock();trigger=Mock()
        with patch.object(capture,'_first_visible',side_effect=[None,trigger,None,None]):
            with self.assertRaises(RuntimeError):capture._select_timeframe(page,'48h')
    def test_model_schema_supports_observed48(self):
        schema=scanner.HEATMAP_SCHEMA['properties']['scans']['items']
        self.assertIn('48h',schema['properties']['observed_timeframe']['enum'])
    def test_normalize48_preserves_identity(self):
        out=task.normalize(sample48(),timeframe='48H',run_id=str(uuid4()),captured_at='2026-01-01T00:00:00Z',image=b'fixture')
        self.assertEqual(out['timeframe'],'48H')
    def test_24image_cannot_be_saved_as48(self):
        raw=sample48();raw['scans'][0]['observed_timeframe']='24h'
        with self.assertRaises(execution.StageFailure) as ctx:
            task.normalize(raw,timeframe='48H',run_id=str(uuid4()),captured_at='2026-01-01T00:00:00Z',image=b'fixture')
        self.assertEqual(ctx.exception.code,'screenshot_identity_mismatch')
    def test48_job_uses_one_analysis_only_on48(self):
        calls=[]
        def cap(directory,*,timeframes):
            calls.append(('capture',timeframes));directory.mkdir();out=[]
            for tf in timeframes:
                p=directory/f'coinglass_btc_heatmap_{tf}.png';p.write_bytes(b'\x89PNG\r\n\x1a\nfixture')
                out.append({'image':str(p),'timeframe':tf})
            return out
        def analyze(images,**kwargs):
            calls.append(('analysis',tuple(x['timeframe'] for x in images)));return sample48()
        fake=types.SimpleNamespace(capture_heatmaps=cap,COINGLASS_HEATMAP_URL=task.SOURCE)
        with tempfile.TemporaryDirectory() as d,patch.dict(sys.modules,{'market_vision.coinglass_heatmap_capture':fake}),patch.object(scanner,'analyze_heatmap_images',side_effect=analyze):
            task.main('48H',str(uuid4()),d)
            self.assertEqual(json.loads((Path(d)/'result.json').read_text())['timeframe'],'48H')
        self.assertEqual(calls,[('capture',('12h','24h','48h')),('analysis',('48h',))])
    def test_existing_db_constraint_is_extended_not_table_dropped(self):
        conn=Mock();conn.__enter__=Mock(return_value=conn);conn.__exit__=Mock(return_value=False)
        conn.execute.return_value.fetchone.return_value={'definition':"CHECK(timeframe IN ('12H','24H'))"}
        store=bridge.JobStore('synthetic')
        with patch.object(store,'connect',return_value=conn):store.initialize()
        sql=' '.join(str(c.args[0]) for c in conn.execute.call_args_list)
        self.assertIn('ALTER TABLE public.ai_collection_bridge_jobs',sql)
        self.assertIn("'48H'",sql);self.assertNotIn('DROP TABLE',sql);self.assertNotIn('DELETE FROM',sql)
    def test48_migration_does_not_repeat_if_already_applied(self):
        conn=Mock();conn.__enter__=Mock(return_value=conn);conn.__exit__=Mock(return_value=False)
        conn.execute.return_value.fetchone.return_value={'definition':"CHECK(timeframe IN ('12H','24H','48H'))"}
        store=bridge.JobStore('synthetic')
        with patch.object(store,'connect',return_value=conn):store.initialize()
        self.assertFalse(any('ALTER TABLE' in str(c.args[0]) for c in conn.execute.call_args_list))

class FailureTests(unittest.TestCase):
    def test_incomplete_model_response_is_not_parsed_or_success(self):
        response=Mock(ok=True);response.json.return_value={'status':'incomplete','usage':{'output_tokens':3500}}
        with self.assertRaises(execution.StageFailure) as ctx:execution.accept_model_response(response)
        self.assertEqual(ctx.exception.code,'analysis_incomplete')
    def test_http_auth_and_quota_are_specific(self):
        for status,expected in [(401,'analysis_unauthorized'),(429,'analysis_rate_limited'),(500,'analysis_http_error')]:
            response=Mock(ok=False,status_code=status);response.json.return_value={}
            with self.subTest(status=status),self.assertRaises(execution.StageFailure) as ctx:execution.accept_model_response(response)
            self.assertEqual(ctx.exception.code,expected)
        response=Mock(ok=False,status_code=429);response.json.return_value={'error':{'code':'insufficient_quota'}}
        with self.assertRaises(execution.StageFailure) as ctx:execution.accept_model_response(response)
        self.assertEqual(ctx.exception.code,'analysis_quota_exceeded')
    def test_safe_failure_keeps_stage_and_never_raw_secret(self):
        def fail(*args):
            execution.stage('capture');raise RuntimeError('PRIVATE_TOKEN fixture')
        with tempfile.TemporaryDirectory() as d,contextlib.redirect_stdout(io.StringIO()) as out:
            self.assertEqual(execution.run_task(fail,'48H',str(uuid4()),d),1)
            result=json.loads((Path(d)/'error.json').read_text())
            code=execution.read_failure(Path(d)/'error.json',str(uuid4()),1)
            self.assertEqual(code,'source_capture_failed');self.assertEqual(result['stage'],'capture')
            self.assertNotIn('PRIVATE_TOKEN',json.dumps(result)+out.getvalue())
    def test_price_uncertain_is_distinguished_from_source(self):
        def fail(*args):
            execution.stage('validation');raise ValueError('Current price is not readable with sufficient confidence')
        with tempfile.TemporaryDirectory() as d:
            execution.run_task(fail,'12H',str(uuid4()),d)
            self.assertEqual(json.loads((Path(d)/'error.json').read_text())['code'],'price_uncertain')
    def test_crashed_child_still_has_a_code(self):
        with tempfile.TemporaryDirectory() as d,contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(execution.read_failure(Path(d)/'absent.json',str(uuid4()),-9),'worker_crashed')
    def test_success_has_no_error_and_no_retry(self):
        main=Mock()
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(execution.run_task(main,'48H',str(uuid4()),d),0)
            self.assertFalse((Path(d)/'error.json').exists())
        main.assert_called_once()

if __name__=='__main__':unittest.main()
