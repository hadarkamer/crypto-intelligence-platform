"""Offline tests of the prepared modules used by actual collector jobs."""
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch
HERE=Path(__file__).resolve().parent
RUNTIME=HERE/'runtime'
sys.path.insert(0,str(RUNTIME))
from market_vision import openai_heatmap_scanner as scanner
import collection_model1_task as task
from install_original_flow import expand_capture
from install_price_detail import expand_detail_capture
from model1_legend import expand_legend_capture
from model1_execution import StageFailure
ERRORS=(ValueError,StageFailure)

def sample():
    def zone(lo,hi):return {'low_price':lo,'high_price':hi,'relative_strength':'strong','confidence':'high'}
    return {'symbol':'BTC','analysis_mode':'visual_screenshot','model':'synthetic-model',
      'usage':{'input_tokens':1,'output_tokens':1},'scans':[{
        'timeframe':'12h','readable':True,'observed_symbol':'BTC','observed_mode':'Symbol',
        'observed_model':'Model 1','observed_timeframe':'12h','blocking_condition':'none',
        'current_price_estimate':100,'current_price_confidence':'high','short_summary':'synthetic',
        'above_price':{'main_zone':zone(110,115),'secondary_zones':[]},
        'below_price':{'main_zone':zone(85,90),'secondary_zones':[]}}]}

def normalize(raw=None):
    return task.normalize(sample() if raw is None else raw,timeframe='12H',
        run_id='00000000-0000-4000-8000-000000000001',captured_at='2026-01-01T00:00:00Z',image=b'synthetic')

class OriginalFlowTests(unittest.TestCase):
    def test_runtime_has_only_reviewed_capture_adaptation(self):
        original=(HERE.parent/'market_vision/coinglass_heatmap_capture.py').read_text()
        expected=expand_legend_capture(expand_detail_capture(expand_capture(original)))
        self.assertEqual((RUNTIME/'market_vision/coinglass_heatmap_capture.py').read_text(),expected)
    def test_observed_schema_is_strict_and_required(self):
        schema=scanner.HEATMAP_SCHEMA['properties']['scans']['items']
        for name in ('readable','observed_symbol','observed_mode','observed_model','observed_timeframe','blocking_condition'):
            self.assertIn(name,schema['properties']);self.assertIn(name,schema['required'])
        self.assertFalse(schema['additionalProperties'])
    def test_missing_observed_evidence_rejected(self):
        value=sample();value['scans'][0].pop('observed_timeframe')
        with self.assertRaises(ERRORS):normalize(value)
    def test_blurred_image_never_saved(self):
        for condition in ('blur','loading','login','challenge','unknown'):
            value=sample();value['scans'][0]['blocking_condition']=condition
            with self.subTest(condition=condition),self.assertRaises(ERRORS):normalize(value)
    def test_wrong_identity_is_rejected(self):
        for name,value in (('observed_model','other'),('observed_mode','Pair'),('observed_timeframe','24h'),('observed_symbol','other'),('readable',False)):
            raw=sample();raw['scans'][0][name]=value
            with self.subTest(name=name),self.assertRaises(ERRORS):normalize(raw)
    def test_numeric_confidence_not_weakened(self):
        raw=sample();raw['scans'][0]['current_price_confidence']='medium'
        with self.assertRaises(ValueError):normalize(raw)
    def test_invalid_numeric_side_not_weakened(self):
        raw=sample();raw['scans'][0]['above_price']['main_zone']['low_price']=99
        with self.assertRaises(ValueError):normalize(raw)
    def test_valid_result_keeps_usage_and_source(self):
        value=normalize();self.assertEqual(value['usage'],{'input_tokens':1,'output_tokens':1})
        self.assertEqual(value['provider'],'OpenAI');self.assertEqual(len(value['zones']),2)
    def test_only_requested_horizon_is_captured_and_analyzed(self):
        calls=[]
        def cap(directory,*,timeframes):
            calls.append(('capture',timeframes));directory.mkdir();out=[]
            for tf in timeframes:
                p=directory/f'coinglass_btc_heatmap_{tf}.png';p.write_bytes(b'\x89PNG\r\n\x1a\nsynthetic')
                os.utime(p,(1767225600,1767225600));out.append({'image':str(p),'timeframe':tf})
            return out
        def analyze(images,**kwargs):
            calls.append(('analysis',tuple(x['timeframe'] for x in images)));return sample()
        fake=types.SimpleNamespace(capture_heatmaps=cap,COINGLASS_HEATMAP_URL=task.SOURCE)
        with tempfile.TemporaryDirectory() as root,patch.dict(sys.modules,{'market_vision.coinglass_heatmap_capture':fake}),patch.object(scanner,'analyze_heatmap_images',side_effect=analyze):
            task.main('12H','00000000-0000-4000-8000-000000000001',root)
            self.assertEqual(json.loads((Path(root)/'result.json').read_text())['captured_at'],'2026-01-01T00:00:00Z')
        self.assertEqual(calls,[('capture',('12h',)),('analysis',('12h',))])
    def test_model_call_is_bounded_and_usage_retained(self):
        raw=sample();raw.pop('model');raw.pop('usage');response=Mock(ok=True)
        response.json.return_value={'status':'completed','output_text':json.dumps(raw),'usage':{'input_tokens':2,'output_tokens':3}}
        with patch.object(scanner.requests,'post',return_value=response) as post:
            value=scanner.analyze_heatmap_images([{'image':'data:image/png;base64,AAAA','timeframe':'12h'}],api_key='synthetic',model='synthetic-model')
        post.assert_called_once();self.assertEqual(post.call_args.kwargs['json']['max_output_tokens'],3500)
        self.assertEqual(value['usage'],{'input_tokens':2,'output_tokens':3})
    def test_unreadable_result_does_not_create_output(self):
        def cap(directory,*,timeframes):
            directory.mkdir();result=[]
            for tf in timeframes:
                p=directory/f'coinglass_btc_heatmap_{tf}.png';p.write_bytes(b'\x89PNG\r\n\x1a\nsynthetic')
                result.append({'image':str(p),'timeframe':tf})
            return result
        raw=sample();raw['scans'][0]['readable']=False
        fake=types.SimpleNamespace(capture_heatmaps=cap,COINGLASS_HEATMAP_URL=task.SOURCE)
        with tempfile.TemporaryDirectory() as root,patch.dict(sys.modules,{'market_vision.coinglass_heatmap_capture':fake}),patch.object(scanner,'analyze_heatmap_images',return_value=raw) as ai:
            with self.assertRaises(ERRORS):task.main('12H','00000000-0000-4000-8000-000000000001',root)
            self.assertFalse((Path(root)/'result.json').exists());ai.assert_called_once()

if __name__=='__main__':unittest.main()
