"""Offline interval/evidence tests. All prices/images/credentials are synthetic."""
import asyncio
import copy
import contextlib
import io
import json
import math
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import AsyncMock,Mock,patch
from uuid import uuid4

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE/'runtime'))
import collection_bridge as bridge
import collection_model1_task as task
from market_vision import openai_heatmap_scanner as scanner
from model1_price_range import validate_price_range,MAX_RANGE_FRACTION
from model1_execution import StageFailure
from model1_evidence_format import evidence_payload,safe_scan
from test_original_flow import sample


def interval():
    return {'low':99.8,'high':100.2,'confidence':'medium',
            'basis':'last_candle_axis_bracket','axis_low':99,'axis_high':101}


def range_sample():
    raw=sample();raw['scans'][0].update(current_price_estimate=None,
        current_price_confidence='low',current_price_range=interval())
    return raw


def normalize(raw):
    return task.normalize(raw,timeframe='12H',run_id=str(uuid4()),
                          captured_at='2026-01-01T00:00:00Z',image=b'fixture')


class RangeTests(unittest.TestCase):
    def test_narrow_range_accepted_without_exact_price(self):
        result=normalize(range_sample())
        self.assertIsNone(result['observed_price'])
        self.assertEqual(result['observed_price_range'],interval())
        self.assertEqual(result['price_reference_type'],'visual_range')
        self.assertEqual(len(result['zones']),2)
    def test_point_only_legacy_unchanged(self):
        result=normalize(sample())
        self.assertEqual(result['observed_price'],100)
        self.assertNotIn('observed_price_range',result)
    def test_point_low_confidence_without_range_still_fails(self):
        raw=sample();raw['scans'][0]['current_price_confidence']='low'
        with self.assertRaises(ValueError):normalize(raw)
    def test_no_midpoint_is_invented_even_when_model_offers_one(self):
        raw=range_sample();raw['scans'][0]['current_price_estimate']=100
        result=normalize(raw);self.assertIsNone(result['observed_price'])
    def test_invalid_ranges_do_not_fallback_to_exact(self):
        for key,value in [('low',0),('high',float('nan')),('low',True),('high','100'),
                          ('low',None),('confidence','low'),('basis','unreadable')]:
            raw=range_sample();raw['scans'][0]['current_price_range'][key]=value
            raw['scans'][0].update(current_price_estimate=100,current_price_confidence='high')
            with self.subTest(key=key,value=value),self.assertRaises(StageFailure):normalize(raw)
    def test_width_policy_not_false_accuracy_claim(self):
        raw=range_sample();raw['scans'][0]['current_price_range'].update(low=98,high=102,axis_low=95,axis_high=105)
        with self.assertRaises(StageFailure) as e:normalize(raw)
        self.assertEqual(e.exception.code,'price_range_too_wide')
    def test_price_ticks_must_bracket_interval(self):
        for values in ({'axis_low':100},{'axis_high':100},{'axis_low':102,'axis_high':98},{'low':100,'high':100}):
            with self.subTest(values=values),self.assertRaises(StageFailure):validate_price_range({**interval(),**values})
    def test_boundary_overlap_is_omitted_not_midpoint_classified(self):
        for lo,hi in ((100.1,100.3),(100.2,102),(98,99.8),(99,101)):
            raw=range_sample()
            raw['scans'][0]['above_price']['main_zone'].update(low_price=lo,high_price=hi)
            out=normalize(raw)
            self.assertEqual(out['omitted_ambiguous_zones'],1)
            self.assertEqual([z['side'] for z in out['zones']],['below'])
    def test_all_ambiguous_does_not_return_empty_success(self):
        raw=range_sample()
        for field in ('above_price','below_price'):
            raw['scans'][0][field]['main_zone'].update(low_price=99,high_price=101)
        with self.assertRaises(StageFailure) as e:normalize(raw)
        self.assertEqual(e.exception.code,'no_unambiguous_zones')
    def test_range_does_not_override_source_identity(self):
        raw=range_sample();raw['scans'][0]['observed_timeframe']='48h'
        with self.assertRaises(StageFailure) as e:normalize(raw)
        self.assertEqual(e.exception.code,'screenshot_identity_mismatch')
    def test_range_does_not_override_unreadable_source(self):
        raw=range_sample();raw['scans'][0]['blocking_condition']='blur'
        with self.assertRaises(StageFailure) as e:normalize(raw)
        self.assertEqual(e.exception.code,'source_not_readable')
    def test_wrong_side_rejected_not_silently_relabelled(self):
        raw=range_sample();raw['scans'][0]['above_price']['main_zone'].update(low_price=95,high_price=96)
        with self.assertRaises(StageFailure) as e:normalize(raw)
        self.assertEqual(e.exception.code,'zones_invalid')
    def test_model_schema_requires_interval_fields(self):
        schema=scanner.HEATMAP_SCHEMA['properties']['scans']['items']
        self.assertIn('current_price_range',schema['required'])
        self.assertFalse(schema['properties']['current_price_range']['additionalProperties'])
    def test_48h_keeps_its_horizon(self):
        raw=range_sample();raw['scans'][0].update(timeframe='48h',observed_timeframe='48h')
        result=task.normalize(raw,timeframe='48H',run_id=str(uuid4()),captured_at='2026-01-01T00:00:00Z',image=b'fixture')
        self.assertEqual(result['timeframe'],'48H')
    def test_low_confidence_zone_still_excluded(self):
        raw=range_sample();raw['scans'][0]['above_price']['main_zone']['confidence']='low'
        self.assertEqual([z['side'] for z in normalize(raw)['zones']],['below'])


class EvidenceTests(unittest.TestCase):
    def test_diagnostic_never_copies_profile_or_free_text(self):
        raw=range_sample()['scans'][0]
        raw.update(email='PRIVATE',short_summary='PRIVATE',headers={'cookie':'PRIVATE'})
        diagnostic=safe_scan(raw)
        self.assertNotIn('PRIVATE',json.dumps(diagnostic))
        self.assertEqual(diagnostic['current_price_range'],interval())
    def test_format_retains_png_but_not_raw_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'capture').mkdir()
            (root/'capture/coinglass_btc_heatmap_48h.png').write_bytes(b'\x89PNG\r\n\x1a\nfixture')
            (root/'analysis_evidence.json').write_text(json.dumps({'analysis':{**range_sample()['scans'][0],'secret':'PRIVATE'}}))
            image,record=evidence_payload(root,'48H')
            self.assertTrue(image.startswith(b'\x89PNG'))
            self.assertNotIn('PRIVATE',json.dumps(record))
            self.assertFalse(record['assessment_validated'])
    def test_absent_capture_retains_diagnostic_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            image,record=evidence_payload(tmp,'12H')
            self.assertIsNone(image);self.assertFalse(record['image_retained'])
    def test_raw_non_png_is_not_image_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'capture').mkdir()
            (root/'capture/coinglass_btc_heatmap_12h.png').write_text('PRIVATE')
            self.assertIsNone(evidence_payload(tmp,'12H')[0])


class FailedJobRetentionTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_job_retains_evidence_before_temp_cleanup(self):
        directory=[]
        async def create_process(*args,**kwargs):
            root=Path(args[-1]);directory.append(root);(root/'capture').mkdir()
            (root/'capture/coinglass_btc_heatmap_48h.png').write_bytes(b'\x89PNG\r\n\x1a\nfixture')
            (root/'error.json').write_text(json.dumps({'code':'price_range_uncertain','stage':'validation'}))
            process=Mock(returncode=1);process.wait=AsyncMock(return_value=1);return process
        with patch.object(bridge.asyncio,'create_subprocess_exec',side_effect=create_process),\
             patch.object(bridge.JobStore,'retain_evidence') as retain,contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(bridge.BridgeError) as e:await bridge.run_existing_scanner('48H',str(uuid4()))
            self.assertEqual(e.exception.code,'price_range_uncertain')
            retain.assert_called_once()
            self.assertTrue(retain.call_args.args[1].startswith(b'\x89PNG'))
        self.assertFalse(directory[0].exists())

if __name__=='__main__':unittest.main()
