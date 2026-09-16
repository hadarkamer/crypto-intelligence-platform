"""Offline model isolation tests. No website, model API or database writes."""
from pathlib import Path
import json
import os
import subprocess
import sys
import unittest
from unittest.mock import patch

HERE=Path(__file__).resolve().parent
RUNTIME=HERE/'runtime'
sys.path.insert(0,str(RUNTIME))
from heatmap_models import source_url,schema_version,model_number,child_environment
from collection_bridge import JobStore,envelope


class FakeResult:
    def __init__(self,value):self.value=value
    def fetchone(self):return self.value
class FakeConnection:
    def __init__(self,values=()):self.values=iter(values);self.calls=[]
    def __enter__(self):return self
    def __exit__(self,*args):return False
    def execute(self,sql,params=None):
        self.calls.append((sql,params))
        return FakeResult(next(self.values,None))


def child_check(model):
    code=r'''
import inspect,json
from heatmap_models import HEATMAP_MODEL,MODEL_LABEL,SOURCE_URL,SCHEMA_VERSION
from market_vision.coinglass_heatmap_capture import capture_heatmaps,_select_model_one
from market_vision.openai_heatmap_scanner import HEATMAP_SCHEMA
from collection_model1_task import normalize
from model1_execution import StageFailure
assert capture_heatmaps.__kwdefaults__['url']==SOURCE_URL
assert HEATMAP_SCHEMA['properties']['scans']['items']['properties']['observed_model']['enum'][0]==MODEL_LABEL
class Locator:
 def count(self):return 1
 @property
 def first(self):return self
 def is_visible(self):return True
 def click(self,**kwargs):pass
class Page:
 def get_by_role(self,role,**kwargs):
  assert role=='button' and kwargs['name'].fullmatch(MODEL_LABEL)
  return Locator()
 def wait_for_timeout(self,*args):pass
_select_model_one(Page())
zone=lambda low,high:{'low_price':low,'high_price':high,'relative_strength':'strong','confidence':'medium'}
for tf in ('12H','24H','48H'):
 scan={'timeframe':tf.lower(),'readable':True,'blocking_condition':'none',
  'observed_symbol':'BTC','observed_mode':'Symbol','observed_model':MODEL_LABEL,'observed_timeframe':tf.lower(),
  'current_price_estimate':None,'current_price_confidence':'low',
  'current_price_range':{'low':100,'high':100.5,'axis_low':90,'axis_high':110,'confidence':'medium','basis':'last_candle_axis_bracket'},
  'above_price':{'main_zone':zone(102,103),'secondary_zones':[]},
  'below_price':{'main_zone':zone(97,98),'secondary_zones':[]},'short_summary':'synthetic'}
 raw={'symbol':'BTC','analysis_mode':'visual_screenshot','model':'synthetic','scans':[scan]}
 result=normalize(raw,timeframe=tf,run_id='00000000-0000-4000-8000-000000000001',captured_at='2026-01-01T00:00:00Z',image=b'synthetic')
 assert result['heatmap_model']==HEATMAP_MODEL and result['schema_version']==SCHEMA_VERSION
 assert result['source_url']==SOURCE_URL and result['timeframe']==tf
 assert result['observed_price'] is None and len(result['zones'])==2
 for wrong in ('Model 1','Model 2','Model 3','other','unknown'):
  if wrong==MODEL_LABEL:continue
  scan['observed_model']=wrong
  try:normalize(raw,timeframe=tf,run_id='x',captured_at='t',image=b'synthetic')
  except StageFailure:pass
  else:raise AssertionError('cross-model result accepted')
 scan['observed_model']=MODEL_LABEL;scan['observed_timeframe']='unknown'
 try:normalize(raw,timeframe=tf,run_id='x',captured_at='t',image=b'synthetic')
 except StageFailure:pass
 else:raise AssertionError('unknown horizon accepted')
print('ok')
'''
    p=subprocess.run([sys.executable,'-c',code],cwd=RUNTIME,env=child_environment(model),capture_output=True,text=True,timeout=20)
    if p.returncode:raise AssertionError(p.stderr[-1500:])
    return p.stdout.strip()


class ModelTests(unittest.TestCase):
    def test_model1_all_timeframes_and_wrong_model_rejection(self):self.assertEqual(child_check(1),'ok')
    def test_model2_all_timeframes_and_wrong_model_rejection(self):self.assertEqual(child_check(2),'ok')
    def test_model3_all_timeframes_and_wrong_model_rejection(self):self.assertEqual(child_check(3),'ok')
    def test_allowlist_rejects_arbitrary_inputs(self):
        for value in (0,4,True,None,'2','https://bad.invalid'):
            with self.subTest(value=value),self.assertRaises(ValueError):model_number(value)
    def test_legacy_source_and_schema_preserved(self):
        self.assertEqual(source_url(1),'https://www.coinglass.com/pro/futures/LiquidationHeatMap?coin=BTC&type=symbol')
        self.assertEqual(schema_version(1),'coinglass-model1.v1')
    def test_child_config_does_not_mutate_parent(self):
        old=os.environ.get('COLLECTION_HEATMAP_MODEL')
        a,b=child_environment(2),child_environment(3)
        self.assertEqual((a['COLLECTION_HEATMAP_MODEL'],b['COLLECTION_HEATMAP_MODEL']),('2','3'))
        self.assertEqual(os.environ.get('COLLECTION_HEATMAP_MODEL'),old)
    def test_envelopes_identify_each_model(self):
        for model in (1,2,3):
            row={'id':'id','timeframe':'48H','status':'queued','heatmap_model':model}
            self.assertEqual(envelope(row)['schema_version'],schema_version(model))
        self.assertEqual(envelope({'id':'id','timeframe':'12H','status':'queued'})['schema_version'],schema_version(1))
    def test_get_scopes_model(self):
        store=JobStore('synthetic',heatmap_model=2);conn=FakeConnection([None])
        with patch.object(store,'connect',return_value=conn):self.assertIsNone(store.get('id'))
        self.assertIn('heatmap_model=%s',conn.calls[0][0]);self.assertEqual(conn.calls[0][1],('id',2))
    def test_latest_scopes_model_and_never_inserts(self):
        store=JobStore('synthetic',heatmap_model=3);conn=FakeConnection([None])
        with patch.object(store,'connect',return_value=conn):self.assertIsNone(store.latest('48H'))
        sql,params=conn.calls[0]
        self.assertIn('heatmap_model=%s',sql);self.assertEqual(params,(3,'48H'))
        self.assertNotIn('INSERT',sql);self.assertIn("interval '6 hours'",sql)
    def test_evidence_scopes_model(self):
        store=JobStore('synthetic',heatmap_model=2);conn=FakeConnection([None])
        with patch.object(store,'connect',return_value=conn):self.assertIsNone(store.evidence('id'))
        self.assertIn('j.heatmap_model=%s',conn.calls[0][0]);self.assertEqual(conn.calls[0][1],('id',2))
    def test_initializer_keeps_old_jobs_as_model1(self):
        store=JobStore('synthetic');conn=FakeConnection()
        with patch.object(store,'connect',return_value=conn):store.initialize()
        sql='\n'.join(s for s,_ in conn.calls)
        self.assertIn('ADD COLUMN IF NOT EXISTS heatmap_model integer NOT NULL DEFAULT 1',sql)
        self.assertIn('ON public.ai_collection_bridge_jobs(heatmap_model,timeframe)',sql)
        self.assertNotIn('DELETE FROM',sql)
        self.assertIn('REVOKE ALL',sql)
    def test_request_id_cannot_switch_model(self):
        from collection_bridge import BridgeError
        prior={'requested_timeframe':'12H','requested_model':1}
        store=JobStore('synthetic',heatmap_model=2);conn=FakeConnection([None,prior])
        with patch.object(store,'connect',return_value=conn),self.assertRaises(BridgeError) as caught:
            store.start('rid','12H')
        self.assertEqual(caught.exception.code,'idempotency_conflict')
    def test_worker_keeps_single_shared_lock(self):
        import collection_bridge as bridge
        self.assertEqual(bridge.WORK_LOCK,748230102)
        self.assertEqual(bridge.CREATE_LOCK,748230101)
    def test_cap_can_fit_one_nine_item_pass_and_remains_bounded(self):
        with patch.dict(os.environ,{'COLLECTION_BRIDGE_HOURLY_LIMIT':'12'}):
            self.assertEqual(JobStore('synthetic').hourly_limit,12)
        with patch.dict(os.environ,{'COLLECTION_BRIDGE_HOURLY_LIMIT':'999999'}):
            self.assertEqual(JobStore('synthetic').hourly_limit,20)

if __name__=='__main__':unittest.main()
