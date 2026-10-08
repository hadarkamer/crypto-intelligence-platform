"""Offline acceptance of the actual installed prominent-row child pipeline."""
import copy
import inspect
import json
import os
import runpy
from pathlib import Path
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE/'runtime'))
from prominent_levels import normalize_prominent, SELECTION_POLICY, CONTRACT
from model1_execution import StageFailure
from model1_evidence_format import safe_scan
from heatmap_models import child_environment
import collection_model1_task as task
import collection_bridge as bridge
from market_vision import openai_heatmap_scanner as scanner

def level(price=110,*,side='above',colour='yellow',score=.9):
    return {'side':side,'shape':'point','price_low':price-.25,'price_high':price+.25,
        'prices':[price],'intensity':'many','row_colour':colour,'confidence':'medium',
        'price_precision':'axis_estimate','evidence_basis':'axis_interpolation',
        'price_evidence':'Axis interpolation between ticks 80 and 120; row center approximate',
        'uncertainty_usd':.25,'strength_score':score,'continuous':True,
        'weak_gap_present':False,'right_edge_visible':True}

def sample():
    return {'symbol':'BTC','analysis_mode':'visual_screenshot','model':'synthetic',
        'scans':[{'timeframe':'12h','readable':True,'blocking_condition':'none',
        'observed_symbol':'BTC','observed_mode':'Symbol','observed_model':'Model 1',
        'observed_timeframe':'12h','current_price_range':{'low':99.9,'high':100.1,
        'confidence':'medium','basis':'last_candle_axis_bracket','axis_low':99,'axis_high':101},
        'price_axis_range':{'low':80,'high':120,'price_evidence':'80.00 — 120.00'},
        'prominent_levels':[level(110),level(90,side='below')],'short_summary':'synthetic'}]}

def green(low=112,high=114):
    return {**level(),'shape':'range','price_low':low,'price_high':high,'prices':[],
        'intensity':'normal','row_colour':'green','strength_score':.7}

def normalized(raw=None):
    return task.normalize(sample() if raw is None else raw,timeframe='12H',
        run_id='00000000-0000-4000-8000-000000000001',captured_at='2026-01-01T00:00:00Z',image=b'synthetic')

class ProminentTests(unittest.TestCase):
    def reject(self,raw):
        with self.assertRaises(StageFailure):normalized(raw)
    def test_installed_scan_requires_variable_prominent_list(self):
        scans=scanner.HEATMAP_SCHEMA['properties']['scans']
        self.assertEqual((scans['minItems'],scans['maxItems']),(1,1))
        scan=scanner.HEATMAP_SCHEMA['properties']['scans']['items']
        self.assertIn('prominent_levels',scan['required'])
        self.assertIn('price_axis_range',scan['required'])
        self.assertNotIn('above_price',scan['properties'])
        schema=scan['properties']['prominent_levels']
        self.assertEqual(schema['maxItems'],40);self.assertNotIn('minItems',schema)
        self.assertFalse(schema['items']['additionalProperties'])
        self.assertEqual(set(schema['items']['required']),set(schema['items']['properties']))
    def test_markers_evidence_and_estimate_are_preserved(self):
        result=normalized();self.assertEqual(result['selection_policy'],SELECTION_POLICY)
        self.assertEqual(result['level_contract_version'],CONTRACT)
        z=result['zones'][0]
        self.assertEqual(z['shape'],'point');self.assertEqual(z['prices'],[110])
        self.assertEqual(z['price_precision'],'axis_estimate');self.assertEqual(z['uncertainty_usd'],.25)
        self.assertEqual(z['price_low'],109.75);self.assertEqual(z['price_high'],110.25)
        self.assertEqual(len(result['evidence']['sha256']),64)
    def test_adjacent_yellow_rows_stay_individual(self):
        raw=sample();raw['scans'][0]['prominent_levels']=[level(110),level(110.5)]
        self.assertEqual([z['prices'] for z in normalized(raw)['zones']],[[110],[110.5]])
    def test_green_range_is_medium_independent_of_sum(self):
        raw=sample();raw['scans'][0]['prominent_levels']=[green()]
        z=normalized(raw)['zones'][0]
        self.assertEqual((z['intensity'],z['shape'],z['prices']),('normal','range',[]))
        raw['scans'][0]['prominent_levels'][0]['intensity']='many';self.reject(raw)
    def test_gap_or_discontinuous_green_is_rejected(self):
        for key,value in [('weak_gap_present',True),('continuous',False)]:
            raw=sample();raw['scans'][0]['prominent_levels']=[{**green(),key:value}]
            self.reject(raw)
    def test_continuous_yellow_band_is_a_many_range_with_explicit_colour(self):
        raw=sample();raw['scans'][0]['prominent_levels']=[{
            **green(),'row_colour':'yellow','intensity':'many'}]
        z=normalized(raw)['zones'][0]
        self.assertEqual((z['intensity'],z['shape'],z['prices']),('many','range',[]))
        self.assertEqual((z['price_low'],z['price_high']),(112,114))
        self.assertEqual((z['row_colour'],z['continuous'],z['weak_gap_present']),('yellow',True,False))
        self.assertEqual(z['price_precision'],'axis_estimate')
        self.assertGreater(z['uncertainty_usd'],0)
    def test_gapped_yellow_range_and_medium_yellow_are_rejected(self):
        for changes in ({'shape':'range','prices':[],'continuous':False},
            {'shape':'range','prices':[],'weak_gap_present':True},{'intensity':'normal'}):
            raw=sample();raw['scans'][0]['prominent_levels'][0].update(changes);self.reject(raw)
    def test_strength_first_inside_each_side_not_price_sort(self):
        raw=sample();raw['scans'][0]['prominent_levels']=[level(104,score=.7),green(),
            level(117,score=.95),level(86,side='below',score=.7),level(95,side='below',score=.9)]
        self.assertEqual([z['prices'] for z in normalized(raw)['zones']],[[117],[104],[],[95],[86]])
    def test_few_never_pads_strong_list(self):
        raw=sample();raw['scans'][0]['prominent_levels'].append({**level(118),'intensity':'few','row_colour':'blue'})
        self.assertEqual(len(normalized(raw)['zones']),2)
    def test_no_fixed_count(self):
        raw=sample();raw['scans'][0]['prominent_levels']=[level(105+i*.5) for i in range(15)]
        self.assertEqual(len(normalized(raw)['zones']),15)
    def test_limit_is_fail_closed_never_truncates(self):
        raw=sample();raw['scans'][0]['prominent_levels']=[level(110)]*41;self.reject(raw)
    def test_exact_point_requires_actual_numeric_evidence(self):
        raw=sample();z=raw['scans'][0]['prominent_levels'][0]
        z.update(price_low=110,price_high=110,prices=[110],price_precision='verified_label',
            evidence_basis='numeric_row_label',uncertainty_usd=0,confidence='high',price_evidence='Price: 110.00')
        self.assertEqual(normalized(raw)['zones'][0]['price_precision'],'verified_label')
        for changes in ({'price_evidence':'yellow row near 111'}, {'evidence_basis':'axis_interpolation'},
            {'confidence':'medium'},{'uncertainty_usd':.1},{'price_high':110.1}):
            altered=copy.deepcopy(raw);altered['scans'][0]['prominent_levels'][0].update(changes);self.reject(altered)
    def test_comma_formatted_exact_evidence(self):
        from prominent_levels import evidence
        self.assertEqual(evidence('Row: $77,420.50', [77420.5]),'Row: $77,420.50')
    def test_estimate_requires_positive_covering_uncertainty(self):
        for changes in ({'uncertainty_usd':0},{'uncertainty_usd':.1},{'price_precision':'verified_label'},
            {'prices':[109]},{'price_low':110,'price_high':110}):
            raw=sample();raw['scans'][0]['prominent_levels'][0].update(changes);self.reject(raw)
    def test_full_current_uncertainty_used_not_midpoint(self):
        raw=sample();raw['scans'][0]['prominent_levels'][0]=level(100.2)
        result=normalized(raw);self.assertEqual(result['omitted_ambiguous_zones'],1)
        self.assertEqual([z['side'] for z in result['zones']],['below'])
    def test_wrong_side_and_off_axis_rejected(self):
        for changes in ({'side':'below'},{'price_high':121},{'price_low':79},{'right_edge_visible':False}):
            raw=sample();raw['scans'][0]['prominent_levels'][0].update(changes);self.reject(raw)
    def test_axis_labels_required_and_cannot_be_legend(self):
        raw=sample();raw['scans'][0]['price_axis_range']['price_evidence']='legend 500M';self.reject(raw)
    def test_numeric_garbage_is_rejected(self):
        for key,value in [('price_low',True),('price_high',float('nan')),('strength_score',float('inf')),
            ('strength_score',1.1),('uncertainty_usd','0.25')]:
            raw=sample();raw['scans'][0]['prominent_levels'][0][key]=value;self.reject(raw)
    def test_empty_or_all_ambiguous_is_not_success(self):
        for levels in ([],[level(100)]):
            raw=sample();raw['scans'][0]['prominent_levels']=levels;self.reject(raw)
    def test_identity_and_source_blocks_preserved(self):
        for key,value in [('observed_model','Model 2'),('observed_timeframe','24h'),
            ('observed_symbol','other'),('observed_mode','Pair'),('blocking_condition','blur')]:
            raw=sample();raw['scans'][0][key]=value;self.reject(raw)
    def test_diagnostic_retains_metadata_for_each_model(self):
        scan=sample()['scans'][0];scan['observed_model']='Model 3'
        safe=safe_scan(scan);self.assertEqual(safe['observed_model'],'Model 3')
        self.assertEqual(safe['prominent_levels'][0]['price_precision'],'axis_estimate')
        self.assertEqual(safe['price_axis_range']['price_evidence'],'80.00 — 120.00')
    def test_live_main_cannot_use_old_contract_response(self):
        source=inspect.getsource(task.main)
        self.assertIn('prominent_levels',source)
        self.assertIn('analysis_response_invalid',source)
    def test_entrypoint_runs_after_installed_normalizer(self):
        source=(HERE/'runtime/collection_model1_task.py').read_text()
        self.assertGreater(source.index("if __name__ == '__main__':"),source.index('def normalize('))
    def test_actual_child_entrypoint_saves_new_contract(self):
        from PIL import Image
        def capture(directory,*,timeframes):
            directory.mkdir()
            path=directory/'coinglass_btc_heatmap_12h.png'
            Image.new('RGB',(10,10)).save(path)
            return [{'image':str(path),'timeframe':'12h'}]
        capture_module=types.SimpleNamespace(capture_heatmaps=capture,COINGLASS_HEATMAP_URL=task.SOURCE)
        scanner_module=types.SimpleNamespace(analyze_heatmap_images=lambda *a,**kw:sample())
        with tempfile.TemporaryDirectory() as root,patch.dict(sys.modules,{
            'market_vision.coinglass_heatmap_capture':capture_module,
            'market_vision.openai_heatmap_scanner':scanner_module}),patch.object(sys,'argv',[
            'collection_model1_task.py','12H','00000000-0000-4000-8000-000000000001',root]):
            with self.assertRaises(SystemExit) as done:
                runpy.run_path(str(HERE/'runtime/collection_model1_task.py'),run_name='__main__')
            self.assertEqual(done.exception.code,0)
            result=json.loads((Path(root)/'result.json').read_text())
            self.assertEqual(result['selection_policy'],SELECTION_POLICY)
            self.assertEqual(len(result['zones']),2)
    def test_automatic_cache_requires_policy_marker(self):
        source=inspect.getsource(bridge.JobStore.start)
        self.assertIn("result->>'selection_policy'='prominent_right_edge.v1'",source)
        # Historical read-only review remains available independently.
        self.assertNotIn('selection_policy',inspect.getsource(bridge.JobStore.latest))
        self.assertIn('selection_policy',inspect.getsource(bridge.run_existing_scanner))
    def test_bounded_single_provider_request(self):
        response=Mock(ok=True);raw=sample();raw.pop('model')
        response.json.return_value={'status':'completed','output_text':json.dumps(raw),'usage':{}}
        with patch.object(scanner.requests,'post',return_value=response) as post:
            scanner.analyze_heatmap_images([{'image':'data:image/png;base64,AAAA','timeframe':'12h'}],api_key='synthetic',model='synthetic')
        post.assert_called_once();self.assertEqual(post.call_args.kwargs['json']['max_output_tokens'],12000)
        self.assertFalse(post.call_args.kwargs['allow_redirects'])
    def test_full_image_detail_pair_requires_one_scan_in_actual_request(self):
        from PIL import Image
        from price_detail_input import make_detail_file
        response=Mock(ok=True)
        response.json.return_value={'status':'completed','output_text':json.dumps(sample()),'usage':{}}
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'coinglass_btc_heatmap_12h.png'
            Image.new('RGB',(1000,800)).save(path)
            image={'image':str(path),'timeframe':'12h'}
            image['price_detail']=make_detail_file(path,{'x':0,'y':20,'width':800,'height':600,'page_width':1000})
            with patch.object(scanner.requests,'post',return_value=response) as post:
                scanner.analyze_heatmap_images([image],api_key='synthetic',model='synthetic')
        post.assert_called_once()
        request=post.call_args.kwargs['json']
        content=request['input'][0]['content']
        self.assertEqual(sum(part['type']=='input_image' for part in content),2)
        scans=request['text']['format']['schema']['properties']['scans']
        self.assertEqual((scans['minItems'],scans['maxItems']),(1,1))
    def test_two_scans_are_rejected_even_if_their_identity_agrees(self):
        raw=sample();raw['scans'].append(copy.deepcopy(raw['scans'][0]))
        with self.assertRaises(StageFailure) as caught:normalized(raw)
        self.assertEqual(caught.exception.code,'analysis_response_invalid')
    def test_all_nine_model_timeframe_identities(self):
        template=sample()
        for model in (1,2,3):
            code='''import json
from heatmap_models import HEATMAP_MODEL, MODEL_LABEL, SOURCE_URL
from collection_model1_task import normalize
raw=json.loads(INPUT)
for tf in ('12H','24H','48H'):
 s=raw['scans'][0];s.update(timeframe=tf.lower(),observed_timeframe=tf.lower(),observed_model=MODEL_LABEL)
 out=normalize(raw,timeframe=tf,run_id='id',captured_at='time',image=b'synthetic')
 assert out['heatmap_model']==HEATMAP_MODEL and out['source_url']==SOURCE_URL
 assert out['selection_policy']=='prominent_right_edge.v1' and out['timeframe']==tf
print('ok')
'''.replace('INPUT',repr(json.dumps(template)))
            p=subprocess.run([sys.executable,'-c',code],cwd=HERE/'runtime',env=child_environment(model),capture_output=True,text=True,timeout=20)
            self.assertEqual(p.returncode,0,p.stderr[-1000:]);self.assertEqual(p.stdout.strip(),'ok')

class HealthTests(unittest.IsolatedAsyncioTestCase):
    async def test_health_reports_actual_installed_policy_without_starting_work(self):
        import app
        from aiohttp.test_utils import TestClient,TestServer
        with patch.dict(os.environ,{'COLLECTION_BRIDGE_ENABLED':'false'}):
            client=TestClient(TestServer(app.create_app()))
            await client.start_server()
            try:
                response=await client.get('/health');data=await response.json()
                self.assertEqual(data['selection_policy'],SELECTION_POLICY)
                self.assertEqual(data['level_contract_version'],CONTRACT)
                self.assertFalse(data['bot_started']);self.assertFalse(data['scheduled_collection'])
            finally:await client.close()

if __name__=='__main__':unittest.main()
