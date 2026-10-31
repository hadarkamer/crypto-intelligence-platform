"""Offline checks; no site, Render job, DB or AI is contacted."""
import contextlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('probe',ROOT/'probe.py')
probe=importlib.util.module_from_spec(spec);spec.loader.exec_module(probe)

def group():
    return {'cpu.max':'200000 100000','memory.max':4*1024**3,'memory.current':100,
            'cpu.stat':{'usage_usec':1000,'nr_throttled':0}}

class Page:
    def __init__(self,fail=False,unsupported=False,changed=False):
        self.enabled=False;self.calls=[];self.fail=fail;self.unsupported=unsupported
        self.changed=changed;self.states=0
    def evaluate(self,script,arg=None):
        if script=='STATE':
            self.states+=1
            return {'label':'24 hour' if self.changed and self.states>1 else '12 hour',
                    'geometry':{'x':10,'y':20,'width':600,'height':300}}
        if 'scrollX' in script:return {'x':0,'y':0}
        if 'attachSelected' in script:
            return {'supported':not self.unsupported,'version':'8.21.0',
                    'entry':{'key':1,'ticker_count':2,'enabled':False}}
        if 'snapshot' in script:
            return {'stats':{'render_calls':1,'render_ms':1,'skipped_paints':1}}
        action=arg['action'] if isinstance(arg,dict) else 'disable'
        self.calls.append(action);self.enabled=action=='enable'
        return {'enabled':self.enabled,'ticker_count':2}
    def wait_for_timeout(self,ms):
        if self.fail and self.enabled:raise RuntimeError('simulated_browser_failure')

class ProbeTests(unittest.TestCase):
    def test_credentials_filtered_and_worker_disabled(self):
        env=probe.safe_env({'OPENAI_API_KEY':'secret','DATABASE_URL':'secret',
            'SUPABASE_SERVICE_ROLE_KEY':'secret','COINGLASS_COLLECTOR_TOKEN':'secret',
            'COINGLASS_COOKIE_HEADER':'source-session','PATH':'/bin'},3)
        self.assertEqual(set(env),{'PATH','COINGLASS_COOKIE_HEADER','COLLECTION_HEATMAP_MODEL',
            'COLLECTION_BRIDGE_ENABLED','PYTHONDONTWRITEBYTECODE','PYTHONUNBUFFERED'})
        self.assertEqual(env['COLLECTION_BRIDGE_ENABLED'],'false')
    def test_requires_separate_two_cpu_four_gib(self):
        probe.require_limits(group())
        for bad in ('100000 100000','max 100000','200000 0'):
            with self.subTest(bad=bad),self.assertRaises(RuntimeError):
                probe.require_limits({**group(),'cpu.max':bad})
    def run_preparation(self,page):
        ready=types.SimpleNamespace(STATE_SCRIPT='STATE',geometry_from_state=lambda x:x['geometry'])
        with patch.dict(sys.modules,{'capture_readiness':ready}),patch.object(probe.resources,'cgroup_snapshot',group):
            return probe.preparation_probe(page)
    def test_three_windows_restore_original(self):
        p=Page();result=self.run_preparation(p)
        self.assertEqual(result['status'],'completed')
        self.assertEqual([r['mode'] for r in result['windows']],['original','limited','restored'])
        self.assertFalse(p.enabled)
        self.assertEqual(p.calls,['enable','disable','disable'])
    def test_exception_always_disables_gate(self):
        p=Page(fail=True)
        with self.assertRaisesRegex(RuntimeError,'simulated'):self.run_preparation(p)
        self.assertFalse(p.enabled);self.assertEqual(p.calls[-1],'disable')
    def test_unknown_hook_never_enabled(self):
        p=Page(unsupported=True)
        self.assertEqual(self.run_preparation(p)['status'],'not_tested')
        self.assertEqual(p.calls,[])
    def test_changed_map_invalidates_comparison(self):
        self.assertEqual(self.run_preparation(Page(changed=True))['status'],'inconclusive')
    def test_candle_pixels_and_ambiguity(self):
        from PIL import Image
        with tempfile.TemporaryDirectory() as td:
            path=Path(td)/'candle.png';im=Image.new('RGB',(30,30))
            for y in range(10,15):im.putpixel((19,y),(234,51,35))
            im.save(path);bounds={'top':2,'bottom':25,'right':20}
            self.assertEqual(probe.model3_points(path,bounds),[(19,10),(19,12),(19,14)])
            im.putpixel((19,20),(234,51,35));im.save(path)
            with self.assertRaisesRegex(RuntimeError,'ambiguous'):probe.model3_points(path,bounds)
    def test_runtime_hash_change_stops(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);(root/'x.py').write_text('changed')
            with patch.object(probe.json,'loads',return_value={'x.py':'wrong'}):
                with self.assertRaisesRegex(RuntimeError,'deployed_runtime_mismatch'):
                    probe.verify_runtime(root)
    def test_capture_wrapper_preserves_both_model_results(self):
        # Real stack-frame/context-manager integration against a fake source.
        # All browser, network and production dependencies remain absent.
        for model in (3,2):
            with self.subTest(model=model),tempfile.TemporaryDirectory() as td:
                runtime=Path(td);out=runtime/'out';out.mkdir()
                filename=runtime/'market_vision/coinglass_heatmap_capture.py'
                code="""from model1_execution import capture_operation
def capture_heatmaps(output,timeframes):
    page=object();path=output/'image.png';path.write_bytes(b'fixture')
    chart_rect={'width':600};hover_snapshot=None
    with capture_operation('timeframe_selection'):pass
    with capture_operation('hover_collection'):
        hover_snapshot={'complete':True,'heatmap_model':MODEL,'timeframe':'12H',
                        'source_time':'2026-10-31T12:00:00Z','rows':[{}]}
    return [{'hover_snapshot':hover_snapshot,'image':str(path)}]
"""
                execution=types.ModuleType('model1_execution')
                execution.capture_operation=lambda name:contextlib.nullcontext()
                execution.failure_detail=lambda exc:{'kind':type(exc).__name__}
                mod=types.ModuleType('market_vision.coinglass_heatmap_capture');mod.MODEL=model
                with patch.dict(sys.modules,{'model1_execution':execution}):
                    exec(compile(code,str(filename),'exec'),mod.__dict__)
                    # Import after the probe patches execution, matching production.
                    original=probe.preparation_probe
                    observations=[]
                    def candle(page,path,geometry,snapshot,model):
                        self.assertIsNotNone(page);self.assertTrue(path.is_file())
                        self.assertTrue(snapshot['complete']);observations.append(model)
                        return {'observed_points':[]}
                    old_path=list(sys.path)
                    try:
                        with patch.dict(os.environ,{},clear=True),patch.dict(sys.modules,{
                                'market_vision':types.ModuleType('market_vision'),
                                'market_vision.coinglass_heatmap_capture':mod}),\
                             patch.object(probe.resources,'cgroup_snapshot',group),\
                             patch.object(probe,'preparation_probe',return_value={'status':'completed'}) as prep,\
                             patch.object(probe,'candle_probe',side_effect=candle):
                            # The cached fake capture imports the wrapped operation lazily.
                            mod.__dict__['capture_operation']=lambda name:execution.capture_operation(name)
                            self.assertEqual(probe.child(runtime,out,model),0)
                            self.assertEqual(prep.call_count,1 if model==3 else 0)
                    finally:sys.path[:]=old_path
                    self.assertEqual(observations,[model])
                    result=json.loads((out/'report.json').read_text())
                    self.assertTrue(result['source_success'])
                    self.assertEqual([x['name'] for x in result['operations']],
                                     ['timeframe_selection','hover_collection'])

if __name__=='__main__':unittest.main(verbosity=2)
