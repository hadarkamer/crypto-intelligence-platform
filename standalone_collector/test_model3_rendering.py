"""Source-independent lifecycle and browser checks; no accounts or network."""
import ast
import hashlib
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch
HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE/'runtime'))
import model3_rendering as rendering
from install_model3_rendering import expand
GEOMETRY={'x':81,'y':336,'width':1428,'height':800,'page_width':1600}
# Independent pristine capture hash: 6b05e7dd62b4a2ef228c2cf31eb75d9598aa36ef0a15c4665871cda085e40b35.
# Include empty fields on every Python version; only empty type_params is normalized.
BASE_AST_SHA='ff5a666e1e7b2274ad30c0584f3a4ba30521e7b604f2bef8b16b68ab55588c4c'
def canonical_ast(value):
    if isinstance(value,ast.AST):
        return [type(value).__name__,[[name,canonical_ast(field)] for name,field in ast.iter_fields(value)
            if not(name=='type_params' and field==[])]]
    if isinstance(value,list):return [canonical_ast(item) for item in value]
    return value


def ast_digest(tree):
    return hashlib.sha256(json.dumps(canonical_ast(tree),separators=(',',':'),ensure_ascii=True).encode()).hexdigest()


class Context:
    def __init__(self):self.scripts=[];self.routes={}
    def add_init_script(self,**kwargs):self.scripts.append(kwargs)
    def route(self,url,fn):self.routes[url]=fn


class Page:
    def __init__(self):
        self.context=Context();self.calls=[];self.enabled=False;self.restore_fail=False
        setattr(self.context,rendering._STATE,True)
        self.entry={'key':1,'enabled':False,'ticker_count':2,
            'canvas':{'rect':{k:GEOMETRY[k] for k in ('x','y','width','height')}}}
        self.supported=True;self.version='8.21.0';self.reason='selected_chart_unobserved'
    def evaluate(self,script,arg=None):
        self.calls.append((script,arg))
        if 'attachSelected' in script:return {'supported':self.supported,'version':self.version,
            'reason':self.reason,'entry':{**self.entry,'enabled':self.enabled}}
        if '.enable(' in script:self.enabled=True;return {**self.entry,'enabled':True}
        if '.disable(' in script:
            if self.restore_fail:raise RuntimeError('synthetic_restore_failure')
            self.enabled=False;return {**self.entry,'enabled':False}
        if '.forcePaint(' in script:return {'key':1,'forced_paints':1}
        raise AssertionError('Unexpected browser call')


class RenderingTests(unittest.TestCase):
    def test_other_models_are_exact_noops(self):
        for model in (1,2):
            rendering.configure_context(object(),model)
            with rendering.preparation(object(),model):pass
            with rendering.observation(object(),model,{}) as prepare:self.assertIsNone(prepare())
    def test_context_uses_one_ordered_init_script_and_no_network_routes(self):
        context=Context();rendering.configure_context(context,3)
        self.assertEqual(context.routes,{})
        self.assertEqual(len(context.scripts),1)
        script=context.scripts[0]['script']
        self.assertTrue(script.startswith((HERE/'runtime/model3_render_gate.js').read_text()))
        self.assertTrue(script.endswith(rendering.DISCOVERY))
        with self.assertRaisesRegex(RuntimeError,'already_configured'):rendering.configure_context(context,3)
    def test_missing_configuration_keeps_original_without_browser_calls(self):
        page=Page();delattr(page.context,rendering._STATE)
        with rendering.observation(page,3,GEOMETRY) as prepare:self.assertIsNone(prepare())
        self.assertEqual(page.calls,[])
    def test_missing_or_unsupported_hook_never_activates_gate(self):
        for reason in ('selected_chart_unobserved','renderer_version_unrecognized','init_hook_unavailable'):
            page=Page();page.supported=False;page.reason=reason
            with rendering.observation(page,3,GEOMETRY) as prepare:self.assertIsNone(prepare())
            self.assertFalse(any('.enable(' in script for script,_ in page.calls))
    def test_discovered_identity_mismatch_is_rejected(self):
        for kind in ('geometry','version'):
            page=Page()
            if kind=='geometry':page.entry['canvas']['rect']['x']+=1
            else:page.version='9.0.0'
            with self.assertRaisesRegex(RuntimeError,'discovery_unverified'):
                with rendering.observation(page,3,GEOMETRY):pass
    def test_scope_activates_clean_capture_and_restores(self):
        page=Page();seen=[]
        def clean(target,paint):
            seen.append(target);self.assertTrue(page.enabled);paint(target);return {'fresh_paint':True}
        with patch.dict(sys.modules,{'model3_clean_frame':types.SimpleNamespace(prepare_frame=clean)}):
            with rendering.observation(page,3,GEOMETRY) as prepare:
                self.assertTrue(prepare()['fresh_paint']);self.assertTrue(page.enabled)
        self.assertEqual(seen,[page]);self.assertFalse(page.enabled)
        self.assertEqual(sum('.enable(' in s for s,_ in page.calls),1)
        self.assertEqual(sum('.disable(' in s for s,_ in page.calls),1)
    def test_source_failure_is_preserved_and_restores(self):
        page=Page()
        with self.assertRaisesRegex(ValueError,'original_source_failure'):
            with rendering.observation(page,3,GEOMETRY):raise ValueError('original_source_failure')
        self.assertFalse(page.enabled)
    def test_restore_failure_rejects_success_without_masking_source_failure(self):
        page=Page();page.restore_fail=True
        with self.assertRaisesRegex(RuntimeError,'synthetic_restore_failure'):
            with rendering.observation(page,3,GEOMETRY):pass
        page=Page();page.restore_fail=True
        with self.assertRaisesRegex(ValueError,'original_source_failure'):
            with rendering.observation(page,3,GEOMETRY):raise ValueError('original_source_failure')
    def test_active_hook_is_not_reused(self):
        page=Page();page.enabled=True
        with self.assertRaisesRegex(RuntimeError,'unexpected_active_hook'):
            with rendering.observation(page,3,GEOMETRY):pass


class DiscoveryBrowserTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from playwright.sync_api import sync_playwright
        cls.p=sync_playwright().start();cls.addClassCleanup(cls.p.stop)
        cls.browser=cls.p.chromium.launch(headless=True);cls.addClassCleanup(cls.browser.close)
    def setUp(self):
        self.context=self.browser.new_context(viewport={'width':400,'height':300})
        self.addCleanup(self.context.close)
        self.context.route('**/*',lambda route:route.abort())
        self.page=self.context.new_page()
        self.page.set_content('<canvas width="200" height="100"></canvas>')
        self.page.evaluate('''() => {
          const canvas=document.querySelector('canvas');
          window.makeApp=()=>({renderer:{canvas,render(){return 'rendered'}},
             ticker:{started:true,count:2,add(){},remove(){}},render(){return this.renderer.render()}});
          window.app=makeApp();
          const r=canvas.getBoundingClientRect();window.rect={x:r.x,y:r.y,width:r.width,height:r.height};
        }''')
    def install(self,prefix=''):
        script=prefix+(HERE/'runtime/model3_render_gate.js').read_text()+'\n'+rendering.DISCOVERY
        self.page.evaluate('source => (0,eval)(source)',script)
    def result(self):return self.page.evaluate('()=>__cgModel3Discovery.attachSelected(rect)')
    def test_matching_hook_attaches_only_the_selected_canvas_and_deduplicates(self):
        self.install();self.assertFalse(self.result()['supported'])
        self.page.evaluate("()=>{__PIXI_APP_INIT__(app,'8.21.0');__PIXI_APP_INIT__(app,'8.21.0')}")
        self.assertTrue(self.result()['supported'])
        self.assertEqual(self.page.evaluate('()=>__cg29RenderLab.snapshot().length'),1)
        self.assertEqual(self.page.evaluate('()=>app.render()'),'rendered')
    def test_unknown_library_version_and_wrong_geometry_keep_original(self):
        self.install();self.page.evaluate("()=>__PIXI_APP_INIT__(app,'9.0.0')")
        self.assertFalse(self.result()['supported'])
        self.assertEqual(self.page.evaluate('()=>__cg29RenderLab.snapshot().length'),0)
        self.page.evaluate("()=>{__PIXI_APP_INIT__(app,'8.21.0');rect.x+=1}")
        self.assertFalse(self.result()['supported'])
    def test_existing_hook_return_receiver_and_arguments_are_preserved(self):
        self.install("globalThis.__PIXI_APP_INIT__=function(a,v){globalThis.prior={receiver:this.flag,version:v,same:a===app};return 'kept'};\n")
        result=self.page.evaluate("()=>__PIXI_APP_INIT__.call({flag:7},app,'8.21.0')")
        self.assertEqual(result,'kept')
        self.assertEqual(self.page.evaluate('()=>prior'),{'receiver':7,'version':'8.21.0','same':True})
        self.assertTrue(self.result()['supported'])
    def test_locked_source_hook_is_not_replaced(self):
        self.install("Object.defineProperty(globalThis,'__PIXI_APP_INIT__',{value:()=>42,writable:false});\n")
        self.assertEqual(self.page.evaluate('()=>__PIXI_APP_INIT__()'),42)
        self.assertFalse(self.result()['supported'])
    def test_ambiguous_canvas_owners_and_replaced_hook_are_rejected(self):
        self.install()
        self.page.evaluate("()=>{__PIXI_APP_INIT__(app,'8.21.0');__PIXI_APP_INIT__(makeApp(),'8.21.0')}")
        self.assertEqual(self.result()['reason'],'ambiguous_chart')
        self.page.evaluate('()=>globalThis.__PIXI_APP_INIT__=()=>42')
        self.assertEqual(self.result()['reason'],'init_hook_unavailable')
    def test_collection_waits_for_application_ready_instead_of_mutating_init(self):
        self.install()
        self.page.evaluate("()=>{window.early={};__PIXI_APP_INIT__(early,'8.21.0')}")
        self.assertFalse(self.result()['supported'])
        self.page.evaluate('()=>Object.assign(early,makeApp())')
        self.assertTrue(self.result()['supported'])


class PreparationBrowserTests(unittest.TestCase):
    """Real browser/input/PNG and readiness; synthetic chart, no source account."""
    @classmethod
    def setUpClass(cls):
        from playwright.sync_api import sync_playwright
        cls.driver=sync_playwright().start();cls.addClassCleanup(cls.driver.stop)
        cls.browser=cls.driver.chromium.launch(headless=True);cls.addClassCleanup(cls.browser.close)

    def page(self,version='8.21.0'):
        from heatmap_models import source_url
        context=self.browser.new_context(viewport={'width':1600,'height':1300})
        self.addCleanup(context.close);rendering.configure_context(context,3)
        html='''<style>body{margin:0}#panel{margin:40px;width:800px}
          canvas{display:block}.cg-toolti-box{position:absolute;left:100px;top:200px}</style>
          <div id="panel" class="LiqHeatmap3_box1__test"><h1>BTC Liquidation Heatmap</h1>
          <button role="combobox" onclick="document.querySelector('#options').hidden=false">48 hour</button>
          <div id="options" hidden>
          <button role="option" onclick="select(12)">12 hour</button>
          <button role="option" onclick="select(24)">24 hour</button>
          <button role="option" onclick="select(48)">48 hour</button></div>
          <canvas width="800" height="400"></canvas>
          <div data-axis="y" role="button">77000 87000</div>
          <div class="MuiCard-root"><a href="https://legend.coinglass.com">Legend NEW</a>
          <div class="shou" data-first-child onclick="this.parentElement.remove()">
          <svg width="20" height="20"><path d="M405 136.798L375.202 107"/></svg></div></div></div>
          <script>
          const canvas=document.querySelector('canvas'),ctx=canvas.getContext('2d');
          window.hours=48;window.dataUpdates=0;window.paints=0;
          const listeners=[];
          const ticker={started:true,get count(){return listeners.length},
            add(fn,scope){listeners.push({fn,scope})},
            remove(fn,scope){for(let i=listeners.length-1;i>=0;i--)
              if(listeners[i].fn===fn&&listeners[i].scope===scope)listeners.splice(i,1)}};
          const renderer={canvas,render(){window.paints++;ctx.fillStyle={12:'#123456',24:'#234567',48:'#345678'}[hours];
            ctx.fillRect(0,0,800,400);return 'drawn'}};
          window.app={renderer,ticker,render(){return this.renderer.render()}};
          ticker.add(()=>{window.dataUpdates++},null);ticker.add(app.render,app);
          __PIXI_APP_INIT__(app,'VERSION');
          function frame(){for(const l of [...listeners])l.fn.call(l.scope);requestAnimationFrame(frame)}
          requestAnimationFrame(frame);
          window.select=h=>{hours=h;document.querySelector('[role=combobox]').textContent=h+' hour';
            document.querySelector('#options').hidden=true;};
          </script>'''.replace('VERSION',version)
        context.route('**/*',lambda route:route.fulfill(status=200,content_type='text/html',body=html))
        page=context.new_page();page.goto(source_url(3));page.wait_for_function('()=>paints>0')
        return page

    def gate(self,page):
        return page.evaluate('()=>__cg29RenderLab.snapshot()')

    def test_real_controls_readiness_and_clean_png_survive_all_horizons(self):
        import tempfile
        from PIL import Image
        from capture_readiness import wait_for_render, geometry_from_state, verify_saved_render
        from market_vision.coinglass_heatmap_capture import _select_timeframe
        from model1_legend import prepare_legend_for_capture
        from types import SimpleNamespace
        for hours,colour in ((12,(18,52,86)),(24,(35,69,103)),(48,(52,86,120))):
            with self.subTest(hours=hours):
                page=self.page()
                with rendering.preparation(page,3):
                    self.assertTrue(self.gate(page)[0]['enabled'])
                    _select_timeframe(page,f'{hours}h')
                    self.assertEqual(prepare_legend_for_capture(page),'closed')
                    state=wait_for_render(page,f'{hours}h',3,2000)
                    self.assertTrue(state['ready'])
                self.assertFalse(self.gate(page)[0]['enabled'])
                self.assertEqual(page.evaluate('()=>app.ticker.count'),2)
                rect=geometry_from_state(state)
                with tempfile.TemporaryDirectory() as folder:
                    path=Path(folder)/'map.png'
                    with rendering.observation(page,3,rect) as clean:
                        clean();page.screenshot(path=str(path),full_page=False)
                        verify_saved_render(page,f'{hours}h',3,state,path,SimpleNamespace(summary=lambda:{}))
                    with Image.open(path) as image:
                        self.assertEqual(image.convert('RGB').getpixel((int(rect['x'])+10,int(rect['y'])+10)),colour)
                self.assertFalse(self.gate(page)[0]['enabled'])
                self.assertEqual(page.evaluate('()=>app.ticker.count'),2)

    def test_only_automatic_paints_are_limited_and_original_callbacks_resume(self):
        page=self.page()
        with rendering.preparation(page,3):
            page.wait_for_timeout(160)
            entry=self.gate(page)[0]
            self.assertGreater(entry['stats']['skipped_paints'],0)
            before=page.evaluate('()=>({data:dataUpdates,paints})')
            self.assertEqual(page.evaluate('()=>app.render()'),'drawn')
            after=page.evaluate('()=>({data:dataUpdates,paints})')
            self.assertGreater(after['paints'],before['paints'])
        before=page.evaluate('()=>({data:dataUpdates,paints})');page.wait_for_timeout(100)
        after=page.evaluate('()=>({data:dataUpdates,paints})')
        self.assertGreater(after['data'],before['data']);self.assertGreater(after['paints'],before['paints'])

    def test_exception_restores_then_the_next_cycle_can_run(self):
        page=self.page()
        with self.assertRaisesRegex(ValueError,'source_failure'):
            with rendering.preparation(page,3):raise ValueError('source_failure')
        self.assertFalse(self.gate(page)[0]['enabled'])
        with rendering.preparation(page,3):self.assertTrue(self.gate(page)[0]['enabled'])
        self.assertFalse(self.gate(page)[0]['enabled'])

    def test_scroll_uses_viewport_coordinates_and_resize_is_rebound_after_preparation(self):
        from capture_readiness import wait_for_render,geometry_from_state
        page=self.page()
        page.evaluate("()=>{document.body.style.height='2400px';document.querySelector('#panel').style.marginTop='800px';scrollTo(0,400)}")
        with rendering.preparation(page,3):
            self.assertTrue(self.gate(page)[0]['enabled'])
            page.evaluate("()=>document.querySelector('canvas').style.width='820px'")
        page.evaluate('()=>scrollTo(0,0)')
        state=wait_for_render(page,'48h',3,2000)
        with rendering.observation(page,3,geometry_from_state(state)):
            self.assertTrue(self.gate(page)[0]['enabled'])
        self.assertFalse(self.gate(page)[0]['enabled'])

    def test_unready_source_or_unknown_engine_preserves_original_path(self):
        for kind in ('version','nodata','ambiguous','source'):
            with self.subTest(kind=kind):
                page=self.page('9.0.0' if kind=='version' else '8.21.0')
                if kind=='nodata':page.evaluate("()=>document.querySelector('#panel').insertAdjacentHTML('beforeend','<div style=\"position:absolute;left:100px;top:180px\">No data</div>')")
                if kind=='ambiguous':page.evaluate("()=>document.querySelector('canvas').after(document.querySelector('canvas').cloneNode())")
                if kind=='source':page.evaluate("()=>history.replaceState(null,'','?coin=ETH&type=symbol')")
                with rendering.preparation(page,3):self.assertFalse(any(x['enabled'] for x in self.gate(page)))
                self.assertEqual(page.evaluate('()=>app.ticker.count'),2)

    def test_new_blocking_notice_still_fails_original_readiness(self):
        from capture_readiness import wait_for_render
        page=self.page()
        with rendering.preparation(page,3):
            page.evaluate("()=>document.querySelector('#panel').insertAdjacentHTML('beforeend','<div style=\"position:absolute;left:100px;top:180px\">No data</div>')")
            state=wait_for_render(page,'48h',3,100)
            self.assertFalse(state['ready']);self.assertEqual(state['reason'],'no-data')
        self.assertFalse(self.gate(page)[0]['enabled'])

    def test_replaced_renderer_is_rejected_not_accepted_as_the_same_chart(self):
        page=self.page()
        with self.assertRaisesRegex(Exception,'render_lab_chart_changed'):
            with rendering.preparation(page,3):page.evaluate('()=>app.renderer={...app.renderer}')


class InstallerTests(unittest.TestCase):
    def test_removing_only_new_scheduling_recovers_original_entire_capture_ast(self):
        target=HERE/'runtime/market_vision/coinglass_heatmap_capture.py'
        tree=ast.parse(target.read_text());counts={'import':0,'configure':0,'scope':0,'prepare':0,'early':0}
        class Strip(ast.NodeTransformer):
            def visit_Import(self,node):
                if len(node.names)==1 and node.names[0].name=='model3_rendering':
                    counts['import']+=1;return None
                return node
            def visit_Expr(self,node):
                if isinstance(node.value,ast.Call):
                    name=ast.unparse(node.value.func)
                    if name=='_model3_rendering.configure_context':counts['configure']+=1;return None
                    if name=='_prepare_model3_frame':counts['prepare']+=1;return None
                return self.generic_visit(node)
            def visit_With(self,node):
                self.generic_visit(node)
                if len(node.items)==1 and isinstance(node.items[0].context_expr,ast.Call) and ast.unparse(node.items[0].context_expr.func)=='_model3_rendering.preparation':
                    counts['early']+=1;return node.body
                if len(node.items)==1 and isinstance(node.items[0].context_expr,ast.Call) and ast.unparse(node.items[0].context_expr.func)=='_model3_rendering.observation':
                    counts['scope']+=1;return node.body
                return node
        restored=Strip().visit(tree)
        self.assertEqual(counts,{'import':1,'configure':1,'scope':1,'prepare':1,'early':1})
        self.assertEqual(ast_digest(restored),BASE_AST_SHA)
    def test_canonical_ast_keeps_empty_fields_and_detects_code_changes(self):
        original=ast.parse('def f(value):\n    return value + 1\n')
        changed=ast.parse('def f(value):\n    return value + 2\n')
        self.assertNotEqual(ast_digest(original),ast_digest(changed))
        self.assertIn(['decorator_list',[]],canonical_ast(original.body[0])[1])
        original.body[0].type_params=[]
        self.assertEqual(ast_digest(original),ast_digest(ast.parse('def f(value):\n    return value + 1\n')))
    def test_double_install_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError,'already_installed'):
            expand((HERE/'runtime/market_vision/coinglass_heatmap_capture.py').read_text())
    def test_render_gate_and_clean_frame_are_identical_to_accepted_laboratory(self):
        expected={'model3_render_gate.js':'8c21349c2a562e77cafb096dd3a62ded6213b5dd',
                  'model3_clean_frame.py':'30fdf47b2066bb20a9eb5e2fa7a8e9aec3bc9dee'}
        for name,sha in expected.items():
            data=(HERE/'runtime'/name).read_bytes()
            self.assertEqual(hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest(),sha,name)
    def test_original_numerical_reader_is_byte_identical(self):
        self.assertEqual(hashlib.sha256((HERE/'runtime/heatmap_hover.py').read_bytes()).hexdigest(),
            '693aa6739a8b1f50d0550cd802dde99cff4c68edfad6c643c1adf8d6bc05f820')


if __name__=='__main__':unittest.main()
