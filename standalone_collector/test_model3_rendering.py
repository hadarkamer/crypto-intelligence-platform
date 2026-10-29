"""No account, database or source access: integration contract and lifecycle."""
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
# Full pristine capture was independently verified as SHA256 6b05e7dd...e40b35.
# ast.dump omits empty lists by default on Python 3.13, unlike CI's 3.11.
# Serialize every field ourselves, normalizing only absent empty type_params.
BASE_AST_SHA='ff5a666e1e7b2274ad30c0584f3a4ba30521e7b604f2bef8b16b68ab55588c4c'


def canonical_ast(value):
    if isinstance(value,ast.AST):
        return [type(value).__name__,[[name,canonical_ast(field)]
            for name,field in ast.iter_fields(value)
            if not(name=='type_params' and field==[])]]
    if isinstance(value,list):return [canonical_ast(item) for item in value]
    return value


def ast_digest(tree):
    return hashlib.sha256(json.dumps(canonical_ast(tree),
        separators=(',',':'),ensure_ascii=True).encode()).hexdigest()


class Context:
    def __init__(self):self.scripts=[];self.routes={}
    def add_init_script(self,**kwargs):self.scripts.append(kwargs)
    def route(self,url,fn):self.routes[url]=fn


class Page:
    def __init__(self):
        self.context=Context();self.calls=[];self.enabled=False;self.restore_fail=False
        setattr(self.context,rendering._STATE,{'chart':True,'pixi':True,'reason':'verified'})
        self.entry={'key':1,'enabled':False,'ticker_count':2,
                    'canvas':{'rect':{k:GEOMETRY[k] for k in ('x','y','width','height')}}}
    def evaluate(self,script,arg=None):
        self.calls.append((script,arg))
        if 'snapshot()' in script:return [{**self.entry,'enabled':self.enabled}]
        if '.enable(' in script:
            self.enabled=True;return {**self.entry,'enabled':True}
        if '.disable(' in script:
            if self.restore_fail:raise RuntimeError('synthetic_restore_failure')
            self.enabled=False;return {**self.entry,'enabled':False}
        if '.forcePaint(' in script:return {'key':1,'forced_paints':1}
        raise AssertionError('Unexpected browser call')


class RenderingTests(unittest.TestCase):
    def test_other_models_are_exact_noops(self):
        for model in (1,2):
            rendering.configure_context(object(),model)
            with rendering.observation(object(),model,{}) as prepare:
                self.assertIsNone(prepare())
    def test_context_has_only_two_public_asset_routes(self):
        context=Context();rendering.configure_context(context,3)
        self.assertEqual(set(context.routes),{rendering.CHART_URL,rendering.PIXI_URL})
        self.assertEqual(len(context.scripts),1)
        self.assertTrue(Path(context.scripts[0]['path']).is_file())
        with self.assertRaisesRegex(RuntimeError,'already_configured'):
            rendering.configure_context(context,3)
    def test_guarded_patch_is_one_inert_attachment_and_rejects_drift(self):
        fixture=b'before;'+rendering.MARKER+b'after;'
        with patch.object(rendering,'CHART_SHA',hashlib.sha256(fixture).hexdigest()):
            actual=rendering.patch_public_chart(fixture)
            self.assertEqual(actual.replace(rendering.INSERT,b'',1),fixture)
            self.assertNotIn(b'.enable(',actual)
            self.assertIsNone(rendering.patch_public_chart(fixture+b' '))
        duplicate=fixture+rendering.MARKER
        with patch.object(rendering,'CHART_SHA',hashlib.sha256(duplicate).hexdigest()):
            self.assertIsNone(rendering.patch_public_chart(duplicate))
    def test_unknown_chart_and_pixi_are_returned_unmodified(self):
        context=Context();rendering.configure_context(context,3)
        for url in (rendering.CHART_URL,rendering.PIXI_URL):
            response=types.SimpleNamespace(status=200,headers={'content-type':'application/javascript','content-encoding':'gzip','etag':'old'},body=lambda:b'new-source-version')
            class Route:
                request=types.SimpleNamespace(url=url)
                def fetch(self,**kwargs):return response
                def fulfill(self,**kwargs):self.output=kwargs
            route=Route();context.routes[url](route)
            self.assertEqual(route.output['body'],b'new-source-version')
            self.assertNotIn('content-encoding',route.output['headers'])
        state=getattr(context,rendering._STATE)
        self.assertFalse(state['chart']);self.assertFalse(state['pixi'])
    def test_fetch_failure_leaves_normal_browser_path_without_retry(self):
        context=Context();rendering.configure_context(context,3)
        class Route:
            request=types.SimpleNamespace(url=rendering.CHART_URL)
            count=0;continued=False
            def fetch(self,**kwargs):self.count+=1;raise TimeoutError('private not logged')
            def continue_(self):self.continued=True
        route=Route();context.routes[rendering.CHART_URL](route)
        self.assertEqual(route.count,1);self.assertTrue(route.continued)
    def test_unrecognized_version_never_activates_hook(self):
        page=Page();setattr(page.context,rendering._STATE,{'chart':True,'pixi':False,'reason':'pixi_version_unrecognized'})
        with rendering.observation(page,3,GEOMETRY) as prepare:self.assertIsNone(prepare())
        self.assertEqual(page.calls,[])
    def test_missing_or_wrong_chart_keeps_original(self):
        for mode in ('missing','geometry'):
            page=Page()
            if mode=='missing':page.evaluate=lambda *args:None
            else:page.entry['canvas']['rect']['x']+=1
            with rendering.observation(page,3,GEOMETRY) as prepare:self.assertIsNone(prepare())
            self.assertFalse(page.enabled)
    def test_scope_activates_clean_capture_and_always_restores(self):
        page=Page();seen=[]
        def clean(target,paint):
            seen.append(target);self.assertTrue(page.enabled);paint(target)
            return {'fresh_paint':True}
        with patch.dict(sys.modules,{'model3_clean_frame':types.SimpleNamespace(prepare_frame=clean)}):
            with rendering.observation(page,3,GEOMETRY) as prepare:
                self.assertTrue(prepare()['fresh_paint']);self.assertTrue(page.enabled)
        self.assertEqual(seen,[page]);self.assertFalse(page.enabled)
        self.assertEqual(sum('.enable(' in script for script,_ in page.calls),1)
        self.assertEqual(sum('.disable(' in script for script,_ in page.calls),1)
    def test_source_failure_is_preserved_and_restores(self):
        page=Page()
        with self.assertRaisesRegex(ValueError,'original_source_failure'):
            with rendering.observation(page,3,GEOMETRY):raise ValueError('original_source_failure')
        self.assertFalse(page.enabled)
    def test_restore_failure_rejects_success_without_masking_original_failure(self):
        page=Page();page.restore_fail=True
        with self.assertRaisesRegex(RuntimeError,'synthetic_restore_failure'):
            with rendering.observation(page,3,GEOMETRY):pass
        page=Page();page.restore_fail=True
        with self.assertRaisesRegex(ValueError,'original_source_failure'):
            with rendering.observation(page,3,GEOMETRY):raise ValueError('original_source_failure')
    def test_already_active_hook_is_not_silently_reused(self):
        page=Page();page.enabled=True
        with self.assertRaisesRegex(RuntimeError,'unexpected_active_hook'):
            with rendering.observation(page,3,GEOMETRY):pass


class InstallerTests(unittest.TestCase):
    def test_removing_only_new_scheduling_recovers_original_entire_capture_ast(self):
        target=HERE/'runtime/market_vision/coinglass_heatmap_capture.py'
        tree=ast.parse(target.read_text());counts={'import':0,'configure':0,'scope':0,'prepare':0}
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
                if len(node.items)==1 and isinstance(node.items[0].context_expr,ast.Call) and ast.unparse(node.items[0].context_expr.func)=='_model3_rendering.observation':
                    counts['scope']+=1;return node.body
                return node
        restored=Strip().visit(tree)
        self.assertEqual(counts,{'import':1,'configure':1,'scope':1,'prepare':1})
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
