"""Offline asset identity and clean-frame browser checks; no account or source scans."""
import hashlib
from pathlib import Path
import unittest
import candidate
from test_clean_frame import CleanFrameTests

ROOT=Path('/tmp/cg29-m3-review')
CHART=(ROOT/'69001-464e676a1cd1221f.js').read_bytes()
PIXI=(ROOT/'10679-f5d0c63436a34cc4.js').read_bytes()


class Context:
    def __init__(self):self.scripts=[];self.routes={}
    def add_init_script(self,**kwargs):self.scripts.append(kwargs)
    def route(self,url,handler):self.routes[url]=handler


class Response:
    status=200
    headers={'content-encoding':'gzip','content-length':'123','etag':'old','content-type':'application/javascript','access-control-allow-origin':'*'}
    def __init__(self,body):self._body=body
    def body(self):return self._body


class Route:
    def __init__(self,url,body):
        self.request=type('Request',(),{'url':url})();self.response=Response(body)
        self.fulfilled=None;self.aborted=False;self.fetch_args=None
    def fetch(self,**kwargs):self.fetch_args=kwargs;return self.response
    def fulfill(self,**kwargs):self.fulfilled=kwargs
    def abort(self):self.aborted=True


class CandidateTests(unittest.TestCase):
    def test_exact_public_assets_and_single_inert_insertion(self):
        self.assertEqual(hashlib.sha256(CHART).hexdigest(),candidate.ASSET_SHA256)
        self.assertEqual(hashlib.sha256(PIXI).hexdigest(),candidate.PIXI_SHA256)
        patched=candidate.patched_asset(CHART)
        self.assertEqual(patched.count(candidate.INSERT),1)
        self.assertEqual(patched.replace(candidate.INSERT,b'',1),CHART)
        self.assertNotIn(b'__cg29RenderLab.enable',patched)
        (Path(__file__).parent/'patched_chart_for_syntax.js').write_bytes(patched)

    def test_source_drift_rejected(self):
        with self.assertRaisesRegex(ValueError,'hash_changed'):
            candidate.patched_asset(CHART+b' ')

    def test_routes_verify_both_assets_and_preserve_other_headers(self):
        context=Context();state=candidate.install_context(context)
        self.assertEqual(set(context.routes),{candidate.ASSET_URL,candidate.PIXI_URL})
        self.assertTrue(Path(context.scripts[0]['path']).is_file())
        for url,body in ((candidate.ASSET_URL,CHART),(candidate.PIXI_URL,PIXI)):
            route=Route(url,body);context.routes[url](route)
            self.assertFalse(route.aborted);self.assertIsNotNone(route.fulfilled)
            self.assertEqual(route.fetch_args,{'timeout':20000,'max_redirects':0,'max_retries':0})
            self.assertNotIn('content-encoding',route.fulfilled['headers'])
            self.assertNotIn('content-length',route.fulfilled['headers'])
            self.assertEqual(route.fulfilled['headers']['access-control-allow-origin'],'*')
            if url==candidate.PIXI_URL:self.assertEqual(route.fulfilled['body'],PIXI)
        self.assertEqual(state['failures'],[]);self.assertTrue(state['chart_seen']);self.assertTrue(state['pixi_seen'])

    def test_changed_chart_or_pixi_aborts_without_fallback(self):
        for url,body in ((candidate.ASSET_URL,CHART),(candidate.PIXI_URL,PIXI)):
            context=Context();state=candidate.install_context(context)
            route=Route(url,body+b' ');context.routes[url](route)
            self.assertTrue(route.aborted);self.assertIsNone(route.fulfilled)
            self.assertEqual(len(state['failures']),1)
            self.assertFalse(state['chart_seen']);self.assertFalse(state['pixi_seen'])

    def test_unverified_assets_cannot_be_declared_ready(self):
        with self.assertRaisesRegex(ValueError,'assets_not_verified'):
            candidate.require_ready(object(),{'chart_seen':False,'pixi_seen':False,'failures':[]})


if __name__=='__main__':unittest.main()
