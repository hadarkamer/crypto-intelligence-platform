"""Synthetic DOM and actual installed failure path; no external source or AI calls."""
import ast
from collections import Counter
import contextlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import MagicMock,Mock,patch

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE/'runtime'))
import capture_readiness as readiness
import model1_execution as execution
from heatmap_models import source_url


def state(reason='render-checks-passed',label='12 hour'):
    return {'ready':reason=='render-checks-passed','reason':reason,'label':label,
        'login_link_visible':True,'account_link_present':False}


class ReadinessTests(unittest.TestCase):
    def page(self,states):
        page=Mock();page.url=source_url(1);page.evaluate.side_effect=states
        return page

    def test_ready_does_not_wait_or_require_account_marker(self):
        page=self.page([state()])
        result=readiness.wait_for_render(page,'12h',1)
        self.assertTrue(result['ready']);self.assertTrue(result['login_link_visible'])
        page.wait_for_function.assert_not_called()
        page.goto.assert_not_called();page.reload.assert_not_called()

    def test_loading_waits_for_real_dom_readiness(self):
        page=self.page([state('loading-indicator'),state()])
        self.assertTrue(readiness.wait_for_render(page,'12h',1,999999)['ready'])
        self.assertEqual(page.wait_for_function.call_args.kwargs['timeout'],60000)
        self.assertEqual(page.wait_for_function.call_args.kwargs['arg'],'12 hour')
        page.wait_for_timeout.assert_called_once_with(1000)

    def test_unfinished_loading_timeout_stays_explicit(self):
        page=self.page([state('loading-indicator'),state('loading-indicator')])
        page.wait_for_function.side_effect=TimeoutError('PRIVATE')
        self.assertEqual(readiness.wait_for_render(page,'12h',1)['reason'],'loading-indicator')

    def test_all_models_horizons_keep_strict_source_identity(self):
        for model in (1,2,3):
            for hours in (12,24,48):
                page=Mock(url=source_url(model));page.evaluate.return_value=state(label=f'{hours} hour')
                with self.subTest(model=model,hours=hours):
                    self.assertTrue(readiness.wait_for_render(page,f'{hours}h',model)['ready'])
            self.assertFalse(readiness.valid_source(source_url(model).replace('coin=BTC','coin=ETH'),model))
            self.assertFalse(readiness.valid_source(source_url(model)+'&coin=BTC',model))
        self.assertFalse(readiness.valid_source(source_url(2),1))
        self.assertFalse(readiness.valid_source(source_url(1).replace('https:','http:'),1))

    def test_wrong_source_never_waits(self):
        page=Mock(url='https://evil.example/?coin=BTC&type=symbol')
        self.assertEqual(readiness.wait_for_render(page,'12h',1)['reason'],'invalid-source')
        page.evaluate.assert_not_called();page.wait_for_function.assert_not_called()

    def test_network_summary_omits_secrets_and_intentional_ad_abort(self):
        page=Mock();diagnostic=readiness.CaptureNetworkDiagnostics(page)
        diagnostic.response_seen(Mock(url='https://api.coinglass.com/private?token=PRIVATE',status=403))
        diagnostic.request_failed(Mock(url='https://doubleclick.net/private',failure='net::ERR_ABORTED'))
        diagnostic.request_failed(Mock(url='https://api.coinglass.com/private?cookie=PRIVATE',failure='PRIVATE'))
        diagnostic.page_error(RuntimeError('PRIVATE'))
        summary=diagnostic.summary()
        self.assertEqual(summary['http_errors'],[{'host_group':'coinglass','count':1,'status':403}])
        self.assertEqual(summary['network_errors'],[{'host_group':'coinglass','count':1,'kind':'other'}])
        self.assertEqual(summary['page_error_count'],1)
        self.assertNotIn('PRIVATE',json.dumps(summary))

    def test_saved_frame_rechecked_without_second_screenshot(self):
        page=self.page([state('loading-indicator')])
        with tempfile.TemporaryDirectory() as directory:
            image=Path(directory)/'coinglass_btc_heatmap_12h.png';image.write_bytes(b'original')
            with self.assertRaises(execution.StageFailure) as failure:
                readiness.verify_saved_render(page,'12h',1,state(),image,Mock(summary=lambda:{}))
            self.assertEqual(failure.exception.code,'source_not_readable')
            self.assertEqual(image.read_bytes(),b'original');page.screenshot.assert_not_called()


class InstalledGateTests(unittest.TestCase):
    def exercise_failure(self,reason):
        from market_vision import coinglass_heatmap_capture as capture,openai_heatmap_scanner as scanner
        import collection_model1_task as task
        from model1_evidence_format import evidence_payload
        driver=MagicMock();driver.__enter__.return_value=driver
        browser=driver.chromium.launch.return_value;context=browser.new_context.return_value
        page=context.new_page.return_value;page.url=source_url(1)
        page.evaluate.side_effect=lambda script: state(reason) if script==readiness.STATE_SCRIPT else {
            'x':0,'y':20,'width':800,'height':600,'page_width':1000}
        page.wait_for_function.side_effect=TimeoutError('PRIVATE')
        png=b'\x89PNG\r\n\x1a\noriginal-fixture'
        page.screenshot.side_effect=lambda *,path,full_page: Path(path).write_bytes(png)
        callbacks={}
        page.on.side_effect=lambda event,callback:callbacks.__setitem__(event,callback)
        def navigate(*args,**kwargs):
            callbacks['response'](Mock(url='https://api.coinglass.com/data?token=PRIVATE',status=503))
        page.goto.side_effect=navigate
        with tempfile.TemporaryDirectory() as directory,patch.dict(os.environ,{
            'COINGLASS_STORAGE_STATE_JSON':'','COINGLASS_COOKIE_HEADER':''}),\
            patch.object(capture,'sync_playwright',return_value=driver),\
            patch.object(capture,'_dismiss_capture_blockers'),patch.object(capture,'_wait_for_heatmap'),\
            patch.object(capture,'_select_model_one'),patch.object(capture,'_select_symbol_mode'),\
            patch.object(capture,'_select_timeframe',new=lambda *args:None),\
            patch('model1_legend.prepare_legend_for_capture'),\
            patch.object(scanner,'analyze_heatmap_images') as ai:
            self.assertEqual(execution.run_task(task.main,'12H','job',directory),1)
            ai.assert_not_called();page.screenshot.assert_called_once()
            report=json.loads((Path(directory)/'error.json').read_text())
            self.assertEqual(report['code'],'source_not_readable')
            self.assertEqual(report['capture_phase'],'render_readiness')
            self.assertEqual(report['capture_operations'][-1]['phase'],'render_readiness')
            self.assertEqual(report['capture_operations'][-1]['status'],'failed')
            self.assertEqual(report['capture_operations'][-2]['phase'],'screenshot')
            self.assertEqual(report['capture_operations'][-2]['status'],'completed')
            self.assertEqual(report['source_readiness']['reason'],reason)
            self.assertIsNone(report['usage']);self.assertFalse((Path(directory)/'result.json').exists())
            image,diagnostic=evidence_payload(directory,'12H')
            self.assertEqual(image,png);self.assertFalse(diagnostic['assessment_validated'])
            self.assertEqual(diagnostic['source_readiness']['reason'],reason)
            self.assertEqual(diagnostic['source_network']['http_errors'][0]['status'],503)
            with contextlib.redirect_stdout(io.StringIO()) as output:
                execution.read_failure(Path(directory)/'error.json','job',1)
            self.assertIn(reason,output.getvalue());self.assertNotIn('PRIVATE',output.getvalue())
            self.assertLess((Path(directory)/'error.json').stat().st_size,4096)

    def test_loading_never_calls_paid_analysis_and_retains_frame(self):self.exercise_failure('loading-indicator')
    def test_blur_never_calls_paid_analysis_and_retains_frame(self):self.exercise_failure('blur')

    def test_installed_gate_preserves_original_screenshot_call(self):
        from install_capture_diagnostics import expand_capture_diagnostics
        from install_capture_readiness import expand_capture_readiness
        original=(HERE.parent/'market_vision/coinglass_heatmap_capture.py').read_text()
        def shots(text):
            return Counter(ast.dump(n,include_attributes=False) for n in ast.walk(ast.parse(text))
                if isinstance(n,ast.Call) and ast.unparse(n.func)=='page.screenshot')
        adapted=(HERE/'runtime/market_vision/coinglass_heatmap_capture.py').read_text()
        self.assertEqual(shots(original),shots(adapted))


class RenderDOMTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from playwright.sync_api import sync_playwright
        cls.driver=sync_playwright().start()
        try:cls.browser=cls.driver.chromium.launch(headless=True)
        except Exception:
            cls.driver.stop();raise unittest.SkipTest('Chromium is not installed locally; synthetic DOM runs during deployment build')

    @classmethod
    def tearDownClass(cls):cls.browser.close();cls.driver.stop()

    def check(self,body):
        page=self.browser.new_page(viewport={'width':1000,'height':800})
        try:
            page.set_content('<button role="combobox">48 hour</button><a href="/login">Login</a>'+body)
            return page.evaluate(readiness.STATE_SCRIPT)
        finally:page.close()

    def test_spinning_loading_overlay_is_rejected(self):
        s=self.check('<div style="position:relative;width:800px;height:500px"><canvas width="800" height="500"></canvas><span class="MuiCircularProgress-root" style="position:absolute;top:200px;left:300px;width:40px;height:40px"></span></div>')
        self.assertEqual(s['reason'],'loading-indicator');self.assertFalse(s['ready'])

    def test_ancestor_blur_is_rejected(self):
        s=self.check('<div style="filter:blur(4px)"><canvas width="800" height="500"></canvas></div>')
        self.assertEqual(s['reason'],'blur')

    def test_sibling_blur_veil_is_rejected(self):
        s=self.check('<div style="position:relative;width:800px;height:500px"><canvas width="800" height="500"></canvas><div style="position:absolute;inset:0;backdrop-filter:blur(4px)"></div></div>')
        self.assertEqual(s['reason'],'blur')

    def test_unrelated_spinner_and_header_login_do_not_reject_chart(self):
        s=self.check('<canvas width="800" height="500"></canvas><span role="progressbar" style="position:absolute;top:650px;width:30px;height:30px"></span>')
        self.assertTrue(s['ready']);self.assertEqual(s['label'],'48 hour');self.assertTrue(s['login_link_visible'])


if __name__=='__main__':unittest.main()
