"""Failure facts are bounded and private, while source errors and controls stay intact."""
import ast
import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import MagicMock,Mock,patch

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE/'runtime'))
import capture_failure_observer as observer
import model1_execution as execution
from model1_evidence_format import evidence_payload
from market_vision import coinglass_heatmap_capture as capture

MODEL3='https://www.coinglass.com/liquidation-heatmap-model3?coin=BTC&type=symbol'
STATE={'heading_visible':True,'chart_present':True,'label':'48 hour','trigger_present':True,
    'trigger_visible':True,'trigger_disabled':False,'trigger_hit_target':False,
    'model_selected':True,'symbol_selected':True,'login_gate_visible':False,
    'dialog_visible':True,'loading_visible':False,'challenge_visible':False,'reason':'visible-dialog'}


class ObserverTests(unittest.TestCase):
    def page(self,state=STATE):
        page=Mock(url=MODEL3)
        page.wait_for_function.return_value.json_value.return_value=state
        return page

    def test_one_bounded_query_keeps_only_fixed_state_and_passive_counts(self):
        page=self.page(dict(STATE,url='PRIVATE',cookie='PRIVATE',label='48 hour'))
        network=Mock();network.summary.return_value={'http_errors':[{'host_group':'coinglass','status':403,'count':2,'url':'PRIVATE'}]}
        error=TimeoutError('PRIVATE intercepts pointer events https://private.invalid?token=PRIVATE')
        observer.observe_capture_failure(page,3,network,error)
        page.wait_for_function.assert_called_once_with(observer.STATE_SCRIPT,arg=3,timeout=1500,polling=1000)
        page.evaluate.assert_not_called();page.screenshot.assert_not_called();page.goto.assert_not_called()
        page.get_by_role.assert_not_called();page.locator.assert_not_called()
        self.assertEqual(error._model1_source_capture_state['source_identity'],'canonical')
        self.assertEqual(error._model1_click_failure,'pointer_intercepted')
        self.assertEqual(error._model1_source_network['http_errors'],[{'host_group':'coinglass','status':403,'count':2}])
        self.assertNotIn('PRIVATE',json.dumps(execution.failure_detail(error)))
        page.wait_for_function.return_value.dispose.assert_called_once()

    def test_observer_timeout_keeps_original_exception_and_passive_counts(self):
        page=self.page();page.wait_for_function.side_effect=TimeoutError('PRIVATE observer')
        network=Mock();network.summary.return_value={'page_error_count':3}
        error=TimeoutError('PRIVATE original');cause=RuntimeError('PRIVATE cause');error.__cause__=cause
        observer.observe_capture_failure(page,3,network,error)
        self.assertIs(error.__cause__,cause)
        self.assertEqual(error._model1_source_capture_state,{'observation_status':'unavailable','source_identity':'canonical','reason':'unverified'})
        self.assertEqual(error._model1_source_network['page_error_count'],3)
        self.assertNotIn('PRIVATE',json.dumps(execution.failure_detail(error)))

    def test_exact_source_scope_and_untrusted_values_are_sanitized(self):
        self.assertEqual(observer.source_identity(MODEL3,3),'canonical')
        self.assertEqual(observer.source_identity(MODEL3,1),'invalid')
        self.assertEqual(observer.source_identity(MODEL3.replace('BTC','ETH'),3),'invalid')
        self.assertEqual(observer.source_identity('https://www.coinglass.com/pro/futures/LiquidationHeatMap?coin=BTC&type=symbol',1),'legacy')
        state=observer.safe_failure_state({'source_identity':[],'reason':{},'label':'PRIVATE','heading_visible':1,'cookie':'PRIVATE'})
        self.assertEqual(state,{'observation_status':'unavailable','source_identity':'unavailable','reason':'unverified'})

    def test_failure_classifier_never_returns_error_text(self):
        markers={'Element is not visible':'not_visible','Element is outside of the viewport':'outside_viewport',
            'element is not stable':'unstable','intercepts pointer events':'pointer_intercepted',
            'Element is not enabled':'disabled','Element was detached from the DOM':'detached',
            'Target page, context or browser has been closed':'target_closed'}
        for marker,wanted in markers.items():
            wrapped=RuntimeError('PRIVATE');wrapped.__cause__=TimeoutError(marker+' PRIVATE')
            self.assertEqual(observer.click_failure(wrapped),wanted)
        self.assertEqual(observer.click_failure(TimeoutError('PRIVATE')),'action_timeout')
        self.assertEqual(observer.click_failure(ValueError('PRIVATE')),'unclassified')

    def test_maximum_capture_failure_report_fits_existing_four_kib_limit(self):
        original=RuntimeError('Timeframe selection did not settle on PRIVATE')
        original.__cause__=type('TargetClosedError',(Exception,),{})('PRIVATE')
        original._model1_capture_phase=max(execution.PHASES,key=len)
        original._model1_source_capture_state={'observation_status':'unavailable','source_identity':'unavailable',
            'reason':'loading-indicator','label':'48 hour',**{k:False for k in observer.BOOL_FIELDS}}
        original._model1_source_readiness={'ready':True,'reason':'render-checks-passed','label':'48 hour',
            'login_link_visible':False,'account_link_present':False}
        original._model1_click_failure='pointer_intercepted'
        original._model1_source_network={'http_errors':[{'host_group':'coinglass','status':599,'count':1000} for _ in range(6)],
            'network_errors':[{'host_group':'coinglass','kind':'connection','count':1000} for _ in range(6)],'page_error_count':1000}
        def main(timeframe,job_id,output):
            execution.stage('capture')
            execution._capture_events.extend({'phase':max(execution.PHASES,key=len),'status':'completed',
                'duration_seconds':359.99999999999999} for _ in range(24))
            raise original
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(execution.run_task(main,'24H','synthetic',directory),1)
            encoded=(Path(directory)/'error.json').read_bytes()
            self.assertLessEqual(len(encoded),4096)
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(execution.read_failure(Path(directory)/'error.json','synthetic',1),'source_capture_failed')
            image,evidence=evidence_payload(directory,'24H')
            self.assertIsNone(image);self.assertEqual(len(evidence['source_network']['http_errors']),6)
            self.assertEqual(evidence['source_capture_state']['reason'],'loading-indicator')

    def test_no_image_failure_facts_survive_log_and_private_evidence(self):
        page=self.page();network=Mock();network.summary.return_value={'http_errors':[{'host_group':'coinglass','status':503,'count':1}]}
        original=TimeoutError('PRIVATE intercepts pointer events')
        def main(timeframe,job_id,output):
            execution.stage('capture');observer.observe_capture_failure(page,3,network,original);raise original
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(execution.run_task(main,'24H','synthetic',directory),1)
            encoded=(Path(directory)/'error.json').read_bytes()
            self.assertLessEqual(len(encoded),4096)
            report=json.loads(encoded);self.assertEqual(report['code'],'source_timeout')
            with contextlib.redirect_stdout(io.StringIO()) as stdout:
                self.assertEqual(execution.read_failure(Path(directory)/'error.json','synthetic',1),'source_timeout')
            logged=json.loads(stdout.getvalue().removeprefix('MODEL1_JOB_FAILURE '))
            image,evidence=evidence_payload(directory,'24H')
            self.assertIsNone(image);self.assertFalse(evidence['image_retained']);self.assertFalse(evidence['assessment_validated'])
            for facts in (logged,evidence):
                self.assertEqual(facts['source_capture_state']['label'],'48 hour')
                self.assertEqual(facts['click_failure'],'pointer_intercepted')
                self.assertEqual(facts['source_network']['http_errors'][0]['status'],503)
                self.assertNotIn('PRIVATE',json.dumps(facts))

    def capture_mocks(self,page):
        driver=MagicMock();browser=driver.__enter__.return_value.chromium.launch.return_value
        browser.new_context.return_value.new_page.return_value=page
        return driver

    def patches(self,page,driver):
        stack=contextlib.ExitStack()
        stack.enter_context(patch.object(capture,'sync_playwright',return_value=driver))
        for name in ('_load_authenticated_storage_state','_load_cookie_header'):
            stack.enter_context(patch.object(capture,name,return_value=None))
        for name in ('_dismiss_capture_blockers','_wait_for_heatmap','_select_model_one','_select_symbol_mode'):
            stack.enter_context(patch.object(capture,name))
        return stack

    def test_failure_observed_before_driver_closes_without_extra_image(self):
        page=self.page();page.evaluate.return_value=False;driver=self.capture_mocks(page)
        original=TimeoutError('PRIVATE intercepts pointer events')
        observed=[]
        def watch(*args):
            self.assertFalse(driver.__exit__.called);observed.append(args[-1]);observer.observe_capture_failure(*args)
        with tempfile.TemporaryDirectory() as directory,self.patches(page,driver),patch.object(capture,'HEATMAP_MODEL',3),patch.object(capture,'_select_timeframe',side_effect=original) as selector,patch.object(capture,'observe_capture_failure',side_effect=watch):
            selector.__name__='_select_timeframe'
            with self.assertRaises(TimeoutError) as caught:capture.capture_heatmaps(directory,timeframes=('24h',),url=MODEL3)
        self.assertIs(caught.exception,original);self.assertEqual(observed,[original])
        self.assertTrue(driver.__exit__.called);page.screenshot.assert_not_called()

    def test_unexpected_observer_error_cannot_replace_original(self):
        page=self.page();page.evaluate.return_value=False;driver=self.capture_mocks(page)
        original=TimeoutError('PRIVATE original')
        with tempfile.TemporaryDirectory() as directory,self.patches(page,driver),patch.object(capture,'HEATMAP_MODEL',3),patch.object(capture,'_select_timeframe',side_effect=original) as selector,patch.object(capture,'observe_capture_failure',side_effect=ValueError('PRIVATE observer')):
            selector.__name__='_select_timeframe'
            with self.assertRaises(TimeoutError) as caught:capture.capture_heatmaps(directory,timeframes=('24h',),url=MODEL3)
        self.assertIs(caught.exception,original);page.screenshot.assert_not_called()

    def test_success_has_zero_observer_calls_and_one_original_image(self):
        page=self.page();page.evaluate.return_value=False;driver=self.capture_mocks(page)
        with tempfile.TemporaryDirectory() as directory,self.patches(page,driver),patch.object(capture,'HEATMAP_MODEL',3),patch.object(capture,'_select_timeframe') as selector,patch('model1_legend.prepare_legend_for_capture'),patch.object(capture,'wait_for_render',return_value={'ready':True,'reason':'render-checks-passed','label':'24 hour'}),patch.object(capture,'verify_saved_render'),patch.object(capture,'observe_capture_failure') as watch:
            selector.__name__='_select_timeframe'
            result=capture.capture_heatmaps(directory,timeframes=('24h',),url=MODEL3)
        self.assertEqual(len(result),1);watch.assert_not_called();page.screenshot.assert_called_once()
        self.assertEqual(page.screenshot.call_args.kwargs['full_page'],True)

    def test_adapter_has_one_error_observer_and_one_existing_screenshot(self):
        tree=ast.parse((HERE/'runtime/market_vision/coinglass_heatmap_capture.py').read_text())
        function=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='capture_heatmaps')
        calls=[n for n in ast.walk(function) if isinstance(n,ast.Call)]
        self.assertEqual(sum(ast.unparse(n.func)=='observe_capture_failure' for n in calls),1)
        self.assertEqual(sum(ast.unparse(n.func)=='page.screenshot' for n in calls),1)
        observers=[node for node in ast.walk(function) if isinstance(node,ast.ExceptHandler)
            and any(isinstance(call,ast.Call) and ast.unparse(call.func)=='observe_capture_failure' for call in ast.walk(node))]
        self.assertEqual(len(observers),1);self.assertIsInstance(observers[0].body[-1],ast.Raise)
        self.assertIsNone(observers[0].body[-1].exc)


class SyntheticBrowserTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from playwright.sync_api import sync_playwright
        cls.driver=sync_playwright().start()
        try:cls.browser=cls.driver.chromium.launch(headless=True)
        except Exception:
            cls.driver.stop();raise unittest.SkipTest('Chromium is not installed locally')

    @classmethod
    def tearDownClass(cls):cls.browser.close();cls.driver.stop()

    def page(self,html):
        context=self.browser.new_context(viewport={'width':1600,'height':1000});page=context.new_page()
        page.route('**/*',lambda route:route.fulfill(status=200,content_type='text/html',
            body=html if route.request.url==MODEL3 else '<!doctype html><html></html>'))
        page.goto(MODEL3);self.addCleanup(context.close);return page

    def test_observed_blocking_dialog_keeps_exact_model_and_unmodified_controls(self):
        page=self.page('''<h1>BTC Liquidation Heatmap</h1><button role="tab" aria-selected="true">Model 3</button>
            <button role="tab" aria-selected="true">Symbol</button><button role="combobox">48 hour</button>
            <canvas width="800" height="400"></canvas><div role="dialog" style="position:fixed;inset:0;background:white">
            Log in to unlock full data <button>Close</button></div>''')
        error=TimeoutError('intercepts pointer events');network=Mock();network.summary.return_value={}
        observer.observe_capture_failure(page,3,network,error)
        state=error._model1_source_capture_state
        self.assertEqual(state['reason'],'login-required');self.assertEqual(state['label'],'48 hour')
        self.assertTrue(state['heading_visible']);self.assertTrue(state['chart_present'])
        self.assertTrue(state['model_selected']);self.assertTrue(state['symbol_selected'])
        self.assertFalse(state['trigger_hit_target']);self.assertTrue(page.get_by_role('dialog').is_visible())
        self.assertEqual(page.get_by_role('combobox').inner_text(),'48 hour')

    def test_visible_challenge_is_observed_but_asset_alone_is_not_a_gate(self):
        for hidden in ('',' style="display:none"'):
            page=self.page('<h1>BTC Liquidation Heatmap</h1><button role="combobox">24 hour</button><canvas width="800" height="400"></canvas><iframe src="https://challenges.cloudflare.com/private?token=PRIVATE"'+hidden+'></iframe>')
            error=TimeoutError('PRIVATE');network=Mock();network.summary.return_value={}
            observer.observe_capture_failure(page,3,network,error)
            state=error._model1_source_capture_state
            self.assertEqual(state['challenge_visible'],not bool(hidden))
            self.assertNotIn('PRIVATE',json.dumps(state))

    def test_btc_combobox_does_not_count_as_a_timeframe_trigger(self):
        page=self.page('<h1>BTC Liquidation Heatmap</h1><button role="combobox">BTC</button><canvas width="800" height="400"></canvas>')
        error=TimeoutError('PRIVATE');network=Mock();network.summary.return_value={}
        observer.observe_capture_failure(page,3,network,error)
        state=error._model1_source_capture_state
        self.assertFalse(state['trigger_present']);self.assertFalse(state['trigger_visible'])
        self.assertNotIn('label',state)


if __name__=='__main__':unittest.main()
