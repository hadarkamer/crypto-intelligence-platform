"""Source controls stay ordinary and gated while reducing repeated queries."""
import ast
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock,patch

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE/'runtime'))
import heatmap_models as models
import model1_execution as execution
from market_vision import coinglass_heatmap_capture as capture
from install_original_flow import expand_capture

MODEL3='https://www.coinglass.com/liquidation-heatmap-model3?coin=BTC&type=symbol'


class Controls:
    def __init__(self,items):self.items=items
    def count(self):return len(self.items)
    @property
    def first(self):return self.items[0]
    def nth(self,index):return self.items[index]


class EfficiencyTests(unittest.TestCase):
    def test_exact_visible_selected_tabs_do_not_click_or_wait(self):
        for label in ('Model 1','Model 2','Model 3','Symbol'):
            tab=Mock();tab.is_visible.return_value=True;tab.get_attribute.return_value='true'
            page=Mock()
            def roles(role,**kwargs):
                self.assertTrue(kwargs['name'].fullmatch(label))
                return Controls([] if role=='button' else [tab])
            page.get_by_role.side_effect=roles
            self.assertTrue(models.select_visible_control(page,label))
            tab.get_attribute.assert_called_once_with('aria-selected')
            tab.click.assert_not_called();page.wait_for_timeout.assert_not_called()

    def test_other_selected_values_keep_ordinary_tab_click(self):
        for selected in ('false','TRUE',None,True):
            tab=Mock();tab.is_visible.return_value=True;tab.get_attribute.return_value=selected
            page=Mock();page.get_by_role.side_effect=lambda role,**kwargs:Controls([] if role=='button' else [tab])
            self.assertTrue(models.select_visible_control(page,'Model 3'))
            tab.click.assert_called_once_with(timeout=2000)

    def test_common_overlays_use_one_exact_query_and_one_ordinary_click(self):
        buttons=[Mock(),Mock()]
        for button in buttons:button.is_visible.return_value=True
        page=Mock();page.get_by_role.return_value=Controls(buttons)
        capture._dismiss_common_overlays(page)
        page.get_by_role.assert_called_once()
        pattern=page.get_by_role.call_args.kwargs['name']
        for label in ('Accept','Accept all','Allow all','I agree','Agree','Got it','OK','Close'):
            self.assertTrue(pattern.fullmatch(label))
        self.assertFalse(pattern.fullmatch('Close trade'));self.assertFalse(pattern.fullmatch('Log in'))
        buttons[0].click.assert_called_once_with(timeout=1500);buttons[1].click.assert_not_called()
        page.wait_for_timeout.assert_called_once_with(300)

    def test_common_overlay_inspection_has_eight_button_bound(self):
        buttons=[Mock() for _ in range(12)]
        for button in buttons:button.is_visible.return_value=False
        page=Mock();page.get_by_role.return_value=Controls(buttons)
        capture._dismiss_common_overlays(page)
        self.assertEqual(sum(button.is_visible.call_count for button in buttons),8)
        for button in buttons:button.click.assert_not_called()
        page.wait_for_timeout.assert_not_called()

    def test_data_gate_appearing_during_common_close_is_not_dismissed(self):
        button=Mock();button.is_visible.return_value=True
        page=Mock(url=MODEL3);page.evaluate.side_effect=[False,True]
        page.get_by_role.return_value=Controls([button])
        with patch.object(capture,'HEATMAP_MODEL',3),self.assertRaises(execution.StageFailure) as caught:
            capture._dismiss_capture_blockers(page)
        self.assertEqual(caught.exception.code,'source_login_required')
        button.click.assert_called_once_with(timeout=1500)
        page.get_by_text.assert_not_called();page.keyboard.press.assert_not_called()

    def test_failed_common_click_attempt_still_rechecks_gate_before_any_later_button(self):
        first=Mock();first.is_visible.return_value=True;first.click.side_effect=TimeoutError('PRIVATE')
        second=Mock();second.is_visible.return_value=True
        page=Mock(url=MODEL3);page.evaluate.side_effect=[False,True]
        page.get_by_role.return_value=Controls([first,second])
        with patch.object(capture,'HEATMAP_MODEL',3),self.assertRaises(execution.StageFailure) as caught:
            capture._dismiss_capture_blockers(page)
        self.assertEqual(caught.exception.code,'source_login_required')
        first.click.assert_called_once_with(timeout=1500);second.click.assert_not_called()
        page.get_by_text.assert_not_called()

    def test_timeframe_subphases_preserve_every_existing_operation_and_timeout(self):
        def function(source):
            return next(node for node in ast.parse(source).body if isinstance(node,ast.FunctionDef) and node.name=='_select_timeframe')
        actual=function((HERE/'runtime/market_vision/coinglass_heatmap_capture.py').read_text())
        phases={node.items[0].context_expr.args[0].value for node in ast.walk(actual)
            if isinstance(node,ast.With) and isinstance(node.items[0].context_expr,ast.Call)
            and ast.unparse(node.items[0].context_expr.func)=='capture_operation'}
        self.assertEqual(phases,{'timeframe_trigger_lookup','timeframe_open','timeframe_option_lookup','timeframe_select','timeframe_confirmation'})
        class Unwrap(ast.NodeTransformer):
            def visit_With(self,node):
                self.generic_visit(node)
                if ast.unparse(node.items[0].context_expr.func)=='capture_operation':return node.body
                return node
        plain=Unwrap().visit(actual)
        expected=function(expand_capture((HERE.parent/'market_vision/coinglass_heatmap_capture.py').read_text()))
        self.assertEqual(ast.dump(plain,include_attributes=False),ast.dump(expected,include_attributes=False))

    def test_trigger_click_timeout_identifies_open_subphase(self):
        trigger=Mock();trigger.is_visible.return_value=True;trigger.click.side_effect=TimeoutError('PRIVATE')
        page=Mock(url=MODEL3);page.evaluate.return_value=False
        class Locator(Controls):
            def filter(self,**kwargs):return self
        page.locator.side_effect=lambda selector:Locator([] if ',' in selector else [trigger])
        def main(timeframe,job_id,output):
            execution.stage('capture')
            with execution.capture_operation('timeframe_selection'):
                execution.control(capture._select_timeframe,page,'12h')
        with tempfile.TemporaryDirectory() as directory,patch.object(models,'HEATMAP_MODEL',3):
            self.assertEqual(execution.run_task(main,'12H','synthetic',directory),1)
            report=json.loads((Path(directory)/'error.json').read_text())
            self.assertEqual(report['code'],'source_timeout')
            failures=[event['phase'] for event in report['capture_operations'] if event['status']=='failed']
            self.assertEqual(failures,['timeframe_open','timeframe_open','timeframe_selection'])
            self.assertNotIn('PRIVATE',json.dumps(report))


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
        context=self.browser.new_context();page=context.new_page()
        page.route('**/*',lambda route:route.fulfill(status=200,content_type='text/html',body=html))
        page.goto(MODEL3)
        self.addCleanup(context.close)
        return page

    def test_native_48_hour_combobox_switches_then_preserves_delayed_login_modal(self):
        page=self.page('''<button role="combobox" onclick="document.querySelector('ul').hidden=false">48 hour</button>
            <ul role="listbox" hidden><li role="option" onclick="document.querySelector('button').textContent='12 hour';document.querySelector('ul').hidden=true;setTimeout(()=>{document.querySelector('#gate').hidden=false},50)">12 hour</li>
            <li role="option">24 hour</li><li role="option">48 hour</li></ul>
            <div id="gate" role="dialog" class="MuiModalDialog-root" hidden>Log in to unlock full data</div>''')
        with patch.object(models,'HEATMAP_MODEL',3),self.assertRaises(execution.StageFailure) as caught:
            execution.control(capture._select_timeframe,page,'12h')
        self.assertEqual(caught.exception.code,'source_login_required')
        self.assertTrue(page.get_by_role('dialog').is_visible())
        self.assertEqual(page.get_by_role('combobox').inner_text(),'12 hour')

    def test_common_close_that_opens_data_modal_stops_before_any_data_close(self):
        page=self.page('''<button onclick="this.remove();document.querySelector('#gate').hidden=false">Close</button>
            <div id="gate" role="dialog" class="MuiModalDialog-root" hidden>Log in to unlock full data
            <button onclick="document.querySelector('#gate').remove()">Close</button></div>''')
        with patch.object(capture,'HEATMAP_MODEL',3),self.assertRaises(execution.StageFailure) as caught:
            capture._dismiss_capture_blockers(page)
        self.assertEqual(caught.exception.code,'source_login_required')
        self.assertTrue(page.get_by_role('dialog').is_visible())


if __name__=='__main__':unittest.main()
