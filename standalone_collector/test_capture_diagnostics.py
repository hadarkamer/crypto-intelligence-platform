"""Exercise the actual installed capture with fake browser calls, never a website."""
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
from unittest.mock import MagicMock,patch

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE/'runtime'))
import model1_execution as execution
from market_vision import coinglass_heatmap_capture as capture
from install_capture_diagnostics import expand_capture_diagnostics

class CaptureDiagnosticsTests(unittest.TestCase):
    def browser(self):
        driver=MagicMock();driver.__enter__.return_value=driver
        browser=driver.chromium.launch.return_value
        context=browser.new_context.return_value
        page=context.new_page.return_value
        page.evaluate.return_value={'x':0,'y':20,'width':800,'height':600,'page_width':1000}
        return driver,browser,context,page
    def failure(self,operation):
        driver,browser,context,page=self.browser()
        {'navigation':page.goto,'screenshot':page.screenshot,'browser_launch':driver.chromium.launch}[operation].side_effect=TimeoutError('PRIVATE_URL_AND_COOKIE')
        with tempfile.TemporaryDirectory() as directory,patch.dict(os.environ,{
            'COINGLASS_STORAGE_STATE_JSON':'','COINGLASS_COOKIE_HEADER':''}),\
            patch.object(capture,'sync_playwright',return_value=driver),\
            patch.object(capture,'_dismiss_capture_blockers'),patch.object(capture,'_wait_for_heatmap'),\
            patch.object(capture,'_select_model_one'),patch.object(capture,'_select_symbol_mode'),\
            patch.object(capture,'_select_timeframe',new=lambda *args:None),patch('model1_legend.prepare_legend_for_capture'):
            def main(*args):
                execution.stage('capture')
                capture.capture_heatmaps(Path(directory)/'capture',timeframes=('12h',))
            self.assertEqual(execution.run_task(main,'12H','job',directory),1)
            path=Path(directory)/'error.json'
            report=json.loads(path.read_text())
            self.assertEqual(report['code'],'source_timeout')
            self.assertEqual(report['capture_phase'],operation)
            self.assertEqual(report['capture_operations'][-1]['phase'],operation)
            self.assertEqual(report['capture_operations'][-1]['status'],'failed')
            self.assertLess(path.stat().st_size,4096)
            with contextlib.redirect_stdout(io.StringIO()) as output:
                execution.read_failure(path,'job',1)
            self.assertNotIn('PRIVATE',json.dumps(report)+output.getvalue())
            self.assertIn(operation,output.getvalue())
        return page
    def test_navigation_timeout_is_precise(self):
        page=self.failure('navigation');page.screenshot.assert_not_called()
    def test_screenshot_timeout_is_precise(self):
        page=self.failure('screenshot');page.screenshot.assert_called_once()
    def test_browser_launch_timeout_is_precise(self):self.failure('browser_launch')
    def test_instrumentation_preserves_every_existing_call_and_argument(self):
        text=(HERE/'runtime/market_vision/coinglass_heatmap_capture.py').read_text()
        tree=ast.parse(text)
        operations=[node for node in ast.walk(tree) if isinstance(node,ast.Call)
            and isinstance(node.func,ast.Name) and node.func.id=='capture_operation']
        self.assertGreaterEqual(len(operations),15)
        # Remove wrappers to establish that reinstrumentation only adds contexts.
        class Unwrap(ast.NodeTransformer):
            def visit_With(self,node):
                self.generic_visit(node)
                if (len(node.items)==1 and isinstance(node.items[0].context_expr,ast.Call)
                    and isinstance(node.items[0].context_expr.func,ast.Name)
                    and node.items[0].context_expr.func.id=='capture_operation'):
                    return node.body
                return node
        plain=ast.unparse(ast.fix_missing_locations(Unwrap().visit(tree)))
        def calls(value):
            return Counter(ast.dump(node,include_attributes=False) for node in ast.walk(ast.parse(value))
                if isinstance(node,ast.Call) and not (
                    isinstance(node.func,ast.Name) and node.func.id=='capture_operation'))
        self.assertEqual(calls(plain),calls(expand_capture_diagnostics(plain)))
    def test_nested_timeframe_failure_keeps_more_specific_phase(self):
        def fail(*args):
            execution.stage('capture')
            with execution.capture_operation('timeframe_selection'):
                def _select_timeframe(page,timeframe):raise TimeoutError('PRIVATE')
                execution.control(_select_timeframe,None,'12h')
        with tempfile.TemporaryDirectory() as directory:
            execution.run_task(fail,'12H','job',directory)
            report=json.loads((Path(directory)/'error.json').read_text())
            self.assertEqual(report['capture_phase'],'select_12h')
    def test_events_are_bounded_and_durations_sanitized(self):
        def fail(*args):
            execution.stage('capture')
            for index in range(100):
                with execution.capture_operation('wait_for_chart'):pass
            raise TimeoutError('PRIVATE')
        with tempfile.TemporaryDirectory() as directory:
            execution.run_task(fail,'12H','job',directory)
            report=json.loads((Path(directory)/'error.json').read_text())
            self.assertEqual(len(report['capture_operations']),24)
            self.assertLess((Path(directory)/'error.json').stat().st_size,4096)

if __name__=='__main__':unittest.main()
