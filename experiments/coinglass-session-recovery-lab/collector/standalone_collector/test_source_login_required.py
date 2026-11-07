"""Observed unlock-data gate regressions; no source, account or AI requests."""
import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE/'runtime'))
import capture_readiness as readiness
import model1_execution as execution
from heatmap_models import source_url


class SourceLoginTests(unittest.TestCase):
    def page(self,states,model=1):
        page=Mock(url=source_url(model));page.evaluate.side_effect=states
        return page

    def test_explicit_login_gate_is_classified_before_control(self):
        page=self.page([True]);calls=[]
        def _select_timeframe(page,timeframe):calls.append(timeframe)
        with self.assertRaises(execution.StageFailure) as caught:
            execution.control(_select_timeframe,page,'12h')
        self.assertEqual(caught.exception.code,'source_login_required')
        self.assertEqual(caught.exception._model1_capture_phase,'select_12h')
        self.assertEqual(calls,[])

    def test_gate_appearing_during_selection_replaces_generic_timeout(self):
        page=self.page([False,True])
        def _select_timeframe(page,timeframe):raise TimeoutError('PRIVATE PAGE TEXT')
        with self.assertRaises(execution.StageFailure) as caught:
            execution.control(_select_timeframe,page,'12h')
        detail=execution.failure_detail(caught.exception)
        self.assertEqual(caught.exception.code,'source_login_required')
        self.assertEqual(detail['source_readiness']['reason'],'login-required')
        self.assertEqual(detail['blocking_condition'],'login')
        self.assertNotIn('PRIVATE',json.dumps(detail))

    def test_successful_control_followed_by_gate_is_not_success(self):
        page=self.page([False,True])
        def _select_timeframe(page,timeframe):return 'selected'
        with self.assertRaises(execution.StageFailure) as caught:
            execution.control(_select_timeframe,page,'24h')
        self.assertEqual(caught.exception.code,'source_login_required')

    def test_other_timeouts_are_preserved(self):
        page=self.page([False,False])
        def _select_timeframe(page,timeframe):raise TimeoutError('PRIVATE PAGE TEXT')
        with self.assertRaises(TimeoutError) as caught:
            execution.control(_select_timeframe,page,'12h')
        self.assertEqual(caught.exception._model1_capture_phase,'select_12h')

    def test_uninspectable_page_cannot_mask_original_selector_failure(self):
        class ClosedPage:
            @property
            def url(self):raise RuntimeError('PRIVATE PAGE ERROR')
        for page in (None,ClosedPage(),self.page([RuntimeError('PRIVATE DOM ERROR'),RuntimeError('PRIVATE DOM ERROR')])):
            def _select_timeframe(page,timeframe):raise TimeoutError('PRIVATE SELECTOR ERROR')
            with self.subTest(page=type(page).__name__),self.assertRaises(TimeoutError) as caught:
                with execution.capture_operation('timeframe_selection'):
                    execution.control(_select_timeframe,page,'12h')
            self.assertEqual(caught.exception._model1_capture_phase,'select_12h')

    def test_normal_unauthenticated_control_is_preserved(self):
        page=self.page([False,False]);calls=[]
        def _select_timeframe(page,timeframe):calls.append(timeframe);return 'selected'
        self.assertEqual(execution.control(_select_timeframe,page,'48h'),'selected')
        self.assertEqual(calls,['48h'])
        page.goto.assert_not_called();page.reload.assert_not_called()

    def test_specific_gate_is_not_dismissed_into_loading_or_blur(self):
        from market_vision import coinglass_heatmap_capture as capture
        page=self.page([True])
        with self.assertRaises(execution.StageFailure) as caught:
            capture._dismiss_capture_blockers(page)
        self.assertEqual(caught.exception.code,'source_login_required')
        self.assertEqual(caught.exception._model1_capture_phase,'overlay_dismissal')
        page.get_by_text.assert_not_called();page.keyboard.press.assert_not_called()

    def test_confirmed_gate_does_not_spend_render_wait(self):
        page=self.page([{'ready':False,'reason':'login-required','label':'48 hour'}],3)
        result=readiness.wait_for_render(page,'12h',3)
        self.assertEqual(result['reason'],'login-required')
        page.wait_for_function.assert_not_called();page.wait_for_timeout.assert_not_called()

    def test_gate_can_end_existing_render_wait(self):
        page=self.page([
            {'ready':False,'reason':'loading-indicator','label':'12 hour'},
            {'ready':False,'reason':'login-required','label':'12 hour'}],3)
        result=readiness.wait_for_render(page,'12h',3)
        self.assertEqual(result['reason'],'login-required')
        self.assertIn('s.reason === "login-required"',page.wait_for_function.call_args.args[0])

    def test_wrong_source_cannot_be_classified_as_coinglass_data_gate(self):
        page=Mock(url='https://evil.example/');page.evaluate.return_value=True
        readiness.require_unblocked_source(page,3,phase='select_12h')
        page.evaluate.assert_not_called()

    def test_actual_failure_transport_retains_fixed_code_without_private_text(self):
        page=self.page([False,True])
        def _select_timeframe(page,timeframe):raise TimeoutError('PRIVATE PAGE TEXT')
        def main(timeframe,job_id,output):
            execution.stage('capture')
            execution.control(_select_timeframe,page,timeframe.lower())
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(execution.run_task(main,'12H','job',directory),1)
            path=Path(directory)/'error.json';report=json.loads(path.read_text())
            self.assertEqual(report['code'],'source_login_required')
            self.assertEqual(report['blocking_condition'],'login')
            self.assertIsNone(report['usage']);self.assertFalse((Path(directory)/'result.json').exists())
            with contextlib.redirect_stdout(io.StringIO()) as output:
                self.assertEqual(execution.read_failure(path,'job',1),'source_login_required')
            self.assertIn('login-required',output.getvalue())
            self.assertNotIn('PRIVATE',path.read_text()+output.getvalue())


if __name__=='__main__':unittest.main()
