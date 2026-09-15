"""Local synthetic fixtures for page initialization; no external resources."""
import contextlib
import io
import unittest
from unittest.mock import Mock, patch
import model1_page_flow as flow

class Tests(unittest.TestCase):
    def setUp(self):
        self.out=io.StringIO()
        self.output=contextlib.redirect_stdout(self.out)
        self.output.__enter__()
    def tearDown(self): self.output.__exit__(None,None,None)
    def test_normalized_boolean_signals(self):
        p=Mock(); p.evaluate.return_value={'account_link_present':True,'login_link_visible':False,'email':'PRIVATE'}
        self.assertEqual(flow.ui_state(p),{'account_link_present':True,'login_link_visible':False})
    def test_already_initialized_no_wait(self):
        p=Mock(); p.evaluate.return_value={'account_link_present':True,'login_link_visible':False}
        flow.before_controls(p); p.wait_for_timeout.assert_not_called()
    def test_initialization_before_controls(self):
        p=Mock(); p.evaluate.side_effect=[{'account_link_present':False,'login_link_visible':True},{'account_link_present':True,'login_link_visible':False}]
        state=flow.before_controls(p)
        self.assertTrue(state['account_link_present']); p.wait_for_timeout.assert_called_once_with(250)
    def test_no_marker_does_not_prove_rejection(self):
        p=Mock(); p.evaluate.return_value={}
        state=flow.before_controls(p,timeout_ms=0)
        self.assertFalse(state['account_link_present'])
        self.assertNotIn('rejected',self.out.getvalue())
    def test_read_error_sanitized(self):
        p=Mock(); p.evaluate.side_effect=RuntimeError('PRIVATE')
        flow.before_controls(p,timeout_ms=0)
        self.assertNotIn('PRIVATE',self.out.getvalue())
    def test_timeout_is_bounded(self):
        p=Mock(); p.evaluate.return_value={}
        with patch.object(flow.time,'monotonic',side_effect=[0,0,100]): flow.before_controls(p,999999)
        p.wait_for_timeout.assert_called_once_with(250)
    def test_failure_report_does_not_call_network(self):
        p=Mock(); p.evaluate.return_value={'account_link_present':False,'login_link_visible':True}
        state=flow.after_render_check(p,False)
        self.assertFalse(state['chart_ready']); p.goto.assert_not_called()
    def test_only_true_is_success(self):
        p=Mock(); p.evaluate.return_value={}
        self.assertFalse(flow.after_render_check(p,'true')['chart_ready'])
    def test_no_profile_text_logged(self):
        p=Mock(); p.evaluate.return_value={'account_link_present':True,'login_link_visible':False,'user':'PRIVATE'}
        flow.before_controls(p); flow.after_render_check(p,True)
        self.assertNotIn('PRIVATE',self.out.getvalue())
    def test_unknown_ui_type_safe(self):
        p=Mock(); p.evaluate.return_value='PRIVATE'
        self.assertEqual(flow.ui_state(p),{'account_link_present':False,'login_link_visible':False})

if __name__=='__main__': unittest.main()
