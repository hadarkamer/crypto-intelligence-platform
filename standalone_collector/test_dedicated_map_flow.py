"""Synthetic worker regressions. No provider, network, credentials, AI or worksheet."""
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock, patch
import os
import sys

import dedicated_map_flow as flow
from dedicated_login_form import LoginFormResult
from model1_execution import StageFailure
from modern_map_task import capture, run_modern_task

BTC12 = flow.PROTECTED_PROBE
BTC24 = flow.MapTarget('BTC', 1, '24H')
ETH48 = flow.MapTarget('ETH', 2, '48H')


class Account:
    dedicated = True

    def __init__(self, restored=False):
        self.state = {'cookies': ['synthetic'] if restored else [], 'origins': []}
        self.events = []
        self.incident = 0
        self.attempts = 0
        self.pending = None
        self.cooldown = None
        self.rotated = False

    def source_cooldown(self, *, limited=False, retry_after_seconds=None):
        if limited:
            self.cooldown = retry_after_seconds or 60
        return {'code':'source_rate_limited' if self.cooldown else 'allowed',
                'retry_after_seconds':self.cooldown}

    def auth_start(self, observed_code=None, retry_after_seconds=None):
        self.events.append('incident')
        if observed_code:
            self.events.append((observed_code,retry_after_seconds))
            self.pending = observed_code
        if self.pending:
            return {'code': self.pending}
        self.incident += 1
        return {'code': 'allowed', 'incident_id': self.incident}

    def credentials(self, incident):
        raise AssertionError('No credentials are used by this fixture')

    def auth_submit(self, incident):
        self.events.append('submit')
        self.attempts += 1
        return True

    def auth_result(self, incident, code, retry_after_seconds=None):
        self.events.append((code,retry_after_seconds))
        if code == 'auth_network_transient' and not self.attempts:
            return {'code':'auth_retry_pending'}
        self.pending = code
        return {'code':code}

    def state_changed(self,context):
        return self.rotated

    @contextmanager
    def observe_proof(self, page):
        yield

    def checkpoint(self, context, incident_id=None, *, proof_page=None):
        self.events.append('checkpoint')
        self.state['cookies'] = ['synthetic']
        self.pending = None


class Flow(TestCase):
    def setUp(self):
        self.page = Mock()
        self.page.url = flow.map_url('BTC')
        self.account = Account()
        self.context = Mock()
        self.select_errors = []
        self.read_errors = []
        self.form_results = []

        def select(page,symbol,model,timeframe,*,guard):
            self.account.events.append(('open',symbol,model,timeframe))
            if self.select_errors:
                err = self.select_errors.pop(0)
                if err: raise err

        def read(page,symbol,model,timeframe,output,*,guard):
            self.account.events.append(('read',symbol,model,timeframe))
            if self.read_errors:
                err = self.read_errors.pop(0)
                if err: raise err
            return (symbol,model,timeframe)

        def form(page,*,timeout_ms,credentials,before_submit):
            self.assertGreater(timeout_ms,0)
            outcome = self.form_results.pop(0) if self.form_results else LoginFormResult('login_submitted',True)
            if outcome.submitted:self.assertTrue(before_submit())
            return outcome

        for name, action in [('submit_official_login',form),('open_verified_map',select),('read_selected_map',read)]:
            where = patch.object(flow,name,side_effect=action)
            where.start();self.addCleanup(where.stop)
        # These tests place an explicit gate on the requested first-party
        # map. Separate guard tests exercise foreign and mismatched targets.
        def guard(page,target):
            raise StageFailure('source_login_required')
        where=patch.object(flow,'guard_surface',side_effect=guard)
        where.start();self.addCleanup(where.stop)

    def run_map(self,target,allow=True):
        return flow.collect_with_dedicated_session(self.account,self.context,self.page,target,None,allow_login=allow)

    def assert_code(self,code,target=BTC12,allow=True):
        with self.assertRaises(StageFailure) as caught:self.run_map(target,allow)
        self.assertEqual(caught.exception.code,code)

    def test_bootstrap_checkpoint_before_failed_read_then_restore(self):
        self.read_errors=[StageFailure('source_timeout')]
        self.assert_code('source_timeout')
        self.assertEqual(self.account.events,['incident','submit',('open','BTC',1,'12H'),'checkpoint',('read','BTC',1,'12H')])
        self.assertEqual(self.run_map(BTC12,allow=False),('BTC',1,'12H'))
        self.assertEqual(self.account.attempts,1)

    def test_public_request_and_unknown_assets_require_canonical_proof_before_new_login_success(self):
        self.select_errors=[StageFailure('source_map_unavailable')]
        self.assert_code('source_map_unavailable',BTC24)
        self.assertNotIn('checkpoint',self.account.events)
        self.assertFalse(any(event[0]=='read' for event in self.account.events if isinstance(event,tuple)))
        self.account=Account()
        self.assertEqual(self.run_map(ETH48),('ETH',2,'48H'))
        self.assertEqual(self.account.events,['incident','submit',('open','BTC',1,'12H'),'checkpoint',
                                             ('open','ETH',2,'48H'),('read','ETH',2,'48H')])

    def test_unavailable_requested_range_does_not_poison_verified_account(self):
        self.select_errors=[None,StageFailure('source_map_unavailable')]
        self.assert_code('source_map_unavailable',BTC24)
        self.assertIsNone(self.account.pending)
        self.assertEqual(self.run_map(BTC12,allow=False),('BTC',1,'12H'))
        self.assertEqual(self.run_map(ETH48,allow=False),('ETH',2,'48H'))
        self.assertEqual(self.account.attempts,1)

    def test_restored_public_range_can_publish_without_asserting_account_verified(self):
        self.account=Account(restored=True)
        self.assertEqual(self.run_map(BTC24),('BTC',1,'24H'))
        self.assertEqual(self.account.events,[('open','BTC',1,'24H'),('read','BTC',1,'24H')])
        self.context.new_page.assert_not_called()

    def test_rotated_state_gets_own_protected_page_before_downstream_failure(self):
        self.account=Account(restored=True)
        self.account.rotated=True
        self.read_errors=[StageFailure('source_timeout')]
        self.assert_code('source_timeout',ETH48)
        self.context.new_page.assert_called_once()
        self.context.new_page.return_value.close.assert_called_once()
        self.assertEqual(self.account.events,[('open','ETH',2,'48H'),('open','BTC',1,'12H'),
                                             'checkpoint',('read','ETH',2,'48H')])
        self.assertEqual(self.account.attempts,0)

    def test_failed_auxiliary_proof_never_stops_independent_range_or_submits(self):
        for reason in ('source_login_required','auth_challenge','source_rate_limited','source_timeout'):
            with self.subTest(reason=reason):
                self.account=Account(restored=True)
                self.account.rotated=True
                self.select_errors=[None,StageFailure(reason)]
                self.assertEqual(self.run_map(BTC24),('BTC',1,'24H'))
                self.assertEqual(self.account.events,[('open','BTC',1,'24H'),('open','BTC',1,'12H'),
                                                     ('read','BTC',1,'24H')])
                self.assertIsNone(self.account.pending)
                self.assertEqual(self.account.attempts,0)
                if reason=='source_rate_limited':self.assertEqual(self.account.cooldown,60)

    def test_explicit_gate_in_restored_map_recovers_and_midscan_gate_stops(self):
        self.account=Account(restored=True)
        self.select_errors=[StageFailure('source_login_required')]
        self.read_errors=[StageFailure('source_login_required')]
        self.assert_code('auth_gate_after_verification')
        self.assertEqual(self.account.attempts,1)
        self.select_errors=[StageFailure('source_login_required')]
        self.assert_code('auth_gate_after_verification',ETH48)
        self.assertEqual(self.account.attempts,1)

    def test_network_or_unavailable_map_does_not_repost(self):
        for reason in ('source_capture_failed','source_timeout','source_map_unavailable','source_navigation_changed'):
            with self.subTest(reason=reason):
                self.account=Account(restored=True)
                self.select_errors=[StageFailure(reason)]
                self.assert_code(reason)
                self.assertEqual(self.account.attempts,0)
                self.assertNotIn('incident',self.account.events)

    def test_ambiguous_submission_checks_map_and_never_resubmits(self):
        self.form_results=[LoginFormResult('login_outcome_unconfirmed',True)]
        self.assertEqual(self.run_map(BTC12),('BTC',1,'12H'))
        self.assertEqual(self.account.attempts,1)
        self.account=Account()
        self.form_results=[LoginFormResult('login_outcome_unconfirmed',True)]
        self.select_errors=[StageFailure('source_login_required')]
        self.assert_code('auth_unconfirmed')
        self.assert_code('auth_unconfirmed',ETH48)
        self.assertEqual(self.account.attempts,1)

    def test_only_pre_submit_transient_can_retry(self):
        self.form_results=[LoginFormResult('login_network_transient'), LoginFormResult('login_submitted',True)]
        self.run_map(BTC12)
        self.assertEqual(self.account.attempts,1)
        self.assertEqual(self.account.events.count('incident'),2)

    def test_explicit_map_rate_limit_keeps_retry_after_and_never_invokes_form(self):
        self.account=Account(restored=True)
        failure=StageFailure('source_rate_limited')
        failure.retry_after_seconds=720
        self.select_errors=[failure]
        self.assert_code('source_rate_limited')
        self.assertEqual(self.account.cooldown,720)
        self.assert_code('source_rate_limited',ETH48)
        self.assertEqual(self.account.events,[('open','BTC',1,'12H')])
        self.assertIsNone(self.account.pending)
        self.assertEqual(self.account.attempts,0)

    def test_hard_form_blocker_deduplicates_disjoint_jobs(self):
        for client,server in [('login_captcha_required','auth_challenge'),('login_verification_required','auth_code_required'),
                              ('login_credentials_rejected','auth_invalid_credentials'),('login_rate_limited','auth_rate_limited')]:
            with self.subTest(client=client):
                self.account=Account()
                duration=180 if client=='login_rate_limited' else None
                self.form_results=[LoginFormResult(client,False,duration)]
                self.assert_code(server)
                self.assert_code(server,ETH48)
                self.assertEqual(self.account.events.count((server,duration)),1)
                self.assertEqual(self.account.attempts,0)

    def test_reuse_probe_cannot_submit_on_empty_or_explicit_gate(self):
        self.assert_code('source_login_required',allow=False)
        self.account=Account(restored=True)
        self.select_errors=[StageFailure('source_login_required')]
        self.assert_code('source_login_required',allow=False)
        self.assertNotIn('incident',self.account.events)


class SafetyAndDispatch(TestCase):
    def test_generated_job_main_dispatches_to_modern_task(self):
        # Exercise the actual collector build-time adapter on its pinned task.
        from importlib.util import module_from_spec, spec_from_file_location
        from shutil import copyfile
        from install_modern_map import install
        repo = Path(__file__).resolve().parent.parent
        with TemporaryDirectory() as folder:
            runtime = Path(folder)
            (runtime/'market_vision').mkdir()
            for name in ('collection_model1_task.py','market_vision/openai_heatmap_scanner.py'):
                copyfile(repo/name,runtime/name)
            install(runtime)
            spec = spec_from_file_location('worker_dispatch_fixture',runtime/'collection_model1_task.py')
            worker = module_from_spec(spec)
            spec.loader.exec_module(worker)
            with patch.dict(os.environ,{'COINGLASS_MODERN_UI_ENABLED':'true'}), \
                 patch('modern_map_task.run_modern_task') as entry:
                worker.main('12H','fixture-id',folder)
            entry.assert_called_once_with('12H','fixture-id',folder)

    def test_aux_budget_caps_navigation_and_expires_without_altering_ordinary_limits(self):
        from modern_map_reader import limited_timeout, map_access_budget
        with patch('modern_map_reader.time.monotonic',side_effect=(0,2,14)):
            with map_access_budget(12):
                self.assertEqual(limited_timeout(40000),10000)
                with self.assertRaises(StageFailure) as caught:limited_timeout(3000)
                self.assertEqual(caught.exception.code,'source_timeout')
        self.assertEqual(limited_timeout(40000),40000)

    def test_foreign_redirect_and_wrong_range_never_authorize_credentials(self):
        page=Mock()
        page.url='https://example.invalid/login'
        with self.assertRaises(StageFailure) as caught:flow.guard_surface(page,BTC12)
        self.assertEqual(caught.exception.code,'source_navigation_changed')
        page.url='https://www.coinglass.com/login'
        with patch.object(flow,'visible_login_blocker',return_value='login_captcha_required'):
            with self.assertRaises(StageFailure) as caught:flow.guard_surface(page,BTC12)
            self.assertEqual(caught.exception.code,'auth_challenge')
        page.url=flow.map_url('BTC')
        with patch.object(flow,'visible_login_blocker',return_value='login_form_visible'), patch.object(flow,'choice',return_value=False):
            with self.assertRaises(StageFailure) as caught:flow.guard_surface(page,BTC12)
            self.assertEqual(caught.exception.code,'source_not_readable')

    def test_worker_runs_sandboxed_without_ai_key_and_cleans_on_failure(self):
        from unittest.mock import MagicMock
        browser, context = MagicMock(),MagicMock()
        browser.new_context.return_value=context
        pw=MagicMock()
        pw.chromium.launch.return_value=browser
        state=Mock(dedicated=True,state={'cookies':[],'origins':[]})
        @contextmanager
        def driver():
            self.assertNotIn('OPENAI_API_KEY',os.environ)
            yield pw
        ai = Mock()
        scanner=SimpleNamespace(analyze_heatmap_images=ai)
        with TemporaryDirectory() as root, patch('playwright.sync_api.sync_playwright',driver), \
             patch.dict(os.environ,{'OPENAI_API_KEY':'fixture','COLLECTION_HEATMAP_SYMBOL':'BTC','COLLECTION_HEATMAP_MODEL':'1'}), \
             patch('session_capture.CaptureSession.from_environment',return_value=state), \
             patch.dict(sys.modules,{'market_vision.openai_heatmap_scanner':scanner}), \
             patch.object(flow,'collect_with_dedicated_session',side_effect=StageFailure('auth_challenge')):
            with self.assertRaises(StageFailure) as caught:run_modern_task('12H','fixture',root)
            self.assertEqual(caught.exception.code,'auth_challenge')
            self.assertEqual(os.environ['OPENAI_API_KEY'],'fixture')
            self.assertFalse((Path(root)/'image.png').exists())
            self.assertFalse((Path(root)/'result.json').exists())
            ai.assert_not_called()
        kwargs=pw.chromium.launch.call_args.kwargs
        self.assertIs(kwargs['chromium_sandbox'],True)
        self.assertNotIn('--no-sandbox',kwargs['args'])
        state.finish.assert_called_once_with(context)
        context.close.assert_called_once();browser.close.assert_called_once()

    def test_legacy_session_is_rejected_before_browser_launch(self):
        with patch('playwright.sync_api.sync_playwright') as driver:
            with self.assertRaises(StageFailure) as caught:
                capture('BTC',1,'12H',None,session=SimpleNamespace(dedicated=False))
            self.assertEqual(caught.exception.code,'session_input_invalid')
            driver.assert_not_called()
