"""Exercise dedicated candidate routes, legacy isolation, job retry and lost save acknowledgement."""
import asyncio
from contextlib import nullcontext
import json
import os
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from uuid import uuid4

from aiohttp import web
from aiohttp.test_utils import make_mocked_request
import app
import configuration_check
import dedicated_pilot
import symbol_jobs


class BridgeError(Exception):
    def __init__(self, code, message='', status=500):
        self.code = code
        self.status = status


class Entry(TestCase):
    def test_new_mode_never_activates_from_classic_cookie(self):
        with patch.dict(os.environ,{'COINGLASS_MODERN_UI_ENABLED':'true','COINGLASS_COOKIE_HEADER':'fixture',
            'COINGLASS_STORAGE_STATE_JSON':'fixture','COINGLASS_SESSION_STORE_ENABLED':'true',
            'COINGLASS_SESSION_KEY':'test-key'},clear=True):
            self.assertFalse(app.source_session_configured())
            self.assertFalse(configuration_check.configuration_status(os.environ)['source_session_present'])
            structure = configuration_check.source_setting_structure(os.environ)
            self.assertEqual(structure,{'account_id_configured':False,'form_secret_configured':False})

    def test_misconfigured_reader_cannot_fall_back_when_dedicated_secrets_are_staged(self):
        from heatmap_models import child_environment
        with patch.dict(os.environ,{'COINGLASS_LOGIN_PASSWORD':'fixture',
                                    'COINGLASS_COOKIE_HEADER':'other-fixture'},clear=True):
            self.assertFalse(app.source_session_configured())
            self.assertFalse(configuration_check.configuration_status(os.environ)['source_session_present'])
            self.assertNotIn('COINGLASS_LOGIN_PASSWORD', child_environment(1))

    def test_non_auth_failures_and_transient_auth_do_not_open_incident_wait(self):
        state = Mock()
        with patch.dict('sys.modules', {'dedicated_auth_state':state}):
            store = Mock()
            for code in ('auth_retry_pending','auth_in_progress','source_timeout','source_map_unavailable','source_rate_limited'):
                self.assertEqual(symbol_jobs.canonical_auth_failure(None,store,'job',code),code)
            state.incident_status.assert_not_called()
            state.incident_status.return_value={'incident_id':'dummy-id'}
            self.assertEqual(symbol_jobs.canonical_auth_failure(None,store,'job','auth_challenge'),'source_login_required')
            state.incident_status.return_value={'incident_id':None}
            self.assertEqual(symbol_jobs.canonical_auth_failure(None,store,'job','auth_challenge'),'auth_challenge')

    def test_old_unbound_request_can_resume_and_terminal_dedicated_event_waits(self):
        status=Mock()
        runtime=Mock()
        db=Mock()
        rid=str(uuid4())
        prior={'id':str(uuid4()),'attempt':1,'status':'failed','logical_request_id':rid,
               'error_code':'source_login_required'}
        with patch.dict('sys.modules',{'collection_bridge':SimpleNamespace(BridgeError=BridgeError),
                 'session_runtime':runtime,'dedicated_auth_state':SimpleNamespace(incident_status=status)}),\
                patch.dict(os.environ,{'COINGLASS_MODERN_UI_ENABLED':'true','COINGLASS_SESSION_STORE_ENABLED':'true'}),\
                patch.object(symbol_jobs,'_source_retry',return_value=None):
            status.return_value={'status':'waiting'}
            with self.assertRaises(BridgeError) as err:
                symbol_jobs._continue_attempt(db,prior,rid,2)
            self.assertEqual(err.exception.code,'login_pending')
            db.execute.assert_not_called()
            for progress in ('none','resolved'):
                db.reset_mock()
                status.return_value={'status':progress}
                symbol_jobs._continue_attempt(db,prior,rid,2)
                self.assertEqual(db.execute.call_count,2)
                self.assertTrue(all('coinglass_login_incidents' not in item.args[0] for item in db.execute.call_args_list))

    def test_source_cooldown_rejects_before_spending_a_job_attempt_and_preserves_its_deadline(self):
        from model1_cache_policy import error_response
        rid=str(uuid4())
        prior={'id':str(uuid4()),'attempt':1,'status':'failed','logical_request_id':rid,
               'error_code':'source_rate_limited'}
        db=Mock()
        retry={'retry_at':'2026-11-26T01:00:00+00:00','retry_after_seconds':7200}
        with patch.object(symbol_jobs,'_source_retry',return_value=retry), patch.dict('sys.modules',{
                'collection_bridge': SimpleNamespace(BridgeError=BridgeError,
                    envelope=lambda row:{'error':{'code':row['error_code']}})}):
            with self.assertRaises(BridgeError) as denied:
                symbol_jobs._continue_attempt(db,prior,rid,2)
            self.assertEqual(denied.exception.code,'source_rate_limited')
            self.assertEqual(error_response(denied.exception)[1]['Retry-After'],'7200')
            db.execute.assert_not_called()
            self.assertEqual(symbol_jobs._job_envelope(db,prior)['error']['retry_at'],retry['retry_at'])

    def test_preflight_requires_dedicated_server_credentials(self):
        checks=dedicated_pilot.ready_configuration({'COINGLASS_COOKIE_HEADER':'fixture'})
        self.assertFalse(any(checks.values()))


class RoutesAndWorker(IsolatedAsyncioTestCase):
    async def test_only_safe_authenticated_status(self):
        token='test'*9
        store=MagicMock()
        application=web.Application()
        app.register_dedicated_status(application,store,True)
        self.assertEqual({r.resource.canonical for r in application.router.routes()},
                         {'/api/collection/coinglass/session-status'})
        route=next(r.handler for r in application.router.routes() if r.method=='GET')
        with patch.dict(os.environ,{'COINGLASS_COLLECTOR_TOKEN':token}),\
                patch.dict('sys.modules',{'session_runtime':MagicMock(),'dedicated_auth_state':MagicMock()}) as modules:
            with self.assertRaises(web.HTTPUnauthorized):
                await route(make_mocked_request('GET','/'))
            headers={'Authorization':'Bearer '+token}
            self.assertEqual((await route(make_mocked_request('GET','/?job_id=bad',headers=headers))).status,400)
            status=modules['dedicated_auth_state'].incident_status
            status.return_value={'incident_id':None,'can_launch':False,'reason':None,'status':'none'}
            response=await route(make_mocked_request('GET','/',headers=headers))
            self.assertEqual(json.loads(response.body),{**status.return_value,'auth_mode':'dedicated'})
            self.assertEqual(response.headers['Cache-Control'],'no-store')

    async def test_job_save_retry_uses_same_result_and_does_not_open_legacy_incident(self):
        store=Mock()
        held=Mock()
        jid=str(uuid4())
        store.claim.return_value=(held,{'id':jid,'symbol':'BTC','heatmap_model':1,'timeframe':'12H'})
        store.finish.side_effect=[TimeoutError,None]
        result={'fixture':True}
        scan=AsyncMock(return_value=(result,b'fixture'))
        runtime=SimpleNamespace(ManagedSession=Mock(),SessionError=RuntimeError)
        old=Mock()
        with patch.dict('sys.modules',{
            'session_runtime':runtime, 'collection_bridge':SimpleNamespace(BridgeError=BridgeError,run_existing_scanner=scan),
            'collection_metrics':SimpleNamespace(logged_phase=lambda *_:nullcontext(),queue_wait=lambda *_:None),
            'operator_login':old}), patch.dict(os.environ,{'COINGLASS_MODERN_UI_ENABLED':'true'}),\
                patch.object(asyncio,'sleep',side_effect=asyncio.CancelledError):
            with self.assertRaises(asyncio.CancelledError):
                await app.run_worker(store)
        self.assertEqual(scan.await_count,1)
        self.assertEqual(store.finish.call_count,2)
        for item in store.finish.call_args_list:
            self.assertEqual(item.args,(jid,result,b'fixture'))
        old.waiting_incident.assert_not_called()
        old.record_login_required.assert_not_called()
        held.close.assert_called_once()
