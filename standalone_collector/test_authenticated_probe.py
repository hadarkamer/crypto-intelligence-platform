"""Offline tests only: synthetic sessions and captures, never CoinGlass/OpenAI."""
import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch, Mock
from aiohttp import web
from aiohttp.test_utils import make_mocked_request

import app

SYNTHETIC_SESSION = 'synthetic_fixture=never_a_real_login'
SYNTHETIC_TOKEN = 'fixture-token-for-tests-only-not-a-real-secret'
PNG = b'\x89PNG\r\n\x1a\nsynthetic-image-not-real-data'


def successful_capture(directory, *, timeframes):
    if timeframes != ('12h',):
        raise AssertionError('wrong timeframe')
    path = directory / 'coinglass_btc_heatmap_12h.png'
    path.write_bytes(PNG)
    return [{'image': str(path), 'timeframe': '12h'}]


class ProbeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)
        self.env = patch.dict(os.environ, {}, clear=True)
        self.env.start()
        self.out = io.StringIO()
        self.output = contextlib.redirect_stdout(self.out)
        self.output.__enter__()

    def tearDown(self):
        self.output.__exit__(None, None, None)
        self.env.stop()
        self.temp.cleanup()

    def run_probe(self, capture=None):
        return app.probe(capture_fn=capture or successful_capture, directory=self.directory)

    def test_missing_session_opens_nothing(self):
        capture = Mock()
        result = self.run_probe(capture)
        self.assertEqual(result['code'], 'SOURCE_SESSION_MISSING')
        self.assertEqual(result['source_attempts'], 0)
        self.assertEqual(result['ai_calls'], 0)
        self.assertEqual(result['sheet_writes'], 0)
        capture.assert_not_called()

    def test_whitespace_session_is_missing(self):
        os.environ['COINGLASS_COOKIE_HEADER'] = '  '
        os.environ['COINGLASS_STORAGE_STATE_JSON'] = '\n'
        self.assertFalse(app.source_session_configured())
        self.assertEqual(self.run_probe()['source_attempts'], 0)

    def test_cookie_reaches_existing_capture_unchanged_once(self):
        os.environ['COINGLASS_COOKIE_HEADER'] = SYNTHETIC_SESSION
        def capture(directory, *, timeframes):
            self.assertEqual(os.environ['COINGLASS_COOKIE_HEADER'], SYNTHETIC_SESSION)
            self.assertEqual(os.environ['MODEL1_NETWORK_DIAGNOSTICS'], 'false')
            return successful_capture(directory, timeframes=timeframes)
        wrapper = Mock(side_effect=capture)
        result = self.run_probe(wrapper)
        wrapper.assert_called_once()
        self.assertEqual(result['status'], 'render_checks_passed')
        self.assertEqual(result['code'], 'VISUAL_REVIEW_REQUIRED')
        self.assertEqual(result['source_attempts'], 1)
        self.assertEqual(result['ai_calls'], 0)
        self.assertEqual(result['sheet_writes'], 0)
        self.assertNotIn(SYNTHETIC_SESSION, self.out.getvalue())
        self.assertNotIn(SYNTHETIC_SESSION, (self.directory/'report.json').read_text())

    def test_storage_state_alternative_is_supported(self):
        state = '{"cookies":[],"origins":[]}'
        os.environ['COINGLASS_STORAGE_STATE_JSON'] = state
        capture = Mock(side_effect=successful_capture)
        result = self.run_probe(capture)
        capture.assert_called_once()
        self.assertTrue(result['source_session_supplied'])
        self.assertEqual(os.environ['COINGLASS_STORAGE_STATE_JSON'], state)

    def test_error_never_echoes_secrets_or_retries(self):
        os.environ['COINGLASS_COOKIE_HEADER'] = SYNTHETIC_SESSION
        capture = Mock(side_effect=RuntimeError(SYNTHETIC_SESSION+' PRIVATE_VALUE'))
        result = self.run_probe(capture)
        capture.assert_called_once()
        self.assertEqual(result['code'], 'SOURCE_CAPTURE_FAILED')
        self.assertNotIn('PRIVATE_VALUE', self.out.getvalue())
        self.assertNotIn(SYNTHETIC_SESSION, json.dumps(result))

    def test_loading_failure_stays_failure(self):
        os.environ['COINGLASS_COOKIE_HEADER'] = SYNTHETIC_SESSION
        SourceNotReady = type('SourceNotReady', (RuntimeError,), {})
        def capture(directory, **kwargs):
            (directory/'not-ready.png').write_bytes(PNG)
            (directory/'not-ready.json').write_text(json.dumps({
                'reason':'loading-indicator', 'label':'12 hour', 'secret':'DO_NOT_COPY'}))
            raise SourceNotReady('DO_NOT_COPY')
        result = self.run_probe(capture)
        self.assertEqual(result['code'], 'SOURCE_NOT_READY')
        self.assertEqual(result['reason'], 'loading-indicator')
        self.assertEqual(result['observed_timeframe'], '12 hour')
        self.assertNotIn('DO_NOT_COPY', json.dumps(result))

    def test_unknown_diagnostic_reason_not_logged(self):
        os.environ['COINGLASS_COOKIE_HEADER'] = SYNTHETIC_SESSION
        def capture(directory, **kwargs):
            (directory/'not-ready.json').write_text(json.dumps({
                'reason':SYNTHETIC_SESSION, 'label':'private account info'}))
            raise RuntimeError('masked')
        result = self.run_probe(capture)
        self.assertEqual(result['reason'], 'source-not-ready')
        self.assertNotIn('observed_timeframe', result)

    def test_old_images_are_removed_when_session_missing(self):
        (self.directory/'coinglass_btc_heatmap_12h.png').write_bytes(PNG)
        (self.directory/'not-ready.png').write_bytes(PNG)
        self.run_probe()
        self.assertFalse((self.directory/'coinglass_btc_heatmap_12h.png').exists())
        self.assertFalse((self.directory/'not-ready.png').exists())

    def test_diagnostics_files_are_private(self):
        os.environ['COINGLASS_COOKIE_HEADER'] = SYNTHETIC_SESSION
        self.run_probe()
        self.assertEqual(self.directory.stat().st_mode & 0o777, 0o700)
        for filename in ('report.json','coinglass_btc_heatmap_12h.png'):
            self.assertEqual((self.directory/filename).stat().st_mode & 0o777, 0o600)

    def test_passive_diagnostics_flag_restored(self):
        os.environ['COINGLASS_COOKIE_HEADER'] = SYNTHETIC_SESSION
        os.environ['MODEL1_NETWORK_DIAGNOSTICS'] = 'true'
        self.run_probe()
        self.assertEqual(os.environ['MODEL1_NETWORK_DIAGNOSTICS'], 'true')

    def test_missing_diagnostics_flag_not_left_behind(self):
        os.environ['COINGLASS_COOKIE_HEADER'] = SYNTHETIC_SESSION
        self.run_probe()
        self.assertNotIn('MODEL1_NETWORK_DIAGNOSTICS', os.environ)

    def test_wrong_timeframe_or_bad_bytes_never_success(self):
        os.environ['COINGLASS_COOKIE_HEADER'] = SYNTHETIC_SESSION
        def wrong(directory, **kwargs):
            out=successful_capture(directory, timeframes=('12h',))
            out[0]['timeframe']='24h'
            return out
        self.assertEqual(self.run_probe(wrong)['code'], 'INVALID_CAPTURE_EVIDENCE')
        def invalid(directory, **kwargs):
            out=successful_capture(directory, timeframes=('12h',))
            Path(out[0]['image']).write_bytes(b'not-png')
            return out
        self.assertEqual(self.run_probe(invalid)['code'], 'INVALID_CAPTURE_EVIDENCE')

    def test_public_health_report_has_no_raw_errors_or_image_path(self):
        data={'status':'failed', 'source_attempts':0, 'error':SYNTHETIC_SESSION,
              'observed_state':{'header':SYNTHETIC_SESSION}, 'image':'private.png'}
        (self.directory/'report.json').write_text(json.dumps(data))
        with patch.object(app, 'DIAG', self.directory):
            result=app.read_public_probe_report()
        self.assertEqual(result, {'status':'failed','source_attempts':0})


class EndpointTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.directory=Path(self.temp.name)
        self.env=patch.dict(os.environ, {}, clear=True)
        self.env.start()
        self.diag=patch.object(app, 'DIAG', self.directory)
        self.diag.start()
        (self.directory/'report.json').write_text(json.dumps({'image':'coinglass_btc_heatmap_12h.png'}))
        (self.directory/'coinglass_btc_heatmap_12h.png').write_bytes(PNG)

    def tearDown(self):
        self.env.stop(); self.diag.stop(); self.temp.cleanup()

    async def test_evidence_denies_no_token_wrong_token_or_short_token(self):
        for configured,supplied in [('',None),('short','Bearer short'),(SYNTHETIC_TOKEN,None),(SYNTHETIC_TOKEN,'Bearer incorrect')]:
            with self.subTest(configured=len(configured), supplied=bool(supplied)):
                os.environ['COINGLASS_COLLECTOR_TOKEN']=configured
                request=make_mocked_request('GET','/api/collection/model1/source-probe/evidence',
                                            headers={'Authorization':supplied} if supplied else {})
                with self.assertRaises(web.HTTPUnauthorized):
                    await app.diagnostic_image(request)

    async def test_correct_bridge_token_can_read_existing_evidence_only(self):
        os.environ['COINGLASS_COLLECTOR_TOKEN']=SYNTHETIC_TOKEN
        request=make_mocked_request('GET','/api/collection/model1/source-probe/evidence',
                                    headers={'Authorization':'Bearer '+SYNTHETIC_TOKEN})
        response=await app.diagnostic_image(request)
        self.assertIsInstance(response, web.FileResponse)
        self.assertEqual(response.headers['Cache-Control'], 'no-store')

    async def test_evidence_path_traversal_is_rejected(self):
        os.environ['COINGLASS_COLLECTOR_TOKEN']=SYNTHETIC_TOKEN
        (self.directory/'report.json').write_text(json.dumps({'image':'../secret.env'}))
        request=make_mocked_request('GET','/evidence',headers={'Authorization':'Bearer '+SYNTHETIC_TOKEN})
        with self.assertRaises(web.HTTPNotFound):
            await app.diagnostic_image(request)

    async def test_old_public_endpoint_removed_even_with_old_flag(self):
        os.environ['PUBLIC_SOURCE_DIAGNOSTIC']='true'
        fake_bridge=types.SimpleNamespace(JobStore=Mock(), register_collection_routes=Mock())
        with patch.dict('sys.modules', {'collection_bridge':fake_bridge}):
            application=app.create_app()
        routes={route.resource.canonical for route in application.router.routes()}
        self.assertNotIn('/source-probe.png',routes)
        self.assertIn('/api/collection/model1/source-probe/evidence',routes)
        fake_bridge.JobStore.assert_not_called()

    async def test_health_exposes_presence_not_cookie_contents(self):
        os.environ['COINGLASS_COOKIE_HEADER']=SYNTHETIC_SESSION
        fake_bridge=types.SimpleNamespace(JobStore=Mock(), register_collection_routes=Mock())
        with patch.dict('sys.modules', {'collection_bridge':fake_bridge}):
            application=app.create_app()
        route=next(r for r in application.router.routes() if r.resource.canonical=='/health' and r.method=='GET')
        response=await route.handler(make_mocked_request('GET','/health'))
        result=json.loads(response.text)
        self.assertTrue(result['source_session_configured'])
        self.assertNotIn(SYNTHETIC_SESSION,response.text)
        self.assertEqual(result['mode'],'not_configured')


if __name__=='__main__':
    unittest.main()
