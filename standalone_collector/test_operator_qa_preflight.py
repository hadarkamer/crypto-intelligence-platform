"""Check sanitization and the exact configured QA Docker entry without any provider."""
import json
from contextlib import redirect_stdout
from io import StringIO
import socket
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import operator_qa_preflight as qa
import test_operator_container_capabilities as fixture


class QAPreflightTests(unittest.TestCase):
    def test_networked_policy_allows_only_neutral_fixture_origin(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            policy = root/'operator_chromium_policy.json'
            policy.write_text(json.dumps({'BrowserSignin':0,'URLAllowlist':['https://www.coinglass.com']}))
            with patch.object(qa,'HERE',root), patch.object(qa,'POLICY',root/'etc'/'qa.json'):
                qa.prepare_offline_policy()
                installed = json.loads(qa.POLICY.read_text())
                self.assertEqual(json.loads(policy.read_text()),installed)
                self.assertEqual(installed['URLAllowlist'],['about:blank','https://qa-source.invalid'])
                self.assertEqual(installed['URLBlocklist'],['*'])
                self.assertEqual(installed['ProxyServer'],'http://127.0.0.1:31497')
                self.assertEqual(installed['DnsOverHttpsMode'],'off')
                self.assertFalse(installed['QuicAllowed'])

    def test_status_never_exposes_raw_fixtures_paths_or_unrecognized_values(self):
        hostile = {'status':'some-token','reason':'http://private.invalid/',
                   'ordinary_chromium_sandbox':'unexpected string','debug':'arbitrary'}
        self.assertEqual(qa.checked_result(hostile,'dual_uid'),{'status':'check_failed'})
        self.assertEqual(qa.checked_result(hostile,'private_viewer'),
                         {'reason':'check_failed','isolated_operator_browser':False})

    def test_independent_capabilities_survive_partial_success(self):
        dual = {'status':'chromium_namespace_sandbox_unavailable','two_uid_confidential_boundary':True,
                'ordinary_chromium_sandbox':False,'provider_requests':0}
        self.assertEqual(qa.checked_result(dual,'dual_uid'),dual)
        complete = {'reason':'ready','isolated_operator_browser':True,'provider_requests':0}
        self.assertEqual(qa.checked_result(complete,'private_viewer'),complete)


class NeutralProxyTests(unittest.TestCase):
    def test_fixed_browser_outcomes_and_loopback_only_resolver_exclusion(self):
        from playwright.sync_api import Error
        with patch.object(fixture, '_private_access_denied', return_value=True), \
             patch('playwright.sync_api.sync_playwright') as factory:
            chromium = factory.return_value.__enter__.return_value.chromium
            page = chromium.launch.return_value.new_page.return_value
            page.url = 'about:blank'
            page.goto.side_effect = [None, Error('net::ERR_TUNNEL_CONNECTION_FAILED private-detail'),
                                     Error('net::ERR_BLOCKED_BY_ADMINISTRATOR private-detail')]
            page.locator.return_value.inner_text.return_value = 'ERR_BLOCKED_BY_ADMINISTRATOR'
            output = StringIO()
            with redirect_stdout(output):
                outcome = fixture._browser_fixture(None, 0, 0, None, 31497, 'qa-source.invalid')
            self.assertEqual(outcome, 'verified')
            args = chromium.launch.call_args.kwargs
            self.assertIs(args['chromium_sandbox'], True)
            self.assertEqual([v for v in args['args'] if v.startswith('--host-resolver-rules=')],
                             ['--host-resolver-rules=MAP * ~NOTFOUND, EXCLUDE 127.0.0.1'])
            self.assertIn('--proxy-server=http://127.0.0.1:31497', args['args'])
            self.assertEqual([call.args[0] for call in page.goto.call_args_list], [
                'about:blank', 'https://qa-source.invalid/offline-operator-fixture',
                'https://denied.invalid/offline-operator-fixture'])
            self.assertIn('allowed_navigation=tunnel_connection_failed', output.getvalue())
            self.assertIn('forbidden_navigation=blocked_by_policy', output.getvalue())
            self.assertNotIn('private-detail', output.getvalue())

    def test_navigation_diagnostics_are_safe_and_separate(self):
        from playwright.sync_api import Error, TimeoutError as NavigationTimeout
        for failure,expected in ((Error('net::ERR_NAME_NOT_RESOLVED'),'name_not_resolved'),
                                 (Error('net::ERR_PROXY_CONNECTION_FAILED'),'proxy_connection_failed'),
                                 (NavigationTimeout('private timeout detail'),'timeout'),
                                 (Error('private internal detail'),'other_error')):
            with self.subTest(expected=expected):
                page = Mock()
                page.goto.side_effect = failure
                self.assertEqual(fixture._qa_navigation(page, 'qa-source.invalid'), expected)
        with self.assertRaises(ValueError):
            fixture._qa_navigation(Mock(), 'not-allowlisted.invalid')
        self.assertEqual(qa.NAVIGATION_RESULTS, fixture.NAVIGATION_RESULTS)
        cleaned = qa.checked_result({'status':'restricted_proxy_unconfirmed',
                                     'allowed_navigation':'name_not_resolved','forbidden_navigation':['secret'],
                                     'proxy_accepted_connection':False}, 'dual_uid')
        self.assertEqual(cleaned, {'status':'restricted_proxy_unconfirmed',
                                   'allowed_navigation':'name_not_resolved','proxy_accepted_connection':False})

    def test_fragmented_incomplete_and_overlong_request_lines(self):
        client = Mock()
        client.recv.side_effect = [b'CON',b'NECT qa-source.invalid:443 HTTP/1.1\r',b'\nignored']
        self.assertEqual(fixture._request_line(client), b'CONNECT qa-source.invalid:443 HTTP/1.1')
        client.recv.side_effect = [b'CONNECT qa-source.invalid:443',b'']
        self.assertEqual(fixture._request_line(client), b'')
        client.recv.side_effect = [b'x'*4096]
        self.assertEqual(fixture._request_line(client), b'')

    def test_real_loopback_sink_distinguishes_accept_from_recognized_target(self):
        # Neither call resolves a hostname. The sink never opens an upstream connection.
        with fixture._OfflineProxy(source_host='qa-source.invalid') as proxy:
            for target,recognized in ((b'unrecognized.invalid',False),(b'qa-source.invalid',True)):
                with socket.socket() as client:
                    client.settimeout(2)
                    client.connect(('127.0.0.1',proxy.port))
                    client.sendall(b'CONNECT '+target+b':443 HTTP/1.1\r\n\r\n')
                    self.assertTrue(client.recv(256).startswith(b'HTTP/1.1 403'))
                self.assertTrue(proxy.accepted)
                self.assertIs(proxy.source_seen,recognized)
                self.assertFalse(proxy.forbidden_seen)
