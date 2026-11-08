"""Opt-in, loopback-only Playwright 1.55 HTTP-transport checks.

Run manually with RUN_LOCAL_ACCOUNT_TRANSPORT=1. No Chromium or CoinGlass request
is started. The adapter's Route.fetch call is mapped to APIRequestContext.get
against temporary 127.0.0.1 fixtures. This verifies real header transmission and
redirect handling by Playwright's API transport, not browser CORS or live login.
"""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib.metadata import version
import os
import threading
import unittest
from urllib.parse import urlsplit

import source_account_diagnostic as account
from test_browser_account_auth import Context, ENV, ORIGIN, Request, Route, TOKEN


class FixtureHandler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def do_GET(self):
        self.server.received.append({
            'path': self.path,
            'obe': self.headers.get('obe'),
            'origin': self.headers.get('origin'),
        })
        status, headers, body = self.server.reply
        self.send_response(status)
        for name, value in headers.items():
            self.send_header(name, value)
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Connection', 'close')
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        # No request paths, headers, or synthetic credentials in the test output.
        pass


def fixture_server():
    server = ThreadingHTTPServer(('127.0.0.1', 0), FixtureHandler)
    server.daemon_threads = True
    server.received = []
    server.reply = (200, {'access-control-allow-origin': ORIGIN}, b'{"success":true}')
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def close_fixture(server, thread):
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)
    if thread.is_alive():
        raise AssertionError('Loopback fixture did not stop')


class TransportResponse:
    def __init__(self, response):
        self.raw = response
        self.status = response.status
        self.headers = response.headers
        self.disposed = 0

    def dispose(self):
        self.disposed += 1
        self.raw.dispose()


class TransportRoute(Route):
    def __init__(self, client, endpoint):
        super().__init__(Request())
        self.client = client
        self.endpoint = endpoint
        self.transport_response = None
        self.fulfilled_status = None
        self.fulfilled_headers = None
        self.fulfilled_body = None

    def fetch(self, **kwargs):
        target = urlsplit(self.endpoint)
        if target.scheme != 'http' or target.hostname != '127.0.0.1':
            raise AssertionError('Only the local fixture may be contacted')
        self.fetches.append(kwargs)
        self.transport_response = TransportResponse(self.client.get(self.endpoint, **kwargs))
        return self.transport_response

    def fulfill(self, **kwargs):
        self.fulfillments.append(kwargs)
        response = kwargs['response']
        self.fulfilled_status = response.status
        self.fulfilled_headers = dict(response.headers)
        # Only the test sink reads these locally generated fixture bytes.
        # The production adapter still never parses/reads the response body.
        self.fulfilled_body = response.raw.body()


@unittest.skipUnless(os.environ.get('RUN_LOCAL_ACCOUNT_TRANSPORT') == '1',
                     'Opt-in loopback transport test; no browser is launched')
class BrowserAccountTransportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if version('playwright') != '1.55.0':
            raise AssertionError('Use the existing pinned Playwright 1.55.0 environment')
        from playwright.sync_api import sync_playwright
        cls.primary, primary_thread = fixture_server()
        cls.addClassCleanup(close_fixture, cls.primary, primary_thread)
        cls.secondary, secondary_thread = fixture_server()
        cls.addClassCleanup(close_fixture, cls.secondary, secondary_thread)
        cls.driver = sync_playwright().start()
        cls.addClassCleanup(cls.driver.stop)
        cls.client = cls.driver.request.new_context()
        cls.addClassCleanup(cls.client.dispose)
        cls.endpoint = 'http://127.0.0.1:%s/account' % cls.primary.server_port

    def setUp(self):
        self.primary.received.clear()
        self.secondary.received.clear()
        self.primary.reply = (200, {'access-control-allow-origin': ORIGIN}, b'{"success":true}')

    def run_adapter(self):
        context = Context()
        account.prepare_account_auth(context, env=ENV)
        self.assertEqual(len(context.routes), 1)
        route = TransportRoute(self.client, self.endpoint)
        context.routes[0][1](route)
        self.assertEqual(len(route.fetches), 1)
        self.assertEqual(len(self.primary.received), 1)
        self.assertEqual(self.primary.received[0]['path'], '/account')
        self.assertEqual(self.primary.received[0]['obe'], TOKEN)
        self.assertEqual(self.primary.received[0]['origin'], ORIGIN)
        self.assertIsNotNone(route.transport_response)
        self.assertEqual(route.transport_response.disposed, 1)
        self.assertEqual(route.fallbacks + route.continues, [])
        return route

    def test_decoded_header_reaches_http_transport_once(self):
        body = b'{"success":true,"data":"SYNTHETIC_PRIVATE_FIXTURE"}'
        self.primary.reply = (200, {'access-control-allow-origin': ORIGIN,
                                    'content-type': 'application/json'}, body)
        route = self.run_adapter()
        self.assertEqual(route.fulfilled_status, 200)
        self.assertEqual(route.fulfilled_body, body)
        self.assertEqual(route.aborts, [])

    def test_same_origin_redirect_target_receives_zero_requests(self):
        target = 'http://127.0.0.1:%s/redirect-sink' % self.primary.server_port
        self.primary.reply = (302, {'location': target,
                                    'access-control-allow-origin': ORIGIN}, b'')
        route = self.run_adapter()
        self.assertEqual(route.fulfillments, [])
        self.assertEqual(len(route.aborts), 1)
        self.assertEqual([r['path'] for r in self.primary.received], ['/account'])

    def test_other_port_redirect_target_receives_zero_requests(self):
        target = 'http://127.0.0.1:%s/redirect-sink' % self.secondary.server_port
        self.primary.reply = (307, {'location': target,
                                    'access-control-allow-origin': ORIGIN}, b'')
        route = self.run_adapter()
        self.assertEqual(route.fulfillments, [])
        self.assertEqual(len(route.aborts), 1)
        self.assertEqual(self.secondary.received, [])

    def test_application_and_http_denials_remain_unchanged(self):
        for status, body in [(200, b'{"success":false,"code":"no_permission"}'),
                             (401, b'Unauthorized'), (403, b'Forbidden'),
                             (429, b'Rate limited')]:
            with self.subTest(status=status):
                self.primary.received.clear()
                self.primary.reply = (status, {'access-control-allow-origin': ORIGIN,
                                               'content-type': 'application/json'}, body)
                route = self.run_adapter()
                self.assertEqual(route.fulfilled_status, status)
                self.assertEqual(route.fulfilled_body, body)
                self.assertEqual(route.aborts, [])

    def test_response_without_cors_permission_is_not_fulfilled(self):
        self.primary.reply = (200, {'content-type': 'application/json'}, b'{"success":true}')
        route = self.run_adapter()
        self.assertEqual(route.fulfillments, [])
        self.assertEqual(len(route.aborts), 1)


if __name__ == '__main__':
    unittest.main()
