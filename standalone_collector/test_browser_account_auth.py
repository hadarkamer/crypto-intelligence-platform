"""Offline contracts for narrowly scoped browser account authentication.

Only synthetic credentials and Request/Route/Response objects are used. These
tests prove adapter behavior, not Chromium CORS behavior or live CoinGlass access.
"""
from contextlib import redirect_stderr, redirect_stdout
import io
import json
import logging
import os
import re
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import source_account_diagnostic as account


ORIGIN = 'https://www.coinglass.com'
PAGE = ORIGIN + '/pro/futures/LiquidationHeatMap?coin=BTC&type=symbol'
ENDPOINT = 'https://capi.coinglass.com/coin-community/api/userapi/info'
TOKEN = 'synthetic+credential%2Fopaque'
ENV = {'COINGLASS_COOKIE_HEADER': 'theme=dark; obe=synthetic%2Bcredential%252Fopaque'}


class Request:
    def __init__(self, *, url=ENDPOINT, method='GET', resource_type='xhr',
                 headers=None, frame_url=PAGE, header_error=None):
        self.url = url
        self.method = method
        self.resource_type = resource_type
        self.frame = SimpleNamespace(url=frame_url)
        self._headers = dict(headers if headers is not None else {
            'origin': ORIGIN, 'accept': 'application/json',
            'referer': ORIGIN + '/', 'language': 'en',
        })
        self.header_error = header_error

    def header_value(self, name):
        if self.header_error:
            raise self.header_error
        for key, value in self._headers.items():
            if key.lower() == name.lower():
                return value
        return None

    def all_headers(self):
        if self.header_error:
            raise self.header_error
        return dict(self._headers)

    @property
    def headers(self):
        return self.all_headers()


class Response:
    def __init__(self, *, status=200, headers=None, body=None, dispose_error=None):
        self.status = status
        self.headers = dict(headers if headers is not None else {
            'access-control-allow-origin': ORIGIN,
            'content-type': 'application/json',
        })
        self.payload = body if body is not None else b'{"success":true,"data":"SYNTHETIC_PRIVATE"}'
        self.disposed = 0
        self.dispose_error = dispose_error
        self.body_reads = 0

    def body(self):
        self.body_reads += 1
        raise AssertionError('Adapter must not inspect account response bytes')

    def json(self):
        self.body_reads += 1
        raise AssertionError('Adapter must not parse account data')

    def text(self):
        self.body_reads += 1
        raise AssertionError('Adapter must not read account text')

    def dispose(self):
        self.disposed += 1
        if self.dispose_error:
            raise self.dispose_error


class Route:
    def __init__(self, request=None, response=None, *, fetch_error=None,
                 fulfill_error=None, abort_error=None):
        self.request = request or Request()
        self.response = response or Response()
        self.fetch_error = fetch_error
        self.fulfill_error = fulfill_error
        self.abort_error = abort_error
        self.fetches = []
        self.fulfillments = []
        self.aborts = []
        self.fallbacks = []
        self.continues = []

    def fetch(self, **kwargs):
        self.fetches.append(kwargs)
        if self.fetch_error:
            raise self.fetch_error
        return self.response

    def fulfill(self, **kwargs):
        self.fulfillments.append(kwargs)
        if self.fulfill_error:
            raise self.fulfill_error

    def abort(self, *args, **kwargs):
        self.aborts.append((args, kwargs))
        if self.abort_error:
            raise self.abort_error

    def fallback(self, **kwargs):
        self.fallbacks.append(kwargs)

    def continue_(self, **kwargs):
        self.continues.append(kwargs)


class Context:
    def __init__(self):
        self.routes = []

    def route(self, pattern, handler):
        self.routes.append((pattern, handler))


class BrowserAccountAuthenticationTests(unittest.TestCase):
    def test_common_setup_and_native_gate_keep_all_nine_identities(self):
        import heatmap_models as models
        import model1_execution as execution
        for model in (1, 2, 3):
            for timeframe in ('12h', '24h', '48h'):
                for gate_states in ([True], [False, True], [False, False]):
                    with self.subTest(model=model, timeframe=timeframe, gate=gate_states), \
                            patch.dict(os.environ, ENV, clear=True), \
                            patch.object(models, 'HEATMAP_MODEL', model):
                        context = Context()
                        execution.prepare_context(context)
                        handlers = [handler for pattern, handler in context.routes
                                    if pattern.fullmatch(ENDPOINT)]
                        self.assertEqual(len(handlers), 1)
                        route = Route(Request(frame_url=models.source_url(model)))
                        handlers[0](route)
                        self.assert_authenticated_once(route)
                        page = Mock(url=models.source_url(model))
                        page.evaluate.side_effect = gate_states
                        calls = []
                        def _select_timeframe(page, selected):
                            calls.append(selected)
                            return 'selected'
                        if any(gate_states):
                            with self.assertRaises(execution.StageFailure) as caught:
                                execution.control(_select_timeframe, page, timeframe)
                            self.assertEqual(caught.exception.code, 'source_login_required')
                            self.assertEqual(caught.exception._model1_capture_phase, 'select_' + timeframe)
                            self.assertEqual(calls, [] if gate_states[0] else [timeframe])
                        else:
                            self.assertEqual(execution.control(_select_timeframe, page, timeframe), 'selected')
                            self.assertEqual(calls, [timeframe])
                        page.reload.assert_not_called()

    def test_setup_registration_failure_keeps_existing_gate_usable(self):
        import model1_execution as execution
        context = Context()
        original = context.route
        def register(pattern, handler):
            if pattern.fullmatch(ENDPOINT):
                raise RuntimeError('SYNTHETIC_PRIVATE_REGISTRATION')
            original(pattern, handler)
        context.route = register
        with patch.dict(os.environ, ENV, clear=True):
            execution.prepare_context(context)
        self.assertEqual(len(context.routes), 1)
        page = Mock(url=PAGE)
        page.evaluate.return_value = True
        def _select_timeframe(*args):
            self.fail('Blocked source must not reach selection')
        with self.assertRaises(execution.StageFailure) as caught:
            execution.control(_select_timeframe, page, '12h')
        self.assertEqual(caught.exception.code, 'source_login_required')

    def install(self, environment=None):
        context = Context()
        account.prepare_account_auth(context, env=dict(ENV if environment is None else environment))
        return context

    def run_route(self, route=None, environment=None):
        context = self.install(environment)
        self.assertEqual(len(context.routes), 1)
        route = route or Route()
        context.routes[0][1](route)
        return route

    def assert_untouched(self, route):
        self.assertEqual(route.fetches, [])
        self.assertEqual(route.fulfillments, [])
        self.assertEqual(route.aborts, [])
        self.assertEqual(route.fallbacks + route.continues, [{}])

    def assert_authenticated_once(self, route, token=TOKEN):
        self.assertEqual(len(route.fetches), 1)
        forwarded = route.fetches[0]
        self.assertEqual(forwarded['headers']['obe'], token)
        self.assertEqual(forwarded['max_redirects'], 0)
        self.assertEqual(forwarded['max_retries'], 0)
        self.assertEqual(forwarded['timeout'], 12000)
        self.assertEqual(route.fallbacks + route.continues, [])

    def test_missing_or_invalid_configuration_installs_no_route(self):
        environments = [
            {}, {'COINGLASS_COOKIE_HEADER': ''},
            {'COINGLASS_COOKIE_HEADER': 'theme=dark'},
            {'COINGLASS_COOKIE_HEADER': 'Cookie: obe=not-a-valid-header'},
            {'COINGLASS_COOKIE_HEADER': 'obe=bad%0D%0AInjected:header'},
            {'COINGLASS_STORAGE_STATE_JSON': 'not-json'},
        ]
        for environment in environments:
            with self.subTest(environment=list(environment)):
                self.assertEqual(self.install(environment).routes, [])

    def test_same_decoder_preserves_plus_and_decodes_exactly_once(self):
        self.assertEqual(account._session_value(ENV), TOKEN)
        route = self.run_route()
        self.assert_authenticated_once(route)
        self.assertEqual(route.fulfillments, [{'response': route.response}])
        self.assertEqual(route.response.disposed, 1)
        self.assertEqual(route.response.body_reads, 0)

    def test_existing_request_headers_are_preserved_and_input_not_mutated(self):
        request = Request(headers={'origin': ORIGIN, 'cookie': 'synthetic-other=value',
                                  'accept': 'application/json', 'cache-ts-v2': '1234'})
        before = dict(request._headers)
        route = self.run_route(Route(request))
        self.assertEqual(route.fetches[0]['headers'], {**before, 'obe': TOKEN})
        self.assertEqual(request._headers, before)

    def test_storage_state_uses_existing_decoder_without_local_storage(self):
        environment = {'COINGLASS_STORAGE_STATE_JSON': json.dumps({
            'cookies': [{'name': 'obe', 'value': 'state%2Btoken',
                         'domain': '.coinglass.com', 'path': '/'}],
            'origins': [{'origin': ORIGIN, 'localStorage': [{'name': 'irrelevant', 'value': 'unused'}]}],
        })}
        route = self.run_route(environment=environment)
        self.assert_authenticated_once(route, 'state+token')

    def test_header_configuration_retains_existing_precedence(self):
        environment = dict(ENV, COINGLASS_STORAGE_STATE_JSON=json.dumps({
            'cookies': [{'name': 'obe', 'value': 'other', 'domain': '.coinglass.com', 'path': '/'}],
        }))
        self.assert_authenticated_once(self.run_route(environment=environment))

    def test_each_context_gets_its_own_credential(self):
        first, second = self.install(), self.install({'COINGLASS_COOKIE_HEADER': 'obe=other-context'})
        a, b = Route(), Route()
        first.routes[0][1](a)
        second.routes[0][1](b)
        self.assert_authenticated_once(a)
        self.assert_authenticated_once(b, 'other-context')

    def test_existing_obe_is_never_replaced_including_empty_or_mixed_case(self):
        for name, value in [('obe', 'frontend-token'), ('ObE', 'different'), ('obe', '')]:
            with self.subTest(name=name, empty=value == ''):
                route = self.run_route(Route(Request(headers={'origin': ORIGIN, name: value})))
                self.assert_untouched(route)

    def test_exact_account_url_and_https_default_port_are_supported(self):
        for url in [ENDPOINT, ENDPOINT + '?cache=synthetic', ENDPOINT.replace('.com/', '.com:443/')]:
            for resource_type in ['xhr', 'fetch']:
                with self.subTest(url=url, resource_type=resource_type):
                    self.assert_authenticated_once(self.run_route(Route(Request(url=url, resource_type=resource_type))))

    def test_registration_pattern_does_not_capture_map_or_foreign_origins(self):
        pattern, _ = self.install().routes[0]
        self.assertIsInstance(pattern, re.Pattern)
        self.assertIsNotNone(pattern.fullmatch(ENDPOINT))
        for url in [
            'https://capi.coinglass.com/api/index/aggregate/liqHeatMap',
            ENDPOINT.replace('capi.coinglass.com', 'not-coinglass.example'),
            ENDPOINT + '/extra', ENDPOINT.replace('https:', 'http:'),
        ]:
            with self.subTest(url=url):
                self.assertIsNone(pattern.fullmatch(url))

    def test_negative_url_scope_is_defended_inside_handler(self):
        urls = [
            ENDPOINT.replace('https:', 'http:'),
            ENDPOINT.replace('.com/', '.com:444/'),
            ENDPOINT.replace('capi.coinglass.com', 'capi.coinglass.com.evil.example'),
            ENDPOINT.replace('capi.coinglass.com', 'www.coinglass.com'),
            ENDPOINT.replace('capi.coinglass.com', 'name@capi.coinglass.com'),
            ENDPOINT + '/extra', ENDPOINT + '#fragment',
            'https://capi.coinglass.com/api/index/aggregate/liqHeatMap',
        ]
        for url in urls:
            with self.subTest(url=url):
                self.assert_untouched(self.run_route(Route(Request(url=url))))

    def test_no_preflight_navigation_or_non_get_credential_injection(self):
        for method, resource_type in [('OPTIONS', 'xhr'), ('POST', 'xhr'),
                                      ('GET', 'document'), ('GET', 'image'), ('GET', 'script')]:
            with self.subTest(method=method, resource_type=resource_type):
                self.assert_untouched(self.run_route(Route(Request(method=method, resource_type=resource_type))))

    def test_foreign_missing_null_or_malformed_origin_is_not_authenticated(self):
        for origin in [None, 'null', 'https://evil.example', ORIGIN + '.evil.example',
                       'http://www.coinglass.com', ORIGIN + '/']:
            with self.subTest(origin=origin):
                headers = {} if origin is None else {'origin': origin}
                self.assert_untouched(self.run_route(Route(Request(headers=headers))))

    def test_request_must_belong_to_coin_glass_page(self):
        for frame_url in ['about:blank', 'https://evil.example/',
                          'https://www.coinglass.com.evil.example/',
                          'http://www.coinglass.com/', 'https://www.coinglass.com:444/',
                          'https://name@www.coinglass.com/']:
            with self.subTest(frame_url=frame_url):
                self.assert_untouched(self.run_route(Route(Request(frame_url=frame_url))))

    def test_redirects_are_not_followed_or_fulfilled(self):
        for status in [300, 301, 302, 303, 304, 307, 308, 399]:
            with self.subTest(status=status):
                response = Response(status=status, headers={
                    'location': 'https://untrusted.example/collect',
                    'access-control-allow-origin': ORIGIN,
                })
                route = self.run_route(Route(response=response))
                self.assert_authenticated_once(route)
                self.assertEqual(len(route.aborts), 1)
                self.assertEqual(route.fulfillments, [])
                self.assertEqual(response.disposed, 1)

    def test_cors_missing_or_disallowed_is_not_synthesized(self):
        for allowed in [None, '', 'null', 'https://evil.example', ORIGIN + ', https://evil.example']:
            with self.subTest(allowed=allowed):
                headers = {} if allowed is None else {'access-control-allow-origin': allowed}
                response = Response(headers=headers)
                route = self.run_route(Route(response=response))
                self.assert_authenticated_once(route)
                self.assertEqual(route.fulfillments, [])
                self.assertEqual(len(route.aborts), 1)
                self.assertEqual(response.headers, headers)
                self.assertEqual(response.disposed, 1)

    def test_upstream_wildcard_cors_is_preserved_without_adding_credentials(self):
        response = Response(headers={'access-control-allow-origin': '*', 'content-type': 'application/json'})
        before = dict(response.headers)
        route = self.run_route(Route(response=response))
        self.assertEqual(route.fulfillments, [{'response': response}])
        self.assertEqual(response.headers, before)
        self.assertNotIn('access-control-allow-credentials', response.headers)

    def test_http_and_application_failures_are_passed_through_unchanged(self):
        for status, body in [(200, b'{"success":false,"error":"permission"}'),
                             (401, b'Unauthorized'), (403, b'Forbidden'),
                             (429, b'Rate limited'), (500, b'Unavailable')]:
            with self.subTest(status=status):
                response = Response(status=status, body=body)
                route = self.run_route(Route(response=response))
                self.assert_authenticated_once(route)
                self.assertEqual(route.fulfillments, [{'response': response}])
                self.assertEqual(response.status, status)
                self.assertEqual(response.payload, body)
                self.assertEqual(response.body_reads, 0)
                self.assertEqual(response.disposed, 1)
                self.assertEqual(route.aborts, [])

    def test_failures_do_not_leak_secrets_or_retry_and_always_release_response(self):
        secret = 'SYNTHETIC_SECRET_DO_NOT_PRINT'
        for fault in ['headers', 'fetch', 'fulfill', 'dispose', 'abort']:
            with self.subTest(fault=fault):
                error = RuntimeError(secret)
                response = Response(dispose_error=error if fault == 'dispose' else None)
                request = Request(header_error=error if fault == 'headers' else None)
                route = Route(request, response, fetch_error=error if fault == 'fetch' else None,
                              fulfill_error=error if fault == 'fulfill' else None,
                              abort_error=error if fault == 'abort' else None)
                if fault == 'abort':
                    response.status = 302
                output = io.StringIO()
                logger = logging.StreamHandler(output)
                logging.getLogger().addHandler(logger)
                try:
                    with redirect_stdout(output), redirect_stderr(output):
                        self.run_route(route)
                finally:
                    logging.getLogger().removeHandler(logger)
                self.assertEqual(output.getvalue(), '')
                self.assertLessEqual(len(route.fetches), 1)
                self.assertEqual(route.fallbacks + route.continues, [])
                if fault in ('headers', 'fetch'):
                    self.assertEqual(response.disposed, 0)
                else:
                    self.assertEqual(response.disposed, 1)
                if fault != 'dispose':
                    self.assertGreaterEqual(len(route.aborts), 1)

    def test_installation_never_calls_separate_account_diagnostic(self):
        with patch.object(account, 'check_account_response', side_effect=AssertionError('No extra GET')):
            self.assert_authenticated_once(self.run_route())

    def test_registration_failure_is_silent_and_does_not_start_a_request(self):
        class ClosedContext:
            def __init__(self):
                self.calls = 0

            def route(self, pattern, handler):
                self.calls += 1
                raise RuntimeError('SYNTHETIC_SECRET_REGISTRATION_ERROR')

        context = ClosedContext()
        output = io.StringIO()
        with redirect_stdout(output), redirect_stderr(output), patch.object(
                account, 'check_account_response', side_effect=AssertionError('No extra GET')):
            account.prepare_account_auth(context, env=ENV)
        self.assertEqual(output.getvalue(), '')
        self.assertEqual(context.calls, 1)

    def test_header_discovered_by_full_header_read_is_not_overwritten(self):
        class LateHeader(Request):
            def header_value(self, name):
                if name.lower() == 'obe':
                    return None
                return super().header_value(name)

        route = self.run_route(Route(LateHeader(headers={'origin': ORIGIN, 'ObE': 'frontend-value'})))
        self.assert_untouched(route)


if __name__ == '__main__':
    unittest.main()
