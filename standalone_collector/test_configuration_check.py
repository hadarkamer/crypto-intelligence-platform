"""Offline fixtures only. Never use live keys or make a source/model request."""
import json
import unittest
from unittest.mock import Mock
from configuration_check import check_configuration, configuration_status

DUMMY = 'synthetic-not-a-real-credential'


class Response:
    def __init__(self, status=200, body=None):
        self.status_code = status
        self.body = json.dumps({'object': 'model', 'id': 'gpt-5.6'}).encode() if body is None else body
        self.closed = False
    def __enter__(self): return self
    def __exit__(self, *args): self.closed = True
    def iter_content(self, chunk_size):
        for i in range(0, len(self.body), chunk_size):
            yield self.body[i:i+chunk_size]


class Checks(unittest.TestCase):
    def run_check(self, response=None, env=None):
        get = Mock(return_value=Response() if response is None else response)
        values = {'OPENAI_API_KEY': DUMMY} if env is None else env
        return check_configuration(env=values, http_get=get), get

    def test_missing_key_never_requests(self):
        r, g = self.run_check(env={})
        g.assert_not_called()
        self.assertFalse(r['openai_key_present'])
        self.assertEqual(r['model_metadata_requests'], 0)

    def test_metadata_success_no_inference_or_source(self):
        r, g = self.run_check()
        g.assert_called_once()
        self.assertEqual(r['openai_check'], 'model_metadata_access_confirmed')
        self.assertEqual((r['inference_requests'], r['source_attempts'], r['sheet_writes']), (0,0,0))
        self.assertNotIn(DUMMY, json.dumps(r))
        self.assertFalse(g.call_args.kwargs['allow_redirects'])
        self.assertTrue(g.call_args.kwargs['stream'])

    def test_known_failure_statuses_no_raw_provider_body(self):
        for status, expected in ((401,'authentication_rejected'),(403,'metadata_permission_denied'),
                                 (404,'model_not_accessible'),(429,'rate_limited'),(502,'metadata_request_failed')):
            with self.subTest(status=status):
                r, g = self.run_check(Response(status, DUMMY.encode()))
                self.assertEqual(r['openai_check'], expected)
                self.assertNotIn(DUMMY, json.dumps(r))
                g.assert_called_once()

    def test_redirect_is_not_followed(self):
        r, g = self.run_check(Response(302, DUMMY.encode()))
        self.assertEqual(r['openai_check'], 'metadata_request_failed')
        self.assertFalse(g.call_args.kwargs['allow_redirects'])

    def test_malformed_metadata_fails_without_exposure(self):
        for body in (b'not json', b'{}', b'[]', b'{"id":"unexpected"}'):
            with self.subTest(body=body):
                r, _ = self.run_check(Response(body=body))
                self.assertNotEqual(r['openai_check'], 'model_metadata_access_confirmed')

    def test_large_response_is_bounded_and_closed(self):
        response = Response(body=b'X' * 65536)
        r, _ = self.run_check(response)
        self.assertEqual(r['openai_check'], 'invalid_metadata_response')
        self.assertTrue(response.closed)

    def test_bad_model_setting_does_not_request(self):
        r, g = self.run_check(env={'OPENAI_API_KEY':DUMMY,'OPENAI_MARKET_SCANNER_MODEL':'https://wrong.example/'})
        self.assertEqual(r['openai_check'], 'invalid_model_setting')
        g.assert_not_called()

    def test_transport_exception_is_redacted_and_not_retried(self):
        g = Mock(side_effect=RuntimeError(DUMMY))
        r = check_configuration(env={'OPENAI_API_KEY': DUMMY}, http_get=g)
        g.assert_called_once()
        self.assertEqual(r['openai_check'], 'metadata_request_failed')
        self.assertNotIn(DUMMY, json.dumps(r))

    def test_metadata_does_not_claim_all_configuration_is_ready(self):
        r, _ = self.run_check()
        self.assertFalse(r['bridge_token_configured'])
        self.assertFalse(r['storage_configured'])
        self.assertFalse(r['source_session_present'])
        self.assertFalse(r['collection_enabled'])

    def test_presence_fields_never_include_values(self):
        r = configuration_status({'OPENAI_API_KEY': DUMMY, 'DATABASE_URL': DUMMY,
            'COINGLASS_COOKIE_HEADER': DUMMY, 'COINGLASS_COLLECTOR_TOKEN': DUMMY * 2,
            'COLLECTION_BRIDGE_ENABLED':'true'})
        self.assertTrue(all(r.values()))
        self.assertNotIn(DUMMY, json.dumps(r))

    def test_whitespace_not_configured(self):
        r, g = self.run_check(env={'OPENAI_API_KEY':' ','DATABASE_URL':' ', 'COINGLASS_COOKIE_HEADER':' '})
        g.assert_not_called()
        self.assertFalse(r['openai_key_present'])
        self.assertFalse(r['source_session_present'])
        self.assertFalse(r['storage_configured'])

    def test_model_alias_mismatch_not_silently_changed(self):
        r, _ = self.run_check(Response(body=b'{"object":"model","id":"gpt-other"}'))
        self.assertFalse(r['model_id_matches_requested'])


if __name__ == '__main__':
    unittest.main()
