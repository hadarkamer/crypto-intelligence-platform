"""Focused simulated CDP contracts. These are NOT Chromium/wire proofs."""
import json
import unittest
from types import SimpleNamespace

from account_cdp import ACCOUNT_URL, PAGE_ORIGIN, AccountHeaderExperiment


DUMMY = 'fixture-only+value'
ENV = {'COINGLASS_COOKIE_HEADER': 'obe=fixture-only%2Bvalue'}


class Session:
    def __init__(self):
        self.calls = []
        self.listeners = {}
        self.fail = set()
        self.detached = False

    def send(self, command, params=None):
        self.calls.append((command, params))
        if command in self.fail:
            raise RuntimeError('dummy transport failure')
        if command == 'Page.getFrameTree':
            return {'frameTree': {'frame': {'id': 'frame-1'}}}
        return {}

    def on(self, name, callback):
        self.listeners[name] = callback

    def remove_listener(self, name, callback):
        if self.listeners.get(name) == callback:
            del self.listeners[name]

    def detach(self):
        self.detached = True


class AccountCDPContracts(unittest.TestCase):
    def make(self):
        session = Session()
        page = SimpleNamespace(url=PAGE_ORIGIN + '/pro/futures/LiquidationHeatMap',
                               context=SimpleNamespace(new_cdp_session=lambda _: session))
        adapter = AccountHeaderExperiment(page, ENV).start()
        self.addCleanup(adapter.close)
        return adapter, session

    def network(self, adapter, *, url=ACCOUNT_URL, method='GET', request_id='net-1', **changes):
        event = {'requestId': request_id, 'frameId': 'frame-1', 'type': 'Fetch',
                 'documentURL': PAGE_ORIGIN + '/pro/futures/LiquidationHeatMap',
                 'initiator': {'type': 'script'}, 'request': {'url': url, 'method': method}}
        event.update(changes)
        adapter._on_request(event)

    def pause(self, adapter, *, url=ACCOUNT_URL, method='GET', headers=None, **changes):
        event = {'requestId': 'fetch-1', 'networkId': 'net-1', 'frameId': 'frame-1',
                 'resourceType': 'Fetch', 'request': {'url': url, 'method': method,
                 'headers': {'Origin': PAGE_ORIGIN} if headers is None else headers}}
        event.update(changes)
        adapter._on_paused(event)

    def extra(self, adapter, value=DUMMY):
        adapter._on_extra_info({'requestId': 'net-1', 'headers': {'obe': value,
                                'Cookie': 'unrelated-dummy-do-not-retain'}})

    def test_allowed_original_request_uses_existing_decoder_and_preserves_headers(self):
        adapter, session = self.make()
        self.network(adapter)
        self.pause(adapter, headers={'Origin': PAGE_ORIGIN, 'Accept': 'application/json', 'obe': None})
        command, params = session.calls[-1]
        self.assertEqual(command, 'Fetch.continueRequest')
        headers = {item['name']: item['value'] for item in params['headers']}
        self.assertTrue(headers['obe'] == DUMMY)
        self.assertEqual(headers['Accept'], 'application/json')
        self.assertEqual(headers['Origin'], PAGE_ORIGIN)
        self.assertEqual(adapter.evidence()['injections'], 1)

    def test_exact_target_method_origin_and_page_provenance(self):
        cases = [
            {'url': ACCOUNT_URL.replace('capi.', 'other.')},
            {'url': ACCOUNT_URL + '/other'},
            {'url': ACCOUNT_URL.replace('https:', 'http:')},
            {'method': 'POST'}, {'method': 'OPTIONS'},
            {'headers': {'Origin': 'https://different.invalid'}},
            {'frameId': 'frame-other'}, {'resourceType': 'Document'},
        ]
        for case in cases:
            with self.subTest(case=list(case)):
                adapter, session = self.make()
                self.network(adapter)
                self.pause(adapter, **case)
                self.assertNotIn('headers', session.calls[-1][1])
                self.assertEqual(adapter.evidence()['injections'], 0)
        adapter, session = self.make()
        self.network(adapter, initiator={'type': 'other'})
        self.pause(adapter)
        self.assertNotIn('headers', session.calls[-1][1])

    def test_existing_empty_or_nonempty_header_is_not_overwritten(self):
        for value in ('', 'existing-dummy'):
            with self.subTest(empty=not value):
                adapter, session = self.make()
                self.network(adapter)
                self.pause(adapter, headers={'Origin': PAGE_ORIGIN, 'ObE': value})
                self.assertNotIn('headers', session.calls[-1][1])
                self.assertEqual(adapter.evidence()['injections'], 0)

    def test_redirect_and_return_to_target_never_reinject(self):
        adapter, session = self.make()
        self.network(adapter)
        self.pause(adapter)
        self.network(adapter, url='https://other.invalid/dummy', redirectResponse={'status': 302})
        self.network(adapter, redirectResponse={'status': 302})
        self.pause(adapter, requestId='fetch-3', redirectedRequestId='fetch-2')
        self.assertNotIn('headers', session.calls[-1][1])
        self.assertEqual(adapter.evidence()['injections'], 1)
        self.extra(adapter)
        adapter._on_terminal({'requestId': 'net-1'})
        self.assertEqual(adapter.evidence()['send_state'], 'not_verified')

    def test_redirect_provenance_rejects_even_fresh_network_id(self):
        adapter, session = self.make()
        self.network(adapter)
        self.pause(adapter, redirectedRequestId='prior-fetch')
        self.assertNotIn('headers', session.calls[-1][1])
        self.assertEqual(adapter.evidence()['injections'], 0)

    def test_pause_before_network_waits_for_verified_original_provenance(self):
        adapter, session = self.make()
        self.pause(adapter)
        self.assertFalse(any(command == 'Fetch.continueRequest' for command, _ in session.calls))
        self.assertEqual(adapter.evidence()['injections'], 0)
        self.network(adapter)
        self.assertEqual(adapter.evidence()['injections'], 1)
        self.assertFalse(adapter._pending)
        self.extra(adapter)
        adapter._on_terminal({'requestId': 'net-1'})
        self.assertIn('headers', session.calls[-1][1])
        self.assertEqual(adapter.evidence()['send_state'], 'verified')

    def test_missing_network_event_is_not_injected_and_cleanup_aborts_pause(self):
        adapter, session = self.make()
        self.pause(adapter)
        self.assertEqual(adapter.evidence()['send_state'], 'not_verified')
        self.assertEqual(adapter.evidence()['injections'], 0)
        adapter.close()
        self.assertTrue(any(command == 'Fetch.failRequest' for command, _ in session.calls))
        self.assertFalse(adapter._pending)
        self.assertEqual(adapter.evidence()['cleanup']['paused_remaining'], 0)

    def test_extra_info_before_or_after_request_correlates_only_once(self):
        for early in (True, False):
            with self.subTest(extra_before_request=early):
                adapter, _ = self.make()
                if early:
                    self.extra(adapter)
                self.network(adapter)
                self.pause(adapter)
                if not early:
                    self.extra(adapter)
                self.assertEqual(adapter.evidence()['send_state'], 'not_verified')
                adapter._on_terminal({'requestId': 'net-1'})
                evidence = adapter.evidence()
                self.assertEqual(evidence['send_state'], 'verified')
                self.assertIs(evidence['header_present'], True)
                self.assertIs(evidence['header_matches'], True)
                self.assertNotIn('account_api_success', evidence)

    def test_missing_or_duplicate_extra_info_is_not_verified(self):
        for count in (0, 2):
            with self.subTest(extra_count=count):
                adapter, _ = self.make()
                self.network(adapter)
                self.pause(adapter)
                for _ in range(count):
                    self.extra(adapter)
                adapter._on_terminal({'requestId': 'net-1'})
                evidence = adapter.evidence()
                self.assertEqual(evidence['send_state'], 'not_verified')
                self.assertIsNone(evidence['header_matches'])

    def test_mismatched_extra_info_does_not_claim_header_match(self):
        adapter, _ = self.make()
        self.network(adapter)
        self.pause(adapter)
        self.extra(adapter, 'different-dummy')
        adapter._on_terminal({'requestId': 'net-1'})
        self.assertIs(adapter.evidence()['header_matches'], False)

    def test_nonascii_or_ambiguous_header_cannot_claim_match(self):
        for headers in ({'obe': '\u2603'}, {'obe': DUMMY, 'ObE': DUMMY}):
            with self.subTest(ambiguous=len(headers) > 1):
                adapter, _ = self.make()
                self.network(adapter)
                self.pause(adapter)
                adapter._on_extra_info({'requestId': 'net-1', 'headers': headers})
                adapter._on_terminal({'requestId': 'net-1'})
                self.assertIs(adapter.evidence()['header_present'], True)
                self.assertIs(adapter.evidence()['header_matches'], False)

    def test_failed_continue_aborts_and_cleanup_is_reported(self):
        adapter, session = self.make()
        self.network(adapter)
        session.fail.add('Fetch.continueRequest')
        self.pause(adapter)
        self.assertEqual(session.calls[-1][0], 'Fetch.failRequest')
        self.assertEqual(adapter.evidence()['send_state'], 'not_verified')
        adapter.close()
        cleanup = adapter.evidence()['cleanup']
        self.assertEqual(cleanup['paused_remaining'], 0)
        self.assertTrue(all(cleanup[k] for k in ('fetch_disabled', 'listeners_removed', 'detached', 'reference_cleared')))
        self.assertFalse(session.listeners)
        self.assertIsNone(adapter._value)

    def test_cleanup_failure_remains_visible(self):
        adapter, session = self.make()
        session.fail.add('Fetch.disable')
        adapter.close()
        self.assertIn('cleanup_disable_failed', adapter.evidence()['errors'])
        self.assertFalse(adapter.evidence()['cleanup']['fetch_disabled'])
        self.assertTrue(adapter.evidence()['cleanup']['detached'])

    def test_no_body_commands_or_raw_evidence_retained(self):
        adapter, session = self.make()
        self.network(adapter)
        self.pause(adapter)
        self.extra(adapter)
        adapter._on_terminal({'requestId': 'net-1'})
        adapter.close()
        for command, _ in session.calls:
            self.assertNotIn('Body', command)
            self.assertNotIn('fulfill', command.lower())
        serialized = json.dumps({'evidence': adapter.evidence(), 'records': adapter._records})
        self.assertNotIn(DUMMY, serialized)
        self.assertNotIn('unrelated-dummy-do-not-retain', serialized)
        self.assertNotIn('Cookie', serialized)


if __name__ == '__main__':
    unittest.main()
