"""Public address assignment only, with no real accounts, keys or network calls."""
from copy import deepcopy
import json
import unittest
from unittest.mock import patch
from . import approved_account_assignment as a

A, B, C, D = ['0x' + v * 40 for v in ('1', '2', '3', '4')]


def config():
    return {'HL_TESTNET_LONG_ACCOUNT_SOURCE': a.MODE,
        'HL_TESTNET_LONG_EXPECTED_SUFFIX': '1111',
        'HL_TESTNET_ACCOUNT_ADDRESS': A, 'HL_TESTNET_AGENT_ADDRESS': B,
        'HL_TESTNET_SHORT_ACCOUNT_ADDRESS': C, 'HL_TESTNET_SHORT_AGENT_ADDRESS': D}


def routes():
    return {'long_account': {'account': A, 'agent': B, 'status': 'CONFIGURED_NOT_VERIFIED'},
            'short_account': {'account': C, 'agent': D, 'status': 'CONFIGURED_NOT_VERIFIED'}}


class Reader:
    def __init__(self):
        self.calls = 0
        self.seen = []
        self.wrong = False
        self.fail = False
        self.mode = 'default'
    def read(self, kind, *, user):
        self.calls += 1
        self.seen.append((kind, user))
        if self.fail:
            raise ValueError('DO_NOT_LOG_CONFIGURATION')
        if kind == 'userAbstraction':
            return 'unifiedAccount' if user == A else self.mode
        if kind != 'userRole':
            raise AssertionError('No other endpoint')
        if user in (A, C):
            return {'role': 'user'}
        return {'role': 'agent', 'data': {'user': A if user == B or self.wrong else C}}


class AssignmentTests(unittest.TestCase):
    def setUp(self):
        p = patch('http.client.HTTPSConnection', side_effect=AssertionError('No real network'))
        p.start()
        self.addCleanup(p.stop)

    def test_no_implicit_use_of_existing_pair(self):
        env = config(); env.pop('HL_TESTNET_LONG_ACCOUNT_SOURCE')
        self.assertEqual(a.public_route_env(env)['HL_TESTNET_LONG_ACCOUNT_ADDRESS'], '')

    def test_approved_alias_uses_only_existing_public_pair(self):
        out = a.public_route_env(config())
        self.assertEqual(out['HL_TESTNET_LONG_ACCOUNT_ADDRESS'], A)
        self.assertEqual(out['HL_TESTNET_LONG_AGENT_ADDRESS'], B)
        self.assertEqual(out['HL_TESTNET_SHORT_ACCOUNT_ADDRESS'], C)
        self.assertEqual(set(out), set(a.PUBLIC_FIELDS))

    def test_input_environment_unchanged(self):
        env = config(); before = deepcopy(env)
        a.public_route_env(env)
        self.assertEqual(env, before)

    def test_secrets_not_read_and_environment_not_enumerated(self):
        class Env(dict):
            def get(self, key, default=None):
                if key.endswith('_KEY') or key in ('DATABASE_URL', 'HL_TESTNET_DATABASE_URL'):
                    raise AssertionError('No secrets')
                return super().get(key, default)
            def __iter__(self):
                raise AssertionError('No environment enumeration')
            def items(self):
                raise AssertionError('No environment enumeration')
        a.public_route_env(Env(config()))

    def test_unknown_mode_rejected(self):
        with self.assertRaises(a.AssignmentError):
            a.public_route_env({**config(), 'HL_TESTNET_LONG_ACCOUNT_SOURCE': 'automatic'})

    def test_missing_bad_or_wrong_suffix_rejected(self):
        for suffix in ('', None, '11', '2222', True):
            with self.assertRaises(a.AssignmentError):
                a.public_route_env({**config(), 'HL_TESTNET_LONG_EXPECTED_SUFFIX': suffix})

    def test_invalid_legacy_address_rejected(self):
        for value in ('', None, '0x' + 'a'*64, '0x' + '0'*40):
            with self.assertRaises(a.AssignmentError):
                a.public_route_env({**config(), 'HL_TESTNET_AGENT_ADDRESS': value})

    def test_master_as_agent_rejected(self):
        with self.assertRaises(a.AssignmentError):
            a.public_route_env({**config(), 'HL_TESTNET_AGENT_ADDRESS': A})

    def test_matching_explicit_pair_is_allowed(self):
        env = {**config(), 'HL_TESTNET_LONG_ACCOUNT_ADDRESS': A, 'HL_TESTNET_LONG_AGENT_ADDRESS': B}
        self.assertEqual(a.public_route_env(env)['HL_TESTNET_LONG_ACCOUNT_ADDRESS'], A)

    def test_conflicting_or_partial_pair_is_not_overwritten(self):
        for account, agent in ((C, D), (A, ''), ('', B)):
            with self.assertRaises(a.AssignmentError):
                a.public_route_env({**config(), 'HL_TESTNET_LONG_ACCOUNT_ADDRESS': account,
                                    'HL_TESTNET_LONG_AGENT_ADDRESS': agent})

    def test_case_normalization(self):
        x = '0x' + 'Ab'*18 + 'aabb'; y = '0x' + 'Cd'*20
        env = {**config(), 'HL_TESTNET_ACCOUNT_ADDRESS': x, 'HL_TESTNET_AGENT_ADDRESS': y,
               'HL_TESTNET_LONG_EXPECTED_SUFFIX': 'aabb'}
        self.assertEqual(a.public_route_env(env)['HL_TESTNET_LONG_ACCOUNT_ADDRESS'], x.lower())

    def test_both_public_links_checked_without_claiming_signing(self):
        reader = Reader(); result = a.review_routes(routes(), client=reader)
        self.assertTrue(result['both_public_links_verified'])
        self.assertEqual(reader.calls, 6)
        self.assertFalse(result['signing_tested'])
        self.assertFalse(result['entry_sending_enabled'])
        self.assertFalse(result['balance_checked'])
        self.assertFalse(result['ownership_verified'])
        self.assertEqual(result['order_requests_sent'], 0)

    def test_default_preserved_as_unresolved(self):
        result = a.review_routes(routes(), client=Reader())
        self.assertEqual(result['accounts']['short_account']['account_mode'], 'default')
        self.assertTrue(result['accounts']['short_account']['account_mode_requires_review'])
        self.assertFalse(result['accounts']['long_account']['account_mode_requires_review'])

    def test_wrong_agent_never_reported_as_verified(self):
        reader = Reader(); reader.wrong = True
        result = a.review_routes(routes(), client=reader)
        self.assertFalse(result['both_public_links_verified'])
        self.assertEqual(result['accounts']['short_account']['status'], 'PUBLIC_AGENT_LINK_MISMATCH')

    def test_read_failure_redacted(self):
        reader = Reader(); reader.fail = True
        result = a.review_routes(routes(), client=reader)
        self.assertFalse(result['both_public_links_verified'])
        self.assertNotIn('DO_NOT_LOG', json.dumps(result))

    def test_unknown_mode_not_guessed(self):
        reader = Reader(); reader.mode = 'PRIVATE_OR_UNRECOGNIZED_TEXT'
        result = a.review_routes(routes(), client=reader)
        self.assertEqual(result['accounts']['short_account']['account_mode'], 'UNKNOWN')
        self.assertNotIn(reader.mode, json.dumps(result))

    def test_public_report_does_not_export_complete_addresses(self):
        text = json.dumps(a.review_routes(routes(), client=Reader()))
        for address in (A, B, C, D):
            self.assertNotIn(address, text)

    def test_missing_pair_needs_no_network(self):
        empty = {role: {'account':None, 'agent':None, 'status':'WAITING_FOR_ACCOUNT'} for role in routes()}
        reader = Reader(); result = a.review_routes(empty, client=reader)
        self.assertFalse(result['both_public_links_verified'])
        self.assertEqual(reader.calls, 0)

    def test_reused_account_rejected_before_any_reads(self):
        r = routes(); r['short_account']['account'] = A
        reader = Reader()
        with self.assertRaises(a.AssignmentError):
            a.review_routes(r, client=reader)
        self.assertEqual(reader.calls, 0)

    def test_invalid_agent_rejected_before_any_reads(self):
        r = routes(); r['short_account']['agent'] = '0x' + 'a'*64
        reader = Reader()
        with self.assertRaises(a.AssignmentError):
            a.review_routes(r, client=reader)
        self.assertEqual(reader.calls, 0)

    def test_read_only_endpoint_types(self):
        reader = Reader(); a.review_routes(routes(), client=reader)
        self.assertEqual(set(kind for kind, _ in reader.seen), {'userRole','userAbstraction'})

    def test_single_missing_role_preserves_other_result(self):
        r = routes(); r['short_account'] = {'account':None, 'agent':None, 'status':'WAITING_FOR_ACCOUNT'}
        result = a.review_routes(r, client=Reader())
        self.assertTrue(result['accounts']['long_account']['public_agent_link_verified'])
        self.assertFalse(result['both_public_links_verified'])

    def test_agent_case_in_response_is_accepted(self):
        class Mixed(Reader):
            def read(self, kind, *, user):
                result = super().read(kind, user=user)
                if result.get('role') == 'agent' if isinstance(result, dict) else False:
                    result['data']['user'] = '0x' + result['data']['user'][2:].upper()
                return result
        self.assertTrue(a.review_routes(routes(), client=Mixed())['both_public_links_verified'])

    def test_route_input_is_not_mutated(self):
        r = routes(); before = deepcopy(r)
        a.review_routes(r, client=Reader())
        self.assertEqual(r, before)


class RoutingIntegrationTests(unittest.TestCase):
    def test_existing_card_router_consumes_approved_assignment(self):
        from .trade_cards import account_routes
        self.assertEqual(account_routes(config()), routes())

    def test_original_router_rejects_same_account_for_both_directions(self):
        from .trade_cards import account_routes, CardError
        with self.assertRaises(CardError):
            account_routes({**config(), 'HL_TESTNET_SHORT_ACCOUNT_ADDRESS': A})

    def test_original_router_still_does_not_fallback_without_opt_in(self):
        from .trade_cards import account_routes
        env = config(); env.pop('HL_TESTNET_LONG_ACCOUNT_SOURCE')
        self.assertEqual(account_routes(env)['long_account']['status'], 'WAITING_FOR_ACCOUNT')

    def test_startup_only_reviews_when_explicitly_enabled(self):
        from . import trade_cards_startup
        from .trade_card_store import CardStore
        from unittest.mock import Mock
        fake = Mock()
        fake.initialize.return_value = False
        fake.probe.return_value = {'separate_connection_verified':True}
        fake.import_legacy_reviews.return_value = []
        env = {**config(), 'HL_TESTNET_CARDS_PHASE1':'record_only_v1',
            'RENDER_SERVICE_ID':'srv-dakptbh594qs7395460g',
            'HL_TESTNET_RUNTIME_MODE':'cancel_monitor_testnet_v1',
            'HL_TESTNET_JOURNAL_BACKEND':'staging_postgres_v1'}
        with patch.object(trade_cards_startup, 'review_routes', return_value={'order_requests_sent':0}) as read, \
             patch('hl_testnet_runtime.trade_card_store.CardStore', return_value=fake):
            first = trade_cards_startup.run(env, journal=object())
            read.assert_not_called()
            second = trade_cards_startup.run({**env,'HL_TESTNET_ACCOUNT_ROUTE_REVIEW':'public_read_only_v1'}, journal=object())
            read.assert_called_once()
        self.assertEqual(first['status'],'CARD_STORAGE_AND_ROUTING_REVIEW_PASSED')
        self.assertEqual(second['account_assignment']['order_requests_sent'],0)
        self.assertFalse(second['entry_sending_enabled'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
