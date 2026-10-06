"""Short stream release fences; no environment changes, keys, HTTP or orders."""
from copy import deepcopy
import unittest
from unittest.mock import patch

from . import checks, two_account_execution as roles


CARD = 'a' * 64
OTHER = 'b' * 64


def continuous_env():
    return dict(RENDER_SERVICE_ID=roles.SERVICE,
        HL_TESTNET_RUNTIME_MODE='long_stream_testnet_v1',
        HL_TESTNET_FILLED_DISPATCH='approved_long_stream_v1',
        HL_TESTNET_LONG_STREAM='approved_alerts_v1',
        HL_TESTNET_SHORT_STREAM='approved_alerts_v1',
        HL_TESTNET_LONG_ENTRY_ENABLED='false',
        HL_TESTNET_SHORT_ENTRY_ENABLED='false',
        HL_TESTNET_TWO_ACCOUNT_EXECUTION='disabled',
        HL_TESTNET_JOURNAL_BACKEND='staging_postgres_v1',
        HL_TESTNET_EMERGENCY_RELEASE=roles.CONTINUOUS_RELEASE,
        HL_TESTNET_EMERGENCY_CLOSE='approved_testnet_v1')


class ContinuousShortScopeTests(unittest.TestCase):
    def setUp(self):
        network = patch('http.client.HTTPSConnection',
                        side_effect=AssertionError('No network in release validation'))
        network.start()
        self.addCleanup(network.stop)

    def test_continuous_release_accepts_any_valid_card_without_enabling_entries(self):
        env = continuous_env()
        before = deepcopy(env)
        self.assertIsNone(roles.short_entry_scope(env))
        for card in (CARD, OTHER):
            self.assertIsNone(roles.short_entry_scope(env, card))
        self.assertEqual(env, before)
        self.assertEqual(env['HL_TESTNET_SHORT_ENTRY_ENABLED'], 'false')

    def test_existing_trial_remains_exact_in_both_releases(self):
        for env in ({'HL_TESTNET_SHORT_TRIAL_CARD_ID': CARD},
                    {**continuous_env(), 'HL_TESTNET_SHORT_TRIAL_CARD_ID': CARD}):
            self.assertEqual(roles.short_entry_scope(env), CARD)
            self.assertEqual(roles.short_entry_scope(env, CARD), CARD)
            with self.assertRaisesRegex(checks.Blocked, 'SHORT_ENTRY_OUTSIDE_EXACT_TRIAL_CARD'):
                roles.short_entry_scope(env, OTHER)

    def test_original_release_without_exact_card_stays_blocked(self):
        for env in ({}, {**continuous_env(), 'HL_TESTNET_EMERGENCY_RELEASE': ''}):
            with self.assertRaisesRegex(checks.Blocked, 'SHORT_ENTRY_OUTSIDE_EXACT_TRIAL_CARD'):
                roles.short_entry_scope(env, CARD)

    def test_each_continuous_boundary_is_required(self):
        baseline = continuous_env()
        for key in baseline:
            env = {k: v for k, v in baseline.items() if k != key}
            with self.subTest(key=key), self.assertRaises(checks.Blocked):
                roles.short_entry_scope(env, CARD)
        for key, value in (
                ('RENDER_SERVICE_ID', 'mainnet_service'),
                ('HL_TESTNET_RUNTIME_MODE', 'single_testnet_attempt_v1'),
                ('HL_TESTNET_EMERGENCY_RELEASE', 'mainnet'),
                ('HL_TESTNET_EMERGENCY_CLOSE', 'disabled'),
                ('HL_TESTNET_SHORT_ENTRY_ENABLED', 'TRUE'),
                ('HL_TESTNET_SHORT_STREAM', 'all_alerts'),
                ('HL_TESTNET_TWO_ACCOUNT_EXECUTION', 'approved_single_attempt_v1'),
                ('HL_TESTNET_FILLED_CARD_ID', CARD),
                ('HL_TESTNET_SAFETY_PIPELINE', 'parallel_pipeline'),
                ('HL_TESTNET_CARD_SYNC', 'parallel_sync')):
            with self.subTest(key=key, value=value), self.assertRaises(checks.Blocked):
                roles.short_entry_scope({**baseline, key: value}, CARD)

    def test_malformed_fence_cannot_widen_a_bounded_experiment(self):
        for value in ('x', 'A' * 64, True, 'a' * 63, 'a' * 65):
            for env in ({'HL_TESTNET_SHORT_TRIAL_CARD_ID': value},
                        {**continuous_env(), 'HL_TESTNET_SHORT_TRIAL_CARD_ID': value}):
                with self.subTest(value=value), self.assertRaises(checks.Blocked):
                    roles.short_entry_scope(env, CARD)

    def test_invalid_candidate_identity_is_rejected_in_continuous_scope(self):
        for value in ('', True, 'a' * 63, 'A' * 64):
            with self.subTest(value=value), self.assertRaises(checks.Blocked):
                roles.short_entry_scope(continuous_env(), value)

    def test_unknown_release_cannot_hide_behind_an_exact_trial(self):
        with self.assertRaisesRegex(checks.Blocked, 'SHORT_ENTRY_SCOPE_CONFIGURATION_REQUIRED'):
            roles.short_entry_scope(dict(HL_TESTNET_EMERGENCY_RELEASE='automatic',
                                        HL_TESTNET_SHORT_TRIAL_CARD_ID=CARD), CARD)


if __name__ == '__main__':
    unittest.main()
