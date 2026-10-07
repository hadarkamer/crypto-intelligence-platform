from copy import deepcopy
import unittest
from unittest.mock import patch

from . import experimental_live_release as release
from .experimental_execution_dispatch import BoundaryError


def environment():
    return dict(HL_TESTNET_RUNTIME_MODE=release.MODE,
                HL_TESTNET_EXPERIMENTAL_DISPATCH=release.DISPATCH,
                HL_TESTNET_EXPERIMENTAL_RELEASE_ID='a' * 64,
                HL_TESTNET_EXPERIMENTAL_ENTRY_ENABLED='false',
                HL_TESTNET_EXPERIMENTAL_PROTECTION_ENABLED='true',
                HL_TESTNET_JOURNAL_BACKEND='staging_postgres_v1',
                HL_TESTNET_LONG_ENTRY_ENABLED='false', HL_TESTNET_SHORT_ENTRY_ENABLED='false',
                HL_TESTNET_EXPERIMENTAL_PLAN_NOT_BEFORE='2026-10-06T18:00:00Z',
                HL_TESTNET_EXPERIMENTAL_ENTRY_UNTIL='2026-10-08T18:00:00Z',
                HL_TESTNET_LONG_ACCOUNT_ADDRESS='0x' + '1' * 40,
                HL_TESTNET_LONG_AGENT_ADDRESS='0x' + '3' * 40,
                HL_TESTNET_SHORT_ACCOUNT_ADDRESS='0x' + '2' * 40,
                HL_TESTNET_SHORT_AGENT_ADDRESS='0x' + '4' * 40)


class ReleaseTests(unittest.TestCase):
    def test_handover_attestation_fences_every_current_release_read(self):
        env=environment()
        env.update(HL_TESTNET_EXPERIMENTAL_ENTRY_ENABLED='true',
            HL_TESTNET_EXPERIMENTAL_HANDOVER=release.HANDOVER)
        loader=release.ReleaseLoader(env)
        self.assertFalse(loader()['entries_enabled'])
        env['HL_TESTNET_EXPERIMENTAL_PREDECESSOR_RETIRED_RELEASE']='a'*64
        self.assertTrue(loader()['entries_enabled'])
        env['HL_TESTNET_EXPERIMENTAL_PREDECESSOR_RETIRED_RELEASE']='b'*64
        value=loader()
        self.assertFalse(value['entries_enabled']); self.assertTrue(value['protection_enabled'])

    def test_default_off_does_not_construct_wallet_or_network(self):
        with patch('hl_testnet_runtime.two_account_execution.wallet_for_role', side_effect=AssertionError), \
             patch('socket.create_connection', side_effect=AssertionError):
            self.assertIsNone(release.configuration({}))
            self.assertIsNone(release.configuration({'HL_TESTNET_RUNTIME_MODE': 'read_only'}))

    def test_expired_or_paused_entries_preserve_protection(self):
        env = environment(); env['HL_TESTNET_EXPERIMENTAL_ENTRY_ENABLED'] = 'true'
        value = release.configuration(env)
        self.assertTrue(release.entry_enabled(value, value['not_before_ms']))
        self.assertFalse(release.entry_enabled(value, value['entry_expires_at_ms']))
        self.assertTrue(value['protection_enabled'])
        env['HL_TESTNET_EXPERIMENTAL_ENTRY_ENABLED'] = 'false'
        value = release.configuration(env)
        self.assertFalse(release.entry_enabled(value, value['not_before_ms']))
        self.assertTrue(value['protection_enabled'])

    def test_live_legacy_entry_or_shared_account_is_rejected(self):
        for key, value in (('HL_TESTNET_LONG_ENTRY_ENABLED', 'true'),
                           ('HL_TESTNET_SHORT_ENTRY_ENABLED', 'true'),
                           ('HL_TESTNET_EXPERIMENTAL_PROTECTION_ENABLED', 'false'),
                           ('HL_TESTNET_RUNTIME_MODE', 'long_stream_testnet_v1'),
                           ('HL_TESTNET_SHORT_ACCOUNT_ADDRESS', '0x' + '1' * 40)):
            with self.subTest(key=key):
                env = environment(); env[key] = value
                with self.assertRaises(BoundaryError):
                    release.configuration(env)

    def test_release_loader_detects_identity_change_but_observes_entry_halt(self):
        env = environment(); loader = release.ReleaseLoader(env)
        env['HL_TESTNET_EXPERIMENTAL_ENTRY_ENABLED'] = 'true'
        self.assertTrue(loader()['entries_enabled'])
        env['HL_TESTNET_EXPERIMENTAL_ENTRY_ENABLED'] = 'false'
        self.assertFalse(loader()['entries_enabled'])
        env['HL_TESTNET_EXPERIMENTAL_RELEASE_ID'] = 'b' * 64
        with self.assertRaisesRegex(BoundaryError, 'IDENTITY_CHANGED'):
            loader()

    def test_invalid_window_or_unrecognized_boolean_rejected_without_env_mutation(self):
        for patch_value in ({'HL_TESTNET_EXPERIMENTAL_ENTRY_UNTIL': '2026-10-06T17:00:00Z'},
                            {'HL_TESTNET_EXPERIMENTAL_ENTRY_ENABLED': 'yes'},
                            {'HL_TESTNET_EXPERIMENTAL_RELEASE_ID': ''}):
            env = {**environment(), **patch_value}; before = deepcopy(env)
            with self.assertRaises(BoundaryError):
                release.configuration(env)
            self.assertEqual(env, before)

    def test_continuous_policy_has_no_global_expiry_but_retains_start_and_halt(self):
        env = environment()
        env.pop('HL_TESTNET_EXPERIMENTAL_ENTRY_UNTIL')
        env.update(HL_TESTNET_EXPERIMENTAL_ENTRY_POLICY=release.CONTINUOUS_ENTRY,
                   HL_TESTNET_EXPERIMENTAL_ENTRY_ENABLED='true')
        value = release.configuration(env)
        self.assertEqual(value['entry_policy'], release.CONTINUOUS_ENTRY)
        self.assertIsNone(value['entry_expires_at_ms'])
        self.assertFalse(release.entry_enabled(value, value['not_before_ms']-1))
        self.assertTrue(release.entry_enabled(value, value['not_before_ms']))
        self.assertTrue(release.entry_enabled(value, value['not_before_ms']+365*86400000))
        env['HL_TESTNET_EXPERIMENTAL_ENTRY_ENABLED'] = 'false'
        paused = release.configuration(env)
        self.assertFalse(release.entry_enabled(paused, value['not_before_ms']+365*86400000))
        self.assertTrue(paused['protection_enabled'])

    def test_missing_deadline_never_silently_enables_continuous_entries(self):
        for policy in (None, release.BOUNDED_ENTRY, '', 'continuous', 'true'):
            with self.subTest(policy=policy):
                env = environment()
                env.pop('HL_TESTNET_EXPERIMENTAL_ENTRY_UNTIL')
                if policy is not None:
                    env['HL_TESTNET_EXPERIMENTAL_ENTRY_POLICY'] = policy
                with self.assertRaises(BoundaryError):
                    release.configuration(env)
        env = environment()
        env['HL_TESTNET_EXPERIMENTAL_ENTRY_POLICY'] = release.CONTINUOUS_ENTRY
        with self.assertRaises(BoundaryError):
            release.configuration(env)

    def test_runtime_policy_shape_rejects_ambiguous_or_incomplete_release(self):
        value = release.configuration(environment())
        value['entries_enabled'] = True
        value.pop('entry_policy')
        self.assertTrue(release.entry_enabled(value, value['not_before_ms']))
        self.assertFalse(release.entry_enabled(value, value['entry_expires_at_ms']))
        for change in ({'entry_expires_at_ms': None}, {'entry_expires_at_ms': True},
                       {'entry_policy': 'unknown'}, {'entry_policy': release.CONTINUOUS_ENTRY},
                       {'not_before_ms': True}):
            with self.subTest(change=change):
                malformed = {**value, **change}
                self.assertFalse(release.entry_window_valid(malformed))
                self.assertFalse(release.entry_enabled(malformed, value['not_before_ms']))
        value['entry_policy'] = release.CONTINUOUS_ENTRY
        value.pop('entry_expires_at_ms')
        self.assertFalse(release.entry_window_valid(value))

    def test_entry_policy_is_pinned_for_existing_loader(self):
        env = environment()
        loader = release.ReleaseLoader(env)
        original = loader()
        env.pop('HL_TESTNET_EXPERIMENTAL_ENTRY_UNTIL')
        env['HL_TESTNET_EXPERIMENTAL_ENTRY_POLICY'] = release.CONTINUOUS_ENTRY
        with self.assertRaisesRegex(BoundaryError, 'IDENTITY_CHANGED'):
            loader()
        continuous = release.ReleaseLoader(env)
        self.assertEqual(continuous(), release.ReleaseLoader(env)())
        env.pop('HL_TESTNET_EXPERIMENTAL_ENTRY_POLICY')
        env['HL_TESTNET_EXPERIMENTAL_ENTRY_UNTIL'] = '2026-10-08T18:00:00Z'
        with self.assertRaisesRegex(BoundaryError, 'IDENTITY_CHANGED'):
            continuous()
        self.assertEqual(release.configuration(env), original)

    def test_continuous_restart_still_requires_same_release_handover_attestation(self):
        env = environment()
        env.pop('HL_TESTNET_EXPERIMENTAL_ENTRY_UNTIL')
        env.update(HL_TESTNET_EXPERIMENTAL_ENTRY_POLICY=release.CONTINUOUS_ENTRY,
                   HL_TESTNET_EXPERIMENTAL_ENTRY_ENABLED='true',
                   HL_TESTNET_EXPERIMENTAL_HANDOVER=release.HANDOVER)
        start = release.configuration(env)['not_before_ms']
        self.assertFalse(release.entry_enabled(release.ReleaseLoader(env)(), start))
        env['HL_TESTNET_EXPERIMENTAL_PREDECESSOR_RETIRED_RELEASE'] = 'a'*64
        self.assertTrue(release.entry_enabled(release.ReleaseLoader(env)(), start+365*86400000))
        env['HL_TESTNET_EXPERIMENTAL_RELEASE_ID'] = 'b'*64
        self.assertFalse(release.entry_enabled(release.ReleaseLoader(env)(), start))
