"""Offline regression for a historical source intent poisoning fresh approvals.

The legacy quote shape reproduces the 2026-10-08 HYPE incident. All state is
synthetic and all persistence uses the existing transactional SQL double.
"""
from contextlib import redirect_stdout
from copy import deepcopy
import io
import json
import socket
import unittest
from unittest.mock import patch

import approved_alert_contract as contract
import approved_alert_outbox as outbox
import approved_alert_producer as producer
import experimental_execution_bridge as bridge
import experimental_execution_forwarder as forwarder
import sol_proximity_experimental_signal as signal
import sol_proximity_experimental_store as store
from approved_alert_producer_selftest import approved_fixture, OutboxDatabase
from experimental_execution_bridge_selftest import maxpain_pending_fixture
from experimental_execution_source_audit_selftest import BASE, CONFIG, M, ms


def with_legacy_intent(state, exemplar):
    """Retain an old delivered notification after an explicit source migration."""
    state = deepcopy(state)
    cutoff = ms(BASE)
    previous = store.PRE_HYPERLIQUID_CONFIGS['HYPE']
    state['source_activated_ms'] = cutoff
    state['source_migration'] = dict(
        coin='HYPE', from_config_sha256=previous, to_config_sha256=CONFIG,
        migrated_ms=cutoff, cancelled_pending=0, preserved_filled_or_unknown=1,
        legacy_price_source='HYPERLIQUID_HYPE_PERPETUAL_TRADE_1M',
        new_price_source='HYPERLIQUID_HYPE_PERPETUAL_TRADE_1M',
        policy='CANCEL_PENDING_DRAIN_FILLED_ORIGINAL_SOURCE_REQUIRE_NEW_BASELINE')
    legacy = signal.initial(cutoff)
    legacy.update(from_config_sha256=previous,
                  source_route='HYPERLIQUID_HYPE_PERPETUAL_TRADE_1M')
    state['legacy_source_state'] = legacy
    intent = deepcopy(exemplar)
    p = intent['payload']
    identity = 'e' * 64
    p.update(position_id=identity, source_observed_ms=cutoff-10*M,
             decision_ms=cutoff-9*M, arm_ms=cutoff-8*M, fill_ms=cutoff-7*M,
             status='OPEN', cycle_id='historical-source-before-migration',
             source_quote=dict(price_source='binance_spot', price_pair='HYPEUSDT',
                               price_market=None, price_instrument=None))
    p.pop('manual_source_restore', None)
    intent.update(intent_id=identity, position_id=identity, status='DELIVERED',
                  expires_ms=p['fill_ms']+M+contract.LEASE_MS,
                  acknowledged_ms=p['fill_ms']+M+1_000)
    state['intents'].insert(0, intent)
    return state


def project(state, now):
    # The durable producer reconstructs without the sender's activation fence.
    return producer.approved_messages(state, now_ms=now, fence_ms=1)


class ApprovedLegacySourceTests(unittest.TestCase):
    def setUp(self):
        guard = patch.object(socket.socket, 'connect',
                             side_effect=AssertionError('Offline tests only'))
        guard.start()
        self.addCleanup(guard.stop)
        self.f = approved_fixture()
        self.state = with_legacy_intent(self.f['state'], self.f['state']['intents'][0])

    def test_historical_only_is_ignored_without_source_mutation_or_replay(self):
        state = deepcopy(self.state)
        state.update(active=[], history=[], intents=state['intents'][:1])
        before = deepcopy(state)
        self.assertEqual(project(state, self.f['now_ms']), [])
        self.assertEqual(project(state, self.f['now_ms']+24*60*M), [])
        self.assertEqual(state, before)

    def test_historical_intent_cannot_block_or_change_fresh_valid_alert(self):
        expected = project(self.f['state'], self.f['now_ms'])
        before = deepcopy(self.state)
        self.assertEqual(project(self.state, self.f['now_ms']), expected)
        self.assertEqual(self.state, before)
        self.assertEqual(len(expected), 1)
        self.assertEqual(expected[0]['kind'], 'ALERT')

    def test_current_wrong_source_still_fails_strict_validation(self):
        self.state['intents'][1]['payload']['source_quote']['price_source'] = 'binance_spot'
        with self.assertRaisesRegex(contract.ContractError, 'APPROVED_ALERT_POLICY'):
            project(self.state, self.f['now_ms'])

    def test_historical_identity_mismatch_is_not_hidden_by_exclusion(self):
        self.state['intents'][0]['intent_id'] = 'f' * 64
        with self.assertRaisesRegex(contract.ContractError, 'APPROVED_SOURCE_IDENTITY'):
            project(self.state, self.f['now_ms'])

    def test_current_identity_mismatch_still_fails(self):
        self.state['intents'][1]['position_id'] = 'f' * 64
        with self.assertRaisesRegex(contract.ContractError, 'APPROVED_SOURCE_IDENTITY'):
            project(self.state, self.f['now_ms'])

    def test_approval_exactly_at_migration_boundary_is_not_historical(self):
        old = self.state['intents'][0]
        old['payload']['fill_ms'] = self.state['source_activated_ms']-M
        old['expires_ms'] = self.state['source_activated_ms']+contract.LEASE_MS
        with self.assertRaisesRegex(contract.ContractError, 'APPROVED_ALERT_POLICY'):
            project(self.state, self.f['now_ms'])

    def test_missing_or_inconsistent_migration_cannot_bypass_policy(self):
        mutations = {
            'no_migration': lambda s: s.pop('source_migration'),
            'wrong_policy': lambda s: s['source_migration'].update(policy='UNKNOWN'),
            'wrong_coin': lambda s: s['source_migration'].update(coin='SOL'),
            'wrong_new_source': lambda s: s['source_migration'].update(new_price_source='binance_spot'),
            'wrong_destination': lambda s: s['source_migration'].update(to_config_sha256='b'*64),
            'invalid_destination': lambda s: (s.update(config_sha256='z'*64),
                    s['source_migration'].update(to_config_sha256='z'*64)),
            'wrong_previous': lambda s: s['legacy_source_state'].update(from_config_sha256='b'*64),
            'invalid_previous': lambda s: (s['source_migration'].update(from_config_sha256='z'*64),
                    s['legacy_source_state'].update(from_config_sha256='z'*64)),
            'same_configuration': lambda s: (s['source_migration'].update(from_config_sha256=CONFIG),
                    s['legacy_source_state'].update(from_config_sha256=CONFIG)),
            'missing_legacy': lambda s: s.pop('legacy_source_state'),
            'mismatched_activation': lambda s: s.update(source_activated_ms=s['source_activated_ms']+1),
            'future_migration': lambda s: (s.update(source_activated_ms=self.f['now_ms']+M),
                    s['source_migration'].update(migrated_ms=self.f['now_ms']+M)),
            'string_migration': lambda s: (s.update(source_activated_ms=str(ms(BASE))),
                    s['source_migration'].update(migrated_ms=str(ms(BASE)))),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                state = deepcopy(self.state)
                mutate(state)
                with self.assertRaisesRegex(contract.ContractError, 'APPROVED_ALERT_POLICY'):
                    project(state, self.f['now_ms'])

    def test_invalid_historical_decision_time_cannot_bypass_validation(self):
        for value in (0, True, 'old', self.state['source_activated_ms']):
            with self.subTest(value=value):
                state = deepcopy(self.state)
                state['intents'][0]['payload']['decision_ms'] = value
                with self.assertRaises((contract.ContractError, TypeError, ValueError)):
                    project(state, self.f['now_ms'])

    def test_admission_expiry_does_not_block_current_stale_cancel(self):
        now = self.f['approved_ms']+2*M
        self.assertEqual(project(self.state, now), [])
        self.state['intents'][1].update(status='CANCELLED_STALE_PRICE', acknowledged_ms=now)
        cancel, = project(self.state, now)
        self.assertEqual(cancel['kind'], 'CANCEL')
        self.assertEqual(cancel['cancel_reason'], 'SOURCE_STALE_PRICE')
        self.assertEqual(contract.immutable(cancel),
                         contract.immutable(project(self.f['state'], self.f['now_ms'])[0]))

    def test_retained_cancellation_survives_legacy_poison_old_ack_and_restart(self):
        for ended_in_history in (False, True):
            with self.subTest(ended_in_history=ended_in_history):
                f = approved_fixture()
                db = f['database']
                old, = project(f['state'], f['now_ms'])
                state = with_legacy_intent(f['state'], f['state']['intents'][0])
                ended = state['active'].pop()
                ended.update(status='TAKE_TOUCHED', terminal_ms=f['approved_ms']+4*M)
                state.update(history=[ended] if ended_in_history else [],
                             intents=state['intents'][:1], bar_cursor_ms=f['approved_ms']+4*M)
                now = f['approved_ms']+5*M
                with db.connect() as conn:
                    self.assertTrue(outbox.synchronize(conn, f['key'], state, now))
                    outbox.acknowledge(conn, [f['key']], old)
                    cancel, = outbox.read(conn, [f['key']])
                    self.assertEqual(cancel['kind'], 'CANCEL')
                    self.assertEqual(contract.immutable(cancel), contract.immutable(old))
                    outbox.acknowledge(conn, [f['key']], cancel)
                # A new connection and a stale snapshot model restart/recovery.
                with db.connect() as conn:
                    self.assertFalse(outbox.synchronize(conn, f['key'],
                        with_legacy_intent(f['state'], f['state']['intents'][0]), f['now_ms']))
                    self.assertEqual(outbox.read(conn, [f['key']]), [])
                row, = db.outbox.values()
                self.assertTrue(row['canceled'])
                self.assertIsNone(row['payload_json'])

    def test_already_admitted_pre_migration_row_keeps_its_required_cancellation(self):
        # This row itself will be excluded by the historical-intent rule.
        # Its durable validated payload must still withdraw old entry permission.
        state = deepcopy(self.f['state'])
        state['config_sha256'] = store.PRE_HYPERLIQUID_CONFIGS['HYPE']
        before = deepcopy(state)
        db = OutboxDatabase()
        with db.connect() as conn:
            self.assertTrue(outbox.synchronize(conn, self.f['key'], state, self.f['now_ms']))
            original, = outbox.read(conn, [self.f['key']])
        migrated_at = self.f['now_ms']+M
        store.migrate_price_source(state, migrated_at, CONFIG, 'HYPE')
        self.assertEqual(project(state, migrated_at), [])
        self.assertEqual(len(state['legacy_source_state']['active']), 1)
        with db.connect() as conn:
            self.assertTrue(outbox.synchronize(conn, self.f['key'], state, migrated_at))
            outbox.acknowledge(conn, [self.f['key']], original)
            cancel, = outbox.read(conn, [self.f['key']])
            self.assertEqual(cancel['kind'], 'CANCEL')
            self.assertEqual(contract.immutable(cancel), contract.immutable(original))
            self.assertEqual(cancel['proof']['source_config_sha256'],
                             store.PRE_HYPERLIQUID_CONFIGS['HYPE'])
            outbox.acknowledge(conn, [self.f['key']], cancel)
        with db.connect() as conn:
            self.assertFalse(outbox.synchronize(conn, self.f['key'], before, self.f['now_ms']))
            self.assertEqual(outbox.read(conn, [self.f['key']]), [])

    def test_real_source_touch_commits_fresh_outbox_despite_old_intent(self):
        f = maxpain_pending_fixture(capture=False)
        state = with_legacy_intent(f['state'], self.f['state']['intents'][0])
        db = OutboxDatabase()
        db.values = deepcopy(f['database'].values)
        db.values[f['key']] = json.dumps(state)
        base = ms(BASE)
        entry = state['active'][0]['entry_price']
        bars = [[base+M, 100, 100.01, 99.99, 100],
                [base+2*M, entry, entry+.001, entry-.001, entry]]
        def committed(key):
            self.assertGreater(db.commits, 0)
            self.assertEqual(key, f['key'])
            self.assertEqual(len(db.outbox), 1)
        with patch.object(store, '_connect', db.connect), patch.dict('os.environ',
                {'EXPERIMENTAL_EXECUTION_BRIDGE_MODE': bridge.APPROVED_MODE}, clear=True), \
                patch.object(forwarder, 'notify_source_commit', side_effect=committed) as wake, \
                redirect_stdout(io.StringIO()) as output:
            store.advance(f['scope'], bars, base+3*M+10_000, config_sha256=CONFIG)
            result = store.snapshot(f['scope'])
        self.assertEqual(result['active'][0]['status'], 'OPEN')
        self.assertNotIn('outbox unavailable', output.getvalue())
        wake.assert_called_once_with(f['key'])
        with db.connect() as conn:
            value, = outbox.read(conn, [f['key']])
        expected, = project(self.f['state'], self.f['now_ms'])
        self.assertEqual(value, expected)
        self.assertEqual(result['intents'][0], state['intents'][0])


if __name__ == '__main__':
    unittest.main()
