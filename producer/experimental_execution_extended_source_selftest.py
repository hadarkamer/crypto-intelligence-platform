"""Offline tests of all eight source envelopes; no formula or alert mutation."""
from copy import deepcopy
from datetime import timedelta
import hashlib
import json
from pathlib import Path
import socket
import unittest
from unittest.mock import patch

import experimental_execution_bridge as bridge
import experimental_execution_contract as contract
from experimental_execution_fixtures import hype_row71205_message, sol_g65_message
import hype_row71205_experimental_store as hype
import sol_g65_experimental_store as g65
from experimental_execution_bridge_selftest import maxpain_pending_fixture
from experimental_execution_source_audit_selftest import BASE, CONFIG, SCOPE, SUBSCRIPTION, ReadOnlySource, ms
from xrp_r2732_experimental_store_selftest import MemoryDatabase

MINUTE = timedelta(minutes=1)


def source_fixture(kind, *, reference=100., offset=30):
    store = hype if kind == 'hype' else g65
    db = MemoryDatabase()
    decision = BASE + timedelta(minutes=offset)
    entry = decision + MINUTE
    now = entry + timedelta(seconds=10)
    with patch.object(store, '_connect', db.connect):
        store.initialize_scope(SUBSCRIPTION, BASE, config_sha256=CONFIG)
        store.reserve_signal(SUBSCRIPTION, decision, entry, reference,
                             {'valid': True, 'signal': True}, now, config_sha256=CONFIG)
        if kind == 'hype':
            claimed = store.claim_pending(SUBSCRIPTION, now, config_sha256=CONFIG)
            assert claimed is not None
            store.finish_attempt(SUBSCRIPTION, claimed['intent_id'], claimed['attempt_token'],
                'DELIVERED', now, message_id=123, config_sha256=CONFIG)
        state = store.snapshot(SUBSCRIPTION)
    return dict(state=state, database=db, store=store, entry=entry, now=now,
                key=store.key_for(SUBSCRIPTION))


def project(f, *, state=None, now=None, fence=BASE):
    builder = bridge.hype_row71205_messages if f['store'] is hype else bridge.sol_g65_messages
    return builder(state or f['state'], now_ms=ms(now or f['now']), fence_ms=ms(fence))


def advance(f, *, open=100., high=100.1, low=99.9, close=100., when=None):
    when = when or f['entry']
    now = when + MINUTE
    with patch.object(f['store'], '_connect', f['database'].connect):
        f['store'].advance_position(SUBSCRIPTION, [dict(open_at=when, open=open, high=high,
            low=low, close=close)], now, config_sha256=CONFIG)
        state = f['store'].snapshot(SUBSCRIPTION)
    return state, now


class ExtendedSourceTests(unittest.TestCase):
    def setUp(self):
        guard = patch.object(socket.socket, 'connect', side_effect=AssertionError('OFFLINE_ONLY'))
        guard.start(); self.addCleanup(guard.stop)

    def test_new_fixtures_match_real_frozen_source_arithmetic(self):
        for kind, builder in [('hype', hype_row71205_message), ('g65', sol_g65_message)]:
            for price in (100., 123.456789, .1532498):
                with self.subTest(kind=kind, price=price):
                    f = source_fixture(kind, reference=price)
                    kw = {'entry' if kind == 'hype' else 'reference': price,
                          'decision_ms': ms(f['entry'] - MINUTE), 'as_of_ms': ms(f['now'])}
                    self.assertEqual(project(f)[0], builder(**kw))

    def test_projection_preserves_source_state_and_outbox(self):
        for kind in ('hype', 'g65'):
            f = source_fixture(kind); before = deepcopy(f['state'])
            self.assertEqual(len(project(f)), 1)
            self.assertEqual(f['state'], before)

    def test_g65_plan_at_original_reference_minute_not_delayed(self):
        f = source_fixture('g65'); value = project(f)[0]
        self.assertEqual(value['kind'], 'PLAN')
        self.assertEqual(value['arm_at'], f['entry'].isoformat())
        self.assertIsNone(value['expires_at'])
        self.assertEqual(f['state']['intents'], [])
        self.assertEqual(value['policy']['capacity'], 1)
        self.assertIsNone(value['policy']['pending_timeout'])

    def test_g65_no_fixed_expiry_and_heartbeat_cannot_claim_new_plan(self):
        f = source_fixture('g65'); initial = project(f)[0]
        state, now = advance(f)
        value = project(f, state=state, now=now)[0]
        self.assertEqual(value['kind'], 'HEARTBEAT')
        self.assertEqual(contract.plan_digest(initial), contract.plan_digest(value))
        self.assertIsNone(value['expires_at'])
        self.assertEqual(contract.moment_ms(value['valid_until']) - ms(now), 90_000)
        value['kind'] = 'PLAN'; value['source_sequence'] = ms(now)*10+1
        with self.assertRaisesRegex(contract.ContractError, 'PROSPECTIVE_PLAN_REQUIRED'):
            contract.validate(value)

    def test_g65_pending_survives_many_days_with_fresh_monitoring_without_formula_expiry(self):
        f = source_fixture('g65'); state = deepcopy(f['state'])
        now = f['entry'] + timedelta(days=10)
        state['active'].update(closed_through=now.isoformat(), monitor_status='VERIFIED',
                               bar_cursor=(now-MINUTE).isoformat())
        value = project(f, state=state, now=now)[0]
        self.assertEqual(value['kind'], 'HEARTBEAT')
        self.assertIsNone(value['expires_at'])

    def test_g65_source_fill_cancels_remaining_execution_entry_not_demo_exposure(self):
        f = source_fixture('g65'); before = project(f)[0]
        state, now = advance(f, high=100.6, close=100.4)
        self.assertEqual(state['active']['phase'], 'OPEN')
        value = project(f, state=state, now=now)[0]
        self.assertEqual(value['kind'], 'CANCEL')
        self.assertEqual(value['cancel_reason'], 'SOURCE_ENTRY_OBSERVED')
        self.assertEqual(contract.plan_digest(before), contract.plan_digest(value))

    def test_g65_cancellation_and_ambiguous_entry_cancel_are_terminal(self):
        for high, expected in [(100.1, 'SOURCE_OBSERVATION_ENDED'), (100.6, 'SOURCE_MONITOR_UNVERIFIED')]:
            f = source_fixture('g65')
            state, now = advance(f, high=high, low=98.9)
            value = project(f, state=state, now=now)[0]
            self.assertEqual(value['kind'], 'CANCEL')
            self.assertEqual(value['cancel_reason'], expected)

    def test_g65_promoted_source_stop_never_rewrites_initial_geometry(self):
        f = source_fixture('g65'); before = project(f)[0]
        state, now = advance(f, open=100.6, high=100.7, low=98.7, close=98.75)
        self.assertIsNotNone(state['active']['lock_triggered_at'])
        value = project(f, state=state, now=now)[0]
        self.assertEqual(value['stop'], before['stop'])
        self.assertEqual(contract.plan_digest(value), contract.plan_digest(before))

    def test_current_source_generation_required_old_venue_positions_excluded(self):
        for kind in ('hype', 'g65'):
            for field, value in [('source_contract_version', 'old-v1'), ('price_source', 'BINANCE_SPOT_TRADE_1M')]:
                f = source_fixture(kind)
                if kind == 'hype':
                    f['state']['intents'][0]['payload'][field] = value
                else:
                    f['state']['active'][field] = value
                self.assertEqual(project(f), [])

    def test_activation_fence_prevents_existing_pending_and_historical_replay(self):
        for kind in ('hype', 'g65'):
            f = source_fixture(kind)
            self.assertEqual(project(f, fence=f['entry'] + timedelta(seconds=1)), [])

    def test_source_gap_and_unverified_monitor_cancel_admission(self):
        for kind in ('hype', 'g65'):
            f = source_fixture(kind)
            self.assertEqual(project(f, now=f['entry']+MINUTE)[0]['cancel_reason'],
                             'SOURCE_MONITOR_NOT_CAUGHT_UP')
            f['state']['active']['monitor_status'] = 'PRICE_GAP'
            self.assertEqual(project(f)[0]['cancel_reason'], 'SOURCE_MONITOR_UNVERIFIED')

    def test_hype_only_delivered_intents_and_90_second_reference(self):
        for status in ('PENDING', 'IN_FLIGHT', 'UNKNOWN', 'FAILED', 'EXPIRED'):
            f = source_fixture('hype'); f['state']['intents'][0]['status'] = status
            self.assertEqual(project(f), [])
        f = source_fixture('hype')
        value = project(f, now=f['entry'] + timedelta(seconds=90))[0]
        self.assertEqual(value['cancel_reason'], 'EXPIRED_REFERENCE')

    def test_hype_source_closed_or_uncertain_blocks_new_entry(self):
        f = source_fixture('hype')
        state, now = advance(f, low=97.)
        self.assertIsNone(state['active'])
        self.assertEqual(project(f, state=state, now=now)[0]['cancel_reason'], 'SOURCE_OBSERVATION_ENDED')

    def test_formula_and_policy_tampering_rejected(self):
        for builder in (hype_row71205_message, sol_g65_message):
            changes = [('side', 'LONG'), ('symbol', 'BTC'), ('stop', '999'),
                       ('take_profit', '1'), ('rule_id', 'RETIRED_FORMULA')]
            for field, replacement in changes:
                value = builder(); value[field] = replacement
                value['occurrence_id'] = contract.occurrence_id(value)
                with self.subTest(builder=builder.__name__, field=field), self.assertRaises(contract.ContractError):
                    contract.validate(value)
            value = builder(); value['policy']['capacity'] = True
            with self.assertRaises(contract.ContractError): contract.validate(value)
        value = sol_g65_message(); value['expires_at'] = value['valid_until']
        with self.assertRaisesRegex(contract.ContractError, 'SOL_G65_TIMES'): contract.validate(value)

    def test_recipient_duplicates_have_one_decision_identity(self):
        for kind in ('hype', 'g65'):
            f = source_fixture(kind); other = source_fixture(kind)
            self.assertNotEqual(f['state']['active']['position_id'], other['state']['active']['position_id'])
            self.assertEqual(project(f)[0], project(other)[0])

    def test_recipient_cancel_dominates_fresh_plan(self):
        f = source_fixture('g65'); cancelled = deepcopy(f['state'])
        cancelled['active']['monitor_status'] = 'AMBIGUOUS'
        second_scope = '2'*64
        second_key = g65.key_for('general-watch:' + second_scope)
        source = ReadOnlySource({f['key']: json.dumps(f['state']), second_key: json.dumps(cancelled)})
        with patch('alert_cards_forwarder._source_dsn', return_value='mock-only'):
            result = bridge.read_experimental({SCOPE, second_scope}, BASE, now=f['now'],
                env={'EXPERIMENTAL_EXECUTION_BRIDGE_MODE': bridge.MODE}, connect=source.connect)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['kind'], 'CANCEL')
        self.assertTrue(all(sql.startswith('SELECT') for sql, _ in source.calls))

    def test_retired_and_restored_maxpain_never_exported_even_with_evidence(self):
        for flag in ('legacy_formula', 'manual_source_restore', 'retired_rule'):
            f = maxpain_pending_fixture()
            p = f['state']['active'][0]
            if flag == 'retired_rule': p['rule_id'] = 'SOL_RETIRED_FORMULA'
            else: p[flag] = True
            self.assertEqual(bridge.maxpain_messages(f['state'], now_ms=f['now_ms'], fence_ms=f['fence_ms']), [])

    def test_shared_contract_and_fixture_copies_identical(self):
        import hl_testnet_runtime
        root = Path(__file__).resolve().parent
        # Resolve the actual receiver package also inside the isolated runner's
        # immutable snapshots; do not depend on the original checkout name.
        other = Path(hl_testnet_runtime.__file__).resolve().parent.parent
        for name in ('experimental_execution_contract.py', 'experimental_execution_fixtures.py'):
            self.assertEqual((root/name).read_bytes(), (other/name).read_bytes())


if __name__ == '__main__':
    unittest.main()
