"""Offline real-source contract, pre-touch projection, persistence regressions."""
from copy import deepcopy
from datetime import timedelta
import json
import socket
import unittest
from unittest.mock import patch

import experimental_execution_contract as contract
import experimental_execution_bridge as bridge
import sol_proximity_experimental_store as store
import sol_proximity_experimental_signal as signal
from maxpain_experimental_specs import HYPE_LONG_TF, SPECS
from experimental_execution_source_audit_selftest import (
    BASE, CONFIG, SUBSCRIPTION, SCOPE, M, _decoded, ms, r2732_fixture, ReadOnlySource)
from xrp_r2732_experimental_store_selftest import MemoryDatabase


def maxpain_pending_fixture(spec=HYPE_LONG_TF, *, growth=False, capture=True, liquidation_amount=100):
    """Return a real source-created pending plan before its arm minute."""
    db, base = MemoryDatabase(), ms(BASE)
    scope = SUBSCRIPTION if spec.coin == 'SOL' else SUBSCRIPTION + ':' + spec.rule_id
    target = 100 + (spec.lower_pct + spec.upper_pct) / 2
    with patch.object(store, '_connect', db.connect):
        store.initialize_scope(scope, base + 10_000, config_sha256=CONFIG)
        store.ingest(scope, _decoded(spec, base + 10_000, 100 + spec.lower_pct + .05),
            [[base, 100, 100.01, 99.99, 100]], base + 10_000,
            config_sha256=CONFIG, execution_enabled=capture)
        store.advance(scope, [[base, 100, 100.01, 99.99, 100]], base + M + 10_000, config_sha256=CONFIG)
        now = base + M + 10_000
        decoded = _decoded(spec, now, target)
        for row in decoded['rows']:
            row['liquidation_amount'] = liquidation_amount
        if growth:
            for row in decoded['rows']:
                row['liquidation_amount'] = (None if liquidation_amount is None else
                    liquidation_amount * 2 ** signal.TIMEFRAMES.index(row['timeframe']))
        store.ingest(scope, decoded, [[base + M, 100, 100.01, 99.99, 100]], now,
            config_sha256=CONFIG, execution_enabled=capture)
        state = store.snapshot(scope)
    return dict(state=state, now_ms=now, fence_ms=base, database=db, scope=scope,
                key=store.key_for(scope))


def maxpain_message(spec=HYPE_LONG_TF):
    f = maxpain_pending_fixture(spec)
    return bridge.maxpain_messages(f['state'], now_ms=f['now_ms'], fence_ms=f['fence_ms'])[0]


def r2732_message():
    f = r2732_fixture()
    return bridge.r2732_messages(f['state'], now_ms=ms(f['now']), fence_ms=ms(BASE))[0]


class BridgeTests(unittest.TestCase):
    def setUp(self):
        guard = patch.object(socket.socket, 'connect', side_effect=AssertionError('Offline tests only'))
        guard.start(); self.addCleanup(guard.stop)

    def test_disabled_reader_never_connects_or_resolves_dsn(self):
        with patch('alert_cards_forwarder._source_dsn', side_effect=AssertionError('must not resolve')):
            self.assertEqual(bridge.read_experimental({SCOPE}, BASE, env={}), [])
            self.assertEqual(bridge.read_experimental({SCOPE}, BASE, env={'EXPERIMENTAL_EXECUTION_BRIDGE_MODE': 'true'}), [])

    def test_all_five_real_pending_producers_validate_before_arm(self):
        for spec in SPECS.values():
            with self.subTest(coin=spec.coin):
                message = maxpain_message(spec)
                self.assertEqual(contract.validate(message), message)
                self.assertLess(contract.moment_ms(message['source_as_of']), contract.moment_ms(message['arm_at']))
                self.assertEqual(message['kind'], 'PLAN')
                self.assertEqual(message['source_state'], 'PENDING')

    def test_disabled_capture_preserves_existing_state_exactly(self):
        for spec in SPECS.values():
            with self.subTest(coin=spec.coin):
                before = maxpain_pending_fixture(spec, capture=False)
                after = maxpain_pending_fixture(spec, capture=True)
                reduced = deepcopy(after['state'])
                for plan in reduced['active']:
                    plan.pop('execution_evidence')
                self.assertEqual(before['state'], reduced)
                self.assertEqual(bridge.maxpain_messages(before['state'], now_ms=before['now_ms'], fence_ms=before['fence_ms']), [])

    def test_unprovable_execution_evidence_never_rolls_back_valid_notification(self):
        baseline = maxpain_pending_fixture(capture=False)
        with patch.object(bridge, '_maxpain_evidence', side_effect=contract.ContractError('MISSING_PROOF')):
            failed = maxpain_pending_fixture(capture=True)
        reduced = deepcopy(failed['state'])
        for plan in reduced['active']:
            self.assertEqual(plan.pop('execution_evidence_error'), 'PROOF_UNAVAILABLE')
        self.assertEqual(reduced, baseline['state'])
        self.assertEqual(bridge.maxpain_messages(failed['state'], now_ms=failed['now_ms'], fence_ms=failed['fence_ms']), [])

    def test_oversized_proof_cannot_exhaust_notification_transaction(self):
        baseline = maxpain_pending_fixture(capture=False)
        with patch.object(bridge, '_maxpain_evidence', return_value={'oversized': 'x' * (800 * 1024)}):
            failed = maxpain_pending_fixture(capture=True)
        reduced = deepcopy(failed['state'])
        for plan in reduced['active']:
            self.assertEqual(plan.pop('execution_evidence_error'), 'PROOF_UNAVAILABLE')
        self.assertEqual(reduced, baseline['state'])

    def test_later_state_growth_sheds_only_optional_execution_evidence(self):
        f = maxpain_pending_fixture()
        state = deepcopy(f['state'])
        original = deepcopy(state)
        for plan in original['active']:
            plan.pop('execution_evidence')
        maximum = len(store._encode(original).encode()) + 1
        with patch.object(store, 'MAX_BYTES', maximum):
            encoded = store._encode(state)
        self.assertEqual(json.loads(encoded), original)
        self.assertEqual(bridge.maxpain_messages(state, now_ms=f['now_ms'], fence_ms=f['fence_ms']), [])
        with patch.object(store, 'MAX_BYTES', 1):
            with self.assertRaisesRegex(ValueError, 'capacity exceeded'):
                store._encode(original)

    def test_source_signal_hash_files_untouched_by_evidence_capture(self):
        # Source workers hash signal/spec code, not the additive store hook.
        import sol_proximity_experimental_worker as worker
        before = {coin: worker.config_hash(spec) for coin, spec in SPECS.items()}
        maxpain_pending_fixture(growth=True)
        self.assertEqual(before, {coin: worker.config_hash(spec) for coin, spec in SPECS.items()})

    def test_real_growth_proof_captures_every_actual_adjacent_tier(self):
        f = maxpain_pending_fixture(growth=True)
        messages = bridge.maxpain_messages(f['state'], now_ms=f['now_ms'], fence_ms=f['fence_ms'])
        self.assertEqual(len(messages), 4)
        self.assertEqual([m['proof']['timeframe'] for m in messages], ['3d', '1w', '2w', '1m'])
        longest = messages[-1]['proof']['cluster_comparisons'][0]['tiers']
        self.assertEqual([t['timeframe'] for t in longest], ['3d', '1w', '2w', '1m'])
        self.assertEqual([t['liquidation_amount'] for t in longest], ['800', '1600', '3200', '6400'])

    def test_all_five_initial_missing_liquidity_plans_match_source_admission(self):
        for spec in SPECS.values():
            with self.subTest(coin=spec.coin):
                plain = maxpain_pending_fixture(spec, capture=False, liquidation_amount=None)
                captured = maxpain_pending_fixture(spec, liquidation_amount=None)
                reduced = deepcopy(captured['state'])
                for plan in reduced['active']:
                    self.assertIsNone(plan.pop('execution_evidence')['liquidation_amount'])
                self.assertEqual(reduced, plain['state'])
                messages = bridge.maxpain_messages(captured['state'], now_ms=captured['now_ms'],
                                                  fence_ms=captured['fence_ms'])
                self.assertEqual(len(messages), 1)
                self.assertIsNone(messages[0]['proof']['liquidation_amount'])
                self.assertEqual(messages[0]['proof']['cluster_comparisons'], [])
                self.assertEqual(contract.validate(messages[0]), messages[0])

    def test_all_five_distinct_additional_targets_allow_missing_liquidity(self):
        for spec in SPECS.values():
            with self.subTest(coin=spec.coin):
                f = maxpain_pending_fixture(spec, liquidation_amount=None)
                now = f['now_ms'] + M
                target = f['state']['active'][0]['target_price'] + .3
                decoded = _decoded(spec, now, target)
                for row in decoded['rows']:
                    row['liquidation_amount'] = None
                with patch.object(store, '_connect', f['database'].connect):
                    store.advance(f['scope'], [[ms(BASE)+M, 100, 100.01, 99.99, 100]], now,
                                  config_sha256=CONFIG)
                    store.ingest(f['scope'], decoded, [[ms(BASE)+2*M, 100, 100.01, 99.99, 100]], now,
                                 config_sha256=CONFIG, execution_enabled=True)
                    state = store.snapshot(f['scope'])
                self.assertEqual(len(state['active']), 2)
                messages = bridge.maxpain_messages(state, now_ms=now, fence_ms=f['fence_ms'])
                self.assertEqual(len(messages), 2)
                self.assertTrue(all(m['proof']['liquidation_amount'] is None and
                                    m['proof']['cluster_comparisons'] == [] for m in messages))

    def test_missing_liquidity_never_proves_growth_exception(self):
        for spec in SPECS.values():
            if not spec.liquidity_growth:
                continue
            with self.subTest(coin=spec.coin):
                f = maxpain_pending_fixture(spec, growth=True, liquidation_amount=None)
                self.assertEqual(len(f['state']['active']), 1)
                messages = bridge.maxpain_messages(f['state'], now_ms=f['now_ms'], fence_ms=f['fence_ms'])
                self.assertEqual(len(messages), 1)
                self.assertEqual(messages[0]['proof']['cluster_comparisons'], [])
                full = maxpain_pending_fixture(spec, growth=True)
                value = bridge.maxpain_messages(full['state'], now_ms=full['now_ms'], fence_ms=full['fence_ms'])[-1]
                self.assertTrue(value['proof']['cluster_comparisons'])
                missing = deepcopy(value); missing['proof']['liquidation_amount'] = None
                with self.assertRaisesRegex(contract.ContractError, 'CLUSTER_INCOMING_AMOUNT_REQUIRED'):
                    contract.validate(missing)
                missing = deepcopy(value)
                missing['proof']['cluster_comparisons'][0]['tiers'][0]['liquidation_amount'] = None
                with self.assertRaises(contract.ContractError):
                    contract.validate(missing)

    def test_missing_amount_is_distinct_from_malformed_nonpositive_metadata(self):
        for amount in (0, -1, True, 'NaN', 'missing'):
            with self.subTest(amount=amount):
                f = maxpain_pending_fixture(liquidation_amount=amount)
                self.assertEqual(len(f['state']['active']), 1)
                self.assertEqual(f['state']['active'][0]['execution_evidence_error'], 'PROOF_UNAVAILABLE')
                self.assertEqual(bridge.maxpain_messages(f['state'], now_ms=f['now_ms'], fence_ms=f['fence_ms']), [])
                value = maxpain_message(); value['proof']['liquidation_amount'] = str(amount)
                with self.assertRaises(contract.ContractError):
                    contract.validate(value)

    def test_fake_liquidity_skipped_tier_and_wrong_target_rejected(self):
        f = maxpain_pending_fixture(growth=True)
        message = bridge.maxpain_messages(f['state'], now_ms=f['now_ms'], fence_ms=f['fence_ms'])[-1]
        for change in ('amount', 'skip', 'target', 'direction'):
            with self.subTest(change=change):
                tampered = deepcopy(message)
                comparison = tampered['proof']['cluster_comparisons'][0]
                if change == 'amount': comparison['tiers'][1]['liquidation_amount'] = '1'
                elif change == 'skip': comparison['tiers'].pop(1)
                elif change == 'target': comparison['tiers'][0]['target_price'] = '105'
                else: comparison['previous_direction'] = -1
                with self.assertRaises(contract.ContractError): contract.validate(tampered)

    def test_post_arm_is_heartbeat_not_new_plan_and_digest_stable(self):
        f = maxpain_pending_fixture()
        before = bridge.maxpain_messages(f['state'], now_ms=f['now_ms'], fence_ms=f['fence_ms'])[0]
        now = contract.moment_ms(before['arm_at'])
        after = bridge.maxpain_messages(f['state'], now_ms=now, fence_ms=f['fence_ms'])[0]
        self.assertEqual(after['kind'], 'HEARTBEAT')
        self.assertEqual(contract.plan_digest(before), contract.plan_digest(after))
        self.assertGreater(after['source_sequence'], before['source_sequence'])

    def test_stale_source_lease_does_not_refresh_from_reader_clock(self):
        f = maxpain_pending_fixture()
        self.assertEqual(bridge.maxpain_messages(f['state'], now_ms=f['now_ms'] + 90_000, fence_ms=f['fence_ms']), [])

    def test_new_snapshot_without_contiguous_prices_cannot_renew_lease(self):
        f = maxpain_pending_fixture()
        now = f['now_ms'] + 90_000
        f['state']['last_snapshot_ms'] = now
        self.assertEqual(bridge.maxpain_messages(f['state'], now_ms=now, fence_ms=f['fence_ms']), [])

    def test_contiguous_prices_renew_heartbeat_lease(self):
        f = maxpain_pending_fixture()
        now = ms(BASE) + 3*M
        with patch.object(store, '_connect', f['database'].connect):
            store.advance(f['scope'], [[ms(BASE)+M, 100, 100.01, 99.99, 100],
                [ms(BASE)+2*M, 100, 100.01, 99.99, 100]], now, config_sha256=CONFIG)
            state = store.snapshot(f['scope'])
        message = bridge.maxpain_messages(state, now_ms=now, fence_ms=f['fence_ms'])[0]
        self.assertEqual(message['kind'], 'HEARTBEAT')
        self.assertEqual(contract.moment_ms(message['source_as_of']), now)

    def test_target_before_entry_emits_cancel_and_no_retroactive_plan(self):
        f = maxpain_pending_fixture()
        p, base = f['state']['active'][0], ms(BASE)
        with patch.object(store, '_connect', f['database'].connect):
            store.advance(f['scope'], [[base+M, 100, 100.01, 99.99, 100],
                [base+2*M, 100, 102, 99.99, 101]], base+3*M, config_sha256=CONFIG)
            state = store.snapshot(f['scope'])
        messages = bridge.maxpain_messages(state, now_ms=base+3*M, fence_ms=f['fence_ms'])
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]['kind'], 'CANCEL')
        self.assertEqual(messages[0]['cancel_reason'], 'TARGET_BEFORE_ENTRY')
        self.assertEqual(messages[0]['occurrence_id'], contract.occurrence_id(messages[0]))

    def test_source_fill_is_cancel_not_testnet_fill(self):
        f = maxpain_pending_fixture()
        p, base = f['state']['active'][0], ms(BASE)
        entry = p['entry_price']
        with patch.object(store, '_connect', f['database'].connect):
            store.advance(f['scope'], [[base+M, 100, 100.01, 99.99, 100],
                [base+2*M, entry+.1, entry+.2, entry-.1, entry]], base+3*M, config_sha256=CONFIG)
            state = store.snapshot(f['scope'])
        message = bridge.maxpain_messages(state, now_ms=base+3*M, fence_ms=f['fence_ms'])[0]
        self.assertEqual((message['kind'], message['cancel_reason']), ('CANCEL', 'SOURCE_ENTRY_OBSERVED'))
        self.assertNotIn('fill_price', message)

    def test_expiry_emits_cancel_even_when_no_new_source_bar(self):
        f = maxpain_pending_fixture()
        end = f['state']['active'][0]['expires_ms']
        value = bridge.maxpain_messages(f['state'], now_ms=end, fence_ms=f['fence_ms'])[0]
        self.assertEqual(value['cancel_reason'], 'EXPIRED_PENDING')
        self.assertEqual(value['valid_until'], value['expires_at'])

    def test_activation_fence_never_backfills_existing_wait(self):
        f = maxpain_pending_fixture()
        self.assertEqual(bridge.maxpain_messages(f['state'], now_ms=f['now_ms'], fence_ms=f['now_ms']+1), [])

    def test_restored_old_source_wait_never_becomes_new_execution_plan(self):
        import maxpain_pending_restore_20261006 as restoration
        from maxpain_pending_restore_20261006_selftest import fixture, NOW, OLD
        state, prices, live, approved = fixture()
        with patch.dict('os.environ', {restoration.ENABLE_ENV:restoration.ENABLE_NONCE}), \
                patch.object(restoration, 'APPROVED', approved):
            result = restoration.apply_fresh(state, prices, live, NOW+5000, NOW+6000)
        self.assertEqual(result['restored'], 1)
        restored = state['active'][0]
        self.assertEqual(restored['source_quote']['price_source'], OLD)
        self.assertIn('manual_source_restore', restored)
        self.assertGreater(restored['arm_ms'], NOW+6000)
        self.assertNotIn('execution_evidence', restored)
        self.assertEqual(bridge.maxpain_messages(state, now_ms=NOW+6000, fence_ms=NOW-10*M), [])
        # Even a later source ingest cannot backfill execution proof for an
        # already-restored active observation or change its old frozen quote.
        before = deepcopy(state)
        bridge.capture_maxpain_evidence(state, {}, before, config_sha256=CONFIG)
        self.assertEqual(state, before)

    def test_r2732_freezes_policy_and_recipient_independent_identity(self):
        first, second = r2732_message(), r2732_message()
        self.assertEqual(first, second)
        self.assertEqual(first['policy']['effective'], 'NEXT_MINUTE')
        self.assertEqual(first['policy']['locked_stop'], '99.75')
        self.assertNotIn('intent_id', first)
        self.assertNotIn('position_id', first)

    def test_r2732_delivered_receipt_required_and_stale_not_replayed(self):
        f = r2732_fixture()
        f['state']['intents'][0]['status'] = 'UNKNOWN'
        self.assertEqual(bridge.r2732_messages(f['state'], now_ms=ms(f['now']), fence_ms=ms(BASE)), [])
        f = r2732_fixture()
        expired = contract.moment_ms(f['intent']['expires_at'])
        message = bridge.r2732_messages(f['state'], now_ms=expired, fence_ms=ms(BASE))[0]
        self.assertEqual((message['kind'], message['cancel_reason']), ('CANCEL', 'EXPIRED_REFERENCE'))

    def test_r2732_unverified_monitor_or_closed_bar_gap_cancel_new_entry(self):
        for status in ('PRICE_GAP', 'NO_PRICE_DATA', 'AMBIGUOUS', None):
            with self.subTest(status=status):
                f = r2732_fixture()
                f['state']['active']['monitor_status'] = status
                value = bridge.r2732_messages(f['state'], now_ms=ms(f['now']), fence_ms=ms(BASE))[0]
                self.assertEqual((value['kind'], value['cancel_reason']), ('CANCEL', 'SOURCE_MONITOR_UNVERIFIED'))
        f = r2732_fixture()
        now = contract.moment_ms(f['intent']['payload']['entry_at']) + 60_000
        value = bridge.r2732_messages(f['state'], now_ms=now, fence_ms=ms(BASE))[0]
        self.assertEqual(value['cancel_reason'], 'SOURCE_MONITOR_NOT_CAUGHT_UP')

    def test_recipient_cancellation_dominates_newer_other_recipient_heartbeat(self):
        f = maxpain_pending_fixture()
        cancelled, pending = deepcopy(f['state']), deepcopy(f['state'])
        cancelled['active'][0]['status'] = 'UNKNOWN'
        pending['bar_cursor_ms'] = ms(BASE)+2*M
        now = BASE+timedelta(minutes=3)
        for scopes in (('1'*64, '2'*64), ('2'*64, '1'*64)):
            values = {}
            for scope, state in zip(scopes, (cancelled, pending)):
                key = store.key_for('general-watch:'+scope+':'+HYPE_LONG_TF.rule_id)
                values[key] = json.dumps(state)
            source = ReadOnlySource(values)
            with patch('alert_cards_forwarder._source_dsn', return_value='postgresql://offline-only'):
                results = bridge.read_experimental(set(scopes), BASE, now=now,
                    env={'EXPERIMENTAL_EXECUTION_BRIDGE_MODE':bridge.MODE}, connect=source.connect)
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0]['kind'], 'CANCEL')

    def test_contract_rejects_tampered_type_price_geometry_identity_and_venues(self):
        good = r2732_message()
        changes = [('entry', 'NaN'), ('entry', True), ('stop', '99'), ('occurrence_id', 'f'*64),
                   ('source_environment', 'testnet'), ('execution_environment', 'mainnet'),
                   ('source_sequence', True), ('source_price_kind', 'MARK'), ('source_state', 'OPEN')]
        for key, value in changes:
            with self.subTest(key=key, value=value):
                bad = deepcopy(good); bad[key] = value
                with self.assertRaises(contract.ContractError): contract.validate(bad)
        bad = deepcopy(good); bad['policy']['locked_stop'] = '99.7'
        with self.assertRaises(contract.ContractError): contract.validate(bad)

    def test_contract_returns_detached_copy(self):
        original = r2732_message()
        checked = contract.validate(original)
        checked['policy']['name'] = 'tampered'
        self.assertNotEqual(original, checked)

    def test_malformed_nested_types_use_contract_error_boundary(self):
        for key in ('kind', 'proof', 'policy', 'side', 'rule_id'):
            for malformed in ([], {}, None, True):
                with self.subTest(key=key, malformed=malformed):
                    bad = r2732_message(); bad[key] = malformed
                    with self.assertRaises(contract.ContractError): contract.validate(bad)

    def test_recipient_local_maxpain_metadata_does_not_duplicate_occurrence(self):
        first = maxpain_message()
        second = deepcopy(first)
        second['proof']['episode_first_ms'] += 1
        second['proof']['episode_generation'] += 1
        self.assertEqual(contract.validate(second)['occurrence_id'], first['occurrence_id'])
        self.assertEqual(contract.plan_digest(first), contract.plan_digest(second))

    def test_readonly_reader_reads_eight_namespaces_and_never_writes(self):
        maxpain = maxpain_pending_fixture()
        source = ReadOnlySource({maxpain['key']: json.dumps(maxpain['state'])})
        # Test each clock separately to keep both references genuinely fresh.
        with patch('alert_cards_forwarder._source_dsn', return_value='postgresql://offline-only'):
            values = bridge.read_experimental({SCOPE}, BASE, now=BASE+timedelta(seconds=70),
                env={'EXPERIMENTAL_EXECUTION_BRIDGE_MODE': bridge.MODE}, connect=source.connect)
        self.assertEqual(len(source.calls), 8)
        self.assertTrue(all(sql.startswith('SELECT value ') for sql, _ in source.calls))
        self.assertIn('default_transaction_read_only=on', source.connections[0][1]['options'])
        self.assertTrue(any(v['family'] == 'maxpain' for v in values))


if __name__ == '__main__':
    unittest.main()
