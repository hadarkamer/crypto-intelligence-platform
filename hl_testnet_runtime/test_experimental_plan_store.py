"""Prospective-plan storage tests with real contract validation and no venue I/O.

Pure transaction doubles test rollback/ack boundaries, not PostgreSQL itself.
The optional durable class requires an explicitly disposable local CI database.
"""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os
import threading
import unittest

import experimental_execution_contract as contract
from . import experimental_plan_store as store

CREATED = '2026-10-06T10:00:10+00:00'
ARM = '2026-10-06T10:01:00+00:00'
BEFORE = '2026-10-06T09:59:00+00:00'
NOW = '2026-10-06T10:00:11+00:00'


def maxpain_plan():
    """A fixed synthetic HYPE plan, accepted by the production source contract."""
    observed = contract.moment_ms('2026-10-06T10:00:00+00:00')
    message = dict(version=contract.VERSION, kind='PLAN', family='maxpain',
        occurrence_id='', rule_id='HYPE_MAXPAIN_DIST05_15_LONG_TF', symbol='HYPE', side='LONG',
        source_environment='mainnet', execution_environment='testnet', source_price_kind='TRADE_1M',
        source_at=contract.iso_ms(observed), created_at=CREATED, arm_at=ARM,
        expires_at='2026-10-07T10:01:00+00:00', entry='98.0', stop='95.0',
        take_profit='100.5', original_target='101.0',
        policy=dict(name='MAXPAIN_LIMIT_PRETOUCH_V1', entry_adverse='2.0', take_fraction='0.5',
            stop_distance_multiplier='5', overlap_target_fraction='0.002', liquidity_growth=True,
            source_price='HYPERLIQUID_HYPE_PERPETUAL_TRADE_1M'),
        proof=dict(source_contract_version=contract.SOURCE_CONTRACT, source_config_sha256='1' * 64,
            cycle_id='synthetic-source-cycle', episode_key='101', episode_generation=1,
            episode_first_ms=observed - 60_000, timeframe='3d', source_side='SHORT',
            source_observed_ms=observed, source_quote=dict(price_source='HYPERLIQUID_HYPE_PERPETUAL_TRADE_1M',
                price_market='perpetual', price_pair='HYPE-PERP', price_instrument='HYPE'),
            liquidation_amount='1000000', cluster_comparisons=[], range24_low=None,
            range24_high=None, source_price='100.0'),
        source_sequence=contract.moment_ms(CREATED) * 10 + 1, source_as_of=CREATED,
        source_state='PENDING', valid_until='2026-10-06T10:01:40+00:00', cancel_reason=None)
    message['occurrence_id'] = contract.occurrence_id(message)
    return contract.validate(message)


def source_update(message, at, *, kind='HEARTBEAT', reason=None):
    result = deepcopy(message)
    at_ms = contract.moment_ms(at)
    result.update(kind=kind, source_as_of=at,
        source_sequence=at_ms * 10 + contract.RANK[kind],
        valid_until=contract.iso_ms(min(at_ms + contract.LEASE_MS,
                                      contract.moment_ms(result['expires_at']))),
        source_state='CANCELLED' if kind == 'CANCEL' else 'PENDING', cancel_reason=reason)
    return contract.validate(result)


def reduce(previous, message, now=NOW, not_before=BEFORE, domain='software'):
    return store.reduce_source(previous, message, now=now, not_before=not_before, domain=domain)


class SourceReducerTests(unittest.TestCase):
    def test_receiver_clock_cannot_regress_on_replay(self):
        state, _ = reduce(None, maxpain_plan())
        with self.assertRaisesRegex(store.PlanStoreError, 'CLOCK_REGRESSION'):
            reduce(state, maxpain_plan(), now=CREATED)

    def test_prospective_plan_waits_and_does_not_authorize_execution(self):
        source = maxpain_plan()
        state, changed = reduce(None, source)
        self.assertTrue(changed)
        self.assertEqual((state['revision'], state['entry_permission'], state['strategy']), (1, 'WAITING', None))
        self.assertEqual(state['plan_digest'], contract.plan_digest(source))
        self.assertNotIn('dispatch_enabled', state)
        source['entry'] = '1'
        self.assertEqual(state['source']['entry'], '98.0')

    def test_first_heartbeat_and_first_plan_at_or_after_arm_are_refused(self):
        source = maxpain_plan()
        cases = [(source_update(source, CREATED), NOW), (source, ARM),
                 (source, '2026-10-06T10:01:01+00:00')]
        for message, now in cases:
            with self.subTest(kind=message['kind'], now=now), self.assertRaisesRegex(
                    store.PlanStoreError, 'PLAN_NOT_RECEIVED_BEFORE_ARM'):
                reduce(None, message, now=now)

    def test_first_plan_before_activation_or_after_lease_is_refused(self):
        for now, before in ((NOW, NOW), ('2026-10-06T10:02:00+00:00', BEFORE)):
            with self.subTest(now=now), self.assertRaisesRegex(store.PlanStoreError, 'PLAN_STALE'):
                reduce(None, maxpain_plan(), now=now, not_before=before)

    def test_future_source_and_future_activation_refused(self):
        for now, before in ((BEFORE, BEFORE), (NOW, ARM)):
            with self.subTest(now=now), self.assertRaisesRegex(store.PlanStoreError, 'FUTURE_SOURCE'):
                reduce(None, maxpain_plan(), now=now, not_before=before)

    def test_real_contract_rejects_forged_price_before_state_mutation(self):
        source = maxpain_plan()
        source['entry'] = '97'
        with self.assertRaises(contract.ContractError):
            reduce(None, source)

    def test_domain_cannot_change_on_restore(self):
        source = maxpain_plan()
        state, _ = reduce(None, source)
        with self.assertRaisesRegex(store.PlanStoreError, 'IMMUTABLE_PLAN_CHANGED'):
            reduce(state, source, domain='testnet')
        with self.assertRaisesRegex(store.PlanStoreError, 'DOMAIN_INVALID'):
            reduce(None, source, domain='mainnet')

    def test_duplicate_does_not_reset_clocks_or_increment_revision(self):
        source = maxpain_plan()
        state, _ = reduce(None, source)
        again, changed = reduce(state, source, now='2026-10-06T10:00:12+00:00')
        self.assertFalse(changed)
        self.assertEqual(again, state)

    def test_recipient_local_episode_bookkeeping_does_not_change_plan_identity(self):
        source = maxpain_plan()
        state, _ = reduce(None, source)
        other = source_update(source, '2026-10-06T10:00:20+00:00')
        other['proof']['episode_generation'] = 3
        other['proof']['episode_first_ms'] += 1000
        other = contract.validate(other)
        self.assertEqual(contract.plan_digest(other), contract.plan_digest(source))
        result, changed = reduce(state, other, now=other['source_as_of'])
        self.assertTrue(changed)
        self.assertEqual(result['occurrence_id'], state['occurrence_id'])
        self.assertEqual(result['plan_digest'], state['plan_digest'])

    def test_same_sequence_recipient_bookkeeping_is_duplicate(self):
        source = maxpain_plan()
        state, _ = reduce(None, source)
        other = deepcopy(source)
        other['proof']['episode_generation'] = 2
        other['proof']['episode_first_ms'] += 1000
        contract.validate(other)
        result, changed = reduce(state, other)
        self.assertFalse(changed)
        self.assertEqual(result, state)

    def test_changed_immutable_plan_cannot_replace_same_occurrence(self):
        source = maxpain_plan()
        state, _ = reduce(None, source)
        changed = source_update(source, '2026-10-06T10:00:20+00:00')
        changed['proof']['source_config_sha256'] = '2' * 64
        self.assertEqual(contract.validate(changed)['occurrence_id'], state['occurrence_id'])
        with self.assertRaisesRegex(store.PlanStoreError, 'IMMUTABLE_PLAN_CHANGED'):
            reduce(state, changed, now=changed['source_as_of'])

    def test_same_sequence_conflicting_source_payload_is_refused(self):
        source = maxpain_plan()
        state, _ = reduce(None, source)
        changed = deepcopy(source)
        changed['valid_until'] = '2026-10-06T10:01:30+00:00'
        contract.validate(changed)
        with self.assertRaisesRegex(store.PlanStoreError, 'SOURCE_SEQUENCE_CONFLICT'):
            reduce(state, changed)

    def test_fresh_heartbeat_refreshes_lease_without_changing_plan(self):
        source = maxpain_plan()
        state, _ = reduce(None, source)
        beat = source_update(source, '2026-10-06T10:01:30+00:00')
        result, changed = reduce(state, beat, now=beat['source_as_of'])
        self.assertTrue(changed)
        self.assertEqual(result['entry_permission'], 'WAITING')
        self.assertEqual(result['plan'], state['plan'])
        self.assertEqual(result['source']['valid_until'], beat['valid_until'])
        self.assertEqual(result['initial_source'], source)

    def test_new_heartbeat_after_lease_gap_cannot_restore_permission(self):
        source = maxpain_plan()
        state, _ = reduce(None, source)
        beat = source_update(source, '2026-10-06T10:01:41+00:00')
        result, changed = reduce(state, beat, now=beat['source_as_of'])
        self.assertTrue(changed)
        self.assertEqual((result['entry_permission'], result['terminal_reason']),
                         ('RETIRED', 'SOURCE_LEASE_EXPIRED'))
        later = source_update(source, '2026-10-06T10:02:00+00:00')
        result, _ = reduce(result, later, now=later['source_as_of'])
        self.assertEqual(result['entry_permission'], 'RETIRED')

    def test_duplicate_at_lease_boundary_retires_once(self):
        source = maxpain_plan()
        state, _ = reduce(None, source)
        result, changed = reduce(state, source, now=source['valid_until'])
        self.assertTrue(changed)
        self.assertEqual((result['entry_permission'], result['terminal_reason']),
                         ('RETIRED', 'SOURCE_LEASE_EXPIRED'))
        again, changed = reduce(result, source, now=source['valid_until'])
        self.assertFalse(changed)
        self.assertEqual(again, result)

    def test_out_of_order_heartbeat_is_inert_only_while_current_lease_fresh(self):
        source = maxpain_plan()
        state, _ = reduce(None, source)
        beat = source_update(source, '2026-10-06T10:01:00+00:00')
        state, _ = reduce(state, beat, now=beat['source_as_of'])
        old = source_update(source, '2026-10-06T10:00:20+00:00')
        result, changed = reduce(state, old, now='2026-10-06T10:01:10+00:00')
        self.assertFalse(changed)
        self.assertEqual(result, state)
        result, changed = reduce(state, old, now=beat['valid_until'])
        self.assertTrue(changed)
        self.assertEqual(result['entry_permission'], 'RETIRED')
        self.assertEqual(result['source_sequence'], beat['source_sequence'])

    def test_cancellation_arriving_first_is_permanent_tombstone(self):
        source = maxpain_plan()
        cancel = source_update(source, '2026-10-06T10:00:20+00:00', kind='CANCEL', reason='TARGET_TOUCHED_FIRST')
        state, _ = reduce(None, cancel, now=cancel['source_as_of'])
        self.assertEqual(state['entry_permission'], 'RETIRED')
        result, changed = reduce(state, source, now='2026-10-06T10:00:21+00:00')
        self.assertFalse(changed)
        self.assertEqual(result, state)
        beat = source_update(source, '2026-10-06T10:00:30+00:00')
        result, _ = reduce(state, beat, now=beat['source_as_of'])
        self.assertEqual((result['entry_permission'], result['terminal_reason']),
                         ('RETIRED', 'TARGET_TOUCHED_FIRST'))

    def test_delayed_older_cancellation_retires_current_plan_and_preserves_strategy(self):
        source = maxpain_plan()
        state, _ = reduce(None, source)
        beat = source_update(source, '2026-10-06T10:01:00+00:00')
        state, _ = reduce(state, beat, now=beat['source_as_of'])
        state['strategy'] = {'filled_quantity': '2', 'owned_position': True}
        cancel = source_update(source, '2026-10-06T10:00:20+00:00', kind='CANCEL', reason='TARGET_TOUCHED_FIRST')
        result, _ = reduce(state, cancel, now='2026-10-06T10:01:10+00:00')
        self.assertEqual(result['source_sequence'], beat['source_sequence'])
        self.assertEqual(result['entry_permission'], 'RETIRED')
        self.assertEqual(result['strategy'], state['strategy'])

    def test_delayed_old_cancellation_retry_does_not_create_another_revision(self):
        source = maxpain_plan()
        state, _ = reduce(None, source)
        beat = source_update(source, '2026-10-06T10:01:00+00:00')
        state, _ = reduce(state, beat, now=beat['source_as_of'])
        cancel = source_update(source, '2026-10-06T10:00:20+00:00', kind='CANCEL', reason='TARGET_TOUCHED_FIRST')
        state, _ = reduce(state, cancel, now='2026-10-06T10:01:10+00:00')
        result, changed = reduce(state, cancel, now='2026-10-06T10:01:11+00:00')
        self.assertFalse(changed)
        self.assertEqual(result, state)


class Result:
    def __init__(self, row=None): self.row = row
    def fetchone(self): return deepcopy(self.row)


class TransactionConnection:
    """Stateful storage protocol double; no SQL engine or network claims."""
    def __init__(self, journal): self.journal = journal
    def execute(self, sql, params=()):
        j = self.journal
        sql = ' '.join(sql.split())
        j.statements.append(sql)
        if sql.startswith('SELECT version,domain'):
            return Result((store.VERSION, j.domain))
        if sql.startswith('SELECT pg_advisory_xact_lock'):
            return Result((None,))
        if sql.startswith('SELECT value,digest,revision'):
            return Result(j.records.get(params[0]))
        if sql.startswith(f'INSERT INTO {store.SCHEMA}.plans'):
            identity, revision, value, digest = params
            j.records[identity] = (json.loads(value), digest, revision)
            return Result()
        if sql.startswith(f'INSERT INTO {store.SCHEMA}.events'):
            if j.fail_event_once:
                j.fail_event_once = False
                raise RuntimeError('SYNTHETIC_EVENT_WRITE_FAILURE')
            identity, revision, *_ = params
            if (identity, revision) in j.events:
                raise AssertionError('DUPLICATE_EVENT_REVISION')
            j.events[(identity, revision)] = tuple(params)
            return Result()
        raise AssertionError('UNSUPPORTED_FAKE_TRANSACTION_STATEMENT')


class FakeJournal:
    _ci = True
    def __init__(self):
        self.records, self.events, self.statements = {}, {}, []
        self.lock = threading.RLock()
        self.domain = 'software'
        self.fail_event_once = self.lose_commit_ack_once = False
    @contextmanager
    def _transaction(self):
        with self.lock:
            saved = deepcopy((self.records, self.events))
            try:
                yield TransactionConnection(self)
            except BaseException:
                self.records, self.events = saved
                raise
            if self.lose_commit_ack_once:
                self.lose_commit_ack_once = False
                raise RuntimeError('SYNTHETIC_COMMIT_ACK_UNKNOWN')


class TransactionBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.journal = FakeJournal()
        self.store = store.PlanStore(self.journal)
        self.message = maxpain_plan()
    def ingest(self, source=None):
        return self.store.ingest(source or self.message, now=NOW, not_before=BEFORE)
    def test_normal_receipt_is_record_only_and_no_schema_ddl_runs(self):
        result = self.ingest()
        self.assertEqual((result['status'], result['record_only']), ('RECORDED', True))
        self.assertEqual(len(self.journal.events), 1)
        self.assertFalse(any('CREATE ' in query for query in self.journal.statements))
    def test_event_failure_rolls_back_plan_and_retry_creates_one_atomic_revision(self):
        self.journal.fail_event_once = True
        with self.assertRaisesRegex(RuntimeError, 'SYNTHETIC_EVENT_WRITE_FAILURE'):
            self.ingest()
        self.assertEqual((self.journal.records, self.journal.events), ({}, {}))
        self.assertEqual(self.ingest()['revision'], 1)
        self.assertEqual(len(self.journal.events), 1)
    def test_lost_commit_ack_returns_no_success_and_retry_is_duplicate(self):
        self.journal.lose_commit_ack_once = True
        with self.assertRaisesRegex(RuntimeError, 'SYNTHETIC_COMMIT_ACK_UNKNOWN'):
            self.ingest()
        self.assertEqual(len(self.journal.records), 1)
        result = self.ingest()
        self.assertEqual((result['status'], result['revision']), ('DUPLICATE', 1))
        self.assertEqual(len(self.journal.events), 1)
    def test_concurrent_duplicate_receipts_create_one_record(self):
        with ThreadPoolExecutor(max_workers=6) as pool:
            replies = list(pool.map(lambda _: self.ingest(), range(12)))
        self.assertEqual(sum(reply['status'] == 'RECORDED' for reply in replies), 1)
        self.assertEqual(len(self.journal.events), 1)
    def test_concurrent_cas_has_one_winner_and_requires_reload_for_other(self):
        result = self.ingest()
        def change(index):
            try:
                self.store.change_strategy(result['occurrence_id'], 1, {'worker': index}, now=NOW)
                return 'SAVED'
            except store.PlanStoreError as error:
                return str(error)
        with ThreadPoolExecutor(max_workers=2) as pool:
            replies = list(pool.map(change, (1, 2)))
        self.assertEqual(sorted(replies), ['EXPERIMENTAL_CONCURRENT_RELOAD_REQUIRED', 'SAVED'])
        self.assertEqual(len(self.journal.events), 2)
    def test_cas_failure_rolls_back_strategy_and_revision(self):
        result = self.ingest()
        self.journal.fail_event_once = True
        with self.assertRaisesRegex(RuntimeError, 'SYNTHETIC_EVENT_WRITE_FAILURE'):
            self.store.change_strategy(result['occurrence_id'], 1, {'phase': 'WAIT'}, now=NOW)
        state = self.store.load(result['occurrence_id'])
        self.assertEqual((state['revision'], state['strategy']), (1, None))
    def test_cas_commit_uncertainty_requires_reload_and_cannot_replay_revision(self):
        result = self.ingest()
        self.journal.lose_commit_ack_once = True
        with self.assertRaisesRegex(RuntimeError, 'SYNTHETIC_COMMIT_ACK_UNKNOWN'):
            self.store.change_strategy(result['occurrence_id'], 1, {'phase': 'WAIT'}, now=NOW)
        with self.assertRaisesRegex(store.PlanStoreError, 'CONCURRENT_RELOAD_REQUIRED'):
            self.store.change_strategy(result['occurrence_id'], 1, {'phase': 'WAIT'}, now=NOW)
        self.assertEqual(self.store.load(result['occurrence_id'])['revision'], 2)
        self.assertEqual(len(self.journal.events), 2)
    def test_damaged_checksum_or_revision_is_refused(self):
        result = self.ingest()
        original = deepcopy(self.journal.records[result['occurrence_id']])
        for damaged in ((original[0], '0' * 64, original[2]),
                        (original[0], original[1], original[2] + 1)):
            self.journal.records[result['occurrence_id']] = damaged
            with self.assertRaisesRegex(store.PlanStoreError, 'STATE_INTEGRITY_FAILURE'):
                self.store.load(result['occurrence_id'])
    def test_software_domain_cannot_be_loaded_as_testnet(self):
        self.ingest()
        self.journal._ci = False
        other = store.PlanStore(self.journal)
        with self.assertRaisesRegex(store.PlanStoreError, 'SCHEMA_OR_DOMAIN_MISMATCH'):
            other.load(self.message['occurrence_id'])
    def test_cancel_entry_permission_does_not_discard_filled_strategy(self):
        result = self.ingest()
        self.store.change_strategy(result['occurrence_id'], 1,
            {'filled_quantity': '3', 'protection_required': True}, now=NOW)
        cancel = source_update(self.message, NOW, kind='CANCEL', reason='TARGET_TOUCHED_FIRST')
        self.store.ingest(cancel, now=NOW, not_before=BEFORE)
        state = self.store.load(result['occurrence_id'])
        self.assertEqual(state['entry_permission'], 'RETIRED')
        self.assertEqual(state['strategy'], {'filled_quantity': '3', 'protection_required': True})

    def test_old_cancellation_commit_uncertainty_retry_keeps_one_tombstone_event(self):
        self.ingest()
        heartbeat = source_update(self.message, '2026-10-06T10:01:00+00:00')
        self.store.ingest(heartbeat, now=heartbeat['source_as_of'], not_before=BEFORE)
        cancel = source_update(self.message, '2026-10-06T10:00:20+00:00',
            kind='CANCEL', reason='TARGET_TOUCHED_FIRST')
        self.journal.lose_commit_ack_once = True
        with self.assertRaisesRegex(RuntimeError, 'SYNTHETIC_COMMIT_ACK_UNKNOWN'):
            self.store.ingest(cancel, now='2026-10-06T10:01:10+00:00', not_before=BEFORE)
        result = self.store.ingest(cancel, now='2026-10-06T10:01:11+00:00', not_before=BEFORE)
        self.assertEqual((result['status'], result['revision']), ('DUPLICATE', 3))
        self.assertEqual(len(self.journal.events), 3)
        state = self.store.load(self.message['occurrence_id'])
        self.assertEqual(state['cancellation'], cancel)
        self.assertEqual(state['initial_source'], self.message)

    def test_real_maxpain_proposal_cannot_commit_across_source_cancellation(self):
        from . import maxpain_execution as execution
        self.ingest()
        identity = self.message['occurrence_id']
        received = self.store.load(identity)
        strategy = execution.receive(execution.initial(), received['initial_source'],
            now_ms=contract.moment_ms(received['created_at']))
        cached = self.store.change_strategy(identity, received['revision'], strategy, now=NOW)
        arm_ms = contract.moment_ms(ARM)
        metadata = {'universe': [{'name': 'HYPE', 'szDecimals': 2}]}
        market = dict(environment='testnet', symbol='HYPE', at_ms=arm_ms, mark_price='100')
        ownership = dict(account_role='long_account', symbol='HYPE',
            all_prior_cards_final=True, unresolved_request=False)
        proposal = execution.entry_admission(cached['strategy'], identity, metadata,
            market, ownership, now_ms=arm_ms)
        self.assertEqual(proposal['status'], 'PROPOSED_SOFTWARE_ONLY')
        cancel = source_update(self.message, ARM, kind='CANCEL', reason='TARGET_TOUCHED_FIRST')
        self.store.ingest(cancel, now=ARM, not_before=BEFORE)
        stale_attempt = execution.begin_entry(cached['strategy'], proposal['action'],
            metadata, market, ownership, now_ms=arm_ms)
        with self.assertRaisesRegex(store.PlanStoreError, 'CONCURRENT_RELOAD_REQUIRED'):
            self.store.change_strategy(identity, cached['revision'], stale_attempt, now=ARM)
        fresh = self.store.load(identity)
        self.assertEqual(fresh['entry_permission'], 'RETIRED')
        self.assertIsNone(fresh['strategy']['records'][identity]['request'])
        reconciled = execution.receive(fresh['strategy'], fresh['cancellation'], now_ms=arm_ms)
        saved = self.store.change_strategy(identity, fresh['revision'], reconciled, now=ARM)
        blocked = execution.entry_admission(saved['strategy'], identity, metadata,
            market, ownership, now_ms=arm_ms)
        self.assertEqual(blocked['status'], 'SOURCE_CANCELED')
        self.assertIsNone(blocked['action'])
        self.assertFalse(blocked['dispatch_enabled'])


@unittest.skipUnless(os.environ.get('HL_JOURNAL_CI_URL'), 'Requires disposable loopback PostgreSQL CI service')
class PlanStorePostgresTests(unittest.TestCase):
    def setUp(self):
        from .postgres_journal import PostgresJournal, SCHEMA
        import psycopg
        self.journal = PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL'])
        with psycopg.connect(**self.journal._parameters) as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS {store.SCHEMA} CASCADE')
            conn.execute(f'DROP SCHEMA IF EXISTS {SCHEMA} CASCADE')
        self.journal.bootstrap()
        self.store = store.PlanStore(self.journal)
        self.assertTrue(self.store.initialize())
        self.message = maxpain_plan()
    def test_real_transactions_concurrent_receipt_and_reopen(self):
        def ingest(_):
            return self.store.ingest(self.message, now=NOW, not_before=BEFORE)
        with ThreadPoolExecutor(max_workers=6) as pool:
            replies = list(pool.map(ingest, range(12)))
        self.assertEqual(sum(reply['status'] == 'RECORDED' for reply in replies), 1)
        reopened = store.PlanStore(self.journal)
        self.assertFalse(reopened.initialize())
        self.assertEqual(reopened.load(self.message['occurrence_id'])['revision'], 1)
    def test_real_cas_and_cancel_tombstone_survive_reopen(self):
        self.store.ingest(self.message, now=NOW, not_before=BEFORE)
        self.store.change_strategy(self.message['occurrence_id'], 1, {'filled_quantity': '2'}, now=NOW)
        cancel = source_update(self.message, NOW, kind='CANCEL', reason='TARGET_TOUCHED_FIRST')
        self.store.ingest(cancel, now=NOW, not_before=BEFORE)
        state = store.PlanStore(self.journal).load(self.message['occurrence_id'])
        self.assertEqual((state['revision'], state['entry_permission']), (3, 'RETIRED'))
        self.assertEqual(state['strategy'], {'filled_quantity': '2'})
        with self.assertRaisesRegex(store.PlanStoreError, 'CONCURRENT_RELOAD_REQUIRED'):
            self.store.change_strategy(self.message['occurrence_id'], 1, {}, now=NOW)


if __name__ == '__main__':
    unittest.main()
