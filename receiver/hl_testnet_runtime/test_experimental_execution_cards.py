"""Real isolated lifecycle/store tests for durable cards; all venue I/O blocked."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from decimal import Decimal
import sqlite3
import threading
import unittest
from unittest.mock import patch

from . import experimental_execution_cards as cards
from . import experimental_execution_archive as archive
from .experimental_execution_state import ExecutionState, StateError, MAX_STATE_BYTES
from .postgres_journal import JournalError
from . import test_execution_history_lifecycle as lifecycle_support
from . import test_approved_alert_lifecycle as approved_support
from .test_approved_alert_lifecycle import alert, cancellation


class TradeCardTests(unittest.TestCase):
    def setUp(self):
        self.fixture = lifecycle_support.ExecutionHistoryLifecycleTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    def test_card_records_planned_and_actual_prices_and_owned_protections(self):
        f = self.fixture
        value, oid = f.start()
        f.protect(value, oid, '1')
        state = f.store.load(); before = deepcopy(state)
        card = cards.cards_from_state(state, domain='software')[0]
        self.assertEqual(state, before)
        self.assertEqual(card['card_id'], value['occurrence_id'])
        self.assertEqual(card['source'], value)
        self.assertEqual(card['original_prices'], {k: value[k] for k in ('entry','stop','take_profit')})
        self.assertEqual(card['summary']['actual_average_entry'], value['entry'])
        self.assertEqual(card['summary']['remaining_quantity'], '1')
        self.assertEqual({p['leg'] for p in card['protections']}, {'STOP', 'TAKE_PROFIT'})
        self.assertTrue(all(p['quantity'] == '1' for p in card['protections']))
        self.assertIsNone(card['summary']['net_pnl'])
        self.assertIsNone(card['summary']['gross_pnl_before_costs'])
        self.assertEqual(card['entry_fills'][0]['order_id'], oid)
        self.assertEqual(f.worker.report()['cards'], [card])

    def test_card_is_identical_after_close_archive_and_restart(self):
        f = self.fixture
        value, _ = f.close()
        cid = value['occurrence_id']
        before = f.store.trade_card(cid)['card']
        self.assertEqual(before['actual_exit_legs'], ['TAKE_PROFIT'])
        self.assertGreater(Decimal(before['summary']['gross_pnl_before_costs']), 0)
        f.compact(); f.restart()
        result = f.store.trade_card(cid)
        self.assertEqual(result['card'], before)
        self.assertEqual(result['location'], 'archived')
        self.assertEqual(f.worker.history_report()['cards'], [before])
        self.assertEqual(f.worker.report()['cards'], [])
        self.assertNotIn(cid, f.store.load()['trades'])

    def test_late_cancellation_preserves_card_and_has_separate_durable_audit(self):
        f = self.fixture
        value, _ = f.close(); cid = value['occurrence_id']
        before = f.store.trade_card(cid)['card']; f.compact()
        cancel = cancellation(value, f.venue.t)
        f.worker.receive([cancel]); f.restart()
        result = f.store.trade_card(cid)
        self.assertEqual(result['card'], before)
        self.assertEqual(result['current_source']['cancellation'], cancel)
        self.assertEqual(result['source_updates'][0]['source']['cancellation'], cancel)
        self.assertEqual(f.store.trade_card(cid, after_update_revision=result['current_source']['revision'])['source_updates'], [])

    def test_concurrent_read_and_archive_never_lose_or_duplicate_card(self):
        f = self.fixture
        value, _ = f.close(); cid = value['occurrence_id']
        expected = f.store.trade_card(cid)['card']
        other = ExecutionState(f.path); barrier = threading.Barrier(2)
        def read():
            barrier.wait()
            return other.trade_card(cid)
        def compact():
            barrier.wait()
            return f.compact()
        with ThreadPoolExecutor(2) as pool:
            read_result = pool.submit(read); archive_result = pool.submit(compact)
            self.assertEqual(read_result.result()['card'], expected)
            self.assertEqual(archive_result.result()['archived'], 1)
        self.assertEqual(other.trade_card(cid)['card'], expected)

    def test_separate_accounts_and_symbols_have_distinct_cards(self):
        f = self.fixture
        first, _ = f.start()
        f.worker.run_once(entries_enabled=False)
        second, _ = f.start(alert(symbol='SOL', side='SHORT', cycle='independent-short'))
        f.worker.run_once(entries_enabled=False)
        result = cards.cards_from_state(f.store.load(), domain='software')
        self.assertEqual(len(result), 2)
        by_id = {row['card_id']: row for row in result}
        self.assertNotEqual(by_id[first['occurrence_id']]['account'], by_id[second['occurrence_id']]['account'])
        for value in (first, second):
            card = by_id[value['occurrence_id']]
            self.assertEqual(card['symbol'], value['symbol'])
            self.assertEqual(card['original_prices']['entry'], value['entry'])
            self.assertTrue(all(a['proposal']['card_id'] == value['occurrence_id'] for a in card['attempts']))

    def test_attempts_are_ordered_and_uncertain_is_not_presented_as_filled(self):
        f = self.fixture; f.venue.lose_reply = True
        value = alert()
        f.worker.receive([value]); f.worker.run_once()
        f.restart()
        card = f.store.trade_card(value['occurrence_id'])['card']
        self.assertEqual(card['attempts'][0]['phase'], 'OUTCOME_UNKNOWN')
        self.assertFalse(card['attempts'][0]['definitely_not_submitted'])
        self.assertEqual(card['entry_fills'], [])
        self.assertIsNone(card['summary']['actual_average_entry'])
        self.assertEqual(card['summary']['unresolved_attempts'], 1)

    def test_unsent_retry_and_wait_reason_survive_restart(self):
        f = self.fixture
        value, _ = f.start(); cid = value['occurrence_id']
        reason = dict(stage='admission', scope='occurrence', reason='TEMPORARILY_BLOCKED',
                      at_ms=f.venue.t, retry_after_ms=f.venue.t+1000,
                      deadline=value['expires_at'], role='long_account', symbol=value['symbol'])
        def set_wait(state):
            state.setdefault('entry_decisions', {})[cid] = reason
            state.setdefault('entry_blocked', {})[cid] = reason['reason']
            state['trades'][cid]['phase'] = 'RETRY_WAIT_UNSENT'
            for request in state['requests'].values():
                request['phase'] = 'ABORTED_UNSENT'
                request['certified_unsent'] = True
        f.store.mutate(set_wait); f.restart()
        card = f.store.trade_card(cid)['card']
        self.assertEqual(card['entry_decision'], reason)
        self.assertEqual(card['wait_reasons'][0], dict(scope='occurrence', reason='TEMPORARILY_BLOCKED'))
        self.assertTrue(card['attempts'][0]['definitely_not_submitted'])
        self.assertEqual(card['status'], 'RETRY_WAIT_UNSENT')

    def test_entry_decision_and_wait_evidence_survive_archive(self):
        f = self.fixture; value, _ = f.close(); cid = value['occurrence_id']
        f.store.mutate(lambda state: state.setdefault('entry_decisions', {}).update({cid:
            dict(stage='preflight', scope='occurrence', reason='WAITED_BEFORE_SUCCESS', at_ms=f.venue.t)}))
        expected = f.store.trade_card(cid)['card']; f.compact(); f.restart()
        self.assertEqual(f.store.trade_card(cid)['card'], expected)

    def test_mismatched_trade_source_or_request_identity_is_rejected(self):
        f = self.fixture; value, _ = f.start(); cid = value['occurrence_id']
        state = f.store.load(); state['trades'][cid]['source']['entry'] = '1'
        with self.assertRaisesRegex(cards.CardError, 'IMMUTABLE'):
            cards.cards_from_state(state, domain='software')
        state = f.store.load()
        next(iter(state['requests'].values()))['proposal']['symbol'] = 'DOGE'
        with self.assertRaisesRegex(cards.CardError, 'ATTEMPT_IDENTITY'):
            cards.cards_from_state(state, domain='software')

    def test_corrupted_archive_cannot_be_displayed_as_verified_card(self):
        f = self.fixture; value, _ = f.close(); cid = value['occurrence_id']; f.compact()
        with f.store._history_transaction() as (db, _state, _save):
            db.execute(f'UPDATE {db.table("history")} SET checksum=? WHERE occurrence_id=?', ('bad', cid))
        with self.assertRaisesRegex(StateError, 'ARCHIVED_HISTORY_INTEGRITY_FAILURE'):
            f.store.trade_card(cid)

    def test_unknown_source_only_and_invalid_page_do_not_create_a_trade(self):
        f = self.fixture; value = alert()
        f.worker.receive([cancellation(value, f.venue.t)]); f.compact()
        self.assertIsNone(f.store.trade_card(value['occurrence_id']))
        self.assertIsNone(f.store.trade_card('f'*64))
        for kwargs in (dict(update_limit=101), dict(after_update_revision=-1)):
            with self.assertRaisesRegex(StateError, 'BOUNDED'):
                f.store.trade_card(value['occurrence_id'], **kwargs)


class MaintenanceReadinessTests(unittest.TestCase):
    def setUp(self):
        self.fixture = lifecycle_support.ExecutionHistoryLifecycleTests()
        self.fixture.setUp(); self.addCleanup(self.fixture.doCleanups)

    def test_availability_failure_reloads_healthy_state_without_mutation(self):
        f = self.fixture; before = f.store.load()
        for failure in (OSError('lost commit acknowledgment'), sqlite3.OperationalError('database locked'),
                        JournalError('PERSISTENCE_UNAVAILABLE_NO_SEND')):
            with self.subTest(failure=type(failure).__name__):
                result = archive.maintenance_readiness(f.store, failure)
                self.assertTrue(result['entries_allowed'])
                self.assertEqual(result['reserved_bytes'], 1024*1024)
                self.assertEqual(f.store.load(), before)

    def test_domain_integrity_capacity_and_programming_failures_remain_blocked(self):
        f = self.fixture
        for failure in (StateError('ARCHIVED_HISTORY_INTEGRITY_FAILURE'),
                        StateError('ISOLATED_STATE_CAPACITY_REVIEW_REQUIRED'),
                        StateError('HISTORY_CLOCK_REGRESSION'),
                        JournalError('JOURNAL_SCHEMA_REQUIRES_REVIEW'),
                        sqlite3.IntegrityError('UNIQUE'), RuntimeError('bug')):
            self.assertFalse(archive.maintenance_readiness(f.store, failure)['entries_allowed'])

    def test_invalid_live_journal_is_not_hidden_by_optional_maintenance(self):
        f = self.fixture
        with patch.object(f.store, 'load', side_effect=StateError('ISOLATED_STATE_INTEGRITY_FAILURE')):
            with self.assertRaisesRegex(StateError, 'INTEGRITY'):
                archive.maintenance_readiness(f.store, OSError('temporary'))

    def test_reloaded_state_without_headroom_cannot_open_more_exposure(self):
        f = self.fixture; state = f.store.load()
        state['events'] = [dict(padding='x'*(MAX_STATE_BYTES-512*1024))]
        with patch.object(f.store, 'load', return_value=state):
            result = archive.maintenance_readiness(f.store, OSError('temporary'))
        self.assertFalse(result['entries_allowed'])
        self.assertEqual(result['status'], 'HISTORY_ACTIVE_CAPACITY_REVIEW_REQUIRED')

    def test_real_archive_rollback_keeps_cards_and_allows_verified_active_journal(self):
        f = self.fixture; value, _ = f.close(); cid = value['occurrence_id']
        before = f.store.trade_card(cid)['card']
        with patch.object(archive._SQL, 'insert', side_effect=sqlite3.OperationalError('database locked')):
            try:
                f.compact()
            except sqlite3.OperationalError as exc:
                self.assertTrue(archive.maintenance_readiness(f.store, exc)['entries_allowed'])
            else:
                self.fail('Expected archive rollback')
        self.assertEqual(f.store.trade_card(cid)['card'], before)
        self.assertEqual(f.store.load()['history']['archived_count'], 0)


class NativeDomainCardTests(unittest.TestCase):
    def test_native_report_cards_preserve_domain_account_and_actual_fill_facts(self):
        f = approved_support.ApprovedContinuousReleaseTests()
        f.setUp(); self.addCleanup(f.doCleanups)
        value = alert()
        f.worker.receive([value]); f.cycle()
        oid = f.venue.oid('ENTRY')
        f.venue.fill(oid, '1', value['entry'])
        for _ in range(3):
            f.cycle(entries=False)
        result = f.worker.report(); card = result['cards'][0]
        self.assertEqual(result['domain'], 'testnet')
        self.assertEqual(card['domain'], 'testnet')
        self.assertEqual(card['account'], f.release['routes']['long_account'])
        self.assertEqual(card['summary']['actual_average_entry'], value['entry'])
        self.assertEqual(card['summary']['remaining_quantity'], '1')
        self.assertTrue(all(attempt['proposal']['account'] == card['account'] for attempt in card['attempts']))
        with self.assertRaisesRegex(cards.CardError, 'DOMAIN_REQUIRED'):
            cards.cards_from_state(f.store.load(), domain='software')


if __name__ == '__main__':
    unittest.main()
