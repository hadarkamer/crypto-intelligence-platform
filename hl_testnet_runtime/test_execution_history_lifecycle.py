"""Independent lifecycle regressions for durable execution history compaction.

The execution worker, SQLite transactions, source contract, request accounting,
and reporting are real. Only exchange I/O is simulated; old venue order history
deliberately remains visible after the worker removes its hot terminal records.
"""
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from approved_alert_fixtures import BASE
from . import experimental_execution_runtime as runtime
from .experimental_execution_state import ExecutionState
from .test_approved_alert_lifecycle import CurrentOnlyExchange, alert, cancellation
from .test_experimental_execution_runtime import ROUTES


class ExecutionHistoryLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        for target in ('http.client.HTTPSConnection', 'socket.create_connection',
                       'hyperliquid_testnet_executor._wallet',
                       'hyperliquid_testnet_executor._signed_body',
                       'hl_testnet_runtime.experimental_execution_runtime._range'):
            blocker = patch(target, side_effect=AssertionError('ISOLATED_HISTORY_NO_IO'))
            blocker.start()
            self.addCleanup(blocker.stop)
        self.path = Path(self.tmp.name) / 'history.isolated-experimental.sqlite3'
        self.venue = CurrentOnlyExchange(BASE + 1000)
        self.store = ExecutionState(self.path)
        self.store.initialize(ROUTES, not_before_ms=BASE - 60000)
        self.store.initialize_history()
        self.restart()

    def restart(self):
        self.store = ExecutionState(self.path)
        self.worker = runtime.IsolatedExecutionRuntime(self.store, self.venue,
                                                       mode=runtime.MODE)

    def start(self, value=None):
        value = value or alert()
        self.worker.receive([value])
        result = self.worker.run_once()
        self.assertEqual(result.get('operation'), 'ENTRY')
        return value, self.venue.oid('ENTRY')

    def protect(self, value, oid, quantity=None):
        quantity = quantity or self.venue.orders[oid]['view']['wire_order']['s']
        self.venue.fill(oid, quantity, value['entry'])
        self.assertEqual(self.worker.run_once(entries_enabled=False).get('operation'),
                         'CREATE_EXIT')
        self.assertEqual(self.worker.run_once(entries_enabled=False).get('operation'),
                         'CREATE_EXIT')
        self.worker.run_once(entries_enabled=False)
        return quantity

    def close(self):
        value, oid = self.start()
        quantity = self.protect(value, oid)
        take_oid = self.venue.oid('TAKE_PROFIT')
        self.venue.fill(take_oid, quantity)
        self.assertEqual(self.worker.run_once(entries_enabled=False).get('operation'),
                         'CANCEL')
        self.worker.run_once(entries_enabled=False)
        self.worker.run_once(entries_enabled=False)
        state = self.store.load()
        self.assertEqual(state['trades'][value['occurrence_id']]['phase'], 'CLOSED')
        report = self.worker.report()['trades'][0]
        self.assertEqual(Decimal(report['remaining_quantity']), 0)
        return value, deepcopy(report)

    def compact(self):
        return self.store.compact_history(now_ms=self.venue.t,
                                          batch_size=32, scan_limit=128, force=True)

    def entries(self):
        return [r for r in self.venue.requests if r['proposal']['operation'] == 'ENTRY']

    def collector_after_closed_archive(self):
        """Use the real raw collector with the software oracle's old facts.

        The retained fixture requests serve only RawTestnetFixture.status's
        HTTP-shaped response construction; they are never restored to the
        worker or supplied as current execution ownership.
        """
        from .experimental_live_provider import LiveEvidenceProvider
        from .test_experimental_live_provider import (ENV, LegacyFixture,
            RawTestnetFixture, UnavailablePrices)
        first, expected = self.close()
        prior_requests = deepcopy(self.store.load()['requests'])
        def oracle_state():
            value = self.store.load()
            value['requests'] = {**prior_requests, **value['requests']}
            return value
        budget = object()
        raw = RawTestnetFixture(self.venue, oracle_state, budget)
        provider = LiveEvidenceProvider(ENV, legacy_store=LegacyFixture(),
            experimental_store=self.store, price_evidence=UnavailablePrices(),
            budget=budget, clock=self.venue.now, info_reader=raw,
            public_reader=raw, lookup_reader=raw.lookup)
        account = ROUTES['long_account']['account']
        _, checkpoint, _, _, _, _ = provider._lane(self.store.load(), account,
            'HYPE', [], provider._inventory(account))
        lane = runtime._lane(account, 'HYPE')
        self.store.mutate(lambda value: value.setdefault('collector_checkpoints', {})
                          .update({lane: checkpoint}))
        self.compact()
        second, _ = self.start(alert(cycle='raw-collector-successor'))
        raw.calls.clear()
        return provider, raw, first, second, expected

    def test_closed_trade_exact_report_survives_compaction_and_restart(self):
        value, expected = self.close()
        cid = value['occurrence_id']
        self.compact()
        state = self.store.load()
        self.assertNotIn(cid, state['trades'])
        self.assertFalse(any(r['proposal']['card_id'] == cid
                             for r in state['requests'].values()))
        self.restart()
        page = self.worker.history_report()
        self.assertEqual(page['trades'], [expected])
        self.assertIn(cid, page['archived_occurrences'])
        self.assertEqual(page['domain'], 'software')
        self.assertIsNone(expected['net_pnl'])
        self.assertGreater(Decimal(expected['gross_pnl_before_costs']), 0)

    def test_duplicate_and_late_cancel_after_restart_do_not_reopen_closed_trade(self):
        value, expected = self.close()
        self.compact()
        before = len(self.venue.requests)
        self.restart()
        self.worker.receive([value, deepcopy(value), cancellation(value, self.venue.t)])
        for _ in range(3):
            self.worker.run_once()
        self.assertEqual(len(self.venue.requests), before)
        self.assertEqual(len(self.entries()), 1)
        self.assertNotIn(value['occurrence_id'], self.store.load()['trades'])
        self.assertEqual(self.worker.history_report()['trades'], [expected])

    def test_fresh_same_symbol_trade_works_while_archived_venue_orders_remain(self):
        first, expected = self.close()
        archived_orders = deepcopy(self.venue.orders)
        self.compact()
        self.restart()
        next_value = alert(cycle='next-independent-occurrence',
                           approved_ms=self.venue.t // 60000 * 60000)
        second, oid = self.start(next_value)
        self.assertNotEqual(second['occurrence_id'], first['occurrence_id'])
        self.protect(second, oid)
        self.assertEqual(len(self.entries()), 2)
        self.assertEqual(self.store.load()['trades'][second['occurrence_id']]['phase'], 'OPEN')
        self.assertEqual(self.worker.history_report()['trades'], [expected])
        for old_oid, old in archived_orders.items():
            self.assertEqual(self.venue.orders[old_oid], old)

    def test_changed_archived_fill_is_rejected_before_new_entry(self):
        first, _ = self.close()
        self.compact()
        self.restart()
        entry_oid = self.venue.oid('ENTRY')
        self.venue.orders[entry_oid]['view']['fills'][0]['price'] = '92'
        self.worker.receive([alert(cycle='changed-history-must-block',
                                   approved_ms=self.venue.t // 60000 * 60000)])
        before = len(self.venue.requests)
        with self.assertRaises(ValueError):
            self.worker.run_once()
        self.assertEqual(len(self.venue.requests), before)
        self.assertEqual(len(self.entries()), 1)
        self.assertNotIn(first['occurrence_id'], self.store.load()['trades'])

    def test_active_partial_fill_is_not_archived_and_protection_survives_restart(self):
        value, oid = self.start()
        self.protect(value, oid, '1')
        self.compact()
        self.restart()
        self.worker.receive([cancellation(value, self.venue.t)])
        self.worker.run_once(entries_enabled=False)
        self.worker.run_once(entries_enabled=False)
        trade = self.store.load()['trades'][value['occurrence_id']]
        self.assertEqual(runtime._remaining(trade), Decimal('1'))
        self.assertEqual(self.venue.orders[oid]['view']['status'], 'CANCELED')
        self.assertEqual(self.worker.history_report()['trades'], [])
        for leg in ('STOP', 'TAKE_PROFIT'):
            order = self.venue.orders[self.venue.oid(leg)]['view']
            self.assertEqual(order['status'], 'OPEN')
            self.assertEqual(order['wire_order']['s'], '1')
        self.assertEqual(len(self.entries()), 1)

    def test_cancel_before_alert_survives_source_only_archive_and_restart(self):
        value = alert(cycle='cancel-before-any-entry')
        self.worker.receive([cancellation(value, self.venue.t)])
        self.compact()
        self.restart()
        self.worker.receive([value, deepcopy(value)])
        self.worker.run_once()
        self.assertEqual(self.venue.requests, [])
        page = self.worker.history_report()
        self.assertEqual(page['trades'], [])
        self.assertIn(value['occurrence_id'], page['archived_occurrences'])

    def test_raw_collector_overlap_replays_archived_fills_without_historic_lookups(self):
        provider, raw, first, second, expected = self.collector_after_closed_archive()
        account = ROUTES['long_account']['account']
        normalized, checkpoint, _, _, _, _ = provider._lane(self.store.load(),
            account, 'HYPE', [], provider._inventory(account))
        current_oid = self.venue.oid('ENTRY')
        self.assertEqual([row['oid'] for row in normalized['orders']], [current_oid])
        self.assertEqual(checkpoint['fills'], [])
        self.assertEqual(checkpoint['terminal_orders'], [])
        lookups = [row[2] for row in raw.calls if row[0] == 'lookup']
        self.assertEqual(lookups, [self.venue.orders[current_oid]['view']['cloid']])
        self.assertNotIn(first['occurrence_id'], self.store.load()['trades'])
        self.assertIn(second['occurrence_id'], self.store.load()['trades'])
        self.assertEqual(self.worker.history_report()['trades'], [expected])

    def test_raw_collector_changed_archived_fill_fails_exact_history_check(self):
        provider, raw, _, _, _ = self.collector_after_closed_archive()
        archived_entry_oid = self.venue.oid('ENTRY', index=0)
        self.venue.orders[archived_entry_oid]['view']['fills'][0]['price'] = '92'
        account = ROUTES['long_account']['account']
        before = len(self.venue.requests)
        with self.assertRaisesRegex(ValueError, 'ARCHIVED_TERMINAL_FILL_CHANGED'):
            provider._lane(self.store.load(), account, 'HYPE', [],
                           provider._inventory(account))
        self.assertEqual(len(self.venue.requests), before)

    def test_archive_insert_failure_rolls_back_but_keeps_partial_fill_protected(self):
        first, _ = self.close()
        second, oid = self.start(alert(symbol='ETH', cycle='active-during-archive-failure'))
        self.venue.fill(oid, '1', second['entry'])
        self.venue.t = BASE + 61000  # Due maintenance; approvals remain fresh.
        self.worker.receive([alert(symbol='SOL', cycle='defer-new-exposure')])
        self.store.mutate(lambda state: state['history'].update(cursor=''))
        with patch('hl_testnet_runtime.experimental_execution_archive._SQL.insert',
                   side_effect=sqlite3.OperationalError('SIMULATED_ARCHIVE_INSERT_FAILURE')) as insert:
            for _ in range(4):
                self.worker.run_once()
        self.assertEqual(insert.call_count, 4)
        state = self.store.load()
        self.assertIn(first['occurrence_id'], state['trades'])
        self.assertEqual(state['history']['archived_count'], 0)
        self.assertEqual(len(self.entries()), 2)
        self.assertEqual(self.worker.report()['history_maintenance_error'],
                         'HISTORY_MAINTENANCE_DEFERRED')
        for leg in ('STOP', 'TAKE_PROFIT'):
            order = self.venue.orders[self.venue.oid(leg)]['view']
            self.assertEqual(order['status'], 'OPEN')
            self.assertEqual(order['wire_order']['s'], '1')

    def test_archive_commit_ack_loss_reloads_committed_state_before_protection(self):
        first, expected = self.close()
        second, oid = self.start(alert(symbol='ETH', cycle='active-during-lost-ack'))
        self.venue.fill(oid, '1', second['entry'])
        self.store.mutate(lambda state: state['history'].update(cursor=''))
        original = self.store.compact_history
        def commit_then_lose_ack(**kwargs):
            original(**kwargs, force=True)
            raise OSError('SIMULATED_ARCHIVE_COMMIT_ACK_LOST')
        with patch.object(self.store, 'compact_history', side_effect=commit_then_lose_ack):
            result = self.worker.run_once()
        self.assertEqual(result.get('operation'), 'CREATE_EXIT')
        self.assertNotIn(first['occurrence_id'], self.store.load()['trades'])
        self.assertEqual(self.worker.history_report()['trades'], [expected])
        stop = self.venue.orders[self.venue.oid('STOP')]['view']
        self.assertEqual(stop['wire_order']['s'], '1')
        self.assertEqual(len(self.entries()), 2)

    def test_active_state_integrity_failure_is_not_hidden_by_maintenance_fallback(self):
        from .experimental_execution_state import StateError
        value, oid = self.start()
        self.venue.fill(oid, '1', value['entry'])
        before = len(self.venue.requests)
        with patch.object(self.store, 'load',
                          side_effect=StateError('ISOLATED_STATE_INTEGRITY_FAILURE')):
            with self.assertRaisesRegex(StateError, 'STATE_INTEGRITY_FAILURE'):
                self.worker.run_once()
        self.assertEqual(len(self.venue.requests), before)

    def test_oversized_history_record_blocks_entries_but_protects_partial_position(self):
        value, oid = self.start()
        self.venue.fill(oid, '1', value['entry'])
        self.worker.receive([alert(symbol='ETH', cycle='deferred-for-history-capacity')])
        with patch.object(self.store, 'compact_history', return_value=dict(
                status='HISTORY_RECORD_CAPACITY_REVIEW_REQUIRED', archived=0,
                oversized_records=1)):
            for _ in range(4):
                self.worker.run_once()
        self.assertEqual(len(self.entries()), 1)
        self.assertEqual(self.worker.report()['history_maintenance_error'],
                         'HISTORY_RECORD_CAPACITY_REVIEW_REQUIRED')
        for leg in ('STOP', 'TAKE_PROFIT'):
            order = self.venue.orders[self.venue.oid(leg)]['view']
            self.assertEqual(order['status'], 'OPEN')
            self.assertEqual(order['wire_order']['s'], '1')


class TestnetDomainHistoryMaintenanceTests(unittest.TestCase):
    """Native-domain control flow with memory SQL and software transport only."""
    def test_failed_history_maintenance_does_not_stop_native_domain_protection(self):
        from . import test_approved_alert_lifecycle as support
        fixture = support.ApprovedContinuousReleaseTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        value = alert()
        fixture.worker.receive([value])
        fixture.cycle()
        oid = fixture.venue.oid('ENTRY')
        fixture.venue.fill(oid, '1', value['entry'])
        fixture.worker.receive([alert(symbol='ETH', cycle='native-deferred-entry')])
        with patch.object(fixture.store, 'compact_history',
                          side_effect=OSError('SIMULATED_HISTORY_OUTAGE')):
            for _ in range(4):
                fixture.cycle()
        self.assertEqual(sum(r['proposal']['operation'] == 'ENTRY'
                             for r in fixture.venue.requests), 1)
        for leg in ('STOP', 'TAKE_PROFIT'):
            order = fixture.venue.orders[fixture.venue.oid(leg)]['view']
            self.assertEqual(order['status'], 'OPEN')
            self.assertEqual(order['wire_order']['s'], '1')
        report = fixture.worker.report()
        self.assertEqual(report['domain'], 'testnet')
        self.assertEqual(report['history_maintenance_error'], 'HISTORY_MAINTENANCE_DEFERRED')


if __name__ == '__main__':
    unittest.main()
