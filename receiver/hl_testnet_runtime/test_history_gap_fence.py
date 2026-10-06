"""Missing-history work fences ENTRY across one account, never exit upkeep."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timezone
import os
import threading
import unittest
from unittest.mock import Mock

from . import emergency_close as emergency, filled_quantity_dispatch as dispatch
from . import card_lifecycle as life, trade_cards, long_stream_runtime as stream
from . import history_gap_recovery as gap, filled_quantity_exits as exits
from .filled_dispatch_store import DispatchStore, DispatchError, SCHEMA
from .postgres_journal import PostgresJournal
from .trade_card_store import CardStore
from .test_card_lifecycle import A, B, T, closed
from .test_filled_quantity_dispatch import AGENT, NoExternal, ROUTES2, Venue, state_from_case
from .test_history_gap_recovery import final_state, MemoryStore, PlannedReader


CI = os.environ.get('HL_JOURNAL_CI_URL')


class HistoryGapFenceTests(NoExternal):
    @staticmethod
    def connection(staged_account=None):
        conn = Mock()
        def execute(sql, parameters=None):
            row = Mock()
            result = ((1,) if parameters == (staged_account,) and staged_account else None)
            row.fetchone.return_value = result
            return row
        conn.execute.side_effect = execute
        return conn

    def test_local_malformed_stage_fails_closed_for_entry(self):
        for marker in ({'version': 'unknown'}, {}, False, 0, ''):
            with self.subTest(marker=marker):
                state = dict(account=A, history_gap_recovery=marker)
                with self.assertRaisesRegex(DispatchError,
                                            'ACCOUNT_HISTORY_GAP_REQUIRES_RECONCILIATION'):
                    emergency.fence(self.connection(), state, 'ENTRY')

    def test_other_market_stage_is_scoped_to_its_account(self):
        conn = self.connection(A)
        with self.assertRaisesRegex(DispatchError, 'ACCOUNT_HISTORY_GAP_REQUIRES_RECONCILIATION'):
            emergency.fence(conn, dict(account=A, symbol='ETH'), 'ENTRY')
        emergency.fence(conn, dict(account=B, symbol='ETH'), 'ENTRY')

    def test_management_keeps_existing_authority_without_gap_or_cap_reads(self):
        for operation in ('CREATE_EXIT', 'MODIFY_EXIT', 'CANCEL',
                          'CANCEL_EXPIRED_ENTRY_REMAINDER', 'CLOSE_PASSED_TAKE'):
            with self.subTest(operation=operation):
                state = dict(account=A, history_gap_recovery={'version': 'unknown'})
                original = deepcopy(state)
                conn = self.connection(A)
                emergency.fence(conn, state, operation)
                conn.execute.assert_not_called()
                self.assertEqual(state, original)

    def test_missing_stage_preserves_normal_entry_fence_without_authorizing_orders(self):
        conn = self.connection()
        state = dict(account=A)
        self.assertIsNone(emergency.fence(conn, state, 'ENTRY'))
        self.assertEqual(state, dict(account=A))

    def test_old_terminal_order_reappearing_on_another_symbol_blocks_account_proof(self):
        state = state_from_case(q='100', stop='100', take='100')
        state['evidence']['snapshot'] = closed(state['bindings'][0])
        old_entry = int(state['bindings'][0]['orders']['ENTRY'][0])
        with self.assertRaisesRegex(DispatchError, 'FINAL_ACCOUNT_ORDER_REAPPEARED_RECONCILE_FIRST'):
            stream._validate_account_inventory(A, [state],
                [dict(coin='DOGE', oid=old_entry)], dict(assetPositions=[]), role='long_account')

    def test_unknown_other_symbol_order_or_position_blocks_account_proof(self):
        state = state_from_case(q='100', stop='100', take='100')
        state['evidence']['snapshot'] = closed(state['bindings'][0])
        for orders, positions, code in (
                ([dict(coin='ETH', oid=9999)], [], 'UNOWNED_ACCOUNT_ORDER_NO_NEW_ENTRY'),
                ([], [dict(position=dict(coin='ETH', szi='1'))], 'UNOWNED_ACCOUNT_POSITION_NO_NEW_ENTRY')):
            with self.subTest(code=code), self.assertRaisesRegex(DispatchError, code):
                stream._validate_account_inventory(A, [state], orders,
                    dict(assetPositions=positions), role='long_account')

    def test_current_owned_partial_position_and_active_legs_keep_management_authority(self):
        for side, role, account, quantity in (('LONG', 'long_account', A, '40'),
                                             ('SHORT', 'short_account', B, '-40')):
            with self.subTest(role=role):
                state = state_from_case(q='40', side=side, stop='40', take='40')
                orders = [dict(coin='DOGE', oid=int(row['oid']))
                          for row in state['evidence']['snapshot']['open_orders']]
                positions = dict(assetPositions=[dict(position=dict(coin='DOGE', szi=quantity))])
                self.assertTrue(stream._validate_account_inventory(
                    account, [state], orders, positions, role=role))

    def test_actual_final_recovery_refuses_other_symbols_revived_terminal_in_both_passes(self):
        target = final_state()
        other = final_state(n=2)
        signal = deepcopy(next(iter(other['originals'].values()))['card']['prepared']['source'])
        signal['symbol'] = 'ETH'
        meta = dict(universe=[dict(name='ETH', szDecimals=2)])
        card = trade_cards.prepare_card(signal, meta, rule_id='SOFTWARE_TEST',
                                        threshold_pct='1.5', record_kind='synthetic_test')
        binding = life.binding_from_card(card, A, ROUTES2, other['bindings'][0]['orders'])
        other.update(symbol='ETH', bucket=life.digest(['testnet', A, 'ETH']),
                     bindings=[binding], originals={card['card_id']: dict(card=card,
                         draft=exits.prepare_entry(card, meta, A, ROUTES2))})
        snap = closed(binding)
        for fill in snap['fills']:
            fill['fill_id'] = 'hl:' + fill['fill_id']
        other['evidence'] = dict(bindings=[binding], snapshot=snap)
        for failure_pass in (1, 2):
            with self.subTest(pass_number=failure_pass):
                store = MemoryStore(target, other)
                venue = Venue()
                venue.t = T + 3 * gap.evidence.DAY_MS
                controller = dispatch.Controller(store, venue, ROUTES2)
                reader = PlannedReader(target['evidence'])
                reader.data['fills'].extend(PlannedReader(other['evidence']).data['fills'])
                current = gap.step(controller, target, reader=reader)['state']
                while venue.now() - current[gap.KEY]['cursor_ms'] > gap.WINDOW_MS:
                    current = gap.step(controller, current, reader=reader)['state']
                before = deepcopy(current)
                reads = [0]
                read = reader.read
                def reappeared(kind, *args, **kwargs):
                    result = read(kind, *args, **kwargs)
                    if kind == 'frontendOpenOrders':
                        reads[0] += 1
                        if reads[0] == failure_pass:
                            return [dict(coin='ETH', oid=int(binding['orders']['ENTRY'][0]))]
                    return result
                reader.read = reappeared
                with self.assertRaisesRegex(DispatchError, 'FINAL_ACCOUNT_ORDER_REAPPEARED_RECONCILE_FIRST'):
                    gap.step(controller, current, reader=reader)
                self.assertEqual(store.load(current['bucket']), before)
                self.assertIn(gap.KEY, store.load(current['bucket']))
                self.assertEqual(store.load(other['bucket']), other)
                self.assertEqual(venue.sent, 0)


@unittest.skipUnless(CI, 'disposable PostgreSQL required')
class HistoryGapFencePostgresTests(NoExternal):
    def setUp(self):
        super().setUp()
        self.journal = PostgresJournal.for_ci(CI)
        with self.journal._transaction() as conn:
            for schema in (SCHEMA, 'hl_testnet_recovery_rehearsal_v1',
                           'hl_testnet_cards_v1', 'hl_testnet_execution_v1'):
                conn.execute(f'DROP SCHEMA IF EXISTS {schema} CASCADE')
        self.journal.bootstrap()
        self.cards = CardStore(self.journal)
        self.cards.initialize()
        self.store = DispatchStore(self.journal)
        self.store.initialize()
        self.meta = dict(universe=[dict(name='DOGE', szDecimals=2),
                                   dict(name='ETH', szDecimals=2)])
        self.venue = Venue()
        self.venue.metadata = lambda: self.meta
        self.controller = dispatch.Controller(self.store, self.venue, ROUTES2)

    def planned(self, symbol, role):
        side = 'LONG' if role == 'long_account' else 'SHORT'
        source = dict(kind='SIGNAL', event_id=role + symbol, symbol=symbol, side=side,
                      entry='10', stop='9.9' if side == 'LONG' else '10.1',
                      take_profit='10.2' if side == 'LONG' else '9.8',
                      at=datetime.fromtimestamp((T - 20000) / 1000, timezone.utc).isoformat())
        card = trade_cards.prepare_card(source, self.meta, rule_id='SOFTWARE_TEST',
                                        threshold_pct='1.5', record_kind='synthetic_test')
        self.cards.record(card)
        state = self.controller.register(card['card_id'])
        state = self.controller.refresh(state['bucket'])
        proposal = dispatch.choose(state, ROUTES2, self.meta,
                                   dict(mark_price='10', at_ms=T), now_ms=T)
        return state, proposal

    def requests(self):
        with self.journal._transaction() as conn:
            return conn.execute(f'SELECT count(*) FROM {SCHEMA}.requests').fetchone()[0]

    def stage(self, marker=None, *, symbol='DOGE'):
        state = self.store.create_bucket(A, symbol)
        def update(conn, current):
            current['history_gap_recovery'] = marker
        return self.store.change(state['bucket'], state['revision'],
                                 'TEST_HISTORY_GAP_STAGE', T, update)

    def owned_fixture(self, **kwargs):
        fixture = state_from_case(**kwargs)
        state = self.store.create_bucket(fixture['account'], fixture['symbol'])
        def update(conn, current):
            for key in ('originals', 'bindings', 'evidence', 'pending'):
                current[key] = deepcopy(fixture[key])
        return self.store.change(state['bucket'], state['revision'],
                                 'TEST_OWNED_EXECUTION_FIXTURE', T, update)

    def test_stage_blocks_cross_symbol_reserve_and_begin_survives_restart(self):
        plan = self.planned('ETH', 'long_account')
        self.stage({'version': 'history_gap_recovery_v1'})
        restarted = DispatchStore(PostgresJournal.for_ci(CI))
        for store in (self.store, restarted):
            with self.subTest(restarted=store is restarted):
                with self.assertRaisesRegex(DispatchError, 'ACCOUNT_HISTORY_GAP_REQUIRES_RECONCILIATION'):
                    store.reserve(*plan, T)
                with self.assertRaisesRegex(DispatchError, 'ACCOUNT_HISTORY_GAP_REQUIRES_RECONCILIATION'):
                    store.prepare_and_begin(*plan, AGENT, T)
                self.assertIsNone(store.load(plan[0]['bucket'])['pending'])
        self.assertEqual(self.requests(), 0)
        self.assertEqual(self.venue.sent, 0)

    def test_other_account_and_management_are_not_blocked_by_gap_fence(self):
        self.stage({'version': 'history_gap_recovery_v1'})
        for symbol in ('DOGE', 'ETH'):
            self.store.create_bucket(A, symbol)
            for operation in ('CREATE_EXIT', 'CANCEL'):
                self.store.action_allowed(dict(account=A, symbol=symbol, operation=operation))
        state, proposal = self.planned('ETH', 'short_account')
        committed, request = self.store.prepare_and_begin(state, proposal, AGENT, T)
        self.assertEqual(request['proposal']['role'], 'short_account')
        self.assertEqual(request['phase'], 'OUTCOME_UNKNOWN')
        self.assertEqual(committed['pending'], request['request_id'])
        self.assertEqual(self.requests(), 1)
        self.assertEqual(self.venue.sent, 0)

    def test_malformed_and_json_null_stage_remain_fenced_until_key_removed(self):
        plan = self.planned('ETH', 'long_account')
        staged = self.stage(None)
        for marker in (None, False, {}, {'version': 'unknown'}):
            if marker is not None:
                def change(conn, current):
                    current['history_gap_recovery'] = marker
                staged = self.store.change(staged['bucket'], staged['revision'],
                                           'TEST_MALFORMED_HISTORY_GAP', T, change)
            with self.subTest(marker=marker), self.assertRaisesRegex(
                    DispatchError, 'ACCOUNT_HISTORY_GAP_REQUIRES_RECONCILIATION'):
                self.store.prepare_and_begin(*plan, AGENT, T)
        self.assertEqual(self.requests(), 0)
        def clear(conn, current):
            current.pop('history_gap_recovery')
        staged = self.store.change(staged['bucket'], staged['revision'],
                                   'TEST_HISTORY_GAP_CLEARED', T, clear)
        self.assertNotIn('history_gap_recovery', staged)
        self.assertEqual(self.requests(), 0)
        self.assertEqual(self.venue.sent, 0)
        _, request = self.store.prepare_and_begin(*plan, AGENT, T)
        self.assertEqual(request['phase'], 'OUTCOME_UNKNOWN')
        self.assertEqual(self.requests(), 1)

    def test_real_owned_stop_preparation_remains_allowed_during_other_symbol_gap(self):
        self.stage({'version': 'history_gap_recovery_v1'}, symbol='ETH')
        state = self.owned_fixture(q='100', take='100')
        proposal = dispatch.choose(state, ROUTES2, self.meta,
                                   dict(mark_price='10', at_ms=T), now_ms=T)
        self.assertEqual((proposal['operation'], proposal['leg']), ('CREATE_EXIT', 'STOP'))
        self.assertTrue(proposal['action']['orders'][0]['r'])
        _, request = self.store.prepare_and_begin(state, proposal, AGENT, T)
        self.assertEqual(request['phase'], 'OUTCOME_UNKNOWN')
        self.assertEqual(request['proposal']['quantity'], '100')
        self.assertEqual(self.requests(), 1)
        self.assertEqual(self.venue.sent, 0)

    def test_real_owned_expired_entry_cancel_remains_allowed_during_gap(self):
        self.stage({'version': 'history_gap_recovery_v1'}, symbol='ETH')
        state = self.owned_fixture(q='40', stop='40', take='40', expiry_seconds=10)
        proposal = dispatch.choose(state, ROUTES2, self.meta,
                                   dict(mark_price='10', at_ms=T), now_ms=T)
        self.assertEqual(proposal['operation'], 'CANCEL_EXPIRED_ENTRY_REMAINDER')
        self.assertEqual(proposal['action']['type'], 'cancel')
        self.assertEqual(proposal['quantity'], '0')
        _, request = self.store.prepare_and_begin(state, proposal, AGENT, T)
        self.assertEqual(request['phase'], 'OUTCOME_UNKNOWN')
        self.assertEqual(self.requests(), 1)
        self.assertEqual(self.venue.sent, 0)

    def test_stage_commit_serializes_before_cross_symbol_attempt(self):
        plan = self.planned('ETH', 'long_account')
        gap = self.store.create_bucket(A, 'DOGE')
        stage_locked = threading.Event()
        release_stage = threading.Event()
        begin_started = threading.Event()
        def create_stage():
            def update(conn, current):
                current['history_gap_recovery'] = {'version': 'history_gap_recovery_v1'}
                stage_locked.set()
                if not release_stage.wait(5):
                    raise AssertionError('TEST_STAGE_RELEASE_REQUIRED')
            return self.store.change(gap['bucket'], gap['revision'],
                                     'TEST_HISTORY_GAP_RACE', T, update)
        def begin():
            begin_started.set()
            try:
                self.store.prepare_and_begin(*plan, AGENT, T)
            except DispatchError as exc:
                return str(exc)
            raise AssertionError('GAP_ENTRY_MUST_BE_BLOCKED')
        with ThreadPoolExecutor(max_workers=2) as pool:
            staged = pool.submit(create_stage)
            try:
                self.assertTrue(stage_locked.wait(5))
                attempted = pool.submit(begin)
                self.assertTrue(begin_started.wait(5))
                self.assertEqual(self.requests(), 0)
            finally:
                release_stage.set()
            staged.result(timeout=5)
            self.assertEqual(attempted.result(timeout=5),
                             'ACCOUNT_HISTORY_GAP_REQUIRES_RECONCILIATION')
        self.assertEqual(self.requests(), 0)
        self.assertEqual(self.venue.sent, 0)
