"""Finite multi-entry trial: expiry, restart, account isolation and cap races."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import time
import unittest
from unittest.mock import Mock, patch

from . import bounded_entry_trial as cap, card_lifecycle as life
from .filled_dispatch_store import DispatchError, DispatchStore, SCHEMA
from .trade_card_store import CardStore
from .postgres_journal import PostgresJournal
from . import test_bounded_entry_trial as fixtures
from .test_bounded_entry_trial import environment, CI
from .test_card_lifecycle import A, B, T
from .test_filled_quantity_dispatch import NoExternal, Venue, ROUTES2
from . import filled_quantity_dispatch as dispatch


class WindowPureTests(NoExternal):
    def test_fixed_five_and_exact_48_hours_requires_existing_testnet_scope(self):
        env = {**environment(), 'HL_TESTNET_ENTRY_ATTEMPT_CAP': cap.WINDOW_MODE}
        scope = cap.configuration(env, 'long_account', A)
        self.assertEqual(scope['max_attempts'], 5)
        self.assertEqual(scope['deadline_ms'] - scope['epoch_ms'], 172800000)
        for key, value in [('RENDER_SERVICE_ID', 'production'),
                           ('HL_TESTNET_EMERGENCY_RELEASE', '')]:
            with self.assertRaises(DispatchError):
                cap.configuration({**env, key: value}, 'long_account', A)

    def test_database_wall_clock_enforces_start_and_exclusive_deadline(self):
        scope = dict(epoch_ms=T, deadline_ms=T+cap.WINDOW_MS)
        conn = Mock()
        for now, active in [(T-1, False), (T, True),
                            (T+cap.WINDOW_MS-1, True), (T+cap.WINDOW_MS, False)]:
            conn.execute.return_value.fetchone.return_value = (now,)
            self.assertEqual(cap._window_active(conn, scope), active)
        conn.execute.return_value.fetchone.return_value = ('wrong',)
        with self.assertRaisesRegex(DispatchError, 'CLOCK_REQUIRED'):
            cap._window_active(conn, scope)

    def test_expiry_rechecks_before_begin_without_writing_or_counting(self):
        env = {**environment(), 'HL_TESTNET_ENTRY_ATTEMPT_CAP': cap.WINDOW_MODE}
        proposal = dict(operation='ENTRY', role='long_account', account=A)
        conn = Mock()
        with patch.object(cap, '_window_active', return_value=False), \
                patch.object(cap, '_attempts', side_effect=AssertionError('NO_COUNT_OR_BEGIN')):
            with self.assertRaisesRegex(DispatchError, 'WINDOW_CLOSED'):
                cap.entry_guard(env, proposal)(conn, dict(account=A), proposal)

    def test_unknown_outcome_blocks_new_entry_even_below_cap(self):
        env = {**environment(), 'HL_TESTNET_ENTRY_ATTEMPT_CAP': cap.WINDOW_MODE}
        proposal = dict(operation='ENTRY', role='long_account', account=A)
        for phase in ['OUTCOME_UNKNOWN', 'ACK_UNVERIFIED', 'REJECTED', 'CONFLICT']:
            with self.subTest(phase=phase), \
                    patch.object(cap, '_window_active', return_value=True), \
                    patch.object(cap, '_attempts', return_value=[dict(phase=phase)]):
                with self.assertRaisesRegex(DispatchError, 'UNRESOLVED_ATTEMPT'):
                    cap.entry_guard(env, proposal)(Mock(), dict(account=A), proposal)

    def test_expired_trial_does_not_disable_exit_or_cancellation_authority(self):
        env = {**environment(), 'HL_TESTNET_ENTRY_ATTEMPT_CAP': cap.WINDOW_MODE}
        for operation in ['CREATE_EXIT', 'CANCEL_ENTRY', 'CANCEL_EXIT']:
            store = Mock()
            proposal = dict(operation=operation, role='long_account', account=A)
            cap.check_entry(store, env, proposal)
            self.assertIsNone(cap.entry_guard(env, proposal))
            store.journal._transaction.assert_not_called()


@unittest.skipUnless(CI, 'disposable PostgreSQL required')
class WindowPostgresTests(NoExternal):
    planned = fixtures.CapPostgresTests.planned
    begin = fixtures.CapPostgresTests.begin

    def setUp(self):
        self.enterContext(patch('hl_testnet_runtime.test_bounded_entry_trial.T', int(time.time()*1000)))
        super().setUp()
        self.j = PostgresJournal.for_ci(CI)
        with self.j._transaction() as conn:
            for schema in (SCHEMA, 'hl_testnet_recovery_rehearsal_v1',
                           'hl_testnet_cards_v1', 'hl_testnet_execution_v1'):
                conn.execute(f'DROP SCHEMA IF EXISTS {schema} CASCADE')
        self.j.bootstrap()
        self.cards = CardStore(self.j)
        self.cards.initialize()
        self.store = DispatchStore(self.j)
        self.store.initialize()
        self.env = environment()
        self.meta = dict(universe=[])
        self.venue = Venue()
        self.venue.metadata = lambda: self.meta
        self.controller = dispatch.Controller(self.store, self.venue, ROUTES2)
        self.env['HL_TESTNET_ENTRY_ATTEMPT_CAP'] = cap.WINDOW_MODE
        self.symbols = ['DOGE', 'ETH', 'SOL', 'BNB', 'BTC', 'XRP']
        self.meta['universe'] = [dict(name=s, szDecimals=2) for s in self.symbols]

    def consume(self, symbol, role):
        state, request = self.begin(self.planned(symbol, role))
        # A positively certified unsent outcome still consumes one attempt.
        certificate = dispatch.DefinitelyUnsent(request, 'TESTNET_REQUEST_BUDGET_PERMIT_EXPIRED')
        self.store.abort_definitely_unsent(state, request, certificate, request['attempt_at_ms']+1)
        return request

    def test_five_per_role_survives_restart_and_does_not_reset_on_unsent(self):
        for role in ['long_account', 'short_account']:
            for index, symbol in enumerate(self.symbols[:5]):
                self.assertFalse(cap.cap_reached(self.store, self.env, role))
                self.consume(symbol, role)
                restarted = DispatchStore(PostgresJournal.for_ci(CI))
                self.assertEqual(cap.cap_reached(restarted, self.env, role), index == 4)
            with self.assertRaisesRegex(DispatchError, 'CAP_REACHED'):
                self.begin(self.planned(self.symbols[5], role))
        self.assertEqual(self.venue.sent, 0)

    def test_concurrent_last_slot_commits_exactly_one_and_allows_only_its_send(self):
        for symbol in self.symbols[:4]:
            self.consume(symbol, 'long_account')
        plans = [self.planned(s, 'long_account') for s in self.symbols[4:]]
        def begin(plan):
            try:
                return self.begin(plan)[1]
            except DispatchError as exc:
                self.assertEqual(str(exc), 'BOUNDED_ENTRY_TRIAL_CAP_REACHED')
                return None
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(begin, plans))
        own = [r for r in results if r]
        self.assertEqual(len(own), 1)
        cap.check_entry(self.store, self.env, own[0]['proposal'])
        other = next(p for _, p in plans if p != own[0]['proposal'])
        with self.assertRaisesRegex(DispatchError, 'CAP_REACHED'):
            cap.check_entry(self.store, self.env, other)
        self.assertFalse(cap.cap_reached(self.store, self.env, 'short_account'))

    def test_unknown_attempt_blocks_another_market_before_cap_and_survives_restart(self):
        _, request = self.begin(self.planned('DOGE', 'long_account'))
        restarted = DispatchStore(PostgresJournal.for_ci(CI))
        cap.check_entry(restarted, self.env, request['proposal'])
        plan = self.planned('ETH', 'long_account')
        with self.assertRaisesRegex(DispatchError, 'UNRESOLVED_ATTEMPT'):
            self.begin(plan)
        with self.assertRaisesRegex(DispatchError, 'UNRESOLVED_ATTEMPT'):
            cap.check_entry(restarted, self.env, plan[1])

    def test_expired_and_future_epochs_block_entries_after_restart(self):
        now = datetime.now(timezone.utc).timestamp()
        for epoch in [now-cap.WINDOW_MS/1000-1, now+3600]:
            env = {**self.env, 'HL_TESTNET_LONG_NOT_BEFORE':
                   datetime.fromtimestamp(epoch, timezone.utc).isoformat()}
            restarted = DispatchStore(PostgresJournal.for_ci(CI))
            self.assertTrue(cap.cap_reached(restarted, env, 'long_account'))
            _, proposal = self.planned('DOGE', 'long_account')
            with self.assertRaisesRegex(DispatchError, 'WINDOW_CLOSED'):
                cap.check_entry(restarted, env, proposal)
            with self.assertRaisesRegex(DispatchError, 'WINDOW_CLOSED'):
                cap.entry_guard(env, proposal)(Mock(), dict(account=A), proposal)
