"""Bounded explicit one-shot controller: no keys, venue, orders or production DB."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext, redirect_stdout
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import io
import os
import uuid
import unittest
from unittest.mock import Mock, patch

from . import single_trial_controller as controller, protection_timing_trial as trial
from . import emergency_close, trade_cards
from .filled_dispatch_store import DispatchError
from .postgres_journal import JournalError, PostgresJournal
from .request_budget import BudgetError
from .test_filled_quantity_dispatch import NoExternal, state_from_case
from .test_filled_quantity_exits import original, META
from .test_long_stream_runtime import env
from .test_card_lifecycle import T

CI = os.environ.get('HL_JOURNAL_CI_URL')
COMMIT = 'a' * 40


def environment(at=None):
    at = at or datetime.fromtimestamp(T / 1000, timezone.utc)
    return {**env(), 'HL_TESTNET_SHORT_ENTRY_ENABLED': 'false',
        'HL_TESTNET_EMERGENCY_CLOSE': emergency_close.APPROVAL,
        'HL_TESTNET_SINGLE_TRIAL_CONTROLLER': controller.MODE,
        'HL_TESTNET_SINGLE_TRIAL_COMMIT': COMMIT, 'RENDER_GIT_COMMIT': COMMIT,
        'HL_TESTNET_SINGLE_TRIAL_RUN_ID': 'hadar-timing-20261002T031000Z',
        'HL_TESTNET_SINGLE_TRIAL_NOT_BEFORE': (at - timedelta(seconds=30)).isoformat(),
        'HL_TESTNET_SINGLE_TRIAL_DEADLINE': (at + timedelta(minutes=20)).isoformat()}


def health():
    return dict(running=True, new_entries_enabled=False, short_entries_enabled=False,
        emergency_close=dict(running=True, last_status='PASS_COMPLETE', last_pass_at_ms=T),
        fill_notifications=dict(long_account=dict(entry_allowed=True)))


class MemoryRuns:
    journal = object()
    def __init__(self):
        self.record = None
        self.calls = []
        self.uncertain_select = False
    def initialize(self):
        pass
    def claim(self, configuration, owner):
        if self.record is not None:
            return False
        self.record = dict(configuration=configuration, owner=owner, stage='WAITING_FOR_FRESH_CARD')
        return True
    def update(self, configuration, owner, stage, **values):
        self.calls.append(stage)
        self.record.update(stage=stage, **values)
        if stage == 'ONE_CANDIDATE_SELECTED' and self.uncertain_select:
            raise JournalError('PERSISTENCE_UNAVAILABLE_NO_SEND')
        return deepcopy(self.record)


class PureTests(NoExternal):
    def setUp(self):
        super().setUp()
        self.clock = [datetime.fromtimestamp(T / 1000, timezone.utc)]
        self.card = original(expiry_seconds=300)[1]['card']
        self.store = MemoryRuns()
        self.runner = Mock(return_value=dict(status='ENTRY_NOT_SUBMITTED', entry_attempts=0))
        self.stopped = Mock()
        self.stopped.is_set.return_value = False
        self.stopped.wait.side_effect = lambda seconds: self.clock.__setitem__(0, self.clock[0] + timedelta(seconds=seconds))

    def run_controller(self, find=None, **kw):
        with redirect_stdout(io.StringIO()):
            controller.execute(environment(), self.stopped, clock=lambda: self.clock[0],
                find=find or Mock(return_value=self.card), runner=self.runner, store=self.store, **kw)

    def test_disabled_default_does_not_start_or_read_database(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(controller.PostgresJournal, 'from_env', side_effect=AssertionError('NO_DB')), \
                patch.object(controller.multiprocessing, 'get_context', side_effect=AssertionError('NO_PROCESS')):
            self.assertIsNone(controller.config({}))
            self.assertFalse(controller.start())
            controller.execute({}, self.stopped)

    def test_stale_controller_config_never_prevents_ordinary_worker_startup(self):
        configuration = environment(datetime.now(timezone.utc))
        configuration['RENDER_GIT_COMMIT'] = 'b' * 40
        output = io.StringIO()
        with patch.dict(os.environ, configuration, clear=True), \
                patch.object(controller.multiprocessing, 'get_context', side_effect=AssertionError('NO_PROCESS')), \
                patch.object(controller.PostgresJournal, 'from_env', side_effect=AssertionError('NO_DB')), \
                redirect_stdout(output):
            self.assertFalse(controller.start())
        self.assertIn('CONTROLLER_DISABLED_REQUIRES_REVIEW', output.getvalue())
        self.assertIn('SINGLE_TRIAL_EXACT_COMMIT_AND_RUN_ID_REQUIRED', output.getvalue())
        with self.assertRaisesRegex(DispatchError, 'SINGLE_TRIAL_EXACT_COMMIT_AND_RUN_ID_REQUIRED'):
            controller.execute(configuration, self.stopped, store=self.store)

    def candidate_hint(self, cards=None, *, used=0, price='10', ready=True, error=None):
        cards = cards or [self.card]
        journal, conn = Mock(), Mock()
        journal._transaction.return_value = nullcontext(conn)
        conn.execute.side_effect = [Mock(), Mock(fetchone=Mock(return_value=None)),
                                   Mock(fetchone=Mock(return_value=(used,)))]
        context = dict(coin='DOGE', user=environment()['HL_TESTNET_LONG_ACCOUNT_ADDRESS'],
            markPx=price, availableToTrade=['100', '100'], maxTradeSzs=['100', '100'])
        reader = Mock()
        reader.read.side_effect = error
        reader.read.return_value = context
        current = health()
        if not ready:
            current['fill_notifications']['long_account']['entry_allowed'] = False
        with patch.object(controller, 'next_cards', return_value=[c['card_id'] for c in cards]), \
                patch.object(controller, 'deployed_health', return_value=current), \
                patch.object(controller, 'DispatchStore') as store, \
                patch.object(controller, 'CardStore') as loaded, \
                patch.object(controller, 'Budget') as budget, \
                patch.object(controller.checks, 'InfoReader', return_value=reader) as factory:
            store.return_value.for_account.return_value = []
            loaded.return_value.load.side_effect = cards
            result = controller.candidate(journal, environment(), controller.config(environment()), self.clock[0])
        return result, reader, factory, budget

    def test_price_hint_skips_crossed_boundary_and_preserves_original_cards(self):
        for price in ('9.8', '9.9', '10.2', '10.3'):
            before = deepcopy(self.card)
            result, reader, factory, budget = self.candidate_hint(price=price)
            self.assertIsNone(result)
            self.assertEqual(self.card, before)
            reader.read.assert_called_once_with('activeAssetData',
                user=environment()['HL_TESTNET_LONG_ACCOUNT_ADDRESS'], coin='DOGE')
            self.assertEqual(factory.call_args.kwargs['priority'], 'background')
            self.assertIs(factory.call_args.kwargs['budget'], budget.return_value)
        result, reader, _, _ = self.candidate_hint(price='10')
        self.assertEqual(result, self.card)

    def test_price_context_is_shared_for_same_account_market_in_one_pass(self):
        first = original(n=2, expiry_seconds=300)[1]['card']
        source = deepcopy(self.card['prepared']['source'])
        source.update(event_id='hint-suitable-card', entry='9.7', stop='9.6', take_profit='10')
        suitable = trade_cards.prepare_card(source, META, rule_id='SOFTWARE_TEST', threshold_pct='1.5',
            record_kind='received_alert', source_expires_at=self.card['source_expires_at'])
        cards = [self.card, first, suitable]
        before = deepcopy(cards)
        result, reader, _, _ = self.candidate_hint(cards, price='9.8')
        self.assertEqual(result, suitable)
        reader.read.assert_called_once()
        self.assertEqual(cards, before)

    def test_no_price_request_when_feed_or_combined_headroom_is_missing(self):
        for used, ready in ((0, False), (407, True)):
            with self.subTest(used=used, ready=ready):
                result, reader, factory, _ = self.candidate_hint(used=used, ready=ready)
                self.assertIsNone(result)
                factory.assert_not_called()
                reader.read.assert_not_called()

    def test_price_budget_blip_waits_without_consuming_candidate(self):
        result, reader, _, _ = self.candidate_hint(error=BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED'))
        self.assertIsNone(result)
        reader.read.assert_called_once()
        finder = Mock(side_effect=[DispatchError('SINGLE_TRIAL_PUBLIC_PRICE_UNAVAILABLE'), self.card])
        self.run_controller(finder)
        self.runner.assert_called_once()
        self.assertEqual(finder.call_count, 2)

    def test_config_requires_fixed_service_commit_disabled_entries_and_short_window(self):
        for key, value in [('RENDER_SERVICE_ID', 'mainnet'), ('RENDER_GIT_COMMIT', 'b' * 40),
                ('HL_TESTNET_LONG_ENTRY_ENABLED', 'true'), ('HL_TESTNET_SHORT_ENTRY_ENABLED', 'true'),
                ('HL_TESTNET_RUNTIME_MODE', 'mainnet'), ('HL_TESTNET_EMERGENCY_CLOSE', ''),
                ('HL_TESTNET_SINGLE_TRIAL_CONTROLLER', 'true'), ('HL_TESTNET_TWO_ACCOUNT_EXECUTION', 'approved_single_attempt_v1'),
                ('HL_TESTNET_SINGLE_TRIAL_DEADLINE', (self.clock[0] + timedelta(minutes=40)).isoformat())]:
            with self.subTest(key=key), self.assertRaises(DispatchError):
                controller.config({**environment(), key: value})

    def test_future_start_waits_without_selection_or_order(self):
        configuration = environment()
        configuration['HL_TESTNET_SINGLE_TRIAL_NOT_BEFORE'] = (self.clock[0] + timedelta(seconds=20)).isoformat()
        finder = Mock(return_value=None)
        self.stopped.is_set.side_effect = [False, False, False, False, True]
        with redirect_stdout(io.StringIO()):
            controller.execute(configuration, self.stopped, clock=lambda: self.clock[0],
                find=finder, runner=self.runner, store=self.store)
        finder.assert_not_called()
        self.runner.assert_not_called()

    def test_expired_configuration_has_no_reservation_or_order(self):
        expired = environment(self.clock[0] - timedelta(hours=1))
        with redirect_stdout(io.StringIO()):
            controller.execute(expired, self.stopped, clock=lambda: self.clock[0],
                runner=self.runner, store=self.store)
        self.assertIsNone(self.store.record)
        self.runner.assert_not_called()

    def test_committed_selection_precedes_sole_runner_and_no_replacement(self):
        finder = Mock(return_value=self.card)
        def run(environment, cid):
            self.assertEqual(self.store.record['stage'], 'ONE_CANDIDATE_SELECTED')
            self.assertEqual(cid, self.card['card_id'])
            self.assertEqual(environment['HL_TESTNET_LONG_ENTRY_ENABLED'], 'false')
            return dict(status='ENTRY_NOT_SUBMITTED', entry_attempts=0)
        self.runner.side_effect = run
        self.run_controller(finder)
        self.runner.assert_called_once()
        finder.assert_called_once()
        self.run_controller(finder)
        self.runner.assert_called_once()
        self.assertEqual(self.store.record['stage'], 'EXPERIMENT_COMPLETE')

    def test_uncertain_selected_commit_never_invokes_runner_or_replays(self):
        self.store.uncertain_select = True
        self.run_controller()
        self.runner.assert_not_called()
        self.run_controller()
        self.runner.assert_not_called()
        self.assertEqual(self.store.record['stage'], 'STOPPED_REQUIRES_REVIEW')

    def test_selected_runner_failure_consumes_run_with_no_replacement(self):
        self.runner.side_effect = DispatchError('TRIAL_RECONCILIATION_REQUIRED')
        finder = Mock(return_value=self.card)
        self.run_controller(finder)
        self.run_controller(finder)
        self.runner.assert_called_once()
        finder.assert_called_once()
        self.assertEqual(self.store.record['stage'], 'STOPPED_REQUIRES_REVIEW')

    def test_parent_stop_after_selection_does_not_start_entry(self):
        self.stopped.is_set.side_effect = [False, False, True]
        self.run_controller()
        self.runner.assert_not_called()
        self.assertEqual(self.store.record['stage'], 'STOPPED_REQUIRES_REVIEW')

    def test_read_only_health_blip_can_wait_within_original_window(self):
        finder = Mock(side_effect=[DispatchError('SINGLE_TRIAL_WORKER_HEALTH_UNAVAILABLE'), self.card])
        self.run_controller(finder)
        self.runner.assert_called_once()
        self.assertEqual(finder.call_count, 2)
        self.assertEqual(self.store.record['configuration'], controller.config(environment()))

    def test_integrity_error_does_not_wait_or_attempt(self):
        finder = Mock(side_effect=DispatchError('STORED_CARD_CHECKSUM_MISMATCH'))
        self.run_controller(finder)
        self.runner.assert_not_called()
        finder.assert_called_once()

    def test_no_candidate_expiry_has_no_attempt(self):
        self.run_controller(Mock(return_value=None))
        self.runner.assert_not_called()
        self.assertEqual(self.store.record['stage'], 'NO_ELIGIBLE_CARD_WITHIN_WINDOW')

    def test_hints_do_not_accept_gap_stale_protection_pending_or_full_quota(self):
        self.assertTrue(controller.ready_hint(self.card, [], health(), T, 0))
        for problem in ('gap', 'stale', 'pending', 'budget', 'running', 'enabled'):
            state, current, used = [], health(), 0
            if problem == 'gap':
                current['fill_notifications']['long_account']['entry_allowed'] = False
            elif problem == 'stale':
                current['emergency_close']['last_pass_at_ms'] = T - 15001
            elif problem == 'pending':
                state = [dict(pending='uncertain', originals={}, symbol='BTC')]
            elif problem == 'budget':
                used = 430
            elif problem == 'running':
                current['running'] = False
            else:
                current['new_entries_enabled'] = True
            with self.subTest(problem=problem):
                self.assertFalse(controller.ready_hint(self.card, state, current, T, used))

    def test_original_controller_deadline_shortens_and_never_renews_trial_grant(self):
        configuration = environment()
        configuration['HL_TESTNET_SINGLE_TRIAL_DEADLINE'] = (self.clock[0] + timedelta(seconds=12)).isoformat()
        self.assertEqual(trial.validate(configuration, self.card, T), T + 12000)
        with self.assertRaisesRegex(DispatchError, 'SINGLE_TRIAL_SELECTION_WINDOW_EXPIRED'):
            trial.validate(configuration, self.card, T + 12000)
        self.assertEqual(trial.validate({**env(), 'HL_TESTNET_SHORT_ENTRY_ENABLED': 'false',
            'HL_TESTNET_EMERGENCY_CLOSE': emergency_close.APPROVAL}, self.card, T), T + 90000)

    def test_spawn_has_private_supervisor_and_stop_never_terminates_it(self):
        configuration = environment(datetime.now(timezone.utc))
        context = Mock()
        with patch.dict(os.environ, configuration, clear=True), \
                patch.object(controller.multiprocessing, 'get_context', return_value=context) as spawn, \
                patch.object(controller, '_process', None), patch.object(controller, '_stop', None):
            self.assertTrue(controller.start())
            spawn.assert_called_once_with('spawn')
            context.Process.assert_called_once()
            self.assertFalse(context.Process.call_args.kwargs['daemon'])
            controller.stop()
            context.Event.return_value.set.assert_called_once()
            context.Process.return_value.terminate.assert_not_called()

    def test_handoff_requires_a_new_committed_protected_checkpoint(self):
        state = state_from_case(q='100', stop='100', take='100', expiry_seconds=300)
        state['evidence']['snapshot']['at_ms'] = T + 1
        result = dict(status='PROTECTED_TIMING_COMPLETE', ongoing_management_required=True,
                      trial_supervisor_stopped=True, deployed_worker_handoff_verified=False)
        with patch.object(controller, 'DispatchStore') as store, \
                patch.object(controller, 'deployed_health', return_value=health()), \
                patch.object(controller.time, 'time_ns', side_effect=[T * 1000000, (T + 2) * 1000000, (T + 3) * 1000000, (T + 4) * 1000000]), \
                patch.object(controller.time, 'monotonic', side_effect=[0, 1]):
            store.return_value.for_account.return_value = [state]
            verified = controller.verify_handoff(environment(), object(), self.card, result)
        self.assertTrue(verified['deployed_worker_handoff_verified'])
        self.assertFalse(result['deployed_worker_handoff_verified'])

    def test_handoff_timeout_or_unknown_supervisor_keeps_review_latch(self):
        for stopped in (False, True):
            result = dict(status='PROTECTED_TIMING_COMPLETE', ongoing_management_required=True,
                          trial_supervisor_stopped=stopped, deployed_worker_handoff_verified=False)
            with patch.object(controller, 'DispatchStore'), \
                    patch.object(controller.time, 'monotonic', side_effect=[0, 46]):
                observed = controller.verify_handoff(environment(), object(), self.card, result)
            self.assertEqual(observed['status'], 'RECONCILIATION_REQUIRED')
            self.assertFalse(observed['deployed_worker_handoff_verified'])

    def test_gunicorn_default_hook_is_inert_and_explicit_hook_runs_after_stream(self):
        from . import gunicorn_conf
        calls = []
        with patch.dict(os.environ, env(), clear=True), \
                patch.object(controller.stream, 'start', side_effect=lambda: calls.append('ordinary')), \
                patch.object(controller, 'start', side_effect=lambda: calls.append('explicit')), \
                redirect_stdout(io.StringIO()):
            gunicorn_conf.post_worker_init(None)
        self.assertEqual(calls, ['ordinary', 'explicit'])


@unittest.skipUnless(CI, 'Requires disposable PostgreSQL CI service')
class PostgresTests(unittest.TestCase):
    def setUp(self):
        self.journal = PostgresJournal.for_ci(CI)
        self.journal.bootstrap()
        with self.journal._transaction() as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS {controller.SCHEMA} CASCADE')
        self.store = controller.RunStore(self.journal)
        self.store.initialize()
        self.configuration = controller.config(environment(datetime.now(timezone.utc)))

    def card(self, *, receipt=True):
        from .trade_card_store import CardStore
        from .alert_cards_intake import ReceiptStore, initialize
        store = CardStore(self.journal)
        store.initialize()
        initialize(self.journal)
        now = datetime.now(timezone.utc)
        source = dict(kind='SIGNAL', event_id='controller-pg-' + uuid.uuid4().hex,
            symbol='DOGE', side='LONG', entry='10', stop='9.9', take_profit='10.2', at=now.isoformat())
        card = trade_cards.prepare_card(source, META, rule_id='SOFTWARE_TEST', threshold_pct='1.5',
            record_kind='received_alert', source_expires_at=(now + timedelta(seconds=300)).isoformat())
        store.record(card)
        if receipt:
            ReceiptStore(self.journal).save(card['card_id'], 'RECORDED', card['card_id'], source)
        return card

    def test_concurrent_claim_and_restart_cannot_run_again(self):
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda owner: self.store.claim(self.configuration, owner), ['a', 'b']))
        self.assertEqual(sum(results), 1)
        self.assertFalse(controller.RunStore(self.journal).claim(self.configuration, 'new-process'))

    def test_unknown_commit_ack_does_not_grant_claim_on_restart(self):
        from contextlib import contextmanager
        original_transaction = self.journal._transaction
        @contextmanager
        def uncertain():
            with original_transaction() as conn:
                yield conn
            raise JournalError('PERSISTENCE_UNAVAILABLE_NO_SEND')
        with patch.object(self.journal, '_transaction', uncertain), self.assertRaises(JournalError):
            self.store.claim(self.configuration, 'a')
        self.assertFalse(self.store.claim(self.configuration, 'b'))

    def test_uncertain_run_globally_blocks_new_run(self):
        self.assertTrue(self.store.claim(self.configuration, 'a'))
        self.store.update(self.configuration, 'a', 'STOPPED_REQUIRES_REVIEW', failure_code='TEST_UNKNOWN')
        replacement = {**self.configuration, 'run_id': 'hadar-timing-20261002T040000Z'}
        self.assertFalse(self.store.claim(replacement, 'b'))

    def test_wrong_owner_cannot_release_or_select_reserved_run(self):
        self.store.claim(self.configuration, 'a')
        with self.assertRaises(DispatchError):
            self.store.update(self.configuration, 'b', 'NO_ELIGIBLE_CARD_WITHIN_WINDOW')

    def test_selected_card_commits_once_and_original_times_are_immutable(self):
        self.store.claim(self.configuration, 'a')
        card = self.card()
        record = self.store.update(self.configuration, 'a', 'ONE_CANDIDATE_SELECTED', card=card)
        self.assertEqual(record['card_id'], card['card_id'])
        self.assertEqual(record['source_at'], card['prepared']['source']['at'])
        self.assertEqual(record['source_expires_at'], card['source_expires_at'])
        with self.assertRaises(DispatchError):
            self.store.update(self.configuration, 'a', 'ONE_CANDIDATE_SELECTED', card=self.card())
        self.assertFalse(self.store.claim({**self.configuration, 'run_id': 'hadar-timing-20261002T043000Z'}, 'b'))

    def test_missing_receipt_or_modified_source_cannot_select(self):
        self.store.claim(self.configuration, 'a')
        card = self.card(receipt=False)
        changed = deepcopy(self.card())
        # Only the presented object is mutated; storage keeps original expiry.
        changed['source_expires_at'] = (datetime.now(timezone.utc) + timedelta(seconds=400)).isoformat()
        for invalid in (card, changed):
            with self.assertRaises(DispatchError):
                self.store.update(self.configuration, 'a', 'ONE_CANDIDATE_SELECTED', card=invalid)
