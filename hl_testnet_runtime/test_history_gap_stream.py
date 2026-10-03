"""History recovery scheduling must never authorize a new Testnet entry."""
from copy import deepcopy
from datetime import datetime, timezone
from unittest.mock import Mock, patch

from . import long_stream_runtime as stream, fill_wakeups as wake
from . import approved_alert_selection as selection
from .filled_dispatch_store import DispatchError
from .test_card_lifecycle import A, B, T, closed
from .test_filled_quantity_dispatch import NoExternal, state_from_case
from .test_fill_wakeups import Clock
from .test_priority_fill_runtime import ready
from .test_filled_quantity_exits import original


DAY_MS = 86400000


class HistoryGapStreamTests(NoExternal):
    @staticmethod
    def historical_flat(side='LONG'):
        state = state_from_case(q='100', side=side, stop='100', take='100')
        state['evidence']['snapshot'] = closed(state['bindings'][0])
        return state

    @staticmethod
    def controller(states, *, now=T + 3 * DAY_MS):
        controller = Mock()
        controller.store.for_account.return_value = states
        controller.venue.now.return_value = now
        controller.venue.env = {}
        controller.routes = {'long_account': {'account': A},
                             'short_account': {'account': B}}
        return controller

    @staticmethod
    def progress(state, status='HISTORY_GAP_RECOVERY_PROGRESS'):
        return dict(status=status, order_requests_sent=0, state=deepcopy(state),
                    cursor_ms=T + DAY_MS,
                    remaining_ms=2 * DAY_MS if status.endswith('PROGRESS') else 0)

    def tick(self, controller, role, *, enabled):
        account = A if role == 'long_account' else B
        return stream.tick(controller, {'account': account},
                           datetime.fromtimestamp(T / 1000, timezone.utc),
                           new_entries=enabled, role=role)

    def test_old_flat_recovery_runs_even_without_source_or_enabled_entries(self):
        for role, side in (('long_account', 'LONG'), ('short_account', 'SHORT')):
            with self.subTest(role=role):
                state = self.historical_flat(side)
                original = deepcopy(state)
                controller = self.controller([state])
                gap = Mock()
                gap.needed.return_value = True
                gap.step.return_value = self.progress(state)
                with patch.object(stream, 'gap', gap), \
                     patch.object(stream, '_unfinished', return_value=False), \
                     patch.object(stream, '_maintain_bucket') as maintain, \
                     patch.object(stream.selection, 'page') as selector:
                    result = self.tick(controller, role, enabled=False)
                self.assertEqual(result['status'], 'HISTORY_GAP_RECOVERY_PROGRESS')
                self.assertEqual(result['order_requests_sent'], 0)
                self.assertEqual(result['new_cards_registered'], 0)
                self.assertEqual(result['recovery_buckets'], 1)
                self.assertEqual(result['recovery_cursor_ms'], T + DAY_MS)
                self.assertEqual(result['recovery_remaining_ms'], 2 * DAY_MS)
                gap.step.assert_called_once_with(controller, state)
                maintain.assert_not_called()
                selector.assert_not_called()
                controller.register.assert_not_called()
                controller.cycle.assert_not_called()
                self.assertEqual(state, original)

    def test_progress_and_final_commit_both_fence_entry_for_current_sweep(self):
        for role, side in (('long_account', 'LONG'), ('short_account', 'SHORT')):
            for status in ('HISTORY_GAP_RECOVERY_PROGRESS',
                           'HISTORY_GAP_RECOVERY_COMPLETE'):
                with self.subTest(role=role, status=status):
                    state = self.historical_flat(side)
                    controller = self.controller([state])
                    gap = Mock()
                    gap.needed.return_value = True
                    gap.step.return_value = self.progress(state, status)
                    with patch.object(stream, 'gap', gap), \
                         patch.object(stream, '_maintain_bucket') as maintain, \
                         patch.object(stream.selection, 'page') as selector, \
                         patch.object(stream, '_account_owned') as owned:
                        result = self.tick(controller, role, enabled=True)
                    self.assertEqual(result['status'], status)
                    self.assertEqual(result['order_requests_sent'], 0)
                    self.assertEqual(result['new_cards_registered'], 0)
                    selector.assert_not_called()
                    owned.assert_not_called()
                    maintain.assert_not_called()
                    controller.register.assert_not_called()
                    controller.cycle.assert_not_called()

    def test_live_maintenance_runs_before_flat_history_recovery(self):
        flat = self.historical_flat()
        live = state_from_case(q='40', stop='20', take='20')
        live['bucket'] = 'f' * 64
        controller = self.controller([flat, live])
        events = []
        gap = Mock()
        gap.needed.side_effect = lambda state, **kwargs: state['bucket'] == flat['bucket']
        def step(controller, state):
            events.append('history')
            return self.progress(state)
        gap.step.side_effect = step
        def maintain(controller, bucket):
            self.assertEqual(bucket, live['bucket'])
            events.append('protection')
            return iter([dict(status='NO_ACTION_NEEDED', order_requests_sent=0)])
        with patch.object(stream, 'gap', gap), \
             patch.object(stream, '_maintain_bucket', side_effect=maintain), \
             patch.object(stream.selection, 'page') as selector:
            result = self.tick(controller, 'long_account', enabled=True)
        self.assertEqual(events, ['protection', 'history'])
        self.assertEqual(result['status'], 'HISTORY_GAP_RECOVERY_PROGRESS')
        selector.assert_not_called()
        controller.register.assert_not_called()

    def test_uncertain_maintenance_error_outranks_successful_history_progress(self):
        flat = self.historical_flat()
        pending = dict(bucket='f' * 64, account=A, symbol='BTC', bindings=[],
                       originals={}, evidence=None, pending='known-unresolved-request')
        controller = self.controller([flat, pending])
        gap = Mock()
        gap.needed.side_effect = lambda state, **kwargs: state['bucket'] == flat['bucket']
        gap.step.return_value = self.progress(flat)
        with patch.object(stream, 'gap', gap), \
             patch.object(stream, '_maintain_bucket',
                          side_effect=DispatchError('OUTCOME_UNRESOLVED_NO_NEW_REQUEST')), \
             patch.object(stream.selection, 'page') as selector:
            result = self.tick(controller, 'long_account', enabled=True)
        self.assertEqual(result['status'], 'EXISTING_RECONCILIATION_REQUIRED')
        self.assertEqual(result['failure_code'], 'OUTCOME_UNRESOLVED_NO_NEW_REQUEST')
        self.assertEqual(result['failure_symbol'], 'BTC')
        self.assertEqual(result['order_requests_sent'], 0)
        self.assertEqual(result['new_cards_registered'], 0)
        gap.step.assert_called_once_with(controller, flat)
        selector.assert_not_called()
        controller.register.assert_not_called()

    def test_history_collection_failure_never_falls_through_to_entry(self):
        state = self.historical_flat()
        controller = self.controller([state])
        gap = Mock()
        gap.needed.return_value = True
        gap.step.side_effect = DispatchError('OBSERVATION_CHANGED_RETRY')
        with patch.object(stream, 'gap', gap), \
             patch.object(stream, '_maintain_bucket') as maintain, \
             patch.object(stream.selection, 'page') as selector:
            result = self.tick(controller, 'long_account', enabled=True)
        self.assertEqual(result['status'], 'EXISTING_RECONCILIATION_REQUIRED')
        self.assertEqual(result['failure_code'], 'OBSERVATION_CHANGED_RETRY')
        self.assertEqual(result['order_requests_sent'], 0)
        self.assertEqual(result['new_cards_registered'], 0)
        selector.assert_not_called()
        maintain.assert_not_called()
        controller.register.assert_not_called()
        controller.cycle.assert_not_called()

    def test_current_terminal_bucket_preserves_disabled_idle_behavior(self):
        for role, side in (('long_account', 'LONG'), ('short_account', 'SHORT')):
            with self.subTest(role=role):
                state = self.historical_flat(side)
                now = state['evidence']['snapshot']['at_ms'] + 1
                controller = self.controller([state], now=now)
                gap = Mock()
                gap.needed.return_value = False
                with patch.object(stream, 'gap', gap), \
                     patch.object(stream, '_unfinished', return_value=False), \
                     patch.object(stream, '_maintain_bucket') as maintain, \
                     patch.object(stream.selection, 'page') as selector:
                    result = self.tick(controller, role, enabled=False)
                self.assertEqual(result['status'], 'ENTRIES_DISABLED')
                gap.step.assert_not_called()
                maintain.assert_not_called()
                selector.assert_not_called()
                controller.cycle.assert_not_called()

    def test_staged_cursor_never_releases_notification_gate_from_old_flat_proof(self):
        for role, side, account in (('long_account', 'LONG', A),
                                    ('short_account', 'SHORT', B)):
            with self.subTest(role=role):
                state = self.historical_flat(side)
                state['history_gap_recovery'] = dict(cursor_ms=T + DAY_MS)
                self.assertFalse(stream._immutable_flat_checkpoint(state))
                controller = self.controller([state])
                feed = wake.FillWakeups({role: account}, clock=Clock())
                ready(feed, account)
                token = feed.begin_reconciliation(account)
                with patch.object(stream, '_account_owned') as current_inventory:
                    self.assertFalse(stream._finish_notification_reconciliation(
                        controller, feed, token, None, T + 3 * DAY_MS))
                current_inventory.assert_not_called()
                self.assertFalse(feed.entry_allowed(account))

    def test_current_clock_after_recovery_cannot_extend_original_source_expiry(self):
        for side in ('LONG', 'SHORT'):
            with self.subTest(side=side):
                card = original(side=side, expiry_seconds=300)[1]['card']
                before = deepcopy(card)
                not_before = datetime.fromtimestamp((T - 60000) / 1000, timezone.utc)
                self.assertTrue(selection.eligible(card, not_before=not_before,
                    now=datetime.fromtimestamp(T / 1000, timezone.utc)))
                self.assertFalse(selection.eligible(card, not_before=not_before,
                    now=datetime.fromtimestamp((T + 3 * DAY_MS) / 1000, timezone.utc)))
                self.assertEqual(card, before)
