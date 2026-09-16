"""Offline integration for the explicitly requested dual-CVD notification.

Fake transaction and Telegram boundaries exercise revocation races, one-at-a-time
claims and uncertain sends. No real network, database, subscription or scan runs.
"""
import asyncio
from contextlib import ExitStack
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import importlib
import os
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import dual_cvd65_delivery as delivery
import main


class MemoryStore:
    """A transaction boundary fake; detector and transition math have own tests."""
    def __init__(self):
        self.rows = []
        self.records = []
        self.scopes = []
        self.claim_calls = 0
        self.releases = []
        self.expire_orphans = False

    def initialize_scope(self, scope, now):
        self.scopes.append(scope)
        return {'scope': scope, 'activated_at': now.isoformat()}

    def record_cycle(self, scope, evaluation, now, *, price_references=None):
        self.records.append((scope, deepcopy(evaluation), now, deepcopy(price_references)))
        return {'record_status': 'RECORDED', 'created_intents': 0, 'counts': {}}

    def seed(self, count=1, *, expired=False, symbol='ZEC'):
        for _ in range(count):
            identity = str(len(self.rows) + 1)
            self.rows.append({'intent_id': identity, 'symbol': symbol,
                'text': delivery.alert.THRESHOLD_HEADER + 'frozen dual-CVD ' + identity,
                'status': 'PENDING', 'attempt_token': None, 'watch_scan_id': 'source-watch',
                'expires_at': datetime.now(timezone.utc) + timedelta(minutes=-1 if expired else 30)})

    def claim_pending(self, scope, now, limit=1):
        if limit != 1:
            raise AssertionError('Reserve only the message about to be attempted')
        self.claim_calls += 1
        for row in self.rows:
            if row['status'] == 'PENDING':
                row.update(status='IN_FLIGHT', attempt_token='attempt-' + row['intent_id'])
                return [deepcopy(row)]
        return []

    def release_unattempted(self, identity, token, now):
        row = self.rows[int(identity) - 1]
        if row['attempt_token'] != token or row['status'] != 'IN_FLIGHT':
            return False
        self.releases.append(identity)
        row.update(status='PENDING', attempt_token=None)
        return True

    def finish_attempt(self, identity, token, status, now):
        row = self.rows[int(identity) - 1]
        if row['attempt_token'] != token or row['status'] != 'IN_FLIGHT':
            return False
        row.update(status=status, attempt_token=None)
        return True

    def settle_orphans(self, scope, now):
        affected = 0
        if self.expire_orphans:
            for row in self.rows:
                if row['status'] == 'IN_FLIGHT':
                    row.update(status='UNKNOWN', attempt_token=None)
                    affected += 1
        return affected


class DeliveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        importlib.reload(delivery)
        self.db = MemoryStore()
        self.boundaries = ExitStack()
        self.addCleanup(self.boundaries.close)
        self.schema = self.boundaries.enter_context(patch.object(delivery.store, 'schema_ready', return_value=True))
        for name in ('initialize_scope', 'record_cycle', 'claim_pending', 'release_unattempted',
                     'finish_attempt', 'settle_orphans'):
            self.boundaries.enter_context(patch.object(delivery.store, name, getattr(self.db, name)))

    async def drain(self, bot, **kwargs):
        kwargs.setdefault('may_deliver', lambda: True)
        return await delivery.drain(bot, 1, **kwargs)

    async def test_initialize_is_idempotent_and_record_keeps_complete_frozen_evaluation(self):
        self.assertTrue(await delivery.initialize(1))
        self.assertTrue(await delivery.initialize(1))
        self.assertEqual(len(self.db.scopes), 1)
        bundle = {'cycle_id': 'source-watch', 'coins': {s: {} for s in ('BTC','ETH','SOL','HYPE','DOGE','ZEC','BNB','XRP')}}
        when = datetime.now(timezone.utc)
        evaluation = {'watch_scan_id': 'source-watch', 'observed_at_utc': when.isoformat(),
                      'source': 'frozen', 'coins': deepcopy(bundle['coins'])}
        with patch.object(delivery.alert, 'evaluate_bundle', return_value=evaluation) as detector:
            await delivery.record_watch(1, bundle, watch_scan_id='source-watch', decision_time=when)
        detector.assert_called_once()
        self.assertIs(detector.call_args.args[0], bundle)
        self.assertEqual(len(self.db.records), 1)
        self.assertEqual(self.db.records[0][1], evaluation)
        evaluation['coins'].clear()
        self.assertEqual(len(self.db.records[0][1]['coins']), 8)

    async def test_price_references_pass_separately_from_canonical_evaluation(self):
        bundle = {'cycle_id': 'source-watch'}
        evaluation = {'watch_scan_id': 'source-watch', 'source': 'frozen'}
        references = {'ZEC': {'SPOT_CVD': {'price': '100'}}}
        with patch.object(delivery.alert, 'evaluate_bundle', return_value=evaluation):
            await delivery.record_watch(1, bundle, watch_scan_id='source-watch',
                decision_time=datetime.now(timezone.utc), price_references=references)
        self.assertEqual(self.db.records[0][1], evaluation)
        self.assertEqual(self.db.records[0][3], references)
        self.assertNotIn('price_references', evaluation)
        self.assertEqual(bundle, {'cycle_id': 'source-watch'})

    async def test_claims_are_bounded_and_positive_ack_is_never_resent(self):
        self.db.seed(3)
        bot = SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(message_id=123)))
        self.assertEqual(await self.drain(bot), 2)
        self.assertEqual([row['status'] for row in self.db.rows], ['DELIVERED','DELIVERED','PENDING'])
        self.assertEqual(bot.send_message.await_count, 2)
        self.assertEqual(await self.drain(bot), 1)
        self.assertEqual(await self.drain(bot), 0)
        self.assertEqual(bot.send_message.await_count, 3)
        self.assertEqual([call.kwargs['text'] for call in bot.send_message.await_args_list],
                         [row['text'] for row in self.db.rows])

    async def test_missing_authorization_callback_never_claims_or_sends(self):
        self.db.seed()
        bot = SimpleNamespace(send_message=AsyncMock())
        self.assertEqual(await delivery.drain(bot, 1), 0)
        self.assertEqual(self.db.claim_calls, 0)
        bot.send_message.assert_not_awaited()

    async def test_selected_profiles_block_existing_pending_experimental_outbox(self):
        self.db.seed(2)
        bot = SimpleNamespace(send_message=AsyncMock())
        for profile in ('SELECTED_EXPERIMENTAL_ONLY', 'ORDINARY_AND_SELECTED_EXPERIMENTAL'):
            with self.subTest(profile=profile), patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': profile}):
                self.assertEqual(await self.drain(bot), 0)
                self.assertFalse(delivery.status()['delivery_allowed_by_profile'])
        self.assertEqual(self.db.claim_calls, 0)
        self.schema.assert_not_called()
        self.assertTrue(all(row['status'] == 'PENDING' for row in self.db.rows))
        bot.send_message.assert_not_awaited()

    async def test_profile_change_during_claim_releases_without_sending(self):
        self.db.seed()
        bot = SimpleNamespace(send_message=AsyncMock())
        for profile in ('SELECTED_EXPERIMENTAL_ONLY', 'ORDINARY_AND_SELECTED_EXPERIMENTAL'):
            def claim(*args, **kwargs):
                rows = self.db.claim_pending(*args, **kwargs)
                os.environ['ALERT_DELIVERY_PROFILE'] = profile
                return rows
            with self.subTest(profile=profile), \
                 patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': 'ALL'}), \
                 patch.object(delivery.store, 'claim_pending', claim):
                self.assertEqual(await self.drain(bot), 0)
        self.assertEqual(self.db.releases, ['1', '1'])
        self.assertEqual(self.db.rows[0]['status'], 'PENDING')
        bot.send_message.assert_not_awaited()

    async def test_ordinary_and_selected_profile_retains_experimental_capture(self):
        bundle = {'cycle_id': 'source-watch'}
        evaluation = {'watch_scan_id': 'source-watch', 'source': 'frozen'}
        with patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': 'ORDINARY_AND_SELECTED_EXPERIMENTAL'}), \
             patch.object(delivery.alert, 'evaluate_bundle', return_value=evaluation) as detector:
            self.assertTrue(delivery.delivery_policy.ordinary_alerts_enabled())
            result = await delivery.record_watch(1, bundle, watch_scan_id='source-watch',
                decision_time=datetime.now(timezone.utc))
            self.assertEqual(result['record_status'], 'RECORDED')
            self.assertFalse(delivery.status()['delivery_allowed_by_profile'])
        detector.assert_called_once()
        self.assertEqual(self.db.records[0][1], evaluation)

    async def test_priority_waits_for_existing_send_and_rechecks_authorization(self):
        await delivery.initialize(1)
        self.db.seed()
        bot = SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(message_id=1)))
        for authorized, expected in ((False, 0), (True, 1)):
            active = [True]
            await delivery._DRAIN_LOCK.acquire()
            # Supervisor recovery remains nonblocking.
            self.assertEqual(await self.drain(bot), 0)
            task = asyncio.create_task(self.drain(bot, limit=1, wait_for_lock=True,
                                                 may_deliver=lambda: active[0]))
            try:
                await asyncio.sleep(0)
                self.assertFalse(task.done())
                bot.send_message.assert_not_awaited()
                active[0] = authorized
            finally:
                delivery._DRAIN_LOCK.release()
            self.assertEqual(await task, expected)
        self.assertEqual(self.db.claim_calls, 1)
        bot.send_message.assert_awaited_once()

    async def test_old_non_zec_claim_is_blocked_and_legacy_zec_gets_threshold_header(self):
        self.db.seed(symbol='BTC')
        self.db.seed()
        self.db.rows[1]['text'] = 'legacy ZEC notification'
        bot = SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(message_id=1)))
        self.assertEqual(await self.drain(bot), 1)
        self.assertEqual([row['status'] for row in self.db.rows], ['FAILED', 'DELIVERED'])
        bot.send_message.assert_awaited_once()
        self.assertEqual(bot.send_message.call_args.kwargs['text'],
                         '<b>סף 2%</b>\nlegacy ZEC notification')

    async def test_revocation_before_claim_or_after_first_send_prevents_further_transport(self):
        self.db.seed(2)
        active = [False]
        bot = SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(message_id=1)))
        await delivery.drain(bot, 1, may_deliver=lambda: active[0])
        bot.send_message.assert_not_awaited()
        self.assertEqual(self.db.claim_calls, 0)
        active[0] = True
        async def first_send(**kwargs):
            active[0] = False
            return SimpleNamespace(message_id=1)
        bot.send_message.side_effect = first_send
        self.assertEqual(await delivery.drain(bot, 1, may_deliver=lambda: active[0]), 1)
        self.assertEqual(self.db.claim_calls, 1)
        self.assertEqual([r['status'] for r in self.db.rows], ['DELIVERED','PENDING'])

    async def test_stop_while_claim_is_awaited_releases_without_a_network_attempt(self):
        self.db.seed()
        active = [True]
        def claim(*args, **kwargs):
            rows = self.db.claim_pending(*args, **kwargs)
            active[0] = False
            return rows
        bot = SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(message_id=1)))
        with patch.object(delivery.store, 'claim_pending', claim):
            await delivery.drain(bot, 1, may_deliver=lambda: active[0])
        bot.send_message.assert_not_awaited()
        self.assertEqual(self.db.releases, ['1'])
        self.assertEqual(self.db.rows[0]['status'], 'PENDING')
        active[0] = True
        self.assertEqual(await delivery.drain(bot, 1, may_deliver=lambda: active[0]), 1)
        bot.send_message.assert_awaited_once()

    async def test_timeout_missing_ack_and_known_rejection_are_terminal(self):
        self.db.seed(3)
        Forbidden = type('Forbidden', (Exception,), {})
        bot = SimpleNamespace(send_message=AsyncMock(side_effect=[TimeoutError('fixture'),
            SimpleNamespace(message_id=None), Forbidden('fixture')]))
        self.assertEqual(await self.drain(bot), 0)
        self.assertEqual([r['status'] for r in self.db.rows], ['UNKNOWN','UNKNOWN','PENDING'])
        self.assertEqual(await self.drain(bot), 0)
        self.assertEqual([r['status'] for r in self.db.rows], ['UNKNOWN','UNKNOWN','FAILED'])
        await self.drain(bot)
        self.assertEqual(bot.send_message.await_count, 3)

    async def test_cancellation_after_claim_leaves_an_uncertain_attempt_that_does_not_retry(self):
        self.db.seed()
        bot = SimpleNamespace(send_message=AsyncMock(side_effect=asyncio.CancelledError))
        with self.assertRaises(asyncio.CancelledError):
            await self.drain(bot)
        self.assertEqual(self.db.rows[0]['status'], 'IN_FLIGHT')
        self.db.expire_orphans = True
        bot.send_message.side_effect = None
        bot.send_message.return_value = SimpleNamespace(message_id=1)
        self.assertEqual(await self.drain(bot), 0)
        self.assertEqual(self.db.rows[0]['status'], 'UNKNOWN')
        self.assertEqual(bot.send_message.await_count, 1)

    async def test_acknowledgement_storage_failure_does_not_authorize_resend(self):
        self.db.seed()
        bot = SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(message_id=1)))
        with patch.object(delivery.store, 'finish_attempt', side_effect=RuntimeError('fixture unavailable store')):
            await self.drain(bot)
        self.assertEqual(self.db.rows[0]['status'], 'IN_FLIGHT')
        self.db.expire_orphans = True
        delivery._NEXT_INIT = 0
        await self.drain(bot)
        self.assertEqual(self.db.rows[0]['status'], 'UNKNOWN')
        self.assertEqual(bot.send_message.await_count, 1)

    async def test_claim_that_expired_before_transport_is_not_sent(self):
        self.db.seed(expired=True)
        bot = SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(message_id=1)))
        self.assertEqual(await self.drain(bot), 0)
        bot.send_message.assert_not_awaited()


class WatchHookTests(unittest.IsolatedAsyncioTestCase):
    async def test_complete_bundle_reaches_rule_before_output_filter_and_ordinary_messages(self):
        order = []
        bundle = {'cycle_id': None, 'coins': {s: {'source': s} for s in ('BTC','ETH','SOL','HYPE','DOGE','ZEC','BNB','XRP')}}
        # No displayed MaxPain opportunity exists. The CVD rule still receives
        # every captured coin, independently of an MP/OI/family threshold.
        def build_bundle(**kwargs):
            bundle['cycle_id'] = kwargs['cycle_id']
            return bundle
        async def collect(**kwargs):
            result = {}
            await kwargs['prepare_watch']([], result)
            return [], result
        async def record(chat_id, value, **kwargs):
            order.append('record_dual')
            self.assertIs(value, bundle)
            self.assertEqual(kwargs['watch_scan_id'], bundle['cycle_id'])
            return {'record_status': 'RECORDED', 'created_intents': 0, 'counts': {}}
        async def drain(*args, **kwargs):
            order.append('drain_dual')
            self.assertTrue(kwargs['may_deliver']())
            return 0
        def display_filter(items):
            order.append('display_filter')
            return items
        async def send(**kwargs):
            order.append('ordinary_send')
            return SimpleNamespace(message_id=1)
        bot = SimpleNamespace(bot=SimpleNamespace(send_message=AsyncMock(side_effect=send)))
        with ExitStack() as stack:
            for target, name, value in (
                (main, 'WATCH_GENERAL_ENABLED', True), (main, 'WATCH_RUNTIME', {'chat_id': 1}),
                (main, '_get_scrape_lock', lambda: asyncio.Lock()),
                (main, 'collect_live_rows_for_watch', collect),
                (main, '_ensure_watch_derivatives_ready', AsyncMock(return_value={})),
                (main.market_confidence_engine, 'capture_snapshot', lambda symbols: {}),
                (main.research_watch_score_capture, 'prepare', lambda rows, snapshot: ([], {}, {})),
                (main.research_watch_score_capture, 'build_bundle', build_bundle),
                (main, '_filter_top8_items', display_filter),
                (main.dual_cvd65_delivery, 'record_watch', AsyncMock(side_effect=record)),
                (main.dual_cvd65_delivery, 'drain', AsyncMock(side_effect=drain)),
                (main.watch_transition_delivery, 'record_watch', AsyncMock(return_value={})),
                (main.watch_transition_delivery, 'drain', AsyncMock(return_value=0)),
                (main.watch_transition_delivery, 'cycle_result', lambda scan: {'status': 'COMPLETE'}),
                (main.manual_formula_alert_delivery, 'run_watch', AsyncMock(return_value=0)),
                (main, '_collect_special_transition_messages', lambda *a, **k: []),
                (main, '_collect_combined_confirmation_messages', lambda *a, **k: []),
                (main, '_send_magnet_watch_reports', AsyncMock(return_value=0)),
                (main.research_event_runtime, 'capture_special_transitions', lambda *a, **k: None),
                (main, '_persist_watch_runtime', lambda: None),
            ):
                stack.enter_context(patch.object(target, name, value))
            result = await main.run_watch_cycle(bot, 1, top8_only=True, general_enabled=True)
        self.assertTrue(result['ok'], result)
        self.assertEqual(order.count('record_dual'), 1)
        # Research precomputes the pure Combined input subset before capture.
        # The full eight-coin CVD bundle still precedes ordinary output filtering.
        output_filter = max(i for i, stage in enumerate(order) if stage == 'display_filter')
        self.assertLess(order.index('record_dual'), output_filter)
        self.assertLess(order.index('drain_dual'), order.index('ordinary_send'))
        self.assertEqual(len(bundle['coins']), 8)

    async def test_supervisor_recovers_pending_only_for_current_general_watch_destination(self):
        bot = SimpleNamespace(bot=SimpleNamespace(send_message=AsyncMock()))
        with ExitStack() as stack:
            runtime = {'chat_id': 1, 'scan_in_progress': False}
            stack.enter_context(patch.object(main, 'WATCH_RUNTIME', runtime))
            stack.enter_context(patch.object(main, 'WATCH_GENERAL_ENABLED', True))
            stack.enter_context(patch.object(main, 'WATCH_TASK', SimpleNamespace(done=lambda: False)))
            stack.enter_context(patch.object(main, '_watch_consumers_active', return_value=True))
            stack.enter_context(patch.object(main.watch_transition_delivery, 'drain', AsyncMock(return_value=0)))
            stack.enter_context(patch.object(main.manual_formula_alert_delivery, 'run_once', AsyncMock(return_value=0)))
            pending = stack.enter_context(patch.object(main.dual_cvd65_delivery, 'drain', AsyncMock(return_value=0)))
            stack.enter_context(patch.object(main.asyncio, 'sleep', AsyncMock(side_effect=asyncio.CancelledError)))
            with self.assertRaises(asyncio.CancelledError):
                await main._watch_supervisor_loop(bot)
            pending.assert_awaited_once()
            self.assertEqual(pending.call_args.args, (bot.bot, 1))
            authorized = pending.call_args.kwargs['may_deliver']
            self.assertTrue(authorized())
            runtime['chat_id'] = 2
            self.assertFalse(authorized())
            runtime['chat_id'] = 1
            main.WATCH_GENERAL_ENABLED = False
            self.assertFalse(authorized())
        bot.bot.send_message.assert_not_awaited()


if __name__ == '__main__':
    unittest.main()
