"""Offline selected-delivery checks through the real Watch entry points."""
import asyncio
from datetime import datetime, timezone
import os
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

import research_watch_decision_capture_watch_selftest as shared_fixture
import watch_transition_watch_path_selftest as watch_fixture


class SelectedWatchTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        env = patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': 'SELECTED_EXPERIMENTAL_ONLY'})
        env.start()
        self.addCleanup(env.stop)

    async def test_whole_watch_collects_and_previews_selected_rules_without_ordinary_sends(self):
        for precompute_error in (False, True):
            with self.subTest(precompute_error=precompute_error):
                fixture = shared_fixture.SharedWatchCaptureTests()
                scope, result, order, archives, captures, bot, bundle = await fixture.run_cycle(
                    precompute_error=precompute_error,
                )
                self.assertTrue(result['ok'], result)
                self.assertTrue(archives)
                self.assertIn('archive', order)
                self.assertIn('preview_manual_sources', order)
                self.assertEqual(scope['manual_formula_alert_delivery'].run_watch.await_count, 2)
                direct, planned = scope['manual_formula_alert_delivery'].run_watch.await_args_list
                self.assertIs(direct.kwargs['c1274_bundle'], bundle)
                self.assertTrue(planned.args[2])
                self.assertTrue(direct.kwargs['may_deliver']())
                self.assertTrue(planned.kwargs['may_deliver']())
                bot.bot.send_message.assert_not_awaited()
                scope['_send_alert_with_confirmation'].assert_not_awaited()
                scope['_send_magnet_watch_reports'].assert_not_awaited()
                scope['dual_cvd65_delivery'].record_watch.assert_not_awaited()
                scope['dual_cvd65_delivery'].drain.assert_not_awaited()
                scope['watch_transition_delivery'].record_watch.assert_not_awaited()
                scope['watch_transition_delivery'].drain.assert_not_awaited()
                scope['research_ordered_experimental_worker'].WORKER.drain_for_watch.assert_not_awaited()
                self.assertEqual((result['sent'], result['combined_sent'], result['magnet_sent']), (0, 0, 0))
                self.assertEqual(scope['WATCH_RUNTIME']['last_sent'], 0)
                self.assertFalse(scope['WATCH_RUNTIME']['scan_in_progress'])

    async def test_supervisor_keeps_manual_recovery_and_suppresses_other_drains(self):
        scope, transition, ensure, bot = await watch_fixture.SupervisorRecoveryTests().one_pass()
        scope['manual_formula_alert_delivery'].run_once.assert_awaited_once()
        callback = scope['manual_formula_alert_delivery'].run_once.await_args.kwargs['may_deliver']
        self.assertTrue(callback())
        scope['dual_cvd65_delivery'].drain.assert_not_awaited()
        transition.assert_not_awaited()

    async def test_restored_ordinary_watch_keeps_only_selected_experimental_paths(self):
        with patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': 'ORDINARY_AND_SELECTED_EXPERIMENTAL'}):
            fixture = shared_fixture.SharedWatchCaptureTests()
            scope, result, order, archives, captures, bot, bundle = await fixture.run_cycle()
            self.assertTrue(result['ok'], result)
            self.assertTrue(archives)
            self.assertIn('archive', order)
            self.assertIn('preview_manual_sources', order)
            self.assertIn('send', order)
            self.assertGreater(bot.bot.send_message.await_count, 0)
            self.assertGreater(scope['_send_alert_with_confirmation'].await_count, 0)
            scope['_send_magnet_watch_reports'].assert_awaited_once()
            scope['watch_transition_delivery'].record_watch.assert_awaited_once()
            transition = scope['watch_transition_delivery'].drain
            transition.assert_awaited_once()
            self.assertEqual(transition.await_args.kwargs['kinds'], ('MAX_PAIN_SCORE_65',))
            self.assertTrue(transition.await_args.kwargs['may_deliver']())
            self.assertGreater(result['sent'], 0)
            self.assertGreater(result['combined_sent'], 0)
            self.assertEqual(scope['WATCH_RUNTIME']['last_formula_sent'], 0)
            scope['research_event_runtime'].capture_special_transitions.assert_called_once()
            scope['research_event_runtime'].capture_combined_confirmation.assert_called_once()
            self.assertEqual(scope['manual_formula_alert_delivery'].run_watch.await_count, 2)
            direct, planned = scope['manual_formula_alert_delivery'].run_watch.await_args_list
            self.assertIs(direct.kwargs['c1274_bundle'], bundle)
            self.assertTrue(planned.args[2])
            self.assertTrue(direct.kwargs['may_deliver']())
            self.assertTrue(planned.kwargs['may_deliver']())
            scope['dual_cvd65_delivery'].record_watch.assert_not_awaited()
            scope['dual_cvd65_delivery'].drain.assert_not_awaited()
            scope['research_ordered_experimental_worker'].WORKER.drain_for_watch.assert_not_awaited()

    async def test_restored_supervisor_recovers_ordinary_and_manual_but_not_other_experiments(self):
        with patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': 'ORDINARY_AND_SELECTED_EXPERIMENTAL'}):
            scope, transition, ensure, bot = await watch_fixture.SupervisorRecoveryTests().one_pass()
            scope['manual_formula_alert_delivery'].run_once.assert_awaited_once()
            self.assertTrue(scope['manual_formula_alert_delivery'].run_once.await_args.kwargs['may_deliver']())
            scope['dual_cvd65_delivery'].drain.assert_not_awaited()
            transition.assert_awaited_once()
            self.assertEqual(transition.await_args.kwargs['kinds'], ('MAX_PAIN_SCORE_65',))
            self.assertTrue(transition.await_args.kwargs['may_deliver']())

    async def test_specific_watch_keeps_collection_but_sends_no_automatic_messages(self):
        rows = [{'symbol': 'DOGE', 'current_price': .1}]
        collect = AsyncMock(return_value=(rows, {}))
        card = AsyncMock()
        watches = {'DOGE': {'target_price': .09, 'start_price': .11}}
        scope = watch_fixture.load_main({'run_specific_watch_cycle'}, {
            'asyncio': asyncio, 'datetime': datetime, 'timezone': timezone,
            'SPECIFIC_WATCHES': watches, 'WATCH_RUNTIME': {},
            '_get_scrape_lock': lambda: asyncio.Lock(),
            'collect_live_rows_for_watch': collect,
            '_build_opportunities_with_regime': Mock(return_value=[]),
            'alert_engine': SimpleNamespace(TIMEFRAMES=['12h']),
            '_find_symbol_current_price': Mock(return_value=.1),
            '_send_alert_with_confirmation': card,
            '_specific_target_reached': Mock(return_value=False),
        })
        bot = SimpleNamespace(bot=SimpleNamespace(send_message=AsyncMock()))
        await scope['run_specific_watch_cycle'](bot, 1)
        collect.assert_awaited_once()
        bot.bot.send_message.assert_not_awaited()
        card.assert_not_awaited()
        self.assertEqual(watches['DOGE']['last_price'], .1)

    async def test_automatic_card_is_silent_and_manual_reply_still_works(self):
        scope, capture = watch_fixture.CardPathTests().card_scope()
        bot = SimpleNamespace(send_message=AsyncMock())
        await scope['_send_alert_with_confirmation'](bot, 1, 'automatic card', {})
        bot.send_message.assert_not_awaited()
        capture.capture_sent_maxpain.assert_not_called()
        capture.capture_special_transitions.assert_not_called()

        manual_capture = Mock()
        scope = watch_fixture.load_main({'_reply_alert_with_archive'}, {
            'datetime': datetime, 'timezone': timezone,
            'research_event_runtime': SimpleNamespace(capture_manual_maxpain_sample=manual_capture),
            '_special_transition_messages': lambda item: [],
        })
        update = SimpleNamespace(message=SimpleNamespace(reply_text=AsyncMock()))
        await scope['_reply_alert_with_archive'](update, 'requested card', {})
        update.message.reply_text.assert_awaited_once_with('requested card', parse_mode='HTML')
        manual_capture.assert_called_once()


class OrdinaryWatchFailureIsolationTests(unittest.IsolatedAsyncioTestCase):
    """A local send failure must not discard the rest of a prepared Watch."""

    async def exercise(self, stage=None, *, failure=None, empty=False, slow=False, magnet_failure=False):
        from telegram.error import TimedOut
        original_load = shared_fixture.load_main
        attempts, heartbeats = [], []
        observed = {}
        failure = failure or TimedOut()

        def load(names, scope, **kwargs):
            result = original_load(names, scope, **kwargs)
            if names == {'run_watch_cycle'}:
                scope['research_event_runtime'].capture_sent_maxpain = Mock()
                original_load({'_send_alert_with_confirmation'}, scope)
                scope['_collect_special_transition_messages'] = lambda *args, **kwargs: ['special 1', 'special 2']
                summary = Mock(return_value='count')
                scope['alert_summary'].format_alert_count_summary = summary
                observed['summary'] = summary
                if magnet_failure:
                    async def magnet(*args, **kwargs):
                        scope['WATCH_RUNTIME']['last_delivery_errors'].append({
                            'symbol': 'SOL', 'error_type': 'TimedOut', 'delivery_status': 'UNKNOWN'})
                        return 2
                    scope['_send_magnet_watch_reports'] = AsyncMock(side_effect=magnet)
                native = scope['run_watch_cycle']

                async def invoke(bot, *args, **kwargs):
                    async def send(**payload):
                        text = payload['text']
                        kind = ('header' if not attempts else
                                'card' if text == 'ordinary card' else
                                'summary' if text == 'count' else
                                text if text.startswith('special ') else
                                'failure_summary' if text.startswith('⚠️') else
                                'header' if text.startswith('✅') else 'combined')
                        attempts.append(kind)
                        if kind == stage:
                            raise failure
                        if slow and kind == 'header':
                            task = asyncio.create_task(asyncio.sleep(0))
                            await asyncio.sleep(.02)
                            heartbeats.append(task.done())
                            await task
                        return SimpleNamespace(message_id=len(attempts))
                    bot.bot.send_message.side_effect = send
                    return await native(bot, *args, **kwargs)
                scope['run_watch_cycle'] = invoke
            return result

        with patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': 'ORDINARY_AND_SELECTED_EXPERIMENTAL'}), \
             patch.object(shared_fixture, 'load_main', load):
            result = await shared_fixture.SharedWatchCaptureTests().run_cycle(empty=empty)
        return result, attempts, observed, heartbeats

    async def test_each_failed_message_keeps_independent_deliveries_and_precise_counts(self):
        for stage in ('header', 'card', 'summary', 'special 1', 'combined'):
            with self.subTest(stage=stage):
                (scope, result, order, archives, captures, bot, bundle), attempts, observed, _ = await self.exercise(stage)
                self.assertTrue(result['ok'], result)
                self.assertEqual(scope['WATCH_RUNTIME']['last_cycle_status'], 'completed_with_delivery_errors')
                self.assertEqual(len(result['delivery_errors']), 1)
                self.assertEqual(result['delivery_errors'][0]['delivery_status'], 'UNKNOWN')
                self.assertEqual(attempts.count(stage), 1)
                self.assertIn('special 2', attempts)
                self.assertIn('combined', attempts)
                scope['_send_magnet_watch_reports'].assert_awaited_once()
                scope['watch_transition_delivery'].drain.assert_awaited_once()
                self.assertEqual(len(archives), 1)
                self.assertEqual(result['sent'], int(stage != 'card'))
                self.assertEqual(scope['WATCH_RUNTIME']['last_sent'], result['sent'])
                self.assertEqual(result['combined_sent'], int(stage != 'combined'))
                if stage == 'card':
                    observed['summary'].assert_not_called()
                    self.assertEqual(scope['research_event_runtime'].capture_sent_maxpain.call_args.kwargs['delivery_status'], 'UNKNOWN')
                if stage == 'special 1':
                    capture = scope['research_event_runtime'].capture_special_transitions.call_args.kwargs
                    self.assertEqual(capture['delivery_status'], 'UNKNOWN')
                    self.assertIsNone(capture['delivered_at_utc'])
                if stage == 'combined':
                    capture = scope['research_event_runtime'].capture_combined_confirmation.call_args.kwargs
                    self.assertEqual(capture['delivery_status'], 'UNKNOWN')
                    self.assertNotIn('delivered_at_utc', capture)

    async def test_definite_rejection_keeps_failed_status_without_retry(self):
        from telegram.error import BadRequest
        (scope, result, *_), attempts, _, _ = await self.exercise('card', failure=BadRequest('controlled rejection'))
        self.assertTrue(result['ok'])
        self.assertEqual(attempts.count('card'), 1)
        self.assertEqual(result['delivery_errors'][0]['delivery_status'], 'DELIVERY_FAILED')
        self.assertEqual(scope['research_event_runtime'].capture_sent_maxpain.call_args.kwargs['delivery_status'], 'DELIVERY_FAILED')

    async def test_lost_connection_response_remains_unknown_at_every_capture(self):
        from telegram.error import NetworkError
        for stage, capture_name in (
            ('card', 'capture_sent_maxpain'),
            ('special 1', 'capture_special_transitions'),
            ('combined', 'capture_combined_confirmation'),
        ):
            with self.subTest(stage=stage):
                (scope, result, *_), attempts, _, _ = await self.exercise(
                    stage, failure=NetworkError('controlled lost response'))
                self.assertTrue(result['ok'])
                self.assertEqual(attempts.count(stage), 1)
                self.assertEqual(result['delivery_errors'][0]['delivery_status'], 'UNKNOWN')
                capture = getattr(scope['research_event_runtime'], capture_name).call_args.kwargs
                self.assertEqual(capture['delivery_status'], 'UNKNOWN')
                self.assertIsNone(capture.get('delivered_at_utc'))

    async def test_mixed_special_batch_does_not_mislabel_individual_events(self):
        from telegram.error import BadRequest
        (scope, result, *_), attempts, _, _ = await self.exercise(
            'special 1', failure=BadRequest('controlled rejection'))
        self.assertTrue(result['ok'])
        self.assertIn('special 2', attempts)
        self.assertEqual(result['delivery_errors'][0]['delivery_status'], 'DELIVERY_FAILED')
        capture = scope['research_event_runtime'].capture_special_transitions.call_args.kwargs
        self.assertEqual(capture['delivery_status'], 'UNKNOWN')
        self.assertIsNone(capture['delivered_at_utc'])

    async def test_legacy_special_abort_preserves_partial_batch_uncertainty(self):
        from telegram.error import BadRequest
        for messages, failed, expected in (
            (['special 1'], 'special 1', 'DELIVERY_FAILED'),
            (['special 1', 'special 2'], 'special 1', 'UNKNOWN'),
            (['special 1', 'special 2'], 'special 2', 'UNKNOWN'),
        ):
            with self.subTest(messages=messages, failed=failed), \
                 patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': 'ORDINARY_AND_SELECTED_EXPERIMENTAL'}):
                scope, capture = watch_fixture.CardPathTests().card_scope()
                scope['_special_transition_messages'] = lambda item: messages
                async def send(**payload):
                    if payload['text'] == failed:
                        raise BadRequest('controlled rejection')
                    return SimpleNamespace(message_id=1)
                bot = SimpleNamespace(send_message=AsyncMock(side_effect=send))
                with self.assertRaises(BadRequest):
                    await scope['_send_alert_with_confirmation'](bot, 1, 'card', {})
                self.assertEqual(capture.capture_sent_maxpain.call_args.kwargs['delivery_status'], 'DELIVERED')
                self.assertEqual(capture.capture_special_transitions.call_args.kwargs['delivery_status'], expected)
                self.assertEqual(bot.send_message.await_count, 2 + int(failed == 'special 2'))

    async def test_slow_send_yields_event_loop_and_preserves_decision_time(self):
        (scope, result, *_), attempts, _, heartbeats = await self.exercise(slow=True)
        self.assertTrue(result['ok'])
        self.assertEqual(heartbeats, [True])
        self.assertEqual(result['delivery_errors'], [])
        capture = scope['research_event_runtime'].capture_sent_maxpain.call_args.kwargs
        self.assertLessEqual(capture['event_time'], capture['delivery_attempted_at_utc'])
        self.assertLessEqual(capture['delivery_attempted_at_utc'], capture['delivered_at_utc'])

    async def test_no_trade_signal_does_not_disable_ordinary_watch(self):
        (scope, result, order, archives, *_), attempts, _, _ = await self.exercise(empty=True)
        self.assertTrue(result['ok'])
        self.assertEqual(result['sent'], 0)
        self.assertIn('header', attempts)
        self.assertEqual(len(archives), 1)
        scope['_send_magnet_watch_reports'].assert_awaited_once()
        scope['watch_transition_delivery'].drain.assert_awaited_once()

    async def test_shutdown_cancellation_is_not_swallowed(self):
        with self.assertRaises(asyncio.CancelledError):
            await self.exercise('header', failure=asyncio.CancelledError())

    async def test_ordinary_and_magnet_failures_preserve_both_results(self):
        (scope, result, order, archives, captures, bot, bundle), attempts, _, _ = await self.exercise(
            'header', magnet_failure=True)
        self.assertTrue(result['ok'])
        self.assertEqual(result['magnet_sent'], 2)
        self.assertEqual([error['symbol'] for error in result['delivery_errors']], ['ordinary_header', 'SOL'])
        summary = bot.bot.send_message.call_args.kwargs['text']
        self.assertIn('כותרת המחזור', summary)
        self.assertIn('SOL', summary)
        self.assertNotIn('ordinary_header', summary)


if __name__ == '__main__':
    unittest.main()
