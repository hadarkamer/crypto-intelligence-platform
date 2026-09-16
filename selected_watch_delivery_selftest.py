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


if __name__ == '__main__':
    unittest.main()
