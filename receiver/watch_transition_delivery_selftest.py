"""Offline integration: real V1 selection, frozen messages and delivery evidence."""
import asyncio
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from telegram.error import BadRequest, TimedOut
import main
import research_event_capture
import research_event_runtime as runtime
import watch_transition_delivery as delivery
import watch_transition_store as store
from maxpain_cvd_short_delivery_selftest import _item

BASE = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)


class MemoryBoundary:
    """Transaction boundary fake; hysteresis uses the real pure state evaluator."""
    def __init__(self):
        self.state = {}
        self.intents = []

    def record(self, scope, watch, observed, items, factory):
        self.state, crossing = store.evaluate_state(self.state, items)
        created = []
        for descriptor in factory(crossing):
            row = {**deepcopy(descriptor), "intent_id": str(len(self.intents)),
                   "watch_scan_id": watch, "policy_version": store.POLICY_VERSION,
                   "episode": 1, "status": "PENDING", "attempt_token": None,
                   "attempted_at": None, "observed_at": observed.isoformat()}
            self.intents.append(row)
            created.append(row)
        return {"state": deepcopy(self.state), "intents": created,
                "crossing_items": crossing, "resets": [], "counts": {}}

    def claim(self, scope, now, limit):
        assert limit == 1  # Never reserve an entire network-send queue in advance.
        for row in self.intents:
            if row['status'] == 'PENDING':
                row.update(status='IN_FLIGHT', attempt_token='attempt-'+row['intent_id'],
                           attempted_at=now.isoformat())
                return [deepcopy(row)]
        return []

    def complete(self, intent_id, token, **kwargs):
        row = self.intents[int(intent_id)]
        assert row['attempt_token'] == token and row['status'] == 'IN_FLIGHT'
        row.update(kwargs)
        return True


class DeliveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.db = MemoryBoundary()
        self.calls = []
        for target, name, value in (
            (delivery, '_READY', True), (delivery, '_NEXT_INIT', 0),
            (delivery, '_DRAIN_LOCK', asyncio.Lock()),
            (delivery, '_STATUS', deepcopy(delivery._STATUS)),
            (store, 'record_cycle', self.db.record), (store, 'claim_pending', self.db.claim),
            (store, 'complete_attempt', self.db.complete), (store, 'settle_orphans', lambda *a: 0),
            (runtime, 'SINK', research_event_capture.DryRunResearchCapture(max_events=100)),
            (runtime.research_event_store.WRITER, 'enqueue', self.remember),
            (runtime.google_sheets_sync, 'enqueue_delivered_event', lambda *a, **k: True),
            (runtime, 'capture_score65_reset', lambda *a, **k: True),
        ):
            patcher = patch.object(target, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.context = runtime.set_watch_context(watch_scan_id='source-watch', formula_timing={})
        self.addCleanup(runtime.reset_watch_context, self.context)

    def remember(self, event, **kwargs):
        self.calls.append((event, kwargs))
        return True

    async def record(self, items, name='source-watch', when=BASE):
        return await delivery.record_watch(1, items, watch_scan_id=name, decision_time=when,
            render_score65=lambda item: main._score_confirmation_transition_message(item, transition_confirmed=True))

    async def prepare(self, items=None):
        items = [_item()] if items is None else items
        await self.record([{**item, 'score': 59} for item in items], name='baseline', when=BASE-timedelta(minutes=30))
        return await self.record(items)

    async def test_bootstrap_no_alert_then_crossing_freezes_original_formula_order(self):
        await self.record([_item()], name='cold')
        self.assertEqual(self.db.intents, [])
        first, later = _item(timeframe='12h', short=.64), _item(timeframe='48h', short=.99)
        await self.prepare([first, later])
        self.assertEqual([i['kind'] for i in self.db.intents], ['MAX_PAIN_SCORE_65']*2)
        text = self.db.intents[0]['payload']['text']
        first['score'] = 1
        self.assertIn('70.00', text)
        self.assertEqual(self.db.intents[0]['payload']['item']['score'], 70)
        self.assertEqual(main.SCORE_CONFIRMATION_STATE, {})

    async def test_recovery_keeps_original_watch_direction_and_source_time(self):
        await self.prepare()
        token = runtime.set_watch_context(watch_scan_id='later-watch')
        try:
            bot = SimpleNamespace(send_message=AsyncMock())
            sent = await delivery.drain(bot, 1)
            self.assertEqual(sent, 1)
            self.assertEqual(bot.send_message.await_count, 2)
            self.assertEqual([c[0].event_type for c in self.calls], ['MAX_PAIN_SCORE_65','FORMULA_MP65_CVD_SHORT'])
            for event, kwargs in self.calls:
                self.assertEqual(event.direction, 'LONG')
                self.assertEqual(event.engine_snapshot['watch_scan_id'], 'source-watch')
                self.assertIn('watch_transition_intent_id', event.engine_snapshot)
                self.assertEqual(datetime.fromisoformat(event.alert_time_utc.replace('Z','+00:00')), BASE)
                self.assertEqual(kwargs['delivery_status'], 'DELIVERED')
                self.assertIsNotNone(kwargs['delivered_at_utc'])
            self.assertEqual(runtime.watch_context_snapshot()['watch_scan_id'], 'later-watch')
            await delivery.drain(bot, 1)
            self.assertEqual(bot.send_message.await_count, 2)
        finally:
            runtime.reset_watch_context(token)

    async def test_timeout_and_rejection_continue_and_never_retry(self):
        await self.prepare()
        bot = SimpleNamespace(send_message=AsyncMock(side_effect=[TimedOut(), BadRequest('fixture')]))
        self.assertEqual(await delivery.drain(bot, 1), 0)
        self.assertEqual([i['status'] for i in self.db.intents], ['UNKNOWN','FAILED'])
        self.assertEqual([k['delivery_status'] for _, k in self.calls], ['UNKNOWN','DELIVERY_FAILED'])
        self.assertTrue(all(k['delivered_at_utc'] is None for _, k in self.calls))
        await delivery.drain(bot, 1)
        self.assertEqual(bot.send_message.await_count, 2)

    async def test_ack_storage_failure_keeps_real_ack_and_never_reclaims_attempt(self):
        await self.prepare()
        bot = SimpleNamespace(send_message=AsyncMock())
        with patch.object(store, 'complete_attempt', side_effect=RuntimeError('fixture DB failure')):
            await delivery.drain(bot, 1)
        self.assertEqual(self.db.intents[0]['status'], 'IN_FLIGHT')
        self.assertEqual(self.calls[0][1]['delivery_status'], 'DELIVERED')
        self.assertFalse(delivery.status()['ready'])
        # Simulate healthy restart. Only the second, never-attempted intent recovers.
        delivery._READY = True
        await delivery.drain(bot, 1)
        self.assertEqual(bot.send_message.await_count, 2)
        self.assertEqual(self.db.intents[0]['status'], 'IN_FLIGHT')

    async def test_cancellation_preserves_uncertain_attempt(self):
        await self.prepare()
        bot = SimpleNamespace(send_message=AsyncMock(side_effect=asyncio.CancelledError))
        with self.assertRaises(asyncio.CancelledError):
            await delivery.drain(bot, 1)
        self.assertEqual(self.db.intents[0]['status'], 'IN_FLIGHT')
        self.assertEqual(self.calls, [])

    async def test_stopped_subscription_never_claims_or_sends(self):
        await self.prepare()
        bot = SimpleNamespace(send_message=AsyncMock())
        await delivery.drain(bot, 1, may_deliver=lambda: False)
        bot.send_message.assert_not_awaited()
        self.assertTrue(all(i['status']=='PENDING' for i in self.db.intents))

    async def test_stop_while_claiming_releases_before_any_network_attempt(self):
        await self.prepare()
        active = [True]
        def claim(*args, **kwargs):
            claimed = self.db.claim(*args, **kwargs)
            active[0] = False
            return claimed
        def release(intent_id, token):
            row = self.db.intents[int(intent_id)]
            self.assertEqual(row['attempt_token'], token)
            row.update(status='PENDING', attempt_token=None, attempted_at=None)
            return True
        bot = SimpleNamespace(send_message=AsyncMock())
        with patch.object(store, 'claim_pending', claim), \
             patch.object(store, 'release_unattempted', release):
            await delivery.drain(bot, 1, may_deliver=lambda: active[0])
        bot.send_message.assert_not_awaited()
        self.assertTrue(all(i['status']=='PENDING' for i in self.db.intents))

    async def test_true_reset_capture_uses_real_card_without_sending(self):
        item = {**_item(), 'score': 59}
        with patch.object(store, 'record_cycle', return_value={
            'resets':[item], 'intents':[], 'counts':{'resets':1},
        }), patch.object(runtime, 'capture_score65_reset', wraps=runtime.capture_score65_reset) as reset:
            await self.record([item])
        reset.assert_called_once_with(item, event_time=BASE, persist=True)

    async def test_capture_failure_does_not_change_success_or_block_next_message(self):
        await self.prepare()
        bot = SimpleNamespace(send_message=AsyncMock())
        with patch.object(delivery, '_capture', side_effect=ValueError('fixture capture')):
            self.assertEqual(await delivery.drain(bot, 1), 1)
        self.assertTrue(all(i['status']=='DELIVERED' for i in self.db.intents))
        self.assertEqual(bot.send_message.await_count, 2)

    async def test_live_watch_database_failure_still_delivers_regular_watch(self):
        item = {**_item(), 'distance_pct': 1}
        async def collect(**kwargs):
            return [], {'watch_derivatives_snapshot': {}, 'watch_prepared_items': [item]}
        bot = SimpleNamespace(bot=SimpleNamespace(send_message=AsyncMock()))
        with patch.object(main, 'WATCH_GENERAL_ENABLED', True), \
             patch.object(main, 'WATCH_RUNTIME', {'chat_id':1}), \
             patch.object(main, '_get_scrape_lock', return_value=asyncio.Lock()), \
             patch.object(main, 'collect_live_rows_for_watch', collect), \
             patch.object(main, '_ensure_watch_derivatives_ready', AsyncMock(return_value={})), \
             patch.object(main.dual_cvd65_delivery, 'record_watch', AsyncMock()), \
             patch.object(main.dual_cvd65_delivery, 'drain', AsyncMock(return_value=0)), \
             patch.object(main, '_send_magnet_watch_reports', AsyncMock(return_value=0)), \
             patch.object(main, '_collect_combined_confirmation_messages', return_value=[]), \
             patch.object(main, '_collect_special_transition_messages', return_value=[]) as special, \
             patch.object(main, '_alert_card', return_value='ordinary card'), \
             patch.object(main, '_send_alert_with_confirmation', AsyncMock()) as ordinary, \
             patch.object(main, '_persist_watch_runtime'), \
             patch.object(runtime, 'capture_special_transitions') as research, \
             patch.object(store, 'record_cycle', side_effect=RuntimeError('fixture DB offline')):
            result = await main.run_watch_cycle(bot, 1, general_enabled=True)
        self.assertTrue(result['ok'])
        ordinary.assert_awaited_once()
        self.assertFalse(special.call_args.kwargs['include_score65'])
        self.assertFalse(research.call_args.kwargs['include_score65'])
        self.assertEqual(self.db.intents, [])
        self.assertEqual(delivery.status()['last_record_status'], 'EVIDENCE_GAP')
        self.assertEqual(result['transition_result']['status'], 'INCOMPLETE')


if __name__ == '__main__':
    unittest.main()
