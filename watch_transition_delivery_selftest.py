"""Offline integration: real V1 selection, frozen messages and delivery evidence."""
import asyncio
import os
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

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

    def claim(self, scope, now, limit, *, kinds=None):
        assert limit == 1  # Never reserve an entire network-send queue in advance.
        for row in self.intents:
            if row['status'] == 'PENDING' and (kinds is None or row['kind'] in kinds):
                row.update(status='IN_FLIGHT', attempt_token='attempt-'+row['intent_id'],
                           attempted_at=now.isoformat())
                return [deepcopy(row)]
        return []

    def complete(self, intent_id, token, **kwargs):
        row = self.intents[int(intent_id)]
        assert row['attempt_token'] == token and row['status'] == 'IN_FLIGHT'
        row.update(kwargs)
        return True

    def release(self, intent_id, token):
        row = self.intents[int(intent_id)]
        assert row['attempt_token'] == token and row['status'] == 'IN_FLIGHT'
        row.update(status='PENDING', attempt_token=None, attempted_at=None)
        return True


class DeliveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        profile = patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': 'ALL'})
        profile.start()
        self.addCleanup(profile.stop)
        self.db = MemoryBoundary()
        self.calls = []
        for target, name, value in (
            (delivery, '_READY', True), (delivery, '_NEXT_INIT', 0),
            (delivery, '_DRAIN_LOCK', asyncio.Lock()),
            (delivery, '_STATUS', deepcopy(delivery._STATUS)),
            (store, 'record_cycle', self.db.record), (store, 'claim_pending', self.db.claim),
            (store, 'complete_attempt', self.db.complete), (store, 'settle_orphans', lambda *a: 0),
            (store, 'release_unattempted', self.db.release),
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

    async def test_selected_only_profile_blocks_existing_pending_outbox(self):
        await self.prepare()
        before = deepcopy(self.db.intents)
        self.assertTrue(before)
        bot = SimpleNamespace(send_message=AsyncMock())
        with patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': 'SELECTED_EXPERIMENTAL_ONLY'}), \
             patch.object(store, 'claim_pending') as claim:
            self.assertEqual(await delivery.drain(bot, 1, may_deliver=lambda: True), 0)
            self.assertFalse(delivery.status()['delivery_allowed_by_profile'])
        claim.assert_not_called()
        bot.send_message.assert_not_awaited()
        self.assertEqual(self.db.intents, before)

    async def test_profile_change_during_claim_releases_without_sending(self):
        await self.prepare()
        allowed = [True]
        def claim(*args, **kwargs):
            rows = self.db.claim(*args, **kwargs)
            allowed[0] = False
            return rows
        def release(intent_id, token):
            row = self.db.intents[int(intent_id)]
            self.assertEqual(row['attempt_token'], token)
            row.update(status='PENDING', attempt_token=None, attempted_at=None)
            return True
        bot = SimpleNamespace(send_message=AsyncMock())
        with patch.object(delivery.delivery_policy, 'ordinary_alerts_enabled', side_effect=lambda: allowed[0]), \
             patch.object(delivery.delivery_policy, 'other_experimental_alerts_enabled', side_effect=lambda: allowed[0]), \
             patch.object(store, 'claim_pending', claim), \
             patch.object(store, 'release_unattempted', release) as released:
            self.assertEqual(await delivery.drain(bot, 1, may_deliver=lambda: True), 0)
        bot.send_message.assert_not_awaited()
        self.assertTrue(all(row['status'] == 'PENDING' for row in self.db.intents))

    async def test_restored_ordinary_profile_records_crossings_without_formula_intents(self):
        with patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': 'ORDINARY_AND_SELECTED_EXPERIMENTAL'}):
            await self.prepare()
            await self.record([_item()], name='same-high-score', when=BASE+timedelta(minutes=30))
            self.assertEqual([row['kind'] for row in self.db.intents], ['MAX_PAIN_SCORE_65'])
            await self.record([{**_item(), 'score': 59}], name='reset', when=BASE+timedelta(minutes=60))
            await self.record([_item()], name='second-crossing', when=BASE+timedelta(minutes=90))
            self.assertEqual([row['kind'] for row in self.db.intents], ['MAX_PAIN_SCORE_65']*2)
            self.assertTrue(delivery.status()['ordinary_delivery_allowed_by_profile'])
            self.assertFalse(delivery.status()['formula_delivery_allowed_by_profile'])
            bot = SimpleNamespace(send_message=AsyncMock())
            self.assertEqual(await delivery.drain(bot, 1), 0)
            self.assertEqual(bot.send_message.await_count, 2)
            self.assertEqual([event.event_type for event, _ in self.calls], ['MAX_PAIN_SCORE_65']*2)

    async def test_old_formula_pending_does_not_starve_new_ordinary_crossing(self):
        await self.prepare()
        bot = SimpleNamespace(send_message=AsyncMock())
        await delivery.drain(bot, 1, kinds=('MAX_PAIN_SCORE_65',))
        bot.send_message.reset_mock()
        self.calls.clear()
        with patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': 'ORDINARY_AND_SELECTED_EXPERIMENTAL'}):
            await self.prepare([_item(symbol='ETH')])
            with patch.object(store, 'claim_pending', wraps=self.db.claim) as claim:
                self.assertEqual(await delivery.drain(bot, 1, limit=1), 0)
            self.assertEqual(claim.call_args.kwargs['kinds'], ('MAX_PAIN_SCORE_65',))
        self.assertEqual(bot.send_message.await_count, 1)
        self.assertEqual([row['status'] for row in self.db.intents], ['DELIVERED', 'PENDING', 'DELIVERED'])
        self.assertEqual([event.event_type for event, _ in self.calls], ['MAX_PAIN_SCORE_65'])

    async def test_requested_kinds_are_intersected_and_empty_never_claims_all(self):
        await self.prepare()
        bot = SimpleNamespace(send_message=AsyncMock())
        with patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': 'ORDINARY_AND_SELECTED_EXPERIMENTAL'}):
            for kinds in ((), [], ('FORMULA_MP65_CVD_SHORT',), ('UNKNOWN_KIND',)):
                with self.subTest(kinds=kinds), patch.object(store, 'claim_pending') as claim:
                    self.assertEqual(await delivery.drain(bot, 1, kinds=kinds), 0)
                    claim.assert_not_called()
                    bot.send_message.assert_not_awaited()
            with patch.object(store, 'claim_pending', wraps=self.db.claim) as claim:
                self.assertEqual(await delivery.drain(bot, 1,
                    kinds=('FORMULA_MP65_CVD_SHORT', 'MAX_PAIN_SCORE_65')), 0)
                self.assertTrue(all(call.kwargs['kinds'] == ('MAX_PAIN_SCORE_65',)
                                    for call in claim.call_args_list))
        self.assertEqual([row['status'] for row in self.db.intents], ['DELIVERED', 'PENDING'])
        self.assertEqual(bot.send_message.await_count, 1)

    async def test_formula_disabled_during_claim_releases_and_continues_ordinary_queue(self):
        await self.prepare()
        claims = []
        def claim(scope, now, limit, *, kinds):
            claims.append(kinds)
            if len(claims) == 1:
                rows = self.db.claim(scope, now, limit, kinds=('FORMULA_MP65_CVD_SHORT',))
                os.environ['ALERT_DELIVERY_PROFILE'] = 'ORDINARY_AND_SELECTED_EXPERIMENTAL'
                return rows
            return self.db.claim(scope, now, limit, kinds=kinds)
        bot = SimpleNamespace(send_message=AsyncMock())
        with patch.object(store, 'claim_pending', claim), \
             patch.object(store, 'release_unattempted', wraps=self.db.release) as release:
            self.assertEqual(await delivery.drain(bot, 1), 0)
        release.assert_called_once_with('1', 'attempt-1')
        self.assertIn('FORMULA_MP65_CVD_SHORT', claims[0])
        self.assertTrue(all(kinds == ('MAX_PAIN_SCORE_65',) for kinds in claims[1:]))
        self.assertEqual(bot.send_message.await_count, 1)
        self.assertEqual([row['status'] for row in self.db.intents], ['DELIVERED', 'PENDING'])
        self.assertIsNone(self.db.intents[1]['attempt_token'])
        self.assertEqual([event.event_type for event, _ in self.calls], ['MAX_PAIN_SCORE_65'])

    async def test_restored_profile_keeps_reset_capture_without_sending(self):
        item = {**_item(), 'score': 59}
        with patch.dict(os.environ, {'ALERT_DELIVERY_PROFILE': 'ORDINARY_AND_SELECTED_EXPERIMENTAL'}), \
             patch.object(store, 'record_cycle', return_value={
                 'resets': [item], 'intents': [], 'counts': {'resets': 1}}), \
             patch.object(runtime, 'capture_score65_reset') as reset:
            await self.record([item])
        reset.assert_called_once_with(item, event_time=BASE, persist=True)
        self.assertEqual(self.db.intents, [])

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
             patch.object(main.manual_formula_alert_delivery, 'run_watch', AsyncMock(return_value=0)), \
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

    async def test_experimental_kind_can_precede_unattempted_ordinary_mp65(self):
        await self.prepare()
        bot = SimpleNamespace(send_message=AsyncMock())
        self.assertEqual(await delivery.drain(bot, 1, kinds=('FORMULA_MP65_CVD_SHORT',)), 1)
        self.assertEqual([row['status'] for row in self.db.intents], ['PENDING', 'DELIVERED'])
        self.assertEqual([event.event_type for event, _ in self.calls], ['FORMULA_MP65_CVD_SHORT'])
        self.assertEqual(await delivery.drain(bot, 1, kinds=('MAX_PAIN_SCORE_65',)), 0)
        self.assertEqual([row['status'] for row in self.db.intents], ['DELIVERED', 'DELIVERED'])
        self.assertEqual(bot.send_message.await_count, 2)

    async def test_watch_waits_for_prior_supervisor_before_sending_formula(self):
        await self.prepare()
        entered, release = asyncio.Event(), asyncio.Event()
        scan_started = [False]
        async def send(**kwargs):
            entered.set()
            await release.wait()
        bot = SimpleNamespace(send_message=AsyncMock(side_effect=send))
        supervisor = asyncio.create_task(delivery.drain(bot, 1, may_deliver=lambda: not scan_started[0]))
        watch = None
        try:
            await asyncio.wait_for(entered.wait(), timeout=2)
            scan_started[0] = True
            watch = asyncio.create_task(delivery.drain(
                bot, 1, kinds=('FORMULA_MP65_CVD_SHORT',), wait_for_lock=True,
                may_deliver=lambda: True,
            ))
            await asyncio.sleep(0)
            self.assertFalse(watch.done())
            release.set()
            self.assertEqual(await asyncio.wait_for(supervisor, timeout=2), 0)
            self.assertEqual(await asyncio.wait_for(watch, timeout=2), 1)
            self.assertEqual([row['status'] for row in self.db.intents], ['DELIVERED', 'DELIVERED'])
            self.assertEqual(bot.send_message.await_count, 2)
        finally:
            release.set()
            await asyncio.gather(supervisor, *([watch] if watch is not None else []), return_exceptions=True)

    async def test_real_watch_sends_all_experiments_before_first_ordinary_message(self):
        item = {**_item(), 'distance_pct': 1}
        await self.record([{**item, 'score': 59}], name='baseline', when=BASE)
        output = []
        async def collect(**kwargs):
            return [], {'watch_derivatives_snapshot': {}, 'watch_prepared_items': [item]}
        async def send(**kwargs):
            output.append(kwargs['text'])
            return SimpleNamespace(message_id=len(output))
        async def dual(bot, chat, **kwargs):
            await bot.send_message(chat_id=chat, text='experimental dual')
            return 1
        async def manual(bot, chat, sources, **kwargs):
            self.assertTrue(sources)
            self.assertTrue(all(row['delivery_status'] == 'NOT_ATTEMPTED' for row in sources))
            self.assertTrue(all(row['event_id'].startswith('watch:') for row in sources))
            self.assertFalse(self.calls, 'Preview cannot claim a native delivery before transport')
            for label in ('experimental manual A', 'experimental manual B'):
                await bot.send_message(chat_id=chat, text=label)
            return 2
        bot = SimpleNamespace(bot=SimpleNamespace(send_message=AsyncMock(side_effect=send)))
        with patch.object(main, 'WATCH_GENERAL_ENABLED', True), \
             patch.object(main, 'WATCH_RUNTIME', {'chat_id': 1}), \
             patch.object(main, 'MAGNET_V1_WATCHES', {}), \
             patch.object(main, '_get_scrape_lock', return_value=asyncio.Lock()), \
             patch.object(main, 'collect_live_rows_for_watch', collect), \
             patch.object(main, '_ensure_watch_derivatives_ready', AsyncMock(return_value={})), \
             patch.object(main.dual_cvd65_delivery, 'record_watch', AsyncMock()), \
             patch.object(main.dual_cvd65_delivery, 'drain', AsyncMock(side_effect=dual)), \
             patch.object(main.manual_formula_alert_delivery, 'run_watch', AsyncMock(side_effect=manual)), \
             patch.object(main, '_send_magnet_watch_reports', AsyncMock(return_value=0)), \
             patch.object(main, '_collect_combined_confirmation_messages', return_value=[]), \
             patch.object(main, '_collect_special_transition_messages', return_value=[]), \
             patch.object(main, '_alert_card', return_value='ordinary card'), \
             patch.object(main, '_persist_watch_runtime'):
            result = await main.run_watch_cycle(bot, 1, general_enabled=True)
        self.assertTrue(result['ok'], result)
        self.assertEqual(output[:3], ['experimental dual', 'experimental manual A', 'experimental manual B'])
        self.assertIn('ניסיוני', output[3])
        self.assertIn('סריקת', output[4])
        self.assertEqual(output[5], 'ordinary card')
        self.assertEqual(self.calls[0][0].event_type, 'FORMULA_MP65_CVD_SHORT')


class PlannedCaptureTests(unittest.TestCase):
    def test_preview_does_not_consume_transitions_or_write_delivered_evidence(self):
        token = runtime.set_watch_context(watch_scan_id='preview-watch')
        states = (runtime._CONFIRMATION_STATE, runtime._SCORE_CONFIRMATION_STATE,
                  runtime._HIGH_SCORE_83_STATE, runtime._DERIVATIVES_HIGH_STATE,
                  runtime._SPOT_FAMILY_HIGH_STATE, runtime._MAGNET_STATE, runtime._COMBINED_STATE)
        originals = [deepcopy(state) for state in states]
        def capture():
            runtime.capture_sent_maxpain(_item(), event_time=BASE, persist=False,
                                         delivery_status='NOT_ATTEMPTED')
            runtime.capture_special_transitions([_item()], event_time=BASE, persist=False,
                                                delivery_status='NOT_ATTEMPTED')
        try:
            with patch.object(runtime.SINK, 'emit', Mock()) as sink, \
                 patch.object(runtime.research_event_store.WRITER, 'enqueue', Mock()) as writer, \
                 patch.object(runtime.google_sheets_sync, 'enqueue_delivered_event', Mock()) as sheets:
                events = runtime.preview_watch_sources(capture)
            self.assertTrue(events)
            self.assertTrue(all(row['capture_stage'] == 'WATCH_PLANNED_ALERT' for row in events))
            self.assertTrue(all(row['engine_snapshot']['watch_scan_id'] == 'preview-watch' for row in events))
            self.assertTrue(all(row['delivery_status'] == 'NOT_ATTEMPTED' for row in events))
            sink.assert_not_called(); writer.assert_not_called(); sheets.assert_not_called()
            self.assertEqual([dict(state) for state in states], originals)
            def failed():
                capture()
                raise ValueError('fixture')
            with self.assertRaises(ValueError):
                runtime.preview_watch_sources(failed)
            self.assertEqual([dict(state) for state in states], originals)
            self.assertIsNone(runtime._PLANNED_CAPTURE.get())
        finally:
            runtime.reset_watch_context(token)


if __name__ == '__main__':
    unittest.main()
