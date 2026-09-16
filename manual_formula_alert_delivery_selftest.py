"""Offline transport boundary tests against the real transactional state reducer."""
import ast
import asyncio
from contextlib import ExitStack
from copy import deepcopy
from datetime import timedelta
import importlib
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import manual_formula_alert_delivery as delivery
from manual_formula_alert_store_selftest import BASE, MemoryDatabase, pair


class DeliveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        importlib.reload(delivery)
        self.db = MemoryDatabase(BASE)
        self.clock = [BASE + timedelta(minutes=2)]
        self.boundaries = ExitStack(); self.addCleanup(self.boundaries.close)
        self.boundaries.enter_context(patch.object(delivery.store, '_connect', self.db.connect))
        self.boundaries.enter_context(patch.object(delivery.store.source, 'load_batch', return_value=([], {'selected_events': 0})))
        self.boundaries.enter_context(patch.object(delivery, '_now', lambda: self.clock[0]))
        self.schema = self.boundaries.enter_context(patch.object(delivery.store, 'schema_ready', return_value=True))

    def seed(self, count=1, *, expired=False):
        delivery.store.initialize_scope(1, BASE)
        state = self.db.read(1)
        delivery.store.record_events(state, [pair()], self.clock[0])
        state['intents'] = state['intents'][:count]
        if expired:
            for row in state['intents']: row['expires_at'] = delivery.store.iso(self.clock[0] - timedelta(seconds=1))
        self.db.write(1, state)

    def rows(self):
        return self.db.read(1)['intents']

    def bot(self, **kwargs):
        kwargs.setdefault('return_value', SimpleNamespace(message_id=123))
        return SimpleNamespace(send_message=AsyncMock(**kwargs))

    async def run_delivery(self, bot, **kwargs):
        kwargs.setdefault('may_deliver', lambda: True)
        return await delivery.run_once(bot, 1, **kwargs)

    async def test_initialize_scope_once_per_destination_without_resetting_fence(self):
        original = delivery.store.initialize_scope
        with patch.object(delivery.store, 'initialize_scope', wraps=original) as initialize:
            self.assertTrue(await delivery.initialize(1))
            self.assertTrue(await delivery.initialize(1))
            self.assertTrue(await delivery.initialize(2))
            self.assertEqual(initialize.call_count, 2)
        self.schema.assert_called_once()
        self.assertEqual(self.db.read(1)['activated_at'], delivery.store.iso(BASE))
        self.assertEqual(self.db.read(2)['activated_at'], delivery.store.iso(BASE))

    async def test_two_attempt_limit_html_and_confirmed_ids_never_retry(self):
        self.seed(3); bot = self.bot()
        self.assertEqual(await self.run_delivery(bot), 2)
        self.assertEqual([r['status'] for r in self.rows()], ['DELIVERED', 'DELIVERED', 'PENDING'])
        self.assertEqual(await self.run_delivery(bot), 1)
        self.assertEqual(await self.run_delivery(bot), 0)
        self.assertEqual(bot.send_message.await_count, 3)
        for call, row in zip(bot.send_message.await_args_list, self.rows()):
            self.assertEqual(call.kwargs, {'chat_id': 1, 'text': row['text'], 'parse_mode': 'HTML'})

    async def test_watch_priority_group_drains_more_than_two_before_return(self):
        self.seed(3); bot = self.bot()
        with patch.object(delivery.store, 'record_watch_events', return_value={'created_intents': 0}) as record:
            self.assertEqual(await delivery.run_watch(bot, 1, [], may_deliver=lambda: True), 3)
        record.assert_called_once()
        self.assertEqual([r['status'] for r in self.rows()], ['DELIVERED'] * 3)
        self.assertEqual(bot.send_message.await_count, 3)

    async def test_watch_recipient_revocation_during_preparation_prevents_send(self):
        self.seed(3); active = [True]; bot = self.bot()
        def record(*args):
            active[0] = False
            return {'created_intents': 0}
        with patch.object(delivery.store, 'record_watch_events', side_effect=record), \
             patch.object(delivery.store, 'claim', wraps=delivery.store.claim) as claim:
            self.assertEqual(await delivery.run_watch(bot, 1, [], may_deliver=lambda: active[0]), 0)
        claim.assert_not_called(); bot.send_message.assert_not_awaited()

    async def test_actual_btc_magnet_preview_creates_c0964_without_native_delivery(self):
        import main
        import research_event_runtime as runtime
        self.seed(0)
        stamp = BASE + timedelta(minutes=1)
        magnet = {'symbol': 'BTC', 'side': 'UPPER', 'count': 3,
                  'members': ['12h', '24h', '48h'], 'min_target': 102,
                  'max_target': 103, 'average_target': 102.5, 'spread_pct': .5,
                  'magnet_quality': 80, 'liquidity_edge_pct': 30}
        evidence = {'modules': {'spot_flow': {'available': True, 'score': 25, 'direction': 'BULLISH'}}}
        token = runtime.set_watch_context(watch_scan_id='current-c0964-watch')
        try:
            with patch.object(main, 'MAGNET_V1_WATCHES', {'BTC': {'chat_id': 1}}), \
                 patch.object(main, 'datetime', SimpleNamespace(now=lambda tz: stamp)), \
                 patch.object(runtime.magnet_v1, 'build_magnets', return_value=[magnet]), \
                 patch.object(runtime.magnet_v1, 'evaluate_confirmation', return_value={'status': 'OBSERVATION'}), \
                 patch.object(runtime.market_confidence_engine, 'combine', return_value=evidence), \
                 patch.object(runtime.research_event_store.WRITER, 'enqueue') as writer:
                events = main._preview_watch_formula_sources(
                    1, [], [], [], None, [{'symbol': 'BTC', 'current_price': 100}], {}, stamp,
                )
                bot = self.bot()
                self.assertEqual(await delivery.run_watch(bot, 1, events, may_deliver=lambda: True), 1)
                await bot.send_message(chat_id=1, text='ordinary Watch header')
            writer.assert_not_called()
            text = bot.send_message.await_args_list[0].kwargs['text']
            self.assertIn('C0964', text)
            self.assertIn('סף 2%', text)
            self.assertIn('<b>הערה</b>: מבוסס בעיקר על אוגוסט ועל עליות', text)
            self.assertEqual(self.rows()[0]['payload']['rule_id'], 'C0964')
            self.assertEqual(self.rows()[0]['payload']['direction'], 'SHORT')
            self.assertTrue(all(event['delivery_status'] == 'NOT_ATTEMPTED' for event in events))
        finally:
            runtime.reset_watch_context(token)

    async def test_watch_delivers_frozen_sol_c1274_bundle_at_one_point_five_percent(self):
        delivery.store.initialize_scope(1, BASE)
        computed = BASE + timedelta(minutes=1)
        coins = {symbol: {} for symbol in delivery.rules.SYMBOLS}
        coins['SOL'] = {
            'status': 'PARTIAL',
            'source_time_errors': [],
            'models': {'futures_flow': {
                'available': True,
                'capture_status': 'AVAILABLE',
                'quality_status': 'PASS',
                'freshness_status': 'FRESH',
                'score': 25,
                'time_families': {
                    'long': {'quality': .65, 'direction': 'BULLISH'},
                },
            }},
            'sources': {'futures': {'quality': {
                'candle_close': BASE.isoformat(),
            }}},
        }
        bundle = {
            'version': delivery.rules._WATCH_SCORE_VERSION,
            'population': delivery.rules._WATCH_SCORE_POPULATION,
            'hash_version': delivery.rules._WATCH_SCORE_HASH_VERSION,
            'status': 'PARTIAL',
            'symbols_expected': list(delivery.rules.SYMBOLS),
            'cycle_id': 'delivery-c1274-watch',
            'computed_at_utc': computed.isoformat(),
            'coins': coins,
        }
        bundle['payload_sha256'] = delivery.rules._watch_digest(bundle)
        references = {'SOL': {'FUTURES_CVD': {
            'status': 'READY', 'component': 'FUTURES_CVD', 'symbol': 'SOL',
            'price': '100', 'price_time_utc': BASE.isoformat(),
            'anchor_time_utc': BASE.isoformat(), 'source': 'BINANCE_SPOT_TRADE_1M',
            'precision': 'CLOSED_1M',
        }}}
        bot = self.bot()
        original = delivery.store.record_watch_events
        with patch.object(delivery.store, 'record_watch_events', wraps=original) as record:
            self.assertEqual(await delivery.run_watch(
                bot, 1, [], c1274_bundle=bundle, price_references=references,
                may_deliver=lambda: True,
            ), 1)
        record.assert_called_once()
        self.assertEqual(record.call_args.kwargs['c1274_bundle'], bundle)
        self.assertEqual(record.call_args.kwargs['price_references'], references)
        row = self.rows()[0]
        self.assertEqual(row['status'], 'DELIVERED')
        self.assertEqual(row['payload']['rule_id'], 'C1274')
        self.assertEqual(row['payload']['symbol'], 'SOL')
        self.assertEqual(row['payload']['direction'], 'LONG')
        self.assertEqual(row['payload']['threshold_bps'], 150)
        self.assertEqual(row['payload']['price_reference']['price'], '100')
        text = bot.send_message.await_args.kwargs['text']
        self.assertIn('סף 1.5%', text)
        self.assertIn('<b>סטופלוס:</b> 98.5', text)
        self.assertIn('<b>טייק פרופיט:</b> 101.5', text)
        self.assertNotIn('מגנט', text)
        self.assertNotIn('החיזוי הפוך', text)

    async def test_later_event_pass_does_not_hide_c1274_scan_status(self):
        bot = self.bot()
        direct = {'created_intents': 0, 'c1274_scan_status': 'NO_MATCH', 'pending': 0}
        legacy = {'created_intents': 0, 'c1274_scan_status': 'NOT_PROVIDED', 'pending': 0}
        with patch.object(delivery.store, 'record_watch_events', side_effect=(direct, legacy)):
            self.assertEqual(await delivery.run_watch(
                bot, 1, [], c1274_bundle={'fixture': True},
                price_references={}, may_deliver=lambda: True,
            ), 0)
            self.assertEqual(await delivery.run_watch(
                bot, 1, [], may_deliver=lambda: True,
            ), 0)
        current = delivery.status()
        self.assertEqual(current['last_summary'], legacy)
        self.assertEqual(current['last_c1274_scan_status'], 'NO_MATCH')
        self.assertEqual(current['last_c1274_summary'], direct)

    async def test_no_callback_or_revocation_before_initialize_never_claims(self):
        self.seed(); bot = self.bot()
        with patch.object(delivery.store, 'claim', wraps=delivery.store.claim) as claim:
            self.assertEqual(await delivery.run_once(bot, 1), 0)
            self.assertEqual(await self.run_delivery(bot, may_deliver=lambda: False), 0)
        claim.assert_not_called(); self.schema.assert_not_called(); bot.send_message.assert_not_awaited()

    async def test_revocation_while_initialize_or_collect_awaited_prevents_claim(self):
        for stage in ('initialize_scope', 'collect'):
            with self.subTest(stage=stage):
                self.seed(); delivery._SCOPES.clear(); active = [True]
                original = getattr(delivery.store, stage)
                def stopped(*args, **kwargs):
                    result = original(*args, **kwargs); active[0] = False
                    return result
                bot = self.bot()
                with patch.object(delivery.store, stage, stopped), patch.object(delivery.store, 'claim', wraps=delivery.store.claim) as claim:
                    await self.run_delivery(bot, may_deliver=lambda: active[0])
                claim.assert_not_called(); bot.send_message.assert_not_awaited()

    async def test_revocation_after_claim_cancels_without_a_network_attempt(self):
        self.seed(); active = [True]; original = delivery.store.claim
        def stopped(*args, **kwargs):
            result = original(*args, **kwargs); active[0] = False
            return result
        bot = self.bot()
        with patch.object(delivery.store, 'claim', stopped):
            self.assertEqual(await self.run_delivery(bot, may_deliver=lambda: active[0]), 0)
        bot.send_message.assert_not_awaited()
        self.assertEqual(self.rows()[0]['status'], 'CANCELLED')
        active[0] = True
        self.assertEqual(await self.run_delivery(bot, may_deliver=lambda: active[0]), 0)
        bot.send_message.assert_not_awaited()

    async def test_revocation_after_first_send_prevents_second_claim(self):
        self.seed(2); active = [True]
        async def stopped(**kwargs):
            active[0] = False
            return SimpleNamespace(message_id=1)
        bot = self.bot(side_effect=stopped)
        self.assertEqual(await self.run_delivery(bot, may_deliver=lambda: active[0]), 1)
        self.assertEqual([r['status'] for r in self.rows()], ['DELIVERED', 'PENDING'])
        bot.send_message.assert_awaited_once()

    async def test_claim_expiring_before_transport_is_cancelled(self):
        self.seed(); original = delivery.store.claim
        def slow_claim(*args, **kwargs):
            result = original(*args, **kwargs)
            self.clock[0] = BASE + timedelta(minutes=11)
            return result
        bot = self.bot()
        with patch.object(delivery.store, 'claim', slow_claim):
            self.assertEqual(await self.run_delivery(bot), 0)
        self.assertEqual(self.rows()[0]['status'], 'CANCELLED')
        bot.send_message.assert_not_awaited()

    async def test_timeout_or_unconfirmed_ack_is_unknown_not_retried(self):
        self.seed(3); bot = self.bot(side_effect=[TimeoutError('fixture'), SimpleNamespace(message_id=None), SimpleNamespace(message_id=True)])
        self.assertEqual(await self.run_delivery(bot), 0)
        self.assertEqual(await self.run_delivery(bot), 0)
        self.assertEqual(await self.run_delivery(bot), 0)
        self.assertEqual([r['status'] for r in self.rows()], ['UNKNOWN'] * 3)
        self.assertEqual(bot.send_message.await_count, 3)

    async def test_zero_negative_string_ack_ids_never_count_as_delivered(self):
        self.seed(3); bot = self.bot(side_effect=[SimpleNamespace(message_id=0), SimpleNamespace(message_id=-1), SimpleNamespace(message_id='5')])
        await self.run_delivery(bot); await self.run_delivery(bot)
        self.assertEqual([r['status'] for r in self.rows()], ['UNKNOWN'] * 3)
        self.assertEqual(delivery.status()['delivered'], 0)

    async def test_definite_telegram_rejections_are_terminal_failed(self):
        self.seed(2)
        errors = [type(name, (Exception,), {})('fixture') for name in ('BadRequest', 'Forbidden')]
        bot = self.bot(side_effect=errors)
        self.assertEqual(await self.run_delivery(bot), 0)
        self.assertEqual(await self.run_delivery(bot), 0)
        self.assertEqual([r['status'] for r in self.rows()], ['FAILED', 'FAILED'])
        self.assertEqual(bot.send_message.await_count, 2)

    async def test_cancellation_keeps_inflight_until_unknown_without_resend(self):
        self.seed(); bot = self.bot(side_effect=asyncio.CancelledError)
        with self.assertRaises(asyncio.CancelledError): await self.run_delivery(bot)
        self.assertEqual(self.rows()[0]['status'], 'IN_FLIGHT')
        self.clock[0] += timedelta(minutes=2)
        bot.send_message.side_effect = None
        self.assertEqual(await self.run_delivery(bot), 0)
        self.assertEqual(self.rows()[0]['status'], 'UNKNOWN')
        bot.send_message.assert_awaited_once()

    async def test_ack_storage_failure_keeps_durable_attempt_and_never_resends(self):
        self.seed(); bot = self.bot()
        with patch.object(delivery.store, 'finish', side_effect=RuntimeError('fixture storage')):
            await self.run_delivery(bot)
        self.assertEqual(self.rows()[0]['status'], 'IN_FLIGHT')
        self.assertIn('acknowledgement:RuntimeError', delivery.status()['last_error_type'])
        delivery._RETRY_AFTER = 0; self.clock[0] += timedelta(minutes=2)
        self.assertEqual(await self.run_delivery(bot), 0)
        self.assertEqual(self.rows()[0]['status'], 'UNKNOWN')
        bot.send_message.assert_awaited_once()

    async def test_overlapping_drain_is_rejected_while_transport_holds_lock(self):
        self.seed(); entered = asyncio.Event(); release = asyncio.Event()
        async def pending(**kwargs):
            entered.set(); await release.wait()
            return SimpleNamespace(message_id=1)
        bot = self.bot(side_effect=pending)
        task = asyncio.create_task(self.run_delivery(bot))
        try:
            await asyncio.wait_for(entered.wait(), timeout=2)
            self.assertEqual(await self.run_delivery(bot), 0)
            self.assertEqual(bot.send_message.await_count, 1)
        finally:
            release.set()
            self.assertEqual(await task, 1)
        self.assertEqual(self.rows()[0]['status'], 'DELIVERED')

    async def test_watch_waits_for_prior_supervisor_send_then_prepares_entire_group(self):
        self.seed(3)
        entered, release = asyncio.Event(), asyncio.Event()
        scan_started = [False]
        async def pending(**kwargs):
            entered.set()
            await release.wait()
            return SimpleNamespace(message_id=1)
        bot = self.bot(side_effect=pending)
        supervisor = asyncio.create_task(self.run_delivery(bot, may_deliver=lambda: not scan_started[0]))
        watch = None
        try:
            await asyncio.wait_for(entered.wait(), timeout=2)
            scan_started[0] = True
            with patch.object(delivery.store, 'record_watch_events', return_value={'created_intents': 0}) as record:
                watch = asyncio.create_task(delivery.run_watch(bot, 1, [], may_deliver=lambda: True))
                await asyncio.sleep(0)
                self.assertFalse(watch.done())
                record.assert_not_called()
                release.set()
                self.assertEqual(await asyncio.wait_for(supervisor, timeout=2), 1)
                self.assertEqual(await asyncio.wait_for(watch, timeout=2), 2)
            record.assert_called_once()
            self.assertEqual([row['status'] for row in self.rows()], ['DELIVERED'] * 3)
            self.assertEqual(bot.send_message.await_count, 3)
        finally:
            release.set()
            await asyncio.gather(supervisor, *([watch] if watch is not None else []), return_exceptions=True)

    async def test_watch_rechecks_destination_after_waiting_for_delivery_lock(self):
        self.seed(3); active = [True]; bot = self.bot()
        self.assertTrue(await delivery.initialize(1))
        await delivery._LOCK.acquire()
        task = asyncio.create_task(delivery.run_watch(bot, 1, [], may_deliver=lambda: active[0]))
        try:
            with patch.object(delivery.store, 'record_watch_events') as record:
                await asyncio.sleep(0)
                self.assertFalse(task.done())
                active[0] = False
                delivery._LOCK.release()
                self.assertEqual(await asyncio.wait_for(task, timeout=2), 0)
            record.assert_not_called()
            bot.send_message.assert_not_awaited()
        finally:
            if delivery._LOCK.locked():
                delivery._LOCK.release()
            await asyncio.gather(task, return_exceptions=True)

    async def test_collection_failure_never_claims_or_transports(self):
        self.seed(); bot = self.bot()
        with patch.object(delivery.store, 'collect', side_effect=RuntimeError('fixture')), patch.object(delivery.store, 'claim') as claim:
            self.assertEqual(await self.run_delivery(bot), 0)
        claim.assert_not_called(); bot.send_message.assert_not_awaited()
        self.assertEqual(self.rows()[0]['status'], 'PENDING')

    async def test_status_is_a_copy_and_never_claims_statistical_qualification(self):
        value = delivery.status(); original = deepcopy(value)
        value['rule_ids'].clear()
        self.assertEqual(delivery.status(), original)
        self.assertTrue(original['experimental'])
        self.assertFalse(original['statistically_qualified'])
        self.assertFalse(original['trade_execution'])


class MainHookTests(unittest.TestCase):
    def test_only_supervisor_calls_delivery_with_destination_and_watch_guards(self):
        tree = ast.parse((Path(__file__).parent / 'main.py').read_text())
        parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
        calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
                 and isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name)
                 and node.func.value.id == 'manual_formula_alert_delivery' and node.func.attr == 'run_once']
        self.assertEqual(len(calls), 1)
        call = calls[0]; chain = []; current = call
        while current in parents:
            current = parents[current]; chain.append(current)
        function = next(n for n in chain if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)))
        self.assertIn('supervisor', function.name)
        self.assertIsInstance(parents[call], ast.Await)
        guard = next(k.value for k in call.keywords if k.arg == 'may_deliver')
        self.assertIsInstance(guard, ast.Lambda)
        text = ast.unparse(guard.body)
        self.assertIn('WATCH_GENERAL_ENABLED', text)
        self.assertIn("not WATCH_RUNTIME.get('scan_in_progress')", text)
        self.assertIn("WATCH_RUNTIME.get('chat_id') == chat_id", text)

    def test_initialization_and_status_are_wired_without_ddl(self):
        tree = ast.parse((Path(__file__).parent / 'main.py').read_text())
        calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                 and isinstance(n.func.value, ast.Name) and n.func.value.id == 'manual_formula_alert_delivery']
        self.assertEqual(sum(n.func.attr == 'initialize' for n in calls), 3)
        self.assertEqual(sum(n.func.attr == 'status' for n in calls), 1)


if __name__ == '__main__':
    unittest.main()
