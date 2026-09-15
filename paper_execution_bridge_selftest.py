"""Paper bridge checks. All bot/storage/quotes are fixtures; no real bot import.

Real delivery MODULE code is loaded with isolated provider stubs, so tests prove
that the integration call exists without touching production DBs or Telegram.
"""
from __future__ import annotations
import asyncio
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import AsyncMock, Mock, patch

import paper_execution_bridge as bridge
from paper_execution_feed import PaperFeed, intake_once, read_inbox


def example(source="dual-cvd65", identity="synthetic-1", side="LONG", now=None):
    now = now or datetime.now(timezone.utc) - timedelta(seconds=2)
    plan = {"symbol": "DEMO", "side": side, "entry": "100.00",
            "stop": "98" if side == "LONG" else "102",
            "take_profit": "104" if side == "LONG" else "96", "at": now.isoformat()}
    payload = {"execution_plan": plan, "private_research": "NEVER_PUBLISH_THIS",
               "text": "SYNTHETIC TEST ONLY"}
    if source == "dual-cvd65":
        payload.update(source_at_utc=now.isoformat(), observation={"status": "MATCH", "symbol": "DEMO", "direction": side})
    else:
        payload.update(symbol="DEMO", direction=side, source_direction="SHORT" if side == "LONG" else "LONG",
                       event_time=now.isoformat(), rule_id="synthetic-rule")
    return {"intent_id": identity, "payload": payload, "expires_at": (now+timedelta(hours=1)).isoformat(),
            "text": "SYNTHETIC TEST ONLY", "attempt_token": "test-attempt", "watch_scan_id": "synthetic-scan"}


class Quotes:
    def __init__(self, now):
        self.now, self.calls = now, 0
        self.bid, self.ask, self.mark = "99.9", "100", "100"
    def read(self, kind, symbol=None):
        self.calls += 1
        if kind == "metaAndAssetCtxs":
            return [{"universe": [{"name": "DEMO", "szDecimals": 2}]}, [{"markPx": self.mark}]]
        if kind != "l2Book" or symbol != "DEMO":
            raise AssertionError("unexpected request")
        return {"coin": "DEMO", "time": int(self.now.timestamp()*1000),
                "levels": [[{"px": self.bid, "sz": "100"}], [{"px": self.ask, "sz": "100"}]]}


def load_delivery(source):
    """Load actual branch code with dependencies replaced BEFORE any import."""
    prefix = "dual_cvd65" if source == "dual-cvd65" else "manual_formula_alert"
    rule_name = "dual_cvd65_alert" if source == "dual-cvd65" else "manual_formula_alert"
    rules = types.ModuleType(rule_name)
    rules.RULE_ID, rules.VERSION, rules.RULES = "synthetic-rule", "synthetic-v1", {"synthetic-rule": {}}
    store_name = prefix+"_store"
    store = types.ModuleType(store_name)
    transition = types.ModuleType("watch_transition_delivery")
    transition.subscription_scope = lambda _: "synthetic-scope"
    filename = "dual_cvd65_delivery.py" if source == "dual-cvd65" else "manual_formula_alert_delivery.py"
    spec = importlib.util.spec_from_file_location("_paper_fixture_"+prefix, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    with patch.dict("sys.modules", {rule_name: rules, store_name: store, "watch_transition_delivery": transition}):
        spec.loader.exec_module(module)
    module.initialize = AsyncMock(return_value=True)
    module._READY = True
    module._finish = AsyncMock()
    return module, store


class BridgeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)
        os.chmod(self.directory, 0o700)
        self.inbox = self.directory / "inbox.paper.jsonl"
        self.env = {"PAPER_BOT_BRIDGE": "paper", "PAPER_BOT_INBOX": str(self.inbox)}
        self.item = example()
        self.network = patch("http.client.HTTPSConnection", side_effect=AssertionError("No network in fixture test"))
        self.network.start()
    def tearDown(self):
        self.network.stop()
        self.temp.cleanup()
    async def forward(self, item=None, source="dual-cvd65", env=None):
        return await bridge.forward_delivered(item if item is not None else self.item, source=source,
                                              environ=self.env if env is None else env)
    def rows(self):
        return [json.loads(row) for row in read_inbox(self.inbox)]
    async def test_disabled_by_default_no_disk_or_thread(self):
        with patch.object(bridge, "_publish", side_effect=AssertionError("must not run")):
            self.assertEqual((await self.forward(env={}))['status'], "DISABLED")
        self.assertFalse(self.inbox.exists())
    async def test_live_testnet_and_unknown_modes_never_allowed(self):
        for mode in ("live", "mainnet", "testnet", "true", "1"):
            self.assertEqual((await self.forward(env={**self.env, "PAPER_BOT_BRIDGE": mode}))['status'], "BLOCKED_CONFIG")
        self.assertFalse(self.inbox.exists())
    async def test_no_plan_waits_without_guessing(self):
        del self.item['payload']['execution_plan']
        self.item['payload']['text'] = "entry=100 stop=98 tp=104 score=99 research threshold=2%"
        self.assertEqual((await self.forward())['status'], "WAITING_FOR_PRICES")
        self.assertFalse(self.inbox.exists())
    async def test_all_three_prices_copied_exactly(self):
        self.assertEqual((await self.forward())['status'], "PUBLISHED")
        row = self.rows()[0]
        for key in ('entry', 'stop', 'take_profit'):
            self.assertEqual(row[key], self.item['payload']['execution_plan'][key])
        self.assertEqual(self.inbox.stat().st_mode & 0o777, 0o600)
    async def test_private_research_not_written(self):
        await self.forward()
        self.assertNotIn("NEVER_PUBLISH_THIS", self.inbox.read_text())
        self.assertNotIn("synthetic-rule", self.inbox.read_text())
        self.assertEqual(set(self.rows()[0]), {'kind','event_id','symbol','side','entry','stop','take_profit','at'})
    async def test_missing_extra_and_float_prices_rejected(self):
        for kind in ('missing','extra','float'):
            item = deepcopy(self.item)
            plan = item['payload']['execution_plan']
            if kind == 'missing': del plan['stop']
            elif kind == 'extra': plan['private_key'] = 'NEVER_PUBLISH_THIS'
            else: plan['entry'] = 100.0
            self.assertEqual((await self.forward(item))['status'], "BLOCKED_INPUT_OR_STORAGE")
        self.assertFalse(self.inbox.exists())
    async def test_symbol_direction_and_time_lineage_checked(self):
        for key, value in [('symbol','OTHER'),('side','SHORT'),('at',datetime.now(timezone.utc).isoformat())]:
            item = deepcopy(self.item); item['payload']['execution_plan'][key] = value
            self.assertEqual((await self.forward(item))['status'], "BLOCKED_INPUT_OR_STORAGE")
        self.assertFalse(self.inbox.exists())
    async def test_manual_final_direction_not_inverted_again(self):
        self.assertEqual((await self.forward(example('manual-formulas',side='SHORT'), 'manual-formulas'))['status'], "PUBLISHED")
        self.assertEqual(self.rows()[0]['side'], 'SHORT')
    async def test_expired_and_future_intents_blocked(self):
        item = deepcopy(self.item); item['expires_at'] = '2020-01-01T00:00:00Z'
        self.assertEqual((await self.forward(item))['status'], "BLOCKED_INPUT_OR_STORAGE")
        future = example(now=datetime.now(timezone.utc)+timedelta(hours=1))
        self.assertEqual((await self.forward(future))['status'], "BLOCKED_INPUT_OR_STORAGE")
    async def test_same_notification_stable_id_new_notification_new_id(self):
        await self.forward(); await self.forward()
        other = deepcopy(self.item); other['intent_id'] = 'synthetic-2'
        await self.forward(other)
        a,b,c = self.rows()
        self.assertEqual(a['event_id'],b['event_id']); self.assertNotEqual(a['event_id'],c['event_id'])
    async def test_relative_nonprivate_and_symlink_paths_blocked(self):
        relative = {**self.env,'PAPER_BOT_INBOX':'inbox.paper.jsonl'}
        self.assertEqual((await self.forward(env=relative))['status'], 'BLOCKED_INPUT_OR_STORAGE')
        os.chmod(self.directory,0o755)
        self.assertEqual((await self.forward())['status'], 'BLOCKED_INPUT_OR_STORAGE')
        os.chmod(self.directory,0o700)
        self.inbox.symlink_to(self.directory/'other')
        self.assertEqual((await self.forward())['status'], 'BLOCKED_INPUT_OR_STORAGE')
    async def test_error_details_not_returned(self):
        with patch.object(bridge,'_publish',side_effect=OSError('NEVER_PUBLISH_THIS')):
            result = await self.forward()
        self.assertEqual(result['status'],'BLOCKED_INPUT_OR_STORAGE')
        self.assertNotIn('NEVER_PUBLISH_THIS',json.dumps(result))
    async def test_unknown_source_and_missing_path_blocked(self):
        self.assertEqual((await self.forward(source='unknown'))['status'], 'BLOCKED_CONFIG')
        self.assertEqual((await self.forward(env={'PAPER_BOT_BRIDGE':'paper'}))['status'], 'BLOCKED_CONFIG')
    async def test_cancellation_is_not_swallowed(self):
        with patch.object(asyncio,'to_thread',side_effect=asyncio.CancelledError()):
            with self.assertRaises(asyncio.CancelledError): await self.forward()
    async def test_paper_end_to_end_with_restart_and_retry(self):
        await self.forward(); await self.forward()
        quotes = Quotes(datetime.now(timezone.utc))
        kw = dict(symbols=('DEMO',),accounts=('paper-a','paper-b'),fee_bps='5',info=quotes,clock=lambda: quotes.now)
        database = self.directory/'test.paper.sqlite3'
        feed = PaperFeed(database,**kw)
        try:
            intake = intake_once(feed,self.inbox)
            self.assertEqual(intake,{'accepted':1,'replayed':1,'rejected':0})
            quotes.now += timedelta(seconds=1)
            feed.poll_once()
            self.assertEqual(feed.snapshot()['snapshot']['trades'][0]['status'],'OPEN')
        finally: feed.close()
        feed = PaperFeed(database,**kw)
        try:
            self.assertEqual(intake_once(feed,self.inbox)['accepted'],0)
            quotes.bid,quotes.ask,quotes.mark = '104','104.1','104'
            quotes.now += timedelta(seconds=1)
            feed.poll_once()
            self.assertEqual(feed.snapshot()['snapshot']['trades'][0]['status'],'CLOSED')
            self.assertEqual(feed.snapshot()['exchange_orders_sent'],0)
        finally: feed.close()
    async def _delivery(self, source, *, delivered=True, no_plan=False, storage_failure=False):
        module,store = load_delivery(source)
        item = example(source)
        if no_plan: del item['payload']['execution_plan']
        store.settle_orphans = Mock(return_value=0)
        store.claim_pending = Mock(side_effect=[[item],[]])
        store.collect = Mock(return_value={'created_intents':1})
        store.claim = Mock(side_effect=[item,None])
        store.utc = bridge._utc
        bot = types.SimpleNamespace(send_message=AsyncMock(return_value=types.SimpleNamespace(message_id=1 if delivered else None)))
        env = {**self.env,'PAPER_BOT_INBOX':str(self.directory/'missing'/'inbox.paper.jsonl')} if storage_failure else self.env
        with patch.dict(os.environ,env):
            count = (await module.drain(bot,1,may_deliver=lambda: True) if source=='dual-cvd65'
                     else await module.run_once(bot,1,may_deliver=lambda: True))
        bot.send_message.assert_awaited_once()
        self.assertEqual(bot.send_message.await_args.kwargs['text'], 'SYNTHETIC TEST ONLY')
        module._finish.assert_awaited_once()
        self.assertEqual(count, int(delivered))
        return module
    async def test_actual_dual_delivery_reaches_paper(self):
        module = await self._delivery('dual-cvd65')
        self.assertEqual(module.status()['paper_bridge']['status'],'PUBLISHED')
        self.assertEqual(len(self.rows()),1)
    async def test_actual_manual_delivery_reaches_paper(self):
        module = await self._delivery('manual-formulas')
        self.assertEqual(module.status()['paper_bridge']['status'],'PUBLISHED')
        self.assertEqual(len(self.rows()),1)
    async def test_unconfirmed_deliveries_do_not_create_paper(self):
        for source in bridge.SOURCES:
            module = await self._delivery(source,delivered=False)
            self.assertNotIn('paper_bridge',module.status())
        self.assertFalse(self.inbox.exists())
    async def test_existing_no_price_notifications_still_send_normally(self):
        for source in bridge.SOURCES:
            module = await self._delivery(source,no_plan=True)
            self.assertEqual(module.status()['paper_bridge']['status'],'WAITING_FOR_PRICES')
        self.assertFalse(self.inbox.exists())
    async def test_paper_storage_failure_does_not_change_telegram_success(self):
        for source in bridge.SOURCES:
            module = await self._delivery(source,storage_failure=True)
            self.assertEqual(module.status()['paper_bridge']['status'],'BLOCKED_INPUT_OR_STORAGE')
            self.assertEqual(module.status()['delivered'],1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
