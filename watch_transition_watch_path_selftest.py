"""Offline regressions for the real Watch card and recovery entry points.

Load the production functions without importing bot startup or market clients.
Telegram, capture sinks and the durable delivery service are explicit fakes.
"""
from __future__ import annotations

import ast
import asyncio
from datetime import datetime, timezone
import html
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock


ROOT = Path(__file__).resolve().parent


def load_main(names, scope):
    tree = ast.parse((ROOT / "main.py").read_text())
    nodes = [node for node in tree.body
             if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
             and node.name in names]
    if {node.name for node in nodes} != set(names):
        raise AssertionError("A tested production entry point is missing")
    future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    module = ast.fix_missing_locations(ast.Module(body=[future, *nodes], type_ignores=[]))
    exec(compile(module, str(ROOT / "main.py"), "exec"), scope)
    return scope


class CardPathTests(unittest.IsolatedAsyncioTestCase):
    def card_scope(self):
        capture = SimpleNamespace(
            capture_sent_maxpain=Mock(return_value=True),
            capture_special_transitions=Mock(return_value=True),
        )
        scope = load_main({
            "_confirmation_state_key", "_score_confirmation_transition_message",
            "_special_transition_messages", "_send_alert_with_confirmation",
        }, {
            "datetime": datetime, "timezone": timezone, "html": html,
            "research_event_runtime": capture,
            "SCORE_CONFIRMATION_STATE": {},
            "SCORE_CONFIRMATION_THRESHOLD": 65.0,
            "SCORE_CONFIRMATION_RESET_THRESHOLD": 60.0,
            "_confirmation_transition_message": Mock(return_value=None),
            "_high_score_83_transition_message": Mock(return_value=None),
        })
        return scope, capture

    async def test_precomputed_watch_card_cannot_bypass_durable_mp65(self):
        scope, capture = self.card_scope()
        bot = SimpleNamespace(send_message=AsyncMock())
        item = {"symbol": "BTC", "side": "SHORT", "timeframe": "24h", "score": 70.0}
        await scope["_send_alert_with_confirmation"](
            bot, 99, "ordinary Watch card", item,
            special_transitions_precomputed=True,
        )
        bot.send_message.assert_awaited_once_with(
            chat_id=99, text="ordinary Watch card", parse_mode="HTML",
        )
        self.assertEqual(scope["SCORE_CONFIRMATION_STATE"], {})
        capture.capture_sent_maxpain.assert_called_once()
        self.assertEqual(capture.capture_sent_maxpain.call_args.kwargs["delivery_status"], "DELIVERED")
        capture.capture_special_transitions.assert_not_called()
        scope["_confirmation_transition_message"].assert_not_called()
        scope["_high_score_83_transition_message"].assert_not_called()

    async def test_independent_card_retains_legacy_special_delivery(self):
        scope, capture = self.card_scope()
        bot = SimpleNamespace(send_message=AsyncMock())
        item = {"symbol": "BTC", "side": "SHORT", "timeframe": "24h", "score": 70.0}
        await scope["_send_alert_with_confirmation"](bot, 99, "independent card", item)
        self.assertEqual(bot.send_message.await_count, 2)
        self.assertIn("אישור Max Pain לפי ציון", bot.send_message.await_args_list[1].kwargs["text"])
        self.assertTrue(scope["SCORE_CONFIRMATION_STATE"]["BTC|24h|SHORT"])
        capture.capture_special_transitions.assert_called_once()


class SupervisorRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def one_pass(self, *, general=True, scan=False, chat=99,
                       magnet=False, coordinator_alive=True, after_restore=None):
        bot = object()
        drain = AsyncMock(return_value=0)
        runtime = {"chat_id": chat, "scan_in_progress": scan}
        # The production loop's final sleep is its iteration boundary. Cancel
        # there, after observing exactly one full supervisor pass.
        sleep = AsyncMock(side_effect=asyncio.CancelledError)
        scope = {}

        async def restore(*args, **kwargs):
            if after_restore is not None:
                after_restore(scope)
            return True

        ensure = AsyncMock(side_effect=restore)
        scope.update({
            "asyncio": SimpleNamespace(CancelledError=asyncio.CancelledError, sleep=sleep),
            "WATCH_GENERAL_ENABLED": general, "WATCH_RUNTIME": runtime,
            "MAGNET_V1_WATCHES": {"BTC": {"chat_id": 99}} if magnet else {},
            "WATCH_TASK": SimpleNamespace(done=lambda: False) if coordinator_alive else None,
            "WATCH_SUPERVISOR_INTERVAL_SECONDS": 15,
            "_ensure_watch_coordinator": ensure,
            "watch_transition_delivery": SimpleNamespace(drain=drain),
        })
        load_main({"_watch_consumers_active", "_watch_supervisor_loop"}, scope)
        with self.assertRaises(asyncio.CancelledError):
            await scope["_watch_supervisor_loop"](SimpleNamespace(bot=bot))
        sleep.assert_awaited_once_with(15)
        self.assertNotEqual(runtime.get("last_cycle_status"), "supervisor_retry")
        return scope, drain, ensure, bot

    async def test_active_coordinator_still_recovers_pending_messages(self):
        scope, drain, ensure, bot = await self.one_pass()
        ensure.assert_not_awaited()
        drain.assert_awaited_once()
        self.assertEqual(drain.await_args.args, (bot, 99))
        may_deliver = drain.await_args.kwargs["may_deliver"]
        self.assertTrue(may_deliver())
        # Recovery receives a live eligibility check, including changes that
        # happen after this supervisor pass has entered the delivery helper.
        scope["WATCH_RUNTIME"]["scan_in_progress"] = True
        self.assertFalse(may_deliver())
        scope["WATCH_RUNTIME"]["scan_in_progress"] = False
        scope["WATCH_GENERAL_ENABLED"] = False
        self.assertFalse(may_deliver())
        scope["WATCH_GENERAL_ENABLED"] = True
        scope["WATCH_RUNTIME"]["chat_id"] = 100
        self.assertFalse(may_deliver())

    async def test_recovery_also_runs_after_coordinator_restore(self):
        scope, drain, ensure, _ = await self.one_pass(coordinator_alive=False)
        ensure.assert_awaited_once()
        drain.assert_awaited_once()
        self.assertEqual(scope["WATCH_RUNTIME"]["supervisor_restarts"], 1)

    async def test_busy_disabled_and_magnet_only_never_recover_general_messages(self):
        for options in (
            {"scan": True},
            {"general": False},
            {"general": False, "magnet": True},
            {"general": True, "chat": None, "magnet": True},
        ):
            with self.subTest(**options):
                _, drain, _, _ = await self.one_pass(**options)
                drain.assert_not_awaited()

    async def test_recipient_change_during_restore_blocks_previous_chat_recovery(self):
        _, drain, ensure, _ = await self.one_pass(
            coordinator_alive=False,
            after_restore=lambda scope: scope["WATCH_RUNTIME"].update(chat_id=100),
        )
        ensure.assert_awaited_once()
        drain.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
