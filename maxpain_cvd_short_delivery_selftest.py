"""Offline integration checks: real Watch functions, fake Telegram and writers.

Only selected function ASTs are loaded from main/runtime. Bot startup, network
clients, collectors and production database connections are never imported.
"""
from __future__ import annotations

import ast
import asyncio
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import html
from pathlib import Path
from types import SimpleNamespace

import google_sheets_sync
import maxpain_cvd_short_alert as formula
import research_event_capture as capture


ROOT = Path(__file__).resolve().parent
TIME = datetime(2026, 9, 10, 10, 0, tzinfo=timezone.utc)


def _load(filename, names, scope):
    tree = ast.parse((ROOT / filename).read_text())
    nodes = [node for node in tree.body
             if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
             and node.name in names]
    assert {node.name for node in nodes} == set(names)
    future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    module = ast.fix_missing_locations(ast.Module(body=[future, *nodes], type_ignores=[]))
    exec(compile(module, filename, "exec"), scope)
    return scope


def _item(symbol="BTC", side="SHORT", timeframe="24h", short=.8):
    bullish = side == "SHORT"
    direction = "BULLISH" if bullish else "BEARISH"
    flow = {
        "available": True, "quality_status": "PASS", "freshness_status": "FRESH",
        "direction": direction, "score": 80.0 if bullish else -80.0,
        "time_families": {"short": {
            "quality": short, "direction": direction, "windows": ["1h", "4h"],
        }},
    }
    return {
        "symbol": symbol, "side": side, "timeframe": timeframe, "score": 70.0,
        "current_price": 100.0, "target_price": 102.0 if bullish else 98.0,
        "target_direction": "UP" if bullish else "DOWN",
        "price_source": "BINANCE_SPOT_TRADE_1M", "price_pair": symbol + "USDT",
        "types": ["NEAR_MAX_PAIN"],
        "market_evidence": {"modules": {
            "futures_flow": deepcopy(flow), "spot_flow": deepcopy(flow),
        }},
    }


def _main_scope(runtime):
    scope = {
        "datetime": datetime, "timezone": timezone, "html": html,
        "maxpain_cvd_short_alert": formula, "research_event_runtime": runtime,
        "FORMULA_ALERT_SENT_KEYS": {}, "SCORE_CONFIRMATION_STATE": {},
        "SCORE_CONFIRMATION_THRESHOLD": 65.0, "SCORE_CONFIRMATION_RESET_THRESHOLD": 60.0,
        "_confirmation_transition_message": lambda item: None,
        "_high_score_83_transition_message": lambda item: None,
        "_derivatives_high_transition_messages": lambda items: [],
        "_spot_family_high_transition_messages": lambda items: [],
    }
    return _load("main.py", {
        "_confirmation_state_key", "_score_confirmation_transition_message",
        "_special_transition_messages", "_collect_special_transition_messages",
        "_send_formula_watch_alerts",
    }, scope)


def test_actual_transition_collection():
    scope = _main_scope(SimpleNamespace())
    collect = scope["_collect_special_transition_messages"]
    item = _item()
    emitted = []
    assert len(collect([item], score65_items=emitted)) == 1
    assert emitted == [item] and emitted[0] is item
    assert collect([item], score65_items=[]) == []
    middle = {**item, "score": 62.0}
    assert collect([middle], score65_items=[]) == []
    assert collect([item], score65_items=[]) == []  # reset is below 60, not 65
    assert collect([{**item, "score": 59.0}], score65_items=[]) == []
    reset_emitted = []
    assert len(collect([item], score65_items=reset_emitted)) == 1
    assert reset_emitted == [item]

    # Different Max Pain horizons produce distinct real transitions, but
    # selection freezes the first both-total-CVD item BEFORE the short check.
    scope["SCORE_CONFIRMATION_STATE"].clear()
    first = _item(timeframe="12h", short=.64)
    later = _item(timeframe="48h", short=.99)
    collected = []
    collect([first, later], score65_items=collected)
    assert collected == [first, later]
    assert formula.select_matches(collected) == []


class FakeBot:
    def __init__(self, failures=0):
        self.failures = failures
        self.attempts = []
        self.delivered = []

    async def send_message(self, **kwargs):
        self.attempts.append(kwargs)
        if self.failures:
            self.failures -= 1
            raise RuntimeError("simulated Telegram failure")
        self.delivered.append(kwargs)


async def test_delivery():
    captures = []
    def remember(match, **kwargs):
        captures.append((match, kwargs))
        return True
    scope = _main_scope(SimpleNamespace(capture_formula_match=remember))
    send = scope["_send_formula_watch_alerts"]
    item = _item()
    bot = FakeBot(failures=1)
    assert await send(bot, 99, [item], watch_scan_id="watch:a", decision_time=TIME) == 0
    assert captures[-1][1]["delivery_status"] == "DELIVERY_FAILED"
    assert captures[-1][1]["delivered_at_utc"] is None
    assert not scope["FORMULA_ALERT_SENT_KEYS"]
    # A failed delivery can retry the exact same Watch; its failure is never
    # marked delivered or inserted into the successful-delivery duplicate set.
    assert await send(bot, 99, [item], watch_scan_id="watch:a", decision_time=TIME) == 1
    assert captures[-1][1]["delivery_status"] == "DELIVERED"
    assert captures[-1][1]["event_time"] == TIME
    assert captures[-1][1]["persist"] is True
    assert captures[-1][1]["delivered_at_utc"] is not None
    assert await send(bot, 99, [item], watch_scan_id="watch:a", decision_time=TIME) == 0
    assert len(bot.attempts) == 2 and len(captures) == 2
    assert await send(bot, 99, [item], watch_scan_id="watch:b", decision_time=TIME) == 1
    assert await send(bot, 99, [item], watch_scan_id="", decision_time=TIME) == 0
    assert bot.delivered[0]["parse_mode"] == "HTML"
    assert "BTC | לונג" in bot.delivered[0]["text"]

    # Failure to archive one sent coin cannot interrupt the next coin or
    # make an already sent Telegram card eligible for duplicate delivery.
    scope["FORMULA_ALERT_SENT_KEYS"].clear()
    def fail_first_capture(match, **kwargs):
        if match.symbol == "BTC":
            raise RuntimeError("simulated archive failure")
        return remember(match, **kwargs)
    scope["research_event_runtime"] = SimpleNamespace(capture_formula_match=fail_first_capture)
    two_bot = FakeBot()
    assert await send(two_bot, 99, [item, _item("SOL")], watch_scan_id="watch:c", decision_time=TIME) == 2
    assert len(two_bot.delivered) == 2
    assert await send(two_bot, 99, [item], watch_scan_id="watch:c", decision_time=TIME) == 0

    # Bad selector data fails open without Telegram or research side effects.
    def broken_selector(items):
        raise ValueError("simulated malformed source batch")
    scope["maxpain_cvd_short_alert"] = SimpleNamespace(select_matches=broken_selector)
    assert await send(two_bot, 99, [item], watch_scan_id="watch:d", decision_time=TIME) == 0
    assert len(two_bot.delivered) == 2


def test_runtime_capture_and_sheets():
    queued, sheets = [], []
    def enqueue(event, **kwargs):
        queued.append((event, kwargs))
        return True
    def sheet_enqueue(event, **kwargs):
        sheets.append(google_sheets_sync.build_delivered_event_payload(event, **kwargs))
        return True
    scope = {
        "datetime": datetime, "timezone": timezone, "replace": replace, "hashlib": hashlib,
        "_WATCH_CONTEXT": ContextVar("test_watch", default={}),
        "SINK": capture.DryRunResearchCapture(max_events=20),
        "research_event_capture": capture, "maxpain_cvd_short_alert": formula,
        "research_event_store": SimpleNamespace(WRITER=SimpleNamespace(enqueue=enqueue)),
        "google_sheets_sync": SimpleNamespace(enqueue_delivered_event=sheet_enqueue),
    }
    _load("research_event_runtime.py", {
        "set_watch_context", "reset_watch_context", "_with_watch_context",
        "_now", "_emit", "capture_formula_match",
    }, scope)
    token = scope["set_watch_context"](watch_scan_id="watch:source", watch_cycle_number=17)
    try:
        match = formula.select_matches([_item()])[0]
        assert scope["capture_formula_match"](match, event_time=TIME, persist=False)
        assert queued == [] and sheets == []
        event = SimpleNamespace(**scope["SINK"].events()[-1])
        assert event.event_kind == "ALERT" and event.event_type == formula.FORMULA_ID
        assert event.direction == "LONG" and event.source_side == "SHORT"
        snapshot = event.engine_snapshot
        assert snapshot["analysis_direction"] == snapshot["displayed_direction"] == "LONG"
        assert snapshot["watch_scan_id"] == "watch:source" and snapshot["watch_cycle_number"] == 17
        assert snapshot["sheet_snapshot_id"] == hashlib.sha256(b"watch:source|BTC|LONG").hexdigest()
        parent = capture.build_maxpain_event(match.item, event_type="MAX_PAIN_SCORE_65", event_time=TIME)
        assert snapshot["formula_match"]["source_event_fingerprint"] == parent.event_fingerprint
        assert snapshot["formula_match"]["source_event_type"] == parent.event_type
        assert event.event_fingerprint != parent.event_fingerprint
        assert snapshot["formula_match"]["experimental"] is True

        assert scope["capture_formula_match"](
            match, event_time=TIME, persist=True, delivery_status="DELIVERY_FAILED",
            delivery_attempted_at_utc=TIME,
        )
        assert len(queued) == 1 and not sheets
        assert queued[-1][1]["capture_stage"] == "TELEGRAM_FORMULA_ALERT"
        assert queued[-1][1]["delivery_status"] == "DELIVERY_FAILED"
        assert scope["capture_formula_match"](
            match, event_time=TIME, persist=True, delivery_status="DELIVERED",
            delivery_attempted_at_utc=TIME, delivered_at_utc=TIME,
        )
        assert len(queued) == 2 and len(sheets) == 1
        # The writer deliberately uses ON CONFLICT DO NOTHING. A failed
        # attempt therefore needs a distinct audit fingerprint, otherwise
        # it would permanently mask the subsequent successful delivery.
        failed_event, failed_kwargs = queued[0]
        delivered_event, delivered_kwargs = queued[1]
        assert failed_event.event_fingerprint != delivered_event.event_fingerprint
        assert delivered_event.event_fingerprint == event.event_fingerprint
        assert failed_event.engine_snapshot["formula_match"]["canonical_event_fingerprint"] == delivered_event.event_fingerprint
        assert failed_event.engine_snapshot["formula_match"]["source_event_fingerprint"] == parent.event_fingerprint
        simulated_db = {}
        for recorded_event, metadata in queued:
            simulated_db.setdefault(recorded_event.event_fingerprint, metadata["delivery_status"])
        assert simulated_db[delivered_event.event_fingerprint] == "DELIVERED"
        assert simulated_db[failed_event.event_fingerprint] == "DELIVERY_FAILED"
        rows = {upsert["sheet"]: upsert["row"] for upsert in sheets[0]["upserts"]}
        row = rows["Telegram_Events"]
        assert row["record_type"] == row["source_record_type"] == formula.FORMULA_ID
        assert row["direction"] == row["analysis_direction"] == row["displayed_direction"] == "LONG"
        assert row["verification_status"] == "DELIVERED"

        # The bearish direction is also inverted exactly once in the source
        # event and then displayed directly on the dedicated formula card.
        bearish = formula.select_matches([_item("ETH", side="LONG")])[0]
        scope["capture_formula_match"](bearish, event_time=TIME)
        event = SimpleNamespace(**scope["SINK"].events()[-1])
        assert event.direction == "SHORT" and event.source_side == "LONG"
        assert event.engine_snapshot["displayed_direction"] == "SHORT"
    finally:
        scope["reset_watch_context"](token)
    assert scope["_WATCH_CONTEXT"].get() == {}


def test_watch_only_hook_structure():
    tree = ast.parse((ROOT / "main.py").read_text())
    helper_calls = []
    for func in tree.body:
        if isinstance(func, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for node in ast.walk(func):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "_send_formula_watch_alerts":
                    helper_calls.append((func, node))
    assert len(helper_calls) == 1
    watch, helper = helper_calls[0]
    assert watch.name == "run_watch_cycle"
    guarded = [node for node in ast.walk(watch) if isinstance(node, ast.If)
               and isinstance(node.test, ast.Name) and node.test.id == "general_enabled"
               and helper in list(ast.walk(node))]
    assert len(guarded) == 1  # manual and Magnet-only paths never call helper
    guard = guarded[0]
    prior_capture = [node for node in ast.walk(guard) if isinstance(node, ast.Call)
                     and isinstance(node.func, ast.Attribute)
                     and node.func.attr == "capture_special_transitions"
                     and node.lineno < helper.lineno]
    assert len(prior_capture) == 2  # delivered and failed status branches
    collected = [node for node in ast.walk(watch) if isinstance(node, ast.IfExp)
                 and isinstance(node.test, ast.Name) and node.test.id == "general_enabled"
                 and any(isinstance(child, ast.Call) and isinstance(child.func, ast.Name)
                         and child.func.id == "_collect_special_transition_messages"
                         for child in ast.walk(node.body))]
    assert len(collected) == 1  # Magnet-only scans do not consume Score65 state
    assert any(isinstance(node, ast.Try) and any(
        isinstance(child, ast.Call) and isinstance(child.func, ast.Attribute)
        and child.func.attr == "reset_watch_context"
        for stmt in node.finalbody for child in ast.walk(stmt)) for node in ast.walk(watch))


def run():
    test_actual_transition_collection()
    asyncio.run(test_delivery())
    test_runtime_capture_and_sheets()
    test_watch_only_hook_structure()
    print("Formula Watch delivery integration self-test: PASS (fake Telegram/writers; no network or DB)")


if __name__ == "__main__":
    run()
