"""Offline tests: synthetic inputs only; no exchange/production imports."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
import importlib.util
import inspect
from pathlib import Path
import socket
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from paper_execution import PaperBroker, PaperError, demo, number

BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def at(second):
    return (BASE + timedelta(seconds=second)).isoformat()


def signal(eid="s1", side="LONG", second=0):
    return {"kind": "SIGNAL", "event_id": eid, "symbol": "DEMO", "side": side,
            "entry": "100", "stop": "98" if side == "LONG" else "102",
            "take_profit": "104" if side == "LONG" else "96", "at": at(second)}


def quote(second=1, *, bid="99.9", ask="100", mark="100", bid_size="100", ask_size="100", eid=None):
    return {"kind": "QUOTE", "event_id": eid or f"q{second}", "symbol": "DEMO", "bid": bid,
            "ask": ask, "mark": mark, "bid_size": bid_size, "ask_size": ask_size, "at": at(second)}


class PaperTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "test.paper.sqlite3"
        self.kw = dict(accounts=("paper-a", "paper-b"), size_steps={"DEMO": "0.01"}, fee_bps="5")
        self.broker = PaperBroker(self.path, **self.kw)
        # The whole test suite, including initialization/import above, uses no provider.
        self.block = patch.object(socket, "socket", side_effect=AssertionError("network is forbidden"))
        self.block.start()

    def tearDown(self):
        self.broker.close()
        self.block.stop()
        self.tmp.cleanup()

    def send(self, event, received=None):
        return self.broker.process(event, received_at=received or event["at"])

    def trade(self, index=0):
        return self.broker.snapshot()["trades"][index]

    def open(self, side="LONG"):
        self.send(signal(side=side))
        self.send(quote(bid="100", ask="100"))

    def assert_reject_unchanged(self, event, received=None):
        before = self.broker.snapshot()
        with self.assertRaises(PaperError):
            self.send(event, received)
        self.assertEqual(before, self.broker.snapshot())

    def test_three_unchanged_prices(self):
        self.send(signal())
        self.assertEqual([o["price"] for o in self.trade()["orders"]], ["100", "98", "104"])
        self.assertTrue(all(o["reduce_only"] for o in self.trade()["orders"][1:]))

    def test_risk_quantity(self):
        self.send(signal())
        self.assertEqual(D(self.trade()["quantity"]), D(10))
        self.assertEqual(D(self.trade()["planned_price_loss_usd"]), D(20))

    def test_round_down_risk(self):
        e = signal(); e["stop"] = "97"
        self.send(e)
        self.assertEqual(D(self.trade()["quantity"]), D("6.66"))
        self.assertLessEqual(D(self.trade()["planned_price_loss_usd"]), D(20))

    def test_wait_for_limit(self):
        self.send(signal()); self.send(quote(bid="101", ask="102", mark="101"))
        self.assertEqual(self.trade()["status"], "WAITING_ENTRY")
        self.assertEqual(self.trade()["fills"], [])

    def test_long_take_profit(self):
        self.open(); self.send(quote(2, bid="104", ask="104.1", mark="104"))
        t = self.trade()
        self.assertEqual(t["status"], "CLOSED")
        self.assertEqual(t["exit_reason"], "TAKE_PROFIT")
        self.assertEqual(D(t["realized_gross_usd"]), D(40))
        self.assertEqual(t["orders"][1]["status"], "CANCELED")
        self.assertEqual(t["orders"][2]["status"], "FILLED")

    def test_long_stop(self):
        self.open(); self.send(quote(2, bid="98", ask="98.1", mark="98"))
        self.assertEqual(self.trade()["exit_reason"], "STOP")
        self.assertEqual(D(self.trade()["realized_gross_usd"]), D(-20))

    def test_short_take_profit(self):
        self.open("SHORT"); self.send(quote(2, bid="95.9", ask="96", mark="96"))
        self.assertEqual(self.trade()["exit_reason"], "TAKE_PROFIT")
        self.assertEqual(D(self.trade()["realized_gross_usd"]), D(40))

    def test_short_stop(self):
        self.open("SHORT"); self.send(quote(2, bid="101.9", ask="102", mark="102"))
        self.assertEqual(D(self.trade()["realized_gross_usd"]), D(-20))

    def test_mark_triggers_not_last_or_bid(self):
        self.open(); self.send(quote(2, bid="105", ask="106", mark="103"))
        self.assertIsNone(self.trade()["exit_reason"])
        self.send(quote(3, bid="103", ask="104", mark="104"))
        self.assertEqual(self.trade()["exit_reason"], "TAKE_PROFIT")
        self.assertEqual(D(self.trade()["fills"][-1]["price"]), D(103))

    def test_gap_can_lose_more_than_20(self):
        self.open(); self.send(quote(2, bid="95", ask="95.1", mark="95"))
        self.assertEqual(D(self.trade()["realized_gross_usd"]), D(-50))
        self.assertEqual(self.trade()["stop"], "98")

    def test_partial_entry_then_exit(self):
        self.send(signal()); self.send(quote(ask_size="3"))
        self.assertEqual(D(self.trade()["entry_filled"]), D(3))
        self.assertEqual(self.trade()["orders"][1]["status"], "ARMED")
        self.send(quote(2, bid="98", ask="98.1", mark="98"))
        self.assertEqual(self.trade()["status"], "CLOSED")
        self.assertTrue(self.trade()["entry_canceled"])
        self.assertEqual(D(self.trade()["exit_filled"]), D(3))

    def test_partial_exit_remains_open(self):
        self.open(); self.send(quote(2, bid="98", ask="98.1", mark="98", bid_size="2"))
        self.assertEqual(self.trade()["status"], "EXITING")
        self.send(quote(3, bid="97", ask="97.1", mark="97", bid_size="8"))
        self.assertEqual(self.trade()["status"], "CLOSED")
        self.assertEqual(D(self.trade()["exit_filled"]), D(10))

    def test_no_liquidity_no_fill(self):
        self.send(signal()); self.send(quote(ask_size="0"))
        self.assertEqual(self.trade()["entry_filled"], "0")

    def test_same_signal_retry_is_not_another_trade(self):
        e = signal(); self.send(e)
        self.assertTrue(self.send(e, at(1))["replayed"])
        self.assertEqual(len(self.broker.snapshot()["trades"]), 1)

    def test_same_id_different_content_rejected(self):
        self.send(signal()); e = signal(); e["take_profit"] = "105"
        self.assert_reject_unchanged(e)

    def test_identical_new_alerts_remain_independent(self):
        self.send(signal("s1")); self.send(signal("s2"))
        self.assertEqual([t["account"] for t in self.broker.snapshot()["trades"]], ["paper-a", "paper-b"])

    def test_opposing_alerts_routed_separately(self):
        self.send(signal("s1")); self.send(signal("s2", "SHORT"))
        self.send(quote(bid="100", ask="100"))
        self.assertTrue(all(t["status"] == "OPEN" for t in self.broker.snapshot()["trades"]))
        self.assertNotEqual(self.trade(0)["account"], self.trade(1)["account"])

    def test_full_account_pool_queues_without_dropping(self):
        for i in range(3): self.send(signal(f"s{i}"))
        self.assertEqual(self.trade(2)["status"], "QUEUED")
        self.send(quote(bid="100", ask="100"))
        self.send(quote(2, bid="104", ask="104.1", mark="104"))
        self.assertEqual(self.trade(2)["status"], "WAITING_ENTRY")
        self.assertEqual(self.trade(2)["entry_filled"], "0")

    def test_liquidity_is_shared_not_duplicated(self):
        self.send(signal("s1")); self.send(signal("s2"))
        self.send(quote(ask_size="12"))
        self.assertEqual(D(self.trade(0)["entry_filled"]), D(10))
        self.assertEqual(D(self.trade(1)["entry_filled"]), D(2))

    def test_no_retrospective_entry(self):
        self.send(signal(), at(5))
        self.send(quote(second=4), at(5))
        self.assertEqual(self.trade()["entry_filled"], "0")

    def test_stale_quote_rejected(self):
        self.send(signal())
        self.assert_reject_unchanged(quote(), at(12))

    def test_future_quote_rejected(self):
        self.assert_reject_unchanged(quote(5), at(4))

    def test_missing_timezone_rejected(self):
        e = signal(); e["at"] = "2026-01-01T00:00:00"
        self.assert_reject_unchanged(e, at(0))

    def test_quote_reordering_rejected(self):
        self.send(quote(2)); self.assert_reject_unchanged(quote(1), at(2))

    def test_duplicate_quote_cannot_fill_twice(self):
        self.send(signal()); e = quote(ask_size="2"); self.send(e)
        self.assertTrue(self.send(e, at(2))["replayed"])
        self.assertEqual(D(self.trade()["entry_filled"]), D(2))

    def test_clock_reversal_rejected(self):
        self.send(quote(2)); self.assert_reject_unchanged(signal(), at(1))

    def test_data_gap_is_flagged(self):
        self.open(); self.send(quote(60, bid="104", ask="104.1", mark="104"))
        self.assertTrue(self.trade()["data_gap"])
        self.assertFalse(self.broker.snapshot()["performance_validated"])

    def test_restart_preserves_order_and_receipt(self):
        self.open(); self.broker.close(); self.broker = PaperBroker(self.path, **self.kw)
        self.assertEqual(self.trade()["status"], "OPEN")
        self.assertTrue(self.send(signal(), at(2))["replayed"])
        self.send(quote(3, bid="104", ask="104.1", mark="104"))
        self.assertEqual(self.trade()["status"], "CLOSED")

    def test_config_change_refused(self):
        with self.assertRaises(PaperError): PaperBroker(self.path, **self.kw, risk_usd="21")

    def test_live_mode_refused(self):
        for mode in ("live", "mainnet", "testnet", "LIVE"):
            with self.subTest(mode=mode), self.assertRaises(PaperError):
                PaperBroker(self.path, **self.kw, mode=mode)

    def test_foreign_database_refused(self):
        path = Path(self.tmp.name) / "foreign.paper.sqlite3"
        with sqlite3.connect(path) as db: db.execute("CREATE TABLE important_data (id INTEGER)")
        before = path.read_bytes()
        with self.assertRaises(PaperError): PaperBroker(path, **self.kw)
        self.assertEqual(path.read_bytes(), before)

    def test_generic_database_path_refused(self):
        with self.assertRaises(PaperError): PaperBroker(Path(self.tmp.name) / "coinglass.db", **self.kw)

    def test_symlink_database_refused(self):
        path = Path(self.tmp.name) / "link.paper.sqlite3"; path.symlink_to(self.path)
        with self.assertRaises(PaperError): PaperBroker(path, **self.kw)

    def test_input_numbers_fail_closed(self):
        for value in ("NaN", "Infinity", "-1", "0", True, None, 100.0, "1e100000", "1e-99999"):
            e = signal(); e["entry"] = value
            with self.subTest(value=value): self.assert_reject_unchanged(e, at(0))

    def test_direction_and_price_validation(self):
        for patch_values in ({"side": "BUY"}, {"stop": "101"}, {"stop": "100"}, {"take_profit": "99"}, {"symbol": "BTC"}):
            e = signal(); e.update(patch_values)
            with self.subTest(values=patch_values): self.assert_reject_unchanged(e)

    def test_missing_price_is_never_inferred(self):
        e = signal(); del e["stop"]
        self.assert_reject_unchanged(e)

    def test_extra_fields_or_secret_not_stored(self):
        e = signal(); e["private_key"] = "DO_NOT_STORE"
        self.assert_reject_unchanged(e)
        self.assertNotIn(b"DO_NOT_STORE", self.path.read_bytes())

    def test_crossed_book_rejected(self):
        self.assert_reject_unchanged(quote(bid="102", ask="101"))

    def test_zero_size_after_rounding_rejected(self):
        e = signal(); e.update(entry="100000", stop="90000", take_profit="110000")
        self.assert_reject_unchanged(e)

    def test_close_cannot_reopen_or_flip(self):
        self.open(); self.send(quote(2, bid="98", ask="98.1", mark="98"))
        original = self.trade()
        self.send(quote(3, bid="105", ask="106", mark="105"))
        self.assertEqual(self.trade(), original)

    def test_atomic_rollback_when_processing_fails(self):
        self.send(signal()); before = self.broker.snapshot()
        with patch.object(self.broker, "_advance", side_effect=RuntimeError("simulated interruption")):
            with self.assertRaises(RuntimeError): self.send(quote())
        self.assertEqual(before, self.broker.snapshot())
        self.assertFalse(self.send(quote())["replayed"])
        self.assertEqual(self.trade()["status"], "OPEN")

    def test_two_connections_deduplicate(self):
        other = PaperBroker(self.path, **self.kw)
        try:
            self.send(signal())
            self.assertTrue(other.process(signal(), received_at=at(1))["replayed"])
            self.assertEqual(len(other.snapshot()["trades"]), 1)
        finally: other.close()

    def test_fees_recorded_as_explicit_estimate(self):
        self.open(); self.send(quote(2, bid="104", ask="104.1", mark="104"))
        self.assertEqual(D(self.trade()["estimated_fees_usd"]), D("1.02"))

    def test_demo_has_no_network_and_no_credentials(self):
        report = demo(Path(self.tmp.name) / "demo")
        self.assertEqual(report["snapshot"]["network_calls"], 0)
        self.assertEqual(report["snapshot"]["trades"][0]["status"], "CLOSED")
        self.assertTrue(report["duplicate_result"]["replayed"])
        with self.assertRaises(FileExistsError): demo(Path(self.tmp.name) / "demo")


if __name__ == "__main__":
    unittest.main(verbosity=2)
