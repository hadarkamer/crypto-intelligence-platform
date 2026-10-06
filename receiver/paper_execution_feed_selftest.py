"""Offline contract/integration tests. All market data is synthetic, not live."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import http.client
import json
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import patch

from paper_execution import PaperError
import paper_execution_feed as feed_module
from paper_execution_feed import (
    PaperFeed, PublicInfo, asset_contexts, book_quote, check_prices,
    decode, intake_once, publish_signal, read_inbox, signal_message,
)

BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def signal(eid="s1", side="LONG"):
    return {"kind": "SIGNAL", "event_id": eid, "symbol": "DEMO", "side": side,
            "entry": "100", "stop": "98" if side == "LONG" else "102",
            "take_profit": "104" if side == "LONG" else "96", "at": BASE.isoformat()}


class Clock:
    second = 0
    def __call__(self):
        return BASE + timedelta(seconds=self.second)


class FakeInfo:
    """Documented /info shapes with invented asset, prices, times and sizes."""
    def __init__(self, clock):
        self.calls, self.requests, self.clock = 0, [], clock
        self.mark, self.bid, self.ask = "100", "99.9", "100"
        self.bid_size, self.ask_size = "100", "100"
        self.decimals, self.failure, self.book_second = 2, None, None

    def read(self, kind, symbol=None):
        self.calls += 1
        self.requests.append((kind, symbol))
        if self.failure == kind:
            raise PaperError("Synthetic connection unavailable")
        if kind == "metaAndAssetCtxs":
            return [{"universe": [{"name": "DEMO", "szDecimals": self.decimals}]}, [{"markPx": self.mark}]]
        if kind != "l2Book":
            raise AssertionError("Unexpected operation")
        second = self.clock.second if self.book_second is None else self.book_second
        return {"coin": symbol, "time": int((BASE+timedelta(seconds=second)).timestamp()*1000),
                "levels": [[{"px": self.bid, "sz": self.bid_size, "n": 1}],
                           [{"px": self.ask, "sz": self.ask_size, "n": 1}]]}


class FeedTests(unittest.TestCase):
    def setUp(self):
        self.net = patch.object(socket, "socket", side_effect=AssertionError("Network forbidden in tests"))
        self.net.start()
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name)
        self.clock = Clock()
        self.info = FakeInfo(self.clock)
        self.kw = dict(symbols=("DEMO",), accounts=("paper-a", "paper-b"), fee_bps="5",
                       info=self.info, clock=self.clock)
        self.feed = PaperFeed(self.path / "test.paper.sqlite3", **self.kw)

    def tearDown(self):
        self.feed.close()
        self.tmp.cleanup()
        self.net.stop()

    def trade(self, index=0):
        return self.feed.broker.snapshot()["trades"][index]

    def poll(self, second):
        self.clock.second = second
        return self.feed.poll_once()

    def test_end_to_end_bot_file_prices_close(self):
        inbox = self.path / "input.paper.jsonl"
        publish_signal(inbox, signal())
        self.assertEqual(intake_once(self.feed, inbox)["accepted"], 1)
        self.assertEqual(self.trade()["status"], "WAITING_ENTRY")
        self.assertEqual([o["price"] for o in self.trade()["orders"]], ["100", "98", "104"])
        self.info.ask_size = "4"
        self.poll(1)
        self.assertEqual(Decimal(self.trade()["entry_filled"]), 4)
        self.info.ask_size = "6"
        self.poll(2)
        self.info.mark, self.info.bid, self.info.ask = "104", "104", "104.1"
        self.poll(3)
        self.assertEqual(self.trade()["status"], "CLOSED")
        self.assertEqual(self.trade()["exit_reason"], "TAKE_PROFIT")
        self.assertEqual(intake_once(self.feed, inbox)["replayed"], 1)
        self.assertEqual(len(self.feed.broker.snapshot()["trades"]), 1)
        self.assertTrue(all(f["simulated"] for f in self.trade()["fills"]))
        self.assertEqual(self.feed.snapshot()["exchange_orders_sent"], 0)

    def test_short_stop_end_to_end(self):
        self.feed.submit_signal(json.dumps(signal(side="SHORT")))
        self.info.bid = "100"
        self.poll(1)
        self.info.mark, self.info.bid, self.info.ask = "102", "101.9", "102"
        self.poll(2)
        self.assertEqual(self.trade()["exit_reason"], "STOP")
        self.assertEqual(self.trade()["status"], "CLOSED")

    def test_no_signal_no_price_poll(self):
        before = self.info.calls
        self.assertEqual(self.feed.poll_once()["results"], [])
        self.assertEqual(self.info.calls, before)

    def test_new_repeated_and_opposing_signals_are_distinct(self):
        for event in (signal("s1"), signal("s2", "SHORT"), signal("s3")):
            self.feed.submit_signal(event)
        self.assertEqual([self.trade(i)["account"] for i in range(3)], ["paper-a", "paper-b", None])
        self.assertEqual(self.trade(2)["status"], "QUEUED")

    def test_restart_deduplicates_input_and_book(self):
        self.feed.submit_signal(signal())
        self.info.ask_size = "2"
        self.poll(1)
        self.feed.close()
        self.feed = PaperFeed(self.path / "test.paper.sqlite3", **self.kw)
        self.assertTrue(self.feed.submit_signal(signal())["replayed"])
        self.assertEqual(self.feed.poll_once()["results"][0]["status"], "NO_NEW_BOOK")
        self.assertEqual(Decimal(self.trade()["entry_filled"]), 2)

    def test_same_id_changed_input_rejected(self):
        self.feed.submit_signal(signal())
        e = signal(); e["entry"] = "101"
        with self.assertRaises(PaperError): self.feed.submit_signal(e)
        self.assertEqual(len(self.feed.broker.snapshot()["trades"]), 1)

    def test_incomplete_or_secret_input_never_stored(self):
        for change in ({"private_key": "not-a-real-key"}, {"received_at": BASE.isoformat()}, {"mode": "live"}):
            event = {**signal(), **change}
            with self.assertRaises(PaperError): self.feed.submit_signal(event)
        for field in ("entry", "stop", "take_profit"):
            event = signal(); del event[field]
            with self.assertRaises(PaperError): self.feed.submit_signal(event)
        self.assertEqual(self.feed.broker.snapshot()["trades"], [])
        self.assertNotIn(b"not-a-real-key", (self.path / "test.paper.sqlite3").read_bytes())

    def test_malformed_field_types_fail_closed(self):
        for key, value in (("symbol", []), ("event_id", None), ("side", []),
                           ("at", None), ("entry", 100.0), ("stop", "NaN")):
            with self.subTest(key=key), self.assertRaises(PaperError):
                self.feed.submit_signal({**signal(), key: value})

    def test_quote_injection_rejected(self):
        with self.assertRaises(PaperError):
            self.feed.submit_signal({"kind": "QUOTE", "mark": "104"})
        with self.assertRaises(PaperError):
            self.feed.submit_signal(signal("hlq.DEMO.123"))

    def test_precision_not_rounded(self):
        with self.assertRaises(PaperError):
            self.feed.submit_signal({**signal(), "entry": "100.001"})
        self.assertEqual(self.feed.broker.snapshot()["trades"], [])
        check_prices(signal(), 6)  # integer prices allowed even with 0 decimal places

    def test_unknown_symbol_not_substituted(self):
        with self.assertRaises(PaperError):
            self.feed.submit_signal({**signal(), "symbol": "ZEC"})

    def test_future_signal_rejected(self):
        self.clock.second = -1
        with self.assertRaises(PaperError): self.feed.submit_signal(signal())

    def test_stale_future_crossed_and_empty_book_withhold(self):
        self.feed.submit_signal(signal())
        self.info.book_second = 0
        before = self.feed.broker.snapshot()
        self.assertEqual(self.poll(11)["results"][0]["status"], "WITHHELD")
        self.info.book_second = 13
        self.assertEqual(self.poll(12)["results"][0]["status"], "WITHHELD")
        self.info.book_second = None
        self.info.bid = "101"
        self.assertEqual(self.poll(14)["results"][0]["status"], "WITHHELD")
        self.assertEqual(self.feed.broker.snapshot(), before)
        book = FakeInfo(self.clock).read("l2Book", "DEMO")
        book["levels"][0] = []
        with self.assertRaises(PaperError): book_quote(book, "DEMO", "100", self.clock())

    def test_no_old_cached_price_during_outage_and_recovery(self):
        self.feed.submit_signal(signal())
        self.poll(1)
        before = self.feed.broker.snapshot()
        self.info.failure = "metaAndAssetCtxs"
        self.assertEqual(self.poll(2)["results"][0]["status"], "WITHHELD")
        self.assertEqual(self.feed.broker.snapshot(), before)
        self.info.failure = None
        self.info.mark, self.info.bid, self.info.ask = "104", "104", "104.1"
        self.poll(40)
        self.assertEqual(self.trade()["status"], "CLOSED")
        self.assertTrue(self.trade()["data_gap"])
        self.assertFalse(self.feed.snapshot()["snapshot"]["performance_validated"])

    def test_missing_mark_never_replaced_by_mid(self):
        self.feed.submit_signal(signal())
        self.info.mark = None
        self.assertEqual(self.poll(1)["results"][0]["status"], "WITHHELD")
        self.assertEqual(self.trade()["entry_filled"], "0")

    def test_changed_precision_withholds(self):
        self.feed.submit_signal(signal())
        self.info.decimals = 3
        self.assertEqual(self.poll(1)["results"][0]["status"], "WITHHELD")

    def test_duplicate_book_new_mark_cannot_reuse_liquidity(self):
        self.feed.submit_signal(signal())
        self.info.ask_size = "2"
        self.poll(1)
        self.info.mark = "104"
        self.info.book_second = 1
        self.assertEqual(self.poll(2)["results"][0]["status"], "NO_NEW_BOOK")
        self.assertEqual(Decimal(self.trade()["entry_filled"]), 2)
        self.assertIsNone(self.trade()["exit_reason"])

    def test_slow_acquisition_withholds(self):
        self.feed.submit_signal(signal())
        with patch.object(feed_module.time, "monotonic", side_effect=[0, 11]):
            self.assertEqual(self.poll(1)["results"][0]["status"], "WITHHELD")
        self.assertEqual(self.trade()["entry_filled"], "0")

    def test_metadata_alignment_delisted_missing_duplicate(self):
        raw = self.info.read("metaAndAssetCtxs")
        variants = [[], [raw[0], []], [{"universe": [raw[0]["universe"][0]]}, [{"markPx": "NaN"}]]]
        x = deepcopy(raw); x[0]["universe"][0]["isDelisted"] = True; variants.append(x)
        x = deepcopy(raw); x[0]["universe"] *= 2; x[1] *= 2; variants.append(x)
        x = deepcopy(raw); x[0]["universe"][0]["szDecimals"] = True; variants.append(x)
        for value in variants:
            with self.subTest(value=value), self.assertRaises(PaperError):
                asset_contexts(value, ("DEMO",))

    def test_json_duplicate_nonfinite_and_extra_fields_rejected(self):
        for raw in ('{"x":1,"x":2}', '{"x":NaN}', b'\xff', '[]', 'null'):
            with self.subTest(raw=raw), self.assertRaises(PaperError): signal_message(raw)

    def test_inbox_partial_tail_and_rejected_row(self):
        path = self.path / "input.paper.jsonl"
        publish_signal(path, signal())
        with path.open("ab") as out: out.write(b'{"not":"a plan"}\n{"kind":')
        self.assertEqual(intake_once(self.feed, path), {"accepted": 1, "replayed": 0, "rejected": 1})
        self.assertEqual(len(read_inbox(path)), 2)

    def test_private_publisher_and_no_side_effect_for_invalid_plan(self):
        path = self.path / "input.paper.jsonl"
        event = signal(); del event["stop"]
        with self.assertRaises(PaperError): publish_signal(path, event)
        self.assertFalse(path.exists())
        publish_signal(path, signal())
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        path.chmod(0o644)
        with self.assertRaises(PaperError): publish_signal(path, signal("s2"))

    def test_file_limits_symlinks_and_fifo(self):
        path = self.path / "input.paper.jsonl"
        path.write_bytes(b'{}\n' * 257)
        with self.assertRaises(PaperError): read_inbox(path)
        path.write_bytes(b'x' * (feed_module.MAX_INPUT + 1))
        with self.assertRaises(PaperError): read_inbox(path)
        link = self.path / "link.paper.jsonl"; link.symlink_to(path)
        with self.assertRaises(OSError): read_inbox(link)
        with self.assertRaises(OSError): publish_signal(link, signal())
        fifo = self.path / "fifo.paper.jsonl"; feed_module.os.mkfifo(fifo)
        with self.assertRaises(PaperError): read_inbox(fifo)

    def test_real_client_requires_opt_in(self):
        with self.assertRaises(PaperError): PublicInfo()

    def test_http_allowlist_payload_and_fixed_destination(self):
        with patch.object(http.client, "HTTPSConnection") as factory:
            conn = factory.return_value
            conn.getresponse.return_value.status = 200
            conn.getresponse.return_value.read.return_value = b'{}'
            client = PublicInfo(allow_public_reads=True)
            client.read("l2Book", "DEMO")
            factory.assert_called_with("api.hyperliquid.xyz", timeout=4)
            args = conn.request.call_args.args
            self.assertEqual(args[:2], ("POST", "/info"))
            self.assertEqual(decode(args[2]), {"type": "l2Book", "coin": "DEMO"})
            for kind, symbol in (("exchange", None), ("order", None), ("userFills", "DEMO"),
                                 ("l2Book", "../exchange"), ("metaAndAssetCtxs", "DEMO")):
                with self.assertRaises(PaperError): client.read(kind, symbol)
            self.assertEqual(client.calls, 1)
            self.assertEqual(conn.request.call_count, 1)
            conn.close.assert_called_once()

    def test_http_redirect_error_timeout_and_oversize(self):
        with patch.object(http.client, "HTTPSConnection") as factory:
            conn = factory.return_value
            client = PublicInfo(allow_public_reads=True)
            for status in (301, 307, 429, 500):
                conn.getresponse.return_value.status = status
                with self.assertRaises(PaperError): client.read("metaAndAssetCtxs")
            conn.getresponse.return_value.status = 200
            conn.getresponse.return_value.read.return_value = b'x' * (feed_module.MAX_RESPONSE+1)
            with self.assertRaises(PaperError): client.read("metaAndAssetCtxs")
            conn.request.side_effect = TimeoutError("raw response or URL must not be logged")
            with self.assertRaisesRegex(PaperError, "connection unavailable"):
                client.read("metaAndAssetCtxs")
            self.assertEqual(conn.close.call_count, 6)

    def test_information_requests_never_contain_prices_or_accounts(self):
        self.feed.submit_signal(signal())
        self.poll(1)
        for kind, symbol in self.info.requests:
            self.assertIn(kind, ("metaAndAssetCtxs", "l2Book"))
            self.assertIn(symbol, (None, "DEMO"))
        self.assertFalse(self.feed.snapshot()["mark_source_timestamp_available"])
        self.assertEqual(self.feed.snapshot()["snapshot"]["network_calls"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
