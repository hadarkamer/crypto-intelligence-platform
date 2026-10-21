"""Explicit artificial fixtures test prospective causality, never historical data."""
import copy
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from research_forward_paper_engine import PaperStudy


BASE = datetime(2026, 10, 21, tzinfo=timezone.utc)


def stamp(seconds):
    return (BASE + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "study.sqlite"
        self.policy = json.loads((Path(__file__).parent / "forward_paper_runtime" / "policy_template.json").read_text())
        self.policy["study_start_utc"] = stamp(0)
        self.study = PaperStudy(self.path, self.policy)
        self.addCleanup(self.temp.cleanup)
        self.addCleanup(lambda: self.study.close())
        self.study.observe_candidates(self.packet("boot", 0.1, [], bootstrap=True))

    def packet(self, ident, received, rows, bootstrap=False):
        return {"poll_id": ident, "request_started_utc": stamp(received - 0.01),
                "response_received_utc": stamp(received), "rows": rows,
                "bootstrap": bootstrap, "source": "psycopg"}

    def row(self, event=1, parent="p1", symbol="DOGE", alert=1, parent_start=0.5):
        scope = next(s for s in self.policy["scopes"] if s["symbol"] == symbol)
        result = {"scope_key": scope["scope_key"], "candidate_key": scope["candidate_key"],
                  "candidate_definition_sha256": scope["candidate_definition_sha256"],
                  "candidate_formula_version": scope["formula_version"],
                  "event_id": event, "source_event_id": event, "symbol": symbol,
                  "formula_direction": scope["direction"], "event_kind": "ALERT",
                  "delivery_status": "DELIVERED", "membership_status": "LIVE",
                  "parent_evidence_eligible": True, "btc_parent_movement_id": parent,
                  "parent_start_time_utc": stamp(parent_start), "match_source_time_utc": stamp(alert)}
        result.update({key: stamp(alert) for key in self.policy["required_markers"]})
        result["parent_confirmed_at_utc"] = stamp(parent_start)
        result["btc_observed_close_utc"] = stamp(alert - 0.2)
        return result

    def candidates(self, received, rows, ident=None):
        return self.study.observe_candidates(self.packet(ident or f"p{received}", received, rows))

    def quotes(self, received, price=100, symbol="DOGE", started=None, ident=None, **extra):
        request = received - 0.1 if started is None else started
        packet = {"packet_id": ident or f"q{received}", "request_started_utc": stamp(request),
                  "response_received_utc": stamp(received),
                  "quotes": [{"symbol": symbol, "bid": str(price), "ask": str(price)}],
                  "source": "BINANCE_SPOT_BOOK_TICKER", **extra}
        return self.study.observe_quotes(packet)

    def book(self, name="DOGE_only"):
        return self.study.summary()["books"][name]

    def open_doge(self):
        self.candidates(2, [self.row()])
        self.quotes(61, started=60)

    def test_bootstrap_and_stale_parent_never_trade(self):
        # A separate fresh bootstrap can contain a post-start parent and is still excluded.
        other_path = Path(self.temp.name) / "bootstrap.sqlite"
        other = PaperStudy(other_path, self.policy)
        try:
            other.observe_candidates(self.packet("first", 2, [self.row()], bootstrap=True))
            other.observe_candidates(self.packet("again", 3, [self.row(event=2)], bootstrap=False))
            self.assertFalse(other.summary()["books"]["DOGE_only"]["choices"])
        finally:
            other.close()
        self.candidates(2, [self.row(parent_start=0), self.row(event=2, parent="older", parent_start=-10)])
        self.assertFalse(self.book()["choices"])

    def test_incomplete_then_eligible_uses_later_receipt(self):
        row = self.row()
        incomplete = dict(row)
        incomplete.pop("match_created_at_utc")
        self.candidates(2, [incomplete])
        self.assertFalse(self.book()["choices"])
        self.assertEqual(self.study.eligible_event_keys(), [])
        self.candidates(65, [row])
        self.assertEqual(self.book()["choices"]["p1"]["selected_at_utc"], stamp(65))
        self.assertEqual(self.book()["choices"]["p1"]["planned_entry_utc"], stamp(120))
        keys = self.study.eligible_event_keys()
        self.assertEqual(keys, [row["scope_key"] + ":1"])
        self.study.close()
        self.study = PaperStudy(self.path, self.policy)
        self.assertEqual(self.study.eligible_event_keys(), keys)

    def test_late_backfill_does_not_replace_winner(self):
        self.candidates(2, [self.row(event=100)])
        self.candidates(3, [self.row(event=2, alert=0.9)])
        self.assertEqual(self.book()["choices"]["p1"]["event_id"], 100)
        self.assertEqual(self.study.summary()["first_eligible_seen_count"], 2)

    def test_same_poll_marker_tie_and_separate_books(self):
        doge = self.row(event=100, alert=1.1)
        eth = self.row(event=200, symbol="ETH", alert=1)
        self.candidates(2, [doge, eth])
        self.assertEqual(self.book("first_per_parent")["choices"]["p1"]["symbol"], "ETH")
        self.assertEqual(self.book()["choices"]["p1"]["symbol"], "DOGE")
        self.assertEqual(self.book("ETH_only")["choices"]["p1"]["symbol"], "ETH")

    def test_preplanned_request_arriving_late_cannot_enter(self):
        self.candidates(2, [self.row()])
        self.quotes(61, started=59)
        self.assertIsNone(self.book()["active"])
        self.assertEqual(self.book()["reservation"], "p1")
        self.quotes(62, started=61)
        self.assertEqual(self.book()["active"]["entry_received_utc"], stamp(62))

    def test_duplicate_and_conflicting_input_identity(self):
        packet = self.packet("same", 2, [self.row()])
        self.study.observe_candidates(packet)
        count = self.study.summary()["journal_events"]
        result = self.study.observe_candidates(packet)
        self.assertTrue(result["idempotent_duplicate"])
        self.assertEqual(result["journal_events"], count)
        changed = dict(packet, rows=[])
        with self.assertRaises(ValueError):
            self.study.observe_candidates(changed)

    def test_pending_reservation_blocks_no_fallback(self):
        self.candidates(2, [self.row()])
        self.quotes(61, started=59)  # Reserved, no qualifying quote yet.
        self.candidates(65, [self.row(event=2, parent="p2", alert=64, parent_start=63)])
        self.quotes(121, started=115)  # Slow response cannot fill p1; p2 gate120 remains blocked.
        self.assertEqual(self.book()["choices"]["p2"]["status"], "BLOCKED_CAPACITY")
        self.quotes(152, started=151)
        self.assertEqual(self.book()["choices"]["p1"]["status"], "EXPIRED_NO_ENTRY_QUOTE")
        self.candidates(153, [self.row(event=3, parent="p2", alert=152, parent_start=63)])
        self.assertEqual(self.book()["choices"]["p2"]["event_id"], 2)
        self.assertIsNone(self.book()["active"])

    def test_later_exit_cannot_free_earlier_planned_capacity(self):
        self.open_doge()
        self.candidates(65, [self.row(event=2, parent="p2", alert=64, parent_start=63)])
        self.quotes(121, price=99.4, started=120)
        self.assertEqual(self.book()["choices"]["p2"]["status"], "BLOCKED_CAPACITY")
        self.assertEqual(self.book()["closed"][0]["status"], "OBSERVED_TARGET")

    def test_free_at_plan_can_enter_despite_busy_at_decision(self):
        self.open_doge()
        self.candidates(65, [self.row(event=2, parent="p2", alert=64, parent_start=63)])
        self.quotes(70, price=99.4, started=69)
        self.quotes(121, started=120)
        self.assertEqual(self.book()["active"]["parent_id"], "p2")

    def test_timeout_equality_releases_before_equal_planned_entry(self):
        self.candidates(2, [self.row(symbol="ETH")])
        self.quotes(60, symbol="ETH", started=60)
        self.candidates(3659, [self.row(event=2, parent="p2", symbol="ETH", alert=3658, parent_start=3657)])
        self.quotes(3660, symbol="ETH", started=3660)
        book = self.book("ETH_only")
        self.assertEqual(book["closed"][0]["status"], "OBSERVED_TIMEOUT")
        self.assertAlmostEqual(float(book["closed"][0]["net_return_pct"]), -0.1)
        self.assertEqual(book["active"]["parent_id"], "p2")

    def test_timeout_needs_request_after_deadline(self):
        self.candidates(2, [self.row(symbol="ETH")])
        self.quotes(61, symbol="ETH", started=60)
        self.quotes(3662, price=101, symbol="ETH", started=3660)
        self.assertIsNotNone(self.book("ETH_only")["active"])
        self.quotes(3663, price=100.2, symbol="ETH", started=3662)
        row = self.book("ETH_only")["closed"][0]
        self.assertEqual(row["status"], "OBSERVED_TIMEOUT")
        self.assertAlmostEqual(float(row["net_return_pct"]), 0.1)

    def test_gap_and_restart_flag_all_totals_retain_trade(self):
        self.open_doge()
        before = self.study.summary()["journal_events"]
        status_reader = PaperStudy(self.path, self.policy, resume=False)
        self.assertFalse(status_reader.summary()["books"]["DOGE_only"]["active"]["path_uncertain"])
        self.assertEqual(status_reader.summary()["journal_events"], before)
        status_reader.close()
        self.study.close()
        self.study = PaperStudy(self.path, self.policy)
        self.assertTrue(self.book()["active"]["path_uncertain"])
        self.quotes(100, price=99.4, started=99)
        book = self.book()
        self.assertEqual(book["completed_all"]["n"], 1)
        self.assertEqual(book["completed_path_uncertain"]["n"], 1)
        self.assertIn("QUOTE_GAP", book["closed"][0]["uncertainty_reasons"])
        self.assertIn("PROCESS_RESTART", book["closed"][0]["uncertainty_reasons"])

    def test_slow_monotonic_and_crossed_quotes_do_not_fill(self):
        self.candidates(2, [self.row()])
        self.quotes(66, started=60)  # wall elapsed6>5
        self.quotes(67, started=66, monotonic_elapsed_seconds=6)
        bad = {"packet_id": "crossed", "request_started_utc": stamp(68), "response_received_utc": stamp(69),
               "source": "BINANCE_SPOT_BOOK_TICKER", "quotes": [{"symbol": "DOGE", "bid": 101, "ask": 100}]}
        self.study.observe_quotes(bad)
        self.assertIsNone(self.book()["active"])
        self.quotes(70, started=69, monotonic_elapsed_seconds=0.1)
        self.assertIsNotNone(self.book()["active"])

    def test_policy_pin_and_source_definition_drift(self):
        altered = copy.deepcopy(self.policy)
        altered["net_cost_pp"] = 0.11
        with self.assertRaises(ValueError):
            PaperStudy(self.path, altered)
        row = self.row()
        row["candidate_definition_sha256"] = "changed"
        self.candidates(2, [row])
        self.assertFalse(self.book()["choices"])
        self.assertEqual(self.study.db.execute("SELECT COUNT(*) FROM journal WHERE kind='POLICY_SOURCE_DRIFT'").fetchone()[0], 1)

    def test_journal_is_append_only_and_time_reversal_rejected(self):
        self.candidates(2, [self.row()])
        result = self.candidates(1.5, [self.row(event=2)])
        self.assertEqual(result["last_observation_rejected"], "Response time reversal")
        self.assertEqual(result["last_response_utc"], stamp(2))
        with self.assertRaises(sqlite3.IntegrityError):
            self.study.db.execute("DELETE FROM journal")

    def test_both_directions_targets_stops_charge_cost_once(self):
        cases = [("DOGE", 99.4, "OBSERVED_TARGET", 0.5),
                 ("DOGE", 100.6, "OBSERVED_STOP", -0.7),
                 ("ETH", 100.6, "OBSERVED_TARGET", 0.5),
                 ("ETH", 99.4, "OBSERVED_STOP", -0.7)]
        for index, (symbol, price, expected_status, expected_net) in enumerate(cases):
            with self.subTest(symbol=symbol, expected_status=expected_status):
                old = self.study
                self.study = PaperStudy(Path(self.temp.name) / f"direction{index}.sqlite", self.policy)
                try:
                    self.study.observe_candidates(self.packet("boot", 0.1, [], bootstrap=True))
                    self.candidates(2, [self.row(symbol=symbol)])
                    self.quotes(61, symbol=symbol, started=60)
                    self.quotes(62, price=price, symbol=symbol, started=61)
                    trade = self.book(symbol + "_only")["closed"][0]
                    self.assertEqual(trade["status"], expected_status)
                    self.assertAlmostEqual(float(trade["net_return_pct"]), expected_net)
                    self.assertEqual(trade["modeled_cost_pp"], "0.1")
                finally:
                    self.study.close()
                    self.study = old

    def test_entry_crash_rolls_back_input_and_fill_atomically(self):
        self.candidates(2, [self.row()])
        original_emit = self.study._emit
        def crash(kind, *args, **kwargs):
            if kind == "PAPER_ENTRY":
                raise RuntimeError("simulated crash before commit")
            return original_emit(kind, *args, **kwargs)
        self.study._emit = crash
        with self.assertRaises(RuntimeError):
            self.quotes(61, started=60, ident="retryable")
        self.study._emit = original_emit
        self.assertIsNone(self.book()["active"])
        self.assertEqual(self.study.db.execute("SELECT COUNT(*) FROM journal WHERE kind='PAPER_ENTRY'").fetchone()[0], 0)
        self.quotes(61, started=60, ident="retryable")
        count = self.study.db.execute("SELECT COUNT(*) FROM journal WHERE kind='PAPER_ENTRY'").fetchone()[0]
        self.assertEqual(count, 2)  # DOGE-only and combined research books.
        self.quotes(61, started=60, ident="retryable")
        self.assertEqual(self.study.db.execute("SELECT COUNT(*) FROM journal WHERE kind='PAPER_ENTRY'").fetchone()[0], count)

    def test_explicit_candidate_key_is_required(self):
        row = self.row()
        row.pop("candidate_key")
        self.candidates(2, [row])
        self.assertFalse(self.book()["choices"])
        self.assertEqual(self.study.eligible_event_keys(), [])

    def test_parent_start_confirmation_and_alert_order_is_causal(self):
        for index, start in enumerate((100, 1.5, 0.75)):
            with self.subTest(parent_start=start):
                row = self.row(event=index + 1, parent=f"bad{index}", parent_start=start, alert=1)
                row["parent_confirmed_at_utc"] = stamp(0.5)
                self.candidates(2 + index, [row])
        self.assertFalse(self.book()["choices"])
        self.assertEqual(self.study.eligible_event_keys(), [])
        self.candidates(5, [self.row(event=10, parent="valid", parent_start=0.5, alert=1)])
        self.assertIn("valid", self.book()["choices"])


if __name__ == "__main__":
    unittest.main()
