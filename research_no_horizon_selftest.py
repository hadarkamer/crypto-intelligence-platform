"""Semantic regressions for the isolated research contract, stream and gate."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import unittest

import research_no_horizon_contract as contracts
import research_no_horizon_first_touch as touch
import research_no_horizon_gate as gate

T = datetime(2026, 9, 22, tzinfo=timezone.utc)


def contract(**changes):
    values = dict(candidate_id="F1", candidate_version="v1", cohort_id="frozen-cohort",
        dataset_id="snapshot-sha256", entry_id="e1", symbol="XRP", direction="LONG",
        decision_time_utc=T, reference_price=100, threshold_pct=1,
        source_route=dict(exchange="binance", market="spot", instrument="XRPUSDT", price_type="trade", interval_seconds=60),
        parent_policy_version="btc-parent-close-reversal-200bps-v1")
    return contracts.make_contract(**{**values, **changes})


def bar(i=0, **prices):
    return {"open_time_utc": T+timedelta(minutes=i), "close_time_utc": T+timedelta(minutes=i+1),
        **{"open": 100, "high": 100.5, "low": 99.5, "close": 100}, **prices}


def run(path, c=None, cutoff=100, **kwargs):
    return touch.evaluate(c or contract(), path, cutoff_utc=T+timedelta(minutes=cutoff), **kwargs)


def row(i, *, status="SUCCESS", parent=None, decision=None):
    c = contract(entry_id="e"+str(i), decision_time_utc=decision or T)
    minute = int((contracts.utc(c["entry_time_utc"])-T).total_seconds()/60)
    outcome = run([bar(minute,high=102)] if status == "SUCCESS" else [bar(minute,low=98)] if status == "FAILURE" else [bar(minute)], c)
    if status == "MISSING":
        outcome = None
    return {"contract": c, "outcome": outcome, "btc_parent_movement_id": parent or "p"+str(i),
        "membership_status": "LIVE", "parent_evidence_eligible": True,
        "parent_start_time_utc": T.isoformat(), "parent_confirmed_at_utc": T.isoformat(),
        "features_observed_at_utc": T.isoformat()}


def qualify(rows, **kwargs):
    return gate.evaluate_gate(rows, as_of_utc=T+timedelta(minutes=100), source_coverage_complete=True, **kwargs)


class ContractTests(unittest.TestCase):
    def test_contract_roundtrip_scope_and_version(self):
        c = contract()
        self.assertEqual(c, contracts.validate_contract(json.loads(json.dumps(c))))
        self.assertIsNone(c["outcome_horizon_minutes"])
        self.assertEqual(contracts.scope_identity(c), contracts.scope_identity(contract(entry_id="e2", reference_price=101)))
        changed = dict(c, threshold_pct=2)
        with self.assertRaises(ValueError): contracts.validate_contract(changed)

    def test_invalid_numbers_times_and_route(self):
        for field, value in [("threshold_pct", 0), ("threshold_pct", 100), ("threshold_pct", float("nan")),
                             ("reference_price", 0), ("reference_price", True), ("decision_time_utc", T.replace(tzinfo=None)),
                             ("source_route", {"exchange": "binance"})]:
            with self.subTest(field=field, value=value), self.assertRaises(ValueError): contract(**{field: value})

    def test_midminute_reference_not_silently_used(self):
        c = contract(decision_time_utc=T+timedelta(seconds=30))
        self.assertEqual(c["entry_time_utc"], (T+timedelta(minutes=1)).isoformat())
        result = run([bar(1, open=101, high=102, close=101)], c)
        self.assertEqual(result["status"], "BLOCKED_ENTRY")
        self.assertFalse(result["entry_verified"])
        valid = contract(decision_time_utc=T+timedelta(seconds=30), reference_price=101)
        self.assertEqual(run([bar(1, open=101, low=100.8, high=101.2, close=101)], valid)["status"], "OPEN")


class StreamTests(unittest.TestCase):
    def test_success_after_sixty_minutes(self):
        result = run([bar(i) for i in range(70)]+[bar(70, high=102)])
        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(result["processed_candles"], 71)

    def test_adverse_first_cannot_be_replaced(self):
        result = run([bar(low=98), bar(1, high=103)])
        self.assertEqual(result["status"], "FAILURE")
        self.assertEqual(result["processed_candles"], 1)

    def test_ambiguous_same_candle(self):
        result = run([bar(high=102, low=98)])
        self.assertEqual(result["status"], "AMBIGUOUS")
        self.assertIsNone(result["success"])

    def test_open_gap_is_known_before_other_extreme(self):
        for direction, opened, expected in [("LONG",102,"SUCCESS"),("LONG",98,"FAILURE"),
                                             ("SHORT",98,"SUCCESS"),("SHORT",102,"FAILURE")]:
            result = run([bar(),bar(1,open=opened, high=103,low=97)],contract(direction=direction))
            self.assertEqual(result["status"],expected)
            self.assertEqual(result["decision_precision"],"OPEN")
            self.assertEqual(result["decision_time_utc"],(T+timedelta(minutes=1)).isoformat())

    def test_gap_before_touch_blocks_but_after_does_not(self):
        self.assertEqual(run([bar(),bar(2,high=102)])["status"],"DATA_MISSING")
        self.assertEqual(run([bar(high=102),bar(2)])["status"],"SUCCESS")

    def test_gap_is_repairable_without_losing_prefix(self):
        gap = run([bar(),bar(2,high=102)])
        resumed = touch.advance(gap,[bar(1),bar(2,high=102)],cutoff_utc=T+timedelta(minutes=100))
        self.assertEqual(resumed,run([bar(),bar(1),bar(2,high=102)]))

    def test_no_touch_cutoff_is_open(self):
        result=run([bar()],cutoff=1)
        self.assertEqual(result["status"],"OPEN")
        self.assertTrue(result["coverage_complete_through_cutoff"])
        self.assertIsNone(result["success"])

    def test_incomplete_supplied_chunk_discloses_backlog(self):
        result=run([bar()],cutoff=2)
        self.assertEqual(result["status"],"OPEN")
        self.assertEqual(result["progress"],"MORE_DATA_REQUIRED")
        self.assertFalse(result["coverage_complete_through_cutoff"])

    def test_no_lookahead_even_in_future_prices(self):
        result=run([bar(),bar(1,high=float("nan"))],cutoff=1)
        self.assertEqual(result["status"],"OPEN")
        self.assertEqual(result["processed_candles"],1)

    def test_checkpoint_chunk_and_json_parity(self):
        path=[bar(i) for i in range(10)]+[bar(10,high=102)]
        one=run(path)
        state=run(path,max_candles=3)
        self.assertEqual(state["processed_candles"],3)
        state=touch.advance(json.loads(json.dumps(state)),path[3:7],cutoff_utc=T+timedelta(minutes=100))
        state=touch.advance(state,path[7:],cutoff_utc=T+timedelta(minutes=100))
        self.assertEqual(state,one)

    def test_cutoff_advance_and_terminal_reuse(self):
        state=run([bar()],cutoff=1)
        state=touch.advance(state,[bar(1,high=102)],cutoff_utc=T+timedelta(minutes=2))
        self.assertEqual(state,run([bar(),bar(1,high=102)],cutoff=2))
        self.assertEqual(touch.advance(state,[{"bad":"ignored"}],cutoff_utc=T+timedelta(minutes=100)),
                         run([bar(),bar(1,high=102)],cutoff=100))

    def test_reject_tamper_backward_cutoff_duplicate_and_bad_bars(self):
        state=run([bar()])
        changed=deepcopy(state); changed["status"]="SUCCESS"
        with self.assertRaises(ValueError): touch.advance(changed,[],cutoff_utc=T+timedelta(minutes=100))
        with self.assertRaises(ValueError): touch.advance(state,[],cutoff_utc=T+timedelta(minutes=99))
        for path in ([bar(),bar()], [bar(high=99)], [bar(close_time_utc=T+timedelta(seconds=30))]):
            with self.assertRaises(ValueError): run(path)

    def test_exchange_inclusive_close_is_not_available_early(self):
        candle=bar(close_time_utc=T+timedelta(seconds=59,milliseconds=999),high=102)
        state=touch.evaluate(contract(),[candle],cutoff_utc=T+timedelta(seconds=59,milliseconds=999))
        self.assertEqual(state["processed_candles"],0)
        self.assertEqual(run([candle],cutoff=1)["status"],"SUCCESS")


class GateTests(unittest.TestCase):
    def test_three_fresh_fail_and_five_probability_only_pass(self):
        self.assertFalse(qualify([row(i) for i in range(3)])["experimental_eligible"])
        result=qualify([row(i) for i in range(5)])
        self.assertTrue(result["experimental_eligible"])
        self.assertFalse(result["asymmetry"]["available"])
        self.assertIsNone(result["asymmetry"]["passes"])
        self.assertTrue(all(result[key] is False for key in ("runtime_authorized","telegram_authorized","trading_authorized")))

    def test_duplicate_waves_do_not_inflate_count(self):
        self.assertFalse(qualify([row(i,parent="same") for i in range(9)])["experimental_eligible"])
        self.assertEqual(qualify([row(1),row(1)])["selected_parents"],1)

    def test_earliest_unknown_not_replaced_by_later_winner(self):
        early=row(0,status="MISSING",parent="p0")
        later=row(9,parent="p0",decision=T+timedelta(minutes=1))
        later["outcome"]=run([bar(1,high=102)],later["contract"])
        result=qualify([later]+[row(i) for i in range(1,5)]+[early])
        self.assertEqual(result["resolved_parents"],4)
        self.assertFalse(result["experimental_eligible"])
        self.assertEqual(result["representatives"][0]["entry_id"],"e0")

    def test_tie_selection_is_deterministic_and_outcome_blind(self):
        a=row(1,status="FAILURE",parent="p0"); b=row(2,parent="p0")
        self.assertEqual(qualify([b,a])["representatives"],qualify([a,b])["representatives"])
        self.assertEqual(qualify([b,a])["failures"],1)

    def test_cohort_conflict_and_incomplete_population_block(self):
        rows=[row(i) for i in range(5)]
        wrong=row(9); wrong["contract"]=contract(entry_id="e9",candidate_version="v2")
        self.assertFalse(qualify(rows+[wrong])["experimental_eligible"])
        changed=deepcopy(rows[0]); changed["outcome"]=None
        self.assertFalse(qualify(rows+[changed])["experimental_eligible"])
        result=gate.evaluate_gate(rows,as_of_utc=T+timedelta(minutes=100),source_coverage_complete=False)
        self.assertFalse(result["experimental_eligible"])

    def test_unknown_parent_or_future_features_not_evidence(self):
        rows=[row(i) for i in range(5)]
        rows[0]["features_observed_at_utc"]=(T+timedelta(seconds=1)).isoformat()
        self.assertEqual(qualify(rows)["resolved_parents"],4)
        rows[0]["btc_parent_movement_id"]=None
        self.assertFalse(qualify(rows)["experimental_eligible"])

    def test_five_wins_cannot_hide_incomplete_sixth_path(self):
        winners=[row(i) for i in range(5)]
        missing=row(9,status="MISSING")
        self.assertFalse(qualify(winners+[missing])["experimental_eligible"])
        pending=row(9,status="OPEN")
        self.assertFalse(qualify(winners+[pending])["experimental_eligible"])
        pending["outcome"]=run([bar(i) for i in range(100)],pending["contract"])
        self.assertTrue(qualify(winners+[pending])["experimental_eligible"])

    def test_stale_open_checkpoint_cannot_hide_later_outcome(self):
        pending=row(9,status="OPEN")
        pending["outcome"]=run([bar()],pending["contract"],cutoff=1)
        self.assertFalse(qualify([row(i) for i in range(5)]+[pending])["experimental_eligible"])

    def test_policy_cannot_reintroduce_three_or_silently_change(self):
        with self.assertRaises(ValueError): gate.make_policy(minimum_waves=3)
        with self.assertRaises(ValueError): gate.make_policy(hit_rate_min_pct=60)

    def test_parent_whitespace_cannot_create_extra_waves(self):
        result=qualify([row(i,parent="same"+" "*i) for i in range(5)])
        self.assertFalse(result["experimental_eligible"])
        self.assertEqual(result["selected_parents"],1)


if __name__ == "__main__":
    unittest.main()
