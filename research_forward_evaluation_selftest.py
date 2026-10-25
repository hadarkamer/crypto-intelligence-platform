"""Artificial fixtures for descriptive evaluation; never production evidence."""
from __future__ import annotations
import copy
import hashlib
import json
import unittest
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from research_forward_evaluation import assess, safe_summary, stable_fingerprint

BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)
def stamp(seconds): return (BASE + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")
def digest(value): return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()).hexdigest()

def empty():
    policy = {"study_start_utc": stamp(0), "books": ["DOGE_only", "ETH_only", "first_per_parent"],
              "scopes": [{"symbol": "DOGE", "scope_key": "doge", "direction": "SHORT", "window_minutes": 1},
                         {"symbol": "ETH", "scope_key": "eth", "direction": "LONG", "window_minutes": 1}],
              "net_cost_pp": .1, "barrier_bps": 50, "entry_expiry_seconds": 90,
              "quote_max_elapsed_seconds": 5, "max_quote_gap_seconds": 30,
              "quote_source": "BINANCE_SPOT_BOOK_TICKER"}
    config = dict(policy); config.pop("study_start_utc")
    s = {"study_id": "test", "journal_seq": 12, "policy_json": policy, "policy_sha256": digest(policy),
         "config_sha256": digest(config), "updated_at_utc": stamp(100),
         "state_json": {"first_seen": {}, "last_observation_rejected": None, "bootstrap_complete": True,
                        "books": {name: {"choices": {}, "active": None, "reservation": None, "closed": []} for name in policy["books"]}},
         "last_status": {"books": {}, "first_eligible_seen_count": 0}}
    baseline = {k: s[k] for k in ("study_id", "policy_sha256", "config_sha256")}
    baseline["study_start_utc"] = stamp(0)
    refresh(s)
    return s, baseline

def refresh(s):
    status = s["last_status"]
    status["first_eligible_seen_count"] = sum(v.get("first_eligible_seen_utc") is not None for v in s["state_json"]["first_seen"].values())
    for name, book in s["state_json"]["books"].items():
        rows = book["closed"]; total = sum((Decimal(c["net_return_pct"]) for c in rows), Decimal(0))
        status["books"][name] = {"completed_all": {"n": len(rows), "counts": dict(Counter(c["status"] for c in rows)),
                                                    "mean_net_return_pct": float(total / len(rows)) if rows else None,
                                                    "net_sum_unit_notional_pct": float(total)}}

def packet(request, received, symbol, midpoint, mono=None):
    values = {"DOGE": "100", "ETH": "100"}; values[symbol] = str(midpoint)
    return {"packet_id": "q-" + str(request), "request_started_utc": stamp(request),
            "response_received_utc": stamp(received), "monotonic_elapsed_seconds": mono if mono is not None else received-request,
            "source": "BINANCE_SPOT_BOOK_TICKER",
            "quotes": [{"symbol": key, "bid": value, "ask": value} for key, value in values.items()]}

def journals(packets):
    return [{"seq": i + 1, "kind": "INPUT_QUOTES", "input_key": "QUOTES:" + p["packet_id"],
             "payload_json": p, "payload_sha256": digest(p)} for i, p in enumerate(sorted(packets, key=lambda p:p["response_received_utc"]))]

def add_case(s, *, symbol="DOGE", parent="p1", event_id=1, selected=10, entry=60.2,
             exit_=70.2, status="OBSERVED_TARGET", exit_mid=None, first=True):
    side = "SHORT" if symbol == "DOGE" else "LONG"
    if exit_mid is None: exit_mid = "99.5" if side == "SHORT" else "100.5"
    planned = (int(selected)//60+1)*60
    choice = {"case_id": symbol+"forward-"+str(event_id), "symbol": symbol, "event_id": event_id,
              "parent_id": parent, "direction": side, "window_minutes": 1,
              "selected_at_utc": stamp(selected), "planned_entry_utc": stamp(planned),
              "entry_expiry_utc": stamp(planned+90), "source_marker_max_utc": stamp(selected-1),
              "selection_sequence": event_id, "status": status}
    eq = {"bid": "100", "ask": "100", "midpoint": "100", "request_started_utc": stamp(entry-.1),
          "response_received_utc": stamp(entry), "source": "BINANCE_SPOT_BOOK_TICKER"}
    xq = {"bid": str(exit_mid), "ask": str(exit_mid), "midpoint": str(exit_mid),
          "request_started_utc": stamp(exit_-.1), "response_received_utc": stamp(exit_), "source": "BINANCE_SPOT_BOOK_TICKER"}
    gross = (Decimal(exit_mid)-100) * (Decimal(-1) if side == "SHORT" else Decimal(1))
    case = {**choice, "entry_midpoint": "100", "exit_midpoint": str(exit_mid), "entry_quote": eq, "exit_quote": xq,
            "entry_received_utc": stamp(entry), "exit_received_utc": stamp(exit_), "deadline_utc": stamp(entry+60),
            "gross_return_pct": str(gross), "net_return_pct": str(gross-Decimal(".1")), "modeled_cost_pp": ".1",
            "path_uncertain": False, "uncertainty_reasons": []}
    for name in [symbol+"_only"] + (["first_per_parent"] if first else []):
        b = s["state_json"]["books"][name]; b["choices"][parent] = copy.deepcopy(choice); b["closed"].append(copy.deepcopy(case))
    s["state_json"]["first_seen"][symbol.lower()+":"+str(event_id)] = {"first_eligible_seen_utc": stamp(selected)}
    p = [packet(planned-10, planned-9.9, symbol, "100"), packet(entry-.1, entry, symbol, "100")]
    t = entry+10
    while t < exit_-.2:
        p.append(packet(t-.1, t, symbol, "100")); t += 10
    p.append(packet(exit_-.1, exit_, symbol, exit_mid))
    refresh(s)
    return case, p


class EvaluationTests(unittest.TestCase):
    def run_audit(self, s, baseline, packets):
        return assess(s, journals(packets), expected_baseline=baseline)

    def test_empty_study_pass_is_descriptive_not_strategy_approval(self):
        s,b=empty();r=self.run_audit(s,b,[])
        self.assertEqual(r["status"],"PASS");self.assertEqual(r["stage"],"COLLECTING")
        self.assertFalse(r["strategy_approved"])

    def test_short_and_long_targets_charge_one_cost_and_shared_rows_not_independent(self):
        for symbol in ("DOGE","ETH"):
            with self.subTest(symbol=symbol):
                s,b=empty();_,p=add_case(s,symbol=symbol);r=self.run_audit(s,b,p)
                self.assertEqual(r["status"],"PASS",r["failures"])
                self.assertEqual(r["counts"]["book_trade_rows"],2)
                self.assertEqual(r["counts"]["unique_closed_cases"],1)
                self.assertAlmostEqual(r["books"][symbol+"_only"]["completed_all"]["mean_net_return_pct"],.4)

    def test_timeout_precedence_and_predeadline_request_late_receipt_wait(self):
        s,b=empty();_,p=add_case(s,symbol="ETH",exit_=120.4,status="OBSERVED_TIMEOUT",exit_mid="101")
        p.append(packet(120.1,120.25,"ETH","101",mono=.15))
        r=self.run_audit(s,b,p)
        self.assertEqual(r["status"],"PASS",r["failures"])

    def test_corrupt_cost_or_return_fails(self):
        for key,value in (("net_return_pct",".3"),("modeled_cost_pp",".2"),("gross_return_pct",".9")):
            s,b=empty();_,p=add_case(s)
            s["state_json"]["books"]["DOGE_only"]["closed"][0][key]=value
            self.assertEqual(self.run_audit(s,b,p)["status"],"FAIL")

    def test_both_directions_stop_uses_original_barrier_and_cost(self):
        for symbol,mid in (("DOGE","100.5"),("ETH","99.5")):
            s,b=empty();_,p=add_case(s,symbol=symbol,status="OBSERVED_STOP",exit_mid=mid)
            r=self.run_audit(s,b,p)
            self.assertEqual(r["status"],"PASS",r["failures"])
            self.assertAlmostEqual(r["books"][symbol+"_only"]["completed_all"]["mean_net_return_pct"],-.6)

    def test_shared_parent_complete_count_and_previous_assessment_changes(self):
        s,b=empty();_,p1=add_case(s,symbol="ETH")
        _,p2=add_case(s,symbol="DOGE",event_id=2,selected=130,entry=180.2,exit_=190.2,first=False)
        r=self.run_audit(s,b,p1+p2)
        self.assertEqual(r["status"],"PASS",r["failures"])
        self.assertEqual(r["counts"]["book_trade_rows"],3)
        self.assertEqual(r["counts"]["unique_closed_cases"],2)
        self.assertEqual(r["counts"]["parent_wave_count"],1)
        self.assertEqual(r["counts"]["closed_wave_count"],1)
        self.assertEqual(r["counts"]["paired_complete_parent_waves"],1)
        again=assess(s,journals(p1+p2),expected_baseline=b,previous_assessment=r)
        self.assertEqual(again["changes"]["new_closed_case_count"],0)

    def test_missing_price_context_or_exit_stays_pending(self):
        s,b=empty();_,p=add_case(s)
        self.assertEqual(self.run_audit(s,b,[])["status"],"PENDING_PRICE_DATA")
        self.assertEqual(self.run_audit(s,b,p[:-1])["status"],"PENDING_PRICE_DATA")
        self.assertEqual(self.run_audit(s,b,p[1:])["status"],"PENDING_PRICE_DATA")

    def test_earlier_crossing_and_wrong_quote_hash_fail(self):
        s,b=empty();_,p=add_case(s)
        self.assertEqual(self.run_audit(s,b,p+[packet(65,65.1,"DOGE","99.4")])["status"],"FAIL")
        rows=journals(p);rows[0]["payload_sha256"]="wrong"
        self.assertEqual(assess(s,rows,expected_baseline=b)["status"],"FAIL")

    def test_slow_packet_is_skipped_and_other_symbol_bad_quote_does_not_remove_valid(self):
        s,b=empty();_,p=add_case(s)
        p.append(packet(65,65.1,"DOGE","99.4",mono=6))
        p[1]["quotes"][1]["bid"]="NaN"
        r=self.run_audit(s,b,p)
        self.assertEqual(r["status"],"PASS",r["failures"])
        self.assertTrue(r["quote_evidence"]["rejected_packet_or_symbol_reasons"])

    def test_gap_without_flag_fails_and_sticky_flag_retained(self):
        s,b=empty();_,p=add_case(s,exit_=110.2)
        p=[p[0],p[1],p[-1]]
        self.assertEqual(self.run_audit(s,b,p)["status"],"FAIL")
        for name in ("DOGE_only","first_per_parent"):
            c=s["state_json"]["books"][name]["closed"][0];c["path_uncertain"]=True;c["uncertainty_reasons"]=["QUOTE_GAP"]
        r=self.run_audit(s,b,p)
        self.assertEqual(r["status"],"PASS",r["failures"])
        self.assertTrue(r["cases"][0]["path"]["observed_gap_uncertain"])

    def test_fingerprint_ignores_heartbeat_but_detects_new_eligible_and_integrity_health(self):
        s,b=empty();changed=copy.deepcopy(s)
        changed["journal_seq"]+=100;changed["updated_at_utc"]=stamp(200);changed["last_status"]["cycles"]=99
        changed["state_json"]["observation_sequence"]=99;changed["state_json"]["last_quotes"]={"DOGE":{}}
        self.assertEqual(stable_fingerprint(s),stable_fingerprint(changed))
        changed["state_json"]["first_seen"]["doge:2"]={"first_eligible_seen_utc":stamp(20)}
        self.assertNotEqual(stable_fingerprint(s),stable_fingerprint(changed))
        changed=copy.deepcopy(s);changed["state_json"]["last_observation_rejected"]="bad"
        self.assertNotEqual(stable_fingerprint(s),stable_fingerprint(changed))
        self.assertEqual(self.run_audit(changed,b,[])["status"],"FAIL")
        changed=copy.deepcopy(s);changed["state_json"]["first_seen"]["doge:2"]={"first_eligible_seen_utc":None,"last_ineligible_reason":"POLICY_SOURCE_DRIFT"}
        self.assertNotEqual(stable_fingerprint(s),stable_fingerprint(changed))
        self.assertEqual(self.run_audit(changed,b,[])["status"],"FAIL")

    def test_policy_or_baseline_drift_is_not_pass(self):
        s,b=empty();s["policy_json"]["net_cost_pp"]=.2
        self.assertEqual(self.run_audit(s,b,[])["status"],"FAIL")
        s,b=empty();b["study_start_utc"]=stamp(1)
        self.assertEqual(self.run_audit(s,b,[])["status"],"FAIL")

    def test_reverse_lifecycle_and_wrong_first_choice_fail(self):
        s,b=empty();_,p=add_case(s)
        s["state_json"]["books"]["DOGE_only"]["choices"]["p1"]["status"]="OPEN"
        self.assertEqual(self.run_audit(s,b,p)["status"],"FAIL")
        s,b=empty();_,p1=add_case(s,symbol="ETH");_,p2=add_case(s,symbol="DOGE",event_id=2,selected=130,entry=180.2,exit_=190.2,first=False)
        wrong=copy.deepcopy(s["state_json"]["books"]["DOGE_only"])
        s["state_json"]["books"]["first_per_parent"]=wrong;refresh(s)
        self.assertEqual(self.run_audit(s,b,p1+p2)["status"],"FAIL")

    def test_open_reserved_blocked_and_expired_are_descriptive_and_not_closed(self):
        for phase in ("OPEN","RESERVED","BLOCKED_CAPACITY","EXPIRED_NO_ENTRY_QUOTE","PLANNED"):
            s,b=empty();case,p=add_case(s)
            for name in ("DOGE_only","first_per_parent"):
                book=s["state_json"]["books"][name];book["closed"]=[];book["choices"]["p1"]["status"]=phase
                if phase=="OPEN":
                    active=copy.deepcopy(case);active["status"]="OPEN";active.pop("exit_quote");active.pop("exit_midpoint");active.pop("exit_received_utc")
                    book["active"]=active
                if phase=="RESERVED":book["reservation"]="p1"
            refresh(s);r=self.run_audit(s,b,[])
            self.assertEqual(r["status"],"PASS",(phase,r["failures"]))
            self.assertEqual(r["counts"]["unique_closed_cases"],0)
            self.assertEqual(r["counts"]["closed_wave_count"],0)
            if phase=="OPEN":
                changed=copy.deepcopy(s)
                changed["state_json"]["books"]["DOGE_only"]["active"]["last_quote_received_utc"]=stamp(100)
                self.assertEqual(stable_fingerprint(s),stable_fingerprint(changed))

    def test_journal_event_names_are_not_choice_status_aliases(self):
        for wrong in ("CAPACITY_BLOCKED","ENTRY_EXPIRED"):
            s,b=empty();_,p=add_case(s)
            for name in ("DOGE_only","first_per_parent"):
                book=s["state_json"]["books"][name];book["closed"]=[];book["choices"]["p1"]["status"]=wrong
            refresh(s)
            self.assertEqual(self.run_audit(s,b,[])["status"],"FAIL")

    def test_quote_receipt_reversal_and_overlapping_book_cases_fail(self):
        s,b=empty();_,p=add_case(s)
        rows=journals(p);rows[0],rows[1]=rows[1],rows[0];rows[0]["seq"],rows[1]["seq"]=1,2
        self.assertEqual(assess(s,rows,expected_baseline=b)["status"],"FAIL")
        s,b=empty();_,p1=add_case(s,exit_=110.2);_,p2=add_case(s,parent="p2",event_id=2,selected=50,entry=80.2,exit_=90.2)
        self.assertEqual(self.run_audit(s,b,p1+p2)["status"],"FAIL")

    def test_safe_summary_no_case_ids_prices_or_raw_source_rows(self):
        s,b=empty();_,p=add_case(s);r=self.run_audit(s,b,p);safe=safe_summary(r);encoded=json.dumps(safe)
        self.assertEqual(safe["status"],"PASS")
        for text in ("DOGEforward-1","entry_midpoint","parent_id","input_key","first_seen"):
            self.assertNotIn(text,encoded)
        self.assertFalse(safe["strategy_approved"])


if __name__ == "__main__":
    suite=unittest.defaultTestLoader.loadTestsFromTestCase(EvaluationTests)
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():raise SystemExit(1)
