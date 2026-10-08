"""Collection audit math, frozen-population bounds, and execution safety tests."""
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import unittest

import research_no_horizon_collection_audit as audit
import research_no_horizon_cohort as cohort
import research_no_horizon_contract as contracts

BASE = datetime(2026,10,7,tzinfo=timezone.utc)
MINUTE = timedelta(minutes=1)


def declaration():
    return {"cohort_version":cohort.VERSION,"cohort_key":"collection-audit-fixture",
        "declared_at_utc":(BASE-timedelta(days=1)).isoformat(),"prior_outcomes_observed":False,
        "symbol":"BTC","price_route":audit.ROUTE,"source_start_utc":BASE.isoformat(),
        "source_end_utc":(BASE+4*MINUTE).isoformat(),"cutoff_utc":(BASE+6*MINUTE).isoformat(),
        "scopes":[{"candidate_key":"FUTURES_CVD_TOTAL_65","base_direction":"SHORT","threshold_pct":0.25}],
        "parts":[{"ordinal":0,"source_start_utc":BASE.isoformat(),
                  "source_end_utc":(BASE+2*MINUTE).isoformat(),"source_row_limit":2},
                 {"ordinal":1,"source_start_utc":(BASE+2*MINUTE).isoformat(),
                  "source_end_utc":(BASE+4*MINUTE).isoformat(),"source_row_limit":2}]}


def bar(index, *, valid=True):
    opened = BASE+index*MINUTE
    return {"open_time_utc":opened.isoformat(),
            "close_time_utc":(opened+MINUTE-timedelta(milliseconds=1)).isoformat(),"valid":valid}


def source_row(ident,minute):
    when = (BASE+minute*MINUTE).isoformat()
    return {"snapshot_set_id":ident,"usable_from_utc":when,"ingested_at_utc":when,
            "watch_scan_id":f"watch-{ident}","metadata_shape_ok":True}


def payload(d=None, *, as_of=None):
    d = d or declaration()
    frozen = cohort.normalize_declaration(d)
    as_of = as_of or BASE+timedelta(minutes=3,seconds=40)
    closed = min(as_of,contracts.utc(d["cutoff_utc"])).replace(second=0,microsecond=0)
    return {"version":audit.VERSION,"declaration_sha256":contracts.digest(frozen),
        "as_of_utc":as_of.isoformat(),"closed_before_utc":closed.isoformat(),
        "archive_rows":[bar(i) for i in range(max(0,int((closed-BASE)/MINUTE)))],
        "parent_rows":[bar(i) for i in range(-1,int((closed-BASE)/MINUTE))],
        "price_row_cap":2*(audit.MAX_AUDIT_DAYS*1440+2),
        "parts":[{"ordinal":p["ordinal"],"row_cap":p["source_row_limit"],
                  "start_utc":p["source_start_utc"],"end_utc":p["source_end_utc"],"rows":[]}
                 for p in frozen["parts"]],"intake_state":None,
        "receipt":{"read_only":"on","database_writes":False,"outcome_reads":False,
                   "extracted_at_utc":as_of.isoformat(),"serializer_timezone":"UTC","mvcc_snapshot":"1:2:"}}


class CoverageMathTests(unittest.TestCase):
    def check(self,rows,end=7,row_cap=20):
        return audit.minute_coverage(rows,start_utc=BASE,closed_before_utc=BASE+end*MINUTE,row_cap=row_cap)

    def test_latest_bar_does_not_hide_multiple_gaps(self):
        report = self.check([bar(i) for i in (0,3,5,6)])
        self.assertEqual(report["missing_minutes"],3)
        self.assertEqual(report["unusable_minutes"],3)
        self.assertEqual(report["longest_gap_minutes"],2)
        self.assertEqual(report["gap_count"],2)
        self.assertEqual([g["minutes"] for g in report["gap_samples"]],[2,1])
        self.assertEqual(report["latest_open_in_range_utc"],bar(6)["open_time_utc"])

    def test_all_empty_and_edge_gaps(self):
        empty = self.check([])
        self.assertEqual((empty["gap_count"],empty["longest_gap_minutes"]),(1,7))
        edges = self.check([bar(i) for i in range(1,6)])
        self.assertEqual([g["minutes"] for g in edges["gap_samples"]],[1,1])
        self.assertEqual(self.check([],end=0)["status"],"NOT_STARTED")

    def test_duplicate_and_invalid_are_present_but_unusable(self):
        rows = [bar(0),bar(0),bar(1,valid=False),bar(2)]
        report = self.check(rows,end=3)
        self.assertEqual(report["missing_minutes"],0)
        self.assertEqual(report["unusable_minutes"],2)
        self.assertEqual((report["duplicate_rows"],report["invalid_rows"]),(1,1))
        self.assertEqual(report["longest_gap_minutes"],2)

    def test_close_time_revalidated_and_current_open_rejected(self):
        row = bar(0)
        row["close_time_utc"]=(BASE+MINUTE).isoformat()
        self.assertEqual(self.check([row],end=1)["invalid_rows"],1)
        with self.assertRaisesRegex(ValueError,"OUTSIDE_RANGE"):
            self.check([bar(1)],end=1)

    def test_cap_overflow_never_invents_exact_missing_count(self):
        report = self.check([bar(0),bar(1),bar(2)],end=7,row_cap=2)
        self.assertFalse(report["counts_exact"])
        self.assertIsNone(report["missing_minutes"])
        self.assertIsNone(report["longest_gap_minutes"])
        self.assertEqual(report["status"],"UNKNOWN_ROW_CAP_EXCEEDED")

    def test_longest_gap_unaffected_by_diagnostic_sample_limit(self):
        rows=[bar(i) for i in range(0,70,2)]
        report=self.check(rows,end=75,row_cap=100)
        self.assertEqual(len(report["gap_samples"]),audit.GAP_SAMPLE_LIMIT)
        self.assertTrue(report["gap_samples_truncated"])
        self.assertEqual(report["longest_gap_minutes"],6)


class AuditBindingTests(unittest.TestCase):
    def test_future_interval_is_unobserved_even_when_current_prices_complete(self):
        report=audit.analyze(declaration(),payload(),as_of_utc=BASE+timedelta(minutes=3,seconds=40))
        self.assertEqual(report["archive"]["expected_minutes"],3)
        self.assertEqual(report["archive"]["status"],"COMPLETE_OBSERVED_MINUTES")
        self.assertEqual(report["parent"]["expected_minutes"],4)
        self.assertTrue(report["future_source_interval_unobserved"])
        self.assertFalse(report["full_observation_cutoff_reached"])
        self.assertEqual(report["full_source_readiness"],"UNKNOWN_METADATA_AUDIT_ONLY")

    def test_source_boundary_and_declared_cap_are_preserved(self):
        p=payload()
        p["parts"][1]["rows"]=[source_row(1,2),source_row(2,2.1),source_row(3,2.2)]
        report=audit.analyze(declaration(),p,as_of_utc=p["as_of_utc"])
        self.assertFalse(report["source"]["count_exact"])
        self.assertEqual(report["source"]["capacity_status"],"EXCEEDED")
        p["parts"][0]["rows"]=[source_row(10,2)]
        with self.assertRaisesRegex(ValueError,"IDENTITY_OR_TIME"):
            audit.analyze(declaration(),p,as_of_utc=p["as_of_utc"])

    def test_global_decision_budget_uses_exact_declared_denominator(self):
        # Real supported grid yields the original 481 cap for 34 scopes.
        import research_watch_scan_formula_maxpain as formulas
        d=declaration()
        d["scopes"]=[{"candidate_key":r["candidate_key"],"base_direction":"SHORT","threshold_pct":0.25}
                     for r in formulas.catalog_records() if r["supported"]][:34]
        for part in d["parts"]:
            part["source_row_limit"]=256
        p=payload(d)
        p["parts"][0]["rows"]=[source_row(i+1,0.5) for i in range(250)]
        p["parts"][1]["rows"]=[source_row(i+251,2.5) for i in range(232)]
        report=audit.analyze(d,p,as_of_utc=p["as_of_utc"])
        self.assertTrue(report["source"]["count_exact"])
        self.assertEqual(report["source"]["max_rows_from_frozen_budgets"],481)
        self.assertEqual(report["source"]["capacity_status"],"EXCEEDED")

    def test_wrong_venue_symbol_receipt_clock_and_plan_are_rejected(self):
        for key,value in (("symbol","XRP"),("price_route","HYPERLIQUID_HYPE_PERP_TRADE_1M")):
            d=declaration(); d[key]=value
            with self.assertRaises(ValueError):
                audit.build_sql(d,as_of_utc=BASE)
        for mutate in (lambda p:p.update(declaration_sha256="0"*64),
                       lambda p:p["receipt"].update(read_only="off"),
                       lambda p:p.update(as_of_utc=(BASE+4*MINUTE).isoformat()),
                       lambda p:p.update(closed_before_utc=(BASE+4*MINUTE).isoformat())):
            p=payload(); mutate(p)
            with self.assertRaises(ValueError):
                audit.analyze(declaration(),p,as_of_utc=BASE+timedelta(minutes=3,seconds=40))

    def test_sql_is_outcome_blind_fixed_route_and_bounded(self):
        d=declaration(); before=deepcopy(d)
        sql=audit.build_sql(d,as_of_utc=BASE)
        self.assertEqual(d,before)
        self.assertIn("c.route='BINANCE_SPOT_TRADE_1M' AND c.symbol='BTC'",sql)
        self.assertIn("i.consumer_version='watch-all-scan-intake-v1' AND i.intake_status='ACCEPTED'",sql)
        self.assertIn("LIMIT p.row_cap+1",sql)
        self.assertIn("c.open_time_utc<b.closed_before_utc",sql)
        self.assertIn("statement_timestamp()",sql)
        for forbidden in ("first_touch","formula_matches","rank(","INSERT ","UPDATE ","DELETE "):
            self.assertNotIn(forbidden,sql)


class FakeConnection:
    class Info:
        transaction_status=0
    info=Info()
    def __init__(self,p,*,read_only="on"):
        self.p=p; self.calls=[]; self.read_only=read_only
    @contextmanager
    def transaction(self):
        self.calls.append("BEGIN")
        yield
        self.calls.append("END")
    def execute(self,sql):
        self.calls.append(sql)
        self.current=sql
        return self
    def fetchone(self):
        return {"read_only":self.read_only}
    def fetchmany(self,n):
        return [{"audit":self.p}]


class ExecuteSafetyTests(unittest.TestCase):
    def test_timeouts_and_read_only_are_established_before_source_read(self):
        p=payload(); conn=FakeConnection(p)
        report=audit.execute(conn,declaration(),as_of_utc=p["as_of_utc"])
        queries=[i for i,s in enumerate(conn.calls) if s.startswith("WITH cfg")]
        self.assertEqual(len(queries),1)
        for setting in ("SET TRANSACTION READ ONLY","SET LOCAL statement_timeout='15000ms'",
                        "SET LOCAL lock_timeout='1000ms'"):
            self.assertLess(conn.calls.index(setting),queries[0])
        self.assertFalse(report["outcome_reads_performed"])
        self.assertEqual(len(report["query_sha256"]),64)

    def test_failed_read_only_assertion_never_reads_source(self):
        conn=FakeConnection(payload(),read_only="off")
        with self.assertRaisesRegex(ValueError,"NOT_READ_ONLY"):
            audit.execute(conn,declaration(),as_of_utc=BASE)
        self.assertFalse(any(s.startswith("WITH cfg") for s in conn.calls))


if __name__ == "__main__":
    unittest.main()
