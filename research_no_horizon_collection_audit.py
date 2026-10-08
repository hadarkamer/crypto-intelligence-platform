"""Outcome-blind, bounded collection diagnostics for a frozen BTC declaration.

Reads metadata only; does not acquire a cohort, register work, evaluate formulas,
or authorize execution. Full source readiness deliberately remains UNKNOWN.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path

import research_no_horizon_cohort as cohort
import research_no_horizon_contract as contracts
import research_no_horizon_manifest_sql as source_sql

VERSION = "no-horizon-collection-metadata-audit-v1"
MAX_AUDIT_DAYS = 8
GAP_SAMPLE_LIMIT = 32
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
MINUTE = timedelta(minutes=1)
UTC = timezone.utc
ROUTE = "BINANCE_SPOT_TRADE_1M"
PARENT_SOURCE = "BINANCE_SPOT_BTCUSDT_1M"


def _ceil_minute(value):
    floor = value.replace(second=0, microsecond=0)
    return floor if value == floor else floor + MINUTE


def normalize_plan(declaration, *, as_of_utc):
    frozen = cohort.normalize_declaration(declaration)
    if frozen["symbol"] != "BTC" or frozen["price_route"] != ROUTE:
        raise ValueError("AUDIT_REQUIRES_FROZEN_BTC_BINANCE_SPOT_ROUTE")
    start, cutoff = (contracts.utc(frozen[k]) for k in ("source_start_utc", "cutoff_utc"))
    if cutoff - start > timedelta(days=MAX_AUDIT_DAYS):
        raise ValueError("AUDIT_INTERVAL_EXCEEDS_EIGHT_DAYS")
    return {"version": VERSION, "declaration": frozen,
            "declaration_sha256": contracts.digest(frozen),
            "as_of_utc": contracts.utc(as_of_utc).isoformat()}


def build_sql(declaration, *, as_of_utc):
    """One SELECT; all clocks capped by statement_timestamp on the source DB.

    Metadata arrays are bounded independently of database constraints. A cap
    breach is reported as a lower bound, never as a complete range count.
    """
    plan = normalize_plan(declaration, as_of_utc=as_of_utc)
    frozen = plan["declaration"]
    literal = source_sql.literal
    start = contracts.utc(frozen["source_start_utc"])
    price_start = _ceil_minute(start)
    parent_start = start.replace(second=0, microsecond=0) - MINUTE
    # Extra slots admit the prior BTC bar and deliberately expose duplicates.
    row_cap = 2 * (MAX_AUDIT_DAYS * 1440 + 2)
    part_values = ",\n".join(
        f"({p['ordinal']},{literal(p['source_start_utc'])}::timestamptz,"
        f"{literal(p['source_end_utc'])}::timestamptz,{p['source_row_limit']})"
        for p in frozen["parts"])
    valid_ohlc = ("c.low>0 AND c.high<'Infinity'::double precision "
                  "AND c.low<=c.high AND c.low<=c.open AND c.low<=c.close "
                  "AND c.high>=c.open AND c.high>=c.close")
    temporal_valid = ("date_trunc('minute',c.open_time_utc)=c.open_time_utc AND "
                      "c.close_time_utc=c.open_time_utc+INTERVAL '1 minute'-INTERVAL '1 millisecond'")
    return f"""WITH cfg AS MATERIALIZED (
 SELECT LEAST({literal(plan['as_of_utc'])}::timestamptz,statement_timestamp()) AS as_of_utc,
   {literal(frozen['cutoff_utc'])}::timestamptz AS cutoff_utc
), bounds AS MATERIALIZED (
 SELECT cfg.*,date_trunc('minute',LEAST(as_of_utc,cutoff_utc)) AS closed_before_utc FROM cfg
), parts(ordinal,start_utc,end_utc,row_cap) AS (VALUES {part_values}),
archive_rows AS MATERIALIZED (
 SELECT c.open_time_utc,c.close_time_utc,
   (({temporal_valid}) AND ({valid_ohlc}) AND c.volume>=0
     AND c.volume<'Infinity'::double precision) IS TRUE AS valid
 FROM public.research_price_archive_bars c CROSS JOIN bounds b
 WHERE c.route='{ROUTE}' AND c.symbol='BTC'
   AND c.open_time_utc>={literal(price_start.isoformat())}::timestamptz
   AND c.open_time_utc<b.closed_before_utc
 ORDER BY c.open_time_utc LIMIT {row_cap+1}
), parent_rows AS MATERIALIZED (
 SELECT c.open_time_utc,c.close_time_utc,
   (({temporal_valid}) AND ({valid_ohlc}) AND c.price_source='{PARENT_SOURCE}') IS TRUE AS valid
 FROM public.research_btc_price_bars c CROSS JOIN bounds b
 WHERE c.open_time_utc>={literal(parent_start.isoformat())}::timestamptz
   AND c.open_time_utc<b.closed_before_utc
 ORDER BY c.open_time_utc LIMIT {row_cap+1}
), intake_parts AS MATERIALIZED (
 SELECT p.ordinal,p.row_cap,p.start_utc,p.end_utc,
   (SELECT COALESCE(jsonb_agg(to_jsonb(k) ORDER BY k.usable_from_utc,k.snapshot_set_id),'[]'::jsonb)
    FROM (SELECT i.snapshot_set_id,i.usable_from_utc,i.ingested_at_utc,i.watch_scan_id,
       (s.snapshot_set_id IS NOT NULL
        AND i.parent_payload_sha256=s.payload_sha256
        AND i.bundle_sha256=s.source_metadata#>>'{{capture_metadata,operational_scores,payload_sha256}}'
        AND jsonb_typeof(s.source_metadata#>'{{capture_metadata,operational_scores,coins,BTC}}')='object'
        AND i.usable_from_utc<=i.ingested_at_utc
        AND i.ingested_at_utc<=b.as_of_utc) IS TRUE AS metadata_shape_ok
     FROM public.research_watch_scan_intakes i
     LEFT JOIN public.research_max_pain_snapshot_sets s USING(snapshot_set_id)
     WHERE i.consumer_version='{source_sql.CONSUMER}' AND i.intake_status='ACCEPTED'
       AND i.usable_from_utc>=p.start_utc AND i.usable_from_utc<LEAST(p.end_utc,b.as_of_utc)
     ORDER BY i.usable_from_utc,i.snapshot_set_id LIMIT p.row_cap+1) k) AS rows
 FROM parts p CROSS JOIN bounds b
)
SELECT jsonb_build_object(
 'version','{VERSION}','declaration_sha256','{plan['declaration_sha256']}',
 'as_of_utc',b.as_of_utc,'closed_before_utc',b.closed_before_utc,
 'archive_rows',COALESCE((SELECT jsonb_agg(to_jsonb(a) ORDER BY open_time_utc) FROM archive_rows a),'[]'::jsonb),
 'parent_rows',COALESCE((SELECT jsonb_agg(to_jsonb(p) ORDER BY open_time_utc) FROM parent_rows p),'[]'::jsonb),
 'price_row_cap',{row_cap},
 'parts',(SELECT jsonb_agg(to_jsonb(p) ORDER BY ordinal) FROM intake_parts p),
 'intake_state',(SELECT jsonb_build_object('updated_at_utc',updated_at_utc,
    'scan_cursor',scan_cursor,'scan_high_water',scan_high_water,'completed_laps',completed_laps)
    FROM public.research_watch_scan_intake_state WHERE consumer_version='{source_sql.CONSUMER}'),
 'receipt',jsonb_build_object('read_only',current_setting('transaction_read_only'),
    'database_writes',false,'outcome_reads',false,'extracted_at_utc',statement_timestamp(),
    'mvcc_snapshot',pg_current_snapshot()::text,'serializer_timezone',current_setting('TimeZone'))
) AS audit FROM bounds b;
"""


def minute_coverage(rows, *, start_utc, closed_before_utc, row_cap):
    """Exact half-open grid accounting; duplicates and invalid bars are unusable.

    No close/open/price values are returned or used to calculate outcomes.
    Empty future ranges are NOT_STARTED, not complete.
    """
    start, end = contracts.utc(start_utc), contracts.utc(closed_before_utc)
    if start != start.replace(second=0, microsecond=0) or end != end.replace(second=0, microsecond=0):
        raise ValueError("AUDIT_MINUTE_BOUNDARIES_REQUIRED")
    if not isinstance(rows, list) or type(row_cap) is not int or row_cap < 1 or len(rows) > row_cap + 1:
        raise ValueError("AUDIT_PRICE_RESPONSE_BOUND_EXCEEDED")
    if end - start > timedelta(days=MAX_AUDIT_DAYS, minutes=2):
        raise ValueError("AUDIT_GRID_BOUND_EXCEEDED")
    by_open = {}
    for row in rows:
        opened, closed = (contracts.utc(row[k]) for k in ("open_time_utc", "close_time_utc"))
        if not start <= opened < end or type(row.get("valid")) is not bool:
            raise ValueError("AUDIT_PRICE_ROW_OUTSIDE_RANGE_OR_INVALID")
        valid = row["valid"] and opened.second == 0 and opened.microsecond == 0 and closed == opened + MINUTE - timedelta(milliseconds=1)
        by_open.setdefault(opened, []).append(valid)
    expected = max(0, int((end-start).total_seconds() // 60))
    missing, unusable, gap_runs, run_start = 0, 0, [], None
    for index in range(expected):
        opened = start + index * MINUTE
        bars = by_open.get(opened, [])
        if not bars:
            missing += 1
        valid = len(bars) == 1 and bars[0]
        if not valid:
            unusable += 1
            if run_start is None:
                run_start = opened
        if valid and run_start is not None:
            gap_runs.append((run_start, opened))
            run_start = None
    if run_start is not None:
        gap_runs.append((run_start, end))
    overflow = len(rows) > row_cap
    duplicate_rows = sum(len(group)-1 for group in by_open.values())
    invalid_rows = sum(not valid for group in by_open.values() for valid in group)
    off_grid_rows = sum(len(group) for opened, group in by_open.items() if opened.second or opened.microsecond)
    status = ("UNKNOWN_ROW_CAP_EXCEEDED" if overflow else "NOT_STARTED" if expected == 0
              else "COMPLETE_OBSERVED_MINUTES" if unusable == 0 and off_grid_rows == 0
              else "INCOMPLETE_OBSERVED_MINUTES")
    latest = max(by_open).isoformat() if by_open else None
    return {"status": status, "range_start_utc": start.isoformat(),
            "range_end_exclusive_utc": end.isoformat(), "expected_minutes": expected,
            "observed_rows": len(rows), "counts_exact": not overflow,
            "missing_minutes": None if overflow else missing,
            "unusable_minutes": None if overflow else unusable,
            "duplicate_rows": duplicate_rows, "invalid_rows": invalid_rows,
            "off_grid_rows": off_grid_rows, "latest_open_in_range_utc": latest,
            "gap_count": None if overflow else len(gap_runs),
            "longest_gap_minutes": None if overflow else max((int((b-a)/MINUTE) for a,b in gap_runs),default=0),
            "gap_samples_exact": not overflow,
            "gap_samples": [{"start_utc": a.isoformat(), "end_exclusive_utc": b.isoformat(),
                             "minutes": int((b-a)/MINUTE)} for a,b in gap_runs[:GAP_SAMPLE_LIMIT]],
            "gap_samples_truncated": len(gap_runs) > GAP_SAMPLE_LIMIT}


def analyze(declaration, payload, *, as_of_utc):
    """Bind diagnostics to one exact declaration. Not an acquisition proof."""
    plan = normalize_plan(declaration, as_of_utc=as_of_utc)
    frozen = plan["declaration"]
    receipt = payload.get("receipt", {})
    if (payload.get("version") != VERSION or payload.get("declaration_sha256") != plan["declaration_sha256"]
            or receipt.get("read_only") != "on" or receipt.get("database_writes") is not False
            or receipt.get("outcome_reads") is not False or receipt.get("serializer_timezone") != "UTC"
            or not receipt.get("mvcc_snapshot")):
        raise ValueError("AUDIT_RESPONSE_IDENTITY_OR_READ_ONLY_RECEIPT_MISMATCH")
    as_of = contracts.utc(payload["as_of_utc"])
    if as_of != min(contracts.utc(as_of_utc), contracts.utc(receipt["extracted_at_utc"])):
        raise ValueError("AUDIT_CLOCK_BOUND_MISMATCH")
    closed_before = min(as_of, contracts.utc(frozen["cutoff_utc"])).replace(second=0,microsecond=0)
    if contracts.utc(payload["closed_before_utc"]) != closed_before:
        raise ValueError("AUDIT_CLOSED_MINUTE_BOUND_MISMATCH")
    start = contracts.utc(frozen["source_start_utc"])
    result = {"version": VERSION, "declaration_sha256": plan["declaration_sha256"],
              "cohort_key": frozen["cohort_key"], "as_of_utc": as_of.isoformat(),
              "snapshot_semantics": "CURRENT_DATABASE_SNAPSHOT_WITH_CAPPED_EVENT_TIME",
              "declared_source_end_utc": frozen["source_end_utc"], "declared_cutoff_utc": frozen["cutoff_utc"],
              "future_source_interval_unobserved": as_of < contracts.utc(frozen["source_end_utc"]),
              "full_observation_cutoff_reached": as_of >= contracts.utc(frozen["cutoff_utc"]),
              "frozen_route": ROUTE, "frozen_symbol": "BTC", "receipt": receipt}
    expected_cap = 2 * (MAX_AUDIT_DAYS * 1440 + 2)
    if payload.get("price_row_cap") != expected_cap:
        raise ValueError("AUDIT_PRICE_CAP_BINDING_MISMATCH")
    for name, price_start in (("archive", _ceil_minute(start)),
                              ("parent", start.replace(second=0,microsecond=0)-MINUTE)):
        result[name] = minute_coverage(payload[name+"_rows"],start_utc=price_start,
                                      closed_before_utc=closed_before,row_cap=expected_cap)
    parts = payload.get("parts")
    if not isinstance(parts,list) or len(parts) != len(frozen["parts"]):
        raise ValueError("AUDIT_COMPLETE_DECLARED_PARTS_REQUIRED")
    part_reports, latest, total, exact, seen = [], None, 0, True, set()
    for p, expected in zip(parts, frozen["parts"]):
        cap = expected["source_row_limit"]
        left, right = (contracts.utc(expected[k]) for k in ("source_start_utc","source_end_utc"))
        if (p.get("ordinal") != expected["ordinal"] or p.get("row_cap") != cap
                or contracts.utc(p["start_utc"]) != left or contracts.utc(p["end_utc"]) != right
                or not isinstance(p.get("rows"),list) or len(p["rows"]) > cap+1):
            raise ValueError("AUDIT_SOURCE_PART_BINDING_OR_ROW_BOUND_MISMATCH")
        watch_ids, metadata_bad = Counter(), 0
        for row in p["rows"]:
            usable = contracts.utc(row["usable_from_utc"])
            ident = row["snapshot_set_id"]
            if (not left <= usable < min(right,as_of) or type(ident) is not int or ident<=0
                    or ident in seen or type(row.get("metadata_shape_ok")) is not bool):
                raise ValueError("AUDIT_SOURCE_ROW_IDENTITY_OR_TIME_MISMATCH")
            seen.add(ident)
            latest = max(latest,usable) if latest else usable
            watch_ids[row["watch_scan_id"]] += 1
            metadata_bad += not row["metadata_shape_ok"]
        count = len(p["rows"])
        overflow = count > cap
        total += count
        exact = exact and not overflow
        part_reports.append({"ordinal":expected["ordinal"],"accepted_source_rows":count,
            "count_exact":not overflow,"source_row_cap":cap,"cap_exceeded":overflow,
            "metadata_shape_failures":metadata_bad,"distinct_watch_scan_ids_in_sample":len(watch_ids),
            "repeated_watch_scan_ids_in_sample":sum(n-1 for n in watch_ids.values()),
            "whole_part_observed":as_of>=right})
    global_cap = min(cohort.MAX_SOURCE_ROWS,cohort.MAX_DECISIONS//len(frozen["scopes"]),
                     sum(p["source_row_limit"] for p in frozen["parts"]))
    any_part_overflow = any(p["cap_exceeded"] for p in part_reports)
    result["source"] = {"population":"ACCEPTED_FROZEN_WATCH_SCORE_CAPTURES",
        "consumer_version":source_sql.CONSUMER,"parts":part_reports,"accepted_source_rows":total,
        "count_exact":exact,"max_rows_from_frozen_budgets":global_cap,
        "capacity_status":"EXCEEDED" if any_part_overflow or total>global_cap else "WITHIN_CAP_SO_FAR",
        "latest_usable_in_range_utc":latest.isoformat() if latest else None,
        "seconds_since_latest_usable_in_range":(as_of-latest).total_seconds() if latest else None,
        "intake_state":payload.get("intake_state"),
        "collection_cadence_completeness":"UNKNOWN_NO_EXPECTED_SCAN_SCHEDULE_IN_DECLARATION"}
    result.update(full_source_readiness="UNKNOWN_METADATA_AUDIT_ONLY",
        acquisition_performed=False,outcome_reads_performed=False,
        known_limitations=["No formula, first-touch outcome, ranking, or gate was evaluated.",
          "An earlier as-of caps event times; it does not reconstruct a historical database snapshot.",
          "Source counts match accepted intakes, not all market opportunities or all MaxPain snapshots.",
          "Current metadata cannot prove raw bundle hashes, feature completeness, parent eligibility, or byte budgets.",
          "Prior BTC bar and parent membership per intake still require original acquisition validation.",
          "Historical revisions or rejected revision attempts cannot be inferred from the current snapshot.",
          "Intake state and recent timestamps do not establish continuous collection or future readiness."])
    return result


def execute(conn, declaration, *, as_of_utc):
    """Run on a supplied idle psycopg connection; no connection is opened here."""
    if int(conn.info.transaction_status) != 0:
        raise ValueError("AUDIT_REQUIRES_IDLE_SOURCE_CONNECTION")
    query = build_sql(declaration,as_of_utc=as_of_utc)
    with conn.transaction():
        conn.execute("SET TRANSACTION READ ONLY")
        conn.execute("SET LOCAL statement_timeout='15000ms'")
        conn.execute("SET LOCAL lock_timeout='1000ms'")
        conn.execute("SET LOCAL TIME ZONE 'UTC'")
        conn.execute("SET LOCAL DateStyle='ISO, YMD'")
        state = conn.execute("SELECT current_setting('transaction_read_only') AS read_only").fetchone()
        if (state["read_only"] if isinstance(state,dict) else state[0]) != "on":
            raise ValueError("AUDIT_SOURCE_TRANSACTION_NOT_READ_ONLY")
        rows = conn.execute(query).fetchmany(2)
        if len(rows) != 1:
            raise ValueError("AUDIT_RESPONSE_CARDINALITY_MISMATCH")
        payload = rows[0]["audit"] if isinstance(rows[0],dict) else rows[0][0]
        if isinstance(payload,str):
            if len(payload.encode("utf-8")) > MAX_RESPONSE_BYTES:
                raise ValueError("AUDIT_RESPONSE_BYTE_LIMIT_EXCEEDED")
            payload = json.loads(payload)
        if len(contracts.canonical(payload).encode("utf-8")) > MAX_RESPONSE_BYTES:
            raise ValueError("AUDIT_RESPONSE_BYTE_LIMIT_EXCEEDED")
    result = analyze(declaration,payload,as_of_utc=as_of_utc)
    result["query_sha256"] = hashlib.sha256(query.encode()).hexdigest()
    result["response_sha256"] = contracts.digest(payload)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--declaration",required=True,type=Path)
    parser.add_argument("--as-of",default=None)
    parser.add_argument("--execute",action="store_true",help="Read source identified by AUDIT_SOURCE_DATABASE_URL")
    args = parser.parse_args()
    declaration = json.loads(args.declaration.read_text())
    as_of = args.as_of or datetime.now(UTC).isoformat()
    if not args.execute:
        # SQL output is reviewable; execution still requires the explicit flag.
        print("BEGIN READ ONLY;\nSET LOCAL statement_timeout='15000ms';\nSET LOCAL lock_timeout='1000ms';\nSET LOCAL TIME ZONE 'UTC';")
        print(build_sql(declaration,as_of_utc=as_of),end="")
        print("COMMIT;")
        return
    dsn = os.environ.get("AUDIT_SOURCE_DATABASE_URL")
    if not dsn:
        parser.error("AUDIT_SOURCE_DATABASE_URL is required with --execute")
    import psycopg
    from psycopg.rows import dict_row
    with psycopg.connect(dsn,autocommit=True,row_factory=dict_row,
            options="-c default_transaction_read_only=on -c statement_timeout=15000 -c lock_timeout=1000") as conn:
        print(json.dumps(execute(conn,declaration,as_of_utc=as_of),ensure_ascii=False,indent=2))


if __name__ == "__main__":
    main()
