"""Bounded read-only PostgreSQL export of original accepted Watch captures.

Uses one repeatable-read read-only transaction, explicit source/time/byte caps,
parameterized queries, original raw capture hashes and LEFT JOIN provenance.
No archive backfill, provider calls, schema changes or production writes occur.
"""
from __future__ import annotations

import argparse
from datetime import timedelta
import json
import os
from pathlib import Path
from typing import Any

import research_no_horizon_contract as contracts
import research_no_horizon_source as source
import research_watch_scan_formula as formulas
import research_watch_scan_intake as intake
import research_btc_parent_movement as btc

KEY_SQL = """SELECT snapshot_set_id,usable_from_utc FROM public.research_watch_scan_intakes
 WHERE consumer_version=%s AND intake_status='ACCEPTED'
 AND usable_from_utc>=%s AND usable_from_utc<%s
 ORDER BY usable_from_utc,snapshot_set_id LIMIT %s"""

ROW_SQL = """SELECT to_jsonb(i) AS intake,
 jsonb_build_object('snapshot_set_id',s.snapshot_set_id,'snapshot_key',s.snapshot_key,
   'payload_sha256',s.payload_sha256,'cycle_id',s.cycle_id,'source',s.source,
   'available_at_utc',s.available_at_utc,'created_at_utc',s.created_at_utc) AS stored_source,
 s.source_metadata#>'{capture_metadata,operational_scores}' AS scores,
 to_jsonb(p) AS btc_parent,to_jsonb(b) AS btc_prior_bar
 FROM public.research_watch_scan_intakes i
 LEFT JOIN public.research_max_pain_snapshot_sets s USING(snapshot_set_id)
 LEFT JOIN LATERAL (SELECT p.* FROM public.research_btc_parent_movements p
   WHERE p.episode_policy_version=%s AND p.start_time_utc<=i.usable_from_utc
   AND (p.end_time_utc IS NULL OR p.end_time_utc>i.usable_from_utc)
   ORDER BY p.start_time_utc DESC LIMIT 1) p ON TRUE
 LEFT JOIN LATERAL (SELECT b.* FROM public.research_btc_price_bars b
   WHERE b.open_time_utc+INTERVAL '1 minute'<=i.usable_from_utc
   AND b.close_time_utc>i.usable_from_utc-INTERVAL '1 minute'
   ORDER BY b.close_time_utc DESC LIMIT 1) b ON TRUE
 WHERE i.consumer_version=%s AND i.snapshot_set_id=ANY(%s::bigint[])
 ORDER BY i.usable_from_utc,i.snapshot_set_id"""

PRICE_SQL = """SELECT route,symbol,open_time_utc,close_time_utc,open,high,low,close
 FROM public.research_price_archive_bars
 WHERE route=%s AND symbol=%s AND open_time_utc>=%s
 AND open_time_utc+INTERVAL '1 minute'<=%s
 ORDER BY open_time_utc LIMIT %s"""


def export_source(conn: Any, *, start_utc: Any, end_utc: Any, cutoff_utc: Any,
                  symbol: str, row_limit: int = 128, byte_limit: int = source.MAX_BYTES,
                  page_size: int = 16) -> dict:
    """Own a fresh connection's read transaction; return a complete export.

    Exceeding a bound raises and rolls back instead of returning a censored
    population. The source interval is half-open; the observation cutoff is a
    price-availability boundary. All accepted intake rows visible in this DB
    snapshot are retained, even when original sources or parents are missing.
    Intake may have been classified after the historical price cutoff: this is
    explicitly retrospective source evidence, not a time-travel claim.
    """
    source.route_for_symbol(symbol)
    start, end, cutoff = (contracts.utc(v) for v in (start_utc, end_utc, cutoff_utc))
    if not start < end <= cutoff or cutoff-start > timedelta(days=source.MAX_DAYS):
        raise ValueError("SOURCE_TIME_OR_DAY_BOUND_INVALID")
    for name, value, maximum in (("row_limit",row_limit,source.MAX_ROWS),
                                ("byte_limit",byte_limit,source.MAX_BYTES),("page_size",page_size,64)):
        if type(value) is not int or not 1 <= value <= maximum:
            raise ValueError("INVALID_BOUND:"+name)
    if int(conn.info.transaction_status) != 0:
        raise ValueError("EXPORT_REQUIRES_FRESH_IDLE_CONNECTION")
    with conn.transaction():
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        conn.execute("SET LOCAL statement_timeout='15000ms'")
        conn.execute("SET LOCAL lock_timeout='1000ms'")
        state = conn.execute("""SELECT current_setting('transaction_read_only') AS read_only,
            current_setting('transaction_isolation') AS isolation,
            pg_current_snapshot()::text AS mvcc_snapshot,
            statement_timestamp() AS extracted_at_utc""").fetchone()
        if state["read_only"] != "on" or state["isolation"] != "repeatable read":
            raise ValueError("READ_ONLY_SNAPSHOT_NOT_ESTABLISHED")
        if cutoff > contracts.utc(state["extracted_at_utc"]):
            raise ValueError("FUTURE_OBSERVATION_CUTOFF")
        keys = conn.execute(KEY_SQL,(intake.VERSION,start,end,row_limit+1)).fetchall()
        if len(keys) > row_limit:
            raise ValueError("SOURCE_ROW_LIMIT_EXCEEDED")
        rows, measured_bytes = [], 0
        for offset in range(0,len(keys),page_size):
            ids = [item["snapshot_set_id"] for item in keys[offset:offset+page_size]]
            page = conn.execute(ROW_SQL,(btc.POLICY_VERSION,intake.VERSION,ids)).fetchall()
            if [item["intake"]["snapshot_set_id"] for item in page] != ids:
                raise ValueError("SOURCE_PAGE_IDENTITY_OR_COVERAGE_MISMATCH")
            measured_bytes += len(formulas.canonical(page).encode())
            if measured_bytes > byte_limit:
                raise ValueError("SOURCE_EXPORT_BYTE_LIMIT_EXCEEDED")
            rows.extend(page)
        price_limit = source.MAX_DAYS*1440
        candles = conn.execute(PRICE_SQL,(source.ARCHIVE_ROUTE,symbol,
            source._entry_time(start),cutoff,price_limit+1)).fetchall()
        if len(candles) > price_limit:
            raise ValueError("SOURCE_CANDLE_LIMIT_EXCEEDED")
        result = {"export_version": source.EXPORT_VERSION,
            "source_start_utc": start.isoformat(), "source_end_utc": end.isoformat(),
            "cutoff_utc": cutoff.isoformat(), "symbol": symbol,
            "source_rows": rows, "candles": candles,
            "source_receipt": {"transaction_mode":"REPEATABLE_READ_READ_ONLY",
                "expected_accepted_rows":len(keys), "expected_accepted_rows_exact":True,
                "rows_complete":True, "truncated":False, "candle_truncated":False,
                "mvcc_snapshot":state["mvcc_snapshot"], "extracted_at_utc":state["extracted_at_utc"],
                "query_sha256":contracts.digest([KEY_SQL,ROW_SQL,PRICE_SQL]),
                "row_limit":row_limit,"byte_limit":byte_limit,"page_size":page_size,
                "source_universe":"ACCEPTED_FROZEN_WATCH_SCORE_CAPTURES",
                "intake_classification":"RETROSPECTIVE_AT_EXTRACTION",
                "auxiliary_decision_bundle_used":False,"database_writes":False}}
        encoded = formulas.canonical(result)
        if len(encoded.encode()) > byte_limit:
            raise ValueError("SOURCE_EXPORT_BYTE_LIMIT_EXCEEDED")
        return json.loads(encoded)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-env",default="RESEARCH_NO_HORIZON_READ_DATABASE_URL")
    parser.add_argument("--source-start",required=True)
    parser.add_argument("--source-end",required=True)
    parser.add_argument("--cutoff",required=True)
    parser.add_argument("--symbol",required=True)
    parser.add_argument("--row-limit",type=int,default=128)
    parser.add_argument("--byte-limit",type=int,default=source.MAX_BYTES)
    parser.add_argument("--output",type=Path,required=True)
    args = parser.parse_args()
    try:
        import psycopg
        from psycopg.rows import dict_row
        dsn = os.environ.get(args.database_env)
        if not dsn:
            raise ValueError("EXPLICIT_READ_DATABASE_ENV_REQUIRED")
        if args.output.exists():
            raise ValueError("OUTPUT_ALREADY_EXISTS")
        with psycopg.connect(dsn,row_factory=dict_row,connect_timeout=5) as conn:
            result = export_source(conn,start_utc=args.source_start,end_utc=args.source_end,
                cutoff_utc=args.cutoff,symbol=args.symbol,row_limit=args.row_limit,byte_limit=args.byte_limit)
        args.output.parent.mkdir(parents=True,exist_ok=True)
        with args.output.open("x",encoding="utf-8") as stream:
            json.dump(result,stream,ensure_ascii=False,allow_nan=False)
            stream.write("\n")
        print(json.dumps({"output":str(args.output),"source_rows":len(result["source_rows"]),
            "candles":len(result["candles"]),"database_writes":False}))
        return 0
    except (ImportError,OSError,ValueError,TypeError,KeyError):
        # Connection exceptions can contain credentials/connection strings;
        # don't echo arbitrary driver exception text in a terminal receipt.
        parser.exit(2,"BLOCKED: export validation or connection setup failed\n")
    except Exception:
        # psycopg driver failures (including timeouts) need the same sanitized
        # boundary; direct export_source callers still receive the real error.
        parser.exit(2,"BLOCKED: read-only database export failed\n")


if __name__ == "__main__":
    raise SystemExit(main())
