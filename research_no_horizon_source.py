"""Revalidate a frozen accepted-Watch population into a no-horizon snapshot.

Pure transformation: no DB, price requests, score recalculation or notifications.
The declared universe is accepted captured scans, not every market opportunity.
Source hashes and DB receipt consistency are checked; this is retrospective
source validation, never a claim of prospective qualification or exchange fills.
"""
from __future__ import annotations

from collections import Counter
import argparse
from datetime import timedelta
import json
from pathlib import Path
from typing import Any, Mapping

import research_no_horizon_contract as contracts
import research_no_horizon_first_touch as touch
import research_watch_scan_formula_maxpain as formulas
import research_watch_scan_intake as intake
import research_watch_scan_measurement as measurement
import research_btc_parent_movement as btc

VERSION = "no-horizon-accepted-watch-source-v3-maxpain"
EXPORT_VERSION = "no-horizon-watch-source-export-v1"
MAX_BYTES = 64 * 1024 * 1024
MAX_ROWS = 256
MAX_DAYS = 31
ARCHIVE_ROUTE = "BINANCE_SPOT_TRADE_1M"
CONSISTENT_READS = {"REPEATABLE_READ_READ_ONLY", "SINGLE_STATEMENT_READ_ONLY",
                    "MANIFEST_ATTESTED_MULTI_READ_V1"}


def route_for_symbol(symbol: str) -> dict:
    if symbol not in measurement.SPOT_SYMBOLS:
        raise ValueError("ONLY_UNAMBIGUOUS_BINANCE_SPOT_SYMBOLS_SUPPORTED")
    return {"exchange": "BINANCE", "market": "SPOT", "instrument": symbol+"USDT",
            "price_type": "TRADE", "interval_seconds": 60}


def _json(value: Any) -> Any:
    return json.loads(formulas.canonical(value))


def _entry_time(usable):
    floor = usable.replace(second=0, microsecond=0)
    return floor if floor == usable else floor + timedelta(minutes=1)


def _parent_evidence(parent, prior, decision):
    """A newer BTC bar cannot be assigned through a stale active parent row."""
    if not isinstance(parent, Mapping) or not isinstance(prior, Mapping):
        return None, None
    try:
        for key in ("start_time_utc", "end_time_utc", "confirmed_at_utc", "observed_through_utc"):
            if parent.get(key) is not None:
                contracts.utc(parent[key])
        closed = contracts.utc(prior["close_time_utc"])
        contracts.utc(prior["open_time_utc"])
        btc.validate_candle(prior)
        if (parent.get("episode_policy_version") != btc.POLICY_VERSION
                or parent.get("price_source") != btc.SOURCE or prior.get("price_source") != btc.SOURCE
                or not measurement._hash(parent.get("btc_parent_movement_id"))
                or parent["btc_parent_movement_id"] != btc._identity(contracts.utc(parent["start_time_utc"]))
                or closed + timedelta(milliseconds=1) > decision
                or contracts.utc(parent["observed_through_utc"]) < closed):
            return None, None
        if parent.get("evidence_eligible") is True and (
                contracts.utc(parent["confirmed_at_utc"]) != contracts.utc(parent["start_time_utc"])
                or parent.get("boundary_reason") != "CAUSAL_CLOSE_REVERSAL"
                or parent.get("direction") not in ("UP", "DOWN")):
            return None, None
    except (KeyError, TypeError, ValueError, OverflowError):
        return None, None
    return parent, prior


def _validate_row(row: Mapping[str, Any], *, start, end, cutoff, symbol) -> tuple[dict, dict]:
    original = row["intake"]
    raw = row["stored_source"]
    if not isinstance(original, Mapping) or not isinstance(raw, Mapping):
        raise ValueError("MISSING_ORIGINAL_SOURCE")
    if (original.get("consumer_version") != intake.VERSION or original.get("intake_status") != "ACCEPTED"
            or type(original.get("snapshot_set_id")) is not int or original["snapshot_set_id"] <= 0
            or original["snapshot_set_id"] != raw.get("snapshot_set_id")):
        raise ValueError("INVALID_ORIGINAL_INTAKE")
    validated = intake.validate_source({**raw, "bundle": row.get("scores")}, now=cutoff, activated_at=start)
    if validated["intake_status"] != "ACCEPTED":
        raise ValueError("RAW_SOURCE_REJECTED:"+str(validated["rejection_reason"]))
    time_fields = ("observed_at_utc", "source_available_at_utc", "source_created_at_utc", "usable_from_utc")
    identity_fields = ("snapshot_key", "parent_payload_sha256", "watch_scan_id", "source_version",
                       "bundle_sha256", "capture_status", "coin_count", "score_slot_count")
    for key in identity_fields:
        if original.get(key) != validated[key]:
            raise ValueError("SOURCE_RECEIPT_MISMATCH:"+key)
    for key in time_fields:
        if contracts.utc(original[key]) != contracts.utc(validated[key]):
            raise ValueError("SOURCE_RECEIPT_MISMATCH:"+key)
    usable = contracts.utc(validated["usable_from_utc"])
    if not start <= usable < end:
        raise ValueError("SOURCE_OUTSIDE_FROZEN_INTERVAL")
    if contracts.utc(original["ingested_at_utc"]) < usable:
        raise ValueError("INTAKE_PRECEDES_SOURCE_USABILITY")
    coin = row["scores"]["coins"][symbol]
    observation = {**validated, "consumer_version": intake.VERSION,
        "population_version": intake.POPULATION, "symbol": symbol,
        "maxpain_slots": coin["maxpain"],
        **{key: coin[key] for key in ("models", "sources", "source_time_errors")}}
    return observation, formulas.evaluate_coin(observation)


def _extraction_blockers(export, rows, receipt):
    """Shared receipt checks; keep feature preflight and snapshot semantics equal."""
    blockers = []
    if receipt.get("transaction_mode") == "MANIFEST_ATTESTED_MULTI_READ_V1":
        # The source belongs to one manifest snapshot; its payload was fetched
        # using separate read-only transactions. Never equate those claims.
        from research_no_horizon_manifest import validate_export_binding
        try:
            validate_export_binding(export)
        except (ValueError, TypeError, KeyError, OverflowError):
            blockers.append("INVALID_MANIFEST_ATTESTED_SOURCE_EXTRACTION")
    if (type(receipt.get("expected_accepted_rows")) is not int or receipt["expected_accepted_rows"] != len(rows)
            or receipt.get("rows_complete") is not True or receipt.get("truncated") is not False
            or receipt.get("expected_accepted_rows_exact",True) is not True
            or receipt.get("candle_truncated",False) is not False
            or receipt.get("transaction_mode") not in CONSISTENT_READS):
        blockers.append("INCOMPLETE_OR_INCONSISTENT_SOURCE_EXTRACTION")
    return blockers


def build_snapshot(export: Mapping[str, Any], candidate_key: str, base_direction: str,
                   symbol: str, threshold_pct: Any) -> dict[str, Any]:
    """Build the existing replay schema; all unknown decisions remain blockers.

    NO_MATCH is established only by the existing three-valued predicate engine.
    MATCH rows with unknown BTC membership remain opportunities. A matched row
    lacking an entry open cannot fabricate a price: its ID is retained in the
    source ledger and blocks completeness. Raw accepted scans are hashed before
    filtering. Auxiliary stored formula matches/outcomes are never consumed.
    """
    source_route = route_for_symbol(symbol)
    threshold_pct = contracts.number(threshold_pct, "threshold_pct")
    if not 0 < threshold_pct < 100:
        raise ValueError("INVALID_SYMMETRIC_THRESHOLD")
    if base_direction not in formulas.DIRECTIONS:
        raise ValueError("INVALID_BASE_DIRECTION")
    candidates = {item["candidate_key"]: item for item in formulas.catalog_records()}
    candidate = candidates.get(candidate_key)
    if candidate is None or not candidate["supported"]:
        raise ValueError("UNSUPPORTED_EXISTING_CANDIDATE")
    encoded = formulas.canonical(export)
    if len(encoded.encode()) > MAX_BYTES:
        raise ValueError("SOURCE_EXPORT_BYTE_LIMIT_EXCEEDED")
    export = json.loads(encoded)
    if export.get("export_version") != EXPORT_VERSION or export.get("symbol") != symbol:
        raise ValueError("SOURCE_EXPORT_IDENTITY_MISMATCH")
    start, end, cutoff = (contracts.utc(export[key]) for key in ("source_start_utc", "source_end_utc", "cutoff_utc"))
    if not start < end <= cutoff or cutoff-start > timedelta(days=MAX_DAYS):
        raise ValueError("SOURCE_TIME_OR_DAY_BOUND_INVALID")
    rows, bars = export.get("source_rows"), export.get("candles")
    if not isinstance(rows, list) or len(rows) > MAX_ROWS or not isinstance(bars, list) or len(bars) > MAX_DAYS*1440:
        raise ValueError("SOURCE_ROW_OR_CANDLE_LIMIT_EXCEEDED")
    receipt = export.get("source_receipt")
    if not isinstance(receipt, Mapping):
        raise ValueError("SOURCE_EXTRACTION_RECEIPT_REQUIRED")
    blockers = _extraction_blockers(export, rows, receipt)
    cleaned, by_open = [], {}
    for raw in bars:
        if raw.get("route") != ARCHIVE_ROUTE or raw.get("symbol") != symbol:
            raise ValueError("ARCHIVED_PRICE_ROUTE_MISMATCH")
        bar = touch._bar(raw, cutoff=cutoff)
        if bar is None or not start.replace(second=0, microsecond=0) <= contracts.utc(bar["open_time_utc"]) < cutoff:
            raise ValueError("PRICE_OUTSIDE_FROZEN_OBSERVATION")
        opened = bar["open_time_utc"]
        if opened in by_open or cleaned and opened <= cleaned[-1]["open_time_utc"]:
            raise ValueError("PRICE_ORDER_OR_DUPLICATE_CONFLICT")
        by_open[opened] = bar
        cleaned.append(bar)
    source_digest = contracts.digest(export)
    selection = {"adapter_version": VERSION, "candidate_key": candidate_key,
        "candidate_definition_sha256": candidate["definition_sha256"], "catalog_sha256": formulas.CATALOG_SHA256,
        "feature_version": formulas.FEATURE_VERSION, "base_direction": base_direction,
        "symbol": symbol, "threshold_pct": contracts.number(threshold_pct, "threshold_pct"),
        "source_start_utc": start.isoformat(), "source_end_utc": end.isoformat(),
        "source_universe": "ACCEPTED_FROZEN_WATCH_SCORE_CAPTURES", "entry_policy": contracts.ENTRY_POLICY}
    dataset_id = "watch-export:"+source_digest
    cohort_id = "watch-cohort:"+contracts.digest(selection)
    ledger, opportunities, seen = [], [], set()
    for raw in rows:
        source_id = (raw.get("intake") or {}).get("snapshot_set_id")
        record = {"snapshot_set_id": source_id}
        try:
            if type(source_id) is not int or source_id in seen:
                raise ValueError("INVALID_OR_DUPLICATE_SOURCE_ID")
            seen.add(source_id)
            observation, result = _validate_row(raw, start=start, end=end, cutoff=cutoff, symbol=symbol)
            match = next(item for item in result["evaluations"]
                         if item["candidate_key"] == candidate_key and item["base_direction"] == base_direction)
            status = match["match_status"]
            record.update(match_status=status, missing_features=match["missing_features"],
                feature_sha256=result["feature_sha256"], usable_from_utc=result["usable_from_utc"],
                source_observed_at_utc=result["source_observed_at_utc"],
                intake_ingested_at_utc=contracts.utc(raw["intake"]["ingested_at_utc"]).isoformat(),
                bundle_sha256=observation["bundle_sha256"], parent_payload_sha256=observation["parent_payload_sha256"])
            if status == "UNKNOWN":
                blockers.append("UNKNOWN_POTENTIALLY_MATCHING_FEATURES")
            if status != "MATCH":
                ledger.append(record)
                continue
            decision = contracts.utc(observation["usable_from_utc"])
            entry_time = _entry_time(decision)
            entry = by_open.get(entry_time.isoformat())
            record["entry_time_utc"] = entry_time.isoformat()
            if entry is None:
                record["blocker"] = "MISSING_MATCHED_ENTRY_OPEN"
                blockers.append(record["blocker"])
                ledger.append(record)
                continue
            parent, prior = _parent_evidence(raw.get("btc_parent"), raw.get("btc_prior_bar"), decision)
            member = measurement._membership(decision, parent, prior)
            if member["membership_status"] != "LIVE":
                blockers.append("UNVERIFIED_MATCHED_BTC_PARENT")
            record["membership_status"] = member["membership_status"]
            args = {"candidate_id": candidate_key, "candidate_version": candidate["definition_sha256"],
                "cohort_id": cohort_id, "dataset_id": dataset_id, "entry_id": f"watch:{source_id}:{symbol}:{base_direction}",
                "symbol": symbol, "direction": match["analysis_direction"], "decision_time_utc": decision.isoformat(),
                "reference_price": entry["open"], "threshold_pct": threshold_pct,
                "source_route": source_route, "parent_policy_version": btc.POLICY_VERSION}
            contracts.make_contract(**args)
            opportunities.append({"contract": args, "btc_parent_movement_id": member["btc_parent_movement_id"],
                "membership_status": member["membership_status"], "parent_evidence_eligible": bool(parent and parent.get("evidence_eligible") is True),
                "parent_start_time_utc": parent.get("start_time_utc") if parent else None,
                "parent_confirmed_at_utc": parent.get("confirmed_at_utc") if parent else None,
                "features_observed_at_utc": contracts.utc(observation["observed_at_utc"]).isoformat(),
                "parent_policy_version": btc.POLICY_VERSION})
        except (ValueError, TypeError, KeyError, OverflowError, StopIteration) as exc:
            record.update(match_status="UNKNOWN_SOURCE", blocker=str(exc))
            blockers.append("INVALID_OR_MISSING_FROZEN_SOURCE")
        ledger.append(record)
    opportunities.sort(key=lambda item: (item["contract"]["decision_time_utc"], item["contract"]["entry_id"]))
    ledger.sort(key=lambda item: str(item["snapshot_set_id"]))
    counts = Counter(item["match_status"] for item in ledger)
    return {"snapshot_version": "no-horizon-snapshot-v1", "dataset_id": dataset_id, "cohort_id": cohort_id,
        "source_route": source_route, "source_coverage_complete": not blockers,
        "cutoff_utc": cutoff.isoformat(), "candles": cleaned, "opportunities": opportunities,
        "source_receipt": {"adapter_version": VERSION, "source_export_sha256": source_digest,
            "selection": selection, "candidate_definition": candidate["definition"],
            "extraction_receipt": receipt, "accepted_source_rows": len(rows),
            "counts": {key: counts[key] for key in ("MATCH", "NO_MATCH", "UNKNOWN", "UNKNOWN_SOURCE")},
            "emitted_opportunities": len(opportunities), "decision_ledger": ledger,
            "source_blockers": sorted(set(blockers)), "source_manifest_sha256": contracts.digest([selection,ledger,receipt]),
            "source_hashes_and_capture_validation": "BLOCKED_ORIGINAL_SOURCE" if counts["UNKNOWN_SOURCE"] else "REVALIDATED_AGAINST_ORIGINAL_CAPTURE_AND_INTAKE",
            "validated_source_rows": len(rows)-counts["UNKNOWN_SOURCE"],
            "intake_classification": "RETROSPECTIVE_AT_EXTRACTION",
            "auxiliary_decision_bundle_used": False,
            "is_prospective_formula_evidence": False, "db_origin_authenticated_by_pure_adapter": False}}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("export",type=Path)
    parser.add_argument("--candidate",required=True)
    parser.add_argument("--base-direction",choices=formulas.DIRECTIONS,required=True)
    parser.add_argument("--symbol",required=True)
    parser.add_argument("--threshold-pct",type=float,required=True)
    parser.add_argument("--output",type=Path,required=True)
    args=parser.parse_args()
    try:
        if args.output.exists():
            raise ValueError("OUTPUT_ALREADY_EXISTS")
        with args.export.open("rb") as stream:
            raw=stream.read(MAX_BYTES+1)
        if len(raw)>MAX_BYTES:
            raise ValueError("SOURCE_EXPORT_BYTE_LIMIT_EXCEEDED")
        from research_no_horizon_replay import _object, _nonfinite
        value=json.loads(raw,object_pairs_hook=_object,parse_constant=_nonfinite)
        if not isinstance(value,dict):
            raise ValueError("SOURCE_EXPORT_OBJECT_REQUIRED")
        result=build_snapshot(value,args.candidate,args.base_direction,args.symbol,args.threshold_pct)
        args.output.parent.mkdir(parents=True,exist_ok=True)
        with args.output.open("x",encoding="utf-8") as stream:
            json.dump(result,stream,ensure_ascii=False,allow_nan=False)
            stream.write("\n")
        print(json.dumps({"output":str(args.output),"source_rows":result['source_receipt']['accepted_source_rows'],
            "counts":result['source_receipt']['counts'],"opportunities":len(result['opportunities']),
            "source_coverage_complete":result['source_coverage_complete'],
            "source_blockers":result['source_receipt']['source_blockers'],"trading_authorized":False}))
        return 0
    except (OSError,ValueError,TypeError,KeyError):
        parser.exit(2,"BLOCKED: source snapshot validation or file operation failed\n")


if __name__=="__main__":
    raise SystemExit(main())
