"""Read-only contract audit of an immutable reconstructed archive database."""
from __future__ import annotations
import argparse
from collections import Counter
import json
from pathlib import Path
import sqlite3

from research_formula_ordered_v7 import ordered_outcome_evidence
from research_telegram_archive_backfill import ENTRY_VERSION, utc
from research_telegram_html_archive import _hash


def audit(database: Path, *, run_key: str, observed_at, output: Path):
    counts, exclusions, metrics_statuses, membership, coverage = Counter(), Counter(), Counter(), Counter(), Counter()
    families, missing_source_reasons, periods, usable_periods, symbols, field_coverage = Counter(), Counter(), Counter(), Counter(), Counter(), Counter()
    paired = contradictions = 0
    violations = []
    source_count = 0
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as conn:
        if conn.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("Reconstructed database failed integrity check")
        duplicate_ids = conn.execute("SELECT outcome_id,COUNT(*) FROM archive_delayed_entry_outcomes WHERE run_key=? GROUP BY outcome_id HAVING COUNT(*)>1 LIMIT 1", (run_key,)).fetchall()
        if duplicate_ids:
            raise ValueError("Duplicate outcome identifiers")
        for key, encoded in conn.execute("SELECT event_key,event_json FROM archive_reconstructed_events WHERE run_key=?", (run_key,)):
            event = json.loads(encoded)
            source_count += 1
            coverage[event["reconstruction_status"]] += 1
            families[event["message_family"]] += 1
            missing_source_reasons.update(event.get("missing_evidence", []))
            periods.update(event["period_scope_ids"])
            if event["reconstruction_status"] != "READY_FOR_SPOT_ENTRY_PATH":
                continue
            symbols[event["symbol"]] += 1
            field_coverage.update(field for field,value in event["features"].items() if value is not None)
            if event.get("calculation_status") == "COMPLETE_64_LABELS":
                usable_periods.update(event["period_scope_ids"])
            membership[event.get("membership_status", "MISSING")] += 1
            by_cell = {}
            labels = conn.execute("SELECT signal_variant,window_minutes,threshold_bps,outcome_id,outcome_json FROM archive_delayed_entry_outcomes WHERE run_key=? AND event_key=?", (run_key, key)).fetchall()
            if event.get("calculation_status") == "COMPLETE_64_LABELS" and len(labels) != 64:
                violations.append({"event_key": key, "reason": "INCOMPLETE_LABEL_MATRIX", "count": len(labels)})
            if event.get("calculation_status") == "UNSUPPORTED_SPOT_SYMBOL" and labels:
                violations.append({"event_key": key, "reason": "UNSUPPORTED_SYMBOL_HAS_LABELS"})
            for variant, window, threshold, outcome_id, payload in labels:
                label = json.loads(payload)
                full_id = _hash(key, variant)
                surrogate = int(full_id[:15], 16)+1
                row = {"event_id": surrogate, "entry_price": event.get("entry_price"), "direction": label["direction"], "decision_time_utc": event["entry_time_utc"], "ordered_outcome": {**label, "event_id": surrogate}}
                check = ordered_outcome_evidence(row, analysis_as_of_utc=observed_at)
                counts[check["reported_status"]] += 1
                # Nondecisive states are intentional, not invalid evidence.
                actual = [reason for reason in check["exclusion_reasons"] if reason != "UNRESOLVED_OR_OPEN_OUTCOME"]
                exclusions.update(actual)
                if label.get("event_id") != full_id or label.get("outcome_id") != outcome_id or label.get("entry_policy_version") != ENTRY_VERSION:
                    violations.append({"event_key": key, "reason": "ARCHIVE_IDENTITY_OR_ENTRY_CONTRACT"})
                by_cell[variant, window, threshold] = label
            for window in (60, 240, 720, 1440):
                for threshold in range(25, 201, 25):
                    normal, inverse = by_cell.get(("NORMAL", window, threshold)), by_cell.get(("INVERSE", window, threshold))
                    if normal and inverse:
                        paired += 1
                        expected = {"SUCCESS": "FAILURE", "FAILURE": "SUCCESS"}.get(normal["status"], normal["status"])
                        if inverse["status"] != expected or normal["measurement_start_utc"] != inverse["measurement_start_utc"] or normal["reference_price"] != inverse["reference_price"]:
                            contradictions += 1
            metric_rows = conn.execute("SELECT metrics_json FROM archive_common_window_metrics WHERE run_key=? AND event_key=?", (run_key, key)).fetchall()
            if event.get("calculation_status") == "COMPLETE_64_LABELS" and len(metric_rows) != 8:
                violations.append({"event_key": key, "reason": "INCOMPLETE_COMMON_WINDOW_MATRIX"})
            for (payload,) in metric_rows:
                metric = json.loads(payload)
                metrics_statuses[metric["status"]] += 1
                if metric["status"] == "READY" and (metric.get("path_complete") is not True or any(metric.get(field) is None or metric[field] < 0 for field in ("mfe_pct", "mae_pct"))):
                    violations.append({"event_key": key, "reason": "INVALID_READY_COMMON_WINDOW_METRIC"})
    report = {"run_key": run_key, "source_scope": "ARCHIVE_ONLY", "source_records": source_count, "reconstruction_status_counts": dict(coverage), "membership_status_counts": dict(membership), "v7_reported_status_counts": dict(counts), "v7_contract_exclusion_counts": dict(exclusions), "paired_normal_inverse_cells": paired, "contradictory_pairs": contradictions, "common_window_status_counts": dict(metrics_statuses), "violations": violations, "observed_at_utc": observed_at.isoformat(), "production_rows_written": 0}
    report.update({"source_message_family_counts":dict(families), "missing_source_evidence_counts":dict(missing_source_reasons), "all_source_records_by_period":dict(periods), "complete_spot_source_events_by_period":dict(usable_periods), "reconstructed_source_event_symbol_counts":dict(symbols), "causal_raw_feature_coverage":dict(field_coverage)})
    report["valid"] = not (violations or exclusions or contradictions)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--run-key", required=True)
    parser.add_argument("--observed-at", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = audit(args.database, run_key=args.run_key, observed_at=utc(args.observed_at), output=args.output)
    print(json.dumps(report, indent=2))
    raise SystemExit(0 if report["valid"] else 1)
