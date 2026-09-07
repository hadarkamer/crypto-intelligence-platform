"""Descriptive candidate cells for the frozen archive delayed-entry cohort.

Select each complete earliest wave cohort before loading any labels. The
artifact keeps failed/missing cells, both periods and orientation variants.
No result becomes a native LIVE formula or prospective validation.
"""
from __future__ import annotations
import argparse
import csv
from collections import Counter, defaultdict
from datetime import datetime, timedelta
import json
from pathlib import Path
import sqlite3

from research_common_window_metrics import aggregate_common_window_metrics
from research_formula_ordered_v7 import candidate_catalog, matches, summarize_scope
from research_telegram_archive_backfill import ENTRY_VERSION, FEATURE_VERSION, TIME_VERSION, WINDOWS, canonical, evaluator_features, parent_policy, utc
from research_telegram_html_archive import SCOPES, ISRAEL, _hash


def summarize(database: Path, *, run_key: str, observed_at: datetime, output: Path):
    output.mkdir(parents=True, exist_ok=True)
    catalog = [candidate for candidate in candidate_catalog(include_extended=True) if candidate.get("research_orientation") != "INVERSE"]
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as conn:
        events = {key: json.loads(encoded) for key, encoded in conn.execute("SELECT event_key,event_json FROM archive_reconstructed_events WHERE run_key=? AND reconstruction_status='READY_FOR_SPOT_ENTRY_PATH' ORDER BY source_time_utc,event_key", (run_key,))}
        statuses = Counter(event.get("calculation_status", "PENDING") for event in events.values())
        if statuses["PENDING"] or statuses["NOT_YET_ENTRY"]:
            raise ValueError("Archive source population still has unprocessed event decisions")
        features = {key: evaluator_features(event) for key, event in events.items()}
        cohorts = {}
        attempts = []
        boundaries = {key: datetime.fromisoformat(lower + "T00:00:00").replace(tzinfo=ISRAEL).astimezone(observed_at.tzinfo) for key, lower in SCOPES.items()}
        for candidate in catalog:
            required = sorted({c["feature"] for c in candidate["conditions"]})
            complete_features = sum(all(field in features[key] and features[key][field] is not None for field in required) for key in events)
            matched = [key for key in events if matches(candidate, features[key], events[key]["analysis_direction"])]
            attempts.append({"formula_id": candidate["formula_id"], "source_messages_matching": len(matched), "source_messages_with_required_fields": complete_features, "required_fields": required, "missing_field_counts": {field: sum(field not in f or f[field] is None for f in features.values()) for field in required}, "status": "DESCRIPTIVE_TEST" if matched else "NO_MATCHES" if complete_features else "BLOCKED_REQUIRED_FIELDS_MISSING", "phase": "DISCOVERY"})
            for key in matched:
                event = events[key]
                if event.get("membership_status") != "LIVE" or not event.get("parent_evidence_eligible"):
                    continue
                for period in event["period_scope_ids"]:
                    if utc(event["parent_start_time_utc"]) < boundaries[period]:
                        continue
                    for variant in ("NORMAL", "INVERSE"):
                        direction = event["analysis_direction"] if variant == "NORMAL" else ("SHORT" if event["analysis_direction"] == "LONG" else "LONG")
                        for symbol in (event["symbol"], "ALL"):
                            group = (candidate["formula_id"], period, variant, direction, symbol, event["btc_parent_movement_id"])
                            moment = utc(event["entry_time_utc"])
                            existing = cohorts.get(group)
                            if existing is None or moment < existing[0]:
                                cohorts[group] = (moment, [key])
                            elif moment == existing[0]:
                                existing[1].append(key)
        selected_ids = {key for _, keys in cohorts.values() for key in keys}
        labels, metrics = {}, {}
        for key in selected_ids:
            for variant, window, threshold, payload in conn.execute("SELECT signal_variant,window_minutes,threshold_bps,outcome_json FROM archive_delayed_entry_outcomes WHERE run_key=? AND event_key=?", (run_key, key)):
                labels[key, variant, window, threshold] = json.loads(payload)
            for variant, window, payload in conn.execute("SELECT signal_variant,window_minutes,metrics_json FROM archive_common_window_metrics WHERE run_key=? AND event_key=?", (run_key, key)):
                metrics[key, variant, window] = json.loads(payload)
    scopes = defaultdict(list)
    for group, (_, keys) in cohorts.items():
        scopes[group[:-1]].extend(keys)
    results = []
    catalog_by_id = {candidate["formula_id"]: candidate for candidate in catalog}
    output_jsonl = output / "archive_candidate_cells.jsonl"
    surrogate_sources = {}
    with output_jsonl.open("w", encoding="utf-8") as handle:
        for scope, keys in sorted(scopes.items()):
            formula_id, period, variant, direction, symbol = scope
            candidate = catalog_by_id[formula_id]
            for window in WINDOWS:
                for threshold in range(25, 201, 25):
                    rows = []
                    for key in keys:
                        event = events[key]
                        label = labels.get((key, variant, window, threshold), {"window_minutes": window, "threshold_bps": threshold, "direction": direction, "status": "DATA_MISSING"})
                        source_id = _hash(event["archive_event_key"], variant)
                        event_id = int(source_id[:15], 16) + 1
                        if event_id in surrogate_sources and surrogate_sources[event_id] != source_id:
                            raise ValueError("Archive evaluator surrogate identity collision")
                        surrogate_sources[event_id] = source_id
                        label = {**label, "event_id": event_id, "archive_event_id": source_id}
                        rows.append({"event_id": event_id, "symbol": event["symbol"], "direction": direction, "entry_price": event.get("entry_price"), "alert_time_utc": event["entry_time_utc"], "decision_time_utc": event["entry_time_utc"], "features_observed_at_utc": event["source_message_time_utc"], "decision_features": features[key], "btc_parent_movement_id": event["btc_parent_movement_id"], "parent_evidence_eligible": event["parent_evidence_eligible"], "membership_status": event["membership_status"], "episode_policy_version": parent_policy.POLICY_VERSION, "ordered_outcome": label, "archive_key": key})
                    result = summarize_scope(rows, analysis_as_of_utc=observed_at)
                    common_by_wave = defaultdict(list)
                    for row in rows:
                        common_by_wave[row["btc_parent_movement_id"]].append(metrics.get((row["archive_key"], variant, window)))
                    wave_metrics, fresh_wave_metrics = [], []
                    fresh_wave_ids = {row["btc_parent_movement_id"] for row in rows if utc(row["decision_time_utc"]) >= observed_at-timedelta(days=14)}
                    for wave_id, members in common_by_wave.items():
                        if all(member and member.get("status") == "READY" and member.get("path_complete") is True for member in members):
                            measured = {**members[0], "mfe_pct": min(member["mfe_pct"] for member in members), "mae_pct": max(member["mae_pct"] for member in members)}
                        else:
                            measured = {"status": "DATA_MISSING"}
                        wave_metrics.append(measured)
                        if wave_id in fresh_wave_ids:
                            fresh_wave_metrics.append(measured)
                    fixed = aggregate_common_window_metrics(wave_metrics, expected_count=len(common_by_wave), window_minutes=window, threshold_bps=threshold)
                    fresh_fixed = aggregate_common_window_metrics(fresh_wave_metrics, expected_count=len(fresh_wave_ids), window_minutes=window, threshold_bps=threshold)
                    eligible = [episode for episode in result["episodes"] if episode["eligible"]]
                    fresh = [episode for episode in eligible if utc(episode["forecast_start_utc"]) >= observed_at - timedelta(days=14)]
                    successes = sum(episode["success"] for episode in eligible)
                    fresh_successes = sum(episode["success"] for episode in fresh)
                    cell = {"candidate_key": _hash("archive-delayed-entry-discovery-candidate-v1", candidate, ENTRY_VERSION, FEATURE_VERSION, TIME_VERSION), "formula_id": formula_id, "conditions": candidate["conditions"], "question_ids": candidate.get("question_ids", []), "period_key": period, "signal_variant": variant, "direction": direction, "symbol": symbol, "window_minutes": window, "threshold_bps": threshold, "entry_policy_version": ENTRY_VERSION, "source_scope": "ARCHIVE_ONLY", "phase": "DISCOVERY", "research_ready": False, "live_union_eligible": False, "number_candidate_patterns_tried": len(catalog)*2, "overlap_group": candidate.get("overlap_group", formula_id), "independent_waves": len(eligible), "decision_waves": result["decision_waves"], "successes": successes, "failures": len(eligible)-successes, "hit_rate_pct_descriptive": 100*successes/len(eligible) if eligible else None, "status_counts": result["all_wave_status_counts"], "fresh_independent_waves": len(fresh), "fresh_successes": fresh_successes, "fresh_failures": len(fresh)-fresh_successes, "fresh_hit_rate_pct_descriptive": 100*fresh_successes/len(fresh) if fresh else None, "fresh_status_counts": result["fresh_wave_status_counts"], "fixed_window_metrics": fixed, "simultaneous_cohort_metrics_method": "MIN_MFE_MAX_MAE_ALL_SIMULTANEOUS_EARLIEST_MEMBERS", "btc_parent_movement_ids": sorted(common_by_wave), "evidence": result}
                    cell.update({"scope_key": _hash(cell["candidate_key"], period, variant, direction, symbol, window, threshold), "fresh_fixed_window_metrics": fresh_fixed, "fresh_decision_waves": len(fresh_wave_ids), "data_partition": "DISCOVERY_ONLY_NO_FUTURE_VALIDATION", "source_population": "CAUSALLY_RECONSTRUCTED_MESSAGES_ONLY", "unobserved_alert_families_are_no_signal": False})
                    handle.write(canonical(cell) + "\n")
                    results.append({key: value for key, value in cell.items() if key not in {"evidence", "conditions"}})
    columns = ["scope_key", "formula_id", "period_key", "signal_variant", "direction", "symbol", "window_minutes", "threshold_bps", "decision_waves", "independent_waves", "successes", "failures", "hit_rate_pct_descriptive", "fresh_independent_waves", "fresh_hit_rate_pct_descriptive", "status_counts", "fixed_window_metrics", "fresh_fixed_window_metrics", "research_ready", "entry_policy_version"]
    with (output / "archive_candidate_cells.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for cell in results:
            writer.writerow({key: canonical(cell[key]) if isinstance(cell.get(key), (dict, list)) else cell.get(key) for key in columns})
    report = {"run_key": run_key, "source_scope": "ARCHIVE_ONLY", "entry_policy_version": ENTRY_VERSION, "phase": "DISCOVERY", "research_ready": False, "source_calculation_counts": dict(statuses), "candidate_patterns_tried_including_inverse": len(catalog)*2, "nonempty_cells_tested": len(results), "candidate_attempts": attempts, "maximum_decisive_independent_waves": max((row["independent_waves"] for row in results), default=0), "periods": list(SCOPES), "observed_at_utc": observed_at.isoformat(), "production_rows_written": 0}
    (output / "archive_candidate_summary.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--run-key", required=True)
    parser.add_argument("--observed-at", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    report = summarize(args.database, run_key=args.run_key, observed_at=utc(args.observed_at), output=args.output_dir)
    print(json.dumps({key: value for key, value in report.items() if key != "candidate_attempts"}, indent=2))
