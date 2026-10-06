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

from research_common_window_metrics import aggregate_common_window_metrics
from research_formula_ordered_v7 import candidate_catalog, matches, summarize_scope
from research_telegram_archive_backfill import ENTRY_VERSION, FEATURE_VERSION, TIME_VERSION, WINDOWS, canonical, evaluator_features, parent_policy, utc
from research_telegram_html_archive import SCOPES, ISRAEL, _hash
from research_telegram_archive_population import population
from research_archive_price_policy import POLICY_VERSION as ARCHIVE_PRICE_POLICY, aggregate_archive_wave_metrics


def summarize(database: Path, *, run_key: str, observed_at: datetime, output: Path,
        expected_sha256=None, supplement_database=None, supplement_run_key=None, supplement_sha256=None):
    output.mkdir(parents=True, exist_ok=True)
    catalog = [candidate for candidate in candidate_catalog(include_extended=True) if candidate.get("research_orientation") != "INVERSE"]
    with population(database, run_key=run_key, expected_sha256=expected_sha256,
            supplement_database=supplement_database, supplement_run_key=supplement_run_key,
            supplement_sha256=supplement_sha256) as (events, load_children, source_names, population_report):
        statuses = Counter(event.get("calculation_status", "PENDING") for event in events.values())
        if statuses["PENDING"] or statuses["NOT_YET_ENTRY"]:
            raise ValueError("Archive source population still has unprocessed event decisions")
        features = {key: evaluator_features(event) for key, event in events.items()}
        entry_contract = ARCHIVE_PRICE_POLICY if supplement_database else ENTRY_VERSION
        catalog_digest = _hash(catalog)
        analysis_key = _hash("archive-candidate-analysis-v2", population_report["sources"], entry_contract, observed_at.isoformat(), catalog_digest)
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
            selected_labels, selected_metrics = load_children(key)
            for variant, window, threshold, payload in selected_labels:
                labels[key, variant, window, threshold] = payload
            for variant, window, payload in selected_metrics:
                metrics[key, variant, window] = payload
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
                        rows.append({"event_id": event_id, "symbol": event["symbol"], "direction": direction, "entry_price": event.get("entry_price"), "alert_time_utc": event["entry_time_utc"], "decision_time_utc": event["entry_time_utc"], "features_observed_at_utc": event["source_message_time_utc"], "decision_features": features[key], "btc_parent_movement_id": event["btc_parent_movement_id"], "parent_evidence_eligible": event["parent_evidence_eligible"], "membership_status": event["membership_status"], "episode_policy_version": parent_policy.POLICY_VERSION, "ordered_outcome": label, "archive_key": key, "source_scope": "ARCHIVE_ONLY", "record_mode": "ARCHIVE", "live_union_eligible": False, "entry_policy_version": event["entry_policy_version"], "price_source_contract": event.get("price_source_contract")})
                    result = summarize_scope(rows, analysis_as_of_utc=observed_at, archive_price_policy=ARCHIVE_PRICE_POLICY if supplement_database else None)
                    common_by_wave = defaultdict(list)
                    for row in rows:
                        common_by_wave[row["btc_parent_movement_id"]].append(metrics.get((row["archive_key"], variant, window)))
                    fresh_wave_ids = {row["btc_parent_movement_id"] for row in rows if utc(row["decision_time_utc"]) >= observed_at-timedelta(days=14)}
                    if supplement_database:
                        fixed = aggregate_archive_wave_metrics(list(common_by_wave.values()), window_minutes=window, threshold_bps=threshold)
                        fresh_fixed = aggregate_archive_wave_metrics([members for wave_id, members in common_by_wave.items() if wave_id in fresh_wave_ids], window_minutes=window, threshold_bps=threshold)
                    else:
                        wave_metrics, fresh_wave_metrics = [], []
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
                    cell = {"candidate_key": _hash("archive-delayed-entry-discovery-candidate-v1", candidate, entry_contract, FEATURE_VERSION, TIME_VERSION), "formula_id": formula_id, "conditions": candidate["conditions"], "question_ids": candidate.get("question_ids", []), "period_key": period, "signal_variant": variant, "direction": direction, "symbol": symbol, "window_minutes": window, "threshold_bps": threshold, "entry_policy_version": entry_contract, "source_scope": "ARCHIVE_ONLY", "phase": "DISCOVERY", "research_ready": False, "live_union_eligible": False, "number_candidate_patterns_tried": len(catalog)*2, "overlap_group": candidate.get("overlap_group", formula_id), "independent_waves": len(eligible), "decision_waves": result["decision_waves"], "successes": successes, "failures": len(eligible)-successes, "hit_rate_pct_descriptive": 100*successes/len(eligible) if eligible else None, "status_counts": result["all_wave_status_counts"], "fresh_independent_waves": len(fresh), "fresh_successes": fresh_successes, "fresh_failures": len(fresh)-fresh_successes, "fresh_hit_rate_pct_descriptive": 100*fresh_successes/len(fresh) if fresh else None, "fresh_status_counts": result["fresh_wave_status_counts"], "fixed_window_metrics": fixed, "simultaneous_cohort_metrics_method": "MIN_MFE_MAX_MAE_ALL_SIMULTANEOUS_EARLIEST_MEMBERS", "btc_parent_movement_ids": sorted(common_by_wave), "evidence": result}
                    cell.update({"scope_key": _hash(cell["candidate_key"], period, variant, direction, symbol, window, threshold), "fresh_fixed_window_metrics": fresh_fixed, "fresh_decision_waves": len(fresh_wave_ids), "data_partition": "DISCOVERY_ONLY_NO_FUTURE_VALIDATION", "source_population": "CAUSALLY_RECONSTRUCTED_MESSAGES_ONLY", "unobserved_alert_families_are_no_signal": False, "analysis_key": analysis_key, "price_source_member_counts": dict(Counter(source_names[key] for key in keys)), "entry_policy_versions": sorted({events[key]["entry_policy_version"] for key in keys}), "source_run_keys": [item["run_key"] for item in population_report["sources"]]})
                    handle.write(canonical(cell) + "\n")
                    results.append({key: value for key, value in cell.items() if key not in {"evidence", "conditions"}})
    columns = ["scope_key", "formula_id", "period_key", "signal_variant", "direction", "symbol", "window_minutes", "threshold_bps", "decision_waves", "independent_waves", "successes", "failures", "hit_rate_pct_descriptive", "fresh_independent_waves", "fresh_hit_rate_pct_descriptive", "status_counts", "fixed_window_metrics", "fresh_fixed_window_metrics", "research_ready", "entry_policy_version", "entry_policy_versions", "price_source_member_counts", "source_run_keys", "analysis_key"]
    with (output / "archive_candidate_cells.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for cell in results:
            writer.writerow({key: canonical(cell[key]) if isinstance(cell.get(key), (dict, list)) else cell.get(key) for key in columns})
    report = {"run_key": run_key, "analysis_key": analysis_key, "catalog_digest": catalog_digest, "population": population_report, "source_scope": "ARCHIVE_ONLY", "entry_policy_version": entry_contract, "phase": "DISCOVERY", "research_ready": False, "source_calculation_counts": dict(statuses), "candidate_patterns_tried_including_inverse": len(catalog)*2, "nonempty_cells_tested": len(results), "candidate_attempts": attempts, "maximum_decisive_independent_waves": max((row["independent_waves"] for row in results), default=0), "periods": list(SCOPES), "observed_at_utc": observed_at.isoformat(), "production_rows_written": 0}
    (output / "archive_candidate_summary.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--run-key", required=True)
    parser.add_argument("--observed-at", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--expected-sha256")
    parser.add_argument("--supplement-database", type=Path)
    parser.add_argument("--supplement-run-key")
    parser.add_argument("--supplement-sha256")
    args = parser.parse_args()
    report = summarize(args.database, run_key=args.run_key, observed_at=utc(args.observed_at), output=args.output_dir, expected_sha256=args.expected_sha256, supplement_database=args.supplement_database, supplement_run_key=args.supplement_run_key, supplement_sha256=args.supplement_sha256)
    print(json.dumps({key: value for key, value in report.items() if key != "candidate_attempts"}, indent=2))
