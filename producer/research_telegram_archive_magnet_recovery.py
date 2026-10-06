"""Versioned, per-message Magnet source repair; no guessed asset association.

The frozen v1 extractor and already measured archive runs are unchanged. This
supplement recovers only printed facts. A separate header, a nearby message,
Telegram's visual ``joined`` CSS class and the legacy Sheet import are never
asset evidence. Missing assets continue to block price measurement.
"""
from __future__ import annotations

import argparse
from collections import Counter
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Mapping

from research_telegram_archive_features import extract_message, NUMBER, SUPPORTED_SYMBOLS, _number
from research_telegram_html_archive import _hash, stage_digest

RECOVERY_VERSION = "telegram-magnet-source-recovery-v1"
FEATURE_VERSION = "telegram-magnet-per-message-features-v1"
DIRECTION_VERSION = "telegram-magnet-upper-long-lower-short-v1"
MAX_SOURCE_ROWS = 20000


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _sha(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def extract_magnet_message(row: Mapping[str, Any], manifest: Mapping[str, Any]) -> dict[str, Any]:
    """Recover direction/components using only this original message's text.

    No optional context argument exists: caller-supplied adjacent/source-link
    values must not silently turn a missing asset into an assigned one.
    """
    if row.get("message_family") != "MAGNET_TARGET":
        raise ValueError("Magnet recovery accepts only MAGNET_TARGET source messages")
    base = extract_message(row, manifest)
    result = deepcopy(base)
    result.update({
        "base_archive_event_key": base["archive_event_key"],
        "base_feature_version": base["feature_version"],
        "base_direction_mapping_version": base["direction_mapping_version"],
        "base_reconstruction_status": base["reconstruction_status"],
        "base_missing_evidence": list(base["missing_evidence"]),
        "feature_version": FEATURE_VERSION,
        "direction_mapping_version": DIRECTION_VERSION,
        "source_recovery_version": RECOVERY_VERSION,
        "archive_event_key": _hash(RECOVERY_VERSION, base["archive_event_key"], FEATURE_VERSION, DIRECTION_VERSION),
        "source_association_policy": "EXPLICIT_ASSET_IN_SAME_MESSAGE_ONLY",
        "source_occurrences": deepcopy(row.get("source_occurrences", [])),
        "source_rows_rewritten": 0,
        "live_union_eligible": False,
        "candidate_eligible": False,
        "training_eligible": False,
    })
    text = re.sub(r"[\u200e\u200f\u202a-\u202e]", "", str(row["message_text"]))
    features, evidence = result["features"], result["field_source_lines"]
    missing, conflicts = result["missing_evidence"], result["conflicts"]

    def put(key: str, value: Any, source: str) -> None:
        if key in features and features[key] != value:
            conflicts.append("CONFLICTING_PRINTED_FIELD:" + key)
            features.pop(key, None)
            evidence.pop(key, None)
            return
        features[key], evidence[key] = value, source

    def numeric(key: str, pattern: str) -> None:
        matches = list(re.finditer(pattern, text, re.MULTILINE))
        if not matches:
            return
        values = [_number(match[1]) for match in matches]
        if len(set(values)) != 1:
            conflicts.append("CONFLICTING_PRINTED_FIELD:" + key)
            features.pop(key, None)
            evidence.pop(key, None)
            return
        put(key, values[0], matches[0][0])

    assets = list(re.finditer(r"^נכס\s*:?\s*([A-Z0-9]+)(?:\s*\|[^\n]*)?\s*$", text, re.MULTILINE))
    symbols = {asset[1] for asset in assets}
    if len(symbols) == 1 and next(iter(symbols)) in SUPPORTED_SYMBOLS:
        result["symbol"] = next(iter(symbols))
        put("event.symbol", result["symbol"], assets[0][0])
        missing[:] = [reason for reason in missing if reason != "UNAMBIGUOUS_SYMBOL_IN_SAME_MESSAGE"]
    elif symbols:
        result["symbol"] = None
        features.pop("event.symbol", None)
        evidence.pop("event.symbol", None)
        if "UNAMBIGUOUS_SYMBOL_IN_SAME_MESSAGE" not in missing:
            missing.append("UNAMBIGUOUS_SYMBOL_IN_SAME_MESSAGE")
        if len(symbols) > 1:
            conflicts.append("CONFLICTING_MAGNET_SYMBOLS")

    # The heading denotes the price target, not the hurt liquidation side.
    # Preserve both labels and never apply Max Pain's inversion here.
    headings = list(re.finditer(r"^מגנט\s+(🔺|🔻)\s+(עליון|תחתון)\s+#([1-9]\d*)\s*$", text, re.MULTILINE))
    headings_valid = len(headings) == 1 and (headings[0][1], headings[0][2]) in {("🔺", "עליון"), ("🔻", "תחתון")}
    if headings_valid:
        heading = headings[0]
        direction = "LONG" if heading[1] == "🔺" else "SHORT"
        printed_directions = {match[1] for match in re.finditer(r"^נכס\s*:?\s*[A-Z0-9]+\s*\|\s*[🟢🔴]?\s*(LONG|SHORT)\b", text, re.MULTILINE)}
        if (base["analysis_direction"] and base["analysis_direction"] != direction) or printed_directions - {direction}:
            conflicts.append("MAGNET_HEADING_DIRECTION_CONFLICT")
        else:
            result["displayed_direction"] = direction
            result["analysis_direction"] = direction
            result["display_direction_inverted"] = False
            missing[:] = [reason for reason in missing if reason != "EXPLICIT_DIRECTION_IN_SAME_MESSAGE"]
            put("magnet.side", "UPPER" if direction == "LONG" else "LOWER", heading[0])
            put("magnet.rank", int(heading[3]), heading[0])
            evidence["analysis_direction"] = heading[0]
    else:
        conflicts.append("MISSING_OR_CONFLICTING_MAGNET_HEADING")

    numeric("magnet.quality", rf"^איכות Magnet Quality:\s*({NUMBER})/100\s*$")
    numeric("magnet.spread_pct", rf"^פיזור Spread:\s*({NUMBER})%\s*$")
    numeric("magnet.liquidity_edge_pct", rf"^נזילות Liquidity Edge:\s*({NUMBER})%\s*$")
    numeric("magnet.target_min", rf"^אזור:\s*\$({NUMBER})(?:\s*[–-]\s*\${NUMBER})?\s*$")
    numeric("magnet.target_max", rf"^אזור:\s*\${NUMBER}\s*[–-]\s*\$({NUMBER})\s*$")
    if "magnet.target_min" in features and "magnet.target_max" not in features and not any(reason.endswith(":magnet.target_max") for reason in conflicts):
        put("magnet.target_max", features["magnet.target_min"], evidence["magnet.target_min"])
    if features.get("magnet.target_min", 0) > features.get("magnet.target_max", float("inf")):
        conflicts.append("REVERSED_MAGNET_TARGET_RANGE")
    if "magnet.quality" in features and not 0 <= features["magnet.quality"] <= 100:
        conflicts.append("MAGNET_QUALITY_OUT_OF_RANGE")
    for key in ("magnet.target_min", "magnet.target_max"):
        if key in features and features[key] <= 0:
            conflicts.append("NONPOSITIVE_PRINTED_FIELD:" + key)
    if features.get("magnet.spread_pct", 0) < 0:
        conflicts.append("NEGATIVE_MAGNET_SPREAD")

    for key, prefix in (("magnet.timeframes", "טווחים:"), ("magnet.liquidity_timeframe", "מקור נזילות: הטווח הרחב בקלאסטר"), ("magnet.printed_confirmation", "מסקנה:")):
        lines = [line for line in text.splitlines() if line.startswith(prefix)]
        values = [line[len(prefix):].strip() for line in lines]
        if len(set(values)) > 1:
            conflicts.append("CONFLICTING_PRINTED_FIELD:" + key)
        elif values and values[0] not in {"", "—"}:
            value = [part.strip() for part in values[0].split(",")] if key == "magnet.timeframes" else values[0]
            put(key, value, lines[0])

    for name, printed in (("price_oi", "Price+OI"), ("futures_cvd", "Futures CVD"), ("spot_cvd", "Spot")):
        key = name + ".signed_total_score"
        numeric(key, rf"^{re.escape(printed)}:[^\n]*?\|\s*ציון\s*({NUMBER})/100\s*$")
        if key in features:
            score = features[key]
            if not -100 <= score <= 100:
                conflicts.append("SIGNED_SCORE_OUT_OF_RANGE:" + key)
            side = "LONG" if score > 0 else "SHORT" if score < 0 else "NEUTRAL"
            for suffix, value in (("total_score", abs(score)), ("direction", side)):
                put(name + "." + suffix, value, evidence[key])
            if result.get("analysis_direction"):
                put(name + ".aligned_score", abs(score) if side == result["analysis_direction"] else -abs(score), evidence[key])

    result["conflicts"] = sorted(set(conflicts))
    result["reconstruction_status"] = "BLOCKED_SOURCE_EVIDENCE" if missing or conflicts else "READY_FOR_SPOT_ENTRY_PATH"
    result["recovery_status"] = "UNRECOVERABLE_WITH_AVAILABLE_SOURCE" if missing or conflicts else "SOURCE_RECOVERED_REQUIRES_PRICE_MEASUREMENT"
    result["calculation_status"] = "BLOCKED_SOURCE_EVIDENCE" if missing or conflicts else "PENDING"
    return result


def run(*, stage_dir: Path, output_dir: Path, expected_stage_digest: str, max_rows: int = MAX_SOURCE_ROWS) -> dict[str, Any]:
    if type(max_rows) is not int or not 1 <= max_rows <= MAX_SOURCE_ROWS:
        raise ValueError("Source recovery requires a bounded row limit")
    manifest_path = stage_dir / "archive_manifest.json"
    source_path = stage_dir / "archive_source_messages.jsonl"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    rows = []
    with source_path.open(encoding="utf-8") as handle:
        for line in handle:
            if len(rows) >= max_rows:
                raise ValueError("Source intake exceeds the bounded row limit")
            rows.append(json.loads(line))
    if manifest.get("prepared_stage_digest") != expected_stage_digest or stage_digest(manifest, rows) != expected_stage_digest:
        raise ValueError("Reviewed source stage digest mismatch")
    targets = [row for row in rows if row.get("message_family") == "MAGNET_TARGET"]
    events = [extract_magnet_message(row, manifest) for row in targets]
    if not events:
        raise ValueError("No Magnet source messages in this reviewed intake")
    run_key = _hash(RECOVERY_VERSION, expected_stage_digest, FEATURE_VERSION, DIRECTION_VERSION)
    # A new supplemental file never rewrites the original stage or SQLite run.
    output_dir.mkdir(parents=True, exist_ok=True)
    events_path = output_dir / "magnet_source_recovery_events.jsonl"
    events_path.write_text("".join(canonical({**event, "source_recovery_run_key": run_key}) + "\n" for event in events), encoding="utf-8")
    report = {
        "source_recovery_version": RECOVERY_VERSION, "run_key": run_key,
        "feature_version": FEATURE_VERSION, "direction_mapping_version": DIRECTION_VERSION,
        "prepared_stage_digest": expected_stage_digest,
        "source_jsonl_sha256": _sha(source_path), "source_scope": "ARCHIVE_ONLY",
        "source_messages_checked": len(rows), "magnet_source_messages": len(events),
        "source_recovered_events": sum(event["reconstruction_status"] == "READY_FOR_SPOT_ENTRY_PATH" for event in events),
        "remaining_blocked_events": sum(event["reconstruction_status"] == "BLOCKED_SOURCE_EVIDENCE" for event in events),
        "direction_counts": dict(Counter(event.get("analysis_direction") or "MISSING" for event in events)),
        "missing_evidence_counts": dict(Counter(reason for event in events for reason in event["missing_evidence"])),
        "conflict_counts": dict(Counter(reason for event in events for reason in event["conflicts"])),
        "feature_coverage_counts": dict(sorted(Counter(key for event in events for key in event["features"]).items())),
        "legacy_import_links_ignored_as_asset_evidence": sum(bool(row.get("existing_source_links")) for row in targets),
        "source_association_policy": "EXPLICIT_ASSET_IN_SAME_MESSAGE_ONLY",
        "source_rows_rewritten": 0, "outcomes_created": 0, "production_rows_written": 0,
        "live_union_eligible": False, "candidate_eligible": False,
        "recovered_events_sha256": _sha(events_path),
        "recovery_limitation": "Symbols absent from target text require independent original-source association; adjacency and visual joined groups do not prove it.",
    }
    (output_dir / "magnet_source_recovery_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--expected-stage-digest", required=True)
    parser.add_argument("--max-rows", type=int, default=MAX_SOURCE_ROWS)
    print(json.dumps(run(**vars(parser.parse_args())), ensure_ascii=False, indent=2))
