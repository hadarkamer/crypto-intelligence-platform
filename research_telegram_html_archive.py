"""Prepare Telegram HTML source evidence without creating LIVE events or labels.

The output is a reproducible staging manifest, not a formula dataset. Identity is
chat + Telegram message id; content/time revisions remain separate audit rows.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
import hashlib
from html.parser import HTMLParser
import json
from pathlib import Path
import re
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

PARSER_VERSION = "telegram-html-source-stage-v1"
SCOPE_VERSION = "research-periods-israel-v1"
ISRAEL = ZoneInfo("Asia/Jerusalem")
UTC = timezone.utc
SCOPES = {
    "ALL_COMPATIBLE_SINCE_20260816": "2026-08-16",
    "SINCE_20260904": "2026-09-04",
}
TIME_POLICIES = ("html-offset-v1", "israel-wall-clock-v1")


def _hash(*parts: Any) -> str:
    return hashlib.sha256(json.dumps(parts, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()


def stage_digest(manifest: Mapping[str, Any], rows: list[dict[str, Any]]) -> str:
    """Bind reviewed provenance, proposed times and links, not only raw text."""
    contents = {
        "manifest": {key: value for key, value in manifest.items() if key != "prepared_stage_digest"},
        "rows": sorted(rows, key=lambda row: (row["source_identity_key"], row["source_revision_sha256"])),
    }
    return hashlib.sha256(json.dumps(contents, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _text(value: str) -> str:
    return "\n".join(line.strip() for line in value.splitlines() if line.strip())


class _Messages(HTMLParser):
    """Read Telegram's structured message/date/text divs; ignore markup layout."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.depth = 0
        self.message_depth: int | None = None
        self.text_depth: int | None = None
        self.message: dict[str, Any] | None = None
        self.parts: list[str] = []
        self.rows: list[dict[str, Any]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr = dict(attrs)
        classes = set((attr.get("class") or "").split())
        if tag == "div":
            self.depth += 1
            if {"message", "default"} <= classes:
                self.message_depth = self.depth
                self.message = {"source_message_id": attr.get("id") or "", "raw_time_title": ""}
                self.parts = []
            elif self.message is not None and {"date", "details"} <= classes:
                self.message["raw_time_title"] = attr.get("title") or ""
            elif self.message is not None and "text" in classes:
                self.text_depth = self.depth
        elif tag == "br" and self.text_depth is not None:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag != "div":
            return
        if self.depth == self.text_depth:
            self.text_depth = None
        if self.depth == self.message_depth and self.message is not None:
            self.message["message_text"] = _text("".join(self.parts))
            self.rows.append(self.message)
            self.message = None
            self.message_depth = None
        self.depth -= 1

    def handle_data(self, data: str) -> None:
        if self.text_depth is not None:
            self.parts.append(data)


def normalize_time(title: str, policy: str) -> dict[str, Any]:
    if policy not in TIME_POLICIES:
        raise ValueError("Unknown explicit time normalization policy")
    match = re.fullmatch(r"(\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2}) UTC([+-]\d{2}:\d{2})", title)
    if not match:
        return {"time_status": "DATA_MISSING", "normalization_version": policy}
    try:
        wall_time = datetime.strptime(match[1], "%d.%m.%Y %H:%M:%S")
        header_time = datetime.fromisoformat(wall_time.isoformat() + match[2])
    except ValueError:
        return {"time_status": "DATA_MISSING", "normalization_version": policy}
    israel_wall_time = wall_time.replace(tzinfo=ISRAEL)
    normalized = header_time if policy == "html-offset-v1" else israel_wall_time
    differs = header_time.utcoffset() != israel_wall_time.utcoffset()
    return {
        "raw_time_title": title,
        "raw_utc_offset": match[2],
        "header_message_time_utc": _iso(header_time),
        "normalized_message_time_utc": _iso(normalized),
        "normalized_message_time_israel": normalized.astimezone(ISRAEL).isoformat(),
        "normalization_version": policy,
        "time_status": "OFFSET_DISAGREEMENT_REVIEW_REQUIRED" if differs else "HEADER_PARSED_UNVERIFIED",
        "event_time_verified": False,
    }


def scope_membership(timestamp: str | None) -> list[str]:
    if not timestamp:
        return []
    local_date = datetime.fromisoformat(timestamp.replace("Z", "+00:00")).astimezone(ISRAEL).date()
    return [key for key, lower in SCOPES.items() if local_date >= date.fromisoformat(lower)]


def classify(text: str) -> str:
    # These are message families, never substitutes for an engine's total score.
    prefixes = (
        ("מגנט 🔺", "MAGNET_TARGET"), ("מגנט 🔻", "MAGNET_TARGET"),
        ("התראה 🚨 קונפירמיישן משולב", "COMBINED_CONFIRMATION"),
        ("ציון ✅ אישור Max Pain לפי ציון", "MAX_PAIN_SCORE_CONFIRMATION"),
        ("אישור ✅ Max Pain", "MAX_PAIN_VERIFIED"),
        ("אישור 🔥🔥 Max Pain חזק", "MAX_PAIN_STRONG"),
        ("ציון 🚨 Max Pain — 83+", "MAX_PAIN_SCORE_83_PLUS"),
        ("ספוט 🚨 Spot CVD חזק", "SPOT_CVD_STRONG"),
        ("עוצמה 🚨 Futures CVD", "FUTURES_CVD_STRENGTH"),
        ("עוצמה 🚨 מחיר + OI", "PRICE_OI_STRENGTH"),
    )
    for prefix, family in prefixes:
        if text.startswith(prefix):
            return family
    if "🎯 Max Pain" in text[:240] and re.search(r"#\d+\s+[A-Z0-9]+\s*/\s*\S+\s*\|\s*[🟢🔴]\s*(?:LONG|SHORT)", text):
        return "WATCH_CANDIDATE"
    if text.startswith(("✅ סריקת Watch Top 8 #", "🧲 Magnet Watch V1 #", "מגנט 🧲")):
        return "SCAN_CONTEXT"
    if text.startswith("━━━━━━━━━━━━━━━━━━━━") and any(marker in text[:100] for marker in ("📊 מחיר + OI", "📈 Futures CVD", "💱 Spot CVD")):
        return "CONTINUATION_UNLINKED"
    return "OTHER_SOURCE_MESSAGE"


def existing_links(payload: Mapping[str, Any], chat_key: str) -> dict[str, list[dict[str, Any]]]:
    """Index the existing v2 Sheet import by source IDs, retaining its provenance.

    This payload is the already imported Sep 4-5 batch, supplied explicitly by
    the operator for the same chat. No scan-counter/time-nearness match is used.
    """
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in payload.get("events", []):
        if not isinstance(event, list) or len(event) < 10:
            raise ValueError("Existing import must use the original Sheet payload events format")
        for message_id in str(event[2]).split(","):
            message_id = message_id.strip()
            if not re.fullmatch(r"message\d+", message_id):
                raise ValueError("Existing event has an invalid Telegram source message id")
            result[_hash("telegram-message-identity-v1", chat_key, message_id)].append({
                "existing_event_id": str(event[0]),
                "existing_snapshot_id": str(event[1]),
                "existing_message_text": _text(str(event[9])),
                "existing_import_version": "telegram-archive-import-v2",
            })
    return dict(result)


def embedded_utc_age_audit(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Compare both clock interpretations with explicitly printed CVD UTC ages.

    An engine capture can predate Telegram delivery, so residuals are descriptive
    provenance evidence and do not certify an exact emission time.
    """
    residuals: list[float] = []
    header_residuals: list[float] = []
    covered_days: Counter[str] = Counter()
    for row in rows:
        if not row.get("normalized_message_time_utc"):
            continue
        observations = set(re.findall(r"CVD עד:\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2}) UTC\s*\|\s*גיל בפועל\s*(\d+)", row["message_text"]))
        if observations:
            covered_days[row["normalized_message_time_israel"][:10]] += 1
        for timestamp, age in observations:
            try:
                source_time = datetime.fromisoformat(timestamp).replace(tzinfo=UTC)
            except ValueError:
                continue
            for field, values in (("normalized_message_time_utc", residuals), ("header_message_time_utc", header_residuals)):
                message_time = datetime.fromisoformat(row[field].replace("Z", "+00:00"))
                values.append((message_time - source_time).total_seconds() / 60 - int(age))
    return {
        "method": "dated-cvd-utc-plus-reported-age-comparison-v1",
        "source_messages_with_dated_utc_and_age": sum(covered_days.values()),
        "distinct_within_message_observations": len(residuals),
        "normalized_residual_minutes_min": min(residuals) if residuals else None,
        "normalized_residual_minutes_max": max(residuals) if residuals else None,
        "header_residual_minutes_min": min(header_residuals) if header_residuals else None,
        "header_residual_minutes_max": max(header_residuals) if header_residuals else None,
        "source_messages_by_israel_date": dict(sorted(covered_days.items())),
        "exact_emission_time_verified": False,
    }


def prepare_archive(
    paths: Iterable[Path], *, chat_key: str, time_policy: str,
    known_links: Mapping[str, list[dict[str, Any]]] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if not chat_key.strip():
        raise ValueError("A stable source --chat-key is required; do not use an export filename")
    if time_policy not in TIME_POLICIES:
        raise ValueError("An explicit --time-policy is required")
    sources = []
    revisions: dict[tuple[str, str], dict[str, Any]] = {}
    excluded = invalid_ids = duplicate_copies = 0
    for path in sorted(paths, key=lambda item: item.name):
        content = path.read_bytes()
        source_hash = hashlib.sha256(content).hexdigest()
        sources.append({"file_name": path.name, "sha256": source_hash, "bytes": len(content)})
        parser = _Messages()
        parser.feed(content.decode("utf-8-sig"))
        parser.close()
        for message in parser.rows:
            message_id = message["source_message_id"]
            if not re.fullmatch(r"message\d+", message_id):
                invalid_ids += 1
                continue
            timing = normalize_time(message["raw_time_title"], time_policy)
            scopes = scope_membership(timing.get("normalized_message_time_utc"))
            if timing.get("normalized_message_time_utc") and not scopes:
                excluded += 1
                continue
            identity = _hash("telegram-message-identity-v1", chat_key, message_id)
            revision = _hash(message["raw_time_title"], message["message_text"])
            occurrence = {"file_name": path.name, "file_sha256": source_hash}
            if (identity, revision) in revisions:
                revisions[identity, revision]["source_occurrences"].append(occurrence)
                duplicate_copies += 1
                continue
            family = classify(message["message_text"])
            links = (known_links or {}).get(identity, [])
            verified_links = [link for link in links if message["message_text"] and message["message_text"] in link["existing_message_text"]]
            row = {
                "source_kind": "TELEGRAM_DESKTOP_HTML_ARCHIVE",
                "record_mode": "ARCHIVE", "parser_version": PARSER_VERSION,
                "source_chat_key": chat_key, "source_message_id": message_id,
                "source_identity_key": identity, "source_revision_sha256": revision,
                "source_occurrences": [occurrence], **message, **timing,
                "message_family": family, "scope_version": SCOPE_VERSION,
                "period_scope_ids": scopes, "source_filter": "ARCHIVE_ONLY", "source_scope": "ARCHIVE_ONLY",
                "existing_source_links": [{key: value for key, value in link.items() if key != "existing_message_text"} for link in verified_links],
                "import_action": "LINK_EXISTING_SOURCE" if verified_links else "STAGE_NEW_SOURCE",
                "identity_status": "EXACT_SOURCE_ID",
                "source_link_status": "MATCHED_ID_AND_TEXT" if verified_links else ("ID_TEXT_CONFLICT" if links else "NO_KNOWN_LINK"),
                "watch_scan_id": None, "btc_parent_movement_id": None,
                "analysis_direction": None, "outcome_method_version": None,
                "training_eligible": False, "candidate_eligible": False,
                "evidence_status": "SOURCE_ONLY_REQUIRES_VERSIONED_RECONSTRUCTION",
                "missing_evidence": ["verified_event_time", "verified_direction_mapping", "immutable_feature_snapshot", "ordered-first-touch-v7", "verified_btc_parent_movement_id"],
            }
            if not timing.get("normalized_message_time_utc"):
                row["import_action"] = "QUARANTINE_MISSING_TIME"
            elif links and not verified_links:
                row["import_action"] = "QUARANTINE_EXISTING_TEXT_CONFLICT"
            revisions[identity, revision] = row
    by_identity: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in revisions.values():
        by_identity[row["source_identity_key"]].append(row)
    conflicts = 0
    for rows in by_identity.values():
        if len(rows) > 1:
            conflicts += 1
            for row in rows:
                row["identity_status"] = "SOURCE_ID_REVISION_CONFLICT"
                row["import_action"] = "QUARANTINE_REVISION_CONFLICT"
    rows = sorted(revisions.values(), key=lambda row: (row.get("normalized_message_time_utc", ""), row["source_message_id"], row["source_revision_sha256"]))
    daily: Counter[str] = Counter()
    for row in rows:
        if row.get("normalized_message_time_israel"):
            daily[row["normalized_message_time_israel"][:10]] += 1
    absent_days = []
    if daily:
        current = date.fromisoformat(SCOPES["ALL_COMPATIBLE_SINCE_20260816"])
        last = date.fromisoformat(max(daily))
        while current <= last:
            if current.isoformat() not in daily:
                absent_days.append(current.isoformat())
            current += timedelta(days=1)
    manifest = {
        "parser_version": PARSER_VERSION, "scope_version": SCOPE_VERSION,
        "mode": "DRY_RUN_SOURCE_STAGING", "applied": False,
        "source_chat_key": chat_key, "time_policy": time_policy, "source_files": sources,
        "source_file_count": len(sources), "unique_source_messages": len(by_identity),
        "source_revision_rows": len(rows), "exact_duplicate_copies_removed": duplicate_copies,
        "conflicting_source_identities": conflicts, "invalid_message_ids_excluded": invalid_ids,
        "excluded_before_20260816": excluded,
        "signal_source_messages": sum(row["message_family"] not in {"SCAN_CONTEXT", "CONTINUATION_UNLINKED", "OTHER_SOURCE_MESSAGE"} for row in rows),
        "message_family_counts": dict(Counter(row["message_family"] for row in rows)),
        "time_status_counts": dict(Counter(row["time_status"] for row in rows)),
        "import_action_counts": dict(Counter(row["import_action"] for row in rows)),
        "source_link_status_counts": dict(Counter(row["source_link_status"] for row in rows)),
        "existing_linked_event_count": len({link["existing_event_id"] for row in rows for link in row["existing_source_links"]}),
        "existing_linked_snapshot_count": len({link["existing_snapshot_id"] for row in rows for link in row["existing_source_links"]}),
        "daily_source_message_counts_israel": dict(sorted(daily.items())),
        "days_without_exported_messages": absent_days,
        "no_signal_inference_allowed": False,
        "periods": [{"period_scope_id": scope, "start_inclusive_israel": lower + "T00:00:00+03:00", "source_revision_rows": sum(scope in row["period_scope_ids"] for row in rows)} for scope, lower in SCOPES.items()],
        "periods_overlap": True, "periods_are_independent_validation": False,
        "fresh_is_rolling_14_days_separate_from_period_filter": True,
        "formula_evidence_rows": 0, "v7_outcomes_created": 0,
        "live_events_created": 0, "independent_waves_created": 0,
        "embedded_utc_age_audit": embedded_utc_age_audit(rows),
    }
    manifest["archive_revision_digest"] = _hash(sorted((row["source_identity_key"], row["source_revision_sha256"]) for row in rows))
    manifest["prepared_stage_digest"] = stage_digest(manifest, rows)
    return manifest, rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--chat-key", required=True, help="Stable chat identity shared by all exports of this chat")
    parser.add_argument("--time-policy", required=True, choices=TIME_POLICIES)
    parser.add_argument("--existing-import", type=Path, help="Original Sep 4-5 v2 Sheet payload for this same chat")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    paths = list(args.directory.glob("messages*.html"))
    if not paths:
        parser.error("No messages*.html export files found")
    known = existing_links(json.loads(args.existing_import.read_text(encoding="utf-8")), args.chat_key) if args.existing_import else {}
    manifest, rows = prepare_archive(paths, chat_key=args.chat_key, time_policy=args.time_policy, known_links=known)
    if args.existing_import:
        manifest["existing_import_sha256"] = hashlib.sha256(args.existing_import.read_bytes()).hexdigest()
        manifest["prepared_stage_digest"] = stage_digest(manifest, rows)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "archive_manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    with (args.output_dir / "archive_source_messages.jsonl").open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    print(json.dumps(manifest, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
