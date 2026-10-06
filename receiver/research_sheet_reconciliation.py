"""Bounded actual Sheet-vs-delivered-event audit, separate from outbox ACKs.

Each page shares the sender's FIFO admission. A complete cycle is compared only
against a frozen 14-day DB population, excluding the newest two minutes. Imports
and demos are never counted as native Telegram deliveries. No Telegram actions.
"""
from __future__ import annotations
from collections import Counter
from datetime import datetime, timedelta, timezone
import json
from typing import Any, Mapping
import time
import google_sheets_sync
import research_sheet_outbox

VERSION = "delivered-sheet-fingerprints-v1"
MAX_EVENTS = research_sheet_outbox.current_publication.TELEGRAM_MAX_ROWS
PAGE_INTERVAL_SECONDS = 60
CYCLE_INTERVAL_SECONDS = 300
_SAFE_FAILURE_CODES = frozenset({
    "SHEETS_UNCONFIGURED", "SHEET_RECEIVER_BUSY", "SHEET_AUDIT_BOUNDS_CHANGED",
    "SHEET_AUDIT_CONNECTION_INTERRUPTED", "SHEET_AUDIT_RESPONSE_TOO_LARGE",
    "SHEET_AUDIT_RECEIVER_V3_REQUIRED", "INVALID_SHEET_AUDIT_RESPONSE",
    "INVALID_OR_DISCONTINUOUS_SHEET_AUDIT_PAGE", "INVALID_SHEET_AUDIT_ROW",
    "INVALID_SHEET_AUDIT_NORMALIZED_COUNT", "AUDIT_POPULATION_NOT_STARTED",
    "AUDIT_POPULATION_EXCEEDS_32000", "DUPLICATE_DATABASE_EVENT_FINGERPRINT",
    "SHEET_AUDIT_CYCLE_EXPIRED",
}) | frozenset(
    "SHEET_AUDIT_HTTP_" + str(code) + "_" + stage
    for code in (404, 408, 429, 500, 502, 503, 504)
    for stage in ("WEBHOOK_RESPONSE", "CONTENT_RESPONSE")
)


def utc(value: Any) -> datetime | None:
    try:
        result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if result.tzinfo is None:
            return None
        return result.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


class SheetReconciler:
    def __init__(self):
        self.next_attempt = 0.0
        self.expected: dict[str, dict[str, Any]] | None = None
        self.seen: Counter[str] = Counter()
        self.unexpected: Counter[str] = Counter()
        self.next_row = 2
        self.last_row: int | None = None
        self.start: datetime | None = None
        self.cutoff: datetime | None = None
        self.normalized = 0
        self.scan_deadline: float | None = None
        self.runtime: dict[str, Any] = {"version": VERSION, "status": "NOT_STARTED", "last_complete": None,
                                      "last_error": None, "last_failure": None, "last_reset": None, "reset_count": 0}

    def status(self) -> dict[str, Any]:
        return dict(self.runtime, next_row=self.next_row, last_row=self.last_row)

    def _begin(self, database_url: str) -> None:
        self.cutoff = datetime.now(timezone.utc) - timedelta(minutes=2)
        self.start = self.cutoff - timedelta(days=14)
        with research_sheet_outbox._connect(database_url) as conn:
            rows = conn.execute('''
                SELECT event_fingerprint,event_type,alert_time_utc
                FROM research_events
                WHERE event_kind='ALERT' AND delivery_status='DELIVERED'
                  AND alert_time_utc >= %s AND alert_time_utc <= %s
                ORDER BY event_id LIMIT %s
            ''', (self.start, self.cutoff, MAX_EVENTS + 1)).fetchall()
        if len(rows) > MAX_EVENTS:
            raise ValueError("AUDIT_POPULATION_EXCEEDS_32000")
        self.expected = {str(row["event_fingerprint"]): dict(row) for row in rows}
        if len(self.expected) != len(rows):
            raise ValueError("DUPLICATE_DATABASE_EVENT_FINGERPRINT")
        self.seen.clear()
        self.unexpected.clear()
        self.next_row, self.last_row, self.normalized = 2, None, 0
        self.scan_deadline = time.monotonic() + research_sheet_outbox.current_publication.AUDIT_MAX_SECONDS
        self.runtime.update(status="SCANNING", expected=len(self.expected), started_at=datetime.now(timezone.utc).isoformat())

    def consume_page(self, page: Mapping[str, Any]) -> bool:
        if not isinstance(page, Mapping):
            raise ValueError("INVALID_OR_DISCONTINUOUS_SHEET_AUDIT_PAGE")
        rows = page.get("rows")
        if (not isinstance(rows, list) or len(rows) > 500
            or page.get("start_row") != self.next_row
            or page.get("next_row") != self.next_row + len(rows)
            or type(page.get("start_row")) is not int or type(page.get("next_row")) is not int
            or type(page.get("last_row")) is not int
            or page["last_row"] < 1
            or page["next_row"] > page["last_row"] + 1
            or (self.last_row is not None and page["last_row"] != self.last_row)
            or page.get("complete") is not (page["next_row"] > page["last_row"])
            or (not rows and not page["complete"])):
            raise ValueError("INVALID_OR_DISCONTINUOUS_SHEET_AUDIT_PAGE")
        if self.expected is None or self.start is None or self.cutoff is None:
            raise ValueError("AUDIT_POPULATION_NOT_STARTED")
        normalized = page.get("normalized_maxpain_rows", 0)
        if type(normalized) is not int or not 0 <= normalized <= len(rows):
            raise ValueError("INVALID_SHEET_AUDIT_NORMALIZED_COUNT")
        seen, unexpected = Counter(), Counter()
        for row in rows:
            if not isinstance(row, Mapping):
                raise ValueError("INVALID_SHEET_AUDIT_ROW")
            fingerprint = str(row.get("event_id") or "")
            if row.get("verification_status") != "DELIVERED" or not fingerprint:
                continue
            if fingerprint in self.expected:
                seen[fingerprint] += 1
            elif self.start <= (utc(row.get("timestamp_utc")) or datetime.min.replace(tzinfo=timezone.utc)) <= self.cutoff:
                unexpected[fingerprint] += 1
        # Commit a complete validated page atomically. A malformed later row
        # must not change earlier counters or any part of the frozen cursor.
        self.last_row = page["last_row"]
        self.seen.update(seen)
        self.unexpected.update(unexpected)
        self.next_row = page["next_row"]
        self.normalized += normalized
        return bool(page["complete"])

    def _record_failure(self, exc: Exception, *, phase: str, reset: bool) -> str:
        error_type = type(exc).__name__
        message = str(exc)
        code = message if message in _SAFE_FAILURE_CODES else error_type
        failure = {
            "at_utc": datetime.now(timezone.utc).isoformat(), "phase": phase,
            "exception_type": error_type, "error_code": code, "scan_reset": reset,
            "start_row": self.next_row, "last_row": self.last_row,
            "scan_started_at": self.runtime.get("started_at"),
            "through_utc": self.cutoff.isoformat() if self.cutoff else None,
            "expected": len(self.expected) if self.expected is not None else None,
            "seen_unique": len(self.seen),
        }
        diagnostic = getattr(exc, "sheet_audit_diagnostic", None)
        if isinstance(diagnostic, dict):
            failure["http"] = {key: diagnostic.get(key) for key in (
                "http_status", "response_stage", "response_host", "content_type", "response_bytes", "elapsed_seconds")}
        self.runtime["last_failure"] = failure
        if reset:
            self.runtime["reset_count"] += 1
            self.runtime["last_reset"] = failure
            self.expected = None
            self.seen.clear()
            self.unexpected.clear()
            self.next_row, self.last_row, self.normalized = 2, None, 0
            self.start, self.cutoff = None, None
            self.scan_deadline = None
            self.runtime.update(expected=None, started_at=None)
        # Retained status survives subsequent successful pages; one bounded,
        # sanitized log record also distinguishes resets from process restarts.
        print("[sheet-audit] " + json.dumps(failure, sort_keys=True), flush=True)
        return code

    def summarize(self) -> dict[str, Any]:
        expected = self.expected or {}
        missing = sorted(set(expected) - set(self.seen))
        duplicates = {key: count for key, count in self.seen.items() if count > 1}
        by_type: dict[str, dict[str, int]] = {}
        for key, row in expected.items():
            group = by_type.setdefault(str(row["event_type"]), {"delivered": 0, "sheet_unique": 0, "missing": 0})
            group["delivered"] += 1
            group["sheet_unique"] += int(key in self.seen)
            group["missing"] += int(key not in self.seen)
        return {"checked_at": datetime.now(timezone.utc).isoformat(),
                "from_utc": self.start.isoformat() if self.start else None,
                "through_utc": self.cutoff.isoformat() if self.cutoff else None,
                "delivered": len(expected), "sheet_unique": len(self.seen),
                "missing": len(missing), "duplicate_ids": len(duplicates),
                "duplicate_extra_rows": sum(n - 1 for n in duplicates.values()),
                "unexpected_delivered_ids": len(self.unexpected),
                "missing_sample": missing[:10], "duplicate_sample": dict(list(duplicates.items())[:10]),
                "by_type": by_type, "normalized_historical_maxpain_rows": self.normalized,
                "_missing_ids": missing}

    def _finish(self, database_url: str) -> dict[str, Any]:
        summary = self.summarize()
        missing = summary.pop("_missing_ids")
        # A previous ACK is not evidence that a row still exists. Requeue only
        # exact current SYNCED generations, leaving active/retrying leases alone.
        # At most 100 idempotent repairs per complete cycle; no new events.
        repaired = 0
        if missing:
            with research_sheet_outbox._connect(database_url) as conn:
                # Apply the repair limit AFTER selecting eligible rows. A
                # pending/retrying prefix must not starve later SYNCED gaps.
                # Lock the selected current generations; concurrent claims or
                # source updates cannot be reset by this repair transaction.
                repaired = conn.execute('''
                    WITH repairable AS (
                        SELECT sheet_name,row_key
                        FROM research_sheet_upsert_outbox
                        WHERE sheet_name='Telegram_Events' AND sync_status='SYNCED'
                          AND row_key=ANY(%s)
                        ORDER BY row_key LIMIT 100 FOR UPDATE SKIP LOCKED
                    )
                    UPDATE research_sheet_upsert_outbox target SET sync_status='PENDING', attempts=0,
                        next_attempt_at_utc=NOW(), synced_at_utc=NULL,
                        last_error='ACTUAL_SHEET_ROW_MISSING', updated_at_utc=NOW()
                    FROM repairable
                    WHERE target.sheet_name=repairable.sheet_name AND target.row_key=repairable.row_key
                      AND target.sync_status='SYNCED'
                ''', ([research_sheet_outbox._json([fingerprint]) for fingerprint in missing],)).rowcount
        summary["previously_synced_rows_requeued"] = repaired
        self.runtime.update(status="GAPS_FOUND" if summary["missing"] or summary["duplicate_ids"] or summary["unexpected_delivered_ids"] else "MATCHED",
                            last_complete=summary, last_error=None)
        self.expected = None
        return summary

    def run_due(self, database_url: str) -> dict[str, Any]:
        now = time.monotonic()
        if now < self.next_attempt:
            return self.status()
        self.next_attempt = now + PAGE_INTERVAL_SECONDS
        phase = "BEGIN"
        try:
            if self.expected is None:
                self._begin(database_url)
            phase = "AGE_CHECK"
            if self.scan_deadline is not None and time.monotonic() >= self.scan_deadline:
                raise ValueError("SHEET_AUDIT_CYCLE_EXPIRED")
            self.runtime["status"] = "SCANNING"
            phase = "READ"
            page = google_sheets_sync.read_telegram_audit_page(start_row=self.next_row, last_row=self.last_row)
            phase = "CONSUME"
            if self.scan_deadline is not None and time.monotonic() >= self.scan_deadline:
                raise ValueError("SHEET_AUDIT_CYCLE_EXPIRED")
            if self.consume_page(page):
                phase = "FINISH"
                self._finish(database_url)
                self.next_attempt = time.monotonic() + CYCLE_INTERVAL_SECONDS
            self.runtime["last_error"] = None
        except google_sheets_sync.SheetReceiverBusy:
            # No request was admitted, so no remote data or row bounds changed
            # within this attempt. Preserve the frozen population and every
            # prior page; ordinary FIFO contention must not restart the cycle.
            # It remains incomplete and will resume at the next bounded turn.
            self.runtime.update(status="DEFERRED_RECEIVER_BUSY", last_error=None)
        except google_sheets_sync.SheetAuditTransportRetry as exc:
            # A transport failure yielded no valid page, so nothing was added
            # to seen/normalized or advanced. Resume this exact page with the
            # same frozen population and boundary at the next bounded turn.
            code = self._record_failure(exc, phase=phase, reset=False)
            self.runtime.update(status="DEFERRED_TRANSPORT_RETRY", last_error=code)
        except Exception as exc:
            # Any unaccepted READ response, including HTTP-200 HTML or auth
            # errors, leaves earlier validated pages unchanged. This remains
            # AUDIT_FAILED until the exact requested page succeeds. Explicit
            # invalid bounds/pages must instead discard the whole scan.
            reset = phase != "READ" or isinstance(exc, (
                google_sheets_sync.SheetAuditBoundsChanged, google_sheets_sync.SheetAuditPageInvalid))
            code = self._record_failure(exc, phase=phase, reset=reset)
            self.runtime.update(status="AUDIT_FAILED", last_error=code)
        return self.status()
