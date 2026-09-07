"""Bounded actual Sheet-vs-delivered-event audit, separate from outbox ACKs.

Each page shares the sender's FIFO admission. A complete cycle is compared only
against a frozen 14-day DB population, excluding the newest two minutes. Imports
and demos are never counted as native Telegram deliveries. No Telegram actions.
"""
from __future__ import annotations
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping
import time
import google_sheets_sync
import research_sheet_outbox

VERSION = "delivered-sheet-fingerprints-v1"
MAX_EVENTS = 20_000
PAGE_INTERVAL_SECONDS = 60
CYCLE_INTERVAL_SECONDS = 300


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
        self.runtime: dict[str, Any] = {"version": VERSION, "status": "NOT_STARTED", "last_complete": None, "last_error": None}

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
            raise ValueError("AUDIT_POPULATION_EXCEEDS_20000")
        self.expected = {str(row["event_fingerprint"]): dict(row) for row in rows}
        if len(self.expected) != len(rows):
            raise ValueError("DUPLICATE_DATABASE_EVENT_FINGERPRINT")
        self.seen.clear()
        self.unexpected.clear()
        self.next_row, self.last_row, self.normalized = 2, None, 0
        self.runtime.update(status="SCANNING", expected=len(self.expected), started_at=datetime.now(timezone.utc).isoformat())

    def consume_page(self, page: Mapping[str, Any]) -> bool:
        rows = page.get("rows")
        if (not isinstance(rows, list) or len(rows) > 500
            or page.get("start_row") != self.next_row
            or page.get("next_row") != self.next_row + len(rows)
            or not isinstance(page.get("last_row"), int)
            or page["last_row"] < 1
            or (self.last_row is not None and page["last_row"] != self.last_row)
            or page.get("complete") is not (page["next_row"] > page["last_row"])
            or (not rows and not page["complete"])):
            raise ValueError("INVALID_OR_DISCONTINUOUS_SHEET_AUDIT_PAGE")
        if self.expected is None or self.start is None or self.cutoff is None:
            raise ValueError("AUDIT_POPULATION_NOT_STARTED")
        self.last_row = page["last_row"]
        for row in rows:
            if not isinstance(row, Mapping):
                raise ValueError("INVALID_SHEET_AUDIT_ROW")
            fingerprint = str(row.get("event_id") or "")
            if row.get("verification_status") != "DELIVERED" or not fingerprint:
                continue
            if fingerprint in self.expected:
                self.seen[fingerprint] += 1
            elif self.start <= (utc(row.get("timestamp_utc")) or datetime.min.replace(tzinfo=timezone.utc)) <= self.cutoff:
                self.unexpected[fingerprint] += 1
        self.next_row = page["next_row"]
        self.normalized += int(page.get("normalized_maxpain_rows") or 0)
        return bool(page["complete"])

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
        with research_sheet_outbox._connect(database_url) as conn:
            for fingerprint in missing[:100]:
                repaired += conn.execute('''
                    UPDATE research_sheet_upsert_outbox SET sync_status='PENDING', attempts=0,
                        next_attempt_at_utc=NOW(), synced_at_utc=NULL,
                        last_error='ACTUAL_SHEET_ROW_MISSING', updated_at_utc=NOW()
                    WHERE sheet_name='Telegram_Events' AND row_key=%s AND sync_status='SYNCED'
                ''', (research_sheet_outbox._json([fingerprint]),)).rowcount
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
        try:
            if self.expected is None:
                self._begin(database_url)
            self.runtime["status"] = "SCANNING"
            page = google_sheets_sync.read_telegram_audit_page(start_row=self.next_row, last_row=self.last_row)
            if self.consume_page(page):
                self._finish(database_url)
                self.next_attempt = time.monotonic() + CYCLE_INTERVAL_SECONDS
            self.runtime["last_error"] = None
        except google_sheets_sync.SheetReceiverBusy:
            # No request was admitted, so no remote data or row bounds changed
            # within this attempt. Preserve the frozen population and every
            # prior page; ordinary FIFO contention must not restart the cycle.
            # It remains incomplete and will resume at the next bounded turn.
            self.runtime.update(status="DEFERRED_RECEIVER_BUSY", last_error=None)
        except Exception as exc:
            self.runtime.update(status="AUDIT_FAILED", last_error=str(exc)[:160])
            # Failures never certify coverage, or reuse an incomplete ID scan.
            self.expected = None
        return self.status()
