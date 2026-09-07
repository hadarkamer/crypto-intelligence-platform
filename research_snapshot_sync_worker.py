"""Recover and maintain Sheet snapshots from bounded committed source groups.

The worker owns no Telegram actions and performs no schema creation. It rebuilds
one complete Watch/symbol/side child set in fresh local memory, stages exact row
payloads through the shared durable outbox, then drains only committed payloads.
Missing past exports and new children are found through immutable source IDs.
"""
from __future__ import annotations

import asyncio
import os
from typing import Any, Dict, Mapping, Sequence

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    dict_row = None

import google_sheets_sync
import research_sheet_outbox
import research_sheet_reconciliation

RECONCILE_VERSION = "committed-snapshot-reconcile-v2-maxpain"
_PASS_LOCK_ID = 4860059309063875574
_SOURCE_LIMIT = 16
_POLL_SECONDS = max(5, int(os.getenv("SHEET_OUTBOX_POLL_SECONDS", "5")))
_MAX_GROUP_CHILDREN = 256
_BACKFILL_DAYS = 14
_TRUE = {"1", "true", "yes", "on"}
_BTC_POLICY = "btc-parent-close-reversal-200bps-v1"


def _database_url() -> str:
    dedicated = os.getenv("RESEARCH_DATABASE_URL", "").strip()
    if dedicated:
        return dedicated
    if os.getenv("RESEARCH_USE_PRIMARY_DATABASE", "").strip().lower() in _TRUE:
        return os.getenv("DATABASE_URL", "").strip()
    return ""


def snapshot_identity(event: Mapping[str, Any]) -> str:
    snapshot = event.get("engine_snapshot")
    snapshot = snapshot if isinstance(snapshot, Mapping) else {}
    return str(snapshot.get("sheet_snapshot_id") or event.get("event_fingerprint") or "")


def rebuild_alert_group(events: Sequence[Mapping[str, Any]]) -> list[Dict[str, Any]]:
    """Deterministic complete aggregation; no process cache or live inputs."""
    if not events or len(events) > _MAX_GROUP_CHILDREN:
        raise ValueError("snapshot group is empty or exceeds bounded child limit")
    identities = {snapshot_identity(event) for event in events}
    if len(identities) != 1 or not next(iter(identities)):
        raise ValueError("snapshot child identities disagree")
    ids = [int(event["event_id"]) for event in events]
    if len(set(ids)) != len(ids):
        raise ValueError("duplicate source event in snapshot group")
    if any(
        event.get("event_kind") != "ALERT"
        or event.get("delivery_status") != "DELIVERED"
        for event in events
    ):
        raise ValueError("snapshot replay accepts committed delivered alerts only")
    if len({(event.get("symbol"), event.get("direction")) for event in events}) != 1:
        raise ValueError("snapshot group combines different symbols or directions")

    aggregate: Dict[str, Any] = {}
    telegram_rows = []
    final_payload: Dict[str, Any] = {}

    def merge(row: Mapping[str, Any]) -> Dict[str, Any]:
        nonlocal aggregate
        aggregate = google_sheets_sync.merge_snapshot_rows(aggregate, row)
        return aggregate

    for event in sorted(events, key=lambda item: (str(item["alert_time_utc"]), int(item["event_id"]))):
        final_payload = google_sheets_sync.build_delivered_event_payload(
            event, delivered_at_utc=event.get("delivered_at_utc"), snapshot_merger=merge
        )
        telegram_rows.extend(final_payload["upserts"][2:])
    return final_payload["upserts"][:2] + telegram_rows


def _mark_sources(conn, events, *, status="STAGED", reason=None) -> None:
    for event in events:
        conn.execute(
            """
            INSERT INTO research_snapshot_sheet_sources
                (source_event_id,reconcile_version,snapshot_id,source_status,rejection_reason,
                 btc_parent_movement_id,next_attempt_at_utc)
            VALUES (%s,%s,%s,%s,%s,%s,NOW()+INTERVAL '5 minutes')
            ON CONFLICT (source_event_id,reconcile_version) DO UPDATE SET
                source_status=EXCLUDED.source_status,
                rejection_reason=EXCLUDED.rejection_reason,
                btc_parent_movement_id=EXCLUDED.btc_parent_movement_id,
                next_attempt_at_utc=EXCLUDED.next_attempt_at_utc,
                staged_at_utc=NOW()
            """,
            (int(event["event_id"]), RECONCILE_VERSION, snapshot_identity(event), status, reason,
             event.get("btc_parent_movement_id")),
        )


def reconcile_sources(conn, *, limit: int = _SOURCE_LIMIT) -> Dict[str, int]:
    """Stage a bounded batch and coverage markers in the same DB transaction."""
    result = {"sources": 0, "groups": 0, "staged_rows": 0, "rejected": 0, "deferred": 0}
    locked = conn.execute("SELECT pg_try_advisory_xact_lock(%s) AS held", (_PASS_LOCK_ID,)).fetchone()
    if not locked or not locked["held"]:
        return result
    # Limit IDs before loading source JSON. The partial candidate index and
    # marker primary key keep replay incremental; no event-ID high-water mark
    # can skip an earlier transaction that committed later.
    bounded_limit = max(1, min(int(limit), 64))
    oldest_limit = max(1, bounded_limit // 2)
    newest_limit = max(0, bounded_limit - oldest_limit)
    candidates = conn.execute(
        """
        WITH eligible AS MATERIALIZED (
            SELECT event.event_id
            FROM research_events event
            LEFT JOIN research_event_btc_movements movement ON movement.event_id=event.event_id
                AND movement.episode_policy_version=%s AND movement.membership_status='LIVE'
            WHERE ((event.event_kind='ALERT' AND event.delivery_status='DELIVERED')
                OR (event.event_kind='DECISION_SAMPLE'
                    AND event.event_type='PROSPECTIVE_NEUTRAL_30M'))
              AND event.alert_time_utc >= NOW() - (%s * INTERVAL '1 day')
              AND NOT EXISTS (
                  SELECT 1 FROM research_snapshot_sheet_sources staged
                  WHERE staged.source_event_id=event.event_id AND staged.reconcile_version=%s
                    AND (staged.source_status <> 'DEFERRED' OR staged.next_attempt_at_utc>NOW())
                    AND staged.btc_parent_movement_id IS NOT DISTINCT FROM movement.btc_parent_movement_id
              )
        ), picked AS MATERIALIZED (
            (SELECT event_id FROM eligible ORDER BY event_id ASC LIMIT %s)
            UNION
            (SELECT event_id FROM eligible ORDER BY event_id DESC LIMIT %s)
        )
        SELECT event.*, movement.btc_parent_movement_id
        FROM picked JOIN research_events event USING (event_id)
        LEFT JOIN research_event_btc_movements movement ON movement.event_id=event.event_id
            AND movement.episode_policy_version=%s AND movement.membership_status='LIVE'
        ORDER BY event.event_id
        """,
        (
            _BTC_POLICY,
            _BACKFILL_DAYS,
            RECONCILE_VERSION,
            oldest_limit,
            newest_limit,
            _BTC_POLICY,
        ),
    ).fetchall()
    processed = set()
    for candidate in candidates:
        if int(candidate["event_id"]) in processed:
            continue
        if candidate["event_kind"] == "ALERT":
            children = conn.execute(
                """
                SELECT event.*, movement.btc_parent_movement_id FROM research_events event
                LEFT JOIN research_event_btc_movements movement ON movement.event_id=event.event_id
                    AND movement.episode_policy_version=%s AND movement.membership_status='LIVE'
                WHERE event.event_kind='ALERT' AND event.delivery_status='DELIVERED'
                  AND COALESCE(NULLIF(event.engine_snapshot->>'sheet_snapshot_id',''),
                               event.event_fingerprint)=%s
                ORDER BY event.event_id LIMIT %s
                """,
                (_BTC_POLICY, snapshot_identity(candidate), _MAX_GROUP_CHILDREN + 1),
            ).fetchall()
            try:
                upserts = rebuild_alert_group(children)
            except ValueError as exc:
                _mark_sources(conn, [candidate], status="REJECTED", reason=str(exc))
                result["rejected"] += 1
                continue
        else:
            # The authorization view verifies exact pair ownership and frozen
            # slot feature hashes. Merely resembling a neutral event is not enough.
            neutral = conn.execute(
                "SELECT * FROM research_prospective_shadow_events WHERE event_id=%s",
                (int(candidate["event_id"]),),
            ).fetchone()
            if neutral is None:
                _mark_sources(conn, [candidate], status="DEFERRED", reason="NO_AUTHORIZED_FROZEN_ANCHOR")
                result["deferred"] += 1
                continue
            payload = google_sheets_sync.build_neutral_snapshot_payload(
                candidate,
                decision_feature_bundle=neutral["decision_feature_bundle"],
                anchor_slot_id=neutral["anchor_slot_id"],
                feature_bundle_policy_version=neutral["feature_bundle_policy_version"],
                feature_bundle_sha256=neutral["feature_bundle_sha256"],
            )
            upserts = payload["upserts"]
            children = [candidate]
        result["staged_rows"] += research_sheet_outbox.stage_upserts(conn, upserts)
        _mark_sources(conn, children)
        processed.update(int(child["event_id"]) for child in children)
        result["sources"] += len(children)
        result["groups"] += 1
    return result


class SnapshotSyncWorker:
    def __init__(self):
        self._task = None
        self._ready = False
        self._sheet_reconciler = research_sheet_reconciliation.SheetReconciler()
        self._runtime: Dict[str, Any] = {"last_result": None, "last_error": None}

    def status(self) -> Dict[str, Any]:
        return {
            "enabled": bool(google_sheets_sync.enabled() and _database_url()),
            "running": bool(self._task and not self._task.done()),
            "ready": self._ready,
            "reconcile_version": RECONCILE_VERSION,
            "backfill_days": _BACKFILL_DAYS,
            "telegram_sheet_reconciliation": self._sheet_reconciler.status(),
            **self._runtime,
        }

    @staticmethod
    def _schema_ready(url: str) -> bool:
        with psycopg.connect(url, row_factory=dict_row, connect_timeout=5,
                             options="-c statement_timeout=15000 -c lock_timeout=1000") as conn:
            row = conn.execute(
                """SELECT to_regclass('research_snapshot_sheet_sources') IS NOT NULL
                      AND to_regclass('research_sheet_upsert_outbox') IS NOT NULL
                      AND to_regclass('research_event_btc_movements') IS NOT NULL AS ready"""
            ).fetchone()
            return bool(row and row["ready"])

    def run_once(self) -> Dict[str, Any]:
        url = _database_url()
        if not google_sheets_sync.enabled() or not url or psycopg is None:
            return {"skipped": "DISABLED_OR_UNCONFIGURED"}
        with psycopg.connect(url, row_factory=dict_row, connect_timeout=5,
                             options="-c statement_timeout=15000 -c lock_timeout=1000") as conn:
            staged = reconcile_sources(conn)
        # The connection context committed both payloads and source coverage.
        delivered = research_sheet_outbox.drain(url, max_rows=32, max_seconds=45)
        audit = self._sheet_reconciler.run_due(url)
        return {"reconcile": staged, "delivery": delivered, "sheet_audit": audit}

    async def start(self) -> bool:
        if self._task and not self._task.done():
            return True
        if not google_sheets_sync.enabled() or not _database_url() or psycopg is None:
            return False
        try:
            self._ready = await asyncio.to_thread(self._schema_ready, _database_url())
        except Exception as exc:
            self._runtime["last_error"] = type(exc).__name__
            return False
        if not self._ready:
            self._runtime["last_error"] = "MISSING_SNAPSHOT_OUTBOX_MIGRATIONS_022_023"
            return False
        await asyncio.to_thread(google_sheets_sync.use_durable_snapshots)
        self._task = asyncio.create_task(self._run(), name="research-snapshot-sheet-sync")
        return True

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        while True:
            try:
                self._runtime["last_result"] = await asyncio.to_thread(self.run_once)
                self._runtime["last_error"] = None
            except Exception as exc:
                self._runtime["last_error"] = type(exc).__name__
                print(f"[snapshot-sheet-sync] pass failed: {type(exc).__name__}", flush=True)
            await asyncio.sleep(_POLL_SECONDS)


WORKER = SnapshotSyncWorker()
