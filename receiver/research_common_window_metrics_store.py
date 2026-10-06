"""Bounded durable maturity queue and immutable READY common-window metrics."""
from __future__ import annotations

from datetime import timedelta
import json
from typing import Any, Iterable, Mapping

from research_common_window_metrics import METHOD_VERSION, WINDOWS, utc


def available(conn) -> bool:
    row = conn.execute("SELECT to_regclass('research_common_window_metrics') IS NOT NULL AS available").fetchone()
    return bool(row and row["available"])


def seed_event(conn, event: Mapping[str, Any]) -> int:
    return _seed_events(conn, [event])


def _seed_events(conn, events: Iterable[Mapping[str, Any]]) -> int:
    rows = []
    for event in events:
        start = utc(event["alert_time_utc"])
        rows.extend({"event_id": int(event["event_id"]), "window_minutes": window,
            "measurement_start_utc": start.isoformat(), "window_end_utc": (start + timedelta(minutes=window)).isoformat()}
            for window in WINDOWS)
    if not rows:
        return 0
    result = conn.execute("""
        INSERT INTO research_common_window_metrics(event_id,window_minutes,method_version,status,
            measurement_start_utc,window_end_utc,next_attempt_at_utc,result)
        SELECT event_id,window_minutes,%s,'PENDING',measurement_start_utc,window_end_utc,
            window_end_utc,'{}'::jsonb
        FROM jsonb_to_recordset(%s::jsonb) AS x(event_id BIGINT,window_minutes INTEGER,
            measurement_start_utc TIMESTAMPTZ,window_end_utc TIMESTAMPTZ)
        ON CONFLICT DO NOTHING
    """, (METHOD_VERSION, json.dumps(rows)))
    return result.rowcount


def seed_bounded_history(conn, *, limit: int = 200) -> int:
    """Scan a bounded primary-key prefix; terminal First Touch still matures.

    Future/new v7 calculations call seed_event directly, so an event not yet
    evaluated at this cursor position is not permanently missed.
    """
    conn.execute("INSERT INTO research_common_window_metrics_cursor(method_version) VALUES (%s) ON CONFLICT DO NOTHING", (METHOD_VERSION,))
    cursor = conn.execute("SELECT last_event_id FROM research_common_window_metrics_cursor WHERE method_version=%s FOR UPDATE", (METHOD_VERSION,)).fetchone()
    candidates = conn.execute("""
        WITH picked AS MATERIALIZED (
            SELECT event_id,alert_time_utc FROM research_events
            WHERE event_id>%s ORDER BY event_id LIMIT %s
        )
        SELECT picked.*, EXISTS(SELECT 1 FROM research_ordered_first_touch_outcomes o
            WHERE o.event_id=picked.event_id AND o.method_version='ordered-first-touch-v7') AS has_v7
        FROM picked ORDER BY event_id
    """, (cursor["last_event_id"], max(1, min(int(limit), 1000)))).fetchall()
    inserted = _seed_events(conn, (row for row in candidates if row["has_v7"]))
    if candidates:
        conn.execute("UPDATE research_common_window_metrics_cursor SET last_event_id=%s WHERE method_version=%s", (candidates[-1]["event_id"], METHOD_VERSION))
    return inserted


def load_due_events(conn, *, limit: int = 4) -> list[dict[str, Any]]:
    """The existing ordered-pass advisory lock protects this queue consumer.

    Each event has four jobs: examining limit*4 index-ordered rows guarantees
    a bounded prefix without scanning the full metric table or event history.
    """
    rows = conn.execute("""
        WITH due AS MATERIALIZED (
            SELECT event_id,next_attempt_at_utc FROM research_common_window_metrics
            WHERE status IN ('PENDING','OPEN','DATA_MISSING') AND method_version=%s
                AND next_attempt_at_utc<=NOW()
            ORDER BY next_attempt_at_utc,event_id,window_minutes LIMIT %s
        ), picked AS (
            SELECT event_id,MIN(next_attempt_at_utc) AS due_at FROM due
            GROUP BY event_id ORDER BY due_at,event_id LIMIT %s
        )
        SELECT e.* FROM picked JOIN research_events e USING(event_id)
        ORDER BY picked.due_at,e.event_id
    """, (METHOD_VERSION, max(1, min(int(limit), 16))*4, max(1, min(int(limit), 16)))).fetchall()
    return [dict(row) for row in rows]


def write_metrics(conn, *, event_id: int, metrics: Mapping[str, Any]) -> bool:
    if metrics.get("method_version") != METHOD_VERSION:
        raise ValueError("wrong common-window metric version")
    result = {**metrics, "event_id": int(event_id)}
    next_attempt = utc(metrics["window_end_utc"])
    if metrics["status"] == "DATA_MISSING":
        next_attempt = utc(metrics["observed_at_utc"]) + timedelta(hours=6)
    row = conn.execute("""
        UPDATE research_common_window_metrics SET status=%s,result=%s::jsonb,
            next_attempt_at_utc=%s,updated_at_utc=NOW()
        WHERE event_id=%s AND window_minutes=%s AND method_version=%s
            AND status IN ('PENDING','OPEN','DATA_MISSING')
            AND (result->>'observed_at_utc' IS NULL OR
                (result->>'observed_at_utc')::timestamptz<=%s::timestamptz)
        RETURNING event_id
    """, (metrics["status"], json.dumps(result, default=str, allow_nan=False), next_attempt,
        int(event_id), int(metrics["window_minutes"]), METHOD_VERSION, metrics["observed_at_utc"])).fetchone()
    return bool(row)


def defer_event(conn, event_id: int, *, reason: str) -> None:
    conn.execute("""
        UPDATE research_common_window_metrics SET status='DATA_MISSING',
            next_attempt_at_utc=NOW()+INTERVAL '6 hours',updated_at_utc=NOW(),
            result=result || jsonb_build_object('status','DATA_MISSING','missing_reason',%s::text)
        WHERE event_id=%s AND method_version=%s AND status IN ('PENDING','OPEN','DATA_MISSING')
    """, (str(reason)[:500], int(event_id), METHOD_VERSION))


def load_by_event_ids(conn, event_ids: Iterable[int], window_minutes: int) -> dict[int, dict[str, Any]]:
    """Exact event/horizon/version query; caller joins each threshold separately.

    Thresholds share a price path, not independent evidence. Preserve the same
    frozen representative selection across statuses and report missing rows.
    """
    ids = sorted(set(int(value) for value in event_ids))
    if window_minutes not in WINDOWS or len(ids) > 10000:
        raise ValueError("invalid or unbounded metric lookup")
    if not ids:
        return {}
    rows = conn.execute("""
        SELECT event_id,window_minutes,method_version,status,measurement_start_utc,
            window_end_utc,result FROM research_common_window_metrics
        WHERE event_id=ANY(%s::bigint[]) AND window_minutes=%s AND method_version=%s
    """, (ids, window_minutes, METHOD_VERSION)).fetchall()
    return {int(row["event_id"]): {**dict(row.get("result") or {}),
        **{key: row[key] for key in ("event_id", "window_minutes", "method_version", "status", "measurement_start_utc", "window_end_utc")}}
        for row in rows}
