"""Bounded BTC-source bootstrap and durable causal event membership.

Only research tables are written. Parent segmentation is independent of signal
outcomes; delivered/prospective events are joined after source bars commit. A
session advisory lock serializes bootstrap, checkpoints and assignments across
multiple processes. No Telegram or formula promotion is performed here.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
import os
from typing import Any, Mapping

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover
    psycopg = None
    dict_row = None

import binance_spot_price_path
import research_btc_parent_movement as policy


_LOCK_ID = 682311007312467990
_TRUE = {"1", "true", "yes", "on"}
_PAGE_MINUTES = 1000
_EVENT_BATCH = 500


def _enabled() -> bool:
    return os.getenv("RESEARCH_BTC_EPISODES_ENABLED", os.getenv(
        "RESEARCH_OUTCOME_ENRICHMENT_ENABLED", ""
    )).strip().lower() in _TRUE


def _database_url() -> str:
    dedicated = os.getenv("RESEARCH_DATABASE_URL", "").strip()
    if dedicated:
        return dedicated
    if os.getenv("RESEARCH_USE_PRIMARY_DATABASE", "").strip().lower() in _TRUE:
        return os.getenv("DATABASE_URL", "").strip()
    return ""


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), default=str, allow_nan=False)


def _json_safe_status(value: Any) -> Any:
    """Copy status trees without leaking internal datetime objects to HTTP JSON."""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {key: _json_safe_status(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe_status(item) for item in value]
    return value


def _write_source_and_parents(conn, candles: list, parents: list[dict]) -> None:
    bars = [policy.validate_candle(candle) for candle in candles]
    if bars:
        # Do not rewrite a historical source and silently change established
        # parent identities. Conflicting provider revisions require a new policy.
        conflict = conn.execute(
            """WITH incoming AS (
                SELECT * FROM jsonb_to_recordset(%s::jsonb) AS x(
                    open_time_utc timestamptz,close_time_utc timestamptz,
                    open double precision,high double precision,
                    low double precision,close double precision))
                SELECT b.open_time_utc FROM research_btc_price_bars b
                JOIN incoming i USING(open_time_utc)
                WHERE (b.close_time_utc,b.open,b.high,b.low,b.close)
                    IS DISTINCT FROM
                    (i.close_time_utc,i.open,i.high,i.low,i.close) LIMIT 1""",
            (_json(bars),),
        ).fetchone()
        if conflict:
            raise ValueError("BTC source revision conflicts with frozen research history")
        conn.execute(
            """INSERT INTO research_btc_price_bars
                (open_time_utc,close_time_utc,open,high,low,close)
                SELECT * FROM jsonb_to_recordset(%s::jsonb) AS x(
                    open_time_utc timestamptz,close_time_utc timestamptz,
                    open double precision,high double precision,
                    low double precision,close double precision)
                ON CONFLICT(open_time_utc) DO NOTHING""", (_json(bars),)
        )
    for parent in parents:
        # Stream order first closes the previous active parent, then inserts
        # its successor, satisfying the unique active-policy index.
        conn.execute(
            """INSERT INTO research_btc_parent_movements (
                btc_parent_movement_id,episode_policy_version,start_time_utc,
                end_time_utc,confirmed_at_utc,direction,evidence_eligible,
                boundary_reason,observed_through_utc,price_source,state_json)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                ON CONFLICT (btc_parent_movement_id) DO UPDATE SET
                    end_time_utc=EXCLUDED.end_time_utc,
                    direction=EXCLUDED.direction,
                    observed_through_utc=EXCLUDED.observed_through_utc,
                    state_json=EXCLUDED.state_json,updated_at_utc=NOW()
                WHERE research_btc_parent_movements.end_time_utc IS NULL
                  AND EXCLUDED.observed_through_utc>=
                      research_btc_parent_movements.observed_through_utc""",
            (parent["btc_parent_movement_id"], policy.POLICY_VERSION,
             parent["start_time_utc"], parent["end_time_utc"],
             parent["confirmed_at_utc"], parent["direction"],
             parent["evidence_eligible"], parent["boundary_reason"],
             parent["observed_through_utc"], policy.SOURCE,
             _json(parent["state_json"])),
        )


def _assign_due_events(conn, *, through: datetime) -> dict:
    # Every delivered alert gets membership independently of label availability.
    # Otherwise an early match lacking price outcomes could disappear, allowing
    # a later winning repeat to become the supposedly earliest evidence anchor.
    # v7 admission is used only for authorized prospective samples, which the
    # current alert-only candidate summary does not count as match evidence.
    rows = conn.execute(
        """WITH picked AS MATERIALIZED (
            SELECT e.event_id,e.alert_time_utc
            FROM research_events e
            WHERE e.alert_time_utc<=%s
              AND e.alert_time_utc >= (
                SELECT MIN(start_time_utc) FROM research_btc_parent_movements
                WHERE episode_policy_version=%s)
              AND ((e.event_kind='ALERT' AND e.delivery_status='DELIVERED')
                OR (e.event_kind='DECISION_SAMPLE' AND EXISTS (
                    SELECT 1 FROM research_ordered_first_touch_outcomes o
                    WHERE o.event_id=e.event_id
                      AND o.method_version='ordered-first-touch-v7')))
              AND NOT EXISTS (SELECT 1 FROM research_event_btc_movements m
                WHERE m.event_id=e.event_id AND m.episode_policy_version=%s)
            ORDER BY e.alert_time_utc,e.event_id LIMIT %s)
            SELECT e.*,to_jsonb(p) AS parent,to_jsonb(b) AS btc_bar FROM picked e
            LEFT JOIN LATERAL (
                SELECT * FROM research_btc_parent_movements p
                WHERE p.episode_policy_version=%s
                  AND p.start_time_utc<=e.alert_time_utc
                  AND (p.end_time_utc IS NULL OR p.end_time_utc>e.alert_time_utc)
                ORDER BY p.start_time_utc DESC LIMIT 1) p ON TRUE
            LEFT JOIN LATERAL (
                SELECT close_time_utc FROM research_btc_price_bars b
                WHERE b.close_time_utc<=e.alert_time_utc
                ORDER BY b.close_time_utc DESC LIMIT 1) b ON TRUE""",
        (through, policy.POLICY_VERSION, policy.POLICY_VERSION,
         _EVENT_BATCH, policy.POLICY_VERSION),
    ).fetchall()
    members = [policy.membership(row, parent=row.get("parent"), btc_bar=row.get("btc_bar"))
               for row in rows]
    if members:
        conn.execute(
            """INSERT INTO research_event_btc_movements (
                event_id,episode_policy_version,btc_parent_movement_id,
                decision_time_utc,btc_observed_close_utc,membership_status)
                SELECT * FROM jsonb_to_recordset(%s::jsonb) AS x(
                    event_id bigint,episode_policy_version text,
                    btc_parent_movement_id text,decision_time_utc timestamptz,
                    btc_observed_close_utc timestamptz,membership_status text)
                ON CONFLICT(event_id,episode_policy_version) DO NOTHING""",
            (_json(members),),
        )
    return {"members_written": len(members), **{
        status.lower(): sum(row["membership_status"] == status for row in members)
        for status in ("LIVE", "BTC_DATA_MISSING", "BOUNDARY_UNVERIFIED")
    }}


class ResearchBTCEpisodeWorker:
    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self.metrics: dict = {"passes": 0, "last_error": None, "last_result": None,
                             "schema_ready": False}

    def status(self) -> dict:
        return {
            "enabled": _enabled(), "database_configured": bool(_database_url()),
            "running": bool(self._task and not self._task.done()),
            "policy_version": policy.POLICY_VERSION,
            "price_source": policy.SOURCE, "reversal_bps": policy.REVERSAL_BPS,
            "policy_origin": "NEW_CONSERVATIVE_ENGINEERING_POLICY",
            "fixed_time_gate": False, "retroactive_pivot": False,
            "automatic_live_promotion": False, "metrics": _json_safe_status(self.metrics),
        }

    async def start(self) -> bool:
        if not _enabled() or not _database_url() or psycopg is None:
            return False
        if not self._task or self._task.done():
            if not await asyncio.to_thread(self._schema_ready):
                return False
            self._task = asyncio.create_task(self._run(), name="research-btc-parent-episodes")
        return True

    def _schema_ready(self) -> bool:
        try:
            with psycopg.connect(_database_url(), row_factory=dict_row, connect_timeout=5,
                                options="-c statement_timeout=5000 -c lock_timeout=1000") as conn:
                row = conn.execute("""SELECT
                    to_regclass('public.research_btc_price_bars') IS NOT NULL
                    AND to_regclass('public.research_btc_parent_movements') IS NOT NULL
                    AND to_regclass('public.research_event_btc_movements') IS NOT NULL
                    AND EXISTS (SELECT 1 FROM pg_trigger
                        WHERE tgname='research_event_btc_membership_v1'
                          AND tgrelid=to_regclass('public.research_event_btc_movements')
                          AND NOT tgisinternal) AS ready""").fetchone()
            ready = bool(row and row.get("ready"))
            self.metrics["schema_ready"] = ready
            self.metrics["last_error"] = None if ready else (
                "BTC episode schema unavailable: apply 021_btc_parent_movements_v1.sql"
            )
            return ready
        except Exception as exc:
            self.metrics["schema_ready"] = False
            self.metrics["last_error"] = f"BTC episode schema check failed: {type(exc).__name__}: {exc}"
            return False

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self) -> None:
        while True:
            delay = 60
            try:
                result = await asyncio.to_thread(self.run_once)
                if result.get("bars_written") and not result.get("caught_up"):
                    delay = 10
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.metrics["last_error"] = f"{type(exc).__name__}: {exc}"
                print(f"[research-btc-episodes] pass failed: {exc!r}", flush=True)
            await asyncio.sleep(delay)

    def run_once(self, *, now: datetime | None = None, fetch_candles=None) -> dict:
        if not _enabled() or not _database_url() or psycopg is None:
            return {"state": "DISABLED"}
        now = policy.utc(now or datetime.now(timezone.utc))
        cutoff = now.replace(second=0, microsecond=0)-timedelta(milliseconds=1)
        fetch = fetch_candles or binance_spot_price_path.fetch_closed_candles
        with psycopg.connect(_database_url(), row_factory=dict_row, connect_timeout=5,
                            options="-c statement_timeout=15000 -c lock_timeout=1000") as conn:
            locked = conn.execute("SELECT pg_try_advisory_lock(%s) AS locked", (_LOCK_ID,)).fetchone()
            conn.commit()
            if not locked or not locked["locked"]:
                return {"state": "BUSY"}
            try:
                current = conn.execute(
                    """SELECT * FROM research_btc_parent_movements
                        WHERE episode_policy_version=%s AND end_time_utc IS NULL""",
                    (policy.POLICY_VERSION,),
                ).fetchone()
                if current:
                    start = policy.utc(current["state_json"]["last_open_time_utc"])+timedelta(minutes=1)
                else:
                    configured = os.getenv("RESEARCH_BTC_EPISODE_START_UTC", "").strip()
                    if configured:
                        start = policy.utc(configured).replace(second=0, microsecond=0)
                    else:
                        row = conn.execute(
                            """SELECT MIN(alert_time_utc) AS first FROM research_events
                                WHERE event_kind='ALERT' AND delivery_status='DELIVERED'
                                  AND alert_time_utc>=NOW()-INTERVAL '14 days'"""
                        ).fetchone()
                        first = row.get("first") if row else None
                        start = (policy.utc(first) if first else now)-timedelta(days=1)
                        start = start.replace(second=0, microsecond=0)
                conn.commit()
                end = min(cutoff, start+timedelta(minutes=_PAGE_MINUTES)-timedelta(milliseconds=1))
                candles = []
                parents = []
                if end >= start+timedelta(minutes=1)-timedelta(milliseconds=1):
                    fetched = fetch("BTC", start, end)
                    if (fetched.get("exchange"), fetched.get("market"), fetched.get("pair"),
                        fetched.get("interval_seconds")) != ("binance", "spot", "BTCUSDT", 60):
                        raise ValueError("BTC episode source provenance does not match policy")
                    candles = list(fetched.get("candles") or [])
                    if any(policy.utc(c.open_time_utc if hasattr(c, "open_time_utc")
                                     else c["open_time_utc"]) < start for c in candles):
                        raise ValueError("BTC source returned candles preceding its requested cursor")
                    parents = policy.advance_parents(candles, previous=current, as_of_utc=end)
                    _write_source_and_parents(conn, candles, parents)
                    conn.commit()
                active = parents[-1] if parents else current
                members = {"members_written": 0}
                if active:
                    observed = policy.utc(active["observed_through_utc"])
                    # A closed minute remains the latest knowable close for
                    # less than one minute; no event can borrow a future bar.
                    through = min(now, observed+timedelta(minutes=1)-timedelta(microseconds=1))
                    members = _assign_due_events(conn, through=through)
                    conn.commit()
                result = {
                    "state": "OK", "bars_written": len(candles),
                    "parents_updated": len(parents),
                    "observed_through_utc": active["observed_through_utc"] if active else None,
                    "caught_up": bool(active and policy.utc(active["observed_through_utc"]) >= cutoff),
                    **members,
                }
                self.metrics.update(passes=self.metrics["passes"]+1,
                                    last_result=result, last_error=None)
                print(f"[research-btc-episodes] {_json(result)}", flush=True)
                return result
            finally:
                conn.rollback()
                conn.execute("SELECT pg_advisory_unlock(%s)", (_LOCK_ID,))
                conn.commit()


WORKER = ResearchBTCEpisodeWorker()
