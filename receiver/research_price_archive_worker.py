"""Bounded continuous 1m archive, durable history cursor and explicit gap repair.

All live tails precede history. Every historical page advances its durable
cursor even when its missing minutes are separately queued for retry. One bad
old provider interval therefore cannot prevent collecting current prices.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
import os
import time

import research_price_archive as archive

LOCK_ID = 682311007312468044
DEFAULT_START = '2026-08-15T21:00:00+00:00'


def _config(name, default, minimum, maximum):
    return max(minimum, min(maximum, int(os.getenv(name, str(default)))))


def retention_floor(route, now):
    if route in (archive.HYPERLIQUID_PERP, archive.HYPERLIQUID_SPOT):
        # The current, not-yet-closed minute can occupy one of the 5,000 slots.
        return now.replace(second=0, microsecond=0)-4999*archive.MINUTE
    return None


def history_window(state, cutoff, now):
    cursor = state['history_cursor_utc']
    floor = retention_floor(state['route'], now)
    if floor:
        cursor = max(cursor, floor)
    if cursor+archive.MINUTE-archive.MILLISECOND > cutoff:
        return None
    return cursor, min(cutoff, cursor+archive.PAGE_MINUTES*archive.MINUTE-archive.MILLISECOND)


def tail_window(latest, cutoff):
    # One overlapping cached minute anchors continuity. Large outages are
    # recovered by the durable history cursor while current tails remain first.
    start = (latest if latest else cutoff.replace(second=0, microsecond=0)-9*archive.MINUTE)
    start = max(start, cutoff.replace(second=0, microsecond=0)-999*archive.MINUTE)
    return start, cutoff


def _raw_fetchers():
    import binance_spot_price_path
    import hyperliquid_perp_price_path
    return {archive.BINANCE_SPOT: binance_spot_price_path.fetch_closed_candles,
            archive.HYPERLIQUID_PERP: hyperliquid_perp_price_path.fetch_closed_candles}


def _record_gaps(conn, route, symbol, start, end, path, now, *, error=None):
    first, last, _ = archive.bounds(start, end)
    # Reconcile this whole checked interval atomically; surrounding gaps are
    # preserved and will be narrowed on their own retry. No fabricated candles.
    with conn.transaction():
        previous = conn.execute("""SELECT COALESCE(max(attempts),0) AS attempts
            FROM research_price_archive_gaps WHERE route=%s AND symbol=%s
            AND start_time_utc>=%s AND end_time_utc<=%s AND status='RETRY'""",
            (route,symbol,first,end)).fetchone()['attempts']
        conn.execute("""DELETE FROM research_price_archive_gaps WHERE route=%s AND symbol=%s
            AND start_time_utc>=%s AND end_time_utc<=%s AND status='RETRY'""",
            (route, symbol, first, end))
        missing = ([{'start_time_utc':first, 'end_time_utc':last+archive.MINUTE-archive.MILLISECOND}]
                   if error else path.get('archive_missing_ranges', []))
        for gap in missing:
            conn.execute("""INSERT INTO research_price_archive_gaps
                (route,symbol,start_time_utc,end_time_utc,status,reason,next_retry_utc,attempts)
                VALUES (%s,%s,%s,%s,'RETRY',%s,%s,%s)
                ON CONFLICT(route,symbol,start_time_utc,end_time_utc) DO UPDATE SET
                    status='RETRY',reason=EXCLUDED.reason,
                    attempts=research_price_archive_gaps.attempts+1,
                    next_retry_utc=EXCLUDED.next_retry_utc,updated_at_utc=NOW()""",
                (route,symbol,archive.utc(gap['start_time_utc']),archive.utc(gap['end_time_utc']),
                 error or 'PROVIDER_MISSING_CLOSED_MINUTES',now+timedelta(minutes=15),previous+1))


def _initialize(conn, now, requested):
    for route, symbol in archive.ACTIVE_ROUTES:
        floor = retention_floor(route, now)
        cursor = max(requested, floor) if floor else requested
        conn.execute("""INSERT INTO research_price_archive_cursors
            (route,symbol,requested_start_utc,history_cursor_utc,unavailable_before_utc)
            VALUES (%s,%s,%s,%s,%s) ON CONFLICT(route,symbol) DO NOTHING""",
            (route,symbol,requested,cursor,floor))
        if floor:
            # Record the moving retention boundary separately from retained bars;
            # older locally archived history remains readable and is never lost.
            conn.execute("""UPDATE research_price_archive_cursors SET
                unavailable_before_utc=%s,history_cursor_utc=GREATEST(history_cursor_utc,%s),
                updated_at_utc=NOW() WHERE route=%s AND symbol=%s""", (floor,floor,route,symbol))
            conn.execute("""UPDATE research_price_archive_gaps SET status='UNAVAILABLE',
                reason='OUTSIDE_PROVIDER_5000_CANDLE_RETENTION',updated_at_utc=NOW()
                WHERE route=%s AND symbol=%s AND end_time_utc<%s AND status='RETRY'""",
                (route,symbol,floor))
            crossing = conn.execute("""SELECT * FROM research_price_archive_gaps
                WHERE route=%s AND symbol=%s AND start_time_utc<%s
                AND end_time_utc>=%s AND status='RETRY'""",(route,symbol,floor,floor)).fetchall()
            for gap in crossing:
                with conn.transaction():
                    conn.execute("""DELETE FROM research_price_archive_gaps WHERE route=%s
                        AND symbol=%s AND start_time_utc=%s AND end_time_utc=%s""",
                        (route,symbol,gap['start_time_utc'],gap['end_time_utc']))
                    for first,last,status,reason in (
                        (gap['start_time_utc'],floor-archive.MILLISECOND,'UNAVAILABLE',
                         'OUTSIDE_PROVIDER_5000_CANDLE_RETENTION'),
                        (floor,gap['end_time_utc'],'RETRY',gap['reason'])):
                        conn.execute("""INSERT INTO research_price_archive_gaps
                            (route,symbol,start_time_utc,end_time_utc,status,reason,next_retry_utc,attempts)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT(route,symbol,start_time_utc,end_time_utc) DO NOTHING""",
                            (route,symbol,first,last,status,reason,gap['next_retry_utc'],gap['attempts']))


def _collect_page(conn, state, start, end, fetchers, now, *, lane):
    route, symbol = state['route'], state['symbol']
    try:
        path = archive.get_path(route,symbol,start,end,fetchers[route],connection=conn,now=now)
        _record_gaps(conn,route,symbol,start,end,path,now)
        latest = max((archive.normalize_bar(c,now=now)['open_time_utc']
                      for c in path['candles']),default=None)
        error = None if path['complete'] else 'PROVIDER_MISSING_CLOSED_MINUTES'
        result = dict(route=route,symbol=symbol,lane=lane,complete=path['complete'],
                      candles=len(path['candles']),cached=path['archive_cached_candles'])
    except Exception as exc:
        # Only the class is retained: provider/DB exception bodies can contain
        # URLs or connection strings. The failed interval remains retryable.
        error = type(exc).__name__
        _record_gaps(conn,route,symbol,start,end,{},now,error=error)
        latest = None
        result = dict(route=route,symbol=symbol,lane=lane,complete=False,error=error)
    column = 'last_tail_attempt_utc' if lane=='tail' else 'last_history_attempt_utc'
    conn.execute(f"""UPDATE research_price_archive_cursors SET {column}=%s,
        latest_open_utc=GREATEST(latest_open_utc,%s),last_error=%s,updated_at_utc=NOW()
        WHERE route=%s AND symbol=%s""", (now,latest,error,route,symbol))
    if lane=='history':
        conn.execute("""UPDATE research_price_archive_cursors SET
            history_cursor_utc=GREATEST(history_cursor_utc,%s)
            WHERE route=%s AND symbol=%s""",
            (end+archive.MILLISECOND,route,symbol))
    return result


def snapshot(conn):
    """Eight indexed edge lookups and a small gap queue; never scan all bars."""
    return conn.execute("""SELECT c.*,b.open_time_utc AS stored_latest_open_utc,
        g.retry_ranges,g.unavailable_ranges FROM research_price_archive_cursors c
        LEFT JOIN LATERAL(SELECT open_time_utc FROM research_price_archive_bars b
            WHERE b.route=c.route AND b.symbol=c.symbol ORDER BY open_time_utc DESC LIMIT 1)b ON TRUE
        LEFT JOIN LATERAL(SELECT count(*) FILTER(WHERE status='RETRY') AS retry_ranges,
            count(*) FILTER(WHERE status='UNAVAILABLE') AS unavailable_ranges
            FROM research_price_archive_gaps g WHERE g.route=c.route AND g.symbol=c.symbol)g ON TRUE
        ORDER BY c.route,c.symbol""").fetchall()


class ResearchPriceArchiveWorker:
    def __init__(self):
        self._task = None
        self.metrics = dict(passes=0,last_error=None,last_result=None,schema_ready=False)

    def status(self):
        return json.loads(json.dumps(dict(enabled=archive.enabled(),
            database_configured=bool(archive.database_url()),
            running=bool(self._task and not self._task.done()),
            retention_policy='PERSIST_CLOSED_BARS_WITHOUT_EXPIRY',
            metrics=self.metrics),default=str))

    def _schema_ready(self):
        if not archive.database_url() or archive.psycopg is None:
            return False
        with archive.psycopg.connect(archive.database_url(),autocommit=True,
                row_factory=archive.dict_row,connect_timeout=5,
                options='-c statement_timeout=5000 -c lock_timeout=1000') as conn:
            self.metrics['schema_ready'] = archive.schema_ready(conn)
            return self.metrics['schema_ready']

    async def start(self):
        if not archive.enabled() or not archive.database_url() or archive.psycopg is None:
            return False
        if not self._task or self._task.done():
            try:
                if not await asyncio.to_thread(self._schema_ready):
                    self.metrics['last_error'] = 'Apply 044_continuous_price_archive.sql'
                    return False
            except Exception as exc:
                self.metrics['last_error'] = type(exc).__name__
                return False
            self._task = asyncio.create_task(self._run(),name='research-continuous-price-archive')
        return True

    async def stop(self):
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self):
        while True:
            started = time.monotonic()
            try:
                await asyncio.to_thread(self.run_once)
                delay = max(0,_config('RESEARCH_PRICE_ARCHIVE_POLL_SECONDS',60,30,3600)
                            -(time.monotonic()-started))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.metrics['last_error'] = type(exc).__name__
                delay = 60
            await asyncio.sleep(delay)

    def run_once(self, *, now=None, fetchers=None, connection=None):
        if connection is None:
            if not archive.enabled() or not archive.database_url() or archive.psycopg is None:
                return {'state':'DISABLED'}
            with archive.psycopg.connect(archive.database_url(),autocommit=True,
                    row_factory=archive.dict_row,connect_timeout=5,
                    options='-c statement_timeout=15000 -c lock_timeout=2000') as conn:
                if not archive.schema_ready(conn):
                    return {'state':'SCHEMA_MISSING'}
                return self.run_once(now=now,fetchers=fetchers,connection=conn)
        conn = connection
        locked = conn.execute('SELECT pg_try_advisory_lock(%s) AS locked',(LOCK_ID,)).fetchone()['locked']
        if not locked:
            return {'state':'BUSY'}
        started = time.monotonic()
        deadline = started+_config('RESEARCH_PRICE_ARCHIVE_BUDGET_SECONDS',60,5,180)
        now = archive.utc(now or datetime.now(timezone.utc))
        cutoff = now.replace(second=0,microsecond=0)-archive.MILLISECOND
        requested = archive.utc(os.getenv('RESEARCH_PRICE_ARCHIVE_START_UTC',DEFAULT_START)).replace(second=0,microsecond=0)
        fetchers = fetchers or _raw_fetchers()
        pages = []
        try:
            _initialize(conn,now,requested)
            states = conn.execute("""SELECT * FROM research_price_archive_cursors
                ORDER BY last_tail_attempt_utc NULLS FIRST,route,symbol""").fetchall()
            for state in states:
                if time.monotonic()>=deadline:
                    break
                row = conn.execute("""SELECT max(open_time_utc) AS latest
                    FROM research_price_archive_bars WHERE route=%s AND symbol=%s""",
                    (state['route'],state['symbol'])).fetchone()
                start,end = tail_window(row['latest'],cutoff)
                pages.append(_collect_page(conn,state,start,end,fetchers,now,lane='tail'))
            history_budget = _config('RESEARCH_PRICE_ARCHIVE_BACKFILL_PAGES',8,1,32)
            # Reserve at most half the history budget for old missing ranges.
            due = conn.execute("""SELECT * FROM research_price_archive_gaps
                WHERE status='RETRY' AND next_retry_utc<=%s
                ORDER BY next_retry_utc,route,symbol LIMIT %s""",
                (now,max(1,history_budget//2))).fetchall()
            for gap in due:
                if time.monotonic()>=deadline:
                    break
                start = gap['start_time_utc']
                floor = retention_floor(gap['route'],now)
                if floor:
                    start = max(start,floor)
                if start>=gap['end_time_utc']:
                    continue
                end = min(gap['end_time_utc'],start+archive.PAGE_MINUTES*archive.MINUTE-archive.MILLISECOND)
                pages.append(_collect_page(conn,gap,start,end,fetchers,now,lane='retry'))
                history_budget -= 1
            for _ in range(history_budget):
                if time.monotonic()>=deadline:
                    break
                states = conn.execute("""SELECT * FROM research_price_archive_cursors
                    WHERE history_cursor_utc<=%s
                    ORDER BY last_history_attempt_utc NULLS FIRST,history_cursor_utc,route,symbol""",
                    (cutoff.replace(second=0,microsecond=0),)).fetchall()
                chosen = next(((s,history_window(s,cutoff,now)) for s in states
                               if history_window(s,cutoff,now)),None)
                if chosen is None:
                    break
                state,(start,end) = chosen
                pages.append(_collect_page(conn,state,start,end,fetchers,now,lane='history'))
            result = dict(state='OK',pages=pages,coverage=snapshot(conn),
                          duration_seconds=round(time.monotonic()-started,3),cutoff_utc=cutoff)
            self.metrics.update(passes=self.metrics['passes']+1,last_result=result,
                                last_error=None,schema_ready=True)
            # Optional freshness projection has its own independent status and
            # must not roll back successfully committed price archival.
            try:
                import research_source_freshness
                if archive.database_url():
                    result['source_freshness'] = research_source_freshness.run_refresh(database_url=archive.database_url())
            except ImportError:
                pass
            except Exception as exc:
                result['source_freshness_error'] = type(exc).__name__
            print('[research-price-archive] '+json.dumps({k:v for k,v in result.items()
                if k not in ('pages','coverage')},default=str),flush=True)
            return result
        finally:
            conn.execute('SELECT pg_advisory_unlock(%s)',(LOCK_ID,))


WORKER = ResearchPriceArchiveWorker()
