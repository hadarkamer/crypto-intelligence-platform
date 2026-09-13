"""Bounded cache-only measurement of every accepted Watch scan.

The existing collector owns prices and the existing BTC worker owns waves.
Each coin is measured once per direction/window, independently of scores or
delivery. No market requests, event synthesis, formula evaluation or messaging.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
import os

import research_btc_parent_movement as btc_policy
import research_price_archive as prices
import research_watch_scan_intake as intake
import research_watch_scan_measurement as measurement

LOCK_ID = 47260913184611
SCAN_PAGE = 32
JOB_LIMIT = 16
POLL_SECONDS = 30
MINUTE = timedelta(minutes=1)


def enqueue(conn, *, now):
    """Caller owns transaction/worker lock; finite laps recover late commits."""
    conn.execute('INSERT INTO research_watch_scan_measurement_state(measurement_version) '
                 'VALUES (%s) ON CONFLICT DO NOTHING', (measurement.VERSION,))
    state = conn.execute('SELECT * FROM research_watch_scan_measurement_state '
                         'WHERE measurement_version=%s FOR UPDATE', (measurement.VERSION,)).fetchone()
    cursor, high = state['scan_cursor'], state['scan_high_water']
    if cursor >= high:
        cursor = 0
        high = conn.execute('SELECT COALESCE(MAX(snapshot_set_id),0) AS id '
                            'FROM research_watch_scan_intakes WHERE consumer_version=%s',
                            (intake.VERSION,)).fetchone()['id']
    page = conn.execute('SELECT snapshot_set_id FROM research_watch_scan_intakes '
                        'WHERE consumer_version=%s AND snapshot_set_id>%s AND snapshot_set_id<=%s '
                        'ORDER BY snapshot_set_id LIMIT %s',
                        (intake.VERSION, cursor, high, SCAN_PAGE)).fetchall()
    recent = conn.execute('SELECT snapshot_set_id FROM research_watch_scan_intakes '
                          'WHERE consumer_version=%s ORDER BY snapshot_set_id DESC LIMIT 2',
                          (intake.VERSION,)).fetchall()
    ids = sorted({r['snapshot_set_id'] for r in page + recent})
    rows = conn.execute('SELECT snapshot_set_id,usable_from_utc FROM research_watch_scan_intakes '
                        "WHERE consumer_version=%s AND intake_status='ACCEPTED' "
                        'AND snapshot_set_id=ANY(%s::bigint[])', (intake.VERSION, ids)).fetchall()
    written = 0
    for row in rows:
        entry = measurement.entry_time(row['usable_from_utc'])
        for symbol in intake.capture.SYMBOLS:
            result = conn.execute('''INSERT INTO research_watch_scan_measurements
                (measurement_version,consumer_version,snapshot_set_id,symbol,
                 usable_from_utc,entry_time_utc,price_route,next_attempt_at_utc)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING''',
                (measurement.VERSION, intake.VERSION, row['snapshot_set_id'], symbol,
                 row['usable_from_utc'], entry, measurement.route_for_symbol(symbol), entry + MINUTE))
            written += result.rowcount
    cursor = page[-1]['snapshot_set_id'] if page else high
    finished = cursor >= high or len(page) < SCAN_PAGE
    if finished:
        cursor = high
    conn.execute('''UPDATE research_watch_scan_measurement_state SET scan_cursor=%s,
        scan_high_water=%s,completed_laps=completed_laps+%s,updated_at_utc=%s
        WHERE measurement_version=%s''', (cursor, high, int(finished), now, measurement.VERSION))
    return written


def _membership_source(conn, decision):
    parent = conn.execute('''SELECT * FROM research_btc_parent_movements
        WHERE episode_policy_version=%s AND start_time_utc<=%s
          AND (end_time_utc IS NULL OR end_time_utc>%s)
        ORDER BY start_time_utc DESC LIMIT 1''',
        (btc_policy.POLICY_VERSION, decision, decision)).fetchone()
    bar = conn.execute('''SELECT open_time_utc,close_time_utc,open,high,low,close FROM research_btc_price_bars
        WHERE close_time_utc<=%s AND close_time_utc>%s-INTERVAL '1 minute'
        ORDER BY close_time_utc DESC LIMIT 1''', (decision, decision)).fetchone()
    return parent, bar


def next_attempt(result, *, now, attempts):
    records = result.get('records') or []
    if result['status'] == 'READY' and result.get('membership', {}).get('membership_status') in {
            'LIVE', 'BOUNDARY_UNVERIFIED'}:
        return None
    retry = now + timedelta(minutes=min(30, 5 * max(1, attempts)))
    future = [intake._utc(r['window_end_utc']) for r in records
              if intake._utc(r['window_end_utc']) > now]
    # Open complete prefixes are revisited at the next full measurement horizon,
    # not recalculated every poll. Missing price/BTC evidence retries separately.
    if result['status'] == 'OPEN' and result.get('membership', {}).get('membership_status') != 'BTC_DATA_MISSING':
        return min(future) if future else retry
    return min([retry, *future])


def write_result(conn, job, result, *, now):
    if (result['version'], result['consumer_version'], result['snapshot_set_id'],
        result['symbol'], result['price_route'], intake._utc(result['entry_time_utc'])) != (
        job['measurement_version'], job['consumer_version'], job['snapshot_set_id'],
        job['symbol'], job['price_route'], job['entry_time_utc']):
        raise ValueError('Measurement result identity mismatch')
    conn.execute('''UPDATE research_watch_scan_measurements SET status=%s,payload=%s::jsonb,
        attempts=attempts+1,next_attempt_at_utc=%s,last_error=%s,updated_at_utc=%s
        WHERE measurement_version=%s AND consumer_version=%s AND snapshot_set_id=%s AND symbol=%s''',
        (result['status'], json.dumps(result, default=str, allow_nan=False, separators=(',', ':')),
         next_attempt(result, now=now, attempts=job['attempts']+1), result.get('error_reason'), now,
         measurement.VERSION, intake.VERSION, job['snapshot_set_id'], job['symbol']))


def process_page(conn, *, now=None, limit=JOB_LIMIT):
    """One atomic bounded pass; failed coins use savepoints and cannot block peers."""
    if type(limit) is not int or not 1 <= limit <= JOB_LIMIT:
        raise ValueError('Invalid Watch measurement job budget')
    now = intake._utc(now or datetime.now(timezone.utc))
    if not conn.execute('SELECT pg_try_advisory_xact_lock(%s) AS held', (LOCK_ID,)).fetchone()['held']:
        return {'locked': True, 'processed': 0}
    counts = {'locked': False, 'enqueued': enqueue(conn, now=now), 'processed': 0,
              'ready': 0, 'open': 0, 'missing': 0, 'errors': 0}
    jobs = conn.execute('''SELECT m.*,i.source_version,i.bundle_sha256,i.parent_payload_sha256,
        i.capture_phase,i.observed_at_utc,i.intake_status,i.source_available_at_utc,
        i.source_created_at_utc FROM research_watch_scan_measurements m
        JOIN research_watch_scan_intakes i USING(consumer_version,snapshot_set_id)
        WHERE m.measurement_version=%s AND m.next_attempt_at_utc<=%s
        ORDER BY m.next_attempt_at_utc,m.snapshot_set_id,m.symbol LIMIT %s
        FOR UPDATE OF m SKIP LOCKED''', (measurement.VERSION, now, limit)).fetchall()
    for job in jobs:
        try:
            with conn.transaction():
                entry = job['entry_time_utc']
                end = min(now.replace(second=0, microsecond=0), entry + timedelta(minutes=1440))
                path = prices.read_path(job['price_route'], job['symbol'], entry, end, connection=conn)
                parent, bar = _membership_source(conn, job['usable_from_utc'])
                observation = dict(job, population_version=intake.POPULATION)
                result = measurement.measure(observation, path, observed_at=now, parent=parent, btc_bar=bar)
                write_result(conn, job, result, now=now)
            status = result['status']
            counts['ready' if status == 'READY' else 'open' if status == 'OPEN'
                   else 'missing' if status in {'NOT_YET_ENTRY','DATA_MISSING_ENTRY','DATA_MISSING'}
                   else 'errors'] += 1
        except Exception as exc:
            # Preserve prior evidence and disclose a retryable error; never leak
            # DSNs or copy full source payloads into logs.
            conn.execute('''UPDATE research_watch_scan_measurements SET status='ERROR',
                attempts=attempts+1,next_attempt_at_utc=%s,last_error=%s,updated_at_utc=%s
                WHERE measurement_version=%s AND consumer_version=%s AND snapshot_set_id=%s AND symbol=%s''',
                (now+timedelta(minutes=5), type(exc).__name__, now,
                 measurement.VERSION, intake.VERSION, job['snapshot_set_id'], job['symbol']))
            counts['errors'] += 1
        counts['processed'] += 1
    return counts


def _enabled():
    return os.getenv('RESEARCH_WATCH_SCAN_MEASUREMENT_ENABLED',
        os.getenv('RESEARCH_OUTCOME_ENRICHMENT_ENABLED', '')).strip().lower() in intake._TRUE


def schema_ready():
    with intake._connect() as conn:
        return all(conn.execute('SELECT to_regclass(%s) AS relation', ('public.' + name,)).fetchone()['relation']
                   for name in ('research_watch_scan_measurement_state','research_watch_scan_measurements',
                                'research_watch_scan_window_results','research_price_archive_bars',
                                'research_btc_parent_movements','research_btc_price_bars'))


def run_once():
    with intake._connect() as conn:
        return process_page(conn)


class WatchScanMeasurementWorker:
    def __init__(self):
        self._task = None
        self._metrics = {'cycles': 0, 'failures': 0, 'last_error': None, 'last': None, 'last_run_utc': None}

    def status(self):
        return {'version': measurement.VERSION, 'population': intake.POPULATION, 'enabled': _enabled(),
                'running': bool(self._task and not self._task.done()), 'cache_only': True,
                'max_coins_per_pass': JOB_LIMIT, 'poll_seconds': POLL_SECONDS,
                'formula_evaluation_enabled': False, **self._metrics}

    async def start(self):
        if self._task and not self._task.done():
            return True
        if not _enabled() or not intake._database_url():
            return False
        if not await asyncio.to_thread(schema_ready):
            self._metrics['last_error'] = 'SCHEMA_UNAVAILABLE'
            return False
        self._task = asyncio.create_task(self._loop(), name='watch-scan-measurements')
        return True

    async def _loop(self):
        while True:
            try:
                result = await asyncio.to_thread(run_once)
                self._metrics.update(cycles=self._metrics['cycles']+1, last=result, last_error=None,
                                     last_run_utc=datetime.now(timezone.utc).isoformat())
                if result['processed'] or result.get('enqueued'):
                    print('[watch-scan-measurement] ' + str(result), flush=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._metrics.update(failures=self._metrics['failures']+1, last_error=type(exc).__name__)
                print('[watch-scan-measurement] failed: ' + type(exc).__name__, flush=True)
            await asyncio.sleep(POLL_SECONDS)

    async def stop(self):
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None


WORKER = WatchScanMeasurementWorker()
