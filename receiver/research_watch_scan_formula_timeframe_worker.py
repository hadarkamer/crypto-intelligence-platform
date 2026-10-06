"""Bounded selected-timeframe research, independent of coin-level formula rows.

Only immutable captured source observations are read. Existing coin measurements
supply outcomes through separate views; this worker never fetches prices, edits
old formula results, changes alert qualification, or sends a message.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
import os

import research_watch_scan_formula_timeframe as formula
import research_watch_scan_intake as intake

LOCK_ID = 54260914054601
SCAN_PAGE = 32
JOB_LIMIT = 32
POLL_SECONDS = 30


def _json(value):
    return json.dumps(value, default=str, allow_nan=False, ensure_ascii=False, separators=(',', ':'))


def register_catalog(conn):
    records = formula.catalog_records()
    existing = {row['candidate_key']: row for row in conn.execute(
        'SELECT * FROM research_watch_scan_tf_formula_catalog WHERE evaluation_version=%s',
        (formula.VERSION,)).fetchall()}
    if set(existing)-{row['candidate_key'] for row in records}:
        raise ValueError('Timeframe catalog removed a frozen candidate')
    for record in records:
        old = existing.get(record['candidate_key'])
        if old:
            if (old['definition'], old['definition_sha256'], old['orientation'], old['supported'],
                    old['unsupported_features'], old['feature_version']) != (
                    record['definition'], record['definition_sha256'], record['orientation'], True,
                    [], formula.FEATURE_VERSION):
                raise ValueError('Timeframe catalog changed without a new version')
            continue
        conn.execute('''INSERT INTO research_watch_scan_tf_formula_catalog
            (evaluation_version,candidate_key,feature_version,definition,definition_sha256,
             orientation,supported,unsupported_features) VALUES (%s,%s,%s,%s::jsonb,%s,%s,true,'[]'::jsonb)''',
            (formula.VERSION, record['candidate_key'], formula.FEATURE_VERSION,
             _json(record['definition']), record['definition_sha256'], record['orientation']))
    return len(records)


def enqueue(conn, *, now):
    """Cyclic intake scan and recent reserve recover late admissions and IDs."""
    conn.execute('INSERT INTO research_watch_scan_tf_formula_state(evaluation_version) '
                 'VALUES (%s) ON CONFLICT DO NOTHING', (formula.VERSION,))
    state = conn.execute('SELECT * FROM research_watch_scan_tf_formula_state '
        'WHERE evaluation_version=%s FOR UPDATE', (formula.VERSION,)).fetchone()
    cursor, high = state['scan_cursor'], state['scan_high_water']
    if cursor >= high:
        cursor = 0
        high = conn.execute('SELECT COALESCE(MAX(snapshot_set_id),0) AS id '
            'FROM research_watch_scan_intakes WHERE consumer_version=%s', (intake.VERSION,)).fetchone()['id']
    page = conn.execute('SELECT snapshot_set_id FROM research_watch_scan_intakes '
        'WHERE consumer_version=%s AND snapshot_set_id>%s AND snapshot_set_id<=%s '
        'ORDER BY snapshot_set_id LIMIT %s', (intake.VERSION, cursor, high, SCAN_PAGE)).fetchall()
    recent = conn.execute('SELECT snapshot_set_id FROM research_watch_scan_intakes '
        'WHERE consumer_version=%s ORDER BY snapshot_set_id DESC LIMIT 2', (intake.VERSION,)).fetchall()
    ids = sorted({row['snapshot_set_id'] for row in page+recent})
    sources = conn.execute('SELECT snapshot_set_id,usable_from_utc FROM research_watch_scan_intakes '
        "WHERE consumer_version=%s AND intake_status='ACCEPTED' AND snapshot_set_id=ANY(%s::bigint[])",
        (intake.VERSION, ids)).fetchall()
    written = 0
    for source in sources:
        for symbol in intake.capture.SYMBOLS:
            result = conn.execute('''INSERT INTO research_watch_scan_tf_formula_samples
                (evaluation_version,consumer_version,snapshot_set_id,symbol,usable_from_utc,next_attempt_at_utc)
                VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING''',
                (formula.VERSION, intake.VERSION, source['snapshot_set_id'], symbol, source['usable_from_utc'], now))
            written += result.rowcount
    cursor = page[-1]['snapshot_set_id'] if page else high
    finished = cursor >= high or len(page) < SCAN_PAGE
    if finished:
        cursor = high
    conn.execute('''UPDATE research_watch_scan_tf_formula_state SET scan_cursor=%s,
        scan_high_water=%s,completed_laps=completed_laps+%s,updated_at_utc=%s WHERE evaluation_version=%s''',
        (cursor, high, int(finished), now, formula.VERSION))
    return written


def write_result(conn, job, result, *, now):
    if (result['version'], result['feature_version'], result['consumer_version'],
            result['snapshot_set_id'], result['symbol'], intake._utc(result['usable_from_utc']),
            result['bundle_sha256'], result['parent_payload_sha256']) != (
            formula.VERSION, formula.FEATURE_VERSION, intake.VERSION, job['snapshot_set_id'], job['symbol'],
            job['usable_from_utc'], job['bundle_sha256'], job['parent_payload_sha256']):
        raise ValueError('Timeframe formula source identity mismatch')
    if formula.digest({key: value for key, value in result.items() if key != 'feature_sha256'}) != result['feature_sha256']:
        raise ValueError('Timeframe formula payload hash mismatch')
    records = {row['candidate_key']: row for row in formula.catalog_records()}
    expected = {(key, tf, direction) for key in records for tf in formula.TIMEFRAMES for direction in formula.DIRECTIONS}
    evaluations = result['evaluations']
    if len(evaluations) != len(expected) or {(e['candidate_key'], e['timeframe'], e['base_direction'])
            for e in evaluations} != expected:
        raise ValueError('Timeframe evaluation grid is incomplete or duplicated')
    for evaluation in evaluations:
        direction = evaluation['base_direction']
        if records[evaluation['candidate_key']]['orientation'] == 'INVERSE':
            direction = 'SHORT' if direction == 'LONG' else 'LONG'
        if evaluation['analysis_direction'] != direction:
            raise ValueError('Timeframe inverse mapping mismatch')
        selection, status = evaluation['selection_status'], evaluation['match_status']
        allowed = {'SELECTED': {'MATCH', 'NO_MATCH', 'UNKNOWN'},
                   'NOT_SELECTED': {'NOT_APPLICABLE'}, 'UNKNOWN': {'UNKNOWN'}}
        if selection not in allowed or status not in allowed[selection]:
            raise ValueError('Timeframe selection and decision disagree')
    conn.execute('''UPDATE research_watch_scan_tf_formula_samples SET status='READY',payload=%s::jsonb,
        attempts=attempts+1,next_attempt_at_utc=NULL,last_error=NULL,updated_at_utc=%s
        WHERE evaluation_version=%s AND consumer_version=%s AND snapshot_set_id=%s AND symbol=%s''',
        (_json(result), now, formula.VERSION, intake.VERSION, job['snapshot_set_id'], job['symbol']))


def activate_if_caught_up(conn, *, now):
    """Publish only complete source coverage; never change the coin adapter."""
    updated = conn.execute('''UPDATE research_watch_scan_tf_formula_runtime
        SET active_evaluation_version=%s,activated_at_utc=%s WHERE singleton=true
        AND active_evaluation_version IS NULL AND NOT EXISTS (
            SELECT 1 FROM research_watch_scan_intakes i
            CROSS JOIN unnest(%s::text[]) coin(symbol)
            LEFT JOIN research_watch_scan_tf_formula_samples f ON f.evaluation_version=%s
                AND f.consumer_version=i.consumer_version AND f.snapshot_set_id=i.snapshot_set_id
                AND f.symbol=coin.symbol
            WHERE i.consumer_version=%s AND i.intake_status='ACCEPTED'
                AND f.status IS DISTINCT FROM 'READY'
        ) RETURNING active_evaluation_version''',
        (formula.VERSION, now, list(intake.capture.SYMBOLS), formula.VERSION, intake.VERSION)).fetchone()
    active = updated or conn.execute('SELECT active_evaluation_version '
        'FROM research_watch_scan_tf_formula_runtime WHERE singleton=true').fetchone()
    return {'activated': bool(updated), 'active_evaluation_version': active['active_evaluation_version']}


def process_page(conn, *, now=None, limit=JOB_LIMIT):
    if type(limit) is not int or not 1 <= limit <= JOB_LIMIT:
        raise ValueError('Invalid timeframe formula job budget')
    now = intake._utc(now or datetime.now(timezone.utc))
    if not conn.execute('SELECT pg_try_advisory_xact_lock(%s) AS held', (LOCK_ID,)).fetchone()['held']:
        return {'locked': True, 'processed': 0}
    counts = {'locked': False, 'catalog': register_catalog(conn), 'enqueued': enqueue(conn, now=now),
              'processed': 0, 'ready': 0, 'errors': 0}
    jobs = conn.execute('''SELECT f.*,i.bundle_sha256,i.parent_payload_sha256
        FROM research_watch_scan_tf_formula_samples f JOIN research_watch_scan_intakes i
        USING(consumer_version,snapshot_set_id)
        WHERE f.evaluation_version=%s AND f.next_attempt_at_utc<=%s
        ORDER BY f.next_attempt_at_utc,f.snapshot_set_id,f.symbol LIMIT %s
        FOR UPDATE OF f SKIP LOCKED''', (formula.VERSION, now, limit)).fetchall()
    ids = sorted({job['snapshot_set_id'] for job in jobs})
    observations = {(row['snapshot_set_id'], row['symbol']): row for row in conn.execute('''
        SELECT consumer_version,population_version,snapshot_set_id,symbol,bundle_sha256,
            parent_payload_sha256,watch_scan_id,usable_from_utc,observed_at_utc,source_available_at_utc,
            source_created_at_utc,source_version,intake_status,capture_phase,models,sources,
            source_time_errors,maxpain_slots FROM research_watch_scan_observations
        WHERE consumer_version=%s AND snapshot_set_id=ANY(%s::bigint[])''', (intake.VERSION, ids)).fetchall()}
    for job in jobs:
        try:
            with conn.transaction():
                observation = observations[(job['snapshot_set_id'], job['symbol'])]
                write_result(conn, job, formula.evaluate_coin(observation), now=now)
            counts['ready'] += 1
        except Exception as exc:
            conn.execute('''UPDATE research_watch_scan_tf_formula_samples SET status='ERROR',
                attempts=attempts+1,next_attempt_at_utc=%s,last_error=%s,updated_at_utc=%s
                WHERE evaluation_version=%s AND consumer_version=%s AND snapshot_set_id=%s AND symbol=%s''',
                (now+timedelta(minutes=5), type(exc).__name__, now,
                 formula.VERSION, intake.VERSION, job['snapshot_set_id'], job['symbol']))
            counts['errors'] += 1
        counts['processed'] += 1
    counts.update(activate_if_caught_up(conn, now=now))
    return counts


def _enabled():
    return os.getenv('RESEARCH_WATCH_SCAN_FORMULA_ENABLED',
        os.getenv('RESEARCH_OUTCOME_ENRICHMENT_ENABLED', '')).strip().lower() in intake._TRUE


def schema_ready():
    with intake._connect() as conn:
        return all(conn.execute('SELECT to_regclass(%s) AS relation', ('public.'+name,)).fetchone()['relation']
            for name in ('research_watch_scan_tf_formula_catalog','research_watch_scan_tf_formula_state',
                         'research_watch_scan_tf_formula_samples','research_watch_scan_tf_formula_runtime',
                         'research_watch_scan_tf_formula_comparisons','research_watch_scan_formula_coverage'))


def run_once():
    with intake._connect() as conn:
        return process_page(conn)


class WatchScanTimeframeFormulaWorker:
    def __init__(self):
        self._task = None
        self._metrics = {'cycles': 0, 'failures': 0, 'last_error': None, 'last': None, 'last_run_utc': None}

    def status(self):
        return {'version': formula.VERSION, 'feature_version': formula.FEATURE_VERSION,
                'population': intake.POPULATION, 'unit': 'ACTUAL_SELECTED_MAXPAIN_TIMEFRAME',
                'enabled': _enabled(), 'running': bool(self._task and not self._task.done()),
                'max_coins_per_pass': JOB_LIMIT, 'poll_seconds': POLL_SECONDS,
                'supported_timeframe_candidates': formula.SUPPORTED_COUNT,
                'source_timeframes': list(formula.TIMEFRAMES),
                'active_evaluation_version': (self._metrics.get('last') or {}).get('active_evaluation_version'),
                'discovery_enabled': False, 'promotion_enabled': False, **self._metrics}

    async def start(self):
        if self._task and not self._task.done():
            return True
        if not _enabled() or not intake._database_url():
            return False
        if not await asyncio.to_thread(schema_ready):
            self._metrics['last_error'] = 'SCHEMA_UNAVAILABLE'
            return False
        self._task = asyncio.create_task(self._loop(), name='watch-scan-timeframe-formulas')
        return True

    async def _loop(self):
        while True:
            try:
                result = await asyncio.to_thread(run_once)
                self._metrics.update(cycles=self._metrics['cycles']+1, last=result, last_error=None,
                                     last_run_utc=datetime.now(timezone.utc).isoformat())
                if result['processed'] or result.get('enqueued'):
                    print('[watch-scan-timeframe-formulas] '+str(result), flush=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._metrics.update(failures=self._metrics['failures']+1, last_error=type(exc).__name__)
                print('[watch-scan-timeframe-formulas] failed: '+type(exc).__name__, flush=True)
            await asyncio.sleep(POLL_SECONDS)

    async def stop(self):
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None


WORKER = WatchScanTimeframeFormulaWorker()
