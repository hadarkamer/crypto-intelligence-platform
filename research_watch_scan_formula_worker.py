"""Bounded evaluation of frozen Watch features, separate from alert research.

No outcome reads, provider requests, discovery, promotions, or delivery. Closed
prior BTC bars supply causal context; outcomes join only after feature decisions.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
import os

import research_watch_scan_formula_asset_context as formula
import research_watch_scan_intake as intake
import research_price_archive as prices

LOCK_ID = 48260913201211
SCAN_PAGE = 32
JOB_LIMIT = 16
POLL_SECONDS = 30


def _json(value):
    return json.dumps(value, default=str, allow_nan=False, ensure_ascii=False, separators=(',', ':'))


def register_catalog(conn):
    """Freeze entire original definitions, including NORMAL/INVERSE identity."""
    records = formula.catalog_records()
    existing = {r['candidate_key']: r for r in conn.execute(
        'SELECT * FROM research_watch_scan_formula_catalog WHERE evaluation_version=%s',
        (formula.VERSION,)).fetchall()}
    if set(existing) - {r['candidate_key'] for r in records}:
        raise ValueError('Watch formula catalog removed candidates without a new version')
    for record in records:
        key = record['candidate_key']
        old = existing.get(key)
        if old:
            if (old['definition_sha256'], old['orientation'], old['supported'], old['unsupported_features'],
                    old['feature_version']) != (record['definition_sha256'], record['orientation'],
                    record['supported'], record['unsupported_features'], formula.FEATURE_VERSION):
                raise ValueError('Watch formula catalog changed without a new version')
            continue
        conn.execute('''INSERT INTO research_watch_scan_formula_catalog
            (evaluation_version,candidate_key,feature_version,definition,definition_sha256,
             orientation,supported,unsupported_features) VALUES (%s,%s,%s,%s::jsonb,%s,%s,%s,%s::jsonb)''',
            (formula.VERSION, key, formula.FEATURE_VERSION, _json(record['definition']),
             record['definition_sha256'], record['orientation'], record['supported'],
             _json(record['unsupported_features'])))
    return len(records)


def enqueue(conn, *, now):
    """Finite cyclic intake scan plus recent reserve also recovers late IDs."""
    conn.execute('INSERT INTO research_watch_scan_formula_state(evaluation_version) '
                 'VALUES (%s) ON CONFLICT DO NOTHING', (formula.VERSION,))
    state = conn.execute('SELECT * FROM research_watch_scan_formula_state '
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
    ids = sorted({r['snapshot_set_id'] for r in page + recent})
    rows = conn.execute('SELECT snapshot_set_id,usable_from_utc FROM research_watch_scan_intakes '
        "WHERE consumer_version=%s AND intake_status='ACCEPTED' AND snapshot_set_id=ANY(%s::bigint[])",
        (intake.VERSION, ids)).fetchall()
    written = 0
    for row in rows:
        for symbol in intake.capture.SYMBOLS:
            result = conn.execute('''INSERT INTO research_watch_scan_formula_samples
                (evaluation_version,consumer_version,snapshot_set_id,symbol,usable_from_utc,next_attempt_at_utc)
                VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING''',
                (formula.VERSION, intake.VERSION, row['snapshot_set_id'], symbol, row['usable_from_utc'], now))
            written += result.rowcount
    cursor = page[-1]['snapshot_set_id'] if page else high
    finished = cursor >= high or len(page) < SCAN_PAGE
    if finished:
        cursor = high
    conn.execute('''UPDATE research_watch_scan_formula_state SET scan_cursor=%s,
        scan_high_water=%s,completed_laps=completed_laps+%s,updated_at_utc=%s WHERE evaluation_version=%s''',
        (cursor, high, int(finished), now, formula.VERSION))
    return written


def write_result(conn, job, result, *, now):
    if (result['version'], result['feature_version'], result['consumer_version'],
        result['snapshot_set_id'], result['symbol'], intake._utc(result['usable_from_utc']),
        result['bundle_sha256'], result['parent_payload_sha256']) != (
        formula.VERSION, formula.FEATURE_VERSION, intake.VERSION, job['snapshot_set_id'], job['symbol'],
        job['usable_from_utc'], job['bundle_sha256'], job['parent_payload_sha256']):
        raise ValueError('Watch formula source identity mismatch')
    expected = {(r['candidate_key'], d) for r in formula.catalog_records() if r['supported']
                for d in ('LONG', 'SHORT')}
    evaluations = result['evaluations']
    if len(evaluations) != len(expected) or {(e['candidate_key'], e['base_direction']) for e in evaluations} != expected:
        raise ValueError('Watch formula evaluation coverage mismatch')
    conn.execute('''UPDATE research_watch_scan_formula_samples SET status='READY',payload=%s::jsonb,
        attempts=attempts+1,next_attempt_at_utc=NULL,last_error=NULL,updated_at_utc=%s
        WHERE evaluation_version=%s AND consumer_version=%s AND snapshot_set_id=%s AND symbol=%s''',
        (_json(result), now, formula.VERSION, intake.VERSION, job['snapshot_set_id'], job['symbol']))


def activate_if_caught_up(conn, *, now):
    """Switch reports atomically only after complete accepted-source coverage.

    Activation has one explicit predecessor. A retiring process cannot move the
    active version backwards during a later rolling deployment. The old frozen
    decisions remain available through the by-version audit views.
    """
    row = conn.execute('SELECT active_evaluation_version FROM research_watch_scan_formula_runtime '
                       'WHERE singleton=true').fetchone()
    previous = getattr(formula, 'PREVIOUS_VERSION', None)
    active = row['active_evaluation_version']
    if active == formula.VERSION or previous is None or active != previous:
        return {'activated': False, 'active_evaluation_version': active}
    # The UPDATE's one statement snapshot defines the coverage cutoff. New
    # observations committed after it are ordinary next-pass work.
    updated = conn.execute('''UPDATE research_watch_scan_formula_runtime
        SET active_evaluation_version=%s,activated_at_utc=%s WHERE singleton=true
        AND active_evaluation_version=%s AND NOT EXISTS (
            SELECT 1 FROM research_watch_scan_intakes i
            CROSS JOIN unnest(%s::text[]) coin(symbol)
            LEFT JOIN research_watch_scan_formula_samples s ON s.evaluation_version=%s
                AND s.consumer_version=i.consumer_version AND s.snapshot_set_id=i.snapshot_set_id
                AND s.symbol=coin.symbol
            WHERE i.consumer_version=%s AND i.intake_status='ACCEPTED'
                AND s.status IS DISTINCT FROM 'READY'
        ) RETURNING active_evaluation_version''',
        (formula.VERSION, now, previous, list(intake.capture.SYMBOLS), formula.VERSION, intake.VERSION)).fetchone()
    return {'activated': bool(updated),
            'active_evaluation_version': updated['active_evaluation_version'] if updated else active}


def process_page(conn, *, now=None, limit=JOB_LIMIT):
    if type(limit) is not int or not 1 <= limit <= JOB_LIMIT:
        raise ValueError('Invalid Watch formula job budget')
    now = intake._utc(now or datetime.now(timezone.utc))
    if not conn.execute('SELECT pg_try_advisory_xact_lock(%s) AS held', (LOCK_ID,)).fetchone()['held']:
        return {'locked': True, 'processed': 0}
    counts = {'locked': False, 'catalog': register_catalog(conn), 'enqueued': enqueue(conn, now=now),
              'processed': 0, 'ready': 0, 'errors': 0}
    jobs = conn.execute('''SELECT f.*,i.bundle_sha256,i.parent_payload_sha256
        FROM research_watch_scan_formula_samples f JOIN research_watch_scan_intakes i
        USING(consumer_version,snapshot_set_id)
        WHERE f.evaluation_version=%s AND f.next_attempt_at_utc<=%s
        ORDER BY f.next_attempt_at_utc,f.snapshot_set_id,f.symbol LIMIT %s
        FOR UPDATE OF f SKIP LOCKED''', (formula.VERSION, now, limit)).fetchall()
    # Load at most one original bundle per selected scan, then retain only
    # those coin observations used in this bounded pass.
    ids = sorted({j['snapshot_set_id'] for j in jobs})
    observations = {(r['snapshot_set_id'], r['symbol']): r for r in conn.execute('''
        SELECT consumer_version,population_version,snapshot_set_id,symbol,bundle_sha256,
            parent_payload_sha256,usable_from_utc,observed_at_utc,source_available_at_utc,
            source_created_at_utc,source_version,intake_status,capture_phase,models,sources,
            source_time_errors,maxpain_slots FROM research_watch_scan_observations
        WHERE consumer_version=%s AND snapshot_set_id=ANY(%s::bigint[])''', (intake.VERSION, ids)).fetchall()}
    # The first READY coin freezes BTC context for the entire scan, including
    # retries and split passes. Later cache enrichment cannot make siblings
    # disagree. New contexts use one bounded BTC-only read per selected scan.
    contexts, context_errors = {}, {}
    if getattr(formula, 'CONTEXT_REQUIRED', False):
        context_versions = [formula.VERSION, *getattr(formula, 'BTC_CONTEXT_PREDECESSORS', ())]
        for snapshot_id in ids:
            try:
                with conn.transaction():
                    source = observations[(snapshot_id, 'BTC')]
                    frozen = conn.execute('''SELECT payload->'btc_context_provenance' AS context
                        FROM research_watch_scan_formula_samples WHERE evaluation_version=ANY(%s::text[])
                        AND consumer_version=%s AND snapshot_set_id=%s AND status='READY'
                        ORDER BY array_position(%s::text[],evaluation_version),symbol LIMIT 1''',
                        (context_versions, intake.VERSION, snapshot_id, context_versions)).fetchone()
                    if frozen:
                        if frozen['context'] is None:
                            raise ValueError('MISSING_FROZEN_BTC_CONTEXT')
                        contexts[snapshot_id] = frozen['context']
                    else:
                        boundary = intake._utc(source['usable_from_utc']).replace(second=0, microsecond=0)
                        path = prices.read_path(prices.BINANCE_SPOT, 'BTC',
                            boundary-timedelta(minutes=240), boundary-timedelta(milliseconds=1), connection=conn)
                        contexts[snapshot_id] = formula.build_btc_context(source, path, computed_at=now)
            except Exception as exc:
                # Savepoints keep another scan usable; do not repeat a failed
                # source query eight times or persist exception contents.
                context_errors[snapshot_id] = exc
    for job in jobs:
        try:
            with conn.transaction():
                observation = observations[(job['snapshot_set_id'], job['symbol'])]
                if job['snapshot_set_id'] in context_errors:
                    raise context_errors[job['snapshot_set_id']]
                if getattr(formula, 'ASSET_CONTEXT_REQUIRED', False):
                    boundary = intake._utc(observation['usable_from_utc']).replace(second=0, microsecond=0)
                    # Original predicates require Spot. HYPE outcome PERP is
                    # never substituted for an unapproved own-Spot lookback.
                    path = None if job['symbol'] == 'HYPE' else prices.read_path(
                        prices.BINANCE_SPOT, job['symbol'], boundary-timedelta(minutes=1440),
                        boundary-timedelta(milliseconds=1), connection=conn)
                    context = contexts[job['snapshot_set_id']]
                    own = formula.build_asset_context(observation, path,
                        btc_context=context, computed_at=now)
                    result = formula.evaluate_coin(observation, btc_context=context, asset_context=own)
                elif getattr(formula, 'CONTEXT_REQUIRED', False):
                    result = formula.evaluate_coin(observation, btc_context=contexts[job['snapshot_set_id']])
                else:
                    result = formula.evaluate_coin(observation)
                write_result(conn, job, result, now=now)
            counts['ready'] += 1
        except Exception as exc:
            conn.execute('''UPDATE research_watch_scan_formula_samples SET status='ERROR',
                attempts=attempts+1,next_attempt_at_utc=%s,last_error=%s,updated_at_utc=%s
                WHERE evaluation_version=%s AND consumer_version=%s AND snapshot_set_id=%s AND symbol=%s''',
                (now + timedelta(minutes=5), type(exc).__name__, now,
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
        return all(conn.execute('SELECT to_regclass(%s) AS relation', ('public.' + name,)).fetchone()['relation']
            for name in ('research_watch_scan_formula_catalog','research_watch_scan_formula_state',
                         'research_watch_scan_formula_samples','research_watch_scan_formula_comparisons',
                         'research_watch_scan_formula_runtime','research_price_archive_bars'))


def run_once():
    with intake._connect() as conn:
        return process_page(conn)


class WatchScanFormulaWorker:
    def __init__(self):
        self._task = None
        self._metrics = {'cycles': 0, 'failures': 0, 'last_error': None, 'last': None, 'last_run_utc': None}

    def status(self):
        return {'version': formula.VERSION, 'feature_version': formula.FEATURE_VERSION,
                'population': intake.POPULATION, 'enabled': _enabled(),
                'running': bool(self._task and not self._task.done()),
                'max_coins_per_pass': JOB_LIMIT, 'poll_seconds': POLL_SECONDS,
                'catalog_candidates': getattr(formula, 'CATALOG_SIZE', 298),
                'supported_candidates': getattr(formula, 'SUPPORTED_COUNT', 34),
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
        self._task = asyncio.create_task(self._loop(), name='watch-scan-formulas')
        return True

    async def _loop(self):
        while True:
            try:
                result = await asyncio.to_thread(run_once)
                self._metrics.update(cycles=self._metrics['cycles']+1, last=result, last_error=None,
                                     last_run_utc=datetime.now(timezone.utc).isoformat())
                if result['processed'] or result.get('enqueued'):
                    print('[watch-scan-formulas] ' + str(result), flush=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._metrics.update(failures=self._metrics['failures']+1, last_error=type(exc).__name__)
                print('[watch-scan-formulas] failed: ' + type(exc).__name__, flush=True)
            await asyncio.sleep(POLL_SECONDS)

    async def stop(self):
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None


WORKER = WatchScanFormulaWorker()
