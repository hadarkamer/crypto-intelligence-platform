"""Bounded real-loader BTC report companion for an owned native test database.

API: prepare(conn) creates and seeds its own schema and returns a small JSON
manifest. The caller has already verified the owned-host marker and owns all
commits, rollbacks, connections and process lifetime. load(conn, manifest,
phase=None) sets that schema, calls the actual load_job_source, invokes
phase('report_loaded') while its compact source is still alive, then computes
the actual worker digest and returns scalar metadata only. full_oracle and
checkpoint are preparation/postmeasurement checks, never measured-loop work.

No provider, report worker loop, price-path calculation, outbox or publication
is run. The fixed 2KiB snapshots are controlled-width synthetic objects, not a
claim about production snapshot shapes or report cardinalities.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
from pathlib import Path
import re
from uuid import uuid4

VERSION = 'ordered-concurrent-report-fixture-v1'
ROOT = Path(__file__).resolve().parents[1]
START = datetime(2026, 9, 8, tzinfo=timezone.utc)
OBSERVED = START + timedelta(hours=8)
PARENTS = 8
EVENTS_PER_PARENT = 250
EVENTS = PARENTS * EVENTS_PER_PARENT
SNAPSHOT_BYTES = 2048
MIGRATIONS = ('001_research_archive_v1.sql', '021_btc_parent_movements_v1.sql')
SOURCE_FILES = (
    'tools/ordered_concurrent_report_fixture.py', 'research_btc_wave_report_worker.py',
    'research_btc_wave_endpoint_report.py', 'research_btc_parent_movement.py',
    'research_outcome_worker.py', 'research_price_archive.py',
    'binance_futures_mark_price_path.py', 'research_feature_matrix.py',
    'research_no_dwell_outcome.py', 'research_ordered_outcome_recovery_store.py',
    'research_session_width.py', 'research_sheet_outbox.py',
    'research_outcome_publication.py', 'canonical_price_path.py',
    'binance_spot_price_path.py', 'hyperliquid_spot_price_path.py', 'google_sheets_sync.py',
    'research_ordered_first_touch.py', 'research_common_window_metrics.py',
    'research_common_window_metrics_store.py',
) + tuple('migrations/'+filename for filename in MIGRATIONS)
TABLE_KEYS = {'research_events':'event_id',
              'research_event_btc_movements':'event_id',
              'research_btc_parent_movements':'btc_parent_movement_id',
              'research_btc_price_bars':'open_time_utc',
              'research_alert_outcomes':'event_id,horizon_minutes'}
COUNTS = {'research_events':EVENTS, 'research_event_btc_movements':EVENTS,
          'research_btc_parent_movements':PARENTS, 'research_btc_price_bars':400,
          'research_alert_outcomes':0}


def _worker():
    import research_btc_wave_report_worker as worker
    return worker


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _schema(conn, manifest):
    from psycopg import sql
    name = manifest.get('schema')
    if (manifest.get('version') != VERSION or type(name) is not str
            or not re.fullmatch(r'ordered_report_replay_[0-9a-f]{24}', name)):
        raise ValueError('Explicit disposable report schema required')
    conn.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(name)))


def _wave(index):
    worker = _worker()
    start = START + timedelta(hours=index)
    end = start + timedelta(hours=1) if index < PARENTS-1 else None
    return {'btc_parent_movement_id':f'synthetic-report-parent-{index:02d}',
            'episode_policy_version':worker.report.POLICY_VERSION,
            'start_time_utc':start, 'end_time_utc':end,
            'evidence_eligible':True, 'boundary_reason':'CAUSAL_CLOSE_REVERSAL',
            'observed_through_utc':(end or OBSERVED)-timedelta(milliseconds=1),
            'closing_boundary_verified':end is not None}


def _snapshot(event_id):
    worker = _worker()
    value = {'price_source':'binance_spot', 'price_exchange':'binance',
             'price_market':'spot', 'price_pair':'BTCUSDT', 'price_instrument':'BTCUSDT',
             'synthetic_fixture':{'event_id':event_id,
                 'features':{'flow_score':75,'oi_score':65,'cvd_score':70}, 'padding':''}}
    remaining = SNAPSHOT_BYTES-len(worker.canonical(value).encode('ascii'))
    if remaining < 0: raise AssertionError('Fixed snapshot budget exceeded')
    value['synthetic_fixture']['padding'] = 'x'*remaining
    if len(worker.canonical(value).encode('ascii')) != SNAPSHOT_BYTES:
        raise AssertionError('Snapshot canonical byte count changed')
    return value


def _event(parent, ordinal):
    event_id = parent*EVENTS_PER_PARENT+ordinal+1
    wave = _wave(parent)
    stamp = wave['start_time_utc']+timedelta(minutes=1, seconds=12*ordinal)
    closed = stamp.replace(second=0, microsecond=0)-timedelta(milliseconds=1)
    direction = 'LONG' if ordinal % 2 == 0 else 'SHORT'
    return {'event_id':event_id, 'event_kind':'ALERT', 'event_type':'MAX_PAIN_ALERT',
            'alert_time_utc':stamp, 'symbol':'BTC', 'direction':direction,
            'score':75.0, 'current_price':100.0, 'delivery_status':'DELIVERED',
            'engine_snapshot':_snapshot(event_id), 'source_side':'SUPPORT', 'timeframe':'1h',
            'target_price':101.0 if direction == 'LONG' else 99.0,
            'initial_target_distance_pct':1.0,
            'event_fingerprint':_sha(f'{VERSION}:event:{event_id}'.encode()),
            'strategy_version':VERSION, 'code_version':VERSION,
            'membership_status':'LIVE', 'membership_policy_version':wave['episode_policy_version'],
            'membership_parent_id':wave['btc_parent_movement_id'],
            'decision_time_utc':stamp, 'btc_observed_close_utc':closed,
            'btc_parent_movement_id':wave['btc_parent_movement_id']}


def _expected_source():
    # Independent of database reads and the source loader. The real pure
    # compactor is allowed here; selection, fields and ordering come from seeds.
    worker = _worker()
    return {'waves':[_wave(index) for index in range(PARENTS)],
            'events':[worker._compact_source_event(_event(parent, ordinal))
                      for parent in range(PARENTS) for ordinal in range(EVENTS_PER_PARENT)]}


def _result(source):
    if (len(source['waves']) != PARENTS or len(source['events']) != EVENTS
            or any(event.get('engine_snapshot_compacted') is not True
                   or event.get('symbol') != 'BTC' for event in source['events'])):
        raise AssertionError('Report fixture population or compaction changed')
    return {'source_sha256':_worker().digest(source), 'waves':PARENTS,
            'events':EVENTS, 'snapshot_compacted':True}


def load(conn, manifest, phase=None):
    """Real source SQL/JSONB decoding and digest; no retained payload return."""
    _schema(conn, manifest)
    source = _worker().load_job_source(conn, OBSERVED)
    if phase is not None:
        phase('report_loaded')
    result = _result(source)
    if result != manifest['expected']:
        raise AssertionError('Measured report result differs from independent oracle')
    return result


def full_oracle(conn, manifest):
    """Compare full canonical source bytes outside all measured rounds."""
    _schema(conn, manifest)
    worker = _worker()
    actual = worker.load_job_source(conn, OBSERVED)
    expected = _expected_source()
    actual_json, expected_json = worker.canonical(actual), worker.canonical(expected)
    if actual_json != expected_json:
        raise AssertionError('Actual report source differs from independently seeded full source')
    result = _result(actual)
    descriptor = {'source_sha256':_sha(expected_json.encode('ascii')),
                  'canonical_bytes':len(expected_json), 'waves':PARENTS, 'events':EVENTS,
                  'reference':'deterministic_seed_records_and_pure_compactor'}
    if result != manifest['expected'] or descriptor != manifest['expected_source']:
        raise AssertionError('Report expected-source manifest changed')
    return {'result':'PASS', 'full_canonical_source_equal':True,
            'expected_source':descriptor, 'loaded_result':result}


def checkpoint(conn, manifest):
    """Exact table fingerprints in bounded batches; never called per round."""
    from psycopg import sql
    _schema(conn, manifest)
    worker = _worker()
    identity = dict(conn.execute('SELECT current_schema() AS schema,current_database() AS database').fetchone())
    if identity != manifest['identity']:
        raise AssertionError('Report fixture database/schema identity changed')
    tables = {}
    for table, keys in TABLE_KEYS.items():
        digest, count = hashlib.sha256(), 0
        query = sql.SQL('SELECT to_jsonb(t) AS row FROM {} AS t ORDER BY {}').format(
            sql.Identifier(table), sql.SQL(',').join(sql.Identifier(key) for key in keys.split(',')))
        with conn.execute(query) as cursor:
            while rows := cursor.fetchmany(128):
                for row in rows:
                    digest.update(worker.canonical(row['row']).encode('ascii'))
                    digest.update(b'\n')
                    count += 1
        if count != COUNTS[table]:
            raise AssertionError('Report fixed source cardinality changed')
        tables[table] = {'rows':count, 'sha256':digest.hexdigest()}
    return {'identity':identity, 'tables':tables, 'fixed_source_cardinality':True}


def prepare(conn):
    """Create a separate synthetic schema; caller commits before measurement."""
    from psycopg import sql
    worker = _worker()
    name = 'ordered_report_replay_'+uuid4().hex[:24]
    conn.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(name)))
    manifest = {'version':VERSION, 'schema':name}
    _schema(conn, manifest)
    for filename in MIGRATIONS:
        conn.execute((ROOT/'migrations'/filename).read_text(), prepare=False)
    for table in TABLE_KEYS:
        if conn.execute(sql.SQL('SELECT count(*) AS n FROM {}').format(sql.Identifier(table))).fetchone()['n']:
            raise ValueError('Refusing populated report fixture schema')
    for parent in range(PARENTS):
        wave = _wave(parent)
        conn.execute('''INSERT INTO research_btc_parent_movements
            (btc_parent_movement_id,episode_policy_version,start_time_utc,end_time_utc,
             confirmed_at_utc,direction,evidence_eligible,boundary_reason,observed_through_utc,
             price_source,state_json,created_at_utc,updated_at_utc)
            VALUES(%s,%s,%s,%s,%s,'UP',TRUE,'CAUSAL_CLOSE_REVERSAL',%s,
                   'BINANCE_SPOT_BTCUSDT_1M','{"synthetic":true}'::jsonb,%s,%s)''',
            (wave['btc_parent_movement_id'],wave['episode_policy_version'],wave['start_time_utc'],
             wave['end_time_utc'],wave['start_time_utc'],wave['observed_through_utc'],START,START))
        for ordinal in range(EVENTS_PER_PARENT):
            event = _event(parent, ordinal)
            closed = event['btc_observed_close_utc']
            conn.execute('''INSERT INTO research_btc_price_bars
                (open_time_utc,close_time_utc,open,high,low,close,created_at_utc)
                VALUES(%s,%s,100,100.1,99.9,100,%s) ON CONFLICT DO NOTHING''',
                (closed+timedelta(milliseconds=1)-timedelta(minutes=1),closed,START))
            conn.execute('''INSERT INTO research_events
                (event_id,schema_version,event_kind,event_type,alert_time_utc,symbol,direction,
                 source_side,timeframe,score,current_price,target_price,initial_target_distance_pct,
                 setup_key,event_fingerprint,strategy_version,code_version,runtime_session_id,
                 delivery_status,engine_snapshot,created_at)
                VALUES(%s,'test','ALERT','MAX_PAIN_ALERT',%s,'BTC',%s,'SUPPORT','1h',75,100,%s,1,
                       %s,%s,%s,%s,%s,'DELIVERED',%s::jsonb,%s)''',
                (event['event_id'],event['alert_time_utc'],event['direction'],event['target_price'],
                 _sha(f'{VERSION}:setup:{event["event_id"]}'.encode()),event['event_fingerprint'],
                 VERSION,VERSION,VERSION,worker.canonical(event['engine_snapshot']),START))
            conn.execute('''INSERT INTO research_event_btc_movements
                (event_id,episode_policy_version,btc_parent_movement_id,decision_time_utc,
                 btc_observed_close_utc,membership_status,created_at_utc)
                VALUES(%s,%s,%s,%s,%s,'LIVE',%s)''',
                (event['event_id'],wave['episode_policy_version'],wave['btc_parent_movement_id'],
                 event['alert_time_utc'],closed,START))
    expected_source = _expected_source()
    expected_json = worker.canonical(expected_source)
    manifest.update(identity=dict(conn.execute('SELECT current_schema() AS schema,current_database() AS database').fetchone()),
        observed_at_utc=OBSERVED.isoformat(), start_utc=START.isoformat(),
        parents=PARENTS, events_per_parent=EVENTS_PER_PARENT, events=EVENTS,
        snapshot_canonical_bytes_each=SNAPSHOT_BYTES, snapshot_canonical_bytes_total=SNAPSHOT_BYTES*EVENTS,
        expected=_result(expected_source),
        expected_source={'source_sha256':_sha(expected_json.encode('ascii')),
            'canonical_bytes':len(expected_json), 'waves':PARENTS, 'events':EVENTS,
            'reference':'deterministic_seed_records_and_pure_compactor'},
        source_cardinality=dict(COUNTS),
        migrations={filename:_sha((ROOT/'migrations'/filename).read_bytes()) for filename in MIGRATIONS},
        limitations=['Synthetic fixed-width non-HYPE snapshots; not production object-shape equivalence.',
                     'Actual report source loading, compaction and digest only; no full report worker, paths, state, outbox or delivery.'])
    del expected_source, expected_json
    manifest['full_oracle'] = full_oracle(conn, manifest)
    manifest['checkpoint'] = checkpoint(conn, manifest)
    return manifest
