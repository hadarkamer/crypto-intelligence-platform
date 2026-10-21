"""Exact source parity and raw-snapshot lifetime across bounded BTC wave reads."""
from copy import deepcopy
from contextlib import nullcontext
from datetime import timedelta
import json
import os
import unittest
from unittest.mock import Mock, patch
import weakref
from uuid import uuid4

import research_btc_wave_endpoint_report as report
import research_btc_wave_report_worker as worker
from research_btc_wave_endpoint_report_selftest import START, event
from research_btc_wave_endpoint_report_selftest import ROUTE, candle
from research_btc_wave_report_worker_selftest import fixtures
from research_outcome_worker import _snapshot_price_provenance


OBSERVED = START + timedelta(hours=5, minutes=15)


class Snapshot(dict):
    """Weak-referenceable source graph; the fixture never holds it strongly."""


class Result:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


def population():
    waves = [{'btc_parent_movement_id': 'boundary', 'start_time_utc': worker.SOURCE_START-timedelta(hours=1),
              'end_time_utc': worker.SOURCE_START+timedelta(hours=1), 'evidence_eligible': True}]
    waves += [{'btc_parent_movement_id': 'wave-'+str(i), 'start_time_utc': START+timedelta(hours=i),
               'end_time_utc': START+timedelta(hours=i+1), 'evidence_eligible': i != 0}
              for i in range(6)]
    for wave in waves:
        wave.update(episode_policy_version=report.POLICY_VERSION, boundary_reason='CAUSAL_CLOSE_REVERSAL',
                    observed_through_utc=START+timedelta(hours=8))
        wave['closing_boundary_verified'] = any(other['start_time_utc'] == wave['end_time_utc']
            and other['evidence_eligible'] for other in waves)
    # Descriptors contain no snapshot graph or large body. Each DB read creates
    # fresh raw objects, as psycopg JSON decoding would, without fixture owners.
    descriptors = [(1, worker.SOURCE_START-timedelta(seconds=1), 'boundary', 'BTC', True),
                   (2, worker.SOURCE_START, 'boundary', 'BTC', False)]
    for i in range(6):
        for j in range(8):
            descriptors.append((100+i*10+j, START+timedelta(hours=i, minutes=j+1),
                                'wave-'+str(i), 'HYPE' if j == 0 else 'BTC', i == 0))
    descriptors += [(900, OBSERVED, 'wave-5', 'ETH', False),
                    (901, OBSERVED+timedelta(seconds=1), 'wave-5', 'BTC', True)]
    return waves, descriptors


class SourceConnection:
    def __init__(self, waves=None, descriptors=None):
        default_waves, default_descriptors = population()
        self.waves = deepcopy(default_waves if waves is None else waves)
        self.descriptors = default_descriptors if descriptors is None else descriptors
        self.refs = []
        self.live_before_wave = []
        self.live_hype_before_wave = []
        self.peak_non_hype = 0
        self.queries = []

    def live_non_hype(self):
        return [eid for eid, symbol, ref in self.refs if symbol != 'HYPE' and ref() is not None]

    def raw_event(self, descriptor):
        eid, stamp, wave_id, symbol, corrupt = descriptor
        row = event(eid, symbol=symbol, wave_id=wave_id)
        row.pop('coin_scope')
        row.pop('btc_parent_movement_id')
        row.update(alert_time_utc=stamp, decision_time_utc=stamp,
                   btc_observed_close_utc=stamp-report.MILLISECOND,
                   source_side='SHORT', timeframe='1h', target_price=110., initial_target_distance_pct=10.)
        snapshot = Snapshot({'price_source': 'hyperliquid' if symbol == 'HYPE' else 'binance_spot',
            'price_pair': symbol+'USDT', 'raw_details': {'event': eid,
            'large_body': (str(eid)+':').ljust(12_000, 'x')}, 'unicode': 'מקור'})
        if corrupt:
            # Invalid to canonicalize, but the original worker discards these
            # rows by date/eligibility before it ever serializes their source.
            snapshot['not_finite'] = float('nan')
        row['engine_snapshot'] = snapshot
        self.refs.append((eid, symbol, weakref.ref(snapshot)))
        self.peak_non_hype = max(self.peak_non_hype, len(self.live_non_hype()))
        return row

    def execute(self, sql, params):
        normalized = ' '.join(sql.split())
        self.queries.append((normalized, params))
        if normalized.startswith('SELECT btc_parent_movement_id FROM research_btc_parent_movements'):
            policy, observed, lower, limit = params
            rows = [wave for wave in self.waves if wave['episode_policy_version'] == policy
                    and wave['start_time_utc'] <= observed
                    and (wave['end_time_utc'] is None or wave['end_time_utc'] >= lower)]
            rows.sort(key=lambda wave: wave['start_time_utc'])
            return Result([{'btc_parent_movement_id': wave['btc_parent_movement_id']} for wave in rows[:limit]])
        if normalized.startswith('SELECT p.btc_parent_movement_id,'):
            wanted = params[0]
            return Result([deepcopy(wave) for wave in sorted(self.waves, key=lambda x: x['start_time_utc'])
                           if wave['btc_parent_movement_id'] in wanted])
        assert normalized.startswith('SELECT e.event_id, e.event_kind, e.event_type,'), normalized
        assert "e.score>=65" in normalized and "e.delivery_status='DELIVERED'" in normalized
        assert 'ORDER BY e.alert_time_utc, e.event_id LIMIT %s' in normalized
        policy, lower, upper, limit = params
        assert policy == report.POLICY_VERSION
        self.live_before_wave.append(self.live_non_hype())
        self.live_hype_before_wave.append([eid for eid, symbol, ref in self.refs
                                          if symbol == 'HYPE' and ref() is not None])
        selected = sorted((item for item in self.descriptors
            if lower <= item[1] < (upper or OBSERVED+timedelta(days=1))), key=lambda x: (x[1], x[0]))[:limit]
        return Result([self.raw_event(item) for item in selected])


def legacy_compacted_source(conn, observed):
    """Frozen pre-change load/filter/compact contract, with the real SQL loader."""
    parents = conn.execute('''SELECT btc_parent_movement_id FROM research_btc_parent_movements
        WHERE episode_policy_version=%s AND start_time_utc<=%s
          AND (end_time_utc IS NULL OR end_time_utc>=%s)
        ORDER BY start_time_utc LIMIT %s''', (report.POLICY_VERSION, observed,
            worker.SOURCE_START, report.MAX_WAVES+1)).fetchall()
    if len(parents) > report.MAX_WAVES:
        raise ValueError('parent cap')
    if not parents:
        return {'waves': [], 'events': []}
    waves, events = report.load_source(conn, [p['btc_parent_movement_id'] for p in parents])
    for wave in waves:
        if wave.get('end_time_utc') and report.utc(wave['end_time_utc']) > observed:
            wave['end_time_utc'] = None
            wave['closing_boundary_verified'] = False
        if wave.get('observed_through_utc'):
            wave['observed_through_utc'] = min(report.utc(wave['observed_through_utc']),
                                              report.latest_closed_cutoff(observed))
    events = [item for item in events if worker.SOURCE_START <= report.utc(item['alert_time_utc']) <= observed]
    eligible = {wave['btc_parent_movement_id'] for wave in waves if wave['evidence_eligible'] is True}
    compact = []
    for item in events:
        if item['btc_parent_movement_id'] not in eligible:
            continue
        row = dict(item)
        if row.get('engine_snapshot_compacted'):
            compact.append(row)
            continue
        frozen = report.snapshot_digest(item.get('engine_snapshot'))
        if 'engine_snapshot_digest' in row and row['engine_snapshot_digest'] != frozen:
            raise ValueError('Frozen engine snapshot changed')
        row['engine_snapshot_digest'] = frozen
        if row.get('symbol') != 'HYPE':
            provenance = _snapshot_price_provenance(item.get('engine_snapshot'))
            row['engine_snapshot'] = {'price_'+key: value for key, value in provenance.items()}
            row['engine_snapshot_compacted'] = True
        compact.append(row)
    return {'waves': waves, 'events': compact}


def duplicate_hype_source(*, body_bytes=12_000):
    """Both directions in each wave, with HYPE and Spot winning ALL separately."""
    source = fixtures()
    source['events'] = []
    for wave_index, wave_id in enumerate(('first', 'second')):
        symbols = ('BTC', 'HYPE', 'HYPE', 'ETH', 'HYPE', 'HYPE', 'BTC')
        if wave_index:
            symbols = ('HYPE', 'BTC', 'HYPE', 'HYPE', 'ETH', 'HYPE', 'BTC')
        for index, symbol in enumerate(symbols):
            row = event(100+wave_index*10+index, symbol=symbol,
                        direction='LONG' if index in (0, 1, 2, 6) else 'SHORT',
                        seconds=wave_index*3600+index+10, wave_id=wave_id)
            row['engine_snapshot'] = {'price_source': 'hyperliquid' if symbol == 'HYPE' else 'binance_spot',
                'price_pair': symbol+'USDT', 'raw_details': {'event': row['event_id'],
                    'body': 'x'*body_bytes}, 'unicode': 'מקור'}
            source['events'].append(row)
    return source


def fetch_fixture(symbol, start, end):
    import hyperliquid_perp_price_path as perp
    first = int((start-START)//report.MINUTE)
    count = int((end+report.MILLISECOND-start)//report.MINUTE)
    route = perp.SOURCE if symbol == 'HYPE' else {**ROUTE, 'symbol': symbol, 'pair': symbol+'USDT'}
    return {**route, 'candles': [candle(first+i, high=101., low=99.5) for i in range(count)]}


def selection_identity(source):
    return [(row['btc_parent_movement_id'], row['coin_scope'], row['direction'], row['event_id'])
            for row in report.select_representatives(source['events'])]


class SourceCompactionTests(unittest.TestCase):
    def test_nonselected_hype_preserves_all_rows_selection_reports_and_closed_cache(self):
        import research_native_hype_mark_supplement as native
        source = duplicate_hype_source()
        original = worker.canonical(source)
        compact = worker.compact_source(source)
        self.assertEqual(worker.canonical(source), original)
        self.assertEqual(selection_identity(compact), selection_identity(source))
        self.assertEqual([row['event_id'] for row in compact['events']],
                         [row['event_id'] for row in source['events']])
        selected_hype = {row['event_id'] for row in report.select_representatives(source['events'])
                         if row['symbol'] == 'HYPE'}
        self.assertEqual(selected_hype, {101, 104, 110, 113})
        for before, after in zip(source['events'], compact['events']):
            self.assertEqual({key: value for key, value in after.items()
                              if key not in {'engine_snapshot', 'engine_snapshot_digest', 'engine_snapshot_compacted'}},
                             {key: value for key, value in before.items() if key != 'engine_snapshot'})
            self.assertEqual(after['engine_snapshot_digest'], report.snapshot_digest(before['engine_snapshot']))
            self.assertEqual(_snapshot_price_provenance(before['engine_snapshot']),
                             _snapshot_price_provenance(after['engine_snapshot']))
            if before['event_id'] in selected_hype:
                self.assertIs(after['engine_snapshot'], before['engine_snapshot'])
                self.assertFalse(after.get('engine_snapshot_compacted'))
                self.assertEqual(native.source_digest(before), native.source_digest(after))
            else:
                self.assertTrue(after['engine_snapshot_compacted'])
                self.assertNotIn('raw_details', after['engine_snapshot'])
        now = START+timedelta(minutes=120)
        raw_report = report.build_report(**source, observed_at=now, include_hype_perp=True, fetcher=fetch_fixture)
        compact_report = report.build_report(**compact, observed_at=now, include_hype_perp=True, fetcher=fetch_fixture)
        # Report entries and price provenance are unchanged; native full source
        # digests were checked separately for every selected HYPE event above.
        self.assertEqual(worker.canonical(raw_report), worker.canonical(compact_report))
        self.assertTrue(any(row['status'] == 'READY' and row['source_scope'] == report.PERP_SCOPE
                            for row in compact_report['records']))
        for previous, incoming in ((source, compact), (compact, source), (compact, compact)):
            cache = report.verified_closed_cache(previous_report=raw_report, previous_source=previous,
                **incoming, observed_at=now, include_hype_perp=True)
            baseline = report.verified_closed_cache(previous_report=raw_report, previous_source=source,
                **source, observed_at=now, include_hype_perp=True)
            self.assertEqual(worker.canonical(list(cache.values())),
                             worker.canonical(list(baseline.values())))
            self.assertTrue(cache)

    def test_digest_present_checkpoint_migrates_without_mutation_and_resumes_identically(self):
        source = duplicate_hype_source()
        legacy = {'waves': source['waves'], 'events': [worker._compact_source_event(row) for row in source['events']]}
        self.assertTrue(all('engine_snapshot_digest' in row for row in legacy['events']))
        job = worker.prepare_job(legacy, START+timedelta(minutes=120))
        worker.advance_job(job, fetcher=fetch_fixture, path_limit=2)
        job = json.loads(worker.canonical(job))  # Actual restart representation.
        original = worker.canonical(job)
        migrated = worker.compact_job(job)
        self.assertIsNot(migrated, job)
        self.assertEqual(worker.canonical(job), original)
        for key in job.keys()-{'source', 'source_digest'}:
            self.assertEqual(migrated[key], job[key], key)
        self.assertEqual(migrated['source_digest'], worker.digest(worker.compact_source(source)))
        self.assertEqual(migrated['source'], json.loads(worker.canonical(worker.compact_source(source))))
        self.assertIs(worker.compact_job(migrated), migrated)
        for current in (job, migrated):
            while not worker.advance_job(current, fetcher=fetch_fixture, path_limit=2)['completed']:
                pass
        raw_report, new_report = worker.finish_job(job), worker.finish_job(migrated)
        raw_report.pop('source_digest')
        new_report.pop('source_digest')
        self.assertEqual(worker.canonical(raw_report), worker.canonical(new_report))

    def test_changed_or_invalid_snapshot_is_rejected_before_migration(self):
        source = duplicate_hype_source()
        for event_id in (101, 102):  # Selected and nonselected full HYPE snapshots.
            with self.subTest(event_id=event_id):
                legacy = {'waves': source['waves'], 'events': [worker._compact_source_event(row) for row in source['events']]}
                row = next(row for row in legacy['events'] if row['event_id'] == event_id)
                row['engine_snapshot'] = {**row['engine_snapshot'], 'changed': True}
                with self.assertRaisesRegex(ValueError, 'Frozen engine snapshot changed'):
                    worker.compact_source(legacy)
        for bad_digest in (None, 'g'*64, 'short', 12):
            compact = worker.compact_source(source)
            next(row for row in compact['events'] if row['event_id'] == 102)['engine_snapshot_digest'] = bad_digest
            with self.assertRaisesRegex(ValueError, 'Invalid frozen engine snapshot digest'):
                worker.compact_source(compact)
        compact = worker.compact_source(source)
        selected = next(row for row in compact['events'] if row['event_id'] == 101)
        selected['engine_snapshot_compacted'] = True
        with self.assertRaisesRegex(ValueError, 'Selected HYPE event requires'):
            worker.compact_source(compact)
        corrupt = duplicate_hype_source()
        corrupt['events'][2]['engine_snapshot']['not_finite'] = float('nan')
        with self.assertRaises(ValueError):
            worker.compact_source(corrupt)

    def test_migration_rejects_changed_source_fields_or_missing_original_digest(self):
        source = duplicate_hype_source()
        legacy = {'waves': source['waves'], 'events': [worker._compact_source_event(row) for row in source['events']]}
        for change in ('score', 'source_digest', 'missing_digest'):
            with self.subTest(change=change):
                job = worker.prepare_job(deepcopy(legacy), START+timedelta(minutes=120))
                if change == 'score':
                    job['source']['events'][2]['score'] -= 1
                elif change == 'source_digest':
                    job['source_digest'] = '0'*64
                else:
                    job.pop('source_digest')
                original = worker.canonical(job)
                with self.assertRaisesRegex(ValueError, 'Frozen job source changed'):
                    worker.compact_job(job)
                self.assertEqual(worker.canonical(job), original)

    def test_migrated_raw_bodies_are_released_before_publication_recheck(self):
        source = duplicate_hype_source()
        legacy = {'waves': source['waves'], 'events': [worker._compact_source_event(row) for row in source['events']]}
        job = worker.prepare_job(legacy, START+timedelta(minutes=120))
        self.assertTrue(worker.advance_job(job, fetcher=fetch_fixture)['completed'])
        serialized = worker.canonical(job)
        selected_hype = {row['event_id'] for row in report.select_representatives(source['events'])
                         if row['symbol'] == 'HYPE'}
        refs, writes = [], []
        class Connection:
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def commit(self): pass
            def rollback(self): pass
            def transaction(self): return nullcontext()
            def execute(self, sql, params=()):
                self.sql = ' '.join(sql.split())
                if self.sql.startswith('UPDATE '):
                    writes.append((self.sql, params))
                return self
            def fetchone(self):
                if self.sql.startswith('SELECT report,'):
                    return {'report': None, 'report_observed_at_utc': None, 'next_report_at_utc': None,
                            'has_pending_job': True, 'state_xmin': '101'}
                if self.sql.startswith('SELECT pending_job,'):
                    decoded = json.loads(serialized)
                    for row in decoded['source']['events']:
                        if row['symbol'] == 'HYPE' and row['event_id'] not in selected_hype:
                            row['engine_snapshot'] = Snapshot(row['engine_snapshot'])
                            refs.append(weakref.ref(row['engine_snapshot']))
                    return {'pending_job': decoded, 'source': None}
                return {'held': True}
        def recheck(conn, observed_at):
            self.assertEqual(observed_at, START+timedelta(minutes=120))
            self.assertEqual(len(refs), 4)
            self.assertTrue(all(ref() is None for ref in refs), 'Legacy checkpoint still owns discarded bodies')
            return worker.compact_source(source)
        with patch.object(worker, '_connect', return_value=Connection()), \
             patch.object(worker, 'load_job_source', side_effect=recheck) as reread, \
             patch.object(worker.research_sheet_outbox, 'stage_upserts', side_effect=lambda conn, rows: len(rows)):
            result = worker.ResearchBTCWaveReportWorker().run_once(now=START+timedelta(hours=3))
        self.assertTrue(result['published'])
        reread.assert_called_once()
        self.assertEqual(len(writes), 1)
        # A modified legacy source cannot be rehashed as a valid new checkpoint,
        # advanced, rechecked or staged while retaining its prior path records.
        broken = json.loads(serialized)
        broken['source']['events'][2]['score'] -= 1
        serialized = worker.canonical(broken)
        with patch.object(worker, '_connect', return_value=Connection()), \
             patch.object(worker, 'advance_job') as advance, \
             patch.object(worker, 'load_job_source') as reread, \
             patch.object(worker.research_sheet_outbox, 'stage_upserts') as stage:
            with self.assertRaisesRegex(ValueError, 'Frozen job source changed'):
                worker.ResearchBTCWaveReportWorker().run_once(now=START+timedelta(hours=3))
        advance.assert_not_called()
        reread.assert_not_called()
        stage.assert_not_called()
        self.assertEqual(len(writes), 1)

    def test_streaming_duplicate_hype_matches_frozen_compaction_and_releases_unused_bodies(self):
        waves, descriptors = population()
        descriptors = [(eid, stamp, wave_id, 'HYPE' if eid % 10 in (0, 1, 4, 5) else symbol, corrupt)
                       for eid, stamp, wave_id, symbol, corrupt in descriptors]
        class DuplicateConnection(SourceConnection):
            def raw_event(self, descriptor):
                row = super().raw_event(descriptor)
                row['direction'] = 'SHORT' if row['event_id'] % 10 >= 4 else 'LONG'
                return row
        legacy = DuplicateConnection(waves, descriptors)
        expected = worker.compact_source(legacy_compacted_source(legacy, OBSERVED))
        current = DuplicateConnection(waves, descriptors)
        actual = worker.load_job_source(current, OBSERVED)
        self.assertEqual(worker.canonical(actual), worker.canonical(expected))
        self.assertEqual(current.queries, legacy.queries)
        selected = {row['event_id'] for row in report.select_representatives(actual['events'])
                    if row['symbol'] == 'HYPE'}
        self.assertEqual(len(selected), 10)
        self.assertTrue(all(set(previous).issubset(selected) for previous in current.live_hype_before_wave))
        self.assertEqual({eid for eid, symbol, ref in current.refs if symbol == 'HYPE' and ref() is not None}, selected)
        self.assertTrue(all(not previous for previous in current.live_before_wave))

    def test_exact_legacy_bytes_filters_hype_and_raw_lifetime(self):
        legacy = SourceConnection()
        expected = legacy_compacted_source(legacy, OBSERVED)
        current = SourceConnection()
        actual = worker.load_job_source(current, OBSERVED)
        self.assertEqual(worker.canonical(actual).encode(), worker.canonical(expected).encode())
        self.assertEqual(worker.digest(actual), worker.digest(expected))
        self.assertEqual(current.queries, legacy.queries)
        expected_ids = [2] + [100+i*10+j for i in range(1, 6) for j in range(8)] + [900]
        self.assertEqual([row['event_id'] for row in actual['events']], expected_ids)
        self.assertEqual(len(actual['waves']), 7)  # Ineligible parents remain in the source contract.
        self.assertIsNone(actual['waves'][-1]['end_time_utc'])
        self.assertFalse(actual['waves'][-1]['closing_boundary_verified'])
        self.assertTrue(all(wave['observed_through_utc'] == report.latest_closed_cutoff(OBSERVED)
                            for wave in actual['waves']))
        hype = [row for row in actual['events'] if row['symbol'] == 'HYPE']
        self.assertEqual(len(hype), 5)
        self.assertTrue(all('raw_details' in row['engine_snapshot']
                            and not row.get('engine_snapshot_compacted') for row in hype))
        self.assertTrue(all(row.get('engine_snapshot_compacted') and 'raw_details' not in row['engine_snapshot']
                            for row in actual['events'] if row['symbol'] != 'HYPE'))
        # No GC calls or timing thresholds: these acyclic graphs must lose all
        # owners before the next DB wave is materialized. HYPE stays intact.
        self.assertTrue(all(not previous for previous in current.live_before_wave), current.live_before_wave)
        self.assertEqual(current.live_non_hype(), [])
        self.assertLessEqual(current.peak_non_hype, 9)
        self.assertGreater(legacy.peak_non_hype, current.peak_non_hype*3)
        self.assertEqual([eid for eid, symbol, ref in current.refs if symbol == 'HYPE' and ref() is not None],
                         [110, 120, 130, 140, 150])

    def test_default_endpoint_still_returns_full_raw_source(self):
        conn = SourceConnection()
        waves, rows = report.load_source(conn, ['wave-1', 'wave-2'])
        self.assertEqual([wave['btc_parent_movement_id'] for wave in waves], ['wave-1', 'wave-2'])
        self.assertEqual([row['event_id'] for row in rows], list(range(110, 118))+list(range(120, 128)))
        self.assertTrue(all('raw_details' in row['engine_snapshot'] for row in rows))
        self.assertTrue(all('engine_snapshot_digest' not in row and 'engine_snapshot_compacted' not in row for row in rows))

    def test_per_wave_cap_precedes_projector_and_excluded_corrupt_rows(self):
        projector = Mock(side_effect=AssertionError('Over-cap batch was projected'))
        with patch.object(report, 'MAX_EVENTS_PER_WAVE', 2):
            with self.assertRaisesRegex(ValueError, 'per-wave budget'):
                report.load_source(SourceConnection(), ['wave-0'], event_projector=projector)
            with self.assertRaisesRegex(ValueError, 'per-wave budget'):
                worker.load_job_source(SourceConnection(), OBSERVED)
        projector.assert_not_called()

    def test_parent_cap_and_empty_population_do_not_read_event_snapshots(self):
        conn = SourceConnection()
        with patch.object(report, 'MAX_WAVES', 2), self.assertRaisesRegex(ValueError, '32_PARENTS'):
            worker.load_job_source(conn, OBSERVED)
        self.assertEqual(conn.refs, [])
        empty = SourceConnection(waves=[], descriptors=[])
        self.assertEqual(worker.load_job_source(empty, OBSERVED), {'waves': [], 'events': []})
        self.assertEqual(empty.refs, [])


@unittest.skipUnless(os.getenv('TEST_DATABASE_URL'), 'TEST_DATABASE_URL required for real PostgreSQL')
class SourceCompactionPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg.conninfo import conninfo_to_dict
        from psycopg.rows import dict_row
        info = conninfo_to_dict(os.environ['TEST_DATABASE_URL'])
        if info.get('host') not in {'localhost', '127.0.0.1', '::1', 'postgres'} or 'test' not in info.get('dbname', '').lower():
            raise RuntimeError('Only local/CI test databases are allowed')
        cls.conn = psycopg.connect(os.environ['TEST_DATABASE_URL'], row_factory=dict_row, autocommit=True)

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def setUp(self):
        self.schema = 'btc_source_test_'+uuid4().hex
        self.conn.execute(f'CREATE SCHEMA "{self.schema}"')
        self.conn.execute(f'SET search_path TO "{self.schema}"')
        self.conn.execute('''CREATE TABLE research_btc_parent_movements (
            btc_parent_movement_id text PRIMARY KEY, episode_policy_version text,
            start_time_utc timestamptz, end_time_utc timestamptz, evidence_eligible boolean,
            boundary_reason text, observed_through_utc timestamptz)''')
        self.conn.execute('''CREATE TABLE research_events (
            event_id bigint PRIMARY KEY, event_kind text, event_type text, alert_time_utc timestamptz,
            symbol text, direction text, score double precision, current_price double precision,
            delivery_status text, engine_snapshot jsonb, source_side text, timeframe text,
            target_price double precision, initial_target_distance_pct double precision,
            event_fingerprint text, strategy_version text, code_version text)''')
        self.conn.execute('''CREATE TABLE research_event_btc_movements (
            event_id bigint, episode_policy_version text, btc_parent_movement_id text,
            membership_status text, decision_time_utc timestamptz, btc_observed_close_utc timestamptz)''')
        source = SourceConnection()
        for wave in source.waves:
            self.conn.execute('''INSERT INTO research_btc_parent_movements VALUES (%s,%s,%s,%s,%s,%s,%s)''',
                tuple(wave[key] for key in ('btc_parent_movement_id', 'episode_policy_version',
                    'start_time_utc', 'end_time_utc', 'evidence_eligible', 'boundary_reason', 'observed_through_utc')))
        for descriptor in source.descriptors:
            row = source.raw_event(descriptor)
            # PostgreSQL JSONB rejects NaN on storage. The synthetic-only case
            # above exercises corrupt excluded graphs; SQL fixture uses JSON null.
            if descriptor[-1]:
                row['engine_snapshot']['not_finite'] = None
            self.insert_event(row)
        # These rows are excluded by the real source SQL before any projector.
        for offset, (key, value) in enumerate((('event_kind', 'DECISION_SAMPLE'),
                ('event_type', 'OTHER_ALERT'), ('delivery_status', 'DELIVERY_FAILED'),
                ('score', 64), ('direction', 'NEUTRAL'))):
            row = source.raw_event((950+offset, START+timedelta(hours=1, minutes=20), 'wave-1', 'BTC', False))
            row[key] = value
            self.insert_event(row)

    def insert_event(self, row):
        keys = ('event_id', 'event_kind', 'event_type', 'alert_time_utc', 'symbol', 'direction',
                'score', 'current_price', 'delivery_status', 'engine_snapshot', 'source_side',
                'timeframe', 'target_price', 'initial_target_distance_pct', 'event_fingerprint',
                'strategy_version', 'code_version')
        self.conn.execute('''INSERT INTO research_events VALUES
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s,%s)''',
            tuple(json.dumps(row[key], ensure_ascii=False, allow_nan=False) if key == 'engine_snapshot'
                  else row[key] for key in keys))
        self.conn.execute('INSERT INTO research_event_btc_movements VALUES (%s,%s,%s,%s,%s,%s)',
            (row['event_id'], row['membership_policy_version'], row['membership_parent_id'],
             row['membership_status'], row['decision_time_utc'], row['btc_observed_close_utc']))

    def tearDown(self):
        self.conn.execute('SET search_path TO public')
        self.conn.execute(f'DROP SCHEMA "{self.schema}" CASCADE')

    def test_real_source_sql_exact_parity_and_candidate_predicates(self):
        with self.conn.transaction():
            self.conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            before = legacy_compacted_source(self.conn, OBSERVED)
            after = worker.load_job_source(self.conn, OBSERVED)
        self.assertEqual(worker.canonical(after).encode(), worker.canonical(before).encode())
        self.assertEqual(worker.digest(after), worker.digest(before))
        self.assertEqual([row['event_id'] for row in after['events']],
                         [2]+[100+i*10+j for i in range(1, 6) for j in range(8)]+[900])
        self.assertEqual(sum(row['symbol'] == 'HYPE' for row in after['events']), 5)

    def test_real_sql_cap_rejects_entire_ineligible_wave_before_projection(self):
        projector = Mock(side_effect=AssertionError('Over-cap batch was projected'))
        with patch.object(report, 'MAX_EVENTS_PER_WAVE', 2):
            with self.assertRaisesRegex(ValueError, 'per-wave budget'):
                report.load_source(self.conn, ['wave-0'], event_projector=projector)
            with self.assertRaisesRegex(ValueError, 'per-wave budget'):
                worker.load_job_source(self.conn, OBSERVED)
        projector.assert_not_called()


if __name__ == '__main__':
    unittest.main()
