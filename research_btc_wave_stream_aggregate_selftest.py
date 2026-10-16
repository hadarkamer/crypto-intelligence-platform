"""Native math parity, bounded provenance and actual PostgreSQL cursor checks."""
from copy import deepcopy
from datetime import timedelta
import json
import os
import unittest
import weakref
from uuid import uuid4

import research_btc_wave_endpoint_report as native
import research_btc_wave_stream_aggregate as stream
from research_btc_wave_endpoint_report_selftest import START, candle, compute, wave


NOW = START + timedelta(hours=2)


def records_fixture(parent_count, *, variations=True):
    template = compute([candle(i, high=102., low=99.5) for i in range(60)])
    rows = []
    for index in range(parent_count):
        row = deepcopy(template)
        row.update(btc_parent_movement_id="parent-"+str(index), source_event_id=index+1,
                   observed_at_utc=NOW, direction="LONG")
        if variations:
            for offset, label in enumerate(row['thresholds']):
                label['status'] = ('SUCCESS', 'FAILURE', 'UNRESOLVED')[(index+offset) % 3]
            if index % 7 == 3:
                row.update(status='OPEN', observation_closed=False, path_complete=False,
                           mfe_pct=None, mae_pct=None, wave_end_utc=None)
            if index % 7 == 5:
                row.update(status='DATA_MISSING', path_complete=False, observed_prefix_complete=False,
                           mfe_pct=None, mae_pct=None, provisional_observed_mfe_pct=None,
                           provisional_observed_mae_pct=None, thresholds=[])
        for source in (native.SPOT_SCOPE, native.PERP_MIXED_SCOPE):
            for coin in ('ALL', 'BTC'):
                rows.append({**row, 'source_scope': source, 'coin_scope': coin})
    return sorted(rows, key=lambda r: (r['btc_parent_movement_id'], r['coin_scope'], r['direction'], r['source_scope']))


def factories(rows):
    groups = sorted({(r['source_scope'], r['coin_scope'], r['direction']) for r in rows})
    def records(group):
        return (r for r in rows if (r['source_scope'], r['coin_scope'], r['direction']) == group)
    def scalars(group, field, closed_ready):
        return (r[field] for r in records(group) if not closed_ready or
                (r['observation_closed'] and r['status'] == 'READY' and r['path_complete']))
    return groups, records, scalars


def assert_native_parity(case, actual, rows, generation_id):
    expected = native.aggregate_records(rows)
    case.assertEqual(len(actual['summary_rows']), len(expected))
    for old, current in zip(expected, actual['summary_rows']):
        current = dict(current)
        for field in ('source_event_ids', 'btc_parent_movement_ids'):
            reference = current.pop(field)
            case.assertEqual(reference, {'storage_version': stream.STORAGE_VERSION,
                'generation_id': str(generation_id), 'kind': field,
                'source_scope': old['source_scope'], 'coin_scope': old['coin_scope'], 'direction': old['direction']})
            selected = [r for r in rows if all(r[k] == old[k] for k in ('source_scope', 'coin_scope', 'direction'))]
            key = 'source_event_id' if field == 'source_event_ids' else 'btc_parent_movement_id'
            case.assertEqual([r[key] for r in selected], old[field])
        case.assertEqual(current, {k: v for k, v in old.items() if k not in {'source_event_ids', 'btc_parent_movement_ids'}})


class StreamingAggregateTests(unittest.TestCase):
    def test_real_native_hype_report_all_source_contracts_and_directions(self):
        from research_btc_source_compaction_selftest import duplicate_hype_source, fetch_fixture
        completed = native.build_report(**duplicate_hype_source(), observed_at=NOW,
                                        include_hype_perp=True, fetcher=fetch_fixture)
        actual = stream.aggregate_factories(*factories(completed['records']), generation_id='native')
        assert_native_parity(self, actual, completed['records'], 'native')
        self.assertEqual({row['source_scope'] for row in actual['summary_rows']},
                         {native.SPOT_SCOPE, native.PERP_SCOPE, native.PERP_MIXED_SCOPE})
        self.assertEqual({row['direction'] for row in actual['summary_rows']}, {'LONG', 'SHORT'})

    def test_native_parity_below_and_above_old_parent_limit(self):
        for count in (1, 2, 31, 32, 33, 129):
            for variations in (False, True):
                with self.subTest(count=count, variations=variations):
                    rows = records_fixture(count, variations=variations)
                    actual = stream.aggregate_factories(*factories(rows), generation_id='test-generation')
                    assert_native_parity(self, actual, rows, 'test-generation')
                    self.assertEqual(actual['record_count'], len(rows))
                    self.assertEqual(actual['missing_representatives'], sum(r['source_scope'] == native.PERP_MIXED_SCOPE
                        and r['status'] == 'DATA_MISSING' for r in rows))

    def test_zero_mae_no_decisions_and_all_active_gates(self):
        for mode in ('zero_mae', 'unresolved', 'all_active', 'all_missing'):
            with self.subTest(mode=mode):
                rows = records_fixture(4, variations=False)
                for row in rows:
                    if mode == 'zero_mae':
                        row.update(mae_pct=0., provisional_observed_mae_pct=0.)
                    elif mode == 'unresolved':
                        row['thresholds'] = [{**x, 'status': 'UNRESOLVED'} for x in row['thresholds']]
                    elif mode == 'all_active':
                        row.update(status='OPEN', observation_closed=False, path_complete=False, mfe_pct=None, mae_pct=None)
                    else:
                        row.update(status='DATA_MISSING', path_complete=False, observed_prefix_complete=False, thresholds=[])
                actual = stream.aggregate_factories(*factories(rows), generation_id='g')
                assert_native_parity(self, actual, rows, 'g')

    def test_scalar_iterators_preserve_current_python_builtin_sum(self):
        rows = records_fixture(129, variations=False)
        for row in rows:
            value = 1e16 if row['btc_parent_movement_id'] == 'parent-0' else 1.
            row.update(mfe_pct=value, provisional_observed_mfe_pct=value)
        actual = stream.aggregate_factories(*factories(rows), generation_id='g')
        assert_native_parity(self, actual, rows, 'g')
        self.assertEqual(actual['summary_rows'][0]['mfe_pct'], sum([1e16]+[1.]*128)/129)

    def test_duplicate_parent_out_of_order_and_invalid_thresholds_fail_closed(self):
        rows = records_fixture(3, variations=False)
        for changed in (rows+[rows[0]], list(reversed(rows))):
            with self.assertRaisesRegex(ValueError, 'twice or out of order'):
                stream.aggregate_factories(*factories(changed), generation_id='g')
        rows[0]['thresholds'] = rows[0]['thresholds'][:7]
        with self.assertRaisesRegex(ValueError, 'exact eight'):
            stream.aggregate_factories(*factories(rows), generation_id='g')

    def test_references_stay_small_and_comparison_overflow_is_not_truncated(self):
        rows = records_fixture(129)
        actual = stream.aggregate_factories(*factories(rows), generation_id='g')
        self.assertTrue(all(len(json.dumps(r['source_event_ids'])) < 300 for r in actual['summary_rows']))
        self.assertNotIn('parent-128', json.dumps(actual))
        too_many = [(native.PERP_MIXED_SCOPE, str(i), 'LONG') for i in range(33)]
        with self.assertRaisesRegex(ValueError, '256-row'):
            stream.aggregate_factories(too_many, lambda group: iter(()), lambda *args: iter(()), generation_id='g')

    def test_full_part_stream_releases_previous_graph_before_next_fetch(self):
        refs = []
        case = self
        class Graph(dict): pass
        class Cursor:
            index = 0
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def execute(self, sql, params): pass
            def fetchmany(self, count):
                case.assertEqual(count, 1)
                case.assertTrue(all(ref() is None for ref in refs))
                if self.index == 3:
                    return []
                self.index += 1
                graph = Graph({'body': 'x'*100_000})
                refs.append(weakref.ref(graph))
                return [{'report': graph}]
        class Connection:
            def cursor(self, *, name):
                case.assertTrue(name)
                return Cursor()
        for item in stream._stream(Connection(), 'SELECT fixture', (), batch_rows=1):
            self.assertEqual(len(item['report']['body']), 100_000)
            del item
        self.assertEqual(len(refs), 3)


@unittest.skipUnless(os.getenv('TEST_DATABASE_URL'), 'TEST_DATABASE_URL required for real PostgreSQL')
class StreamingAggregatePostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg.conninfo import conninfo_to_dict
        from psycopg.rows import dict_row
        cls.dsn = os.environ['TEST_DATABASE_URL']
        info = conninfo_to_dict(cls.dsn)
        if info.get('host') not in {'localhost', '127.0.0.1', '::1', 'postgres'} or 'test' not in info.get('dbname', '').lower():
            raise RuntimeError('Only local/CI test databases are allowed')
        cls.conn = psycopg.connect(cls.dsn, row_factory=dict_row, autocommit=True)

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def setUp(self):
        self.schema = 'wave_stream_test_'+uuid4().hex
        self.conn.execute(f'CREATE SCHEMA "{self.schema}"')
        self.conn.execute(f'SET search_path TO "{self.schema}"')
        self.conn.execute('''CREATE TABLE research_btc_wave_report_generations(
            generation_id uuid PRIMARY KEY,observed_at_utc timestamptz,parent_count integer,status text)''')
        self.conn.execute('''CREATE TABLE research_btc_wave_report_parts(
            generation_id uuid,ordinal integer,parent_id text,report jsonb,report_digest text,
            PRIMARY KEY(generation_id,ordinal),UNIQUE(generation_id,parent_id))''')

    def tearDown(self):
        self.conn.execute('SET search_path TO public')
        self.conn.execute(f'DROP SCHEMA "{self.schema}" CASCADE')

    def seed(self, count, *, variations=True):
        generation_id = str(uuid4())
        rows = records_fixture(count, variations=variations)
        self.conn.execute('INSERT INTO research_btc_wave_report_generations VALUES(%s,%s,%s,%s)',
                          (generation_id, NOW, count, 'CAPTURED'))
        # Store in reverse lexical order: aggregate queries must impose native order.
        for ordinal, parent_id in enumerate(reversed(sorted({r['btc_parent_movement_id'] for r in rows}))):
            selected = [r for r in rows if r['btc_parent_movement_id'] == parent_id]
            parent = wave(parent_id)
            if not selected[0]['observation_closed']:
                parent['end_time_utc'] = None
            completed = {'method_version': native.METHOD_VERSION, 'observed_at_utc': NOW,
                         'waves': [parent], 'records': selected, 'summary_rows': native.aggregate_records(selected)}
            self.conn.execute('INSERT INTO research_btc_wave_report_parts VALUES(%s,%s,%s,%s::jsonb,%s)',
                (generation_id, ordinal, parent_id, stream._canonical(completed), stream._digest(completed)))
        manifest = {'storage_version': stream.STORAGE_VERSION, 'generation_id': generation_id,
                    'parent_count': count, 'source_manifest_digest': 'a'*64}
        return generation_id, manifest, rows

    def aggregate(self, generation_id, manifest, conn=None):
        with self.conn.transaction():
            self.conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            return stream.aggregate_generation(conn or self.conn, generation_id, NOW, manifest)

    def test_real_server_cursors_exact_math_and_full_history_manifest(self):
        for count in (31, 33, 129):
            with self.subTest(count=count):
                generation_id, manifest, rows = self.seed(count)
                opened = []
                owner = self.conn
                class RecordingConnection:
                    def execute(self, *args, **kwargs): return owner.execute(*args, **kwargs)
                    def cursor(self, *args, **kwargs):
                        self_outer.assertTrue(kwargs.get('name'))
                        cursor = owner.cursor(*args, **kwargs)
                        opened.append(cursor)
                        return cursor
                self_outer = self
                actual = self.aggregate(generation_id, manifest, RecordingConnection())
                assert_native_parity(self, actual, rows, generation_id)
                self.assertEqual(actual['parent_count'], count)
                self.assertEqual(actual['source_digest'], 'a'*64)
                self.assertEqual(actual['records']['generation_id'], generation_id)
                self.assertEqual(len(actual['report_manifest_digest']), 64)
                self.assertEqual(opened[0].itersize, 1)
                self.assertTrue(all(cursor.closed for cursor in opened))
                self.assertTrue(all(cursor.itersize <= stream.CURSOR_ROWS for cursor in opened))

    def test_missing_or_changed_part_cannot_produce_root(self):
        for failure in ('missing_report', 'tampered_report', 'deleted_parent'):
            with self.subTest(failure=failure):
                generation_id, manifest, rows = self.seed(33)
                if failure == 'deleted_parent':
                    self.conn.execute('DELETE FROM research_btc_wave_report_parts WHERE generation_id=%s AND ordinal=0', (generation_id,))
                elif failure == 'missing_report':
                    self.conn.execute('UPDATE research_btc_wave_report_parts SET report=NULL WHERE generation_id=%s AND ordinal=0', (generation_id,))
                else:
                    self.conn.execute("UPDATE research_btc_wave_report_parts SET report=jsonb_set(report,'{changed}','true') WHERE generation_id=%s AND ordinal=0", (generation_id,))
                with self.assertRaises(ValueError):
                    self.aggregate(generation_id, manifest)

    def test_duplicate_and_wrong_parent_records_rejected_even_with_fresh_digest(self):
        for failure in ('duplicate', 'wrong_parent'):
            with self.subTest(failure=failure):
                generation_id, manifest, rows = self.seed(2)
                selected = self.conn.execute('SELECT report FROM research_btc_wave_report_parts WHERE generation_id=%s AND ordinal=0', (generation_id,)).fetchone()['report']
                if failure == 'duplicate':
                    selected['records'].append(dict(selected['records'][0]))
                else:
                    selected['records'][0]['btc_parent_movement_id'] = 'wrong'
                self.conn.execute('UPDATE research_btc_wave_report_parts SET report=%s::jsonb,report_digest=%s WHERE generation_id=%s AND ordinal=0',
                    (stream._canonical(selected), stream._digest(selected), generation_id))
                with self.assertRaises(ValueError):
                    self.aggregate(generation_id, manifest)

    def test_real_json_numbers_use_builtin_sum_order(self):
        generation_id, manifest, rows = self.seed(33, variations=False)
        for parent in sorted({r['btc_parent_movement_id'] for r in rows}):
            stored = self.conn.execute('SELECT ordinal,report FROM research_btc_wave_report_parts WHERE generation_id=%s AND parent_id=%s',
                                       (generation_id, parent)).fetchone()
            value = 1e16 if parent == 'parent-0' else 1.
            for row in stored['report']['records']:
                row.update(mfe_pct=value, provisional_observed_mfe_pct=value)
            for row in rows:
                if row['btc_parent_movement_id'] == parent:
                    row.update(mfe_pct=value, provisional_observed_mfe_pct=value)
            self.conn.execute('UPDATE research_btc_wave_report_parts SET report=%s::jsonb,report_digest=%s WHERE generation_id=%s AND ordinal=%s',
                (stream._canonical(stored['report']), stream._digest(stored['report']), generation_id, stored['ordinal']))
        actual = self.aggregate(generation_id, manifest)
        assert_native_parity(self, actual, rows, generation_id)


if __name__ == '__main__':
    unittest.main()
