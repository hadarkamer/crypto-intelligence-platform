"""Isolated real producer -> durable approved outbox -> bounded sender tests."""
from contextlib import contextmanager, redirect_stdout
from copy import deepcopy
import io
import json
import os
from pathlib import Path
import threading
import unittest
from unittest.mock import patch
from uuid import uuid4

import approved_alert_contract as contract
import approved_alert_outbox as outbox
import approved_alert_producer as producer
import experimental_execution_bridge as bridge
import experimental_execution_forwarder as forwarder
import experimental_execution_transport as transport
import sol_proximity_experimental_store as store
from experimental_execution_bridge_selftest import maxpain_pending_fixture
from experimental_execution_forwarder_selftest import environment
from experimental_execution_source_audit_selftest import BASE, CONFIG, M, SCOPE, ms
from experimental_execution_transport_selftest import ack, config
from maxpain_experimental_specs import HYPE_LONG_TF, SPECS
from sol_proximity_experimental_worker import render_alert
from xrp_r2732_experimental_store_selftest import MemoryDatabase


class Rows:
    def __init__(self, rows=()): self.rows = deepcopy(list(rows))
    def fetchone(self): return self.rows[0] if self.rows else None
    def fetchall(self): return deepcopy(self.rows)


class OutboxDatabase(MemoryDatabase):
    """Transactional SQL double, including savepoints and commit failure."""
    def __init__(self):
        super().__init__()
        self.outbox, self.commits, self.fail_commit = {}, 0, False

    @contextmanager
    def connect(self, database_url=None, **options):
        with self.lock:
            before = deepcopy((self.values, self.outbox))
            savepoint = None
            db = self
            class Connection:
                def execute(self, sql, params=None):
                    nonlocal savepoint
                    db.calls.append((sql, params))
                    if sql.startswith('SAVEPOINT '):
                        savepoint = deepcopy(db.outbox); return Rows()
                    if sql.startswith('ROLLBACK TO SAVEPOINT '):
                        db.outbox = deepcopy(savepoint); return Rows()
                    if sql.startswith('RELEASE SAVEPOINT '): return Rows()
                    if sql.startswith('SELECT pg_advisory_xact_lock'): return Rows()
                    if sql.startswith('INSERT INTO bot_settings'):
                        db.values.setdefault(params[0], params[1]); return Rows()
                    if sql.startswith('SELECT value FROM bot_settings'):
                        return Rows([{'value': db.values[params[0]]}] if params[0] in db.values else [])
                    if sql.startswith('UPDATE bot_settings'):
                        db.values[params[1]] = params[0]; return Rows()
                    if sql.startswith('SELECT occurrence_id,source_position_id'):
                        return Rows(v for (key, _), v in db.outbox.items()
                                    if key == params[0] and not v['canceled'])
                    if sql.startswith('SELECT count(*)'):
                        return Rows([{'count': sum(v['payload_json'] is not None for v in db.outbox.values())}])
                    if sql.startswith('SELECT occurrence_id FROM approved_alert_execution_outbox'):
                        return Rows(v for (key, identity), v in db.outbox.items()
                                    if key == params[0] and identity in params[1])
                    if sql.startswith('INSERT INTO approved_alert_execution_outbox'):
                        key, identity, position, raw, sequence, canceled = params
                        old = db.outbox.get((key, identity))
                        if old and (old['canceled'] or old['source_sequence'] >= sequence): return Rows()
                        db.outbox[key, identity] = dict(source_key=key, occurrence_id=identity,
                            source_position_id=position, payload_json=raw, source_sequence=sequence,
                            canceled=canceled, acknowledged_sequence=old['acknowledged_sequence'] if old else 0)
                        return Rows([{'occurrence_id': identity}])
                    if sql.startswith('SELECT payload_json,occurrence_id FROM approved_alert_execution_outbox'):
                        found = [v for (key, _), v in db.outbox.items() if key == params[0]
                                 and v['acknowledged_sequence'] < v['source_sequence']]
                        return Rows(sorted(found, key=lambda v: (not v['canceled'], v['source_sequence']))[:params[1]])
                    if sql.startswith('UPDATE approved_alert_execution_outbox'):
                        seq, keys, identity, exact = params
                        for key in keys:
                            row = db.outbox.get((key, identity))
                            if row and row['source_sequence'] == exact and row['acknowledged_sequence'] < seq:
                                row['acknowledged_sequence'] = seq
                                if row['canceled']: row['payload_json'] = None
                        return Rows()
                    raise AssertionError('Unexpected SQL: ' + sql)
            try:
                yield Connection()
                if self.fail_commit:
                    raise RuntimeError('SIMULATED_COMMIT_FAILURE')
                self.commits += 1
            except Exception:
                self.values, self.outbox = before
                raise


def approved_fixture(spec=HYPE_LONG_TF, *, gap=False):
    f = maxpain_pending_fixture(spec, capture=False)
    db = OutboxDatabase(); db.values = deepcopy(f['database'].values)
    f['database'] = db
    p = f['state']['active'][0]
    base, entry = ms(BASE), p['entry_price']
    touched = entry - .013456789 if gap else entry
    bars = [[base+M, 100, 100.01, 99.99, 100],
            [base+2*M, touched, touched+.001, touched-.001, touched]]
    now = base+3*M+10_000
    with patch.object(store, '_connect', db.connect), patch.dict('os.environ',
            {'EXPERIMENTAL_EXECUTION_BRIDGE_MODE': bridge.APPROVED_MODE}, clear=True):
        store.advance(f['scope'], bars, now, config_sha256=CONFIG)
        f['state'] = store.snapshot(f['scope'])
    f.update(now_ms=now, approved_ms=base+3*M, bars=bars)
    return f


def projected(f, *, now=None):
    return producer.approved_messages(f['state'], now_ms=f['now_ms'] if now is None else now,
                                      fence_ms=f['fence_ms'])


def approved_env():
    return environment(EXPERIMENTAL_EXECUTION_FORWARD_MODE=transport.APPROVED_MODE,
                       EXPERIMENTAL_EXECUTION_BRIDGE_MODE=bridge.APPROVED_MODE)


class ApprovedProducerTests(unittest.TestCase):
    def test_all_five_only_after_engine_touch_and_exact_rendered_levels(self):
        for spec in SPECS.values():
            with self.subTest(coin=spec.coin):
                pending = maxpain_pending_fixture(spec, capture=False)
                self.assertEqual(projected(pending), [])
                f = approved_fixture(spec, gap=True)
                value, = projected(f)
                p = f['state']['intents'][0]['payload']
                text = render_alert(p)
                self.assertIn('רמת הכניסה במעקב נגעה במחיר: <b>'+format(p['fill_price'], '.8g')+'</b>', text)
                self.assertEqual(value['entry'], contract.price(format(p['fill_price'], '.8g')))
                self.assertNotEqual(value['entry'], contract.price(p['entry_price']))
                self.assertEqual(value['stop'], contract.price(format(p['stop_price'], '.8g')))
                self.assertEqual(value['take_profit'], contract.price(format(p['take_price'], '.8g')))
                self.assertEqual(value['approved_at'], contract.iso_ms(f['approved_ms']))
                self.assertEqual(contract.validate(value), value)
                self.assertEqual(len(f['database'].outbox), 1)

    def test_telegram_ack_or_failed_delivery_does_not_retimestamp_approval(self):
        f = approved_fixture()
        initial = projected(f)
        for status in ('PENDING', 'IN_FLIGHT', 'DELIVERED', 'UNKNOWN', 'FAILED'):
            f['state']['intents'][0].update(status=status, acknowledged_ms=f['now_ms']+15_000)
            self.assertEqual(projected(f), initial)
        self.assertEqual(projected(f, now=f['approved_ms']+90_000), [])
        self.assertEqual(producer.approved_messages(f['state'], now_ms=f['now_ms'],
                                                    fence_ms=f['approved_ms']+1), [])

    def test_source_terminal_cancels_remainder_even_after_admission_deadline(self):
        f = approved_fixture()
        original = projected(f)[0]
        p = f['state']['active'].pop()
        p.update(status='TAKE_TOUCHED', terminal_ms=f['approved_ms']+5*M)
        f['state']['history'].append(p)
        value, = projected(f, now=f['approved_ms']+6*M)
        self.assertEqual(value['kind'], 'CANCEL')
        self.assertEqual(contract.immutable(value), contract.immutable(original))
        self.assertNotIn('exchange_fill', value)

    def test_delivery_stale_price_withdrawal_cancels_open_source_after_deadline(self):
        f = approved_fixture()
        original = projected(f)[0]
        canceled_at = f['approved_ms']+2*M
        f['state']['intents'][0].update(status='CANCELLED_STALE_PRICE', acknowledged_ms=canceled_at)
        value, = projected(f, now=canceled_at)
        self.assertEqual(value['kind'], 'CANCEL')
        self.assertEqual(value['cancel_reason'], 'SOURCE_STALE_PRICE')
        self.assertEqual(value['source_as_of'], contract.iso_ms(canceled_at))
        self.assertEqual(contract.immutable(value), contract.immutable(original))

    def test_source_outbox_is_atomic_and_wake_happens_after_successful_commit(self):
        f = maxpain_pending_fixture(capture=False)
        db = OutboxDatabase(); db.values = deepcopy(f['database'].values)
        base = ms(BASE)
        bars = [[base+M, 100, 100.01, 99.99, 100], [base+2*M, 98, 98.01, 97.99, 98]]
        now = base+3*M+10_000
        def committed(key):
            self.assertGreater(db.commits, 0)
            self.assertTrue(db.outbox)
            self.assertEqual(json.loads(db.values[key])['active'][0]['status'], 'OPEN')
        with patch.object(store, '_connect', db.connect), patch.dict('os.environ',
                {'EXPERIMENTAL_EXECUTION_BRIDGE_MODE': bridge.APPROVED_MODE}, clear=True), \
                patch.object(forwarder, 'notify_source_commit', side_effect=committed) as wake:
            db.fail_commit = True
            with self.assertRaisesRegex(RuntimeError, 'COMMIT_FAILURE'):
                store.advance(f['scope'], bars, now, config_sha256=CONFIG)
            self.assertEqual(db.outbox, {}); wake.assert_not_called()
            db.fail_commit = False
            store.advance(f['scope'], bars, now, config_sha256=CONFIG)
            wake.assert_called_once()

    def test_outbox_or_wake_failure_never_rolls_back_source_decision(self):
        f = maxpain_pending_fixture(capture=False)
        db = OutboxDatabase(); db.values = deepcopy(f['database'].values)
        base = ms(BASE)
        with patch.object(store, '_connect', db.connect), patch.dict('os.environ',
                {'EXPERIMENTAL_EXECUTION_BRIDGE_MODE': bridge.APPROVED_MODE}, clear=True), \
                patch.object(outbox, 'synchronize', side_effect=RuntimeError('PRIVATE_TEST_VALUE')), \
                redirect_stdout(io.StringIO()) as output:
            store.advance(f['scope'], [[base+M, 100, 100.01, 99.99, 100],
                [base+2*M, 98, 98.01, 97.99, 98]], base+3*M+10_000, config_sha256=CONFIG)
        self.assertEqual(json.loads(db.values[f['key']])['active'][0]['status'], 'OPEN')
        self.assertNotIn('PRIVATE_TEST_VALUE', output.getvalue())
        self.assertIn('source decision preserved', output.getvalue())

    def test_durable_cancel_survives_source_history_pruning_restart_and_old_ack(self):
        f = approved_fixture(); db = f['database']; old = projected(f)[0]
        state = deepcopy(f['state']); state.update(active=[], history=[], intents=[],
                                                   bar_cursor_ms=f['approved_ms']+5*M)
        with db.connect() as conn:
            outbox.synchronize(conn, f['key'], state, f['approved_ms']+6*M)
            outbox.acknowledge(conn, [f['key']], old)
            value, = outbox.read(conn, [f['key']])
        self.assertEqual(value['kind'], 'CANCEL')
        self.assertEqual(value['occurrence_id'], old['occurrence_id'])
        with db.connect() as conn:
            outbox.acknowledge(conn, [f['key']], value)
            self.assertEqual(outbox.read(conn, [f['key']]), [])
            # A restored stale source snapshot cannot revive the tombstone.
            outbox.synchronize(conn, f['key'], f['state'], f['now_ms'])
        row, = db.outbox.values()
        self.assertTrue(row['canceled']); self.assertIsNone(row['payload_json'])

    def test_capacity_never_blocks_existing_replay_or_cancellation(self):
        f = approved_fixture(); db = f['database']; value = projected(f)[0]
        with patch.object(outbox, 'MAX_PENDING', 1), db.connect() as conn:
            outbox.acknowledge(conn, [f['key']], value)
            self.assertFalse(outbox.synchronize(conn, f['key'], f['state'], f['now_ms']))
            ended = deepcopy(f['state']); ended.update(active=[], history=[], intents=[],
                                                       bar_cursor_ms=f['approved_ms'])
            self.assertTrue(outbox.synchronize(conn, f['key'], ended, f['approved_ms']+M))
            cancel, = outbox.read(conn, [f['key']])
            self.assertEqual(cancel['kind'], 'CANCEL')
            outbox.acknowledge(conn, [f['key']], cancel)
            self.assertEqual(outbox.read(conn, [f['key']]), [])

    def test_reader_fence_and_sender_recovery_ack_only_transport_state(self):
        f = approved_fixture(); db = f['database']
        before = deepcopy(db.values)
        values = []
        def reader(scopes, fence, **kwargs):
            return bridge.read_approved(scopes, fence, connect=db.connect, **kwargs)
        def acknowledge(scopes, value, **kwargs):
            return bridge.acknowledge_approved(scopes, value, connect=db.connect, **kwargs)
        cfg = config(mode=transport.APPROVED_MODE)
        with patch('alert_cards_forwarder._source_dsn', return_value='offline-only'):
            first = transport.Sender().tick({SCOPE}, cfg, now_ms=f['now_ms'], reader=reader,
                post=lambda v, *_a, **_k: (values.append(v) or ack(v)), acknowledge=acknowledge)
            second = transport.Sender().tick({SCOPE}, cfg, now_ms=f['now_ms'], reader=reader,
                post=lambda *_a, **_k: self.fail('acknowledged approval replayed'), acknowledge=acknowledge)
        self.assertEqual((first['recorded'], second['attempted']), (1, 0))
        self.assertEqual(db.values, before)
        self.assertEqual(len(values), 1)
        self.assertTrue(all('CREATE ' not in sql and 'ALTER ' not in sql for sql, _ in db.calls))

    def test_sender_keeps_cancel_until_ack_persisted_after_restart(self):
        f = approved_fixture(); value = projected(f)[0]
        state = deepcopy(f['state']); state.update(active=[], history=[], bar_cursor_ms=f['approved_ms'])
        cancel = producer.approved_messages(state, now_ms=f['approved_ms']+M, fence_ms=f['fence_ms'])[0]
        cfg = config(mode=transport.APPROVED_MODE)
        values = []
        with patch.object(bridge, 'acknowledge_approved', side_effect=RuntimeError('SOURCE_ACK_LOST')):
            first = transport.Sender().tick({SCOPE}, cfg, now_ms=f['now_ms'], reader=lambda *_a, **_k: [cancel],
                post=lambda v, *_a, **_k: (values.append(v) or ack(v)))
        self.assertEqual(first['deferred'], 1)
        second = transport.Sender().tick({SCOPE}, cfg, now_ms=f['now_ms'], reader=lambda *_a, **_k: [cancel],
            post=lambda v, *_a, **_k: ack(v, 'DUPLICATE'), acknowledge=lambda *_a, **_k: True)
        self.assertEqual(second['duplicates'], 1)

    def test_background_starts_once_and_wakes_without_polling_delay(self):
        started, delivered = threading.Event(), threading.Event()
        calls = []
        def tick(*_a, **_k):
            calls.append(1)
            (started if len(calls) == 1 else delivered).set()
            return dict(status='COMPLETED', attempted=0)
        try:
            with patch.object(transport.Sender, 'tick', side_effect=tick), patch.object(forwarder, '_emit'):
                worker = forwarder.maybe_start(env=approved_env())
                self.assertTrue(started.wait(2))
                self.assertIs(forwarder.maybe_start(env=approved_env()), worker)
                forwarder.notify_source_commit(bridge.approved_source_keys([SCOPE])[0])
                self.assertTrue(delivered.wait(2))
                self.assertEqual(len(calls), 2)
                self.assertTrue(forwarder.stop_background(timeout=2))
        finally:
            forwarder.stop_background(timeout=2)
        self.assertIsNone(forwarder.maybe_start(env={}))


@unittest.skipUnless(os.environ.get('TEST_DATABASE_URL'), 'Explicit local/CI PostgreSQL required')
class ApprovedOutboxPostgreSQLTests(unittest.TestCase):
    """Real SQL/savepoint/locking proof, never a live or configured source DB."""
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict, make_conninfo
        from psycopg.rows import dict_row
        test_url = os.environ['TEST_DATABASE_URL']
        info = conninfo_to_dict(test_url)
        if info.get('host') not in {'localhost', '127.0.0.1', '::1', 'postgres'} or not (
                info.get('dbname', '').startswith('test_') or info.get('dbname', '').endswith('_test')):
            raise ValueError('Explicit local test database required')
        cls.name = 'test_approved_alert_' + uuid4().hex
        cls.admin = psycopg.connect(test_url, autocommit=True)
        cls.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(cls.name)))
        cls.dsn = make_conninfo(test_url, dbname=cls.name)
        cls.connect = staticmethod(lambda *_a, **_k: psycopg.connect(cls.dsn, row_factory=dict_row))
        try:
            with cls.connect() as conn:
                conn.execute('CREATE TABLE bot_settings(key text PRIMARY KEY,value text NOT NULL)')
                conn.execute((Path(__file__).parent/'migrations/050_approved_alert_execution_outbox.sql').read_text())
        except BaseException:
            cls.admin.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(cls.name)))
            cls.admin.close()
            raise

    @classmethod
    def tearDownClass(cls):
        from psycopg import sql
        try:
            cls.admin.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(cls.name)))
        finally:
            cls.admin.close()

    def test_atomic_source_commit_concurrent_replay_ack_race_and_compaction(self):
        from concurrent.futures import ThreadPoolExecutor
        f = maxpain_pending_fixture(capture=False)
        scope, key, base = f['scope'], f['key'], ms(BASE)
        with self.connect() as conn:
            conn.execute('INSERT INTO bot_settings(key,value) VALUES(%s,%s)',
                         (key, json.dumps(f['state'])))
        bars = [[base+M, 100, 100.01, 99.99, 100], [base+2*M, 98, 98.01, 97.99, 98]]
        now = base+3*M+10_000
        with patch.object(store, '_connect', self.connect), patch.dict('os.environ',
                {'EXPERIMENTAL_EXECUTION_BRIDGE_MODE': bridge.APPROVED_MODE}), \
                patch.object(forwarder, 'notify_source_commit'):
            with ThreadPoolExecutor(max_workers=4) as pool:
                list(pool.map(lambda _: store.advance(scope, bars, now, config_sha256=CONFIG), range(4)))
            state = store.snapshot(scope)
        with self.connect() as conn:
            approval, = outbox.read(conn, [key])
            self.assertEqual(approval['kind'], 'ALERT')
            self.assertEqual(conn.execute('SELECT count(*) AS count FROM approved_alert_execution_outbox').fetchone()['count'], 1)
        ended = deepcopy(state); ended.update(active=[], history=[], intents=[], bar_cursor_ms=base+4*M)
        with self.assertRaisesRegex(RuntimeError, 'ROLLBACK_SOURCE'):
            with self.connect() as conn:
                outbox.synchronize(conn, key, ended, base+5*M)
                raise RuntimeError('ROLLBACK_SOURCE')
        with self.connect() as conn:
            self.assertEqual(outbox.read(conn, [key])[0]['kind'], 'ALERT')
            with patch.object(outbox, 'MAX_PENDING', 1):
                outbox.synchronize(conn, key, ended, base+5*M)
            outbox.acknowledge(conn, [key], approval)
            cancel, = outbox.read(conn, [key])
            self.assertEqual(cancel['kind'], 'CANCEL')
            outbox.acknowledge(conn, [key], cancel)
        # New connection is the restart proof; compacted identity is permanent.
        with self.connect() as conn:
            self.assertEqual(outbox.read(conn, [key]), [])
            outbox.synchronize(conn, key, state, now)
            row = conn.execute('SELECT canceled,payload_json FROM approved_alert_execution_outbox').fetchone()
            self.assertTrue(row['canceled']); self.assertIsNone(row['payload_json'])


if __name__ == '__main__':
    unittest.main()
