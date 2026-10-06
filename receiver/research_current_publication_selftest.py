"""Latest frozen slots, real SQL bootstrap, retention and original queue states."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import unittest
from uuid import uuid4

import google_sheets_sync as sheets
from google_sheets_sync_selftest import Event, NeutralEvent
import research_current_publication as current
import research_sheet_outbox as outbox
from research_outcome_publication_selftest import SlotDatabase
from research_sheet_fresh_delivery_selftest import _database, _add, NOW


def snapshot(identity='s1', timestamp='2026-09-13T12:00:00.123456Z', **fields):
    return {'sheet': 'Snapshots', 'key': 'snapshot_id', 'row': {
        'snapshot_id': identity, 'timestamp_utc': timestamp,
        'symbol': 'BTC', 'direction': 'LONG', **fields}}


class CurrentTests(unittest.TestCase):
    def test_real_producers_project_complete_rows_without_mixing_sources(self):
        event = sheets.build_delivered_event_payload(Event().to_dict())
        original = deepcopy(event)
        timestamp = next(i['row']['timestamp_utc'] for i in event['upserts'] if i['sheet'] == 'Snapshots')
        for item in event['upserts']:
            if item['sheet'] not in current.CONFIG:
                continue
            projected = current.project(item, snapshot_time=timestamp)
            config = current.BY_SHEET[projected['sheet']]
            self.assertEqual(set(projected['row']), set(config['headers']))
            self.assertEqual(current.source_time(projected['source_time_utc']), current.source_time(timestamp))
            self.assertEqual(current.project(projected), projected)
        self.assertEqual(event, original)
        # Neutral rows clear absent alert measurements instead of inheriting
        # whichever delivered event previously occupied the same slot.
        neutral = sheets.build_neutral_snapshot_payload(NeutralEvent().to_dict(), decision_feature_bundle={})
        candidate = next(i for i in neutral['upserts'] if i['sheet'] == 'Snapshots')
        row = current.project(candidate)['row']
        self.assertIsNone(row['price_oi_total_score'])
        self.assertTrue(row['no_alert_snapshot'])
        for fields in ({'symbol':'ADA'}, {'direction':'NEUTRAL'}, {'timestamp_utc':'2026-09-13'}, {'unbudgeted':1}):
            with self.assertRaises(ValueError):
                current.project(snapshot(**fields))

    def test_ordering_repairs_and_stale_ack_keep_the_new_generation(self):
        db = SlotDatabase(_database().conn)
        old = current.project(snapshot())
        self.assertEqual(current.stage_projected(db, old), 1)
        self.assertEqual(current.stage_projected(db, old), 0)
        claim = outbox._claim_lane(db, count=1, token='claimed', sheet='Snapshots_Current', recent=False)[0]
        newer = current.project(snapshot('s2', '2026-09-13T12:00:00.123457Z', reference_price=200))
        self.assertEqual(current.stage_projected(db, newer), 1)
        self.assertEqual(current.stage_projected(db, old), 0)
        stored = dict(db.conn.execute('SELECT * FROM research_sheet_upsert_outbox').fetchone())
        self.assertEqual(json.loads(stored['payload'])['row']['snapshot_id'], 's2')
        self.assertIsNone(stored['claim_token'])
        self.assertEqual(stored['sync_status'], 'PENDING')
        self.assertNotEqual(stored['payload_sha256'], claim['payload_sha256'])
        repaired = current.project(snapshot('s2', '2026-09-13T12:00:00.123457Z', reference_price=None))
        self.assertEqual(current.stage_projected(db, repaired), 1)
        self.assertIsNone(json.loads(db.conn.execute('SELECT payload FROM research_sheet_upsert_outbox').fetchone()[0])['row']['reference_price'])
        self.assertEqual(outbox.stage_upserts(db, [snapshot('s0')]), 0)

    def test_held_history_and_expired_telegram_are_never_claimed(self):
        db = _database()
        for name in current.CONFIG:
            _add(db, name, 'retained', NOW, sync_status='RETRY', attempts=7)
        _add(db, 'Telegram_Events', 'expired', NOW - 16 * 86400 - 1)
        _add(db, 'Telegram_Events', 'undated', None)
        before = [tuple(row) for row in db.conn.execute('SELECT * FROM research_sheet_upsert_outbox')]
        self.assertEqual(outbox._claim_batch(db, 8, 'none'), [])
        self.assertEqual(before, [tuple(row) for row in db.conn.execute('SELECT * FROM research_sheet_upsert_outbox')])
        _add(db, 'Telegram_Events', 'boundary', NOW - 16 * 86400)
        self.assertEqual([row['row_key'] for row in outbox._claim_batch(db, 8, 'fresh')], ['boundary'])


@unittest.skipUnless(os.getenv('TEST_DATABASE_URL'), 'Local/CI TEST_DATABASE_URL required')
class PostgreSQLCurrentTests(unittest.TestCase):
    def setUp(self):
        import psycopg
        from psycopg.conninfo import conninfo_to_dict
        from psycopg.rows import dict_row
        dsn = os.environ['TEST_DATABASE_URL']
        info = conninfo_to_dict(dsn)
        if (info.get('host') not in {'localhost','127.0.0.1','::1','postgres'} or
                not (info.get('dbname','').startswith('test_') or info.get('dbname','').endswith('_test'))):
            raise ValueError('Explicit local test database required')
        self.conn = psycopg.connect(dsn, row_factory=dict_row, options='-c statement_timeout=15000')
        self.addCleanup(self.conn.close)
        self.addCleanup(self.conn.rollback)
        schema = 'test_current_' + uuid4().hex
        self.conn.execute('CREATE SCHEMA ' + schema)
        self.conn.execute('SET search_path TO ' + schema)
        root = Path(__file__).parent / 'migrations'
        source = (root / '022_ordered_formula_research.sql').read_text()
        table = source.split('CREATE TABLE IF NOT EXISTS research_sheet_upsert_outbox',1)[1].split(';',1)[0]
        self.conn.execute('CREATE TABLE research_sheet_upsert_outbox' + table)
        self.conn.execute((root / '026_research_sheet_fresh_delivery.sql').read_text(), prepare=False)

    def test_production_sql_bootstrap_preserves_history_and_newer_live_slots(self):
        for identity, timestamp in [('old','2026-09-13T11:00:00Z'), ('recent','2026-09-13T12:00:00Z')]:
            item = snapshot(identity, timestamp)
            key = json.dumps([identity], separators=(',', ':'))
            live = {'sheet':'תצוגת לייב','key':'snapshot_id','row':{
                'snapshot_id':identity,'מטבע':'BTC','כיוון נבדק':'LONG','זמן סריקה':'display only'},
                'source_time_utc':timestamp}
            for payload in (item, live):
                self.conn.execute('''INSERT INTO research_sheet_upsert_outbox
                    (sheet_name,row_key,payload,payload_sha256,sync_status,attempts)
                    VALUES(%s,%s,%s::jsonb,%s,'RETRY',7)''',
                    (payload['sheet'],key,json.dumps(payload),identity))
        before = self.conn.execute('SELECT * FROM research_sheet_upsert_outbox ORDER BY sheet_name,row_key').fetchall()
        self.assertEqual(current.seed_legacy(self.conn), 2)
        self.assertEqual(current.seed_legacy(self.conn), 0)
        for row in self.conn.execute("SELECT * FROM research_sheet_upsert_outbox WHERE sheet_name=ANY(%s)", (list(current.SHEETS),)):
            self.assertEqual(row['payload']['row']['snapshot_id'], 'recent')
            self.assertEqual(row['source_time_utc'], datetime(2026,9,13,12,tzinfo=timezone.utc))
            self.assertEqual(row['sync_status'], 'PENDING')
        self.assertEqual(current.stage_projected(self.conn, current.project(snapshot('new','2026-09-13T13:00:00Z'))), 1)
        self.assertEqual(current.seed_legacy(self.conn), 0)
        self.assertEqual(current.stage_projected(self.conn, current.project(snapshot())), 0)
        after = self.conn.execute('SELECT * FROM research_sheet_upsert_outbox WHERE sheet_name=ANY(%s) ORDER BY sheet_name,row_key', (list(current.CONFIG),)).fetchall()
        self.assertEqual(before, after)
        now = datetime.now(timezone.utc)
        for identity, timestamp in [('expired',now-timedelta(days=17)), ('fresh',now-timedelta(days=1))]:
            outbox.stage_upserts(self.conn,[{'sheet':'Telegram_Events','key':'event_id','row':{
                'event_id':identity,'timestamp_utc':timestamp.isoformat()}}])
        rows = outbox._claim_lane(self.conn, count=8, token=str(uuid4()), sheet='Telegram_Events', recent=False)
        self.assertEqual([row['row_key'] for row in rows], ['["fresh"]'])


if __name__ == '__main__':
    unittest.main()
