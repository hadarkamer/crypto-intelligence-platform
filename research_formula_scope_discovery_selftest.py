"""Bounded historical scope discovery, rollback and partial-cell recovery.

Run with TEST_DATABASE_URL for the actual PostgreSQL gate; without it only
the local relational tests run. Never falls back to a production DSN.
"""
from datetime import timedelta
import os
from pathlib import Path
import unittest
from uuid import uuid4

import research_formula_ordered_store as store
from research_formula_freshness_selftest import SQLiteConnection,NOW

CATALOG=store.evaluator.candidate_catalog(include_extended=True)[:3]
QUEUE=store.questions.VERSION+':scope-cell-discovery-v1'
TABLES='''
CREATE TABLE research_ordered_formula_matches (
    candidate_key TEXT,event_id BIGINT,symbol TEXT,direction TEXT,alert_time_utc TIMESTAMPTZ,
    PRIMARY KEY(candidate_key,event_id));
CREATE TABLE research_ordered_formula_scopes (
    scope_key TEXT PRIMARY KEY,candidate_key TEXT,symbol TEXT,direction TEXT,
    window_minutes INTEGER,threshold_bps INTEGER,period_key TEXT,period_start_utc TIMESTAMPTZ,
    result TEXT DEFAULT '{}',
    UNIQUE(candidate_key,symbol,direction,window_minutes,threshold_bps,period_key));
'''


class SQLiteDiscoveryConnection(SQLiteConnection):
    def __init__(self):
        super().__init__()
        self.db.execute('DROP TABLE research_ordered_formula_scopes')
        self.db.executescript(TABLES)
        self.db.execute('''CREATE TABLE research_event_scan_cursors(
            queue_key TEXT PRIMARY KEY,last_event_id BIGINT NOT NULL DEFAULT 0,
            high_water_event_id BIGINT NOT NULL DEFAULT 0,updated_at_utc TEXT,
            CHECK(last_event_id<=high_water_event_id))''')

    def cursor(self): return self
    def __enter__(self): return self
    def __exit__(self,*args): return False
    def executemany(self,sql,rows):
        for row in rows: self.execute(sql,row)


class DiscoveryChecks:
    def event(self,event_id):
        self.conn.execute('INSERT INTO research_events VALUES(%s,%s,%s,%s,%s)',
            (event_id,NOW,'ALERT','DELIVERED','LONG'))

    def match(self,event_id,candidate=0,symbol='BTC',direction='LONG',when=NOW):
        key=CATALOG[candidate]['formula_id'] if isinstance(candidate,int) else candidate
        self.conn.execute('INSERT INTO research_ordered_formula_matches VALUES(%s,%s,%s,%s,%s)',
            (key,event_id,symbol,direction,when))
        return key,symbol,direction

    def state(self):
        return self.conn.execute('SELECT last_event_id,high_water_event_id FROM research_event_scan_cursors WHERE queue_key=%s',
            (QUEUE,)).fetchone()

    def cells(self):
        return {(r['candidate_key'],r['symbol'],r['direction'],r['period_key']):r['n']
            for r in self.conn.execute('''SELECT candidate_key,symbol,direction,period_key,count(*) AS n
                FROM research_ordered_formula_scopes GROUP BY candidate_key,symbol,direction,period_key''').fetchall()}

    def test_retained_history_page_drains_with_continuous_fresh_cells(self):
        for event_id in range(1,5): self.event(event_id)
        for candidate in range(3): self.match(1,candidate)
        self.match(2,0,'ETH','SHORT')
        self.conn.commit()
        expected={(c['formula_id'],symbol,'LONG',period) for c in CATALOG
            for symbol in ('BTC','ALL') for period in store.PERIODS}
        expected|={(CATALOG[0]['formula_id'],symbol,'SHORT',period)
            for symbol in ('ETH','ALL') for period in store.PERIODS}
        for iteration in range(8):
            # The first lap remains fixed while sources continue arriving.
            if iteration:
                self.event(4+iteration)
            fresh=self.match(4+iteration,0,f'NEW{iteration}')
            summary=store.discover_scope_cells(self.conn,CATALOG,[fresh],page_limit=2,cell_limit=4)
            self.conn.commit()
            self.assertEqual(self.state()['high_water_event_id'],4)
            self.assertEqual(summary['scope_discovery_source_ids'],2)
            self.assertLessEqual(summary['scope_cells_created'],4)
            done=expected&set(self.cells())
            self.assertGreaterEqual(len(done),2*(iteration+1))
            self.assertEqual(summary['scope_discovery_cursor'],self.state()['last_event_id'])
            self.assertEqual(summary['scope_discovery_page_retained'],iteration<7)
            self.reconnect()
        self.assertTrue(expected<=set(self.cells()))
        self.assertTrue(all(self.cells()[cell]==32 for cell in expected))
        self.assertEqual(self.state()['last_event_id'],2)

    def test_rollback_replays_registration_and_later_lap_recovers_old_matches(self):
        for event_id in range(1,5): self.event(event_id)
        self.match(3)
        self.conn.commit()
        first=store.discover_scope_cells(self.conn,CATALOG,page_limit=2)
        self.assertEqual(first['scope_cells_created'],0)
        self.conn.commit()
        attempt=store.discover_scope_cells(self.conn,CATALOG,page_limit=2)
        self.assertEqual(attempt['scope_cells_created'],4)
        self.conn.rollback()
        self.assertEqual(self.cells(),{})
        self.assertEqual(self.state()['last_event_id'],2)
        replay=store.discover_scope_cells(self.conn,CATALOG,page_limit=2)
        self.assertEqual(replay,attempt)
        self.conn.commit()
        # A match inserted below the completed cursor must be discovered on
        # the next finite lap, even though it is absent from current intake.
        self.match(1,1,'SOL','SHORT')
        self.conn.commit()
        late=store.discover_scope_cells(self.conn,CATALOG,page_limit=2)
        self.assertEqual(late['scope_cells_created'],4)
        self.assertIn((CATALOG[1]['formula_id'],'SOL','SHORT',next(iter(store.PERIODS))),self.cells())

    def test_partial_cell_repairs_all_32_scopes_without_overwriting_evidence(self):
        self.event(1)
        self.match(1)
        period=next(iter(store.PERIODS))
        store.register_scopes(self.conn,[CATALOG[0]],{'BTC'},directions=['LONG'],include_all=False,period_keys=[period])
        saved=self.conn.execute('SELECT scope_key FROM research_ordered_formula_scopes ORDER BY scope_key LIMIT 1').fetchone()['scope_key']
        self.conn.execute('DELETE FROM research_ordered_formula_scopes WHERE scope_key<>%s',(saved,))
        self.conn.execute('UPDATE research_ordered_formula_scopes SET result=%s WHERE scope_key=%s',
            ('{"preserved":true}',saved))
        summary=store.discover_scope_cells(self.conn,CATALOG,page_limit=2)
        self.assertEqual(summary['scope_cells_created'],4)
        self.assertEqual(set(self.cells().values()),{32})
        self.assertEqual(self.conn.execute('SELECT result FROM research_ordered_formula_scopes WHERE scope_key=%s',
            (saved,)).fetchone()['result'],'{"preserved":true}')

    def test_page_and_catalog_bounds_do_not_claim_global_pending_count(self):
        for event_id in range(1,140): self.event(event_id)
        self.match(1,'retired-candidate')
        self.match(2,0,when=store.SOURCE_START_UTC-timedelta(seconds=1))
        self.match(139,1)
        self.conn.commit()
        first=store.discover_scope_cells(self.conn,CATALOG,page_limit=999)
        self.assertEqual(first['scope_discovery_source_ids'],128)
        self.assertEqual(first['scope_cells_created'],0)
        self.assertEqual(first['scope_cells_waiting_scope'],'CURRENT_DISCOVERY_PAGE_AND_OBSERVED_BATCH')
        self.assertEqual(self.cells(),{})
        self.conn.commit()
        second=store.discover_scope_cells(self.conn,CATALOG,page_limit=999)
        self.assertEqual(second['scope_discovery_source_ids'],11)
        self.assertEqual(second['scope_cells_created'],4)


class SQLiteDiscoveryTests(DiscoveryChecks,unittest.TestCase):
    def setUp(self): self.conn=SQLiteDiscoveryConnection()
    def tearDown(self): self.conn.close()
    def reconnect(self): pass


@unittest.skipUnless(os.environ.get('TEST_DATABASE_URL'),'TEST_DATABASE_URL required for actual PostgreSQL gate')
class PostgreSQLDiscoveryTests(DiscoveryChecks,unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict
        from psycopg.rows import dict_row
        cls.psycopg,cls.sql,cls.dict_row=psycopg,sql,staticmethod(dict_row)
        cls.dsn=os.environ['TEST_DATABASE_URL']
        info=conninfo_to_dict(cls.dsn)
        if (info.get('host') not in {'localhost','127.0.0.1','::1','postgres'}
                or not (info.get('dbname','').startswith('test_') or info.get('dbname','').endswith('_test'))):
            raise ValueError('Scope discovery integration requires an explicit local/CI test database')

    def connect(self):
        conn=self.psycopg.connect(self.dsn,row_factory=self.dict_row,connect_timeout=5,
            options='-c statement_timeout=10000 -c lock_timeout=1000')
        conn.execute(self.sql.SQL('SET search_path TO {}').format(self.sql.Identifier(self.schema)))
        conn.commit()
        return conn

    def setUp(self):
        self.schema='test_discovery_'+uuid4().hex
        self.conn=self.psycopg.connect(self.dsn,row_factory=self.dict_row,connect_timeout=5)
        self.conn.execute(self.sql.SQL('CREATE SCHEMA {}').format(self.sql.Identifier(self.schema)))
        self.conn.execute(self.sql.SQL('SET search_path TO {}').format(self.sql.Identifier(self.schema)))
        self.conn.execute('''CREATE TABLE research_events(event_id BIGINT PRIMARY KEY,alert_time_utc TIMESTAMPTZ,
            event_kind TEXT,delivery_status TEXT,direction TEXT)''')
        self.conn.execute(TABLES,prepare=False)
        migration=Path(__file__).parent/'migrations'/'038_runtime_event_scan_bounds.sql'
        self.conn.execute(migration.read_text(),prepare=False)
        self.conn.commit()

    def reconnect(self):
        self.conn.close()
        self.conn=self.connect()

    def tearDown(self):
        self.conn.close()
        with self.psycopg.connect(self.dsn,connect_timeout=5,autocommit=True) as conn:
            conn.execute(self.sql.SQL('DROP SCHEMA {} CASCADE').format(self.sql.Identifier(self.schema)))


if __name__=='__main__': unittest.main()
