"""Relational freshness/fairness tests; optional actual PostgreSQL 040 gate.

CI must set TEST_DATABASE_URL to an explicit local test database. No production
DSN fallback or network transport is used. SQLite runs the same selection SQL
locally, adapting only PostgreSQL placeholders, ANY and transaction syntax.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import re
import sqlite3
import unittest
from uuid import uuid4

import research_formula_ordered_store as store

NOW = datetime(2026, 9, 7, 12, tzinfo=timezone.utc)
PERIOD = next(iter(store.PERIODS))
SCHEMA = """
CREATE TABLE research_events (
    event_id BIGINT PRIMARY KEY, alert_time_utc TIMESTAMPTZ NOT NULL,
    event_kind TEXT NOT NULL, delivery_status TEXT NOT NULL, direction TEXT NOT NULL);
CREATE TABLE research_ordered_feature_screens (
    event_id BIGINT NOT NULL, feature_version TEXT NOT NULL,
    PRIMARY KEY(event_id,feature_version));
CREATE TABLE research_ordered_formula_scopes (
    scope_key TEXT PRIMARY KEY, candidate_key TEXT NOT NULL, period_key TEXT NOT NULL,
    last_evaluated_at_utc TIMESTAMPTZ, result TEXT NOT NULL);
"""


class SQLiteConnection:
    def __init__(self):
        self.db = sqlite3.connect(':memory:')
        self.db.row_factory = sqlite3.Row
        self.db.create_function('NOW', 0, lambda: NOW.isoformat())
        self.db.executescript(SCHEMA)
        self.db.execute('''CREATE TABLE research_ordered_scope_schedule_state (
            scheduler_key TEXT PRIMARY KEY, next_ticket BIGINT NOT NULL DEFAULT 0,
            updated_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)''')
        self.calls = []

    def execute(self, sql, params=()):
        self.calls.append((sql, params))
        args = iter(params)
        flattened = []

        def parameter(match):
            value = next(args)
            if match.group() == 'ANY(%s)':
                flattened.extend(value)
                return 'IN (' + ','.join('?' for _ in value) + ')'
            flattened.append(value.isoformat() if isinstance(value, datetime) else value)
            return '?'

        translated = re.sub(r'ANY\(%s\)|%s', parameter, sql).replace('=IN ', ' IN ')
        translated = translated.replace(' FOR UPDATE', '')
        return self.db.execute(translated, flattened)

    def commit(self): self.db.commit()
    def rollback(self): self.db.rollback()
    def close(self): self.db.close()


class RelationalFreshnessChecks:
    def scope(self, key, evaluated=None, *, candidate='current', period=PERIOD):
        self.conn.execute('''INSERT INTO research_ordered_formula_scopes
            VALUES(%s,%s,%s,%s,%s)''', (key, candidate, period, evaluated, '{"frozen":true}'))

    def event(self, event_id, *, when=NOW, kind='ALERT', delivered='DELIVERED', direction='LONG'):
        self.conn.execute('INSERT INTO research_events VALUES(%s,%s,%s,%s,%s)',
            (event_id, when, kind, delivered, direction))

    def screen(self, event_id, version=None):
        self.conn.execute('INSERT INTO research_ordered_feature_screens VALUES(%s,%s)',
            (event_id, version or store.questions.VERSION))

    def test_live_siblings_progress_after_already_screened_magnet_tail(self):
        for event_id in range(1, 351):
            self.event(event_id)
        for event_id in range(319, 351):
            self.screen(event_id)
        # A v2 screen is not evidence of the current catalog's coverage.
        self.screen(318, 'retired-feature-catalog')
        self.event(351, when=NOW+timedelta(seconds=1))
        self.event(352, delivered='UNKNOWN')
        self.event(353, kind='DECISION_SAMPLE')
        self.event(354, direction='NEUTRAL')
        self.event(355, when=store.SOURCE_START_UTC-timedelta(seconds=1))
        first = store.recent_unscreened_event_ids(self.conn, now=NOW, limit=128)
        self.assertEqual(first, list(range(318, 286, -1)))
        for event_id in first:
            self.screen(event_id)
        self.assertEqual(store.recent_unscreened_event_ids(self.conn, now=NOW, limit=5),
            [286, 285, 284, 283, 282])
        for event_id in range(95, 287):
            self.screen(event_id)
        self.assertEqual(store.recent_unscreened_event_ids(self.conn, now=NOW), [])
        # Exhausting the finite tail must not be treated as global coverage.
        self.assertEqual(self.conn.execute('''SELECT count(*) AS n FROM research_events e
            WHERE event_id<95 AND NOT EXISTS (SELECT 1 FROM research_ordered_feature_screens s
                WHERE s.event_id=e.event_id AND s.feature_version=%s)''',
                (store.questions.VERSION,)).fetchone()['n'], 94)

    def test_null_backfill_and_old_refresh_share_one_scope_budget_after_restart(self):
        for i in range(6):
            self.scope(f'old-{i}', NOW-timedelta(days=2, minutes=6-i))
        self.scope('new-0')
        self.scope('obsolete', NOW-timedelta(days=10), candidate='old-catalog')
        self.scope('legacy', NOW-timedelta(days=10), period='LEGACY_UNSCOPED')
        self.conn.commit()
        seen = []
        for ticket in range(6):
            # New NULL rows keep arriving faster than evaluation completes.
            self.scope(f'new-{ticket+1}')
            self.conn.commit()
            batch = store.due_scopes(self.conn, 1, candidate_keys=['current'])
            self.assertEqual(batch.schedule_ticket, ticket)
            chosen = batch[0]['scope_key']
            seen.append(chosen)
            self.assertEqual(chosen.startswith('old-'), ticket % 2 == 0)
            self.assertEqual(set(batch[0].keys()), {'scope_key','candidate_key','period_key',
                'last_evaluated_at_utc','result'})
            self.conn.execute('UPDATE research_ordered_formula_scopes SET last_evaluated_at_utc=%s WHERE scope_key=%s',
                (NOW+timedelta(minutes=ticket), chosen))
            # Persisted timestamps can jump across a clock correction; the
            # starting lane is determined by a committed ticket, not the clock.
            self.conn.execute('UPDATE research_ordered_scope_schedule_state SET updated_at_utc=%s',
                (NOW+timedelta(days=100 if ticket % 2 else -100),))
            self.conn.commit()
            self.reconnect()
        self.assertEqual(seen[::2], ['old-0', 'old-1', 'old-2'])
        self.assertEqual(seen[1::2], ['new-0', 'new-1', 'new-2'])

    def test_rollback_replays_ticket_and_unselected_scopes_stay_pending(self):
        for i in range(4):
            self.scope(f'new-{i}')
            self.scope(f'old-{i}', NOW-timedelta(hours=4-i))
        self.conn.commit()
        first = store.due_scopes(self.conn, 4, candidate_keys=['current'])
        self.assertEqual([r['scope_key'] for r in first], ['old-0','new-0','old-1','new-1'])
        self.conn.rollback()
        again = store.due_scopes(self.conn, 4, candidate_keys=['current'])
        self.assertEqual(again.schedule_ticket, first.schedule_ticket)
        self.assertEqual(list(again), list(first))
        self.conn.commit()
        following = store.due_scopes(self.conn, 4, candidate_keys=['current'])
        self.assertEqual([r['scope_key'] for r in following], ['new-0','old-0','new-1','old-1'])
        self.assertEqual(self.conn.execute('''SELECT count(*) AS n FROM research_ordered_formula_scopes
            WHERE last_evaluated_at_utc IS NULL''').fetchone()['n'], 4)

    def test_empty_lane_uses_full_budget_and_empty_catalog_has_no_ticket(self):
        for i in range(8):
            self.scope(f'new-{i}')
        self.assertEqual(store.due_scopes(self.conn, 4, candidate_keys=[]), [])
        self.assertEqual(self.conn.execute('SELECT count(*) AS n FROM research_ordered_scope_schedule_state').fetchone()['n'], 0)
        first = store.due_scopes(self.conn, 4, candidate_keys=['current'])
        self.assertEqual([r['scope_key'] for r in first], ['new-0','new-1','new-2','new-3'])
        self.assertEqual(len({r['scope_key'] for r in first}), 4)


class SQLiteFreshnessTests(RelationalFreshnessChecks, unittest.TestCase):
    def setUp(self): self.conn = SQLiteConnection()
    def tearDown(self): self.conn.close()
    def reconnect(self):
        # SQLite's in-memory database survives on this connection. Scheduler
        # objects are recreated each call; PostgreSQL below reconnects for real.
        pass


class ExperimentalFairnessTests(unittest.TestCase):
    def test_priority_preserves_both_ordinary_lanes_with_single_scope_budget(self):
        refresh = {'scope_key':'old','result':{'frozen':True}}
        initial = {'scope_key':'new','result':{'frozen':True}}
        priority = [{'scope_key':f'qualified-{i}'} for i in range(8)]
        original = deepcopy([refresh, initial, priority])
        first = []
        for ticket in range(6):
            ordinary = [refresh, initial] if ticket % 2 == 0 else [initial, refresh]
            batch = store.ScopeScheduleBatch(ordinary, ticket)
            merged = store.interleave_experimental_refresh(batch, priority, limit=10)
            first.append(merged[0]['scope_key'])
            self.assertEqual(len(merged), 10)
            self.assertEqual(len({r['scope_key'] for r in merged}), 10)
        self.assertEqual(first, ['old','new','qualified-0','new','old','qualified-0'])
        self.assertEqual([refresh, initial, priority], original)
        overlapping = store.interleave_experimental_refresh(
            store.ScopeScheduleBatch([refresh, initial], 0), [refresh, refresh], limit=2)
        self.assertEqual({r['scope_key'] for r in overlapping}, {'old','new'})


@unittest.skipUnless(os.environ.get('TEST_DATABASE_URL'), 'TEST_DATABASE_URL required for actual PostgreSQL gate')
class PostgreSQLFreshnessTests(RelationalFreshnessChecks, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict
        from psycopg.rows import dict_row
        cls.psycopg, cls.sql, cls.dict_row = psycopg, sql, staticmethod(dict_row)
        cls.dsn = os.environ['TEST_DATABASE_URL']
        info = conninfo_to_dict(cls.dsn)
        if (info.get('host') not in {'localhost','127.0.0.1','::1','postgres'}
                or not (info.get('dbname','').startswith('test_') or info.get('dbname','').endswith('_test'))):
            raise ValueError('Freshness integration requires an explicit local/CI test database')

    def connect(self):
        conn = self.psycopg.connect(self.dsn, row_factory=self.dict_row, connect_timeout=5,
            options='-c statement_timeout=10000 -c lock_timeout=1000')
        conn.execute(self.sql.SQL('SET search_path TO {}').format(self.sql.Identifier(self.schema)))
        conn.commit()
        return conn

    def setUp(self):
        self.schema = 'test_freshness_' + uuid4().hex
        self.conn = self.psycopg.connect(self.dsn, row_factory=self.dict_row, connect_timeout=5)
        self.conn.execute(self.sql.SQL('CREATE SCHEMA {}').format(self.sql.Identifier(self.schema)))
        self.conn.execute(self.sql.SQL('SET search_path TO {}').format(self.sql.Identifier(self.schema)))
        self.conn.execute(SCHEMA, prepare=False)
        migration = Path(__file__).parent / 'migrations' / '040_formula_scope_fairness.sql'
        for _ in range(2):  # Real migration must be idempotent.
            self.conn.execute(migration.read_text(), prepare=False)
        self.conn.commit()

    def reconnect(self):
        self.conn.close()
        self.conn = self.connect()

    def test_scope_lanes_use_bounded_existing_index_without_sorting_backlog(self):
        # Reproduce both a large never-evaluated lane and an old refresh lane.
        # The production index has NULLS FIRST. An implicit NULLS LAST ORDER
        # forces sorting the refresh backlog despite its IS NOT NULL filter.
        self.conn.execute('''CREATE INDEX test_scope_schedule ON
            research_ordered_formula_scopes(last_evaluated_at_utc ASC NULLS FIRST,scope_key)
            WHERE period_key IN ('ALL_COMPATIBLE_SINCE_20260816','SINCE_20260904')''')
        self.conn.execute('''INSERT INTO research_ordered_formula_scopes
            SELECT md5(i::text),'current',%s,
                   CASE WHEN i%%2=0 THEN %s::timestamptz-i*INTERVAL '1 second' END,'{}'
            FROM generate_series(1,20000) AS series(i)''', (PERIOD,NOW))
        self.conn.execute('ANALYZE research_ordered_formula_scopes')
        self.conn.commit()
        selected_queries=[]
        connection=self.conn
        class Capture:
            def execute(self,sql,params=()):
                if sql.startswith('SELECT scope_key FROM research_ordered_formula_scopes'):
                    selected_queries.append((sql,params))
                return connection.execute(sql,params)
        batch=store.due_scopes(Capture(),8,candidate_keys=['current'])
        self.assertEqual(len(batch),8)
        self.assertEqual(len(selected_queries),2)
        for sql,params in selected_queries:
            plan=self.conn.execute('EXPLAIN (ANALYZE,FORMAT JSON) '+sql,params).fetchone()['QUERY PLAN'][0]['Plan']
            def nodes(node):
                return [node]+[child for nested in node.get('Plans',[]) for child in nodes(nested)]
            scanned=nodes(plan)
            self.assertEqual(plan['Actual Rows'],8)
            self.assertFalse(any(node['Node Type'] in {'Sort','Incremental Sort','Seq Scan'} for node in scanned))
            index=next(node for node in scanned if node.get('Index Name')=='test_scope_schedule')
            self.assertLessEqual(index['Actual Rows'],8)

    def tearDown(self):
        self.conn.close()
        with self.psycopg.connect(self.dsn, connect_timeout=5, autocommit=True) as conn:
            conn.execute(self.sql.SQL('DROP SCHEMA {} CASCADE').format(self.sql.Identifier(self.schema)))


if __name__ == '__main__': unittest.main()
