"""Relational checks for finite laps, retries, rollback and delayed delivery."""
import sqlite3
import unittest

from research_event_scan import claim_event_page, retain_unprocessed_tail


class Connection:
    def __init__(self):
        self.db = sqlite3.connect(':memory:')
        self.db.row_factory = sqlite3.Row
        self.db.execute('CREATE TABLE research_events(event_id INTEGER PRIMARY KEY,delivered INTEGER)')
        self.db.execute('''CREATE TABLE research_event_scan_cursors(
            queue_key TEXT PRIMARY KEY,last_event_id INTEGER NOT NULL DEFAULT 0,
            high_water_event_id INTEGER NOT NULL DEFAULT 0,updated_at_utc TEXT,
            CHECK(last_event_id<=high_water_event_id))''')

    def execute(self, sql, params=()):
        sql = sql.replace('%s', '?').replace(' FOR UPDATE', '').replace('NOW()', 'CURRENT_TIMESTAMP')
        return self.db.execute(sql, params)

    def page(self, key='queue', cap=2):
        return claim_event_page(self, key, limit=cap, predicate='e.delivered=%s', params=(1,))


class Tests(unittest.TestCase):
    def test_fixed_high_water_wrap_and_late_eligibility(self):
        conn = Connection()
        conn.db.executemany('INSERT INTO research_events VALUES(?,?)', [(i, i % 2 == 0) for i in range(1,9)])
        self.assertEqual(conn.page(), [2,4])
        conn.db.execute('UPDATE research_events SET delivered=1 WHERE event_id=1')
        conn.db.executemany('INSERT INTO research_events VALUES(?,1)', [(9,),(10,)])
        self.assertEqual(conn.page(), [6,8])
        state = conn.db.execute('SELECT * FROM research_event_scan_cursors').fetchone()
        self.assertEqual((state['last_event_id'],state['high_water_event_id']), (8,8))
        # A growing tail cannot postpone the old row's next lap.
        self.assertEqual(conn.page(), [1,2])
        self.assertEqual(conn.page(), [4,6])
        self.assertEqual(conn.page(), [8,9])
        self.assertEqual(conn.page(), [10])
        self.assertEqual(conn.page(), [1,2])

    def test_rollback_replays_same_page_and_keys_are_independent(self):
        conn = Connection()
        conn.db.executemany('INSERT INTO research_events VALUES(?,1)', [(i,) for i in range(1,7)])
        conn.db.commit()
        self.assertEqual(conn.page(), [1,2])
        conn.db.commit()
        self.assertEqual(conn.page(), [3,4])
        conn.db.rollback()
        self.assertEqual(conn.page(), [3,4])
        self.assertEqual(conn.page('other'), [1,2])

    def test_unprocessed_due_suffix_is_not_skipped(self):
        conn = Connection()
        conn.db.executemany('INSERT INTO research_events VALUES(?,1)', [(i,) for i in range(1,10)])
        self.assertEqual(conn.page(cap=8), list(range(1,9)))
        # Only rows 2 and 4 needed computation; 6 and 8 must remain next.
        retain_unprocessed_tail(conn, 'queue', 4)
        self.assertEqual(conn.page(cap=8), list(range(5,10)))
        with self.assertRaisesRegex(RuntimeError,'unclaimed'):
            retain_unprocessed_tail(conn, 'queue', 99)

    def test_empty_page_finishes_lap_without_excluding_future_retries(self):
        conn = Connection()
        conn.db.execute('INSERT INTO research_events VALUES(5,0)')
        self.assertEqual(conn.page(), [])
        state = conn.db.execute('SELECT * FROM research_event_scan_cursors').fetchone()
        self.assertEqual((state['last_event_id'],state['high_water_event_id']), (5,5))
        conn.db.execute('UPDATE research_events SET delivered=1')
        self.assertEqual(conn.page(cap=0), [5])


if __name__ == '__main__':
    unittest.main()
