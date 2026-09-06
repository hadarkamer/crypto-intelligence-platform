"""Admission and source safeguards of the bounded BTC research worker."""

import os
import unittest
from unittest.mock import patch

import research_btc_episode_worker as worker
import research_btc_parent_movement as policy
from research_btc_parent_movement_selftest import bar, START


class Connection:
    def __init__(self, conflict=None):
        self.calls = []
        self.conflict = conflict

    def execute(self, query, params=None):
        self.calls.append((query,params))
        return self

    def fetchone(self):
        return self.conflict

    def fetchall(self):
        return []


class WorkerTests(unittest.TestCase):
    def test_source_revision_is_rejected_before_any_write(self):
        conn = Connection(conflict={"open_time_utc":START})
        with self.assertRaisesRegex(ValueError,"revision"):
            worker._write_source_and_parents(conn,[bar(0,100)],[])
        self.assertEqual(len(conn.calls),1)
        self.assertNotIn("INSERT",conn.calls[0][0])

    def test_malformed_source_is_rejected_before_database_calls(self):
        conn = Connection()
        with self.assertRaises(ValueError):
            worker._write_source_and_parents(conn,[bar(0,100)|{"low":-1}],[])
        self.assertEqual(conn.calls,[])

    def test_parent_write_order_closes_active_before_successor(self):
        candles=[bar(i,x) for i,x in enumerate([100,102,110,107.8,110])]
        parents=policy.advance_parents(candles,as_of_utc=candles[-1]["close_time_utc"])
        conn=Connection()
        worker._write_source_and_parents(conn,candles,parents)
        writes=[params for query,params in conn.calls if "INSERT INTO research_btc_parent_movements" in query]
        self.assertEqual([params[0] for params in writes],
                         [parent["btc_parent_movement_id"] for parent in parents])
        self.assertTrue(all(params[3] is not None for params in writes[:-1]))
        self.assertIsNone(writes[-1][3])

    def test_disabled_worker_performs_no_database_work(self):
        with patch.dict(os.environ,{"RESEARCH_BTC_EPISODES_ENABLED":"0"}):
            result=worker.ResearchBTCEpisodeWorker().run_once()
        self.assertEqual(result,{"state":"DISABLED"})

    def test_research_primary_database_still_requires_explicit_route(self):
        with patch.dict(os.environ,{"RESEARCH_DATABASE_URL":"", "DATABASE_URL":"test",
                                    "RESEARCH_USE_PRIMARY_DATABASE":"0"}):
            self.assertEqual(worker._database_url(),"")


if __name__=="__main__":
    unittest.main()
