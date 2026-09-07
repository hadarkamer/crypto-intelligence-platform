"""Admission and source safeguards of the bounded BTC research worker."""

import ast
import asyncio
import builtins
from datetime import timedelta
import json
import os
from pathlib import Path
from types import SimpleNamespace
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
    def test_nonempty_pass_status_and_health_are_json_safe(self):
        class LiveConnection(Connection):
            def __enter__(self): return self
            def __exit__(self,*args): return False
            def commit(self): pass
            def rollback(self): pass
            def fetchone(self):
                return {"locked":True} if "pg_try_advisory_lock" in self.calls[-1][0] else None
        subject=worker.ResearchBTCEpisodeWorker()
        candle=bar(0,100)
        with patch.dict(os.environ,{"RESEARCH_BTC_EPISODES_ENABLED":"1",
                                    "RESEARCH_DATABASE_URL":"test",
                                    "RESEARCH_BTC_EPISODE_START_UTC":START.isoformat()}), \
             patch.object(worker,"psycopg",SimpleNamespace(connect=lambda *a,**k:LiveConnection())), \
             patch.object(worker,"claim_event_page",return_value=[]):
            result=subject.run_once(now=START+timedelta(minutes=1),fetch_candles=lambda *args:{
                "exchange":"binance","market":"spot","pair":"BTCUSDT",
                "interval_seconds":60,"candles":[candle]})
        self.assertEqual(result["bars_written"],1)
        self.assertEqual(result["observed_through_utc"],candle["close_time_utc"])
        json.dumps(subject.status())
        self.assertEqual(subject.status()["metrics"]["last_result"]["observed_through_utc"],
                         candle["close_time_utc"].isoformat())
        self.assertEqual(subject.metrics["last_result"]["observed_through_utc"],
                         candle["close_time_utc"])
        # Execute the production health handler with isolated external services;
        # aiohttp's real JSON encoder must accept a completed worker status.
        from aiohttp import web
        source=ast.parse(Path(__file__).with_name("main.py").read_text())
        handler=next(node for node in source.body
                     if isinstance(node,ast.AsyncFunctionDef) and node.name=="health")
        empty=lambda:{}
        module=SimpleNamespace(status=empty,WORKER=SimpleNamespace(status=empty))
        names={node.id for node in ast.walk(handler) if isinstance(node,ast.Name)}
        namespace={name:(empty if name.endswith("_status") else module)
                   for name in names if not hasattr(builtins,name)}
        # Watch health was added after this fixture: stub its runtime state,
        # not Python's bool/sorted builtins or the handler's JSON encoder.
        namespace.update(web=web,research_btc_episode_worker=SimpleNamespace(WORKER=subject),
                         WATCH_GENERAL_ENABLED=False,WATCH_RUNTIME={},
                         MAGNET_V1_WATCHES={},WATCH_TASK=None,WATCH_SUPERVISOR_TASK=None)
        exec(compile(ast.Module(body=[handler],type_ignores=[]),"main.py","exec"),namespace)
        response=asyncio.run(namespace["health"](None))
        self.assertEqual(response.status,200)
        self.assertEqual(json.loads(response.text)["btc_episodes"]["metrics"]["last_result"]["bars_written"],1)

    def test_status_recursively_copies_datetime_lists(self):
        subject=worker.ResearchBTCEpisodeWorker()
        subject.metrics["nested"]={"items":[START,{"timestamp":START}]}
        status=subject.status()
        json.dumps(status)
        self.assertEqual(status["metrics"]["nested"]["items"][1]["timestamp"],START.isoformat())
        self.assertIs(subject.metrics["nested"]["items"][0],START)

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
