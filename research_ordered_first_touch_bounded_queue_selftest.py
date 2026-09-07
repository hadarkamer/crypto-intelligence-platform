"""Execute bounded v7 repair/open queues against their unbounded baseline.

SQLite supplies the relational semantics.  Only PostgreSQL clock expressions
are translated into numeric minutes; predicates, row ordering, prefix limits,
grouping and the final event limit come from the actual worker SQL.
"""

from __future__ import annotations

from pathlib import Path
import random
import re
import sqlite3
from types import SimpleNamespace

import research_formula_schema_admin
import research_outcome_worker as worker


ROOT = Path(__file__).resolve().parent
NOW = 100_000


def _captured_query():
    calls = []
    connection = SimpleNamespace(
        execute=lambda query, params: calls.append((query, params))
        or SimpleNamespace(fetchall=lambda: [])
    )
    worker.ResearchOutcomeWorker._load_ordered_first_touch_due_events(
        connection, 8
    )
    return calls[0][0]


def _lane_queries(query, lane):
    following = "missing_events" if lane == "open_events" else "pooled"
    body = query.split(f"{lane} AS MATERIALIZED (", 1)[1].split(
        f"), {following} AS", 1
    )[0]
    body = " ".join(body.split())
    body = body.replace(
        "NOW() - ( (SELECT backfill_days FROM settings) * INTERVAL '1 day' )",
        f"{NOW} - (SELECT backfill_days FROM settings) * 1440",
    ).replace(
        "NOW() - INTERVAL '6 hours'", f"{NOW} - 360"
    ).replace(
        "LEAST( e.alert_time_utc + ordered.window_minutes * INTERVAL '1 minute', "
        "date_trunc('minute', NOW()) - INTERVAL '1 millisecond' )",
        f"MIN(e.alert_time_utc + ordered.window_minutes, {NOW} - 1.0/60000)",
    )
    assert "NOW()" not in body and "INTERVAL" not in body
    assert "WHERE ordered.method_version='ordered-first-touch-v7'" in body
    assert "AND ordered.method_version=( SELECT method_version FROM settings )" in body
    assert "LIMIT (SELECT batch_limit * expected_rows FROM settings)" in body
    prefix = (
        "WITH settings(method_version,backfill_days,expected_rows,batch_limit) "
        "AS (VALUES (?,?,?,?)) "
    )
    baseline = re.sub(
        r"ORDER BY ordered\.(observed_through_utc|updated_at_utc), "
        r"ordered\.event_id, ordered\.window_minutes, ordered\.threshold_bps "
        r"LIMIT \(SELECT batch_limit \* expected_rows FROM settings\)",
        "", body,
    )
    assert baseline != body
    return prefix + body, prefix + baseline


def _check_schema_contract():
    schema = " ".join((ROOT / "migrations/020_ordered_first_touch_v7.sql")
                      .read_text(encoding="utf-8").split())
    assert "window_minutes IN (60, 240, 720, 1440)" in schema
    assert "threshold_bps IN (25, 50, 75, 100, 125, 150, 175, 200)" in schema
    assert "method_version = 'ordered-first-touch-v7'" in schema
    assert "PRIMARY KEY ( event_id, window_minutes, threshold_bps, method_version )" in schema
    assert worker._ORDERED_FIRST_TOUCH_ROW_COUNT == 4 * 8
    migration = ROOT / "migrations/024_ordered_first_touch_repair_queue.sql"
    sql = " ".join(migration.read_text(encoding="utf-8").split())
    assert migration.resolve() in research_formula_schema_admin.MIGRATION_PATHS
    assert "CREATE INDEX IF NOT EXISTS idx_ordered_first_touch_missing_queue" in sql
    assert "( updated_at_utc, event_id, window_minutes, threshold_bps )" in sql
    assert "WHERE method_version = 'ordered-first-touch-v7' AND status = 'DATA_MISSING'" in sql
    # The operational index migration must not rewrite existing evidence.
    raw = migration.read_text(encoding="utf-8")
    executable = "\n".join(line for line in raw.splitlines()
                           if not line.lstrip().startswith("--"))
    assert len([part for part in executable.split(";") if part.strip()]) == 1
    assert executable.strip().startswith("CREATE INDEX IF NOT EXISTS")


def run():
    _check_schema_contract()
    query = _captured_query()
    rng = random.Random(721)
    windows = (60, 240, 720, 1440)
    thresholds = tuple(range(25, 201, 25))
    method = worker._ORDERED_FIRST_TOUCH_METHOD_VERSION
    for lane, status in (("open_events", "OPEN"),
                         ("missing_events", "DATA_MISSING")):
        bounded, baseline = _lane_queries(query, lane)
        with sqlite3.connect(":memory:") as conn:
            conn.executescript("""
                CREATE TABLE research_events (
                    event_id INTEGER PRIMARY KEY, alert_time_utc REAL
                );
                CREATE TABLE research_ordered_first_touch_outcomes (
                    event_id INTEGER, window_minutes INTEGER,
                    threshold_bps INTEGER, method_version TEXT, status TEXT,
                    observed_through_utc REAL, updated_at_utc REAL,
                    PRIMARY KEY(event_id,window_minutes,threshold_bps,method_version)
                );
            """)

            def add(event_id, *, row_count=32, event_time=NOW - 1000,
                    observed=None, updated=None, row_status=status,
                    row_method=method):
                conn.execute("INSERT INTO research_events VALUES (?,?)",
                             (event_id, event_time))
                records = []
                for window, threshold in [(w, t) for w in windows
                                          for t in thresholds][:row_count]:
                    observed_at = (event_time + 5 if observed is None else observed)
                    updated_at = NOW - 500 if updated is None else updated
                    records.append((event_id, window, threshold, row_method,
                                    row_status, observed_at, updated_at))
                rng.shuffle(records)
                conn.executemany("INSERT INTO research_ordered_first_touch_outcomes "
                                 "VALUES (?,?,?,?,?,?,?)", records)

            # More than a full batch of earlier rows fail source/time filters.
            # Capping before eligibility would wrongly hide every valid event.
            for event_id in range(70):
                add(event_id, event_time=NOW - 15 * 1440, updated=NOW - 800)
            for event_id in range(70, 80):
                add(event_id, observed=NOW, updated=NOW - 359)
            add(80, row_status="SUCCESS", updated=NOW - 800)
            add(81, row_method="audit-v6", updated=NOW - 800)

            # Full 32-row events tied at the earliest eligible queue time.
            # A batch-row cap (instead of batch*32) would return only one event.
            for event_id in range(100, 180):
                add(event_id)
            for limit in (1, 2, 8, 64):
                params = (method, 14, 32, limit)
                actual = conn.execute(bounded, params).fetchall()
                assert actual == conn.execute(baseline, params).fetchall()
                assert [row[0] for row in actual] == list(range(100, 100 + limit))

            # Partial and interleaved observations keep their minimum queue
            # timestamp even if the bounded prefix cuts an event's later rows.
            conn.execute("DELETE FROM research_ordered_first_touch_outcomes")
            conn.execute("DELETE FROM research_events")
            for event_id in range(200):
                add(event_id, row_count=rng.randint(1, 32),
                    event_time=NOW - rng.randint(800, 1500))
            for event_id in range(200):
                conn.execute(
                    "UPDATE research_ordered_first_touch_outcomes "
                    "SET observed_through_utc=observed_through_utc+threshold_bps/25, "
                    "updated_at_utc=?+threshold_bps/25 WHERE event_id=?",
                    (NOW - rng.randint(380, 1000), event_id),
                )
            for limit in (1, 2, 8, 64):
                params = (method, 14, 32, limit)
                assert conn.execute(bounded, params).fetchall() == (
                    conn.execute(baseline, params).fetchall()
                )
                assert conn.execute(bounded, ("future-v8", 14, 32, limit)).fetchall() == []

            conn.execute("DELETE FROM research_ordered_first_touch_outcomes")
            assert conn.execute(bounded, (method, 14, 32, 8)).fetchall() == []
    print("ordered First Touch v7 bounded queue self-test: PASS")


if __name__ == "__main__":
    run()
