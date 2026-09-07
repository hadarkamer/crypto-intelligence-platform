"""Outbox claim/index parity without touching a production database.

Execute the actual selection in SQLite after translating only NOW(), the
parameter marker and PostgreSQL's locking suffix. This proves the redundant
index guard leaves eligibility, global fairness and full row identities
unchanged; locking/lease clauses are asserted against the original contract.
SQLite does not simulate PostgreSQL's concurrent row locks.
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
INDEX_GUARD = "AND sync_status IN ('PENDING', 'RETRY', 'IN_FLIGHT')"
ORDER = ("next_attempt_at_utc, created_at_utc, event_id, "
         "window_minutes, threshold_bps")


def _claim_query(limit):
    calls = []
    connection = SimpleNamespace(
        execute=lambda query, params: calls.append((query, params))
        or SimpleNamespace(fetchall=lambda: [])
    )
    assert worker.ResearchOutcomeWorker._claim_ordered_first_touch_outbox(
        connection, limit
    ) == []
    assert len(calls) == 1  # Selection and lease are one atomic statement.
    query, params = calls[0]
    query = re.sub(r"--[^\n]*", "", query)
    return " ".join(query.split()), params


def _check_lease_contract(query):
    assert query.count("FOR UPDATE SKIP LOCKED") == 1
    assert query.count("UPDATE research_ordered_first_touch_sync_outbox") == 1
    assert "FOR UPDATE SKIP LOCKED LIMIT %s" in query
    assert f"ORDER BY {ORDER}" in query
    assert "attempts=queued.attempts + 1" in query
    assert "claim_token=gen_random_uuid()" in query
    assert "claimed_payload_sha256=queued.payload_sha256" in query
    assert "lease_expires_at_utc=NOW() + INTERVAL '2 minutes'" in query
    assert "last_attempt_at_utc=NOW()" in query
    assert "claimed_at_utc=NOW()" in query
    assert "next_attempt_at_utc=NOW()" in query
    assert "synced_at_utc=NULL" in query and "last_error=NULL" in query
    for key in ("event_id", "window_minutes", "threshold_bps",
                "method_version", "destination"):
        assert f"queued.{key}=picked.{key}" in query
    assert "queued.payload, queued.attempts, queued.claim_token, " in query
    # No split lanes: no extra rows locked, duplicates or fairness change from
    # independently limiting the retry and expired-lease selections.
    assert "UNION" not in query
    assert query.count("LIMIT %s") == 1


def _check_index_contract():
    path = ROOT / "migrations/025_ordered_first_touch_sync_claim_queue.sql"
    assert path.resolve() in research_formula_schema_admin.MIGRATION_PATHS
    executable = re.sub(r"--[^\n]*", "", path.read_text(encoding="utf-8"))
    sql = " ".join(executable.split())
    assert len([item for item in sql.split(";") if item.strip()]) == 1
    assert sql.startswith("CREATE INDEX IF NOT EXISTS "
                          "idx_ordered_first_touch_sync_claim_queue")
    assert "destination, next_attempt_at_utc ASC, created_at_utc ASC, " \
           "event_id, window_minutes, threshold_bps" in sql
    assert "WHERE sync_status IN ('PENDING', 'RETRY', 'IN_FLIGHT')" in sql
    return executable


def run():
    index_sql = _check_index_contract()
    query, params = _claim_query(999)
    assert params == (worker._ORDERED_FIRST_TOUCH_OUTBOX_LIMIT,)
    assert _claim_query(0)[1] == (1,)
    assert _claim_query(-8)[1] == (1,)
    assert _claim_query(8)[1] == (8,)
    _check_lease_contract(query)
    picked = query.split("WITH picked AS (", 1)[1].split(
        ") UPDATE research_ordered_first_touch_sync_outbox", 1
    )[0]
    assert INDEX_GUARD in picked
    original = picked.replace(INDEX_GUARD, "")

    def sqlite_query(sql):
        return sql.replace("NOW()", str(NOW)).replace(
            "FOR UPDATE SKIP LOCKED", ""
        ).replace("%s", "?")

    revised = sqlite_query(picked)
    original = sqlite_query(original)
    with sqlite3.connect(":memory:") as conn:
        conn.execute("""
            CREATE TABLE research_ordered_first_touch_sync_outbox (
                event_id INTEGER, window_minutes INTEGER,
                threshold_bps INTEGER, method_version TEXT,
                destination TEXT, sync_status TEXT,
                next_attempt_at_utc INTEGER, created_at_utc INTEGER,
                lease_expires_at_utc INTEGER,
                PRIMARY KEY(event_id,window_minutes,threshold_bps,
                            method_version,destination)
            )
        """)
        conn.executescript(index_sql)
        conn.executescript(index_sql)  # Reapplying migration is idempotent.
        assert conn.execute(revised, (8,)).fetchall() == []
        rows = []
        for event in range(1, 171):
            for window in (60, 240, 720, 1440):
                for threshold in range(25, 201, 25):
                    status = ("PENDING", "RETRY", "IN_FLIGHT", "SYNCED",
                              "DEAD_LETTER")[event % 5]
                    # All combinations of lane, due/future deadline, NULL or
                    # expired/live lease. Deadline equality is eligible.
                    due_at = NOW + (event % 3 - 1) * 500
                    lease_at = (None if event % 7 == 0 else
                                NOW + (event % 4 - 2) * 300)
                    rows.append((event, window, threshold, "ordered-first-touch-v7",
                                 "GOOGLE_SHEETS", status, due_at,
                                 NOW - event % 9, lease_at))
        # Destination and method are part of the exact identity; the claim
        # must not silently impose a new method-version filter or conflate
        # paired directions/thresholds/windows with the same event id.
        rows.extend([
            (999, 60, 25, "audit-version", "OTHER", "PENDING", 1, 1, None),
            (999, 60, 25, "audit-version", "GOOGLE_SHEETS", "PENDING", 2, 1, None),
            (999, 60, 50, "ordered-first-touch-v7", "GOOGLE_SHEETS",
             "IN_FLIGHT", NOW + 999, 1, NOW - 1),
        ])
        random.Random(725).shuffle(rows)
        conn.executemany("INSERT INTO research_ordered_first_touch_sync_outbox "
                         "VALUES (?,?,?,?,?,?,?,?,?)", rows)

        for limit in (1, 2, 8, 128, 9999):
            expected = conn.execute(original, (limit,)).fetchall()
            actual = conn.execute(revised, (limit,)).fetchall()
            assert actual == expected, limit
            assert len(actual) == len(set(actual)), "duplicate full identities"
        all_due = conn.execute(revised, (9999,)).fetchall()
        assert all(row[4] == "GOOGLE_SHEETS" for row in all_due)
        assert all_due[0] == (999, 60, 25, "audit-version", "GOOGLE_SHEETS")
        # An expired lease is eligible even with a future next-attempt date;
        # retaining the original OR is essential for stale-lease recovery.
        assert (999, 60, 50, "ordered-first-touch-v7", "GOOGLE_SHEETS") in all_due

        # Exhaust consecutive batches and prove no eligible row is starved or
        # duplicated by the common ordering, including interleaved lanes.
        consumed = []
        while True:
            batch = conn.execute(revised, (8,)).fetchall()
            if not batch:
                break
            consumed.extend(batch)
            conn.executemany("""
                UPDATE research_ordered_first_touch_sync_outbox
                SET sync_status='SYNCED'
                WHERE event_id=? AND window_minutes=? AND threshold_bps=?
                  AND method_version=? AND destination=?
            """, batch)
        assert consumed == all_due
        assert len(consumed) == len(set(consumed))
    print("ordered First Touch sync claim parity selftest passed")


if __name__ == "__main__":
    run()
