"""Execute the v7 sample-selection predicate without remote dependencies."""

from __future__ import annotations

import sqlite3
from unittest.mock import patch

import research_outcome_worker as worker


class _CaptureConnection:
    def execute(self, query, params):
        self.query = query
        self.params = params
        return self

    def fetchall(self):
        return []


def run() -> None:
    capture = _CaptureConnection()
    with patch('research_event_scan.claim_event_page',return_value=[]):
        worker.ResearchOutcomeWorker._load_ordered_first_touch_due_events(capture, 8)
    sample_query = capture.query.split("new_samples AS MATERIALIZED (", 1)[1]
    sample_query = sample_query.split("), open_events AS MATERIALIZED (", 1)[0]
    # The real PostgreSQL source-page tests exercise this finite ID binding;
    # here every fixture row is inside the single supplied page.
    sample_query = sample_query.replace('WHERE e.event_id=ANY(%s::bigint[])\n                  AND','WHERE')
    # Run the actual source selection, CASE authorization and ordering.
    # Fixture timestamps are all eligible. Remove only PostgreSQL-specific
    # clock arithmetic; SQLite supplies the remaining SQL boolean semantics.
    start = sample_query.index("                  AND e.alert_time_utc >= NOW()")
    end = sample_query.index("                  AND NOT EXISTS (", start)
    sample_query = sample_query[:start] + sample_query[end:]
    query = (
        "WITH settings(method_version,rejection_policy,expected_rows,batch_limit) "
        "AS (VALUES (?,?,?,?)) " + sample_query
    )
    method, rejection_policy = capture.params[:2]
    bindings = (method, rejection_policy, 32, 8)
    authorizations = []
    with sqlite3.connect(":memory:") as conn:
        conn.create_function(
            "authorization_checked", 1,
            lambda event_id: authorizations.append(event_id) or event_id,
        )
        conn.executescript("""
            CREATE TABLE research_events (
                event_id INTEGER PRIMARY KEY, alert_time_utc TEXT,
                event_kind TEXT, delivery_status TEXT, direction TEXT
            );
            CREATE TABLE research_outcome_event_rejections (
                event_id INTEGER, rejection_policy_version TEXT
            );
            CREATE TABLE research_ordered_first_touch_outcomes (
                event_id INTEGER, method_version TEXT
            );
            CREATE TABLE allowed (event_id INTEGER PRIMARY KEY);
            CREATE VIEW research_prospective_shadow_events AS
                SELECT authorization_checked(event_id) AS event_id FROM allowed;
        """)
        for event_id in range(50):
            conn.execute(
                "INSERT INTO research_events VALUES (?,?,?,?,?)",
                (event_id, f"2026-09-06T10:{event_id:02d}:00",
                 "DECISION_SAMPLE", "NOT_APPLICABLE", "LONG"),
            )
            conn.executemany(
                "INSERT INTO research_ordered_first_touch_outcomes VALUES (?,?)",
                [(event_id, method)] * 32,
            )
            conn.execute("INSERT INTO allowed VALUES (?)", (event_id,))

        assert conn.execute(query, bindings).fetchall() == []
        # Already-complete samples must never hydrate the authorization view.
        assert authorizations == []

        # Twelve newer unauthorized samples exceed the eight-row batch size.
        # They must not hide an older authorized sample behind a candidate cap.
        conn.execute(
            "DELETE FROM research_ordered_first_touch_outcomes WHERE event_id>=38"
        )
        conn.execute("DELETE FROM allowed WHERE event_id>=38")
        conn.execute(
            "DELETE FROM research_ordered_first_touch_outcomes WHERE event_id=0"
        )
        rows = conn.execute(query, bindings).fetchall()
        assert [row[0] for row in rows] == [0]

        conn.execute(
            "INSERT INTO research_outcome_event_rejections VALUES (?,?)",
            (0, rejection_policy),
        )
        assert conn.execute(query, bindings).fetchall() == []
    print("ordered First Touch v7 queue self-test: PASS")


if __name__ == "__main__":
    run()
