"""Regression contract for the indexed prospective Shadow authorization view."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
import re

import research_formula_schema_admin


ROOT = Path(__file__).resolve().parent
SOURCE_MIGRATION = (
    ROOT / "migrations" / "013_prospective_decision_feature_freeze_v1.sql"
)
INDEXED_MIGRATION = (
    ROOT / "migrations" / "018_prospective_shadow_view_indexed_union_v1.sql"
)
ANCHOR_MIGRATION = ROOT / "migrations" / "008_prospective_neutral_anchors_v1.sql"
ARCHIVE_MIGRATION = ROOT / "migrations" / "001_research_archive_v1.sql"


def _view_body(sql: str) -> str:
    marker = "CREATE OR REPLACE VIEW research_prospective_shadow_events AS\n"
    return sql.split(marker, 1)[1].split(
        "\n\nCOMMENT ON VIEW research_prospective_shadow_events", 1
    )[0].rstrip(";\n")


def _projection(branch: str) -> tuple[str, ...]:
    selected = branch.split("SELECT\n", 1)[1].split(
        "\nFROM research_prospective_anchor_slots slot", 1
    )[0]
    return tuple(
        re.sub(r"\s+", " ", line.strip().rstrip(","))
        for line in selected.splitlines()
        if line.strip()
    )


def _predicates(branch: str) -> tuple[str, ...]:
    where = branch.split("\nWHERE ", 1)[1]
    return tuple(
        re.sub(r"\s+", " ", predicate.strip())
        for predicate in re.split(r"\n\s+AND\s+", where)
    )


def _legacy_expansion(slots, existing_event_ids):
    return [
        (slot_id, event_id)
        for slot_id, long_event_id, short_event_id, authorized in slots
        for event_id in (long_event_id, short_event_id)
        if authorized and event_id in existing_event_ids
    ]


def _indexed_expansion(slots, existing_event_ids):
    long_branch = [
        (slot_id, long_event_id)
        for slot_id, long_event_id, _short_event_id, authorized in slots
        if authorized and long_event_id in existing_event_ids
    ]
    short_branch = [
        (slot_id, short_event_id)
        for slot_id, _long_event_id, short_event_id, authorized in slots
        if authorized and short_event_id in existing_event_ids
    ]
    return [*long_branch, *short_branch]


def run() -> None:
    source_body = _view_body(SOURCE_MIGRATION.read_text(encoding="utf-8"))
    indexed_sql = INDEXED_MIGRATION.read_text(encoding="utf-8")
    indexed_body = _view_body(indexed_sql)
    branches = indexed_body.split("\nUNION ALL\n")

    assert indexed_sql.count(
        "CREATE OR REPLACE VIEW research_prospective_shadow_events AS"
    ) == 1
    executable_sql = "\n".join(
        line for line in indexed_sql.splitlines() if not line.lstrip().startswith("--")
    )
    for mutation in ("INSERT", "UPDATE", "DELETE", "ALTER", "TRUNCATE", "DROP"):
        assert re.search(rf"\b{mutation}\b", executable_sql) is None

    assert len(branches) == 2
    assert "CROSS JOIN LATERAL" not in indexed_body
    assert "VALUES (slot.long_event_id), (slot.short_event_id)" not in indexed_body
    assert "\nUNION\n" not in indexed_body
    assert (
        "JOIN research_events event ON event.event_id = slot.long_event_id"
        in branches[0]
    )
    assert "slot.short_event_id" not in branches[0].split("\nWHERE ", 1)[0].split(
        "JOIN research_events event", 1
    )[1]
    assert (
        "JOIN research_events event ON event.event_id = slot.short_event_id"
        in branches[1]
    )
    assert "slot.long_event_id" not in branches[1].split("\nWHERE ", 1)[0].split(
        "JOIN research_events event", 1
    )[1]

    source_projection = _projection(source_body)
    assert len(source_projection) == 33
    assert _projection(branches[0]) == source_projection
    assert _projection(branches[1]) == source_projection

    # No authorization condition may be dropped, weakened, or added during
    # this plan-only rewrite.  Both indexed branches retain the migration-013
    # fail-closed predicate list verbatim and in the same order.
    source_predicates = _predicates(source_body)
    assert len(source_predicates) == 20
    assert _predicates(branches[0]) == source_predicates
    assert _predicates(branches[1]) == source_predicates

    # UNION ALL changes only pair expansion order.  It neither loses a side
    # nor deduplicates two authorized pair references.  Include missing-event
    # and unauthorized fixtures to cover inner-join and fail-closed behavior.
    slots = [
        (1, 101, 102, True),
        (2, 201, 202, False),
        (3, 301, 302, True),
        (4, 401, 402, True),
    ]
    existing_event_ids = {101, 102, 201, 202, 301, 401}
    legacy = _legacy_expansion(slots, existing_event_ids)
    indexed = _indexed_expansion(slots, existing_event_ids)
    assert Counter(indexed) == Counter(legacy)
    assert len(indexed) == len(set(indexed))
    assert Counter(slot_id for slot_id, _event_id in indexed)[1] == 2
    assert (3, 302) not in indexed
    assert (4, 402) not in indexed
    assert not any(slot_id == 2 for slot_id, _event_id in indexed)

    # These existing UNIQUE/PRIMARY KEY btree indexes are the intended paths
    # after consumer event_id filters are pushed into the two explicit joins.
    anchor_schema = ANCHOR_MIGRATION.read_text(encoding="utf-8")
    archive_schema = ARCHIVE_MIGRATION.read_text(encoding="utf-8")
    assert "long_event_id BIGINT NOT NULL UNIQUE" in anchor_schema
    assert "short_event_id BIGINT NOT NULL UNIQUE" in anchor_schema
    assert "CHECK (long_event_id <> short_event_id)" in anchor_schema
    assert "event_id BIGSERIAL PRIMARY KEY" in archive_schema

    migration_paths = research_formula_schema_admin.MIGRATION_PATHS
    assert migration_paths.count(INDEXED_MIGRATION.resolve()) == 1
    assert (
        migration_paths.index(INDEXED_MIGRATION.resolve())
        > migration_paths.index(SOURCE_MIGRATION.resolve())
    )

    print("research_prospective_shadow_view_selftest: PASS")


if __name__ == "__main__":
    run()
