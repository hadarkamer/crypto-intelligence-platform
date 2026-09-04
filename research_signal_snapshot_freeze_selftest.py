"""Static checks for migration 023's Stage-4 PostgreSQL boundary."""
from __future__ import annotations

import re
from pathlib import Path

import research_formula_schema_admin
import research_signal_snapshot as snapshots
from research_signal_snapshot_selftest import (
    _build,
    _canonical_inputs,
    _derivatives,
    _strong_payload,
)


ROOT = Path(__file__).resolve().parent
MIGRATION = ROOT / "migrations" / "023_signal_snapshot_freeze_v1.sql"
EXPECTED_EVENT_TYPES = {
    snapshots.MAX_PAIN_EVENT_TYPE,
    snapshots.MAGNET_EVENT_TYPE,
    snapshots.COMBINED_EVENT_TYPE,
    snapshots.PROJECTION_EVENT_TYPE,
}


def _function_body(sql: str, function_name: str) -> str:
    match = re.search(
        rf"CREATE OR REPLACE FUNCTION\s+(?:public\.)?{function_name}\b.*?"
        rf"AS \$function\$(.*?)\$function\$;",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    assert match, f"missing function {function_name}"
    return match.group(1)


def run() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")
    assert MIGRATION.resolve() in research_formula_schema_admin.MIGRATION_PATHS

    reserved = _function_body(
        sql, "research_signal_snapshot_v1_reserved_type"
    )
    declared_types = set(
        re.findall(
            r"'([A-Z][A-Z0-9_]+)'",
            reserved,
        )
    )
    assert declared_types == EXPECTED_EVENT_TYPES
    assert snapshots.CAPTURE_STAGE == "SILENT_SIGNAL_SNAPSHOT"
    assert snapshots.CONTRACT_VERSION == "research-signal-snapshot-v1"
    assert snapshots.STRATEGY_VERSION == "signal-snapshot-v1"

    payload = _strong_payload()
    derivatives = _derivatives()
    opportunities, magnets, directional = _canonical_inputs(
        payload, derivatives
    )
    batch = _build(
        payload,
        opportunities=opportunities,
        magnets=magnets,
        directional=directional,
        derivatives=derivatives,
    )
    assert {event.event_type for event in batch.events} == EXPECTED_EVENT_TYPES
    projection_event = next(
        event
        for event in batch.events
        if event.event_type == snapshots.PROJECTION_EVENT_TYPE
    )
    combined_event = next(
        event
        for event in batch.events
        if event.event_type == snapshots.COMBINED_EVENT_TYPE
    )
    projection_signal = projection_event.engine_snapshot["signal_snapshot"]
    projection = projection_event.engine_snapshot["projection"]
    signal = combined_event.engine_snapshot["signal_snapshot"]
    archive_reference = signal["archive_reference"]
    derivatives_reference = signal["derivatives_reference"]

    envelope = _function_body(
        sql, "assert_research_signal_snapshot_v1_envelope"
    )
    declared_text_arrays = [
        set(re.findall(r"'([^']+)'", contents))
        for contents in re.findall(
            r"ARRAY\[(.*?)\]::TEXT\[\]",
            envelope,
            flags=re.IGNORECASE | re.DOTALL,
        )
    ]
    for emitted_keyset in (
        set(projection_signal),
        set(projection),
        set(signal),
        set(archive_reference),
        set(derivatives_reference),
        set(projection["counts"]),
        set(archive_reference["row_payload_sha256"][0]),
        set(archive_reference["max_pain_targets"][0]),
    ):
        assert emitted_keyset in declared_text_arrays, emitted_keyset
    for token in (
        "DECISION_SAMPLE",
        "NOT_APPLICABLE",
        "formula_authorized",
        "outcome_authorized",
        "telegram_delivery_allowed",
        "trade_execution_allowed",
        "RESEARCH_PASSIVE",
        "research_max_pain_snapshot_sets",
        "research_max_pain_snapshot_symbols",
        "research_max_pain_snapshot_rows",
        "derivatives_read_started_at_utc",
        "derivatives_read_completed_at_utc",
        "evaluation_status",
        "max_pain_targets",
        "INDEPENDENT_RAW_SOURCE_FAMILIES_V1",
        "COINGLASS_MAX_PAIN",
        "INTERVAL '15 minutes'",
    ):
        assert token in envelope, token
    assert envelope.count("IS DISTINCT FROM 'false'::JSONB") == 4
    sha_helper = _function_body(sql, "research_signal_snapshot_v1_sha256")
    integer_helper = _function_body(
        sql, "research_signal_snapshot_v1_nonnegative_integer"
    )
    assert "COALESCE" in sha_helper and "COALESCE" in integer_helper
    assert projection["evaluation_status"] == "EVALUABLE"
    assert set(combined_event.engine_snapshot["source_families"]) <= set(
        snapshots.VOTING_SOURCE_FAMILIES
    )
    assert combined_event.engine_snapshot["vote_count"] == len(
        combined_event.engine_snapshot["source_families"]
    )

    assert "BEFORE INSERT OR UPDATE ON public.research_events" in sql
    assert "BEFORE UPDATE OR DELETE ON public.research_events" in sql
    assert "BEFORE TRUNCATE ON public.research_events" in sql
    assert "DEFERRABLE INITIALLY DEFERRED" in sql
    assert "LOCK TABLE public.research_events IN SHARE ROW EXCLUSIVE MODE" in sql
    assert sql.index(
        "LOCK TABLE public.research_events IN SHARE ROW EXCLUSIVE MODE"
    ) < sql.index("DO $prior_install$")
    assert (
        "DROP INDEX IF EXISTS public.uq_research_signal_snapshot_projection_key_v1"
        in sql
    )
    assert "CREATE UNIQUE INDEX uq_research_signal_snapshot_projection_key_v1" in sql
    assert "assert_research_signal_snapshot_v1_set_complete" in sql
    assert "PERFORM public.assert_research_signal_snapshot_v1_envelope" in sql
    assert "DO $column_acl_cleanup$" in sql
    assert "DO $trigger_acl_cleanup$" in sql
    assert "research_max_pain_snapshot_sets_snapshot_set_id_seq" in sql
    assert "research_max_pain_snapshot_rows_snapshot_row_id_seq" in sql
    assert "archive_row.short_max_pain > archive_row.current_price" in sql
    assert "archive_row.long_max_pain < archive_row.current_price" in sql
    assert "claimed.member_count + 1.0" in sql
    assert "family_event.direction = candidate.direction" in sql

    # The migration freezes storage/provenance only.  Formula and outcome
    # schemas remain outside its dependency graph and therefore gain no new
    # authority path from Stage 4.
    identifiers = set(
        re.findall(
            r"\b(?:FROM|JOIN|INTO|UPDATE|TABLE|SEQUENCE|FUNCTION|ON)\s+"
            r"(?:public\.)?([a-z0-9_]+)",
            sql,
            flags=re.IGNORECASE,
        )
    )
    assert not any(name.startswith("research_formula") for name in identifiers)
    assert "research_alert_outcomes" not in identifiers
    for module_path in (
        ROOT / "research_signal_snapshot.py",
        ROOT / "research_signal_snapshot_runtime.py",
        ROOT / "research_signal_snapshot_store.py",
    ):
        module_source = module_path.read_text(encoding="utf-8").lower()
        assert "import research_formula" not in module_source
        assert "import telegram" not in module_source
        assert "trade_execution_allowed\": true" not in module_source

    print("research_signal_snapshot_freeze_selftest: PASS")


if __name__ == "__main__":
    run()
