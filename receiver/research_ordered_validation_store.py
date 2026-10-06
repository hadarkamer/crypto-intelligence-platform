"""Bounded PostgreSQL adapter for immutable prospective ordered-v7 validation.

Called inside the research worker transaction. Never commits, sends messages,
alters source records, or imports archive evidence into the LIVE universe.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, Mapping, Sequence

import research_ordered_validation as validation
import research_formula_ordered_v7 as evidence

TABLES = ("research_ordered_validation_acceptance", "research_ordered_validation_freezes",
          "research_ordered_validation_waves", "research_ordered_validation_evaluations")


def schema_status(conn: Any) -> dict[str, Any]:
    rows = conn.execute("SELECT name,to_regclass(name) IS NOT NULL AS present FROM unnest(%s::text[]) AS name", (list(TABLES),)).fetchall()
    missing = [row["name"] for row in rows if not row["present"]]
    return {"schema_present": not missing, "missing_tables": missing}


def freeze_scope(conn: Any, scope: Mapping[str, Any], candidate_definition: Mapping[str, Any],
                 *, acceptance_policy: Mapping[str, Any] | None = None) -> dict[str, Any]:
    existing = conn.execute("SELECT registration FROM research_ordered_validation_freezes WHERE scope_key=%s",
                            (scope["scope_key"],)).fetchone()
    if existing:
        registration = existing["registration"]
        validation.assert_same_registration(registration, scope, candidate_definition)
        if acceptance_policy is not None and validation.canonical(acceptance_policy) != validation.canonical(registration.get("acceptance_policy")):
            raise ValueError("acceptance cannot be changed after freeze; new candidate/scope version required")
        return registration
    exact = validation.binding(scope, candidate_definition)
    if acceptance_policy is None:
        configured = conn.execute("SELECT policy FROM research_ordered_validation_acceptance WHERE binding_sha256=%s",
                                  (validation.digest(exact),)).fetchone()
        acceptance_policy = configured["policy"] if configured else None
    # Caller now is deliberately not used as the freeze clock; historical replay
    # cannot make previously observed market outcomes look prospective.
    clock = conn.execute("SELECT clock_timestamp() AS now").fetchone()["now"]
    registration = validation.freeze(scope, candidate_definition, frozen_at_utc=clock,
                                     acceptance_policy=acceptance_policy)
    conn.execute("""INSERT INTO research_ordered_validation_freezes
        (freeze_id,scope_key,definition_sha256,frozen_at_utc,registration)
        VALUES(%s,%s,%s,%s,%s::jsonb) ON CONFLICT(scope_key) DO NOTHING""",
        (registration["freeze_id"], scope["scope_key"], registration["definition_sha256"],
         registration["frozen_at_utc"], validation.canonical(registration)))
    saved = conn.execute("SELECT registration FROM research_ordered_validation_freezes WHERE scope_key=%s",
                         (scope["scope_key"],)).fetchone()["registration"]
    validation.assert_same_registration(saved, scope, candidate_definition)
    return saved


def register_supported_acceptance(conn: Any, scope: Mapping[str, Any],
                                  candidate_definition: Mapping[str, Any]) -> bool:
    """Install the predeclared policy before a new supported scope freezes."""
    import research_ordered_acceptance_policy as acceptance
    exact = validation.binding(scope, candidate_definition)
    policy = acceptance.policy_for(exact, candidate_definition)
    if policy is None:
        return False
    validation.validate_acceptance(policy, exact)
    conn.execute("""INSERT INTO research_ordered_validation_acceptance
        (policy_version,binding_sha256,policy) VALUES(%s,%s,%s::jsonb)
        ON CONFLICT DO NOTHING""", (policy["policy_version"], policy["binding_sha256"],
        validation.canonical(policy)))
    saved = conn.execute("SELECT policy FROM research_ordered_validation_acceptance WHERE binding_sha256=%s",
                         (validation.digest(exact),)).fetchone()
    if not saved or validation.canonical(saved["policy"]) != validation.canonical(policy):
        raise ValueError("immutable acceptance policy registration conflict")
    return True


def _enrich_rows(conn: Any, rows: Sequence[Mapping[str, Any]], contract: Mapping[str, Any]) -> list[dict[str, Any]]:
    if contract["source_scope"] != "LIVE":
        raise ValueError("native DB validation adapter only supports explicitly LIVE source")
    ids = sorted({row["event_id"] for row in rows})
    if not ids:
        return []
    metadata = conn.execute("""SELECT e.event_id,e.event_kind,e.delivery_status,
         p.start_time_utc AS parent_start_time_utc,m.btc_parent_movement_id,
         m.episode_policy_version,m.membership_status,p.evidence_eligible AS parent_evidence_eligible
        FROM research_events e LEFT JOIN research_event_btc_movements m
          ON m.event_id=e.event_id AND m.episode_policy_version=%s
        LEFT JOIN research_btc_parent_movements p ON p.btc_parent_movement_id=m.btc_parent_movement_id
          AND p.episode_policy_version=m.episode_policy_version
        WHERE e.event_id=ANY(%s)""", (contract["parent_policy_version"], ids)).fetchall()
    by_id = {row["event_id"]: row for row in metadata}
    return [{**row, **by_id.get(row["event_id"], {}),
             "source_scope": "LIVE" if by_id.get(row["event_id"], {}).get("event_kind") == "ALERT"
                and by_id.get(row["event_id"], {}).get("delivery_status") == "DELIVERED" else "UNVERIFIED",
             "period_key": contract["period_key"], "period_version": contract["period_version"]}
            for row in rows]


def _lock_representatives(conn: Any, registration: Mapping[str, Any], rows: Sequence[Mapping[str, Any]],
                          *, establish_entries: bool = True) -> list[str]:
    groups = defaultdict(list)
    for row in rows:
        if row.get("btc_parent_movement_id") and row.get("parent_start_time_utc"):
            groups[row["btc_parent_movement_id"]].append(row)
    prior_rows = conn.execute(
        "SELECT btc_parent_movement_id,representative_event_ids,representative_sha256,representative_conflict FROM research_ordered_validation_waves WHERE freeze_id=%s ORDER BY btc_parent_movement_id LIMIT 5001",
        (registration["freeze_id"],)).fetchall()
    if len(prior_rows) > 5000:
        raise ValueError("frozen wave ledger exceeds bounded validation budget; incremental paging required")
    prior = {row["btc_parent_movement_id"]: row for row in prior_rows}
    conflicts = {wave for wave, item in prior.items() if item["representative_conflict"]}
    if not establish_entries:
        # An initial or partial backfill cannot know the first matching source.
        # Keep prior immutable entries/conflicts, but do not freeze a provisional
        # later observation or mistake absent earlier rows for an entry mutation.
        return sorted(conflicts)
    required_features = sorted({condition["feature"] for condition in registration["binding"]["conditions"]})
    for wave, members in groups.items():
        earliest = min(evidence._decision_time(row) for row in members)
        selected = [row for row in members if evidence._decision_time(row) == earliest]
        ids = sorted({row["event_id"] for row in selected})
        # Unrelated captured fields can arrive later without changing this
        # candidate's entry. Only its frozen predicate features are evidence for
        # completion; retain exact values and missing-vs-present distinction.
        # Actual entry identity, timing, price and every required value remain
        # guarded, even when a changed value would still pass the threshold.
        signature = validation.digest({
            "version": registration["binding"]["representative_fingerprint_version"],
            "members": sorted([{"event_id": row["event_id"], "time": earliest.isoformat(),
                "snapshot_id": row.get("snapshot_id"), "symbol": row.get("symbol"),
                "direction": row.get("direction"), "entry_price": row.get("entry_price"),
                "predicate_features": {key: {"present": key in (row.get("decision_features") or {}),
                    "value": (row.get("decision_features") or {}).get(key)} for key in required_features}}
                for row in selected], key=lambda item: item["event_id"]),
        })
        if wave in prior:
            if signature != prior[wave]["representative_sha256"]:
                conflicts.add(wave)
                conn.execute("UPDATE research_ordered_validation_waves SET representative_conflict=TRUE,updated_at_utc=NOW() WHERE freeze_id=%s AND btc_parent_movement_id=%s",
                             (registration["freeze_id"], wave))
            continue
        start = evidence._utc(selected[0]["parent_start_time_utc"])
        phase = "PROSPECTIVE" if start > evidence._utc(registration["frozen_at_utc"]) else "DISCOVERY"
        conn.execute("""INSERT INTO research_ordered_validation_waves
            (freeze_id,btc_parent_movement_id,phase,parent_start_time_utc,representative_event_ids,representative_sha256)
            VALUES(%s,%s,%s,%s,%s::jsonb,%s) ON CONFLICT DO NOTHING""",
            (registration["freeze_id"], wave, phase, start, validation.canonical(ids), signature))
    return sorted(conflicts)


def evaluate_scope(conn: Any, scope: Mapping[str, Any], rows: Sequence[Mapping[str, Any]], *,
                   now: datetime, candidate_definition: Mapping[str, Any],
                   source_coverage_complete: bool, truncated: bool = False,
                   common_window_rows: Mapping[Any, Mapping[str, Any]] | None = None,
                   registered_attempts: int | None = None) -> dict[str, Any]:
    if len(rows) > 5000:
        raise ValueError("bounded validation row budget exceeded")
    registration = freeze_scope(conn, scope, candidate_definition)
    prepared = _enrich_rows(conn, rows, registration["binding"])
    establish_entries = source_coverage_complete and not truncated
    conflicts = _lock_representatives(conn, registration, prepared, establish_entries=establish_entries)
    if common_window_rows is None:
        present = conn.execute("SELECT to_regclass('research_common_window_metrics') AS relation").fetchone()["relation"]
        if present:
            import research_common_window_metrics_store as windows
            common_window_rows = windows.load_by_event_ids(conn, [row["event_id"] for row in prepared], scope["window_minutes"])
    if registered_attempts is None:
        registered_attempts = conn.execute("SELECT COUNT(*) AS n FROM research_ordered_formula_scopes WHERE period_key<>'LEGACY_UNSCOPED'").fetchone()["n"]
    # Freshly registered scopes use the real DB clock even if the worker pass
    # began a few seconds earlier. No later data is imported by this adjustment.
    as_of = max(evidence._utc(now), evidence._utc(registration["frozen_at_utc"]))
    result = validation.evaluate(registration, prepared, analysis_as_of_utc=as_of,
        source_coverage_complete=source_coverage_complete, truncated=truncated,
        common_window_rows=common_window_rows, registered_attempts=registered_attempts,
        representative_conflicts=conflicts)
    result["representative_entries_frozen"] = bool(establish_entries and result["source_coverage_complete"])
    for episode in result["episodes"] if result["representative_entries_frozen"] else []:
        conn.execute("""UPDATE research_ordered_validation_waves SET evidence=%s::jsonb,updated_at_utc=NOW()
            WHERE freeze_id=%s AND btc_parent_movement_id=%s AND evidence IS DISTINCT FROM %s::jsonb""",
            (validation.canonical(episode), registration["freeze_id"], episode["btc_parent_movement_id"], validation.canonical(episode)))
    sha = validation.digest(result)
    conn.execute("""INSERT INTO research_ordered_validation_evaluations(freeze_id,evidence_sha256,result,evaluated_at_utc)
        VALUES(%s,%s,%s::jsonb,%s) ON CONFLICT DO NOTHING""",
        (registration["freeze_id"], sha, validation.canonical(result), now))
    return {**result, "evidence_sha256": sha}
