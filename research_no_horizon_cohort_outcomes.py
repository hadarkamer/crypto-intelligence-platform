"""Prepare global representative outcomes from one complete anchored cohort.

Parts remain transport units. Source decisions and causal representatives are
frozen before prices are validated; no outcome is evaluated during preparation.
Only the selected earliest representative of each parent becomes an outcome job
entry. Every declared scope remains visible, including empty and blocked scopes.
"""
from __future__ import annotations

import ast
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

import research_no_horizon_cohort as cohort
import research_no_horizon_cohort_coverage as coverage
import research_no_horizon_contract as contracts
import research_no_horizon_first_touch as first_touch
import research_no_horizon_gate as gate
import research_no_horizon_replay as replay
import research_no_horizon_source as source
import research_no_horizon_store as child
import research_watch_scan_formula as formulas

VERSION = "no-horizon-global-representative-outcome-preparation-v1"
MAX_PREPARED_BYTES = 256 * 1024 * 1024
MAX_EXPANDED_BYTES = 256 * 1024 * 1024
MAX_CANDLES = 31 * 1440
_AUTHORITY = {"runtime_authorized": False, "telegram_authorized": False, "trading_authorized": False}
_SCOPE_KEYS = ("scope_id", "candidate_key", "base_direction", "analysis_direction", "threshold_pct", "candidate")


class CohortInputBlocked(ValueError):
    """The full source/parent decision population cannot establish its members."""
    def __init__(self, receipt):
        self.receipt = receipt
        super().__init__("GLOBAL_COHORT_SOURCE_OR_PARENT_PREFLIGHT_BLOCKED")


@dataclass(frozen=True)
class PreparedCohort:
    identity: dict
    payload: dict
    plan_id: str


def _encoded(value: Any, maximum: int) -> tuple[str, int]:
    """Bound serialized bytes while encoding, before retaining another copy."""
    pieces, size = [], 0
    encoder = json.JSONEncoder(sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    for piece in encoder.iterencode(value):
        size += len(piece.encode("utf-8"))
        if size > maximum:
            raise ValueError("GLOBAL_OUTCOME_SERIALIZED_BYTE_LIMIT_EXCEEDED")
        pieces.append(piece)
    return "".join(pieces), size


def implementation() -> dict:
    """Bind the complete repository-local import closure, including coordinator."""
    root = Path(__file__).resolve().parent
    pending = [Path(module.__file__).stem for module in
               (cohort, coverage, contracts, first_touch, gate, replay, source, child, formulas)]
    pending += [Path(__file__).stem, "research_no_horizon_cohort_store"]
    if any(not (root / (name + ".py")).is_file() for name in pending):
        raise ValueError("GLOBAL_OUTCOME_REQUIRED_IMPLEMENTATION_MISSING")
    files = {}
    while pending:
        name = pending.pop()
        path = root / (name + ".py")
        if path.name in files or not path.is_file():
            continue
        data = path.read_bytes()
        files[path.name] = hashlib.sha256(data).hexdigest()
        for node in ast.walk(ast.parse(data)):
            if isinstance(node, ast.Import):
                pending.extend(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                pending.append(node.module.split(".")[0])
    catalog = root / "docs" / "ordered_research_question_map.json"
    if not catalog.is_file():
        raise ValueError("GLOBAL_OUTCOME_REQUIRED_CATALOG_MISSING")
    files["docs/ordered_research_question_map.json"] = hashlib.sha256(catalog.read_bytes()).hexdigest()
    return {"preparation_version": VERSION, "cohort_version": cohort.VERSION,
        "coverage_version": coverage.VERSION, "child_versions": child._versions(),
        "implementation_files": dict(sorted(files.items()))}


def _prices(raw_bars, declared):
    cutoff = contracts.utc(declared["cutoff_utc"])
    start = source._entry_time(contracts.utc(declared["source_start_utc"]))
    cleaned = []
    for raw in raw_bars:
        if (not isinstance(raw, Mapping) or raw.get("route") != declared["price_route"]
                or raw.get("symbol") != declared["symbol"]):
            raise ValueError("COHORT_PRICE_ROUTE_OR_SYMBOL_MISMATCH")
        bar = first_touch._bar(raw, cutoff=cutoff)
        if bar is None or not start <= contracts.utc(bar["open_time_utc"]) < cutoff:
            raise ValueError("COHORT_PRICE_OUTSIDE_FROZEN_OBSERVATION")
        if cleaned and contracts.utc(bar["open_time_utc"]) <= contracts.utc(cleaned[-1]["open_time_utc"]):
            raise ValueError("COHORT_PRICE_ORDER_OR_DUPLICATE_CONFLICT")
        cleaned.append(bar)
    return cleaned


def _scope_plan(ordinal, full_scope, *, declared, receipt, candles, dataset_id, cohort_id):
    scope = {key: full_scope[key] for key in _SCOPE_KEYS}
    representatives = full_scope["representatives"]
    records = {record["global_ordinal"]: record for record in full_scope["decision_ledger"]}
    by_open = {bar["open_time_utc"]: bar for bar in candles}
    opportunities, missing = [], []
    for representative in representatives:
        record = records[representative["global_ordinal"]]
        if (any(record.get(key) != value for key, value in representative.items())
                or record["match_status"] != "MATCH" or record["parent_blockers"]
                or record["membership_status"] != "LIVE" or record["parent_evidence_eligible"] is not True):
            raise ValueError("GLOBAL_REPRESENTATIVE_LEDGER_MISMATCH")
        opened = source._entry_time(contracts.utc(record["decision_time_utc"])).isoformat()
        entry = by_open.get(opened)
        if entry is None:
            missing.append({**representative, "entry_time_utc": opened})
            continue
        args = {"candidate_id": scope["candidate_key"],
            "candidate_version": scope["candidate"]["definition_sha256"],
            "cohort_id": cohort_id, "dataset_id": dataset_id, "entry_id": record["entry_id"],
            "symbol": declared["symbol"], "direction": scope["analysis_direction"],
            "decision_time_utc": record["decision_time_utc"], "reference_price": entry["open"],
            "threshold_pct": scope["threshold_pct"], "source_route": source.route_for_symbol(declared["symbol"]),
            "parent_policy_version": record["parent_policy_version"]}
        contracts.make_contract(**args)
        opportunities.append({"contract": args,
            **{key: record[key] for key in ("btc_parent_movement_id", "membership_status",
                "parent_evidence_eligible", "parent_start_time_utc", "parent_confirmed_at_utc", "parent_policy_version")},
            "features_observed_at_utc": record["source_observed_at_utc"]})
    result = {"ordinal": ordinal, "scope": scope, "representatives": representatives,
        "input_error": "MISSING_SELECTED_ENTRY_OPEN" if missing else None,
        "missing_entry_ids": [item["entry_id"] for item in missing], "missing_entry_locators": missing,
        "snapshot_metadata": None, "snapshot_sha256": None, "normalized_entries": []}
    if missing:
        return result, 0
    opportunities.sort(key=lambda item: (contracts.utc(item["contract"]["decision_time_utc"]), item["contract"]["entry_id"]))
    metadata = {"snapshot_version": replay.SNAPSHOT_VERSION, "dataset_id": dataset_id, "cohort_id": cohort_id,
        "source_route": source.route_for_symbol(declared["symbol"]), "source_coverage_complete": True,
        "cutoff_utc": declared["cutoff_utc"], "opportunities": opportunities, "gate_policy": declared["gate_policy"],
        "source_receipt": {"adapter_version": VERSION, "declaration_sha256": receipt["declaration_sha256"],
            "anchor_sha256": receipt["anchor_sha256"], "coverage_receipt_sha256": receipt["receipt_sha256"],
            "scope_id": scope["scope_id"], "counts": full_scope["source_counts"],
            "accepted_source_rows": receipt["accepted_source_rows"], "emitted_opportunities": len(opportunities),
            "selected_representatives": representatives, "decision_ledger_sha256": contracts.digest(full_scope["decision_ledger"]),
            "source_blockers": [], "scope_pooling": False, "prior_outcome_receipts_consumed": 0,
            "selection": {**{key: scope[key] for key in ("candidate_key", "base_direction", "analysis_direction", "threshold_pct")},
                "candidate_definition_sha256": scope["candidate"]["definition_sha256"], "symbol": declared["symbol"]},
            "representative_selection": "GLOBAL_EARLIEST_DECISION_THEN_ENTRY_ID_WITHOUT_OUTCOMES_V1", **_AUTHORITY}}
    snapshot = {**metadata, "candles": candles}
    normalized = replay.prepare_snapshot(snapshot)[3]
    encoded, size = _encoded(snapshot, MAX_EXPANDED_BYTES)
    result.update(snapshot_metadata=metadata, snapshot_sha256=hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
        normalized_entries=normalized)
    return result, size


def prepare_cohort_submission(declaration, anchor, load_part) -> PreparedCohort:
    """Validate one complete cohort, freeze representatives, then prepare prices.

    A blocked source/parent scope rejects the whole admission before local writes.
    Insufficient and empty scopes are retained for descriptive evaluation. Missing
    selected entry prices block the affected scope without replacing its members.
    """
    declared = cohort.normalize_declaration(declaration)
    frozen_anchor = cohort.validate_anchor(declared, anchor)
    if not callable(load_part):
        raise ValueError("COHORT_PART_LOADER_REQUIRED")
    first_bars = None

    def collect(ordinal):
        nonlocal first_bars
        loaded = load_part(ordinal)
        bars = loaded.get("candles") if isinstance(loaded, Mapping) else None
        if not isinstance(bars, list) or len(bars) > MAX_CANDLES:
            raise ValueError("COHORT_PRICE_ARRAY_LIMIT_EXCEEDED")
        encoded, _ = _encoded(bars, source.MAX_BYTES)
        if ordinal == 0:
            first_bars = json.loads(encoded)
        else:
            left = source._entry_time(contracts.utc(declared["parts"][ordinal]["source_start_utc"]))
            expected = [bar for bar in first_bars if contracts.utc(bar["open_time_utc"]) >= left]
            if encoded != contracts.canonical(expected):
                raise ValueError("COHORT_PRICE_SUFFIX_MISMATCH")
        return loaded

    receipt = coverage.preflight_cohort(declared, frozen_anchor, collect)
    if any(scope["coverage_status"] == "BLOCKED" for scope in receipt["scopes"]):
        raise CohortInputBlocked(receipt)
    # All representatives now exist independently of entry or later path prices.
    candles = _prices(first_bars, declared)
    del first_bars
    price_sha = contracts.digest(candles)
    dataset_id = "watch-global-dataset:" + contracts.digest({"declaration_sha256": receipt["declaration_sha256"],
        "anchor_sha256": receipt["anchor_sha256"], "candles_sha256": price_sha})
    cohort_id = "watch-global-cohort:" + receipt["declaration_sha256"]
    plans, expanded = [], 0
    for ordinal, scope in enumerate(receipt["scopes"]):
        plan, size = _scope_plan(ordinal, scope, declared=declared, receipt=receipt,
            candles=candles, dataset_id=dataset_id, cohort_id=cohort_id)
        expanded += size
        if expanded > MAX_EXPANDED_BYTES:
            raise ValueError("GLOBAL_OUTCOME_EXPANDED_BYTE_LIMIT_EXCEEDED")
        plans.append(plan)
    payload = {"coverage_receipt": receipt, "candles": candles, "scope_plans": plans,
        "dataset_id": dataset_id, "cohort_id": cohort_id, "cutoff_utc": declared["cutoff_utc"],
        "source_route": source.route_for_symbol(declared["symbol"])}
    encoded, size = _encoded(payload, MAX_PREPARED_BYTES)
    # Isolate prepared immutable identities from caller-owned mutable input.
    payload = json.loads(encoded)
    identity = {"preparation_version": VERSION, "declaration_sha256": receipt["declaration_sha256"],
        "anchor_sha256": receipt["anchor_sha256"], "coverage_receipt_sha256": receipt["receipt_sha256"],
        "candles_sha256": price_sha, "payload_sha256": hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
        "payload_bytes": size, "expanded_snapshot_bytes": expanded, "gate_policy": declared["gate_policy"],
        "versions": implementation()}
    return PreparedCohort(identity, payload, contracts.digest(identity))


def validate_prepared(prepared: PreparedCohort, *, check_implementation=True) -> PreparedCohort:
    """Validate internal preparation before persistence or historical reading.

    Historical validation checks frozen hashes and bindings without rerunning
    today's source/catalog policy. This is not an import API for audit receipts.
    """
    if not isinstance(prepared, PreparedCohort) or type(check_implementation) is not bool:
        raise ValueError("INTERNAL_PREPARED_COHORT_REQUIRED")
    identity, payload = prepared.identity, prepared.payload
    encoded, size = _encoded(payload, MAX_PREPARED_BYTES)
    receipt = payload["coverage_receipt"]
    if (contracts.digest(identity) != prepared.plan_id
            or identity["payload_sha256"] != hashlib.sha256(encoded.encode("utf-8")).hexdigest()
            or identity["payload_bytes"] != size or identity["candles_sha256"] != contracts.digest(payload["candles"])
            or identity["coverage_receipt_sha256"] != receipt["receipt_sha256"]
            or receipt["receipt_sha256"] != contracts.digest({key: value for key, value in receipt.items() if key != "receipt_sha256"})
            or any(identity[key] != receipt[key] for key in ("declaration_sha256", "anchor_sha256", "gate_policy"))
            or identity["declaration_sha256"] != contracts.digest(receipt["cohort_declaration"])
            or payload["cutoff_utc"] != receipt["cutoff_utc"]
            or len(payload["candles"]) > MAX_CANDLES or len(payload["scope_plans"]) != len(receipt["scopes"])
            or any(scope["coverage_status"] == "BLOCKED" for scope in receipt["scopes"])):
        raise ValueError("PREPARED_COHORT_IDENTITY_OR_PAYLOAD_MISMATCH")
    expanded = 0
    for ordinal, (plan, scope) in enumerate(zip(payload["scope_plans"], receipt["scopes"])):
        if (plan["ordinal"] != ordinal or plan["scope"] != {key: scope[key] for key in _SCOPE_KEYS}
                or plan["representatives"] != scope["representatives"]):
            raise ValueError("PREPARED_COHORT_SCOPE_OR_REPRESENTATIVE_MISMATCH")
        metadata = plan["snapshot_metadata"]
        if plan["input_error"] is not None:
            if (plan["input_error"] != "MISSING_SELECTED_ENTRY_OPEN" or metadata is not None
                    or plan["snapshot_sha256"] is not None or not plan["missing_entry_ids"]
                    or plan["normalized_entries"]
                    or plan["missing_entry_ids"] != [item["entry_id"] for item in plan["missing_entry_locators"]]
                    or not set(plan["missing_entry_ids"]) <= {item["entry_id"] for item in plan["representatives"]}):
                raise ValueError("PREPARED_COHORT_BLOCKED_SCOPE_MISMATCH")
            continue
        if metadata is None or plan["missing_entry_ids"] or plan["missing_entry_locators"]:
            raise ValueError("PREPARED_COHORT_SCOPE_SNAPSHOT_REQUIRED")
        snapshot = {**metadata, "candles": payload["candles"]}
        serialized, child_size = _encoded(snapshot, MAX_EXPANDED_BYTES)
        expanded += child_size
        source_receipt = metadata["source_receipt"]
        if (plan["snapshot_sha256"] != hashlib.sha256(serialized.encode("utf-8")).hexdigest()
                or any(metadata[key] != payload[key] for key in ("dataset_id", "cohort_id", "cutoff_utc", "source_route"))
                or metadata["gate_policy"] != identity["gate_policy"] or metadata["source_coverage_complete"] is not True
                or any(source_receipt[key] != identity[key] for key in ("declaration_sha256", "anchor_sha256", "coverage_receipt_sha256"))
                or source_receipt["scope_id"] != scope["scope_id"]
                or source_receipt["selected_representatives"] != plan["representatives"]
                or sorted(item["contract"]["entry_id"] for item in metadata["opportunities"])
                    != sorted(item["entry_id"] for item in plan["representatives"])
                or sorted(item["contract"]["entry_id"] for item in plan["normalized_entries"])
                    != sorted(item["entry_id"] for item in plan["representatives"])):
            raise ValueError("PREPARED_COHORT_SNAPSHOT_BINDING_MISMATCH")
        if check_implementation and replay.prepare_snapshot(snapshot)[3] != plan["normalized_entries"]:
            raise ValueError("PREPARED_COHORT_NORMALIZED_ENTRY_MISMATCH")
    if expanded != identity["expanded_snapshot_bytes"] or expanded > MAX_EXPANDED_BYTES:
        raise ValueError("PREPARED_COHORT_EXPANDED_BYTE_MISMATCH")
    if check_implementation and (identity["preparation_version"] != VERSION or identity["versions"] != implementation()):
        raise ValueError("PREPARED_COHORT_IMPLEMENTATION_MISMATCH")
    return prepared
