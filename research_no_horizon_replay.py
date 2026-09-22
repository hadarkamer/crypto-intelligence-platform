"""Replay a bounded, explicitly sourced snapshot without network or DB writes.

The output is a research receipt. Source metadata and BTC memberships are
caller-supplied evidence, never an authorization or an attestation from a DB.
See docs/NO_HORIZON_RESEARCH_V1.md for the snapshot schema and limitations.
"""
from __future__ import annotations

import argparse
from bisect import bisect_left
from collections import Counter
import json
from pathlib import Path
from typing import Any, Mapping

import research_no_horizon_contract as contracts
import research_no_horizon_first_touch as first_touch
import research_no_horizon_gate as gate

SNAPSHOT_VERSION = "no-horizon-snapshot-v1"
RECEIPT_VERSION = "no-horizon-replay-receipt-v1"
MAX_INPUT_BYTES = 256 * 1024 * 1024
MAX_OPPORTUNITIES = 10000
MAX_SOURCE_CANDLES = 1000000


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key: " + key)
        result[key] = value
    return result


def _nonfinite(value):
    raise ValueError("nonfinite JSON value: " + value)


def load_snapshot(path: Path) -> dict[str, Any]:
    with path.open("rb") as stream:
        raw = stream.read(MAX_INPUT_BYTES + 1)
    if len(raw) > MAX_INPUT_BYTES:
        raise ValueError("snapshot exceeds the bounded input size")
    value = json.loads(raw, object_pairs_hook=_object, parse_constant=_nonfinite)
    if not isinstance(value, dict):
        raise ValueError("snapshot must be an object")
    return value


def replay_snapshot(snapshot: Mapping[str, Any], *, batch_size: int = 1024,
                    candle_budget: int = 2000000) -> dict[str, Any]:
    """One shared price series, one exact candidate/cohort, many opportunities.

    Contract arguments are passed unchanged to make_contract. Every event must
    identify the snapshot dataset, cohort and source route. A source budget
    exhausted before labels are evaluated blocks the aggregate gate; it never
    pretends that the remaining events were evaluated with zero occurrences.
    """
    if type(batch_size) is not int or not 1 <= batch_size <= 100000:
        raise ValueError("batch_size must be an integer from 1 to 100000")
    if type(candle_budget) is not int or not 1 <= candle_budget <= 100000000:
        raise ValueError("candle_budget must be an integer from 1 to 100000000")
    if snapshot.get("snapshot_version") != SNAPSHOT_VERSION:
        raise ValueError("unsupported snapshot version")
    required = {"snapshot_version", "dataset_id", "cohort_id", "source_route",
                "source_coverage_complete", "cutoff_utc", "candles", "opportunities"}
    if set(snapshot) - required - {"source_receipt", "gate_policy"} or required - set(snapshot):
        raise ValueError("snapshot has missing or unsupported fields")
    if type(snapshot["source_coverage_complete"]) is not bool:
        raise ValueError("source_coverage_complete must be an explicit boolean")
    for field in ("dataset_id", "cohort_id"):
        if not isinstance(snapshot[field], str) or not snapshot[field].strip():
            raise ValueError(field + " must identify the frozen input")
    cutoff = contracts.utc(snapshot["cutoff_utc"])
    bars, events = snapshot["candles"], snapshot["opportunities"]
    if not isinstance(bars, list) or len(bars) > MAX_SOURCE_CANDLES:
        raise ValueError("bounded candle array required")
    if not isinstance(events, list) or len(events) > MAX_OPPORTUNITIES:
        raise ValueError("bounded opportunity array required")
    times = [contracts.utc(bar["open_time_utc"]) for bar in bars]
    if any(a >= b for a, b in zip(times, times[1:])):
        raise ValueError("shared price series must be chronological without duplicates")
    normalized = []
    seen = set()
    scope = None
    metadata_fields = {
        "btc_parent_movement_id", "membership_status", "parent_evidence_eligible",
        "parent_start_time_utc", "parent_confirmed_at_utc", "features_observed_at_utc",
        "parent_policy_version",
    }
    # Validate and retain the full decision population before reading outcomes.
    for event in events:
        if not isinstance(event, dict) or set(event) - metadata_fields - {"contract"}:
            raise ValueError("invalid opportunity fields")
        contract = contracts.make_contract(**event["contract"])
        if any(contract[key] != snapshot[key] for key in ("dataset_id", "cohort_id", "source_route")):
            raise ValueError("opportunity does not belong to this snapshot source")
        identity = contracts.scope_identity(contract)
        if scope is None:
            scope = identity
        if identity != scope:
            raise ValueError("snapshot must contain one exact candidate scope")
        if contract["entry_id"] in seen:
            raise ValueError("duplicate opportunity entry_id")
        seen.add(contract["entry_id"])
        if contracts.utc(contract["entry_time_utc"]) > cutoff:
            raise ValueError("opportunity entry is beyond the observable cutoff")
        normalized.append({**{key: event[key] for key in metadata_fields if key in event},
                           "contract": contract})
    # Deterministic processing budget, independent of the caller's row order.
    normalized.sort(key=lambda row: (contracts.utc(row["contract"]["decision_time_utc"]),
                                    row["contract"]["entry_id"]))
    remaining = candle_budget
    states = []
    incomplete = []
    for row in normalized:
        contract = row["contract"]
        state = first_touch.initialize(contract, cutoff_utc=cutoff)
        while remaining and state["status"] not in first_touch.TERMINAL + ("BLOCKED_ENTRY", "DATA_MISSING"):
            index = bisect_left(times, contracts.utc(state["next_open_utc"]))
            previous = state["processed_candles"]
            suffix = (bars[i] for i in range(index, len(bars)))
            state = first_touch.advance(state, suffix, cutoff_utc=cutoff,
                                        max_candles=min(batch_size, remaining))
            consumed = state["processed_candles"] - previous
            remaining -= consumed
            if state["progress"] != "MORE_DATA_REQUIRED" or not consumed:
                break
        done = state["status"] in first_touch.TERMINAL or (
            state["status"] == "OPEN" and state["coverage_complete_through_cutoff"] is True)
        if not done:
            incomplete.append(contract["entry_id"])
        row["outcome"] = state
        states.append({"entry_id": contract["entry_id"], "outcome": state})
    # Both population completeness and completed computation are needed for an
    # aggregate eligibility result. Price-only snapshots can still be replayed.
    aggregate = gate.evaluate_gate(normalized, as_of_utc=cutoff,
        source_coverage_complete=snapshot["source_coverage_complete"] and not incomplete,
        policy=snapshot.get("gate_policy"))
    counts = Counter(item["outcome"]["status"] for item in states)
    return {
        "receipt_version": RECEIPT_VERSION, "snapshot_sha256": contracts.digest(snapshot),
        "dataset_id": snapshot["dataset_id"], "cohort_id": snapshot["cohort_id"],
        "source_route": snapshot["source_route"], "source_receipt": snapshot.get("source_receipt"),
        "source_provenance_verified_by_this_tool": False,
        "cutoff_utc": cutoff.isoformat(), "opportunities": len(normalized),
        "source_candles": len(bars), "candle_budget": candle_budget,
        "candle_evaluations": candle_budget - remaining, "batch_size": batch_size,
        "incomplete_entry_ids": incomplete, "computation_complete": not incomplete,
        "status_counts": {key: counts[key] for key in first_touch.STATUSES},
        "outcomes": states, "gate": aggregate,
        "runtime_authorized": False, "telegram_authorized": False, "trading_authorized": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--batch-size", type=int, default=1024)
    parser.add_argument("--candle-budget", type=int, default=2000000)
    args = parser.parse_args()
    try:
        receipt = replay_snapshot(load_snapshot(args.snapshot), batch_size=args.batch_size,
                                  candle_budget=args.candle_budget)
        serialized = json.dumps(receipt, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
        args.output.parent.mkdir(parents=True, exist_ok=True)
        # A fresh receipt cannot silently replace an earlier experiment.
        with args.output.open("x", encoding="utf-8") as stream:
            stream.write(serialized)
        print(json.dumps({"output": str(args.output), "opportunities": receipt["opportunities"],
                          "computation_complete": receipt["computation_complete"],
                          "experimental_eligible": receipt["gate"]["experimental_eligible"],
                          "trading_authorized": False}, allow_nan=False))
        return 0
    except (OSError, ValueError, TypeError, KeyError) as exc:
        parser.exit(2, "BLOCKED: " + str(exc) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
