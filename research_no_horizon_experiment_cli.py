"""Freeze and run bounded exploratory scope plans against an existing export.

All work is local. No network, environment credentials, production mutation,
background daemon, notification or trading path is provided.
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import sqlite3
import sys
from typing import Any

import research_no_horizon_replay as replay
import research_no_horizon_source as source
from research_no_horizon_experiment import LocalExperimentStore, SourcePreflightBlocked, ParentCoverageBlocked, prepare_submission
from research_no_horizon_preflight import preflight_source_features
from research_no_horizon_parent_coverage import preflight_parent_coverage

MAX_SCOPES_BYTES = 64 * 1024
MAX_SCOPES = 64


def _finite_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        replay._nonfinite(value)
    return parsed


def load_json(path: Path, *, max_bytes: int) -> Any:
    """Bound input bytes and reject duplicate keys and nonfinite JSON numbers."""
    with path.open("rb") as stream:
        raw = stream.read(max_bytes + 1)
    if len(raw) > max_bytes:
        raise ValueError("input exceeds its bounded byte limit")
    return json.loads(raw, object_pairs_hook=replay._object,
                      parse_constant=replay._nonfinite, parse_float=_finite_float)


def _write_report(path, result):
    serialized = json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    with path.open("x", encoding="utf-8") as stream:
        stream.write(serialized)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path)
    commands = parser.add_subparsers(dest="command", required=True)
    submit = commands.add_parser("submit")
    submit.add_argument("export", type=Path)
    submit.add_argument("--scopes", type=Path, required=True)
    submit.add_argument("--plan-key")
    submit.add_argument("--preflight-output", type=Path)
    submit.add_argument("--parent-coverage-output", type=Path)
    submit.add_argument("--require-parent-coverage", action="store_true",
        help="Require complete matched-parent coverage reaching the gate minimum in EVERY declared scope; not gate qualification.")
    submit.add_argument("--allow-incomplete-source", action="store_true",
        help="Freeze an explicit diagnostic policy; source blockers and false gate eligibility are preserved.")
    preflight = commands.add_parser("preflight")
    preflight.add_argument("export", type=Path)
    preflight.add_argument("--scopes", type=Path, required=True)
    preflight.add_argument("--output", type=Path, required=True)
    coverage = commands.add_parser("coverage")
    coverage.add_argument("export", type=Path)
    coverage.add_argument("--scopes", type=Path, required=True)
    coverage.add_argument("--output", type=Path, required=True)
    run = commands.add_parser("run")
    run.add_argument("plan_id")
    run.add_argument("--worker-id", required=True)
    run.add_argument("--scope-budget", type=int, default=1)
    run.add_argument("--candle-budget", type=int, default=1024)
    run.add_argument("--entry-budget", type=int, default=128)
    run.add_argument("--batch-size", type=int, default=128)
    report = commands.add_parser("report")
    report.add_argument("plan_id")
    report.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command not in ("preflight", "coverage") and args.database is None:
            raise ValueError("--database is required for submit, run and report")
        if args.command in ("run", "report") and not args.database.is_file():
            raise ValueError("local database does not exist")
        if args.command == "submit":
            if args.parent_coverage_output is not None and not args.require_parent_coverage:
                raise ValueError("--parent-coverage-output requires --require-parent-coverage")
            if (args.preflight_output is not None and args.parent_coverage_output is not None
                    and args.preflight_output.resolve() == args.parent_coverage_output.resolve()):
                raise ValueError("source and parent coverage outputs must be distinct")
        if args.command in ("submit", "preflight", "coverage"):
            export = load_json(args.export, max_bytes=source.MAX_BYTES)
            scopes = load_json(args.scopes, max_bytes=MAX_SCOPES_BYTES)
            if not isinstance(export, dict):
                raise ValueError("source export must be an object")
            if not isinstance(scopes, list) or not 1 <= len(scopes) <= MAX_SCOPES:
                raise ValueError("scope list must contain between 1 and 64 scopes")
        if args.command == "preflight":
            result = preflight_source_features(export, scopes)
            _write_report(args.output, result)
            print(json.dumps({"output": str(args.output),
                "ready_for_outcome_research": result["ready_for_outcome_research"],
                "receipt_sha256": result["receipt_sha256"], "trading_authorized": False}))
            return 0 if result["ready_for_outcome_research"] else 2
        if args.command == "coverage":
            result = preflight_parent_coverage(export, scopes)
            _write_report(args.output, result)
            print(json.dumps({"output": str(args.output),
                "all_scopes_potentially_sufficient": result["all_scopes_potentially_sufficient"],
                "any_scope_potentially_sufficient": result["any_scope_potentially_sufficient"],
                "receipt_sha256": result["receipt_sha256"], "trading_authorized": False}))
            return 0 if result["all_scopes_potentially_sufficient"] else 2
        if args.command == "submit":
            # The whole scope set is checked once, before opening/creating SQLite.
            try:
                prepared = prepare_submission(export, scopes,
                    allow_incomplete_source=args.allow_incomplete_source,
                    require_parent_coverage=args.require_parent_coverage)
            except ParentCoverageBlocked as exc:
                if args.preflight_output is not None:
                    _write_report(args.preflight_output, exc.source_receipt)
                if args.parent_coverage_output is not None:
                    _write_report(args.parent_coverage_output, exc.receipt)
                print(json.dumps(exc.receipt, ensure_ascii=False, allow_nan=False))
                print("BLOCKED: MATCHED_PARENT_COVERAGE_PREFLIGHT_BLOCKED", file=sys.stderr)
                return 2
            except SourcePreflightBlocked as exc:
                if args.preflight_output is not None:
                    _write_report(args.preflight_output, exc.receipt)
                print(json.dumps(exc.receipt, ensure_ascii=False, allow_nan=False))
                print("BLOCKED: SOURCE_FEATURE_PREFLIGHT_BLOCKED", file=sys.stderr)
                return 2
            if args.preflight_output is not None:
                _write_report(args.preflight_output, prepared.receipt)
            if args.parent_coverage_output is not None:
                _write_report(args.parent_coverage_output, prepared.parent_coverage_receipt)
        with LocalExperimentStore(args.database) as store:
            if args.command == "submit":
                plan_id = store._submit_prepared(prepared, plan_key=args.plan_key)
                result = {"plan_id": plan_id, "trading_authorized": False}
            elif args.command == "run":
                result = store.run_plan(args.plan_id, args.worker_id,
                    scope_budget=args.scope_budget, candle_budget=args.candle_budget,
                    entry_budget=args.entry_budget, batch_size=args.batch_size)
            else:
                result = store.report(args.plan_id)
                _write_report(args.output, result)
                result = {"plan_id": args.plan_id, "output": str(args.output),
                          "trading_authorized": False}
        print(json.dumps(result, ensure_ascii=False, allow_nan=False))
        return 0
    except (OSError, ValueError, TypeError, KeyError, sqlite3.Error) as exc:
        parser.exit(2, "BLOCKED: " + str(exc) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
