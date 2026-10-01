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
from typing import Any

import research_no_horizon_replay as replay
import research_no_horizon_source as source
from research_no_horizon_experiment import LocalExperimentStore

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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    submit = commands.add_parser("submit")
    submit.add_argument("export", type=Path)
    submit.add_argument("--scopes", type=Path, required=True)
    submit.add_argument("--plan-key")
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
        if args.command != "submit" and not args.database.is_file():
            raise ValueError("local database does not exist")
        if args.command == "submit":
            export = load_json(args.export, max_bytes=source.MAX_BYTES)
            scopes = load_json(args.scopes, max_bytes=MAX_SCOPES_BYTES)
            if not isinstance(export, dict):
                raise ValueError("source export must be an object")
            if not isinstance(scopes, list) or not 1 <= len(scopes) <= MAX_SCOPES:
                raise ValueError("scope list must contain between 1 and 64 scopes")
        with LocalExperimentStore(args.database) as store:
            if args.command == "submit":
                plan_id = store.submit_plan(export, scopes, plan_key=args.plan_key)
                result = {"plan_id": plan_id, "trading_authorized": False}
            elif args.command == "run":
                result = store.run_plan(args.plan_id, args.worker_id,
                    scope_budget=args.scope_budget, candle_budget=args.candle_budget,
                    entry_budget=args.entry_budget, batch_size=args.batch_size)
            else:
                result = store.report(args.plan_id)
                serialized = json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
                with args.output.open("x", encoding="utf-8") as stream:
                    stream.write(serialized)
                result = {"plan_id": args.plan_id, "output": str(args.output),
                          "trading_authorized": False}
        print(json.dumps(result, ensure_ascii=False, allow_nan=False))
        return 0
    except (OSError, ValueError, TypeError, KeyError, sqlite3.Error) as exc:
        parser.exit(2, "BLOCKED: " + str(exc) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
