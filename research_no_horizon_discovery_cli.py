"""Plan bounded catalog research, register it, and read single-window rankings.

Build is offline. Registration writes only explicit frozen acquisition requests.
Ranking opens the explicit destination in read-only mode and never runs jobs.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

import research_no_horizon_acquisition_store as acquisition
import research_no_horizon_discovery as discovery
import research_no_horizon_ranking as ranking
import research_no_horizon_postgres_store as executor
from research_no_horizon_cohort_cli import _read
import research_no_horizon_contract as contracts
from research_no_horizon_worker import _connect, _connect_source as _connect_readonly

MAX_PLAN_BYTES = 4 * 1024 * 1024
MAX_RESULT_BYTES = 16 * 1024 * 1024


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build")
    build.add_argument("--declaration", required=True, type=Path)
    build.add_argument("--base-direction", action="append", required=True, choices=("LONG", "SHORT"))
    build.add_argument("--threshold-pct", action="append", required=True, type=float)
    build.add_argument("--candidate-key", action="append")
    build.add_argument("--windows", type=int, default=1)
    build.add_argument("--output", required=True, type=Path)
    for name in ("register", "rank"):
        command = commands.add_parser(name)
        command.add_argument("--plan", required=True, type=Path)
        command.add_argument("--output", required=True, type=Path)
        if name == "rank":
            command.add_argument("--executor-plan-id", required=True)
            command.add_argument("--window-ordinal", type=int, default=0)
    args = parser.parse_args(argv)
    try:
        if args.output.exists() or args.output.is_symlink():
            raise ValueError("OUTPUT_ALREADY_EXISTS")
        if not args.output.parent.is_dir():
            raise ValueError("OUTPUT_DIRECTORY_MUST_EXIST")
        input_path = args.declaration if args.command == "build" else args.plan
        if args.output.resolve() == input_path.resolve():
            raise ValueError("INPUT_OUTPUT_PATH_COLLISION")
        value = _read(input_path, MAX_PLAN_BYTES)
        if args.command == "build":
            result = discovery.build_plan(value, base_directions=args.base_direction,
                thresholds_pct=args.threshold_pct, candidate_keys=args.candidate_key,
                window_count=args.windows)
        else:
            plan = discovery.validate_plan(value)
            if args.command == "rank":
                if not 0 <= args.window_ordinal < len(plan["calendar_plan"]["windows"]):
                    raise ValueError("INVALID_DISCOVERY_WINDOW")
                if (len(args.executor_plan_id) != 64
                        or any(ch not in "0123456789abcdef" for ch in args.executor_plan_id)):
                    raise ValueError("EXPLICIT_EXECUTOR_PLAN_ID_REQUIRED")
            url = os.getenv("RESEARCH_NO_HORIZON_DATABASE_URL", "").strip()
            if not url:
                raise ValueError("EXPLICIT_RESEARCH_DATABASE_SETTING_REQUIRED")
            connect = _connect if args.command == "register" else _connect_readonly
            with connect(url) as conn:
                schema = acquisition if args.command == "register" else executor
                if not schema.schema_status(conn)["schema_present"]:
                    raise ValueError("EXISTING_RESEARCH_SCHEMA_REQUIRED")
                if args.command == "register":
                    result = discovery.register_plan(acquisition.AcquisitionStore(conn), plan)
                else:
                    report = executor.PostgresCohortStore(conn).report(args.executor_plan_id)
                    result = ranking.rank_report(plan, report, window_ordinal=args.window_ordinal)
        encoded = contracts.canonical(result) + "\n"
        limit = MAX_PLAN_BYTES if args.command == "build" else MAX_RESULT_BYTES
        if len(encoded.encode("utf-8")) > limit:
            raise ValueError("DISCOVERY_OUTPUT_BYTE_LIMIT_EXCEEDED")
        with args.output.open("x", encoding="utf-8") as stream:
            stream.write(encoded)
        print(contracts.canonical({"output": str(args.output),
            "runtime_authorized": False, "telegram_authorized": False, "trading_authorized": False}))
        return 2 if args.command == "rank" and result.get("global_ranking_complete") is not True else 0
    except Exception as exc:
        # Driver errors can contain credentials. Retry the same immutable plan
        # after interrupted registration; no scope/window is silently skipped.
        print("DISCOVERY_FAILED: " + type(exc).__name__, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
