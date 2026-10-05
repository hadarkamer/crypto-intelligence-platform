"""Freeze a cross-window selector, register it, or read its exact executor reports.

Build is offline. Registration uses the existing explicit acquisition queue.
Selection opens only the explicit destination in read-only mode; it runs no jobs.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

import research_no_horizon_acquisition_store as acquisition
import research_no_horizon_selection as selection
import research_no_horizon_postgres_store as executor
import research_no_horizon_contract as contracts
from research_no_horizon_cohort_cli import _read
from research_no_horizon_worker import _connect, _connect_source as _connect_readonly

MAX_PLAN_BYTES = 4 * 1024 * 1024
MAX_REPORTS_BYTES = 128 * 1024 * 1024
MAX_RESULT_BYTES = 64 * 1024 * 1024


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build")
    build.add_argument("--declaration", required=True, type=Path)
    build.add_argument("--base-direction", action="append", required=True, choices=("LONG", "SHORT"))
    build.add_argument("--threshold-pct", action="append", required=True, type=float)
    build.add_argument("--candidate-key", action="append")
    build.add_argument("--windows", type=int, default=1)
    build.add_argument("--top-k", type=int, required=True)
    build.add_argument("--required-eligible-windows", type=int, required=True)
    build.add_argument("--output", required=True, type=Path)
    for name in ("register", "select"):
        command = commands.add_parser(name)
        command.add_argument("--plan", required=True, type=Path)
        command.add_argument("--output", required=True, type=Path)
        if name == "select":
            command.add_argument("--executor-plan-id", action="append", required=True,
                                 help="Repeat in exact calendar window order, including every window")
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
            result = selection.build_plan(value, base_directions=args.base_direction,
                thresholds_pct=args.threshold_pct, candidate_keys=args.candidate_key,
                window_count=args.windows, top_k=args.top_k,
                required_eligible_windows=args.required_eligible_windows)
        else:
            plan = selection.validate_plan(value)
            if args.command == "select":
                ids = args.executor_plan_id
                if (len(ids) != len(plan["discovery_plan"]["calendar_plan"]["windows"])
                        or len(set(ids)) != len(ids)
                        or any(len(item) != 64 or any(ch not in "0123456789abcdef" for ch in item)
                               for item in ids)):
                    raise ValueError("EXACT_ORDERED_EXECUTOR_PLAN_IDS_REQUIRED")
            url = os.getenv("RESEARCH_NO_HORIZON_DATABASE_URL", "").strip()
            if not url:
                raise ValueError("EXPLICIT_RESEARCH_DATABASE_SETTING_REQUIRED")
            connect = _connect if args.command == "register" else _connect_readonly
            with connect(url) as conn:
                schema = acquisition if args.command == "register" else executor
                if not schema.schema_status(conn)["schema_present"]:
                    raise ValueError("EXISTING_RESEARCH_SCHEMA_REQUIRED")
                if args.command == "register":
                    result = selection.register_plan(acquisition.AcquisitionStore(conn), plan)
                else:
                    backend = executor.PostgresCohortStore(conn)
                    reports, total_bytes = [], 0
                    for plan_id in ids:
                        report = backend.report(plan_id)
                        total_bytes += len(contracts.canonical(report).encode("utf-8"))
                        if total_bytes > MAX_REPORTS_BYTES:
                            raise ValueError("SELECTION_REPORTS_BYTE_LIMIT_EXCEEDED")
                        reports.append(report)
                    result = selection.select_reports(plan, reports)
        encoded = contracts.canonical(result) + "\n"
        limit = MAX_PLAN_BYTES if args.command == "build" else MAX_RESULT_BYTES
        if len(encoded.encode("utf-8")) > limit:
            raise ValueError("SELECTION_OUTPUT_BYTE_LIMIT_EXCEEDED")
        with args.output.open("x", encoding="utf-8") as stream:
            stream.write(encoded)
        print(contracts.canonical({"output": str(args.output),
            "runtime_authorized": False, "telegram_authorized": False, "trading_authorized": False}))
        return 2 if args.command == "select" and result.get("selection_complete") is not True else 0
    except Exception as exc:
        # Driver messages may contain credentials or source payloads.
        print("SELECTION_FAILED: " + type(exc).__name__, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
