"""Freeze or evaluate one prospective cohort from verified research reports.

All commands read training evidence from the explicit research destination.
Only register opens a write connection; no command runs acquisition or outcomes.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

import research_no_horizon_acquisition_store as acquisition
import research_no_horizon_contract as contracts
import research_no_horizon_postgres_store as executor
import research_no_horizon_selection as selection
import research_no_horizon_validation as validation
from research_no_horizon_cohort_cli import _read
from research_no_horizon_worker import _connect, _connect_source as _connect_readonly

MAX_PLAN_BYTES = 16 * 1024 * 1024
MAX_REPORTS_BYTES = 128 * 1024 * 1024
MAX_RESULT_BYTES = 64 * 1024 * 1024


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("build", "register", "evaluate"):
        command = commands.add_parser(name)
        command.add_argument("--declaration" if name == "build" else "--plan",
                             required=True, type=Path)
        command.add_argument("--selection-plan", required=True, type=Path)
        command.add_argument("--training-executor-plan-id", action="append", required=True,
                             help="Repeat for every training window in exact calendar order")
        command.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.output.exists() or args.output.is_symlink():
            raise ValueError("OUTPUT_ALREADY_EXISTS")
        if not args.output.parent.is_dir():
            raise ValueError("OUTPUT_DIRECTORY_MUST_EXIST")
        input_path = args.declaration if args.command == "build" else args.plan
        if args.output.resolve() in (input_path.resolve(), args.selection_plan.resolve()):
            raise ValueError("INPUT_OUTPUT_PATH_COLLISION")
        value = _read(input_path, MAX_PLAN_BYTES)
        selected = selection.validate_plan(_read(args.selection_plan, MAX_PLAN_BYTES))
        ids = args.training_executor_plan_id
        if (len(ids) != len(selected["discovery_plan"]["calendar_plan"]["windows"])
                or len(set(ids)) != len(ids)
                or any(len(item) != 64 or any(ch not in "0123456789abcdef" for ch in item)
                       for item in ids)):
            raise ValueError("EXACT_ORDERED_TRAINING_PLAN_IDS_REQUIRED")
        url = os.getenv("RESEARCH_NO_HORIZON_DATABASE_URL", "").strip()
        if not url:
            raise ValueError("EXPLICIT_RESEARCH_DATABASE_SETTING_REQUIRED")
        with _connect_readonly(url) as conn:
            schema = acquisition if args.command == "evaluate" else executor
            if not schema.schema_status(conn)["schema_present"]:
                raise ValueError("EXISTING_RESEARCH_SCHEMA_REQUIRED")
            backend = executor.PostgresCohortStore(conn)
            reports, total_bytes = [], 0
            for plan_id in ids:
                report = backend.report(plan_id)
                total_bytes += len(contracts.canonical(report).encode("utf-8"))
                if total_bytes > MAX_REPORTS_BYTES:
                    raise ValueError("VALIDATION_TRAINING_REPORTS_BYTE_LIMIT_EXCEEDED")
                reports.append(report)
            evidence = {"selection_plan": selected, "selection_reports": reports}
            if args.command == "build":
                result = validation.build_plan(value, **evidence)
            else:
                plan = validation.validate_plan(value, **evidence)
                if args.command == "evaluate":
                    result = validation.evaluate_plan(acquisition.AcquisitionStore(conn),
                                                      backend, plan, **evidence)
        if args.command == "register":
            # Finish immutable training reads before opening the write session.
            with _connect(url) as conn:
                if not acquisition.schema_status(conn)["schema_present"]:
                    raise ValueError("EXISTING_RESEARCH_SCHEMA_REQUIRED")
                result = validation.register_plan(acquisition.AcquisitionStore(conn),
                                                  plan, **evidence)
        encoded = contracts.canonical(result) + "\n"
        limit = MAX_PLAN_BYTES if args.command == "build" else MAX_RESULT_BYTES
        if len(encoded.encode("utf-8")) > limit:
            raise ValueError("VALIDATION_OUTPUT_BYTE_LIMIT_EXCEEDED")
        with args.output.open("x", encoding="utf-8") as stream:
            stream.write(encoded)
        print(contracts.canonical({"output": str(args.output),
            "runtime_authorized": False, "telegram_authorized": False, "trading_authorized": False}))
        return 2 if args.command == "evaluate" and result.get("validation_complete") is not True else 0
    except Exception as exc:
        # Never echo driver messages, credentials, or underlying source payloads.
        print("VALIDATION_FAILED: " + type(exc).__name__, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
