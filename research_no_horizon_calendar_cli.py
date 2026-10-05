"""Build and register a finite calendar of frozen no-horizon research cohorts.

Build is offline. Registration uses only the explicit destination database and
the existing acquisition queue. It does not collect source data or run outcomes.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

import research_no_horizon_acquisition_store as acquisition
import research_no_horizon_calendar as calendar
from research_no_horizon_cohort_cli import _read
import research_no_horizon_contract as contracts
from research_no_horizon_worker import _connect

MAX_PLAN_BYTES = 4 * 1024 * 1024


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build")
    build.add_argument("--declaration", required=True, type=Path)
    build.add_argument("--windows", required=True, type=int)
    build.add_argument("--output", required=True, type=Path)
    register = commands.add_parser("register")
    register.add_argument("--plan", required=True, type=Path)
    register.add_argument("--output", required=True, type=Path)
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
            result = calendar.build_plan(value, window_count=args.windows)
        else:
            plan = calendar.validate_plan(value)
            write_url = os.getenv("RESEARCH_NO_HORIZON_DATABASE_URL", "").strip()
            if not write_url:
                raise ValueError("EXPLICIT_RESEARCH_DATABASE_SETTING_REQUIRED")
            with _connect(write_url) as conn:
                if not acquisition.schema_status(conn)["schema_present"]:
                    raise ValueError("ACQUISITION_SCHEMA_REQUIRED")
                result = calendar.register_plan(acquisition.AcquisitionStore(conn), plan)
        encoded = contracts.canonical(result) + "\n"
        if len(encoded.encode("utf-8")) > MAX_PLAN_BYTES:
            raise ValueError("CALENDAR_OUTPUT_BYTE_LIMIT_EXCEEDED")
        with args.output.open("x", encoding="utf-8") as stream:
            stream.write(encoded)
        print(contracts.canonical({"plan_sha256": result.get("plan_sha256"),
            "output": str(args.output), "runtime_authorized": False,
            "telegram_authorized": False, "trading_authorized": False}))
        return 0
    except Exception as exc:
        # Connection errors can contain credentials. Partial registration is
        # recoverable by retrying the same frozen plan; no dates are advanced.
        print("CALENDAR_FAILED: " + type(exc).__name__, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
