"""Register, acquire and inspect explicitly declared frozen research cohorts.

No schema creation, provider access, calendar scheduling or delivery. The two
database settings are explicit; source queries use separate read-only sessions.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

import research_no_horizon_acquisition_store as acquisition
import research_no_horizon_cohort as cohort
from research_no_horizon_cohort_cli import _read
import research_no_horizon_contract as contracts
import research_no_horizon_manifest as transport
from research_no_horizon_worker import _connect, _connect_source


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("register", "run", "report"):
        command = commands.add_parser(name)
        command.add_argument("--output", required=True, type=Path)
        if name == "register":
            command.add_argument("--declaration", required=True, type=Path)
            command.add_argument("--request-key")
        elif name == "report":
            command.add_argument("request_id")
            command.add_argument("--include-proofs", action="store_true")
        else:
            command.add_argument("--request-id")
            command.add_argument("--worker-id", required=True)
            command.add_argument("--leaf-budget", type=int, default=4)
            command.add_argument("--lease-seconds", type=int, default=120)
    args = parser.parse_args(argv)
    try:
        if args.output.exists() or args.output.is_symlink():
            raise ValueError("OUTPUT_ALREADY_EXISTS")
        if not args.output.parent.is_dir():
            raise ValueError("OUTPUT_DIRECTORY_MUST_EXIST")
        declaration = None
        if args.command == "register":
            if args.output.resolve() == args.declaration.resolve():
                raise ValueError("INPUT_OUTPUT_PATH_COLLISION")
            # Invalid declarations fail before opening the destination database.
            declaration = cohort.normalize_declaration(_read(args.declaration, transport.MAX_MANIFEST_BYTES))
        if args.command == "run":
            if not 1 <= args.leaf_budget <= 32 or not 1 <= args.lease_seconds <= 3600:
                raise ValueError("INVALID_ACQUISITION_BUDGET")
            if not args.worker_id or args.worker_id != args.worker_id.strip():
                raise ValueError("EXPLICIT_WORKER_ID_REQUIRED")
        write_url = os.getenv("RESEARCH_NO_HORIZON_DATABASE_URL", "").strip()
        read_url = os.getenv("RESEARCH_NO_HORIZON_READ_DATABASE_URL", "").strip()
        if not write_url or args.command == "run" and not read_url:
            raise ValueError("EXPLICIT_RESEARCH_DATABASE_SETTINGS_REQUIRED")
        with _connect(write_url) as conn:
            if not acquisition.schema_status(conn)["schema_present"]:
                raise ValueError("ACQUISITION_SCHEMA_REQUIRED")
            backend = acquisition.AcquisitionStore(conn)
            if args.command == "register":
                request_id = backend.register_request(declaration, request_key=args.request_key)
                result = backend.report(request_id)
            elif args.command == "run":
                result = backend.run_once(args.worker_id,
                    source_connection_factory=lambda: _connect_source(read_url),
                    request_id=args.request_id, leaf_budget=args.leaf_budget,
                    lease_seconds=args.lease_seconds)
                if result is None:
                    result = {"claimed": False, "reason": "NO_DUE_COMPATIBLE_REQUEST",
                        "request_id": args.request_id, "runtime_authorized": False,
                        "telegram_authorized": False, "trading_authorized": False}
            else:
                result = backend.report(args.request_id, include_proofs=args.include_proofs)
        with args.output.open("x", encoding="utf-8") as stream:
            stream.write(contracts.canonical(result) + "\n")
        print(contracts.canonical({"request_id": result.get("request_id"),
            "status": result.get("status"), "output": str(args.output),
            "runtime_authorized": False, "telegram_authorized": False,
            "trading_authorized": False}))
        return 2 if result.get("status") == "BLOCKED" else 0
    except Exception as exc:
        # Driver diagnostics can contain credentials, SQL and source payloads.
        # A blocked acquisition's full evidence belongs in its explicit report.
        print("ACQUISITION_FAILED: " + type(exc).__name__, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
