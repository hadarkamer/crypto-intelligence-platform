"""Local submission, bounded execution and reporting of one global cohort."""
from __future__ import annotations

import argparse
from pathlib import Path
import sqlite3
import sys

import research_no_horizon_cohort as cohort
from research_no_horizon_cohort_cli import _read
import research_no_horizon_cohort_outcomes as preparation
import research_no_horizon_cohort_store as store
import research_no_horizon_contract as contracts
import research_no_horizon_manifest as transport

_AUTHORITY = {"runtime_authorized": False, "telegram_authorized": False, "trading_authorized": False}


def _write(path, value):
    with Path(path).open("x", encoding="utf-8") as stream:
        stream.write(contracts.canonical(value) + "\n")


def _paths(args):
    database, output = Path(args.db), Path(args.output)
    if str(database) == ":memory:":
        raise ValueError("PERSISTENT_COHORT_DATABASE_REQUIRED")
    inputs = [Path(value) for value in ([args.declaration, args.anchor, *args.part]
        if args.command == "submit" else [])]
    if output.exists() or output.is_symlink():
        raise ValueError("OUTPUT_ALREADY_EXISTS")
    if (output.resolve() == database.resolve() or any(
            path.resolve() in (output.resolve(), database.resolve()) for path in inputs)):
        raise ValueError("INPUT_DATABASE_OUTPUT_PATH_COLLISION")
    if not output.parent.is_dir():
        raise ValueError("OUTPUT_DIRECTORY_MUST_EXIST")
    if args.command != "submit" and not database.is_file():
        raise ValueError("EXISTING_COHORT_DATABASE_REQUIRED")
    if args.command != "submit":
        # A typo or unrelated SQLite file must not trigger coordinator/child
        # bootstrap writes before an unknown-plan error is discovered.
        probe = sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True)
        try:
            names = {row[0] for row in probe.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if not {"cohort_store_version", "cohort_plans"} <= names:
                raise ValueError("EXISTING_COHORT_DATABASE_REQUIRED")
            version = probe.execute("SELECT version FROM cohort_store_version WHERE singleton=1").fetchone()
            if version is None or version[0] != store.VERSION:
                raise ValueError("UNSUPPORTED_COHORT_DATABASE_VERSION")
            if probe.execute("SELECT 1 FROM cohort_plans WHERE plan_id=?", (args.plan_id,)).fetchone() is None:
                raise ValueError("UNKNOWN_COHORT_PLAN")
        finally:
            probe.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("submit", "run", "report"):
        command = commands.add_parser(name)
        command.add_argument("--db", required=True)
        command.add_argument("--output", required=True)
        if name == "submit":
            command.add_argument("--declaration", required=True)
            command.add_argument("--anchor", required=True)
            command.add_argument("--part", action="append", required=True,
                help="Assembled export path, repeated in declared ordinal order")
            command.add_argument("--cohort-key")
        else:
            command.add_argument("plan_id")
        if name == "run":
            command.add_argument("--worker-id", required=True)
            command.add_argument("--scope-budget", type=int, default=1)
            command.add_argument("--candle-budget", type=int, default=1024)
            command.add_argument("--entry-budget", type=int, default=128)
            command.add_argument("--batch-size", type=int, default=128)
    args = parser.parse_args(argv)
    try:
        _paths(args)
        if args.command == "submit":
            declaration = cohort.normalize_declaration(_read(args.declaration, transport.MAX_MANIFEST_BYTES))
            anchor = _read(args.anchor, cohort.MAX_ANCHOR_BYTES)
            if len(args.part) != len(declaration["parts"]):
                raise ValueError("EXACT_COHORT_PART_FILE_SET_REQUIRED")
            try:
                prepared = preparation.prepare_cohort_submission(declaration, anchor,
                    lambda ordinal: _read(args.part[ordinal], declaration["parts"][ordinal]["source_byte_limit"]))
            except preparation.CohortInputBlocked as exc:
                # No SQLite connection has been opened. Preserve the complete
                # rejected source/parent receipt for all declared scopes.
                _write(args.output, {"status": "INPUT_BLOCKED", "coverage_receipt": exc.receipt,
                    "database_opened": False, **_AUTHORITY})
                return 2
            with store.LocalCohortStore(args.db) as database:
                plan_id = database._submit_prepared(prepared, cohort_key=args.cohort_key)
                receipt = database.report(plan_id)
        else:
            with store.LocalCohortStore(args.db) as database:
                receipt = database.report(args.plan_id) if args.command == "report" else database.run_cohort(
                    args.plan_id, args.worker_id, scope_budget=args.scope_budget,
                    candle_budget=args.candle_budget, entry_budget=args.entry_budget, batch_size=args.batch_size)
        _write(args.output, receipt)
        print(contracts.canonical({"plan_id": receipt["plan_id"], "output": str(args.output),
            "all_scopes_processed": receipt["all_scopes_processed"],
            "computation_complete": receipt["computation_complete"], **_AUTHORITY}))
        return 2 if receipt["all_scopes_processed"] and not receipt["computation_complete"] else 0
    except (ValueError, KeyError, TypeError, OSError, UnicodeError, OverflowError, sqlite3.Error) as exc:
        print("BLOCKED: " + str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
