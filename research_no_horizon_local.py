"""Explicit local no-horizon jobs: submit, bounded run, status and receipt.

No daemon, production connection, notifications or trading activation.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3

import research_no_horizon_replay as replay
from research_no_horizon_store import LocalResearchStore


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    submit = commands.add_parser("submit")
    submit.add_argument("snapshot", type=Path)
    submit.add_argument("--job-key")
    run = commands.add_parser("run")
    run.add_argument("--worker-id", required=True)
    run.add_argument("--job-id")
    run.add_argument("--candle-budget", type=int, default=1024)
    run.add_argument("--entry-budget", type=int, default=128)
    run.add_argument("--batch-size", type=int, default=128)
    run.add_argument("--lease-seconds", type=int, default=60)
    status = commands.add_parser("status")
    status.add_argument("job_id")
    receipt = commands.add_parser("receipt")
    receipt.add_argument("job_id")
    receipt.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        # Read commands never create a new accidental database.
        if args.command != "submit" and not args.database.is_file():
            raise ValueError("local database does not exist")
        with LocalResearchStore(args.database) as store:
            if args.command == "submit":
                job_id = store.submit_snapshot(replay.load_snapshot(args.snapshot), job_key=args.job_key)
                result = store.get_job(job_id)
            elif args.command == "run":
                result = store.run_once(args.worker_id, job_id=args.job_id,
                    candle_budget=args.candle_budget, entry_budget=args.entry_budget,
                    batch_size=args.batch_size, lease_seconds=args.lease_seconds)
                if result is None:
                    result = {"claimed": False, "reason": "NO_CLAIMABLE_JOB"}
            elif args.command == "status":
                result = store.get_job(args.job_id)
            else:
                result = store.get_receipt(args.job_id)
                if result is None:
                    raise ValueError("cohort is not completely processed; receipt unavailable")
                serialized = json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
                with args.output.open("x", encoding="utf-8") as stream:
                    stream.write(serialized)
                result = {"job_id": args.job_id, "output": str(args.output),
                    "receipt_sha256": result["receipt_sha256"],
                    "experimental_eligible": result["gate"]["experimental_eligible"],
                    "trading_authorized": False}
        print(json.dumps(result, ensure_ascii=False, allow_nan=False))
        return 0
    except (OSError, ValueError, TypeError, KeyError, sqlite3.Error) as exc:
        parser.exit(2, "BLOCKED: " + str(exc) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
