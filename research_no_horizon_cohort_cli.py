"""Local single-cohort anchor/coverage tools; never opens a database connection."""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

import research_no_horizon_cohort as cohort
import research_no_horizon_cohort_coverage as coverage
import research_no_horizon_contract as contracts
import research_no_horizon_manifest as transport


def _read(path, limit):
    with Path(path).open("rb") as stream:
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise ValueError("COHORT_FILE_BYTE_LIMIT_EXCEEDED")
    return transport._strict_json(raw.decode("utf-8", errors="strict"))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("anchor-sql", "seal-anchor", "coverage"):
        command = commands.add_parser(name)
        command.add_argument("--declaration", required=True)
        command.add_argument("--output", required=True)
        if name != "anchor-sql":
            command.add_argument("--anchor", required=True)
        if name == "coverage":
            command.add_argument("--part", action="append", required=True,
                help="Assembled export path; repeat in declared ordinal order, including empty parts")
    args = parser.parse_args(argv)
    try:
        declaration = cohort.normalize_declaration(_read(args.declaration, transport.MAX_MANIFEST_BYTES))
        status = 0
        if args.command == "anchor-sql":
            output = cohort.anchor_sql(declaration)
        else:
            anchor = _read(args.anchor, cohort.MAX_ANCHOR_BYTES)
            if args.command == "seal-anchor":
                receipt = cohort.seal_anchor(declaration, anchor)
            else:
                if len(args.part) != len(declaration["parts"]):
                    raise ValueError("EXACT_COHORT_PART_FILE_SET_REQUIRED")
                receipt = coverage.preflight_cohort(declaration, anchor,
                    lambda ordinal: _read(args.part[ordinal], declaration["parts"][ordinal]["source_byte_limit"]))
                status = 0 if receipt["all_scopes_potentially_sufficient"] else 2
            output = contracts.canonical(receipt) + "\n"
        # Validate everything before creating a new file. Existing evidence is
        # never overwritten, including input/output path collisions.
        with Path(args.output).open("x", encoding="utf-8") as stream:
            stream.write(output)
        return status
    except (ValueError, TypeError, KeyError, OSError, UnicodeError, OverflowError) as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
