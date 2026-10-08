"""Export one verified research publication from an explicit read-only database.

The output directory is new and contains canonical evidence and its Markdown
view. Publication never registers, acquires, executes or delivers research.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

import research_no_horizon_acquisition_store as acquisition
import research_no_horizon_contract as contracts
import research_no_horizon_postgres_store as executor
import research_no_horizon_publication as publication
import research_no_horizon_selection as selection
from research_no_horizon_cohort_cli import _read
from research_no_horizon_worker import _connect_source as _connect_readonly

MAX_PLAN_BYTES = 16 * 1024 * 1024
MAX_REPORTS_BYTES = 128 * 1024 * 1024
MAX_RESULT_BYTES = 96 * 1024 * 1024
MAX_MARKDOWN_BYTES = 16 * 1024 * 1024


def _write_bundle(directory, encoded, markdown):
    # Reserve the directory exclusively after validation and bounds checks.
    # No existing directory or evidence file is replaced, including races.
    directory.mkdir()
    created = []
    try:
        for name, content in (("publication.json", encoded), ("report.md", markdown)):
            path = directory / name
            with path.open("x", encoding="utf-8", newline="\n") as stream:
                created.append(path)
                stream.write(content)
    except Exception:
        # Clean only files created by this invocation. Cleanup is best effort;
        # the two filesystem writes are not a cross-file atomic transaction.
        for path in reversed(created):
            try:
                path.unlink()
            except OSError:
                pass
        try:
            directory.rmdir()
        except OSError:
            pass
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--selection-plan", required=True, type=Path)
    parser.add_argument("--training-executor-plan-id", action="append", required=True,
                        help="Repeat for every training window in exact calendar order")
    parser.add_argument("--output-directory", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        output = args.output_directory
        if output.exists() or output.is_symlink():
            raise ValueError("OUTPUT_DIRECTORY_ALREADY_EXISTS")
        if not output.parent.is_dir():
            raise ValueError("OUTPUT_PARENT_DIRECTORY_MUST_EXIST")
        if output.resolve() in (args.plan.resolve(), args.selection_plan.resolve()):
            raise ValueError("INPUT_OUTPUT_PATH_COLLISION")
        plan = _read(args.plan, MAX_PLAN_BYTES)
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
            if not acquisition.schema_status(conn)["schema_present"]:
                raise ValueError("EXISTING_RESEARCH_SCHEMA_REQUIRED")
            backend = executor.PostgresCohortStore(conn)
            reports, total_bytes = [], 0
            for plan_id in ids:
                report = backend.report(plan_id)
                total_bytes += len(contracts.canonical(report).encode("utf-8"))
                if total_bytes > MAX_REPORTS_BYTES:
                    raise ValueError("PUBLICATION_TRAINING_REPORTS_BYTE_LIMIT_EXCEEDED")
                reports.append(report)
            result = publication.publish_plan(acquisition.AcquisitionStore(conn), backend,
                plan, selection_plan=selected, selection_reports=reports)
        encoded = contracts.canonical(result) + "\n"
        markdown = publication.render_markdown(result)
        if (len(encoded.encode("utf-8")) > MAX_RESULT_BYTES
                or len(markdown.encode("utf-8")) > MAX_MARKDOWN_BYTES):
            raise ValueError("PUBLICATION_OUTPUT_BYTE_LIMIT_EXCEEDED")
        _write_bundle(output, encoded, markdown)
        print(contracts.canonical({"output_directory": str(output),
            "publication_sha256": result["publication_sha256"], "state": result["state"],
            "runtime_authorized": False, "telegram_authorized": False, "trading_authorized": False}))
        return 2 if result["state"] == "INCOMPLETE" else 0
    except Exception as exc:
        # Driver messages can contain credentials or source payloads.
        print("PUBLICATION_FAILED: " + type(exc).__name__, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
