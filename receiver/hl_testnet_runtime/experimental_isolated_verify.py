"""Reproducible offline verification of the experimental execution candidate.

The parent copies Python sources/requirements and named test fixtures into temporary snapshots and
launches test children with a fresh environment. The existing test-only guard is
installed before importing tests. This is a Python I/O boundary, not an OS
sandbox. Neither deployment nor live account/database access is supported.

Example (use the Python environment with the project's test dependencies)::

    python -m hl_testnet_runtime.experimental_isolated_verify \
        --producer-root /path/to/producer --output /path/to/new-report-directory

``--postgres`` may initialize the existing verified, unmodified PostgreSQL
distribution in a disposable directory. No externally supplied database URL is
accepted. It refuses root normally. Producer database tests use a second empty
database in that same temporary server; their UUID databases are discarded with
the server. PostgreSQL is stopped before its data directory is removed.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from urllib.parse import urlsplit

from .render_preflight import core


SCHEMA = "experimental_isolated_verification_v1"
PRODUCER_MODULES = (
    "xrp_r2732_experimental_signal_selftest", "xrp_r2732_experimental_store_selftest",
    "xrp_r2732_experimental_worker_selftest", "xrp_r2732_hyperliquid_migration_selftest",
    "hype_row71205_experimental_signal_selftest", "hype_row71205_experimental_store_selftest",
    "hype_row71205_experimental_worker_selftest", "hype_row71205_source_probe_selftest",
    "sol_g65_experimental_selftest", "sol_proximity_experimental_selftest",
    "maxpain_experimental_refresh_selftest", "maxpain_hyperliquid_migration_selftest",
    "maxpain_pending_restore_20261006_selftest", "alert_cards_pipeline_selftest",
    "alert_cards_hook_selftest", "alert_cards_u21_selftest", "alert_cards_forwarder_selftest",
)
ENVIRONMENT_NAMES = frozenset({"PATH", "LANG", "TZ", "PYTHONUNBUFFERED",
    "PYTHONDONTWRITEBYTECODE", "PYTHONHASHSEED", "PYTHONPATH", "LC_CTYPE", "HL_JOURNAL_CI_URL",
    "TEST_DATABASE_URL"})
TEST_FIXTURES = ("raw_exact_cancelled_13_fixture.json",)


def file_digest(files):
    return hashlib.sha256(json.dumps(files, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def candidate_files(root):
    """Include only specifically named existing regression data, never arbitrary JSON."""
    root = Path(root)
    files = core.input_files(root)
    for name in TEST_FIXTURES:
        path = root / name
        if path.is_symlink():
            raise core.PreflightError("MANIFEST_SYMLINK_REFUSED")
        if path.exists():
            if not path.is_file() or path.stat().st_size > 1024 * 1024:
                raise core.PreflightError("TEST_FIXTURE_INVALID")
            files[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    return files


def snapshot_sources(root, destination):
    """Copy allowlisted files; exclude environment files, git metadata and other data."""
    root, destination = Path(root).resolve(), Path(destination)
    before = candidate_files(root)
    destination.mkdir(mode=0o700, parents=True, exist_ok=False)
    for relative, expected in before.items():
        raw = (root / relative).read_bytes()
        if hashlib.sha256(raw).hexdigest() != expected:
            raise core.PreflightError("CANDIDATE_CHANGED_DURING_SNAPSHOT")
        output = destination / relative
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(raw)
    if candidate_files(root) != before or candidate_files(destination) != before:
        raise core.PreflightError("CANDIDATE_CHANGED_DURING_SNAPSHOT")
    return {"files": before, "sha256": file_digest(before), "file_count": len(before)}


def git_head(root):
    """Read version identity only; credentials and remote URLs are never read."""
    try:
        result = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root,
            env=core.clean_environment(root), capture_output=True, timeout=10, check=False)
        head = result.stdout.decode("ascii", errors="ignore").strip()
        return head if result.returncode == 0 and re.fullmatch(r"[0-9a-f]{40}", head) else None
    except (OSError, subprocess.TimeoutExpired):
        return None


def producer_modules(root):
    names = set(PRODUCER_MODULES)
    names.update(path.stem for path in Path(root).glob("experimental_execution_*selftest.py"))
    # Explicit modules must exist; silently omitting missing tests loses evidence.
    return sorted(names)


def safe_identifier(test):
    identifier = test.id()
    return identifier if re.fullmatch(r"[A-Za-z0-9_.]{1,300}", identifier) else "unidentified_test"


class Result(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.failure_details = []

    def _record(self, test, error):
        if len(self.failure_details) < 30:
            self.failure_details.append({"test_id": safe_identifier(test),
                "exception_type": type(error[1]).__name__})

    def addError(self, test, error):
        self._record(test, error)
        super().addError(test, error)

    def addFailure(self, test, error):
        self._record(test, error)
        super().addFailure(test, error)

    def addSubTest(self, test, subtest, error):
        if error is not None:
            self._record(test, error)
        super().addSubTest(test, subtest, error)


def run_child(suite_name, root, output):
    """Called only by the clean-environment child process."""
    if set(os.environ) - ENVIRONMENT_NAMES:
        raise core.PreflightError("UNEXPECTED_CHILD_ENVIRONMENT")
    if suite_name != "producer" and "TEST_DATABASE_URL" in os.environ:
        raise core.PreflightError("UNEXPECTED_PRODUCER_DATABASE")
    from .render_preflight.guard import install
    attempts = install()
    sys.path.insert(0, str(root))
    loader = unittest.TestLoader()
    if suite_name == "runtime":
        suite = loader.discover(str(root / "hl_testnet_runtime"), pattern="test_*.py", top_level_dir=str(root))
        suite.addTests(loader.loadTestsFromNames([
            "hyperliquid_testnet_executor_selftest", "alert_cards_forwarder_selftest"]))
    else:
        suite = loader.loadTestsFromNames(producer_modules(root))
    # Text traces remain in the private log; structured reports contain IDs/counts.
    result = unittest.TextTestRunner(verbosity=1, resultclass=Result).run(suite)
    skipped_ids = [safe_identifier(test) for test, _reason in result.skipped]
    report = {"suite": suite_name, "tests_run": result.testsRun,
        "passed": result.testsRun - len(result.skipped) - len(result.errors) - len(result.failures),
        "skipped": len(result.skipped), "failures": len(result.failures), "errors": len(result.errors),
        "unexpected_successes": len(result.unexpectedSuccesses), "successful": result.wasSuccessful(),
        "network_attempts_blocked": len(attempts), "skipped_ids": skipped_ids,
        "failure_details": result.failure_details,
        "postgresql_configured": ("HL_JOURNAL_CI_URL" in os.environ if suite_name == "runtime"
                                  else "TEST_DATABASE_URL" in os.environ)}
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return 0 if result.wasSuccessful() and not attempts else 1


def final_status(stages, stable):
    if not stable or any(row.get("exit_code") != 0 or row.get("successful") is not True
                         or row.get("network_attempts_blocked") != 0 for row in stages):
        return "FAILED"
    return "PARTIAL_POSTGRESQL_NOT_VERIFIED" if any(row["skipped"] for row in stages) else "PASSED"


def create_producer_database(pg, ci_url):
    """Create the empty admin DB only on this runner's newly started server."""
    from .render_preflight.guard import PRODUCER_ADMIN_DB, database_target, producer_target
    runtime = database_target(core.validate_ci_url(ci_url), "hl_journal_ci")
    if not pg.started or pg.ci_url != ci_url or pg.status != "FRESH_LOOPBACK_POSTGRES18":
        raise core.PreflightError("FRESH_DATABASE_REQUIRED")
    result = pg._command(["createdb", "-h", str(pg.directory), "-p", str(runtime["port"]),
                          "-U", "preflight", PRODUCER_ADMIN_DB])
    if result["exit_code"] != 0 or result.get("failure_code"):
        raise core.PreflightError("PRODUCER_DATABASE_CREATE_FAILED")
    url = urlsplit(ci_url)._replace(path="/" + PRODUCER_ADMIN_DB).geturl()
    producer_target(url, runtime)
    return url


def cleanup_database(pg, temporary, report):
    """Stop while its data directory exists. Preserve private data if stop fails."""
    if pg is not None:
        try:
            was_started = pg.started
            pg.stop()
            if pg.started:
                raise core.PreflightError("POSTGRES_CLEANUP_FAILED")
            report["postgresql_cleanup"] = "STOPPED" if was_started else "NOT_STARTED"
        except Exception:
            report.update(status="FAILED", failure_code="POSTGRES_CLEANUP_FAILED",
                          postgresql_cleanup="FAILED")
            return
    if temporary is not None:
        try:
            shutil.rmtree(temporary)
        except OSError:
            report.update(status="FAILED", failure_code="TEMPORARY_CLEANUP_FAILED")


def verify(root, producer_root, output, *, postgres=False):
    root, producer_root, output = Path(root).resolve(), Path(producer_root).resolve(), Path(output).resolve()
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    output.chmod(0o700)
    started = datetime.now(timezone.utc).isoformat()
    report = {"schema": SCHEMA, "generated_at_utc": started, "status": "FAILED",
        "deployed": False, "live_account_access": False, "git_push_performed": False,
        "postgresql": "NOT_REQUESTED", "producer_postgresql": "NOT_CONFIGURED",
        "postgresql_cleanup": "NOT_STARTED",
        "limits": ["The test guard is a Python I/O boundary, not an OS network sandbox.",
                   "Synthetic exchange evidence does not prove live Testnet behavior.",
                   "Skipped PostgreSQL tests are not passes."], "stages": []}
    pg = None
    temporary = None
    try:
        temporary = tempfile.mkdtemp(prefix="experimental-isolated-")
        temp = Path(temporary)
        receiver_copy, producer_copy = temp / "receiver", temp / "producer"
        candidates = {"receiver": snapshot_sources(root, receiver_copy),
                      "producer": snapshot_sources(producer_root, producer_copy)}
        for name, original in (("receiver", root), ("producer", producer_root)):
            candidates[name]["git_head"] = git_head(original)
        report["candidates"] = candidates
        ci_url = producer_url = None
        if postgres:
            pg_dir = temp / "postgres"
            pg_dir.mkdir(mode=0o700)
            pg = core.DisposablePostgres(root, pg_dir)
            ci_url = pg.start()
            report["postgresql"] = pg.status
            if ci_url is not None:
                producer_url = create_producer_database(pg, ci_url)
                report["producer_postgresql"] = "FRESH_LOOPBACK_PRODUCER_DATABASE"
        for suite_name, snapshot in (("runtime", receiver_copy), ("producer", producer_copy)):
            child_output = output / (suite_name + ".json")
            env = core.clean_environment(receiver_copy, ci_url)
            if suite_name == "producer" and producer_url:
                env["TEST_DATABASE_URL"] = producer_url
            result = core.run_process([sys.executable, "-m", "hl_testnet_runtime.experimental_isolated_verify",
                "--child", suite_name, "--root", str(snapshot), "--output", str(child_output)],
                cwd=snapshot, env=env, log_path=output / (suite_name + ".log"), timeout=1800)
            row = {"suite": suite_name, **result}
            if child_output.is_file():
                row.update(json.loads(child_output.read_text()))
                row["report_sha256"] = hashlib.sha256(child_output.read_bytes()).hexdigest()
            report["stages"].append(row)
        stable = (candidate_files(receiver_copy) == candidates["receiver"]["files"]
                  and candidate_files(producer_copy) == candidates["producer"]["files"])
        report["snapshot_unchanged_after_tests"] = stable
        report["workspace_unchanged_since_snapshot"] = (
            candidate_files(root) == candidates["receiver"]["files"]
            and candidate_files(producer_root) == candidates["producer"]["files"])
        report["status"] = final_status(report["stages"],
                                        stable and report["workspace_unchanged_since_snapshot"])
    except Exception as error:
        report["failure_code"] = str(error) if isinstance(error, core.PreflightError) else "HARNESS_FAILED"
    finally:
        cleanup_database(pg, temporary, report)
        (output / "summary.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--producer-root", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--postgres", action="store_true")
    parser.add_argument("--child", choices=("runtime", "producer"), help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.child:
        return run_child(args.child, args.root.resolve(), args.output)
    if args.producer_root is None:
        parser.error("--producer-root is required")
    report = verify(args.root, args.producer_root, args.output, postgres=args.postgres)
    summary = {key: value for key, value in report.items() if key not in {"candidates", "stages"}}
    summary["stages"] = [{key: value for key, value in row.items() if key != "skipped_ids"}
                         for row in report["stages"]]
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 1 if report["status"] == "FAILED" else 0


if __name__ == "__main__":
    raise SystemExit(main())
