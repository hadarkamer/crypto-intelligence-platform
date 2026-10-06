"""Offline subprocess wrapper. Network guard is installed before repo imports."""
import argparse
import json
import os
from pathlib import Path
import re
import runpy
import sys
import unittest
from urllib.parse import urlsplit

MAX_FAILURE_DETAILS = 16
EXCEPTION_TYPES = frozenset({
    "AssertionError", "RuntimeError", "ValueError", "TypeError", "KeyError", "AttributeError",
    "ImportError", "ModuleNotFoundError", "TimeoutError", "OSError", "ConnectionError",
    "OperationalError", "ProgrammingError", "IntegrityError", "InternalError", "InterfaceError",
    "NotSupportedError", "PermissionError", "FileNotFoundError", "MemoryError", "RecursionError",
})
ERROR_CODES = frozenset({
    "PREFLIGHT_EXTERNAL_NETWORK_DISABLED", "PREFLIGHT_NONLOCAL_DATABASE_DISABLED",
    "PREFLIGHT_SUBPROCESS_REFUSED", "PREFLIGHT_CHILD_DATABASE_CHANGED", "NONLOCAL_DATABASE_REFUSED",
    "UNEXPECTED_CHILD_ENVIRONMENT", "FRESH_DATABASE_REQUIRED", "UNEXPECTED_EXTERNAL_NETWORK_ATTEMPT",
    "FILL_NOT_PROTECTED", "RECORDER_NOT_CLEANED_UP", "NONDETERMINISTIC_TRACE",
    "RECORDER_DID_NOT_DRAIN", "RECORDER_WORKER_LEAK", "WRITER_DID_NOT_START",
})


def exception_detail(exc):
    """Type allowlist and exact fixed codes only; never stringify an exception."""
    name = type(exc).__name__
    values = getattr(exc, "args", ())
    code = values[0] if type(values) is tuple and len(values) == 1 and type(values[0]) is str else None
    return {"exception_type": name if name in EXCEPTION_TYPES else "OtherError",
            "code": code if code in ERROR_CODES else "UNCLASSIFIED"}


def safe_test_id(test):
    identifier = getattr(test, "test_case", test).id()
    if (type(identifier) is str and len(identifier) <= 256
            and re.fullmatch(r"[A-Za-z0-9_.]+", identifier)
            and identifier.startswith(("hl_testnet_runtime.", "hyperliquid_testnet_executor_selftest.",
                                       "alert_cards_forwarder_selftest.", "unittest.loader."))):
        return identifier
    return "unidentified_test"


class DiagnosticTestResult(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.failure_details = []

    def _capture(self, test, error):
        if len(self.failure_details) < MAX_FAILURE_DETAILS:
            self.failure_details.append({"test_id": safe_test_id(test), **exception_detail(error[1])})

    def addError(self, test, err):
        self._capture(test, err)
        super().addError(test, err)

    def addFailure(self, test, err):
        self._capture(test, err)
        super().addFailure(test, err)

    def addSubTest(self, test, subtest, err):
        if err is not None:
            self._capture(test, err)
        super().addSubTest(test, subtest, err)


def write_failure_report(path, exc):
    report = {"schema": "render_preflight_failure_v1", "successful": False, **exception_detail(exc)}
    path.write_text(json.dumps(report, sort_keys=True) + "\n")


def install_network_guard():
    from .guard import install
    return install()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", choices=("timing_tests", "recorder", "feed", "runtime", "full_suite"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        return run_task(args)
    except Exception as exc:
        write_failure_report(args.output, exc)
        return 1


def run_task(args):
    attempts = install_network_guard()
    allowed = {"PATH", "LANG", "TZ", "PYTHONUNBUFFERED", "PYTHONDONTWRITEBYTECODE",
               "PYTHONHASHSEED", "PYTHONPATH", "HL_JOURNAL_CI_URL", "LC_CTYPE"}
    if set(os.environ) - allowed:
        raise RuntimeError("UNEXPECTED_CHILD_ENVIRONMENT")
    if "HL_JOURNAL_CI_URL" in os.environ:
        u = urlsplit(os.environ["HL_JOURNAL_CI_URL"])
        if (u.scheme != "postgresql" or u.hostname != "127.0.0.1" or u.path != "/hl_journal_ci"
                or u.username != "preflight" or not u.password or u.query or u.fragment):
            raise RuntimeError("NONLOCAL_DATABASE_REFUSED")
    if args.task in ("runtime", "full_suite") and "HL_JOURNAL_CI_URL" not in os.environ:
        raise RuntimeError("FRESH_DATABASE_REQUIRED")
    if args.task in ("timing_tests", "full_suite"):
        loader = unittest.TestLoader()
        if args.task == "timing_tests":
            suite = loader.loadTestsFromNames(["hl_testnet_runtime.test_passive_timing",
                                               "hl_testnet_runtime.test_passive_timing_hooks"])
        else:
            suite = loader.discover("hl_testnet_runtime", pattern="test_*.py", top_level_dir=".")
            suite.addTests(loader.loadTestsFromNames(["hyperliquid_testnet_executor_selftest",
                                                      "alert_cards_forwarder_selftest"]))
        result = unittest.TextTestRunner(verbosity=1, resultclass=DiagnosticTestResult).run(suite)
        report = {"tests_run": result.testsRun, "failures": len(result.failures), "errors": len(result.errors),
                  "skipped": len(result.skipped), "unexpected_successes": len(result.unexpectedSuccesses),
                  "successful": result.wasSuccessful(), "network_attempts_blocked": len(attempts),
                  "failure_details": result.failure_details}
        args.output.write_text(json.dumps(report, indent=2) + "\n")
        if not result.wasSuccessful() or result.skipped or attempts:
            return 1
    else:
        module = "hl_testnet_runtime.benchmark_passive_" + args.task
        sys.argv = [module, "--output", str(args.output)]
        if args.task == "runtime":
            sys.argv += ["--rounds", "4"]
        runpy.run_module(module, run_name="__main__")
        if attempts:
            raise RuntimeError("UNEXPECTED_EXTERNAL_NETWORK_ATTEMPT")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        # Detailed tracebacks could contain runtime state; the public summary uses fixed codes.
        print("PREFLIGHT_CHILD_FAILED", file=sys.stderr)
        raise SystemExit(1)
