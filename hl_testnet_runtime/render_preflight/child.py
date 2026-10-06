"""Offline subprocess wrapper. Network guard is installed before repo imports."""
import argparse
import json
import os
from pathlib import Path
import runpy
import sys
import unittest
from urllib.parse import urlsplit


def install_network_guard():
    from .guard import install
    return install()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", choices=("timing_tests", "recorder", "feed", "runtime", "full_suite"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
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
        result = unittest.TextTestRunner(verbosity=1).run(suite)
        report = {"tests_run": result.testsRun, "failures": len(result.failures), "errors": len(result.errors),
                  "skipped": len(result.skipped), "unexpected_successes": len(result.unexpectedSuccesses),
                  "successful": result.wasSuccessful(), "network_attempts_blocked": len(attempts)}
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
