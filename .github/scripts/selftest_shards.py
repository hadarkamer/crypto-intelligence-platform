"""Run bounded partitions of every tracked self-test and verify their receipts."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

VERSION = "git-tracked-selftest-shards-v1"


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False)


def _git(*args):
    return subprocess.run(["git", *args], check=True, stdout=subprocess.PIPE).stdout


def _snapshot():
    scripts = sorted(path.decode("utf-8") for path in
                     _git("ls-files", "-z", "--", "*_selftest.py").split(b"\0") if path)
    if not scripts or len(scripts) != len(set(scripts)):
        raise ValueError("Self-test inventory must be nonempty and unique")
    return {"commit": _git("rev-parse", "HEAD").decode("ascii").strip(),
            "tree": _git("rev-parse", "HEAD^{tree}").decode("ascii").strip(),
            "scripts": scripts}


def _partition(snapshot, index, count):
    if type(count) is not int or not 1 <= count <= 256:
        raise ValueError("Invalid shard count")
    if type(index) is not int or not 0 <= index < count:
        raise ValueError("Invalid shard index")
    return snapshot["scripts"][index::count]


def _manifest(snapshot, index, count, completed):
    return {"version": VERSION, "checkout_commit_sha": snapshot["commit"],
            "checkout_tree_sha": snapshot["tree"],
            "inventory_sha256": hashlib.sha256(canonical(snapshot["scripts"]).encode()).hexdigest(),
            "inventory_count": len(snapshot["scripts"]),
            "shard_index": index, "shard_count": count,
            "planned_scripts": _partition(snapshot, index, count),
            "completed_scripts": completed}


def run_shard(index, count, output):
    output = Path(output)
    if output.exists() or output.is_symlink() or not output.parent.is_dir():
        raise ValueError("Manifest output must be a new file in an existing directory")
    snapshot = _snapshot()
    planned = _partition(snapshot, index, count)
    completed = []
    for script in planned:
        print("::group::Self-test " + script, flush=True)
        try:
            subprocess.run([sys.executable, script], check=True)
        finally:
            print("::endgroup::", flush=True)
        completed.append(script)
    # A failing child cannot create a receipt claiming the whole shard passed.
    receipt = _manifest(snapshot, index, count, completed)
    with output.open("x", encoding="utf-8") as stream:
        stream.write(canonical(receipt) + "\n")
    print(f"Completed shard {index}/{count}: {len(completed)} of {len(snapshot['scripts'])} tracked scripts", flush=True)
    return receipt


def _pairs(items):
    value = {}
    for key, item in items:
        if key in value:
            raise ValueError("Duplicate manifest key")
        value[key] = item
    return value


def verify_shards(count, directory):
    snapshot = _snapshot()
    _partition(snapshot, 0, count)
    directory = Path(directory)
    expected_names = {f"ci-selftests-{index}.json" for index in range(count)}
    if not directory.is_dir() or {path.name for path in directory.iterdir()} != expected_names:
        raise ValueError("Exact complete shard manifest set required")
    combined = []
    for index in range(count):
        path = directory / f"ci-selftests-{index}.json"
        if not path.is_file() or path.is_symlink():
            raise ValueError("Shard manifest must be a regular file")
        receipt = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_pairs)
        planned = _partition(snapshot, index, count)
        expected = _manifest(snapshot, index, count, planned)
        if canonical(receipt) != canonical(expected):
            raise ValueError(f"Shard {index} checkout, inventory or completion mismatch")
        combined.extend(receipt["completed_scripts"])
    if len(combined) != len(set(combined)) or sorted(combined) != snapshot["scripts"]:
        raise ValueError("Shard union must cover each tracked self-test exactly once")
    print(f"Verified all {len(combined)} tracked self-tests across {count} shards at {snapshot['commit']}", flush=True)
    return len(combined)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run")
    run.add_argument("--shard-index", type=int, required=True)
    run.add_argument("--shard-count", type=int, required=True)
    run.add_argument("--manifest", type=Path, required=True)
    verify = commands.add_parser("verify")
    verify.add_argument("--shard-count", type=int, required=True)
    verify.add_argument("--manifests-directory", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "run":
            run_shard(args.shard_index, args.shard_count, args.manifest)
        else:
            verify_shards(args.shard_count, args.manifests_directory)
        return 0
    except Exception as exc:
        print("SELFTEST_SHARDS_FAILED: " + type(exc).__name__ + ": " + str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
