"""Finite two-process experiment; calls the existing generated task directly.

Never starts app.py, collection_bridge, a queue, a scheduler, or a database.
Source and OpenAI credentials stay in the process environment. Only the two
existing source-session variables and the existing OpenAI configuration pass
to children. Output is private job logs, not an HTTP endpoint.
"""
from __future__ import annotations

import argparse
import base64
import contextlib
import gzip
import hashlib
import json
import os
from pathlib import Path
import resource
import runpy
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone

BASE_SHA = "f6adfc6fea05bfd82a59098433ad1c2f31f6267f"
SERVICE = "srv-dakhacou01pc73f58aug"
LIMIT_JOBS = 12
JOB_SECONDS = 360
EXPERIMENT_SECONDS = 2400
SAMPLE_SECONDS = 0.5
MEMORY_STOP_FRACTION = 0.85
SESSION_KEYS = ("COINGLASS_COOKIE_HEADER", "COINGLASS_STORAGE_STATE_JSON")
AI_KEYS = ("OPENAI_API_KEY", "OPENAI_MARKET_SCANNER_MODEL")
SYSTEM_KEYS = ("PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "TZ",
               "SSL_CERT_FILE", "SSL_CERT_DIR", "REQUESTS_CA_BUNDLE",
               "PLAYWRIGHT_BROWSERS_PATH", "LD_LIBRARY_PATH")


def stamp():
    return datetime.now(timezone.utc).isoformat()


def emit(event, **data):
    print("CG2 " + json.dumps({"event": event, **data}, separators=(",", ":"),
                             allow_nan=False), flush=True)


def safe_environment(source):
    env = {k: source[k] for k in SYSTEM_KEYS + SESSION_KEYS + AI_KEYS if source.get(k)}
    env.update(PYTHONUNBUFFERED="1", PYTHONDONTWRITEBYTECODE="1",
               COLLECTION_HEATMAP_MODEL="3", COLLECTION_BRIDGE_ENABLED="false",
               DATABASE_URL="", SUPABASE_SERVICE_ROLE_KEY="",
               COINGLASS_COLLECTOR_TOKEN="")
    return env


def read_json(path, default=None):
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return default


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def proc_stats():
    found = {}
    for item in Path("/proc").iterdir():
        if not item.name.isdigit():
            continue
        try:
            data = (item / "stat").read_text()
            fields = data[data.rindex(")") + 2:].split()
            found[int(item.name)] = {
                "state": fields[0],
                "ppid": int(fields[1]), "group": int(fields[2]),
                "ticks": int(fields[11]) + int(fields[12]),
                "start": int(fields[19]), "rss": int(fields[21]) * os.sysconf("SC_PAGE_SIZE"),
            }
        except (OSError, ValueError, IndexError):
            pass
    return found


def process_members(root, all_stats):
    members = {p for p, v in all_stats.items() if p == root or v["group"] == root}
    while True:
        more = {p for p, v in all_stats.items() if v["ppid"] in members}
        if more <= members:
            return members
        members |= more


def pss_bytes(pid):
    try:
        for line in Path(f"/proc/{pid}/smaps_rollup").read_text().splitlines():
            if line.startswith("Pss:"):
                return int(line.split()[1]) * 1024
    except (OSError, ValueError):
        pass
    return None


def cgroup_snapshot():
    # Resolve the process's actual cgroup, not an unrelated ancestor by guess.
    paths = [Path("/sys/fs/cgroup")]
    try:
        relative = next(l.split(":", 2)[2] for l in Path("/proc/self/cgroup").read_text().splitlines()
                        if l.startswith("0::"))
        nested = Path("/sys/fs/cgroup") / relative.lstrip("/")
        if (nested / "cpu.stat").is_file():
            paths.insert(0, nested)
    except (OSError, StopIteration):
        pass
    root = next((p for p in paths if (p / "cpu.stat").is_file()), None)
    if root is None:
        return {"available": False}
    result = {"available": True}
    for name in ("memory.current", "memory.max", "memory.peak", "cpu.max"):
        try:
            value = (root / name).read_text().strip()
            result[name] = int(value) if value.isdigit() else value
        except OSError:
            result[name] = None
    for name in ("cpu.stat", "memory.events"):
        try:
            result[name] = {k: int(v) for k, v in (line.split() for line in (root / name).read_text().splitlines())}
        except (OSError, ValueError):
            result[name] = None
    return result


def validate_result(path, job_id, timeframe):
    data = read_json(path / "result.json")
    if not isinstance(data, dict):
        return False, "result_missing", None
    evidence = data.get("evidence", {})
    identity = (data.get("run_id") == job_id and data.get("symbol") == "BTC"
                and data.get("heatmap_model") == 3 and data.get("timeframe") == timeframe
                and evidence.get("artifact_id") == job_id)
    image = path / "image.png"
    if not identity:
        return False, "result_identity_mismatch", data
    if not image.is_file() or digest(image) != evidence.get("sha256"):
        return False, "image_digest_mismatch", data
    if data.get("provider") != "OpenAI" or not data.get("model"):
        return False, "analysis_identity_missing", data
    snapshot = data.get("hover_snapshot")
    if not isinstance(snapshot, dict) or snapshot.get("complete") is not True:
        return False, "complete_hover_missing", data
    return True, None, data


def run_child(runtime, output, timeframe, job_id):
    os.umask(0o077)
    inherited = safe_environment(os.environ)
    os.environ.clear()
    os.environ.update(inherited)
    output.mkdir(parents=True, exist_ok=False)
    sys.path.insert(0, str(runtime))
    import model1_execution as execution
    events = []
    epoch = time.monotonic()
    current = ["startup", epoch]
    original_stage = execution.stage
    original_operation = execution.capture_operation
    original_remember = execution.remember_analysis
    facts = {}

    def stage(value):
        now = time.monotonic()
        events.append({"kind": "stage", "name": current[0], "start_seconds": current[1] - epoch,
                       "end_seconds": now - epoch, "seconds": now - current[1]})
        current[:] = [value, now]
        return original_stage(value)

    @contextlib.contextmanager
    def operation(name):
        began = time.monotonic()
        success = False
        try:
            with original_operation(name):
                yield
            success = True
        finally:
            ended = time.monotonic()
            events.append({"kind": "capture_operation", "name": name,
                           "start_seconds": began - epoch, "end_seconds": ended - epoch,
                           "seconds": ended - began, "success": success})

    def remember(raw):
        original_remember(raw)
        facts.update(execution._analysis_facts)

    execution.stage = stage
    execution.capture_operation = operation
    execution.remember_analysis = remember
    began_at = stamp()
    sys.argv = [str(runtime / "collection_model1_task.py"), timeframe, job_id, str(output)]
    exit_code = 1
    try:
        runpy.run_path(sys.argv[0], run_name="__main__")
        exit_code = 0
    except SystemExit as exc:
        exit_code = exc.code if isinstance(exc.code, int) else 1
    finally:
        stage("experiment_finished")
        own, children = resource.getrusage(resource.RUSAGE_SELF), resource.getrusage(resource.RUSAGE_CHILDREN)
        measurement = {"started_at": began_at, "finished_at": stamp(),
                       "elapsed_seconds": time.monotonic() - epoch, "exit_code": exit_code,
                       "events": events, "analysis_facts": facts,
                       "reaped_process_cpu_seconds": own.ru_utime + own.ru_stime + children.ru_utime + children.ru_stime,
                       "forbidden_modules_loaded": sorted(set(sys.modules) & {"collection_bridge", "psycopg", "psycopg2"})}
        (output / "measurement.json").write_text(json.dumps(measurement, allow_nan=False))
    return exit_code


def clean_group(process):
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            pass
    process.wait(timeout=3)


def clean_tracked_children(job):
    # Chromium descendants can have their own process group. Only kill PIDs
    # actually observed under this job and still carrying the same start time.
    for sig in (signal.SIGTERM, signal.SIGKILL):
        current = proc_stats()
        for pid, start in job["seen_processes"]:
            if (pid != os.getpid() and pid in current and current[pid]["start"] == start
                    and current[pid]["state"] != "Z"):
                try:
                    os.kill(pid, sig)
                except ProcessLookupError:
                    pass
        time.sleep(0.1)


def observe(job, stats):
    members = process_members(job["process"].pid, stats)
    rss = 0
    pss = 0
    pss_complete = True
    for pid in members:
        val = stats[pid]
        job["cpu_samples"][(pid, val["start"])] = val["ticks"]
        job["seen_processes"][(pid, val["start"])] = True
        rss += val["rss"]
        value = pss_bytes(pid)
        if value is None:
            pss_complete = False
        else:
            pss += value
    job["peak_tree_rss_bytes"] = max(job["peak_tree_rss_bytes"], rss)
    if pss_complete:
        job["peak_tree_pss_bytes"] = max(job["peak_tree_pss_bytes"] or 0, pss)
    return len(members)


def run_batch(runtime, root, timeframes, mode, round_number, deadline, state):
    batch = {"mode": mode, "round": round_number, "started_at": stamp(),
             "cgroup_before": cgroup_snapshot(), "samples": [], "jobs": []}
    began = time.monotonic()
    jobs = []
    stop_reason = None
    high_memory = 0
    try:
        for tf in timeframes:
            if state["launched"] >= LIMIT_JOBS or time.monotonic() >= deadline:
                stop_reason = "experiment_limit"
                break
            job_id = str(uuid.uuid4())
            directory = root / job_id
            log = (root / (job_id + ".private.log")).open("w")
            command = [sys.executable, str(Path(__file__).resolve()), "--child", "--runtime", str(runtime),
                       "--output", str(directory), "--timeframe", tf, "--job-id", job_id]
            process = subprocess.Popen(command, env=safe_environment(os.environ),
                                       stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            state["launched"] += 1
            job = {"id": job_id, "timeframe": tf, "process": process, "directory": directory,
                   "log": log, "started_mono": time.monotonic(), "launched_at": stamp(),
                   "cpu_samples": {}, "seen_processes": {}, "peak_tree_rss_bytes": 0,
                   "peak_tree_pss_bytes": None, "termination_reason": None}
            jobs.append(job)
            emit("job_started", job_id=job_id, mode=mode, round=round_number, timeframe=tf,
                 launched_at=job["launched_at"], requested_number=state["launched"])
        while any(j["process"].poll() is None for j in jobs):
            now = time.monotonic()
            stats = proc_stats()
            group = cgroup_snapshot()
            counts = {j["id"]: observe(j, stats) for j in jobs}
            batch["samples"].append({"elapsed_seconds": now - began, "cgroup": group, "process_counts": counts})
            limit, used = group.get("memory.max"), group.get("memory.current")
            high_memory = high_memory + 1 if isinstance(limit, int) and isinstance(used, int) and used > limit * MEMORY_STOP_FRACTION else 0
            if high_memory >= 4:
                stop_reason = "memory_above_85_percent_for_2_seconds"
            if now >= deadline:
                stop_reason = "experiment_deadline"
            for job in jobs:
                if job["process"].poll() is None and (stop_reason or now - job["started_mono"] > JOB_SECONDS):
                    job["termination_reason"] = stop_reason or "job_360_second_deadline"
                    clean_group(job["process"])
            time.sleep(SAMPLE_SECONDS)
    finally:
        for job in jobs:
            clean_group(job["process"])
            clean_tracked_children(job)
            job["log"].close()
            valid, error, result = validate_result(job["directory"], job["id"], job["timeframe"])
            measurement = read_json(job["directory"] / "measurement.json")
            failure = read_json(job["directory"] / "error.json")
            remaining = proc_stats()
            survivors = [pid for (pid, start) in job["seen_processes"]
                         if pid in remaining and remaining[pid]["start"] == start and remaining[pid]["state"] != "Z"]
            item = {"id": job["id"], "timeframe": job["timeframe"], "launched_at": job["launched_at"],
                    "returncode": job["process"].returncode,
                    "valid": valid and job["process"].returncode == 0 and not survivors,
                    "validation_error": error, "termination_reason": job["termination_reason"],
                    "measurement": measurement, "failure": failure, "result": result,
                    "sampled_tree_cpu_seconds": sum(job["cpu_samples"].values()) / os.sysconf("SC_CLK_TCK"),
                    "peak_tree_rss_bytes": job["peak_tree_rss_bytes"], "peak_tree_pss_bytes": job["peak_tree_pss_bytes"],
                    "surviving_process_count": len(survivors)}
            if measurement and measurement["forbidden_modules_loaded"]:
                item["valid"] = False
                item["validation_error"] = "unexpected_database_module"
            batch["jobs"].append(item)
            emit("job_finished", job_id=job["id"], timeframe=job["timeframe"], valid=item["valid"],
                 code=(failure or {}).get("code") or item["validation_error"] or job["termination_reason"],
                 elapsed_seconds=(measurement or {}).get("elapsed_seconds"), returncode=item["returncode"],
                 surviving_process_count=len(survivors))
        batch.update(finished_at=stamp(), elapsed_seconds=time.monotonic() - began,
                     cgroup_after=cgroup_snapshot(), stop_reason=stop_reason)
    return batch


def emit_evidence(report):
    raw = json.dumps(report, separators=(",", ":"), allow_nan=False).encode()
    encoded = base64.b64encode(gzip.compress(raw)).decode()
    chunks = [encoded[i:i+6000] for i in range(0, len(encoded), 6000)]
    for index, chunk in enumerate(chunks):
        emit("evidence_chunk", index=index, count=len(chunks), data=chunk)
    emit("evidence_complete", sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw), chunks=len(chunks))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--child", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--timeframe", choices=("12H", "24H"))
    parser.add_argument("--job-id")
    args = parser.parse_args()
    runtime = args.runtime.resolve()
    if args.child:
        return run_child(runtime, args.output, args.timeframe, args.job_id)
    os.umask(0o077)
    # The launcher must explicitly mark this as an isolated one-off process.
    if os.getenv("CG2_ISOLATED_ONE_OFF") != "1" or os.getenv("RENDER_SERVICE_ID") != SERVICE:
        raise RuntimeError("isolated_one_off_guard_missing")
    if os.getenv("RENDER_GIT_COMMIT") != BASE_SHA:
        raise RuntimeError("base_deployment_revision_mismatch")
    env = safe_environment(os.environ)
    os.environ.clear()
    os.environ.update(env)
    required = ("collection_model1_task.py", "model1_execution.py", "heatmap_hover.py",
                "market_vision/coinglass_heatmap_capture.py", "market_vision/openai_heatmap_scanner.py",
                "provenance.json")
    if not all((runtime / name).is_file() for name in required):
        raise RuntimeError("generated_runtime_missing")
    if not env.get("OPENAI_API_KEY") or not any(env.get(k) for k in SESSION_KEYS):
        raise RuntimeError("existing_source_or_openai_configuration_missing")
    if env.get("OPENAI_MARKET_SCANNER_MODEL") != "gpt-5.4-mini":
        raise RuntimeError("analysis_model_differs_from_baseline")
    group = cgroup_snapshot()
    report = {"base_sha": BASE_SHA, "started_at": stamp(), "runtime_hashes": {n: digest(runtime/n) for n in required},
              "provenance": read_json(runtime/"provenance.json"), "hardware": group,
              "sample_seconds": SAMPLE_SECONDS, "job_timeout_seconds": JOB_SECONDS,
              "total_timeout_seconds": EXPERIMENT_SECONDS, "memory_stop_fraction": MEMORY_STOP_FRACTION,
              "source_calls_limit": LIMIT_JOBS, "ai_calls_limit": LIMIT_JOBS,
              "database_writes": 0, "app_worker_started": False,
              "metrics_note": "Tree CPU is sampled and can miss short-lived processes. RSS double-counts shared pages; PSS is proportional. Cgroup CPU/memory covers the entire isolated job. Operation durations can nest. PNGs are hashed, not exported.",
              "batches": []}
    emit("preflight", **{k: report[k] for k in ("base_sha", "hardware", "sample_seconds", "memory_stop_fraction", "source_calls_limit", "ai_calls_limit")},
         run=args.run, ai_model=env["OPENAI_MARKET_SCANNER_MODEL"], database_credentials_passed=False)
    if not args.run:
        return 0
    if int(Path("/proc/self/stat").read_text().split(" ", 1)[0]) != os.getpid():
        raise RuntimeError("process_namespace_not_suitable_for_proc_measurement")
    if not group.get("available") or not isinstance(group.get("memory.max"), int):
        raise RuntimeError("finite_cgroup_resource_limits_required")
    quota = str(group.get("cpu.max", "")).split()
    if len(quota) != 2 or not all(v.isdigit() for v in quota):
        raise RuntimeError("finite_cpu_quota_required")
    cpu_cores = int(quota[0]) / int(quota[1])
    if not (1.95 <= cpu_cores <= 2.05 and 3.8 * 1024**3 <= group["memory.max"] <= 4.2 * 1024**3):
        raise RuntimeError("requested_2cpu_4gib_environment_not_verified")
    deadline = time.monotonic() + EXPERIMENT_SECONDS
    state = {"launched": 0}
    with tempfile.TemporaryDirectory(prefix="cg2-private-") as temporary:
        root = Path(temporary)
        stop = False
        try:
            for cycle in range(1, 4):
                modes = ("serial", "parallel") if cycle % 2 else ("parallel", "serial")
                timeframes = ("12H", "24H") if cycle % 2 else ("24H", "12H")
                for mode in modes:
                    groups = [(t,) for t in timeframes] if mode == "serial" else [timeframes]
                    for tfs in groups:
                        batch = run_batch(runtime, root, tfs, mode, cycle, deadline, state)
                        report["batches"].append(batch)
                        if batch["stop_reason"] or any(not j["valid"] for j in batch["jobs"]):
                            stop = True
                            break
                    if stop:
                        break
                if stop:
                    break
        finally:
            report.update(finished_at=stamp(), jobs_launched=state["launched"], stopped_early=stop,
                          cgroup_after=cgroup_snapshot())
            emit_evidence(report)
    return int(stop)


if __name__ == "__main__":
    raise SystemExit(main())
