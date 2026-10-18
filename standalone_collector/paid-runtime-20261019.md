# Paid runtime acceptance — 2026-10-19 Israel / 2026-10-18 UTC

The user upgraded the existing decision-hub-model1-collector. Verified service srv-dakhacou01pc73f58aug in workspace tea-d94cq7e7r5hc73dee88g now runs 0.5c-512mb. Actual cgroup cpu.max=50000 100000, memory.max=536870912. User-triggered upgrade deploy dep-dbajsp67bikc73etv8a0 is live at unchanged production commit cf2413f6a34ff9cac33e27e957990eeab2c3c0f7. Auto-deploy OFF.

Exactly one guarded temporary runtime test was executed through the paid service's Shell. It did not deploy or replace production files. A separately staged generated runtime from PR175 b62bce2ed6aa32282efcd9084cee1c7685897ab1 was used; all 70 relevant source files matched that GitHub tree. The bundle SHA256 was 658329c4491e099633458851f1c04a76850353f19adb348aedd8b254e87c961c.

The temporary runner acquired the existing global collection-worker advisory lock only while no queued/running work existed. It checked for arriving work once per second, bounded each case at 180 seconds, and stopped at 430 MiB anonymous+kernel+socket memory to preserve headroom for the running service. Source children received only source-session and OS settings; no OpenAI, database or collector-token credentials. No schema, job or application-data writes occurred. Source credentials remained inside their existing Render service.

Model 2/12H was stopped after 28.219 seconds by memory_headroom_guard before completing capture. Peak cgroup current memory reached 512 MiB. Peak anonymous memory was 403.88 MiB, kernel 31.19 MiB, socket 2.14 MiB; individual peaks need not be simultaneous. File cache peaked at 126.32 MiB. memory.events max rose by 1441; OOM and OOM-kill counters stayed zero. This proves memory-limit pressure and a protective stop, not an OOM crash, an exact required memory size, or a new reader validation defect. Metrics include the existing server and temporary diagnostic parent; they are not standalone Chrome heap usage.

Model 3 was deliberately NOT started after the first resource stop. There were ZERO AI requests, ZERO production saves, ZERO deploys initiated by the agent, and no retries. The existing service health and store readiness were true after the run. Guard PID was absent, no Chrome/Chromium/Playwright process remained, and the worker lock was released. Only a source log existed in the private output directory; no new map/result was produced.

Release gate remains OPEN/FAILED on the current 512 MiB plan. Recommend confirming a 1c-2g compute plan for the SAME service, then running only the remaining runtime cases before any coordinated collector/app release and live hover-save verification. Do not repeat historical source/GPT tests. No additional plan change was made.

Collector PR175 and app PR81 remain drafts at b62bce2ed6aa32282efcd9084cee1c7685897ab1 and ebbf1e1efc1c375903fe3dcb4084de8fe1e1fe31. Their existing exact-head CI remains successful. The existing app hover migration remains unapplied; do not create a duplicate migration.

Measured result:
```json
{
  "model": 2,
  "timeframe": "12H",
  "status": "failed",
  "kind": "PAID_RUNTIME_RESULT",
  "exit_code": -15,
  "guard_reason": "memory_headroom_guard",
  "total_seconds": 28.219,
  "peak_mib": {
    "anon": 403.88,
    "file": 126.32,
    "kernel": 31.19,
    "sock": 2.14,
    "current": 512
  },
  "cpu_delta": {
    "usage_usec": 11994056,
    "user_usec": 8394803,
    "system_usec": 3599252,
    "nice_usec": 0,
    "core_sched.force_idle_usec": 0,
    "nr_periods": 283,
    "nr_throttled": 208,
    "throttled_usec": 43469771,
    "nr_bursts": 0,
    "burst_usec": 0
  },
  "memory_events_delta": {
    "low": 0,
    "high": 0,
    "max": 1441,
    "oom": 0,
    "oom_kill": 0,
    "oom_group_kill": 0,
    "sock_throttled": 3
  },
  "ai_calls": 0,
  "data_writes": 0
}
```
