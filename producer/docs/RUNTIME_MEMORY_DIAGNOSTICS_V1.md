# Runtime memory diagnosis

After PR151 was deployed, Render reported another 2Gi OOM event on
2026-10-09 at 18:08:25 UTC. Application logs already show a process restart
at 18:06:54 UTC; event delivery time is not the exact kill time. The two
completed WATCH sources before that restart remained stored. The earlier
page ownership correction remains valid, but did not establish service
memory stability.

This change adds aggregate observations to locate the remaining consumption.
It does not truncate history, clear caches, change formula definitions,
change scheduling or enable any worker or trading path.

## Samples

Each `[MEMORY_DIAGNOSTIC]` JSON line identifies a process session with its
PID, process start ticks and a generated boot UUID. Render instance names
can be reused across process restarts.

- Current Python process RSS and Linux cgroup v2 current/max memory,
  anonymous/file memory and OOM event counters, where readable.
- Process counts and RSS by Python, Node, Chromium and other categories.
  Process command arguments, environment, paths and original names are
  never emitted. RSS totals can double-count shared pages and must not be
  equated with container memory.
- Open page count at page setup/close and browser lifecycle boundaries.
- Aggregate OI reference cache entry, history-row, window and sample counts.
  Counts use array lengths; they do not traverse or log market values or
  measure retained bytes. A partial sample is marked explicitly.

The existing Watch supervisor emits at most one periodic sample per minute.
Additional samples bracket collection, browser/page ownership and Watch
processing. `scoring_start` / `scoring_end` bracket derivatives readiness,
snapshot capture and core score preparation, before bundle construction and
Combined evaluation. `archive_start` / `archive_end` bracket the entire
live-row collection function, including passive collection and enrichment.
`playwright_exit` records leaving the Playwright scope even on a failed or
cancelled entry/teardown; it is not proof of successful driver termination.
End samples can still include local variables retained by their caller;
compare subsequent idle samples before concluding retention.

Sampling is synchronous and best-effort, with fixed file, byte and process
entry caps and a 50ms budget checked between filesystem operations. This
is not a hard deadline for blocked kernel reads or stdout. Missing Linux
interfaces, permission failures, malformed data or exhausted limits produce
partial measurements; never interpret missing data as zero memory. No new
dependency, background task, network request, heap dump or tracing session
is introduced.

## Interpretation and next action

Compare samples within the same boot UUID. A rising main-process RSS with
no browser children differs from browser peaks or growing file memory.
Correlate cache counts and processing phases; a simultaneous change alone
does not prove causation. The per-process view is a bounded, non-atomic
snapshot and cannot see processes outside its namespace.

Only after attribution should a correction change allocation or retention.
Preserve the frozen research requests and source availability timestamps.
A later healthy restart does not repair historical missing observations or
prove multi-cycle stability. These diagnostics are not formula outcomes.
