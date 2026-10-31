# Isolated CoinGlass concurrency experiment

Status: cookie reproduction and a second comparison completed on 2026-10-31.
The unchanged cookie function succeeded. The second comparison ran six scans
and six AI calls, including two truly parallel pairs, then stopped on a 24H
identity mismatch. Five results were accepted. No production fix was made.

User-approved scope: BTC Model 3, 12H and 24H; sequential versus simultaneous
execution, three repetitions, at most 12 fresh maps and 12 existing-model AI
calls. Use the existing OpenAI key. This is not acceptance of eight coins,
72 simultaneous jobs, application storage/UI, or scheduled operation.

Base collector deployment: `f6adfc6fea05bfd82a59098433ad1c2f31f6267f`.
Production service: `srv-dakhacou01pc73f58aug`.
No changes to the production branch, service, database, queue, hourly limit,
shared scheduler, reader, validation thresholds, or application are required.

## Execution environment

Planned execution is a finite Render one-off job derived from the existing
collector build, using `plan-srv-010` (2 CPU / 4 GB according to Render's
one-off-job documentation). It is separate compute; the service start command
is never invoked. Before a live run, verify the deployed SHA, configured
provider/model, one-off job identity, actual cgroup CPU/memory limits and cost.
The old static-site build lab remains unchanged.

The task rejects any base revision other than the pinned deployed commit,
requires the existing `gpt-5.4-mini` configuration, strips database and unrelated
credentials before imports, and starts the generated collection task directly.
Each child has its own session/process group, UUID and private output folder.
No public web endpoint or durable production storage is created.

The job is explicitly marked `CG2_ISOLATED_ONE_OFF=1` by its launcher. This
marker does not by itself prove isolation: the operator must create an actual
Render one-off job, not execute the script in a running production shell.

## Measurement and stopping

- Record actual child start/end, original stage changes and capture operations.
- Sample process-tree RSS/PSS/CPU and whole-job cgroups every 0.5 seconds.
- Preserve per-job identity, normalized numeric evidence, result/image hashes,
  failure detail, original validation outcomes, AI model and token usage.
- Tree CPU sampled from `/proc` can miss very short-lived children. RSS can
  double-count shared pages; PSS apportions them. Cgroup counters include the
  measurement process and all children. These are distinct measures.
- Durations of nested capture operations must not be added together.
- Stop starting new work after a failed batch; keep successful sibling results.
- Keep the production Model 3 deadline of 360 seconds per job. The whole
  experiment is limited to 40 minutes and 12 jobs without automatic retries.
- Terminate at more than 85% of the measured memory limit over four samples
  (about two seconds). The planning acceptance target is at least 30% headroom;
  the emergency stop is not a relaxed acceptance threshold.
- Terminate only tracked experimental process groups/children, then verify that
  no live tracked children remain. The one-off instance ends with its command.

Order alternates serial/parallel between repetitions to reduce simple order
bias. Map contents can change between comparisons; no claim of identical live
pixels is made. The reader's complete-row and source checks remain enabled.

Evidence is emitted to private job logs as gzip/base64 chunks with a SHA-256
checksum. Reassemble it into the experiment report before closing the task.
Original screenshots are hashed but not exported by this harness; this is an
explicit evidence limitation, not an assertion of manual visual review.

## Offline verification

`python -m unittest discover -s experiments -p 'test_coinglass_concurrency_20261031.py' -v`

Tests use synthetic tasks, credentials and resource readings. They verify
credential isolation, identity/digest checks, bounded launches, trace handling,
process cancellation, evidence reconstruction and the entire 12-task protocol.
Synthetic timings and resource values are never capacity measurements.
The local executor virtualizes PIDs independently of `/proc`; production runs
fail closed unless `/proc/self/stat` agrees with `os.getpid()`.

## First live attempt (historical)

The user explicitly authorized temporary Render CLI access after the approval
review block. One-off job `job-dbj0sn0ae00c73ctlvfg` ran on `plan-srv-010` from
15:50:52 to 15:53:13 UTC. The harness verified a 2-CPU quota and 4 GiB memory.

- Model 3 / 12H succeeded in 54.121 seconds, with 43 complete source rows and
  4 normalized zones; AI analysis took 3.893 seconds (7,741 tokens).
- Model 3 / 24H failed in 51.412 seconds at `overlay_dismissal`, with
  `source_capture_failed` / `hover_cookie_overlay`, before screenshot, hover
  collection or AI analysis. The current diagnostic does not distinguish the
  exact cookie-button/banner state, so no reader change was made by this test.
- No parallel jobs ran. No concurrency speedup, p95 or eight-coin capacity can
  be inferred. Do not automatically retry or relax the source validation.
- Cgroup CPU use averaged 85.94% and 96.02% of the two available cores over
  the respective complete batches. Throttling occurred in 410/523 and 476/518
  CPU periods. Peak kernel-accounted memory for the whole experiment was
  1,023,275,008 bytes (0.953 GiB). These are not per-process RSS measurements.
- Cleanup reported zero tracked surviving processes. Render confirmed the job
  ended. Production plan, instance count and deployed SHA were unchanged.
- The CLI token was revoked immediately after the attempt; the account page
  showed `No provisioned CLI tokens.`

Evidence reconstructed from four private log chunks: 116,939 bytes, SHA-256
`3db3d4e4ef969e0a73f9f6db30ad1d0ba6758096bced696b7324ffafee3874d4`.
The raw evidence is kept privately with the experiment report, not committed
to this repository. The production reader was not changed after the failure.
Subsequent diagnostic changes and results are recorded below.

## Cookie reproduction and second comparison

A source-only BTC / Model 3 / 24H probe ran in one-off job
`job-dbj13qvf3r2c73elukeg` from 16:06:03 to 16:07:36 UTC, with no AI or
database credentials. Before/after observations and screenshots showed the
original cookie function removing the banner successfully in 3.698 seconds.
The complete capture took 62.717 seconds and returned 90 complete rows.
About 6.1 seconds of observation overhead makes this unsuitable as a speed
baseline; observation can affect timing, so this does not establish a fix.

The comparison harness then gained a failure-only cookie observer. Successful
captures execute no extra UI observations. No reader, timeout, acceptance check,
or production setting changed. Probe commit: `562b6c3c41989e80ea55ae12756f0698f828dcd3`;
comparison commit: `37c6c32f309afbd218db688588aa08bb94cf9fa1`.

One-off job `job-dbj15tc9v7es738re50g` ran from 16:10:37 to 16:17:33 UTC
on verified 2 CPU / 4 GiB limits. Its failed status reflects the required
stop after an invalid result, not a failure to launch the experiment.

| Batch | 12H seconds | 24H seconds | Total batch seconds | Accepted |
|---|---:|---:|---:|---:|
| Round 1 serial | 69.361 | 61.550 | 132.300 | 2/2 |
| Round 1 parallel | 118.727 | 118.157 | 119.404 | 2/2 |
| Round 2 parallel | 126.382 | 125.095 (rejected) | 127.194 | 1/2 |

The total includes process launch, sampling and cleanup. One complete paired
comparison saved 12.896 seconds (9.748%, 1.108x throughput). Child start gaps
were 2.519 and 0.211 milliseconds, and execution overlapped. Each job retained
its own process, directory and UUID. Round 2 serial and round 3 did not run.
The six AI calls used 45,572 tokens, including the rejected result.

The rejected 24H job `d5889a67-58d5-47be-a576-ad5dc2ebd845` completed source
selection, readiness, screenshot, post-capture readiness, hover and AI analysis.
AI reported `observed_timeframe=12h`; validation rejected it with
`screenshot_identity_mismatch`. Its successful 12H sibling was retained.
No cookie failure, deadline failure, OOM or resource stop occurred. The exact
submitted PNG and full AI response were not exported, so neither crossed maps
nor AI misreading is established. The next isolated diagnostic should retain
that exact PNG, before/after source identity and the AI response on mismatch.
Do not weaken validation or relabel the failed result.

Serial CPU averages were 90.082% and 89.195% of the two-core capacity; parallel
averages were 95.977% and 95.720%. Capture time grew from 63.893/55.747 seconds
serially to 113.103/113.188 seconds in the first parallel pair. AI analysis in
that pair took 3.946/3.279 seconds. These observations support CPU contention
as a performance limit, but do not prove the cause of the identity failure.
Whole-comparison kernel memory peaked at 1.496506 GiB, leaving 62.587% headroom.

This is one complete timing comparison and five accepted results out of six,
not a reliability estimate, p95 or proof of eight-coin/72-job capacity. After
identity diagnosis, compare the same pair on isolated 4-CPU compute to test the
CPU bottleneck. No such larger run or infrastructure change has been performed.

Twelve distinct offline tests passed: the combined eight harness and three
probe tests, then the added failure-observer regression test. Cleanup found
zero tracked survivors. Both one-off jobs ended. Production SHA, plan and
instance count remained unchanged, and its health endpoint was healthy.
The temporary CLI token was revoked; the account showed
`No provisioned CLI tokens.` Local temporary CLI configuration was removed.

Private evidence (not committed):

- `chat2_cookie_probe_evidence_20261031.json`: 431,375 bytes; SHA-256
  `1e558ceb16a1075a5d005b6abdb00fb67a32ce867fb0f51a9d84874f0371f96f`.
- `chat2_concurrency_comparison2_evidence_20261031.json`: 339,767 bytes; SHA-256
  `afded152278e844a028f45186f86f8c075b90c59bad7df46b9218060677e775b`.

