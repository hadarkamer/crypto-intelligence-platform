# Isolated CoinGlass concurrency experiment

Status: prepared; no live source or AI calls have been performed by this experiment.

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

## Access blocker

The installed Render connector lacks one-off job creation. Render CLI v2.34.0
was installed locally, and browser login succeeded. Automatic approval review
rejected the `Authorize CLI` click because it would create a token with access
to Render resources, valid up to seven days, without explicit authorization for
that access grant. No token was authorized and no live job was started.
Obtain explicit approval for that grant before retrying; revoke it when the
experiment is finished. Do not work around the rejection.
