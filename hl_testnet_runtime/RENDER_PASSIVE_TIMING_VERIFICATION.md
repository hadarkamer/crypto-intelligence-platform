# Passive timing — isolated Render verification

Date: 2026-10-06. **Final current-candidate Render result: PASSED.**
The isolated Render build-container run completed at **12:15:07 UTC** with
**2,074 full-suite tests, zero failures, zero errors, zero skips, and zero
unexpected successes**. All benchmark stages passed, and the disposable
PostgreSQL instance stopped successfully. The two earlier static attempts remain
failed attempts; the free web-runtime run remains incomplete.

The results support proceeding to a separately approved, monitored **Testnet**
deployment of this candidate. They do not establish zero risk or live execution
latency. Current observed web-service capacity does not justify an upgrade for
this addition; the shared database has less CPU headroom. No bot deployment,
live environment change, Mainnet action, or paid upgrade was performed.

## Scope and authorization

The approved action is isolated Render testing of the passive timing candidate.
Two **free, isolated test services** are used: a web service with an inert HTTP
status server, and a static service whose build runs the same offline harness.
Both use synthetic exchange responses and a fresh local test database. Neither
starts the bot. The static service publishes only a bounded `summary.json` and
`index.html` after the harness reports `PASSED`.
No paid upgrade, live-service deployment, live environment change, or live trading
operation is part of this verification.

The user's standing rule remains: every proposed bot change must first be tested
thoroughly in Codex, and activation in the bot requires the user's explicit
approval for that tested change. Approval for these isolated tests is not
approval to deploy the candidate to the bot.

## Candidate and infrastructure pins

| Item | Exact identity |
| --- | --- |
| Code baseline | `3084c5c7cd53b70d0569086ba78a78f8fc53a9ac` |
| Current candidate commit | `a8617621530cfc3e5ec0b4c2fec62c9e4b03e80f` |
| Current candidate tree | `3ef1d36c22b4d505062de072b4e522b7503888f6` |
| Candidate branch | `codex/passive-timing-render-check-20261006` |
| Current input manifest SHA-256 | `355f9906733f52b6e243a72207694e525e636494437a7fe5ab72e5dd59b53f08` |
| Manifest scope | 446 Python and requirements files |
| Render workspace | `tea-d94cq7e7r5hc73dee88g` |
| Free web test service | `srv-db2dhnmk1f9s73a2m4k0` |
| Web test deployment, earlier candidate | `dep-db2dmnui0phs73eackj0` |
| Candidate still deployed to web test service | `333651bf78eb074e50d250b9f6a3705274878c87` |
| Web test manifest SHA-256 | `9fe525cbb2dc74c9678507250acc75d9b8289f3a11d0f4947a4eb36473cd4bc4` |
| Static verification service | `srv-db2dv53ncjis73ec9lng` |
| Final static deployment, completed 12:15:10 UTC | `dep-db2e9ljncjis73edeio0` |
| Live bot service, unchanged | `srv-dakptbh594qs7395460g` |
| Existing shared database, unchanged | `dpg-dab7rc2d0e5s73dkb9l0-a` |

Both test services have automatic deployment disabled and use the same candidate
branch. Their build dependency is
unmodified PostgreSQL 18.6, downloaded from
`https://ftp.postgresql.org/pub/source/v18.6/postgresql-18.6.tar.bz2`, with source
SHA-256 `555610c24d53e4316da5b7d3fc25c279d96856d5e0e23ee308c328c5fa881d9f`.
The build verifies the archive before extraction. Cached builds check the source
marker and the required executables. The test runner initializes a new database
only as a non-root user; it does not bypass PostgreSQL's root-startup protection.

The initial test deployment (`dep-db2dhnuk1f9s73a2m56g`, commit
`7fbe25a56765f2a270a8443f991aacb7d1773088`) stopped during PostgreSQL
configuration because Render lacked `flex`. No runtime test ran in that attempt.
The test-only builder was extended with official flex 2.6.4 source, verifying
SHA-256 `e87aae032bf07c26f85ac0ed3250998c37621d95f8bd748b31f15b33c45ee995`
and the maintainer-published SHA-1
`fafece095a0d9890ebd618adb1f242d8908076e1` before extraction. It builds only
the scanner executable; the candidate bot code did not change. The environment
pin update automatically triggered the second test deployment. That attempt
compiled flex successfully but stopped in PostgreSQL configuration because
`--without-ssl` is not accepted by PostgreSQL 18. The unsupported flag was
removed; SSL is an opt-in PostgreSQL build feature. These were test-environment
setup failures, before any runtime test executed. Both attempts are retained
as failed attempts, not passes.

## Completed local evidence

After the latest test-guard correction described below, the local harness and
build-dependency suites passed **38 tests under the installed guard**. They cover manifest integrity,
environment isolation, local database restrictions, network/process guards,
single-start coordination, timeout/output limits, cleanup failure handling, and
the inert HTTP interface.

The earlier local harness smoke report at
[render_preflight_local_20261006.json](render_preflight_local_20261006.json) has status
**PARTIAL**, because PostgreSQL was explicitly disabled for this harness smoke
run. Its manifest is the earlier `9fe525...cd4bc4` pin, not the current
`355f99...b53f08` pin. Its results must not be presented as a complete durable or
Render run, or as a new smoke run of the current candidate.

| Local harness stage | Result |
| --- | --- |
| Timing component and hook tests | 28 passed; no failures, errors, skips, or unexpected successes |
| Recorder benchmark | 14 conditions; 27,648 measured calls; zero blocked network attempts |
| Feed benchmark | 120 exact feed-state parity pairs; zero blocked network attempts |
| Durable runtime benchmark in this smoke run | Not run: PostgreSQL explicitly disabled |
| Complete runtime suite in this smoke run | Not run |

In that local smoke run, average added recorder-call time ranged from **8.0 to
21.6 microseconds**, depending on payload and producer count. Added time for a
single feed update averaged **30.4 to 34.0 microseconds** across the reported
cases. These figures describe the local measured calls, not live exchange or
Render latency.

Earlier completed Codex trading comparisons and their limitations are recorded
in [PASSIVE_TIMING_VERIFICATION.md](PASSIVE_TIMING_VERIFICATION.md), with the
saved local benchmark data. The harness smoke result does not replace those
results or count their repeated cases as new unique tests.

## Isolation and interpretation boundaries

- Children receive an explicit environment allowlist. Live service settings,
  credentials, and database URLs are not copied. Preconfigured database URL
  settings are rejected by the runner.
- The disposable database uses a generated password, a loopback listener, and
  the dedicated `hl_journal_ci` database. Native psycopg calls are checked against
  the generated connection parameters. Existing databases are not test inputs.
- A Python audit guard blocks non-loopback socket operations. Allowed Python
  descendant scripts receive the same guard and a sanitized environment; their
  selected database URL must match the fresh local URL. **These are application
  safeguards, not an OS network sandbox.** Pinned code and the existing exchange
  doubles remain part of the isolation boundary.
- Harness stages run serially; fixtures recreate schemas only in their dedicated
  CI database. The suite still exercises its explicit concurrency scenarios.
  There is no shared production database or concurrent live trading worker.
- Subprocess time and output are bounded. PostgreSQL cleanup errors fail the
  result. Missing PostgreSQL yields `PARTIAL`; the command-line check returns a
  distinct nonzero status for that incomplete result.
- Public endpoints expose bounded status and selected numeric results. They
  offer no run controls, raw logs, filesystem access, or trading operations.
- The Render runtime benchmark uses four measured rounds. The free runtime and
  the Render build container are different execution environments. Build-container
  results establish tested functional compatibility; neither environment and
  its temporary local PostgreSQL database reproduce the live service's CPU
  allocation, shared-database latency, exchange traffic, or capacity.
- All exchange prices, fills, orders, and exchange timestamps are synthetic.
  Real elapsed clocks measure local processing. A disabled-recording comparison
  uses the candidate with telemetry off, not pristine pre-change code.

## Existing live capacity: read-only observations

Source: [render_capacity_20261006.json](render_capacity_20261006.json).
The recorded window is **2026-10-05 11:00 UTC through 2026-10-06 11:03 UTC**
(14:00 through 14:03 Israel time on the corresponding dates).

The live web service uses plan `1c-2g`: one CPU and 2 GiB memory, with one instance.
The database uses plan `0.1c-256mb`: 0.1 CPU, 256 MiB memory, a 1 GB provisioned
disk, and PostgreSQL 18.

| Metric | Sample mean | Sample maximum |
| --- | --- | --- |
| Web-service CPU / allocated CPU | 17.6% | 25.7% |
| Web-service memory | 103.3 MiB / 5.0% | 122.1 MiB / 6.0% |
| Database CPU / allocated CPU | 69.4% | 81.6% |
| Database memory | 73.5 MiB / 28.7% | 89.0 MiB / 34.8% |
| Database active connections | 1.57 | 4 |

There is **no evidence in these samples that the web service needs an upgrade
for this telemetry addition**. The local implementation adds no database or
exchange requests in the verified paths. This conclusion is limited to the
observed workload and candidate; it is not a guarantee of future capacity.

The database has less CPU headroom: approximately 69% average and 82% at the
largest returned sample. That makes it the more constrained resource in this
snapshot, but does not by itself establish throttling or a need to upgrade for
passive timing. A general capacity decision would require evidence of workload
growth or database latency and contention.

The service CPU request used MAX aggregation in 300-second buckets; its sample mean is the mean of those returned bucket maxima, not a direct mean of instantaneous CPU usage. Shorter spikes are not bounded
by these samples. The database AVG and MAX queries returned the same values, so
their apparent agreement is not independent peak evidence. Sample counts differ
slightly between series.

Direct SQL inspection was blocked by the database's empty external IP allowlist.
The allowlist was left unchanged. **Actual disk usage is unknown**; the 1 GB
figure is provisioned capacity, not measured free space. No paid upgrade or
configuration change was made from this capacity review.

## Free web-service run — incomplete

This run used commit `333651bf78eb074e50d250b9f6a3705274878c87` and manifest
`9fe525cbb2dc74c9678507250acc75d9b8289f3a11d0f4947a4eb36473cd4bc4`.
Its build completed at **11:31:58 UTC**. By **11:45:43 UTC**, the runtime benchmark
had completed the following stages:

| Check | Current result |
| --- | --- |
| Build and pinned PostgreSQL dependency | Passed in third test deployment; unmodified pinned PostgreSQL 18.6 |
| Timing component and hook tests | 28 passed in Render; zero failures/errors/skips |
| Recorder benchmark | 27,648 calls across 14 conditions; zero blocked network attempts |
| Feed parity benchmark | 120 exact state parity pairs; zero blocked network attempts |
| Fresh local PostgreSQL startup | Completed for the durable benchmark |
| Durable trading benchmark | 80 measured scenarios plus 20 warmups; 80 trace-equality comparisons passed |
| Complete runtime / adapter suite | INCOMPLETE: no final report observed |
| PostgreSQL cleanup | Unverified for this run |
| Overall free-runtime result | INCOMPLETE |

The mean local runtime subtotal was **6,341.330 ms with recording disabled** and
**6,597.223 ms enabled**, a difference of **4.035% of the disabled mean**.
Mean simulated fill-to-verified-projection duration was **1,366.124 ms disabled**
and **1,436.946 ms enabled**. These are measured samples from a constrained free
instance, not production response-time estimates or an isolated causal estimate
of telemetry cost.

The last observed metrics were at approximately **11:47 UTC**, about 15 minutes
after startup. Free-service idling is a possible explanation for the missing
completion, **not an established cause**. The absent full-suite result cannot
be converted into a pass or attributed conclusively to idling.

## First static build verification — failed, guard issue reproduced

Service `srv-db2dv53ncjis73ec9lng`, deployment `dep-db2dv5bncjis73ec9mr0`, ran
the earlier `333651bf...` candidate in a **Render build container**. The saved
machine record is
[render_build_attempt1_20261006.json](render_build_attempt1_20261006.json).
Its final result, observed at **11:52:33 UTC**, was **FAILED**.

The timing tests, recorder benchmark, feed benchmark, and durable benchmark
completed before the full-suite failure. The durable benchmark again ran
**80 measured scenarios plus 20 warmups**, with **80 trace-equality comparisons**
passing. Every scenario retained eight simulated order requests and 174
simulated reader calls. The fresh local PostgreSQL instance was stopped.

| Build-container metric | Recording disabled | Recording enabled |
| --- | --- | --- |
| Mean local runtime subtotal | 511.980 ms | 514.497 ms |
| Mean simulated fill-to-verified-projection duration | 112.791 ms | 112.354 ms |

The runtime-mean difference was **+0.492% of the disabled mean**. The slightly
lower enabled fill-to-projection mean is not evidence of a speedup. The small
sample and shared build environment do not establish zero overhead, production
latency, or production capacity.

The full suite ran **2,063 tests: zero failures, two errors, zero skips**. The
two recorded errors were:

- An `ImportError` loading `hl_testnet_runtime.test_app_card_delivery`.
- `PREFLIGHT_SUBPROCESS_REFUSED` in the SDK wire-order/hash compatibility test.

The initial local reproduction identified the guard refusing the exact metadata
command `/sbin/ldconfig -p` during native-library discovery. That narrow command
was allowed, and 36 local harness/build tests passed. The subsequent Render run
showed that this was only a partial correction: an additional optional discovery
fallback still encountered an incompatible denial exception. No bot behavior
was changed. The failed attempt is retained; its result is not retrospectively
converted into a pass.

## Second static attempt — retained failure

Deployment `dep-db2e3uflk1mc73b3ba30`, started at **11:56:41 UTC**, used candidate
`905bd2e6a36ae6edf24e84c7cd37c970cc73bb71` and manifest
`ec45e804a99001f74fa833470f9b882883320d623ab68bddd54bc5382b98c969`.
It completed at **12:02:50 UTC** with **2,065 tests, one failure, two errors,
and zero skips**. The crypto-import and SDK-hash errors remained; the new targeted
guard regression also failed. All four benchmark/component stages passed again,
and the temporary PostgreSQL stopped successfully. This overall result is
**FAILED**. See
[render_build_attempt2_20261006.json](render_build_attempt2_20261006.json).

The follow-up local reproduction showed that the guard raised `RuntimeError`
when denying an optional native-library discovery subprocess. The discovery
code expects an `OSError` when an attempted execution is unavailable or denied,
so that it can continue through its fallback path. The current guard raises
`PermissionError`, an `OSError` subclass, while continuing to block the same
disallowed executions. This preserves normal failure handling without allowing
additional commands. It does not establish that a particular native library was
missing. The correction passed 38 local tests under the installed guard; the
final Render rerun subsequently passed, as recorded below. No bot code or live service
was changed by this harness correction.

## Final static build verification — passed

Deployment `dep-db2e9ljncjis73edeio0` started at **12:08:54 UTC**, using
commit `a8617621530cfc3e5ec0b4c2fec62c9e4b03e80f` and manifest
`355f9906733f52b6e243a72207694e525e636494437a7fe5ab72e5dd59b53f08`.
The harness reported **PASSED at 12:15:07 UTC** after 180.647 seconds of checks,
and Render reported the static deployment live at **12:15:10 UTC**. The
static build command exports its bounded summary only after a passed result.

Machine evidence:
[render_build_verification_20261006.json](render_build_verification_20261006.json).
The SHA-256 of that saved evidence file is `b450afbfc568bdcfa8086770cae339103c1b51a4f58d63573c2017feb8b5182c`.
This hashes the connector-derived evidence file, not independently downloaded
bytes of the public endpoint.

| Check | Final result |
| --- | --- |
| Candidate / manifest validation | Passed; exact 446-file pin matched |
| Build dependencies | Passed; verified unmodified PostgreSQL 18.6 |
| Timing component and hook tests | 28 passed; zero failures/errors/skips |
| Recorder benchmark | 27,648 measured calls across 14 conditions |
| Feed parity benchmark | 120 exact feed-state parity pairs passed |
| Fresh local PostgreSQL | Started for durable checks and successfully stopped afterward |
| Durable trading benchmark | 80 measured scenarios plus 20 warmups; four measured rounds |
| Trading trace equality | 80 checks passed; eight simulated order requests and 174 reader calls per scenario |
| Complete runtime / adapter / harness suite | 2,074 passed; zero failures/errors/skips/unexpected successes |
| Final result | PASSED |

The separate 28-test component stage repeats tests included in the full suite;
it must not be added to 2,074 as a unique-test count.

| Final build-container metric | Recording disabled | Recording enabled |
| --- | --- | --- |
| Mean local runtime subtotal | 518.401 ms | 516.701 ms |
| Mean simulated fill-to-verified-projection duration | 113.373 ms | 113.131 ms |

The runtime-mean difference is **−1.700 ms (−0.328% of the disabled mean)**.
This is a noisy sampled difference, not evidence that telemetry accelerates
execution. Added recorder-call time averaged **11.365–27.034 microseconds**,
depending on payload and producer count. Added single-feed-update time averaged
**32.258–33.017 microseconds**. The recorder and feed stages reported zero blocked
network attempts.

Across completed cloud benchmark runs, the runtime-mean difference varied:
+4.035% on the constrained free web instance and +0.492%, −1.899%, and −0.328%
in the three build-container runs. These environments and candidate harness
versions differ; do not pool them into a production estimate. The original Codex
12-round runtime comparison was +0.339% with a block-bootstrap interval crossing
zero. Together these measurements demonstrate nonzero per-call recording work
and stable tested trading behavior, without establishing zero overall overhead
or a live-service latency bound.

## Readiness decision and unchanged live state

No unresolved candidate behavior failure was found in the completed local and
final isolated Render checks. The earlier Render failures were in the test
builder/guard and were corrected and rerun. The full-suite pass is from a
**Render build container**, not a completed full-suite run on the free runtime
instance or on the live bot. Real exchange delays, current shared-database disk
usage and contention, and future traffic are not established by this run.

A separately approved rollout may proceed to the existing **Testnet bot only**,
with telemetry initially off until its explicit activation step. Preserve the
strategy, exchange request budgets, 180-second full audit interval, and exact
service/runtime gating. Compare health, CPU/RAM, database load, and execution
behavior after activation; removing/disabling `HL_TESTNET_TIMING_TELEMETRY`
and restarting/redeploying the service returns the recorder to its default off
state, because the setting is read at startup. Detailed telemetry remains bounded in memory
and is lost on restart or archive eviction.

There is no measured reason to upgrade the current `1c-2g` web-service plan for
this addition. The database's sampled CPU load deserves monitoring, but these
results do not establish that it needs an immediate upgrade for passive timing.
Actual database disk usage remains unverified. Any paid upgrade requires
separate approval.

A final read-only live-deployment check still returned
`dep-db2ccjuk1f9s739v50g0`, baseline commit
`3084c5c7cd53b70d0569086ba78a78f8fc53a9ac`, completed at 09:59:27 UTC.
The isolated test services remain separate and have automatic deployment
disabled. Testing authorization does not authorize deployment or activation in
the bot.
