# Passive timing — local pre-deployment verification

Date: 2026-10-06. Baseline: `3084c5c7cd53b70d0569086ba78a78f8fc53a9ac`.

Result: local checks passed. This local verification did not push code, deploy
to Render, update a live environment, request the exchange, sign for a live
account, or change a live account. The implementation is opt-in and disabled
by default. Subsequent owner-authorized isolated Render testing is documented
in [RENDER_PASSIVE_TIMING_VERIFICATION.md](RENDER_PASSIVE_TIMING_VERIFICATION.md);
it does not authorize deployment or activation in the bot.

## Evidence

| Check | Result |
| --- | --- |
| Complete runtime suite with local PostgreSQL | 1,949 passed, no skips, 74.824 s |
| Exchange adapter and alert forwarder self-tests | 87 passed, no skips |
| Existing coordination, budget, emergency and transport cases repeated with timing enabled | 137 passed, no skips, 16.933 s |
| Exact original-baseline versus enabled-timing traces | Equal in all four scenarios |
| Whitespace/error check | `git diff --check` passed |

The first two rows cover 2,036 tests. The 137-case run repeats selected tests
with the actual recorder continuously enabled; it is not 137 additional unique
tests. It accepted 499 timing events with zero invalid, busy/full queue drops,
recording errors or sink errors. The bounded archive evicted 435 older events,
as designed.

There are 31 new tests: 18 recorder tests, 10 hook tests, two parameterized
PostgreSQL trace tests and one actual HTTP route privacy test.

## Trading comparisons

Each scenario entered 100 units through partial fills of 40, 30 and 30,
established and resized both exits, filled the take-profit, canceled the orphan
stop, and reopened the saved state through a fresh controller. The lost-reply
variant restarted after an uncertain stop submission and reconciled its outcome.
Both account roles were tested independently.

| Scenario | Order requests, baseline / timing | Public reader calls, baseline / timing | Durable events, baseline / timing |
| --- | --- | --- | --- |
| Long, normal | 8 / 8 | 174 / 174 | 31 / 31 |
| Short, normal | 8 / 8 | 174 / 174 | 31 / 31 |
| Long, lost stop reply | 8 / 8 | 174 / 174 | 30 / 30 |
| Short, lost stop reply | 8 / 8 | 174 / 174 | 30 / 30 |

Comparisons included full ordered venue-call arguments and order payloads,
store-call counts, durable requests, nonces, events, revisions, final state and
trade projections. Only the three new reporting fields (`last_entry_at_ms`,
`stop_quantity`, `take_profit_quantity`) were excluded when comparing to the
old version. Final closure was verified in all four scenarios. The canonical
baseline and enabled trace files were byte-identical, SHA-256:
`a415e62ca3aa2067523be09c282038fd632b14214104875aaa7bbeb405bba507`.

Separate exact comparisons also passed with a full diagnostic queue, its lock
held, and a stalled custom test writer. Existing tests exercised concurrent
observations, emergency takeover, shared request budgets, bounded lock waits,
lost acknowledgements, expired permits and restart handling with timing enabled.

The strategy, quantities, entry settings, request-budget limits, protection
priority and 180-second full-audit interval were not modified.

## Implementation and findings fixed during review

- Producers capture bounded immutable fields and try the diagnostic lock once.
  They perform no serialization, output, database work or exchange calls.
- The bounded background worker retains recent events in memory. A proposed
  stdout writer was removed after review identified contention with existing
  trading logs. Default diagnostic recording now performs no output I/O.
- Notification samples are limited to 64 identities per frame; all validated
  notifications still follow the original trading path. Deduplication of the
  diagnostic sample happens after releasing the feed lock. A 10,000-row frame
  was tested against disabled timing with identical notification state.
- Cycle and refresh records run after existing outcome handling and internal
  locks. They cannot replace the original result or exception, and no recording
  was inserted between transport admission and submission.
- Trade records use the already-computed reporting projection. Bucket/revision,
  account role, card and entry-order IDs connect the records without more reads.
- Malformed messages and heartbeat/disconnection gaps are recorded outside
  the feed lock. Snapshots, duplicates and obsolete sessions do not produce
  false live-fill receipt records.
- Public `/` and `/healthz` expose diagnostic counters only. Detailed events are
  available through the existing authenticated `/internal/testnet-diagnostics/v1`
  route, under its existing response-size limit and `no-store` policy.

## Meaning and limits of the measurements

`notification_received` records local receipt time and the separately supplied
exchange event time. Local monotonic durations are comparable only within one
recorder session. Cross-clock differences are not clock-synchronization proof.

`refresh_completed` measures method duration, including waiting for a shared
observation. It preserves that observation's original `evidence_at_ms`; returning
a cached proof does not make it new. `trade_observed` timestamps the existing
reporting path seeing saved protection evidence. It is an upper bound on local
verification latency, not the exact exchange activation time. Existing stop
public-status timestamps retain their original meaning. Reports include current
stop/take quantities so partial-fill protection changes can be distinguished.

The queue holds at most 256 records by default; the recent archive and trade
deduplication cache each hold at most 64. Drops and archive evictions have
counters. These are temporary diagnostics, not complete history: records may
be evicted and are lost on process restart. No monitoring automation or durable
timing collector was added.

The local suite used real PostgreSQL 18.6 and a deterministic exchange double.
Codex exposes only UID 0, so scratch PostgreSQL binaries had only the application
root-startup refusal bypassed. Database identity, ownership checks and engine
logic were otherwise unchanged. Initial baseline runs exposed an executor
filesystem EOF error; the final suite used a fresh `/dev/shm` cluster with
`io_method=sync`, normal transaction settings, and loopback-only networking.
Two baseline subprocess dependency errors were resolved by a scratch virtual
environment and both tests passed on rerun. None of these adaptations changed
the repository or the deployed service.

This validates tested decisions, persistence/reconnection behavior and call
counts. It does not measure Render scheduling, real exchange latency, physical
disk durability, or guarantee zero overhead or zero production risk.

## Reproduction and activation

With the repository's pinned dependencies and a disposable loopback PostgreSQL
18 database in `HL_JOURNAL_CI_URL`:

```bash
python -m unittest discover -q -s hl_testnet_runtime -t . -p 'test_*.py'
python -m unittest -q hyperliquid_testnet_executor_selftest alert_cards_forwarder_selftest
```

The repeatable enabled/disabled and stalled-recorder comparisons are in
`test_passive_timing_durable.py`; transport, projection, lock and privacy checks
are in `test_passive_timing_hooks.py`, `test_passive_timing.py` and
`test_testnet_diagnostics.py`.

Future activation requires `HL_TESTNET_TIMING_TELEMETRY=passive_v1` together with
the existing exact Testnet service and `long_stream_testnet_v1` runtime. That
setting was not changed in Render. No Mainnet activation is implemented.

## Actual local timing measurements — 2026-10-06 follow-up

The follow-up used real `perf_counter_ns` elapsed times. The recorder and
controller code were not changed. New benchmark scripts and results are local
only. **No deployment is authorized by these results.** The user's standing
instruction is recorded in the repository's `AGENTS.md`: thoroughly test every
bot change in Codex, then obtain explicit user approval for that specific change
before deployment or any push that would trigger deployment.

### Cost of a diagnostic record call

`benchmark_passive_recorder.py` measured 27,648 calls across 14 conditions,
24 balanced rounds, two payload sizes, one/two producers and deliberate
full-buffer, lock-contention and hung-writer conditions. Warmup, recorder setup,
thread start, drain and cleanup were excluded from timed calls.

| Payload, one producer | Disabled mean | Enabled mean | Added mean | Enabled p95 |
| --- | ---: | ---: | ---: | ---: |
| Notification, 10 fields | 0.605 microseconds | 8.918 microseconds | 8.313 microseconds | 10.125 microseconds |
| Trade observation, 23 fields | 1.017 microseconds | 20.725 microseconds | 19.707 microseconds | 29.014 microseconds |

Two concurrent producers added mean caller costs of 8.505 and 19.969
microseconds, respectively. Healthy runs had no drops or errors. Saturated
conditions intentionally dropped diagnostic samples; caller means stayed
between 9.06 and 20.16 microseconds across payloads and forced failure modes.
Observed single-call maxima reached 1.056 ms normally and 1.236 ms under forced
saturation. These are sample extremes, not guaranteed bounds. Even the empty
timer measurement had a 0.284 ms outlier, showing local scheduling noise.

Short batches primarily measure record/enqueue cost; this is not a full trading
hook measurement or a sustained throughput guarantee. Python thread scheduling
may serialize the two producers. Results: `passive_recorder_benchmark_20261006.json`.

### Actual controller and local PostgreSQL

`benchmark_passive_runtime.py` completed 240 measured scenario runs after 20
warmups: 12 rounds, long/short, normal/lost stop reply, and five recorder modes.
The mode order rotated and reversed. Every run covered 40/30/30 partial fills,
exit resizing, take-profit closure, orphan-stop cancellation and recovery from
saved state. The lost-reply case additionally recreated the controller after
the uncertain stop submission.

Timing compared the same candidate code with its recorder disabled or enabled.
It therefore measures activation cost, not every residual wrapper cost against
the pristine pre-change commit. Earlier original-baseline comparisons establish
trace equality, not original-baseline performance.

| Measured interval | Recorder disabled | Recorder enabled |
| --- | ---: | ---: |
| Runtime subtotal, mean (48 complete scenarios each) | 357.618 ms | 358.831 ms |
| Runtime subtotal, p95 | 416.497 ms | 404.714 ms |
| Runtime subtotal, maximum | 507.388 ms | 426.947 ms |
| Simulated fill to verified local protection projection, mean (144 fills each) | 80.804 ms | 80.569 ms |
| Simulated fill to verified local protection projection, p95 | 99.476 ms | 95.744 ms |
| Simulated fill to verified local protection projection, maximum | 178.311 ms | 119.371 ms |

The mean whole-scenario difference was **+1.213 ms (+0.34% of disabled mean)**.
The local samples do not resolve a consistent slowdown of that size: a
descriptive bootstrap interval for mean paired differences was -6.900 to
+9.072 ms, using the 12 whole rounds as sampling blocks. It is not an equivalence
test, production confidence guarantee, or proof of zero overhead. Lower times
in individual enabled/failure-mode samples should not be interpreted as speedups.

Runtime subtotal sums disjoint `tick` and existing reporting calls. Fixture/DB
setup, registration, trace queries and cleanup are excluded. Fill-to-projection
starts after the simulated fill is installed and ends when the existing report
confirms both exits cover the filled quantity. It includes local controller/DB
and reporting work, plus simulated recovery when applicable. It does **not**
measure exchange activation, delivery latency, remote latency, the normal
scheduler's wait, or the 180-second periodic audit.

All 260 runs, including warmups, retained identical canonical trading traces
for their scenario: eight simulated order requests and 174 simulated reader
calls each, complete closure and three fully covered entry fills. Healthy
enabled runs accepted 1,584 diagnostic records with zero drops or errors.
Full, contended and hung recording modes also retained identical trading
results; deliberate diagnostic drops were counted. This benchmark ran each
controller scenario serially because its CI fixture resets shared schemas.

Results and unchanged raw samples: `passive_runtime_benchmark_20261006.json`.
The disposable PostgreSQL instance was stopped after completion. The original
180-second interval, strategy, production settings, request budget and live bot
were not changed. No live exchange requests or signing operations occurred.

### Notification hook, including parsing and state updates

`benchmark_passive_feed.py` completed 120 measured off/on pairs across ten
conditions (12 pairs each, plus warmups). It timed the actual `_receive` path,
including parsing, validation, notification state changes and diagnostic hooks.
Input construction, bootstrap, recorder startup/drain and assertions were outside
the timer. Domain decision time remained synthetic to allow exact state parity;
the elapsed stopwatch and recorder timestamps were real.

| Fresh frame | Enabled full receive mean, long / short | Added cost, long / short |
| --- | ---: | ---: |
| One update | 0.042 / 0.045 ms | mean 0.030 / 0.032 ms |
| 64 updates | 0.738 / 0.703 ms | mean 0.596 / 0.592 ms |
| 10,000 updates (maximum stress frame) | 17.982 / 17.900 ms | paired median 0.778 / 0.691 ms |

At maximum frame size, disabled-side scheduling outliers made the added mean
unstable (+0.300 / -0.123 ms); the table explicitly uses paired medians for that
row. Do not infer a speedup. The largest observed positive paired difference
there was 1.954 ms. Twelve pairs cannot establish production tail bounds.

All exact feed states, health values, account separation, dirty symbols,
reconciliation flags and wake states matched. The active recorder accepted and
exported all 3,096 expected samples with zero drops or errors. Only the first
64 identities per new frame were recorded; all validated notification identities
still affected the normal feed state. Duplicate 1/64-update frames produced no
new diagnostic records. Feed socket threads were never started and network
attempt guards remained unused. This measures local receipt processing, not
exchange event-to-receipt latency or downstream protection installation.

Results: `passive_feed_benchmark_20261006.json`.

### Reproduce the additional benchmarks

Run sequentially from the repository root with pinned dependencies. The runtime
command additionally requires `HL_JOURNAL_CI_URL` pointing to a **disposable**
loopback database named `hl_journal_ci`; its test fixture resets CI schemas.
Never point it at a live journal or run it concurrently with other CI fixtures.

```bash
python -m hl_testnet_runtime.benchmark_passive_recorder --output /tmp/recorder.json
python -m hl_testnet_runtime.benchmark_passive_runtime --output /tmp/runtime.json
python -m hl_testnet_runtime.benchmark_passive_feed --output /tmp/feed.json
```

The pre-existing functional suites were not rerun merely to increase counts:
this follow-up added no production-code changes. Every measured runtime and feed
scenario performed its own trading/state parity and protection assertions.
