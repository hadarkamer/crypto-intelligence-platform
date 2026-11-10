# Watch failed-poll diagnostics repair

Base: f4167cc6694f509070f9a9c54063d26b98f66b10. No deployment or merge.

The existing collector discarded diagnostic row evidence precisely when its
verification gate failed. The existing capture helper now summarizes the final
poll of each failed attempt. The existing retry receipt and archive JSON retain
those counts, including when a later attempt succeeds.

The summary records raw/parsed/classified/rejected counts, rejection categories,
unclassified rows, and the local DOM-read completion time. Missing placeholders
and unparseable text have separate counters. This is an observation timestamp,
not CoinGlass's source computation time. Existing cycle binding, timeframe,
attempt, selected-window and verification metadata remain available.

No raw failed table, symbol list, cookies, credentials, or page body is saved.
Raw capture status remains UNAVAILABLE; failure_summary is separately available.
Counts are for the last poll of each attempt, not every poll. The existing
512-row classification limit, three-attempt limit and envelope byte limits apply.
Reason counts overlap: one rejected row can lack both target and amount.
The existing archive retention policy is unchanged; no expiry, deletion or
historical rewrite was added.

No additional DOM/network read, retry, background task, database write, schema,
dependency, Telegram interface or trading change is introduced. Existing parser
rules, 30-row threshold, seven-window atomicity and failure behavior are retained.
Diagnostic failure falls back to the existing unavailable marker.

## Validation

- Before-fix regression reproduced against the base reader: failure_summary
  absent. The same regression passes with the repair.
- 89 unittest cases: 88 passed, zero failures/errors, one skipped PostgreSQL
  atomic-claim case. Archive attachment and base archive suites also passed.
- Six integrated scenarios run the actual collector, retries, parser, archive
  builder and persistence SQL code: rejection, recovery, diagnostic failure,
  archive failure, complete Watch rejection, complete Watch recovery. The Watch
  recovery runs actual scoring and reaches the simulated delivery boundary.
- DOM/browser, market-provider, Telegram/execution and database connections are
  simulated. External socket connections are forbidden during the final suite.
  Existing delivery and restart suites cover ACK/error/deduplication behavior.
- The two-window incident shape (50 raw rows, 28/25 accepted, 22/25 missing Long
  fields) remains rejected across three attempts; all summaries survive archive
  serialization. These are controlled fixtures, not recovered incident rows.
- Recovered seven-window input returns 350 rows. A diagnostic or archive failure
  does not change acceptance/rejection. Cancellation closes the retry page.
- A real local PostgreSQL run was attempted, but this environment denied AF_UNIX
  socket creation (PermissionError). Real DB roundtrip and atomic-claim validation
  remain unverified here. No production database was used for testing.

Reproduction with repository dependencies installed:

```sh
python -m unittest coinglass_dom_reader_selftest research_dom_capture_selftest research_dom_capture_archive_selftest.FailedPollPipelineTests selected_watch_delivery_selftest research_watch_decision_capture_watch_selftest watch_transition_delivery_selftest watch_restart_delivery_selftest
python research_dom_capture_archive_selftest.py
python research_max_pain_archive_selftest.py
```

Optional real-DB pipeline validation accepts only TEST_DATABASE_URL with a local
Unix socket or an allowlisted local/CI host and an explicitly test-named database.
It creates and removes a unique disposable child database. The script entry point
runs the integration class so the existing GitHub Actions self-test loop includes
it. No workflow or infrastructure change is required. Never point it to production.
pgserver was used only as a local test dependency;
requirements.txt is unchanged.

## Resource observation and rollout boundary

Local 50-row benchmark: median 0.390 ms over 100 summary calls; observed maximum
8.917 ms; tracemalloc peak 4,735 bytes. A representative failed capture with three
summary receipts serialized to 2,163 bytes. These are isolated measurements, not
production service peak-memory estimates. No extra browser or DB operation occurs.

The repair improves diagnosis; it does not itself restore missing CoinGlass
values or make an incomplete Watch cycle valid. Live diagnosis requires a
separately approved deployment and a naturally occurring failed attempt.
Rollback is reverting this change; existing historical JSON requires no migration.

## Whole-mechanism resource assessment (2026-11-10)

Production remains f4167cc; this repair has not been deployed. Read-only Render
and existing database observations cover Nov 7 14:00 UTC through approximately
Nov 10 14:20 UTC. GB below means decimal bytes / 1e9.

| Observation | Result |
| --- | --- |
| Render service memory, 4,339 one-minute samples | median 1.004 GB; p95 1.428 GB; max 1.558 GB; limit 2.147 GB |
| Existing cgroup observations | highest observed 1.686 GB (78.5% of limit), Nov 10 09:10 UTC |
| Service CPU, 4,340 minute AVG samples | mean 67.7% of one CPU; p95/max 100%; 1,137 samples (26.2%) at least 90% |
| Watch collection, 145 cycles | 143 complete, median 309 seconds, max 421 seconds; two failed, 378 and 710 seconds |
| Passive collection, 145 cycles | 144 complete, median 165 seconds; one failed, 546 seconds |
| Full observations ten minutes after Watch cleanup | 145/145 had zero browser descendants; one process session in this window |
| Mean Python RSS ten minutes after cleanup | Nov 7: 879 MB (20 samples); Nov 8: 891 MB (48); Nov 9: 928 MB (48); Nov 10: 938 MB (29) |
| Postgres, five-minute samples | memory max 488 MB / 1.074 GB; connections median 8, max 19 |

Runtime journal periodic observations independently observed browser RSS up to
854 MB, up to seven browser-named descendants and one other descendant. RSS sums
double-count shared pages and are not container memory. Post-cleanup samples at
30/120 seconds can overlap the subsequent passive collection. They are not proof
of a leaked browser. All 600-second observations above had COMPLETE coverage.

The highest cgroup reading is valid, but that observation's process enumeration
was PARTIAL due to its time budget. No memory.events/OOM counters were available
from these stored observations. No MEMORY_DIAGNOSTIC log entries were returned
for the window; the existing journal/observation tables supplied the evidence.
Sampled maxima cannot exclude shorter unobserved peaks.

Current DB reads found no blocked sessions; observed idle-in-transaction ages
were under 0.4 seconds. This is a spot check, not a historical lock guarantee.
Watch's next natural cycle completed Nov 10 14:07:28 UTC with delivery_errors=0.

### Matched isolated whole-pipeline workload

Separate fresh Python processes ran base and repaired collector/capture code
against the same six-scenario integration fixture: ten repetitions each after
warmup, 60 scenarios including 20 full Watch cycles per version. Only assertions
requiring the new summary were omitted from the baseline comparison. DOM,
browser, market providers, database connection and transport were simulated;
actual parsing, collection coordination, archive construction/persistence SQL,
Watch orchestration and scoring ran. All external socket connections were denied.

| Measurement | Base | Repair |
| --- | ---: | ---: |
| Whole 60-scenario workload wall time | 13.351 s | 13.579 s |
| Whole workload process CPU time | 13.386 s | 13.606 s |
| Failed Watch median wall time | 161.0 ms | 169.6 ms |
| Recovered Watch median wall time | 416.8 ms | 422.5 ms |
| Highest sampled Python RSS during Watch | 120.15 MB | 120.32 MB |
| RSS after warmup / after ten repetitions | 108.25 / 115.81 MB | 108.15 / 119.62 MB |
| Open descriptors before / after | 5 / 5 | 5 / 5 |
| Threads before / after | 1 / 1 | 1 / 1 |
| Remaining child processes | 0 | 0 |

Whole-workload CPU increased 1.65% in this one paired run. These short synthetic
measurements include test doubles, assertions and the sampling thread; they do
not estimate production Chromium/Node memory, network latency or DB concurrency.
RSS rose across repeated fixtures in both versions. This experiment cannot
establish absence of a long-term leak or attribute the small RSS difference to
the repair. No additional browser/read/retry/DB write is introduced by the repair.

### Assessment

No observed evidence of memory-limit exhaustion caused the diagnosed row-count
failure. The service has about 462 MB headroom against the highest stored cgroup
sample, not a guaranteed reserve. Sustained CPU pressure and the roughly 59 MB
rise in post-cleanup Python RSS warrant attention independently of this repair.
Browser cleanup is supported by the 145 complete delayed observations; overall
memory stability is not proven. No resource scaling, new monitor, retention
change, formula change, trade change or deployment was performed.
