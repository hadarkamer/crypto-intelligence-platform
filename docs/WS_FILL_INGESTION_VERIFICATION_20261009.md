# Direct WebSocket fill ingestion — 9 October 2026

Status: VERIFIED AND DEPLOYED. Final isolated verification passed on
9 October 2026 at 13:39:07 UTC. After the user's explicit approval for this
tested change, the live Testnet deployment completed at 13:46:37 UTC
(16:46:37 Israel). Startup verification completed at 13:48:35 UTC.

## Approved live deployment

- Existing Testnet service: `hl-testnet-check-yoyo`, `srv-dakptbh594qs7395460g`.
- Branch `paper-trading-v1` advanced by a non-forced, expected-head update from
  `93c7c6005dca34f4da10459f508434833e579ef1` to the exact tested candidate
  `d86ed93ff6339eaa71c80394b99b726112046feb`.
- Candidate tree `b24a8bc6a32f2202b515f4dc0643e826d4785c39` matched the local
  candidate and isolated verification. Independent verification recomputed all
  535 source/dependency hashes with zero mismatches.
- Auto-deploy was confirmed disabled. One manual deployment was triggered:
  `dep-db4evkfavr4c73e98keg`. Render reports `live` for that exact commit,
  finished at `2026-10-09T13:46:37.612027Z`. Its build also passed all 66
  existing build-time runtime tests.
- At 13:47:01 UTC, the new process reported ownership contention and kept new
  entries disabled while the previous process retired. No intervention was
  needed to complete ownership transfer.
- At 13:47:47 UTC, `/healthz` returned HTTP 200, `EXPERIMENTAL_OWNER_RUNNING`,
  ownership `HELD`, entries enabled, no current-cycle error and a cycle age of
  1.397 seconds.
- At 13:48:35 UTC, the second check again returned HTTP 200, active ownership,
  enabled entries and no current-cycle error. The cycle timestamp advanced
  from `1791553665965` to `1791553709348`; its age was 5.979 seconds.
- Warning/error log queries returned no records from startup at 13:46:20 UTC
  through 13:48:30 UTC. The exact candidate remained the latest live deployment.
- These checks confirm deployment and progressing worker cycles. The public
  health endpoint does not expose per-account feed status or ingestion counters;
  these checks do not prove a new live fill was ingested or a trade completed.
  No diagnostic exchange requests or synthetic live orders were sent.
- No runtime configuration, account routing, strategy, exchange request budget
  or Mainnet setting was changed during deployment.

## What changes

Previously the persistent connection notified the worker that account activity
had changed. The worker needed successful REST reconciliation before saving the
execution and its attributable quantity. The new worker drains received fill
facts at the start of its next cycle, before REST collection, into the existing
execution-state transaction. Socket callbacks do not write the database or send
orders.

- WebSocket and REST use the same canonical fill parser and exchange identity.
- A valid fill for an exactly bound, actively bot-managed order updates the
  saved order fills and attributable trade fills atomically. The queue
  acknowledges that exact batch only after commit.
- Repeated delivery, reconnect replay, a retry after an uncertain commit, and
  later REST overlap do not add the same execution twice. Concurrent arrivals
  are not removed by acknowledgment of an older batch.
- Complete stored checkpoints still reject changed historical facts after the
  recent account observation buffer has pruned a redundant copy.
- Fills without exact durable order ownership remain account observations.
  They do not invent a bot assignment, a manual profit allocation or position.
  REST can subsequently establish ownership of an initially unbound order.
- Invalid allocation, including an incomplete out-of-order batch, records a review reason and
  blocks the affected lane until complete reconciliation. A malformed pending
  batch blocks that account's new entries while REST and the peer account's
  valid work can continue. A database failure retains the batch and stops the
  cycle before further work.
- The existing manual-management rules remain in force. Stream reception cannot
  take a human-managed coin back under bot control.

There is no new service, table, journal, database migration, strategy, account
route or exchange request-budget setting. The bounded pending queue belongs to
the existing feed, and durable facts use existing state and account observations.

## What still requires REST

A received execution proves that execution. It does not certify complete order
inventory, final order status, continuous price coverage or an unbroken history.
Only a complete reconciliation advances the full-history recovery cursor and
releases the applicable safety gates. Recovery starts from that completed
cursor, not the newest WebSocket timestamp. Initial snapshots and reconnect
replay cannot independently authorize new entries.

No routine REST query was removed by this package. The measured improvement is
earlier durable knowledge of a fill, including when the following REST call
fails. Sending or resizing protection still requires the existing independent
evidence and safety checks. This is not a measured live latency or global
request-reduction claim.

The pending queue and recent account history retain their bounded capacities
(4,096 fills each). Exhaustion fails closed; REST advancement can prune old
recent observations and permit a retry. These buffers are not a complete
permanent archive of all manual activity. Existing active-trade and archival
execution records remain responsible for attributable bot history.

## Exact candidate

- Deployed baseline: `93c7c6005dca34f4da10459f508434833e579ef1`.
- Candidate branch: `codex/ws-fill-ingestion-20261009`.
- Candidate commit: `d86ed93ff6339eaa71c80394b99b726112046feb`.
- Local equivalent: `40fcf98e87a96f6a1d4da059ef725ceea3bcbdf6`.
- Identical candidate/test tree: `b24a8bc6a32f2202b515f4dc0643e826d4785c39`.
- Isolated verification commit: `5de49616728ee94d321c71a353b9501bec4553e8`.
- Source/dependency manifest: `71eac3145aef7953339c0f65316f61a67da5a14c189f70f7341448843b64862e`, 535 files.
- Canonical source-map digest used by local probes: `950da8397845498bb2d3bf7f930214247d5df0d230ebe076616a1e48a0993b50`.
- Existing isolated static-build service: `srv-db2dv53ncjis73ec9lng`.
- Isolated build: `dep-db4epmrl550s73bh6rv0`.

This report and its JSON evidence are documentation outside the executable
source/dependency manifest. Later documentation commits do not change the
candidate code identified above.

## Verification

The final guarded local suite discovered 3,060 tests: 2,492 passed, 568 required
PostgreSQL and were explicitly skipped locally, with zero failures, errors or
unexpected successes. No external network call was attempted, and all 535
source/dependency hashes remained unchanged. Local skips do not verify database
behavior; the isolated run below is required for that evidence.

The first full local attempt exposed inconsistent fixture identities in exit
and archive tests: saved software executions used names such as `fill1_0`, while
the raw REST fixtures represented the same executions as `hl:<numeric tid>`.
The fixtures were corrected to maintain one identity across the saved state,
raw collector and archive. Existing assertions were retained. Production
invariants were not weakened. The corrected full local run passed as above.

Focused and integration coverage includes canonical REST/WebSocket parity,
partial entry and exit fills, duplicates, reconnects, old sessions, buffer limits,
concurrent arrivals, rollback, failed commit, committed-but-unacknowledged replay,
restart, direct persistence before failing REST, unbound orders, manual lanes,
account scope, terminal immutability, overfills, reordered entry/exit evidence,
changed historical facts, missing REST overlap, protection of the peer account,
retention recovery and previously resolved/pruned reconciliation episodes.

The actual live evidence-provider adapter is exercised with a synthetic raw
exchange; the tests do not substitute a permissive provider for reconciliation.

### Isolated PostgreSQL result

The first isolated full run (`dep-db4elcbbc2fs73bi3bj0`) ran all 3,060 tests
with zero skips and errors, and one failed assertion in the new PostgreSQL
rollback test. The test expected the injected internal exception text, whereas
the established PostgreSQL journal deliberately returns the sanitized
`PERSISTENCE_UNAVAILABLE_NO_SEND` error. The test now checks the exact expected
type and code for each backend, independently proves that execution reached
the injected failure after allocation, and still requires the complete prior
state to remain unchanged and the queue to remain unacknowledged. Production
code did not change. The first run's PostgreSQL instance stopped successfully;
its sanitized result is retained in `verification/ws-fill-ingestion-20261009/render-attempt1-summary.json`.

The corrected isolated run (`dep-db4epmrl550s73bh6rv0`) passed against the exact
535-file manifest above, using a fresh loopback PostgreSQL 18 instance,
synthetic exchange fixtures, credential-free child environments and
network/subprocess guards. Its final result was recorded at 13:39:07 UTC;
the static verification build finished at 13:39:10 UTC.

- Full suite: **3,060 tests passed**, with zero failures, errors, skips or
  unexpected successes and exit code zero. This includes all 568 cases skipped
  locally for lack of PostgreSQL.
- Timing suite: 28 tests passed, with zero failures, errors or skips.
- Recorder: 27,648 measured calls across 14 conditions completed successfully.
- Feed: all 120 exact state-parity comparisons matched.
- Runtime: all 80 trace-equality comparisons matched across four rounds.
- Durable database verification completed; the temporary PostgreSQL instance
  stopped successfully.
- All five preflight stages passed. The stage counts overlap and must not be
  added together as a count of unique tests.
- No live exchange account or existing database was used. These results verify
  the isolated candidate, not completed trades or latency on the live service.

The sanitized final evidence is retained in
`verification/ws-fill-ingestion-20261009/render-final-summary.json`.

## Measurements against the deployed baseline

These are controlled synthetic scenarios with unchanged request-budget rules;
they are not production rate ceilings or latency measurements.

| Scenario | Baseline reads / weight | Candidate reads / weight |
|---|---:|---:|
| Protected position, 60 seconds | 14 / 174 | 14 / 174 |
| Two empty accounts, 60 seconds | 6 / 84 | 6 / 84 |
| Reused entry preparation after 5 seconds | 3 / 42 | 3 / 42 |
| Due activity hint on inactive account | 6 / 85 | 6 / 85 |
| Reuse an already complete fill-history interval | 0 / 0 | 0 / 0 |
| Fetch a missing due fill-history interval | 1 / 20 | 1 / 20 |

No extra synthetic order action occurred. All measured routine request counts
and weights matched the deployed baseline. New or truncated histories may still
require pagination under the existing recovery policy.

For a known partial fill of one unit, a separate probe measured:

| Observation point | Baseline saved quantity | Candidate saved quantity |
|---|---:|---:|
| After socket callback, before worker processing | 0 | 0 |
| After direct processing, before REST | 0 | 1 |
| After duplicate delivery and direct processing | 0 | 1 |
| After the subsequent REST call fails | 0 | 1 |
| After successful complete REST reconciliation | 1 | 1 |

The candidate direct-processing stages made zero REST calls and zero order
submissions. Final canonical execution facts matched REST-only processing.
Both baseline and candidate probes attempted zero external network calls and
verified that their input source hashes remained unchanged.

## Activation and remaining scope

Before approval, no live branch, live runtime configuration or live service
deployment had been changed for this candidate. A read-only Render check during final verification
confirmed the live Testnet service still runs baseline
`93c7c6005dca34f4da10459f508434833e579ef1`, with automatic deployment disabled
and the previous deployment `dep-db4e3rqd0e5s73emgcl0` still current.
The subsequent explicitly approved deployment and startup verification are
recorded above. A successful isolated test or healthy startup is not proof of
a completed live exchange trade.

Telegram card integration, outbound IP separation, permanent manual-activity
archiving, and complete tracking of unfilled orders opened and canceled between
observations are separate work. Direct fill ingestion alone does not implement
those features.
