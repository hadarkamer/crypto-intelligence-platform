# PostgreSQL execution of frozen no-horizon cohorts

This adapter moves the existing, source-validated global cohort calculation
from a local research session into a resumable PostgreSQL work queue. It uses
the existing source preparation, globally earliest causal-parent
representatives, first-touch engine and atomic probability gate unchanged.
One admitted cohort is one immutable research population; transport parts are
never independent experiments.

## Completed development boundary

- Additive migration `056_no_horizon_runtime.sql` stores immutable plans,
  one indexed shared candle series per plan, per-scope entries/checkpoints,
  fenced work accounting and immutable terminal receipts.
- `PostgresCohortStore.submit_cohort(declaration, anchor, load_part)` performs
  the existing full source/parent preflight before the first database write.
  Unknown source evidence blocks admission. Complete insufficient/empty
  scopes remain in the denominator; selected missing entry prices remain
  explicit `INPUT_BLOCKED` scopes.
- `ResearchNoHorizonWorker` consumes admitted work in bounded background
  passes and is connected to the existing independent startup, shutdown and
  health hooks. One worker's startup failure does not prevent other workers
  from starting. Disabled means no connection, task or schema mutation.

Automatic source acquisition, calendar-based cohort creation, broad candidate
search/ranking, no-horizon prospective validation registration and delivery
binding remain separate development. The worker does not infer a new scope,
cutoff or policy when the queue is empty. It does not implement continuously
extended live entries by mutating frozen cohorts. Adding either intake mode
requires an explicit population/revision contract.

## Admission and implementation identity

Use an idle psycopg connection with `dict_row`; each public store operation
owns and completes its transaction. No method creates tables. The private
prepared-plan helper is not an external receipt-import API. A caller supplies
the original complete anchored parts to the public admission method, which
revalidates the complete population using the existing pure preparation.
The trusted acquisition must retain the original exact SQL and raw proof bytes;
database integrity hashes do not independently authenticate their origin.

The PostgreSQL plan identity binds the original prepared-plan identity and the
runtime backend implementation, including its migration. It intentionally
differs from a local SQLite plan ID. Source contracts, selected entry IDs,
outcomes and gate results are comparable across backends; storage-specific
receipt hashes are not expected to be identical.

A named cohort key cannot be repointed to changed input, cutoff or policy.
Exact resubmission is idempotent. Final admission seals the materialized input
set against later insertions as well as modifications. Resume requires the
same backend implementation. Incompatible retained jobs do not occupy the
claim queue for a newer implementation, and remain reportable as historical
evidence. The October 4–18 declaration remains bound to its original commit;
this adapter does not replace that declaration or its scheduled execution.

## Bounded concurrent execution

Claims use PostgreSQL row locking with `SKIP LOCKED`, database-clock leases and
monotonic fencing tokens. Queue ordering preserves age across newly admitted
and previously processed scopes. Computation reads a coherent snapshot without
holding a write lock, consumes only a bounded candle suffix, then commits its
checkpoint, work count and any final receipt atomically. The final mutation
requires the same owner, fence, cursor, consumed-work count and unexpired lease.
A lost worker can lose only its uncommitted batch, never a committed prefix.

Prices retain the original entry rule: the first minute open at or after the
decision, including the same minute on an exact boundary. Missing prefixes,
same-minute ambiguity and open outcomes keep the existing semantics. Neither a
later winner nor a price gap changes the selected representative population.

Reports use a coherent read snapshot and verify complete materialization,
checkpoints, work accounting and terminal receipts. Every declared scope is
visible. A scope counts as an executed outcome trial only after committed
work; an empty scope's committed finalization counts once, while an
input-blocked scope does not. Completed computation and gate qualification
remain separate facts. The existing probability policy is unchanged; the
no-horizon asymmetry route remains unavailable.

## Explicit activation settings

The worker defaults off and never inherits an older research/Watch flag.

| Setting | Meaning / default |
|---|---|
| `RESEARCH_NO_HORIZON_ENABLED` | Explicit worker opt-in; default off |
| `RESEARCH_NO_HORIZON_DATABASE_URL` | Explicit database; no primary/legacy fallback |
| `RESEARCH_NO_HORIZON_POLL_SECONDS` | 30; allowed 5–3600 |
| `RESEARCH_NO_HORIZON_CANDLE_BUDGET` | 4096 per pass; allowed 1–100000 |
| `RESEARCH_NO_HORIZON_ENTRY_BUDGET` | 64 per pass; allowed 1–2048 |
| `RESEARCH_NO_HORIZON_BATCH_SIZE` | 128 per fetch; allowed 1–4096 |
| `RESEARCH_NO_HORIZON_LEASE_SECONDS` | 120; allowed 30–600 |

The DSN may point to the existing research database with an appropriately
authorized role; no new hosted service or database is required by this code.
The existing explicitly gated schema installer knows migration 056. Worker
startup only inspects schema availability and never installs a migration.

Each pass opens its own bounded-timeout connection. Shutdown stops admission
of another pass and waits for the in-flight thread to commit or roll back;
it does not detach a database thread that could overlap a restarted worker.
Public health records only compact work identifiers/counters and sanitized
error types, never credentials, query text or raw research payloads.

The receipt fields `runtime_authorized`, `telegram_authorized` and
`trading_authorized` stay false. Background research execution is a distinct
explicit opt-in; it grants no signal promotion, delivery or trading authority.
The worker makes no provider or notification requests and leaves both the
ordered-v7 pipeline and the legacy Formula quarantine unchanged.

## Verification

`research_no_horizon_postgres_store_postgres_selftest.py` uses an explicit
disposable `TEST_DATABASE_URL`. It exercises the
real migration, PostgreSQL leases/transactions, local-engine parity, bounded
restart, immutable materialization and complete/blocked/empty populations.
The no-database store and worker tests cover rejected admission, opt-in gates,
budgets, sanitized health and graceful shutdown. The existing lifecycle test
also verifies that a failed new worker does not prevent other workers starting.

Implementation or synthetic test completion is not production deployment,
market evidence, formula qualification or trading authorization.
