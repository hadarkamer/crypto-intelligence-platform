# Automatic acquisition of explicitly registered frozen cohorts

This layer connects the existing single-anchor source transport to the
[PostgreSQL cohort executor](NO_HORIZON_POSTGRES_RUNTIME_V1.md). A caller registers
an exact normalized declaration once; bounded background passes collect its
source evidence, reconstruct every declared part and submit the fully validated
cohort. No manually assembled intermediate export files are required.

It does not generate research populations, change candidate predicates, choose
new dates, advance a frozen cutoff or schedule repeated experiments. Registration
is not prospective formula validation, and automatic source admission grants no
runtime signal, Telegram or trading authority.

## Fixed request and source boundary

Migration `057_no_horizon_acquisition.sql` adds separate requests, anchors,
transport proofs and terminal receipts. Registration binds the normalized
declaration and acquisition/executor implementation. An optional request key is
an immutable alias; changing the alias cannot create another instance of the
same request. A changed declaration or implementation requires an explicit new
identity, and a retained old request is never silently upgraded.

Both the destination and source database clocks must reach the later of the
declared timestamp and price cutoff before acquisition reads the source
population. Merely registering a future request opens no source connection.
Database-clock checks, transaction setup and schema inspection are control
queries, not source-population reads.

The population remains accepted Watch intake rows visible in the one anchor
statement snapshot within the declared half-open intervals. It is not all market
opportunities. A later accepted row does not expand an existing request. This
retrospective source contract and the original prior-outcome-knowledge field are
preserved; a caller-supplied declaration time does not establish when an
experiment was prospectively registered.

Each source query owns a separate, explicitly read-only transaction. UTC,
timestamp/float serialization and bounded statement/lock timeouts are fixed.
The source and destination connections are distinct, even if their explicit
settings point to the same database. Neither setting falls back to a primary or
legacy database URL. A source role should independently have only the required
SELECT privileges; transaction enforcement is not role provisioning.

## One anchor and resumable exact proofs

The first committed anchor covers all declared transport parts in one statement
snapshot. Its exact generated SQL, untouched PostgreSQL JSONB response text and
hashes are retained. Later source batches reuse the anchor's pinned BTC-parent
payloads; later price pages must match the anchor's exact manifest. These reads
occur in separate transactions and do not claim one long-lived MVCC snapshot.

Each bounded pass resumes only missing proof tasks. Committed anchors and proofs
are immutable. A crash before an anchor is committed may repeat acquisition;
once persisted, that anchor is never replaced on retry. The collector preserves
the full source response, fetch receipt and query for each accepted proof and
freshly validates them before complete part assembly and admission.

Missing, duplicate, extra or changed leaves block the frozen request. A transient
connection failure can retry the same missing task against the same anchor; it
cannot produce a fresh sample. Causal parent changes after anchoring do not
replace the original pinned parent. Unknown feature or parent evidence blocks
the whole cohort during the existing full preflight. Valid empty/insufficient
scopes remain admitted for descriptive execution, and missing entry prices keep
their existing explicit `INPUT_BLOCKED` behavior.

The existing source, declaration, anchor, row and candle limits still apply.
Acquisition additionally caps retained canonical transport-proof bytes at
512 MiB per request. SQL/proof overhead counts toward that bound. The collector
checks a conservative reservation before a fetch and actual retained bytes
before insertion. An oversized or over-budget response blocks admission and
retains a bounded diagnostic including size/hash when available, explicitly
marking any unretained raw response. It never presents a truncated proof as
complete. These limits bound accepted/retained evidence; they are not a strict
process peak-memory guarantee because a driver receives a query result before
Python can validate its size.

## Leases and executor handoff

Requests use expiring database-clock leases, monotonic fencing and oldest-work
queue ordering. Every proof write checks the current owner and fence. Request
identity, committed evidence and terminal receipts cannot be rewritten.

Admission calls the existing public `PostgresCohortStore.submit_cohort`, which
reruns the full source/parent preflight. Its internal transaction guard locks the
acquisition request and checks its live lease before and after executor writes,
so an expired owner cannot commit a new executor population. All original
executor input validation remains mandatory.

Executor admission and the final acquisition receipt are two commits. The
handoff uses the immutable `acquisition:<request_id>` cohort key. If a process
stops between these commits, a subsequent owner reuses exactly the admitted
executor plan and records the missing link. It does not create another trial.

## Runtime and explicit commands

The existing research worker continues executing admitted cohorts first. Its
optional acquisition pass follows. A source, configuration or acquisition
failure is recorded separately and does not discard completed executor work.
Shutdown waits for the current bounded pass, including acquisition.

| Setting | Meaning |
|---|---|
| `RESEARCH_NO_HORIZON_ENABLED` | Existing overall worker opt-in; default off |
| `RESEARCH_NO_HORIZON_DATABASE_URL` | Explicit destination database |
| `RESEARCH_NO_HORIZON_ACQUISITION_ENABLED` | Additional acquisition opt-in; default off |
| `RESEARCH_NO_HORIZON_READ_DATABASE_URL` | Explicit source database, separate read-only connection |
| `RESEARCH_NO_HORIZON_ACQUISITION_LEAF_BUDGET` | Default 4; allowed 1–32 source queries per pass, including anchor capture |
| `RESEARCH_NO_HORIZON_LEASE_SECONDS` | Existing lease setting also applies to acquisition |

Schema installation remains an explicit separate action. Neither registration,
worker startup nor an acquisition pass applies a migration. Public health
contains compact request/plan identifiers, progress counters and error types;
it contains no database URLs, SQL or raw evidence.

For an already installed research schema and explicitly configured databases:

```bash
python research_no_horizon_acquisition_cli.py register \
  --declaration new-cohort.json --request-key new-cohort \
  --output registration.json

python research_no_horizon_acquisition_cli.py run \
  --request-id REQUEST_ID --worker-id manual-research --leaf-budget 4 \
  --output acquisition-pass-001.json

python research_no_horizon_acquisition_cli.py report REQUEST_ID \
  --include-proofs --output acquisition-evidence.json
```

The explicit `run` command performs one bounded pass; background execution uses
both opt-in flags. Output files are create-only. A blocked request produces a
diagnostic receipt and CLI exit 2; input/connection errors return 1. A waiting
request or completed pass returns 0. Admission means validated source reached
the executor, not that computation is complete or the formula qualified.

## Preserved experiment and remaining work

The October 4–18 declaration and scheduled evaluation remain pinned to their
original commit `e8d25e57`. This acquisition implementation does not register,
reinterpret or replace that experiment. New requests have their own identities.

Finite repeated population generation and upfront registration are provided by
[the calendar layer](NO_HORIZON_CALENDAR_V1.md). [Bounded catalog discovery and
single-window ranking](NO_HORIZON_DISCOVERY_V1.md) use these existing paths.
Version-aware prospective validation and result delivery remain separate work. The first-touch engine, probability policy and unavailable
asymmetry route are unchanged. No provider acquisition, production deployment,
Telegram message or trade is performed by this implementation.
