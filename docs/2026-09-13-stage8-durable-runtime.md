# Stage 8 durable research runtime

This runbook joins the frozen Stage-8 contract, read-only Shadow pipeline,
append-only PostgreSQL registry, outcome-blind selector, and authoritative
outcome reader into one operational sequence. It does not enable a production
worker, Telegram, LIVE delivery, or trade execution.

## Frozen decision rule

An exact binding can become research-qualified only when all shared authority
checks pass and one of the two evidence routes passes:

```text
at least 5 distinct eligible btc_parent_movement_id values per passing route
AND (probability route OR common-window asymmetry route)
```

There is no three-parent shortcut. Missing, ambiguous, duplicate, stale,
non-terminal, or source-incompatible evidence remains `UNKNOWN`; it is never
converted to a failure or a zero.

Every successful result is limited to `EXPERIMENTAL_RESEARCH_ONLY`.
`live_authorized`, `telegram_authorized`, and `trade_authorized` are always
false in Python and are independently forced false by PostgreSQL.

## End-to-end sequence

1. Apply migrations `001` through `051` to a staging PostgreSQL 15+ database
   after an administrator has created the five dedicated Stage-8 roles.
2. Register one exact frozen binding with
   `research_stage8_registrar_v1`. PostgreSQL assigns `frozen_at_utc` and the
   immutable freeze identity; callers cannot supply or backdate either value.
3. Run the opt-in Shadow coordinator with
   `research_stage8_reader_v1` in one read-only, repeatable-read transaction.
   It reads the bounded attempt cohort, projects exactly one fact per attempt,
   and selects the earliest known match per eligible BTC parent without
   reading outcomes.
4. Persist the complete fact batch with
   `research_stage8_fact_writer_v1` in one `REPEATABLE READ` transaction. The
   database fixes the Watch archive high-water, independently revalidates the
   selected capture and its causal sources, links each attempt to its source
   slot and direction-specific event, derives the closed
   `selection_fact_identity_sha256`, and seals the full population, including
   false, unknown, and noneligible rows. A deferred database guard prevents an
   unsealed batch from being committed or completed in a later transaction.
5. Persist the predicted representative set with
   `research_stage8_selector_writer_v1`. The database compares every predicted
   identity with its server-derived fact identity and recomputes the complete
   earliest-match-per-parent selection before accepting the append.
6. Read the durable selection with `research_stage8_reader_v1`. In the same
   read-only, repeatable-read snapshot, replay every causal projection source,
   read the exact Ordered First Touch V7 cells and common-window metrics, and
   calculate a caller-side diagnostic for the frozen atomic gate. The fixed
   plan uses 15 bounded `SELECT` statements and accepts no caller-supplied
   representatives or outcomes. It always returns
   `research_qualified=false`; hashes created by the reader are not server
   authority.
7. Persist only the adapter's closed evaluation payload with
   `research_stage8_evaluator_writer_v1`. PostgreSQL replays each fact from the
   exact server-persisted Watch snapshot, revalidates the sealed population,
   and recomputes probability, Wilson lower bound, MFE/MAE ratio, dominance,
   median paired edge, route membership, and the atomic result from the
   embedded evidence. Only this trigger may mint a research-only qualification,
   and only after every selected representative's fixed outcome horizon has
   elapsed according to the database clock.

Each write role has a distinct database URL and cannot be replaced by a
generic application URL. `RESEARCH_STAGE8_DATABASE_SCHEMA` must name the exact
schema containing migration 051 (normally `public`). Every registry entrypoint
pins that schema ahead of `pg_catalog` and explicitly places `pg_temp` last,
then verifies the resolved relation identity. The read-side role has `SELECT`
only and must have neither schema `CREATE` nor a shadowable `search_path`.

## Evaluation lifetime

An evaluation receipt is an immutable, point-in-time audit of one exact
selection and one exact source snapshot. The frozen route policy retains
`OPEN`, `UNRESOLVED`, `DATA_MISSING`, and absent evidence as disclosed route
exclusions; it does not silently turn them into failures or require the other
route to become available. Consequently a later source revision does not
rewrite, revoke, or reinterpret an older receipt.

There is deliberately no implicit "latest" or current-qualification consumer
in Stage 8. Any future runtime consumer must first add a server-defined current
view or perform a fresh authoritative evaluation and select it by an explicit
server ordering. It must never treat an arbitrary historical
`research_qualified=true` row as current delivery, LIVE, or trading authority.

## Identity boundary

Before persistence, the selector may emit only
`expected_selection_fact_identity_sha256`. It is a deterministic prediction
over 21 closed causal fields and is not database authority. Full fact hashes,
free-form metadata, and reason text remain audit-only and cannot change the
representative set.

The fact trigger derives the same identity from authoritative attempt, slot,
event, a directly reconstructed containing parent, and the latest causal
BTC bar. It never treats the materialized event-membership table as projection
or qualification authority. Only after a one-to-one durable readback may the
outcome adapter normalize it to the authoritative
`selection_fact_identity_sha256` consumed by acceptance.

The persisted prospective attempt, slot, and event rows remain an upstream
trust boundary. Migration 051 cross-binds their identities, clocks, coverage,
feature references, and immutable source rows, but it does not claim to be a
second implementation of every semantic check in the Python v4 anchor
validator (including the complete frozen-source family and feature-bundle
calendar rules). None of the five Stage-8 roles has DML authority on those
source tables. Staging must therefore admit anchor rows only through the
existing validated prospective producer and reject any broad/direct source
table writer grant. Closing that boundary cryptographically would require a
separately authorized validator identity or a full server-side validator port;
it must not be approximated by trusting a caller-supplied `VALID` flag or hash.

## Verification gates

Before staging use, all dependency-free selftests, Python compilation,
`tabnanny`, migration checks, and `git diff --check` must pass. A disposable
PGlite run is useful for migration syntax, clean apply/reapply, canonical hash
vectors, and single-session data flow. It is not evidence for native
multi-session role isolation.

The release gate is PostgreSQL 15+ with fresh dedicated logins. It must prove:

- clean `001 -> 051` apply and idempotent reapply;
- fact, seal, selection, outcome replay, and evaluation round trips;
- rejection of forged facts, subsets, route flags, recomputed hashes, and
  append-only mutations;
- rejection of future source reads and evaluation before the latest selected
  representative's fixed 60-minute horizon;
- exact role grants, absence of writer cross-capabilities, no schema `CREATE`,
  and reader write rejection;
- no Stage-8 or generic runtime role can insert, update, or delete prospective
  anchor source rows outside the validated upstream producer;
- qualification failure with four distinct parents or with fewer than five
  valid parents in either individual route;
- no LIVE, Telegram, outbox, delivery, or trading authority.

## Rollout and real evidence clock

Deployment is a separate, explicitly approved operation. The safe order is:

1. create roles and staging credentials;
2. apply migration `051` in staging;
3. run the native PostgreSQL gate;
4. register selected exact bindings;
5. run Shadow and persistence in observation-only mode;
6. review append-only receipts and resource bounds;
7. separately approve any production migration.

Production evidence starts only at the database-issued freeze. Tests may use
an explicitly marked, administrator-created historical fixture in a disposable
schema to avoid waiting for the wall clock, but production must never backdate
the freeze. Consequently the engineering path can finish immediately, while a
positive real research result still requires at least five genuinely distinct
post-freeze BTC parent movements and enough elapsed market time for the exact
60-minute outcome cells to close.
