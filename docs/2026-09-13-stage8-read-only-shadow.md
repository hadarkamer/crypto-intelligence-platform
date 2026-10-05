# Stage-8 read-only Shadow coordinator

`research_stage8_shadow.py` is an intentionally incomplete, disabled-by-default
coordinator.  It can prove that the read side of the Stage-8 chain fits together;
it cannot persist a selection, read outcomes, qualify research, deliver an alert,
or trade.

## Default and authority boundary

Importing the module loads only Python's standard library.  Calling `main()` or
`run_shadow_from_environment()` without the exact opt-in token below returns
`DISABLED` before loading Stage-8 modules, opening a database session, or using a
network dependency.  The coordinator module contains no database driver and no
Telegram, LIVE, outbox, delivery, trading, or writer interface.  Its concrete
PostgreSQL dependency module is imported only after exact opt-in and complete
configuration.

Every result hard-codes these values to `false`:

- `research_qualified`
- `selection_persisted`
- `durable_outcome_evidence_verified`
- `telegram_authorized`
- `live_authorized`
- `trade_authorized`
- `outbox_authorized`
- `persistence_authorized`

Selection persistence must be a separate append-only command with its own role.
Only after that command has persisted and independently re-read the exact batch
may a separate trusted, DB-owned outcome reader be considered.  The Shadow
coordinator has neither operation.

## Exact configuration

All fields are mandatory.  There are no aliases and no fallback to `DATABASE_URL`,
`RESEARCH_DATABASE_URL`, or another runtime DSN.

| Environment variable | Exact requirement |
| --- | --- |
| `RESEARCH_STAGE8_SHADOW_ENABLED` | literal `TRUE`; whitespace/case/truthy coercion is rejected |
| `RESEARCH_STAGE8_SHADOW_MODE` | `READ_ONLY` |
| `RESEARCH_STAGE8_SHADOW_READER_DATABASE_URL` | dedicated caller-supplied reader DSN with explicit host, port, database, reader user, password and SSL mode |
| `RESEARCH_STAGE8_SHADOW_DATABASE_TARGET_SHA256` | SHA-256 of the exact DSN bytes |
| `RESEARCH_STAGE8_SHADOW_EXPECTED_ROLE` | `research_stage8_reader_v1` |
| `RESEARCH_STAGE8_SHADOW_SCOPE_ID` | one frozen Stage-8 scope ID |
| `RESEARCH_STAGE8_SHADOW_CANDIDATE_ID` | one frozen candidate ID |
| `RESEARCH_STAGE8_SHADOW_THRESHOLD_BPS` | canonical base-10 integer text |
| `RESEARCH_STAGE8_SHADOW_START_UTC` | canonical UTC with six fractional digits and `Z` |
| `RESEARCH_STAGE8_SHADOW_END_UTC` | canonical UTC with six fractional digits and `Z`, after start |
| `RESEARCH_STAGE8_SHADOW_MAX_ATTEMPTS` | canonical integer in `1..1000` |

Any truthy `RESEARCH_STAGE8_SHADOW_{TELEGRAM,LIVE,TRADING,OUTBOX,PERSIST}_ENABLED`
blocks the run before a session is opened.  Other process-level delivery or
trading variables cannot grant authority because there is no corresponding
method or code path and every authorization result is overwritten to `false`.

## Injected read-only interface

The coordinator consumes an object implementing only:

```python
open_read_only_session(
    *, database_url, expected_role, database_target_sha256
) -> context_manager
verify_read_only_session(session) -> mapping
registry_reference_from_connection(session, exact_binding) -> mapping
read_bounded_attempt_cohort_from_connection(
    session, *, start_utc, end_utc, symbols, page_size, max_pages
) -> mapping
project_exact_binding_attempts_from_connection(
    session, *, exact_binding, attempt_ids
) -> mapping
```

Tests and controlled callers may inject that Protocol directly.  When an enabled
caller supplies no object, the coordinator lazy-loads
`research_stage8_shadow_postgres.build_dependencies()`; a missing driver or
runtime contract returns sanitized `DEPENDENCIES_UNAVAILABLE`.  This fallback is
not reached on the default disabled path and never searches another DSN variable.

## Concrete PostgreSQL boundary

`research_stage8_shadow_postgres.py` imports only standard-library modules at
module import time.  Its explicit factory then lazy-loads psycopg and the trusted
registry, coverage, projection-adapter and transaction-identity modules.  The
factory itself opens no connection and reads no environment variable.

Before connecting, the implementation verifies the SHA-256 of the exact DSN and
requires one explicit target: non-empty `host`, `port`, `dbname`,
`user=research_stage8_reader_v1`, `password`, and `sslmode`.  Multi-host targets,
service/passfile/options indirection, implicit TLS policy and unrecognized
connection fields are rejected.  Verified TLS modes additionally require an
explicit CA path.

The context manager opens exactly one dict-row psycopg connection with:

- autocommit off and `READ ONLY REPEATABLE READ`;
- a five-second connection timeout;
- a statement timeout no greater than ten seconds; and
- a lock timeout no greater than one second.

It verifies the actual database role, transaction mode, timeouts, backend PID,
transaction start and snapshot before allowing any delegated read.  The only
delegates are `registry_reference_from_connection(...)`,
`read_bounded_attempt_cohort_from_connection(...)`, and
`project_exact_binding_attempts_from_connection(...)`, all on that same
connection.  Exit always attempts rollback and close, including exception paths.
There is no commit method or write/evaluation/delivery delegate.

The session attestation must prove the exact target fingerprint, exact reader
role and read-only `REPEATABLE READ`.  It also carries the exact `backend_pid`,
canonical transaction start, and database snapshot ID.  The coordinator
recomputes `transaction_identity_sha256` with
`transaction_identity_from_fields(...)`, requires the coverage and adapter
receipts to use that same identity, and re-verifies the unchanged attestation
after every read.

The coverage dependency must return the public bounded-cohort handoff with status
`COMPLETE_BOUNDED_COHORT`.  The coordinator invokes
`validate_attempt_cohort_handoff(...)`, then consumes its sorted exact attempt
IDs, full audit receipt, outcome-free population receipt hash, and handoff hash.
It does not infer IDs from counts or private implementation details.

The projection dependency is called exactly once for the entire cohort and one
exact binding.  Both sides support `1..1000` attempts, so a cohort is never
chunked, truncated, or cherry-picked.  The adapter result is accepted only when
all envelopes and both self-hashed receipts declare
`EXACT_BINDING_FULL_COHORT`, carry the same exact-binding hash, preserve the
exact requested/found/missing-ID partition, use at most eight bounded queries,
and bind the unchanged transaction, snapshot, archive high-water, source-code
manifest, Watch observation, facts, and parent evidence.  A found attempt must
have exactly one exact-binding fact and authority ledger.  A missing attempt
must remain an explicit authority-free `UNKNOWN` row.

## Read chain and deliberate stop

For a complete cohort of up to 1,000 attempts, the chain is:

1. verify read-only repeatable-read session;
2. read and verify the durable exact-binding registry reference;
3. read the outcome-free complete cohort and its exact IDs;
4. read the full-cohort exact-binding adapter facts, Watch authority, and parent
   evidence in one bounded call;
5. run the pure outcome-blind representative selector;
6. re-verify the same read transaction and close it.

A complete selector result stops at
`AWAITING_SEPARATE_SELECTION_PERSISTENCE` and includes an internal
`persistence_package`.  The package preserves, without re-reading:

- the verified registry reference and exact binding;
- the complete coverage receipt and public cohort handoff;
- the exact adapter result, population receipt, and authority receipt; and
- the selector result and representative-set evidence.

The pre-persistence selector carries only predicted
`expected_selection_fact_identity_sha256` values.  Each is derived from the
closed causal selection identity (binding, attempt/source identity, exact
anchor event, candidate-match state, and canonical parent authority); the full
projected-fact hash and free-form reason/metadata fields remain audit-only and
cannot steer the representative set.  The selector's
`selection_attestation_sha256` is exactly the hash of its closed structural
batch, not acceptance provenance and not a claim that PostgreSQL verified it.
The separate writer must first persist and seal the facts, let the database
derive its authoritative `selection_fact_identity_sha256` values from linked
source rows, and require equality for the entire attempt ledger and every
representative.  Acceptance provenance can be created only after that durable
readback; Shadow never creates it.

`validate_persistence_package(...)` verifies the full transport hash, a separate
outcome-free identity hash, and the cross-layer binding/receipt/attempt links.
The full coverage receipt remains audit-only: changing outcome aggregate counts
can change the package's transport hash, but cannot change its outcome-free
identity or representative selection.  The package grants no write authority;
only a separately authorized append-only writer may consume it.

Any partial handoff, missing adapter row, unknown fact or membership, incomplete
authority ledger, transaction/binding mismatch, or selector blocker yields
`BLOCKED_UNKNOWN_OR_INCOMPLETE_EVIDENCE` (or a sanitized
`READ_ONLY_CHAIN_FAILED`) and no persistence package.  A complete empty cohort is
also blocked rather than treated as evidence.  Errors never copy the DSN into a
receipt.

This component still does not make Stage 8 runnable in production.  It performs
no persistence and has no trusted outcome-reader API.  Consequently
`research_qualified` and durable outcome verification remain hard-false even
after a successful read-only Shadow run.

The network-free suites cover the disabled import path, concrete factory wiring,
strict DSN parsing, role/timeout/transaction validation, rollback/close, exact
delegation, 33/1,000-attempt cohorts and mismatch/unknown failures.  An optional
real-PostgreSQL boundary test is gated solely by
`TEST_STAGE8_SHADOW_READER_DATABASE_URL`; it requires a visibly test-only local
database and the dedicated reader login, and verifies that PostgreSQL itself
rejects a write inside the Shadow transaction.
