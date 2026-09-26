# Stage 8 bounded source-coverage receipt

`research_stage8_coverage_receipt.py` is the opt-in, count-only runner for the
Stage-8 operational-score source audit. It is not a formula search or an
acceptance report. It never discovers a database URL, runs migrations, repairs
records, persists a cursor, prints source payloads, or reports score values.

## Required explicit configuration

Use a staging database or read replica with a dedicated SELECT-only role. The
role needs schema `USAGE` and `SELECT` on the eight source tables listed in the
[source-audit contract](2026-09-13-operational-score-source-audit.md). Supply
only these environment variables:

```text
RESEARCH_STAGE8_AUDIT_DATABASE_URL
RESEARCH_STAGE8_AUDIT_START_UTC
RESEARCH_STAGE8_AUDIT_END_UTC
```

The database value must itself declare exactly one `host` and `port`, plus
`dbname`, `user`, `password` and a non-fallback `sslmode`. `service`,
`servicefile`, `passfile`, multi-host targets, partial conninfo and ambient
`PG*` completion are rejected before `connect()`. `verify-ca` and
`verify-full` additionally require an explicit `sslrootcert`; `require` and
an explicit local `disable` are also accepted. This v1 policy prevents libpq
from completing the target, role or password fields; it does not claim to
force the server's authentication method or disable every ambient libpq
transport mechanism.

The bounds must include an explicit UTC offset and describe a predeclared
half-open cohort. The program deliberately does not inspect `DATABASE_URL`,
provider variables, `.env` files or runtime configuration. If any required
value is absent, it emits `UNKNOWN_NOT_QUERIED` with null counts and does not
attempt a connection. That result must never be read as an empty corpus.
Exit status is fail-closed: `0` only for `COMPLETE_BOUNDED_COHORT`, `2` for a
query failure, `3` for `UNKNOWN_NOT_QUERIED`, and `4` for `BOUNDED_PARTIAL`.

Run:

```bash
python research_stage8_coverage_receipt.py
```

The DSN is never included in the JSON receipt. A connection error discloses
only the exception type. PostgreSQL is opened with `autocommit=false`,
`default_transaction_read_only=on`, `REPEATABLE READ`, a five-second connect
timeout, one-second statement timeout, one-second lock timeout and 30-second
idle-in-transaction timeout. The transaction is rolled back after reading.

## Fixed bounds and output

The v1 runner uses the locally hashed Stage-8 definition:

- all eight declared symbols;
- the fixed five-minute maximum Watch-capture age;
- Ordered First Touch v7 at 60 minutes;
- every predeclared threshold from 25 through 200 bps;
- pages of 25 attempts, at most 40 pages and 30 seconds of orchestration time.

The 30 seconds are an orchestration budget, checked before and after every SQL
read by the real source adapter as well as between pages. It is not described
as an exact process-kill deadline: server-side statement execution is
separately capped at one second, while network/client return and bounded local
validation are outside an exact wall-time guarantee. Expiry discards the
unfinished page and yields
`BOUNDED_PARTIAL`, never a count from a half-read page. A complete result is
only `COMPLETE_BOUNDED_COHORT`: completion applies to the specified time range
and high-water snapshot, not all history.

Every returned page must repeat the exact manifest-controlled sampler,
capture, label and BTC-parent versions, the 300-second age, the 60-minute
window and all eight thresholds. Each attempt must contain the complete unique
`LONG|SHORT × 8 thresholds` matrix and both parent-membership slots. Page size,
cursor state, strictly increasing attempt IDs and the final high-water ID are
checked before a receipt can say complete. `manifest_axes_verified` records
that result. Every page also carries a hash of the PostgreSQL backend,
transaction start and snapshot; all hashes must match, so separately acquired
repeatable-read pages cannot be aggregated as one complete cohort. The page
reducer is internal to the runner. A page that finishes after the wall-clock
budget remains partial.

The receipt contains counts for original attempt statuses, anchor authority,
Watch-capture validity, raw and reported outcome states, parent-membership
validity, distinct verified parent IDs, reason codes, and fully valid
anchor/capture/parent/outcome-cell intersections. It omits operational score
values and raw source rows. An outcome-free `attempt_population_sha256` binds
the exact ordered attempt-ID population to its query, high-water and database
transaction without disclosing those IDs. A separate
`outcome_free_population_receipt_sha256` binds only cohort, attempt-status and
transaction fields; representative selection must use this value so label or
outcome changes cannot alter selection identity. `receipt_sha256` binds the
complete audit receipt, including outcome coverage, and is not a selector
input.
Dynamic validator details are stripped from aggregate reason keys. Only
allowlisted stable reason codes are emitted; unknown strings collapse to a
category-level `*_UNRECOGNIZED_REASON`, preventing source text or connection
material from leaking through diagnostics. `last_page_read_started_at_utc` is
the final page's database read-start cutoff, not a query-finish timestamp.

It does **not** determine whether any fixed candidate matched, select a parent
representative, calculate hit rate or asymmetry, establish five independent
candidate occurrences, qualify a formula, or authorize Telegram, LIVE,
deployment or trading. Those remain separate versioned gates.

## Bounded attempt-cohort handoff

`read_bounded_attempt_cohort_from_connection(conn, ...)` accepts the same bounds,
page reader and clock arguments as `run_bounded_audit`. It executes that same
bounded page traversal **once**, retains the exact validated ordered attempt
IDs, and returns them beside the unchanged count-only coverage receipt:

```text
version = stage8-bounded-attempt-cohort-handoff-v1
status
coverage_receipt
attempt_ids
outcome_free_population_receipt_sha256
handoff_sha256
```

The IDs come from the pages that produced the coverage receipt, not a second
query or a reconstruction from counts. They include attempted rows with UNKNOWN
source authority; completeness describes traversal, not evidence eligibility.
Later projection, representative selection and registry checks must retain and
validate that exact population in the same caller-owned transaction.

`validate_attempt_cohort_handoff(value, expected_handoff_sha256=...)` verifies
the full audit receipt separately, then recomputes the outcome-free population
receipt, exact query axes/hash, count, transaction identity, high-water and
`attempt_population_sha256` from the returned IDs. IDs are strictly increasing,
unique positive int64 values; no subset, reordered population or duplicate can
match the original population hash. The optional expected hash pins a trusted
previous handoff. Hashes are consistency checks, not authentication of a caller
that invents and rehashes every input; preserve the trusted runner provenance.

The handoff identity hashes only its version, the outcome-free population
receipt hash and attempt IDs. It does **not** hash the full `receipt_sha256`,
outcome counts, label results, candidate values or raw rows. Changing only
outcome audit metadata can change the full audit hash without changing the
handoff identity. No raw outcomes or scores are added to either output.

`BOUNDED_PARTIAL`, deadline exhaustion, non-atomic snapshots or an unknown
cohort cannot produce a usable handoff: IDs and `handoff_sha256` are null and
the validator rejects it. An empty but complete cohort can be identified, but
cannot supply formula evidence. The existing `run_bounded_audit` API and its
count-only receipt/hash are unchanged; its output still contains no attempt IDs.

Focused test:

```bash
python research_stage8_coverage_receipt_selftest.py
```
