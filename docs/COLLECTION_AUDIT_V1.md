# Bounded collection audit

`research_no_horizon_collection_audit.py` diagnoses the exact accepted Watch
population and two distinct BTC minute archives required by the frozen research.
It cannot acquire a preregistered cohort, evaluate a formula, rank candidates,
write either database, or authorize trading. It does not alter the declaration.

Run `python research_no_horizon_collection_audit.py --declaration declaration.json`
to print the read-only transaction and metadata SELECT. To execute using psycopg,
set `AUDIT_SOURCE_DATABASE_URL` outside command history and add `--execute`.
`--as-of` may narrow the observation bound; the database clock independently caps
it. The CLI emits JSON to stdout, so the caller owns durable evidence storage.

## Meaning of the results

- Price coverage counts every closed minute in a half-open interval. The current
  open minute is excluded. Missing minutes, present but invalid minutes, and
  duplicates are distinct. Gap samples contain unusable intervals, and only the
  first 32 are printed; longest gap and gap count use the whole bounded interval.
- The archive route is fixed to Binance BTC spot trade candles. The separate
  parent-bar archive also requires its frozen Binance BTC source identity and
  includes the preceding minute needed for causal context.
- Accepted source counts use exactly the original consumer/status/usable-time
  predicate, including the original half-open part boundaries. Each part reads
  at most its original cap plus one. Overflow counts are lower bounds. The
  aggregate also checks the unchanged source-times-scope decision budget.
- Source metadata shape/hash-field consistency is diagnostic, not a full replay
  of the raw bundle verifier. No expected scan cadence is present in a frozen
  declaration, so source continuity cannot be established from a row count.
- A complete current minute grid is not complete future observation. Future
  source time remains explicitly unobserved. Full source readiness is always
  `UNKNOWN_METADATA_AUDIT_ONLY`; originals must still pass acquisition validation.
- Current rows cannot reveal historical revisions or failed revision attempts.
  Immutable archive guards are defined in migration 044, not attested by this
  report. The report makes no claim about a worker's status.

## Bounded workload and existing indexes

The declaration span is limited to eight days. Archive input has a defensive
23,045-row limit per table, including a sentinel; this is above the valid minute
grid to expose corrupted duplicates without an unbounded response. Source parts
use their frozen limits (at most 257 rows per part). Query responses are limited
to eight MiB. A read-only transaction, 15-second statement timeout, and one-second
lock timeout are established before reading source data.

`research_price_archive_bars` uses the `(route,symbol,open_time_utc)` primary key
from migration 044. `research_btc_price_bars` uses its time primary key from 021.
Accepted intakes use `(consumer_version,usable_from_utc DESC)` with the accepted
status predicate from 052. The metadata join is by snapshot-set primary key;
intake state is by consumer primary key. Time filters compare bare indexed
columns against fixed bounds, rather than wrapping those columns in functions.

Before a first live run, an operator can run `EXPLAIN (FORMAT JSON)` on the one
SELECT inside the same read-only/time-limited transaction. Do not use unbounded
`EXPLAIN ANALYZE` or create indexes through the audit. If the expected index is
missing or a timeout occurs, retain that failure and fix deployment readiness
separately. Timeouts never yield a completeness assertion.

Run `python research_no_horizon_collection_audit_selftest.py` for gap math,
closed-minute boundaries, frozen caps, identity bindings, and executor ordering.
The SQL itself additionally requires execution against an isolated PostgreSQL
compatible fixture; these unit tests alone do not claim live SQL acceptance.
