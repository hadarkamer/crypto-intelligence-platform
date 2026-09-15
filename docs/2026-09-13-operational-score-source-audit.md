# Stage 8: operational score source audit preparation

This stage prepares a bounded, read-only audit of prospective neutral anchor
attempts and the separately captured Watch operational scores. It does not run
the audit against a database by default, connect a new runtime consumer, or
change scoring, capture, delivery, formula search, qualification or deployment.
It follows the [Stage 7 capture contract](2026-09-13-watch-operational-score-capture.md).

The implementation is `research_operational_score_source_audit.py`. Its pure
validators inspect supplied records without database or network access. The
connection adapter is an explicit opt-in read path:

```python
audit_anchor_attempt_page_from_connection(
    conn,
    *,
    symbols,
    start_utc,
    end_utc,
    max_capture_age_seconds,
    windows=(60, 240, 720, 1440),
    thresholds_bps=(25, 50, 75, 100, 125, 150, 175, 200),
    page_size=100,
    cursor=None,
    absolute_deadline_monotonic=None,
    monotonic=time.monotonic,
)
```

The caller owns the connection and transaction. They must be read-only; the
adapter checks `transaction_read_only` and the transaction isolation level.
It does not establish permission to access a production database, change
connection settings, write results, run migrations or repair source records.
The connection must return mapping rows, for example with
`psycopg.rows.dict_row`; the default tuple row format fails fast.

Supply an explicit nonempty Top8 symbol list, timezone-aware start/end values,
a positive finite maximum capture age, and a page size from 1 to 100. Requested
windows and thresholds must be nonempty, unique subsets of the supported
defaults. The time range is half-open, `[start_utc, end_utc)`, and filters
`source_candle_open_utc`, not decision time; failed attempts without a decision
time remain in scope.

## Cohort and exact anchor evidence

The audit starts from `research_prospective_anchor_attempts`, restricted to
`prospective-neutral-anchor-v4-decision-features-frozen` and the caller's
explicit symbol/time cohort. It retains `COVERAGE_EXCLUDED` and `UNEVALUABLE`
attempts as well as `EVALUABLE` attempts. Starting from successful slots or
complete labels would conceal source failures and change the denominator.
Attempts are not independent observations and repeated attempts are not new
market waves.

An `EVALUABLE` attempt is inspected against its exact slot and the slot's exact
LONG/SHORT event pair. Matching only a symbol, nearby time, or one event is not
sufficient. The immutable sampler, source-slot, decision-time, input and
feature-bundle identities must agree. The decision feature bundle is the
slot-owned frozen bundle, with its existing policy and digest checked; it is
not reconstructed from current inputs or copied into an old event.

This audit requires the original bundle's `model_score_status=ABSENT`. That is
an explicit statement that operational model scores were not part of the
original frozen decision features. A separately joined Watch capture does not
change that status, make the original bundle complete in another sense, or
retroactively turn the capture into an original decision input. Missing or
inconsistent pair/bundle evidence remains a reported validation failure.

## Operational capture selection precedes validation

For each eligible decision-time lookup, select the latest `WATCH_SHARED`
archive row that is durably prior to the decision and within the caller's
explicit `max_capture_age_seconds` bound. Selection does not require an
operational block to exist or to declare any particular version or status.
Durable prior availability requires:

```text
max(available_at_utc, created_at_utc) <= decision_time_utc
```

The inner `computed_at_utc` cannot substitute for durable availability. Rank
rows by this durable timestamp, then by `snapshot_set_id`, both descending.
Selection is made without filtering on block presence, version, capture status,
validity, score, model availability, completeness or later outcome. Require
and validate `watch-operational-scores-v2` only after selecting the archive row.
If the latest qualifying row has no block, a wrong-version block, or a failed,
malformed, unavailable or invalid block, preserve that result; do not silently
fall back to an older attractive or valid score. Missing or too-old archive
coverage is also explicit. v1 blocks are not promoted to v2 or rewritten.

The audit verifies the inner operational block digest using the v2
`json-integer-float-zero-normalized-v1` hash representation. The parent archive
identity and outer payload hash are retained as references only: this narrow
read does not load and re-hash the complete outer archive payload, so it must
not report the outer payload as cryptographically verified.

Validation also requires the exact top-level code-file manifest and SHA-256
shapes, a nonempty matching cycle identity, an input-universe hash, and the
exact liquidated-source-side semantics. A `COMPLETE` block must account for
at least 56 pre-display input rows. Available models and members require their
source-window references and timestamps; numerical scores alone do not meet
the source contract.

Read model availability, capture status, source-time errors, freshness and
source identities separately from numerical scores. An unavailable fallback
zero is not an observed neutral score. Preserve the observed HYPE operational
source identity and its distinction from the official research price overlay;
a recorded Bybit/PERP or fallback identity is not forced to Spot. A join does
not establish price-policy compatibility.

## Outcome coverage and BTC source evidence

Left-join every requested Ordered First Touch v7 direction/window/threshold
cell for the exact paired events. The defaults are two directions, four
windows and eight thresholds: 64 requested cells per attempt, including
explicit unknown cells when event authority is missing. A missing cell
remains missing; it is not dropped, treated as zero, relabelled as failure or
replaced with another outcome method. `source_status` preserves the raw stored
status, while `reported_status` follows the canonical v7 interpretation; for
example, a stored `UNRESOLVED` may report `AMBIGUOUS` or `NO_TOUCH` according to
its diagnostics. Missing labels report `UNKNOWN`, not `OPEN` or failure.
Nondecisive `OPEN`, `AMBIGUOUS` and `NO_TOUCH` cells retain explicit canonical
reasons and an `UNKNOWN` audit status, not structural `VALID` status. Outcome
rows require `created_at_utc <= updated_at_utc <=` the page's audit read-start
cutoff, and observed/decision times no later than `updated_at_utc`. Creation
must not predate the event or measurement start, but may precede the eventual
terminal observation. Later, missing or inconsistent timestamps remain
`UNKNOWN`. Source text must contain exactly three ordered, nonempty segments:
`reference|path|provenance`. Extra, unknown or malformed segments are rejected.
Outcome completeness does not decide whether the attempt belongs in the audit
cohort or which operational capture is selected.

HYPE's v7 path text does not identify an instrument. The audit therefore
requires persisted `calculation_audit.price_provenance.instrument` identifying
`@107`; without that evidence the cell remains `UNKNOWN`. It does not infer or
invent the instrument from path text, the symbol, or operational capture data.

Inspect each event's exact BTC membership record under
`btc-parent-close-reversal-200bps-v1`, its exact referenced parent under that
same policy, and the exact archived BTC bar identified by
`btc_observed_close_utc`. Check the immutable decision time, causal parent
interval, membership/parent eligibility agreement and closed-bar freshness;
do not substitute a nearby bar, another parent, or a time bucket. The source
remains `BINANCE_SPOT_BTCUSDT_1M`. Missing, unverified or inconsistent evidence
is visible rather than converted into LIVE membership. See the
[BTC parent movement policy](BTC_PARENT_MOVEMENT_POLICY.md).

## Bounded pagination and snapshot limits

The adapter uses high-water-bounded keyset pagination, not an unbounded cohort
load or offset pagination. Its cursor is bound to the exact cohort and query
specification; it is not reusable with changed symbols, time bounds, capture
age, page size or requested outcome cells. Pages traverse `attempt_id` in
ascending order. The high-water bound limits the attempt
population traversed. It does not freeze joined records or guarantee an
immutable view of later page calls.
Duplicate database projection rows fail explicitly with `ValueError`; the
adapter does not silently choose one duplicate as authoritative.

Snapshot claims must match the actual connection state. Under statement-level
isolation, statements can see different committed data. A non-autocommit
`REPEATABLE READ` or `SERIALIZABLE` read-only transaction can provide a stable
transaction snapshot while it remains open. The adapter does not claim that
separate page calls share a snapshot merely because they share a cursor. The
caller must preserve and disclose the actual transaction boundary if a
single-snapshot audit is required.

When the optional absolute monotonic deadline is supplied, it is checked
immediately before and after every SQL read. Expiry raises
`AuditDeadlineExceeded` and returns no half-built page. This is a cooperative
between-query boundary, not a query-cancellation facility; the caller must
separately cap each in-flight statement (the coverage runner uses one second).

The returned page exposes `scope`, `rows`, `high_water_attempt_id`,
`next_cursor`, `snapshot_consistency`, `transaction_isolation` and
`read_started_at_utc`. It also exposes `transaction_identity_sha256`, a hash
of backend, transaction start and PostgreSQL snapshot that a caller can use to
reject pages from different transactions; `cross_page_snapshot_guaranteed`
is always false because the standalone adapter does not own later calls.
Coverage and projection both use the pure
`transaction_identity_from_fields()` helper in this module. Its exact hashed
payload is `{version, backend_pid, transaction_started_at_utc,
database_snapshot_id}`, with version `stage8-postgres-transaction-identity-v1`.
The timestamp is normalized to UTC with six fractional digits and `Z`; the
snapshot is canonical PostgreSQL `xmin:xmax:xip-list` text. Missing or invalid
fields fail closed. Read-only status, isolation, timeout and observation time
remain separately checked metadata and are not alternative transaction hashes.
The returned helper dictionary adds `transaction_identity_sha256` to that
payload. Callers must use this common definition when comparing reader receipts,
not hash the projection adapter's wider transaction metadata object.
Each row retains the original `attempt`, separate `anchor_authority` and
`capture` results, all `outcome_cells`, and `parent_memberships` by direction.
`delivery_state` is `UNKNOWN_NOT_AUDITED`.

`examined` and `emitted` both count the consumed, returned attempts and exclude
lookahead.
`has_more` means a `next_cursor` is present.
`population_page_complete` means there is no next cursor and this bounded
high-water traversal has reached its end. It does not establish complete
historical population coverage, complete joined evidence or snapshot
consistency.

## Checks and interpretation boundary

Run the focused, local preparation check with:

```bash
python research_operational_score_source_audit_selftest.py
```

The self-test exercises supplied records and the adapter contract; it is not a
production coverage report. A successful test does not establish that the
requested real-world cohort is present, complete or usable. Default preparation
does not run database queries or wire the adapter into runtime workers.

Audit findings describe source identity, chronology, validity and coverage
only. They do not establish Telegram delivery, statistical independence,
LONG/SHORT asymmetry, formula qualification, statistical acceptance or approval
to trade, publish or deploy. There is no formula search, model optimization,
score replay, historical rewrite or deployment in this stage.
