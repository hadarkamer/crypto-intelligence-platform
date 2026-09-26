# Stage-8 trusted projection PostgreSQL adapter

Status: local research-only implementation consumed by the disabled-by-default
Shadow and durable registry paths. It is not wired to a production worker,
Telegram, LIVE delivery or trading, and it does not open a database connection
or change a transaction.

## Contract

`research_stage8_projection_db_adapter.py` exposes:

```python
project_attempts_from_connection(
    conn,
    *,
    attempt_ids=[...],          # sorted, unique, positive; at most 32
    max_wall_seconds=30.0,      # may only be reduced
) -> dict

project_exact_binding_attempts_from_connection(
    conn,
    *,
    exact_binding={...},          # one validated frozen binding
    attempt_ids=[...],            # sorted, unique, positive; at most 1,000
    max_wall_seconds=30.0,        # one cumulative bound; may only be reduced
) -> dict
```

The first API retains the small discovery surface and projects every applicable
first-tranche binding. The full-cohort API calls `project_binding_fact` exactly
once for each found attempt and never expands the 432-cell binding family. Its
top-level result, query scope, population receipt and authority receipt all bind
`projection_mode=EXACT_BINDING_FULL_COHORT` and the exact
`exact_binding_sha256`. A missing attempt remains an explicit `UNKNOWN` row with
no invented fact.

`conn` must already be a mapping-row PostgreSQL connection with
`autocommit=False`, `transaction_read_only=on`, `transaction_isolation` equal
to `repeatable read`, and a positive `statement_timeout` no greater than 10
seconds. The adapter neither commits nor rolls back.

The frozen contract manifest is
`5a3ee3af6a73467f3ead09fbe9684a8f60101f97fa7064472b228a31468e6bef`, the
source audit is `operational-score-source-audit-v2`, and the adapter version is
`stage8-projection-postgres-adapter-v1`.

## One bounded snapshot

The full path uses at most eight SELECT statements in one transaction:

1. transaction identity and safety settings;
2. Watch archive high-water, before any Watch payload is inspected;
3. the exact requested attempt IDs;
4. exact v4 slots for those attempts;
5. the slots' exact LONG and SHORT events;
6. direct causal BTC membership from exact-policy parents and source bars for those events;
7. one LATERAL latest-Watch selection per attempt; and
8. the same transaction identity again.

The transaction hash is produced by the source-audit module's shared pure
`transaction_identity_from_fields()` helper, using
`stage8-postgres-transaction-identity-v1`, backend PID, UTC transaction start
(`YYYY-MM-DDTHH:MM:SS.ffffffZ`) and exact PostgreSQL snapshot text. It matches
the bounded coverage reader for the same server transaction. The adapter keeps
read-only, isolation, timeout and observation fields in its transaction output
and validates them separately; hashing that entire wider object is not the
transaction identity protocol.

The Watch query is bounded by the previously read archive high-water. It picks
the latest `WATCH_SHARED` row ordered by
`GREATEST(available_at_utc,created_at_utc) DESC, snapshot_set_id DESC`, provided
both durable timestamps are no later than the anchor decision and the row is
within the frozen 300-second age. Payload validity is deliberately not a
selection predicate: a newer malformed row is selected and fails closed; an
older attractive row is never substituted. A row created after the decision
cannot supply that decision.

Parent selection reads each event directly, chooses its containing exact-policy
BTC parent (`start <= decision < end`, or an open parent), and chooses the latest
BTC bar closed no later than that decision. Ties between containing parents use
descending start time and ascending parent ID. The source bar must be less than
one minute old and must close at or after the parent start. The query constructs
the six canonical `research_btc_parent_movement.membership` fields itself:
eligible parents confirmed by the decision yield `LIVE`; ineligible parents
yield `BOUNDARY_UNVERIFIED`; missing or incompatible evidence yields
`BTC_DATA_MISSING` with no parent ID. A confirmation after the decision also
yields `BTC_DATA_MISSING`, preserving the observed bar timestamp as the canonical
function does. Actual selected parent/bar rows remain available for validation.

The query does not read the materialized event-membership table. Parent identity
therefore does not depend on that table being populated or on outcome existence.

The adapter then builds the exact selection attestation required by
`research_stage8_feature_projection`. The small API projects the whole
applicable first tranche; the full-cohort API projects only its caller-supplied,
validated exact binding. Both independently recompute each fact hash and call
the strict fact validator with out-of-band selection and Watch-code hashes. The
caller cannot supply a fact, receipt or authority hash to either API.

## Output authority

Every found attempt is retained. In first-tranche mode its `fact_ledger`
contains every projected binding; in exact-binding mode it contains exactly one
entry. Neither mode returns winners only:

- the full projected fact and `exact_binding_sha256`;
- an independently recomputed `expected_fact_sha256`;
- the database-built Watch selection attestation and expected hash;
- the exact Watch scoring-code manifest and expected hash;
- direction-specific raw event/membership/parent/BTC-bar source;
- canonical parent-membership evidence and expected hash; or, only for a
  genuinely `UNEVALUABLE`/`COVERAGE_EXCLUDED` attempt, the exact noneligibility
  proof and expected hash.

Missing or invalid parent material is hashed as `UNKNOWN`; it is not called a
noneligible parent and must block representative selection. Missing attempt IDs
are also retained as explicit `UNKNOWN` rows with no fabricated fact.

The top-level source manifest exposes separate hashes for the projection
module, adapter module and parent-evidence implementation. Transaction ID,
PostgreSQL snapshot ID, archive high-water, exact requested/found/missing IDs,
attempt-population hash and the complete outcome-free authority-ledger hash are
bound into `exact_attempt_population_receipt_sha256`. `truncated` is always
false on success.

This exact-ID receipt is not the coverage reader's
`outcome_free_population_receipt_sha256`. Registry integration must validate
and bind that separate coverage receipt to the same exact IDs and attempt
population. It must never substitute the full coverage receipt hash, because
that full receipt also describes label/outcome coverage.

## Explicit exclusions and limits

- No outcome or materialized event-membership table appears in adapter SQL. Labels, MFE/MAE, qualification and
  representative choice are not read or inferred.
- Exact-list completeness is not corpus completeness. The upstream bounded
  coverage reader remains responsible for constructing the complete cohort.
- Hashes are deterministic integrity bindings, not signatures. A durable
  registry must compare them with independently frozen implementation and
  producer-code expectations.
- The monotonic wall deadline is cumulative across reads, projection work and
  final receipt construction; it is checked around every query and every
  attempt. The required PostgreSQL `statement_timeout` separately bounds a
  query that does not return promptly.
- No automatic retry, fallback capture, connection discovery, migration apply,
  persistence or runtime side effect is present.

## Verification

The network-free suite covers SELECT-only SQL inspection, outcome-table
exclusion, query/high-water order, exact population retention, hard bounds,
wrong and changing transactions, late persistence, newer-invalid capture
selection, direct causal parent membership, unchanged parent/selection identities
with missing or conflicting materialized memberships and outcomes, parent-source
UNKNOWN handling, non-evaluable proofs, forged
self-rehashed facts, exact-binding mismatch, and 33/1,000-ID full-cohort inputs
without first-tranche expansion. A separate opt-in test applies migrations
001--021 to a random disposable schema and exercises both projection modes on a
full EVALUABLE graph using real PostgreSQL wire semantics.
