# Stage-8 durable outcome DB adapter

`research_stage8_outcome_db_adapter.py` is the read-only bridge between one
durably stored, outcome-blind Stage-8 selection and the frozen acceptance
evaluator. Its output is a caller-side diagnostic and a closed persistence
request, never an authoritative research qualification. It is intentionally
not a worker or a deployment entrypoint.

## Public API

```python
evaluate_selection_outcomes_from_connection(
    conn,
    exact_binding,
    *,
    selection_record_sha256,
) -> dict
```

The caller supplies an already-open mapping-row PostgreSQL connection in a
read-only `REPEATABLE READ` transaction. Both `current_user` and `session_user`
must be the dedicated `research_stage8_reader_v1` role. Its search path must
resolve the Stage-8 and outcome relations in one trusted schema, followed by
`pg_catalog` and only then any `pg_temp` schema; the reader must not have
`CREATE` on that trusted schema. The database session `TimeZone` must be
exactly `UTC`, so PostgreSQL's JSON representation of `timestamptz` rows is
stable across the outcome reader and persistence validator. The only lookup
identity is the durable
`selection_record_sha256`. There is no parameter for representatives,
outcomes, excursions, source hashes, receipts, database URLs, Telegram, LIVE,
or trading state.

## Fixed same-snapshot read plan

One invocation performs exactly fifteen bounded `SELECT` statements. Seven
belong to the outcome envelope and eight are the frozen projection adapter's
full-population replay:

1. Read and validate the server transaction identity.
2. Read the exact selection and its exact-binding registry row.
3. Read the referenced fact batch and completeness seal.
4. Read the complete sealed fact population (maximum 1,000 facts), left-joined
   to immutable events.
5. Re-read all original attempt, slot, direction-event, Watch archive, BTC
   parent, and BTC-bar sources for the batch's exact attempt IDs. Parent
   membership is reconstructed directly from the event time, containing parent
   interval, and latest causal bar; the legacy materialized membership table is
   audit-only.
6. Regenerate exactly one binding fact per attempt and compare the complete
   sealed fact population by causal semantics.
7. Read the exact Ordered First Touch V7 cell for every stored representative.
8. Read the exact common-window result for every stored representative.
9. Re-read the transaction identity and require the same backend, start time,
   and PostgreSQL snapshot.

The shared transaction identity is
`research_operational_score_source_audit.transaction_identity_from_fields`.
The adapter rejects autocommit, non-read-only transactions, isolation other
than `REPEATABLE READ`, statement timeouts above ten seconds or disabled, a
wrong/elevated session role, an unsafe or shadowed relation search path, a
changed snapshot, source-code drift during the read, more than 1,000 facts,
more than 1,000 representatives, more than fifteen queries, or more than thirty
seconds of local wall time. The exact ten-file source manifest is sampled both
before the first query and after the final query; the two samples must match.

## Reconstruction and evidence rules

Representatives are reconstructed only from
`research_stage8_selection_read_v1.representative_identities`.  The adapter
does not rename the selector's predicted identity into authority. It requires
each stored `expected_selection_fact_identity_sha256` to resolve one-to-one to
the database-trigger-derived `selection_fact_identity_sha256`, validates the
closed identity payload and digest, and only then normalizes a diagnostic
acceptance row. Each identity must also resolve to exactly one row in the
sealed fact batch and exactly one matching `research_events` row. The adapter
validates the selection, batch, seal, record hashes, full fact-record
population, fact and Watch hashes, exact event/fact identity, parent evidence,
frozen representative-set digest, and prospective freeze boundary. The full
projected `fact_sha256` remains replay/audit evidence and never becomes the
structural selection identity.

The replay covers every attempt in the sealed batch, including false, unknown,
and noneligible facts—not only selected winners. Fact comparison removes only
transaction-specific Watch query/attestation identity. Parent comparison keeps
the event, directly derived parent ID/start/direction, eligibility, observed
BTC close, exact BTC bar, and price source. Later parent end, confirmation/audit
progress, observed-through, and boundary-reason fields are not mistaken for a
different causal observation. Any causal difference makes replay `UNKNOWN` and
blocks the whole selection.

Probability evidence is read only from the exact
`research_ordered_first_touch_outcomes` primary-key cell:

- event ID;
- 60-minute frozen window;
- exact frozen threshold;
- `ordered-first-touch-v7` method;
- representative direction and measurement start.

Only `SUCCESS` and `FAILURE` are decisive.  `OPEN`, ambiguous same-candle
touch, no-touch `UNRESOLVED`, `DATA_MISSING`, a missing row, a duplicate, or an
identity/source mismatch remains `UNKNOWN` and is never converted to failure.

Asymmetry evidence is read only from the exact
`research_common_window_metrics` primary-key cell.  A usable row must be
`READY`, full fixed-window, observation-closed, prefix/path complete, have the
expected candle count, use finite nonnegative MFE/MAE, match the immutable
event price, and preserve the frozen Spot route.  Binance scopes require the
canonical Binance Spot pair.  HYPE requires Hyperliquid HYPE/USDT Spot with
the explicit `@107` instrument.  A zero MAE is retained as a valid observed
metric but the acceptance evaluator keeps the ratio unavailable; it is never
promoted to infinity.

Each route owns its own valid parent set.  Evidence missing from one route
does not block the other, and parent counts are never borrowed across routes.
The acceptance gate remains exactly:

```text
at least 5 distinct valid btc_parent_movement_id
AND (probability route OR asymmetry route)
```

## Receipt and trust boundary

The result binds the exact transaction identity, local source-file manifest,
registry/selection/batch/seal raw-row hashes, the complete sealed fact set,
the full ordered BTC-parent list, and per-representative event, fact, V7, and
common-window source hashes.  The evidence receipt and whole result have
separate SHA-256 identities.

The caller fact-replay receipt binds the exact attempt population, regenerated fact
and parent semantic hashes, projection source manifest, and shared PostgreSQL
transaction identity. A closed persistence payload embeds the full fact-replay
receipt, full outcome-evidence receipt, full evaluation, their hashes, and the
exact outcome-adapter source hash; a detached caller evidence hash is not an
authority channel.

Every adapter result and its embedded persistence request keep
`research_qualified=false`. A successful Python replay can populate diagnostic
`all_causal_fact_semantics_verified` and `atomic_gate_passed`, but
`durable_selection_server_recomputed`, `authoritative_fact_replay_verified`,
`durable_fact_source_authority_verified`, and
`durable_outcome_atomic_gate_evidence_verified` remain false. The blocker
`SERVER_DB_REPLAY_ATTESTATION_REQUIRED` is always present. Legacy wire-version
names and evidence labels are retained for compatibility; neither their names
nor caller-recomputed hashes confer server authority.

The evaluator writer submits the untouched closed request. The database
trigger independently replays the sealed fact population against its source
rows and recomputes the frozen atomic probability-or-asymmetry gate. Only a
validated readback of the persisted server-derived `evaluation` may report
research qualification. The stored `persistence_payload.evaluation` remains
the original non-qualifying caller diagnostic, with its original hash. It is
not the authoritative evaluation and must not be used in its place.

Even the persisted qualifying result is research-only: it grants no runtime
action. The read-only adapter itself sends and persists nothing.

Every result remains `EXPERIMENTAL_RESEARCH_ONLY`; `live_authorized`,
`telegram_authorized`, and `trade_authorized` are always false.

## Least-privilege database role

`research_stage8_reader_v1` needs schema `USAGE` and `SELECT` only on:

- `research_stage8_registry_read_v1`;
- `research_stage8_selection_read_v1`;
- `research_stage8_fact_batch_read_v1`;
- `research_stage8_fact_read_v1`;
- `research_stage8_fact_seal_read_v1`;
- `research_events`;
- `research_ordered_first_touch_outcomes`;
- `research_common_window_metrics`;
- `research_max_pain_snapshot_sets`;
- `research_prospective_anchor_attempts`;
- `research_prospective_anchor_slots`;
- `research_event_btc_movements`;
- `research_btc_parent_movements`;
- `research_btc_price_bars`.

It must not receive `INSERT`, `UPDATE`, `DELETE`, sequence, function-execution,
outbox, delivery, Telegram, LIVE, or trading privileges for this reader path.

## Verification

`research_stage8_outcome_db_adapter_selftest.py` is the dependency-light,
network-free adversarial suite.  It covers both independent passing routes,
the five-parent floor, exact threshold binding, nondecisive preservation,
duplicate/missing/substituted evidence, causal replay mismatches, permitted
transaction-audit differences, Spot-route enforcement including HYPE `@107`,
zero denominator behavior, transaction drift, closed persistence hashes,
hard-false caller qualification/server-authority flags, rejection of rehashed
self-qualification claims even with five parents, and the absence of a caller
evidence-injection API.

`research_stage8_outcome_db_adapter_postgres_selftest.py` is an optional real
PostgreSQL gate.  It runs only with an explicit local/CI `TEST_DATABASE_URL`
whose database name is visibly test-only, uses the real migrations in a random
disposable schema, persists five post-freeze parent representatives, checks all
fourteen required read grants and absence of write grants, executes the real
fifteen-query path without a mock on a fresh reader-authority session, counts
all fifteen actual connection executions independently, verifies that the
caller diagnostic remains unqualified and only the persisted server-derived
result qualifies for research, mutates a causal parent field to prove that the
persisted server replay and qualification fail closed, and proves that the
reader transaction itself rejects writes.
