# Source-backed no-horizon research and durable local jobs

This continuation builds on the additive engine in
[`NO_HORIZON_RESEARCH_V1.md`](NO_HORIZON_RESEARCH_V1.md). It reads a fixed Watch
source population, reconstructs candidate decisions from the captured inputs,
and persists bounded research computations locally. Production schedules,
database schema, stored labels, registrations and alert delivery are unchanged.

## The source population

A run fixes the source interval, analysis cutoff, symbol, original candidate
definition, base direction, symmetric barrier and exact price route before
evaluating outcomes. The population is accepted Watch intake rows in the
declared interval. It is not every market opportunity, every delivered alert,
or the external V9 strategy ledger.

The PostgreSQL exporter uses a fresh connection and a read-only, repeatable-read
transaction. Source rows and price rows have independent limits. Row and byte
limits cannot silently turn a partial extraction into a complete cohort. No
production database URL is inferred from the service environment.

The adapter validates the captured score bundle and its binding to the immutable
source and intake record. It re-evaluates the unchanged supported candidate
predicates using the existing captured-feature evaluator. Stored match rows and
fixed-window labels do not decide membership in the new experiment. Unknown
features remain distinct from a known non-match, and missing evidence blocks
qualification rather than removing an inconvenient observation.

The current adapter, `no-horizon-accepted-watch-source-v3-maxpain`, reuses
`watch-scan-formulas-v2-maxpain` and
`watch-captured-total-and-maxpain-features-v2`. In addition to the original
three model totals and Israel-weekend feature, it passes the already validated
coin's captured `maxpain_slots` to that existing evaluator. The unchanged catalog
now has 82 supported definitions and 216 unsupported definitions. No source
collector, score calculation, SQL query or migration is added. MaxPain proof and
availability remain directional; incomplete proof is unknown, not a zero score.
The exact fields, direction mapping and version boundaries are documented in
[NO_HORIZON_FEATURE_COVERAGE_V2.md](NO_HORIZON_FEATURE_COVERAGE_V2.md).

### Upstream Flow read failures

On a Flow cache miss, `market_confidence_engine._cached_flow` analyzes Futures
and Spot once each and handles their exceptions separately. A successful
market result is retained unchanged; only the failed market receives the
existing unavailable/`NO_DATA` result and its exception reason. No immediate
retry, expired-data substitution or new score calculation rule is introduced.

If either market raises, the partial result is not cached. A later caller may
make a fresh attempt; it cannot fill or rewrite a snapshot already captured.
When neither market raises, existing cache expiry, invalidation and deep-copy
behavior remain unchanged, including successfully read empty-data results.

This affects operational callers of `_cached_flow` if deployed, including
snapshot capture, `combine` and `attach_to_opportunities`; it is not limited to
offline research. Direct callers of `coinglass_flow_engine.analyze_symbol`
retain that function's existing exception behavior. Missing historical models
remain unavailable, and their corresponding research decisions remain UNKNOWN.
This isolation limits failure propagation; it does not repair a database outage
or prove that the other market would have been available in an old capture.

The explicit source scope currently supports the unambiguous Binance spot
route. HYPE and implicit spot/perpetual or exchange substitutions are excluded.
The new entry price is the actual open of the first full minute at or after
source availability, as defined by the existing no-horizon contract. This does
not model order latency, fees, funding, slippage or actual exchange fills.

BTC parent assignment is causal at the decision time. Parent identity, policy,
confirmation, source and observed coverage must agree with the preceding closed
BTC minute. Missing or unverified parents remain blockers. The earliest matched
decision per parent is chosen before outcomes; another symbol, timestamp or
later winner cannot supply an extra independent case.

Database bindings and canonical hashes establish the checked source lineage;
they are not cryptographic attestations from the exchange. Reading a source
retrospectively does not make the experiment a prospectively registered test.

## Durable computation

The SQLite store owns research snapshots, leases, checkpoints and receipts in a
separate local file. It does not write to the production database. An identical
snapshot and policy can be submitted idempotently. Reusing an explicit job key
with different content is an error.

Claims are transactional and fenced. Reclaiming an expired lease invalidates the
old worker's right to commit; a late worker cannot overwrite newer checkpoints.
Each processing call has explicit entry and candle budgets. Persisted states
resume the remaining price suffix after interruption. A partial cohort cannot
produce a qualifying final receipt.

A snapshot is immutable, including its cutoff and dataset identity. A later
cutoff or changed source creates a new job and receipt. This release does not
silently transfer checkpoints across revised datasets. It supports restart
within a frozen experiment, not a continuously mutating live portfolio.

The export transport version remains `no-horizon-watch-source-export-v1`.
Reusing the same captured export with a newer feature adapter creates new
version-bound research inputs; it does not amend old results. Plans bind the
source/feature versions and repository dependency closure. Old plans must use
their pinned code and cannot resume under the expanded adapter. The original
October experiment stays pinned to `e8d25e57` and is unchanged.

Research eligibility remains separate from runtime authority. Runtime,
Telegram and trading authorization fields remain false in the resulting
receipts.

## Reproducible commands

The exporter requires an explicitly supplied `RESEARCH_NO_HORIZON_READ_DATABASE_URL`.
The connection should use the operator's read-only research credentials. The
exporter additionally enforces a read-only transaction; it never falls back to
the production service's default database variable.

```bash
python research_no_horizon_export.py --source-start 2026-10-01T04:00:00Z --source-end 2026-10-01T12:00:00Z --cutoff 2026-10-01T12:00:00Z --symbol BTC --output source.json
python research_no_horizon_source.py source.json --candidate SPOT_CVD_TOTAL_65 --base-direction LONG --symbol BTC --threshold-pct 0.25 --output snapshot.json
python research_no_horizon_local.py --database research.sqlite submit snapshot.json --job-key btc-source-example
python research_no_horizon_local.py --database research.sqlite run --worker-id local-1 --job-id JOB_ID --candle-budget 1024
python research_no_horizon_local.py --database research.sqlite status JOB_ID
python research_no_horizon_local.py --database research.sqlite receipt JOB_ID --output receipt.json
```

Use the returned job ID. Each `run` invocation performs one bounded batch;
repeat it while the job is pending. `COMPLETE` means the price computation is
complete, not that the formula qualified. The final gate and its blockers are
inside the receipt. `BLOCKED` indicates that the frozen input cannot complete
at least one required price calculation. Receipts are unavailable until the
whole cohort has been processed. Output files are create-only.

The export defaults to at most 128 accepted rows and 64 MiB, with explicit
maximums of 256 rows and 31 days of price observation. These are computation
limits, not an outcome expiry. Exceeding an export limit aborts the extraction.
Separately extracted cohorts cannot be summed as independent evidence without
a distinct combined-cohort and representative-selection contract.

Verification commands:

```bash
python -m unittest research_no_horizon_selftest research_no_horizon_replay_selftest research_no_horizon_source_selftest research_no_horizon_store_selftest research_no_horizon_local_selftest
python research_no_horizon_source_postgres_selftest.py
```

The PostgreSQL tests require the existing explicit local/CI `TEST_DATABASE_URL`.
They create disposable test databases and are skipped without it. A skip is
not PostgreSQL verification. The repository's full CI runs them with PostgreSQL 18.

## Why there is no asymmetry admission shortcut

For ideal first-touch fills at symmetric barriers, a decisive result pays +1R
or -1R. If the win fraction is p, gross mean R is 2p-1 and gross profit factor
is p/(1-p). These are transformations of the same probability, not independent
evidence of a different payoff asymmetry. A lower odds threshold would silently
weaken the probability policy.

Full-window MFE/MAE, completed BTC-wave excursions and dynamic stop/target
strategies use other endpoints or payouts. In a one-minute terminal candle,
the full high and low may also occur after the decisive touch. Those values
cannot supply an honestly equivalent asymmetry-only admission route here.
The existing five-parent probability route is retained; a separately defined
compatible asymmetry route remains explicitly unavailable.

V9's ZEC SL4/TP64 dynamic-stop strategy remains a separate calculation. Its
source archive was not found in the October 1 availability check. No capacity
comparison or V9 result can be inferred from this implementation.

## Boundaries

This is a reviewable research intake and execution path. It does not activate a
Render worker, migrate the production schema, merge the review branch, change
alert settings or execute trades. Production integration remains a separate
versioned adapter and deployment task after this path has been reviewed.

The source extraction, real-data run and CI receipts are kept with the execution
evidence, rather than committing private snapshots to the repository.
# Bounded manifest transport

The source adapter also supports the separately attested
`MANIFEST_ATTESTED_MULTI_READ_V1` mode. Its complete source snapshot is anchored
by one read-only manifest statement; later bounded payload fetches must match
the anchor byte-for-byte. See [the manifest transport contract](NO_HORIZON_MANIFEST_TRANSPORT_V1.md)
for its distinct transaction claims and retained proof requirements.
