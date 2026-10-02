# Global cohort outcomes and local resume v1

This layer executes the globally selected representatives from one
[single-anchor multipart cohort](NO_HORIZON_MULTIPART_COHORT_V1.md).
It creates one research job per declared **global scope**, not per transport
part. The existing first-touch engine, local child store and atomic research
gate are reused unchanged. There is no production worker, migration, network
reader, notification or trading integration.

## Admission and fixed representatives

`research_no_horizon_cohort_outcomes.prepare_cohort_submission(declaration,
anchor, load_part)` freshly validates all anchored exports with
`preflight_cohort`. No supplied coverage or outcome receipt is accepted as a
substitute. All source decisions and causal parent memberships must be complete
across every declared scope. Any `BLOCKED` coverage raises `CohortInputBlocked`
with the complete coverage receipt before a plan or child job is submitted.

Complete `INSUFFICIENT` and empty scopes remain admissible for descriptive
research. They remain in the declared denominator and cannot pass the unchanged
gate without the required resolved-parent outcomes and probability evidence.
Five matched parent groups alone never qualify a formula.

Global coverage selects the earliest decision, then lexical entry ID, per parent
without consuming outcomes. Only those representatives become child entries.
Every raw source decision remains in the global coverage ledger. A later
winning match from the same parent cannot replace an earlier losing, unresolved,
ambiguous or unavailable representative.

## One price series and cutoff

The first part's candle manifest covers the full global source start through the
common cutoff. Every later part must supply exactly its suffix beginning at the
first minute at or after that part's source start. Both content and omitted
timestamps must agree. An inconsistent suffix rejects preparation; the runner
does not fill a gap from a different part or independently captured series.

After global representative selection, the shared series is validated through
the existing OHLC and route checks. Price gaps remain gaps. A gap before first
touch produces missing outcome evidence; a valid terminal prefix is not
invalidated by later gaps. No continuity or price-path filter changes the
selected representative set.

Entry remains the first minute open at or after the decision, including the
same minute when the decision lies exactly on its boundary. The reference price
comes from that actual open. If a selected entry open is missing, its whole
scope is stored as `INPUT_BLOCKED`, with all selected IDs and exact missing
locators retained. No fake price, replacement representative or partial child
job is created for that scope. Other declared scopes remain visible and may run.

Every contract carries global dataset/cohort identities bound to the complete
declaration, common anchor and shared prices. No contract uses a transport part
as an independent experimental cohort. The cutoff never advances during resume;
a changed cutoff or input creates a new plan.

## Frozen preparation and resource bounds

The prepared payload contains the original global coverage receipt, one shared
normalized candle series, every declared scope, selected representatives,
entry errors, and frozen child snapshot specifications and normalized entries.
Raw captured source exports are not duplicated into the coordinator database.
Their exact hashes and anchor binding remain in the retained coverage evidence;
the original exports and raw transport proofs remain external research inputs.

The plan identity binds that entire payload, price hash, coverage receipt,
policy and repository-local implementation closure. The closure includes
preparation, coordinator, source validation, catalog, coverage, contract,
first-touch, gate and child store files. Mandatory implementation roots and the
catalog map must exist. Resume requires the same implementation; historical
reports validate the stored identities without regenerating today's formulas
or representatives.

All multipart source limits still apply. Preparation adds a 256-MiB compact
payload bound and a 256-MiB bound on the sum of expanded child snapshot bytes.
The normalized global price series is at most 44,640 candles. Each global scope
has at most 2,048 parent representatives, within the existing replay limits.
This is a new representative snapshot contract, not a greater-than-256-row
export passed into the single-export adapter or `LocalExperimentStore`.

The child store materializes its own prices per scope. The global expansion
budget counts that repeated input; the one shared preparation array is not a
claim of zero repeated storage or a strict peak-memory cap.

## Durable coordinator and bounded execution

`research_no_horizon_cohort_store.LocalCohortStore` uses a local SQLite file and
new versioned coordinator tables alongside the unchanged `LocalResearchStore`.
Each thread or process owns a separate connection. A future incompatible
coordinator schema rejects before child bootstrap writes.

Submission freezes the entire validated plan before any child job is created.
An optional `cohort_key` binds a durable alias to one exact plan; it cannot be
repointed to changed inputs or policy. Identical submission is idempotent.

`run_cohort(plan_id, worker_id, scope_budget=1, candle_budget=1024,
entry_budget=128, batch_size=128)` schedules global scopes round-robin and
reuses child leases, fencing tokens, atomic checkpoints and immutable receipts.
The candle budget is shared across the scopes attempted by that call. A durable
work record keyed by child job and fencing token records that exact committed
batch. Concurrent workers' later progress cannot inflate the caller's reported
consumption. A lost or expired lease cannot commit stale work.

A crash after child creation but before linking can reuse that exact unclaimed
child. A previously claimed or processed orphan cannot import unaccounted
outcomes into the coordinator. A crash after a committed batch retains that
progress; it does not repeat the same candle prefix on resume.

Integrity checks compare the exact frozen child identity, actual materialized
price table, normalized entries, checkpoint set/counters, source metadata and
final receipt. Foreign children, inserted price rows, altered scope manifests
and rehashed but wrongly bound receipts reject. Verification uses coherent
SQLite read snapshots so legitimate concurrent commits do not create false
integrity failures. These hashes and local triggers detect inconsistency; they
do not authenticate a database origin or defend against an attacker replacing
the entire database and implementation.

## Reports and research meaning

Reports include every declared scope, raw coverage decisions, fixed global
representatives, input errors, job status, completed outcomes and the existing
gate receipt. The gate's selected IDs must equal the frozen representative IDs.
Counts, gates and probabilities are never pooled across scopes or parts.

`declared_scopes` counts the full frozen scope denominator. `outcome_trials_executed`
counts scopes with at least one committed work batch, including a zero-candle
finalization of an empty scope; merely creating or leasing a child does not
count. Input-blocked scopes stay in the declared denominator but have no child
work. This is a denominator for this exact plan, not all prior research attempts.

`OPEN` remains open at cutoff. Same-candle unresolved ordering remains
`AMBIGUOUS`; missing prefixes remain missing evidence. The atomic gate remains
the existing conjunction of enough resolved parent representatives and a
passing compatible metric route. Its current probability policy is unchanged;
the no-horizon asymmetry route remains unavailable. Qualification is descriptive
research evidence, not a profitability or prospective-validation claim.

The embedded coverage receipt remains an outcome-free artifact with its original
`coverage_only` and unavailable-runner flags. Execution state and outcomes live
in the outer coordinator report. Neither layer authorizes runtime, Telegram or
trading. Passing a research gate does not change those false authority fields.

Final child receipts are immutable. A coordinator report contains a hash of its
entire content; a run report also hashes its per-call consumption and attempted
scope fields. Compare final `report` receipts when checking bounded-run versus
uninterrupted parity, since per-call work descriptions intentionally differ.

## Local CLI

```bash
python research_no_horizon_cohort_outcome_cli.py submit \
  --db cohort.sqlite --declaration cohort.json --anchor anchor.json \
  --part part-0.json --part part-1.json \
  --cohort-key cohort-example --output submitted.json

python research_no_horizon_cohort_outcome_cli.py run "$COHORT_PLAN_ID" \
  --db cohort.sqlite --worker-id local-research \
  --scope-budget 2 --candle-budget 2048 --entry-budget 64 --batch-size 128 \
  --output batch-001.json

python research_no_horizon_cohort_outcome_cli.py report "$COHORT_PLAN_ID" \
  --db cohort.sqlite --output final-report.json
```

Use the `plan_id` returned by submission and a new output path for every command.
All outputs are exclusive creations. Existing evidence and input files are not
overwritten. The CLI requires a persistent database and checks run/report schema
and plan existence read-only before mutable store initialization.

Submission completes pure admission before opening SQLite. Unknown source or
parent evidence exits 2 and saves the complete blocked coverage receipt without
creating a database. The direct API constructor may already have created an
empty local schema before `submit_cohort` is called. Structural/file errors exit
1. Valid stored or pending work exits 0. A fully processed plan with incomplete
outcome evidence exits 2 while retaining its diagnostic report. A completed
descriptive plan that fails qualification is still a completed computation.

These commands never connect to PostgreSQL, an exchange or a notification
service. No new historical market experiment is implied by synthetic tests or
by publication of the implementation.
