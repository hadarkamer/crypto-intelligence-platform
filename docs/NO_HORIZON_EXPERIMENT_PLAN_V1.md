# Frozen local exploratory experiment plans v1

This layer applies the existing no-horizon source adapter and local job store to
an explicit list of scopes. It reads an existing source export and runs locally.
It does not extract additional production data, connect to an exchange, change
the PostgreSQL schema, start a production worker, send alerts or execute trades.
See [the source and local job contract](NO_HORIZON_SOURCE_AND_LOCAL_JOBS_V1.md)
and [the outcome contract](NO_HORIZON_RESEARCH_V1.md).

## What is frozen

One plan names one source export, one symbol, one observation cutoff and a
complete explicit scope list. Each scope contains an existing candidate key, a
base direction and a symmetric percentage threshold. The candidate's analysis
direction can differ from its base direction; inverse definitions must retain
both identities.

The plan identity binds the source content, complete scope set, candidate
definitions/catalog, source adapter, effective gate policy and implementation
identities. Reordering the same valid scope set does not create additional
trials. Duplicate equivalent scopes within a plan are rejected, not silently
deduplicated. Changing the export, cutoff, candidate definition, policy or supported
implementation creates a different plan. A named plan key cannot silently move
to a different definition.

The plan is fixed before this runner evaluates its children. This is an
**exploratory trial denominator for this exact plan**, not the total number of
historical research attempts. It cannot prove that the source history or
candidate results were previously unseen. It does not turn retrospective data
into prospective evidence or correct selection across prior experiments.

The scope list is declared explicitly; there is no implicit search of the full
repository catalog. Unsupported declarations are not silently replaced with a
different formula. Unknown or incomplete source decisions remain visible and
cannot be treated as known non-matches.

## Execution and completeness are separate

New submissions first run a whole-plan source/feature preflight. Each accepted
source row is revalidated once using the existing captured-feature and
three-valued predicate evaluator, then accounted for in every declared scope.
Intake acceptance, feature availability and predicate decisibility are reported
separately, including unavailable-feature reasons and original row identities.
The preflight does not build entries, assign parents, replay prices or evaluate
the research gate. Passing it is permission to proceed with those checks, not
evidence of a qualified formula.

The default admission policy, `REQUIRE_DECIDABLE_SOURCE_V1`, blocks the entire
submission if extraction is incomplete or any selected scope has `UNKNOWN` or
`UNKNOWN_SOURCE`. A ready scope does not run ahead of another blocked scope.
Unavailable captured zeros are not valid neutral scores. A known false
conjunct still makes `NO_MATCH` even when another condition is missing; unrelated
missing models do not prevent evaluating a decidable selected predicate.

The pure `preflight_source_features(export, scopes)` API returns a deterministic
receipt with the complete scope list and a receipt hash. `prepare_submission`
raises `SourcePreflightBlocked`, carrying that receipt, before any plan/job
record is inserted. CLI submission checks before opening SQLite, so blocked
submission does not create a database. The class constructor may already have
initialized an empty local schema for a direct API caller.

For explicitly diagnostic historical work only, submission can select
`DIAGNOSTIC_ALLOW_INCOMPLETE_SOURCE_V1` using `allow_incomplete_source=True`
or the CLI's `--allow-incomplete-source`. The policy and complete preflight
receipt are frozen into the plan identity. There is no run-time override.
Source blockers, unknown decisions, partial outcome interpretation and the
existing qualification gate are unchanged. This preserves the ability to
inspect partial historical evidence without silently treating it as complete.

The SQLite schema version is unchanged. Historical reports remain readable;
the existing implementation hash fence still rejects resuming an old plan with
different code. Previously frozen source exports and outcome receipts are not
rewritten by preflight or by the new admission defaults.

## Optional matched-parent coverage before outcomes

`preflight_parent_coverage(export, scopes, gate_policy=None)` in
`research_no_horizon_parent_coverage.py` adds a separate, versioned feasibility
receipt. The existing feature preflight API, receipt and descriptive admission
default remain unchanged. Coverage reuses a freshly computed feature preflight,
then applies the existing parent-evidence and causal-membership checks to each
`MATCH`. It neither looks up entry prices nor evaluates price paths or the gate.
The preceding BTC candle is validated only as evidence of causal membership.

Every exact scope retains all source decisions, including `NO_MATCH`, `UNKNOWN`,
`UNKNOWN_SOURCE` and zero-match scopes. Verified matches are grouped by
`btc_parent_movement_id`; each complete scope records the earliest decision and
then lexical entry ID per group before any outcomes are read. An open parent is
allowed when its causal evidence is valid. Checkpoint advancement alone is not
a contradiction; conflicting causal identity or closing boundaries are.

| Coverage status | Meaning |
|---|---|
| `BLOCKED` | Incomplete source decisions or unverifiable/conflicting matched-parent evidence; the proven upper bound is null and representatives are not selected from the surviving subset |
| `INSUFFICIENT` | Complete decisions and matched-parent evidence, with fewer groups than the effective gate minimum; this exact scope cannot pass the unchanged gate |
| `POSSIBLE` | Complete decisions and at least the required number of groups; entry availability, resolved outcomes, probability, asymmetry and qualification remain untested |

`matched_parent_count` is the number of distinct groups with individually valid
membership records. When blocked it is only a partial diagnostic count; conflicting
groups remain explicitly identified. `matched_parent_upper_bound` is populated
only for a complete scope: resolved earliest representatives can number no more
than these groups. Missing membership is never treated as zero matches, and a
later valid match cannot replace an unverified earlier representative. Missing
parent data on a known `NO_MATCH` is irrelevant to that scope's coverage.

The minimum comes from a complete policy validated with `gate.make_policy`.
The standalone API accepts a different complete, explicitly versioned policy;
plan admission always uses the child runner's existing default policy. Counts
are never pooled across scopes. Neither five groups nor the word `POSSIBLE`
claims statistical independence, good probability/asymmetry or a qualified
formula. The existing atomic gate remains the sole qualification calculation.

The receipt reports both `all_scopes_potentially_sufficient` and
`any_scope_potentially_sufficient`. `ready_for_outcome_research` in this receipt
means the former only. `gate` remains null and `outcome_evaluation` and
`gate_evaluation` remain `NOT_EVALUATED`.

For a plan explicitly intended to require sufficient coverage in **every**
declared scope, use `require_parent_coverage=True` or CLI
`--require-parent-coverage`. This freezes
`REQUIRE_ALL_SCOPES_MATCHED_PARENT_COVERAGE_V1` and the full coverage receipt into
`plan_identity.parent_coverage_admission`. If any scope is blocked or insufficient,
`ParentCoverageBlocked` carries the complete receipt before plan or child writes;
the CLI checks before opening SQLite. This is an optional whole-plan admission
requirement, deliberately stricter than the per-scope research gate. A zero-match
scope does not invalidate another scope's evidence, and no scope is silently
discarded. The existing `allow_incomplete_source` diagnostic option does not
override an explicitly requested coverage requirement.

Omitting the option retains descriptive outcome research on small cohorts. It
does not claim parent coverage was checked, and no parent admission field is
added to the identity. Historical reports remain readable; the existing code
identity fence prevents resuming old plans with changed code. No SQLite schema,
source export, historical result, gate, runtime or trading permission changes.

Every report accounts for every declared scope, including work not yet run.
A partial report is useful for progress, but cannot claim that its completed
subset represents the full search.

| Axis | Meaning |
|---|---|
| Declared population | Every frozen scope has exactly one ledger row; no winners-only export |
| Plan processing | Whether declared work remains unsubmitted, pending or running, or has reached a documented result/blocker |
| Source completeness | Whether the candidate's complete accepted-source population could be validated and evaluated without unknown potentially matching decisions |
| Child computation | Whether matched price paths have a terminal prefix or are caught up through the fixed cutoff |
| Research gate | Whether the existing versioned five-parent probability-or-compatible-asymmetry gate passes |

These axes must not substitute for each other. A child may have finished its
price computation while missing parent evidence blocks its research gate.
Source blockers can exist even when no price opportunities were emitted.
An open outcome caught up through the cutoff is a completed observation at that
cutoff, not a timed-out failure. A missing required price path remains blocked.

An emitted-opportunity count of zero can coexist with unknown source decisions
or missing entry prices; it does not then prove zero matching decisions. Only
valid, complete source processing with no matches supports an evaluated
zero-match claim. Retain source MATCH/NO_MATCH/UNKNOWN counts and blockers even
when no opportunities were emitted. Unsubmitted work, unsupported definitions,
missing sources and unknown predicates are never converted into proven zero
matches. Zero decisive outcomes also differs from zero matched opportunities:
open and ambiguous outcomes remain separately visible.

Child jobs and receipts use their existing content identities. An identical
child can be reused by another plan; it is not a newly independent experiment
or new market observation. A restart resumes persisted progress. A fresh
cutoff requires fresh immutable inputs and children; this version does not
silently extend or rewrite earlier receipts.

## Bounds and commands

The CLI accepts source exports up to the source adapter's 64 MiB bound and a
scope file of at most 64 KiB containing 1–64 scopes. The existing source adapter
also caps an export at 256 accepted rows and 31 days. Those are engineering
limits, not claims about statistical sufficiency or permission to omit rows.
Exceeding a source limit must be resolved with a correctly declared narrower
cohort, not silent truncation.

Plan admission also requires canonical UTF-8 source bytes multiplied by the
normalized scope count to be at most 256 MiB. This bounds the declared expansion
before preparing child snapshots. It is an admission estimate, not an exact
ceiling on SQLite file size: indexed candles, metadata, receipts and journaling
add overhead.

Example `scopes.json`:

```json
[
  {"candidate_key":"FUTURES_CVD_TOTAL_65","base_direction":"LONG","threshold_pct":1.5},
  {"candidate_key":"FUTURES_CVD_TOTAL_65","base_direction":"SHORT","threshold_pct":1.5}
]
```

The source export supplies the symbol and cutoff. These example declarations
do not assert that either scope has evidence or passes its gate.

```bash
python research_no_horizon_experiment_cli.py preflight source.json --scopes scopes.json --output source-preflight.json
python research_no_horizon_experiment_cli.py coverage source.json --scopes scopes.json --output parent-coverage.json
python research_no_horizon_experiment_cli.py --database experiment.sqlite submit source.json --scopes scopes.json --plan-key declared-plan-v1 --preflight-output admitted-source-preflight.json
python research_no_horizon_experiment_cli.py --database coverage-experiment.sqlite submit source.json --scopes scopes.json --plan-key coverage-required-v1 --require-parent-coverage --parent-coverage-output admitted-parent-coverage.json
python research_no_horizon_experiment_cli.py --database experiment.sqlite run PLAN_ID --worker-id local-1 --scope-budget 1 --candle-budget 1024 --entry-budget 128 --batch-size 128
python research_no_horizon_experiment_cli.py --database experiment.sqlite report PLAN_ID --output experiment-report.json
```

`preflight` needs no database and writes its complete report even when source
readiness is false: exit 0 means ready; exit 2 means blocked. Invalid global
inputs also exit 2 without manufacturing a valid report. Existing outputs are
never overwritten. `submit` performs its own single preflight; it does not
trust a caller-supplied readiness receipt. On a source block it prints the full
receipt, optionally saves it with `--preflight-output`, and exits 2 before any
database access. The standalone command is for inspecting source readiness;
it need not be run separately before every `submit`.

`coverage` also needs no database and preserves a complete receipt when blocked
or insufficient. It exits 0 only when every scope is `POSSIBLE`, otherwise 2.
Invalid global inputs exit 2 without a fabricated receipt. Submission computes
its own feature and requested coverage checks once; it never trusts imported
readiness receipts. `--parent-coverage-output` requires
`--require-parent-coverage` and must differ from `--preflight-output`. On a parent
coverage block, the CLI prints the full coverage receipt and can save both the
feature and coverage receipts before returning 2 without database access.

These checks apply to the declared frozen population. They do not authorize
adding observations to an already reviewed cohort until it passes, pooling old
receipts, changing the scope set after results, or exceeding existing input
bounds by splitting one undeclared global cohort into independent trials.

Each `run` is bounded and returns; there is no background daemon. Repeating it
continues persisted work. The candle budget is shared across that invocation's
scope work, not reset to the full allowance for every child. A scope budget
limits orchestration work independently of consumed candles: even a no-match
or blocked scope can require source preparation without consuming any prices.
The runner selects children belonging to this plan rather than draining every
job from the local database.

Reports may be exported before all work finishes, with unfinished states intact.
Every output uses exclusive creation; an existing report is never overwritten.
`run` and `report` reject a missing database path rather than creating an empty
database by accident. Duplicate JSON keys and nonfinite JSON numbers are
rejected at input.

Admission preflight revalidates each capture once across the declared scopes.
Downstream snapshot preparation still revalidates captures and candidate evidence
per scope. The child store also retains full snapshot payloads and indexed candles
for each distinct child snapshot. Repeated scopes therefore cost preparation
time and disk space even when their underlying prices overlap. Scope-count,
input-size and execution limits do not make that cost disappear. This slice
does not implement shared price-blob storage, cross-scope label caching or
threshold-independent projection caches.

## Interpretation boundaries

- Report successes, failures, open/ambiguous/missing outcomes and resolved
  parents separately for each exact scope. Do not pool percentages across
  candidates, directions, thresholds, entry rules or source revisions.
- Five thresholds evaluated on one BTC parent remain one market group in each
  relevant scope. They never become five independent parents. Neither repeated
  plan execution nor child-job reuse advances the evidence count.
- Report completion is not research qualification, and research qualification
  is not a profitability or prospective-validation result.
- The existing compatible no-horizon asymmetry route is still unavailable.
  Running more scopes does not create it or import fixed-window asymmetry.
- All runtime, Telegram and trading authority remains false. A plan or report
  is not an instruction to a trading runtime.

Verification uses the existing genuine captured-score fixtures through the
real source adapter, local SQLite implementation and CLI. It checks bounded
progress, complete scope reporting, restart, input rejection and immutable
exports. Fixture outcomes are software evidence only.
