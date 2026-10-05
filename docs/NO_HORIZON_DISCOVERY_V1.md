# Bounded catalog discovery and single-window descriptive ranking

This layer expands explicit research choices into frozen calendar declarations,
registers them through the existing acquisition queue, and ranks the resulting
verified executor report. It adds no migration, worker, source collector, label
engine or acceptance policy.

## Freeze the search before acquisition

`research_no_horizon_discovery.build_plan` takes a valid first-cohort template,
explicit base directions, explicit symmetric thresholds, optional candidate keys
and a finite window count. Omit candidate keys to select every supported catalog
definition; supplying keys declares a bounded subset.

The plan retains the full catalog manifest, including definition hashes,
orientations, unsupported features and selected/unselected status. The current
catalog has 298 definitions, of which 34 are supported by accepted Watch data.
The supported fields are the three captured model totals and the original
Israel-weekend feature. Unsupported fields are not inferred from missing data.

All candidate, direction and threshold combinations are declared before
acquisition. Duplicate or invalid choices are rejected. Scope identity, derived
analysis direction, gate policy, source route and resource limits use the
existing cohort normalizer. Candidate aliases and inverse definitions retain
their separate version identities; their count is not a count of independent
strategies.

The exact grid must fit the existing 64-scope cap:
- 34 supported definitions × one base direction × one threshold = 34 scopes.
- The same catalog × two base directions = 68 scopes and is rejected.
- Explicit subsets can use multiple directions or thresholds within the cap.

There is no top-64 truncation, outcome-based selection or automatic splitting
into separately anchored populations. The existing 16,384 source-scope decision
budget also applies: 34 scopes permit at most 481 actual source rows, and 64
scopes permit 256. The sum of the declared part row limits can lower the
actual ceiling further; the plan exposes both ceilings. Overflow blocks the original complete population; it does
not take the first rows or silently increase the budget. These are bounds,
not guarantees that a future population will fit.

The generated calendar freezes each complete declaration and the relevant
implementation hashes. Plan validation regenerates the whole object before
registration. Registration reuses the calendar's strict database-clock check
and per-window idempotency. Newly late windows are rejected; partially committed
registrations can be resumed without duplicates or date changes.

A file's declared time and content hash do not prove when it was created or
that outcomes were unknown. The registration receipt provides actual database
creation timestamps, not prospective formula validation.

## Rank one exact frozen population

`research_no_horizon_ranking.rank_report` accepts a discovery plan, one verified
global-cohort executor report and its calendar window ordinal. It requires the
exact declared window, full scope population, implementation bindings and report
integrity. It cannot combine different windows, symbols, snapshots or datasets.

The ranker checks selected representatives against the full decision ledger and
recomputes every available gate from its first-touch checkpoints and causal
parent metadata. The entire recomputed gate must match the stored gate. Changing
stored probability, sample count or eligibility and recalculating an outer hash
does not make inconsistent evidence acceptable.

All declared scopes remain in the result, including zero matches, insufficient
samples, pending work and blocked inputs/outcomes. If any scope is unfinished,
the result is diagnostic only and assigns no final ranks. A finished scope's
original atomic gate eligibility remains visible even while another scope is
pending; it is not a completed search result or publication authorization.
Terminal status alone
does not establish complete evidence: missing data and blocked entries remain
unranked even when the executor has finished handling them.

Among scopes with complete usable evidence and at least one resolved parent,
the descriptive order is fixed:
1. Wilson 95% lower bound, descending.
2. Resolved parent count, descending.
3. Hit rate, descending.
4. Scope ID, ascending, to resolve ties deterministically.

A small sample can receive a descriptive rank while remaining experimentally
ineligible. Eligibility uses the unchanged atomic gate: at least five resolved
causal parents and a passing compatible probability/asymmetry route with no
evidence blockers. The default probability policy requires at least 70% hits
and a Wilson lower bound of at least 40%. An explicitly versioned valid policy
in the frozen declaration is preserved exactly; the ranker does not replace it
with defaults. Compatible no-horizon asymmetry remains
unavailable. Ranking does not relax any condition.

OPEN and AMBIGUOUS representatives remain explicit. Source parts are transport
units, not extra trials. The earliest representative of each BTC parent is
selected before outcomes; a later winning occurrence cannot replace an earlier
loser or missing outcome.

## Commands and evidence boundary

```bash
python research_no_horizon_discovery_cli.py build \
  --declaration new-first-cohort.json \
  --base-direction SHORT --threshold-pct 0.25 \
  --windows 4 --output new-discovery-plan.json

python research_no_horizon_discovery_cli.py register \
  --plan new-discovery-plan.json --output discovery-registration.json

python research_no_horizon_discovery_cli.py rank \
  --plan new-discovery-plan.json --window-ordinal 0 \
  --executor-plan-id EXECUTOR_PLAN_ID --output window-000-ranking.json
```

Build is offline. Registration uses only the explicit
`RESEARCH_NO_HORIZON_DATABASE_URL` and existing migrations 056–057. Ranking
opens that destination in a separate read-only session and calls the existing
verified report API; it does not execute pending jobs or acquire source data.
The executor plan ID is available in the acquisition report once admission
finishes. No primary/source database fallback is used.

A request blocked by source/parent preflight before admission has no executor
plan to rank. Its full population failure remains in the acquisition report;
absence of a ranking must not be presented as successful discovery.

Inputs and outputs are bounded strict JSON; outputs are create-only. An
unfinished ranking writes its diagnostic result and returns exit 2. Invalid
input, unavailable schema or connection failure returns exit 1 with a sanitized
exception type. No schema is installed automatically.

The CLI obtains reports from the explicit trusted database. Pure JSON hashes
bind content and expose inconsistencies; they do not authenticate an external
database or prove genuine market observations. Synthetic tests are not market
evidence. No actual leading formula or live candidate is asserted by this
development increment.

## Remaining stages

This completes bounded catalog-plan generation, existing-queue registration
and single-window descriptive ranking. Candidate selection across multiple
testing windows, version-aware prospective validation and result publication
remain separate work. Adjacent windows can share BTC parents; their counts and
probabilities are never pooled by this implementation.

All runtime, Telegram and trading authority remains false. No merge,
deployment, production migration/configuration or provider call is implied.
The original October experiment stays pinned to `e8d25e57`; this layer does not
rewrite its declarations, source evidence, gate or scheduled evaluation.
