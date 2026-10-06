# Frozen candidate selection across research windows

The selector completes the comparison step after bounded discovery. It consumes
every declared calendar window and returns exact, versioned scope identities for
a later prospective validation stage. It does not evaluate future validation,
publish alerts, or change the existing outcome or atomic gate contracts.

## Freeze the selection policy before collecting data

`research_no_horizon_selection.build_plan` accepts the same original declaration,
explicit directions, thresholds, optional supported catalog subset and finite
window count as discovery. Two additional integers are mandatory:

- `top_k`: maximum number of selected scopes, from 1 through the declared scope
  count. It is a ceiling, not a promise to fill the shortlist.
- `required_eligible_windows`: number of windows in which the original atomic
  gate must pass, from 1 through the declared window count.

Both choices are part of an exact versioned policy. The original normalized
template, policy, total window count and selector implementation are hashed into
a new template cohort key before the discovery calendar is built. Consequently,
changing either policy choice, the selector code or the total window count changes
every resulting acquisition declaration, including the first one. An old request
cannot silently acquire a new selector or an extended selection denominator.
The original template and all existing frozen experiments remain unchanged.

The full catalog, scope grid, gate policy and current implementation hashes are
still bound by discovery. Existing limits apply unchanged. Validation rebuilds the
entire plan and rejects mutated or stale versions before registration or reading
executor results. This module is separate from the existing acquisition,
calendar, ranking, label and gate implementations, preserving their frozen hashes.

`register_plan` validates the whole selector and uses the existing strict calendar
registration. Actual database creation times must be no later than each source window's start.
An interrupted registration resumes the same exact declarations without skipping
late or missing windows. Registration is per request, not one all-window transaction.

Content binding and registration timing are different facts. A plan file or
executor report alone does not prove pre-registration: executor submission can
occur independently of the acquisition queue. Selection results therefore keep
`policy_registration_verified=False`. A claim of timely registration requires
trusted acquisition registration records and the request-to-executor linkage.
No new prospective-evidence claim is made here.

## Select from complete, verified window reports

`select_reports(plan, reports)` takes raw compatible executor reports in exact
calendar order. It requires exactly one report per declared window and calls the
existing evidence-verifying ranker for each. It does not accept precomputed
rankings or unverified scalar statistics. Wrong windows, duplicates, altered
versions, changed gates or missing populations are rejected.

Every declared scope and window stays visible. An unfinished scope in any window
suppresses all final cross-window ranks and selections. Empty, blocked, small,
OPEN and AMBIGUOUS evidence remains explicit, with the original per-window gates.
A completed selection may correctly contain no selected scope. A source request
blocked before executor admission has no report; it must remain visible in the
acquisition report and prevents this complete selection. It cannot be skipped or
replaced with an invented empty window.

A scope is descriptively rankable across windows only when it is rankable in
every window. Its fixed order is:

1. Lowest per-window Wilson lower bound, descending.
2. Smallest per-window resolved-parent count, descending.
3. Lowest per-window hit rate, descending.
4. Scope ID, ascending.

These minima are descriptive comparison keys. They are not a combined confidence
interval, a pooled probability, or a new atomic qualification gate. Small samples
can still rank descriptively. A rankable scope is selectable only if its unchanged
atomic gate passes in at least `required_eligible_windows`; the first `top_k`
selectable scopes are returned. Ineligible higher-ranked scopes do not consume
selection slots. Selection never changes a window's original eligibility.

A custom valid versioned gate policy remains exact. The usual default gate still
requires at least five resolved causal parents together with its compatible
probability route. There is no three-parent validation shortcut and no invented
no-horizon asymmetry metric.

The result retains exact selected scope tuples and full candidate definitions
with their definition hashes. Future validation must carry those exact tuples;
rebuilding a Cartesian grid from their candidate keys, directions and thresholds
could create unselected combinations and is not an equivalent selection.

## Parent overlap and the validation boundary

The result records known matched BTC-parent IDs across every scope and window,
including unselected scopes and blocked or pending outcomes. Repeated IDs retain
their window ordinals. Counts, successes, losses and probabilities are never
pooled across windows or scopes. Passing several windows is not proof of
independent replication.

The parent inventory is explicitly **matched decisions only**. Executor ledgers
do not attach BTC-parent IDs to NO_MATCH decisions, so this inventory cannot be
described as all market parents observed during research. The acquisition anchor
proofs contain pinned parent payloads for all source rows if that broader
inventory is needed later.

The separate prospective validation implementation follows these requirements
(see [NO_HORIZON_VALIDATION_V1.md](NO_HORIZON_VALIDATION_V1.md)):

- Bind exact selected candidate versions, scope tuples, policy and source reports.
- Begin after the latest discovery outcome cutoff, with trusted database
  registration before the validation source interval.
- Exclude known research parents and continuing movements that began before the
  validation interval; never let an old wave become fresh evidence.
- Select the earliest eligible representative without outcomes and preserve every
  exclusion and denominator.
- Apply the original gate to fresh evidence without pooling discovery counts.

No selection output is a validated formula, profitability claim or delivery
authorization. Multiple-testing correction and prospective evaluation are not
implemented by this selector.

## Commands

```bash
python research_no_horizon_selection_cli.py build \
  --declaration new-first-cohort.json \
  --base-direction SHORT --threshold-pct 0.25 \
  --windows 2 --top-k 3 --required-eligible-windows 1 \
  --output new-selection-plan.json

python research_no_horizon_selection_cli.py register \
  --plan new-selection-plan.json --output selection-registration.json

python research_no_horizon_selection_cli.py select \
  --plan new-selection-plan.json \
  --executor-plan-id WINDOW_000_EXECUTOR_PLAN_ID \
  --executor-plan-id WINDOW_001_EXECUTOR_PLAN_ID \
  --output selection-result.json
```

Build is offline. Register uses the existing acquisition schema and only the
explicit `RESEARCH_NO_HORIZON_DATABASE_URL`. Select opens that same explicit
destination read-only, loads each exact executor report, and executes no worker,
source collector or provider request. Terminal reports are immutable; an
unfinished report yields a diagnostic result rather than a partial selection.
No primary/source database fallback or automatic schema installation exists.

Repeated executor IDs must be unique, valid hashes, and cover the full window
count. Plans are bounded to 4 MiB, aggregate serialized source reports to 128 MiB,
and selection output to 64 MiB. Exceeding a bound fails without dropping windows
or truncating evidence. Files are create-only. A diagnostic incomplete result
returns exit 2, invalid input or connection errors return sanitized exit 1, and a
complete result returns exit 0 even when it selects no candidate.

Synthetic SQLite/PostgreSQL tests exercise the same outcome and causal-parent
pipeline; they are not market evidence. The original October experiment remains
pinned to `e8d25e57`. Runtime, Telegram and trading authority stays false.
Prospective validation of the exact selected scopes is implemented separately in
[NO_HORIZON_VALIDATION_V1.md](NO_HORIZON_VALIDATION_V1.md), with trusted registration
timing, linked executor evidence and fresh-parent exclusions. Result publication
remains a separate development step.
