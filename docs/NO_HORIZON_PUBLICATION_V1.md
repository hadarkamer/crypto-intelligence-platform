# Verified no-horizon research publication

This layer exports one prospective validation as canonical JSON and a readable
Markdown report. It uses the existing evaluator and retains the complete selected
scope denominator. It does not create a new statistical estimator, cohort,
database table, worker or delivery mechanism.

## Reverify before publication

`research_no_horizon_publication.publish_plan` reads the trusted acquisition and
executor chain through `validation.evaluate_plan`. It takes the frozen validation
plan, frozen selection plan and every ordered raw training report. Registration
timing, exact selected definitions, original population, fresh-parent exclusions
and the original atomic gate are checked again by that evaluator.

There is no command that publishes an arbitrary `validation-result.json` as
authenticated evidence. The pure `publish_reports` API delegates to
`validation.evaluate_reports` and assumes that its supplied records came from
the trusted store interfaces. Content hashes alone cannot prove that origin.

The publication includes its version and implementation fingerprint, the full
unchanged validation plan and result, linked source hashes, an exact scope
summary and its `publication_sha256`. Candidate definitions, registration times,
training-window report identities, request/declaration/anchor/executor bindings,
fresh and excluded parent IDs/start times, original blocked evidence and all
limitations remain in the JSON envelope. Raw market proofs/checkpoints remain
in their referenced stores; the publication does not duplicate all archived
source responses or authenticate external market data.

The publisher has its own fingerprint. Adding this presentation layer does not
change the acquisition, executor, ranking, selection or validation code. Older
experiments remain tied to their original implementation fingerprints.

## Complete denominator and copied metrics

Every selected scope appears in stable declared order, with its exact candidate
version, base and analysis directions, threshold, original status, fresh/excluded
parent counts, gate evidence and exclusion reasons. Outcome counts, hit rate,
Wilson lower bound and gate decisions are copied from verified evidence. Display
rounding does not alter the canonical numeric values or recompute a probability.
Unavailable values remain unavailable; they are not displayed as zero.

The publication state is one of:

| State | Meaning |
| --- | --- |
| `INCOMPLETE` | The full selected processing denominator has not finished. No scope receives final qualification. |
| `COMPLETE_NO_QUALIFICATION` | Processing is complete and no scope passes the prospective gate. |
| `COMPLETE_QUALIFIED` | Processing is complete and at least one scope passes the verified prospective gate. |

A completed empty cohort or a completed blocked scope is not automatically a
successful hypothesis. Local gate eligibility and final global qualification
are displayed distinctly. Pending or unadmitted scopes keep diagnostic rows.
Publication never drops failed, blocked, unresolved or unqualified scopes.

`render_markdown` checks the publication seal, implementation, evidence bindings
and exact rebuilt presentation before rendering. This is structural verification;
it neither authenticates arbitrary JSON nor reruns database acquisition. The
Markdown report carries the same publication hash as the canonical envelope.
Identical inputs and implementation produce identical output; no current wall
clock, random ID, output path or extra selection affects the result.

## Command and filesystem behavior

```bash
python research_no_horizon_publication_cli.py \
  --plan validation-plan.json --selection-plan selection-plan.json \
  --training-executor-plan-id TRAINING_WINDOW_000_PLAN_ID \
  --training-executor-plan-id TRAINING_WINDOW_001_PLAN_ID \
  --output-directory new-research-report
```

The command requires the explicit `RESEARCH_NO_HORIZON_DATABASE_URL` and an
existing compatible research schema. It opens only the existing read-only
connection, reads every training executor report in exact calendar order, and
derives the future request/executor from the frozen plan. The database session
closes before local outputs are created. There is no primary/source database
fallback, schema install, request registration, provider call or queue execution.

Each input plan is limited to 16 MiB; aggregate serialized training reports to
128 MiB; the publication JSON to 96 MiB; and the Markdown report to 16 MiB. These
are accepted-content/output limits, not a peak-memory guarantee. Exceeding a limit
fails without truncating scopes, windows or evidence.

The output parent must exist. The output directory must be new and is reserved
exclusively after both outputs are validated and bounded. It contains
`publication.json` and `report.md`; existing paths are never replaced. Write
failure triggers best-effort cleanup of the invocation's own files and directory.
The pair of filesystem writes is not a cross-file atomic transaction.

Complete publication returns exit 0 even when no scope qualifies. Incomplete
publication writes its diagnostic report and returns exit 2. Malformed evidence,
unavailable schemas, collisions, bounds or operational failures return sanitized
exit 1 without echoing driver messages or credentials.

## Meaning and authority

The report is a research result. A prospective gate pass does not prove
profitability or statistical independence and does not correct for multiple
hypothesis testing. BTC-parent groups retain their operational causal meaning.
The exact declared minimum/probability policy stays unchanged; compatible
no-horizon asymmetry remains unavailable. No rolling cutoff, scope/window
pooling or selective presentation is introduced.

Runtime, Telegram and trading authority remains false throughout the envelope
and its rows. Export does not enqueue messages or write publication/outbox state.
Synthetic SQLite/PostgreSQL tests exercise implementation behavior; observed
future market evidence must come from the registered experiment itself. The
original October experiment and its scheduled evaluation remain pinned to
`e8d25e57`.
