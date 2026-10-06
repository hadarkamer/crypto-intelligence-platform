# Prospective validation of exact selected scopes

This layer freezes one future cohort from a completed candidate selection,
registers it through the existing acquisition queue, and evaluates its linked
executor evidence on fresh BTC parents. It adds no migration, worker, provider,
price-label engine, publication path or trading connection.

## Freeze the actual selected hypotheses

`research_no_horizon_validation.build_plan` takes a future cohort template, the
frozen selection plan and every raw training-window executor report. It runs the
existing selector again. A supplied shortlist, rehashed scalar statistics or a
handpicked parent blacklist is not sufficient. Selection must be complete and
contain at least one selected scope.

The future declaration contains exactly the selected candidate/direction/threshold
tuples, with their original definition hashes and derived analysis directions.
It does not take a Cartesian product of selected values. Symbol, price route and
the complete versioned gate policy must match the research population. The future
source interval, parts and fixed outcome cutoff are explicit new choices.

The source interval must start strictly after the latest outcome cutoff of every
training window. `prior_outcomes_observed` must explicitly be false for this new
population; the implementation rejects true rather than rewriting it. Historical
research can remain retrospective. Future validation does not retroactively
prove that discovery was preregistered.

The plan binds the selection result hash, all training report identities, exact
selected scopes, the complete known matched-parent inventory, validation policy,
future declaration and current implementation versions. Its cohort key commits
to these choices before registration. Validation reconstructs the plan from the
same full training evidence; altered or stale plans cannot silently continue.
Existing acquisition, executor, ranking, selection, label and gate files remain
unchanged, preserving their frozen implementation identities.

## Verify database timing and the actual execution link

Registration uses only the existing research destination and strict acquisition
registration. The actual immutable request creation time must fall within:

```text
latest training cutoff <= database request creation <= validation source start
```

The database clock is checked before creating a request. Existing exact requests
retain their original creation time on retry. A caller's `declared_at_utc` or a
file timestamp cannot substitute for that database evidence.

Evaluation derives the expected request identity from the frozen declaration and
acquisition implementation. It reads the actual registration and acquisition
records, then the executor plan linked by the admitted acquisition receipt. It
checks request, declaration, implementation, anchor and executor identities
throughout that chain. An unrelated executor report with similar statistics is
not an equivalent validation result.

Waiting, incomplete or blocked acquisition produces an explicit diagnostic, not
a successful validation. Evaluation neither advances acquisition nor executes
pending outcomes. Once admitted, the complete original executor report is
verified using the existing ranking verification helpers, including the exact
scope and part denominators, shared source population, causal representatives,
checkpoint contracts and recomputed original gates. Arbitrary exact shortlists
are verified directly; no fabricated discovery grid is introduced.

## Use genuinely new parent groups

A parent can enter the fresh evidence population only when both are true:

- Its immutable start is at or after the validation source interval's start.
- Its ID is absent from every matched decision in every training scope/window,
  including unselected scopes and unsuccessful or unresolved representatives.

The training inventory covers matched decisions, not all market parents: existing
NO_MATCH ledger entries have no parent ID. The start-time condition also excludes
continuing movements that were not previously matched by a research formula.

Whole parent groups are excluded using only IDs and causal timestamps. The
original earliest representative of every surviving parent is retained. Later
winners never replace earlier losers. Excluded parents and their reasons remain
visible; exclusions do not create extra trials or independent evidence.

The original atomic gate is recomputed over fresh surviving outcomes only. There
is no pooling with training counts, between scopes, or across validation windows.
The default gate still needs at least five resolved fresh parents with the
original compatible probability conditions. A custom valid versioned policy
remains unchanged. There is no three-parent shortcut or invented asymmetry route.

OPEN and AMBIGUOUS remain unresolved. Missing evidence for a surviving parent
blocks its fresh gate. An original INPUT_BLOCKED scope stays blocked because its
outcomes were not computed. Under the explicit validation policy, an old excluded
parent's DATA_MISSING outcome can remain excluded while complete fresh survivors
are evaluated; exclusion is fixed before inspecting that outcome and every
original record remains verified and disclosed.

If any original scope is unfinished, all final prospective qualifications are
suppressed. `validation_complete` describes completion of the full processing
denominator, not universal success: complete results can contain blocked scopes
or no qualified scope. Each scope exposes its fresh gate and whether it passed
under verified prospective timing. No cutoff is extended until a candidate
passes, and no different dataset is resumed as the old experiment.

## Commands and trust boundary

```bash
python research_no_horizon_validation_cli.py build \
  --declaration future-cohort.json --selection-plan selection-plan.json \
  --training-executor-plan-id TRAINING_WINDOW_000_PLAN_ID \
  --training-executor-plan-id TRAINING_WINDOW_001_PLAN_ID \
  --output validation-plan.json

python research_no_horizon_validation_cli.py register \
  --plan validation-plan.json --selection-plan selection-plan.json \
  --training-executor-plan-id TRAINING_WINDOW_000_PLAN_ID \
  --training-executor-plan-id TRAINING_WINDOW_001_PLAN_ID \
  --output validation-registration.json

python research_no_horizon_validation_cli.py evaluate \
  --plan validation-plan.json --selection-plan selection-plan.json \
  --training-executor-plan-id TRAINING_WINDOW_000_PLAN_ID \
  --training-executor-plan-id TRAINING_WINDOW_001_PLAN_ID \
  --output validation-result.json
```

All commands require the explicit `RESEARCH_NO_HORIZON_DATABASE_URL` and read every
training report from that trusted destination in exact calendar order. Build and
evaluate use a read-only connection. Register finishes the read-only training
session before opening a separate write session. No primary/source database
fallback, automatic schema install or worker execution is allowed.

Input plans are bounded to 16 MiB, aggregate serialized training reports to
128 MiB, and result output to 64 MiB. These are accepted-evidence/output limits,
not a peak-memory guarantee. Exceeding a bound fails without truncating windows
or source evidence. Files are create-only. Incomplete evaluation writes a
diagnostic and returns exit 2; complete evaluation returns 0 even when no scope
qualifies; malformed input or operational failure returns sanitized exit 1.

Pure evaluation APIs assume their registration and source records were obtained
from the trusted store interfaces. Hashes check bindings and internal consistency;
they do not authenticate an arbitrary external JSON file or prove market data
truth. Synthetic tests validate implementation behavior, not real market results.

A fresh holdout gate pass is neither profitability proof nor a correction for
multiple testing. BTC-parent grouping does not prove statistical independence.
Runtime, Telegram and trading authority stays false. Result publication remains
a separate development stage. The original October experiment stays pinned to
`e8d25e57`; this layer does not alter its declarations or scheduled evaluation.
