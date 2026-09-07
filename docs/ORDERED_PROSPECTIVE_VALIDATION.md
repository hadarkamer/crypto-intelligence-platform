# Ordered v7 prospective validation

`research_ordered_validation_store.evaluate_scope` is the bounded, transactional
worker adapter. Migration 031 creates immutable registrations, one evolving
outcome record per frozen scope/BTC parent, and append-only evaluation revisions.
It never writes source events, sends messages, or merges an archive into LIVE.

## Exact freeze and later evidence

Registration freezes the complete candidate definition, research direction and
orientation, symbol, threshold, horizon, source, fixed period and cutoff,
feature/catalog version, BTC parent policy, v7 label method, common-window method
and acceptance policy if an exactly compatible one exists. The production
adapter uses PostgreSQL `clock_timestamp()`, never a historical caller clock.
The same scope cannot be refrozen or repaired by changing criteria. A changed
definition needs a new versioned scope and a new actual freeze time.

A whole parent movement beginning strictly after the freeze may contribute to
PROSPECTIVE. Any movement beginning on/before freeze remains DISCOVERY, including
alerts and outcomes arriving later in that old movement. Period boundaries also
exclude whole crossing movements. The two overlapping periods are reported
independently, never counted as additional evidence or a holdout split.

The earliest condition completion and every simultaneous member are selected
before labels. Their identity/price/features are frozen in the wave ledger.
The entry fingerprint includes event ID, time, snapshot ID, symbol, research
direction, entry price and the exact frozen predicate feature values. Unrelated
feature enrichment is not a new entry and does not create a conflict. A changed
required value remains a conflict even if it still passes the same threshold.
Conditions and the real freeze clock register immediately, but wave entry
choices wait until source/membership coverage is complete and untruncated.
An initial backfill therefore cannot accidentally freeze a later alert before
its earlier source has arrived. Partial passes never replace existing ledger
entries or their evidence; previously recorded conflicts remain visible.
Updating OPEN to SUCCESS/FAILURE updates that same wave; it never creates a new
trial. A late earlier entry or a changed decision snapshot records a sticky
representative conflict instead of choosing the better result. Prior evidence
and failures remain in append-only result revisions.

## Metrics and eligibility

Each phase keeps SUCCESS, FAILURE, OPEN, AMBIGUOUS, NO_TOUCH and DATA_MISSING
separate. Only valid decisive v7 labels enter the hit-rate denominator. Five
resolved future parents meet the regular sample minimum. FRESH separately needs
three future parents whose entire wave evidence is in the current rolling 14
days. FRESH expires automatically; sample minima alone never imply acceptance.

Full-window metrics validate the original/explicit inverse measured event ID,
entry price, direction, canonical Spot instrument/source, exact horizon and the
closed-minute boundary contract. Partial entry/trailing minutes stay disclosed
and excluded. All First Touch statuses participate in the common-window cohort;
missing metrics cannot silently select successes. Within a simultaneous wave,
use minimum MFE and maximum MAE; then compute sum(wave MFE)/sum(wave MAE), giving
each wave one weight. A zero denominator is undefined. Median wave MFE/MAE and
probability remain separate metrics. `all_period_metrics` is descriptive; only
the separate prospective phase may support future validation.

## Acceptance compatibility, not invented thresholds

The legacy `frozen_oos_protocol_v1` binds its 0.60 hit rate, 0.50 Wilson lower
bound, 1.50 median ratio and 1.20 geometric ratio to a no-fixed-horizon protocol
with six thresholds, a different BTC movement width and 20-wave minimum.
`research_formula_acceptance.py` binds different phase thresholds to controls
and recency weighting. Neither is an exactly compatible ordered-v7,
eight-threshold, full-common-window policy. Their numeric values are not copied.

Until such a documented contract exists, the ledger and metrics continue to run
with `MISSING_EXACT_ORDERED_V7_ACCEPTANCE_CONTRACT`; no relevance is asserted.
This is a missing method definition, not a human approval requirement. The
validated configuration format has explicit probability and asymmetry gates,
their OR/AND combination, a documented source/rationale, an exact binding hash,
and a fixed tested-family multiplicity disclosure. It has no production numeric
defaults. The family size is disclosed with successful and failed attempts;
adding tests beyond the frozen family blocks acceptance under that contract.
The current implementation does not claim an automatic multiple-testing
correction or multiply probabilities from overlapping formulas.

Only the metrics required by a bound path gate that path. A documented
probability-only path is not blocked merely because the separate common-window
asymmetry cohort is unavailable. Missing asymmetry metrics remain explicit, and
an AND contract still requires both paths. No contract means no acceptance.

Registering a newly compatible policy cannot retroactively qualify outcomes
already examined under a missing policy. A new version freezes the policy and
starts future assessment on later complete waves while retaining old results.

## Verification and limitations

The selftests exercise real v7/common-window calculators, non-minute entry,
whole-wave freeze boundaries, period/source exclusion, status denominators,
fresh expiry, zero MAE, exact instrument/reference identity, distinct acceptance
paths, missing contracts and trial-family changes. Adapter fixtures verify
database-clock use, one-time freeze, stable representatives and retained entry
conflicts. Fixtures are not a substitute for PostgreSQL migration/runtime checks.
Operational BTC grouping is explicitly versioned and is not proof of statistical
independence. No old historical results become prospective after deployment.
