# Stage 8: first fixed model-only research tranche

This is the **pure local definition freeze**, not a durable prospective
registration. By itself it does not start a prospective evidence clock,
connect a database, qualify a formula, activate a worker, send Telegram or
permit trading. The separate durable research runtime is documented in
[`2026-09-13-stage8-durable-runtime.md`](2026-09-13-stage8-durable-runtime.md).
The wider formula catalog remains outside this fixed first tranche. Existing
or already inspected parent evidence cannot become prospective by assigning it
this new version.

The pure declaration is `research_stage8_contract.py`; its SHA-256 is:

```text
5a3ee3af6a73467f3ead09fbe9684a8f60101f97fa7064472b228a31468e6bef
```

`frozen_manifest()` returns an independent JSON copy. `validate_manifest()`
requires that exact frozen definition, not merely a caller-supplied hash.
`exact_binding(scope_id=..., candidate_id=..., threshold_bps=...)` binds one
predeclared cell to the complete manifest digest. `validate_exact_binding()`
rejects changed fields even when their binding hash is recalculated. The
module has no observation, database, network, qualification or registry API.

## Fixed first tranche

| Axis | Frozen choice |
|---|---|
| Source | Every v4 attempt in an explicitly declared symbol/time audit cohort; Watch v2 stays a separate sidecar |
| Projection | Observed signed model score multiplied by +1 for LONG or -1 for SHORT |
| Predicate | One of positioning, futures flow or spot flow with aligned score at least 65 |
| Direction | LONG and SHORT separately: six definitions |
| Horizon | 60 minutes only for this first tranche |
| Barrier | All existing eight thresholds, 25 through 200bps in 25bps steps |
| Scope | Seven Binance singleton scopes, one exact ALL_BINANCE7 pool, and separate HYPE Spot `@107` scope |
| Search family | 6 definitions × 9 scopes × 8 thresholds = 432 exact cells |
| Vote | At most one outcome-blind representative per exact binding and BTC parent |
| Result ceiling | Experimental research only; no qualification or delivery code here |

The score-65 predicate already exists in the
[captured question catalog](../research_ordered_question_catalog.py). Starting
with 60 minutes is an outcome-blind engineering choice to validate the complete
path with the shortest existing horizon. Every existing barrier is declared;
there is no best-barrier replacement. The 300-second maximum capture age is a
fixed initial engineering bound, not an optimized or empirically validated
freshness claim. Changing it requires a new version and real freeze.

This is not the full formula catalog. MaxPain, liquidity, composite predicates,
time families and longer horizons are outside this first predicate tranche,
not deleted from the project. Their admission requires a separately versioned
catalog expansion before evaluating its prospective evidence. Report all 432
tested cells and their overlapping parent identities; the policy does not
claim correction for multiple testing or proven profitability.

## Source and representative boundaries

The [source audit](2026-09-13-operational-score-source-audit.md) remains the
authority boundary. Keep the original v4 `model_score_status=ABSENT`; never
insert joined Watch scores into old decision bundles. Select the latest
durably prior `WATCH_SHARED` record before validating it. Do not skip a failed
latest capture in favor of an older valid one. Missing model scores and
unavailable fallback zero remain UNKNOWN rather than neutral or false.

The source declaration also preserves MaxPain slot status, the four additive
components, and liquidated-side inversion: source SHORT means price UP and
candidate LONG; source LONG means price DOWN and candidate SHORT. A selected slot is neither delivery proof
nor proof of signal/no-signal. None of these fields is a first-tranche predicate.

HYPE's operational model provenance remains distinct from its canonical
research outcome route. Do not relabel a perpetual/fallback operational route
as Hyperliquid Spot. Missing or incompatible required provenance remains
UNKNOWN. The separate HYPE outcome scope requires explicit `@107` evidence.
ALL_BINANCE7 is defined upfront, not obtained by deleting HYPE or any other
unavailable symbol after reading data. Any missing member of an exact pooled
scope remains visible and cannot silently change that scope.

Collapse duplicate attempts by exact anchor slot/event, then select the earliest
valid matching decision for each BTC parent by decision time, symbol, slot ID,
then event ID, before inspecting labels. A missing outcome never licenses a
later representative. Unknown earlier membership or match eligibility blocks
representative completeness; it is not permission to choose a later winner.
One parent is one operational evidence group across every coin in a pool, not
a mathematical guarantee of statistical independence. See the
[BTC parent policy](BTC_PARENT_MOVEMENT_POLICY.md).

## Atomic experimental threshold

For one exact binding, experimental relevance requires **at least five distinct
BTC parents in the passing route AND (probability OR asymmetry)**. The same
outcome-blind representative population underlies both routes. Each route must
disclose its usable parent IDs and have at least five itself; counts cannot be
borrowed between routes or from another threshold, direction, scope or formula.

| Route | Initial predeclared numerical policy |
|---|---|
| Probability | Hit rate ≥70% and Wilson 95% lower bound ≥40% |
| Asymmetry | Full-window sum-MFE/sum-MAE ratio ≥1.5, favorable dominance ≥60%, and positive median paired MFE−MAE |

The numbers are reused from the existing
[documented research policy](ORDERED_V7_ACCEPTANCE_POLICY_V1.md), not selected
from this tranche's outcomes. Its old AND combination and three-parent FRESH
exception are explicitly not imported. A passing probability route does not
need an available asymmetry route, and vice versa; common source/provenance and
representative-completeness failures still block both.

All parent votes are unweighted. Wilson uses the existing exact
`z=1.959963984540054` two-sided 95% lower-bound formula without continuity
correction. Full-window MFE/MAE come from each single frozen representative,
not a mean of ratios or a min/max synthetic combination of coins. Dominance is
the percentage with MFE greater than MAE; paired edge is the median of their
differences. A positive summed MFE with zero summed MAE keeps the existing
ordered-policy `ZERO_DENOMINATOR` state and a null JSON ratio; this first
version leaves the asymmetry route unavailable, without blocking an otherwise
passing probability route. This is a conservative policy limit, not a claim
that favorable movement was absent. Zero/zero is undefined and does not pass.
No NaN or Infinity is serialized.

Probability uses decisive SUCCESS/FAILURE labels. Retain OPEN, UNRESOLVED,
DATA_MISSING and unknown records. Raw persisted status and terminal reason are
separate from derived AMBIGUOUS or NO_TOUCH reporting states. Full-horizon
asymmetry must use the complete fixed 60-minute price path, never excursions
truncated at first touch. Outcome read cutoffs are explicit; mutable current
rows cannot establish unavailable historical revisions.

## Prospective timing and checks

The durable registry must persist the exact binding before prospective evidence
begins. This version requires every counted parent to start strictly after that
database-issued freeze; a parent already underway at registration cannot be
promoted as a new prospective unit. Local constants, filesystem timestamps and
retrospective outcome recomputation cannot backdate a registry freeze.

Run `python research_stage8_contract_selftest.py`. The checks are network/DB-free
and cover all 432 identities, nested mutation, canonical hash types, the atomic
5-plus OR declaration, unavailable-source semantics and the no-runtime boundary.
They verify the declaration, not actual qualifying evidence or DB coverage.
