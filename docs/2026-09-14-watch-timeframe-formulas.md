# Selected-timeframe formula research

This increment connects 34 unchanged definitions from the original 298-candidate
catalog: 32 liquidity definitions and two selected/opposite score-difference
definitions, including their existing inverse counterparts. It complements the
170 coin-level definitions in the active v5 adapter. It does not invent formulas
or treat the added timeframe breakdowns as independent market cases.

## Exact source meaning

The capture contains seven real Max Pain source timeframes for each coin. Within
each timeframe, `slot.selected` identifies the side actually selected by the
operational scorer. Selection prefers higher total score, then a nearer active
target, then source LONG on an exact tie. Source SHORT maps to price LONG and
source LONG maps to price SHORT. The inverse candidate changes the predicted
price direction while keeping these base predicates unchanged.

For that selected slot, liquidity share is the frozen `near_share_pct`, verified
against `100 * near_amount / (near_amount + far_amount)` and the same timeframe's
captured source amounts. The original 0.05 tolerance is retained. SUPPORTS means
share >=60, OPPOSES means <=40, otherwise BALANCED. The six original half-open
share bands retain their exact boundaries. Missing or negative amounts and a
zero denominator are unknown; an observed zero against a positive other amount
is a valid 0% or 100% share.

Selected/opposite score difference uses the two frozen scores from the same
timeframe. It is not a coin-wide average. An inactive opposite target has no
score, so its difference is unknown; valid captured liquidity amounts can still
be used. Source clocks, quote/target provenance, captured selection and score
integrity are checked locally per timeframe. A missing unrelated timeframe
cannot invalidate a valid local observation. Checking additive score integrity
does not resume the deferred component-hypothesis research.

## Selection and missing evidence

Only the actual selected base direction enters a timeframe's formula population.
The other direction is `NOT_SELECTED` / `NOT_APPLICABLE`, never a fabricated
NO_MATCH control. Both known inactive targets are likewise not applicable.
Unresolved selection is UNKNOWN in both directions; it can block a later or
tied anchor. For a valid selected direction, the original three-valued
conjunction is retained, including a known false condition when another feature
is missing.

## Storage and outcomes

The supplementary worker uses separate `research_watch_scan_tf_formula_*`
tables and views. One immutable result is stored per coin and captured scan,
with a complete 34-candidate x 7-timeframe x 2-base-direction evaluation grid.
It reads existing frozen observations and performs no provider or outcome
queries while deciding whether a formula matched. Work is bounded to 32 coins
per pass, with cyclic intake traversal, a recent reserve, per-coin error
isolation and a five-minute retry for actual processing failures.

The new reporting pointer activates only after every accepted scan has all
eight coin results READY. The old v1-v5 catalogs, results, reporting pointer and
views are unchanged. `research_watch_scan_formula_coverage` lists exactly the
298 original identities and distinguishes COIN, SELECTED_TIMEFRAME and
UNSUPPORTED support. After activation it shows 204 supported and 94 unsupported
definitions; it is coverage metadata, not a union of statistical outcomes.

Every new cohort partition and comparison carries its timeframe explicitly.
Price outcomes are read from the existing single coin/scan measurement using
the same source identity, entry policy, price route and LIVE BTC membership.
The existing four outcome windows, eight barriers, conservative tied-member
handling and earliest-unknown blocking remain unchanged. A BTC parent wave is
still one independent wave across coins, aliases, directions, barriers and
timeframes. The new views are descriptive and do not authorize promotion,
experimental alerts or trading.

## Rollout and validation

Migration 054 creates only the additive supplement and coverage view. Apply
only that exact migration after the full CI suite passes. Runtime performs
schema readiness checks and owns an independent advisory lock. Main starts,
stops and reports the worker through the existing research lifecycle. Existing
Watch scheduling and the newly deployed dual-CVD experimental alert are intact.

Pure tests verify original-catalog identity, scorer selection, thresholds,
source evidence, inverses and not-applicable handling. PostgreSQL tests verify
immutable complete grids, caught-up activation, retries, late admissions,
timeframe cohorts, unknown blocking, reused outcomes and preservation of old
results. Production verification compares all pre-existing v1-v5 payload
digests under a fixed source-ID cutoff and checks the new coverage and source
bindings. No manual Watch run or test notification is needed.
