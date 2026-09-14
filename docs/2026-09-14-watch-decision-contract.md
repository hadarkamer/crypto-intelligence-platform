# Frozen Watch decision predicate contract

This increment specifies and exercises an **inactive, outcome-blind contract**
for evidence saved by PR43. It does not install a recurring evaluator, persist
formula samples, create statistical cohorts, change a reporting pointer, or
enable discovery, promotion, alerts or trading. Production formula coverage
remains **204 of the original 298 definitions**.

The executable mapping and probe live in `research_watch_decision_contract.py`.
Contract version is `watch-decision-contract-v1`; selection version is
`watch-frozen-item-cluster-qualified-group-v1`. Population labels are
`MAXPAIN_SELECTED_ITEM`, `MAGNET_CLUSTER`, `QUALIFIED_COMBINED_GROUP` and the
blocked `EVENT_REQUIRED` family.
They retain the original candidate IDs, definitions, definition hashes and
NORMAL/INVERSE pairing. The probe consumes already archived evidence; it does
not rerun Watch, rescore sources, rebuild Magnet clusters, infer delivered
events, retrieve prices or read outcomes. Its source is the sibling
`source_metadata.capture_metadata.operational_decisions`, introduced by
`docs/2026-09-14-watch-decision-capture.md`.

## The 42-definition mapping

All counts below include the existing inverse counterparts.

| Original predicate family | Definitions | Meaning in this contract |
| --- | ---: | --- |
| Captured MaxPain confirmation equality | 8 | Literal status of the actual selected operational item; its item/timeframe population is explicit. |
| Captured Magnet confirmation equality | 8 | Literal status of each actual captured Magnet evaluation; its cluster population is explicit. |
| Combined top-item mean thresholds and bands | 22 | Exact top-item mean within the separately declared qualified-Combined-candidate population. |
| `COMBINED` | 2 | Blocked: requires original event identity. |
| `COMBINED_LIQUIDITY_SUPPORTS` | 2 | Blocked: requires original event identity and verified selected top-item liquidity. |

Thus **38 predicates can be exercised under the declared operational
populations**, while four remain event gated. This is a mapping/probe count,
not an increase in active research coverage or a count of useful formulas.
The other 52 unsupported definitions are outside this bounded step.

Four of the eight MaxPain status predicates ask for `NOT_CONFIRMED` or
`OBSERVATION`, including inverses. Those labels are **unreachable in the
currently captured MaxPain engine version**. They remain literal equality
diagnostics; they are not silently repaired, discarded, or described as newly
productive research formulas.

## Source and observation identity

Every interpretation first requires an accepted source intake, matching parent
and score-bundle identity, and successful
`research_watch_decision_capture.validate_bundle`. The read-only capture view's
`SOURCE_BOUND` status is insufficient by itself. Freeze the capture, feature,
selection and population versions alongside each source reference. Keep source
availability and computation times causal; the parent source cannot be usable
before its evidence exists.

The new units check all same-coin MaxPain source-timeframe clocks. A MaxPain
score includes consensus/cluster contributions across timeframes; Magnet
maximal-cluster selection depends on the whole target set; and a Combined top
item or qualification can depend on a different group member. A future or
invalid nonlocal source clock is therefore relevant even when that timeframe
is not the selected top item or a final Magnet member. Price/OI and Futures
clocks also apply where they feed confirmation or qualification. Spot does not
vote in these confirmation engines and does not create an unrelated veto.

The three populations must not be flattened into one coin-level score:

| Population | Observation identity within one accepted coin/scan | Selection evidence |
| --- | --- | --- |
| MaxPain item confirmations | Actual displayable selected item, retaining `item_id`, timeframe and source side | Exact `displayable_item_ids`, frozen selected score slot and matching item/source proof. |
| Magnet evaluations | Actual captured cluster, retaining `magnet_id`, side and source item | Captured evaluation record and literal result; never the confirmed-only map. |
| Qualified Combined candidates | Actual group key and source side, retaining `top_item_id` | Qualified group/candidate parity and exact ordered items. |

All identities also retain snapshot/Watch identity and source hashes. A coin
can have two Combined direction groups, seven selected MaxPain items, and
multiple Magnet clusters. They are distinct observations, **not independent
market cases**. This probe does not invent a fixed statistical cluster-selection
policy or a cross-scan cluster lifecycle from the captured object hashes.

A valid complete population with no actual units is `EMPTY`; the probe does
not manufacture a unit or a direction just to emit a negative evaluation.
Missing capture or selected-item truncation instead creates an explicit
unknown population diagnostic while preserving unaffected known units.

For MaxPain and Combined, source `SHORT` maps to predicted price `LONG`, and
source `LONG` maps to price `SHORT`. Magnet `UPPER` maps directly to price
`LONG`; `LOWER` maps directly to price `SHORT`. An inverse definition retains
the same base observation and predicate proof and changes only its analysis
direction. It does not invert a confirmation label, replace the selected item,
or evaluate an unselected source side as a control.

## Literal confirmation rules

Current MaxPain results are `BELOW_SCORE`, `CONFLICT`, `UNCONFIRMED`,
`CONFIRMED` and `STRONG_CONFIRMED`. Current Magnet results are `OBSERVATION`,
`NOT_CONFIRMED`, `LIQUIDITY_UNAVAILABLE`, `LIQUIDITY_CONFLICT`, `CONFIRMED` and
`STRONG_CONFIRMED`. Preserve the captured spellings and their engine identity.

A valid observed status unequal to an original predicate's requested status is
a known `NO_MATCH`. In particular, a known MaxPain `BELOW_SCORE` is unequal to
all four catalog status labels; it is not renamed `NOT_CONFIRMED`. A captured
Magnet `LIQUIDITY_UNAVAILABLE` is a known operational status, even though it
does not prove valid liquidity amounts. This distinction permits literal status
comparisons without inventing an unrelated liquidity feature.

A missing status, unrecognized status, invalid source, `ERROR` or
`NOT_EVALUATED` record is `UNKNOWN` for the affected interpretation. An absence
from the operational **confirmed-only** Magnet map cannot establish any
negative status. Actual evaluation records retain negative, missing and error
cases separately. Missing evidence should invalidate only the source/selection
or feature that depends on it; an unrelated failed Magnet evaluation does not
by itself erase a verified MaxPain confirmation.

## Qualified Combined top-item rules

The original top-item numerical predicates use inclusive cutoffs
55, 60, 65, 70, 75 and 80, and half-open bands `[55,60)`, `[60,65)`, `[65,70)`,
`[70,75)` and `[75,80)`. Keep those exact predicates and their inverses.

The operational group contains the retained, displayable Watch items for one
coin and source side, after the original Watch input selection. Order is
highest score, then earliest original `TIMEFRAMES` index. The first item is
the top item. Qualification requires at least two captured unique signal
keys; no new score threshold or later outcome chooses the item or group.

Use the actual top item's frozen `average_score_all_timeframes`. The capture
binds it to the same source side's scored slots. A coin-wide replacement,
another timeframe, an average across selected items, or the leading score
itself is not a substitute. Keep both source direction groups when both exist.

The historical extractor exposes this feature only inside a direction-verified
`COMBINED_CONFIRMATION` event. The new operational population instead consists
of **qualified candidates at the captured scan**, including repeated active
candidates that generate no new message. Retaining an original numerical
predicate and definition hash does **not** make those populations equivalent.
Future reports must identify the population explicitly and must not pool the
new candidate observations with the historical event series.

Within this qualified-candidate population, a valid qualified group can produce
`MATCH` or `NO_MATCH`. An actual known unqualified group is
`NOT_APPLICABLE`, not a fabricated control. A complete source with no groups
has an `EMPTY` population and no synthetic evaluations. Unresolved qualification or top-item
selection remains `UNKNOWN`. Missing historical capture, capture failure and
required missing selected items also remain unknown; they do not prove that a
group was absent. A known false numerical condition retains the original
three-valued conjunction semantics once selection itself is established.

## The four event-gated definitions

`COMBINED` and `COMBINED_LIQUIDITY_SUPPORTS` explicitly require
`event.event_type == 'COMBINED_CONFIRMATION'`. Candidate qualification cannot
supply that feature. The probe leaves these four definitions blocked rather
than assigning a fabricated event type or a misleading `NO_MATCH`.

The production event hook records `COMBINED_CONFIRMATION` for both successful
and failed delivery attempts. **Event identity alone does not prove delivery**.
The original predicate does not itself demand delivery success; a future
event-source contract must preserve its intended eligibility and must explicitly
state whether failed events are included. A delivery-only interpretation would
need its own declared population and a durable successful-delivery receipt.

The current Combined event snapshot contains the top-item mean, components and
confirmation, but does not contain its near amount, far amount or selected
liquidity share. Combined `liquidity_imbalances` lists may refer to a different
timeframe and are insufficient. The liquidity conjunction needs an exact
event/top-item/source join and actual nonnegative selected/opposite amounts,
a positive denominator, and a share consistent with those amounts under the
original 0.05 tolerance. Missing inputs are not zero. SUPPORTS retains the
original selected-share threshold of at least 60%.

Joining a later event or delivery receipt to an earlier scan must not classify
an earlier price entry using future information. The future event contract must
establish availability, entry policy, source identity and eligibility before
any outcome reuse. This increment does not perform that join or backfill
historical operational decisions.

## Future persistence and outcome gates

The next bounded step is an additive persistent adapter and cohort integration
for the declared supported operational populations. It must preserve existing
coin/timeframe results and pointers, and separately version source selection,
population, feature extraction, evaluation and result identity. Decide and test
how repeated or tied items/clusters enter each cohort before reporting any
statistics; a successful probe is not that policy.

Reuse existing coin/scan measurements only with exact compatible source,
availability/entry, price-route, measurement-method and LIVE BTC membership
identity. Read outcomes only after the observation and predicate are fixed.
Carry population and selection versions through cohort partitions. Preserve the
existing four windows, eight barriers, conservative tied-member treatment,
earliest-UNKNOWN blocking and NORMAL/INVERSE outcome direction. Unselected or
ineligible observations are not neutral controls. A BTC parent wave remains
one independent case across coins, timeframes, clusters, directions and
formulas. In particular, matching many predicates in one wave does not create
independent confirmation of performance.

The four event-gated definitions remain separate unfinished work after this
contract. Continuous formula discovery, historical archive completion, HYPE
completion and validation on new independent cases also remain on the ordered
project plan. The deferred component hypotheses are not resumed here.

## Verification and reporting limits

The selftest and an explicitly invoked read-only probe can verify unchanged
catalog identities, source bindings, semantic selection, literal statuses,
inverse direction and missingness using actual saved captures. Probe results
describe those specific source snapshots only. They do not prove that recurring
research evaluation or persistent cohorts have been installed, that any formula
has predictive value, or that new alerts were sent. Active coverage stays
**204/298** until a later adapter rollout passes its own integration and
activation gates.

Authoritative source references are `research_ordered_question_catalog.py`
(`extended_features` and `candidates`), `main.py` (Combined builders and
collector), `research_event_runtime.py` (`capture_combined_confirmation`),
`market_confidence_engine.py` (`_confirmation`), `magnet_v1.py`
(`evaluate_confirmation`) and `research_watch_decision_capture.py`.
