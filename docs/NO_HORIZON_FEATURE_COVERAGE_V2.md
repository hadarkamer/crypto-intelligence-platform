# No-horizon captured MaxPain feature coverage v2

This increment connects the existing Watch MaxPain v2 evaluator to the
no-horizon source adapter, local experiments, parent coverage, multipart cohorts
and frozen discovery. It expands usable input contracts without changing the
original catalog, first-touch label or evidence gate.

## Exact versions and coverage

| Binding | Current version |
|---|---|
| Source adapter | `no-horizon-accepted-watch-source-v3-maxpain` |
| Formula evaluator | `watch-scan-formulas-v2-maxpain` |
| Captured features | `watch-captured-total-and-maxpain-features-v2` |
| Source-feature preflight | `no-horizon-source-feature-preflight-v2-directional` |
| Discovery planner | `no-horizon-frozen-catalog-discovery-plan-v2-maxpain` |
| Source export transport, unchanged | `no-horizon-watch-source-export-v1` |

All 298 catalog definitions retain their exact IDs, orientations and definition
hashes. The original 34 supported definitions retain their predicate semantics.
An additional 48 definitions become supported: 24 existing predicates and their
24 inverse definitions, covering own/opposite average thresholds and score bands,
plus full/partial consensus. The remaining 216 definitions stay explicitly
unsupported. Aliases, inverse definitions and overlapping score bands are not
independent strategies or additional market observations.

The existing adapter adds these four fields to the original model totals and
Israel-weekend feature:

| Field | Captured evidence |
|---|---|
| `event.direction_mapping_valid` | Proven mapping between source target side and requested base direction |
| `max_pain.average_score_all_timeframes` | Rounded average of active captured scores for the mapped source side |
| `max_pain.opposite_average_score_all_timeframes` | Corresponding average for the opposite source side, with its own valid proof |
| `max_pain.consensus_hits_full` | Whether all timeframes with a closest active target agree with the mapped side |

No new event, score, source read, SQL query or migration is introduced. The
already validated coin's captured `maxpain_slots` and operational source rows
are passed to `research_watch_scan_formula_maxpain.evaluate_coin`. The exact
existing contract is retained; see
[the Watch MaxPain v2 contract](2026-09-13-all-watch-scan-maxpain-formulas.md).

## Directional proof and missing inputs

Source SHORT maps to base LONG; source LONG maps to base SHORT. An inverse
definition keeps the same base predicates and flips only the analysis direction.
The aggregate uses the seven actual captured timeframes in fixed order:
12h, 24h, 48h, 3d, 1w, 2w, 1m. A known inactive target is excluded from the mean;
an available zero score is included. Missing rows or slots cannot become a
partial aggregate presented as complete. No active target makes that side
unavailable.

The evaluator validates all seven source rows, fourteen slot identities and
explicit states, source targets and quote timestamps, additive component sums,
and recorded consensus and cluster counts. Consensus is based on the closest
active target, with an exact distance tie belonging to source SHORT. It is not
the selected-score flag or a score threshold. Directional provenance records
the contributing values, denominator, mean, consensus, quote identity and
validation reasons. Invalid MaxPain proof leaves the independent original model
features available when their own proof is valid.

Availability is checked for each requested base direction. The preflight v2
receipt stores `feature_availability[feature][base_direction]`, including row
counts, unavailable ordinals and reason counts. Repeated predicates or thresholds
do not count a source row twice for the same feature and direction. Unknown
inputs still produce `UNKNOWN` unless a known false conjunct establishes
`NO_MATCH`; unknown potentially matching decisions block ordinary admission.
Neither feature availability nor a decidable match demonstrates parent coverage,
successful outcomes or qualification.

## Bounds and frozen compatibility

The 64-scope cap and 16,384 source-scope decision budget are unchanged. Selecting
all 82 supported definitions already exceeds the scope cap with one direction
and one threshold. Discovery therefore requires an explicit bounded candidate
subset; for example, 32 definitions × two directions × one threshold = 64 scopes.
An explicit 34-scope subset permits at most 481 actual source rows; 64 scopes
permit at most 256, subject to the declared part caps. There is no automatic
splitting, truncation or budget increase. See
[the discovery commands](NO_HORIZON_DISCOVERY_V1.md).

Source/feature versions and the repository dependency closure are frozen in
plans and declarations. This expansion changes those bindings and requires new
plan identities. Current code rejects older frozen plans rather than silently
reevaluating or transferring their checkpoints. Use the original pinned code
for an old plan. The original October experiment remains pinned to `e8d25e57`;
its declarations, scheduled evaluation and evidence are unchanged.

The source remains accepted Watch intake, not every market opportunity. The
outcome price route is unchanged; operational MaxPain quotes do not substitute
for it. No providers are called by this expansion. The atomic five-parent gate,
its complete-evidence requirement and probability policy remain unchanged;
compatible no-horizon asymmetry is still unavailable. Actual future market
validation still requires registered future observations and fresh BTC parents.
Runtime, Telegram and trading authority remain false.
