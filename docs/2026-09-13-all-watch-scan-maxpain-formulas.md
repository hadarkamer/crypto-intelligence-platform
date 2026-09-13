# Frozen Watch MaxPain aggregates: adapter v2

This is one additional implementation step within research from all scans.
It expands the unchanged existing catalog from 34 to **82 supported definitions**
and keeps the other **216 definitions explicitly unsupported**. It does not start
new formula discovery, cluster/CVD component hypothesis research, B4, 240m family
normalization, archive completion, HYPE completion or prospective acceptance.

## Exact feature contract

`watch-scan-formulas-v2-maxpain` uses
`watch-captured-total-and-maxpain-features-v2`. The original 298 definitions,
orientations and full canonical hashes are unchanged. The original 34 predicates
retain exactly their v1 classifications, including missing features. Each coin
has 164 decisions: 82 definitions × two base directions, without multiplying
observations by the seven MaxPain horizons.

The new fields are `max_pain.average_score_all_timeframes`,
`max_pain.opposite_average_score_all_timeframes`,
`max_pain.consensus_hits_full`, and proven `event.direction_mapping_valid`.
The last name is an existing predicate name; no alert event is created.

`alert_engine.build_opportunities` computes each side's average from its active
SCORED values in the fixed order 12h,24h,48h,3d,1w,2w,1m, rounding the active sum
divided by its actual count to two decimal places. The adapter reuses those
frozen values. A known inactive target is excluded from the denominator; an
available zero score is included. Source SHORT maps to base LONG and source LONG
to base SHORT. An inverse candidate uses the same base predicates and flips only
the outcome direction.

The adapter deliberately requires all seven source rows and fourteen explicit
slot states for a complete aggregate. A MISSING_INPUT row/slot is unavailable,
not an inactive zero and not an invitation to report a partial average as a
complete one. No active source target means the side is unavailable. Failure in
MaxPain feature proof does not discard the independent legacy model features.

The ordinary consensus is the count of timeframes whose closest still-active
target belongs to that source side. Exact distance ties belong to SHORT, matching
`analysis.side_from_distances`. It is neither the scorer's selected flag, a
count of scores above 65, nor the per-timeframe gap consensus. The adapter checks
the recorded numerator/denominator against the source target distances and
requires a positive known denominator before exposing full/partial consensus.

All contributing slots must bind to the captured source target and price and
have causal source timestamps no later than the original bundle computation.
The original three calculation checks are reproduced without rescoring: rounded
additive component sum within 0.01 of score, hits not exceeding total, and cluster
count not exceeding hits. These are source integrity checks, not component
hypothesis research. Invalid proof stays unavailable rather than manufacturing
a known-false direction predicate.

`maxpain_provenance_by_direction` records included/excluded/missing timeframes,
individual frozen values, denominator, sum, rounded mean, opposite-side coverage,
consensus counts, source quote identity/times and validation reasons. Operational
HYPE quotes stay distinct from its established Hyperliquid perpetual outcome
price route. No market provider is called and no price route is relabelled.

## Version transition and comparisons

Migration 049 retains the v1 catalog and immutable 68-decision samples and adds
explicit support for immutable v2 samples with 164 decisions. Catalog versions
are separate; source receipts, price measurements and BTC waves are shared.

The existing evaluation and comparison view names expose one active version.
The same names with `_by_version` retain the complete audit history. Migration
installation leaves v1 active. The worker atomically activates v2 only after
every accepted scan/coin visible to the activation statement has a READY v2
sample, including accepted receipts not yet enqueued. Activation and the final
sample commit together. New observations after that snapshot are normal next-pass
work. The explicit predecessor prevents a retiring process from downgrading a
newer active version. Reapplying 049 preserves the active pointer.

The causal source/measurement joins, earliest MATCH/NO_MATCH cohorts, all tied
members, earlier UNKNOWN blocking, ALL versus individual-coin scopes, four
measurement horizons and eight v7 barriers are unchanged. OPEN measurement
windows remain OPEN even after an early first touch. All-scans formula evidence
remains descriptive and cannot trigger discovery, promotions or delivery.

The worker still processes at most 16 coins per 30-second pass with atomic
cursor/queue/catalog/results, per-coin savepoints and durable retries. The existing
enable flag and health key are retained; health now reports both processing and
active evaluation versions. The old v1 PostgreSQL regression suite still runs on
its frozen adapter against the shared upgraded schema.

## Validation and remaining coverage

Pure tests verify active denominators, zero versus missing data, exact rounding,
source direction, consensus ties, original integrity checks, inverse orientation,
legacy decision equality and deterministic payload hashes. Isolated PostgreSQL
tests compare real producer outputs, preserve v1 while v2 is partial, prove atomic
activation/rollback, block premature activation for un-enqueued receipts and keep
version-specific cohorts separate. Four sampled production observations (BTC and
HYPE, earliest and latest scan) have valid source proof and unchanged legacy
decisions before rollout. Full CI and production verification follow deployment.

Remaining candidates must be classified by applicable feature contracts:
per-timeframe liquidity and selected/opposite differences need a timeframe
feature key while sharing the same price outcome; causal prior-price features
need a separate lookback adapter; Combined/top-item and confirmation predicates
cannot be synthesized from absence of delivery. Missing 15m/1h/4h MaxPain
timeframes must not be substituted with 12h/24h/month horizons. New component
hypotheses remain deferred.
