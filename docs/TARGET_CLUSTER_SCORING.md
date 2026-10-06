# Exact-target MaxPain cluster attribution

From this change onward, every MaxPain target is scored using the strongest
valid same-side cluster containing its exact quoted target price. A cluster
elsewhere on the same side does not contribute any component to this target.

The existing candidate rules are preserved: the closest active side of each
timeframe supplies at most one target, at least two timeframes are required,
and a valid contiguous price window has full width at most 1% of its mean.
All valid windows are considered, including nested windows. Selection among
windows containing the target uses the existing ranking: final points, member
count, narrower spread, then represented liquidity. Density, coverage and the
adjacent-timeframe liquidity-growth multiplier are unchanged.

Membership means exact equality of the quoted numeric price. An identical
price represented by another timeframe qualifies. Merely falling between
the bounds, within a tolerance, or on the same side does not. A target whose
timeframe supports the opposite side can qualify only through an exact price
already represented in the candidate cluster. No eligible cluster gives zero
confidence, density, coverage, growth and multiplier components.

Primary scores, opposite scores, forced directions, timeframe averages,
TARGET_CLUSTER labels and captured research components use this rule.
Captured slots and displayed results carry TARGET_EXACT_PRICE_V1 provenance;
member target prices accompany cluster members in capture/output. Existing
archive bundles already record the alert_engine.py SHA, so old immutable
observations remain identifiable and are not rewritten or relabelled.

This change does not alter trading execution, subscriptions, collectors,
timers, liquidity-growth thresholds or previously persisted history.

Verification: target_cluster_score_selftest.py covers a weaker relevant
cluster beside a stronger unrelated one, isolated targets, exact same-price
membership in another timeframe, rejection of between-bounds and near-equal
prices, nested candidate windows, missing/invalid evidence, opposite scoring,
forced output, averages and frozen capture consistency.
