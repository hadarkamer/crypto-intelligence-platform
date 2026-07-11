# Technical Specification — Alert Score v2.1

## Final score

### Proximity — 0..20
distance_pct = abs(max_pain_target - binance_price) / binance_price * 100

### Directional Alignment — 0..20
- Consensus: 0..15
- BTC Like: 0..5
BTC Like scores only when consensus is at least 5 timeframes.

### Target Clustering — 0..15
cluster_spread_pct =
(max_target - min_target) / average_target * 100

Requires at least 3 targets on the dominant side.

### Liquidity Balance — -10..+10
balance =
(near_liquidity - far_liquidity) /
(near_liquidity + far_liquidity)

balance_points = balance * 10

### Final Priority
Raw maximum = 65:
20 + 20 + 15 + 10

priority = clamp(raw_score / 65 * 100, 0, 100)

## Removed
Liquidity Density was removed from the score and display because it requires
multiple historical samples and is not reliable enough for the current stage.

HIGH_LIQUIDITY_CLOSE_DISTANCE was removed because it depended on Liquidity Density.

## Not part of score
- Setup Strength
- Data Quality
- Multiple alerts for the same coin
- Historical persistence
- General market bias

## Automatic Watch
- Runs in memory.
- Does not save a full snapshot.
- Sends Telegram only for a new alert above the threshold.
- Uses cooldown to prevent duplicate alerts.
- /watch_stop stops it.
- /watch_status shows runtime state.


## HIGH_LIQUIDITY_CLOSE_DISTANCE

This is an alert type only and does not add a separate score component.

Conditions:
- distance_pct <= 1.0
- near_liquidity_ratio >= 1.8

near_liquidity_ratio =
current near-side liquidity /
average near-side liquidity for the same coin across current snapshot timeframes

This uses no historical data and does not compare one coin's absolute dollars to another coin.


## Stage 23 — Timeframe-adjusted High Liquidity Close Distance

This component contributes 0..10 points.

adjusted_liquidity =
near_liquidity / sqrt(timeframe_hours)

adjusted_near_liquidity_ratio =
current adjusted liquidity /
average adjusted liquidity of the same coin across current snapshot timeframes

The component scores only when distance_pct <= 1.0.

Points:
- ratio < 1.10: 0
- 1.10 <= ratio < 1.30: 2
- 1.30 <= ratio < 1.60: 4
- 1.60 <= ratio < 2.00: 6
- 2.00 <= ratio < 2.50: 8
- ratio >= 2.50: 10

The alert type HIGH_LIQUIDITY_CLOSE_DISTANCE is emitted from 6 points upward.

New raw maximum = 75.
priority = clamp(raw_score / 75 * 100, 0, 100)
