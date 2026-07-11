# Stage 23 — Adjusted High Liquidity Close Distance Score

Implemented:
- HIGH_LIQUIDITY_CLOSE_DISTANCE is now a score component: 0..10.
- It remains an alert type when score is at least 6/10.
- Liquidity Density remains excluded.
- No historical data is required.

Timeframe adjustment:
adjusted_liquidity =
near_liquidity / sqrt(timeframe_hours)

Timeframe hours:
12h=12, 24h=24, 48h=48, 3d=72, 1w=168, 2w=336, 1m=720

Adjusted ratio:
adjusted_near_liquidity_ratio =
current adjusted liquidity /
average adjusted liquidity of the same coin across current snapshot timeframes

Scoring, only when distance <= 1%:
- <1.10: 0
- 1.10-1.29: 2
- 1.30-1.59: 4
- 1.60-1.99: 6
- 2.00-2.49: 8
- >=2.50: 10

Final raw maximum:
75 points

Components:
- Proximity: 0..20
- Directional Alignment: 0..20
- Target Clustering: 0..15
- High Liquidity Close Distance: 0..10
- Liquidity Balance: -10..+10

Tests:
1. /collect
2. /alert_check 10
3. Verify Adjusted Near Liquidity Ratio
4. Verify High Liquidity Close Distance score
5. /watch_now
