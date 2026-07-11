# Technical Specification — Alert Score v2

## Final calculation

### 1. Proximity — 0..20
Uses Binance price only.

distance_pct = abs(max_pain_target - binance_price) / binance_price * 100

- <= 0.25%: 20
- >= 2.00%: 0
- linear interpolation in between

### 2. Directional Alignment — 0..20
One combined component:
- Consensus: 0..15
- BTC Like: 0..5

BTC Like scores only when the coin has consensus of at least 5 timeframes.
It reinforces an internally coherent signal; it cannot rescue a split signal.

### 3. Target Clustering — 0..15
Uses targets on the coin's dominant side.

cluster_spread_pct =
(max_target - min_target) / average_target * 100

Requires at least 3 timeframes:
- <=0.50%: 15
- <=1.00%: 12
- <=2.00%: 8
- <=3.00%: 4
- >3.00%: 0

### 4. Liquidity Density — 0..20
Historical normalization for the same coin, timeframe and side.

relative_liquidity =
current_near_liquidity / historical_median_near_liquidity

density =
relative_liquidity / max(distance_pct, 0.10)

Points:
- >=4.0: 20
- >=2.5: 16
- >=1.5: 12
- >=1.0: 8
- >=0.5: 4
- below 0.5: 0

At least 3 historical samples are required.
Without a baseline, this component gets 0 and a yellow quality note is shown.

### 5. Liquidity Balance — -10..+10
balance = (near_liquidity - far_liquidity) /
          (near_liquidity + far_liquidity)

balance_points = balance * 10

Equal liquidity gives 0.
A significantly weaker near side subtracts points.

### 6. Final Priority
Raw maximum = 85:
20 + 20 + 15 + 20 + 10

priority = clamp(raw_score / 85 * 100, 0, 100)

## Explicitly excluded from score
- Setup Strength
- Data Quality
- Number of alerts for the same coin
- Historical direction persistence
- General market bias

## Data quality
Shown only as a Hebrew note at the end:
- yellow: partial/non-critical
- orange: materially reduced reliability
- red: critical data problem

## Multiple alerts
Every coin/timeframe alert stays separate.
Each card states:
- other same-direction timeframes
- opposite-direction timeframes

No bonus or penalty is applied.

## File roles

- main.py:
  Telegram commands, database access, collection orchestration, watch loop,
  history baseline calculation and alert rendering.

- alert_engine.py:
  Alert Score v2 formulas, alert types and ranking.

- analysis.py:
  Shared calculations: consensus, BTC similarity, gap, market summaries.

- coinglass_dom_reader.py:
  Playwright DOM collection and timeframe verification.

- live_price_provider.py:
  Binance Futures Mark/Spot prices and distance recalculation.

- decision_engine.py:
  Older Setup Strength engine. Kept for /score and /score_top only.
  It is not part of Alert Priority.

- hyperliquid_reader.py:
  Diagnostic Hyperliquid browser probe. Not part of Alert Score yet.

- env.example:
  Environment variable template.

- requirements.txt:
  Runtime dependencies.

- runtime.txt:
  Python runtime version.

- README.md:
  Deployment and test summary.

- __init__.py:
  Empty package marker. It should remain empty.

## Removed files
No .pyc files and no __pycache__ directories are included.
Compiled Python files are deployment artifacts and must not be committed.
