# Alert Score V4 — Agreed Model

## Total: 100

### Directional Alignment — 30
- Consensus: 15
- BTC Like: 8
- Market: 7, continuous
- For BTC, BTC Like is excluded and its 8 points move to Consensus:
  Consensus 23 + Market 7.

### Target Proximity — 30
Only the distance between Binance current price and the nearest Max Pain
target. Dynamic allowed-distance thresholds remain:
BTC 2.5%, ETH 2.7%, ranks 3-10 3%, ranks 11-20 3.5%, others 4%.

### Cluster Confidence — 30
Eligibility:
1. At least 3 timeframes in the same direction.
2. At least 3 targets within 1.5% of the median target.

Components:
- Target density: 12
- Participating timeframes: 8
  - 3=2, 4=4, 5=6, 6=7, 7=8
- Meaningful liquidity accumulation: 10

Liquidity transition thresholds:
- 12h -> 24h: 15%
- 24h -> 48h: 20%
- 48h -> 3d: 15%
- 3d -> 1w: 25%
- 1w -> 2w: 25%
- 2w -> 1m: 30%

Transition scoring:
- below threshold: 0
- at threshold: 0.5
- double threshold or more: 1
- between them: continuous

### Relative Gap — 10
Relative advantage of the nearest target over the farther target.

## Removed
- Target Attraction multiplication
- adjusted-liquidity multiplier
- sqrt time normalization
- Liquidity Balance as a score
