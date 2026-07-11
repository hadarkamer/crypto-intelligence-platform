# Stage 32 — Alert scoring and display fixes

- Liquidity Balance removed from score and moved below anomaly types.
- Near Share >=60% is green; <=40% is red; otherwise neutral.
- Score redistribution:
  - Proximity: 30
  - Directional Alignment: 20
  - Target Clustering: 20
  - High Liquidity Close Distance: 30
- BTC alerts exclude BTC Like:
  - Consensus max 15
  - Market Schema max 5
- More than one anomaly type is marked green.
- Same-direction additional timeframes are shown only when their Priority > 50.
- The obsolete 350/231 warning is suppressed.
