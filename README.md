# Stage 44 — Timeframe Integrity and Collect Audit

- CoinGlass is collected internally in the order:
  24h, 12h, 48h, 3d, 1w, 2w, 1m.
- 24h establishes the default-page baseline.
- 12h must produce a different fingerprint and can no longer reuse 24h data.
- 12h and 24h receive extended polling and two clean-page retries.
- Public output order remains 12h, 24h, 48h, 3d, 1w, 2w, 1m.
- Alerts and Watch score only symbols present in all seven timeframes.
- /collect saves only complete seven-timeframe symbols.
- /collect reports expected and actual database writes and incomplete symbols.
- The Alerts waiting message now clearly states that Alerts waits for Watch.


## Stage 45 — defaultdict hotfix
- Added the missing `from collections import defaultdict` import to `main.py`.
- Fixes the shared NameError affecting `/alerts`, `/collect`, and Watch cycles.


## Stage 46 — Alert display layout
- Added current Binance price to every alert card.
- Added the nearest Max Pain target price.
- Moved the all-timeframe average score directly below the current timeframe score.
- Kept the current timeframe score as the primary score.
- Removed the duplicate average score line from the bottom of the card.


## Stage 47 — Scoring rebuild
- Applied Stage 46 alert display changes.
- Replaced Target Attraction with Target Proximity.
- Rebuilt Cluster Confidence with two minimum-three-timeframe gates.
- Added transition-specific liquidity growth thresholds.
- Increased Relative Gap to 10 points.
- Directional Alignment is now 30 points: 15/8/7.
