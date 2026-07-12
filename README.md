# Stage 37 — Rebuilt Watch, Live Alerts and New Score

## Watch
- Starts only after /watch_on.
- Runs immediately and then every 15 minutes indefinitely.
- Stops only after /watch_stop.
- Every scan sends all results with Score >=70 (up to 10).
- If none reach 70, the highest-scoring result is still sent.

## /alerts
- Uses a fresh live scan and is independent of /collect.
- If Watch is scanning, /alerts pauses/cancels that scan temporarily.
- After /alerts finishes, Watch resumes automatically.

## Score (100)
- Directional Alignment: 35
- Target Attraction: 35
- Target Clustering: 25
- Relative Gap Advantage: 5

## Adjusted liquidity
The long timeframe amount already contains liquidity visible in shorter
timeframes. The new calculation uses only positive incremental liquidity
between consecutive timeframes, normalized by sqrt(delta hours). It becomes a
moderate 0.90..1.10 multiplier rather than a separate large score.

## Dynamic Max Pain distance
- BTC: 2.5%
- ETH: 2.7%
- CoinGlass ranks 3-10: 3.0%
- ranks 11-20: 3.5%
- remaining tracked coins: 4.0%

## Display
- One Score for the current timeframe at the top.
- Average Score across all seven timeframes at the bottom.
- Market score is continuous.
