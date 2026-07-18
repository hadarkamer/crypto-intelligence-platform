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


## Stage 48 — Minimum tradable distance display filter
- Internal scoring is unchanged.
- `/alerts` and Watch omit opportunities whose remaining distance is below
  `MIN_DISPLAY_DISTANCE_PCT`.
- Default threshold: `0.15%`.
- The threshold can be changed with the environment variable
  `MIN_DISPLAY_DISTANCE_PCT`.
- The Watch fallback result also respects this filter.
- `/coin` and stored data are unchanged.


## Stage 49 — Dynamic crypto price formatting
- Price calculations are unchanged.
- Binance current price and Max Pain targets use adaptive decimal precision.
- Low-priced assets such as DOGE are no longer rounded to two decimals.
- Trailing zeros are removed for readability.

## Stage 50 — TradingView technical signal Shadow Mode
- Added `POST /webhooks/tradingview` for technical indicator alerts.
- Added isolated `technical_signals` storage for SQLite and PostgreSQL.
- Added symbol/timeframe/direction normalization and duplicate protection.
- Added `GET /technical/status` and Telegram command `/technical_status`.
- Technical signals are stored and displayed only; liquidity scoring is unchanged.
- Configure `TRADINGVIEW_WEBHOOK_SECRET` before enabling the webhook.


## Stage 51 — Tradable Max Pain filtering

- Minimum displayed opportunity distance is now 0.5%.
- 0.7%–1.3% is labeled as the preferred trading-distance band.
- SHORT targets already crossed by the live Binance price are excluded.
- LONG targets already crossed by the live Binance price are excluded.
- Crossed targets do not participate in direction, consensus, clustering or scoring.

## Stage 61
Current-price calculations use Binance USD-M Futures Mark Price. HYPE is resolved as HYPEUSDT through exact matching and Binance exchange metadata.
