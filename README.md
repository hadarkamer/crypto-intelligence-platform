- Combined Price+OI conclusions compare the Regime with the price direction implied by the Max-Pain pain-side label, while preserving the existing LONG/SHORT label and display wording.

## Stage 79 — Compact alert layout and 0.8% display threshold
- The minimum distance for a primary Max Pain opportunity is now 0.8%.
- Below-threshold rows remain visible in the seven-timeframe summary with a yellow marker, actual proximity percentage, and directional score.
- Alert/Watch headers and counter-direction values use a compact one-line layout.
- Price+OI windows retain Price/OI strength detail in a compact three-line block per timeframe.
- Max Pain scoring formulas are unchanged.

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

## Stage 62 — BTC Directional Alignment
- Altcoin consensus: 0–15 points.
- Same-timeframe BTC confirmation: continuous 0–15 points from BTC total score.
- Opposite-timeframe BTC direction: continuous penalty up to 10 points.
- BTC itself: consensus only, scaled to 0–30.
- Market breadth remains display-only and is excluded from scoring.

## Stage 77 — Price + OI Regime
Stage 77 adds an independent 30-minute Price + Open Interest context layer using CoinGlass V4. It does not change the existing alert score. Only `COINGLASS_API_KEY` is required in Render; the bot stores raw Price/OI changes and a five-state classification for later calibration.

## Stage 77 historical Price+OI reference
For BTC, ETH, SOL, HYPE, DOGE, ZEC, BNB and XRP, `/oi_backfill` stores a separate 30-day Price+OI history and `/oi_stats SYMBOL` shows P25/Median/P75/P90/P95 by 30m/1h/4h/12h/24h.
The live Price+OI Regime now uses P25 as the minimum valid movement for the same symbol and timeframe. Strength labels are: Weak/Noise, Normal, Elevated, Strong, Extreme. Price and OI strengths remain separate. Max-Pain scoring is unchanged.

## Stage 87 — Data Foundation

Stage 87 adds only isolated historical-data foundations and validation. It does
not change alert scores or trading decisions.

- `/oi_backfill [180|365]` refreshes Price+OI history and reference ranges.
- `/flow_backfill [180|365]` stores official aggregated Futures and Spot Buy/Sell + CVD
  Buy/Sell 30m history. CVD is not calculated yet.
- `/oi_validation SYMBOL` displays Price/OI timestamp and reference quality.
