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
For BTC, ETH, SOL, HYPE, DOGE, ZEC, BNB and XRP, `/oi_backfill` stores a separate Price+OI history (180 days by default) and `/oi_stats SYMBOL` shows P25/Median/P75/P90/P95 by 30m/1h/4h/12h/24h.
The live Price+OI Regime now uses P25 as the minimum valid movement for the same symbol and timeframe. Strength labels are: Weak/Noise, Normal, Elevated, Strong, Extreme. Price and OI strengths remain separate. Max-Pain scoring is unchanged.

## Stage 87 — Data Foundation

Stage 87 adds only isolated historical-data foundations and validation. It does
not change alert scores or trading decisions.

- `/oi_backfill [180|365]` refreshes Price+OI history and reference ranges.
- `/flow_backfill [180|365]` stores official aggregated Futures and Spot Buy/Sell + CVD
  Buy/Sell 30m history. CVD is not calculated yet.
- `/oi_validation SYMBOL` displays Price/OI timestamp and reference quality.

## Stage 88 — Read-only CVD Flow Engine

Stage 88 adds `/flow_state SYMBOL` and `/flow_stats SYMBOL`.
It reads the Futures/Spot Buy-Sell and official CVD history saved by Stage 87.2,
compares CVD changes with P25/P50/P75/P90 baselines for the same symbol, market
and timeframe, and produces Momentum/Trend/Structure Flow conclusions. It does not
modify Alerts, Watch, Max-Pain score or LONG/SHORT selection.


## Stage 89.1
Market Evidence uses unweighted agreement and displays eight Futures/Spot CVD windows. Price+OI now supports 48h, 72h and 7d.

## Stage 90.3 — ordered display sections
Alert and Watch cards are separated into Max Pain, Price+OI, Futures CVD, Spot CVD, and a final combined summary. Two redundant explanatory notes were removed. Calculations are unchanged.

## Stage 90.4 — ordered display + Price+OI Early Shift conflict
- Preserves the ordered alert display sections from Stage 90.3.
- Price+OI Early Shift against the expected trade direction now creates Conflict, matching Futures Early Shift behavior.
- Price+OI Early Shift aligned with the trade does not block Confirmation.
- Spot remains secondary information only and does not participate in Confirmation or Conflict.

## Stage 91 — Live Futures/Spot CVD freshness

- Futures and Spot CVD are refreshed automatically every 30 minutes when `COINGLASS_API_KEY` is configured.
- The refresh remains separate from Max-Pain DOM scans and does not share DOM snapshots between commands.
- Each market is stored in its own table; `(symbol, candle_time)` primary keys prevent duplicate candles.
- The downloader keeps a one-candle overlap so the latest boundary candle can be refreshed safely through UPSERT.
- Flow sections now show the latest stored UTC candle, its age, and a stale warning when it exceeds the configured freshness tolerance.
- Render logs now print one refresh summary for every symbol and for both Futures and Spot.
- Optional environment settings:
  - `FLOW_COLLECTION_INTERVAL_MINUTES` (default `30`)
  - `FLOW_FRESHNESS_TOLERANCE_MINUTES` (default `35`)

## Stage 91.1 — Fast CVD refresh
The automatic Futures+Spot CVD refresh checks every 5 minutes by default. CVD freshness is displayed from the close of the official 30-minute CoinGlass candle, avoiding an artificial extra 30 minutes of age.


## Stage 92 — Directional alert and separate confirmation

- `/alert SYMBOL long|short` uses the existing full directional scoring and the normal alert card layout.
- Confirmation/strong-confirmation/conflict transitions can produce a short separate Telegram message.
- Repeated unchanged statuses are suppressed per symbol, timeframe and direction during the running process.

## Stage 97 — accurate session-composition baselines

- ACTIVE/WEEKEND ratios are split at exact New York session boundaries.
- Price, OI, Futures CVD and Spot CVD use historical windows with a similar
  ACTIVE/WEEKEND composition instead of splitting observations or blending
  percentile thresholds.
- Weighted and ordinary percentile calculations now use one consistent linear
  interpolation definition.
- Open 30-minute CVD candles are excluded until close plus a two-minute grace
  period, including protection from legacy open rows already in the database.
- Historical Price/OI windows with missing 30-minute references are skipped
  instead of being compared by row index.

## Stage 103 — synchronized Watch, Magnet Watch and Liquidity V2

- `/watch_on` and `/watch_on_top8` run through one deadline-anchored 30-minute
  coordinator. A cycle starts immediately on activation; following cycles are
  aligned to UTC half-hour boundaries plus the closed-CVD grace period.
- Max Pain DOM collection and the single-flight Price+OI/CVD refresh run in
  parallel. Alert calculation starts only after both paths finish or the
  bounded derivatives timeout is reached. Existing freshness gates still
  exclude unavailable or stale evidence.
- `/watch_magnet_v1 SYMBOL` subscribes Magnet to that same snapshot and does
  not start another DOM browser or derivatives collector. Use
  `/watch_magnet_v1_status` and `/watch_magnet_v1_stop [SYMBOL]` to inspect or
  stop it.
- `/oi_refresh` forces one paired live Price+OI snapshot. It never runs the
  isolated historical Backfill.
- The automatic historical refresh remains due every 24 hours and refreshes
  the latest three days. A partial symbol set does not advance that clock;
  requests retry with backoff and a PostgreSQL advisory lock prevents a second
  service instance from running the same Backfill.
- Magnet Liquidity V2 treats the first available timeframe as a named baseline,
  subtracts cumulative overlap from later timeframes and normalizes each added
  layer by the square root of its added hours. Liquidity Edge is amount-weighted
  and Consistency reflects the strength of conflicting layers.
- Distance/reachability is intentionally absent from Liquidity V2. Magnet
  Quality, the legacy score, legacy alert thresholds and existing Max Pain
  distance/proximity calculations are unchanged.
- `/market_state BTC [LONG|SHORT]` is registered as a Telegram command and
  reports the latest stored Price+OI, Futures CVD, Spot context and Confirmation.
