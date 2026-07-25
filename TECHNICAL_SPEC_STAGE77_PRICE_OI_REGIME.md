# Stage 77 — Price + Open Interest Regime

## Goal
Add an independent Price+OI context layer without changing any Stage 76 score or Max Pain formula.

## Data
- Price: the bot's existing live-price provider.
- Open Interest: CoinGlass V4 `GET /api/futures/open-interest/exchange-list`, using the `All` row (`open_interest_usd`).
- Authentication: `COINGLASS_API_KEY` from Render, sent as `CG-API-KEY`.
- Collection cadence: every 30 minutes.
- History retention: 60 days, to support later empirical calibration.

## Calculations
For consecutive snapshots:

`price_change_pct = (price_now - price_previous) / price_previous * 100`

`oi_change_pct = (oi_now - oi_previous) / oi_previous * 100`

Stage 77 does **not** define a market-significance threshold and does **not** assign intensity grades. Raw changes are stored so future thresholds can be learned from history instead of guessed.

## Five states
1. `BULLISH_BUILDUP`: Price > 0 and OI > 0.
2. `BEARISH_BUILDUP`: Price < 0 and OI > 0.
3. `SHORT_COVERING`: Price > 0 and OI < 0.
4. `LONG_UNWINDING`: Price < 0 and OI < 0.
5. `NEUTRAL_INCONCLUSIVE`: Price or OI is flat between snapshots.

The first observation (or missing data) is `UNAVAILABLE`, not a market state.

## Three separate layers
1. Existing Stage 76 score — unchanged.
2. Price+OI Regime — independent state plus raw deltas.
3. Composite conclusion — text only. It explains whether Price+OI confirms, conflicts with, or does not confirm the selected direction. It never changes the numeric score.

## Fail-safe
A CoinGlass/API/DB failure must never stop normal alerts or Watch. Missing Regime data is displayed as unavailable while Stage 76 continues normally.
