# Stage 75 — Directional Average and Minimum-Liquidity Alerts

## Directional average correction

The displayed average is now calculated separately for LONG and SHORT.
For an alert in direction `d`, the average is:

`Average_d = sum(Score_d(tf)) / number_of_available_timeframes_d`

A timeframe whose leading direction is SHORT therefore contributes its LONG
score to the LONG average, not its leading SHORT score. The same rule applies
in reverse to SHORT alerts.

The current-timeframe Score remains unchanged. The correction affects the
secondary average, its sorting tie-breaker, and the all-timeframe score block.

## BTC confirmation verification

BTC confirmation continues to use BTC's calculated Score from the exact same
timeframe as the altcoin alert. It does not use BTC's all-timeframe average.

## Minimum-liquidity alerts

Command:

`/alerts_liq MIN_USD [LIMIT]`

Example:

`/alerts_liq 1000000`

For each scored result:

`Total Liquidity = Near-side Liquidity + Opposite-side Liquidity`

Only results satisfying:

`Total Liquidity > MIN_USD`

are displayed. The regular `/alerts` command remains unchanged.
