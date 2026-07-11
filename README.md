# Stage 27 — Aggregate Market Schema and Collect Fix

Implemented:

1. Market is no longer a binary per-timeframe label.
2. Market is calculated from the complete current snapshot:
   all valid saved assets × all available timeframes.
3. Each alert receives the percentage of all market indications
   that support its direction.

Market score:
- support <= 50%: 0/3
- support 50%..100%: linear 0..3

Formula:
market_points =
max(0, (market_support_pct - 50) / 50 * 3)

Example:
- 50% support: 0/3
- 60% support: 0.60/3
- 75% support: 1.50/3
- 100% support: 3/3

Additional fixes:
- /collect shows only the six agreed daily commands.
- Filtered row validation now uses:
  saved symbols × 7 timeframes.
- A valid 33-symbol snapshot with 231 rows no longer receives
  the false warning "Expected around 350 rows, got 231".
- Existing incorrect validation_errors disappear after the next /collect.

Daily commands:
- /collect
- /alerts
- /coin BTC
- /watch_on
- /watch_status
- /watch_stop
