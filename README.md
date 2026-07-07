Crypto DOM Display Fix Patch

Fixes:
- Removes Hour column from Telegram displays.
- /coin shows only the latest snapshot across all timeframes.
- /range shows only the latest row for the requested coin/timeframe.
- Removes empty delta columns.
- Keeps CoinGlass-provided distance percentages instead of recomputing/overwriting them.
- Filters known non-crypto symbols such as XAU and MU.
