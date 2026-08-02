# Stage 91.1 — Fast CVD Refresh

- Automatic Futures and Spot CVD refresh now checks every 5 minutes by default.
- The collector still stores official CoinGlass 30-minute candles; no scoring or baseline formula changed.
- The loop compensates for download duration to avoid schedule drift.
- Telegram freshness is measured from the 30-minute candle close, not its opening timestamp.
- Existing primary keys and UPSERT behavior continue preventing duplicates.
- `/flow_backfill` remains available for a manual refresh but should no longer be needed for routine freshness.
