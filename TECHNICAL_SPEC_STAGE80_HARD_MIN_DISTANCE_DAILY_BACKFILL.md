# Stage 80 — Hard 0.8% Max Pain Minimum + Daily Historical Backfill

## Approved behavior

- The alert timeframe must have Max Pain distance of at least **0.8%**.
- Distance below 0.8% receives **0 proximity points** and cannot create an alert.
- Existing dynamic upper thresholds by coin size remain unchanged.
- Price+OI historical Backfill runs automatically every 24 hours.
- The automatic Backfill is isolated from Max Pain scoring, Alerts and Watch.
- Manual `/oi_backfill` remains available and shares the same overlap lock.
- Existing Max Pain labels and the combined conclusion text remain unchanged.

## Optional environment variables

- `HISTORY_BACKFILL_INTERVAL_HOURS=24`
- `HISTORY_BACKFILL_STARTUP_DELAY_SECONDS=60`
