# Stage 81 — Persisted Backfill Freshness Check

## Change
The automatic historical Price+OI backfill no longer runs unconditionally after every deploy and then sleeps for 24 hours.

At startup, after the existing stabilization delay, the service reads the latest completed backfill timestamp from persistent storage.

- If no completed run exists, or the last run is at least 24 hours old, one automatic backfill runs.
- If the last run is newer than 24 hours, the download is skipped.
- The service checks freshness hourly so a continuously running deployment still refreshes when the 24-hour threshold is reached.
- Manual `/oi_backfill` runs are recorded too and reset the 24-hour freshness clock.
- The existing overlap lock remains active.

## Unchanged
- Max Pain labels and composite conclusion text/logic.
- HYPE API behavior.
- Max Pain scoring and dynamic size/rank bands.
- Live Price+OI collection.
