# Stage 26 — Watch Runtime Hardening

Implemented:
- /watch_stop cancels an active browser scan, not only future cycles.
- /collect and Watch share one browser/scrape lock.
- Watch skips a cycle silently if /collect is already running.
- First automatic scan waits 90 seconds after service startup by default.
- /watch_status shows:
  - whether a scan is active
  - last and next scan
  - opportunities and candidates
  - alerts sent
  - top score and leading coin/timeframe
- Watch logs top_score/top candidate after each completed scan.

Environment:
WATCH_STARTUP_DELAY_SECONDS=90

Tests:
1. /watch_on
2. During an active scan, run /watch_stop and confirm cancellation.
3. Start /collect, then enable Watch and confirm no second browser starts.
4. /watch_status and confirm top candidate is shown after a completed scan.
