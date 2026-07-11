# Stage 35 — WATCH_SCAN_TASK Final Fix

Fixed:
- Added exactly one top-level `WATCH_SCAN_TASK = None`.
- Preserved runtime resets inside watch_loop/watch_off.
- Confirmed both functions declare it global.
- No scoring, collection, alert, database, or Telegram logic was changed.

Expected:
- /watch_on starts without NameError.
- /watch_status shows the live scan state.
- /watch_stop cancels an active scan safely.
