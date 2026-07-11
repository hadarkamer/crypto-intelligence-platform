# Stage 34 — WATCH_SCAN_TASK Fix

Fixed:
- Added the missing global `WATCH_SCAN_TASK = None`.
- Ensured `watch_loop` and `watch_off` declare and initialize it safely.
- Prevented the Watch manager from entering a rapid error loop.
- `/watch_stop` can now safely cancel an active scan.
- No scoring, alert, collect, or Telegram command logic was changed.

Expected behavior:
1. `/watch_on` acknowledges activation.
2. Watch starts the first scan.
3. No recurring `NameError: WATCH_SCAN_TASK is not defined`.
4. `/watch_status` shows the live cycle state.
5. `/watch_stop` stops active scanning.
