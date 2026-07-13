# Stage 42 — Commands and 24h Fix

- Removed `/alert`; only `/alerts` remains.
- No command or scan starts automatically after deploy/restart.
- Old queued Telegram commands are discarded during webhook setup.
- Telegram webhook replies immediately, so long scans are not resent.
- Duplicate Telegram update IDs are ignored.
- `/watch_on` remains the only path that creates the Watch loop.
- `/watch_status` never starts a scan.
- `/watch_stop` stops the loop.
- Removed the unused scheduled collection function.
- 24h is retried up to four times on fresh browser pages.
- Other rejected timeframes are retried twice.
- A timeframe is marked missing only after all retries fail.
