# Stage 28 — Reliable 15-Minute Watch Cycles

Implemented:
- Watch runs on a fixed 15-minute cadence.
- Scans never overlap.
- Every completed cycle sends one Telegram summary.
- Alerts above threshold are sent as separate detailed messages.
- Failed cycles send one Telegram warning.
- /watch_status shows last cycle status, missing timeframes and top candidate.
- Startup delay is honored before the first automatic scan.
- CoinGlass timeframe loading retries were strengthened.
- If one timeframe still fails, the cycle reports it in Telegram and retries on the next 15-minute slot.

Expected behavior:
1. /watch_on
2. First automatic scan begins after the startup delay.
3. Every completed scan sends a summary.
4. Next scan is scheduled exactly 15 minutes after the prior scheduled slot.
5. /watch_stop cancels an active scan and disables future cycles.
