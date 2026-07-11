# Stage 29 — Manual Watch Start and Single Snapshot

Watch:
- Always starts OFF after deploy/restart.
- No automatic scrape occurs after deploy.
- /watch_on is required for each new runtime session.
- First scan begins immediately after /watch_on.
- Further scans run every 15 minutes.
- Every completed cycle sends a Telegram summary.
- /watch_stop cancels an active scan.

Collect:
- /collect remains a manual refresh for /alerts and /coin.
- The previous stored snapshot is deleted before the new one is saved.
- No collection history accumulates.
- Watch scans stay in memory and are not saved.

Daily commands:
- /collect
- /alerts
- /coin BTC
- /watch_on
- /watch_status
- /watch_stop
