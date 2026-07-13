# Stage 40 — Manual-Only Alert and Watch Rebuild

## Core rule
No Alert or Watch action runs automatically after deploy/restart.

## /alert and /alerts
- Run only after a direct Telegram command.
- Exactly one live scan is executed per command.
- Duplicate manual scans are blocked.
- They never start, stop, resume, or duplicate Watch.
- If Watch currently uses the browser, Alert waits for the shared lock.

## /watch_on
- Creates exactly one Watch loop.
- A second /watch_on does not create another loop.
- First scan starts immediately.
- After a scan completes, the loop waits 15 minutes and scans again.
- No persisted loop is restored after deploy/restart.

## /watch_stop
- Cancels the active Watch scan and the one Watch loop.
- It is the only command that stops a running loop.

## /watch_status
- Responds immediately.
- Never triggers a scan.
- Displays Israel time and countdown.

## Alert display
- Current Binance price.
- Nearest Max Pain target price.
- Target direction: up/down.
- Liquidity at risk: longs/shorts.
- Current timeframe Score remains primary.
- Average Score across all timeframes is shown at the bottom and used only
  as a secondary ordering signal.
