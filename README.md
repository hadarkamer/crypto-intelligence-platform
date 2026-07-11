# Stage 36 — Complete Separation Between /collect and Watch

Implemented:
- /collect never enables, schedules, or changes Watch.
- A manual /collect has priority over the browser resource.
- A scheduled Watch cycle is deferred silently while /collect is active.
- No Telegram warning is sent merely because /collect is running.
- /watch_on during /collect activates Watch but waits for collection to finish.
- Watch and collect can no longer open two CoinGlass browsers simultaneously.
- Completed Watch cycles still send a Telegram summary.
- Pending Telegram commands are preserved during deploy/restart.
