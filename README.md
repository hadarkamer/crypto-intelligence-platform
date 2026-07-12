# Stage 39 — Persistent Watch Across Restarts

Implemented:
- Watch starts only after /watch_on on a fresh installation.
- Once manually enabled, its state, chat ID and next scan time are stored.
- Render/service restarts restore the same Watch loop automatically.
- A restart does not count as /watch_stop.
- Only /watch_stop permanently disables Watch.
- Exactly one Watch task exists in each running process.
- /watch_status responds immediately without waiting for scraping locks.
- Status displays Israel time and a countdown to the next scan.
- Last scan/result state is persisted for status after restart.
- Telegram pending updates are preserved during webhook resets.
