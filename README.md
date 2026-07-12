# Stage 38 — Single Persistent Watch Loop

The Watch architecture was rebuilt around one task:

- No Watch manager task is created during deploy/startup.
- /watch_on creates exactly one `WATCH_TASK`.
- Repeated /watch_on calls cannot create another loop.
- The loop runs:
  scan -> send results -> wait 15 minutes -> scan again.
- It repeats indefinitely.
- Only /watch_stop cancels the loop.
- /watch_stop also cancels the active scan.
- /watch_status reads the actual asyncio task, not only a database flag.
- /alerts never cancels the Watch loop; it waits for the shared browser lock.
- Telegram pending updates are preserved across deploys.
