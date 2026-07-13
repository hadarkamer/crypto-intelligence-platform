# Stage 41 — Strict Manual Alert and Watch

This version removes all automatic Alert and Watch startup behavior.

## Startup
- Deploy/restart creates no Watch task.
- Legacy persisted `watch_enabled` state is forcibly reset to 0.
- No Watch runtime is restored.
- No scan can start from startup code.

## /alert and /alerts
- Run only after a direct Telegram command.
- Exactly one live scan per command.
- Duplicate manual Alert scans are blocked.
- Alert never starts, stops, cancels, resumes, or modifies Watch.
- Alert and Watch only share the browser lock.

## /watch_on
- The only code path that creates WATCH_TASK.
- Creates one loop only.
- Repeated calls cannot create a second loop.
- First scan starts immediately.
- Next scan begins 15 minutes after the previous scan finishes.

## /watch_stop
- Cancels the active Watch scan and the single Watch loop.

## /watch_status
- Returns status only.
- Never starts or schedules a scan.
