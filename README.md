# Stage 33 — Watch Webhook and Feedback Fix

Root cause:
- /watch_on was sent during the deploy/restart window.
- Startup used drop_pending_updates=True, so Telegram commands waiting during
  the restart could be discarded before the new service handled them.

Fixes:
- Pending Telegram updates are preserved during webhook reset.
- /watch_on immediately replies that the command was received.
- Clear Render logs were added for every incoming Telegram update.
- /watch_on activation and immediate scheduling are logged.
- A Telegram application error handler now reports handler failures to both
  Render logs and the user.
- Watch still starts OFF after deploy and requires manual /watch_on.

Expected log after /watch_on:
[webhook] update received; ... text='/watch_on'
[watch] /watch_on received
[watch] manual activation saved
[watch] scan started
