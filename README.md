# Stage 43 — Full Command Rebuild

## Command contract

- `/collect`: one manual full scan; saves a DB snapshot only when all seven
  timeframes are present.
- `/alerts`: one manual full live scan; does not save a snapshot.
- `/coin SYMBOL`: reads the most recent saved snapshot; it does not scan.
- `/watch_on`: the only code path that creates the persistent Watch loop.
- `/watch_status`: read-only; never starts a scan.
- `/watch_stop`: cancels the active Watch scan and the persistent loop.

## Scanner behavior

- CoinGlass/Playwright is protected by one shared lock.
- `/collect`, `/alerts`, and Watch cannot open browsers in parallel.
- Every scoring scan requires all seven timeframes.
- 24h receives longer verification polling and two clean-page retries.
- Other timeframes receive one clean-page retry.
- Partial scans are rejected instead of being scored.
- Production screenshots were removed to reduce memory use.
- Page, context, and browser are closed explicitly after every scan.

## Watch behavior

- No scan starts on deploy or restart.
- One `/watch_on` creates one task.
- A failed cycle sends a Telegram error and the loop tries again after 15 minutes.
- Every successful cycle sends all results above 70, or the single best result.
- Only `/watch_stop` ends the loop.
