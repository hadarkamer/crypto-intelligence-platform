# Watch restart cadence and Telegram timeout incident

User report: five notification rounds in one hour, a shared Watch `Timed out` error, notifications at :01/:31 and CVD one candle behind.

## Confirmed production evidence (UTC; Israel UTC+3)

| Watch started | First event decision | Trigger |
|---|---|---|
| 09:02:15 | 09:06:20 | Scheduled |
| 09:12:52 | 09:16:00 | Deployment restart, PR 24 |
| 09:32:15 | 09:36:08 | Scheduled |
| 09:37:45 | 09:40:49 | Deployment restart, PR 25 |
| 09:51:02 | 09:54:15 | Deployment restart, PR 26 |

Three operator deployments added three immediate scans to the two scheduled scans during 12:00–13:00 Israel time. `watch_loop` skipped its scheduling wait for the first cycle, including supervisor restoration after every deployment. These were distinct cycles, not evidence of duplicated database rows.

The two failed rounds reached Telegram delivery: SOL Magnet events at 09:41:13 and 09:54:39 were recorded as DELIVERY_FAILED; other events from each same scan had already been delivered. The generic Watch exception path did not log the original exception/phase. The user's TimedOut message is consistent with that delivery failure, but the historical logs do not retain its transport exception or prove whether restart-related load caused it. Telegram's installed client used the default five-second read timeout. A failed SOL report raised out of the shared cycle, preventing subsequent symbols from being sent.

The 10:58:02 restart scan sent its first event at 11:01:22 with CVD closing 10:30. The scheduled 11:02:15 scan used CVD closing 11:00 and sent at 11:06:31. The 11:28:48 restart scan sent at 11:31:54 with CVD closing 11:00. CoinGlass's next 11:30 close was collected by 11:34:24. The immediate scan also ran across the 11:32:15 slot, so the coordinator subsequently waited until the next half hour. At 11:45 the latest production scan used CVD closing 11:30 for all Top8. This evidence shows off-schedule startup scans before the two-minute closed-candle grace, not a persistent half-hour collector lag.

Sources: Render deploy history, indexed `research_max_pain_snapshot_sets`/`research_events` queries and `[formula-timing]`/`[flow-live]` application logs. No manual Telegram messages, scans or research runs were triggered during diagnosis.

## Fix contract

- Automatic coordinator restoration waits for the next UTC :02:15/:32:15 slot (current settings). Explicit user Watch start commands retain their existing immediate-first-scan behavior.
- A single monotonic `bot_settings` row atomically claims automatic slots across simultaneous server instances. Already-claimed or older slots cannot be replayed. Database claim failure skips that slot rather than sending an unguarded duplicate. Crash after claim may leave an incomplete round; recovery waits for the next slot instead of replaying potentially accepted Telegram messages.
- A failed Magnet report records its symbol/error class and continues to the remaining subscribers. A Telegram timeout is UNKNOWN, not claimed as definitely undelivered. No automatic resend is introduced.
- Telegram client read/write timeouts are 20 seconds, connect timeout 10 seconds. This reduces sensitivity to the old five-second limit; it cannot guarantee external Telegram availability.
- Partial delivery gets a precise summary. Collection failure and general-delivery failure remain visible with their respective phase. Health exposes the next slot, last claimed slot, result and per-symbol delivery errors.
- CVD timestamp interpretation, grace period, formulas, scores, capture population and research outcomes remain unchanged.

## Verification

Local restart/delivery regression checks: 11 cases, of which 10 pass locally and one requires explicit local/CI PostgreSQL. Includes concurrent claims with fresh SQLite connections, repeated restarts, closed-candle boundary, claim collision/failure, cycle error, explicit user start, cancellation, timeout without retry, continuing other symbols and partial-cycle health/message semantics. PostgreSQL case uses a disposable local test database and is required in CI. Existing operational score capture and runtime scheduler checks pass locally. Full CI and production evidence are recorded in the incident continuation checkpoint after deployment.

## Research continuation

Stage 7 operational-score capture remains deployed; statistical Stage 8 has not started. Resolve this incident before continuing the cluster/CVD research plan. Prior checkpoint branch retains Stage 5/6/7 evidence.
