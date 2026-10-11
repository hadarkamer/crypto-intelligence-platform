# External activity correction — verification

Status: tested candidate; not deployed to either live Testnet account.

## Candidate

- Source commit: `4d818385732cfaf67dab78669cd3354c84ba19f2`.
- Tested tree: `4856e40e7a4680584f8073f9a2bff4473d9f64c5`.
- Source/dependency manifest: `8e0003713e6fb010a5c4b86722b13f346831c246e62fc0bcb2725f151e4cc829` (530 files).
- Isolated test commit: `8d561a17ccf6d1c1b64ae19ab3595fc108101d35`.
- Isolated Render deployment: `dep-db4c6svlot8c738hk5kg`, completed 2026-10-09.
- This verification note is a documentation-only follow-up; it changes no tested source or dependency.

## Behavior

External orders, fills and current positions are recorded independently of bot requests. Their external origin alone does not block the whole account. An externally occupied coin is kept separate from new bot entries.

External intervention in an active bot coin enters explicit human management. Automation for that coin pauses: the bot does not resize, cancel, recreate or override its exchange orders. Other coins can continue, subject to the existing real safety and exchange constraints. Previously sent bot requests still require resolution.

Two distinct complete observations confirming no position, no remaining coin orders and no unresolved bot request finish the old trade as `MANUALLY_CLOSED`. A later independent alert may enter that coin. Reports retain actual exchange quantity separately from historical attributable bot fills; manual allocation and PnL are not invented.

Known newer same-coin feed events invalidate a prepared exit, including events after reconnect. Conflicting immutable execution facts retain the existing safety checks. System-specific stop cancellation still follows the original protection repair path.

## Verification

- Guarded local full suite: 2,955 tests, zero failures/errors/network attempts; 553 PostgreSQL cases skipped locally.
- Existing isolated Render harness: fresh loopback PostgreSQL 18, synthetic exchange only.
- Full isolated suite: **2,955 tests, zero failures, errors, skips or unexpected successes**.
- Final harness status: `PASSED`; durable checks `COMPLETED`; PostgreSQL stopped successfully.
- 28 timing tests passed; recorder/feed/runtime stages passed, including 80 runtime trace equality checks.
- Explicit PostgreSQL regression covers manual partial close, persisted handoff, reconstructed store/provider after restart, full manual close, two-observation release, archival, another restart and a later same-coin trade.
- No live exchange, live account database, Telegram send or live bot deployment was used for verification.

## Synthetic request consumption

These are bounded synthetic scenarios, not live account telemetry or a four-account capacity test. Quota constants and existing entry replay counts are unchanged.

| Scenario | Baseline reads / weight | Candidate reads / weight |
| --- | ---: | ---: |
| Prepared entry | 3 / 42 | 3 / 42 |
| Protected position, 60 seconds | 14 / 174 | 14 / 174 |
| Two empty accounts, 60 seconds | 4 / 44 | 6 / 84 |
| Due activity hint on inactive account | 3 / 43 | 6 / 85 |

Complete already-funded account fill intervals add zero requests. An eligible uncovered empty history interval adds one read / 20 units; pagination can cost more. Optional history is deferred during protection, uncertain requests, prepared entries, expired observations or budget waits.

## Limits and deployment

- Recent external event history is capped at 512 events, with an eviction count; recent fill identities are bounded. This is not a permanent complete manual trading archive.
- An unfilled manual order created and canceled entirely between snapshots may not be observed.
- An external actor cannot always be identified as a particular human; external activity is not falsely attributed to a bot request.
- Internal reports expose the warning and actual position. Telegram display/notification transport remains unchanged and is not connected to the new engine by this patch.
- Direct durable ingestion of WebSocket fill payloads remains a separate proposed change.
- No strategy formulas, quota constants, real-account support or outbound IP configuration were changed.
- Per `AGENTS.md`, activating this specific tested change requires explicit user deployment approval. Neither `main` nor `paper-trading-v1` was updated.
