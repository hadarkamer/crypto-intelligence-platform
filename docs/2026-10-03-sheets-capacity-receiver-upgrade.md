# Telegram Sheet capacity recovery — receiver deployment pending

The approved workbook `1ci_T6v2r0MeGc3ErOsaY3ftMFo94m4syGF9U94X0fQQ` was inspected directly on 2026-10-03. `Telegram_Events` has exactly 32,000 data rows. A bounded timestamp-column read found all 32,000 timestamps valid and none outside the protected sixteen-day window at inspection time. Therefore the receiver correctly refused inserts rather than overwrite recent evidence. The fourteen-day audit reported 206 missing identities at its last completed checkpoint; that checkpoint is dated, not a current final count.

The workbook allocates 8,560,228 cells. Growing Telegram capacity to 40,000 data rows adds at most 120,000 cells: 8,680,228 total, below the existing 9,000,000-cell soft guard. The patch retains that guard, all fifteen columns, the sixteen-day protection, event-key idempotency, archived history, and full PostgreSQL evidence. It does not clear, delete, shift or recreate existing rows. The receiver allocates extra rows only when a validated insert requires them.

## Required order

1. In the existing bound Apps Script project, replace `Code.gs` with the reviewed version in this branch. Preserve Script Properties, the webhook secret, workbook binding, deployment URL, access scope and response authentication.
2. Update the **existing** web-app deployment to a new version. A GitHub commit or enlarging the grid alone does not deploy Apps Script or change its hard limit.
3. Confirm the receiver version/ACK and a real durable pending Telegram payload before merging the accompanying Python publication/audit capacity changes into `main`. Do not count failed HTTP replies or deferred rows as delivered.
4. Let the existing PostgreSQL outbox replay the missing rows idempotently. No Telegram messages or exchange requests are sent for this recovery. Do not start a duplicate backfill worker.
5. Complete a new frozen fourteen-day Sheet-vs-database audit with no missing, duplicate or mismatched event identities. A successful write ACK alone is not a complete audit.

Tests: network-free Apps Script suite covers the 40,000-row boundary, protected-row preservation, expired-slot reuse, frozen audit pagination, duplicate retries, whole-batch rejection and the unchanged workbook cell guard. Python publication, reconciliation and fresh-delivery tests passed locally; PostgreSQL coverage is required in CI before merge.

This branch is a prepared repair. Apps Script has not been deployed by this turn, and the live hard limit is not claimed to be 40,000 yet. Browser fallback needs authorization because the available Google Drive connector cannot update an Apps Script deployment.
