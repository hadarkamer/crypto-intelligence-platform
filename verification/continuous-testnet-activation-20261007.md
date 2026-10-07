# Continuous Testnet activation — 2026-10-07

## Outcome
Deployment and continuous demo activation completed. No Mainnet authorization or action.
Verified at approximately 20:04 UTC (23:04 Asia/Jerusalem).

## Source and deployment identities
- Receiver: branch paper-trading-v1, commit b3c066967ab3d27c93b7bbaaeb20a75cd5ab1be9.
- Initial protection-only deploy: dep-db3a84d9fdbs73afvrd0, live 19:57:57 UTC.
- Continuous activation deploy: dep-db3aa8nlot8c73f4rvu0, live 20:02:40 UTC.
- Producer: branch main, commit bed88012d7a935d4964726d8826439766b481773.
- Producer deploy: dep-db3a9jrncjis73evgk90, live 20:02:08 UTC.
- Release identifier: 558b6b0549a4e60c2d7ab9893649f9d66909896911417ad43a85339790358a62.
- Durable prospective source fence: 2026-10-07T19:53:09.217Z.
- Independent blob/mode comparison: receiver 529 tested files identical and 183 parent-only files preserved; producer 347 tested files identical and 124 parent-only files preserved. No strategy/source edits beyond previously verified release.

## Initialization and cutover
Through Render's internal Web Shell:
- Verified existing Testnet journal, dispatch and request-budget schemas.
- Created the separate Testnet execution state only because absent, with exact existing long/short account and agent routes.
- Initialized additive execution history.
- Validated release configuration and legacy protection configuration.
- Legacy journal finality check returned FINAL.
- Applied the explicit producer migrations/050_approved_alert_execution_outbox.sql; verified column shape.
- Resolved enabled durable watch subscription privately; all five approved MaxPain source keys exist. No strategy flags or Telegram destinations changed.
- New shared forwarding authentication installed without printing or storing credentials in this report.
- Preserved database external-access restrictions.
- Kept candidate entries off during predecessor transition. Render marked old deploy dep-db2hbd49v7es73c64qu0 deactivated; Web Shell explicitly reported old instance pssgd unavailable and switched to zpctr. Only then recorded retirement attestation and enabled candidate entries.
- Producer auto-deploy temporarily disabled to avoid duplicate builds during promotion; restored to On Commit afterward. Receiver remains auto-deploy off.
- Environment updates themselves trigger Render deployments; no redundant manual trigger used.
- Obsolete ALERT_CARDS_FORWARD_MODE transport disabled after the receiver switched; Telegram alerts and formula configuration preserved.

## Observed final state
Receiver /healthz:
- experimental_testnet.status: EXPERIMENTAL_OWNER_RUNNING
- running: true
- new_entries_enabled: true
- process_ownership: HELD, held=true
- predecessor_retirement_operator_attested: true
- legacy_protection_retained: false
- cross_process_ownership_verified: false (this field explicitly does not substitute for operator Render evidence)

Local configuration/state read:
- domain=testnet; policy=continuous_v1; expiry=null; protection=true
- separate_accounts=true; handover_recorded=true
- sources=0, trades=0, requests=0 at final inspection

Producer:
- --check-config returned CONFIGURED with one existing subscription scope.
- Background forwarding completed repeated cycles without deferred/duplicate/recording errors.
- No new eligible source existed at inspection, so no new exchange fill or round-trip trading result is claimed.

## Scope and operational limits
- This approved v2 bridge covers the five existing MaxPain rules for SOL/HYPE/DOGE/XRP/ETH, subject to the exact Testnet contract availability and ordinary entry/protection checks.
- It does not forward older U21/R2732/HYPE_ROW71205/SOL_G65/dual-CVD paths; no claim that every Telegram formula is wired into this v2 executor.
- Existing same-account/same-coin overlap block remains. Different markets can operate concurrently.
- Source archive credentials are unnecessary for these approved v2 MaxPain messages and were not copied.
- Source price logs showed Bybit HYPE HTTP 403 fallbacks at 20:04:22 UTC, followed by successful Hyperliquid HYPE price 87.4885. Subsequent source freshness audit OK.
- Full exchange behavior still depends on forthcoming natural signals/fills. No synthetic order was injected to manufacture success.
