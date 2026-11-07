# BTC full-flow laboratory

This directory is a one-shot experiment, not a deployment or a new production scanner.

- Application base: `ec974db6c35741d765580aab7c366f42ce6b83b7` (Chat 1 isolated real-queue harness).
- Collector: `9fefed7d606fa335536b5b59eb2eef3b18c88252` (Model 3 early preparation candidate, still not deployed).
- Scope: BTC, Models 1/2/3, 12H/24H/48H, existing serial worker and batch hook.
- Entry: existing `CoinglassSheetBatch` in real Chromium. The lab page title describes the BTC action; no production button is renamed.
- Actual stages: batch hook → application collect core and HMAC tickets → HTTP → durable PostgreSQL queue → real worker → unchanged scanner subprocess and Chromium capture/reader → existing result validation → real application SQL → price input components and read-only renderer → reload.
- Controlled boundaries: source HTML, AI provider response, fixed clock, signed-in test identity, local SQL adapter instead of hosted Supabase transport, loopback wrapper instead of deployed TanStack server routing.

The default run has **no real source or AI calls and no production credentials**. Its resource numbers measure this fixture workload; they do not estimate live capacity or API cost.

## Acceptance sequence

1. Reject a model/field mismatch before a scan.
2. Activate the actual full-sheet button twice: exactly nine jobs and nine saved analyses.
3. Check model/timeframe/field/artifact identity, price inputs, persisted data and reload.
4. Repeat a terminal poll: no duplicate write. Reject a tampered ticket.
5. Advance the isolated clock beyond the cache window and run another nine-map cycle. Return a deliberately wrong observed timeframe for Model 3 / 24H. Preserve its preceding valid value while saving its siblings.
6. Resume from the same button: exactly one new job, for the failed map. Successful siblings remain unchanged.
7. Check the exact capture hash against the actual AI image input, queue evidence and application audit records. Retain controlled raw AI response and sanitized request for every child.
8. Verify all processes close and the worker advisory lock is released.

Expected total: 19 scanner children, 18 accepted results, one deliberately rejected identity, 18 application analysis records and nine final map rows. The additional dedup request creates a request mapping, not an additional scan. Status polls are not scans.

## Run and evidence

Workflow: `.github/workflows/btc-full-flow-lab.yml`, only on `test/btc-full-flow-20261101`. Requires disposable PostgreSQL on loopback; `run.py` creates `queue_lab` and `legacy_transaction_test` and refuses existing database names through normal CREATE DATABASE failure. Do not point it at a hosted or production database. No production environment is inherited by the collector process.

The artifact `btc-full-flow-evidence` contains activity logs, queue transitions, resource samples, exact input images, sanitized requests, fixture responses, saved rows, browser assertions and cleanup. UI screenshots are internal inspection evidence. Artifacts expire after 14 days; the user report and textual evidence should be retained separately.

Resource samples run every 0.2 seconds. RSS is a sum over a process tree and can count shared pages more than once. Sampled CPU omits processes that live entirely between samples. Report those limitations and do not extrapolate a nine-way or 72-way capacity from a single-worker fixture run.

This laboratory does not integrate the separate concurrency queue candidate. Hosted auth/RLS transport, real provider quality, external account interference, interrupted provider calls, multi-worker recovery and live saturation remain separate acceptance gates.
