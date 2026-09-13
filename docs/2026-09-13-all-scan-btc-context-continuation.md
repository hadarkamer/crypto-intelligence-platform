# Continuation — all-Watch BTC context, 2026-09-13

## Start here

This supersedes `docs/2026-09-13-all-scan-maxpain-continuation.md` for current
runtime state. The previous document retains intake, measurement and MaxPain
contracts and earlier evidence. Do not repeat the completed deployments or audits.

Production main: `64b5898716a9ec31bb92b7fd84ffe5c1e0247292`.
Production and tested tree: `2a4bbedd8fa6a9f4718afafb9fe3c3f15f404e4e`.
PR37: https://github.com/hadarkamer/crypto-intelligence-platform/pull/37
Tested head: `86ea6c072a89340e87bf581f2e7c67b90e0f730b`.
CI run34782050862/job103790726729 passed all 117 selftest files, including 12 new
pure and 6 new PostgreSQL18 tests. All 40 local v1/v2/v3 pure tests also passed.
Independent review identified a shared-context split-pass issue; the fix and a
late-cache-fill regression passed before the tested commit was merged.

Migration050 installed successfully after CI and before merge. SHA256:
`078ee33a947e2a45165eb2761b1a7a3447cf420e1a40394233ea408f5cff6fcf`.
Installation retained active v2 and all 24,928 existing v2 decisions.
Merge at20:56:42UTC followed the scheduled Watch completion at20:36:06.989015UTC.
Render deployment: `dep-dajgs30u01pc7394gfh0`.
Next scheduled Watch after merge: 21:02:15UTC. No manual scan or notification test.

Implementation worktree: `/workspace/scratch/1a8c1bcb93ce/all-watch-scan-prior-price`.
Continuation worktree: `/workspace/scratch/1a8c1bcb93ce/watch-state`.
Remote durable branch: `checkpoint/research-continuation-20260913`.
Design: `docs/2026-09-13-all-watch-scan-btc-context.md`.
Evidence: `docs/2026-09-13-all-scan-btc-context-verification.json`.

## Authorized plan and current position

Hadar requests one implementation step per turn, efficiently and in simple Hebrew:
1. Research from all scans — IN PROGRESS. This step added24 existing BTC-context definitions,82→106.
2. Continuous new-formula discovery — NOT STARTED by this step.
3. Historical archive completion — pending.
4. HYPE integration completion — pending.
5. Formula validation on new cases — pending.

Completed stages: PR33 immutable Watch intake; PR34 causal measurements; PR35
total-model formulas/comparisons; PR36 MaxPain aggregates; PR37 shared priorBTC
context. The same 298 definitions and conditions remain frozen, including original
normal/inverse identities and aliases.106 are now supported; 192 remain unsupported.
Full original catalog SHA256 is still
`b3ed42745e935ea1295267afb70135b97d1d3534579ec5f8070b61929e862082`.
No new formula was invented or promoted in this step.

The remaining contracts are documented in the design. Of the original 76 prior-price
definitions, 24 BTC-only definitions are implemented and 52 own-price/relative-price
definitions remain. Other groups: timeframe liquidity34, Combined top-item22,
captured confirmation16, sequence46, deferred MaxPain components10, same-timeframe
score difference2, absent15m/1h/4h MaxPain8 and standaloneCombined2 (total192).

Next bounded step: choose the next exact applicable contract using this classification,
with particular attention to causal own-price/relative-price or score-change history.
Do not blindly connect all192. HYPE's current outcome is Hyperliquid PERP; an own-price
feature explicitly defined onSpot cannot silently use that PERP path. LegacyHYPE
Spot cache presence alone does not establish an approved equivalent all-scan contract.
BTC context avoids that issue and is valid for all 8 coins. Per-timeframe liquidity
cannot be replaced by an arbitrary coin average or maximum; Combined/captured
confirmation statuses cannot be synthesized from missing delivery.

Cluster/CVD component hypothesis research, B4 and240m family normalization remain
deferred. Do not start topic2 or trading execution without changing the task scope.

## Current version and frozen context

Core: `research_watch_scan_formula_btc_context.py`.
Worker: `research_watch_scan_formula_worker.py`; migration050.
Evaluation: `watch-scan-formulas-v3-btc-context`.
Feature: `watch-captured-total-maxpain-and-btc-context-features-v3`.
Shared context: `watch-prior-btc-spot-closed-1m-v1`.
Previous v1/v2 cores and frozen evidence remain unchanged.

Two new predicate fields:
`historical.closed_1m.1h.btc_direction` and
`historical.closed_1m.4h.btc_market_regime`.
The first uses exact prior-hour return sign (UP/DOWN/FLAT). The second reuses the
fixed 0.50 range-efficiency rule from `research_past_price_features.py`:
`past-price-spot-1m-v2-regime` / `closed-range-efficiency-50pct-v1`.
FLAT means exactly zero return; RANGE is a separate regime label. Missing is neither.

The cutoff is floor(source usable time,minute), excluding the current minute.
Do not use the later outcome entry cutoff, which would leak prices after the scan
decision. Only archivedBTC Spot candles are read; exact closed1m continuity is
required independently for60 and240 minutes. One BTC240-minute path is shared
across all selected coin jobs for a scan. No providers, own-coin paths, outcome
or BTC membership reads enter feature calculation.

Context proof binds source/population/consumer versions, scanID, bundle and parent
hashes, usable time, cutoff, exact source/window metrics and calculation versions.
Computation time is validated but excluded from deterministic context identity.
Each v3 coin sample stores `btc_context_provenance` and 212 decisions =106×2 base
directions. The two BTC state values are absolute and equal for both base directions;
inverse formulas preserve base predicates and flip only outcome direction.

The first committed READY coin freezes the entire scan's context. Split passes
and retries read that validated sibling context instead of rebuilding from a later
cache state. A missing 4h window can coexist with a known 1h window; late archive
completion never changes the already-frozen context for other coins. READY refers
to completed decision coverage, including explicitUNKNOWNs, not universal source
availability. Database read errors are retryable, distinct from missing prices.

Worker budget remains 16 coins per30 seconds. Each distinct scan's context read is
protected by a savepoint and failed once per pass; errors retry after 5 minutes and
other scans proceed. Existing advisory lock prevents competing processors.
V2 remains active until all accepted scans×8 coins have READY v3. The final sample
and active pointer commit together. Current views contain only the active version;
`_by_version` audit views preserve history. Earlier-adapter processes cannot downgrade.

Measurement/source/BTCwave identities and v7 outcomes remain unchanged. Seven MaxPain
timeframes and eight coins do not become independent waves. HYPE outcomes stay
`HYPERLIQUID_HYPE_PERP_TRADE_1M`; all 8coins share a separately identified BTC Spot
context. Captured available HYPE Spot CVD stays legitimate and must not be forced unknown.
Evidence remains descriptive; discovery, promotion and prospective flags are off.

## Verified production results

Render became live at 20:58:08.370706 UTC on instance `s7psl`.
Automatic activation completed at 21:02:42.874014 UTC. Final health was read at
21:04:09.986196 UTC: all three research workers running with zero failures/errors,
active v3. The ordinary 21:02 Watch was running without an error; it was not
manually triggered. Last-completed fields were still null after restart because
that first scheduled cycle had not finished at the verification timestamp.

- 19 accepted scans × 8 coins = 152 READY v3 samples, with no pending or error jobs.
- 32,224 decisions = 152 × 106 × 2: 2,350 MATCH, 29,874 NO_MATCH, zero UNKNOWN in this captured data.
- Independent raw BTC checks covered 19 scans and 38 completed lookback windows: zero boundary, count, return/range, state or source-route mismatches.
- All 152 coins matched their source identity and shared exactly one frozen context per scan, with no coverage or feature mismatch.
- All 24,928 overlapping v2 decisions were exactly equal as JSON, including order, missing features and inverse direction.
- Both old versions remained unchanged: v1 144 samples/full-payload digest `22f8e04e3158ff350add79d0ef94e1dc`; v2 152 samples/digest `7664b6bb9725c6ba4b3e0f9e3d14606b`.
- Current views expose v3 only; audit views retain 9,792 v1, 24,928 v2 and 32,224 v3 decisions. All prospective flags remain false.
- All 19 HYPE samples use BTC Spot context with their unchanged Hyperliquid PERP TRADE outcome route.
- BTC and HYPE snapshot1231 passed canonical payload and context hash recomputation after JSONB round trip. Both context hashes are `30d34ecb450bf9fd957ac96a7107317d9b57eca544d0f7c56af35e132a5b7e53`.
- All 152 measurements still belong to ONE live BTC parent wave. Counts of scans, coins and formulas are not independent market cases.

Exact verification SQL and results are stored in the evidence JSON. No further
testing or deployment is needed for this completed step.

## Efficient continuation

Use connected GitHub and read-only Render PostgreSQL tools. Use the authenticated
Render Shell only for explicit staged schema admin and compact/health when needed;
reuse existing browser runtime. `formulaHealthCommand` prints FORMULA_HEALTH JSON.
Workspace `tea-d94cq7e7r5hc73dee88g`; service `srv-d94ek17lk1mc73b4tb90`;
database `dpg-d94d641kh4rs73evvih0-a`.

Do not rerun migrations 048 or049 on a v3 runtime; they only know older payload counts.
Apply only exact new migration files after full CI, then deploy after Watch completion.
Necessary implementation/migration/deployment is already authorized; no new permission
question. Never run manual Watch, Telegram tests/messages, unsupported HYPE Spot price
requests or trades. Do not expose DSNs, secrets, chat IDs or captured notifications.

Local requests/dotenv/psycopg remain absent. Do not install/search again. Pure tests can
use an in-memory requests get/post deny stub for import only; CI uses real dependencies
and disposable PostgreSQL 18. No provider calls are allowed in these tests.
Git-backed code and continuation documents stay in GitHub. Full tool stores may not
survive a new turn; read this document instead of guessing old stored variables.
