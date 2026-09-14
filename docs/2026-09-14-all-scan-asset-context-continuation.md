# Continuation — all-Watch own Spot context, 2026-09-14

## Start here

This supersedes `docs/2026-09-13-all-scan-btc-context-continuation.md` for runtime
state. Earlier continuation documents preserve intake, outcome, MaxPain and BTC
contracts. Do not repeat completed deployments or broad historical audits.

Production main: `5e2b0e5c0495c4736efcc4ad84062402c37391c0`.
Production/tested tree: `b85a2f044e7be88107cfd1909c7b9c958ea0a8fb`.
PR39: https://github.com/hadarkamer/crypto-intelligence-platform/pull/39
Tested head: `5fd923a5157da08dda9aa12f9e4964b6191bd0a9`.
CI run34805642528/job103856883738 passed all119 selftest files at04:23:40UTC,
including13 new pure and6 new disposable PostgreSQL18 tests. Local53 pure tests
passed with a requests import-only deny stub. Independent review found no blocker.

Migration051 installed after CI and before merge; SHA256:
`482a4cc1b95870e69e87291388cbcec9ddeade7cde6c587400ae100e57e5fe8d`.
Installation retained activev3 and its57,664 decisions. Merge04:24:47UTC followed
a health read04:24:35UTC confirming Watch waiting, completed04:05:55UTC, errornull.
Render deploy `dep-dajne48jo6nc73ci7t50` became live04:26:08.249413UTC,
instance `f7xjv`. Next ordinary Watch04:32:15UTC. No manual Watch or notifications.

Implementation worktree: `/workspace/scratch/1a8c1bcb93ce/all-watch-scan-own-price`.
Continuation worktree: `/workspace/scratch/1a8c1bcb93ce/watch-state`.
Durable branch: `checkpoint/research-continuation-20260913`.
Design: `docs/2026-09-14-all-watch-scan-asset-context.md`.
Evidence: `docs/2026-09-14-all-scan-asset-context-verification.json`.

## Authorized plan and current position

Hadar requests one bounded step per turn, efficiently and in simple Hebrew:
1. Research from all scans — IN PROGRESS. This step adds52 existing definitions,106→158.
2. Continuous discovery of new formulas — not started by this step.
3. Historical archive completion — pending.
4. HYPE integration completion — pending.
5. Formula validation on new cases — pending.

Completed: immutable Watch intake(PR33), causal measurements(PR34), total-score
formulas/comparisons(PR35), MaxPain aggregates(PR36), frozen shared BTC context(PR37),
and prior own-Spot/relative-BTC context(PR39). Original298 definitions remain intact;
158 are supported and140 unsupported. Original catalog SHA256 remains
`b3ed42745e935ea1295267afb70135b97d1d3534579ec5f8070b61929e862082`.
No formula was invented, selected for promotion or traded in this step.

The52 newly supported normal/inverse definitions comprise36 own lookback direction,
6 own4h regime,4 relative-BTC1h and6 capturedSpotCVD65-plus-own1h-alignment definitions.
Support means implemented evaluation with explicit missing-data states, not that all
coins have available inputs. HYPE's nine own-price features remain unavailable.

Remaining140 contracts: selected-item timeframe liquidity34, Combined top-item
averages22, captured confirmation16, sequence entry/score-change/family-order46,
deferred MaxPain components10, same-timeframe selected/opposite score difference2,
absent15m/1h/4h MaxPain8, and standalone Combined event type2.

Next bounded step: inspect one exact remaining group and its actual captured source
contract before implementing it. Score-change/sequence requires an explicit causal
predecessor and event identity; all-scan observations cannot silently become selected
notification events. Timeframe liquidity needs the actual selected item's timeframe,
not a coin average or maximum. Missing Combined/confirmation delivery cannot be
synthesized. Do not blindly connect all140 or move to topic2 yet.

Cluster/CVD component hypothesis research, B4 and240m family normalization remain
deferred. Available HYPE captured SpotCVD is valid. HYPE outcome remains Hyperliquid
PERP TRADE; legacy HYPE Spot cache presence is not authorization to substitute a
Spot own-price contract or to relabel PERP as Spot.

## Current adapter and rollout contracts

Core: `research_watch_scan_formula_asset_context.py`; worker same existing file.
Evaluation: `watch-scan-formulas-v4-asset-context`.
Feature: `watch-captured-total-maxpain-btc-and-asset-context-features-v4`.
Own context: `watch-prior-asset-spot-closed-1m-v1`.
BTC context remains `watch-prior-btc-spot-closed-1m-v1`.
Earlier v1/v2/v3 adapters and rows remain immutable for audit.

Nine new fields: own direction15m/30m/1h/4h/12h/24h; own4h market regime;
own1h return minus BTC1h return; own1h alignment to base direction. Six windows
reuse `research_past_price_features.py` exactly, including independent continuity,
strict Spot route/OHLC validation and original0.50 range-efficiency regime.
Cutoff is floor(source usable time,minute), excluding the current minute and any
future outcome entry information. Each of7 Spot coins reads at most1440 prior
cached minutes. HYPE makes no own-price cache/provider read.

Shared BTC context preference is currentv4 READY sibling, then previousv3 READY
sibling, otherwise one BTC240-minute cache read per selected scan. A saved missing
window remains missing after archive repair. BTC's own1h/4h windows reuse those
exact frozen shared proofs, including missing ones; its other windows use the own
24h path. Relative strength uses the frozen shared BTC hour. Inverse formulas retain
the base predicates and flip outcome direction only. BTC self-relative return is0.

Own context stores raw six-window proof, LONG/SHORT features and unavailability,
source identity, symbol, cutoff and BTC-context hash under a canonical context hash.
READY coin payload has316 decisions and includes both context proofs. Each first
successful coin evaluation freezes its own evidence. Missing data is an explicit
UNKNOWN where needed; known-false conjunctions remain NO_MATCH.

Worker budget remains16 coins per30 seconds. Shared-context and per-coin database
read failures are isolated by savepoints, retried after5 minutes and do not freeze
missing features as if a read succeeded. No providers/outcomes/BTC-membership reads
enter feature evaluation. v4 activates only after every accepted scan×8coins is
READY. Old active results remain visible during backfill; audit views retain all
versions. Older workers cannot downgrade the active version.

## Production verification

Automatic activation is recorded at04:35:08.302785UTC. The final read04:37:01UTC
confirmed35 accepted scans×8coins=280 READY samples, no pending/missing formula
jobs, no errors, and316 decisions percoin. The first34 scans were backfilled; the
ordinary04:32 Watch contributed the35th scan, processed automatically byv4.

- 88,480 current decisions:12,408 MATCH,72,852 NO_MATCH,3,220 UNKNOWN. Unknowns are expected HYPE own-Spot predicates, not processing failures.
- All57,664 oldv3 decisions are exactly equal as complete JSON; every prior feature/missing reason and frozen BTC context is preserved. All298 definitions, hashes and orientations are identical;52 support flags were added.
- All280 coins pass source/context bindings and share one BTC context per scan. BTC own1h/4h proofs equal that shared frozen context. The8 new-scan rows correctly have no previousv3 counterpart.
- Independent raw-price checks passed1,428 historical windows plus42 new-scan windows=1,470 across245 Spot coin samples. Both base directions, regime, alignment and490 relative-return cells pass; BTC self-relative return is0. All six windows are complete in this checked dataset.
- All35 HYPE samples keep nine own-Spot fields unavailable in both base directions (630 cells). All70 captured SpotCVD cells remain known; every overlapping CVD value is unchanged. HYPE outcome routes remain Hyperliquid PERP TRADE.
- v1/v2/v3 full-payload digests and counts are unchanged from the same-query predeployment baseline. The evidence JSON stores the exact query and both results; do not compare digests from a differently concatenated prior-turn query.
- Current views exposev4 only. Audit views retain9,792 v1,24,928 v2,57,664 v3 and88,480 v4 decisions. Prospective flags remain false; discovery and promotion remain disabled.
- Measurements still represent ONE independent live BTC parent wave. The35 scans,8 coins and158 formula definitions do not establish independent statistical cases or proven performance.

Final health04:37:06.831717UTC confirms all three research workers running with
zero failures/errors and activev4. The ordinary Watch completed successfully at
04:36:05.200032UTC, statuswaiting, last_errornull, nextscheduled05:02:15UTC.
Measurement OPEN/missing future windows remain normal and separate from completed
formula evaluation. No manual scan, message, trade or scheduling change occurred.

BTC/ETH/HYPE snapshot1231 have already passed canonical complete-payload, own-context
and BTC-context SHA256 recomputation after JSONB readback. No loaded proof changed
from numeric round trips; all three reuse BTC hash
`30d34ecb450bf9fd957ac96a7107317d9b57eca544d0f7c56af35e132a5b7e53`.

Exact read-only verification SQL and results are in the evidence JSON. This step
is complete; no further testing or deployment is needed for it.

## Efficient continuation and permissions

Use connected GitHub and read-only Render PostgreSQL. Existing authenticated Render
Shell is reserved for explicit staged schema admin and compact health checks.
Reuse browser `renderShell` and `formulaHealthCommand`; read terminal output via
`getByRole('main').allTextContents()` (not only listitems). Workspace
`tea-d94cq7e7r5hc73dee88g`, service `srv-d94ek17lk1mc73b4tb90`, DB
`dpg-d94d641kh4rs73evvih0-a`.

Do not rerun migrations048/049/050 afterv4 activation; their checks only know older
decision counts. Apply only an exact new migration after CI, and deploy after Watch
completion. Necessary implementation/migration/deployment is already authorized.
Never run manual Watch, Telegram tests/messages, unsupported HYPE Spot requests or
trades. Do not expose DSNs, secrets, recipient identifiers or captured messages.

Local requests/dotenv/psycopg remain absent. Do not install/search again. Import-only
requests get/post deny stubs permit pure tests; full CI has real dependencies and
disposable PostgreSQL18. Git-backed code and continuation docs stay in GitHub.
Tool stores may not survive another turn; use this document for continuation.
