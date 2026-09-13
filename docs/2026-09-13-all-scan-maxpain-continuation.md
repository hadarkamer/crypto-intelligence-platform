# Continuation — all-Watch MaxPain formulas, 2026-09-13

## Start here

This supersedes `docs/2026-09-13-all-scan-formulas-continuation.md` for the current
runtime. Earlier intake and measurement contracts remain in that document.

Production main: `d0c72f88aa83add18dc94c67de6146ca78afa91c`.
Production/tested tree: `068b19246b22c5381510c0bd89eb1dd8ac6c9dd7`.
PR 36: https://github.com/hadarkamer/crypto-intelligence-platform/pull/36
Tested PR head: `940edfbeb355dafddd8a2238d7db052949e9e7d9`.
CI run 34780278739 / job 103785909670 passed all 115 selftest files, including
13 new pure tests and 5 new PostgreSQL 18 integration tests, none skipped.
The previous 15 pure and 11 PostgreSQL formula tests also passed.

Migration 049 installed successfully after CI, before deployment. SHA256:
`e0e7f114601e56c70984f6f83f0d0e83c2aaeea313150e93a5366721ed9d933e`.
Installation retained active v1 and all 9,792 existing decisions.
Render deploy `dep-dajgbd5g1s2s73cfac3g` live at 20:22:34.440615UTC,
instance `77ttn`. The merged tree equals the tested tree.
Watch 20:02 finished at 20:06:28.358281UTC; merge at 20:21:06UTC followed it.
No manual Watch scan or test notification was run. Restart clears in-memory
last-completed fields; the next scheduled Watch remains 20:32:15UTC.

Implementation worktree: `/workspace/scratch/1a8c1bcb93ce/all-watch-scan-maxpain`.
Continuation worktree: `/workspace/scratch/1a8c1bcb93ce/watch-state`.
Remote durable branch: `checkpoint/research-continuation-20260913`.
Design: `docs/2026-09-13-all-watch-scan-maxpain-formulas.md`.
Live evidence: `docs/2026-09-13-all-scan-maxpain-verification.json`.

## Authorized sequence and current position

Hadar requested one implementation step per turn, in this order:
1. Research from all scans — IN PROGRESS; this turn expands existing-formula coverage from 34 to 82.
2. Continuous new-formula discovery — NOT STARTED by this turn.
3. Historical archive completion — pending.
4. HYPE integration completion — pending.
5. Validation on new cases — pending.

Four stages now exist: PR33 immutable all-scan intake, PR34 causal measurements,
PR35 initial total-model formulas and comparisons, PR36 frozen MaxPain aggregates.
The full catalog still contains the same 298 definitions and original conditions;
82 are supported and 216 remain explicitly unsupported. The count includes the
original normal/inverse identities and aliases, not 82 newly discovered formulas.
Whole-list definition SHA256 remains
`b3ed42745e935ea1295267afb70135b97d1d3534579ec5f8070b61929e862082`.

Next bounded step: classify and implement the next exact applicable feature
contract among the remaining 216, with a separate version where necessary.
Per-timeframe liquidity/selected-opposite fields need an explicit timeframe key
while sharing the same price outcome; causal prior-price features need their own
lookback contract. Combined/top-item and confirmation predicates cannot be invented
from missing delivery. Do not promise that all 216 apply unchanged to all scans.
Do not substitute 12h/24h/month for missing 15m/1h/4h MaxPain horizons.

Cluster/CVD component hypothesis research, B4 and 240m family normalization remain
deferred. Do not silently start topic2 or repeat broad historical-plan audits.

## Frozen feature and version contracts

New core: `research_watch_scan_formula_maxpain.py`; legacy core remains unchanged.
Worker: `research_watch_scan_formula_worker.py`; schema: migration049.
Processing version: `watch-scan-formulas-v2-maxpain`.
Feature version: `watch-captured-total-and-maxpain-features-v2`.
New fields: `max_pain.average_score_all_timeframes`,
`max_pain.opposite_average_score_all_timeframes`, `max_pain.consensus_hits_full`,
and proven `event.direction_mapping_valid`. No alert event is synthesized.

Means exactly match original active-side producer semantics: active SCORED slots
in 12h,24h,48h,3d,1w,2w,1m order, float sum / active count, Python round(...,2).
Known inactive targets are excluded; known zero scores are included. Complete
seven source rows and fourteen explicit slots are required. Missing sources are
UNKNOWN, not partial averages or zeros. Source SHORT maps to base LONG, source
LONG to base SHORT. Ordinary closest-active-target consensus uses SHORT for ties;
it is neither selected flags, scores above65 nor gap consensus. Original three
calculation guards and causal quote/target proof are checked without rescoring.

Each coin has 164 frozen decisions =82 definitions ×2 base directions. Seven
MaxPain horizons do not multiply price outcomes or independent observations.
Inverse formulas preserve base predicates and flip only outcome direction.
Legacy34 decisions and missing-feature lists remain identical across versions.
`maxpain_provenance_by_direction` retains all source values, denominators,
inactive/missing timeframes, consensus, quote identities and validation reasons.

Migration049 preserves v1 samples/catalog and adds a singleton runtime pointer.
Original evaluation/population/anchor/outcome/comparison view names expose only
the active version; corresponding `_by_version` names retain both histories.
The worker activates v2 atomically with the final feature sample only when every
accepted scan ×8 coins visible to that statement has READY v2, including accepted
receipts not yet enqueued. Rollback leaves both pointer and samples uncommitted.
New arrivals after activation are normal next-pass work. A retiring predecessor
cannot downgrade the pointer; reapplying049 retains it. Do not rerun048 on production.

Worker remains bounded to16 coins per30-second pass, with savepoints and durable
retries. Health `watch_scan_formulas` shows processing and active versions.
No providers, alert synthesis, manual notifications, promotions or discovery.
Source measurements remain v1, causal next-full-minute entry and ordered-first-touch-v7,
4 windows and8 barriers. Selection still uses earliest arm cohorts within BTC waves,
retains ties and blocks earlier/tied UNKNOWN. OPEN windows never become decisive
merely because an early barrier was touched. Evidence remains descriptive.

HYPE outcome route remains `HYPERLIQUID_HYPE_PERP_TRADE_1M`. Captured operational
quotes and available HYPE Spot CVD are legitimate independent source fields;
do not relabel them as outcome prices or force available flow to UNKNOWN by symbol.

## Verified production results

Automatic activation completed at 20:26:35.616672UTC. Final health at
20:28:07.290325UTC shows active v2, all three research workers running,
zero failures/errors, and Watch waiting for its normal 20:32:15UTC schedule.

- 18 accepted scans ×8 coins =144 READY v2 samples; zero pending or missing jobs.
- 23,616 decisions =144 ×82 ×2; 1,604 MATCH, 22,012 NO_MATCH and0 UNKNOWN in this captured data.
- Current views expose v2 only. Audit views retain9,792 v1 decisions alongside v2.
- All9,792 overlapping legacy decisions are exactly equal, including missing features and directions.
- All144 v1 payloads remain unchanged; full payload digest `22f8e04e3158ff350add79d0ef94e1dc`, feature digest `060560e6e17d308d1b5f014e65a044ce`.
- Independent raw-source calculations checked288 side profiles: zero denominator, value, average, opposite-average, consensus, mapping or rounding differences; all288 valid.
- Zero identity violations across144 samples, zero direction violations across23,616 decisions, and zero causal-time/target-price violations across2,016 source slots.
- BTC and HYPE snapshot1231 payloads passed canonical hash recomputation after JSONB round trip.
- All18 HYPE outcome measurements use `HYPERLIQUID_HYPE_PERP_TRADE_1M`; frozen operational quotes remain separately identified.
- All144 measurement memberships still belong to ONE LIVE BTC parent wave. Formula/coin/scan counts are not independent market cases or prospective validation.

The JSON evidence includes the exact read-only verification statements and results.
No further tests or deployment are needed for this completed step.

## Efficient continuation

Use connected GitHub and read-only Render PostgreSQL tools. The authenticated
Render browser Shell is for explicit staged schema admin and /health when needed.
Reuse existing browser runtime; persistent variable `formulaHealthCommand` prints
a compact FORMULA_HEALTH JSON marker. Never expose DSNs, secrets or chat IDs.
Workspace `tea-d94cq7e7r5hc73dee88g`, service `srv-d94ek17lk1mc73b4tb90`,
database `dpg-d94d641kh4rs73evvih0-a`.

Local runtime lacks requests/dotenv/psycopg; do not repeat installs or searches.
Run pure tests locally and PostgreSQL tests in full CI. Verify tested-tree equality,
apply only the exact required migration, and deploy after a scheduled Watch finishes.
Necessary implementation/migration/deploy is authorized; do not ask again.
No manual Watch, Telegram tests/messages, unsupported HYPE Spot outcome requests,
or trade execution. Git-backed code and checkpoints stay in GitHub.
