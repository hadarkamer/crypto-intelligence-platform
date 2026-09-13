# Continuation — all-Watch formulas, 2026-09-13

## Start here

Production main: `934d3a633f149e002957ff3317f6aa33836f9e67`.
Production/tested code tree: `7341a1dccda06d803cd046fdd6039fd2256ce463`.
PR 35: https://github.com/hadarkamer/crypto-intelligence-platform/pull/35
CI run 34778323249/job 103780510070 passed all 113 selftest files, including 15 new
pure and 11 isolated PostgreSQL 18 tests (none of the new tests skipped).

Render deploy `dep-dajfpaqjnfac73f0n5a0` live 19:44:00.463871UTC, instance `cj49m`.
Migration 048 installed via the explicit staged schema admin after CI.
Migration SHA256: `8297b87d9b1d2d8acf790bd4065a63008828fa0a978b9f1cfe202f70363cc15e`.
Watch 19:32 completed 19:36:11.526117UTC; merge 19:42:34UTC followed completion.
Health after deploy is waiting for 20:02:15UTC, with no Watch/research errors.
Last-completed fields are null after process restart; no new Watch has been forced.

The current code and this checkpoint are in `checkpoint/research-continuation-20260913`.
Local continuation worktree: `/workspace/scratch/1a8c1bcb93ce/watch-state`.
Implementation worktree: `/workspace/scratch/1a8c1bcb93ce/all-watch-scan-formulas`.
Read the design: `docs/2026-09-13-all-watch-scan-formulas.md`.
Structured evidence: `docs/2026-09-13-all-scan-formulas-verification.json`.

## Authorized sequence and current position

Hadar requested one implementation step per turn, in this order:
1. Research from all scans — IN PROGRESS; this turn completed the first formula adapter and comparison.
2. Continuous discovery of new formulas — NOT STARTED by this turn.
3. Historical archive completion — pending.
4. HYPE integration completion — pending.
5. Formula validation on new cases — pending.

The first topic now has three implemented stages: PR 33 immutable all-scan intake,
PR 34 causal price/wave measurements, PR 35 existing total-model formula evaluation.
Do not repeat those builds or a broad historical-plan audit.

Next bounded work: determine and connect further exact, applicable feature
contracts for the currently unsupported definitions. Distinguish fields that
already exist in the frozen Watch bundle, fields needing causal past-price
features, and predicates tied to delivered-alert semantics that do not apply
unchanged to non-alert scans. Do not promise all 264 are directly mappable.
Existing catalog conditions must remain unchanged. Use a new adapter/support
version for expanded coverage. Preserve the original source/measurement identities,
unknown-data rules, inverse semantics and wave counting.

Cluster/CVD component hypothesis research, B4 and 240m family normalization remain
deferred. Do not resume them through this feature adapter or start topic2 silently.
Do not invent confirmation statuses, event types, histories or absent 15m/1h/4h
MaxPain horizons. Do not average the seven captured horizons and label the result
as an existing formula field without proving exact semantic equivalence.

## Verified production state

At 2026-09-13T19:48:32.924862+00:00:
- 17 accepted Watch scans × 8 coins = 136 immutable ready formula samples.
- 298 complete original candidate definitions preserved: 34 supported, 264 explicitly unsupported.
- 9,248 decisions = 136 × 34 × 2 base directions; 688 MATCH, 8,560 NO_MATCH, 0 UNKNOWN in this current captured data.
- All 34 definitions and 8 coins covered ; 0 waiting jobs, 0 errors, 0 prospective-evidence flags.
- All 136 measurements have LIVE membership in ONE BTC wave.
- ALL-scope earliest cohorts contain 560 feature links: 60 matched and 500 controls;
 maximum 8 tied coins, 0 unknown-blocked members. These are not 560 independent cases.
- Independent SQL checked 816 single-model decisions: 0 predicate mismatches,
 0 unavailable models treated as known; 785 below 65 controls; 102 known HYPE checks.
- 0 source identity, inverse orientation, unsupported-evaluation or per-coin coverage errors.
- HYPE outcome route only `HYPERLIQUID_HYPE_PERP_TRADE_1M`.
- BTC andHYPE payload hashes read back from PostgreSQL match canonical feature hashes.
- 136 ready sample digest: `a1b51110789e9c89799b67ab9a41a5ba`.
- New worker completes a bounded initial backlog automatically; no manual run was used.

Current available data does include captured HYPE Spot CVD. The unsupported Spot
restriction is about outcome price routing; never force an available flow model
to UNKNOWN by symbol. Unavailable captured fallback 0 remains UNKNOWN. Positioning
window references normally lack latest_time: latest prices/OI have their own
source-level fetched timestamps. The adapter checks those and causal references.
The existing operational fallback with empty window maps is intentionally preserved.

## Implementation contracts

Core: `research_watch_scan_formula.py`.
Worker: `research_watch_scan_formula_worker.py`.
Schema: `migrations/048_watch_scan_formulas.sql`.
Tests: `research_watch_scan_formula_selftest.py`,
`research_watch_scan_formula_postgres_selftest.py`, lifecycle test in
`research_ordered_runtime_selftest.py`.

Evaluation version `watch-scan-formulas-v1`.
Feature version `watch-captured-total-model-features-v1`.
Catalog whole-list SHA256
`b3ed42745e935ea1295267afb70135b97d1d3534579ec5f8070b61929e862082`.
Catalog drift fails closed and requires a new adapter version.
Features: price_oi.aligned_score, futures_cvd.aligned_score,
spot_cvd.aligned_score, time.weekend (source observation, Asia/Jerusalem).
34 includes both normal/inverse identities and original aliases; no new formulas.

Tri-state conjunction: all pass MATCH; known false conjunct NO_MATCH even if another
feature is missing; otherwise UNKNOWN. Missing-feature lists retained in all cases.
INVERSE evaluates the same base features, flips only outcome direction.
Complete original candidate JSON/hashes retained; existing formula_contract alone
would omit orientation/identity metadata and is not used as the new identity.

Tables: research_watch_scan_formula_catalog, research_watch_scan_formula_state,
research_watch_scan_formula_samples.
Views: research_watch_scan_formula_evaluations, research_watch_scan_formula_wave_population,
research_watch_scan_formula_wave_anchors, research_watch_scan_formula_wave_outcomes,
research_watch_scan_formula_comparisons.

Selection version `watch-first-arm-cohort-unknown-blocks-v1`:
earliest MATCH/NO_MATCH per candidate/base direction/BTC wave/symbol_scope; all ties.
Scopes ALL and each coin remain separate. Earlier/tied UNKNOWN blocks eligibility,
including unfinished feature jobs. Selection is before any outcome join. Missing
or losing first anchors are never replaced with later winners. LIVE membership
and exact source/version/entry identities required. Measurement backlog without
membership is outside current wave comparisons until its membership is available.

Only completed frozen READY windows enter decisive counts. An early first touch
in OPEN remains OPEN in comparisons. Cohort priority matches the existing all-members
policy: DATA_MISSING, OPEN, AMBIGUOUS, NO_TOUCH, FAILURE, SUCCESS.
Full-window MFE/MAE require every eligible member's window READY, use min MFE/max MAE.
4 windows 60/240/720/1440 × 8 barriers 25..200bps share unchanged v7 measurement labels.
Descriptive matched/control/union/shared wave counts do not constitute an acceptance
test. No confidence/causal/prospective claim, no discovery/promotions/delivery.

Worker: 16 coin jobs/pass, 30 sec interval, 32 receipt IDs/lap plus recent 2,
advisory xact lock48260913201211, atomic queue/cursor/catalog/results,
per-coin savepoints, 5 min error retry, immutable READY feature payloads.
Flag inherits RESEARCH_OUTCOME_ENRICHMENT_ENABLED; optional
RESEARCH_WATCH_SCAN_FORMULA_ENABLED override. Health key watch_scan_formulas.
No provider requests, outcome reads, alert synthesis, Telegram or Sheets writes.

## Efficient continuation

Use connected GitHub and read-only Render PostgreSQL tools. Local runtime lacks
requests/dotenv/psycopg; do not repeat dependency installation/search. Pure tests
work locally; meaningful PostgreSQL tests run in CI. Every tracked *_selftest.py
is discovered automatically. Review tested tree equality before merge/deploy.

Render workspace tea-d94cq7e7r5hc73dee88g, service srv-d94ek17lk1mc73b4tb90,
DB dpg-d94d641kh4rs73evvih0-a. Authenticated browser Render Shell exists for schema
admin and /health only when connectors lack the action. The existing browser tab
automatically follows instance changes; do not initialize/reset its runtime.
The latest persistent Node variable `formulaHealthCommand` prints a compact
FORMULA_HEALTH marker. Do not rerun already applied 048 migration.

Do not run manual Watch, Telegram tests/messages, unsupported HYPE Spot price
requests, or trade execution. Wait for a scheduled Watch to finish before another
deployment. Authorized necessary fixes/migrations/deploys do not need another
permission question. Git-backed code and continuation documents stay in GitHub.
