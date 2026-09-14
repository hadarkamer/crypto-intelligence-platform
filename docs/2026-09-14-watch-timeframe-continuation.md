# Continuation — selected-timeframe research, 2026-09-14

Read `docs/2026-09-14-watch-timeframe-verification.json` first. That companion
file is authoritative for final CI, migration, merge, deployment and production
verification. **Production rollout and verification are COMPLETE.** Continue from
the remaining plan below; do not repeat this rollout or the broad historical audit.
Design: `docs/2026-09-14-watch-timeframe-formulas.md`.

## Release being validated

- PR42: https://github.com/hadarkamer/crypto-intelligence-platform/pull/42
- Current head: `3f39da0e46577e97f8d73cfd9462f3d0a14b16af`.
- Current tree: `ca93f85dd931212cdaf0ad63d8d73fc09ef5c4bf`.
- Final CI: run `34812489993`, job `103876428747`; all126 selftest files passed,
  including7 new PostgreSQL tests and16 new pure tests.
- Migration: `054_watch_scan_timeframe_formulas.sql`.
- Migration SHA256: `9e51da79420b9cdcac86006b977be090e601cd9924a20b2c041c1a510cd64cbc`.

The first CI attempt failed a test fixture. The complete pure fixture was
corrected to target 120 and opposed amounts 200/400; assertions and core behavior
were unchanged. Use the final head/CI result above, not an earlier attempt.
Migration054 completed before merge and preserved activev5 and170 supported
definitions until the supplement was caught up. PR42 merged as
`fb66972785826aea9a7c308980317d9cb5d86b03`, with the exact tested tree.
Render deploy `dep-dajp3p7qj5pc73be9fl0` became live at06:20:40.037108UTC.
The supplement activated at06:25:22.719357UTC. Final health at06:26:01UTC confirms
all four research workers running without errors and Watch waiting for06:32:15UTC.

Implementation worktree: `/workspace/scratch/1a8c1bcb93ce/all-watch-scan-timeframe-liquidity`.
Continuation worktree: `/workspace/scratch/1a8c1bcb93ce/watch-state`.
Durable continuation branch: `checkpoint/research-continuation-20260913`.

## Design and restart contract

This adds 34 unchanged original definitions: 32 standalone liquidity and 2
selected/opposite score differences, including their existing inverses. The
current coin adapter remains v5 with 170 definitions and 340 decisions per coin.
**Combined coverage204/298 is activated and verified.**
This is a separate selected-timeframe supplement, not a replacement coin adapter.

Core: `research_watch_scan_formula_timeframe.py`; worker:
`research_watch_scan_formula_timeframe_worker.py`. Evaluation version:
`watch-scan-timeframe-formulas-v1`. Feature version:
`watch-captured-selected-timeframe-liquidity-and-score-difference-v1`.
Selection version: `watch-selected-timeframe-first-arm-unknown-blocks-v1`.

Each coin has one immutable READY result with 34 × 7 × 2 = 476 decisions. The seven
captured horizons are 12h, 24h, 48h, 3d, 1w, 2w, 1m. Selection uses the actual captured
`slot.selected`, verified against higher score, nearer active target, then source
LONG on an exact tie. Source SHORT implies price LONG; source LONG implies price
SHORT. Inverse definitions retain base predicates and reverse outcome direction.

Only the selected direction enters its timeframe cohort. The other direction,
including both known inactive targets, is NOT_SELECTED/NOT_APPLICABLE rather
than a control. Unresolved selection is UNKNOWN in both directions and blocks
later or tied anchors. Local missing inputs do not invalidate another timeframe.
Liquidity uses the selected slot's frozen near/far/share, checked against that
timeframe's source amounts; missing inputs never become zero. The same-timeframe
score difference needs both actual scores; an inactive opposite remains unknown.

The worker reads immutable source observations and the existing pure total-score
extractor. It does not rebuild v5/BTC/own-price contexts or read outcomes/providers.
Budget: 32 coins per 30 seconds; cyclic intake plus recent reserve, per coin isolation,
five-minute processing-error retry. Source identities, hashes, the full grid,
selection/status pairs and inverse direction are checked before READY.

Separate `research_watch_scan_tf_formula_*` catalog/state/samples/runtime and
views preserve all old v1–v5 relations and rows. The new runtime starts NULL and
activates only after every accepted scan × 8 coins is READY. The new coverage view,
`research_watch_scan_formula_coverage`, retains 298 original identities and shows
COIN 170 / SELECTED_TIMEFRAME 34 / UNSUPPORTED 94 after activation, with definition/hash/
orientation agreement. New cohort partitions retain timeframe everywhere and
reuse the same coin measurements, four windows, eight barriers and conservative
tied-member policy. Additional horizons never multiply independent BTC waves.
Research remains descriptive; no discovery, qualification, promotion or trading
is enabled by this supplement.

## Remaining ordered plan

1. Research from all scans — IN PROGRESS; this34-definition supplement is complete.
2. Continuous discovery of new formulas — pending.
3. Historical archive completion — pending.
4. HYPE integration completion — pending.
5. Formula validation on new independent cases — pending.

After this rollout,94 original definitions remain unsupported: Combined-dependent
liquidity 2, Combined top-item 22, captured confirmation 16, sequence entry/family
order 34, deferred MaxPain components 10, absent short MaxPain horizons 8, and
standalone Combined event 2. Combined identity, selected top-item and confirmation
labels are not represented by ordinary captured totals; do not manufacture them.
Choose the next bounded step only after checking its actual source contract.

Cluster/CVD component hypotheses, B4 and 240m family normalization remain deferred.
HYPE's valid captured CVD/OI totals participate; own Spot-price features remain
unavailable and its outcome route remains Hyperliquid PERP TRADE. The remaining
HYPE phase is not completed by this step.

## Ordinary Watch and dual-CVD checkpoint

The first ordinary 06:02 UTC scan after dual-CVD activation produced an ACCEPTED
receipt from source 2026-09-14T06:05:53.032992Z:8 NO_MATCH, 0 UNKNOWN, 0 MATCH and
no intent. This verifies scheduled evaluation, not a matching alert delivery.
Watch completed at 06:06:11.502537 UTC; next scheduled run was 06:32:15 UTC.
The standalone dual-CVD alert and normal Watch schedule are unchanged here.

## Completed production checks and next step

All38 accepted scans ×8coins =304 READY results,476 decisions each. There are
zero pending/error/missing jobs, bad grids, source-lineage mismatches or catalog
definition changes. Coverage is exactly170COIN +34SELECTED_TIMEFRAME +94UNSUPPORTED.
All pre-existing v1–v5 READY payload counts and digests match under cutoff
snapshot_set_id<=1302 using the exact same aggregation query before/after.

Latest scan1304 has3,808 unique decisions and1,904 inverse pairs, with zero
selection/orientation/partner mismatches. Its counts are344MATCH,1,558NO_MATCH,
2UNKNOWN and1,904NOT_APPLICABLE. BTC,ETH,HYPE complete payload hashes survive
JSONB readback, and replay from each frozen source reproduces the payload exactly.
A bounded BTC/12h/LONG/liquidity-balanced/60m/25bps comparison reads the existing
outcome correctly; it is one descriptive failed arm, with no statistical test.
The entire measurement population still has ONE independent LIVE BTC parent wave.

The first ordinary dual-CVD receipt remains ACCEPTED after deployment, with
eight valid NO_MATCH and no intents. New-process health resets its in-memory
last-record counters to NOT_OBSERVED; the durable receipt proves the earlier
scheduled evaluation. Actual matching notification delivery is not yet observed.

The next bounded step is to inspect and capture actual Combined identity,
selected top-item and confirmation evidence for future scans. Old absent fields
must remain UNKNOWN; ordinary mean totals cannot stand in for these predicates.
Do not begin discovery, archive/HYPE completion or statistical promotion as a
side effect of this capture work. Continue the user's ordered plan one step at a time.

Durable read-only SQL is in `docs/verification/timeframe/`:
`state.sql`, `decision-parity.sql`, `comparison-plan.sql`, `comparison.sql`.
The parity check expands only the latest complete scan: 3,808 decisions and 1,904
inverse pairs. Avoid unfiltered outcome/comparison counts; use the supplied
single candidate/timeframe/coin/direction/window/barrier slice and a short timeout.
Do not sum timeframe wave counts as independent cases.

Use GitHub/read-only Render first and existing staged schema tooling when needed.
Do not rerun old coverage migrations, trigger manual Watch, send test messages,
request unsupported HYPE prices or trade. Local requests/dotenv/psycopg are absent;
do not install or search again. CI supplies the real dependencies and PostgreSQL.
