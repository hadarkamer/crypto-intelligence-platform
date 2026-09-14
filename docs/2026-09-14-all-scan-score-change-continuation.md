# Continuation — all-Watch score changes, 2026-09-14

## Start here

This supersedes `docs/2026-09-14-all-scan-asset-context-continuation.md` for runtime
state. Earlier documents preserve source, measurement, outcome, MaxPain and price
contracts. Do not repeat completed implementations or broad historical audits.

PR40: https://github.com/hadarkamer/crypto-intelligence-platform/pull/40
Tested head: `bde8674efc454143bbbb3a21393f4beca39cdd5f`.
Tested tree: `f5f5f6c63b84924ddb749a28bfa986aca0d07306`.
CI run34807448950/job103861991696 passed all121 selftest files at04:53:07UTC.
Local66 pure tests passed, including13 new.
Six focused PostgreSQL18 tests and independent read-only code review cover the
source-selection, rollout, late-intake and retry contracts below.

Implementation worktree: `/workspace/scratch/1a8c1bcb93ce/all-watch-scan-score-change`.
Continuation worktree: `/workspace/scratch/1a8c1bcb93ce/watch-state`.
Durable branch: `checkpoint/research-continuation-20260913`.
Design: `docs/2026-09-14-all-watch-scan-score-change.md`.
Evidence: `docs/2026-09-14-all-scan-score-change-verification.json`.

Production main: `6e3e98dd80aa4e190d1de7fb1a060aa502f5eaef`.
Merged04:54:55UTC; merged tree equals the exact tested tree. Migration052 installed
after CI, before merge, retaining activev4 and88,480 decisions; the340-decision
constraint and predecessor index were verified. Migration SHA256:
`d5d396aa76b3145fc483916c9312bcaaf825bda19c98ee50ec11cfaf8f45d0f3`.
Health04:54:37UTC confirmed Watchwaiting, completed04:36:05UTC, noerror,
nextscheduled05:02:15UTC. Render deploy `dep-dajns8dg1s2s73ckp03g`.
Render became live at04:56:12.478754UTC. Final production verification follows below.

## Authorized plan and current position

Hadar requests one bounded implementation step per turn, efficiently and in simple
Hebrew. The ordered topics remain:
1. Research from all scans — IN PROGRESS. This step connects12 original score-change definitions,158→170.
2. Continuous discovery of new formulas — not started by this step.
3. Historical archive completion — pending.
4. HYPE integration completion — pending.
5. Formula validation on new cases — pending.

Completed: PR33 immutable intake, PR34 causal measurements, PR35 total-score
formulas/comparisons, PR36 MaxPain aggregates, PR37 shared frozen BTC context,
PR39 own Spot context and PR40 prior-scan score change.170 of the unchanged298
definitions are supported;128 remain unsupported. The original catalog SHA256:
`b3ed42745e935ea1295267afb70135b97d1d3534579ec5f8070b61929e862082`.
No newly invented formula, promotion, trading execution or scheduling change.

Remaining128: timeframe liquidity34 (32 standalone,2 also require Combined event),
Combined top-item22, captured confirmation16, sequence entry/family order34,
deferred MaxPain components10, same-timeframe selected/opposite score difference2,
absent15m/1h/4h MaxPain8, standalone Combined event2.

Next candidate bounded step: a separate explicitly identified timeframe observation
dimension for32 liquidity and2 same-timeframe score-difference definitions. The
capture already includes selected/near/far/share and opposite scores per timeframe;
it has no single selected coin-wide value. Do not pick an arbitrary timeframe or
average the liquidity. Reuse existing coin outcomes without multiplying independent
BTC waves. Assess storage/view/cohort impact before changing the current coin model.

Combined top-item and confirmation contracts remain unrepresented in Watch capture:
only modules were retained, not actual captured confirmation labels, Combined
candidate identity or top_item. Never alias them to a normal mean or infer
NOT_CONFIRMED from absence. Sequence entry and family ordering need actual event
identity; the score-change step does not manufacture those events.

Cluster/CVD component hypothesis research, B4 and240m family normalization remain
deferred. HYPE captured SpotCVD is valid; own Spot-price features remain unavailable;
outcomes remain Hyperliquid PERP TRADE. Do not silently substitute other prices.

## V5 score-change contract

Core `research_watch_scan_formula_score_change.py`; worker existing module.
Evaluation `watch-scan-formulas-v5-score-change`;340 decisions percoin.
Feature `watch-captured-total-maxpain-btc-asset-and-score-change-features-v5`.
Context `watch-prior-scan-aligned-total-change-30m-v1`.
Selector `watch-latest-earlier-distinct-scan-30m-no-skip-v1`.

Three keys `sequence.30m.{price_oi,futures_cvd,spot_cvd}.score_change` subtract
prior aligned total from current aligned total. Strictly>0 strengthens, strictly<0
weakens;0 matches neither. Three models×two conditions×normal/inverse gives12
definitions. Inverse keeps base predicates and flips outcome direction only.

This explicitly versions a new all-scan predecessor population. It is not equivalent
to delivered-alert history, whose loader could skip missing model scores. Here the
nearest accepted distinct watch_scan_id is selected consistently for all models,
with earlier observed time and usable time in[currentusable−30minutes,currentusable).
The lower boundary is inclusive and upper exclusive. Source availability/creation
remain part of usable time. A timestamp tie is ambiguous, never resolved byID.
Missing nearest values stay missing; no older numeric fallback. No prior means
UNKNOWN, not0. Do not widen30minutes for scheduling jitter. Initial35scanpreflight
had19 eligible predecessor scans and16 without one inside the exact window.

Selection uses indexed intake metadata LIMIT2, independently of prior formula job
readiness. Original current/prior model evidence is validated using the existing
aligned-score source contract. The first READY v5 coin freezes predecessor IDs
(including absence/ties) for all sibling coins and retries. A later intake admission
cannot change them. The selected source models, timing, identities and proof are
retained under a canonical context hash; validation rederives their values.

V5 reuses v4 frozen own-price proof for each old coin. Shared BTC preference is
v5→v4→v3. No later cache fill may change any of the158 previous decisions. New
scans alone build the usual causal price contexts. No outcome, provider or delivered
event read enters score-change evaluation.

Worker remains16 coins/30seconds; shared errors are isolated per scan, individual
source errors percoin, retry5minutes. ERROR is not source absence. Migration052
admits340 decisions and indexes accepted intake by usable time. The current pointer
switches atomically only after all accepted scans×8coins are READY. Old adapters
cannot downgrade it; current views expose one version and audit views retain history.

## Verification

Automatic activation is recorded at05:05:50.911211UTC after all35 older scans
finished. The ordinary05:02 Watch contributed a36th accepted scan, subsequently
processed byv5. Final database read05:06:52UTC confirmed36scans×8coins=288 READY
samples, zero missing/pending formula jobs and zero errors,340 decisions percoin.

- Currentv5 has97,920 decisions:14,660 MATCH,76,876 NO_MATCH,6,384 UNKNOWN. The missing cases are explicit source-availability outcomes, not job failures.
- All88,480 overlapping v4 decisions are exactly equal as full JSON, including all prior features/missing reasons and both frozen BTC/asset contexts. All298 original definitions, hashes and orientations remain unchanged; only12 support flags were added.
- All four older versions retain their original counts and full-payload digests under the exact same pre/post query. The evidence stores both results and the concatenation algorithm; differently concatenated prior-turn digests are not comparable.
- Independent source verification covered all288 coins and1,728 directional score-change cells:960 known cells from20 eligible-predecessor scans,768 unknown cells from16 scans without a predecessor inside30minutes. All current/prior raw score, source-time, direction, delta, missing-reason and projection checks pass; no selector mismatch or later-intake drift.
- All source/context bindings pass. Shared BTC contexts remain one per scan; the8 new-scan rows correctly have no v4 predecessor sample. HYPE's captured CVD/OI scores participate in this score-change contract; its separate own-Spot limitation remains unchanged.
- Current views exposev5 only. Audit views retain9,792 v1,24,928 v2,57,664 v3,88,480 v4 and97,920 v5 decisions. All prospective flags remain false; discovery/promotion remain disabled.
- Measurements still represent ONE independent live BTC parent wave.36 scans and170 formula definitions do not establish independent market cases or proven statistical performance.

Final health05:08:28.008378UTC confirms all three research workers running with
zero failures/errors and activev5. The ordinary Watch completed successfully at
05:06:00.806353UTC, statuswaiting, last_errornull, nextscheduled05:32:15UTC.
No manual scan, notification, trade or scheduling change occurred.

Canonical complete-payload and all three context hashes already pass JSONB readback
for BTCsnapshot1231(NO_PREDECESSOR), ETHsnapshot1232(SELECTED) and
HYPEsnapshot1232(SELECTED). The exact hashes are in the evidence JSON.

The exact read-only SQL and results are preserved in the evidence JSON. This step
is complete; no further testing or deployment is needed for it.

## Efficient continuation

Use connected GitHub/read-only Render PostgreSQL. Workspace
`tea-d94cq7e7r5hc73dee88g`, service `srv-d94ek17lk1mc73b4tb90`, database
`dpg-d94d641kh4rs73evvih0-a`. Existing authenticated Render Shell is for explicit
staged schema admin and compact health. Browser variables `renderShell` and
`formulaHealthCommand`; read terminal via main.allTextContents(), not listitem only.

Do not rerun older coverage migrations048–051 afterv5. Apply only an exact new
migration after full CI; deploy after the ordinary Watch completes. Implementation,
migration and deployment are already authorized. No manual Watch, Telegram tests
or messages, unsupported HYPE own-Spot requests or trades. Never expose secrets,
DSNs, recipient IDs or captured notifications.

Local requests/dotenv/psycopg remain absent; do not install/search again. Pure tests
may use import-only requests get/post deny stubs. CI uses real dependencies and
disposable PostgreSQL18. Keep git-backed code/docs in GitHub. Tool stores may not
survive another turn, so continue from this document.
