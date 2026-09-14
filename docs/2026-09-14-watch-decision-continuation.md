# Continuation — frozen Watch decisions, 2026-09-14

This continues `docs/2026-09-14-watch-timeframe-continuation.md`. Read the final
verification record for this release before acting on deployment state. The
implementation, full CI, migration, merge, deployment and normal-scan validation
are COMPLETE. Read `docs/2026-09-14-watch-decision-verification.json` for the final
verified state; continue from the next bounded step below. Do not repeat completed earlier rollouts
or the broad historical audit. Design: `docs/2026-09-14-watch-decision-capture.md`.

## Verified release

- PR43: https://github.com/hadarkamer/crypto-intelligence-platform/pull/43
- Merged main: `b1ad1e49b6506620dcff3262bcc715089c4a876d`.
- Exact tested/merged tree: `c8d8fd7233c5f5880fe4373310ea16af06e45fa6`.
- Full CI: all 129 selftest files passed, including six new PostgreSQL tests.
  Final CI run/job: `34815129451` / `103884116992`.
- Applied migration: `055_watch_scan_decision_captures.sql`.
- Migration SHA256: `78d1e0a8b00614f6a14e19422f2d3bdda0c425423867b28fe1dd40ba2274ab0d`.
- Render deploy: `dep-dajpl0oae00c73edf4pg`, live at06:57:27.511937UTC.
  Health at06:57:57UTC verifies exact merged commit and all four research workers
  running without errors. Watch remains scheduled for07:02:15UTC.
- The ordinary07:02:15UTC Watch cycle completed at07:06:17.283446UTC.
  Snapshot1308 has a COMPLETE decision capture and all8SOURCE_BOUND coin rows.
  Frozen-source semantic validation, canonical JSONB hash and all deployed code
  hashes passed. The next normal cycle remains07:32:15UTC.

Implementation worktree: `/workspace/scratch/1a8c1bcb93ce/watch-combined-capture`.
Continuation worktree: `/workspace/scratch/1a8c1bcb93ce/watch-state`.
Durable continuation branch: `checkpoint/research-continuation-20260913`.

## What this increment adds

Normal shared Watch now freezes the operational decisions that already accompany
its scores: selected MaxPain items and literal confirmations, all evaluated
Combined groups, exact group top items, qualification and signal lists, and all
actual Magnet evaluations including negative, missing and error results. The
ordinary retained/displayable/optional Top8 input subset is preserved. Combined
runs once; the same successful candidate list, including an empty list, reaches
the existing later collector, which still owns lifecycle state and delivery.

The new sibling `source_metadata.capture_metadata.operational_decisions` uses
`watch-operational-decisions-v1` and population
`watch-all-scan-operational-decisions-v1`. It is included before the immutable
parent archive hash is computed. Its own canonical hash binds the original
score bundle, input universe, scan identity, computation time and code versions.
There are eight coin entries, a 256 KiB bound, and explicit completeness/missing
evidence. A bounded FAILED diagnostic preserves the base score capture and
ordinary alert path if serialization or evidence validation fails.

Migration 055 adds only the read-only view
`research_watch_scan_decision_captures`. Every accepted scan retains eight rows;
old missing blocks remain NOT_CAPTURED. SOURCE_BOUND proves source linkage only.
Consumers must call `research_watch_decision_capture.validate_bundle` before
interpreting evidence. Computed time remains SQL text so malformed captures do
not break the view. All delivery/prospective-evidence flags remain false.

No formula evaluator, supported definition, research worker, schedule or outcome
contract is added here. Coverage remains **204/298**: coin v5 supports 170 original
definitions and the selected-timeframe supplement supports 34. The 94 remaining
definitions stay unsupported. Prior v1–v5 and timeframe payloads/pointers must be
unchanged; the complete production comparison passed under cutoff1306.

## Completed ordinary-scan verification

Snapshot1308 contains56 core selected items,12 Combined groups,4 qualified
candidates and28 actual Magnet evaluations. All eight coin contexts are COMPLETE
with zero missing reasons. The206,575-byte block is below the256KiB bound.
Every MaxPain status in this scan is BELOW_SCORE; Magnet results are18
NOT_CONFIRMED,9OBSERVATION and1LIQUIDITY_CONFLICT. Negative literal states are
retained and validated. No confirmed Magnet or delivered Combined event is
inferred from the four qualified candidates.

The global capture counters report224 prepared items and107 Magnet evaluations
across all source symbols. The compact coin projection above contains only the
eight core symbols; these are different scopes, not lost or duplicated records.
All39 older accepted scans remain NOT_CAPTURED (312 coin rows), and the new
scan has8SOURCE_BOUND rows. All flags still require consumer validation and deny
delivered/prospective status. Historical evidence was not manufactured.

At07:08:04UTC,40 accepted scans have320READY coin-v5 and320READY timeframe
results, with zero processing errors. Coverage remains204/298 with zero definition
drift and both active pointers unchanged. All six old evaluation-version groups'
counts and complete-payload digests match exactly at snapshot_set_id<=1306 using
the same before/after query. The population still has ONE independent LIVE BTC
parent wave; scans, timeframes and inverse definitions do not multiply cases.

Health after the normal cycle confirms Watch waiting without error and all four
research workers running with zero failures. The dual-CVD rule has a third durable
ACCEPTED receipt, with8NO_MATCH,0UNKNOWN,0MATCH and zero intents. This validates
scheduled evaluation; no matching notification delivery or performance is claimed.

Read-only reproduction files are in `docs/verification/decision-capture/`:
`verify.py`, `binding.sql`, `state.sql`, and `preservation.sql`. Run the Python
verifier from the deployed project directory. It reads a single accepted source
and validates frozen proof without importing main or invoking providers/sends.
Do not rerun the production rollout or wait for another scan merely to repeat
these completed checks.

## Next bounded step: evaluator contract and original-definition mapping

After the live capture is verified, map the **42 relevant original definitions**:
22 Combined top-item, 16 captured confirmation, two Combined-dependent liquidity,
and two standalone Combined-event definitions. This is a source/selection and
population contract step; capturing evidence does not automatically activate all
42 or establish that their existing event semantics are satisfied.

Specify the versioned observation unit, source-side selection, group/top-item
identity, cohort membership, UNKNOWN and NOT_APPLICABLE rules, and outcome reuse
before implementing or activating an adapter. Preserve original definitions,
hashes and inverse semantics. Source SHORT means price LONG; source LONG means
price SHORT. Inverse predicates retain their base proof and reverse outcome
direction only. Use the actual chosen top item and its source/frozen liquidity;
ordinary coin means or another timeframe are not substitutes.

Preserve literal MaxPain BELOW_SCORE, CONFLICT and UNCONFIRMED labels. Do not
silently rename them catalog NOT_CONFIRMED or OBSERVATION. An active Combined
candidate is not a delivered COMBINED_CONFIRMATION event. Any interpretation
requiring actual delivery or sequence entry needs its own proven source contract.
Historical absence, capture failure and missing selected items remain UNKNOWN;
absence from a confirmed-only map is never a false predicate. Use frozen sources
only, without later outcomes, source rebuilding or historical decision backfill.

## Remaining ordered plan

1. Research from all scans — IN PROGRESS; this operational capture increment is
   complete. The bounded evaluator contract above follows.
2. Continuous discovery of new formulas — pending.
3. Historical archive completion — pending.
4. HYPE integration completion — pending.
5. Formula validation on new independent cases — pending.

The remaining 94 definitions comprise the 42 above, sequence entry/family order
34, deferred MaxPain components 10 and absent short MaxPain horizons 8. This
capture does not complete sequence/event semantics or create missing horizons.
Cluster/CVD component hypotheses, B4 and 240m family normalization remain deferred.
HYPE's valid captured CVD/OI totals participate; own Spot-price features remain
unavailable and outcomes remain Hyperliquid PERP TRADE. The separate HYPE phase
is not completed here. Discovery, promotion and trading remain disabled.

Continue one bounded step at a time. Use existing GitHub and read-only Render
verification. Do not rerun old coverage migrations, trigger manual Watch, send
test messages, request unsupported HYPE prices or trade. Local requests/dotenv/
psycopg are absent; do not install or search again. CI supplies real dependencies
and disposable PostgreSQL. Keep the final continuation and evidence in GitHub.
