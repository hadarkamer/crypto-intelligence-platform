# Continuation: standalone experimental dual-CVD Watch alert

## User request and scope

The user asked which historical plan steps remain, then explicitly requested
an experimental alert for both Futures CVD and Spot CVD total scores >=65 in
the same supporting direction, predicting that shared direction. This turn
implements that specific new live alert. It does not advance unsupported
research predicates or claim that the requested formula statistically qualified.

## Remaining ordered research plan

1. Research from all scans: 170 of the original 298 candidate definitions are
   supported by the current immutable v5 adapter; 128 remain unsupported.
2. Continuous discovery of new formulas.
3. Historical archive completion.
4. HYPE integration completion.
5. Validation on new independent cases.

The 128 unsupported definitions comprise liquidity 34 (32 standalone plus two
Combined), Combined top 22, captured confirmation 16, sequence entry/family 34,
deferred Max Pain components 10, same-timeframe score difference two, absent
short-timeframe Max Pain eight, standalone Combined two. The next bounded
research increment can inspect the 32 standalone liquidity and two same-TF
score differences without changing the other predicates or creating extra
independent waves. Cluster/CVD component hypotheses, B4 and 240m family
normalization remain deferred by the user's earlier instructions.

## New alert contract

- Canonical identity:
  `captured-question-search-v3-experimental-binding:CORE_FUTURES_CVD_SPOT_CVD_TOTAL_65`.
- Rule version: `dual-cvd65-experimental-watch-v1`.
- Both valid signed model totals have magnitude >=65, inclusive, and both
  explicitly support LONG or both explicitly support SHORT. The prediction
  follows that shared direction. Valid low totals or opposing directions are
  NO_MATCH. Missing, stale or inconsistent evidence is UNKNOWN.
- Use the exact frozen all-eight-symbol operational bundle from normal shared
  Watch, before display thresholds, top-eight output filtering and ordinary
  Telegram sends. No OI, Max Pain, short-family, own-price or statistical gate.
  HYPE is eligible when both its captured CVD sources are valid.
- First fresh post-activation match fires; continuous matching direction does
  not repeat. Valid NO_MATCH resets immediately at65; UNKNOWN preserves state.
  A changed direction may fire. The same symbol/direction/two-CVD-candle
  generation cannot fire again after a restart or within-generation reset.
- A persistent per-recipient activation timestamp excludes old captures.
  An ordinary active general Watch subscription and its current destination
  authorize delivery; authorization is rechecked after the DB claim.
- Explicit migration053 creates separate scopes, receipts and intents. Runtime
  only checks readiness. State, replay receipt and frozen message commit in one
  transaction. The supervisor recovers pending messages between normal scans.
- Expiry is min(source+10m, Futures close+30m, Spot close+30m). Two messages per
  drain; each send has a20-second timeout. A positive integer Telegram
  message_id is required to mark DELIVERED. Unknown/failed/orphaned attempts
  are never resent; an orphan becomes UNKNOWN after two minutes.
- Original existing MP65 and MP65+CVD-short behavior, Watch scheduling,
  operational scores, current research adapter and qualification gates are
  unchanged. No trades or manual/test Telegram messages are sent.

## Code and verification pointers

Implementation is in `dual_cvd65_alert.py`, `dual_cvd65_store.py`,
`dual_cvd65_delivery.py`; source/delivery/startup/health hooks are in `main.py`.
Migration `053_dual_cvd65_experimental_watch.sql` is registered in the explicit
schema admin and must be applied by exact basename alone. The migration adds
no research activation update or historical backfill.

The three new self-test files are matched by CI's `*_selftest.py` glob: 13
pure detector tests,11 fake-bot/Watch integration tests,8 PostgreSQL tests.
Existing Watch-path fake injections were adjusted for the additional hook.
Local pure detector tests and six existing AST Watch-path tests passed.
Full runtime dependencies and PostgreSQL validation use the existing GitHub
Actions PostgreSQL18 service; do not install or hunt for local dependencies.

PR: https://github.com/hadarkamer/crypto-intelligence-platform/pull/41
Tested head: `a6a031186e3b7fb85127bd4a413112f7ca9d1592`.
Tested tree: `5b01e7474aefc0975b9796ec2b1c17bb72d03079`.
Migration053 SHA256:
`e7924acf7699b76af6a777ff83eb012238d03d1ec877483fa129700b14d439b8`.
Live rollout and verification evidence are in the companion JSON; its statuses
are authoritative and must distinguish startup readiness from an actual
post-activation Watch evaluation or real matching notification.

## Resume safely

Use GitHub and read-only Render connectors first. The existing Render Shell is
only needed for staged exact migration admin, local /health, or read-only
inspection with the deployed detector. Do not trigger a manual Watch cycle or
send a test Telegram message. Inspect the new `dual_cvd65_experimental` health
entry and the separate dual-CVD state/outbox counts after the next ordinary
Watch scan; do not print chat IDs, tokens, DSNs or captured message contents.

Then continue one bounded research step at a time. Reuse the frozen v5 adapter,
170 supported catalog records and existing code/context from the earlier
score-change continuation. Do not repeat the broad historical audit or add
new independent-wave counts merely from additional coins or horizons.

## Completed rollout

PR41 merged as `426d0ada329226bbdb0df6edf390d707c8ecc6ce`.
Render deploy `dep-dajoh2lg1s2s73cl7dug` became live at
2026-09-14 05:40:43.593888 UTC. All124 self-test files passed; the
new13 detector,11 delivery/integration and8 PostgreSQL tests all passed.
Migration053 completed successfully before merge. Watch was idle after its
05:35:47.695580 UTC scheduled completion when the change was merged.

At05:42:11.413210 UTC the deployed commit was verified; general Watch,
coordinator and supervisor were active, dual-CVD readiness was true with
zero evidence gaps, and all three research workers were running with zero
failures. One subscription scope activated at05:40:39.842537 UTC.
No receipts or intents existed yet, as expected before the first new scan.
The next ordinary Watch was scheduled for06:02:15 UTC.

A read-only run of the deployed detector on the latest accepted captured
scan (snapshot1302,source05:35:35.159239 UTC,CVDcloses05:30 UTC) produced
8 NO_MATCH,0 MATCH,0 UNKNOWN, including valid HYPE inputs. This was a
read-only compatibility check of pre-activation data, not a new Watch
scan, a historical live alert replay or a real delivery test.
Actual first post-activation scheduled evaluation and any matching
notification are still to be verified on the normal schedule.
