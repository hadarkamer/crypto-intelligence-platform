# Sheets synchronization checkpoint — 2026-09-07

## Verified rollout, 05:45–05:50 UTC

Production commit `512274e9b0676a5f8b97380586a64b2408c2169f` is live on
Render deployment `dep-daf4tlks728c738hfho0`. Migrations 025–028 committed
at 05:45:22.186 UTC; schema apply error is null and both the source sync
and ordered formula workers are running. Earlier installation attempts
rolled back completely: 025 first exceeded its 15-second statement limit,
and the next attempt completed 025 but timed out in 026. The final release
allows 60 seconds per statement only during those two installation files,
resets to 15 seconds after each, and retains the 1-second lock timeout.
026 contains multiple statements and completed in about 69 seconds total.
Dedicated installer tests verify the exact filename allowlist, reset,
rollback, unchanged later migrations and unchanged untargeted installer.

Post-installation PostgreSQL checks found all five tested queue/period
indices valid and ready and the source timestamp trigger enabled. EXPLAIN
uses the new ordered index for the v7 claim and the new source-time index
for fresh generic delivery, without a global queue sort before LIMIT.
The generic worker subsequently confirmed 10 of 10 claimed deliveries.
Ordered v7 delivery confirmed four rows on its second pass; the first
eight-row attempt was unconfirmed and remains retriable. This verifies
progress, not completion of the backlog or sustained throughput.

Actual Sheet readback at 05:46–05:49 UTC found September 7 alert records
in Telegram_Events, including ZEC Magnet at 08:37 Israel time. The first
September 7 visible-view arrivals were explicitly neutral prospective
anchors, not alerts. Do not equate these rows with independent evidence
or claim the full visible-view alert backlog was cleared from this sample.
At 05:43 UTC the source had 781 delivered alert records from September 7;
all were pending then. Four were acknowledged by 05:47 UTC.

Both new LIVE periods are registered: 2,432 scopes per period, 4,864 total,
plus all 2,432 legacy scopes preserved. At 05:48:59 UTC, 124 ALL-period
and 132 SINCE-September-4 scopes had been evaluated; each period had at
most one provisional wave and zero ready formulas. Input replay remains
incomplete, so these are not final period statistics. Read-only guards
found zero inconsistent OPEN counters, zero exposed rates for incomplete
coverage and zero parent-wave starts crossing the relevant cutoffs.
Formula_Results row 1160 was read back with the SINCE period version,
DATA_MISSING=1, OPEN=0 and withheld rates, verifying the exported status
distinction rather than merely inspecting the code.

Archive migration 028 is installed but production archive tables have not
been loaded. The verified portable source intake was saved separately:
14,354 unique messages, 11,466 signal messages, 318 messages since September
4, with 216 exact prior-import event links to 119 existing snapshots.
August 30–September 3 has no supplied export coverage. Every source remains
ARCHIVE_ONLY and ineligible for formula evidence. Prepared-stage digest:
`92939e9facb691381fd14d4117b85377903cabc130c2945d060cb9eafda48c56`.
The retained ZIP SHA256 is
`3e2361bbe01aca406c64bbac857e8029e2f39b85c34bb1d26621729b91e3ca32`.

Remaining work: finish backlog/replay and monitor actual delivery; deploy
the separately authenticated Google receiver update (currently unversioned,
batch size 1); reconstruct and validate archive causal features, times,
v7 outcomes and native duplicate/parent mapping before statistical union;
implement common-window asymmetry and future validation; expand research
beyond the seven total-score-65 candidate families. No regular/FRESH
relevance or trade authorization is implied by this infrastructure release.
The Sheet research questions and field dictionary contain the final
readback audit. Earlier sections below are historical checkpoints.

## Authorized continuation, 2026-09-07

The user accepted the proposed synchronization, status-reporting, archive and
date-period steps and requested their execution. This supersedes the older
pending-authorization note below. Current changes have not yet been verified
on production at this checkpoint.

- Migration025 and FIFO delivery admission remain included in the release.
- Migration026 adds validated source UTC, newest/backlog lanes and a persistent
  delivery cursor. Batch-size-one delivery retains both shares across restarts.
  Staging stays compatible before026; the new sender explicitly defers until
  its migration is installed. Retry, lease, claim-token and generation guards
  remain enforced.
- Formula reports distinguish SUCCESS, FAILURE, OPEN, AMBIGUOUS, NO_TOUCH and
  DATA_MISSING. Invalid quality metadata cannot qualify an outcome. Incomplete
  populations withhold rates and excursion metrics.
- Migration027 creates separate native LIVE calculation scopes for
  ALL_COMPATIBLE_SINCE_20260816 and SINCE_20260904, using Israel-midnight
  cutoffs. Legacy results remain audit records. Whole parent waves crossing a
  cutoff are excluded with coverage counts; every underlying event/outcome
  remains stored. Overlapping periods are never additional validation evidence.
- Migration028 and explicit import tools isolate Telegram source messages from
  LIVE/v7 evidence. A portable SQLite intake can be retained when a production
  import connection is unavailable. This is not archive feature/outcome/wave
  reconstruction and does not qualify an archive formula.
- Apps Script source-time header validation is tested in the repository, but
  its deployed Google receiver still requires a separately authenticated update.
  The production endpoint reported `unversioned` and batch size1 at05:16UTC.
- All52 Python self-test scripts passed in40.28seconds. Apps Script batch and
  timestamp contract tests also passed. PostgreSQL plans, migrations and actual
  Sheet arrival remain post-deployment checks; SQLite tests are not PostgreSQL
  concurrency/planner verification.
- The Sheet Formula_Results grid was expanded to10000rows for two period
  populations and preserved legacy results. Q61/Q62/Q66/Q67/Q69 coverage and
  Q72 period/archive status were updated without changing the original questions.

Remaining research gates: causal archive feature reconstruction and verified v7
labels/parent mapping; full common-window MFE/MAE and independent future
validation; broader question-driven candidate search and direction/source
coverage. No regular or FRESH relevance is enabled by this release alone.

## Production evidence (read-only, approximately 03:11–03:18 UTC)

- Production remains on `677ec0a6bf100a71acc18ae0f3d5ef3a5d07ac90`.
- Since midnight in Israel (2026-09-06 21:00 UTC), 603 `ALERT`
  source records are marked `DELIVERED`. All 603 have an exact
  `Telegram_Events` outbox key and are `PENDING`, without an acknowledgement.
  These are alert records, not independent market waves or necessarily
  distinct Telegram messages.
- No September 7 alert was present in the inspected Sheet snapshot. This
  is a delivery backlog, not evidence that the source emitted no signal.
- Generic delivery is progressing, but orders each preferred sheet by
  oldest due/updated time. Recent alerts remain behind historical work.
- The v7 outbox claim has a separate, confirmed statement timeout. Its
  production EXPLAIN used a sequential scan and global sort of roughly
  96,836 active rows before LIMIT 8. Three later acknowledgements demonstrate
  intermittent progress, not recovery of the backlog.
- Latest source formula checkpoint: seven candidates, 2,432 scopes, at
  most one independent BTC wave and one recent wave per exact scope;
  zero research-ready scopes. No regular or FRESH formula is qualified.

## Prepared locally, NOT deployed

1. Migration `025_ordered_first_touch_sync_claim_queue.sql` adds an ordered
   partial index covering PENDING, RETRY, and IN_FLIGHT v7 delivery rows.
2. The claim query adds an equivalent active-status predicate so PostgreSQL
   can use that index. The original single atomic SKIP LOCKED claim,
   identity, due/expired-lease rules, claim token, and payload-hash ACK
   protections are unchanged.
3. Shared delivery admission now remembers FIFO turns and waits at most
   120 seconds BEFORE any DB claim. The existing reentrant HTTP lock stays
   in place. This mitigates sender starvation separately from the SQL fix.

All 48 Python `*_selftest.py` scripts passed, including the two new queue
and concurrency tests. Independent review approved the diff. SQLite query
parity is not a substitute for PostgreSQL concurrent-lock/planner testing.
Production index validity, the new plan, acknowledgement progress, and
absence of duplicates must be verified after an authorized deployment.

## Sheet timestamp correction

The live-view A1 header again read `מה`, while the exporter supplies the
exact key `זמן סריקה`. No audited repository writer modifies row 1, so the
actor or other source of this regression is unverified. Do not blame the
user or claim the regression has been permanently fixed.

The header was restored. Another 29 blank cells, A1286:A1314, were filled
only after matching snapshot ID, symbol, direction, and UTC timestamp to
the frozen Snapshot outbox and `research_events`, then verifying Israel
time. Readback confirmed all 29 values and their date/time format. Eleven
subsequent writes included their source time naturally. The final read of
1,324 data rows found no blank source-time cells on non-DEMO populated rows.
No NOW,
First Touch decision time, or fabricated time was used. Q69 and Q70 record
the remaining backlog and unresolved header regression.

## Next scoped steps

- Obtain deployment authority for this new patch; do not infer it from
  approval of the preceding commit. Apply migration025 with the compatible
  code and verify on production before claiming the queue is repaired.
- Separately implement and test fair fresh-alert/backlog scheduling in
  the generic outbox. Use canonical source UTC time, not replay insertion
  or update time; retain an oldest-due share, including batch-size-one
  fallback, and all retry/lease/generation guards. The v7 index alone does
  NOT fix this priority problem.
- Add receiver header-contract validation before writes when the deployed
  Apps Script can be updated. Missing `זמן סריקה` must produce an explicit
  failure, not silent blank timestamps and a successful acknowledgement.
- Continue research on valid versioned evidence; keep missing, open,
  ambiguous, and closed-without-touch states separate. Do not qualify a
  formula or count overlapping outcomes as independent waves.
