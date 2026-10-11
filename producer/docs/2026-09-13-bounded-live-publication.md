# Stage 6: bounded live Sheet publication

The workbook was using about 8.07 million allocated cells against the existing
9 million safety limit. Formula_Current and Outcomes_Current were already
bounded. This change bounds the remaining growing publication lanes while
keeping the canonical research, classifications, cohorts and Telegram behavior.

| Destination | Stable slot key | Maximum data rows | Columns |
| --- | --- | ---: | ---: |
| Snapshots_Current | symbol,direction | 16 | 59 |
| Live_Current | מטבע,כיוון נבדק | 16 | 26 |
| MaxPain_Current | symbol,timeframe,source_side | 176 | 35 |
| Telegram_Events | event_id | 32,000 | 15 |

MaxPain includes all 11 timeframes supported by historical source exports; the
active capture currently uses seven. A missing slot is not an invented zero.
These are the latest **captured source events**, not continuously refreshed
market quotes. Different slots can refer to different events and times.
Live_Current adds explicit UTC alongside the original Israel display time.

## Publication and history

The generic sender no longer claims Snapshots, תצוגת לייב or MaxPain_TF. Their
existing sheet rows and all outbox states remain unchanged. New complete source
groups project into current slots inside the source transaction. Newer source
timestamps win, with source identity as the deterministic tie-break. Same-source
repairs replace the complete row, including blanks. PostgreSQL and the receiver
both reject source regression; exact-generation ACK/lease semantics remain.

On process startup, two indexed legacy-lane queries seed the latest snapshot
and MaxPain slots. Live rows come from the exact selected snapshot key. Seeding
does not reset source markers, replay the event population or copy old ACKs.
Accepted rows are PENDING until delivered. A concurrent newer slot wins.

## Telegram retention and audit

Archive the existing native tab as Telegram_Archive_20260913 before rollout.
The receiver requires this archive to exist before reusing any Telegram row.
It appends until 32,000 rows, then replaces only the oldest physical row whose
valid source timestamp is strictly older than 16 days. It never sorts, shifts
or deletes rows. Existing IDs update in place. If no expired row exists, the
whole request fails before writing and remains retryable. Incoming expired or
undated payloads cannot evict anything; the sender holds such pending records
in their existing DB queue state without acknowledging them.

Sixteen days is a protected minimum, not a scheduled deletion deadline. Older
rows may remain when there is space. Full canonical history stays in PostgreSQL;
the initial archive preserves the existing Sheet-specific historical material.

The audit still freezes a 14-day delivered-alert population ending two minutes
before its start. Its maximum population is now 32,000. Each scan expires after
24 hours, including a check after the HTTP response. This is less than the
almost 48-hour retention margin: an expected row cannot be recycled during a
valid scan. Appends beyond frozen last_row and replacements of expired rows
cannot move protected row positions. Expired scans reset visibly and cannot
certify a result. Stage 5 transport retry recovery is retained.

## Rollout and verification

1. Preserve the Telegram archive and create the three fixed-size current tabs
   with exact headers and source/interpretation notes.
2. Test Python projection/SQL, stale generations, source clocks, held history,
   and actual 32,000-row receiver retention plus concurrent paged readback.
3. Publish a backwards-compatible receiver version on the existing deployment.
4. Merge only the CI-verified backend head, then verify the automatic Render
   deploy and source workers. Check current tab keys/timestamps against DB
   payloads and confirm the old three lanes stop changing.
5. Record verification results in the continuation checkpoint.

No DB migration, research rule change, extra market fetch or Telegram action is
part of this step. The next research step is durable capture of exact operational
model scores before display thresholds, followed by the cluster/CVD hypotheses.
