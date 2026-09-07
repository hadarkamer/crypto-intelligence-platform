# Research decision freshness — 2026-09-07

The reported approximately 29-hour delay is reproducible in the `Outcomes`
Google Sheet. During the 16:44–16:54 UTC checks on September 7, the latest populated v7
`observed_through_utc` was September 6 at 11:37:59.999 UTC. An actual example
is event 6636, ETH SHORT, 240 minutes, 50 bps, outcome ID
`6636|240|50|ordered-first-touch-v7`. This is a source-data watermark, not a
worker execution timestamp or a claim that all earlier events are covered.

## Diagnosis

- The ordered First Touch export queue had 167,177 PENDING rows and 286 RETRY
  rows. Its FIFO selection still exported generations created on September 6.
  New calculations could not reach Sheets promptly behind this history.
- The generic six-tab delivery queue already alternated fresh and historical
  work. The separate ordered outcome exporter did not use that scheduler.
- The formula worker was running: `Formula_Results` contained evaluations from
  September 7 at 16:42 UTC. Reading only the first rows mixed old retained
  versions with current work and gave an incomplete picture of freshness.
- Current feature screening was also incomplete: at 16:48 UTC, 839 of 4,429
  eligible delivered alerts had current-v3 screens. In the latest 256 alerts,
  0/16 Max Pain, 0/39 Spot CVD and 0/17 Futures CVD alerts were screened,
  compared with 111/129 Magnet alerts. A latest-32 tail repeatedly selected
  the same later arrivals while an ascending cursor continued historical work.
- Formula scope registration could add 256 never-evaluated scopes per pass,
  while evaluation processed at most 128 within a time budget. Always choosing
  NULL evaluation times first postponed existing-result refreshes.

These are separate queues. Recent successful worker runs, a recent maximum
timestamp, and a complete decision population are different claims.

## Required behavior and verification

1. Ordered outcome delivery reserves capacity for recent observed source data
   and for the historical queue, including when receiver capacity is one row.
   Repeated claims or retries must not manufacture source freshness.
2. Formula ingestion gives unscreened current-version alerts in a bounded recent
   source window a turn while preserving the ascending history scan.
3. Existing formula results and never-evaluated scopes both receive evaluation
   capacity. Experimental priority must retain ordinary queue progress.
4. Verify real PostgreSQL claiming, rollback/restart fairness, and exact-payload
   ACK protection. Keep all v7 label, direction, evidence and acceptance gates.
5. After deployment, verify a newly computed v7 outcome in the actual Sheet by
   its outcome ID and source observation time. Confirm older queued outcomes
   still advance. Report remaining gaps separately from the improved newest
   watermark.

The existing hourly research task was updated at 16:55 UTC to compare source,
computation and Sheet timestamps,
check the active formula version, and distinguish missing coverage from a
negative research result. It must not mark the analysis complete merely because
the scheduled task executed successfully. Its schedule and research acceptance
rules were preserved; no duplicate task was created.

The HYPE archive supplement transfer remains outside this fix and has not been
approved or performed. Native HYPE reference provenance and missing historical
source evidence remain separately auditable limitations.

## Initial production verification

PR 11 was merged as `af0537843b1f374e95e834990ff2536e5fe74ee5` and
became live at 17:33:11 UTC. Its final head passed all 79 selftest files,
including 28 real PostgreSQL checks, source integrity and Apps Script contracts.
Migrations 039 and 040 committed after the delivery expression index was built
concurrently and verified both valid and ready. The exporter checks both flags
and falls back to FIFO if an index build is incomplete.

At 17:38:20 UTC, actual Sheet row 8649 held v7 outcome
`11729|60|50|ordered-first-touch-v7`, with measurement start 17:10:38.045183
and source observations through 17:33:59.999. The newest observation watermark
was about 4 minutes 20 seconds old instead of approximately 29 hours. This OPEN
outcome is an observation update, not a completed trade decision.

The 17:39:29 delivery batch also acknowledged historical event 6561 from
September 6 alongside current event 11727. Historical export continues without
blocking the fresh lane. In a pinned population of 256 pre-deploy alerts,
Max Pain screening initially advanced from 0/16 to 6/16 and Spot CVD from 0/36
to 8/36; these are partial coverage measurements, not formula qualifications.

The first deployment also exposed two intermittent, previously unbounded
queries: whole-history DISTINCT scope discovery inside formula INGEST_MATCHES,
and BTC episode membership assignment before its final LIMIT. Their timeouts
can roll back useful intake work. Follow-up verification must include bounded
discovery, preserved history progress, and consecutive completed formula cycles
before claiming stable analysis. The historical population remains incomplete.

The follow-up replaces whole-history scope discovery with an indexed page of
at most 128 source IDs, plus the current ingestion batch. A page with remaining
scope cells stays pending; a bounded cell lookup avoids scanning all scopes.
BTC membership assignment admits at most 500 source candidates split between
recent delivered alerts, recent decision samples and a finite historical lap.
Already assigned sources are excluded before sample outcome admission. Existing
LIVE, causal closed-bar and immutable BTC_DATA_MISSING policies are unchanged.
Both scanners reuse migration 038 and commit cursor progress with their writes;
neither needs a new schema migration or a larger timeout.
