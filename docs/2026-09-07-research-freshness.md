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
