# Outcome recovery for current Max Pain waves

## Verified incident

On 2026-09-08 at approximately 06:52 UTC, the live Outcome worker was running
without a current error, but its historical and OPEN queues lagged behind the
captured decision population. A recent source timestamp in Sheets did not prove
that all older selected representatives had been updated.

The requested BTC parents are:

| Display wave | Parent ID | Start UTC | End UTC |
| --- | --- | --- | --- |
| 9 | fd74e66a8ab5675cdf922162054b817b1c8669eab97f613495c7562c8d49883c | 2026-09-06 23:03:59.999 | 2026-09-07 15:32:59.999 |
| 10 | 4de51eac407704fd0db96d1c5c5b3017205869bc2c035c1b97a8676ef7c4cd2a | 2026-09-07 15:32:59.999 | Active at incident audit |

The native delivered `MAX_PAIN_ALERT score >= 65` population contained 68 events
in wave 9 and 101 in wave 10. Only 35 and 3, respectively, had all 32 v7 outcome
rows, and none had a READY common-window metric. Existing rows were often still
observed only through their initial minutes on September 6 or 7. The BTC source
updater itself was current through 2026-09-08 06:47:59.999 UTC.

First qualifying events per coin/direction, chosen before inspecting outcomes:

| Wave | Event IDs |
| --- | --- |
| 9 | 8007, 8008, 8175, 8641, 9325, 9752, 9850, 10932 |
| 10 | 11221, 11355, 11356, 13021, 13345 |

The native history cursor was at event 368 of high-water 11778. The ordinary
recent intake considered only 128 latest alerts, while OPEN refresh and
common-window work also had earlier work ahead of these decisions.

## HYPE source evidence

The operational provider reads Hyperliquid `allMids["HYPE"]`, a perpetual
instrument. Captured rows preserve `price_source=hyperliquid` and
`price_pair=HYPEUSDT`, but omit exact market/instrument metadata. They therefore
cannot satisfy the native Spot `@107` reference gate. Of 68 delivered HYPE Max
Pain alerts since September 6 at the audit cutoff, none had a native v7 outcome.

Do not rewrite those immutable references as Spot. The separately versioned
Binance Futures MARK derivation uses its own next-full-minute entry, retains the
original event/source/reference and parent membership, and stays distinct from
canonical Spot evidence. Public price retrieval needs no private archive upload.

## Measurement and acceptance constraints

- Canonical v7 outcomes retain 60/240/720/1440-minute horizons and eight symmetric
  thresholds: 25, 50, 75, 100, 125, 150, 175 and 200 basis points.
- Full BTC-wave endpoint measurement has its own version. A closed wave ends at
  its recorded BTC boundary; an active wave is explicitly provisional through
  the latest fully closed source minute.
- Choose the earliest qualifying decision before fetching outcomes. Additional
  coins or alerts in one parent do not create another independent BTC wave.
- Keep price gaps, same-minute two-barrier ambiguity, missing provenance and
  unverified parent boundaries visible. They must not become synthetic wins,
  losses or replacement representatives.
- Probability counts decided cases and exposes its denominator. Full-window
  asymmetry uses the same complete cohort and ratio of summed MFE to summed MAE;
  it cannot be taken from excursions truncated at the first barrier touch.
- Native exact-type candidate events currently occur in parents 2, 8, 9 and 10.
  The presence of ten BTC parents alone does not establish ten candidate waves.
  Earlier archive `WATCH_CANDIDATE` rows require their own documented source
  contract; a report must not silently infer equivalence from a prior chat.

## Deployment acceptance

Record the tested commit, migration result, requested-queue coverage, actual
Sheet delivery and explicit report cutoff after completing the recovery.

## Native HYPE recovery after the production price probe

PR14 deployed as `83035da3c59787f4eb4d55195de84282e24660fc`; migrations 041
and 042 applied. A public Binance Futures MARK request from Render returned
HTTP 451, although the same source was available in the research environment.
The dated full-wave report remains explicitly Binance MARK and must not be
silently replaced with another price series.

The ongoing native HYPE recovery therefore uses Hyperliquid's independent
`candleSnapshot` source for the exact `HYPE` perpetual instrument. Migration 043
creates separate PERP event, outcome and common-metric tables. Scope
`DERIVED_NATIVE_HYPE_PERP` and adapter `native-hype-perp-derived-outcomes-v1`
retain the original alert/reference and use their own next-full-minute entry.
Neither canonical Spot nor existing MARK evidence is rewritten.

The v7 capacity defaults to 32 events per pass, independently of the legacy
eight-event limit, with half the capacity retained for ordinary work. OPEN
refresh is due at the next fixed horizon or within 30 minutes; HYPE uses its
persisted derived entry for that boundary. Failed paths retain a 15-minute
retry. Actual pass duration and recovery counters are exposed in health.

The full-wave Sheet snapshot at 2026-09-08 07:21:03 UTC contains all eight
thresholds. Wave 9 ends at 2026-09-07 15:32:59.999 UTC and active wave 10 is
observed through 2026-09-08 07:19:59.999 UTC. Four qualifying BTC parents, three
closed and one active, are represented; this is not ten independent cases.
It is a dated full-wave report, distinct from the worker's fixed horizons.
