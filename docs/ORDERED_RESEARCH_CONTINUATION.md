# Recurring ordered-v7 research

This additive path continues the September 6 First Touch repair. It reads
committed decision snapshots and `ordered-first-touch-v7` outcomes from
PostgreSQL. It does not depend on the Google mirror being up to date.

## Global BTC movement policy

`btc-parent-close-reversal-200bps-v1` is a new conservative engineering choice,
not a previously user-specified numerical definition or an optimized trading
rule. It uses the largest researched price target, 2%, as a close-to-extreme
reversal threshold. Smaller zigzags remain one parent movement. All coins,
directions, thresholds, horizons, and formulas share that parent identity.

Only closed Binance Spot BTCUSDT one-minute candles establish a boundary. The
new movement begins when the reversal becomes observable; a later pivot is
never backdated. The incomplete beginning of the archive and a discontinuity
remain ineligible until a fully observed reversal establishes a boundary.
There is no 24-hour or 72-hour wait, survival requirement, or return-to-entry
condition. Operational separation is not proof of statistical independence.

## Formula evidence

The new research path starts with frozen simple and paired family conditions
and `STRICT_TRIPLE_TOTAL_65`. Direction, coin, all eight 25–200 bps thresholds,
and four 60/240/720/1440-minute horizons remain distinct.

This first runtime screen contains seven fixed conditions/combinations at total
score 65, using delivered alerts. It does not yet execute the full research
question catalog, compare silent/no-signal populations, or perform independent
prospective and full-horizon acceptance. Those limits are part of each result.

Match selection precedes label inspection. Multiple coins or alerts in one BTC
movement contribute at most one evidence unit to a result. A repeated-alert
condition starts at the required repeated observation. Missing, ambiguous,
open, and no-touch outcomes cannot become losses or successes by inference.
While source ingestion or membership assignment is incomplete, usable rates
and sample-count eligibility remain blocked. A later successful alert cannot
replace an earlier unresolved matching alert from the same wave.

Five resolved parent movements satisfy the standard sample-count gate; three
within the last 14 days satisfy the FRESH count gate. A count gate alone does
not establish probability, movement asymmetry, out-of-sample validity, or
permission to issue an experimental alert. The legacy v6 native engine remains
quarantined. New result rows explicitly identify any remaining validation gap.

The v7 MFE/MAE measurements end at the decisive first-touch candle; they are
not full-horizon movement-potential estimates. This scope must remain visible
when displaying their ratio.

## Durable Google mirror

Snapshots can be reconstructed from committed source records after missed
best-effort deliveries. Reconciliation stages exact versioned row payloads;
only acknowledgment of the claimed payload generation marks it synchronized.
Formula episodes and result rows use the same generic destination outbox.
The ordered outcome outbox remains separate and retains its original identity.

When the old Apps Script receiver times out, the backend reduces request size
and defers unclaimed rows within a bounded pass. This improves delivery
reliability but does not remove the receiver's repeated full-sheet reads.
Deploying `google_apps_script/Code.gs` still requires a signed-in Google editor
session. Reading/writing a Sheet through the connector does not grant access
to deploy its bound Apps Script.

## Deployment and observation

Apply exact additive migrations 021, 022, and 023 using the existing targeted
schema installer. The new background loops expose status through `/health` as
`btc_episodes`, `formula_ordered_v7`, and `snapshot_sync`. Each has its own
bounded work batches and verifies its own tables. Bootstrap and replay advance
automatically across passes; no manual rerun or new chat is required.

Verify source-bar progress, LIVE event-to-parent mappings, candidate-specific
episodes, result scopes, and actual receiver acknowledgments separately.
Historical replay is descriptive and must not be presented as prospective
out-of-sample validation.
