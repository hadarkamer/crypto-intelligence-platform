# Model 3 preparation and candle-tooltip diagnosis

Prepared 2026-10-31. **Corrected live experiment succeeded; temporary CLI authorization revoked.**
This is a finite diagnostic, not a product fix or deployment.

## Reviewed versions

- Collector: `f6adfc6fea05bfd82a59098433ad1c2f31f6267f`.
- Undeployed six-hour cadence fix: trade-plan-pad PR120,
  `e3fd4d116eaecaf43e870a8b5ae3aa676a334e71`.
- PR120 A/B isolated checks passed previously. GitHub CI run41219869234 now
  completed successfully. No cadence settings are applied by this experiment.
- Runtime hashes guard the generated collector actually used by the diagnostic.

The cadence fix belongs to the application scheduler, while this diagnostic
observes the separate source collector. Changing eligibility from hourly to
six-hourly cannot by itself change one capture's browser work or candle reader.

## Exact live scope

One disposable Render one-off instance attached to the existing collector,
2 CPU / 4 GiB. Two sequential captures at most: Model3/12H, then Model2/12H
as an OHLC positive control. Five minutes maximum per capture; abort at 85%
memory, on first source failure or leaked child, and never retry automatically.
No app worker, queue, scheduler, AI, database connection or application writes.
Children receive only the existing source-session settings and system settings.
No credentials or account contents are emitted. No production settings change.

For Model 3, before timeframe selection, identify the current chart through the
existing guarded Pixi init hook. Measure three seconds of unchanged rendering,
three seconds with the existing 0.5 FPS gate, then three seconds restored.
Observe cgroup CPU, memory, actual render calls/time and skipped paints.
Always disable the gate after the experiment, including an exception path.
Reject comparison validity if the label or geometry changes. Unknown hook or
renderer versions yield an explicitly untested result, never a fallback patch.
The normal capture then continues unchanged, including its existing later gate.

After normal source hover succeeds, hover verified latest-candle pixels and
inspect visible bounded OHLC/liquidity tooltip text. Apply the original candle
reader and timestamp validation to both models. Record exact parser success
and rejection separately from the source text. No price is inferred from pixels;
pixels only locate hover coordinates. A missing tooltip at these positions does
not prove that no other chart control can expose OHLC.

Output contains source success, phase timings, cgroup samples, render lifecycle
status and candle observations. Stage timings can nest and must not be summed.
The added diagnostic windows and hover observations must be excluded from a
production latency estimate. Raw temporary captures are deleted at exit.

## Interpretation

Reduced render calls plus lower CPU in the limited window, with CPU returning
in the restored window, would support painting as a preparation cost. It does
not establish that enabling the gate throughout preparation is safe or reduces
full capture latency. That would require a separate functional fix and A/B tests.
Model2 OHLC success is required before interpreting a Model3 miss. One timeframe
per model is a targeted diagnosis, not full nine-map acceptance or capacity proof.

## Offline validation

`python3 -m unittest discover -s experiments/model3-preparation-20261031 -p test_probe.py -v`

Twelve tests passed, including real child cleanup and parent evidence roundtrip: credential filtering; hardware guard; three measurement
windows; restoration on exception; unknown-hook rejection; changing-map
invalidation; candle pixel ambiguity; runtime mismatch; and capture-wrapper
integration for both models using a source fixture. These are diagnostic-tool
tests, not a live-site result or new A/B acceptance of a functional reader fix.

## Launch after Render CLI authorization

The Render connector exposes logs/metrics but no one-off create operation.
The official CLI is prepared. New CLI access still requires the user's approval.
Use private CLI configuration, verify the existing account/workspace/service and
deployed revision, then create one one-off job with `plan-srv-010`. Deliver only
`probe.py`, `resources.py`, and `runtime-hashes.json` into a temporary directory
of that one-off instance. Do not modify or deploy service source. Start with
`CG3_DIAGNOSTIC_ONE_OFF=1 python3 probe.py --runtime standalone_collector/runtime`.
The invocation path must refer to the temporary diagnostic directory.
Check completed job state, decoded output hash and zero surviving children;
revoke the temporary CLI authorization afterward.

## Harness correction after first live attempt

Job `job-dbj291dg1s2s73ej5q80` stopped after the first child at cleanup: the
extracted resource helper lacked `import time`. No second capture ran and the
parent did not emit measurement evidence. Add the missing import; preserve
first-child evidence and stop if cleanup itself raises. Twelve offline tests now
include a real owned-process cleanup and full parent output with/without a
cleanup exception. One corrected manual diagnostic run follows; no scheduled
retry is installed and no product file is changed.

## Completed live result

Corrected job `job-dbj2bd7b9u1s73cs1f0g` succeeded, 17:30:53–17:32:41 UTC.
Both Model3/12H and Model2/12H source captures passed, zero surviving children,
zero cleanup errors, zero AI calls and database writes.

Model 3 CPU across the original/limited/restored three-second windows was
98.4% / 53.2% / approximately 100% of two CPUs, with render calls 6 / 1 / 6.
Same label and geometry. This supports a painting contribution, not the safety
or latency of a full earlier-gate implementation.

Three Model 3 latest-candle positions returned liquidity Price and Liquidation
Leverage, not OHLC. Model 2 returned a timestamp-matched OHLC candle and the
existing reader accepted Close 77549.4. Absence at these three positions is
not proof that no other source control exposes OHLC.

Timeframe selection was 12.88s vs 2.19s; legend preparation 3.83s vs 0.15s;
row hover 3.25s vs 3.78s (Model3 vs Model2). Total diagnostic durations include
extra measurement work, so are not production scan latency estimates.

Three source attempts total across the failed harness attempt and one corrected
run. No scheduled retry or production change. The temporary CLI token was
revoked and a subsequent whoami was rejected. PR120 remains draft/unmerged.
