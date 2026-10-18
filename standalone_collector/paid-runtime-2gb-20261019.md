# Intended-runtime acceptance — 19 October 2026, Israel

The user-upgraded `decision-hub-model1-collector` now runs on `1c-2g`.
The actual runtime cgroup confirmed one CPU and 2147483648 bytes of memory.
Production still runs `cf2413f6a34ff9cac33e27e957990eeab2c3c0f7`.
No production code, schema, application data, configuration or deploy was changed.

The reviewed source reader `b62bce2ed6aa32282efcd9084cee1c7685897ab1`
was exercised from its verified temporary bundle, using the existing authenticated
source session without moving or printing credentials. Guard code is at test
commit `52b129a36ba2fdadea43f1369bcc01301dfc4e45`.

## Actual source results

| Case | Outcome | Rows | Candidate pools | Hover seconds | Full guarded case seconds | Peak total cgroup MiB |
|---|---|---:|---:|---:|---:|---:|
| Model 2 / 12H | captured | 79 | 3 | 15.265 | 42.433 | 1108.13 |
| Model 3 / 48H | failed before hover | — | — | — | 49.600 | 1027.94 |
| Model 1 / 12H | captured | 129 | 7 | 13.156 | 38.688 | 1135.43 |

Exactly three source cases ran, each once. The first guarded invocation (PID 95)
stopped after Model 3 failed. After verifying process cleanup and service health,
only the unstarted Model 1 case was executed (PID 291), using the same guard with
`CASES = ((1, '12H'),)` and a separate output directory. No Model 2/3 retry occurred.
A shell-quoting assertion in the Model 1 preparation command failed before any
directory creation or source execution; correcting that command did not rerun a case.

The guard used the existing single-worker advisory lock, yielded to production
queue work, capped each case at 180 seconds, and retained a 1700 MiB headroom limit.
Source children received no OpenAI or database capability. No memory-limit,
OOM, or OOM-kill event occurred in these cases. Peaks include the existing service,
diagnostic parent, browser and file cache; they are not browser heap measurements.

## Model 3 release blocker

Observed error: `source_timeout`, `TimeoutError`, `capture_phase=overlay_dismissal`,
`click_failure=action_timeout`. The phase occurs directly after legend preparation
and maps to the existing `close_heatmap_settings` call. The settings drawer close
did not complete within the existing bounded action/state checks. This failed
phase took 5.60 seconds; legend preparation took 11.80 seconds. The retained
screenshot and numeric hover sweep were not reached.

The existing failure observer could not obtain a detailed DOM state. No pointer
interception, disabled button, unstable element or source access gate was proved.
The precise reason the close action/state wait failed therefore remains OPEN.
CPU usage was 48.662 CPU seconds in 49.600 elapsed seconds, with throttling in
473 of 496 scheduling periods. This establishes CPU saturation during this case,
but does not independently prove that saturation caused the close failure.
Two HTTP 429s were classified only as other-host responses, with no causal link
established. This is neither an OpenAI failure nor evidence of another memory
upgrade being needed.

Per the user's instruction, record this failed case and continue the remaining
independent check rather than repeat it or relax validations. Next focused work:
distinguish the close click from the hidden-state wait with safe operation-level
evidence, then correct only the proven cause and validate that repaired Model 3
case in this same runtime. Do not increase timeouts or deploy on inference alone.

## Numeric and service checks

Independent Decimal arithmetic passed for both successful captures: complete
row totals, each candidate's member sum and displayed-column percentage, unique
row IDs, and non-overlapping candidate membership. These remain candidate pools
pending the normal visual GPT check. Units are `source_metric`, not verified USD;
the denominator is `displayed_latest_column`, not historical-period liquidity.

After the final case: no guard or Chromium process remained, the advisory lock
was available, queued/running bridge jobs numbered zero, `/health` reported OK,
and store readiness passed. No temporary browser profile remained. The verified
temporary test directory was removed after measurements were preserved.

Zero OpenAI requests, zero production data writes, zero deploys or plan changes.
Collector PR175 and app PR81 remain unchanged drafts. Live new-hover save/display
acceptance still requires the coordinated release and existing PR81 migration;
do not add or rerun a duplicate migration. Historical completed tests remain valid
within their recorded scope and must not be rerun merely to resume this work.

Machine-readable measurements: `paid-runtime-2gb-20261019.json`.
