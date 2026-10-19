# Model 3 readiness checkpoint — 19 October 2026

Status: NOT ready for production. PR175 and app PR81 remain draft/unmerged. No production deployment or database write in this investigation. Do not rerun Models 1–2.

## Measured findings on the intended paid service

Actual resources: 1 CPU, 2 GiB. Browser/runtime versions: Playwright 1.55.0, requests 2.32.5, Pillow 12.3.0. All nine preceding guarded cases ended, with zero OOM events. Cases use the existing worker lock and yield to queued production work.

- Cookie Decline and Model 3 settings Close can time out at 3 seconds before native keyboard activation. Candidate allows 6 seconds for those Model 3 actions only; no retry or forced DOM removal.
- Synthetic pointer/notification approaches were rejected. They did not reliably preserve native tooltip/anchor checks.
- Narrowing viewport to 1000×1000 completed a 36-row sweep in 108.594 seconds, but changed price coverage. Rejected; original 1600×1000 retained.
- Original viewport completed 127 rows in 119.999 seconds; entire source/GPT check took 189.629 seconds, peak current memory 1289.81 MiB. One OpenAI request returned HTTP 200. GPT correctly rejected the purple/unfinished image (readable=false, blocking_condition=blur). This is not a successful end-to-end scan.
- That test bundle mistakenly carried an unexpanded error-code module, masking the validation failure as worker_crashed. New bundles use the generated runtime with source_not_readable defined.
- A subsequent same-page three-point rendering probe found a proper chart both before and after hover, unchanged geometry, and valid native tooltip text. Its intentional render_probe_complete stop is diagnostic completion, not an application source failure.

## Focused candidate repair

Model 3 exposes numeric price-axis ticks as visible DOM text. Require a valid visible axis and stable rendered frames before capture; compare it with the hover coverage and reject changes. Keep native inputs, original viewport and source. Retain raw GPT failure evidence before grounding validation. Allow 180 seconds for the measured 120-second native sweep, within the existing 300-second worker budget; no new retries. Application duration validation must match.

Local prepare checks before the final budget adjustment: 106 + 14 + 121 + 143 tests passed, zero live source/API calls. The adjusted deadline test and complete paid-runtime acceptance remain required.

One new guarded Model 3/48H acceptance is running under /tmp/hover-model3-readiness-acceptance-20261019-0b6ab49. Bundle SHA256: 2642c46f50208caa73fae0e5011530a26a11632b3553d3f079a6c6993c58f902. Maximum one API request, zero database writes. Do not relaunch this path.

Remaining gates: paid-runtime 12H/24H/48H source + GPT validation, current app validators, coordinated migration/release once, actual save/read/display verification. Source amounts remain source_metric unless an explicit currency unit is present; percentages cover displayed latest column, not historical totals.

## Root cause isolated after the initial checkpoint

A passive before/after probe on the paid service proved that the full-page screenshot changes Model 3's displayed price range. Numeric axis ticks stayed 77000–87000 after cookie dismissal, then became 89000–99000 immediately after screenshot. The unchanged before/after cookie readings rule out cookie dismissal in this reproduced event. Evidence: model3-axis-transition-20261019.json. The price-axis equality gate prevented accepting mismatched image/hover data.

Candidate fix under verification: keep viewport width 1600, use height 1300 for Model 3 so its observed 800px chart/header fit, and take a viewport screenshot. Models 1–2 retain their existing viewport and full-page capture. Check numeric axis equality around the retained frame and throughout hover. This changes the capture mechanism, not the source numbers or model confidence requirements.

One earlier acceptance stopped in browser_launch with TargetClosedError, before source/API. Two separate launch-only checks, including the restricted child environment, passed afterward. Its exact process-exit cause was not retained; do not claim it is explained by the chart bug. A fixed-field launch observer was added to subsequent test bundles only.

No production deployment or database write. API calls so far remain one, the rejected unfinished image.

## Current continuation point

- Collector PR175 candidate head: 73aeb29988006632f7237b4e3e8c57dd9713216b. App PR81 head: bd82e38d5c33781dda8e744ae5649e3b7479dabf. Both remain draft/unmerged.
- Final local clean prepare: 106 + 14 + 121 + 143 tests passed. App focused hover/quantity/display checks: 24 passed. Collector GitHub workflow 39216317020 passed; app migration and RLS workflows passed; app build was still running at checkpoint.
- One active serial guarded acceptance: /tmp/hover-model3-viewport-acceptance-20261019-0b6ab49, guard PID 3188, bundle SHA256 af7efad788bf06fdbee74e17f77d572cdc99b43de11a69676baddf35dbbe8e93. Cases: Model 3 48H -> 12H -> 24H. Stops on any failure; at most one API request per case; no database writes. Do not launch another. Last observed stage: 48H hover_collection, after the before/after screenshot axis gate passed.
- Existing local disposable PGlite database review-data/hover-current-isolated-pg reopened successfully as legacy_transaction_test with both save functions; do not rebuild it unnecessarily. Its acceptance writes must be rolled back.
- No production merge, deployment or database change.


## Continuation: actual frame-update cost isolated

No production merge/deploy/database write. No Model 1/2 source rerun. The one prior rejected-image OpenAI request remains the only API request; subsequent source cases stopped before analysis.

All preceding candidates finished, sequentially:
- Native reader, correct full-size viewport screenshot: 48H hover_deadline, 254.361 seconds overall; peak current 1020.09 MiB.
- Text-only predicate per browser call: hover_deadline, 254.401 seconds; no useful timing improvement. Rejected.
- Single pointer event + read per browser call: hover_deadline, 259.000 seconds. Rejected.
- 1000x1300 viewport, native reader: hover_update_timeout during calibration, 115.503 seconds. Rejected; never accepted reduced coverage.
- One page-side row batch, full visibility reads/native calibration/anchors: hover_deadline, 258.428 seconds. Rejected as production-ready.

A bounded ten-row timing probe completed intentionally without AI or application writes. It found 141 native rows (price step 90.81; about 5.71px per row), 62.802 seconds calibration. Mean pointer-dispatch time 0.34ms; actual text-read work 8.38ms; full wait per changed tooltip 959.79ms, with two predicate observations. All three ordinary native-pointer anchors matched the sampled price, value, timestamp and unit. This isolates delayed page updates, not expensive string extraction, as the dominant cost. Evidence: model3-row-profile-20261019.json. Intentional probe termination is not an application acceptance result.

The page-side batch prototype passed 22 local reader regressions plus explicit Model 3 stale/changed-time and missing-row fixtures, but remains TEST ONLY because paid-runtime timing failed. No event/batch/narrow-viewport code has been added to the production PR.

Current active acceptance root: /tmp/hover-model3-full-chromium-20261019-73aeb29.
Bundle SHA256: ae9616e0bd0c36c7c9878391d8496cea5c9db1a975d7b7cf79d39605e44bfa7e.
This isolated candidate changes only Model 3's browser launch to supported channel=chromium (new headless/full Chromium); it restores the original native reader, full 1600x1300 viewport and all source checks. Sequential Model 3 48H -> 12H -> 24H, stop on first failure, max one GPT call per successful source case, zero application writes, same 1 CPU/2 GiB, same guard and deadlines. Inspect this root before any next launch. Do not duplicate it.

Collector/app draft heads remain 73aeb29988006632f7237b4e3e8c57dd9713216b and bd82e38d5c33781dda8e744ae5649e3b7479dabf. All exact-head CI workflows are successful. Bases remain cf2413f6a34ff9cac33e27e957990eeab2c3c0f7 and 7d21e93ebd8c649bf7a741332fd1999b9cbb647d. New source/GPT success, app replay and live coordinated release are still open gates.
