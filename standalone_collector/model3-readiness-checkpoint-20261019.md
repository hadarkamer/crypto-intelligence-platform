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
