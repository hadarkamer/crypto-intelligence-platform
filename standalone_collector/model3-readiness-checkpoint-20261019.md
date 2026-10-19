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
