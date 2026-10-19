# Model 3 checkpoint — 2026-10-19 09:04 UTC

Source acceptance on paid Render 1 CPU / 2 GiB is complete for 12H, 24H, 48H. No repeat source scans or GPT calls are needed for these cases.

- 12H: 133 rows, 3 pools, 286.251 sec, peak cgroup 1403.51 MiB.
- 24H: 141 rows, 5 pools, original source/GPT 221.464 sec. Retained-data-only replay validated exact DOM axis bounds; no additional API call.
- 48H: 141 rows, 1 pool, 249.960 sec, peak cgroup 1203.59 MiB.
- No OOM. Guard PIDs 4669 and 4891 finished. Fresh shell persisted session configuration matches validated session (secrets not exposed).
- Exact final collector preparation: 388 tests passed. Final head CI success.
- App focused tests: 39 passed, production build / RLS / migration dry-run passed.
- Actual retained results passed authenticated isolated save/read/worksheet adapter and UI quantity tests; conflict preserves evidence.

## Applied once — do not repeat

- Migration 20261017103906_coinglass_hover_evidence.sql applied and journalled. Function md5s: 1cc4c036baab63db307b4c596b290f84 and ca99c94065e2715d8a9fd208b103e928. Existing SECURITY INVOKER/grants preserved.
- Collector PR175 merged to app/model1-standalone-20260915 at 0a6066949f44cffbe39a69c3bfb58de63ffc9145.
- App PR81 merged to main at e5416ec4b694a8a3e9a3a75195d833cb463304b4.
- Render ONE deployment started: dep-dbatpjgm7kps73a98oe0 (autoDeploy OFF), correct merge SHA.
- Lovable ONE publish started: beb58cc4-eea7-404d-aed2-e703385aeacf, URL https://trade-plan-pad.lovable.app.

## Remaining

Monitor these exact deployments. Verify live health and actual application save/display with a clearly marked isolated archived test record, without disturbing shared/manual worksheets or starting Models 1–2. Do not claim full readiness before that verification.
