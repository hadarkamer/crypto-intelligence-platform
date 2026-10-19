# Model 3 release complete — 2026-10-19

Model 3 passed source/GPT acceptance for 12H, 24H and 48H on the intended paid Render runtime (1 CPU, 2 GiB), and authenticated save/read/display verification in the published application.

## Released once — do not repeat

- Collector PR175 merged to app/model1-standalone-20260915 at 0a6066949f44cffbe39a69c3bfb58de63ffc9145.
- App PR81 merged to main at e5416ec4b694a8a3e9a3a75195d833cb463304b4.
- Migration 20261017103906 applied and journalled once after pre/post function hash and permission verification.
- Render deploy dep-dbatpjgm7kps73a98oe0 LIVE since 09:06 UTC. Auto-deploy remains off.
- Lovable publish beb58cc4-eea7-404d-aed2-e703385aeacf requested once; the published application was subsequently opened authenticated and all three retained result sets rendered correctly.
- Postdeployment /health returned HTTP200, collection mode, displayed-hover.v1, configured source session and ready database.

## Acceptance

- 12H: 133 rows, 3 pools, 286.251 seconds total.
- 24H: 141 rows, 5 pools, 221.464 seconds original source/GPT. Repaired axis-evidence validation used the retained matching DOM labels and response; no new inference or source request.
- 48H: 141 rows, 1 pool, 249.960 seconds total.
- Maximum observed cgroup memory: 1403.51 MiB; no OOM. Model 3 has bounded 240-second hover / 360-second worker / 390-second app running budgets. Existing 18-minute field deadline retained.
- 388 collector preparation tests and 39 app focused tests passed. Final-head GitHub collector/build/RLS/migration checks passed.
- Live real authenticated RPC saved all three outputs, reread preserved numeric evidence, and three audit records were written. Inactive, non-default acceptance template and archived session 09d0cf0c-d22b-4435-b0f9-bafc7d4b186b isolate the evidence from active worksheets and automation.
- Authenticated published UI screenshot is in acceptance/model3-live-app-acceptance-20261019.jpg. Browser auth request timed out in the tool but subsequent fresh navigation confirmed successful login; no credentials exposed.

No Models 1–2 source reruns, duplicate deployments, extra package purchases, continuous scans, OCR/SAM or retries were introduced. No existing user worksheet was overwritten. Quantities retain source units, and percentages refer to the displayed latest column, not a sum across historical time. Future site or cookie changes can still require ordinary maintenance.

The acceptance artifacts in this directory retain the validated source results and release report. There are no remaining Model 3 acceptance tasks from this scope. Do not rerun these cases merely to reconstruct this status.
