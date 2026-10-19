# Model 3 readiness checkpoint — 19 October 2026

Status: NOT ready for production. No production merge, deployment, or database write during this investigation. Do not rerun Models 1–2. Production branches are unchanged. Historical measurements are retained in earlier revisions of this checkpoint.

## Exact reviewed drafts

- Collector PR175: `95b7386b434c271e1b0783ec3f1d1322a383e023`, target `app/model1-standalone-20260915` at `cf2413f6a34ff9cac33e27e957990eeab2c3c0f7`.
- App PR81: `bd82e38d5c33781dda8e744ae5649e3b7479dabf`, main `7d21e93ebd8c649bf7a741332fd1999b9cbb647d`.
- Both remain draft, unmerged, mergeable; rechecked in this continuation. All exact-head CI passed. Collector clean preparation: 385 tests passed. App focused validation/quantity/display: 24 tests passed.
- No repeated migration. Live save functions still need the reviewed hover-evidence migration after all gates pass.

## Proven causes and accepted repairs

1. Model 3 Close/Decline can take more than three seconds before native activation. Its actions allow six seconds and verify actual closure; no forced DOM deletion.
2. Full-page screenshots changed the source price axis immediately (77000–87000 to 89000–99000). Cookie dismissal did not change it. Model 3 now uses a 1600x1300 viewport and viewport screenshot, with axis equality checks before/after image and hover. Models 1–2 keep their original capture.
3. Source WebGL uses ANGLE/SwiftShader software Vulkan on the paid 1-CPU/2-GiB service. Ten-row profiling measured about 960ms per tooltip update but only 8.38ms reading visible text. Rendering/update waiting dominates; the cookie and OpenAI key do not explain this reproduced timing failure.
4. The current draft calibrates two native price transitions in two-pixel steps, refines every changed interval, reads all expected-price rows in one bounded page evaluation, and validates three native pointer anchors with disappearance/re-entry and exact price/value/time/unit comparison. Complete grid, source axis, snapshot and evidence checks remain mandatory.

## Finished current-source acceptance

Root `/tmp/hover-model3-adaptive-reader-20261019-73aeb29`, guard PID4154: FINISHED. Bundle SHA256 `390a313662c98c284485712b6254a3e6a081efa3aab3c2260aad78db7d5d2a05`.

- **48H validated**: 141 rows, one accepted pool, hover169.790s, total249.960s, exactly one successful GPT call, peak cgroup1203.59MiB. Exact saved-image hash verified; thumbnail visually checked: correct full map, unobstructed axis, correct model. Result exported with SHA256 `51a6a1707d6c8b2ec8d3d1a36e830eff4e6a95761be8887f96de54b8028761cd`. Current app validator and table adapter also accepted it (one level, source value5080000). **Do not repeat this successful source case.**
- **12H failed**: hover_deadline, total270.267s, zero AI calls, peak1419.89MiB.
- **24H not started**, due to stop-on-failure.

The 48H 'saving' stage saved isolated files only; it did not write application data. Values are source_metric unless currency is explicitly verified; percentages cover the displayed latest column, not historical totals.

## Rejected configurations

Full Chromium/channel and reduced/narrow viewport variants did not meet acceptance and are not integrated. Device-pixel-ratio reduction left the backing buffer unchanged; discarded. Two bounded offline rendering comparisons finished without source/API calls. The corrected synchronized benchmark found default frame times355/219/288/211ms versus ThreadCount=1 times276/292/290/214ms, same pixels; no useful improvement and no proof the configuration was honored. Do not add SwiftShader.ini, browser flags, DPR overrides, OCR, SAM, or dependencies from those experiments.

## ONLY active check — inspect before doing anything else

Root `/tmp/hover-model3-bounded240-20261019-95b7386`, guard PID4669.
Bundle SHA256 `ebdaf58c254478c2136ef5e6cb73d823a0033e68e12de4125c062c098e443508`.

This diagnostic measures completion of the remaining Model3 cases, 12H then24H, with a bounded240s hover and360s whole-case allowance. It changes only the isolated reader budget relative to95b; production draft remains180s/300s. It uses the existing worker lock, yields to queued production work, guards memory, runs one browser, stops on the first failure and makes at most one existing GPT call per fully collected case (two maximum). No application writes, no deployment. Last observed:12H hover_collection. Do not relaunch this path or launch parallel source checks.

Total OpenAI calls before this active guard: TWO (earlier rejected unfinished image, successful48H). Count any completed new cases from this guard's summary before reporting a new total.

## Remaining gates

- Read the current guard result and base any timeout alignment on actual measurements; no open-ended retries.
- Complete12H/24H source and GPT validation. Preserve successful48H instead of recollecting.
- Replay all three real outputs through current app validation, authenticated transactional save/read/conflict preservation and real component rendering. Prepared scripts are under review-data; reuse existing disposable PGlite database, always rollback.
- Verify exact heads and compatibility, apply the existing migration once, merge by merge-commit and perform one coordinated deployment per service, then verify live authenticated save/read/display without overwriting user work.
- Never declare full readiness based on source-only success or CI alone.
