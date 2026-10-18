# Model 3 focused runtime diagnosis — 2026-10-19 Israel

## Result and current boundary

Two UI activation blockers are corrected and demonstrated on the actual paid 1 CPU / 2 GiB service. Full Model 3 capture is still NOT ready: its numeric reader reaches the existing 120-second hover deadline while the source browser is progressing slowly. Keep PR175 DRAFT and unmerged. No production deployment or application change was made.

Collector correction: `0b6ab49a71d1238ff91b12d640bc7accaa742b02`, parent `b62bce2ed6aa32282efcd9084cee1c7685897ab1`. Only `heatmap_hover.py` and `test_heatmap_hover.py` changed on the production candidate branch. App PR81 remains unchanged. Production remains `cf2413f6a34ff9cac33e27e957990eeab2c3c0f7`, Render deploy `dep-dbak2em7bikc73eukjv0`.

## What failed, and what the evidence proves

1. The Model 3 settings Close button existed, was enabled and visible, lay inside the viewport and was the actual hit target. It was not covered by cookies. The mouse action timed out before starting a click. A passive frame sample observed just one animation frame in 782.1 ms. Frame-dependent actionability waiting is implicated; this alone does not establish that CPU saturation was the sole cause.
2. Native keyboard activation of the same scoped Close control closed the settings panel. The next blocker was the existing cookie Decline mouse action, which also timed out before starting a click and left the banner visible.
3. Using native Enter activation for both observed buttons allowed settings, cookies, geometry, screenshot and render-readiness gates to pass. Visible/enabled preconditions and actual hidden-state checks remain. No force click, DOM removal, retry, longer production timeout or new dependency was introduced.
4. Once those blockers were removed, the remaining numeric reader was measured. In the final case it failed with `source_capture_failed / hover_collection / hover_deadline`, not a provider or OpenAI error. The reader retained its existing 120-second limit. The full case lasted 192.970 seconds. The test-only outer guard was 210 seconds so that the existing reader could report its own deadline after approximately 70.85 seconds of setup; it did not terminate this final case.

## Four sequential isolated cases in this session

Only Model 3 / 48H was started. Models 1–2 were not rerun. Every next case addressed a new observed blocker or added missing measurement; no automatic retry or concurrent source case was used.

| Case | Source behavior | Outcome | Full seconds | Peak total cgroup MiB |
|---|---|---|---:|---:|
| Close inspection | Original b62 reader, passive control inspection | Settings Close mouse timeout; panel remained visible | 55.293 | 1034.73 |
| Settings correction | Enter on settings Close only | Settings closed; cookie mouse activation failed | 60.925 | 986.69 |
| Both controls corrected | Enter on both controls | Reached hover; outer 180-second test guard stopped it before reader deadline | 180.163 | 1019.72 |
| Reader profile | Same corrected source; periodic test-only timing, outer 210-second guard | Reader's own `hover_deadline`; no outer guard stop | 192.970 | 1042.43 |

The final two source bundles were identical in production behavior. The diagnostic observer added timing/count output only. Final profile bundle SHA256: `e2062e749a9b2a3f6f3f3c4c8f227705c24922deb826212ab6f289a93f267430`. Earlier controls bundle: `87bafb088a4144fa4cdf9b86e1f79d9a9391cab1a3be9cc89f5f9e8a3d4b83b1`.

## Where the hover time went

| Operation | Calls | Total seconds | Maximum single call seconds |
|---|---:|---:|---:|
| Native mouse movement | 75 | 57.1286 | 0.9110 |
| Wait until old tooltip disappears during calibration | 33 | 27.6237 | 1.6784 |
| Read visible tooltip during calibration | 33 | 26.2969 | 0.8985 |
| Read expected-price tooltip during row sweep | 8 | 6.2390 | 0.8134 |
| Other DOM evaluations | 3 | 1.3155 | 0.8074 |

At reader elapsed 115.405 seconds, 38 tooltip strings had been returned. These counts include repeated calibration samples and are NOT counts of complete, accepted grid rows. No complete column, pool result, amount or percentage was accepted.

The existing calibration walks pixels until several native row boundaries are established and clears/re-enters for each such sample. On this Model 3 source, each browser operation costs roughly 0.8 seconds. This repeated interaction cost is the measured bottleneck. The observations do not prove which internal CoinGlass rendering routine is responsible.

Total cgroup CPU usage was 191.624344 CPU seconds over 192.970 wall seconds (~99.3% of one CPU). 1912 of 1930 scheduling periods were throttled. These measurements include the existing server, guard and browser. They substantiate CPU pressure; they do not establish that a larger plan alone is sufficient or required.

Peak memory: anonymous 471.09 MiB, file cache 566.16 MiB, total cgroup 1042.43 MiB. No memory-limit, OOM or OOM-kill event occurred. The recorded 512 MiB historical failure is distinct from this 2 GiB reader deadline.

For comparison, previously completed actual-runtime cases (NOT repeated here) were Model 1 / 12H: 129 rows, 13.156 seconds hover, 38.688 seconds full; Model 2 / 12H: 79 rows, 15.265 seconds hover, 42.433 seconds full. They use different source models/timeframes; this is not a controlled model-only speed benchmark.

## Verification, cleanup and resume instructions

- Local final candidate: all 20 focused hover regressions passed in 40.242 seconds. Real-browser fixtures verify trusted native activation, preservation of settings/other dialogs, rejection of disabled controls and sticky/late banners.
- Exact-head GitHub validation passed: run `39177623384`, check `117823449302`. No source scan or AI call was part of that CI run.
- Source children had no AI/database credentials; diagnostic parent used read-only database access and the existing global worker lock. Queued production work would stop the test. ZERO OpenAI requests, application/queue writes, deployments or plan changes.
- Cleanup confirmed: all four own temporary test roots removed after preserving safe evidence, zero own test/Chromium processes, worker lock available, zero queued/running bridge jobs, service health and store readiness true. Cgroup memory after cleanup: 368.85 MiB.
- Browser terminal observation briefly timed out after the source test was running; recovery read the same result. No source case was restarted because of that UI transport timeout.
- No session/cookie values, raw private logs, images or browser profiles are included in this report or repository evidence.

Next work should focus on reducing verified calibration/pointer round-trips, preserving full-grid, source-time, expected-price and final-anchor checks. Do not compensate by skipping rows, accepting partial scans or merely raising deadlines. Measure a concrete change before any additional paid plan decision. Full Model 3 source acceptance, its unobscured GPT verification and coordinated application persistence/display acceptance remain open. Do not redeploy or rerun Models 1–2 simply to reproduce this checkpoint.

Values retain `source_metric` units and `displayed_latest_column` scope. Nothing here upgrades source values to verified USD or makes displayed-column percentages historical-period totals.
