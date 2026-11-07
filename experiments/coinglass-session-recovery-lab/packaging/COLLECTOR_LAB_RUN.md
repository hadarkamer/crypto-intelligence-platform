# Collector fixtures: Render execution contract

Prepared scripts, not yet executed against Chromium in this preparation phase. Do not infer a browser pass from syntax checks or the local session-handoff test result.

Preserved paths:

- `/lab/collector/standalone_collector/runtime`: unchanged generated collector at `33bad18f8be322009108584a6b71f3d65fa9b351`.
- `/lab/collector/experiments/model3-preparation-fix-20261031/check_capture.py`: existing synthetic HTML fixture; only its pinned HTML assignment is read by AST.
- `/opt/playwright/chromium-1187/chrome-linux/chrome`: prepared Chromium140 executable, Playwright1.55 dependencies unchanged.
- `/lab/packaging`: temporary lab scripts.
- `/lab/evidence/collector`: synthetic HTML, PNG, native-hover JSON and matrix report. No real session data belongs here.

Run the new handoff tests, then the full browser matrix once:

```sh
python -I -B /lab/packaging/test_session_handoff.py
python -I -B /lab/packaging/collector_matrix.py
```

These commands do not rerun the prior collector readiness/unit suite. The app/PostgreSQL serial checks use the separately reviewed `native_serial_lab.py` command; their results must remain separately identified.

The matrix invokes **14 serial supervised workers**:

| Worker group | Expected result |
|---|---|
| Models1,2,3 × 12H,24H,48H | Nine actual `capture_heatmaps` calls, nine PNGs, nine complete native-hover JSON artifacts. Each fixture must have40 price rows and its requested model/timeframe identity; no AI module/client is loaded. |
| One gates worker per model | Three horizons per model, eight checks per horizon: ready, no-data, request-limit, loading, visible login, wrong timeframe, login appearing after a successful control, login appearing after a failed control. **72 DOM/control checks total.** |
| Setup-failure injection | Real browser/context creation, injected failure, independently verified descendant cleanup. |
| Timeout injection | Real browser/context creation, TERM-ignoring worker, supervisor timeout and independently verified descendant cleanup. |

The worker uses the existing synthetic tooltip/canvas HTML with the unchanged native screenshot/hover functions. Route fulfillment is restricted to the exact fixed source-document URL; everything else is aborted. Contexts are offline, service workers are blocked, Chromium background networking is disabled and hostname resolution is denied. Python networking is blocked. The URL represents a fulfilled local fixture, not a contact with CoinGlass. No real credentials, AI replies, database operations or live numerical data are used by this matrix.

Each worker has a45-second work budget and4-second cleanup budget within a nominal50-second case budget. The supervisor verifies its Linux PID namespace, subreaper and pidfd capability before starting a browser. It records observed browser processes, terminates/reaps owned descendants including detached groups, and independently checks both process census and kernel child lists. An unverified cleanup stops the matrix before another worker begins. These are userspace budgets and observed cleanup outcomes, not a guarantee under kernel stalls or external SIGKILL.

Output:

- Handoff tests print one `suite=session_handoff` JSON summary with test/failure/error counts, dummy-only status and zero source/browser/AI calls. TTY payload values/hashes are not emitted.
- Matrix stdout is one bounded `format=collector-browser-matrix.v1` JSON report. The report includes expected/completed case counts, capture count, validated native-hover/PNG counts, cleanup result and individual supervised case records. Raw child stdout/stderr is never forwarded.
- The complete nonsecret matrix report is also `/lab/evidence/collector/matrix.json`. Successful capture artifacts are in case directories such as `/lab/evidence/collector/m1-12h-capture/` (`fixture.html`, `coinglass_btc_heatmap_12h.png`, `hover.json`).
- A passing top-level status requires every expected case to pass and its cleanup to be verified. Nine PNGs alone are not a nine-case pass; native-hover identity and values must also validate.

Only for a deliberate diagnostic rerun, the same supervisor can run one case:

```sh
python -I -B /lab/packaging/collector_matrix.py --single --model 1 --timeframe 12H --case capture
```

Allowed cases are `capture`, `gates`, `failure`, `timeout`. Capture directories are never overwritten; preserve/archive earlier synthetic evidence before an explicitly chosen rerun. Do not invoke a single capture as a preflight and then blindly run the matrix into the same directory.

The handoff's synthetic-cookie conversion and this browser matrix are complementary checks. Neither establishes account authentication, entitlement, real Cookie completeness, source reliability or production concurrency/performance. A future genuine BTC/Model1/12H source trial remains separately authorized and must retain the existing access/readiness gates and native reader.
