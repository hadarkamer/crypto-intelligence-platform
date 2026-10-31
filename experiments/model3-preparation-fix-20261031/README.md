# Model 3: enable existing paint limit during preparation

Prepared 2026-10-31. **Draft; not deployed or live-source accepted.**

## Product correction

Base collector: `f6adfc6fea05bfd82a59098433ad1c2f31f6267f`, deployed on
`app/model1-standalone-20260915` / `srv-dakhacou01pc73f58aug`.
The existing gate previously started after timeframe selection, legend handling
and readiness. Those operations were the measured preparation bottleneck.

Wrap those existing operations in a preparation scope using the SAME guarded
Pixi hook and 0.5 FPS gate. Activate only for Model 3 when the original source,
selected canvas and ready geometry can be verified. Otherwise retain the
original path. Convert the observed page rectangle to viewport coordinates.
Restore before the existing observation scope, which binds final geometry and
retains its clean-frame, PNG, readiness and numerical-reader checks.

Product changes are limited to `model3_rendering.py` and its existing installer.
Only two generated runtime files differ: that module and the capture module.
The render-gate JavaScript, clean-frame module, numerical reader, timeouts,
prices, AI configuration, queue, quota, cadence and Models 1/2 are unchanged.
No obsolete product function was found for removal: the observation scope is
still needed for final geometry and clean capture; its lifecycle is reused.
The old assumption that only the late scope is needed is what was corrected.

If the source replaces its renderer while this scope is active, restoration
fails closed. Synthetic tests verify rejection; whether the live source does
this during timeframe selection is one of the outstanding acceptance questions.

## A — isolated correction

27 focused tests passed in `test_model3_rendering`, including real Chromium with
synthetic HTML: all three horizons, actual timeframe and legend controls,
readiness, original PNG pixels, direct rendering/data callbacks, limited
automatic paint, scroll/resize, unknown renderer, no-data, mid-scope failure,
restoration and the next cycle. The installer AST check proves the complete
original capture is recovered when scheduling wrappers are removed. Existing
hash checks preserve the numerical reader, gate JavaScript and clean frame.

The full unchanged build procedure also passed its four suites: 106, 14, 122 and
233 test executions. Suites overlap; these are not 475 distinct scenarios.
Runtime dependencies matched Playwright 1.55.0, requests 2.32.5 and Pillow 12.3.0.

## B — isolated integration, with explicit limits

`check_capture.py` ran the actual generated capture, native controls, screenshot,
full row reader, candidate computation, Responses client, grounding, validation
and result-file writing for all nine model/timeframe pairs. Only the source HTML
and Responses HTTP reply were fixtures. Each result had 40 observed rows, one
source PNG, one synthetic analysis request and the correct model/timeframe.
Browsers were closed before analysis. Only Model 3 enabled the early gate.

`check_application.py` replayed those actual task results through the existing
PR120 lab at application SHA `e3fd4d116eaecaf43e870a8b5ae3aa676a334e71`, based on
`683145321fa72ec9db7e46fb5a677d1b41ee35ea`. It ran the real app relay, bridge,
orchestration, collector client/validator, SQL in disposable PGlite and React
button/renderer in JSDOM, with the pending six-hour correction applied only in
the test database. Two scheduled cycles, nine pairs, night exclusion, a controlled
Model3/24H failure, preservation of successful horizons, retry of only that
horizon, refresh/reload and released leases passed. Five shared SQL regression
suites also passed. There were 19 logical jobs and 58 status polls, not 58 scans.

The collector queue HTTP transport in this app lab remains simulated; it replays
real offline task outputs and explicitly rebases fixture IDs/timestamps. It does
not start a real Render worker or connect its PostgreSQL queue. This layered
integration is **not a complete live end-to-end acceptance**, and source/AI
simulation does not prove that the live preparation is safe or faster. No paid
source/AI operation or production application write was performed in these tests.

Offline result summary: `offline-results.json`. No production performance
improvement is claimed. There was no source-fixture test failure requiring a
product adjustment; browser download initially used its working alternate mirror.

## Prepared live check — not launched

Use one disposable Render one-off, **2 CPU / 4 GiB**, attached to the known
collector revision. Do not change or deploy the production service. Reuse only
the existing source session inside that one-off; children receive no AI or DB
credentials. Temporary CLI authorization from the previous diagnosis was revoked.
The current connector supports reads but not creating a one-off job.

Five sequential source captures maximum, no retries:

1. Base Model3/12H.
2. Candidate Model3/12H.
3. Candidate Model3/24H.
4. Candidate Model3/48H.
5. Candidate Model2/12H as unchanged-path control.

Stop on the first source failure, cleanup error, surviving child, 85% memory or
300 seconds per capture (at most 25 minutes of capture time plus bounded cleanup
and setup). This is runtime compute usage on Render; it is not a paid AI call.
Record step durations, CPU-seconds, memory, gate events, exact source rows,
original PNGs including failures and zero surviving children. Preserve failures;
do not repeat a scan automatically. Revoke temporary CLI access after completion.

Use `source_acceptance.py` with the previous diagnostic `probe.py` and
`resources.py` from collector diagnostic commit
`fcd28a33653dd7821b561c72aafcce51e32836b3`. Verified helper SHA256 values:

- probe.py: `6d3db9641ca82acfcc696fe4679d387c32b8c64b390050b484d862c6501a7963`
- resources.py: `1f78de1cfed78ddba02ab65a310d418c9221e8e599a3d6305ef993ebce5a15b4`

Make private base/candidate copies of the deployed runtime, excluding bytecode.
Replace only the two reviewed candidate runtime files in the candidate copy.
Verify both against `runtime-hashes.json` before any source access. The existing
`standalone_collector/hover_source_check.py` supplies its single-capture function.
Neither runtime is a deployment or modifies the service's original files.

```bash
CG3_EARLY_SOURCE_ONE_OFF=1 python source_acceptance.py \
  --diagnostic /private/diagnostic/probe.py \
  --source-check /original/standalone_collector/hover_source_check.py \
  --base-runtime /private/base/runtime \
  --candidate-runtime /private/candidate/runtime \
  --manifest /private/runtime-hashes.json
```

Three offline harness checks passed: exactly five sequential children, immediate
stop with retained evidence on source failure, and retention/stop on cleanup
failure. They do not count as successful live captures.

## Reproduce offline checks

Set an isolated dependency/browser path if needed. No source credentials are
required. Do not run the live acceptance entry point as part of the build.

```bash
python standalone_collector/prepare.py
(cd standalone_collector && python -m unittest test_model3_rendering -v)
python -m unittest discover -s experiments/model3-preparation-fix-20261031 -p test_source_acceptance.py -v
```

Run `check_capture.py --model N --timeframe TF --output /private/results/N-TF`
for all nine pairs in fresh subprocesses, then:

```bash
PGLITE_ROOT=/test-deps/node_modules/@electric-sql/pglite \
python experiments/model3-preparation-fix-20261031/check_application.py \
  --app /isolated/trade-plan-pad-at-e3fd4d1 --evidence /private/results
```

The app lab uses PGlite 0.5.13 and the application's installed dependencies.
It verifies the original lab's SHA256, writes a temporary local variant and
deletes it at exit. Production SQL/settings are never accessed.

## Release decision

Do not merge/deploy from offline tests alone. First complete the source check,
review its correctness and timing, and address any remaining integration gap.
No guarantee of improved duration, capacity or 72-map concurrency is made.
PR120 cadence remains a separate unmerged configuration operation; this change
does not solve missing GitHub schedule events or change Model 3's price interval.
Confirm current heads, active jobs and coordinated ownership before a later
approved release. Rollback would restore the original two product files and
rebuild on the verified base; no database migration or data rollback is involved.
