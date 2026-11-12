# Temporary account-header CDP experiment

Product baseline: `33bad18f8be322009108584a6b71f3d65fa9b351`.
This directory is not imported by the collector and does not change its code,
cookies, storage state, map readers, access guards, queues or configuration.

## Preparation status (2026-11-12)

- 14 simulated CDP contract tests passed. Python compilation, workflow YAML
  parsing and shell syntax checks passed. These are not browser evidence.
- Previous run #82 installed Playwright 1.49.1 but Chromium failed to launch;
  no actual CDP transport test ran. The filtered error did not establish an OS
  security mechanism as its cause. No local browser retry is authorized.
- This preparation corrects the target to Playwright 1.55.0, explicitly installed
  and verified by `standalone_collector/prepare.py` after the initial build install.
- One additional dummy run is authorized via a one-shot exact-branch push, followed
  by removal of that trigger. No default-branch or deployment change is authorized.
- No CoinGlass request, production write, Render change or PR #132 change.

## Scope and evidence

`account_cdp.py` uses the unchanged account diagnostic's `_session_value` decoder
and a Playwright CDP session on the same Chromium page. It overrides `obe` only
for one original, page-initiated GET at the exact account origin/path, with the
expected main frame and Origin. Existing headers are preserved; `None` is absent.
Redirect descendants are never injected, including a return to the same URL.
Production defaults are fixed. Alternate test endpoints are restricted to the
same account path on explicit `http://127.0.0.1:port` origins.

The adapter never fetches, reads or replaces a response body. Chromium ExtraInfo
is immediately reduced to booleans and technical request IDs. Ambiguous redirect
chains or missing events report `not_verified`. Fetch-before-Network ordering is
handled by a bounded pending pause, aborted during cleanup if unresolved.
No result asserts account acceptance.

`test_chromium.py` runs one real browser with `chromium_sandbox=False` under the explicit dummy-only exception, the existing
advertising route and the existing single-navigation guard. It checks:

1. Allowed request: actual mock-server header agrees with Chromium ExtraInfo.
2. Other requests, POST, OPTIONS and existing headers are unchanged.
3. Same-origin redirect and return to the exact account URL do not carry/reinject `obe`.
4. A redirect to another local origin does not carry `obe`.
5. A response without the mock's CORS permission remains unreadable to the page.
6. Wildcard CORS with credentials remains rejected by the page.
7. Simulated dropped ExtraInfo does not produce false verification, while the
   request itself still uses the real browser and mock server.

The page alone consumes the dummy response and verifies it is unchanged. The
observer never consumes it. The existing Playwright 1.55.0 route implementation
automatically answers browser preflight OPTIONS; this behavior is not altered.
Therefore these tests distinguish real response-CORS enforcement and an explicit
OPTIONS request from native preflight delivery to the mock server. They do not
claim to validate a real site's server-side preflight policy.

Cleanup closes/detaches the adapter, contexts, browser and servers and independently
checks processes, sockets, threads and temporary profiles. A failed cleanup stays
a failure. Launch failures retain bounded, allowlisted technical reason/exit/process evidence,
not raw errors. No headers, response bodies, account data, screenshots or traces
are retained. A generic EPERM is not proof of namespace or AppArmor denial.

## Prepared Actions invocation

Existing workflow: `.github/workflows/standalone-heatmap-check.yml`.
Experiment branch: `test/coinglass-account-cdp-20261112`.
The temporary push trigger matches only this branch and the reviewed previous
head; run attempts beyond the first are excluded. It is removed after the run.

Only `account_cdp_dummy` is eligible on this branch; the original validation/source
job is skipped. The dummy job requires the single authorized push, Ubuntu 24.04, one
runner, read-only repository permissions and a 15-minute total timeout. No cache or
artifact is uploaded. The sanitized result is printed in the run log.

It installs Playwright **1.55.0** and its bundled Chromium, without changing product
dependencies. This targets the build configuration read at the baseline, not a
fresh measurement of the deployed Render binary. Actual browser/package versions
are checked and reported separately by the fixture. The internal Chromium sandbox
is explicitly off for this local dummy exception; CORS and web security stay on.
External isolation is not equivalent to Chromium sandbox protection.

After installation, the browser and local servers run as the runner's unprivileged
user in disposable network/PID namespaces with only loopback. The Actions agent
stays outside; no production secrets are passed. Before browser launch, the fixture verifies non-root execution, loopback-only
networking and no outbound route, a clean environment and dummy-only targets.
A failed gate stops the run; no further fallback is permitted. The PID namespace and outer deadline
bound descendant lifetime, including cancellation. Cleanup also removes temporary
browser installation and fixture files. This revised configuration has **not yet been executed**.

Prior documented calculated compute including #82: $0.138. A 15-minute standard Linux run adds
at most $0.09 at $0.006/minute, for a planned $0.228; this is not an actual invoice.
No additional run has yet consumed minutes. Existing artifact storage charges remain separate.

## Next authorization boundary

After the single authorized successful real dummy run,
the active Render worker's actual auth-source presence/use still needs a filtered
read-only check and the live BTC / Model 1 / 12H attempt needs separate approval.
Dummy success would prove the experiment's transport feasibility, not a CoinGlass fix.

References:
- https://docs.github.com/en/actions/how-tos/manage-workflow-runs/manually-run-a-workflow
- https://playwright.dev/python/docs/browsers
- https://github.com/microsoft/playwright/blob/v1.55.0/packages/playwright-core/src/server/chromium/crNetworkManager.ts
- https://chromedevtools.github.io/devtools-protocol/tot/Fetch/#method-continueRequest
- https://chromedevtools.github.io/devtools-protocol/tot/Network/#event-requestWillBeSentExtraInfo
