# CoinGlass account authentication candidate — isolated only

Base: `33bad18f8be322009108584a6b71f3d65fa9b351`.

The observed browser account request omitted `obe`, while the separate account
diagnostic accepted the configured credential when sent in that header. This
proves a transport difference; restoring live map access remains unverified.

## Change

Three existing product files change:

- `source_account_diagnostic.py` reuses its unchanged credential decoder to
  supply only a missing header on the page's existing account GET. Exact HTTPS
  host/path, XHR/fetch, trusted frame and Origin are required. Map requests and
  existing frontend headers are untouched.
- `model1_execution.py` calls the adapter in its existing context setup, after
  Cookie loading and before page creation. This hook is shared by Models 1–3.
- `prepare.py` includes the existing account module in generated runtime and
  includes the focused adapter contracts in its existing verification command.

The intercepted account request uses Playwright `route.fetch` once with zero
redirects/retries and a 12-second bound. Redirects are rejected. Server CORS
permission is required; response status, headers and contents are otherwise
forwarded unchanged. Response data is not parsed or logged and is disposed.
This uses Playwright's context-associated HTTP transport and Cookie jar; it is
not identical to native XHR. No extra account check is started.

## Verified locally, without CoinGlass or production

- 45 focused tests passed against generated runtime: 21 new adapter contracts,
  11 existing login guards, 6 capture/cleanup contracts and 7 source-reload rules.
- 2 additional integration tests passed: all nine model/timeframe identities
  retain native success/blocking behavior, and registration failure preserves
  the original guard. These use synthetic page objects, not live scans.
- 5 transport tests passed with pinned Playwright 1.55.0: eight synthetic HTTP
  requests to temporary loopback fixtures; redirect destinations received zero
  requests. Responses/disposal and fixture cleanup were checked.
- No Chromium, CoinGlass, AI, real account, database or Render access occurred.
- Existing installers generated both baseline and candidate runtime; dependency
  installation and the five broad previously completed test commands were
  suppressed for generation. A full `prepare.py` test run is not claimed.
- Generated capture, source guards, Hover, AI scanner, queue and identity modules
  are byte-identical to baseline. Only generated `model1_execution.py` changes
  and `source_account_diagnostic.py` is added. Existing account functions have
  identical ASTs; only `prepare_context` changes in execution logic.
- Security review found no blocking scope, redirect or secret-output issue.

Focused local commands (after generating runtime):

```sh
python -m unittest test_browser_account_auth test_source_login_required test_capture_diagnostics test_source_reload -v
RUN_LOCAL_ACCOUNT_TRANSPORT=1 python -m unittest test_browser_account_transport -v
```

## Remaining gates

Real Chromium routing/CORS behavior, live account acceptance, full source
capture and app → AI → save → display → refresh → next-cycle acceptance are
not yet verified. No merge, deployment or production setting change is allowed
by this isolated implementation approval.

The old temporary header observer cannot be reused unchanged: it clears auth
environment before context setup, and an original browser Request does not
report headers used by `route.fetch`. A separately approved source attempt must
load this candidate through the existing isolated wrapper with its private
in-memory state, updated candidate pins and unchanged worker coordination.
Do not restore secrets to output/files or claim the old observation proves this
candidate's account acceptance. No new laboratory, service or worker is needed.

Concurrency and recovery branches were checked read-only. Neither modifies the
three root product files relative to the relevant base. Keep this patch on its
own branch; do not copy old experiment/runtime snapshots over it.
