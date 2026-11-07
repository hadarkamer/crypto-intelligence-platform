# Synthetic-only session handoff

`session_handoff.py` accepts exactly two public synthetic fixtures: `single_obe` and `full_header`. The latter contains four cookie pairs, including a value containing an additional `=`. Every unrecognized candidate is rejected; there is no live flag, account action, browser launch or AI path.

The installed collector is read from `/lab/collector/standalone_collector/runtime`. The capture module and its `_load_cookie_header` source must match the pinned runtime. Only that function is extracted by AST; the capture module is not imported. All cookie pairs pass through the unchanged conversion. The fixture is handed to a fixed child through a newly constructed environment; ambient production/session/storage/AI/DB/proxy variables are not copied.

Render commands:

```sh
python -B /lab/packaging/test_session_handoff.py
python -B /lab/packaging/session_handoff.py --fixture-id full_header
```

`--tty` exercises the candidate-entry boundary with a public fixture only. It requires a controlling TTY, disables echo before its prompt, uses noncanonical byte input, and restores the original terminal settings. The reader consumes at most 65,536 total bytes, including editing bytes and newline, and uses a 20-second monotonic deadline. It never falls back to stdin. Prefixes such as `Cookie:`, duplicated cookie names, control characters, empty values and malformed pairs are rejected. Structurally plausible real credentials are still rejected by the exact-fixture allowlist. Only fixed reason codes and nonsecret booleans/counts leave the process; no submitted values or token hashes are emitted.

The new local test suite passed 10 tests in 1.426 seconds without a browser or PostgreSQL. Tests include actual PTY no-echo/restoration; prefix/duplicate/nonfixture errors; a 32-byte oversized-input test that reads exactly 32 bytes; exact newline/read-budget boundaries; deadline/EOF/interruption; four-cookie installed conversion; and actual fixed-child environment/argv hygiene. These results establish this synthetic contract, not a working account, entitlement, real candidate completeness or browser-process cleanup. Render execution is reported separately after the scripts run there.

This handoff is intentionally separate from `collector_matrix.py` / `collector_browser_fixture.py`, which exercise synthetic source HTML with the actual screenshot/native-hover path, and from `native_serial_lab.py`, which owns the local PostgreSQL/app checks. Neither simulated fixture access nor a syntactically complete header proves source authentication.

A future genuine candidate requires separate authorization, normal-account BTC/Model1/12H access evidence, a reviewed change replacing the fixture-only boundary, and the real lab supervisor/cleanup boundary. Preserve the collector's cookie conversion, reader and login/readiness gates. Do not change Render service credentials, reuse an active worker/browser, invent an HTTP authorization header, or treat this script's tests as permission for a live source request.

Limits: Python strings/environment memory are not securely erased; the OS can inspect process environment with sufficient privilege. Catchable termination is deferred across child creation and cleanup. SIGKILL/kernel stalls are not a proven cleanup guarantee. The fixed child never spawns Chromium; Chromium descendant cleanup is tested by the separate matrix supervisor.
