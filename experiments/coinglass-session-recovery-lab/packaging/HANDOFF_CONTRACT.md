# One-shot private session handoff and retained fixture modes

The existing fixture modes still accept exactly two public synthetic fixtures: `single_obe` and `full_header`. The latter contains four cookie pairs, including a value containing an additional `=`. The separately authorized `--live-tty` mode now accepts a private Cookie-header value for **one BTC/Model1/12H source-only trial** through the fixed `collector_matrix.live_single` supervisor. There is no AI, database, product worker or retry in that mode.

The installed collector is read from `/lab/collector/standalone_collector/runtime`. The capture module and its `_load_cookie_header` source must match the pinned runtime. Only that function is extracted by AST; the capture module is not imported. All cookie pairs pass through the unchanged conversion. The fixture is handed to a fixed child through a newly constructed environment; ambient production/session/storage/AI/DB/proxy variables are not copied.

Render commands:

```sh
python -B /lab/packaging/test_session_handoff.py
python -B /lab/packaging/session_handoff.py --fixture-id full_header
python -B /lab/packaging/test_session_handoff.py --live-input-tests
```

`--tty` remains public-fixture-only. Both TTY modes require a controlling terminal, disable echo before the prompt, use noncanonical byte input and restore the original settings. The reader consumes at most65,536 total bytes, including editing bytes and newline, with a20-second monotonic deadline; it never falls back to stdin. Prefixes such as `Cookie:`/`Set-Cookie:`, controls and malformed name/value pairs are rejected. Live syntax validation does not require `obe` or any other specific cookie name, and permits empty values, repeated names and additional `=` characters as the existing loader does. It does not prove session validity or completeness. The original strict fixture validation is unchanged.

For the approved private handoff, the user prepares the complete header value before starting this single command:

```sh
python -B /lab/packaging/session_handoff.py --live-tty
```

After the private prompt appears, the user pastes the value and presses Enter within20seconds. The total timing remains **45seconds of work including TTY input, plus15seconds reserved for cleanup:60seconds maximum** under the supervisor's deadline contract. There is no second waiting stage and the clock does not restart after input. The candidate enters only this process's memory, then a freshly allowlisted child environment as `COINGLASS_COOKIE_HEADER`. It is never an argv value, persisted file, log, Render environment setting or output field. No runtime override or inline-cookie flag is accepted in live mode. Core dumps are disabled before private input; inability to establish that guard prevents input.

The existing lab filesystem lock and one-use attempt marker belong to the live supervisor. It performs the single source trial, bounded descendant cleanup and fixed-schema report sanitization. Only that sanitized report is printed; failed or missing result validation is reported with a fixed reason, without exception text or candidate data. An inconclusive attempt must not trigger an automatic retry.

The earlier10-test synthetic suite and evidence are retained. Eight focused changed-input tests passed locally with public fixtures and a mocked live supervisor: loader-compatible headers without `obe`, malformed input, real PTY no-echo/restoration and timeout, CLI restrictions, timing before input, core-limit guards and sanitized failure output. They made zero source/browser/database/AI calls. These checks do not establish a working account, entitlement, real candidate completeness or browser-process cleanup; the actual one-shot result is separate evidence.

This handoff is intentionally separate from `collector_matrix.py` / `collector_browser_fixture.py`, which exercise synthetic source HTML with the actual screenshot/native-hover path, and from `native_serial_lab.py`, which owns the local PostgreSQL/app checks. Neither simulated fixture access nor a syntactically complete header proves source authentication.

Authorization is limited to the approved one-shot trial and unchanged collector conversion, reader and login/readiness gates. No Render service credentials are changed, no active browser/production worker is reused, and no HTTP authorization header is invented. The default synthetic tests remain independently runnable.

Limits: Python strings/environment memory are not securely erased; the OS can inspect process environment with sufficient privilege. Catchable termination is deferred across child creation and cleanup. SIGKILL/kernel stalls are not a proven cleanup guarantee. The fixed child never spawns Chromium; Chromium descendant cleanup is tested by the separate matrix supervisor.
