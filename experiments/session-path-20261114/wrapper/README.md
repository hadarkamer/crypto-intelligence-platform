# CoinGlass session reuse wrapper — prepared, not live-ready

This private preparation is a new copy of `coinglass_session_restore_20261113`.
The original package and all its manifest hashes are unchanged. Product candidate
`ace67103638375679c93552204e1573c2b5cb4f5` supplies byte-identical
`session_state.py`, `session_runtime.py` and `session_capture.py`. There is no new
state format, database lock, service, authentication request or header injection.
The installed reader remains pinned to the existing `33bad18` runtime.

## Current blockers

No personal session transfer or permanent encrypted storage has been approved.
No source call, production write, installation or deployment was performed for
this package. The actual worker inspection reported that PyCryptodome is absent;
`from Crypto.Cipher import AES` therefore fails. This wrapper now bundles the approved version
3.21.0 privately, without installing it in the worker virtualenv. The exact PyPI
cp36-abi3/manylinux2014 x86-64 wheel is pinned by SHA-256 and unpacked into
`vendor/` during package preparation, never downloaded or installed during a
source attempt. Entry and lock keeper independently verify the archive, exact
extracted bytes, private permissions, import origin and native AES-GCM operation
**before personal input or database use**. Only their process-local import paths
change; the browser child receives no dependency path, database URL or key.
Transfer/storage consent and the final real-browser wrapper integration remain
required; a bundled library alone is not a completed login solution.

The private candidate table and verified collector grants must already exist
under the storage approval. This package performs no DDL or grants. The encryption
key must be present in private worker configuration and remain parent-only. The
normal worker's managed-session enable flag is not switched on by this package.

## Existing primitives connected

The keeper retains the existing exclusive `WORK_LOCK` on its own PostgreSQL
connection. That same connection supplies `PgSessionStore` and `ManagedSession`.
Queue inspection is still SELECT-only; no job is created, claimed or completed.
The keeper launches the existing source-only observer through the candidate's
`SessionTransport` pipe. The browser child receives neither the database URL nor
the encryption key. Its legacy Cookie Header is empty, while the worker's original
header remains unchanged. There is no environment-state fallback during reuse.

The unchanged `CaptureSession` consumes private state before the driver starts,
attaches before navigation, and returns its snapshot before native context close.
The existing passive access observation remains separate: a saved session does
not establish access to 12H, and a changed cookie does not establish renewal.
The candidate may preserve an accepted session after an unrelated Hover failure.

The existing keeper control pipe now carries three fixed commands:

- `S`: start only after the supervisor has verified keeper identity.
- `C`: permanently prevent any future source launch and acknowledge quiescence.
- `R`: release only after the supervisor has verified descendant cleanup.

The cancellation consumer and launch boundary share one event loop, with no
await between the final cancellation check and process creation. Thus a cleanup
acknowledgement cannot precede a later launch. All observer descendants remain in
the existing census; only the exact keeper PID/start identity is excluded. Raw
session state never enters this control stream or the technical stdout relay.
The existing private pipe remains the only browser-to-parent state handoff.

Normal completion and failures persist through the candidate before lock release.
If quiescence or descendant cleanup cannot be verified, the keeper is retained and
the receipt explicitly reports incomplete cleanup/operator intervention. That
exceptional path is not claimed to have completed within 60 seconds. No retry is
performed.

## Future commands — not authorization to run

Only after transfer/storage/live-run approval and the existing maintenance,
worker-version, queue and lock checks, from `/project/src` using the worker's
actual interpreter, the proposed commands are:

```sh
./.venv/bin/python -B /tmp/coinglass_session_reuse_20261114/approved_entry.py --bootstrap
./.venv/bin/python -B /tmp/coinglass_session_reuse_20261114/approved_entry.py --reuse
```

Each mode has a separate exclusive once-marker and filtered receipt:
`attempt-bootstrap.json` or `attempt-reuse.json`. Evidence directories are also
separate: `source_evidence-bootstrap` and `source_evidence-reuse`. No earlier
attempt or evidence is overwritten. Each invocation remains one BTC/Model 1/12H
browser, one navigation, no reload/retry, and a 60-second budget including cleanup.

Bootstrap validates the worker's temporary cookie-only state and refuses an
existing database slot. Reuse requires the earlier filtered receipt to prove
12H access, successful persistence, bounded cleanup and lock release. It then
checks that the stored revision is exactly the one saved by bootstrap before
beginning a new candidate lease. It does not bootstrap again, use the old header,
or fall back to an environment snapshot. A separate observer process and browser
are launched for the second invocation.

The temporary state may remain in the disabled worker during the same maintenance
window, but is omitted from the reuse keeper's environment. At the end, removal
of that temporary state and restoration of the original bridge-enabled value
must be applied together through the previously approved existing-build process.
The worker must not resume with experimental authentication enabled. Do not leave
the worker paused while preparing dependencies or investigating results.

## What was tested

Existing Actions run #84 proved the real application command with Python 3.11.11,
Playwright 1.55.0, Chromium 140.0.7339.16 and PostgreSQL 16.15: distinct processes,
state revision 3 then 5, stored-state reuse and ownership. That was a dummy-source
application-path test, not this source-only wrapper or real CoinGlass access.

The changed wrapper was separately tested through the **actual** entry command,
dummy worker procfs checks, real supervisor/keeper/source subprocesses, candidate
ManagedSession/SessionTransport/CaptureSession, and private pipe. Twelve focused
tests passed in 7.749 seconds. Actual SIGTERM was tested during delayed setup and
after source-child start; both preserved quiescence, cleanup and lock release.
Post-test owned process and temporary-directory counts were zero.

These wrapper tests explicitly substitute HTTP health, Playwright metadata,
PostgreSQL/store service boundaries, source-browser/page objects and the local
kernel census limitation. They use dummy values only. Local AES import/version
checking is real, except for the negative missing-dependency test. They do not
re-prove PostgreSQL encryption/concurrency, open Chromium, call CoinGlass, or
establish compatibility of this exact wrapper with a real browser.

Consequently, access to protected 12H, live reuse after browser closure, and
renewal after expiry remain separate unproven outcomes. This package is prepared
code, not a deployed or proven login solution. PR #132 is untouched.

## Bundled dependency preparation, 15 November 2026

The wheel SHA-256 is
`0714206d467fc911042d01ea3a1847c847bc10884cf674c82e12915cfe1649f8`
(size 2,254,555 bytes), published by PyPI for PyCryptodome 3.21.0. Its abi3 tag
supports CPython 3.6+ on Linux x86-64 with glibc 2.17+. The final integration must
measure actual loading under the worker-compatible Python 3.11.11 runtime.
A local Python 3.12.14 native import/roundtrip check passed; it is only packaging
evidence, not that compatibility result.

The package includes the unchanged wheel and its complete extracted tree. All
dependency directories are mode 0700 and files mode 0600. The loader rejects
missing, changed, additional or symlinked files and any Crypto module already
loaded from another location. The recorded path is the verified relative path
`vendor/Crypto/Cipher/AES.py`; no authentication values are recorded.

This is a temporary trial dependency only. A permanent product release must
install the pinned dependency through its official build path and verify that
build, rather than copy a temporary library on each scan.
