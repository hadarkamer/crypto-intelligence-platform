# Pinned receiver dependency for producer CI

The producer's existing self-tests import the real `hl_testnet_runtime` package,
which lives on `paper-trading-v1` in the same repository. The `main` branch does
not contain that package. A clean reconstruction of main commit
`039d0ed5f2308e59dc8fdc6cd5f516a8a2390995` fails before running
`approved_alert_failure_isolation_selftest.py`, through the producer, forwarder
and transport test imports, with `ModuleNotFoundError: No module named
'hl_testnet_runtime'`.

The dependency was introduced in producer commit
[`bed88012d7a935d4964726d8826439766b481773`](https://github.com/hadarkamer/crypto-intelligence-platform/commit/bed88012d7a935d4964726d8826439766b481773).
GitHub's associated-pull-requests endpoint returned no PR for that commit. Its
message and the receiver release message identify the same isolated verification
build. The later producer and receiver fix commits likewise identify one shared
verification build and were recorded one second apart:

| Release | Producer commit | Receiver commit | Shared verification build |
| --- | --- | --- | --- |
| October 7 | `bed88012d7a935d4964726d8826439766b481773` | `b3c066967ab3d27c93b7bbaaeb20a75cd5ab1be9` | `dep-db38ltqjnfac7392fneg` |
| October 8 | `039d0ed5f2308e59dc8fdc6cd5f516a8a2390995` | `919ba37c3cff917e7d7c0c4c2ffc33a9d6ae8a20` | `dep-db3lkgmi0phs73ameddg` |

The October 8 build reference resolves to verification source commit
`5d834c2fd289a82af0db3bdd84437f676ba8b48a`. Render reports that build-only run as
`build_failed`; the release commit messages report the isolated checks. These
references establish the source pairing, not successful application deployment
or permission to activate anything. The encoded verification bundle has not been
compared byte for byte with this dependency checkout; the build's complete tree
is not claimed to equal the receiver release tree.

The `verify` CI job checks out receiver commit
[`919ba37c3cff917e7d7c0c4c2ffc33a9d6ae8a20`](https://github.com/hadarkamer/crypto-intelligence-platform/commit/919ba37c3cff917e7d7c0c4c2ffc33a9d6ae8a20)
into `.ci/receiver`, using non-cone sparse checkout for exactly three paths:

- `hl_testnet_runtime/`
- `experimental_execution_contract.py`
- `experimental_execution_fixtures.py`

The two root files are required by the existing shared-copy identity test. Their
Git blob IDs match the producer's copies:

| Object | Git object ID |
| --- | --- |
| Receiver complete tree | `b6bdd86bb73f60d5ede4b5d147baf86128b33a0b` |
| Receiver package tree | `490b4d6676b1ef9715a4fdf484c475c87258ad2c` |
| Shared contract blob | `6b0d8d0a2bd20a78c0ae5afe157714a71d7b2ef2` |
| Shared fixtures blob | `1b190c5f5bf064f5985e49d57e66848799ff4af4` |

Before tests, CI verifies the receiver commit and package tree and compares both
shared files byte for byte. `PYTHONPATH` adds this checkout only within the
`verify` job. Main-checkout script execution retains the producer root as its
first import path. The existing pinned `requirements.txt` remains the dependency
installation command; the receiver's optional SDK requirements are not installed.

The main checkout's `git ls-files '*_selftest.py'` still defines the full test
inventory. The secondary checkout supplies imports and does not replace, remove
or add scripts to that inventory. Compilation, source-integrity checks, the
Google Sheets receiver contract, the PostgreSQL test service and the separate
11-test collector job remain in place. Any test failure still fails `verify`.

This is a CI dependency repair. It changes no producer or receiver runtime
source, deployment configuration, live service, strategy, source acquisition or
transaction settings. A full CI pass is still required before treating the
dependency repair as verified. Local execution with this exact package and the
two root files passed all 35 tests in
`approved_alert_failure_isolation_selftest.py` and
`experimental_execution_transport_selftest.py`, without installing the optional
SDK. YAML parsing and comparison confirm that all prior workflow behavior is
preserved except the two dependency setup steps and `verify`-only `PYTHONPATH`.
