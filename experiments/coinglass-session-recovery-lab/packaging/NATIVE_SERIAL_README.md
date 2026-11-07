# Native PostgreSQL adapter for the existing serial fixture

Run explicitly in the approved temporary Render lab, as its non-root lab user:

```sh
python /lab/packaging/native_serial_lab.py --lab-root /lab --evidence-dir /lab/evidence/serial-20261107
```

Choose a new evidence-directory name for each attempt. PostgreSQL binaries are resolved from `initdb` on PATH; `--postgres-bin /path/to/bin` is available. There is no external-DSN option. The harness timeout is2400seconds, plus the separately bounded PostgreSQL startup and cleanup periods; this is not a hard total wall-time limit. TERM, INT and HUP request cleanup. The worker image itself remains idle; importing this module does not run a test.

Inputs are current app74006045, collector33bad18 and the existing SERIAL harness5cc8e326. `native_serial_manifest.json` pins306 exact product/harness/runtime inputs. The adapter checks them before and after execution and fingerprints all supplied app/collector source. Source files are not rewritten. The original harness's generated backend/UI build artifacts remain ordinary temporary test outputs.

The frozen `run.py` is hash-checked and adapted in memory only: substitute the owned loopback PostgreSQL base and native postmaster PID for Docker discovery; propagate the controlled temporary directory; add fixture guards; supply an explicit allowlist to every child. An AST-level unit check verifies all original acceptance assertions are unchanged except the obsolete Docker-container-count assertion. The original one-worker19jobs/18ready/one deliberately failed identity, exact retry, image/audit identity, SQL persistence, UI inputs and reload assertions remain intact.

The wrapper creates a fresh non-root PostgreSQL16 cluster in an owned temporary directory, binds127.0.0.1 on an unused unprivileged port, keeps UTC and `wal_level=logical`, and disables JIT. Only the original two new lab databases are created. The runtime reports the actual PostgreSQL patch version;16.10 is the assembly baseline. No hosted/staging/production database, real account session, API key or collector configuration is accepted or inherited.

The reviewed `collector_matrix` Linux helpers establish a verified subreaper and pidfd ownership after confirming the `/proc` PID namespace and absence of existing children. Sampled process RSS measures the native PG plus harness trees; samples do not establish ownership. Cleanup first gives the harness its normal cleanup path, then stops the owned cluster. A final pidfd-backed census kills/reaps any remaining adopted descendants, including detached children. Startup errors and intermediate cleanup errors cannot skip that final census. The run fails if cleanup or input integrity is unverified. A forced service kill cannot produce a successful cleanup receipt; an interrupted run without its final receipts is inconclusive.

Python audit hooks permit loopback/Unix IPC only. Actual scanner contexts are offline and source responses/AI replies use the frozen synthetic fixtures. Browser HTTP/WebSocket guards reject non-loopback requests outside source fulfillment; service workers are blocked. Node guards check fetch, WebSocket, TCP destinations and DNS. Every guard records a synthetic forbidden-host canary without connecting to that host. These are software guards for this fixed fixture workload, not an OS firewall or a sandbox for arbitrary code. Successful native acceptance additionally requires guard receipts for all19 offline source contexts, the UI process and Node, with no unexpected blocked destination.

Evidence is private under the chosen directory:

- `native-result.json`, `native-cleanup.json`, `native-resources.json`, `adapter-receipt.json`, `guards/*.json`.
- Original `summary.json`, `queue.json`, `application-evidence.json`, browser screenshots/assertions, source/AI fixture evidence, resources, queue transitions and cleanup.
- `supervised-harness.log` and the original child logs. Only fixed synthetic fixture credentials are present in child environments; real credentials are never requested or logged.

Pass requires **both** `native-result.json.status == passed` and the original `summary.json.status == passed`. This is offline application integration, not proof of a real CoinGlass session, real provider quality, hosted authentication/RLS or production deployment.

Local validation completed before publication:10 targeted adapter unit tests passed; no local PostgreSQL or Chromium retry was attempted. The tests cover unchanged assertions, fail-closed source pins, remote-DSN rejection, environment stripping, evidence-path containment, PG startup-timeout cleanup, guard receipts/canaries and cleanup continuation after an injected failure. Original170 product/collector offline tests were not repeated for these adapter-only changes. Real native SQL/browser acceptance must run on Render.
