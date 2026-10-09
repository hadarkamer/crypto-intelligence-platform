# Original eight-request standalone runner

Active scope (2026-10-09): follow the
[formula-discovery-only plan](FORMULA_DISCOVERY_ONLY_PLAN_2026-10-09.md).
The active plan prefers the existing Oct6 connector bridge and bounded manual
commands. Native `tick` is an optional alternative after a dedicated read-only
source is configured, using separate compatible state. Never switch transport
after accepting proof. The continuous runner remains optional implementation;
unattended hosting is not an active-plan dependency. The original registration,
evidence and checkpoint contracts below are unchanged.

This runner closes the manual orchestration gap for the October 2026 registered
discovery experiment. It does not create a new population, change a registration,
install Production migrations, send notifications, place orders or automatically
register prospective validation. All existing frozen research modules remain
unchanged. A completed discovery selection is still descriptive research.

## Inputs and transport

Use these retained evidence bundles, extracted into separate read-only paths:

| Input | SHA256 |
| --- | --- |
| `crypto_prospective_discovery_evidence_2026-10-06.zip` | `0fa6e922a5241484611cd8b88b0853e10e69a0ec4984480cd39ba517f15c2ed8` |
| `crypto_pipeline_integration_evidence_2026-10-06.zip` | `41d13c14ff190f4e6514f79af88af7d3e75c61ecd712aacbeedc858c97314049` |

`--checkpoint` points to `prospective_discovery`. `--adapter-dir` points to
`oct6_future_execution`. Restore its `runtime/node_modules` from the baseline's
`runtime_dependencies.tar.gz`. The runner verifies the exact reviewed helper,
bridge, Node server and dependency lock bytes before loading the helper. PGlite
0.5.8 and pglite-socket 0.2.11 preserve the original registry. Use Linux, Python
3.12+, Node and the repository's Python requirements. No local database server
installation is required for this original registry.

The runner uses the existing native `AcquisitionStore.run_once`, source reader,
PostgreSQL executor and selector. It **does not** synthesize MCP envelopes.
`DIRECT_POSTGRES_NATIVE` is a separately sealed transport mode. Adoption requires
all eight requests to remain WAITING with zero proofs and no external transport
directory. Previously acquired MCP evidence must continue through its original
verified connector path. A native checkpoint cannot silently adopt a different
endpoint or discard its source identity after it has fetched evidence.

The source DSN comes only from an explicitly named environment variable, by
default `RESEARCH_NO_HORIZON_READ_DATABASE_URL`. Never put credentials in command
arguments, logs, commits or checkpoint archives. It must specify a single host,
database and user. Remote connections require hostname-verifying TLS and system
CA roots. Use a provisioned read-only source role. The native reader also sets
each transaction read-only and installs its timeout before the source statement.
No Production write connection is inherited. Loopback source connections are
only accepted with the explicit `--allow-local-source` test switch.

## Commands

These are path examples, not provisioned host resources:

```bash
python research_no_horizon_registered_runner.py status \
  --checkpoint /data/baseline/prospective_discovery \
  --adapter-dir /data/integration/oct6_future_execution \
  --work-dir /data/research-work --source-id confirmed-source-id

python research_no_horizon_registered_runner.py tick --enable-acquisition \
  --checkpoint /data/baseline/prospective_discovery \
  --adapter-dir /data/integration/oct6_future_execution \
  --work-dir /data/research-work --source-id confirmed-source-id

python research_no_horizon_registered_runner.py run --enable-acquisition \
  --checkpoint /data/baseline/prospective_discovery \
  --adapter-dir /data/integration/oct6_future_execution \
  --work-dir /data/research-work --source-id confirmed-source-id
```

All commands verify exact original request IDs, declarations and original
creation timestamps. The source connection factory is not called before the
destination database admits a due claim; the source database clock checks the
cutoff again. The four first-window requests become due at **2026-10-12 00:00Z**;
the four second-window requests become due at **2026-10-16 00:00Z**. Both are
03:00 Israel. These are earliest acquisition times, not completion promises.

A default tick visits at most eight requests with one acquisition query and one
bounded execution pass per request. A durable cursor preserves fairness when
the request budget is smaller. Each group uses its own two ordered native
window reports. There is no pooling, shortcut selection or replay registration.
Native scientific rejection remains BLOCKED; operational connection errors
remain retryable and are logged only by type. A failed request does not starve
later requests. The continuous runner polls every 300 seconds and exits when
all four groups are SELECTED or BLOCKED_ACQUISITION. Logs include error count,
blocked groups and selection completion. Cancellation is checked between units;
an in-flight native transaction finishes or reaches its configured timeout.

## Durability and shutdown

The whole work directory must live on a durable writable volume. Scratch or an
ephemeral web-service filesystem does not satisfy this requirement. The frozen
Node server closes and dumps the registry before a success receipt is emitted.
The process lock spans the entire run. An additional child-owned registry lock
survives exec, and a Linux parent-death signal requests clean Node shutdown if
the Python parent dies. A restart cannot concurrently open that same registry.
The host supervisor must manage the whole process group and allow enough
termination grace for the configured source budget plus clean checkpointing.
Use a restart policy that restarts failures, not successfully completed research.

Render cron services do not support persistent disks. Do not deploy this
file-backed runner as an ephemeral cron and claim its state is durable. A
supervised worker with a persistent volume is a compatible hosting shape. This
change does not provision it, select a Render workspace or verify a live source.
`source_configured=true` indicates an environment value, not successful live
authentication or verified deployment.

Checkpoint export and restore:

```bash
python research_no_horizon_registered_runner.py export \
  --checkpoint /data/baseline/prospective_discovery \
  --adapter-dir /data/integration/oct6_future_execution \
  --work-dir /data/research-work --source-id confirmed-source-id \
  --archive /backups/research-001.zip

python research_no_horizon_registered_runner.py restore \
  --checkpoint /data/baseline/prospective_discovery \
  --adapter-dir /data/integration/oct6_future_execution \
  --work-dir /data/restored-work --source-id confirmed-source-id \
  --archive /backups/research-001.zip
```

Export is create-only and contains the clean database snapshot (including
native SQL/raw-response proofs), original registry identity, native transport
and endpoint seals, fair cursor and operational receipts. It does not contain a
DSN. Restore requires a new directory, verifies its complete manifest, checks
the original registrations against the actual restored database and validates
native provenance. Invalid restoration never receives a success receipt. The
archive supplements, rather than replaces, the immutable original input bundles.
No remote backup upload is performed by this module.

## Gates still outside this increment

- Confirmed live source, source role and actual collection coverage.
- Durable host provisioning and observing an unattended invocation on that host.
- Both original future acquisition windows, their complete source evidence and
  research results. Window source caps and the 16,384 decision budget are unchanged.
- Exact selected candidates registered for subsequent fresh-parent validation.
- Any change to supported formula features, compatible asymmetry or trading
  execution/authorization. No such functionality is enabled by this runner.

Hashes preserve bytes and linkage; they do not independently authenticate an
arbitrary external JSON source. Native authority flags remain unchanged.

## Verification

The new scheduler selftests exercise early gating, identity bindings, fairness,
retry, cancellation and ordered selection. Its optional PostgreSQL integration
requires two explicit distinct local disposable `test_` databases; it creates
engineering fixtures and does not claim prospective market evidence. The
collection audit has a separate metadata-only contract and tests documented in
`COLLECTION_AUDIT_V1.md`. Original-eight acceptance uses the retained registry
and must confirm eight WAITING requests and zero source calls before cutoff.
