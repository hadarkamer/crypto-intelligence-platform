# Original research worker: host preparation and activation

Scope update (2026-10-10): this document is retained as archived preparation for
an optional implementation. The active
[formula-discovery-only plan](FORMULA_DISCOVERY_ONLY_PLAN_2026-10-09.md) reuses
human-initiated bounded research passes. New automatic-worker development,
provisioning, deployment and enablement below are not active-plan steps or
instructions to perform them. This supersedes the temporary scope expansion at
21:17 UTC on 2026-10-10. It does not stop or reconfigure the existing independent
Hyperliquid Testnet bot or its workers. This scope note makes no claim about
current hosting or deployment status.

This change packages PR148's existing runner for one persistent background
worker. It does not alter the eight original registrations, native acquisition
queries, execution engine, discovery policy, deadlines, or Production service.
The worker starts **disabled**. Building an image or opening a draft PR does not
provision a Render service or establish live source access.

## Concrete service specification

| Setting | Value |
|---|---|
| Separate service | `crypto-registered-research-oct2026` |
| Type / runtime | Background worker / Docker, Linux amd64 |
| Source branch | `research/registered-worker-host-20261008` |
| Automatic deploy / previews | Off / off |
| Instances | Exactly one |
| Persistent disk | 5 GB at `/var/data/no-horizon` |
| Render shutdown grace | 300 seconds |
| Supervisor drain budget | 240 seconds, then bounded process-group cleanup |
| Runner interval | 300 seconds |
| Initial enable flag | `NO_HORIZON_SUPERVISOR_ENABLED=0` |
| Source secret | `RESEARCH_NO_HORIZON_READ_DATABASE_URL` |

Confirm the intended Render workspace before any resource operation. Read its
source database region and confirm the paid worker plan before provisioning.
The Blueprint generator requires an explicit region and plan; it has no default
region and makes no API calls. The initial 2 GB plan is a preparation choice,
not a measured guarantee of future market workload memory use.

```bash
python tools/render_research_worker_blueprint.py \
  --region CONFIRMED_REGION --plan 1c-2g --output render-research-worker.yaml
```

The output is JSON, which is a valid YAML document. Apply it as a **separate**
Blueprint with auto-sync disabled. Do not replace the existing service's
Blueprint. Verify the target branch's exact tested commit before a manual
deploy. Keep the worker disabled during setup.

The Dockerfile pins both official base-image digests. Its dedicated requirements
include the complete tested Python import closure; private ZIPs, database state,
secrets and Node dependencies from the evidence are excluded from the image.
CI builds the image and checks disabled startup and clean shutdown on a mounted
volume. That check does not authenticate the live source or establish Render
disk durability.

## Private inputs and existing native state

Keep `PYTHONDONTWRITEBYTECODE=1` during every command. It is already set in the
image. Otherwise Python can add bytecode beneath the immutable input tree and
the subsequent integrity check will reject the changed installation.

After the disk is mounted, transfer the two original ZIPs named in
`NO_HORIZON_REGISTERED_RUNNER_V1.md` and the retained native-state ZIP through a
private authenticated file-transfer route into `/var/data/no-horizon/seed`.
Do not put the ZIPs in Git, public image layers, build arguments or public URLs.
Render's service-specific SSH/SCP endpoint must come from the confirmed service.

Stage and verify the exact inputs:

```bash
python research_no_horizon_worker_bootstrap.py stage \
  --persistent-root /var/data/no-horizon \
  --baseline-zip /var/data/no-horizon/seed/crypto_prospective_discovery_evidence_2026-10-06.zip \
  --integration-zip /var/data/no-horizon/seed/crypto_pipeline_integration_evidence_2026-10-06.zip

python research_no_horizon_worker_bootstrap.py verify \
  --persistent-root /var/data/no-horizon
```

Bootstrap checks the original ZIP hashes, validates bounded paths and dependency
links, fsyncs private staging, and atomically installs `inputs`. Repeating stage
only verifies an identical installation. It never registers or opens research
state. Verification derives its expected inventory again from the pinned ZIPs;
editing the extracted files and the local manifest together does not pass.

Restore the retained native state **once into a new work directory**:

```bash
python research_no_horizon_registered_runner.py restore \
  --checkpoint /var/data/no-horizon/inputs/baseline/prospective_discovery \
  --adapter-dir /var/data/no-horizon/inputs/integration/oct6_future_execution \
  --work-dir /var/data/no-horizon/work --source-id crypto-research-source \
  --node /usr/local/bin/node \
  --archive /var/data/no-horizon/seed/original_eight_native_runner_state.zip
```

The Oct8 zero-proof state archive's SHA256 is
`3ff106cb6713bb59eace5c1b8d7da9a31309d3653a3e60fceb7f35602914da51`.
It is an initial checkpoint, not a replacement for subsequent accepted proofs.
On normal restart reuse the existing work directory. Never restore this initial
archive over progressed research state or regenerate registration timestamps.

## Source preflight and activation

Provision a dedicated source login using the scoped grants in
`NO_HORIZON_SOURCE_ROLE_V1.md`. Supply its connection string as the runtime
secret; never use the Production write role or inherit `DATABASE_URL`.
The remote endpoint must support hostname-verifying TLS with system CA roots.
If an internal endpoint cannot satisfy that contract, choose an appropriate
verified endpoint; do not weaken TLS or switch on the loopback-test exception.

The separate metadata-only command can diagnose the setup before activation:

```bash
python research_no_horizon_source_preflight.py --execute \
  --source-id crypto-research-source
```

It reads PostgreSQL catalogs, session settings and privileges, not market rows.
A PASS is a point-in-time check of the documented access contract, not proof of
arbitrary SQL behavior, future permissions, or research-data completeness.

After validated inputs, successful original-state restoration and source setup,
set `NO_HORIZON_SUPERVISOR_ENABLED=1` and manually redeploy the separate worker.
Each enabled, nonterminal start performs fresh metadata preflight and retains a
sanitized receipt. Transient connection failures wait and retry. An incompatible
source role, changed inputs or unverified state is held for operator correction.
Completed research remains held without reauthenticating or reacquiring.

Metadata preflight may authenticate before the acquisition cutoff. The native
market reader still opens only after its destination claim is due and enforces
the source database cutoff again: October 12 and 16, 2026 at 00:00 UTC / 03:00 Israel.
No market result can be inferred from preflight or heartbeat success.

## Host acceptance and recovery

Read `/var/data/no-horizon/supervisor/heartbeat.json`. It separates disabled,
running, retry, terminal, error-hold and stopped states. Its timestamp reports
process observation, not durable hosting or trading authorization. The durable
runner receipts remain under `work/receipts`; each successful receipt follows
clean registry shutdown. Inspect state and counts without dumping source rows.

Validate one enabled pre-cutoff invocation, a controlled stop and restart, and
unchanged original IDs/creation times with eight WAITING requests and zero
proofs. Observe the same disk-backed state after the actual Render restart
before marking host durability verified. A local process test alone is not that
evidence. After the deadlines, preserve the complete native source evidence and
apply the original per-window selection rules.

For a native backup, disable the worker so it remains accessible for SSH but
the child runner is stopped. Run the existing runner `export` command with a
new archive filename, copy that archive to independent private storage, and
verify restoration into a separate directory before relying on it. Reenable
the original worker after the export. Render disk snapshots are not a
substitute for this database-native export/restore path.

If a terminal latch exists, restart validates the linked final receipt and
snapshot and holds completion; it never starts another acquisition loop.
Do not delete a latch, reseal edited input, change source identity, or replace a
progressed registry to make a failed check pass. Diagnose the actual mismatch.

## Official infrastructure references

- [Render background workers](https://render.com/docs/background-workers)
- [Persistent disks and their limitations](https://render.com/docs/disks)
- [Shutdown and deploy behavior](https://render.com/docs/deploys)
- [Blueprint specification](https://render.com/docs/blueprint-spec)
- [Docker build and runtime secrets](https://render.com/docs/docker)

Native research behavior and immutable experiment details are documented in
`NO_HORIZON_REGISTERED_RUNNER_V1.md` and the original Oct6 evidence bundles.
