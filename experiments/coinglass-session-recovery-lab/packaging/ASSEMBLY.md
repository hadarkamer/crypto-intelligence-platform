# Temporary isolated CoinGlass fixture worker

This package is a reviewable test artifact, not a product change. It is intended only for the approved temporary Render worker: **one instance, 1 CPU / 2 GB, no more than 24 hours and within the separately controlled USD 6 total budget**. Creating a worker does not authorize live CoinGlass, live AI, a new Session, production configuration changes, or product deployment.

## Repository and context

Publish only below `experiments/coinglass-session-recovery-lab/` on `lab/coinglass-session-recovery-20261107`, based on collector `33bad18f8be322009108584a6b71f3d65fa9b351`. Existing product paths at repository root must remain byte-identical. The nested `render.yaml` points to the nested `packaging/Dockerfile` and uses the nested directory as its build context. It does not change the repository's existing Blueprint or service.

The assembled context contains:

- `app/`: only verified manifest-listed source files from app `74006045c0259f77eda6a641816cdf7bb33f4d19`, the existing serial fixture from `5cc8e326153aad2a5d39006e92333b4bf740bcc8`, and verified SQL files. No workflows are added or changed by packaging.
- `collector/`: only manifest-listed entrypoint/fixture/test files and the verified reconstructed `33bad18` runtime. The runtime provenance explicitly records that the reconstruction manifest itself is not a fresh live-runtime attestation.
- `packaging/`: this Dockerfile, idle process, dependency pins, isolated fixture drivers and their tests. These additions are temporary lab infrastructure, never copied into the product root.
- `manifests/`: the five source provenance manifests, including the eight current app test/dependency files required by the native runner.
- `.dockerignore` and `render.yaml`: reviewed copies for the context root.

`assemble_context.py` verifies every source file's Git blob hash or recorded SHA-256 before copying its exact bytes. It accepts only explicit manifest entries and explicit lab filenames, rejects symlinks and traversal, requires UTF-8 content, and refuses to overwrite an existing context. Its sibling publish manifest gives the exact local path, remote path, byte length and SHA-256 of every publishable file. Downloaded dependencies, virtual environments, binaries, browser profiles, Session material, `.env` files, logs, generated UI bundles, evidence and caches are excluded. A separate high-confidence pattern scan checks private keys and common OpenAI/GitHub/AWS credential formats; it reports only path, line and rule on a match. The receipt distinguishes this limited heuristic scan from source-hash and file-allowlist verification and does not claim an exhaustive secret audit.

Example local assembly after all fixture-driver files are finalized:

```sh
python nov7_render_lab/packaging/assemble_context.py \
  --lab nov7_recovery_preparation/lab \
  --out nov7_render_lab/context
```

The total text payload and largest file are measured in `context-publish-manifest.json`; do not infer Docker image size or build duration from the source payload size. The browser, native packages and UI dependencies are installed during the authorized image build rather than committed as binaries.

## Container assembly

All three official base images are pinned to verified linux/amd64 manifest digests in `base-images.json`: Python 3.12.12 slim Bookworm, Node 24.10.0 Bookworm, and PostgreSQL 16.10 Bookworm. Render uses linux/amd64. Node installs pg 8.16.3 from its lockfile; official Bun 1.4.4 installs the existing application's frozen lockfile with lifecycle scripts disabled.

The final image uses the Python base, so it inherits no PostgreSQL entrypoint, volume or exposed-port metadata. PostgreSQL native binaries, share files and their matching libpq are copied from its pinned stage. Common runtime libraries are installed in the final image. Optional LLVM JIT support is not claimed: the isolated PostgreSQL runner must set `jit=off`. Build commands check `postgres`, `initdb` and `psql` versions without initializing or starting a database.

Python dependencies are pinned in `requirements.txt`. Playwright 1.55.0 installs Chromium build 1187 and its operating-system dependencies. The browser fixture uses `/opt/playwright/chromium-1187/chrome-linux/chrome`; the normal Playwright installation also provides the headless-shell package. Build output records installed Python package versions in `/opt/lab-python-installed.txt`.

The final user is `lab`, UID 10001, with `/bin/bash`. `/home/lab/.ssh` exists with mode 0700 for the existing Render Shell capability; no key is generated or registered. `/lab` and its evidence directory are owned by this user. Native PostgreSQL therefore runs as an unprivileged user and needs no root-guard workaround.

## Startup and explicit test execution

The only default startup is `tini -- python /lab/packaging/worker_idle.py`. It does not import product code, connect to anything, initialize PostgreSQL, launch Chromium, scan, analyze, or write application data. It rejects root execution and unexpected source/AI/database credentials with a fixed message that contains no values. A ready message is proof only of the inert container startup.

The Blueprint has one worker, automatic deploys disabled, and no environment group, configured credential, disk, database, public route, service port, scaling or scheduled task. The worker cannot use the production Session because none is supplied. Local fixture runners must still independently enforce their loopback-only and dummy-input contracts; this package does not claim a platform-wide outbound firewall.

Tests are invoked explicitly through the authorized Shell after the idle deployment has been verified. The native serial runner owns its new local PostgreSQL cluster and its cleanup; `QUEUE_LAB_NODE=/opt/lab-node`. Source fixtures use the verified collector runtime at `/lab/collector/standalone_collector/runtime`. The separate driver documentation defines each bounded command and its success evidence. No live candidate is accepted by the dummy handoff wrapper.

## Limits and completion controls

Before deletion, retrieve the actual sanitized JSON receipts through the existing Web Shell and persist them outside the worker: `/lab/evidence/collector/matrix.json`, and `summary.json`, `native-result.json`, `native-cleanup.json`, `native-resources.json`, `resources.json`, and `cleanup.json` under the selected serial evidence directory (the documented example is `/lab/evidence/serial-20261107`). Preserve the selected synthetic PNG/hover artifacts and their hashes if they support a reported assertion. A list of filenames or hashes alone is not a retained copy of the evidence. Inspect only the fixed fixture paths; do not export environment variables, process environments, arbitrary home directories, credentials or raw supervised logs. Use bounded chunks for larger artifacts and verify the reconstructed byte count and hash before deleting the worker. No additional export API, service, provider or public URL is required.

The reviewed drivers already print bounded sanitized summaries to their own output. An interactive Shell's output is not assumed to be the service's PID1 output or automatically retrievable through service-log APIs. If the operator deliberately sends selected nonsecret receipt chunks to PID1 stdout for the existing log retrieval tool, verify retrieval and content integrity before treating that as an export. The current Render logging reference states a Hobby retention period of seven days and a limit of6,000 application-generated lines per minute; neither provides a durable evidence archive or proves retrieval after resource deletion. The preferred route is the existing Shell with immediately saved, hash-verified receipts.

The Docker image cannot enforce a billing cap by sleeping or exiting: a restartable worker can continue accruing charges. The operator must record creation time, monitor total build/runtime cost, and delete the approved temporary worker by the 24-hour deadline or earlier budget/stop condition. No watchdog service, schedule, resource increase or persistent disk is introduced here.

Local validation covers the current Render JSON schema, fixed resource settings, inert non-root startup, credential rejection, image digests and assembly path safety. It does **not** prove that a Docker build succeeds, a Render Shell is available, PostgreSQL starts, Chromium works, or the serial integration passes. Those require the separately approved deployment and explicit isolated tests. No local failed browser or PostgreSQL experiment was repeated for this package.

Current references were retrieved directly and read on 2026-11-07: `https://render.com/docs/blueprint-spec.md`, `https://render.com/docs/compute-plans.md`, `https://render.com/docs/service-types.md`, `https://render.com/docs/logging.md`, and `https://render.com/schema/render.yaml.json`. Schema validation is structural; it is not service creation or a billing action.
