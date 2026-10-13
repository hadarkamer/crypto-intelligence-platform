# Isolated Render build check

This branch prepares one **new Render Static Site**, named
`ordered-native-build-check-20261013`. It runs native PostgreSQL correctness
checks during its build and publishes two small result files. It does not start
the bot, change an existing service, connect to a production database, send
orders, or perform a memory benchmark.

The exact `render_create_static_site` request is in [service.json](service.json).
No service has been created by these files. A prior automatic approval review
blocked the GitHub push and required explicit authorization for that repository
destination. This prepared branch does not bypass that existing restriction.

## Exact proposed action

- Push **only** branch `codex/ordered-native-build-check-20261013` to
  `https://github.com/hadarkamer/crypto-intelligence-platform`; do not merge it.
- Freeze and verify that branch at the exact approved commit **before** creating
  the new static site in workspace `tea-d94cq7e7r5hc73dee88g`. Creation starts the
  initial build automatically; do not trigger a second build. Verify the actual
  build's source commit against the approved commit. Later automatic deploys
  are disabled. No preview is requested; verify returned preview settings if needed.
- Use build command `python tools/render_native_build/entrypoint.py`, publish
  directory `native-build-public`, and only the two environment variables in
  the specification. Attach no existing environment group, secret, database,
  persistent disk, or existing service. Build minutes may count against the
  workspace allowance.

`PYTHON_VERSION=3.11.9` overrides this base branch's `.python-version` value
3.11.11. `SKIP_INSTALL_DEPS=true` prevents automatic installation of the bot's
full dependency set; the adapter installs only existing pins
`psycopg[binary]==3.2.3` and `requests==2.32.3`.

## What the build does

The unchanged October 6 source builder compiles checksum-pinned PostgreSQL 18.6
and Flex 2.6.4 into checkout-local directories without sudo. It still needs a
working compiler, make and m4. Its SHA-256 is
`08e665705098479fcceb4366bfb4d7c882538c3ddcecb917a6312a82ff479320`.
It came from `hl_testnet_runtime/render_preflight/build_postgres.py`; its new
two-directory-deep location preserves the builder's checkout-root calculation.
The earlier successful Render build is historical precedent, not a guarantee
of the current host's toolchain.

The adapter then invokes the unchanged native runner with the frozen October 12
runtime contract. It requires non-root CPython 3.11.9, glibc 2.36, psycopg 3.2.3,
native PostgreSQL 18.6 and consistent process identities. Unknown production
driver/libpq dimensions remain unknown. Sources are the exact seven tool/contract
files plus the application binding change copied from local commit
`f8a81b17d5213fe15c8613c9fe2b6535887d83bc`; the branch base is `3c5bdb9`.

One fresh private directory under `/tmp/onpg-*` contains the disposable cluster
and receipts. The runner creates its own database and checks **32 synthetic
rows, full result parity, five positive input cases and cleanup**, with **zero
memory passes**. PostgreSQL is used only inside this build container; its
processes end before the static site is published. The build container can use
the network for source/dependency downloads; this is not a Docker
`--network none` run.

## Evidence and publication boundary

All six exact JSON receipts are emitted only to private Render build logs as
`[NATIVE_BUILD_RECEIPT]` records. Each file has byte length, SHA-256, ordered
base64 chunks, a file-completion marker and a final manifest. Failure paths emit
whatever receipts exist plus at most 8 KiB tails from four fixed diagnostic
logs, clearly marked if truncated. No PostgreSQL data files, environment values,
source code or arbitrary files are included.

After the build, retrieve those private log records and reconstruct **all six
receipt byte streams**. Verify chunk order/count, completion markers, byte
lengths and SHA-256, then check their binding chains and the public summary's
receipt hashes. Log emission by itself is not proof that retrievable evidence
was retained. Missing/truncated receipts prevent final acceptance.

The public directory is assembled separately and contains only `summary.json`
and `index.html`: PASS status, exact build commit, runtime versions, 32/5 counts,
zero memory passes, receipt hashes and explicit limitations. It contains no
raw receipt, host path, database, market observation, ranking, environment or
log. Publication requires READY preflight, verified native correctness, matching
source/driver/host receipts, and successful cleanup.

A failed or blocked test exits nonzero and prints a bounded failure summary to
private build logs; it does not publish a new PASS page. An older successful
page is not evidence for a later failed build: always bind the page and logs to
the exact requested commit. The candidate binding change is still not deployed
to production. Native correctness PASS would not prove a memory improvement or
resolve the production OOM issue; those remain separate verification work.

## Local publication checks

`python tools/render_native_build/entrypoint_selftest.py` passed **4 tests** using
synthetic bound receipts and the actual frozen verifier. The tests cover receipt
integrity, public-output isolation, exact private-log reconstruction, and path/size
rejection. They used the existing `worker_host_venv` interpreter; **zero native
PostgreSQL runs, Render builds or memory experiments** were performed.
