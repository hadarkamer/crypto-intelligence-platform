# Isolated native PostgreSQL comparison

The authorized isolated Render Static Site `srv-db724ad9fdbs73ep61j0` builds branch
`codex/ordered-native-build-check-20261013`. Auto deploy and previews are disabled.
The build command remains `python tools/render_native_build/entrypoint.py` and
now dispatches to `memory_entrypoint.py`. Existing production services and the
parallel Hyperliquid test bot are outside this experiment.

## Bounded experiment

First run the existing 32-row native correctness gate. Then run A1, B1, B2, A2
sequentially, each with a fresh owned native PostgreSQL cluster and separate
preparation/measurement processes. A is the original application module from
`3c5bdb9ae5a450d1b3fb0cd4c71bab599d252ad3`; B is the scalar array binding fix.
Their pinned module SHA-256 values are recorded in the adapter and staging receipt.
All other copied application and measurement files are byte-identical.

Each arm uses 2,000 synthetic rows, one scope, 12 passes, one persistent executor
thread, a fresh connection per pass, and rollback to the unchanged checkpoint.
The same observer policy records the actual binding type without rewriting it.
Full semantic/canonical checks run before and after the measured interval.
GC stays enabled with unchanged thresholds; no forced collection or trimming.
Warm summary is fixed in advance to passes 4–12. All 12 RSS endpoints are retained.
Sampled RSS, lifetime high-water RSS, wall/CPU/observer time are separate quantities.
Two fresh replicates are engineering replication, not 24 independent trials.

The PostgreSQL 18.6 builder is unchanged and checksum-pinned. The runtime contract
requires CPython 3.11.9, glibc 2.36 and psycopg 3.2.3. Unknown production loaded
driver/libpq details remain unknown. Installs use the two existing pins only.
All servers bind owned loopback/private sockets, validate their marker and process
identity, and finish before publication. No service worker or trading code starts.

## Evidence and publication

Private logs carry 32 allowlisted artifacts: six correctness receipts, six files
for each of four memory arms, comparison.json and staging.json. Each memory arm
includes its sealed evidence.zip, containing the exact samples, receipt, fixture
manifest and artifact checksums. Transport caps remain 256 KiB per file / 1 MiB
total; chunks are 2,048 base64 characters with SHA-256 and completion records.
Retrieve every paginated log response and independently reconstruct original
bytes before accepting results. Missing evidence invalidates final acceptance.

The public static tree contains only index.html and a bounded summary.json with
synthetic numeric aggregates, runtime versions, exact build commit, receipt hashes
and limitations. No database files, raw journals, source, environment values or
arbitrary logs are published. A failed build leaves any older public page in
place; an old page is never evidence for a new commit.

This comparison cannot establish production OOM resolution or concurrent-worker
safety. It does not deploy the candidate application fix to the production bot.
