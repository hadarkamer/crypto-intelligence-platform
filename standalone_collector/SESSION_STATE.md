# Managed CoinGlass session candidate — not deployed

Base: `33bad18f8be322009108584a6b71f3d65fa9b351`.
Branch: `fix/coinglass-session-state-20261113`.

This candidate addresses the missing browser-to-next-run state handoff. It does
not establish that CoinGlass renews authentication, that a transferred session
opens 12H, or that an expired session can be renewed without a person.

## Existing infrastructure destination

Recommended destination: the existing worker PostgreSQL database, private table
`collector_private.coinglass_session_state`. `session_state.sql` is a candidate
migration only; it is never applied at application startup. No database, table,
role or production secret has been created for this candidate.

The row is AES-256-GCM encrypted. A separate 32-byte key, base64 encoded in private
worker configuration `COINGLASS_SESSION_KEY`, remains outside the database and
browser child. The existing build path needs the pinned `pycryptodome==3.21.0`
dependency; adding it to root requirements alone would not install it in this
service, so the candidate's existing preparation step includes the pin.

Before production use, verify actual schema ownership, default privileges,
existing named-role grants and the collector's database role. Grant that verified
role only schema USAGE and table SELECT/INSERT/UPDATE; no frontend/API grants.
REVOKE FROM PUBLIC alone does not revoke grants already held by named roles.
The migration does not assert compatibility of a pre-existing table.

## Coordination and state lifecycle

The writer is the existing worker parent while it holds the existing exclusive
advisory lock `748230102`. Browser children do not receive DATABASE_URL or the
encryption key. No new lock, scheduler or network service is introduced.

Managed mode is explicit: `COINGLASS_SESSION_STORE_ENABLED=true`. Default behavior
is unchanged. When enabled, a missing/corrupt/blocked row never falls back to a
static environment snapshot or the old Cookie Header.

1. An explicit, separately approved bootstrap can insert an absent row; it cannot
   overwrite an existing row, even one requiring review.
2. Begin locks/checks the row, validates/decrypts its state, and commits `in_use`
   with a fresh revision and run ID **before** launching the browser.
3. The existing storage loader receives that state. The child has an empty legacy
   Cookie Header; the worker's original Header remains untouched. State variables
   are removed from the child environment before starting Playwright's driver.
4. The page creates its normal account request. The observer inspects only its
   outer boolean success, using the response already received by the browser.
   Playwright returns the whole response body in memory; this is neither a new
   request nor a download byte cap. No response body/profile is retained.
5. Before closing the context, scoped CoinGlass cookies are collected, including
   supported attributes and new CoinGlass identities. No localStorage, IndexedDB,
   Google state or whole browser profile is collected.
6. A bounded anonymous pipe returns one correlated snapshot to the parent.
   State never enters scanner stdout, result files, diagnostic evidence or job
   records. The pipe is drained while the child runs and is closed on failure.
7. An atomic compare-and-set verifies revision, run ID and `in_use` before saving
   the updated encrypted state. A stale run cannot overwrite a later revision.

`ready` means the exact natural account response reported success and no source
login guard rejected the run; it is not 12H access proof or renewal proof. A Hover
failure alone does not discard a valid update. A later pending/failed account
observation cannot be marked ready. An access guard or explicit rejection marks
the row blocked; this does not diagnose token expiry. Uncertain observations are
unconfirmed. Failed saves/crashes leave a non-ready row, never an automatic old
state fallback. Errors are returned as filtered codes.

There is deliberately no invented refresh endpoint, token conversion, added map
header, keepalive traffic, automatic personal-login loop or synthetic account
flag. No one-alert/operator replacement workflow has been implemented: that is
an emergency fallback requiring a proven need, not completion of automation.

## Focused verification and its limits

`test_session_state.py` exercises real PostgreSQL SQL in the existing PGlite
runtime and real AES-GCM: write/read, SQL-trigger failure, expiry/structure,
tamper/key failure, CAS/lock ownership, private PUBLIC ACLs and database-process
restart. PGlite has one backend, so it does not prove independent-backend lock
contention or production role isolation.

`test_session_runtime.py` uses real OS pipes and Python subprocesses. Its combined
integration test runs the actual encrypted store and pipe together, reloads the
new state, and proves a Hover process error does not discard it. Additional tests
cover missing/mismatched/oversized receipts, failed spawn, a retained writer,
cancellation and cleanup. Test-only state/key values are synthetic.

`test_session_capture.py` covers the actual generation adapters and final cleanup
order. Account responses/page/context/process boundaries are explicitly replaced
by fixtures. This is not a real Playwright or CoinGlass compatibility test.

Run with the existing PGlite package and PyCryptodome available:

```sh
python -B -m unittest -v test_session_state test_session_runtime test_session_capture
```

The optional PGlite suites explicitly skip if the existing package is unavailable;
a skip must not be reported as PostgreSQL verification. No Actions job or source
attempt is added by these files.

## Remaining live boundary

The separate `coinglass-session-restore-20261113-preparation` package and its
source-reader pins remain unchanged. It proves preparation only, not live restore.
The next limited 12H attempt still requires explicit sensitive-transfer consent.
The initial temporary Render variable is a bootstrap test input, not the ongoing
writeback destination. This candidate is not installed by that temporary test.

Production deployment, creation/grants of the private row, its key, and activation
of managed mode remain unapproved. Real 12H access, later-run reuse, and renewal
after expiry are three separate evidence requirements; none is inferred from the
isolated tests or from an immediate personal-browser login.
