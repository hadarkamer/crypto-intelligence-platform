# Isolated queue contract checks — 1 November 2026

Chat 2: expansion planning for eight coins, three heatmap models and three
horizons. This experiment covers duplicate requests, competing workers and
late writes after ownership changes. It is not a live scan or capacity test.

## Implementation and isolation

- Candidate: `experiments/work_queue_candidate.py`.
- Integration suite: `experiments/test_work_queue_postgres.py`.
- CI: `.github/workflows/isolated-work-queue-check.yml`, triggered only by
  relevant code pushes to `test/coinglass-concurrency-20261031`.
- Candidate code and corrected fixture: `a51169837ce7262268d4a90d327dfb65ed1d8cf2`.
- [GitHub Actions run](https://github.com/hadarkamer/crypto-intelligence-platform/actions/runs/41292185861).

The candidate uses the **actual generated collector's `JobStore`**, connection
method and `ai_collection_bridge_jobs` table in a disposable PostgreSQL service.
`prepare.py` generates that runtime through the existing adapters and runs the
existing offline contracts. The experiment adds columns and indexes only to the
fixture database. No production migration is supplied or executed.

The candidate rejects connections other than the explicit loopback test fixture.
It is not copied by `prepare.py`, imported by the app, installed in HTTP routes,
or used by the live worker. No existing source file was changed. The workflow has
read-only repository permissions, no deployment step, no source session and no
AI key. The fixture's password is a public disposable test value, not a secret.

Local PostgreSQL could not run under this workspace's process/UID restrictions.
The database checks therefore run in a separate GitHub runner and PostgreSQL
container, not against a simulated database or the user's existing database.
Temporary local dependency downloads were removed.

## Queue rules exercised

1. Insert the deterministic work ID using a database conflict guard. A second
   request reads the committed row; it cannot reset progress or a saved result.
   A conflicting configuration for the same work is rejected.
2. Claim a row atomically with `FOR UPDATE SKIP LOCKED`. Other workers can claim
   sibling jobs. An owner ID, increasing generation and expiry identify each
   assignment; renewal, external-start reservation and completion all check them.
3. Reject expired ownership, including before reassignment. After reassignment,
   the previous generation cannot renew or overwrite result, image, status or
   error, even when the worker UUID is reused.
4. Reclaim an expired job automatically **only before** the external-start
   boundary. Once that boundary is recorded, an expired job remains unavailable
   for automatic reassignment; the outcome requires later reconciliation.

Expiry is forced with a test-only SQL update using the server clock; no long
sleep and no mocked database are used. Concurrent clients use distinct real
connections. A held row-lock test additionally forces contention and proves a
different job can be claimed while that lock is still held.

## Results

**PASS** on PostgreSQL 16.15, 1 November 2026, 09:16:47 UTC (11:16:47 Israel).
All 12 PostgreSQL integration tests and all 17 work-identity tests passed.
The existing build ran 106 + 14 + 122 + 226 offline test executions successfully
(these groups include repeated contracts at different adapter stages).

| Requested scenario | Observed result |
| --- | --- |
| Duplicate requests | 16 concurrent requests returned the same ID; exactly one job persisted. |
| Two competing workers | Exactly one claimed the single job. With two jobs, both workers received different jobs; a held lock on one job did not block its sibling. |
| Stale worker returns | Expired and old-generation renewal/start/completion were rejected. A newer result, image, status and error remained unchanged by late success or failure writes. |

Additional passes: 72 distinct commands survive duplicate submission; a new
batch, tenant or source scope remains distinct; conflicting configuration is
rejected; completed work is not reset; external-start reservation succeeds once
and expiry does not automatically repeat it; a failed sibling leaves successes
saved; wrong-owner and repeated completion are rejected; disconnect rolls back
an uncommitted database change.

The runner's PostgreSQL container and network were removed successfully.
Production branch readback remained `f6adfc6fea05bfd82a59098433ad1c2f31f6267f`.
A read-only health check at 09:09:54 UTC returned HTTP 200, `ok=true`, database
readiness true and worker concurrency 1. This is a point-in-time check, not a
claim of continuous production monitoring.

The first run, `41291812990`, passed the existing offline build and identity
tests, but all queue cases stopped in fixture setup: `TRUNCATE` omitted the
existing evidence table's foreign-key dependency. The correction explicitly
resets that third fixture table too; no queue algorithm or scan code changed.
The fixture health check was also bound to its actual test user/database to
remove irrelevant connection errors from the container log.

## Limits and next step

This is part of test A, the queue protection checks. It does not complete test A
or test B (the full app → queue → source → AI → storage → UI path). The existing
single live worker is unchanged. Legacy workers must not be connected to the
candidate schema: they do not understand the new ownership fence.

The suite uses TESTA–TESTH. Seventy-two distinct queue records do not demonstrate
72 simultaneous captures, source-account isolation, latency, compute capacity,
or an approved real coin list. No AI call is made, and the external-start guard
does not establish exactly-once provider execution. It also does not revoke a
running browser or process. Worker stop/heartbeat behavior remains to integrate.

Next implementation: durable capture/evidence and analysis checkpoints, with
crash-before-AI and failure-after-AI tests, so only the unfinished stage resumes.
Then connect the reviewed components to the repaired scan version in an isolated
full flow, preserving result provenance and app/manual-edit merge protections.
Configuration immutability, authorization, lifecycle/retention, scheduling and
unknown-outcome reconciliation still need production integration and review.
The candidate is not an HTTP API: it does not integrate the current route
authorization, admission/hourly limits, cache policy or evidence-read endpoints.
These existing protections must be preserved or explicitly adapted before any
live connection. Image-write fencing here is tested in the jobs record only;
the existing evidence endpoint and app merge path remain outside this test.

SQL references: [INSERT conflict handling](https://www.postgresql.org/docs/16/sql-insert.html)
and [SELECT row locking](https://www.postgresql.org/docs/16/sql-select.html).
