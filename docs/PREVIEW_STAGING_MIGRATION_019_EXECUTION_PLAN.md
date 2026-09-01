# PREVIEW staging migration 019 execution plan

Status: `RUNBOOK_READY_POSTCOMMIT_VERIFIER_REQUIRED_APPLY_FORBIDDEN`

This document records the final static audit and the bounded execution plan for
`migrations/019_preview_first_message_reservation_consumption_v1.sql`. It does
not authorize a database connection, SQL execution, migration application,
runtime persistence, candidate-service configuration, deployment or Telegram
delivery.

## Exact target and source

| Field | Required value |
| --- | --- |
| Render Postgres ID | `dpg-dab7rc2d0e5s73dkb9l0-a` |
| Render name | `crypto-intelligence-staging-db` |
| Database name | `crypto_intelligence_staging_db` |
| PostgreSQL major version | `18` |
| Migration | `migrations/019_preview_first_message_reservation_consumption_v1.sql` |
| SHA-256 | `81690a298a029b3bb131f7906e496d17748dbfb32124d87f438882e14e7e9c05` |

The database URL is a secret and must never be committed, printed or included
in an execution receipt. Only Render's internal URL for the exact target above
may be accepted by the future runner. The shared Production database and every
external Render hostname must be rejected before a connection attempt.

## Audit conclusion

Migration 019 is additive and self-contained. It does not reference an object
from migrations 001-018. Its complete intended object set is:

- two append-only tables:
  `research_preview_first_message_reservations` and
  `research_preview_first_message_consumptions`;
- two trigger functions:
  `validate_preview_first_message_consumption()` and
  `prevent_preview_first_message_storage_mutation()`;
- five triggers: one exact/current-reservation validator and four mutation or
  truncate blockers;
- the reservation-to-consumption composite foreign key and the declared
  primary-key, unique and check constraints.

The consumption table binds a confirmed delivery to the exact reservation and
requires the confirmation timestamp to fall inside the reservation window. The
tables reject update, delete and truncate operations. The migration itself
does not insert application data or grant runtime authority.

The existing `research_formula_schema_admin.py` is explicitly rejected for
this staging operation. Its `MIGRATION_PATHS` contains 18 migrations (001-017
and 019), so using it on the dedicated database would exceed the authorized
019-only scope. It also accepts any non-empty `RESEARCH_DATABASE_URL` without
pinning the exact staging host and database identity. Its broad
`FORMULA_SCHEMA_APPLY` switch is not a suitable authorization boundary for
this operation.

## Implemented local installer and remaining boundary

The isolated runner now exists locally as
`research_preview_staging_migration_019_admin.py`, with a dedicated self-test.
Application remains forbidden: the runner is not registered on Render, has no
configured database URL and has never opened a database connection. Its
implementation satisfies these requirements:

1. It is not imported by `ai_candidate_main.py`, the dispatcher, a handler,
   scheduler or worker.
2. It accepts only a dedicated secret such as
   `FORMULA_PREVIEW_STAGING_DATABASE_URL`; it never falls back to
   `DATABASE_URL` or `RESEARCH_DATABASE_URL`.
3. It requires a new exact one-shot switch such as
   `FORMULA_PREVIEW_STAGING_MIGRATION_019_APPLY=1` and refuses execution if any
   generic schema-apply or primary-database override is enabled.
4. Before connecting, it parses the URL without logging it and pins the exact
   internal host and database name supplied by verified Render metadata. It
   rejects the Production database, external endpoints and target ambiguity.
5. It verifies the migration file path and exact SHA-256 above before opening a
   connection. It executes that one file only.
6. It uses one connection and one explicit transaction, takes the same schema
   advisory transaction lock used by the existing schema administrator, sets
   finite lock and statement timeouts, and sets the local search path to
   `public, pg_catalog` before executing any migration DDL.
7. Under the lock and immediately before DDL, it verifies that all two tables,
   two functions and five trigger names are absent. Any complete or partial
   prior state is a blocker; the runner must not repair, replace or drop it.
8. It verifies the complete expected catalog state inside the same transaction,
   including table placement in `public`, constraint bindings, function and
   trigger placement, trigger enablement and the five trigger-to-table/action
   mappings. A mismatch raises an error and rolls back.
9. It commits exactly once only after every verification succeeds. Every
   exception before that point rolls back and reports a non-secret failure
   classification.
10. It emits a non-secret receipt containing the target IDs, migration
    checksum, precondition result, verified object set and commit result. It
    must never emit the database URL, password or raw connection parameters.

The current migration uses unqualified object names. The future runner's fixed
transaction-local search path is therefore a required defense, not an optional
convenience. Schema qualification may instead be added directly to the SQL,
but doing so changes the pinned checksum and requires this audit to be updated
before application.

## Local Workflow entry point

`research_preview_staging_migration_019_workflow.py` now registers exactly one
task around the dedicated installer. The task accepts no user input beyond the
SDK-supplied `TaskContext`, reads only its own process environment, has a
60-second timeout and explicitly configures zero automatic retries. It exposes
no Render client, scheduler, cron trigger or candidate-service integration.

The installed `render==1.0.1` SDK registry verifies this exact local state:

```text
workflow_name=preview-staging-migration-019
task_name=preview_staging_migration_019_once
task_count=1
task_plan=flex
task_timeout_seconds=60
task_max_retries=0
task_retry_wait_ms=1000
task_retry_backoff_scaling=1.0
task_external_arguments=0
sdk_registry_verified=true
workflow_resource_created=false
workflow_task_registered_on_render=false
workflow_runs=0
```

The standalone Render CLI required for a local dev-server task listing is not
present in the current temporary environment. No CLI was installed or replaced
for this check. Direct inspection of the installed SDK registry and task
options passed without executing the task.

## Separately authorized application sequence

The dedicated runner and its self-test are now prepared locally. Actual
application still requires a new explicit approval and a separate, zero-retry
Render-internal execution boundary. The approved execution sequence is:

1. Re-read the Render database identity and status; require the exact target to
   be `available` and keep its external allow list empty.
2. Prepare a temporary manual Render-internal one-shot execution surface with
   zero automatic retries. Do not attach the candidate service.
3. Configure only the exact internal database URL and the dedicated 019 switch.
   Keep every generic schema-apply flag disabled.
4. Run exactly once. Do not retry an uncertain result automatically.
5. Require the non-secret receipt to report the pinned checksum, clean
   precondition, exact verified object set and committed transaction.
6. Immediately disable the apply switch, remove the database URL and release a
   closed Workflow version; do not wait for another approval to close it.
7. Perform one separately bounded read-only post-commit verification of the
   catalog. This is verification only and must not write application rows.
8. Delete the closed temporary Workflow only after reconciliation is recorded.
9. Keep the candidate service disconnected. Runtime database registration and
   the first PREVIEW delivery remain later, separately approved decisions.

## Rollback and uncertain-result boundary

- Before commit, any error must roll back the entire transaction. No cleanup
  DDL or second attempt is allowed automatically.
- If the execution result is uncertain, first inspect the catalog in a
  read-only transaction. Do not rerun the migration based on missing logs.
- After a verified commit and before any runtime row exists, removal would
  require a separately reviewed and approved compensating migration. It must
  first prove both tables are empty and have no unexpected dependents, then
  drop the consumption table before the reservation table and finally remove
  the two functions in one transaction.
- If either table contains a row, destructive rollback is forbidden. Freeze
  future runtime work and reconcile the append-only records manually.

No compensating SQL is authorized or executed by this plan.

## Evidence available before implementation

The completed Render-internal read-only preflight returned
`READY_FOR_SEPARATE_MIGRATION_019_DECISION`. It observed the exact dedicated
database, a read-only transaction, the required `public`/PL/pgSQL prerequisites
and absence of all migration-019 objects. That observation supports this plan
but is not application authority and must be repeated under the migration lock
immediately before future DDL.

All 51 tracked self-tests pass in isolated dependency scopes: 49 core tests use
only `requirements.txt`, while both Workflow self-tests run in a clean venv
using `requirements.preview-staging-workflow.txt`. The core environment
was also verified not to contain the Render SDK. Every tracked Python module
compiles. No disposable PostgreSQL server is available in the current
workspace, so this audit does not claim a PostgreSQL execution dry-run.

The GitHub verification job now preserves the Production dependency pins and
routes only files ending in `_workflow_selftest.py` to the temporary Workflow
venv. This also provides the correct isolated route for the future
migration-019 Workflow self-test without merging incompatible `aiohttp` pins.

## Current state

```text
migration_019_plan_ready=true
migration_019_dedicated_installer_prepared=true
migration_019_dedicated_installer_executed=false
migration_019_workflow_entrypoint_prepared=true
migration_019_workflow_runbook_prepared=true
migration_019_workflow_resource_created=false
migration_019_workflow_task_registered_on_render=false
migration_019_workflow_runs=0
migration_019_postcommit_readonly_verifier_prepared=false
migration_019_apply_authorized=false
migration_019_applied=false
candidate_service_connected=false
runtime_database_registered=false
database_connections_this_step=0
sql_queries_executed_this_step=0
database_writes_this_step=0
render_configuration_changed_this_step=false
candidate_service_changed_this_step=false
telegram_api_calls_this_step=0
selftests_passed=51
core_selftests_passed=49
isolated_workflow_selftests_passed=2
workflow_dependency_scope_isolated=true
project_compilation_passed=true
remote_ci_ready=true
remote_push_performed=false
research_evidence_effect=NONE
live_effect=NONE
```

The next bounded step is local implementation and self-testing of a dedicated
read-only post-commit/uncertain-result verifier. It must not push a commit,
create a Render resource, configure a database URL, connect to PostgreSQL or
apply the migration.
