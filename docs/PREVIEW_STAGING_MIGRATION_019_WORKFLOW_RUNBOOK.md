# PREVIEW staging migration 019 Workflow runbook

Status: `ROTATED_USER_SOURCE_PUBLICATION_PENDING_WORKFLOW_REDEPLOY_REQUIRED_APPLY_FORBIDDEN`

This runbook defines a separately approved application of migration 019 to the
dedicated PREVIEW staging database. The closed Workflow resource and its task
are now registered as recorded below. The remaining instructions do not
authorize a write-capable configuration, task run, database connection, SQL
statement or migration.

## Fixed source and target

The registered Workflow remains closed on the previously verified commit. A
new source snapshot pins the separately scoped rotation user below and is
pending exact publication. It must never be moved to either Production
deployment branch or used to recreate the deleted read-only preflight Workflow.

| Field | Required value |
| --- | --- |
| Repository | Repository containing the approved source commit |
| Dedicated branch | `preview-staging-migration-019` |
| Currently deployed remote commit | `861554d914f21089457d5ee91147260d050efddb` |
| Currently deployed source tree | `14eb4dd1106f398284a363b6feed4e37206171ec` |
| Rotated-user remote commit | `PENDING_EXACT_TREE_PUBLICATION` |
| Rotated-user audited local commit | `PENDING_LOCAL_COMMIT` |
| Rotated-user identical source tree | `PENDING_LOCAL_COMMIT` |
| Workflow name | `preview-staging-migration-019` |
| Workflow entry point | `research_preview_staging_migration_019_workflow.py` |
| Registered task | `preview_staging_migration_019_once` |
| Render Postgres ID | `dpg-dab7rc2d0e5s73dkb9l0-a` |
| Database name | `crypto_intelligence_staging_db` |
| Rotated database user | `crypto_intelligence_staging_migration_019` |
| Migration SHA-256 | `81690a298a029b3bb131f7906e496d17748dbfb32124d87f438882e14e7e9c05` |

If any source or migration byte changes, stop. Do not update the commit or hash
inside an execution step; repeat the local audit and obtain a new approval.

## Exact Workflow creation fields

Render Workflows are not Blueprint-compatible. The resource was created through
the Render Workflow flow under a separate approval with these fields:

| Field | Required value |
| --- | --- |
| Service type | Workflow |
| Name | `preview-staging-migration-019` |
| Branch | `preview-staging-migration-019` |
| Commit | Exact approved code commit above |
| Root directory | Repository root / empty |
| Region | `oregon` |
| Runtime | Python 3 |
| Build command | `pip install -r requirements.preview-staging-workflow.txt` |
| Run command | `python research_preview_staging_migration_019_workflow.py` |
| Auto-deploy | Off |
| Task count | Exactly 1 |
| Task plan | `flex` |
| Task timeout | 60 seconds |
| Automatic retries | 0 |
| Scheduling | None; manual trigger only |

Initial creation must register the task in a closed configuration. Creation is
not application authority and must not be combined with the write-capable
configuration or task trigger.

## Closed initial configuration

Use service-level variables only. Do not link an environment group, candidate
service or shared Production configuration. Render stores all values as
strings, so use the exact string values below:

```text
FORMULA_PREVIEW_STAGING_MIGRATION_019_APPLY=0
FORMULA_PREVIEW_STAGING_DATABASE_URL=<unset>
FORMULA_PREVIEW_STAGING_DATABASE_PREFLIGHT=0
FORMULA_SCHEMA_APPLY=0
RESEARCH_SCHEMA_APPLY=0
RESEARCH_USE_PRIMARY_DATABASE=0
DATABASE_URL=<unset>
RESEARCH_DATABASE_URL=<unset>
```

The database URL must never be committed, pasted into a run input, printed in
logs or inherited from an environment group. It may later be stored only as a
service-level secret on this temporary Workflow.

## Separate authorization gates

The operation has four boundaries:

1. Push the exact approved code commit to the new dedicated branch.
2. Create and register the closed Workflow with no database URL.
3. Configure and release one write-capable Workflow version.
4. Manually trigger exactly one task run and immediately close the
   write-capable configuration.

The original boundaries 1 and 2 are complete for the closed, currently
deployed version. Rotated-user source publication and a closed redeploy must
now complete before boundary 3 can be considered. Boundaries 3 and 4 remain
unauthorized.

Each of the first three boundaries requires a separate explicit approval. The
fourth approval must authorize both the single trigger and its mandatory
fail-safe closure. Do not pause for another approval between the terminal run
state and setting the apply flag back to `0` and removing the database URL.

No approval for these boundaries authorizes candidate-service integration,
runtime persistence, a Telegram call, PREVIEW delivery, Stage 6, research
evidence or LIVE behavior.

## Write-capable configuration — future approval only

Obtain the Internal URL directly from
`crypto-intelligence-staging-db`. Before saving it, validate without logging:

```text
scheme=postgresql or postgres
host=dpg-dab7rc2d0e5s73dkb9l0-a
port=5432 or omitted
database=crypto_intelligence_staging_db
user=crypto_intelligence_staging_migration_019
password=present
query=<absent>
fragment=<absent>
```

Set only:

```text
FORMULA_PREVIEW_STAGING_DATABASE_URL=<exact dedicated Internal URL secret>
FORMULA_PREVIEW_STAGING_MIGRATION_019_APPLY=1
```

Keep the preflight and all generic schema-apply flags at `0`; keep generic
database URLs unset. Release the configuration from the same approved source
commit with auto-deploy still off. Configuration must not trigger the task.

## Pre-trigger checklist

All checks must pass immediately before the single manual trigger:

- Workflow name, source branch and deployed commit match the fixed values.
- Exactly one task is registered with the exact name, `flex` plan, 60-second
  timeout, zero retries and no task arguments.
- There are zero earlier runs for this Workflow and no run is queued or active.
- The dedicated database is `available`, PostgreSQL major version 18, and its
  external IP allow list is empty.
- The dedicated Internal URL secret is configured only on this Workflow.
- The dedicated apply flag is exactly `1`; the preflight and generic mutation
  flags are exactly `0`; both generic database URL variables are absent.
- Migration file SHA-256 equals the fixed hash.
- Candidate service and Production service/database fingerprints are unchanged.
- A separate post-commit/uncertain-result read-only verifier is already
  prepared and tested. The current absence-check preflight is not sufficient.
- The approval explicitly covers one trigger plus immediate fail-safe closure.

Any mismatch blocks the trigger. Do not correct a mismatch and continue within
the same execution approval.

## One-time run and mandatory closure

The future execution must follow this order without automatic retry:

1. Manually trigger `preview_staging_migration_019_once` exactly once. Its
   input is the empty JSON array `[]`; it accepts no user arguments.
2. Record the Workflow/version/task/run IDs, exact deployed commit, attempt
   count and retry count. Never record the environment values.
3. Wait for one terminal state. Render's `completed` state is not proof of
   migration success; inspect the returned safe JSON status.
4. Immediately set `FORMULA_PREVIEW_STAGING_MIGRATION_019_APPLY=0`, remove
   `FORMULA_PREVIEW_STAGING_DATABASE_URL`, and release the closed configuration
   from the same commit. This closure is mandatory for success, failure,
   timeout, missing result and uncertain commit alike.
5. Verify the current Workflow version has the apply flag at `0`, no database
   URL, all generic flags disabled, zero queued/active runs and zero automatic
   retries.
6. Run the separately approved read-only post-commit verifier before any
   deletion, second trigger, runtime registration or persistence decision.

A second trigger is forbidden unless reconciliation proves the first attempt
did not commit and a new explicit execution approval is granted.

## Exact success receipt

A successful task result must be JSON-serializable, contain no secret and
match every value below. The receipt is an unsigned operational record stored
with the Workflow run.

```text
status=MIGRATION_019_APPLIED_AND_VERIFIED
mode=ONE_SHOT_RENDER_INTERNAL_MIGRATION_019_ONLY
render_postgres_id=dpg-dab7rc2d0e5s73dkb9l0-a
database_name=crypto_intelligence_staging_db
database_user_verified=true
internal_target_verified=true
migration_filename=019_preview_first_message_reservation_consumption_v1.sql
migration_sha256=81690a298a029b3bb131f7906e496d17748dbfb32124d87f438882e14e7e9c05
precondition_clean=true
preexisting_object_count=0
catalog_verification_passed=true
verified_table_count=2
verified_function_count=2
verified_trigger_count=5
verified_schema_object_count=9
verified_constraint_bindings=true
database_connections=1
transactions_started=1
migration_files_executed=1
commit_attempts=1
transaction_committed=true
schema_mutation_committed=true
migration_019_applied=true
application_rows_written=0
automatic_retry_allowed=false
candidate_service_connected=false
runtime_database_registered=false
handler_registered=false
scheduler_registered=false
worker_registered=false
dispatch_allowed=false
delivery_allowed=false
telegram_api_calls=0
stage6_activated=false
research_evidence_effect=NONE
live_effect=NONE
```

Even this complete receipt is provisional until the independent read-only
post-commit verifier confirms the catalog.

## Failure and uncertain-result matrix

| Observed result | Classification | Required action |
| --- | --- | --- |
| `MIGRATION_019_FAILED_CLOSED` | No commit was reported | Close configuration immediately; run read-only verification; do not retry. |
| `MIGRATION_019_COMMIT_OUTCOME_UNCERTAIN` | Commit may or may not have reached PostgreSQL | Close immediately; require manual read-only reconciliation; do not retry or run compensating DDL. |
| Timeout, infrastructure failure, missing/malformed JSON | Outcome uncertain regardless of Render run label | Close immediately and use the same manual reconciliation path. |
| Success JSON missing any exact field | Receipt invalid | Treat as uncertain, close and reconcile read-only. |
| Success JSON plus independent catalog match | Applied and verified | Keep runtime disconnected; proceed only to cleanup and a later persistence decision. |

Task-level automatic retries are zero. A Render run showing one attempt is
required. A second attempt or retry count above zero invalidates the execution
record and requires reconciliation.

## Prepared read-only verifier

The existing `research_preview_staging_readonly_preflight.py` proves absence
before migration and intentionally blocks when migration objects exist. It does
not verify every post-commit constraint or trigger mapping and therefore cannot
serve as the uncertain-result reconciler. A dedicated replacement is now
prepared locally:

| Field | Required value |
| --- | --- |
| Source commit | `PENDING_EXACT_TREE_PUBLICATION` |
| Runner | `research_preview_staging_migration_019_readonly_verifier.py` |
| Workflow entry point | `research_preview_staging_migration_019_readonly_verifier_workflow.py` |
| Workflow name | `preview-staging-migration-019-readonly-verifier` |
| Task name | `preview_staging_migration_019_readonly_verify_once` |
| Plan | `flex` |
| Timeout | 30 seconds |
| Automatic retries | 0 |
| Trigger | Manual only, empty input `[]` |

The verifier:

- pins the same exact internal target and rejects Production/external URLs;
- forces a read-only session and explicit read-only transaction;
- verifies the two tables, two functions, five trigger mappings, constraint
  bindings and zero application rows;
- classifies one repeatable-read snapshot as `APPLIED_VERIFIED`, `NOT_APPLIED`, or
  `PARTIAL_OR_CONFLICTING`;
- always rolls back, performs zero writes and exposes no secret;
- has its own manual, zero-retry Workflow boundary and remains disconnected
  from the candidate service.

Its future service-level configuration must set only
`FORMULA_PREVIEW_STAGING_MIGRATION_019_VERIFY=1` and the exact dedicated
internal database URL. The migration apply flag, preflight flag and all generic
mutation flags must remain `0`; generic database URLs must remain absent. After
one result, remove the database URL, set the verifier flag to `0`, release the
closed version and delete the temporary verifier Workflow after the result is
recorded.

All 53 local tests pass: 50 core tests and three isolated Workflow tests. This
preparation still does not authorize Workflow creation, configuration,
database connection or migration application.

## Final cleanup after reconciliation

After the safe receipt and independent catalog result are recorded:

1. Reconfirm the migration Workflow has no database URL, apply flag `0`, zero
   active/queued runs and auto-deploy off.
2. Delete the temporary migration Workflow. Do not delete the staging database.
3. Verify the Workflow ID returns `404` and that no Workflow with its ID or name
   remains in the workspace list.
4. Confirm the staging database remains `available` with an empty external IP
   allow list.
5. Confirm candidate and Production services, databases and Telegram state are
   unchanged.

If deletion fails, keep the configuration closed and stop. Do not trigger or
reconfigure the resource while cleanup is unresolved.

## Rejected paths

| Path | Reason rejected |
| --- | --- |
| Broad `research_formula_schema_admin.py` | Applies 18 migrations and does not pin the exact target. |
| Candidate Web Service | Would mix schema mutation with the laboratory Bot lifecycle. |
| Deleted read-only preflight Workflow | Its task and result contract are absence/read-only only. |
| Background worker or cron | Can restart or recur and violate the one-shot boundary. |
| Blueprint or environment group | Workflows are not Blueprint-compatible; shared secrets create target ambiguity. |
| External database URL or IP allow-list change | Internal execution is available and external access must stay closed. |
| Automatic retry after any failure | Can apply or collide with an already committed first attempt. |

## Current state

```text
runbook_ready=true
approved_remote_commit=PENDING_EXACT_TREE_PUBLICATION
audited_local_commit=PENDING_LOCAL_COMMIT
approved_source_tree=PENDING_LOCAL_COMMIT
remote_parent_commit=861554d914f21089457d5ee91147260d050efddb
dedicated_remote_branch_created=true
remote_source_published=false
remote_source_transport=GITHUB_GIT_DATA_API
direct_git_push_performed=false
rotated_database_user=crypto_intelligence_staging_migration_019
credential_rotation_source_ready=true
new_database_credential_created=false
old_database_credential_revoked=false
workflow_id=wfl-dabaq0740ujc73abpcg0
workflow_version_id=wfv-dabaq0740ujc73abpct0
workflow_task_id=tsk-dabaqsqj0c7s738afm0g
workflow_build_succeeded=true
workflow_resource_created=true
workflow_task_registered_on_render=true
workflow_task_count=1
workflow_task_plan=flex
workflow_task_timeout_seconds=60
workflow_task_max_retries=0
workflow_auto_deploy=false
workflow_schedule_configured=false
workflow_deployed_source_commit=861554d914f21089457d5ee91147260d050efddb
workflow_source_redeploy_required=true
workflow_database_url_configured=false
workflow_apply_enabled=false
workflow_runs=0
database_external_allowlist_empty=true
workspace_wide_allow_rule_present=true
database_specific_allow_rule_count=0
database_specific_temporary_allow_rule_present=false
effective_external_traffic_allowed=false
network_reconciliation_completed=true
network_reconciliation_required=false
postcommit_readonly_verifier_prepared=true
postcommit_verifier_workflow_resource_created=false
postcommit_verifier_workflow_runs=0
migration_019_apply_authorized=false
migration_019_applied=false
database_connections_this_step=0
sql_queries_executed_this_step=0
database_writes_this_step=0
render_configuration_changed_this_step=true
render_network_configuration_changed_this_step=false
candidate_service_changed_this_step=false
telegram_api_calls_this_step=0
research_evidence_effect=NONE
live_effect=NONE
```

The closed Workflow exists and the database-specific external allow list is
empty. Render's API and Dashboard independently confirm that external traffic
to the staging database is blocked; the workspace-wide allow rule does not
override the empty PostgreSQL rule set. The next bounded step is exact source
publication only. A later separate step must redeploy the Workflow closed from
that commit, with no database URL and apply flag `0`, before credential rotation
or any write-capable configuration can be considered.
