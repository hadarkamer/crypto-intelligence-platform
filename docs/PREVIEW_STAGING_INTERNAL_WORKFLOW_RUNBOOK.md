# PREVIEW staging internal preflight Workflow runbook

This runbook records a separately approved, one-time read-only preflight
against the dedicated staging database. The isolated Workflow was created,
executed once, safely closed and deleted. Nothing in this document authorizes
recreating it, running another task, applying a database migration, changing
the candidate service, calling Telegram or activating PREVIEW.

## Selected execution path

The selected path used a dedicated Render Workflow named
`preview-staging-readonly-preflight`. It registers exactly one manually
triggered task:

```text
preview_staging_readonly_preflight_once
```

The Workflow is isolated from `crypto-ai-agent-candidate`. The candidate web
service keeps its existing build command, start command, environment and Free
compute plan.

The candidate service has commit-triggered auto-deploy enabled only for
`ai-production-analytics`. The Workflow source branch is deliberately
`preview-staging-preflight-workflow`; never push the Workflow preparation via
the candidate branch.

| Field | Required value |
| --- | --- |
| Repository | The repository containing the approved runner commit |
| Branch | `preview-staging-preflight-workflow` |
| Region | `oregon` |
| Runtime | Python 3 |
| Build command | `pip install -r requirements.preview-staging-workflow.txt` |
| Run command | `python research_preview_staging_readonly_preflight_workflow.py` |
| Auto-deploy | Off |
| Task plan | `flex` |
| Task timeout | 30 seconds |
| Automatic retries | 0 |
| Scheduling | None; manual trigger only |

Render Workflows are not currently compatible with Blueprints, so do not add
this service to `render.yaml`. The resource was created through the CLI under
explicit approval and retains auto-deploy off.

## Fail-closed configuration sequence

The Workflow was created and registered initially with no database URL and
with the preflight flag disabled:

```text
FORMULA_PREVIEW_STAGING_DATABASE_PREFLIGHT=0
FORMULA_PREVIEW_STAGING_DATABASE_URL=<unset>
FORMULA_SCHEMA_APPLY=0
RESEARCH_SCHEMA_APPLY=0
RESEARCH_USE_PRIMARY_DATABASE=0
```

Registration shows exactly one task. Do not trigger it in this state.

Under a later, separately approved configuration step, the dedicated internal
URL was stored directly as a Render secret and the preflight flag was enabled.
The configuration was released from the same approved source commit. No task
run, database connection or SQL statement occurred during configuration.

Under a further explicit execution approval, the task ran exactly once and
returned `READY_FOR_SEPARATE_MIGRATION_019_DECISION`. It opened one connection,
executed one read-only query, rolled the transaction back and performed zero
writes. The preflight flag was then returned to `0`, the database URL was
removed and a closed Workflow version was released from the same commit.

Only after a separate execution approval, add the dedicated database's
**Internal URL** as a secret named
`FORMULA_PREVIEW_STAGING_DATABASE_URL`, then set
`FORMULA_PREVIEW_STAGING_DATABASE_PREFLIGHT=1`. Obtain the value directly from
the Internal tab of `crypto-intelligence-staging-db`; never copy the Production
database URL, use the external hostname, print the URL, or commit it.

The internal target must resolve to all of the following before the task can
open a connection:

```text
Render Postgres ID: dpg-dab7rc2d0e5s73dkb9l0-a
Internal host: dpg-dab7rc2d0e5s73dkb9l0-a
Database: crypto_intelligence_staging_db
Port: 5432 or omitted
```

## Separately approved one-time run — completed

The later execution step must perform these actions in order:

1. Confirm the Workflow's deployed commit is the approved runner commit.
2. Confirm exactly one registered task, the `flex` plan, 30-second timeout and
   zero retries.
3. Confirm the dedicated staging database is available and has no external IP
   allow-list entries.
4. Confirm the two preflight environment values are present only on the
   isolated Workflow and all three schema-apply flags are disabled.
5. Manually trigger `preview_staging_readonly_preflight_once` exactly once.
   The task has no user arguments, so its CLI input is the empty JSON array
   `[]`.
6. Wait for a terminal result and record only the safe JSON result. Never copy
   environment values or connection strings into logs or the repository.
7. Set the preflight flag back to `0`, remove the database URL, and verify the
   secret is absent.
8. Delete the temporary Workflow after its logs and result have been reviewed.

All eight actions were completed. Deletion was accepted by Render and verified
both by a `404` lookup for the recorded Workflow ID and by an empty Workflow
list. The dedicated staging database was not deleted.

There is no automatic retry after an infrastructure failure, timeout or
uncertain result. A second trigger requires a new explicit approval.

## Allowed result boundary

A successful result may only report either readiness for a later migration
decision or a blocker. It must retain all of these properties:

```text
schema_mutation_allowed=false
migration_apply_allowed=false
candidate_service_connected=false
runtime_registered=false
delivery_allowed=false
telegram_api_calls=0
database_writes=0
stage6_activated=false
research_evidence_effect=NONE
live_effect=NONE
```

`READY_FOR_SEPARATE_MIGRATION_019_DECISION` is not authority to apply migration
`019`. Migration remains a later, independently approved step.

## Rejected execution paths

| Path | Reason rejected |
| --- | --- |
| Existing candidate Web Service | Changing its start command or runtime would affect the laboratory bot lifecycle. |
| One-off Job based on candidate | Render does not support one-off jobs for a Free web service. |
| Background worker | The runner exits; a continuous worker can restart it and create an unintended loop. |
| Cron Job | It requires a schedule and has a minimum monthly service charge, creating avoidable recurrence and cost. |
| External database connection | The isolated client cannot resolve Render's external database hostname, and external access is closed again. |

## Current state

```text
workflow_entrypoint_prepared=true
workflow_requirements_isolated=true
workflow_auto_deploy=false
workflow_source_branch=preview-staging-preflight-workflow
workflow_source_commit=120c49d3d7c085da44535b207d61ef64e0982ea5
workflow_id=wfl-dab91ek9v7es73ce1pm0
workflow_version_id=wfv-dab99rvavr4c73f74u1g
workflow_task_id=tsk-dab9ai0put7g008coa20
workflow_execution_version_id=wfv-dab96l3tqb8s73f72jd0
workflow_execution_task_id=tsk-dab96u0put7g008coa1g
workflow_execution_run_id=trn-08l4gdab992favr4c73f72smg
candidate_auto_deploy_branch=ai-production-analytics
source_branch_overlap=false
remote_history_mode=GITHUB_API_SQUASHED_SNAPSHOT
local_commit_history_preserved=true
workflow_resource_created=true
workflow_deployed=false
workflow_ever_deployed=true
workflow_task_registered=false
workflow_task_was_registered=true
workflow_task_runs=1
workflow_execution_attempts=1
workflow_execution_retries=0
workflow_execution_status=completed
workflow_result=READY_FOR_SEPARATE_MIGRATION_019_DECISION
workflow_current_version_task_runs=0
workflow_database_url_configured=false
workflow_database_url_committed=false
workflow_preflight_enabled=false
workflow_schema_apply_flags_disabled=true
workflow_cleanup_completed=true
workflow_resource_present=false
workflow_resource_deleted=true
workflow_deletion_verified_404=true
remote_push_performed=true
render_configuration_changed=true
candidate_service_changed=false
database_connections=1
read_only_queries_executed=1
transaction_rolled_back=true
database_writes=0
migration_019_objects_present=false
migration_019_applied=false
telegram_api_calls=0
```
