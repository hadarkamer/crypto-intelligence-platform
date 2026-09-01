# PREVIEW staging internal preflight Workflow runbook

This runbook defines a later, separately approved, one-time read-only preflight
against the dedicated staging database. Nothing in this document authorizes a
push, Workflow creation, deployment, task run, database migration, candidate
service change, Telegram call, or PREVIEW activation.

## Selected execution path

Use a dedicated Render Workflow named
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
this service to `render.yaml`. Create it only in a later explicitly approved
Dashboard or CLI step.

## Fail-closed configuration sequence

Create and register the Workflow initially with no database URL and with the
preflight flag disabled:

```text
FORMULA_PREVIEW_STAGING_DATABASE_PREFLIGHT=0
FORMULA_PREVIEW_STAGING_DATABASE_URL=<unset>
FORMULA_SCHEMA_APPLY=0
RESEARCH_SCHEMA_APPLY=0
RESEARCH_USE_PRIMARY_DATABASE=0
```

Registration must show exactly one task. Do not trigger it in this state.

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

## Separately approved one-time run

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
candidate_auto_deploy_branch=ai-production-analytics
source_branch_overlap=false
workflow_resource_created=false
workflow_deployed=false
workflow_task_registered=false
workflow_task_runs=0
remote_push_performed=false
render_configuration_changed=false
candidate_service_changed=false
database_connections=0
sql_queries_executed=0
database_writes=0
migration_019_applied=false
telegram_api_calls=0
```
