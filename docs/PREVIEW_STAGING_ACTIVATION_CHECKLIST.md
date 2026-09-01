# PREVIEW_ONLY staging activation checklist

This checklist prepares the private experimental Telegram route without
activating delivery, Stage 6, public opt-in, research evidence, or any LIVE
effect.

## Fixed staging target

| Field | Value |
| --- | --- |
| Render service | `crypto-ai-agent-candidate` |
| Service ID | `srv-da3bd1lg1s2s73d867qg` |
| Branch | `ai-production-analytics` |
| Start command | `python ai_candidate_main.py` |

Do not apply these settings to either production service.

## Safe configuration-only state

Use the Render Dashboard and choose **Save only**. Do not rebuild, deploy, or
restart the service during this step.

Set only these non-secret safety controls:

```text
FORMULA_PREVIEW_STAGING_ENABLED=0
FORMULA_PREVIEW_STAGING_KILL_SWITCH=1
FORMULA_PREVIEW_STAGING_OWNER_APPROVED=0
```

Leave the following values unset until their exact approved values are known:

```text
FORMULA_PREVIEW_STAGING_TEST_CHAT_ID
FORMULA_PREVIEW_STAGING_RUNTIME_COMMIT
FORMULA_PREVIEW_STAGING_ACTIVATION_APPROVAL_ID
```

Never invent, guess, or copy a production chat ID into the staging test-chat
field.

## Values required before any later activation decision

| Environment variable | Required value | Validation |
| --- | --- | --- |
| `FORMULA_PREVIEW_STAGING_TEST_CHAT_ID` | Exact private staging chat ID | Non-zero integer |
| `FORMULA_PREVIEW_STAGING_RUNTIME_COMMIT` | Commit actually deployed to the staging service | 40 lowercase hexadecimal characters |
| `FORMULA_PREVIEW_STAGING_ACTIVATION_APPROVAL_ID` | Separately approved activation record ID | 64 lowercase hexadecimal characters |

These values only complete prerequisites. The current code contract is
`CONFIGURE_ONLY_ACTIVATION_FORBIDDEN`, so even a complete configuration keeps
`effective_enabled`, connector registration, and delivery forced off.

## Verification after a separately approved deployment

The `/health` response must report a `preview_staging` object with all of these
safety properties:

```text
effective_enabled=false
kill_switch_engaged=true
connector_registration_allowed=false
delivery_allowed=false
public_opt_in=false
stage6_activated=false
research_evidence_effect=NONE
live_effect=NONE
```

The health response may expose whether a test chat is configured and its hash,
but it must not expose the raw chat ID or activation approval ID.

## Separate authorization boundary

Changing the configuration-only state does not authorize a deployment, a Bot
API call, a Telegram message, activation, or Stage 6. Each of those requires a
later explicit decision. The Stage 5 READY/WAITING_DATA check remains independent
and cannot activate this route automatically.

Before requesting that later decision, prepare and fingerprint an activation
candidate with `research_experimental_preview_activation_record.py`. The
candidate must report all of the following:

```text
status=PREPARED_NOT_APPROVED
scope=PRIVATE_TEST_CHAT_PREVIEW_ONLY
route=TEST_ALLOWLIST
test_chat_count=1
approval_granted=false
activation_approval_id=null
connector_registration_allowed=false
delivery_allowed=false
```

An activation candidate id is not an activation approval id. Do not copy it to
Render. The approval record must be created in a later, explicit step and must
bind the candidate, the exact commit actually intended for deployment and the
same private-chat fingerprint.

## Local activation-gate preparation

Before any connector is registered, the pure local activation gate must verify
the content-addressed approval record against the actual deployed commit and the
same private-chat hash. Separate authorization fields are required for Render
configuration, deployment, connector registration, Telegram dispatch and the
first Preview message, as well as a separate kill-switch release authorization.
An earlier approval that leaves any of those fields false cannot be reused as
delivery authority.

The local gate remains unregistered and therefore must report all of the
following even when its hypothetical prerequisites are complete:

```text
registration_required=true
activation_allowed=false
connector_registration_allowed=false
delivery_allowed=false
telegram_api_calls=0
stage6_activated=false
research_evidence_effect=NONE
live_effect=NONE
```

The dedicated candidate health may observe the gate using Render's
`RENDER_GIT_COMMIT` metadata and the optional
`FORMULA_PREVIEW_STAGING_ACTIVATION_APPROVAL_RECORD_JSON` value. The observation
must expose only match/readiness booleans and blockers—never the raw approval id,
private chat id or approval record. Leaving the record unset is the default and
must fail closed without preventing the read-only candidate service from
starting.

## Runtime connector candidate

The staging registration may consume the fingerprint-verified observe-only
status and prepare a connector candidate id. An exact, unchanged candidate may
then transition idempotently to `RUNTIME_BOT_REGISTERED_NO_DISPATCH`. This is
connector lifecycle registration only: activation remains false, no dispatch
method is exposed, and the dispatcher rejects the registered runtime Bot before
any `send_message` call. In the current safe Render state even the candidate
must remain unprepared—and the connector unregistered—because the feature flag
is off, the kill switch is engaged and the action approval record is absent.

```text
current_render_connector_registered=false
hypothetical_verified_classification=RUNTIME_BOT_REGISTERED_NO_DISPATCH
activation_allowed=false
dispatch_exposed=false
handler_registered=false
scheduler_registered=false
worker_registered=false
telegram_api_calls=0
```

## First-message one-shot authorization candidate

After a fingerprint-verifiable `RUNTIME_BOT_REGISTERED_NO_DISPATCH` receipt
exists, a separate pure contract may bind exactly one prepared request to that
registration, the unchanged activation gate, the private-chat hash and the
message hash. The content-addressed one-shot key must reject replay and must not
expose the raw chat id or message text.

This step prepares a candidate only. Its candidate id is not an authorization
id and cannot be copied into configuration. The module remains disconnected
from `ai_candidate_main.py`, every production surface and the dispatcher.

```text
status=PREPARED_NOT_AUTHORIZED
authorization_granted=false
authorization_consumed=false
dispatch_allowed=false
delivery_allowed=false
handler_registered=false
scheduler_registered=false
worker_registered=false
telegram_api_calls=0
stage6_activated=false
research_evidence_effect=NONE
live_effect=NONE
```

## Separate owner approval record

Preparing the one-shot candidate is not owner approval. A separate approval
record must verify the complete candidate, identify `Hadar Kamar` in the
`OWNER` role, include an explicit statement, authorize exactly one first
PREVIEW message, and expire no more than 15 minutes after its approval time.
Its fingerprint must differ from the candidate id.

The record may never authorize production, public opt-in, Stage 6, research
evidence or LIVE. A valid record still has no runtime authority until a later,
separate application boundary verifies that it is current and unconsumed.

```text
status=APPROVED_NOT_APPLIED
owner_approval_verified=true
approval_applied=false
authorization_consumed=false
dispatch_allowed=false
delivery_allowed=false
handler_registered=false
scheduler_registered=false
worker_registered=false
telegram_api_calls=0
research_evidence_effect=NONE
live_effect=NONE
```

## Observe-only application gate

A later local gate may verify the full candidate and owner-approval record at
an explicit UTC observation time. It must reject times before approval, the
expiry timestamp and every later time. It must also check both the owner
approval id and the one-shot key against previously consumed ids.

Readiness is not application authority. Even a complete result remains signed
as `READY_NOT_APPLIED` and stays disconnected from the candidate service and
dispatcher. Persistent consumption and atomic application are still absent.

```text
status=READY_NOT_APPLIED
approval_current=true
owner_approval_unconsumed=true
one_shot_unconsumed=true
application_prerequisites_satisfied=true
application_allowed=false
approval_applied=false
authorization_consumed=false
dispatch_allowed=false
delivery_allowed=false
handler_registered=false
scheduler_registered=false
worker_registered=false
telegram_api_calls=0
research_evidence_effect=NONE
live_effect=NONE
```

## Reservation and consumption contract

The next pure contract may prepare two append-only candidates but may not write
them. A reservation candidate must bind the ready application gate, approval,
one-shot key and exact request. Future persistence must atomically claim unique
owner-approval and one-shot keys before any dispatch attempt.

A consumption candidate requires both an atomically persisted reservation and
a confirmed delivery receipt inside the approval window. Duplicate consumption
keys are blocked. An uncertain delivery result must keep the reservation closed,
prepare no consumption candidate, forbid automatic retry and require manual
reconciliation.

```text
reservation_status=RESERVATION_PREPARED_NOT_PERSISTED
consumption_status=CONSUMPTION_PREPARED_NOT_PERSISTED
atomic_persistence_required=true
append_only_required=true
automatic_retry_after_uncertain_allowed=false
persistence_applied=false
reservation_persisted=false
consumption_persisted=false
authorization_consumed=false
dispatch_allowed=false
database_writes=0
telegram_api_calls_by_this_contract=0
research_evidence_effect=NONE
live_effect=NONE
```

## Unregistered storage adapter

The isolated migration prepares two append-only tables and is listed only in
the explicit one-shot schema installer. Registration does not apply it: the
installer still requires both `FORMULA_SCHEMA_APPLY=1` and a permitted database
URL. Reservation uses unique approval, one-shot, request and reservation keys.
Consumption must reference the exact reservation binding, fall inside its
approval window and win an insert-only compare-and-set; concurrent or repeated
claims return no row rather than updating existing state.

The adapter defaults to `DATABASE_UNREGISTERED`. Its SQL can currently execute
only against an explicitly marked transaction double whose transaction is
already active. It does not create a database connection, begin or commit a
transaction, dispatch a message or integrate with the candidate service.

```text
migration=019_preview_first_message_reservation_consumption_v1.sql
migration_registered=true
migration_applied=false
database_registered=false
default_execution_scope=DATABASE_UNREGISTERED
transaction_scope_required=true
runtime_execution_supported=false
dispatch_allowed=false
delivery_allowed=false
database_connections=0
database_writes=0
telegram_api_calls=0
research_evidence_effect=NONE
live_effect=NONE
```

## Read-only database preflight — 2026-09-01

The registered migration was not applied. A single Render PostgreSQL query was
executed inside the provider-enforced read-only transaction against the known
project database. The target identity and schema prerequisites matched the
expected environment, and none of migration `019`'s objects existed before a
future application attempt.

```text
render_postgres_id=dpg-d94d641kh4rs73evvih0-a
database_name=crypto_intelligence_db
postgres_version=18.4
current_schema=public
transaction_read_only=on
public_schema_usage=true
public_schema_create=true
plpgsql_available=true
reservation_table_exists=false
consumption_table_exists=false
validation_function_exists=false
append_only_function_exists=false
conflicting_triggers=0
migration_applied=false
database_writes=0
```

## Dedicated staging database — provisioned 2026-09-01

A separate Render PostgreSQL instance now isolates future PREVIEW storage work
from the shared Production database. Provisioning alone does not connect the
candidate service, apply a migration or enable runtime persistence.

```text
render_postgres_id=dpg-dab7rc2d0e5s73dkb9l0-a
render_name=crypto-intelligence-staging-db
database_name=crypto_intelligence_staging_db
plan=free
region=oregon
postgres_version=18
status=available
external_ip_allowlist_entries=0
expires_at_utc=2026-10-01T07:24:32Z
candidate_service_connected=false
migration_019_applied=false
runtime_database_registered=false
production_database_changed=false
```

## Dedicated staging preflight — connection blocked

The new database reached `available`, but the connected Render read-only query
tool failed before executing SQL because its connection did not satisfy the new
database's SSL/TLS requirement. One delayed retry returned the same connection
error. No browser fallback, network-rule change or non-TLS bypass was used.

```text
render_postgres_id=dpg-dab7rc2d0e5s73dkb9l0-a
preflight_status=BLOCKED_CONNECTION_SSL_REQUIRED
sql_queries_executed=0
schema_objects_created=0
migration_019_applied=false
candidate_service_connected=false
external_ip_allowlist_entries=0
production_database_changed=false
database_writes=0
```

## Temporary Render CLI authorization — 2026-09-01

The official Render CLI binary was downloaded into an isolated temporary
directory and verified against the official release checksum. Device
authorization completed for the expected Render workspace. The CLI token is
stored only in the isolated temporary config and expires automatically; no
database connection or SQL command was run during this step.

```text
render_cli_version=2.25.0
render_cli_checksum_verified=true
cli_binary_scope=TEMPORARY_ISOLATED
cli_config_scope=TEMPORARY_ISOLATED
workspace=My Workspace
cli_token_expires_in_days=7
database_connections=0
sql_queries_executed=0
migration_019_applied=false
candidate_service_connected=false
production_database_changed=false
```

## Dedicated staging CLI preflight — allow-list blocked

The authenticated official Render CLI attempted one non-interactive connection
to the dedicated staging database. Render rejected the external client before
`psql` connected because its IP address is not present in the database allow
list. No allow-list rule was added, no transaction began and no SQL executed.

```text
render_postgres_id=dpg-dab7rc2d0e5s73dkb9l0-a
preflight_status=BLOCKED_EXTERNAL_IP_NOT_ALLOWLISTED
connection_attempts=1
database_connections_established=0
sql_queries_executed=0
allowlist_changes=0
schema_objects_created=0
migration_019_applied=false
candidate_service_connected=false
production_database_changed=false
database_writes=0
```

## Temporary staging external access — allowed 2026-09-01

One temporary PostgreSQL inbound rule now permits only the external address
reported by the isolated CLI client. The rule uses a single-host `/32` CIDR;
the address itself is intentionally omitted from this repository. No broad
internet rule exists. This access is limited to the dedicated staging database
and must be removed immediately after the read-only preflight.

```text
render_postgres_id=dpg-dab7rc2d0e5s73dkb9l0-a
temporary_inbound_rule_count=1
temporary_cidr_scope=/32
broad_inbound_rule_present=false
removal_required_after_preflight=true
database_connections_established=0
sql_queries_executed=0
schema_objects_created=0
migration_019_applied=false
candidate_service_connected=false
production_database_changed=false
database_writes=0
```

## Dedicated staging CLI preflight — local client missing

The Render CLI reached its local PostgreSQL client handoff, but this isolated
environment does not contain `psql` or an installed Python PostgreSQL driver.
Execution stopped before any database connection. No client package was
installed as part of the preflight step. The single temporary `/32` inbound
rule remains active pending the next explicitly approved action.

```text
render_postgres_id=dpg-dab7rc2d0e5s73dkb9l0-a
preflight_status=BLOCKED_LOCAL_PSQL_MISSING
local_psql_available=false
installed_python_postgres_drivers=0
client_install_changes=0
temporary_inbound_rule_count=1
temporary_cidr_scope=/32
database_connections_established=0
sql_queries_executed=0
schema_objects_created=0
migration_019_applied=false
candidate_service_connected=false
production_database_changed=false
database_writes=0
```

## Temporary PostgreSQL client — prepared 2026-09-01

The Ubuntu 24.04 signed package indexes were downloaded into the existing
isolated temporary directory. `postgresql-client-16` and `libpq5` were then
downloaded and extracted there without `apt install`, system package changes or
a global `PATH` update. Both package SHA-256 values matched the signed index
metadata. The extracted client starts successfully and links to OpenSSL 3 with
no missing library dependency.

```text
psql_version=16.15
ubuntu_package_version=16.15-0ubuntu0.24.04.1
package_hashes_verified=true
package_install_scope=TEMPORARY_ISOLATED
system_packages_changed=false
global_path_changed=false
tls_library=OpenSSL_3
missing_runtime_libraries=0
temporary_inbound_rule_count=1
database_connections_established=0
sql_queries_executed=0
migration_019_applied=false
candidate_service_connected=false
production_database_changed=false
database_writes=0
```

## Dedicated staging CLI preflight — DNS blocked

The isolated `psql` client started through the authenticated Render CLI and the
CLI selected the dedicated staging database's external endpoint. The local
environment could not resolve that Render hostname, so `psql` stopped before a
database connection. A direct resolver check confirmed the same limitation.
No alternate DNS service, hard-coded address or network bypass was used.

```text
render_postgres_id=dpg-dab7rc2d0e5s73dkb9l0-a
preflight_status=BLOCKED_EXTERNAL_DNS_RESOLUTION
psql_started=true
database_connections_established=0
transactions_started=0
sql_queries_executed=0
dns_bypass_attempts=0
temporary_inbound_rule_count=1
temporary_rule_removal_required=true
schema_objects_created=0
migration_019_applied=false
candidate_service_connected=false
production_database_changed=false
database_writes=0
```

## Temporary staging external access — removed 2026-09-01

The official Render API first confirmed the expected database identity and
exactly one inbound rule. A narrowly scoped Postgres update then set
`ipAllowList` to an empty list. The update response and an independent GET
readback both reported zero rules; the database was `available` at readback.
No other database property was included in the update request.

```text
render_postgres_id=dpg-dab7rc2d0e5s73dkb9l0-a
api_update_field=ipAllowList
api_update_value=EMPTY_LIST
api_update_status=200
post_update_database_status=available
post_update_inbound_rule_count=0
external_traffic_allowed=false
database_connections_established=0
transactions_started=0
sql_queries_executed=0
schema_objects_created=0
migration_019_applied=false
candidate_service_connected=false
production_database_changed=false
database_writes=0
```

## Render-internal read-only preflight runner — prepared locally

The external CLI route cannot resolve the Render Postgres hostname from the
current execution environment. A separate one-shot runner is therefore
prepared for a later, explicitly approved Render-internal execution. It is not
imported by the candidate service and is not deployed or configured yet.

The runner accepts only the dedicated variable below and never reads the
generic `DATABASE_URL`. It rejects the Production database, Render's external
hostname and every enabled schema-apply flag:

```text
FORMULA_PREVIEW_STAGING_DATABASE_PREFLIGHT=1
FORMULA_PREVIEW_STAGING_DATABASE_URL=<Render internal URL for dpg-dab7rc2d0e5s73dkb9l0-a>
```

Its only database statements are an explicit `BEGIN ... READ ONLY`, one
identity/schema `SELECT`, and `ROLLBACK`. A successful result may establish
preflight readiness only; it cannot apply migration `019`, register persistence
or activate PREVIEW delivery.

```text
runner=research_preview_staging_readonly_preflight.py
runner_scope=ONE_SHOT_RENDER_INTERNAL_READ_ONLY
runtime_imported=false
render_configuration_applied=false
deployed=false
executed=false
selftests_passed=48
project_compilation_passed=true
schema_mutation_allowed=false
migration_apply_allowed=false
candidate_service_connected=false
database_connections=0
sql_queries_executed=0
database_writes=0
```

## Isolated Render Workflow path — prepared locally

The selected later execution path is a dedicated, manually triggered Render
Workflow. Its only task wraps the existing fail-closed runner with the `flex`
plan, a 30-second timeout and zero automatic retries. The Workflow has separate
dependencies and is never imported by `crypto-ai-agent-candidate`.

The exact configuration, temporary internal-URL sequence, cleanup requirements
and rejected alternatives are recorded in
`docs/PREVIEW_STAGING_INTERNAL_WORKFLOW_RUNBOOK.md`.

```text
workflow_name=preview-staging-readonly-preflight
workflow_task=preview_staging_readonly_preflight_once
workflow_plan=flex
workflow_timeout_seconds=30
workflow_max_retries=0
workflow_auto_deploy=false
workflow_source_branch=preview-staging-preflight-workflow
candidate_auto_deploy_branch=ai-production-analytics
source_branch_overlap=false
manual_trigger_only=true
workflow_resource_created=false
workflow_deployed=false
workflow_task_runs=0
remote_push_performed=false
candidate_service_changed=false
database_connections=0
sql_queries_executed=0
database_writes=0
migration_019_applied=false
telegram_api_calls=0
```
