# Metadata-only source access preflight

`research_no_horizon_source_preflight.py` explicitly authenticates to the
configured source and inspects PostgreSQL system catalogs. It never selects
source-table rows, evaluates a formula, builds an acquisition anchor, changes a
registration, or writes the runner's endpoint seal. It can run before October
12 because it produces access metadata, not prospective market evidence.

The DSN must name one host, database and login. The same host/port/database/user
encoding used by the registered runner supplies `endpoint_sha256`. Credentials
and the unhashed connection identity are excluded from the receipt. Remote
connections force `sslmode=verify-full`, system CA roots, a five-second connect
timeout, a read-only transaction and a fifteen-second statement timeout. The
metadata query confirms TLS is active on its own backend. The separate local
test switch is rejected by the production receipt validator.

The preflight rejects ambient `PGHOSTADDR`, `PGSERVICE`, `PGOPTIONS`, or a
`PGPORT` default absent from the DSN. Use the dedicated source variable and
explicit DSN settings, not a shared production environment group.

## Required relations

| Relation in `public` | Purpose |
| --- | --- |
| `research_watch_scan_intakes` | Registered source population |
| `research_max_pain_snapshot_sets` | Captured source payload and scores |
| `research_btc_parent_movements` | Causal parent membership |
| `research_btc_price_bars` | Prior Bitcoin bar |
| `research_price_archive_bars` | Frozen price route |
| `research_watch_scan_intake_state` | Collection metadata audit only |

All six must be ordinary or partitioned tables with table-level SELECT,
compatible required column types, and row security disabled. Schema USAGE is
required. Requiring table-level SELECT covers the native reader's `to_jsonb`
of whole rows. No sequence access, extension installation, schema creation,
source migration or source write permission is required.

## Role contract

A passing role is a dedicated login with none of `SUPERUSER`, `CREATEDB`,
`CREATEROLE`, `REPLICATION`, or `BYPASSRLS`. The current and session users must
match the configured login, and the actual database must match the DSN.

The check rejects all role memberships, including read-only memberships. This
keeps indirect inheritance and `SET ROLE` paths outside the accepted contract;
grant the six table privileges directly. It also rejects database ownership or
CREATE; ownership or CREATE in a non-system schema; ownership or write
privileges on non-system relations anywhere in the current database; sequence
USAGE/UPDATE; and executable user-schema SECURITY DEFINER routines. Relation
checks include column-level INSERT/UPDATE/REFERENCES grants and privileges
inherited through PUBLIC. Trigger/event-trigger routines are excluded from the
callable-routine check because PostgreSQL does not permit ordinary direct calls.

An executable SECURITY DEFINER function can be harmless; the conservative
contract still blocks it. This preflight does not alter PUBLIC privileges or
other users to make a check pass. Any shared grants that prevent this restricted
role require a separate, reviewed administrator decision.

The following is an administrator-reviewed provisioning template, **not an
operation performed by the module**. In `psql`, `source_database` is a supplied
identifier variable. Set the password through an interactive prompt or the
provider's secret mechanism; do not put it in a SQL file or command argument.

```sql
CREATE ROLE no_horizon_research_reader LOGIN
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT
    NOREPLICATION NOBYPASSRLS;

GRANT CONNECT ON DATABASE :"source_database" TO no_horizon_research_reader;
GRANT USAGE ON SCHEMA public TO no_horizon_research_reader;
GRANT SELECT ON TABLE
    public.research_watch_scan_intakes,
    public.research_max_pain_snapshot_sets,
    public.research_btc_parent_movements,
    public.research_btc_price_bars,
    public.research_price_archive_bars,
    public.research_watch_scan_intake_state
TO no_horizon_research_reader;

ALTER ROLE no_horizon_research_reader SET default_transaction_read_only = on;
```

The interactive `psql` command is `\password no_horizon_research_reader`.
`default_transaction_read_only=on` is a default, not an authorization boundary;
the actual ACL and ownership checks are still necessary. Default TEMP access is
not treated as persistent source-write permission. The preflight creates no
temporary objects.

## Explicit invocation and receipt use

```bash
python research_no_horizon_source_preflight.py --execute \
  --source-id confirmed-source-id \
  --source-url-env RESEARCH_NO_HORIZON_READ_DATABASE_URL
```

The script prints one bounded JSON receipt and returns zero only for PASS.
FAIL reports failed named checks; ERROR reports an exception type without the
driver's message, SQL, DSN or traceback. The source ID is an operator-supplied
non-secret identifier, not authentication by itself.

`validate_receipt(receipt, source_id=..., endpoint_sha256=...)` verifies the
receipt's SHA256, complete passing check set, remote TLS mode and endpoint
binding. `endpoint_identity(raw_dsn, source_id=...)` parses the current remote
DSN locally without opening a connection. A supervisor may explicitly run this
preflight at enabled, nonterminal startup and retain its receipt. Disabled or
already completed execution must not imply another source connection. A
successful preflight does not change acquisition cutoffs or claim market-data
coverage.

The receipt is a local integrity record, not a signature or independent remote
attestation. Its checks describe one authenticated session and can become stale
if privileges or server configuration change. It does not inspect every
extension, arbitrary function's external effects, permissions in other
databases, or future administrative changes. Accordingly,
`global_privilege_proof=false`, `deployment_verified=false`, and
`market_rows_read=false` remain explicit even on PASS.

## Verification

The selftests cover TLS/DSN parsing, source fingerprint compatibility, all role
and relation gates, credential sanitization, receipt binding, and the real
psycopg parameter parser. With an explicit local test-named PostgreSQL admin
DSN in `SOURCE_PREFLIGHT_TEST_DATABASE_URL` or CI's `TEST_DATABASE_URL`, the
optional test creates its own empty database and restricted role, verifies a
pass, then verifies rejection of a column UPDATE grant and CREATEDB privilege.
It removes both fixture objects afterward. No market rows are inserted or read.

Source requirements come from `research_no_horizon_manifest_sql.py`,
`research_no_horizon_collection_audit.py`, and migrations 007, 021, 044 and 046.
Privilege decisions are derived from PostgreSQL catalog/ACL metadata during the
explicit preflight; source collection completeness is deliberately unproven.
