"""Explicit metadata-only authentication and source-role preflight.

No source table rows, cohort queries, registration files or endpoint seals are
read/written. A PASS checks the stated current-database privilege contract; it
does not prove arbitrary SQL effects, future privileges or deployed execution.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
import re

VERSION = "no-horizon-source-metadata-preflight-v1"
TRANSPORT = "DIRECT_POSTGRES_NATIVE"
MAX_RESPONSE_BYTES = 1024 * 1024
TABLE_COLUMNS = {
    "research_watch_scan_intakes": {
        "snapshot_set_id": "int8", "consumer_version": "text", "intake_status": "text",
        "usable_from_utc": "timestamptz", "ingested_at_utc": "timestamptz",
        "watch_scan_id": "text", "bundle_sha256": "text", "parent_payload_sha256": "bpchar"},
    "research_max_pain_snapshot_sets": {
        "snapshot_set_id": "int8", "snapshot_key": "bpchar", "payload_sha256": "bpchar",
        "cycle_id": "text", "source": "text", "available_at_utc": "timestamptz",
        "created_at_utc": "timestamptz", "source_metadata": "jsonb"},
    "research_btc_parent_movements": {
        "btc_parent_movement_id": "text", "episode_policy_version": "text",
        "start_time_utc": "timestamptz", "end_time_utc": "timestamptz",
        "confirmed_at_utc": "timestamptz", "direction": "text", "evidence_eligible": "bool",
        "boundary_reason": "text", "observed_through_utc": "timestamptz",
        "price_source": "text", "state_json": "jsonb"},
    "research_btc_price_bars": {
        "open_time_utc": "timestamptz", "close_time_utc": "timestamptz",
        "open": "float8", "high": "float8", "low": "float8", "close": "float8",
        "price_source": "text"},
    "research_price_archive_bars": {
        "route": "text", "symbol": "text", "open_time_utc": "timestamptz",
        "close_time_utc": "timestamptz", "open": "float8", "high": "float8",
        "low": "float8", "close": "float8", "volume": "float8"},
    "research_watch_scan_intake_state": {
        "consumer_version": "text", "updated_at_utc": "timestamptz",
        "scan_cursor": "int8", "scan_high_water": "int8", "completed_laps": "int8"},
}
ADMIN_FLAGS = ("rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls")
FORBIDDEN_COUNTS = ("memberships", "owned_schemas", "create_schemas", "owned_relations",
                    "writable_relations", "writable_sequences", "callable_definer_routines")
CHECK_NAMES = frozenset(("identity_matches", "login_without_admin_flags", "no_role_memberships",
    "no_database_ownership_or_create", "no_schema_ownership_or_create",
    "no_relation_ownership_or_writes", "no_sequence_mutation_privileges",
    "no_callable_user_security_definer_routines", "source_relations_readable_and_compatible",
    "transaction_read_only", "transport_verified"))


def encoded(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2,
                       allow_nan=False) + "\n").encode()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def seal(value):
    return {**value, "receipt_sha256": digest(encoded(value))}


def source_configuration(raw, *, source_id, allow_local_source=False):
    """Parse locally; return private driver input separately from safe identity."""
    from psycopg.conninfo import conninfo_to_dict
    if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,120}", source_id or ""):
        raise ValueError("INVALID_SOURCE_ID")
    if not raw:
        raise ValueError("SOURCE_NOT_CONFIGURED")
    config = conninfo_to_dict(raw)
    host = config.get("host", "")
    if (not host or "," in host or host.startswith("/") or not config.get("user")
            or not config.get("dbname") or any(key in config for key in ("service", "hostaddr", "options"))):
        raise ValueError("EXPLICIT_SINGLE_SOURCE_ENDPOINT_REQUIRED")
    port = config.get("port", "5432")
    if not re.fullmatch(r"[0-9]{1,5}", port) or not 1 <= int(port) <= 65535:
        raise ValueError("INVALID_SOURCE_PORT")
    local = host in ("localhost", "127.0.0.1", "::1")
    if local and not allow_local_source:
        raise ValueError("LOCAL_SOURCE_REQUIRES_EXPLICIT_TEST_SWITCH")
    # PGHOSTADDR/PGSERVICE can change routing independently of a parsed host.
    # A missing DSN port must not silently inherit PGPORT with a different seal.
    if (os.getenv("PGHOSTADDR") or os.getenv("PGSERVICE") or os.getenv("PGOPTIONS")
            or ("port" not in config and os.getenv("PGPORT"))):
        raise ValueError("AMBIENT_LIBPQ_ROUTING_OR_OPTIONS_REJECTED")
    endpoint = {key: config.get(key, "5432" if key == "port" else "")
                for key in ("host", "port", "dbname", "user")}
    identity = {"transport": TRANSPORT, "source_id": source_id,
                "endpoint_sha256": digest(encoded(endpoint)),
                "sslmode": "disable" if local else "verify-full"}
    return config, identity


def endpoint_identity(raw, *, source_id):
    """Pure local remote-endpoint parsing for a supervisor's retained receipt gate."""
    return source_configuration(raw, source_id=source_id)[1]


def metadata_sql():
    """Catalog-only statement; no FROM/JOIN against any market-data relation."""
    names = ",".join("('" + name + "')" for name in TABLE_COLUMNS)
    return f"""WITH identity AS (
 SELECT r.* FROM pg_catalog.pg_roles r WHERE r.rolname=current_user
), wanted(name) AS (VALUES {names}), relations AS (
 SELECT w.name,c.oid,c.relkind,c.relrowsecurity,c.relforcerowsecurity,
   COALESCE(pg_catalog.has_table_privilege(c.oid,'SELECT'),false) AS selectable,
   (SELECT COALESCE(jsonb_object_agg(a.attname,t.typname),'{{}}'::jsonb)
    FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_type t ON t.oid=a.atttypid
    WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped) AS columns
 FROM wanted w LEFT JOIN pg_catalog.pg_class c
   ON c.oid=pg_catalog.to_regclass('public.'||w.name)
), user_relations AS (
 SELECT c.* FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema'
), user_schemas AS (
 SELECT n.* FROM pg_catalog.pg_namespace n
 WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema'
)
SELECT jsonb_build_object(
 'checked_at_utc',statement_timestamp(),
 'database_matches',current_database()=%s,'user_matches',current_user=%s,
 'session_matches',current_user=session_user,
 'read_only',current_setting('transaction_read_only'),
 'ssl_in_use',COALESCE((SELECT ssl FROM pg_catalog.pg_stat_ssl WHERE pid=pg_backend_pid()),false),
 'role',jsonb_build_object('rolcanlogin',r.rolcanlogin,'rolsuper',r.rolsuper,
   'rolcreatedb',r.rolcreatedb,'rolcreaterole',r.rolcreaterole,
   'rolreplication',r.rolreplication,'rolbypassrls',r.rolbypassrls),
 'database_owner',(SELECT datdba=r.oid FROM pg_catalog.pg_database WHERE datname=current_database()),
 'database_create',pg_catalog.has_database_privilege(current_database(),'CREATE'),
 'memberships',(SELECT count(*) FROM pg_catalog.pg_auth_members WHERE member=r.oid),
 'owned_schemas',(SELECT count(*) FROM user_schemas WHERE nspowner=r.oid),
 'create_schemas',(SELECT count(*) FROM user_schemas WHERE pg_catalog.has_schema_privilege(oid,'CREATE')),
 'owned_relations',(SELECT count(*) FROM user_relations WHERE relowner=r.oid),
 'writable_relations',(SELECT count(*) FROM user_relations WHERE
   CASE WHEN relkind IN ('r','p','v','m','f') THEN
     (pg_catalog.has_table_privilege(oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') OR
      pg_catalog.has_any_column_privilege(oid,'INSERT,UPDATE,REFERENCES')) ELSE false END),
 'writable_sequences',(SELECT count(*) FROM user_relations WHERE
   CASE WHEN relkind='S' THEN pg_catalog.has_sequence_privilege(oid,'USAGE,UPDATE') ELSE false END),
 'callable_definer_routines',(SELECT count(*) FROM pg_catalog.pg_proc p
   JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
   WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema' AND p.prosecdef
     AND p.prorettype NOT IN ('pg_catalog.trigger'::regtype,'pg_catalog.event_trigger'::regtype)
     AND pg_catalog.has_function_privilege(p.oid,'EXECUTE')),
 'public_schema_usage',pg_catalog.has_schema_privilege('public','USAGE'),
 'relations',(SELECT jsonb_agg(jsonb_build_object('name',name,'present',oid IS NOT NULL,
   'kind',relkind::text,'selectable',selectable,'row_security',relrowsecurity,
   'force_row_security',relforcerowsecurity,'columns',columns) ORDER BY name) FROM relations)
) AS preflight FROM identity r;
"""


def assess(payload, identity):
    if not isinstance(payload, dict) or len(encoded(payload)) > MAX_RESPONSE_BYTES:
        raise ValueError("INVALID_BOUNDED_METADATA_RESPONSE")
    for field in FORBIDDEN_COUNTS:
        if type(payload.get(field)) is not int or payload[field] < 0:
            raise ValueError("INVALID_PRIVILEGE_COUNT")
    role = payload.get("role") or {}
    if any(type(role.get(field)) is not bool for field in (*ADMIN_FLAGS, "rolcanlogin")):
        raise ValueError("INVALID_ROLE_METADATA")
    for field in ("database_matches", "user_matches", "session_matches", "database_owner",
                  "database_create", "ssl_in_use", "public_schema_usage"):
        if type(payload.get(field)) is not bool:
            raise ValueError("INVALID_IDENTITY_OR_TRANSPORT_METADATA")
    checked = datetime.fromisoformat(payload["checked_at_utc"].replace("Z", "+00:00"))
    if checked.tzinfo is None:
        raise ValueError("METADATA_CLOCK_MUST_HAVE_TIMEZONE")
    rows = payload.get("relations")
    if (not isinstance(rows, list) or len(rows) != len(TABLE_COLUMNS)
            or {row.get("name") for row in rows} != set(TABLE_COLUMNS)):
        raise ValueError("EXACT_REQUIRED_RELATION_SET_REQUIRED")
    relations = []
    for row in rows:
        expected = TABLE_COLUMNS[row["name"]]
        columns = row.get("columns") or {}
        relations.append({"name": row["name"], "present": row.get("present") is True,
            "ordinary_table": row.get("kind") in ("r", "p"),
            "select_all_columns": row.get("selectable") is True,
            "row_security_disabled": row.get("row_security") is False and row.get("force_row_security") is False,
            "required_column_types_match": all(columns.get(key) == value for key, value in expected.items())})
    checks = {
        "identity_matches": all(payload[field] for field in ("database_matches", "user_matches", "session_matches")),
        "login_without_admin_flags": role["rolcanlogin"] and not any(role[field] for field in ADMIN_FLAGS),
        "no_role_memberships": payload["memberships"] == 0,
        "no_database_ownership_or_create": not payload["database_owner"] and not payload["database_create"],
        "no_schema_ownership_or_create": payload["owned_schemas"] == payload["create_schemas"] == 0,
        "no_relation_ownership_or_writes": payload["owned_relations"] == payload["writable_relations"] == 0,
        "no_sequence_mutation_privileges": payload["writable_sequences"] == 0,
        "no_callable_user_security_definer_routines": payload["callable_definer_routines"] == 0,
        "source_relations_readable_and_compatible": payload["public_schema_usage"] and
            all(all(value is True for key, value in row.items() if key != "name") for row in relations),
        "transaction_read_only": payload.get("read_only") == "on",
        "transport_verified": identity["sslmode"] == "disable" or payload["ssl_in_use"],
    }
    passed = all(checks.values())
    return seal({"version": VERSION, **identity, "checked_at_utc": checked.astimezone(timezone.utc).isoformat(),
        "status": "PASS" if passed else "FAIL", "access_contract_satisfied": passed,
        "checks": checks, "relations": relations,
        "failed_checks": sorted(key for key, value in checks.items() if value is not True),
        "metadata_only": True, "market_rows_read": False, "database_writes": False,
        "registration_state_changed": False, "source_endpoint_seal_written": False,
        "global_privilege_proof": False, "deployment_verified": False,
        "local_test_transport": identity["sslmode"] == "disable"})


def validate_receipt(receipt, *, source_id, endpoint_sha256):
    """Validate a retained PASS and its byte-linkage; this is not signed attestation."""
    if not isinstance(receipt, dict):
        raise ValueError("PREFLIGHT_RECEIPT_REQUIRED")
    body = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    checks = receipt.get("checks") or {}
    if (receipt.get("receipt_sha256") != digest(encoded(body)) or receipt.get("version") != VERSION
            or receipt.get("transport") != TRANSPORT or receipt.get("source_id") != source_id
            or receipt.get("endpoint_sha256") != endpoint_sha256
            or receipt.get("sslmode") != "verify-full" or receipt.get("local_test_transport") is not False
            or receipt.get("status") != "PASS" or receipt.get("access_contract_satisfied") is not True
            or set(checks) != CHECK_NAMES or any(value is not True for value in checks.values())
            or receipt.get("failed_checks") != [] or receipt.get("metadata_only") is not True
            or any(receipt.get(key) is not False for key in ("market_rows_read", "database_writes",
                "registration_state_changed", "source_endpoint_seal_written", "global_privilege_proof", "deployment_verified"))):
        raise ValueError("PREFLIGHT_RECEIPT_BINDING_OR_ACCESS_FAILED")
    checked = datetime.fromisoformat(receipt["checked_at_utc"].replace("Z", "+00:00"))
    if checked.tzinfo is None:
        raise ValueError("PREFLIGHT_RECEIPT_CLOCK_INVALID")
    return True


def execute(raw, *, source_id, allow_local_source=False):
    """One explicit connection, one metadata statement, no filesystem writes."""
    safe_id = source_id if isinstance(source_id, str) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,120}", source_id) else None
    identity = {"transport": TRANSPORT, "source_id": safe_id, "endpoint_sha256": None, "sslmode": None}
    try:
        import psycopg
        from psycopg.rows import dict_row
        config, identity = source_configuration(raw, source_id=source_id, allow_local_source=allow_local_source)
        with psycopg.connect(raw, row_factory=dict_row, connect_timeout=5, sslmode=identity["sslmode"],
                **({} if identity["sslmode"] == "disable" else {"sslrootcert": "system"}),
                options="-c default_transaction_read_only=on -c timezone=UTC -c statement_timeout=15000 "
                        "-c lock_timeout=1000 -c idle_in_transaction_session_timeout=20000 "
                        "-c search_path=pg_catalog -c application_name=no_horizon_source_preflight") as connection:
            with connection.transaction():
                connection.execute("SET TRANSACTION READ ONLY")
                rows = connection.execute(metadata_sql(), (config["dbname"], config["user"])).fetchmany(2)
                if len(rows) != 1 or set(rows[0]) != {"preflight"}:
                    raise ValueError("INVALID_METADATA_RESPONSE_CARDINALITY")
                result = assess(rows[0]["preflight"], identity)
        return result
    except Exception as error:
        # No DSN, private identity values, SQL text or driver diagnostics escape.
        return seal({"version": VERSION, **identity, "checked_at_utc": datetime.now(timezone.utc).isoformat(),
            "status": "ERROR", "access_contract_satisfied": False, "error_type": type(error).__name__,
            "metadata_only": True, "market_rows_read": False, "database_writes": False,
            "registration_state_changed": False, "source_endpoint_seal_written": False,
            "global_privilege_proof": False, "deployment_verified": False})


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--source-url-env", default="RESEARCH_NO_HORIZON_READ_DATABASE_URL")
    parser.add_argument("--allow-local-source", action="store_true", help="isolated local test only")
    args = parser.parse_args(argv)
    if (not args.execute or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.source_url_env)
            or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,120}", args.source_id)):
        parser.error("explicit execution and valid source identifiers required")
    receipt = execute(os.getenv(args.source_url_env, ""), source_id=args.source_id,
                      allow_local_source=args.allow_local_source)
    print(encoded(receipt).decode(), end="")
    return 0 if receipt["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
