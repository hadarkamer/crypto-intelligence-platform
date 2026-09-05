"""Database-free adversarial checks for the authoritative Stage4/Wave reader.

The disposable-PostgreSQL migration and privilege tests are deliberately a
separate, explicitly approved stage.  This file exercises the Python trust
boundary with a deterministic fake connection only.
"""

from __future__ import annotations

import ast
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
from pathlib import Path
import os
import re
from types import SimpleNamespace
from unittest.mock import patch

import research_formula_schema_admin
import research_market_movement as movement
import research_signal_formula_exploration as exploration
import research_signal_formula_exploration_reader as reader
import research_signal_formula_exploration_selftest as fixtures


UTC = timezone.utc
AS_OF = fixtures.AS_OF
SNAPSHOT_KEY = fixtures._h("snapshot-71")
SOURCE_CATALOG_SHA256 = "a5" * 32
MIGRATION_PATH = (
    Path(__file__).resolve().parent
    / "migrations"
    / "024_formula_exploration_authoritative_reader_v1.sql"
)
OUTCOME_MIGRATION_PATH = (
    Path(__file__).resolve().parent
    / "migrations"
    / "025_formula_exploration_outcomes_v1.sql"
)
NO_SIGNAL_OUTCOME_MIGRATION_PATH = (
    Path(__file__).resolve().parent
    / "migrations"
    / "026_stage4_no_signal_outcomes_v1.sql"
)
OUTCOME_VIEW_DEFINITION_SHA256 = "b6" * 32
NO_SIGNAL_VIEW_DEFINITION_SHA256 = "d8" * 32
NO_SIGNAL_RAW_CATALOG_SHA256 = "e1" * 32
NO_SIGNAL_TRIGGER_CATALOG_SHA256 = "f2" * 32


class _Rows:
    def __init__(self, rows=()):
        if rows is None:
            self.rows = []
        elif isinstance(rows, dict):
            self.rows = [deepcopy(rows)]
        else:
            self.rows = [deepcopy(row) for row in rows]
        self.index = 0

    def fetchone(self):
        if self.index >= len(self.rows):
            return None
        row = self.rows[self.index]
        self.index += 1
        return deepcopy(row)

    def fetchall(self):
        rows = self.rows[self.index :]
        self.index = len(self.rows)
        return deepcopy(rows)


def _statement_operation(sql: object) -> str:
    without_comments = re.sub(r"/\*.*?\*/", " ", str(sql), flags=re.DOTALL)
    match = re.search(r"\b([A-Za-z]+)\b", without_comments)
    return match.group(1).upper() if match else ""


def _normalized_sql(sql: str) -> str:
    return " ".join(sql.split())


def _assert_role_graph_contract(sql: str) -> None:
    edge_match = re.search(
        r"authority_membership_edges\s+AS\s*\((.*?)\)\s*,\s*"
        r"graph_role_ids",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    assert edge_match is not None
    edge_block = _normalized_sql(edge_match.group(1))
    edge_where = re.search(
        r"\bWHERE\s+membership\.roleid\s+IN\s*\(\s*"
        r"SELECT\s+reachable\.role_oid\s+FROM\s+"
        r"authority_reachable\s+reachable\s*\)\s+OR\s+"
        r"membership\.roleid\s+IN\s*\(\s*"
        r"SELECT\s+required\.role_oid\s+FROM\s+"
        r"required_role_ids\s+required\s*\)\s+OR\s+"
        r"membership\.member\s+IN\s*\(\s*"
        r"SELECT\s+required\.role_oid\s+FROM\s+"
        r"required_role_ids\s+required\s*\)\s*$",
        edge_block,
        flags=re.IGNORECASE,
    )
    assert edge_where is not None, edge_block
    assert "where false" not in edge_block.lower()

    graph_match = re.search(
        r"graph_role_ids\s*\(\s*role_oid\s*\)\s+AS\s*\((.*?)\)\s*"
        r"SELECT\s+(?:pg_catalog\.)?jsonb_build_object",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    assert graph_match is not None
    graph_block = _normalized_sql(graph_match.group(1)).lower()
    for endpoint in ("roleid", "member", "grantor"):
        assert re.search(
            rf"\bselect\s+edge\.{endpoint}\s+from\s+"
            r"authority_membership_edges\s+edge\b",
            graph_block,
        ), graph_block

    normalized = _normalized_sql(sql).lower()
    assert re.search(
        r"'nodes'.*?from\s+graph_role_ids\s+graph_role.*?"
        r"'membership_edges'.*?from\s+authority_membership_edges\s+edge",
        normalized,
    )


def _assert_max_pain_007_boundary(sql: str) -> None:
    start = sql.index("-- Migration 007 is part of the same installer transaction")
    end_marker = "'Migration 007 Max-Pain archive functions are not intact';"
    end = sql.index(end_marker, start) + len(end_marker)
    boundary = sql[start:end]
    expected_triggers = {
        "research_max_pain_snapshot_sets": (
            "trg_research_max_pain_set_complete",
            "trg_research_max_pain_sets_append_only",
            "trg_research_max_pain_sets_no_truncate",
        ),
        "research_max_pain_snapshot_symbols": (
            "trg_research_max_pain_symbol_complete",
            "trg_research_max_pain_symbols_append_only",
            "trg_research_max_pain_symbols_no_truncate",
        ),
        "research_max_pain_snapshot_rows": (
            "trg_research_max_pain_row_complete",
            "trg_research_max_pain_rows_append_only",
            "trg_research_max_pain_rows_no_truncate",
        ),
    }
    for relation_name, trigger_names in expected_triggers.items():
        inventory = re.search(
            rf"\(\s*'{re.escape(relation_name)}'\s*,\s*ARRAY\s*\["
            r"(.*?)\]\s*::\s*TEXT\s*\[\s*\]\s*\)",
            boundary,
            flags=re.IGNORECASE | re.DOTALL,
        )
        assert inventory is not None, relation_name
        assert tuple(re.findall(r"'([^']+)'", inventory.group(1))) == trigger_names

    trigger_contracts = (
        r"trigger_row\.tgenabled\s*<>\s*'O'",
        r"trigger_row\.tgqual\s+IS\s+NOT\s+NULL",
        r"WHEN\s+trigger_row\.tgname\s+LIKE\s+'%_complete'\s+THEN\s+5",
        r"WHEN\s+trigger_row\.tgname\s+LIKE\s+'%_append_only'\s+THEN\s+27",
        r"WHEN\s+trigger_row\.tgname\s+LIKE\s+'%_no_truncate'\s+THEN\s+34",
        r"trigger_row\.tgdeferrable\s*<>\s*\(\s*"
        r"trigger_row\.tgname\s+LIKE\s+'%_complete'",
        r"trigger_row\.tginitdeferred\s*<>\s*\(\s*"
        r"trigger_row\.tgname\s+LIKE\s+'%_complete'",
        r"trigger_row\.tgconstraint\s*<>\s*0\s*\)\s*<>\s*\(\s*"
        r"trigger_row\.tgname\s+LIKE\s+'%_complete'",
        r"function_namespace\.nspname\s*<>\s*'public'",
        r"function_row\.proowner\s*<>\s*trusted_owner",
        r"WHEN\s+trigger_row\.tgname\s+LIKE\s+'%_complete'\s+THEN\s+"
        r"'assert_research_max_pain_snapshot_complete'\s+ELSE\s+"
        r"'prevent_research_max_pain_archive_mutation'",
    )
    for pattern in trigger_contracts:
        assert re.search(
            pattern, boundary, flags=re.IGNORECASE | re.DOTALL
        ), pattern

    function_check = re.search(
        r"IF\s*\(\s*SELECT\s+COUNT\(\*\).*?"
        r"Migration 007 Max-Pain archive functions are not intact",
        boundary,
        flags=re.IGNORECASE | re.DOTALL,
    )
    assert function_check is not None
    function_block = function_check.group(0)
    for function_name in (
        "assert_research_max_pain_snapshot_complete",
        "prevent_research_max_pain_archive_mutation",
    ):
        assert function_name in function_block
    for pattern in (
        r"function_row\.pronargs\s*=\s*0",
        r"pg_get_function_identity_arguments\s*\([^)]*\)\s*=\s*''",
        r"function_row\.prorettype\s*=\s*'pg_catalog\.trigger'::REGTYPE",
        r"function_row\.proowner\s*=\s*trusted_owner",
        r"NOT\s+function_row\.prosecdef",
        r"function_row\.proconfig\s+IS\s+NULL",
        r"function_row\.provolatile\s*=\s*'v'",
        r"NOT\s+function_row\.proisstrict",
        r"function_row\.proparallel\s*=\s*'u'",
        r"function_row\.proacl\s+IS\s+NULL",
        r"language_row\.lanname\s*=\s*'plpgsql'",
        r"pg_get_functiondef\s*\([^)]*\)\s+IS\s+NOT\s+NULL",
        r"\)\s*<>\s*2\s+THEN",
    ):
        assert re.search(
            pattern, function_block, flags=re.IGNORECASE | re.DOTALL
        ), pattern


def _assert_static_rejection(validator, mutated_sql: str) -> None:
    try:
        validator(mutated_sql)
    except (AssertionError, ValueError):
        return
    raise AssertionError("static contract validator accepted a weakened SQL mutant")


def _assert_delegated_writer_acl_normalization(sql: str) -> None:
    without_comments = re.sub(r"--[^\n]*", " ", sql)
    normalized = _normalized_sql(without_comments).lower()
    role = "research_stage4_no_signal_outcome_writer_v1"
    assert normalized.count(f"'{role}'") == 2
    assert re.search(
        r"namespace_row\.nspacl,\s*"
        r"pg_catalog\.acldefault\('n',\s*namespace_row\.nspowner\)\s*"
        r"\)\s*\)\s*acl\s*where\s+not\s*\(\s*"
        r"acl\.grantee\s*<>\s*0\s+and\s+"
        r"pg_catalog\.pg_get_userbyid\(acl\.grantee\)\s*=\s*"
        rf"'{role}'\s+and\s+"
        r"acl\.privilege_type\s*=\s*'usage'\s+and\s+"
        r"not\s+acl\.is_grantable\s*\)",
        normalized,
    )
    relation_match = re.search(
        r"relation_row\.relacl,\s*pg_catalog\.acldefault\(\s*'r',\s*"
        r"relation_row\.relowner\s*\)\s*\)\s*\)\s*acl\s*"
        r"where\s+not\s*\(\s*acl\.grantee\s*<>\s*0\s+and\s+"
        r"pg_catalog\.pg_get_userbyid\(acl\.grantee\)\s*=\s*"
        rf"'{role}'(.*?)acl\.privilege_type\s*=\s*'select'\s+and\s+"
        r"not\s+acl\.is_grantable\s*\)",
        normalized,
    )
    assert relation_match is not None
    relation_filter = relation_match.group(1)
    assert "relation_row.relname in" in relation_filter
    for relation_name in (
        "research_events",
        "research_max_pain_snapshot_sets",
        "research_max_pain_snapshot_symbols",
        "research_max_pain_snapshot_rows",
    ):
        assert relation_filter.count(f"'{relation_name}'") == 1


def _assert_no_signal_attestation_contract(sql: str) -> None:
    normalized = _normalized_sql(sql).lower()

    def cte(name: str, successor: str) -> str:
        match = re.search(
            rf"\b{re.escape(name)}\s+as\s*\((.*?)\)\s*,\s*"
            rf"{re.escape(successor)}\s+as\s*\(",
            normalized,
            flags=re.DOTALL,
        )
        assert match is not None, name
        return match.group(1)

    raw_catalog = cte("raw_catalog_payload", "raw_catalog_digest")
    raw_order = (
        "'columns'",
        "'ordinal'",
        "'name'",
        "'type'",
        "'not_null'",
        "'identity'",
        "'generated'",
        "'collation'",
        "'default'",
        "'constraints'",
        "'deferrable'",
        "'deferred'",
        "'validated'",
        "'no_inherit'",
        "'indexes'",
        "'access_method'",
        "'unique'",
        "'primary'",
        "'exclusion'",
        "'immediate'",
        "'valid'",
        "'ready'",
        "'live'",
    )
    cursor = -1
    for token in raw_order:
        cursor = raw_catalog.find(token, cursor + 1)
        assert cursor >= 0, token
    for token in (
        "pg_catalog.pg_get_expr(",
        "pg_catalog.pg_get_constraintdef(",
        "pg_catalog.pg_get_indexdef(",
        "order by attribute.attnum",
        "order by constraint_row.conname",
        "order by index_relation.relname",
        "constraint_row.contype in ('c', 'f', 'p', 'u')",
    ):
        assert token in raw_catalog, token
    raw_digest = cte("raw_catalog_digest", "trigger_catalog_payload")
    assert "sha256(pg_catalog.convert_to( payload::text, 'utf8' ))" in raw_digest

    trigger_catalog = cte("trigger_catalog_payload", "trigger_catalog_digest")
    trigger_order = (
        "'triggers'",
        "'name'",
        "'type'",
        "'enabled'",
        "'function'",
        "'definition'",
        "'functions'",
        "'owner'",
        "'security_definer'",
        "'leakproof'",
        "'volatile'",
        "'parallel'",
        "'language'",
        "'acl'",
        "'config'",
        "'body_sha256'",
    )
    cursor = -1
    for token in trigger_order:
        cursor = trigger_catalog.find(token, cursor + 1)
        assert cursor >= 0, token
    for token in (
        "order by trigger_row.tgname",
        "order by function_row.proname",
        "pg_catalog.pg_get_triggerdef(",
        "pg_catalog.convert_to(function_row.prosrc, 'utf8')",
    ):
        assert token in trigger_catalog, token
    trigger_digest = cte("trigger_catalog_digest", "carrier_dependencies")
    assert "sha256(pg_catalog.convert_to( payload::text, 'utf8' ))" in trigger_digest

    triggers = cte("carrier_triggers", "carrier_function_inventory")
    assert "pg_catalog.count(*) = 3" in triggers
    assert "and not trigger_row.tgisinternal" in triggers
    for trigger_name in (
        "trg_research_stage4_no_signal_outcome_v1_validate",
        "trg_research_stage4_no_signal_outcome_v1_immutable",
        "trg_research_stage4_no_signal_outcome_v1_no_truncate",
    ):
        assert trigger_name in triggers

    view_columns = cte("carrier_columns", "raw_carrier")
    raw_columns = cte("raw_columns", "raw_constraint_status")
    for block in (view_columns, raw_columns):
        assert "attribute.attacl" in block
        assert "pg_catalog.cardinality(attribute.attacl)" in block

    raw_constraints = cte("raw_constraint_status", "raw_catalog_payload")
    for token in (
        "pg_catalog.count(*) = 27",
        "constraint_row.contype in ('c', 'f', 'p', 'u')",
        "constraint_row.contype not in ('c', 'f', 'p', 'u', 'n')",
        "constraint_row.convalidated is distinct from true",
        "constraint_row.conislocal",
        "constraint_row.coninhcount <> 0",
        "server_version_num",
        "->> 'conenforced'",
        "constraint_row.conparentid <> 0",
        "'not null %i'",
        "postgresql-generated not null names are not authority",
    ):
        assert token in raw_constraints, token
    assert normalized.count(
        "cross join raw_constraint_status constraints"
    ) == 2
    assert normalized.count("constraints.ready") == 2

    writer_role = cte("writer_role", "writer_source_authority")
    for token in (
        "not role_row.rolinherit",
        "not role_row.rolsuper",
        "not role_row.rolcreatedb",
        "not role_row.rolcreaterole",
        "not role_row.rolreplication",
        "not role_row.rolbypassrls",
        "pg_catalog.pg_auth_members",
        "pg_catalog.has_database_privilege",
        "pg_catalog.has_schema_privilege",
    ):
        assert token in writer_role
    writer_source = cte("writer_source_authority", "writer_unexpected_authority")
    assert "pg_catalog.count(*) = 4" in writer_source
    assert "pg_catalog.has_table_privilege" in writer_source
    assert "pg_catalog.aclexplode" in writer_source
    unexpected = cte("writer_unexpected_authority", "source_hardening")
    for token in (
        "pg_catalog.has_sequence_privilege",
        "pg_catalog.has_table_privilege",
        "pg_catalog.has_any_column_privilege",
    ):
        assert token in unexpected

    hardening = cte("source_hardening", "comment_receipts")
    for token in (
        "relrowsecurity",
        "relforcerowsecurity",
        "pg_catalog.pg_policy",
        "pg_catalog.pg_rewrite",
        "pg_catalog.count(*) = 1",
        "rewrite_row.rulename = '_return'",
        "pg_catalog.pg_inherits",
    ):
        assert token in hardening
    for token in (
        reader.NO_SIGNAL_REFERENCE_HASH_CONTRACT_VERSION,
        reader.NO_SIGNAL_OUTCOME_HASH_CONTRACT_VERSION,
        "view_raw_catalog_sha256",
        "table_raw_catalog_sha256",
        "view_trigger_catalog_sha256",
        "table_trigger_catalog_sha256",
        "no_signal_writer_authority_attested",
    ):
        assert token.lower() in normalized


class _FakeConnection:
    """A result router that also enforces the reader's zero-write SQL surface."""

    def __init__(self, driver):
        self.driver = driver
        self.closed = False

    def __enter__(self):
        self.driver.calls.append("connection_enter")
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()
        return False

    def execute(self, sql, params=None):
        normalized = " ".join(str(sql).split())
        lowered = normalized.lower()
        operation = _statement_operation(sql)
        if operation not in {"BEGIN", "SELECT", "WITH", "LOCK", "SET", "ROLLBACK"}:
            raise AssertionError(f"authoritative reader attempted SQL write: {operation}")
        if operation == "LOCK" and "access share" not in lowered:
            raise AssertionError("authoritative reader used a stronger-than-read lock")
        if operation == "SET" and not (
            "transaction" in lowered or "statement_timeout" in lowered
        ):
            raise AssertionError("authoritative reader changed a non-transaction setting")

        marker = None
        for candidate in (
            "begin",
            "schema_lock",
            "session",
            "probe_stage4",
            "probe_wave",
            "probe_outcomes",
            "probe_no_signal_outcomes",
            "outcomes_attestation",
            "no_signal_outcomes_attestation",
            "attestation",
            "load_projection_keys",
            "load_corpus_stage4",
            "load_corpus_wave",
            "load_corpus_outcomes",
            "load_corpus_no_signal_outcomes",
            "load_latest_terminal_projection",
            "load_stage4",
            "load_wave",
            "rollback",
        ):
            if f"formula_exploration_reader:{candidate}" in lowered:
                marker = candidate
                break
        if marker is None:
            if operation == "SET" and "statement_timeout" in lowered:
                marker = "statement_timeout"
            elif operation == "ROLLBACK":
                marker = "rollback"
            else:
                raise AssertionError(f"unexpected reader SQL: {normalized}")
        self.driver.calls.append(marker)
        self.driver.params.append((marker, deepcopy(params)))
        self.driver.sql_statements.append((marker, normalized))

        if marker == "session":
            return _Rows(self.driver.session)
        if marker == "attestation":
            return _Rows(self.driver.attestation)
        if marker == "outcomes_attestation":
            return _Rows(self.driver.outcomes_attestation)
        if marker == "no_signal_outcomes_attestation":
            return _Rows(self.driver.no_signal_outcomes_attestation)
        if marker == "load_projection_keys":
            return _Rows(self.driver.projection_rows)
        if marker == "load_corpus_stage4":
            return _Rows(self.driver.corpus_stage4_rows)
        if marker == "load_corpus_wave":
            return _Rows(self.driver.corpus_wave_rows)
        if marker == "load_corpus_outcomes":
            return _Rows(self.driver.outcome_rows)
        if marker == "load_corpus_no_signal_outcomes":
            return _Rows(self.driver.no_signal_outcome_rows)
        if marker == "load_latest_terminal_projection":
            return _Rows(self.driver.latest_terminal_rows)
        if marker == "load_stage4":
            return _Rows(self.driver.stage4_rows)
        if marker == "load_wave":
            return _Rows(self.driver.wave_rows)
        return _Rows()

    def rollback(self):
        self.driver.calls.append("rollback")
        if self.driver.rollback_error is not None:
            raise self.driver.rollback_error

    def commit(self):
        raise AssertionError("authoritative reader must never commit")

    def close(self):
        if not self.closed:
            self.driver.calls.append("close")
            self.closed = True
            if self.driver.close_error is not None:
                raise self.driver.close_error


class _FakeDriver:
    def __init__(
        self,
        *,
        stage4_rows=None,
        wave_rows=None,
        projection_rows=None,
        latest_terminal_rows=None,
        corpus_stage4_rows=None,
        corpus_wave_rows=None,
        outcome_rows=None,
        no_signal_outcome_rows=None,
    ):
        self.stage4_rows = list(
            _stage4_view_rows() if stage4_rows is None else stage4_rows
        )
        self.wave_rows = list(
            _wave_view_rows() if wave_rows is None else wave_rows
        )
        default_corpus_stage4 = (
            self.stage4_rows
            if corpus_stage4_rows is None
            else corpus_stage4_rows
        )
        default_corpus_wave = (
            self.wave_rows if corpus_wave_rows is None else corpus_wave_rows
        )
        self.corpus_stage4_rows = list(default_corpus_stage4)
        self.corpus_wave_rows = list(default_corpus_wave)
        self.projection_rows = list(
            _projection_key_rows(self.corpus_stage4_rows)
            if projection_rows is None
            else projection_rows
        )
        self.latest_terminal_rows = list(
            _latest_terminal_projection_rows(self.stage4_rows)
            if latest_terminal_rows is None
            else latest_terminal_rows
        )
        self.outcome_rows = list(
            _outcome_view_rows() if outcome_rows is None else outcome_rows
        )
        self.no_signal_outcome_rows = list(
            _no_signal_outcome_view_rows()
            if no_signal_outcome_rows is None
            else no_signal_outcome_rows
        )
        self.calls: list[str] = []
        self.params: list[tuple[str, object]] = []
        self.sql_statements: list[tuple[str, str]] = []
        self.connect_args = None
        self.connect_kwargs = None
        self.rollback_error = None
        self.close_error = None
        self.session = {
            "analysis_as_of_utc": AS_OF,
            "database_snapshot_id": "900:900:",
            "session_user": reader.TRUSTED_READER_ROLE,
            "current_user": reader.TRUSTED_READER_ROLE,
            "database_name": "research",
            "transaction_isolation": "repeatable read",
            "transaction_read_only": "on",
            "in_recovery": False,
        }
        self.attestation = {
            "reader_role_ready": True,
            "migration_022_attested": True,
            "migration_023_attested": True,
            "stage4_view_attested": True,
            "wave_view_attested": True,
            "raw_access_absent": True,
            "source_catalog_sha256": SOURCE_CATALOG_SHA256,
        }
        self.outcomes_attestation = {
            "outcomes_view_attested": True,
            "raw_outcomes_access_absent": True,
            "stage4_source_catalog_sha256": SOURCE_CATALOG_SHA256,
            "outcomes_view_definition_sha256": (
                OUTCOME_VIEW_DEFINITION_SHA256
            ),
        }
        self.no_signal_outcomes_attestation = {
            "no_signal_outcomes_view_attested": True,
            "no_signal_outcomes_table_attested": True,
            "no_signal_writer_authority_attested": True,
            "raw_no_signal_outcomes_access_absent": True,
            "stage4_source_catalog_sha256": SOURCE_CATALOG_SHA256,
            "no_signal_outcomes_view_definition_sha256": (
                NO_SIGNAL_VIEW_DEFINITION_SHA256
            ),
            "raw_catalog_sha256": NO_SIGNAL_RAW_CATALOG_SHA256,
            "view_raw_catalog_sha256": NO_SIGNAL_RAW_CATALOG_SHA256,
            "table_raw_catalog_sha256": NO_SIGNAL_RAW_CATALOG_SHA256,
            "trigger_catalog_sha256": NO_SIGNAL_TRIGGER_CATALOG_SHA256,
            "view_trigger_catalog_sha256": (
                NO_SIGNAL_TRIGGER_CATALOG_SHA256
            ),
            "table_trigger_catalog_sha256": (
                NO_SIGNAL_TRIGGER_CATALOG_SHA256
            ),
            "view_reference_hash_contract": (
                reader.NO_SIGNAL_REFERENCE_HASH_CONTRACT_VERSION
            ),
            "table_reference_hash_contract": (
                reader.NO_SIGNAL_REFERENCE_HASH_CONTRACT_VERSION
            ),
            "view_outcome_hash_contract": (
                reader.NO_SIGNAL_OUTCOME_HASH_CONTRACT_VERSION
            ),
            "table_outcome_hash_contract": (
                reader.NO_SIGNAL_OUTCOME_HASH_CONTRACT_VERSION
            ),
        }

    def connect(self, *args, **kwargs):
        self.connect_args = args
        self.connect_kwargs = dict(kwargs)
        self.calls.append("connect")
        return _FakeConnection(self)


class _PagedFakeConnection(_FakeConnection):
    def execute(self, sql, params=None):
        if "formula_exploration_reader:load_projection_keys" in str(sql).lower():
            index = self.driver.projection_page_index
            if index >= len(self.driver.projection_pages):
                raise AssertionError("full corpus requested an unexpected page")
            self.driver.projection_rows = list(
                self.driver.projection_pages[index]
            )
            self.driver.projection_page_index += 1
        return super().execute(sql, params)


class _PagedFakeDriver(_FakeDriver):
    def __init__(self, projection_pages, **kwargs):
        super().__init__(**kwargs)
        self.projection_pages = [list(page) for page in projection_pages]
        self.projection_page_index = 0

    def connect(self, *args, **kwargs):
        self.connect_args = args
        self.connect_kwargs = dict(kwargs)
        self.calls.append("connect")
        return _PagedFakeConnection(self)


def _stage4_view_rows(signals=None, *, evaluations=None):
    supplied = list(
        signals
        if signals is not None
        else (
            fixtures._signal(101, exploration.MAX_PAIN_EVENT_TYPE),
            fixtures._signal(102, exploration.MAGNET_EVENT_TYPE),
        )
    )
    projection = fixtures._projection(supplied, evaluations=evaluations)
    archive = fixtures._archive_set()
    rows = []
    for event in (*supplied, projection):
        row = deepcopy(event)
        metadata = event["engine_snapshot"]["signal_snapshot"]
        claim = (
            event["engine_snapshot"]["projection"]
            if event["event_type"] == exploration.PROJECTION_EVENT_TYPE
            else metadata["archive_reference"]
        )
        row.update(
            {
                "event_created_at": fixtures.DECISION + timedelta(seconds=1),
                "claimed_snapshot_set_id": claim["snapshot_set_id"],
                "claimed_snapshot_key": claim["snapshot_key"],
                "archive_snapshot_set_id": archive["snapshot_set_id"],
                "archive_snapshot_key": archive["snapshot_key"],
                "archive_payload_sha256": archive["payload_sha256"],
                "archive_cycle_time_utc": archive["cycle_time_utc"],
                "archive_available_at_utc": archive["available_at_utc"],
                "archive_source": archive["source"],
                "archive_research_eligible": archive["research_eligible"],
                "archive_created_at_utc": fixtures.AVAILABLE,
            }
        )
        rows.append(row)
    return rows


def _projection_key_rows(stage4_rows=None):
    source_rows = list(
        _stage4_view_rows() if stage4_rows is None else stage4_rows
    )
    return [
        {
            "projection_event_id": row["event_id"],
            "snapshot_key": row["claimed_snapshot_key"],
            "projection_decision_time_utc": row["alert_time_utc"],
            "projection_created_at_utc": row["event_created_at"],
            "event_type": row["event_type"],
        }
        for row in source_rows
        if row.get("event_type") == exploration.PROJECTION_EVENT_TYPE
    ]


def _latest_terminal_projection_rows(stage4_rows=None):
    source_rows = list(
        _stage4_view_rows() if stage4_rows is None else stage4_rows
    )
    projections = [
        row
        for row in source_rows
        if row.get("event_type") == exploration.PROJECTION_EVENT_TYPE
    ]
    if not projections:
        return []
    projections.sort(
        key=lambda row: (row["alert_time_utc"], row["event_id"]),
        reverse=True,
    )
    row = projections[0]
    payload = row["engine_snapshot"]["projection"]
    return [
        {
            "projection_event_id": row["event_id"],
            "projection_event_fingerprint": row["event_fingerprint"],
            "snapshot_set_id": row["claimed_snapshot_set_id"],
            "snapshot_key": row["claimed_snapshot_key"],
            "projection_decision_time_utc": row["alert_time_utc"],
            "projection_created_at_utc": row["event_created_at"],
            "archive_cycle_time_utc": row["archive_cycle_time_utc"],
            "event_type": row["event_type"],
            "projection_status": payload["status"],
        }
    ]


def _outcome_view_rows():
    rows = []
    for event_id in (101, 102):
        row = fixtures._outcome(event_id)
        row["outcome_created_at"] = (
            fixtures.DECISION + timedelta(minutes=60, seconds=1)
        )
        rows.append(row)
    return rows


def _explicit_no_signal_observation():
    stage4_rows = _stage4_view_rows()
    events = [reader._event_from_stage4_row(row) for row in stage4_rows]
    projection = next(
        row
        for row in events
        if row["event_type"] == exploration.PROJECTION_EVENT_TYPE
    )
    signals = [
        row
        for row in events
        if row["event_type"] in exploration.SIGNAL_EVENT_TYPES
    ]
    return next(
        item
        for item in exploration.build_stage4_frames(
            projection,
            fixtures._archive_set(),
            signals,
            analysis_as_of_utc=AS_OF,
        )
        if item.to_dict()["direction"] == "SHORT"
        and item.to_dict()["explicit_no_signal"] is True
    )


def _no_signal_outcome_view_rows():
    observation = _explicit_no_signal_observation()
    body = observation.to_dict()
    row = fixtures._no_signal_outcome(observation)
    reference_receipt = {
        "contract_version": (
            exploration.STAGE4_NO_SIGNAL_OUTCOME_REFERENCE_POLICY_VERSION
        ),
        "projection_event_id": body["projection_event_id"],
        "projection_event_fingerprint": body[
            "projection_event_fingerprint"
        ],
        "snapshot_set_id": body["snapshot_set_id"],
        "snapshot_key": body["snapshot_key"],
        "set_payload_sha256": fixtures._h("archive-payload-71"),
        "symbol": body["symbol"],
        "symbol_manifest_payload_sha256": fixtures._h("manifest-ETH-71"),
        "source_timeframe": "12h",
        "snapshot_row_id": 701,
        "snapshot_row_payload_sha256": fixtures._h("row-ETH-12h-71"),
        "official_price": {
            "price": 2500.0,
            "source": "binance_spot",
            "exchange": "binance",
            "market": "spot",
            "pair": "ETHUSDT",
            "instrument": "ETHUSDT",
            "interval": "1m",
            "fetched_at_utc": reader._iso(
                fixtures.DECISION - timedelta(seconds=5),
                field="fixture fetched_at_utc",
            ),
            "observed_at_utc": reader._iso(
                fixtures.DECISION - timedelta(seconds=10),
                field="fixture observed_at_utc",
            ),
            "candle_open_time_utc": None,
            "candle_close_time_utc": None,
            "policy_status": "PASS",
        },
    }
    cell_identity = hashlib.sha256(
        (
            "stage4-explicit-no-signal-outcome-carrier-v1|"
            f"{body['projection_event_fingerprint']}|{body['symbol']}|"
            f"{body['direction']}|60"
        ).encode("utf-8")
    ).hexdigest()
    row.update(
        {
            "reference_receipt": reference_receipt,
            "cell_identity_sha256": cell_identity,
            "outcome_created_at": (
                fixtures.DECISION + timedelta(minutes=60, seconds=1)
            ),
        }
    )
    reference_receipt_sha256 = reader._no_signal_reference_receipt_hash(
        reference_receipt,
        projection_event_id=body["projection_event_id"],
        projection_event_fingerprint=body["projection_event_fingerprint"],
        snapshot_set_id=body["snapshot_set_id"],
        snapshot_key=body["snapshot_key"],
        symbol=body["symbol"],
        reference_price=row["reference_price"],
    )
    row["reference_receipt_sha256"] = reference_receipt_sha256
    row["outcome_payload_sha256"] = reader._no_signal_outcome_payload_hash(
        row,
        projection_event_id=body["projection_event_id"],
        projection_event_fingerprint=body["projection_event_fingerprint"],
        snapshot_set_id=body["snapshot_set_id"],
        snapshot_key=body["snapshot_key"],
        symbol=body["symbol"],
        direction=body["direction"],
        horizon_minutes=60,
        decision_time_utc=reader._utc(
            row["decision_time_utc"], field="fixture decision_time_utc"
        ),
        measured_at_utc=reader._utc(
            row["measured_at_utc"], field="fixture measured_at_utc"
        ),
        cell_identity_sha256=cell_identity,
        reference_receipt_sha256=reference_receipt_sha256,
    )
    assert set(reader.NO_SIGNAL_OUTCOME_VIEW_COLUMNS) <= set(row)
    return [row]


def _source_provenance(symbol: str) -> dict:
    return {
        "source": "binance_spot",
        "upstream_source": "binance_spot",
        "quality_status": "PASS",
        "price_exchange": "binance",
        "price_market": "spot",
        "price_pair": f"{symbol}USDT",
        "price_instrument_id": f"{symbol}USDT",
        "price_timeframe": "1m",
        "fallback_used": False,
        "fallback_policy": "PROVIDER_ATTESTED_NO_FALLBACK",
    }


def _wave_anchor_at(
    symbol: str,
    price: int,
    eligible: datetime,
) -> movement.NeutralPriceAnchor:
    closed = eligible - timedelta(milliseconds=1)
    return movement.NeutralPriceAnchor.build_prospective(
        symbol=symbol,
        eligible_at_utc=eligible,
        decision_time_utc=eligible + timedelta(seconds=15),
        source_price_candle_open_utc=eligible - timedelta(minutes=1),
        source_price_candle_close_utc=closed,
        observed_at_utc=closed,
        refresh_completed_at_utc=eligible + timedelta(seconds=2),
        price=price,
        source_provenance=_source_provenance(symbol),
    )


def _wave_anchor(symbol: str, price: int) -> movement.NeutralPriceAnchor:
    return _wave_anchor_at(
        symbol,
        price,
        fixtures.CYCLE + timedelta(minutes=2),
    )


def _wave_view_row(
    anchor: movement.NeutralPriceAnchor,
    identity: movement.MovementIdentity,
) -> dict:
    advanced = movement.advance(None, anchor, identity=identity)
    transition = advanced.transitions[0]
    membership = advanced.memberships[0]
    member_payload = membership.to_dict()
    transition_payload = transition.to_dict()
    anchor_payload = anchor.to_dict()
    return {
        "membership_receipt_sha256": membership.membership_receipt_sha256,
        "emitted_by_transition_receipt_sha256": (
            transition.transition_receipt_sha256
        ),
        "membership_contract_version": membership.contract_version,
        "membership_stream_id": membership.stream_id,
        "membership_movement_id": membership.movement_id,
        "membership_anchor_id": membership.anchor_id,
        "membership_anchor_receipt_sha256": membership.anchor_receipt_sha256,
        "membership_ordinal": membership.ordinal,
        "membership_classification": membership.classification,
        "membership_eligible_at_utc": membership.eligible_at_utc,
        "membership_decision_time_utc": membership.decision_time_utc,
        "membership_price": membership.price,
        "membership_receipt": member_payload,
        "membership_created_at_utc": membership.decision_time_utc,
        "transition_receipt_sha256": transition.transition_receipt_sha256,
        "previous_transition_receipt_sha256": (
            transition.previous_transition_receipt_sha256
        ),
        "transition_contract_version": transition.contract_version,
        "transition_chain_ordinal": 1,
        "transition_type": transition.transition_type,
        "transition_stream_id": transition.post_state.stream_id,
        "transition_namespace": transition.post_state.namespace,
        "transition_symbol": transition.post_state.symbol,
        "transition_movement_id": transition.post_state.movement_id,
        "transition_trigger_anchor_id": transition.trigger_anchor_id,
        "transition_trigger_eligible_at_utc": (
            transition.trigger_eligible_at_utc
        ),
        "transition_trigger_decision_time_utc": (
            transition.trigger_decision_time_utc
        ),
        "transition_pre_state_sha256": transition.pre_state_sha256,
        "transition_post_state_sha256": transition.post_state.state_sha256,
        "transition_post_state": transition.post_state.to_dict(),
        "transition_receipt": transition_payload,
        "transition_created_at_utc": transition.trigger_decision_time_utc,
        "anchor_id": anchor.anchor_id,
        "anchor_receipt_sha256": anchor.anchor_receipt_sha256,
        "anchor_contract_version": anchor.contract_version,
        "anchor_symbol": anchor.symbol,
        "anchor_origin": anchor.origin,
        "anchor_sampler_version": anchor.sampler_version,
        "anchor_eligible_at_utc": anchor.eligible_at_utc,
        "anchor_decision_time_utc": anchor.decision_time_utc,
        "anchor_source_price_candle_open_utc": (
            anchor.source_price_candle_open_utc
        ),
        "anchor_source_price_candle_close_utc": (
            anchor.source_price_candle_close_utc
        ),
        "anchor_observed_at_utc": anchor.observed_at_utc,
        "anchor_refresh_completed_at_utc": anchor.refresh_completed_at_utc,
        "anchor_price": anchor.price,
        "anchor_source": anchor.source,
        "anchor_upstream_source": anchor.upstream_source,
        "anchor_price_exchange": anchor.price_exchange,
        "anchor_price_market": anchor.price_market,
        "anchor_price_pair": anchor.price_pair,
        "anchor_price_instrument_id": anchor.price_instrument_id,
        "anchor_price_timeframe": anchor.price_timeframe,
        "anchor_quality_status": anchor.quality_status,
        "anchor_fallback_used": anchor.fallback_used,
        "anchor_fallback_policy": anchor.fallback_policy,
        "anchor_price_candle_identity_basis": (
            anchor.price_candle_identity_basis
        ),
        "anchor_source_input_fingerprint": anchor.source_input_fingerprint,
        "anchor_source_record_created_at_utc": (
            anchor.source_record_created_at_utc
        ),
        "anchor_receipt": anchor_payload,
        "anchor_created_at_utc": anchor.decision_time_utc,
    }


def _wave_view_rows():
    return [
        _wave_view_row(
            _wave_anchor("ETH", 2500), movement.MovementIdentity.for_symbol("ETH")
        ),
        _wave_view_row(
            _wave_anchor("BTC", 60_000), movement.MovementIdentity.btc_parent()
        ),
    ]


def _set_membership_row(
    row: dict,
    member: movement.MovementMembership,
    *,
    emitted_by: str | None = None,
) -> None:
    row.update(
        {
            "membership_receipt_sha256": member.membership_receipt_sha256,
            "membership_contract_version": member.contract_version,
            "membership_stream_id": member.stream_id,
            "membership_movement_id": member.movement_id,
            "membership_anchor_id": member.anchor_id,
            "membership_anchor_receipt_sha256": (
                member.anchor_receipt_sha256
            ),
            "membership_ordinal": member.ordinal,
            "membership_classification": member.classification,
            "membership_eligible_at_utc": member.eligible_at_utc,
            "membership_decision_time_utc": member.decision_time_utc,
            "membership_price": member.price,
            "membership_receipt": member.to_dict(),
            "membership_created_at_utc": member.decision_time_utc,
        }
    )
    if emitted_by is not None:
        row["emitted_by_transition_receipt_sha256"] = emitted_by


def _set_transition_row(
    row: dict,
    transition: movement.MovementTransition,
) -> None:
    row.update(
        {
            "transition_receipt_sha256": transition.transition_receipt_sha256,
            "previous_transition_receipt_sha256": (
                transition.previous_transition_receipt_sha256
            ),
            "transition_contract_version": transition.contract_version,
            "transition_chain_ordinal": 1,
            "transition_type": transition.transition_type,
            "transition_stream_id": transition.stream_id,
            "transition_namespace": transition.post_state.namespace,
            "transition_symbol": transition.post_state.symbol,
            "transition_movement_id": transition.movement_id,
            "transition_trigger_anchor_id": transition.trigger_anchor_id,
            "transition_trigger_eligible_at_utc": (
                transition.trigger_eligible_at_utc
            ),
            "transition_trigger_decision_time_utc": (
                transition.trigger_decision_time_utc
            ),
            "transition_pre_state_sha256": transition.pre_state_sha256,
            "transition_post_state_sha256": transition.post_state.state_sha256,
            "transition_post_state": transition.post_state.to_dict(),
            "transition_receipt": transition.to_dict(),
            "transition_created_at_utc": transition.trigger_decision_time_utc,
        }
    )


def _rebuilt_membership(row: dict, **changes) -> movement.MovementMembership:
    original = movement.MovementMembership.from_dict(
        row["membership_receipt"]
    )
    fields = {
        "stream_id": original.stream_id,
        "movement_id": original.movement_id,
        "anchor_id": original.anchor_id,
        "anchor_receipt_sha256": original.anchor_receipt_sha256,
        "ordinal": original.ordinal,
        "classification": original.classification,
        "eligible_at_utc": original.eligible_at_utc,
        "decision_time_utc": original.decision_time_utc,
        "price": original.price,
    }
    fields.update(changes)
    return movement._make_membership(**fields)


def _cross_symbol_wave_row() -> dict:
    """Build receipts that individually verify but join ETH to BTC state."""

    eth_row, btc_row = _wave_view_rows()
    anchor = movement.NeutralPriceAnchor.from_dict(eth_row["anchor_receipt"])
    btc_transition = movement.MovementTransition.from_dict(
        btc_row["transition_receipt"]
    )
    transition = movement._make_transition(
        previous_transition_receipt_sha256=None,
        transition_type=movement.OPENED,
        trigger_anchor_id=anchor.anchor_id,
        trigger_eligible_at_utc=anchor.eligible_at_utc,
        trigger_decision_time_utc=anchor.decision_time_utc,
        pre_state_sha256=None,
        post_state=btc_transition.post_state,
    )
    member = movement._make_membership(
        stream_id=transition.stream_id,
        movement_id=transition.movement_id,
        anchor_id=anchor.anchor_id,
        anchor_receipt_sha256=anchor.anchor_receipt_sha256,
        ordinal=1,
        classification=movement.START_MEMBER,
        eligible_at_utc=anchor.eligible_at_utc,
        decision_time_utc=anchor.decision_time_utc,
        price=anchor.price,
    )
    result = deepcopy(eth_row)
    _set_transition_row(result, transition)
    _set_membership_row(
        result,
        member,
        emitted_by=transition.transition_receipt_sha256,
    )
    return result


def _post_state_link_mismatch_rows() -> list[dict]:
    row = _producer_direction_established_count3_row()
    chain_ordinal = row["transition_chain_ordinal"]
    original = movement.MovementTransition.from_dict(
        row["transition_receipt"]
    )
    state = original.post_state
    identity = movement.MovementIdentity._build(
        namespace=state.namespace,
        symbol=state.symbol,
    )
    unlinked_state = movement._make_state(
        identity=identity,
        movement_id=state.movement_id,
        status=state.status,
        direction=state.direction,
        started_anchor_id=state.started_anchor_id,
        started_eligible_at_utc=state.started_eligible_at_utc,
        started_decision_time_utc=state.started_decision_time_utc,
        start_price=state.start_price,
        extreme_anchor_id=fixtures._h("unlinked-post-state-anchor"),
        extreme_eligible_at_utc=state.extreme_eligible_at_utc,
        extreme_price=state.extreme_price,
        last_member_anchor_id=fixtures._h("unlinked-post-state-anchor"),
        last_member_eligible_at_utc=state.last_member_eligible_at_utc,
        last_member_decision_time_utc=state.last_member_decision_time_utc,
        last_member_price=state.last_member_price,
        member_count=state.member_count,
        consecutive_non_extremes=state.consecutive_non_extremes,
    )
    transition = movement._make_transition(
        previous_transition_receipt_sha256=(
            original.previous_transition_receipt_sha256
        ),
        transition_type=original.transition_type,
        trigger_anchor_id=original.trigger_anchor_id,
        trigger_eligible_at_utc=original.trigger_eligible_at_utc,
        trigger_decision_time_utc=original.trigger_decision_time_utc,
        pre_state_sha256=original.pre_state_sha256,
        post_state=unlinked_state,
    )
    _set_transition_row(row, transition)
    row["transition_chain_ordinal"] = chain_ordinal
    row["emitted_by_transition_receipt_sha256"] = (
        transition.transition_receipt_sha256
    )
    return [row, _wave_view_rows()[1]]


def _canonical_impossible_continuation(
    transition_type: str,
) -> dict:
    """Return individually canonical receipts with impossible transition semantics."""

    row = _wave_view_rows()[0]
    anchor = movement.NeutralPriceAnchor.from_dict(row["anchor_receipt"])
    identity = movement.MovementIdentity.for_symbol(anchor.symbol)
    started_anchor_id = fixtures._h(
        f"impossible-{transition_type}-started-anchor"
    )
    movement_id = movement._sha256(
        "market-movement-identity",
        {
            "stream_id": identity.stream_id,
            "started_anchor_id": started_anchor_id,
        },
    )
    started_eligible = anchor.eligible_at_utc - timedelta(minutes=30)
    started_decision = anchor.decision_time_utc - timedelta(minutes=30)
    if transition_type in {
        movement.DIRECTION_ESTABLISHED,
        movement.EXTREME_EXTENDED,
    }:
        # Canonical MovementState, but impossible for either transition:
        # direction is still pending and the non-extreme streak is already 1.
        direction = movement.PENDING_DIRECTION
        start_price = anchor.price
        extreme_anchor_id = started_anchor_id
        extreme_eligible = started_eligible
        extreme_price = start_price
        consecutive_non_extremes = 1
        classification = (
            movement.DIRECTIONAL_EXTREME_MEMBER
            if transition_type == movement.DIRECTION_ESTABLISHED
            else movement.EXTREME_EXTENSION_MEMBER
        )
    else:
        # Canonical directional-extreme state mislabeled NON_EXTREME_OBSERVED.
        direction = movement.UP_DIRECTION
        start_price = anchor.price - Decimal("100")
        extreme_anchor_id = anchor.anchor_id
        extreme_eligible = anchor.eligible_at_utc
        extreme_price = anchor.price
        consecutive_non_extremes = 0
        classification = movement.NON_EXTREME_MEMBER
    state = movement._make_state(
        identity=identity,
        movement_id=movement_id,
        status=movement.OPEN_STATUS,
        direction=direction,
        started_anchor_id=started_anchor_id,
        started_eligible_at_utc=started_eligible,
        started_decision_time_utc=started_decision,
        start_price=start_price,
        extreme_anchor_id=extreme_anchor_id,
        extreme_eligible_at_utc=extreme_eligible,
        extreme_price=extreme_price,
        last_member_anchor_id=anchor.anchor_id,
        last_member_eligible_at_utc=anchor.eligible_at_utc,
        last_member_decision_time_utc=anchor.decision_time_utc,
        last_member_price=anchor.price,
        member_count=2,
        consecutive_non_extremes=consecutive_non_extremes,
    )
    transition = movement._make_transition(
        previous_transition_receipt_sha256=fixtures._h(
            f"impossible-{transition_type}-previous-transition"
        ),
        transition_type=transition_type,
        trigger_anchor_id=anchor.anchor_id,
        trigger_eligible_at_utc=anchor.eligible_at_utc,
        trigger_decision_time_utc=anchor.decision_time_utc,
        pre_state_sha256=fixtures._h(
            f"impossible-{transition_type}-pre-state"
        ),
        post_state=state,
    )
    member = movement._make_membership(
        stream_id=state.stream_id,
        movement_id=state.movement_id,
        anchor_id=anchor.anchor_id,
        anchor_receipt_sha256=anchor.anchor_receipt_sha256,
        ordinal=state.member_count,
        classification=classification,
        eligible_at_utc=anchor.eligible_at_utc,
        decision_time_utc=anchor.decision_time_utc,
        price=anchor.price,
    )
    _set_transition_row(row, transition)
    row["transition_chain_ordinal"] = 2
    _set_membership_row(
        row,
        member,
        emitted_by=transition.transition_receipt_sha256,
    )
    return row


def _producer_direction_established_count3_row() -> dict:
    """Build OPENED -> equal NON_EXTREME -> direction via the producer."""

    identity = movement.MovementIdentity.for_symbol("ETH")
    direction_slot = fixtures.CYCLE + timedelta(minutes=2)
    opened_anchor = _wave_anchor_at(
        "ETH", 2500, direction_slot - timedelta(minutes=60)
    )
    equal_anchor = _wave_anchor_at(
        "ETH", 2500, direction_slot - timedelta(minutes=30)
    )
    direction_anchor = _wave_anchor_at("ETH", 2600, direction_slot)
    opened = movement.advance_market_movement(
        None,
        opened_anchor,
        identity=identity,
    )
    equal = movement.advance_market_movement(
        opened.cursor,
        equal_anchor,
    )
    direction = movement.advance_market_movement(
        equal.cursor,
        direction_anchor,
    )
    assert opened.transitions[0].transition_type == movement.OPENED
    assert (
        equal.transitions[0].transition_type
        == movement.NON_EXTREME_OBSERVED
    )
    assert (
        direction.transitions[0].transition_type
        == movement.DIRECTION_ESTABLISHED
    )
    assert direction.transitions[0].post_state.member_count == 3
    row = _wave_view_row(direction_anchor, identity)
    _set_transition_row(row, direction.transitions[0])
    row["transition_chain_ordinal"] = 3
    _set_membership_row(
        row,
        direction.memberships[0],
        emitted_by=direction.transitions[0].transition_receipt_sha256,
    )
    return row


def _impossible_pending_non_extreme_row(
    *, member_count: int, equal_price: bool
) -> dict:
    row = _wave_view_rows()[0]
    anchor = movement.NeutralPriceAnchor.from_dict(row["anchor_receipt"])
    identity = movement.MovementIdentity.for_symbol(anchor.symbol)
    started_anchor_id = fixtures._h(
        f"pending-non-extreme-start-{member_count}-{equal_price}"
    )
    movement_id = movement._sha256(
        "market-movement-identity",
        {
            "stream_id": identity.stream_id,
            "started_anchor_id": started_anchor_id,
        },
    )
    started_eligible = anchor.eligible_at_utc - timedelta(
        minutes=30 * (member_count - 1)
    )
    started_decision = anchor.decision_time_utc - timedelta(
        minutes=30 * (member_count - 1)
    )
    start_price = (
        anchor.price if equal_price else anchor.price - Decimal("100")
    )
    state = movement._make_state(
        identity=identity,
        movement_id=movement_id,
        status=movement.OPEN_STATUS,
        direction=movement.PENDING_DIRECTION,
        started_anchor_id=started_anchor_id,
        started_eligible_at_utc=started_eligible,
        started_decision_time_utc=started_decision,
        start_price=start_price,
        extreme_anchor_id=started_anchor_id,
        extreme_eligible_at_utc=started_eligible,
        extreme_price=start_price,
        last_member_anchor_id=anchor.anchor_id,
        last_member_eligible_at_utc=anchor.eligible_at_utc,
        last_member_decision_time_utc=anchor.decision_time_utc,
        last_member_price=anchor.price,
        member_count=member_count,
        consecutive_non_extremes=1,
    )
    transition = movement._make_transition(
        previous_transition_receipt_sha256=fixtures._h(
            f"pending-non-extreme-previous-{member_count}-{equal_price}"
        ),
        transition_type=movement.NON_EXTREME_OBSERVED,
        trigger_anchor_id=anchor.anchor_id,
        trigger_eligible_at_utc=anchor.eligible_at_utc,
        trigger_decision_time_utc=anchor.decision_time_utc,
        pre_state_sha256=fixtures._h(
            f"pending-non-extreme-pre-{member_count}-{equal_price}"
        ),
        post_state=state,
    )
    member = movement._make_membership(
        stream_id=state.stream_id,
        movement_id=state.movement_id,
        anchor_id=anchor.anchor_id,
        anchor_receipt_sha256=anchor.anchor_receipt_sha256,
        ordinal=member_count,
        classification=movement.NON_EXTREME_MEMBER,
        eligible_at_utc=anchor.eligible_at_utc,
        decision_time_utc=anchor.decision_time_utc,
        price=anchor.price,
    )
    _set_transition_row(row, transition)
    row["transition_chain_ordinal"] = member_count
    _set_membership_row(
        row,
        member,
        emitted_by=transition.transition_receipt_sha256,
    )
    return row


def _rows_with_duplicate_transition():
    rows = _wave_view_rows()
    duplicate = deepcopy(rows[0])
    original = movement.MovementMembership.from_dict(
        duplicate["membership_receipt"]
    )
    second_member = movement._make_membership(
        stream_id=original.stream_id,
        movement_id=original.movement_id,
        anchor_id=original.anchor_id,
        anchor_receipt_sha256=original.anchor_receipt_sha256,
        ordinal=original.ordinal,
        classification=movement.DIRECTIONAL_EXTREME_MEMBER,
        eligible_at_utc=original.eligible_at_utc,
        decision_time_utc=original.decision_time_utc,
        price=original.price,
    )
    duplicate.update(
        {
            "membership_receipt_sha256": (
                second_member.membership_receipt_sha256
            ),
            "membership_contract_version": second_member.contract_version,
            "membership_stream_id": second_member.stream_id,
            "membership_movement_id": second_member.movement_id,
            "membership_anchor_id": second_member.anchor_id,
            "membership_anchor_receipt_sha256": (
                second_member.anchor_receipt_sha256
            ),
            "membership_ordinal": second_member.ordinal,
            "membership_classification": second_member.classification,
            "membership_eligible_at_utc": second_member.eligible_at_utc,
            "membership_decision_time_utc": second_member.decision_time_utc,
            "membership_price": second_member.price,
            "membership_receipt": second_member.to_dict(),
        }
    )
    rows.append(duplicate)
    return rows


def _with_driver(driver: _FakeDriver, callback):
    original_psycopg = reader.psycopg
    original_dict_row = reader.dict_row
    reader.psycopg = driver
    reader.dict_row = object()
    try:
        return callback()
    finally:
        reader.psycopg = original_psycopg
        reader.dict_row = original_dict_row


def _load(driver: _FakeDriver):
    database_url = "postgresql://exploration-reader@db.example/research"
    with patch.dict(
        os.environ,
        {
            "RESEARCH_DATABASE_URL": database_url,
            "RESEARCH_SIGNAL_SNAPSHOT_DATABASE_URL": database_url,
            "RESEARCH_MARKET_MOVEMENT_DATABASE_URL": database_url,
            "RESEARCH_USE_PRIMARY_DATABASE": "0",
        },
        clear=False,
    ):
        return _with_driver(
            driver,
            lambda: reader.load_authoritative_stage4_wave(
                SNAPSHOT_KEY,
                database_url=database_url,
            ),
        )


def _load_current(driver: _FakeDriver):
    database_url = "postgresql://exploration-reader@db.example/research"
    with patch.dict(
        os.environ,
        {
            "RESEARCH_DATABASE_URL": database_url,
            "RESEARCH_SIGNAL_SNAPSHOT_DATABASE_URL": database_url,
            "RESEARCH_MARKET_MOVEMENT_DATABASE_URL": database_url,
            "RESEARCH_USE_PRIMARY_DATABASE": "0",
        },
        clear=False,
    ):
        return _with_driver(
            driver,
            lambda: reader.load_latest_authoritative_stage4_current(
                database_url=database_url,
            ),
        )


def _load_corpus(
    driver: _FakeDriver,
    *,
    horizon_minutes: int = 60,
    lookback_days: int = 120,
    projection_limit: int = 128,
    before_cursor=None,
):
    database_url = "postgresql://exploration-reader@db.example/research"
    with patch.dict(
        os.environ,
        {
            "RESEARCH_DATABASE_URL": database_url,
            "RESEARCH_SIGNAL_SNAPSHOT_DATABASE_URL": database_url,
            "RESEARCH_MARKET_MOVEMENT_DATABASE_URL": database_url,
            "RESEARCH_USE_PRIMARY_DATABASE": "0",
        },
        clear=False,
    ):
        return _with_driver(
            driver,
            lambda: reader.load_authoritative_stage4_corpus(
                horizon_minutes=horizon_minutes,
                lookback_days=lookback_days,
                projection_limit=projection_limit,
                before_cursor=before_cursor,
                database_url=database_url,
            ),
        )


def _load_complete_corpus(
    driver: _FakeDriver,
    *,
    horizon_minutes: int = 60,
    lookback_days: int = 120,
    projection_limit: int = 128,
    wall_budget_ms: int = reader.DEFAULT_FULL_CORPUS_WALL_BUDGET_MS,
):
    database_url = "postgresql://exploration-reader@db.example/research"
    with patch.dict(
        os.environ,
        {
            "RESEARCH_DATABASE_URL": database_url,
            "RESEARCH_SIGNAL_SNAPSHOT_DATABASE_URL": database_url,
            "RESEARCH_MARKET_MOVEMENT_DATABASE_URL": database_url,
            "RESEARCH_USE_PRIMARY_DATABASE": "0",
        },
        clear=False,
    ):
        return _with_driver(
            driver,
            lambda: reader.load_complete_authoritative_stage4_corpus(
                horizon_minutes=horizon_minutes,
                lookback_days=lookback_days,
                projection_limit=projection_limit,
                wall_budget_ms=wall_budget_ms,
                database_url=database_url,
            ),
        )


def _load_with_target(
    driver: _FakeDriver,
    database_url: str,
    *,
    aligned_database_url: str = "",
):
    with patch.dict(
        os.environ,
        {
            "RESEARCH_DATABASE_URL": aligned_database_url,
            "RESEARCH_SIGNAL_SNAPSHOT_DATABASE_URL": aligned_database_url,
            "RESEARCH_MARKET_MOVEMENT_DATABASE_URL": aligned_database_url,
            "RESEARCH_USE_PRIMARY_DATABASE": "0",
            "DATABASE_URL": "",
        },
        clear=False,
    ):
        return _with_driver(
            driver,
            lambda: reader.load_authoritative_stage4_wave(
                SNAPSHOT_KEY,
                database_url=database_url,
            ),
        )


def _expect_call_failure(callback, contains: str = "") -> None:
    try:
        callback()
    except (RuntimeError, TypeError, ValueError) as exc:
        if contains:
            assert contains.lower() in str(exc).lower(), (contains, str(exc))
    else:
        raise AssertionError("unsafe authoritative-reader input was accepted")


def _expect_failure(driver: _FakeDriver, contains: str = "") -> None:
    _expect_call_failure(lambda: _load(driver), contains)


def _check_complete_corpus_single_snapshot_traversal() -> None:
    first_key = _projection_key_rows()[0]
    older_unreturned_key = deepcopy(first_key)
    older_unreturned_key["projection_event_id"] = 899
    older_unreturned_key["snapshot_key"] = fixtures._h(
        "full-corpus-older-snapshot"
    )
    older_unreturned_key["projection_decision_time_utc"] = (
        fixtures.DECISION - timedelta(minutes=30)
    )
    older_unreturned_key["projection_created_at_utc"] = (
        fixtures.DECISION - timedelta(minutes=30) + timedelta(seconds=1)
    )
    driver = _PagedFakeDriver(
        projection_pages=[
            [first_key, older_unreturned_key],
            [],
        ]
    )
    complete = _load_complete_corpus(driver, projection_limit=1)
    receipt_only = complete.receipt_dict()
    assert "observations" not in receipt_only
    assert type(complete.candidate_observations) is tuple
    assert all(
        type(row)
        is reader.stage4_candidate_search.CompactStage4CandidateObservation
        and not hasattr(row, "__dict__")
        for row in complete.candidate_observations
    )
    storage = receipt_only["observation_storage"]
    assert storage["count"] == len(complete.candidate_observations)
    assert storage["ordered_chain_sha256"] == (
        reader.stage4_candidate_search.compact_observation_chain_sha256(
            complete.candidate_observations
        )
    )
    _expect_call_failure(
        lambda: reader.CompleteAuthoritativeStage4CorpusResult(
            complete.attestation_receipt_sha256,
            complete._receipt_json,
            list(complete.candidate_observations),
        ),
        "immutable tuple",
    )
    embedded = complete.receipt_dict()
    embedded["observations"] = []
    _expect_call_failure(
        lambda: reader.CompleteAuthoritativeStage4CorpusResult._from_payload(
            embedded, complete.candidate_observations
        ),
        "embeds observation rows",
    )
    if complete.candidate_observations:
        valid = complete.candidate_observations[0]
        mutable_nested = reader.stage4_candidate_search.CompactStage4CandidateObservation(
            observation_id=valid.observation_id,
            projection_event_id=valid.projection_event_id,
            projection_decision_time_utc=valid.projection_decision_time_utc,
            symbol=valid.symbol,
            direction=valid.direction,
            features=SimpleNamespace(
                true_mask=valid.features.true_mask,
                combined_vote_count=valid.features.combined_vote_count,
            ),
            wave_binding=valid.wave_binding,
            outcome=valid.outcome,
        )
        _expect_call_failure(
            lambda: reader.stage4_candidate_search.compact_observation_chain_sha256(
                (mutable_nested,)
            ),
            "type is invalid",
        )
    payload = complete.to_dict()
    traversal = payload["traversal"]
    expected_next = {
        "projection_decision_time_utc": fixtures._iso(fixtures.DECISION),
        "projection_event_id": first_key["projection_event_id"],
    }
    assert driver.calls.count("connect") == 1
    assert driver.calls.count("begin") == 1
    assert driver.calls.count("session") == 1
    assert driver.calls.count("attestation") == 1
    assert driver.calls.count("outcomes_attestation") == 1
    assert driver.calls.count("no_signal_outcomes_attestation") == 1
    assert driver.calls.count("load_projection_keys") == 2
    assert driver.calls[-2:] == ["rollback", "close"]
    assert "commit" not in driver.calls
    significant = {
        "schema_lock",
        "session",
        "probe_stage4",
        "probe_wave",
        "probe_outcomes",
        "probe_no_signal_outcomes",
        "attestation",
        "outcomes_attestation",
        "no_signal_outcomes_attestation",
        "load_projection_keys",
        "load_corpus_stage4",
        "load_corpus_wave",
        "load_corpus_outcomes",
        "load_corpus_no_signal_outcomes",
    }
    for index, call in enumerate(driver.calls):
        if call in significant:
            assert driver.calls[index - 1] == "statement_timeout", (
                call,
                driver.calls,
            )
    timeout_statements = [
        sql
        for marker, sql in driver.sql_statements
        if marker == "statement_timeout"
    ]
    assert timeout_statements
    timeout_values = [
        int(re.search(r"'([0-9]+)ms'", sql).group(1))
        for sql in timeout_statements
    ]
    assert all(
        1 <= value <= reader.MAX_READER_STATEMENT_TIMEOUT_MS
        for value in timeout_values
    )

    assert payload["analysis_as_of_utc"] == fixtures._iso(AS_OF)
    assert payload["database_snapshot_id"] == "900:900:"
    assert payload["cursor"] == {
        "order": "projection_decision_time_utc DESC, projection_event_id DESC",
        "before": None,
        "next": None,
        "has_more": False,
    }
    assert traversal["status"] == "COMPLETE"
    assert traversal["single_database_snapshot"] is True
    assert traversal["eof_proven"] is True
    assert traversal["analysis_as_of_utc"] == payload["analysis_as_of_utc"]
    assert traversal["database_snapshot_id"] == payload["database_snapshot_id"]
    assert traversal["page_count"] == 2
    assert traversal["page_size"] == 1
    assert traversal["max_pages"] == reader.MAX_FULL_CORPUS_PAGES
    assert traversal["max_projections"] == reader.MAX_FULL_CORPUS_PROJECTIONS
    assert traversal["max_observations"] == reader.MAX_FULL_CORPUS_OBSERVATIONS
    assert traversal["pages"][0]["before"] is None
    assert traversal["pages"][0]["next"] == expected_next
    assert traversal["pages"][0]["has_more"] is True
    assert traversal["pages"][1]["before"] == expected_next
    assert traversal["pages"][1]["next"] is None
    assert traversal["pages"][1]["has_more"] is False
    assert all(
        page["page_attestation_receipt_sha256"]
        for page in traversal["pages"]
    )
    expected_chain_hash = hashlib.sha256(
        reader._canonical_json(
            {
                "kind": "authoritative-full-corpus-page-chain-v1",
                "database_snapshot_id": payload["database_snapshot_id"],
                "page_attestation_receipts": [
                    page["page_attestation_receipt_sha256"]
                    for page in traversal["pages"]
                ],
            }
        ).encode("utf-8")
    ).hexdigest()
    assert traversal["page_receipts_sha256"] == expected_chain_hash
    assert traversal["aggregate_page_sha256"] == expected_chain_hash
    assert payload["counts"]["projections"] == 1
    assert payload["counts"]["observations"] == len(
        payload["observations"]
    )
    assert payload["ready_for_candidate_search"] is True

    unevaluable_stage4_rows = _stage4_view_rows(
        signals=[],
        evaluations=[
            {
                "symbol": "ETH",
                "status": "UNEVALUABLE",
                "reason": "PRICE_OI_UNAVAILABLE",
            }
        ],
    )
    unevaluable_driver = _FakeDriver(
        projection_rows=_projection_key_rows(unevaluable_stage4_rows),
        corpus_stage4_rows=unevaluable_stage4_rows,
        corpus_wave_rows=[],
        outcome_rows=[],
        no_signal_outcome_rows=[],
    )
    unevaluable = _load_complete_corpus(unevaluable_driver).to_dict()
    assert unevaluable["counts"]["projections"] == 1
    assert unevaluable["counts"]["stage4_events"] == 1
    assert unevaluable["counts"]["signal_events"] == 0
    assert unevaluable["counts"]["observations"] == 0
    assert unevaluable["observations"] == []
    assert unevaluable["traversal"]["eof_proven"] is True
    assert unevaluable["traversal"]["page_count"] == 1
    assert unevaluable_driver.calls.count("load_projection_keys") == 1
    assert unevaluable_driver.calls[-2:] == ["rollback", "close"]

    capped_driver = _PagedFakeDriver(
        projection_pages=[[first_key, older_unreturned_key]]
    )
    with patch.object(reader, "MAX_FULL_CORPUS_PAGES", 1):
        _expect_call_failure(
            lambda: _load_complete_corpus(
                capped_driver, projection_limit=1
            ),
            "before EOF",
        )
    assert capped_driver.calls[-2:] == ["rollback", "close"]
    assert capped_driver.calls.count("connect") == 1

    projection_capped_driver = _FakeDriver()
    with patch.object(reader, "MAX_FULL_CORPUS_PROJECTIONS", 0):
        _expect_call_failure(
            lambda: _load_complete_corpus(projection_capped_driver),
            "projection cap",
        )
    assert projection_capped_driver.calls[-2:] == ["rollback", "close"]

    observation_capped_driver = _FakeDriver()
    with patch.object(reader, "MAX_FULL_CORPUS_OBSERVATIONS", 1):
        _expect_call_failure(
            lambda: _load_complete_corpus(observation_capped_driver),
            "observation cap",
        )
    assert observation_capped_driver.calls[-2:] == ["rollback", "close"]

    stalled_driver = _PagedFakeDriver(
        projection_pages=[
            [first_key, older_unreturned_key],
            [first_key],
        ]
    )
    _expect_call_failure(
        lambda: _load_complete_corpus(stalled_driver, projection_limit=1),
        "before_cursor",
    )
    assert stalled_driver.calls[-2:] == ["rollback", "close"]

    base_payload = _load_corpus(_FakeDriver()).to_dict()
    inconsistent_payload = deepcopy(base_payload)
    inconsistent_payload["counts"]["observations"] += 1
    inconsistent_page = reader.AuthoritativeStage4CorpusResult._from_payload(
        inconsistent_payload
    )
    count_driver = _FakeDriver()
    with patch.object(
        reader,
        "_load_authoritative_stage4_corpus_page",
        return_value=inconsistent_page,
    ):
        _expect_call_failure(
            lambda: _load_complete_corpus(count_driver),
            "observation count",
        )
    assert count_driver.calls[-2:] == ["rollback", "close"]

    duplicate_cursor = {
        "projection_decision_time_utc": fixtures._iso(fixtures.DECISION),
        "projection_event_id": first_key["projection_event_id"],
    }
    duplicate_first_payload = deepcopy(base_payload)
    duplicate_first_payload["cursor"] = {
        "order": "projection_decision_time_utc DESC, projection_event_id DESC",
        "before": None,
        "next": duplicate_cursor,
        "has_more": True,
    }
    duplicate_second_payload = deepcopy(base_payload)
    duplicate_second_payload["cursor"] = {
        "order": "projection_decision_time_utc DESC, projection_event_id DESC",
        "before": duplicate_cursor,
        "next": None,
        "has_more": False,
    }
    duplicate_pages = [
        reader.AuthoritativeStage4CorpusResult._from_payload(
            duplicate_first_payload
        ),
        reader.AuthoritativeStage4CorpusResult._from_payload(
            duplicate_second_payload
        ),
    ]
    duplicate_driver = _FakeDriver()

    def duplicate_page(**_kwargs):
        return duplicate_pages.pop(0)

    with patch.object(
        reader,
        "_load_authoritative_stage4_corpus_page",
        side_effect=duplicate_page,
    ):
        _expect_call_failure(
            lambda: _load_complete_corpus(duplicate_driver),
            "duplicated an observation",
        )
    assert duplicate_driver.calls[-2:] == ["rollback", "close"]

    stalled_first_payload = deepcopy(duplicate_first_payload)
    stalled_second_payload = deepcopy(duplicate_first_payload)
    stalled_second_payload["cursor"]["before"] = duplicate_cursor
    stalled_pages = [
        reader.AuthoritativeStage4CorpusResult._from_payload(
            stalled_first_payload
        ),
        reader.AuthoritativeStage4CorpusResult._from_payload(
            stalled_second_payload
        ),
    ]
    stalled_receipt_driver = _FakeDriver()

    def stalled_page(**_kwargs):
        return stalled_pages.pop(0)

    with patch.object(
        reader,
        "_load_authoritative_stage4_corpus_page",
        side_effect=stalled_page,
    ):
        _expect_call_failure(
            lambda: _load_complete_corpus(stalled_receipt_driver),
            "did not advance strictly",
        )
    assert stalled_receipt_driver.calls[-2:] == ["rollback", "close"]

    deadline_driver = _PagedFakeDriver(
        projection_pages=[[first_key, older_unreturned_key]]
    )

    def traversal_deadline_clock():
        if "load_corpus_no_signal_outcomes" in deadline_driver.calls:
            return 31.0
        return 0.0

    with patch.object(
        reader.time,
        "monotonic",
        side_effect=traversal_deadline_clock,
    ):
        _expect_call_failure(
            lambda: _load_complete_corpus(
                deadline_driver,
                projection_limit=1,
                wall_budget_ms=reader.MIN_FULL_CORPUS_WALL_BUDGET_MS,
            ),
            "budget exhausted",
        )
    assert deadline_driver.calls[-2:] == ["rollback", "close"]
    assert deadline_driver.calls.count("load_projection_keys") == 1

    postprocessing_driver = _FakeDriver()

    def postprocessing_deadline_clock():
        if (
            postprocessing_driver.calls
            and postprocessing_driver.calls[-1] == "rollback"
        ):
            return 31.0
        return 0.0

    with patch.object(
        reader.time,
        "monotonic",
        side_effect=postprocessing_deadline_clock,
    ):
        _expect_call_failure(
            lambda: _load_complete_corpus(
                postprocessing_driver,
                wall_budget_ms=reader.MIN_FULL_CORPUS_WALL_BUDGET_MS,
            ),
            "local aggregation",
        )
    assert postprocessing_driver.calls[-2:] == ["rollback", "close"]
    assert postprocessing_driver.calls.count("load_projection_keys") == 1

    for invalid_budget in (
        reader.MIN_FULL_CORPUS_WALL_BUDGET_MS - 1,
        reader.MAX_FULL_CORPUS_WALL_BUDGET_MS + 1,
    ):
        invalid_driver = _FakeDriver()
        _expect_call_failure(
            lambda value=invalid_budget, fake=invalid_driver: (
                _load_complete_corpus(fake, wall_budget_ms=value)
            ),
            "wall_budget_ms",
        )
        assert "connect" not in invalid_driver.calls

    invalid_lookback_driver = _FakeDriver()
    _expect_call_failure(
        lambda: _load_complete_corpus(
            invalid_lookback_driver,
            lookback_days=reader.MAX_FULL_CORPUS_LOOKBACK_DAYS + 1,
        ),
        "lookback_days",
    )
    assert "connect" not in invalid_lookback_driver.calls


def _check_latest_current_reader_boundary() -> None:
    current_driver = _FakeDriver()
    current = _load_current(current_driver)
    receipt = current.receipt_dict()
    rows = current.current_observations
    assert type(current) is reader.LatestAuthoritativeStage4CurrentResult
    assert receipt["status"] == "AVAILABLE"
    assert receipt["available"] is True
    assert receipt["freshness_evaluated"] is False
    assert receipt["freshness_policy_owner"] == "DOWNSTREAM_CONSUMER"
    assert receipt["outcomes_loaded"] is False
    assert "observations" not in receipt
    assert "current_observations" not in receipt
    assert type(rows) is tuple
    assert [(row.symbol, row.direction) for row in rows] == [
        ("ETH", "LONG"),
        ("ETH", "SHORT"),
    ]
    assert all(
        type(row)
        is reader.stage4_candidate_search.CompactCurrentStage4Observation
        and not hasattr(row, "__dict__")
        and not hasattr(row, "outcome")
        and "outcome" not in row
        for row in rows
    )
    assert all(
        reader.stage4_candidate_search.validate_compact_current_observation(
            row,
            analysis_as_of_utc=receipt["analysis_as_of_utc"],
        )
        is row
        for row in rows
    )
    storage = receipt["observation_storage"]
    assert storage == {
        "format": "DETACHED_IMMUTABLE_CURRENT_COMPACT_TUPLE",
        "schema_version": (
            reader.stage4_candidate_search.CURRENT_OBSERVATION_SCHEMA_VERSION
        ),
        "hash_contract_version": (
            reader.stage4_candidate_search.CURRENT_OBSERVATION_CHAIN_HASH_VERSION
        ),
        "count": 2,
        "ordered_chain_sha256": (
            reader.stage4_candidate_search.compact_current_observation_chain_sha256(
                rows
            )
        ),
    }
    assert receipt["latest_projection"]["projection_status"] == "COMPLETED"
    assert receipt["latest_projection"]["current_observation_count"] == 2
    assert receipt["source_attestation"]["outcome_interface_access"] == (
        "NOT_REQUESTED"
    )
    assert current_driver.calls.count("connect") == 1
    assert current_driver.calls.count("begin") == 1
    assert current_driver.calls.count("session") == 1
    assert current_driver.calls.count("attestation") == 1
    assert current_driver.calls.count("load_latest_terminal_projection") == 1
    assert current_driver.calls.count("load_stage4") == 1
    assert current_driver.calls.count("load_wave") == 1
    assert current_driver.calls[-2:] == ["rollback", "close"]
    assert "commit" not in current_driver.calls
    for forbidden_call in (
        "probe_outcomes",
        "probe_no_signal_outcomes",
        "outcomes_attestation",
        "no_signal_outcomes_attestation",
        "load_corpus_outcomes",
        "load_corpus_no_signal_outcomes",
    ):
        assert forbidden_call not in current_driver.calls
    latest_params = dict(current_driver.params)[
        "load_latest_terminal_projection"
    ]
    assert latest_params == (AS_OF,)

    mismatched_selector = _latest_terminal_projection_rows()[0]
    mismatched_selector["projection_event_id"] += 1
    mismatch_driver = _FakeDriver(
        latest_terminal_rows=[mismatched_selector]
    )
    _expect_call_failure(
        lambda: _load_current(mismatch_driver),
        "selector and hydrated cohort disagree",
    )
    assert "load_stage4" in mismatch_driver.calls
    assert "load_wave" not in mismatch_driver.calls
    assert mismatch_driver.calls[-2:] == ["rollback", "close"]

    missed_selector = _latest_terminal_projection_rows()[0]
    missed_selector["projection_status"] = "MISSED_CAUSAL_WINDOW"
    missed_driver = _FakeDriver(latest_terminal_rows=[missed_selector])
    missed = _load_current(missed_driver)
    missed_receipt = missed.receipt_dict()
    assert missed_receipt["status"] == (
        "LATEST_PROJECTION_MISSED_CAUSAL_WINDOW"
    )
    assert missed_receipt["available"] is False
    assert missed.current_observations == ()
    assert missed_receipt["latest_projection"]["projection_status"] == (
        "MISSED_CAUSAL_WINDOW"
    )
    assert "load_stage4" not in missed_driver.calls
    assert "load_wave" not in missed_driver.calls

    no_projection_driver = _FakeDriver(latest_terminal_rows=[])
    no_projection = _load_current(no_projection_driver)
    assert no_projection.receipt_dict()["status"] == "NO_TERMINAL_PROJECTION"
    assert no_projection.current_observations == ()
    assert "load_stage4" not in no_projection_driver.calls
    assert "load_wave" not in no_projection_driver.calls

    evaluations = [
        {"symbol": "ETH", "status": "EVALUABLE", "reason": None},
        {
            "symbol": "SOL",
            "status": "UNEVALUABLE",
            "reason": "PRICE_OI_UNAVAILABLE",
        },
    ]
    partial_rows = _stage4_view_rows(signals=[], evaluations=evaluations)
    partial_driver = _FakeDriver(stage4_rows=partial_rows)
    partial = _load_current(partial_driver)
    assert [(row.symbol, row.direction) for row in partial.current_observations] == [
        ("ETH", "LONG"),
        ("ETH", "SHORT"),
    ]
    assert partial.receipt_dict()["latest_projection"]["evaluation_status"] == (
        "PARTIAL"
    )

    missing_wave_driver = _FakeDriver(wave_rows=[])
    missing_wave = _load_current(missing_wave_driver)
    assert missing_wave.receipt_dict()["available"] is True
    assert all(
        row.wave_binding.status == "UNAVAILABLE"
        and row.wave_binding.btc_parent_movement_id is None
        for row in missing_wave.current_observations
    )

    sql = _normalized_sql(reader._LOAD_LATEST_TERMINAL_PROJECTION_SQL)
    assert "ORDER BY alert_time_utc DESC, event_id DESC LIMIT 1" in sql
    assert "projection,status" in sql
    assert "= 'COMPLETED'" not in sql
    assert reader.OUTCOME_VIEW not in sql
    assert reader.NO_SIGNAL_OUTCOME_VIEW not in sql

    descriptor = reader.descriptor()
    assert descriptor["current_source_contract_version"] == (
        reader.CURRENT_SOURCE_CONTRACT_VERSION
    )
    assert descriptor["latest_current_reader_available"] is True
    assert descriptor["latest_current_outcomes_loaded"] is False
    assert descriptor["latest_current_freshness_policy_owner"] == (
        "DOWNSTREAM_CONSUMER"
    )


def run() -> None:
    _check_latest_current_reader_boundary()
    _check_complete_corpus_single_snapshot_traversal()
    assert reader._tagged_sha256(("tag",), ("₪",), field="fixture") == (
        hashlib.sha256("tag=3:₪".encode("utf-8")).hexdigest()
    )
    assert reader._tagged_sha256(("tag",), (None,), field="fixture") == (
        hashlib.sha256(b"tag=-1:").hexdigest()
    )
    assert MIGRATION_PATH.exists()
    assert OUTCOME_MIGRATION_PATH.exists()
    assert NO_SIGNAL_OUTCOME_MIGRATION_PATH.exists()
    assert MIGRATION_PATH in research_formula_schema_admin.MIGRATION_PATHS
    assert OUTCOME_MIGRATION_PATH in research_formula_schema_admin.MIGRATION_PATHS
    assert (
        NO_SIGNAL_OUTCOME_MIGRATION_PATH
        in research_formula_schema_admin.MIGRATION_PATHS
    )
    migration_sql = MIGRATION_PATH.read_text(encoding="utf-8")
    outcome_migration_sql = OUTCOME_MIGRATION_PATH.read_text(encoding="utf-8")
    no_signal_outcome_migration_sql = NO_SIGNAL_OUTCOME_MIGRATION_PATH.read_text(
        encoding="utf-8"
    )
    assert reader.SOURCE_CONTRACT_VERSION in migration_sql
    assert reader.TRUSTED_READER_ROLE in migration_sql
    assert reader.STAGE4_VIEW in migration_sql
    assert reader.WAVE_VIEW in migration_sql
    assert reader.OUTCOME_VIEW in outcome_migration_sql
    assert reader.OUTCOME_VIEW_CONTRACT_VERSION in outcome_migration_sql
    assert reader.NO_SIGNAL_OUTCOME_VIEW in no_signal_outcome_migration_sql
    assert (
        reader.NO_SIGNAL_OUTCOME_VIEW_CONTRACT_VERSION
        in no_signal_outcome_migration_sql
    )
    for no_signal_boundary_token in (
        "research_stage4_no_signal_outcomes_v1",
        "research_stage4_no_signal_outcome_writer_v1",
        "trg_research_stage4_no_signal_outcome_v1_validate",
        "trg_research_stage4_no_signal_outcome_v1_immutable",
        "trg_research_stage4_no_signal_outcome_v1_no_truncate",
        "has_any_column_privilege",
        "source_catalog_sha256",
        "security_barrier=true",
        "security_invoker=false",
    ):
        assert no_signal_boundary_token in reader._NO_SIGNAL_OUTCOMES_ATTESTATION_SQL
    _assert_no_signal_attestation_contract(
        reader._NO_SIGNAL_OUTCOMES_ATTESTATION_SQL
    )
    for weakened_fragment, replacement in (
        ("pg_catalog.count(*) = 3", "pg_catalog.count(*) >= 3"),
        ("pg_catalog.count(*) = 4", "pg_catalog.count(*) >= 4"),
        ("pg_catalog.pg_policy", "ignored_policy_catalog"),
        ("pg_catalog.pg_inherits", "ignored_inheritance_catalog"),
        ("pg_catalog.has_sequence_privilege", "false /* sequence */"),
        (
            "no_signal_writer_authority_attested",
            "no_signal_writer_authority_unattested",
        ),
    ):
        assert weakened_fragment in reader._NO_SIGNAL_OUTCOMES_ATTESTATION_SQL
        _assert_static_rejection(
            _assert_no_signal_attestation_contract,
            reader._NO_SIGNAL_OUTCOMES_ATTESTATION_SQL.replace(
                weakened_fragment, replacement
            ),
        )
    assert reader.NO_SIGNAL_OUTCOME_VIEW in reader._PROBE_NO_SIGNAL_OUTCOMES_SQL
    assert "LIMIT 0" in reader._PROBE_NO_SIGNAL_OUTCOMES_SQL
    view_options = re.findall(
        r"WITH\s*\(\s*security_barrier\s*=\s*true\s*,\s*"
        r"security_invoker\s*=\s*false\s*\)",
        migration_sql,
        flags=re.IGNORECASE,
    )
    assert len(view_options) == 2
    assert "CREATE TABLE" not in migration_sql.upper()
    assert not re.search(
        r"\bWHERE\s+function_namespace\.nspname\s*=\s*'public'\s+WHERE\b",
        migration_sql,
        flags=re.IGNORECASE,
    ), "migration contains two consecutive WHERE clauses"

    # A boolean returned by the catalog query is authoritative only when its
    # SQL binds the exact trigger, function, index and constraint identities.
    for catalog_token in (
        "tgfoid",
        "tgtype",
        "tgdeferrable",
        "tginitdeferred",
        "proowner",
        "prosecdef",
        "proconfig",
        "indrelid",
        "pg_get_indexdef",
        "pg_get_constraintdef",
        "source_catalog_sha256",
    ):
        assert catalog_token in migration_sql, catalog_token
    final_boundary = migration_sql.split("DO $final_catalog_assertions$", 1)[1]
    assert "research_price_collection_attempts" in final_boundary
    assert re.search(
        r"REVOKE\s+ALL\s+ON\s+TABLE[^;]*"
        r"research_price_collection_attempts[^;]*"
        r"FROM\s+research_formula_exploration_reader_v1",
        migration_sql,
        flags=re.IGNORECASE,
    )

    runtime_attestation_sql = reader._ATTESTATION_SQL
    for source_relation in (
        "research_events",
        "research_max_pain_snapshot_sets",
        "research_max_pain_snapshot_symbols",
        "research_max_pain_snapshot_rows",
        "research_price_collection_attempts",
        "research_neutral_price_anchors",
        "research_market_movement_transitions",
        "research_market_movement_memberships",
    ):
        assert source_relation in runtime_attestation_sql
    for catalog_token in (
        "tgfoid",
        "tgtype",
        "tgdeferrable",
        "tginitdeferred",
        "proowner",
        "prosecdef",
        "proconfig",
        "indrelid",
        "pg_get_indexdef",
        "pg_get_constraintdef",
        "pg_get_viewdef",
        "has_any_column_privilege",
        "has_sequence_privilege",
        "has_database_privilege",
        "source_catalog_sha256",
    ):
        assert catalog_token in runtime_attestation_sql, catalog_token
    assert "unexpected.tgrelid = event_relation.oid" not in runtime_attestation_sql
    assert re.search(
        r"unexpected\.tgrelid\s*=\s*pg_catalog\.to_regclass\(\s*"
        r"'public\.research_events'\s*\)",
        runtime_attestation_sql,
        flags=re.IGNORECASE,
    )

    # Migration 026 adds one separately attested least-privilege writer.  Its
    # exact non-grantable grants post-date migration 024's frozen receipt, so
    # only those entries may be normalized out of the legacy catalog hash.
    schema_catalog_sql = reader._CATALOG_SCHEMA_CTE
    relation_catalog_sql = reader._CATALOG_RELATIONS_CTE
    for catalog_sql in (schema_catalog_sql, relation_catalog_sql):
        assert "research_stage4_no_signal_outcome_writer_v1" in catalog_sql
        assert "AND NOT acl.is_grantable" in catalog_sql
    assert "acl.privilege_type = 'USAGE'" in schema_catalog_sql
    assert "acl.privilege_type = 'SELECT'" in relation_catalog_sql
    for relation_name in (
        "research_events",
        "research_max_pain_snapshot_sets",
        "research_max_pain_snapshot_symbols",
        "research_max_pain_snapshot_rows",
    ):
        assert relation_name in relation_catalog_sql
    assert runtime_attestation_sql.count(
        "pg_catalog.has_database_privilege("
    ) == 1
    assert reader._NO_SIGNAL_OUTCOMES_ATTESTATION_SQL.count(
        "pg_catalog.has_database_privilege("
    ) == 1
    _assert_delegated_writer_acl_normalization(migration_sql)
    _assert_delegated_writer_acl_normalization(runtime_attestation_sql)
    # int2vector casts retain PostgreSQL's zero lower bound.  Comparing them
    # directly with ordinary one-based arrays rejects equal contents, so every
    # exact index-option comparison must normalize through ordered UNNEST.
    assert re.search(
        r"index_row\.indkey::SMALLINT\[\]\s*=\s*ARRAY\[0\]",
        migration_sql,
    ) is None
    assert re.search(
        r"index_row\.indoption::SMALLINT\[\]\s*=",
        migration_sql,
    ) is None
    for normalized_vector in (
        "WITH ORDINALITY AS key_entry(value, ordinality)",
        "WITH ORDINALITY AS option_entry(value, ordinality)",
        "WITH ORDINALITY AS key_option(value, ordinality)",
    ):
        assert normalized_vector in migration_sql

    max_pain_function_names = (
        "assert_research_max_pain_snapshot_complete",
        "prevent_research_max_pain_archive_mutation",
    )
    for function_name in max_pain_function_names:
        assert function_name in migration_sql
        assert function_name in runtime_attestation_sql
    assert re.search(
        r"JSONB_ARRAY_LENGTH\s*\(\s*functions_payload\s*\)\s*<>\s*28",
        migration_sql,
        flags=re.IGNORECASE,
    )
    assert re.search(
        r"jsonb_array_length\s*\(\s*"
        r"catalog_functions_payload\.payload\s*\)\s*=\s*28",
        runtime_attestation_sql,
        flags=re.IGNORECASE,
    )

    # Ownership is discovered from the source catalog rather than encoded as
    # an environment-specific role name; role membership and view options are
    # included in both the installed and runtime catalog boundaries.
    assert "stage4_owner_oid" in migration_sql
    assert "stage4_owner_name" in migration_sql
    assert re.search(
        r"SELECT\s+relation_row\.relowner\s+INTO\s+stage4_owner_oid.*?"
        r"public\.research_events",
        migration_sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    assert "expected_owner_source" in runtime_attestation_sql
    for catalog_sql in (migration_sql, runtime_attestation_sql):
        assert "pg_auth_members" in catalog_sql
        assert "security_barrier=true" in catalog_sql
        assert "security_invoker=false" in catalog_sql
        assert re.search(
            r"cardinality\s*\([^)]*reloptions[^)]*\)\s*=\s*2",
            catalog_sql,
            flags=re.IGNORECASE,
        )
    for role_option in (
        "rolcanlogin",
        "rolinherit",
        "rolsuper",
        "rolcreatedb",
        "rolcreaterole",
        "rolreplication",
        "rolbypassrls",
    ):
        assert role_option in migration_sql
        assert role_option in runtime_attestation_sql
    for guc_token in (
        "statement_timeout=20000",
        "lock_timeout=2000",
        "idle_in_transaction_session_timeout=30000",
        "search_path=pg_catalog",
        "timezone=UTC",
        "row_security=off",
    ):
        assert guc_token in reader.CONNECTION_OPTIONS
    for migration_guc in (
        "SET LOCAL search_path = pg_catalog",
        "SET LOCAL TIME ZONE 'UTC'",
        "SET LOCAL DateStyle = 'ISO, YMD'",
        "SET LOCAL IntervalStyle = 'postgres'",
        "SET LOCAL extra_float_digits = 3",
        "SET LOCAL quote_all_identifiers = off",
    ):
        assert migration_guc in migration_sql

    for deparser_pin in (
        "DateStyle=ISO,YMD",
        "IntervalStyle=postgres",
        "extra_float_digits=3",
        "quote_all_identifiers=off",
    ):
        assert f"-c {deparser_pin}" in reader.CONNECTION_OPTIONS
    deparser_keys = (
        "date_style",
        "interval_style",
        "extra_float_digits",
        "quote_all_identifiers",
        "search_path",
        "time_zone",
    )
    for deparser_key in deparser_keys:
        assert f"'{deparser_key}'" in reader._CATALOG_DEPARSE_GUCS_CTE
        assert f"'{deparser_key}'" in migration_sql
    assert re.search(
        r"'deparser_gucs'\s*,\s*deparser_gucs_payload",
        migration_sql,
        flags=re.IGNORECASE,
    )
    assert re.search(
        r"'deparser_gucs'\s*,\s*"
        r"catalog_deparser_gucs_payload\.payload",
        reader._CATALOG_RECEIPT_CTES,
        flags=re.IGNORECASE,
    )
    for expected_pair in (
        r"'date_style'\s*,\s*'ISO, YMD'",
        r"'interval_style'\s*,\s*'postgres'",
        r"'extra_float_digits'\s*,\s*'3'",
        r"'quote_all_identifiers'\s*,\s*'off'",
        r"'search_path'\s*,\s*'pg_catalog'",
        r"'time_zone'\s*,\s*'UTC'",
    ):
        assert re.search(
            expected_pair, migration_sql, flags=re.IGNORECASE
        )
        assert re.search(
            expected_pair,
            reader._CATALOG_RECEIPT_CTES,
            flags=re.IGNORECASE,
        )

    _assert_role_graph_contract(migration_sql)
    _assert_role_graph_contract(reader._CATALOG_ROLES_CTE)
    for graph_sql in (migration_sql, reader._CATALOG_ROLES_CTE):
        false_edge_mutant = re.sub(
            r"WHERE\s+membership\.roleid\s+IN",
            "WHERE FALSE AND membership.roleid IN",
            graph_sql,
            count=1,
            flags=re.IGNORECASE,
        )
        assert false_edge_mutant != graph_sql
        _assert_static_rejection(
            _assert_role_graph_contract, false_edge_mutant
        )
        for endpoint in ("roleid", "member", "grantor"):
            empty_node_mutant = re.sub(
                rf"SELECT\s+edge\.{endpoint}\s+FROM\s+"
                r"authority_membership_edges\s+edge",
                "SELECT NULL::oid FROM authority_membership_edges edge",
                graph_sql,
                count=1,
                flags=re.IGNORECASE,
            )
            assert empty_node_mutant != graph_sql
            _assert_static_rejection(
                _assert_role_graph_contract, empty_node_mutant
            )

    _assert_max_pain_007_boundary(migration_sql)
    for weakened_007 in (
        migration_sql.replace(
            "trigger_row.tgenabled <> 'O'",
            "trigger_row.tgenabled <> 'A'",
            1,
        ),
        migration_sql.replace(
            "AND function_row.proowner = trusted_owner",
            "AND TRUE",
            1,
        ),
    ):
        assert weakened_007 != migration_sql
        _assert_static_rejection(
            _assert_max_pain_007_boundary, weakened_007
        )

    driver = _FakeDriver()
    result = _load(driver)
    assert len(result.observations) == 2
    assert {
        item.to_dict()["wave_binding"]["status"] for item in result.observations
    } == {"BOUND"}
    result_payload = result.to_dict()
    assert result_payload["source_attestation"]["source_authority_attested"] is True
    assert (
        result_payload["source_attestation"]["source_catalog_sha256"]
        == SOURCE_CATALOG_SHA256
    )
    assert result_payload["formula_registry_effect"] == "NONE"
    assert result_payload["authority_effect"] == "NONE"
    assert result_payload["delivery_channel"] == "NONE"
    assert result_payload["live_eligible"] is False
    assert result_payload["telegram_delivery_allowed"] is False
    assert result_payload["trade_execution_allowed"] is False
    required_order = [
        "begin",
        "schema_lock",
        "session",
        "probe_stage4",
        "probe_wave",
        "attestation",
        "load_stage4",
        "load_wave",
    ]
    positions = [driver.calls.index(marker) for marker in required_order]
    assert positions == sorted(positions), driver.calls
    assert driver.calls[-2:] == ["rollback", "close"], driver.calls
    assert "commit" not in driver.calls
    stage4_params = dict(driver.params)["load_stage4"]
    stage4_values = (
        tuple(stage4_params.values())
        if isinstance(stage4_params, dict)
        else tuple(stage4_params)
    )
    assert SNAPSHOT_KEY in stage4_values
    assert reader.MAX_STAGE4_ROWS + 1 in stage4_values
    assert driver.connect_kwargs.get("autocommit") is True
    assert "default_transaction_read_only=on" in driver.connect_kwargs.get(
        "options", ""
    )
    assert "repeatable read read only" in reader._BEGIN_SQL.lower()
    assert reader.SCHEMA_LOCK_ID == research_formula_schema_admin.SCHEMA_LOCK_ID
    assert str(reader.SCHEMA_LOCK_ID) not in reader._SCHEMA_LOCK_SQL
    schema_lock_params = dict(driver.params)["schema_lock"]
    assert reader.SCHEMA_LOCK_ID in tuple(schema_lock_params)
    wave_params = dict(driver.params)["load_wave"]
    assert reader.MAX_WAVE_ROWS + 1 in tuple(wave_params)

    corpus_driver = _FakeDriver()
    corpus = _load_corpus(corpus_driver)
    corpus_payload = corpus.to_dict()
    assert len(corpus.observations) == 2
    assert corpus.counts == {
        "projections": 1,
        "stage4_events": 3,
        "signal_events": 2,
        "wave_rows": 2,
        "outcome_rows": 3,
        "signal_outcome_rows": 2,
        "no_signal_outcome_rows": 1,
        "observations": 2,
        "available_outcomes": 2,
        "unavailable_outcomes": 0,
        "explicit_no_signal_observations": 1,
        "distinct_btc_parent_movements": 1,
    }
    available = [
        item.to_dict()
        for item in corpus.observations
        if item.to_dict()["outcome"]["status"] == "AVAILABLE"
    ]
    assert len(available) == 2
    assert available[0]["outcome"]["horizon_minutes"] == 60
    assert available[0]["outcome"]["label_fields_exposed_as_features"] is False
    no_signal_available = next(
        item for item in available if item["explicit_no_signal"] is True
    )
    assert no_signal_available["direction"] == "SHORT"
    assert no_signal_available["outcome"]["carrier_type"] == (
        "STAGE4_NO_SIGNAL_CELL"
    )
    assert no_signal_available["outcome"]["source_event_ids"] == []
    assert corpus_payload["source_attestation"]["outcomes_view_attested"] is True
    assert corpus_payload["source_attestation"]["raw_outcomes_access_absent"] is True
    assert corpus_payload["source_attestation"][
        "no_signal_outcomes_view_attested"
    ] is True
    assert corpus_payload["source_attestation"][
        "no_signal_outcomes_table_attested"
    ] is True
    assert corpus_payload["source_attestation"][
        "no_signal_writer_authority_attested"
    ] is True
    assert corpus_payload["source_attestation"][
        "raw_no_signal_outcomes_access_absent"
    ] is True
    assert corpus_payload["source_attestation"][
        "no_signal_outcomes_raw_catalog_sha256"
    ] == NO_SIGNAL_RAW_CATALOG_SHA256
    assert corpus_payload["source_attestation"][
        "no_signal_outcomes_trigger_catalog_sha256"
    ] == NO_SIGNAL_TRIGGER_CATALOG_SHA256
    assert corpus_payload["source_attestation"][
        "no_signal_reference_hash_contract_version"
    ] == reader.NO_SIGNAL_REFERENCE_HASH_CONTRACT_VERSION
    assert corpus_payload["source_attestation"][
        "no_signal_outcome_hash_contract_version"
    ] == reader.NO_SIGNAL_OUTCOME_HASH_CONTRACT_VERSION
    assert corpus_payload["source_attestation"]["outcome_view"] == (
        reader.OUTCOME_VIEW
    )
    assert corpus_payload["ready_for_candidate_search"] is True
    assert corpus_payload["blockers"] == []
    assert corpus_payload["dataset_readiness"][
        "ready_for_formula_effect_research"
    ] is True
    assert corpus_payload["dataset_readiness"]["label_coverage_complete"] is True
    assert corpus_payload["dataset_readiness"]["wave_coverage_complete"] is True
    assert corpus_payload["formula_registry_effect"] == "NONE"
    assert corpus_payload["authority_effect"] == "NONE"
    assert corpus_payload["delivery_channel"] == "NONE"
    assert corpus_payload["live_eligible"] is False
    assert corpus_payload["telegram_delivery_allowed"] is False
    assert corpus_payload["trade_execution_allowed"] is False
    assert corpus.cursor == {
        "order": "projection_decision_time_utc DESC, projection_event_id DESC",
        "before": None,
        "next": None,
        "has_more": False,
    }
    corpus_order = [
        "begin",
        "schema_lock",
        "session",
        "probe_stage4",
        "probe_wave",
        "probe_outcomes",
        "probe_no_signal_outcomes",
        "attestation",
        "outcomes_attestation",
        "no_signal_outcomes_attestation",
        "load_projection_keys",
        "load_corpus_stage4",
        "load_corpus_wave",
        "load_corpus_outcomes",
        "load_corpus_no_signal_outcomes",
    ]
    corpus_positions = [corpus_driver.calls.index(name) for name in corpus_order]
    assert corpus_positions == sorted(corpus_positions), corpus_driver.calls
    assert corpus_driver.calls[-2:] == ["rollback", "close"]
    assert "commit" not in corpus_driver.calls
    projection_params = dict(corpus_driver.params)["load_projection_keys"]
    assert projection_params[-1] == reader.MAX_PROJECTION_LIMIT + 1
    corpus_stage4_params = dict(corpus_driver.params)["load_corpus_stage4"]
    assert corpus_stage4_params[1:] == (
        reader.MAX_STAGE4_ROWS + 1,
        reader.MAX_CORPUS_STAGE4_ROWS + 1,
    )
    corpus_wave_params = dict(corpus_driver.params)["load_corpus_wave"]
    assert corpus_wave_params[1:] == (
        reader.MAX_WAVE_ROWS + 1,
        reader.MAX_CORPUS_WAVE_ROWS + 1,
    )
    corpus_outcome_params = dict(corpus_driver.params)["load_corpus_outcomes"]
    assert corpus_outcome_params == ([101, 102], 60, 3)
    no_signal_outcome_params = dict(corpus_driver.params)[
        "load_corpus_no_signal_outcomes"
    ]
    assert no_signal_outcome_params == ([900], 60, 2)
    assert "(alert_time_utc, event_id) <" in reader._LOAD_PROJECTION_KEYS_SQL
    assert "CROSS JOIN LATERAL" in reader._LOAD_CORPUS_STAGE4_SQL
    assert "CROSS JOIN LATERAL" in reader._LOAD_CORPUS_WAVE_SQL
    assert reader.OUTCOME_VIEW in reader._LOAD_CORPUS_OUTCOMES_SQL
    assert "research_alert_outcomes" not in reader._LOAD_CORPUS_OUTCOMES_SQL
    assert (
        reader.NO_SIGNAL_OUTCOME_VIEW
        in reader._LOAD_CORPUS_NO_SIGNAL_OUTCOMES_SQL
    )
    assert (
        "research_stage4_no_signal_outcomes_v1"
        not in reader._LOAD_CORPUS_NO_SIGNAL_OUTCOMES_SQL
    )

    descriptor = reader.descriptor()
    assert descriptor["corpus_source_contract_version"] == (
        reader.CORPUS_SOURCE_CONTRACT_VERSION
    )
    assert descriptor["outcomes_loaded"] is True
    assert descriptor["outcomes_loaded_capability"] is True
    assert descriptor["no_signal_outcome_view"] == reader.NO_SIGNAL_OUTCOME_VIEW
    assert descriptor["runtime_wired"] is True
    assert descriptor["runtime_wiring_scope"] == (
        "DISCOVERY_INGESTION_OBSERVABILITY_ONLY"
    )
    assert descriptor["candidate_search_runtime_wired"] is True
    assert descriptor["candidate_search_readiness_evaluated_per_corpus"] is True
    assert descriptor["ready_for_candidate_search"] is False
    assert descriptor["formula_registry_effect"] == "NONE"

    older_projection = deepcopy(_projection_key_rows()[0])
    older_projection["projection_event_id"] = 899
    older_projection["snapshot_key"] = fixtures._h("older-snapshot")
    older_projection["projection_decision_time_utc"] = (
        fixtures.DECISION - timedelta(minutes=30)
    )
    older_projection["projection_created_at_utc"] = (
        fixtures.DECISION - timedelta(minutes=30) + timedelta(seconds=1)
    )
    paged_driver = _FakeDriver(
        projection_rows=[_projection_key_rows()[0], older_projection]
    )
    paged = _load_corpus(paged_driver, projection_limit=1)
    assert paged.cursor["has_more"] is True
    assert paged.next_cursor == {
        "projection_decision_time_utc": fixtures._iso(fixtures.DECISION),
        "projection_event_id": _projection_key_rows()[0][
            "projection_event_id"
        ],
    }

    cursor_driver = _FakeDriver()
    before_cursor = {
        "projection_decision_time_utc": fixtures._iso(
            fixtures.DECISION + timedelta(microseconds=1)
        ),
        "projection_event_id": (
            _projection_key_rows()[0]["projection_event_id"] + 1
        ),
    }
    cursor_page = _load_corpus(cursor_driver, before_cursor=before_cursor)
    assert cursor_page.cursor["before"] == before_cursor
    cursor_params = dict(cursor_driver.params)["load_projection_keys"]
    assert before_cursor["projection_decision_time_utc"] in cursor_params
    assert before_cursor["projection_event_id"] in cursor_params

    empty_corpus_driver = _FakeDriver(
        projection_rows=[], corpus_stage4_rows=[], corpus_wave_rows=[], outcome_rows=[]
    )
    empty_corpus = _load_corpus(empty_corpus_driver)
    assert empty_corpus.observations == ()
    assert empty_corpus.counts["projections"] == 0
    assert empty_corpus.counts["outcome_rows"] == 0
    assert empty_corpus.counts["signal_outcome_rows"] == 0
    assert empty_corpus.counts["no_signal_outcome_rows"] == 0
    assert empty_corpus.to_dict()["ready_for_candidate_search"] is False
    assert "EMPTY_COHORT" in empty_corpus.to_dict()["blockers"]
    assert "load_corpus_stage4" not in empty_corpus_driver.calls
    assert "load_corpus_wave" not in empty_corpus_driver.calls
    assert "load_corpus_outcomes" not in empty_corpus_driver.calls
    assert "load_corpus_no_signal_outcomes" not in empty_corpus_driver.calls

    for flag in ("outcomes_view_attested", "raw_outcomes_access_absent"):
        unsafe_outcome_view = _FakeDriver()
        unsafe_outcome_view.outcomes_attestation[flag] = False
        _expect_call_failure(
            lambda fake=unsafe_outcome_view: _load_corpus(fake), "attest"
        )
        assert "load_projection_keys" not in unsafe_outcome_view.calls
        assert unsafe_outcome_view.calls[-2:] == ["rollback", "close"]

    wrong_catalog_outcome_view = _FakeDriver()
    wrong_catalog_outcome_view.outcomes_attestation[
        "stage4_source_catalog_sha256"
    ] = "c7" * 32
    _expect_call_failure(
        lambda: _load_corpus(wrong_catalog_outcome_view), "not bound"
    )
    assert "load_projection_keys" not in wrong_catalog_outcome_view.calls

    for flag in (
        "no_signal_outcomes_view_attested",
        "no_signal_outcomes_table_attested",
        "no_signal_writer_authority_attested",
        "raw_no_signal_outcomes_access_absent",
    ):
        unsafe_no_signal_view = _FakeDriver()
        unsafe_no_signal_view.no_signal_outcomes_attestation[flag] = False
        _expect_call_failure(
            lambda fake=unsafe_no_signal_view: _load_corpus(fake), "attest"
        )
        assert "load_projection_keys" not in unsafe_no_signal_view.calls
        assert unsafe_no_signal_view.calls[-2:] == ["rollback", "close"]

    wrong_no_signal_catalog = _FakeDriver()
    wrong_no_signal_catalog.no_signal_outcomes_attestation[
        "stage4_source_catalog_sha256"
    ] = "e9" * 32
    _expect_call_failure(
        lambda: _load_corpus(wrong_no_signal_catalog), "not bound"
    )
    assert "load_projection_keys" not in wrong_no_signal_catalog.calls

    missing_no_signal_attestation = _FakeDriver()
    missing_no_signal_attestation.no_signal_outcomes_attestation = None
    _expect_call_failure(
        lambda: _load_corpus(missing_no_signal_attestation), "attestation"
    )
    assert "load_projection_keys" not in missing_no_signal_attestation.calls

    for receipt_field in (
        "stage4_source_catalog_sha256",
        "no_signal_outcomes_view_definition_sha256",
        "raw_catalog_sha256",
        "view_raw_catalog_sha256",
        "table_raw_catalog_sha256",
        "trigger_catalog_sha256",
        "view_trigger_catalog_sha256",
        "table_trigger_catalog_sha256",
    ):
        malformed_no_signal_attestation = _FakeDriver()
        malformed_no_signal_attestation.no_signal_outcomes_attestation[
            receipt_field
        ] = "0" * 63
        _expect_call_failure(
            lambda fake=malformed_no_signal_attestation: _load_corpus(fake),
            "receipt",
        )
        assert "load_projection_keys" not in (
            malformed_no_signal_attestation.calls
        )

    for receipt_field in (
        "view_raw_catalog_sha256",
        "table_raw_catalog_sha256",
        "view_trigger_catalog_sha256",
        "table_trigger_catalog_sha256",
    ):
        mismatched_no_signal_attestation = _FakeDriver()
        mismatched_no_signal_attestation.no_signal_outcomes_attestation[
            receipt_field
        ] = "c3" * 32
        _expect_call_failure(
            lambda fake=mismatched_no_signal_attestation: _load_corpus(fake),
            "catalog receipts",
        )
        assert "load_projection_keys" not in (
            mismatched_no_signal_attestation.calls
        )

    for contract_field in (
        "view_reference_hash_contract",
        "table_reference_hash_contract",
        "view_outcome_hash_contract",
        "table_outcome_hash_contract",
    ):
        mismatched_hash_contract = _FakeDriver()
        mismatched_hash_contract.no_signal_outcomes_attestation[
            contract_field
        ] = "forged-hash-contract-v1"
        _expect_call_failure(
            lambda fake=mismatched_hash_contract: _load_corpus(fake),
            "hash contract receipt mismatch",
        )
        assert "load_projection_keys" not in mismatched_hash_contract.calls

    duplicate_projection = _projection_key_rows()[0]
    duplicate_keyset = _FakeDriver(
        projection_rows=[duplicate_projection, deepcopy(duplicate_projection)]
    )
    _expect_call_failure(
        lambda: _load_corpus(duplicate_keyset), "duplicate projection"
    )
    assert "load_corpus_stage4" not in duplicate_keyset.calls

    future_projection_row = deepcopy(_projection_key_rows()[0])
    future_projection_row["projection_created_at_utc"] = AS_OF + timedelta(
        microseconds=1
    )
    future_projection = _FakeDriver(projection_rows=[future_projection_row])
    _expect_call_failure(lambda: _load_corpus(future_projection), "causal corpus")
    assert "load_corpus_stage4" not in future_projection.calls

    truncated_corpus = _FakeDriver(
        projection_rows=_projection_key_rows(),
        corpus_stage4_rows=[deepcopy(_stage4_view_rows()[0])]
        * (reader.MAX_STAGE4_ROWS + 1)
    )
    _expect_call_failure(lambda: _load_corpus(truncated_corpus), "bound")
    assert "load_corpus_wave" not in truncated_corpus.calls

    duplicate_outcome_rows = _outcome_view_rows()
    duplicate_outcome_rows.append(deepcopy(duplicate_outcome_rows[0]))
    duplicate_outcomes = _FakeDriver(outcome_rows=duplicate_outcome_rows)
    _expect_call_failure(lambda: _load_corpus(duplicate_outcomes), "bound")

    wrong_horizon_rows = _outcome_view_rows()
    wrong_horizon_rows[0]["horizon_minutes"] = 240
    wrong_horizon = _FakeDriver(outcome_rows=wrong_horizon_rows)
    _expect_call_failure(lambda: _load_corpus(wrong_horizon), "wrong horizon")

    future_outcome_rows = _outcome_view_rows()
    future_outcome_rows[0]["outcome_created_at"] = AS_OF + timedelta(
        microseconds=1
    )
    future_outcome = _FakeDriver(outcome_rows=future_outcome_rows)
    _expect_call_failure(lambda: _load_corpus(future_outcome), "future row")

    invalid_method_rows = _outcome_view_rows()
    invalid_method_rows[0]["outcome_method_version"] = "forged-method"
    invalid_method_rows[1]["outcome_method_version"] = "forged-method"
    invalid_method = _FakeDriver(outcome_rows=invalid_method_rows)
    _expect_call_failure(
        lambda: _load_corpus(invalid_method), "failed closed validation"
    )

    duplicate_no_signal_rows = _no_signal_outcome_view_rows()
    duplicate_no_signal_rows.append(deepcopy(duplicate_no_signal_rows[0]))
    duplicate_no_signal = _FakeDriver(
        no_signal_outcome_rows=duplicate_no_signal_rows
    )
    _expect_call_failure(lambda: _load_corpus(duplicate_no_signal), "bound")

    wrong_no_signal_horizon_rows = _no_signal_outcome_view_rows()
    wrong_no_signal_horizon_rows[0]["horizon_minutes"] = 240
    wrong_no_signal_horizon = _FakeDriver(
        no_signal_outcome_rows=wrong_no_signal_horizon_rows
    )
    _expect_call_failure(
        lambda: _load_corpus(wrong_no_signal_horizon), "wrong horizon"
    )

    future_no_signal_rows = _no_signal_outcome_view_rows()
    future_no_signal_rows[0]["outcome_created_at"] = AS_OF + timedelta(
        microseconds=1
    )
    future_no_signal = _FakeDriver(
        no_signal_outcome_rows=future_no_signal_rows
    )
    _expect_call_failure(
        lambda: _load_corpus(future_no_signal), "non-causal row"
    )

    outside_no_signal_rows = _no_signal_outcome_view_rows()
    outside_no_signal_rows[0]["symbol"] = "SOL"
    outside_no_signal = _FakeDriver(
        no_signal_outcome_rows=outside_no_signal_rows
    )
    _expect_call_failure(
        lambda: _load_corpus(outside_no_signal), "outside the bounded cohort"
    )

    forged_no_signal_identity_rows = _no_signal_outcome_view_rows()
    forged_no_signal_identity_rows[0]["cell_identity_sha256"] = "f" * 64
    forged_no_signal_identity = _FakeDriver(
        no_signal_outcome_rows=forged_no_signal_identity_rows
    )
    _expect_call_failure(
        lambda: _load_corpus(forged_no_signal_identity), "identity receipt"
    )

    forged_reference_rows = _no_signal_outcome_view_rows()
    forged_reference_rows[0]["reference_receipt"] = deepcopy(
        forged_reference_rows[0]["reference_receipt"]
    )
    forged_reference_rows[0]["reference_receipt"]["symbol"] = "BTC"
    forged_reference = _FakeDriver(
        no_signal_outcome_rows=forged_reference_rows
    )
    _expect_call_failure(
        lambda: _load_corpus(forged_reference), "reference receipt identity"
    )

    forged_reference_body_rows = _no_signal_outcome_view_rows()
    forged_reference_body_rows[0]["reference_receipt"] = deepcopy(
        forged_reference_body_rows[0]["reference_receipt"]
    )
    forged_reference_body_rows[0]["reference_receipt"]["official_price"][
        "policy_status"
    ] = "FORGED"
    forged_reference_body = _FakeDriver(
        no_signal_outcome_rows=forged_reference_body_rows
    )
    _expect_call_failure(
        lambda: _load_corpus(forged_reference_body),
        "reference receipt hash mismatch",
    )

    forged_reference_hash_rows = _no_signal_outcome_view_rows()
    forged_reference_hash_rows[0]["reference_receipt_sha256"] = "3a" * 32
    forged_reference_hash = _FakeDriver(
        no_signal_outcome_rows=forged_reference_hash_rows
    )
    _expect_call_failure(
        lambda: _load_corpus(forged_reference_hash),
        "reference receipt hash mismatch",
    )

    noncanonical_reference_time_rows = _no_signal_outcome_view_rows()
    noncanonical_reference_time_rows[0]["reference_receipt"] = deepcopy(
        noncanonical_reference_time_rows[0]["reference_receipt"]
    )
    noncanonical_reference_time_rows[0]["reference_receipt"][
        "official_price"
    ]["fetched_at_utc"] = (
        fixtures.DECISION - timedelta(seconds=5)
    ).isoformat()
    noncanonical_reference_time = _FakeDriver(
        no_signal_outcome_rows=noncanonical_reference_time_rows
    )
    _expect_call_failure(
        lambda: _load_corpus(noncanonical_reference_time), "canonical UTC text"
    )

    forged_outcome_body_rows = _no_signal_outcome_view_rows()
    forged_outcome_body_rows[0]["price_at_horizon"] += 1.0
    forged_outcome_body = _FakeDriver(
        no_signal_outcome_rows=forged_outcome_body_rows
    )
    _expect_call_failure(
        lambda: _load_corpus(forged_outcome_body), "payload hash mismatch"
    )

    forged_outcome_hash_rows = _no_signal_outcome_view_rows()
    forged_outcome_hash_rows[0]["outcome_payload_sha256"] = "4b" * 32
    forged_outcome_hash = _FakeDriver(
        no_signal_outcome_rows=forged_outcome_hash_rows
    )
    _expect_call_failure(
        lambda: _load_corpus(forged_outcome_hash), "payload hash mismatch"
    )

    invalid_no_signal_method_rows = _no_signal_outcome_view_rows()
    invalid_no_signal_method_rows[0]["outcome_method_version"] = (
        "forged-no-signal-method"
    )
    invalid_no_signal_method = _FakeDriver(
        no_signal_outcome_rows=invalid_no_signal_method_rows
    )
    _expect_call_failure(
        lambda: _load_corpus(invalid_no_signal_method),
        "payload hash mismatch",
    )

    missing_no_signal = _load_corpus(
        _FakeDriver(no_signal_outcome_rows=[])
    )
    missing_no_signal_payload = missing_no_signal.to_dict()
    assert missing_no_signal_payload["counts"]["outcome_rows"] == 2
    assert missing_no_signal_payload["counts"]["signal_outcome_rows"] == 2
    assert missing_no_signal_payload["counts"]["no_signal_outcome_rows"] == 0
    assert missing_no_signal_payload["counts"]["available_outcomes"] == 1
    assert missing_no_signal_payload["counts"]["unavailable_outcomes"] == 1
    assert missing_no_signal_payload["ready_for_candidate_search"] is False
    assert "CANONICAL_NO_SIGNAL_OUTCOME_NOT_MATERIALIZED" in (
        missing_no_signal_payload["blockers"]
    )

    missing_wave = _load_corpus(_FakeDriver(corpus_wave_rows=[]))
    missing_wave_payload = missing_wave.to_dict()
    assert missing_wave_payload["counts"]["wave_rows"] == 0
    assert missing_wave_payload["counts"]["distinct_btc_parent_movements"] == 0
    assert missing_wave_payload["ready_for_candidate_search"] is False
    assert any(
        "WAVE_MEMBERSHIP_MISSING" in blocker
        for blocker in missing_wave_payload["blockers"]
    )

    missing_outcome = _load_corpus(_FakeDriver(outcome_rows=[]))
    assert {
        item.to_dict()["outcome"]["status"]
        for item in missing_outcome.observations
    } == {"AVAILABLE", "OUTCOME_UNAVAILABLE"}
    missing_outcome_payload = missing_outcome.to_dict()
    assert missing_outcome_payload["counts"]["outcome_rows"] == 1
    assert missing_outcome_payload["counts"]["signal_outcome_rows"] == 0
    assert missing_outcome_payload["counts"]["no_signal_outcome_rows"] == 1
    assert missing_outcome_payload["ready_for_candidate_search"] is False
    assert "STAGE4_SIGNAL_OUTCOME_MISSING" in missing_outcome_payload["blockers"]

    invalid_corpus_arguments = (
        {"horizon_minutes": 30},
        {"horizon_minutes": 60, "lookback_days": 0},
        {
            "horizon_minutes": 60,
            "lookback_days": reader.MAX_LOOKBACK_DAYS + 1,
        },
        {"horizon_minutes": 60, "projection_limit": 0},
        {
            "horizon_minutes": 60,
            "projection_limit": reader.MAX_PROJECTION_LIMIT + 1,
        },
        {
            "horizon_minutes": 60,
            "before_cursor": {"projection_event_id": 900},
        },
    )
    for invalid_arguments in invalid_corpus_arguments:
        invalid_driver = _FakeDriver()
        _expect_call_failure(
            lambda args=invalid_arguments, fake=invalid_driver: _with_driver(
                fake,
                lambda: reader.load_authoritative_stage4_corpus(
                    database_url=(
                        "postgresql://exploration-reader@db.example/research"
                    ),
                    **args,
                ),
            )
        )
        assert "connect" not in invalid_driver.calls

    decimal_wave_rows = _wave_view_rows()
    assert decimal_wave_rows[0]["anchor_receipt"]["price"] == "2500"
    assert decimal_wave_rows[0]["membership_receipt"]["price"] == "2500"
    decimal_wave_rows[0]["anchor_price"] = Decimal("2500.00")
    decimal_wave_rows[0]["membership_price"] = Decimal("2500.00")
    decimal_result = _load(_FakeDriver(wave_rows=decimal_wave_rows))
    assert {
        item.to_dict()["wave_binding"]["status"]
        for item in decimal_result.observations
    } == {"BOUND"}

    direction_count3_row = _producer_direction_established_count3_row()
    direction_count3_transition = movement.MovementTransition.from_dict(
        direction_count3_row["transition_receipt"]
    )
    assert (
        direction_count3_transition.transition_type
        == movement.DIRECTION_ESTABLISHED
    )
    assert direction_count3_transition.post_state.member_count == 3
    direction_count3_result = _load(
        _FakeDriver(
            wave_rows=[direction_count3_row, _wave_view_rows()[1]]
        )
    )
    assert {
        item.to_dict()["wave_binding"]["status"]
        for item in direction_count3_result.observations
    } == {"BOUND"}

    zero_signal_result = _load(
        _FakeDriver(stage4_rows=_stage4_view_rows([]))
    )
    zero_signal_payload = zero_signal_result.to_dict()
    assert zero_signal_payload["signal_events"] == []
    assert len(zero_signal_payload["observations"]) == 2
    assert all(
        observation["explicit_no_signal"] is True
        and observation["source_event_ids"] == []
        and observation["source_event_fingerprints"] == []
        for observation in zero_signal_payload["observations"]
    )

    unavailable_result = _load(_FakeDriver(wave_rows=[]))
    unavailable_payload = unavailable_result.to_dict()
    assert unavailable_payload["memberships"] == []
    assert unavailable_payload["transitions"] == []
    assert {
        observation["wave_binding"]["status"]
        for observation in unavailable_payload["observations"]
    } == {"UNAVAILABLE"}

    for flag in (
        "reader_role_ready",
        "migration_022_attested",
        "migration_023_attested",
        "stage4_view_attested",
        "wave_view_attested",
        "raw_access_absent",
    ):
        unsafe = _FakeDriver()
        unsafe.attestation[flag] = False
        _expect_failure(unsafe, "attest")
        assert "load_stage4" not in unsafe.calls
        assert "load_wave" not in unsafe.calls
        assert unsafe.calls[-2:] == ["rollback", "close"]

    missing_attestation = _FakeDriver()
    missing_attestation.attestation = None
    _expect_failure(missing_attestation, "attestation")
    assert "load_stage4" not in missing_attestation.calls

    for malformed_catalog_receipt in (None, "", "0" * 63, "A" * 64):
        unsafe_catalog = _FakeDriver()
        unsafe_catalog.attestation["source_catalog_sha256"] = (
            malformed_catalog_receipt
        )
        _expect_failure(unsafe_catalog, "catalog receipt")
        assert "load_stage4" not in unsafe_catalog.calls
        assert "load_wave" not in unsafe_catalog.calls
        assert unsafe_catalog.calls[-2:] == ["rollback", "close"]

    wrong_session = _FakeDriver()
    wrong_session.session["current_user"] = "research_market_movement_owner"
    _expect_failure(wrong_session, "reader")
    assert "attestation" not in wrong_session.calls
    assert "load_stage4" not in wrong_session.calls
    wrong_database = _FakeDriver()
    wrong_database.session["database_name"] = "other_research"
    _expect_failure(wrong_database, "target differs")
    assert wrong_database.calls == [
        "connect",
        "begin",
        "schema_lock",
        "session",
        "rollback",
        "close",
    ]
    assert "attestation" not in wrong_database.calls
    assert "load_stage4" not in wrong_database.calls
    non_read_only = _FakeDriver()
    non_read_only.session["transaction_read_only"] = "off"
    _expect_failure(non_read_only, "read")
    wrong_isolation = _FakeDriver()
    wrong_isolation.session["transaction_isolation"] = "read committed"
    _expect_failure(wrong_isolation, "repeatable")

    no_projection = _FakeDriver(
        stage4_rows=[
            row
            for row in _stage4_view_rows()
            if row["event_type"] != exploration.PROJECTION_EVENT_TYPE
        ]
    )
    _expect_failure(no_projection, "projection")
    duplicate_projection_rows = _stage4_view_rows()
    second_projection = deepcopy(duplicate_projection_rows[-1])
    second_projection["event_id"] = 901
    second_projection["setup_key"] = fixtures._h("setup-901")
    second_projection["event_fingerprint"] = fixtures._h("event-901")
    duplicate_projection_rows.append(second_projection)
    _expect_failure(
        _FakeDriver(stage4_rows=duplicate_projection_rows), "projection"
    )
    missing_sibling_rows = _stage4_view_rows()
    del missing_sibling_rows[0]
    _expect_failure(_FakeDriver(stage4_rows=missing_sibling_rows), "committed")
    missing_archive_rows = _stage4_view_rows()
    for row in missing_archive_rows:
        row["archive_snapshot_set_id"] = None
        row["archive_snapshot_key"] = None
    _expect_failure(_FakeDriver(stage4_rows=missing_archive_rows), "archive")

    mutated_rows = _stage4_view_rows()
    mutated_rows[0]["score"] = float(mutated_rows[0]["score"]) + 1.0
    _expect_failure(_FakeDriver(stage4_rows=mutated_rows), "commit")

    plus_zero_signal = fixtures._signal(
        101, exploration.MAX_PAIN_EVENT_TYPE
    )
    plus_zero_signal["score"] = 0.0
    signed_zero_rows = _stage4_view_rows([plus_zero_signal])
    signed_zero_rows[0]["score"] = -0.0
    assert exploration.signal_event_set_commitment([plus_zero_signal]) != (
        exploration.signal_event_set_commitment(
            [{**plus_zero_signal, "score": -0.0}]
        )
    )
    _expect_failure(_FakeDriver(stage4_rows=signed_zero_rows), "commit")

    forged_wave_rows = _wave_view_rows()
    forged_wave_rows[0]["membership_receipt"] = deepcopy(
        forged_wave_rows[0]["membership_receipt"]
    )
    forged_wave_rows[0]["membership_receipt"]["movement_id"] = "0" * 64
    _expect_failure(_FakeDriver(wave_rows=forged_wave_rows), "forg")
    dangling_wave_rows = _wave_view_rows()
    dangling_wave_rows[0]["emitted_by_transition_receipt_sha256"] = "f" * 64
    _expect_failure(_FakeDriver(wave_rows=dangling_wave_rows), "conflict")

    _expect_failure(
        _FakeDriver(wave_rows=[_cross_symbol_wave_row()]),
        "canonical receipts",
    )
    membership_link_mismatches = (
        {"stream_id": fixtures._h("unlinked-stream")},
        {"movement_id": fixtures._h("unlinked-movement")},
        {"ordinal": 2},
        {"classification": movement.NON_EXTREME_MEMBER},
    )
    for membership_changes in membership_link_mismatches:
        mismatched_rows = _wave_view_rows()
        mismatched_member = _rebuilt_membership(
            mismatched_rows[0], **membership_changes
        )
        _set_membership_row(
            mismatched_rows[0],
            mismatched_member,
            emitted_by=mismatched_rows[0]["transition_receipt_sha256"],
        )
        _expect_failure(
            _FakeDriver(wave_rows=mismatched_rows),
            "canonical receipts",
        )
    post_state_link_rows = _post_state_link_mismatch_rows()
    post_state_link_row = post_state_link_rows[0]
    post_state_link_anchor = movement.NeutralPriceAnchor.from_dict(
        post_state_link_row["anchor_receipt"]
    )
    post_state_link_member = movement.MovementMembership.from_dict(
        post_state_link_row["membership_receipt"]
    )
    post_state_link_transition = movement.MovementTransition.from_dict(
        post_state_link_row["transition_receipt"]
    )
    assert post_state_link_transition.transition_type == (
        movement.DIRECTION_ESTABLISHED
    )
    assert post_state_link_transition.post_state.member_count == 3
    assert post_state_link_transition.trigger_anchor_id == (
        post_state_link_member.anchor_id
    ) == post_state_link_anchor.anchor_id
    assert post_state_link_transition.post_state.last_member_anchor_id != (
        post_state_link_anchor.anchor_id
    )
    assert post_state_link_transition.post_state.extreme_anchor_id == (
        post_state_link_transition.post_state.last_member_anchor_id
    )
    assert reader._canonical_json(post_state_link_anchor.to_dict()) == (
        reader._canonical_json(
            reader._typed_anchor_payload(post_state_link_row)
        )
    )
    assert reader._canonical_json(post_state_link_member.to_dict()) == (
        reader._canonical_json(
            reader._typed_membership_payload(post_state_link_row)
        )
    )
    assert reader._canonical_json(post_state_link_transition.to_dict()) == (
        reader._canonical_json(
            reader._typed_transition_payload(post_state_link_row)
        )
    )
    # This passes the semantic validator.  Therefore the authoritative load
    # can reject it only through its explicit post-state/member/anchor link.
    reader._validate_transition_state_semantics(
        post_state_link_transition,
        chain_ordinal=post_state_link_row["transition_chain_ordinal"],
    )
    _expect_failure(
        _FakeDriver(wave_rows=post_state_link_rows),
        "typed columns conflict",
    )

    for impossible_type in (
        movement.DIRECTION_ESTABLISHED,
        movement.EXTREME_EXTENDED,
        movement.NON_EXTREME_OBSERVED,
    ):
        impossible_row = _canonical_impossible_continuation(impossible_type)
        impossible_transition = movement.MovementTransition.from_dict(
            impossible_row["transition_receipt"]
        )
        if impossible_type == movement.DIRECTION_ESTABLISHED:
            assert (
                impossible_transition.post_state.direction
                == movement.PENDING_DIRECTION
            )
            assert (
                impossible_transition.post_state.consecutive_non_extremes
                == 1
            )
            assert impossible_transition.post_state.member_count == 2
        _expect_failure(
            _FakeDriver(wave_rows=[impossible_row]),
            "semantics conflict",
        )

    for pending_non_extreme in (
        _impossible_pending_non_extreme_row(
            member_count=3,
            equal_price=True,
        ),
        _impossible_pending_non_extreme_row(
            member_count=2,
            equal_price=False,
        ),
    ):
        pending_transition = movement.MovementTransition.from_dict(
            pending_non_extreme["transition_receipt"]
        )
        assert (
            pending_transition.transition_type
            == movement.NON_EXTREME_OBSERVED
        )
        assert (
            pending_transition.post_state.direction
            == movement.PENDING_DIRECTION
        )
        _expect_failure(
            _FakeDriver(wave_rows=[pending_non_extreme]),
            "semantics conflict",
        )

    impossible_opened_rows = _wave_view_rows()
    assert impossible_opened_rows[0]["transition_type"] == movement.OPENED
    impossible_opened_rows[0]["transition_chain_ordinal"] = 999
    _expect_failure(
        _FakeDriver(wave_rows=impossible_opened_rows),
        "semantics conflict",
    )

    for created_at_field in ("event_created_at", "archive_created_at_utc"):
        future_stage4 = _stage4_view_rows()
        future_stage4[0][created_at_field] = AS_OF + timedelta(microseconds=1)
        _expect_failure(
            _FakeDriver(stage4_rows=future_stage4), "creation timestamps"
        )
    for created_at_field in (
        "membership_created_at_utc",
        "transition_created_at_utc",
        "anchor_created_at_utc",
    ):
        future_wave = _wave_view_rows()
        future_wave[0][created_at_field] = AS_OF + timedelta(microseconds=1)
        _expect_failure(_FakeDriver(wave_rows=future_wave), "creation timestamps")

    _expect_failure(
        _FakeDriver(wave_rows=_rows_with_duplicate_transition()),
        "duplicate transition",
    )

    mismatched_target = _FakeDriver()
    _expect_call_failure(
        lambda: _load_with_target(
            mismatched_target,
            "postgresql://exploration-reader@db.example/research",
            aligned_database_url=(
                "postgresql://archive-reader@db.example/research-other"
            ),
        ),
        "same research database",
    )
    assert "connect" not in mismatched_target.calls
    for invalid_url in (
        "postgresql://exploration-reader@db-a.example,db-b.example/research",
        (
            "postgresql://exploration-reader@db.example/research"
            "?hostaddr=192.0.2.10"
        ),
    ):
        invalid_target = _FakeDriver()
        _expect_call_failure(
            lambda value=invalid_url, fake=invalid_target: _load_with_target(
                fake, value
            ),
            "target is invalid",
        )
        assert "connect" not in invalid_target.calls

    primary_cleanup_driver = _FakeDriver(
        stage4_rows=[
            row
            for row in _stage4_view_rows()
            if row["event_type"] != exploration.PROJECTION_EVENT_TYPE
        ]
    )
    primary_cleanup_driver.rollback_error = RuntimeError(
        "rollback cleanup sentinel"
    )
    primary_cleanup_driver.close_error = RuntimeError(
        "close cleanup sentinel"
    )
    try:
        _load(primary_cleanup_driver)
    except reader.CohortIntegrityError as exc:
        assert "projection" in str(exc).lower()
        assert "cleanup sentinel" not in str(exc).lower()
    except BaseException as exc:  # pragma: no cover - fail with useful type
        raise AssertionError(
            f"cleanup masked the primary CohortIntegrityError: {exc!r}"
        ) from exc
    else:  # pragma: no cover - unsafe success
        raise AssertionError("primary Stage-4 failure was not raised")
    assert primary_cleanup_driver.calls[-2:] == ["rollback", "close"]

    for cleanup_attribute in ("rollback_error", "close_error"):
        cleanup_only_driver = _FakeDriver()
        setattr(
            cleanup_only_driver,
            cleanup_attribute,
            RuntimeError(f"{cleanup_attribute} cleanup-only sentinel"),
        )
        try:
            _load(cleanup_only_driver)
        except reader.AuthoritativeReaderError as exc:
            assert type(exc) is reader.AuthoritativeReaderError
            assert "cleanup failed" in str(exc).lower()
        except BaseException as exc:  # pragma: no cover - fail with type
            raise AssertionError(
                f"cleanup-only failure escaped unwrapped: {exc!r}"
            ) from exc
        else:  # pragma: no cover - unsafe success
            raise AssertionError("cleanup-only failure was ignored")
        assert cleanup_only_driver.calls[-2:] == ["rollback", "close"]

    reader_path = Path(reader.__file__)
    tree = ast.parse(reader_path.read_text(encoding="utf-8"))
    imported_roots = {
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imported_roots.update(
        (node.module or "").split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    )
    assert imported_roots.isdisjoint(
        {
            "ai_telegram",
            "main",
            "research_formula_store",
            "research_formula_worker",
            "research_market_movement_store",
            "research_outcome_worker",
            "research_signal_snapshot_store",
        }
    )
    called_attributes = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    assert "commit" not in called_attributes
    for name, sql in vars(reader).items():
        if not name.endswith("_SQL") or not isinstance(sql, str):
            continue
        executable_sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
        executable_sql = re.sub(r"'(?:''|[^'])*'", "''", executable_sql)
        for write_pattern in (
            r"\bINSERT\s+INTO\b",
            r"\bUPDATE\s+[A-Za-z_]",
            r"\bDELETE\s+FROM\b",
            r"\bMERGE\s+INTO\b",
            r"\bTRUNCATE\b",
            r"\bCREATE\b",
            r"\bALTER\b",
            r"\bDROP\b",
            r"\bGRANT\b",
            r"\bREVOKE\b",
        ):
            assert not re.search(
                write_pattern, executable_sql, flags=re.IGNORECASE
            ), (name, write_pattern)

    print("research_signal_formula_exploration_reader_selftest: PASS")
    print("Read-only repeatable snapshot and SQL order: PASS")
    print("Schema drift and fabricated source rows fail closed: PASS")


if __name__ == "__main__":
    run()
