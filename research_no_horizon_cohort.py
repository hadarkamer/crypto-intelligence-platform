"""One predeclared cohort, adjacent bounded parts, and one SQL anchor snapshot.

This module only validates objects and generates read-only SQL. Sealing records
the trusted collector's query provenance; hashes do not authenticate a database
or prove historical outcomes were unseen. No source rows, labels, or jobs are
combined here. Existing per-part transport and source limits remain unchanged.
"""
from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
import hashlib
import json
from typing import Any, Mapping

import research_no_horizon_contract as contracts
import research_no_horizon_manifest as transport
import research_no_horizon_manifest_sql as part_sql
import research_no_horizon_parent_coverage as coverage
import research_no_horizon_preflight as features
import research_no_horizon_source as source
import research_no_horizon_gate as gate
import research_watch_scan_formula as formulas

VERSION = "no-horizon-single-anchor-multipart-cohort-v1"
ANCHOR_VERSION = "no-horizon-single-anchor-multipart-manifest-v1"
QUERY_IDENTITY = "no-horizon-global-part-anchor-select-v1"
MAX_PARTS = 8
MAX_SOURCE_ROWS = 2048
MAX_TOTAL_BYTES = 256 * 1024 * 1024
MAX_SOURCE_SCOPE_DECISIONS = 16384
MAX_TOTAL_ROWS = MAX_SOURCE_ROWS
MAX_DECISIONS = MAX_SOURCE_SCOPE_DECISIONS
MAX_ANCHOR_BYTES = 8 * 1024 * 1024
MAX_DAYS = 31


def _budgets():
    return {"max_parts": MAX_PARTS, "max_source_rows": MAX_SOURCE_ROWS,
        "max_total_input_bytes": MAX_TOTAL_BYTES,
        "max_source_scope_decisions": MAX_SOURCE_SCOPE_DECISIONS,
        "max_anchor_bytes": MAX_ANCHOR_BYTES, "max_global_days": MAX_DAYS}


def _bindings():
    return {"source_export_version": source.EXPORT_VERSION,
        "source_adapter_version": source.VERSION, "feature_preflight_version": features.VERSION,
        "parent_coverage_version": coverage.VERSION, "manifest_transport_version": transport.VERSION,
        "manifest_sql_version": part_sql.VERSION, "gate_version": gate.VERSION,
        "catalog_sha256": formulas.CATALOG_SHA256, "feature_version": formulas.FEATURE_VERSION,
        "parent_policy_version": source.btc.POLICY_VERSION}


def _fields(value, required, optional=()):
    if not isinstance(value, Mapping) or not set(required) <= set(value) or set(value) - set(required) - set(optional):
        raise ValueError("INCOMPLETE_OR_UNKNOWN_COHORT_FIELDS")


def _integer(value, name, maximum, minimum=1):
    return transport._integer(value, name, maximum, minimum)


def normalize_declaration(value: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the entire population declaration without consulting its data."""
    required = {"cohort_version", "cohort_key", "declared_at_utc", "prior_outcomes_observed",
        "symbol", "price_route", "source_start_utc", "source_end_utc", "cutoff_utc", "scopes", "parts"}
    _fields(value, required, {"gate_policy", "version_bindings", "resource_budget"})
    if value["cohort_version"] != VERSION:
        raise ValueError("UNSUPPORTED_COHORT_VERSION")
    key = value["cohort_key"]
    if not isinstance(key, str) or not key or key != key.strip() or "\x00" in key or len(key.encode("utf-8")) > 256:
        raise ValueError("INVALID_COHORT_KEY")
    if type(value["prior_outcomes_observed"]) is not bool:
        raise ValueError("EXPLICIT_PRIOR_OUTCOME_KNOWLEDGE_REQUIRED")
    source.route_for_symbol(value["symbol"])
    if value["price_route"] != part_sql.ROUTE:
        raise ValueError("UNSUPPORTED_COHORT_PRICE_ROUTE")
    declared, start, end, cutoff = (contracts.utc(value[k]) for k in
        ("declared_at_utc", "source_start_utc", "source_end_utc", "cutoff_utc"))
    if not start < end <= cutoff or cutoff-start > timedelta(days=MAX_DAYS):
        raise ValueError("INVALID_GLOBAL_COHORT_TIME_BOUNDS")
    from research_no_horizon_experiment import _scopes
    scopes = [{key: item[key] for key in ("candidate_key", "base_direction", "threshold_pct")}
              for item in _scopes(value["scopes"])]
    raw_parts = value["parts"]
    if not isinstance(raw_parts, list) or not 1 <= len(raw_parts) <= MAX_PARTS:
        raise ValueError("COHORT_PART_LIMIT_EXCEEDED")
    parts, cursor, byte_budget = [], start, 0
    for ordinal, raw in enumerate(raw_parts):
        _fields(raw, {"ordinal", "source_start_utc", "source_end_utc"},
                {"source_row_limit", "max_candle_rows", "page_size", "source_byte_limit"})
        if type(raw["ordinal"]) is not int or raw["ordinal"] != ordinal:
            raise ValueError("COHORT_PART_ORDINAL_MISMATCH")
        left, right = (contracts.utc(raw[k]) for k in ("source_start_utc", "source_end_utc"))
        if left != cursor or not left < right <= end:
            raise ValueError("COHORT_PART_INTERVAL_GAP_OR_OVERLAP")
        part = {"ordinal": ordinal, "source_start_utc": left.isoformat(), "source_end_utc": right.isoformat(),
            "source_row_limit": _integer(raw.get("source_row_limit", source.MAX_ROWS), "source_row_limit", source.MAX_ROWS),
            "max_candle_rows": _integer(raw.get("max_candle_rows", transport.MAX_CANDLES), "max_candle_rows", transport.MAX_CANDLES),
            "page_size": _integer(raw.get("page_size", transport.MAX_PAGE_ROWS), "page_size", transport.MAX_PAGE_ROWS),
            "source_byte_limit": _integer(raw.get("source_byte_limit", source.MAX_BYTES), "source_byte_limit", source.MAX_BYTES)}
        byte_budget += part["source_byte_limit"]
        parts.append(part)
        cursor = right
    if cursor != end:
        raise ValueError("COHORT_PARTS_DO_NOT_COVER_GLOBAL_INTERVAL")
    if byte_budget > MAX_TOTAL_BYTES:
        raise ValueError("DECLARED_COHORT_BYTE_BUDGET_EXCEEDED")
    bindings, budgets = _bindings(), _budgets()
    for name, expected in (("version_bindings", bindings), ("resource_budget", budgets)):
        if name in value and contracts.canonical(value[name]) != contracts.canonical(expected):
            raise ValueError("COHORT_VERSION_OR_RESOURCE_BINDING_MISMATCH")
    return {"cohort_version": VERSION, "cohort_key": key, "declared_at_utc": declared.isoformat(),
        "prior_outcomes_observed": value["prior_outcomes_observed"], "symbol": value["symbol"],
        "price_route": part_sql.ROUTE, "source_start_utc": start.isoformat(),
        "source_end_utc": end.isoformat(), "cutoff_utc": cutoff.isoformat(), "scopes": scopes,
        "gate_policy": coverage._policy(value.get("gate_policy")), "parts": parts,
        "version_bindings": bindings, "resource_budget": budgets}


def anchor_sql(declaration: Mapping[str, Any]) -> str:
    """Generate one statement containing every existing bounded part SELECT."""
    frozen = normalize_declaration(declaration)
    selects = []
    for part in frozen["parts"]:
        plan = {**part, "cutoff_utc": frozen["cutoff_utc"], "symbol": frozen["symbol"],
                "route": frozen["price_route"]}
        sql = part_sql.manifest_sql(plan).strip()
        if not sql.endswith(";"):
            raise ValueError("UNEXPECTED_PART_SQL_SHAPE")
        selects.append(f"SELECT {part['ordinal']}::integer AS ordinal, (\n{sql[:-1]}\n) AS manifest")
    body = "\nUNION ALL\n".join(selects)
    return f"""WITH cohort_parts AS MATERIALIZED (
{body}
)
SELECT jsonb_build_object(
  'anchor_version',{part_sql.literal(ANCHOR_VERSION)},
  'declaration_sha256',{part_sql.literal(contracts.digest(frozen))},
  'parts',(SELECT jsonb_agg(jsonb_build_object('ordinal',ordinal,'manifest',manifest)
                           ORDER BY ordinal) FROM cohort_parts),
  'extraction_receipt',jsonb_build_object(
    'transaction_mode','SINGLE_STATEMENT_READ_ONLY','consistent_read','ONE_STATEMENT_SNAPSHOT',
    'read_only',current_setting('transaction_read_only'),
    'mvcc_snapshot',pg_current_snapshot()::text,'extracted_at_utc',statement_timestamp(),
    'serializer','POSTGRESQL_JSONB_TEXT_UTF8_V1','serializer_timezone',current_setting('TimeZone'),
    'query_identity',{part_sql.literal(QUERY_IDENTITY)},'database_writes',false)) AS anchor;
"""


def _query_hash(declaration):
    return hashlib.sha256(anchor_sql(declaration).encode("utf-8")).hexdigest()


def _copy_anchor(value):
    if not isinstance(value, Mapping):
        raise ValueError("COHORT_ANCHOR_OBJECT_REQUIRED")
    encoded = contracts.canonical(value)
    transport._bytes(encoded, MAX_ANCHOR_BYTES)
    return transport._strict_json(encoded)


def seal_anchor(declaration: Mapping[str, Any], raw_anchor: Mapping[str, Any]) -> dict[str, Any]:
    """Attach the exact generated SQL hash once; never replace an existing hash.

    The caller must preserve the actual SQL/raw response as provenance evidence.
    This pure operation cannot verify which server executed that SQL.
    """
    frozen = normalize_declaration(declaration)
    anchor = _copy_anchor(raw_anchor)
    if "anchor_sha256" in anchor:
        raise ValueError("COHORT_ANCHOR_ALREADY_SEALED")
    query_sha = _query_hash(frozen)
    try:
        receipts = [anchor["extraction_receipt"]] + [part["manifest"]["receipt"] for part in anchor["parts"]]
        for receipt in receipts:
            if "query_sha256" in receipt and receipt["query_sha256"] != query_sha:
                raise ValueError("COHORT_QUERY_HASH_MISMATCH")
            receipt["query_sha256"] = query_sha
        anchor["anchor_sha256"] = contracts.digest(anchor)
    except (KeyError, TypeError) as exc:
        raise ValueError("INCOMPLETE_COHORT_ANCHOR") from exc
    return validate_anchor(frozen, anchor)


def validate_anchor(declaration: Mapping[str, Any], anchor: Mapping[str, Any]) -> dict[str, Any]:
    """Require an intact complete root plus all parts from the exact one query."""
    frozen = normalize_declaration(declaration)
    value = _copy_anchor(anchor)
    _fields(value, {"anchor_version", "declaration_sha256", "parts", "extraction_receipt", "anchor_sha256"})
    if (value["anchor_version"] != ANCHOR_VERSION
            or value["declaration_sha256"] != contracts.digest(frozen)
            or value["anchor_sha256"] != contracts.digest({k: v for k, v in value.items() if k != "anchor_sha256"})):
        raise ValueError("COHORT_ANCHOR_IDENTITY_OR_HASH_MISMATCH")
    receipt = value["extraction_receipt"]
    _fields(receipt, {"transaction_mode", "consistent_read", "read_only", "mvcc_snapshot",
                     "extracted_at_utc", "query_identity", "database_writes", "query_sha256",
                     "serializer", "serializer_timezone"})
    query_sha = _query_hash(frozen)
    if (receipt["transaction_mode"] != "SINGLE_STATEMENT_READ_ONLY"
            or receipt["consistent_read"] != "ONE_STATEMENT_SNAPSHOT" or receipt["read_only"] != "on"
            or receipt["database_writes"] is not False or receipt["query_identity"] != QUERY_IDENTITY
            or receipt["serializer"] != "POSTGRESQL_JSONB_TEXT_UTF8_V1"
            or not isinstance(receipt["serializer_timezone"], str) or not receipt["serializer_timezone"].strip()
            or receipt["query_sha256"] != query_sha
            or not isinstance(receipt["mvcc_snapshot"], str) or not receipt["mvcc_snapshot"].strip()):
        raise ValueError("COHORT_COMMON_QUERY_PROVENANCE_MISMATCH")
    extracted = contracts.utc(receipt["extracted_at_utc"])
    if extracted < max(contracts.utc(frozen["declared_at_utc"]), contracts.utc(frozen["cutoff_utc"])):
        raise ValueError("COHORT_ANCHOR_PRECEDES_DECLARATION_OR_CUTOFF")
    if not isinstance(value["parts"], list) or len(value["parts"]) != len(frozen["parts"]):
        raise ValueError("COMPLETE_COHORT_PART_SET_REQUIRED")
    source_ids, source_count, raw_bytes, parent_pins = set(), 0, 0, {}
    for expected, record in zip(frozen["parts"], value["parts"]):
        _fields(record, {"ordinal", "manifest"})
        if type(record["ordinal"]) is not int or record["ordinal"] != expected["ordinal"]:
            raise ValueError("COHORT_ANCHOR_PART_ORDER_MISMATCH")
        part = part_sql._pinned_manifest(record["manifest"])
        part_receipt = part["receipt"]
        if (part["symbol"] != frozen["symbol"] or part["route"] != frozen["price_route"]
                or any(contracts.utc(part[key]) != contracts.utc(reference) for key, reference in (
                    ("source_start_utc", expected["source_start_utc"]),
                    ("source_end_utc", expected["source_end_utc"]), ("cutoff_utc", frozen["cutoff_utc"])))
                or part_receipt.get("query_identity") != "no-horizon-pinned-parent-manifest-select-v2"
                or part_receipt.get("database_writes") is not False
                or part_receipt["query_sha256"] != query_sha
                or part_receipt["mvcc_snapshot"] != receipt["mvcc_snapshot"]
                or part_receipt.get("serializer") != receipt["serializer"]
                or part_receipt.get("serializer_timezone") != receipt["serializer_timezone"]
                or contracts.utc(part_receipt["extracted_at_utc"]) != extracted
                or any(part_receipt[name] != expected[declared] for name, declared in (
                    ("source_cap", "source_row_limit"), ("candle_cap", "max_candle_rows"), ("page_size", "page_size")))):
            raise ValueError("COHORT_PART_IDENTITY_CAP_OR_SNAPSHOT_MISMATCH")
        count = len(part["source_entries"])
        total = sum(item["byte_length"] for item in part["source_entries"] + part["candle_pages"])
        if total > expected["source_byte_limit"]:
            raise ValueError("COHORT_PART_DECLARED_RAW_BYTE_LIMIT_EXCEEDED")
        for entry in part["source_entries"]:
            if entry["snapshot_set_id"] in source_ids:
                raise ValueError("DUPLICATE_SOURCE_ID_ACROSS_COHORT_PARTS")
            source_ids.add(entry["snapshot_set_id"])
            pin_text = entry["btc_parent_payload_text"]
            parent = json.loads(pin_text, parse_float=Decimal)
            parent_id = parent.get("btc_parent_movement_id") if isinstance(parent, dict) else None
            if isinstance(parent_id, str) and parent_id:
                if parent_id in parent_pins and parent_pins[parent_id] != pin_text:
                    raise ValueError("CONFLICTING_PARENT_PINS_IN_COMMON_ANCHOR")
                parent_pins[parent_id] = pin_text
        source_count += count
        raw_bytes += total
    if (source_count > MAX_SOURCE_ROWS or raw_bytes > MAX_TOTAL_BYTES
            or source_count * len(frozen["scopes"]) > MAX_SOURCE_SCOPE_DECISIONS):
        raise ValueError("COHORT_GLOBAL_INPUT_OR_DECISION_BUDGET_EXCEEDED")
    return value
