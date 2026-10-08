"""Bounded read-only acquisition of one already declared global cohort.

The database supplies exact JSONB text; local hashes preserve those bytes, not
authenticate their origin. All parts share the existing single anchor. This
adapter neither creates declarations nor writes source or execution databases.
"""
from __future__ import annotations

import hashlib
from typing import Mapping

import research_no_horizon_cohort as cohort
import research_no_horizon_contract as contracts
import research_no_horizon_manifest as transport
import research_no_horizon_manifest_sql as queries

VERSION = "no-horizon-read-only-cohort-acquisition-v1"
_PROOF_FIELDS = {"proof_version", "kind", "declaration_sha256", "anchor_sha256",
    "part_ordinal", "ordinals", "sql", "query_sha256", "raw_response_text",
    "raw_response_sha256", "raw_response_bytes"}
# Outer JSON escapes retained payload strings. Leaf bytes remain independently
# limited by the original manifest; this is only a response envelope budget.
MAX_ANCHOR_RESPONSE_BYTES = 2 * cohort.MAX_ANCHOR_BYTES
MAX_CHUNK_RESPONSE_BYTES = 6 * queries.SOURCE_PAGE_SIZE * transport.MAX_CHUNK_BYTES + 1024 * 1024


class SourceNotDue(Exception):
    """The source database clock has not reached declaration/cutoff eligibility."""


NotDue = SourceNotDue


class IntegrityError(ValueError):
    """Rejected fetched evidence; retain its proof or explicit size diagnostic."""
    def __init__(self, message, *, proof=None, diagnostic=None):
        super().__init__(message)
        self.proof = proof
        self.diagnostic = diagnostic


def _sha(text):
    return hashlib.sha256(text.encode("utf-8", errors="strict")).hexdigest()


def _proof(declaration, *, kind, anchor_sha256, part_ordinal, ordinals, sql, raw):
    return {"proof_version": VERSION, "kind": kind,
        "declaration_sha256": contracts.digest(declaration), "anchor_sha256": anchor_sha256,
        "part_ordinal": part_ordinal, "ordinals": list(ordinals), "sql": sql,
        "query_sha256": _sha(sql), "raw_response_text": raw,
        "raw_response_sha256": _sha(raw), "raw_response_bytes": len(raw.encode("utf-8"))}


def _raw_json(data):
    return bytes(data).decode("utf-8", errors="strict")


def _configure_cursor(cursor):
    # Cursor-local: never alter a caller's connection/global JSON parser.
    from psycopg.types.json import set_json_loads
    set_json_loads(_raw_json, cursor)


def _execute(conn, declaration, sql, column, maximum):
    if int(conn.info.transaction_status) != 0:
        raise ValueError("ACQUISITION_REQUIRES_IDLE_SOURCE_CONNECTION")
    due = max(contracts.utc(declaration[key]) for key in ("declared_at_utc", "cutoff_utc"))
    with conn.transaction():
        conn.execute("SET TRANSACTION READ ONLY")
        conn.execute("SET LOCAL TIME ZONE 'UTC'")
        conn.execute("SET LOCAL DateStyle='ISO, YMD'")
        conn.execute("SET LOCAL extra_float_digits=3")
        conn.execute("SET LOCAL statement_timeout='15000ms'")
        conn.execute("SET LOCAL lock_timeout='1000ms'")
        state = conn.execute("SELECT clock_timestamp() AS now_utc, "
            "current_setting('transaction_read_only') AS read_only").fetchone()
        if state["read_only"] != "on":
            raise ValueError("ACQUISITION_SOURCE_TRANSACTION_NOT_READ_ONLY")
        if contracts.utc(state["now_utc"]) < due:
            raise SourceNotDue("SOURCE_DATABASE_CLOCK_PRECEDES_DECLARATION_OR_CUTOFF")
        with conn.cursor() as cursor:
            _configure_cursor(cursor)
            cursor.execute(sql)
            rows = cursor.fetchmany(2)
            if len(rows) != 1 or not isinstance(rows[0], Mapping) or set(rows[0]) != {column}:
                raise ValueError("ACQUISITION_RESPONSE_CARDINALITY_OR_COLUMN_MISMATCH")
            raw = rows[0][column]
            if isinstance(raw, str) and len(raw.encode("utf-8")) > maximum:
                raise IntegrityError("ACQUISITION_RESPONSE_BYTE_LIMIT_EXCEEDED", diagnostic={
                    "query_sha256": _sha(sql), "raw_response_sha256": _sha(raw),
                    "raw_response_bytes": len(raw.encode("utf-8")), "response_byte_limit": maximum,
                    "reason": "ACQUISITION_RESPONSE_BYTE_LIMIT_EXCEEDED",
                    "truncated": True, "raw_retained": False, "raw_response_retained": False})
            transport._bytes(raw, maximum)
            return raw


def _raw_proof(declaration, proof, *, kind, anchor_sha256, part_ordinal, ordinals, sql):
    if not isinstance(proof, Mapping) or set(proof) != _PROOF_FIELDS:
        raise ValueError("ACQUISITION_PROOF_FIELDS_MISMATCH")
    maximum = MAX_ANCHOR_RESPONSE_BYTES if kind == "anchor" else MAX_CHUNK_RESPONSE_BYTES
    raw = proof["raw_response_text"]
    transport._bytes(raw, maximum)
    expected = _proof(declaration, kind=kind, anchor_sha256=anchor_sha256,
        part_ordinal=part_ordinal, ordinals=ordinals, sql=sql, raw=raw)
    if (type(proof["raw_response_bytes"]) is not int
            or contracts.canonical(proof) != contracts.canonical(expected)):
        raise ValueError("ACQUISITION_PROOF_BINDING_MISMATCH")
    parsed = transport._strict_json(raw)
    if not isinstance(parsed, dict):
        raise ValueError("ACQUISITION_RESPONSE_OBJECT_REQUIRED")
    return parsed


def validate_anchor_proof(declaration, proof):
    declared = cohort.normalize_declaration(declaration)
    raw = _raw_proof(declared, proof, kind="anchor", anchor_sha256=None,
        part_ordinal=None, ordinals=[], sql=cohort.anchor_sql(declared))
    anchor = cohort.seal_anchor(declared, raw)
    if anchor["extraction_receipt"]["serializer_timezone"] != "UTC":
        raise ValueError("ACQUISITION_SERIALIZER_TIMEZONE_MISMATCH")
    return anchor


def read_anchor(conn, declaration):
    declared = cohort.normalize_declaration(declaration)
    sql = cohort.anchor_sql(declared)
    raw = _execute(conn, declared, sql, "anchor", MAX_ANCHOR_RESPONSE_BYTES)
    proof = _proof(declared, kind="anchor", anchor_sha256=None, part_ordinal=None,
        ordinals=[], sql=sql, raw=raw)
    try:
        validate_anchor_proof(declared, proof)
    except (ValueError, TypeError, KeyError, OverflowError) as exc:
        raise IntegrityError("ACQUISITION_ANCHOR_VALIDATION_FAILED", proof=proof) from exc
    return proof


def _part(declaration, anchor, part_ordinal):
    declared = cohort.normalize_declaration(declaration)
    frozen = cohort.validate_anchor(declared, anchor)
    transport._integer(part_ordinal, "acquisition part ordinal", len(frozen["parts"]) - 1)
    if frozen["extraction_receipt"]["serializer_timezone"] != "UTC":
        raise ValueError("ACQUISITION_SERIALIZER_TIMEZONE_MISMATCH")
    return declared, frozen, frozen["parts"][part_ordinal]["manifest"]


def leaf_tasks(declaration, anchor):
    declared = cohort.normalize_declaration(declaration)
    frozen = cohort.validate_anchor(declared, anchor)
    result = []
    for part in frozen["parts"]:
        manifest = part["manifest"]
        for start in range(0, len(manifest["source_entries"]), queries.SOURCE_PAGE_SIZE):
            result.append({"part_ordinal": part["ordinal"], "kind": "source",
                "ordinals": list(range(start, min(start + queries.SOURCE_PAGE_SIZE,
                                                  len(manifest["source_entries"]))))})
        result.extend({"part_ordinal": part["ordinal"], "kind": "candles", "ordinals": [page["ordinal"]]}
                      for page in manifest["candle_pages"])
    return result


def _chunk_query(manifest, kind, ordinals):
    if kind == "source":
        sql = queries.source_chunk_sql(manifest, ordinals)
        if list(ordinals) != sorted(ordinals):
            raise ValueError("ACQUISITION_SOURCE_ORDINALS_MUST_BE_ORDERED")
        return sql
    if kind == "candles" and isinstance(ordinals, list) and len(ordinals) == 1:
        return queries.candle_chunk_sql(manifest, ordinals[0])
    raise ValueError("ACQUISITION_CHUNK_KIND_OR_ORDINALS_INVALID")


def _check_receipt(receipt, *, declared, anchor, kind):
    query_identity = ("no-horizon-pinned-parent-source-chunk-select-v2" if kind == "source"
                      else "no-horizon-exact-candle-chunk-select-v1")
    if (not isinstance(receipt, Mapping) or receipt.get("read_only") != "on"
            or receipt.get("database_writes") is not False
            or receipt.get("serializer_timezone") != "UTC"
            or receipt.get("query_identity") != query_identity
            or not isinstance(receipt.get("mvcc_snapshot"), str) or not receipt["mvcc_snapshot"].strip()
            or contracts.utc(receipt["fetched_at_utc"]) < max(contracts.utc(declared["declared_at_utc"]),
                contracts.utc(declared["cutoff_utc"]), contracts.utc(anchor["extraction_receipt"]["extracted_at_utc"]))
            or kind == "source" and receipt.get("parent_snapshot_mode") != queries.PARENT_SNAPSHOT_MODE):
        raise ValueError("ACQUISITION_CHUNK_QUERY_PROVENANCE_MISMATCH")


def _leaf_text(entry, expected):
    raw = transport._bytes(entry.get("payload_text"), transport.MAX_CHUNK_BYTES)
    if (type(entry.get("byte_length")) is not int or entry["byte_length"] != len(raw)
            or entry["byte_length"] != expected["byte_length"]
            or entry.get("sha256") != hashlib.sha256(raw).hexdigest()
            or entry["sha256"] != expected["sha256"]):
        raise ValueError("ACQUISITION_LEAF_HASH_OR_BYTE_LENGTH_MISMATCH")
    return transport._strict_json(entry["payload_text"])


def validate_chunk_proof(declaration, anchor, proof):
    if not isinstance(proof, Mapping):
        raise ValueError("ACQUISITION_PROOF_OBJECT_REQUIRED")
    declared, frozen, manifest = _part(declaration, anchor, proof.get("part_ordinal"))
    kind, ordinals = proof.get("kind"), proof.get("ordinals")
    sql = _chunk_query(manifest, kind, ordinals)
    response = _raw_proof(declared, proof, kind=kind, anchor_sha256=frozen["anchor_sha256"],
        part_ordinal=proof["part_ordinal"], ordinals=ordinals, sql=sql)
    _check_receipt(response.get("receipt"), declared=declared, anchor=frozen, kind=kind)
    if response.get("kind") != kind or response.get("overflow") is not False:
        raise ValueError("ACQUISITION_CHUNK_KIND_OR_OVERFLOW")
    if kind == "source":
        entries = response.get("entries")
        if (type(response.get("requested_count")) is not int
                or type(response.get("returned_count")) is not int
                or response["requested_count"] != len(ordinals)
                or response["returned_count"] != len(ordinals)
                or not isinstance(entries, list) or len(entries) != len(ordinals)
                or any(not isinstance(entry, dict) or type(entry.get("ordinal")) is not int for entry in entries)
                or [entry["ordinal"] for entry in entries] != ordinals):
            raise ValueError("ACQUISITION_SOURCE_CHUNK_INCOMPLETE_OR_EXTRA")
        for entry in entries:
            expected = manifest["source_entries"][entry["ordinal"]]
            parsed = _leaf_text(entry, expected)
            if (type(entry.get("snapshot_set_id")) is not int
                    or entry["snapshot_set_id"] != expected["snapshot_set_id"]
                    or not isinstance(parsed, dict)
                    or (parsed.get("intake") or {}).get("snapshot_set_id") != expected["snapshot_set_id"]):
                raise ValueError("ACQUISITION_SOURCE_LEAF_IDENTITY_MISMATCH")
        return [{"ordinal": entry["ordinal"], "payload_text": entry["payload_text"]} for entry in entries]
    expected = manifest["candle_pages"][ordinals[0]]
    parsed = _leaf_text(response, expected)
    if (type(response.get("ordinal")) is not int or response["ordinal"] != ordinals[0]
            or type(response.get("row_count")) is not int or response["row_count"] != expected["row_count"]
            or not isinstance(parsed, list) or len(parsed) != expected["row_count"]):
        raise ValueError("ACQUISITION_CANDLE_PAGE_IDENTITY_OR_COUNT_MISMATCH")
    return [{"ordinal": ordinals[0], "payload_text": response["payload_text"]}]


def _read_chunk(conn, declaration, anchor, part_ordinal, kind, ordinals):
    declared, frozen, manifest = _part(declaration, anchor, part_ordinal)
    sql = _chunk_query(manifest, kind, ordinals)
    raw = _execute(conn, declared, sql, "chunk", MAX_CHUNK_RESPONSE_BYTES)
    proof = _proof(declared, kind=kind, anchor_sha256=frozen["anchor_sha256"],
        part_ordinal=part_ordinal, ordinals=ordinals, sql=sql, raw=raw)
    try:
        validate_chunk_proof(declared, frozen, proof)
    except (ValueError, TypeError, KeyError, OverflowError) as exc:
        raise IntegrityError("ACQUISITION_CHUNK_VALIDATION_FAILED", proof=proof) from exc
    return proof


def read_source_chunk(conn, declaration, anchor, part_ordinal, ordinals):
    return _read_chunk(conn, declaration, anchor, part_ordinal, "source", ordinals)


def read_candle_chunk(conn, declaration, anchor, part_ordinal, page_ordinal):
    return _read_chunk(conn, declaration, anchor, part_ordinal, "candles", [page_ordinal])


def assemble_part(declaration, anchor, part_ordinal, source_proofs, candle_proofs):
    declared, frozen, manifest = _part(declaration, anchor, part_ordinal)
    sources, candles = [], []
    for proofs, kind, target in ((source_proofs, "source", sources), (candle_proofs, "candles", candles)):
        maximum = len(manifest["source_entries" if kind == "source" else "candle_pages"])
        if not isinstance(proofs, (list, tuple)) or len(proofs) > maximum:
            raise ValueError("ACQUISITION_PART_PROOF_COUNT_EXCEEDED")
        for proof in proofs:
            if proof.get("part_ordinal") != part_ordinal or proof.get("kind") != kind:
                raise ValueError("ACQUISITION_PART_PROOF_IDENTITY_MISMATCH")
            target.extend(validate_chunk_proof(declared, frozen, proof))
    export = transport.assemble_export(manifest, sources, candles)
    transport._bytes(contracts.canonical(export), declared["parts"][part_ordinal]["source_byte_limit"])
    return export
