"""Assemble bounded later reads against a single-snapshot source manifest.

Exact UTF-8 chunk proofs are verified before JSON parsing. The compact exported
receipt binds the parsed payload and anchor manifest; raw text proofs remain in
external artifacts. This is an external-assembler/DB provenance attestation,
not cryptographic authentication of a database or prospective formula evidence.
No database, network, filesystem, model evaluation, or runtime action occurs.
"""
from __future__ import annotations

from datetime import timedelta
import hashlib
import json
import math
import re
from typing import Any, Mapping

import research_no_horizon_contract as contracts

MODE = "MANIFEST_ATTESTED_MULTI_READ_V1"
VERSION = MODE
EXPORT_VERSION = "no-horizon-watch-source-export-v1"
ROUTE = "BINANCE_SPOT_TRADE_1M"
MAX_BYTES = 64 * 1024 * 1024
MAX_MANIFEST_BYTES = 1024 * 1024
MAX_CHUNK_BYTES = 2 * 1024 * 1024
MAX_SOURCE_ROWS = 256
MAX_CANDLES = 31 * 1440
MAX_PAGES = 1024
MAX_PAGE_ROWS = 2048
_IDENTITY = ("export_version", "symbol", "route", "source_start_utc", "source_end_utc", "cutoff_utc")
_ASSURANCE = "EXTERNALLY_ATTESTED_NOT_CRYPTOGRAPHICALLY_AUTHENTICATED"


def _bytes(value: Any, limit: int) -> bytes:
    if not isinstance(value, str):
        raise ValueError("payload_text must be an exact UTF-8 string")
    raw = value.encode("utf-8", errors="strict")
    if len(raw) > limit:
        raise ValueError("manifest transport byte limit exceeded")
    return raw


def _integer(value, name, maximum, minimum=0):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError("invalid manifest " + name)
    return value


def _hash(value):
    if not isinstance(value, str) or re.fullmatch("[0-9a-f]{64}", value) is None:
        raise ValueError("invalid manifest SHA256")
    return value


def _strict_json(text):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON object key")
            result[key] = value
        return result
    def number(value):
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError("nonfinite JSON number")
        return parsed
    def constant(_):
        raise ValueError("nonfinite JSON constant")
    result = json.loads(text, object_pairs_hook=pairs, parse_float=number, parse_constant=constant)
    # Reject escaped unpaired surrogates as well as non-UTF-8 input strings.
    contracts.canonical(result).encode("utf-8", errors="strict")
    return result


def _manifest(manifest):
    if not isinstance(manifest, Mapping):
        raise ValueError("manifest object required")
    _bytes(contracts.canonical(manifest), MAX_MANIFEST_BYTES)
    manifest = _strict_json(contracts.canonical(manifest))
    if (manifest.get("transport_version") != MODE or manifest.get("export_version") != EXPORT_VERSION
            or manifest.get("route") != ROUTE or manifest.get("symbol") not in
            {"BTC", "ETH", "SOL", "DOGE", "ZEC", "BNB", "XRP"}):
        raise ValueError("unsupported manifest identity or mode")
    start, end, cutoff = (contracts.utc(manifest[key]) for key in
        ("source_start_utc", "source_end_utc", "cutoff_utc"))
    if not start < end <= cutoff or cutoff-start > timedelta(days=31):
        raise ValueError("invalid manifest time bounds")
    receipt = manifest.get("receipt")
    if not isinstance(receipt, Mapping):
        raise ValueError("anchor receipt required")
    if (receipt.get("transaction_mode") != "SINGLE_STATEMENT_READ_ONLY" or receipt.get("read_only") != "on"
            or receipt.get("source_overflow") is not False or receipt.get("candle_overflow") is not False):
        raise ValueError("incomplete or writable anchor snapshot")
    if not isinstance(receipt.get("mvcc_snapshot"), str) or not receipt["mvcc_snapshot"].strip():
        raise ValueError("anchor MVCC snapshot required")
    _hash(receipt.get("query_sha256"))
    if contracts.utc(receipt["extracted_at_utc"]) < cutoff:
        raise ValueError("anchor extraction precedes cutoff")
    source_cap = _integer(receipt.get("source_cap"), "source_cap", MAX_SOURCE_ROWS, 1)
    candle_cap = _integer(receipt.get("candle_cap"), "candle_cap", MAX_CANDLES, 1)
    source_count = _integer(receipt.get("source_count"), "source_count", source_cap)
    candle_count = _integer(receipt.get("candle_count"), "candle_count", candle_cap)
    page_size = _integer(receipt.get("page_size"), "page_size", MAX_PAGE_ROWS, 1)
    for key, count in (("source_count_capped_plus_one", source_count), ("candle_count_capped_plus_one", candle_count)):
        if type(receipt.get(key)) is not int or receipt[key] != count:
            raise ValueError("anchor cap+1 count is incomplete")
    # Optional redundant SQL flags cannot contradict the required cap+1 proof.
    for key in ("truncated", "candle_truncated"):
        if key in receipt and receipt[key] is not False:
            raise ValueError("truncated anchor snapshot")
    for key in ("rows_complete", "candles_complete"):
        if key in receipt and receipt[key] is not True:
            raise ValueError("incomplete anchor snapshot")
    entries, pages = manifest.get("source_entries"), manifest.get("candle_pages")
    if (not isinstance(entries, list) or len(entries) != source_count
            or not isinstance(pages, list) or len(pages) > MAX_PAGES):
        raise ValueError("manifest entry count mismatch")
    previous, ids, raw_bytes = None, set(), 0
    for ordinal, entry in enumerate(entries):
        if not isinstance(entry, Mapping) or type(entry.get("ordinal")) is not int or entry["ordinal"] != ordinal:
            raise ValueError("source manifest ordinal mismatch")
        source_id = _integer(entry.get("snapshot_set_id"), "snapshot_set_id", 2**63-1, 1)
        usable = contracts.utc(entry["usable_from_utc"])
        key = (usable, source_id)
        if source_id in ids or not start <= usable < end or previous is not None and key <= previous:
            raise ValueError("source manifest identity/order mismatch")
        previous = key
        ids.add(source_id)
        _hash(entry.get("sha256"))
        raw_bytes += _integer(entry.get("byte_length"), "source byte_length", MAX_CHUNK_BYTES, 1)
    previous, count = None, 0
    for ordinal, page in enumerate(pages):
        if not isinstance(page, Mapping) or type(page.get("ordinal")) is not int or page["ordinal"] != ordinal:
            raise ValueError("candle manifest ordinal mismatch")
        first, last = (contracts.utc(page[key]) for key in ("first_open_utc", "last_open_utc"))
        if (first > last or previous is not None and first <= previous
                or first < start.replace(second=0, microsecond=0) or last+timedelta(minutes=1) > cutoff
                or any(value.second or value.microsecond for value in (first, last))):
            raise ValueError("candle manifest bounds/order mismatch")
        previous = last
        count += _integer(page.get("row_count"), "page row_count", page_size, 1)
        _hash(page.get("sha256"))
        raw_bytes += _integer(page.get("byte_length"), "candle byte_length", MAX_CHUNK_BYTES, 1)
    if count != candle_count or raw_bytes > MAX_BYTES:
        raise ValueError("manifest candle count or total byte limit mismatch")
    return manifest


def _chunks(chunks, entries):
    if not isinstance(chunks, (list, tuple)) or len(chunks) != len(entries):
        raise ValueError("complete exact chunk set required")
    indexed = {}
    for chunk in chunks:
        if not isinstance(chunk, Mapping):
            raise ValueError("chunk object required")
        ordinal = _integer(chunk.get("ordinal"), "chunk ordinal", len(entries)-1)
        if ordinal in indexed:
            raise ValueError("duplicate chunk ordinal")
        raw = _bytes(chunk.get("payload_text"), MAX_CHUNK_BYTES)
        entry = entries[ordinal]
        if len(raw) != entry["byte_length"] or hashlib.sha256(raw).hexdigest() != entry["sha256"]:
            raise ValueError("raw chunk hash/byte length mismatch")
        indexed[ordinal] = _strict_json(chunk["payload_text"])
    return [indexed[ordinal] for ordinal in range(len(entries))]


def _payload_identity(export):
    return {key: export[key] for key in _IDENTITY}


def _validate_payload(export, manifest):
    if _payload_identity(export) != _payload_identity(manifest):
        raise ValueError("assembled export identity differs from anchor")
    rows, bars = export.get("source_rows"), export.get("candles")
    if not isinstance(rows, list) or len(rows) != len(manifest["source_entries"]) or not isinstance(bars, list):
        raise ValueError("assembled payload counts mismatch")
    for row, entry in zip(rows, manifest["source_entries"]):
        intake = row.get("intake") if isinstance(row, Mapping) else None
        if (not isinstance(intake, Mapping) or type(intake.get("snapshot_set_id")) is not int
                or intake["snapshot_set_id"] != entry["snapshot_set_id"]
                or contracts.utc(intake["usable_from_utc"]) != contracts.utc(entry["usable_from_utc"])):
            raise ValueError("assembled source identity/order mismatch")
        raw = row.get("stored_source")
        if isinstance(raw, Mapping) and raw.get("snapshot_set_id") not in (None, entry["snapshot_set_id"]):
            raise ValueError("assembled stored source identity mismatch")
    if len(bars) != manifest["receipt"]["candle_count"]:
        raise ValueError("assembled candle count mismatch")
    cursor, previous = 0, None
    start, cutoff = contracts.utc(manifest["source_start_utc"]), contracts.utc(manifest["cutoff_utc"])
    for page in manifest["candle_pages"]:
        group = bars[cursor:cursor+page["row_count"]]
        cursor += page["row_count"]
        for bar in group:
            if not isinstance(bar, Mapping) or bar.get("route") != manifest["route"] or bar.get("symbol") != manifest["symbol"]:
                raise ValueError("assembled candle source identity mismatch")
            opened = contracts.utc(bar["open_time_utc"])
            if (opened.second or opened.microsecond or previous is not None and opened <= previous
                    or opened < start.replace(second=0, microsecond=0) or opened+timedelta(minutes=1) > cutoff):
                raise ValueError("assembled candle order/bounds mismatch")
            previous = opened
        if (contracts.utc(group[0]["open_time_utc"]) != contracts.utc(page["first_open_utc"])
                or contracts.utc(group[-1]["open_time_utc"]) != contracts.utc(page["last_open_utc"])):
            raise ValueError("assembled candle page bounds mismatch")


def _binding(export, manifest_sha):
    return contracts.digest({"identity": _payload_identity(export), "source_rows": export["source_rows"],
        "candles": export["candles"], "anchor_manifest_sha256": manifest_sha})


def _receipt(manifest, manifest_sha, binding):
    return {"transaction_mode": MODE, "expected_accepted_rows": manifest["receipt"]["source_count"],
        "expected_accepted_rows_exact": True, "expected_candles": manifest["receipt"]["candle_count"],
        "rows_complete": True, "truncated": False, "candle_truncated": False,
        "anchor_manifest": manifest, "anchor_manifest_sha256": manifest_sha,
        "canonical_payload_sha256": binding, "raw_chunk_proofs_verified_by_assembler": True,
        "raw_chunk_proof_location": "EXTERNAL_ARTIFACTS", "provenance_assurance": _ASSURANCE,
        "db_origin_authenticated_by_this_tool": False, "is_prospective_formula_evidence": False}


def assemble_export(manifest, source_chunks, candle_chunks):
    """Chunks are lists of {ordinal, payload_text}; wrapper metadata is ignored.

    They may arrive out of order, but the exact required ordinal set is mandatory.
    A source payload is one full source-row object; a candle payload is an array.
    The original chunk strings should be retained externally with the manifest.
    """
    anchor = _manifest(manifest)
    rows = _chunks(source_chunks, anchor["source_entries"])
    pages = _chunks(candle_chunks, anchor["candle_pages"])
    for page, entry in zip(pages, anchor["candle_pages"]):
        if not isinstance(page, list) or len(page) != entry["row_count"]:
            raise ValueError("candle chunk row count mismatch")
    export = {**_payload_identity(anchor), "source_rows": rows, "candles": [bar for page in pages for bar in page]}
    _validate_payload(export, anchor)
    anchor_sha = contracts.digest(anchor)
    export["source_receipt"] = _receipt(anchor, anchor_sha, _binding(export, anchor_sha))
    _bytes(contracts.canonical(export), MAX_BYTES)
    return export


def validate_export_binding(export):
    """Validate compact binding; raw-text proof verification is external attestation.

    This detects accidental/tampered payload changes against its retained receipt.
    Someone able to replace the manifest and receipt can forge the attestation;
    this method therefore never claims authentication of the originating DB.
    """
    if not isinstance(export, Mapping):
        raise ValueError("assembled export object required")
    _bytes(contracts.canonical(export), MAX_BYTES)
    receipt = export.get("source_receipt")
    if not isinstance(receipt, Mapping) or receipt.get("transaction_mode") != MODE:
        raise ValueError("unsupported assembled receipt mode")
    anchor = _manifest(receipt.get("anchor_manifest"))
    _validate_payload(export, anchor)
    anchor_sha = contracts.digest(anchor)
    expected = _receipt(anchor, anchor_sha, _binding(export, anchor_sha))
    if dict(receipt) != expected:
        raise ValueError("assembled manifest/payload receipt binding mismatch")


validate_export_receipt = validate_export_binding
