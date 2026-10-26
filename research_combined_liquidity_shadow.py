"""Prospective DOGE Combined selected-slot evidence, never a formula input.

The existing event queue remains authoritative and untouched. This module
freezes a delivered producer call after that queue accepted it, then writes
bounded audit evidence separately. A later matching source row is only a
current persisted-material link, never proof of original insertion/visibility.
"""
from __future__ import annotations

import base64
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timezone
from functools import lru_cache
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import queue
import sqlite3
import threading
import time
from uuid import uuid4

import research_consumed_inputs as graph

VERSION = "doge-combined-selected-slot-shadow-v1"
CONTRACT = {
    "version": VERSION, "scope": "DOGE delivered COMBINED_CONFIRMATION producer calls",
    "selection": "Exact event symbol/timeframe/source_side; unique raw top-item slot and compact event slot; no ranking or fallback",
    "mapping": "SHORT liquidation side -> LONG price direction and target above price; LONG -> SHORT and target below",
    "share": "100 * selected near_amount / (near_amount + opposite far_amount)",
    "reported_share_tolerance_percentage_points": 0.05,
    "alignment": {"SUPPORTS_min_inclusive": 60, "OPPOSES_max_inclusive": 40, "otherwise": "BALANCED"},
    "epoch": "Source event clock >= this process module-start clock. Registry retains first durable writer's reported process-start only; restart coverage gaps remain explicit.",
    "enrolled_in_existing_study": False, "modifies_source_event_or_features": False,
    "source_link": "Current stored full event material, SQL JSON numeric semantics and UTC clocks; not first insertion or original visibility",
}
CONTRACT_SHA256 = graph.digest(CONTRACT)
PROCESS_STARTED_AT_UTC = datetime.now(timezone.utc).isoformat()
CODE_FILES = ("research_combined_liquidity_shadow.py", "research_event_runtime.py",
              "research_event_capture.py", "research_event_store.py", "research_consumed_inputs.py",
              "research_ordered_question_catalog.py", "main.py", "analysis.py", "research_watch_decision_capture.py")
MAX_RAW_BYTES = 8 * 1024 * 1024
MAX_COMPRESSED_BYTES = 2 * 1024 * 1024
MAX_QUEUE_BYTES = 8 * 1024 * 1024
MAX_QUEUE_ITEMS = 4
MAX_LINK_BYTES = 256 * 1024
LINK_ATTEMPTS = 8
LINK_WAIT_SECONDS = 0.5
SOURCE_FIELDS = ("schema_version", "event_kind", "event_type", "alert_time_utc", "symbol", "direction",
                 "source_side", "timeframe", "score", "current_price", "target_price", "initial_target_distance_pct",
                 "categories", "setup_key", "event_fingerprint", "strategy_version", "code_version",
                 "runtime_session_id", "capture_stage", "delivery_status", "delivery_attempted_at_utc",
                 "delivered_at_utc", "engine_snapshot")
EVENT_FIELDS = set(SOURCE_FIELDS) - {"runtime_session_id", "capture_stage", "delivery_status", "delivery_attempted_at_utc", "delivered_at_utc"}
_LOCK = threading.Lock()
_WORKER_LOCK = threading.Lock()
_QUEUE = queue.Queue(maxsize=MAX_QUEUE_ITEMS)
_WORKER = None
_STORE = None
_METRICS = dict(submitted=0, persisted=0, partial=0, failed=0, dropped=0,
                linked=0, unlinked=0, link_failures=0, queued_items=0, queued_bytes=0,
                skipped={}, last=None, last_link=None)
_SKIPS = {"SOURCE_NOT_QUEUED", "NOT_DELIVERED", "NOT_IN_SCOPE", "PRE_PROCESS_EPOCH"}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def utc(value):
    value = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise ValueError("NAIVE_CLOCK")
    return value.astimezone(timezone.utc)


def canonical_original(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str, allow_nan=False)


def legacy_material(value):
    paths = []
    def walk(item, path):
        if isinstance(item, datetime):
            paths.append({"path": path, "source_text": str(item), "normalized_text": graph.normalized(item)})
        elif isinstance(item, dict):
            for key, child in item.items():
                walk(child, path + [key])
        elif isinstance(item, (list, tuple)):
            for index, child in enumerate(item):
                walk(child, path + [index])
    walk(value, [])
    return canonical_original(value), paths


def _validate_original(value, encoded, paths):
    original = json.loads(encoded)
    if canonical_original(original) != encoded:
        raise ValueError("SHADOW_ORIGINAL_NOT_CANONICAL")
    seen = set()
    for bridge in paths:
        path = tuple(bridge["path"])
        if not path or path in seen:
            raise ValueError("SHADOW_ORIGINAL_CLOCK_PATH_INVALID")
        seen.add(path)
        parent = original
        for key in path[:-1]:
            parent = parent[key]
        key = path[-1]
        if parent[key] != bridge["source_text"] or utc(bridge["source_text"]).isoformat() != bridge["normalized_text"]:
            raise ValueError("SHADOW_ORIGINAL_CLOCK_BRIDGE_INVALID")
        parent[key] = bridge["normalized_text"]
    if graph.normalized(original) != value:
        raise ValueError("SHADOW_ORIGINAL_BINDING_INVALID")


@lru_cache(maxsize=1)
def code_versions():
    root = Path(__file__).resolve().parent
    return {name: hashlib.sha256((root / name).read_bytes()).hexdigest() if (root / name).is_file() else None for name in CODE_FILES}


def database_url():
    dedicated = os.getenv("RESEARCH_DATABASE_URL", "").strip()
    allowed = os.getenv("RESEARCH_USE_PRIMARY_DATABASE", "").strip().lower() in {"1", "true", "yes", "on"}
    return dedicated or (os.getenv("DATABASE_URL", "").strip() if allowed else "")


def record_skip(reason):
    """Fixed reason codes only; never expose event content or database URLs."""
    reason = reason if reason in _SKIPS else "NOT_IN_SCOPE"
    with _LOCK:
        _METRICS["skipped"][reason] = _METRICS["skipped"].get(reason, 0) + 1


def number(value):
    if isinstance(value, bool) or value is None:
        return None
    try:
        result = float(value)
    except (ValueError, TypeError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def normalize_source_material(row):
    """Normalize exactly the columns serialized by the existing event writer."""
    material = {name: row.get(name) for name in SOURCE_FIELDS}
    for name in ("categories", "engine_snapshot"):
        if isinstance(material[name], str):
            material[name] = json.loads(material[name])
    for name in ("alert_time_utc", "delivery_attempted_at_utc", "delivered_at_utc"):
        if material[name] is not None:
            material[name] = utc(material[name]).isoformat()
    return graph.normalized(material)


def derive_context(event, selected_top_item):
    """A new, namespaced audit contract. Never mutates or repairs source data."""
    prefix = "shadow.combined_liquidity."
    def result(status, reason, **values):
        return {prefix + "capture_status": status, prefix + "reason": reason,
                "enrolled_in_existing_study": False, "modifies_source_event_or_features": False, **values}
    if event.get("event_type") != "COMBINED_CONFIRMATION" or event.get("symbol") != "DOGE":
        return result("NOT_APPLICABLE", "NOT_IN_SCOPE")
    symbol, tf, side = event.get("symbol"), event.get("timeframe"), event.get("source_side")
    expected_direction = {"SHORT": "LONG", "LONG": "SHORT"}.get(side)
    if expected_direction is None or event.get("direction") != expected_direction:
        return result("UNVERIFIED", "DIRECTION_MAPPING_INVALID")
    if (selected_top_item.get("symbol"), selected_top_item.get("timeframe"), selected_top_item.get("side")) != (symbol, tf, side):
        return result("UNVERIFIED", "SELECTED_TUPLE_MISMATCH")
    price, target = number(event.get("current_price")), number(event.get("target_price"))
    if price is None or target is None or price <= 0 or target <= 0 or not ((target > price) if expected_direction == "LONG" else (target < price)):
        return result("UNVERIFIED", "TARGET_DIRECTION_INVALID")
    for key in ("current_price", "target_price", "score"):
        if number(event.get(key)) is None or number(event.get(key)) != number(selected_top_item.get(key)):
            return result("UNVERIFIED", "EVENT_TOP_ITEM_VALUE_MISMATCH")
    snapshot = event.get("engine_snapshot") or {}
    if snapshot.get("alert_side") != side:
        return result("UNVERIFIED", "EVENT_DISPLAY_SIDE_MISMATCH")
    sources = []
    for value in (selected_top_item.get("maxpain_timeframes"), snapshot.get("maxpain_timeframes")):
        if not isinstance(value, (list, tuple)):
            return result("MISSING", "SELECTED_SLOT_ARRAY_MISSING")
        matches = [slot for slot in value if isinstance(slot, dict) and (slot.get("timeframe"), slot.get("source_side")) == (tf, side)]
        if not matches:
            return result("MISSING", "SELECTED_SLOT_MISSING")
        if len(matches) != 1:
            return result("UNVERIFIED", "SELECTED_SLOT_AMBIGUOUS")
        sources.append(matches[0])
    raw_slot, event_slot = sources
    for key in ("score", "target_price", "near_amount", "far_amount", "near_share_pct"):
        a, b = number(raw_slot.get(key)), number(event_slot.get(key))
        if a is None or b is None:
            return result("MISSING", "SELECTED_SLOT_VALUE_MISSING")
        if a != b:
            return result("UNVERIFIED", "RAW_COMPACT_SLOT_VALUE_MISMATCH")
    if number(raw_slot["score"]) != number(event["score"]) or number(raw_slot["target_price"]) != target:
        return result("UNVERIFIED", "EVENT_SLOT_VALUE_MISMATCH")
    for key in ("near_amount", "far_amount", "near_share_pct"):
        if key in selected_top_item and number(selected_top_item[key]) != number(raw_slot[key]):
            return result("UNVERIFIED", "ROOT_SLOT_VALUE_MISMATCH")
    near, far, reported = number(raw_slot["near_amount"]), number(raw_slot["far_amount"]), number(raw_slot["near_share_pct"])
    if near < 0 or far < 0 or near + far <= 0 or not 0 <= reported <= 100:
        return result("UNVERIFIED", "AMOUNT_DOMAIN_INVALID")
    share = 100 * near / (near + far)
    if not math.isclose(share, reported, rel_tol=0, abs_tol=0.05):
        return result("UNVERIFIED", "REPORTED_SHARE_MISMATCH")
    alignment = "SUPPORTS" if share >= 60 else "OPPOSES" if share <= 40 else "BALANCED"
    return result("VALID", "EXACT_SELECTED_SLOT", **{
        prefix + "selected_item_id": f"{symbol}|{tf}|{side}", prefix + "selected_amount": near,
        prefix + "opposite_amount": far, prefix + "selected_share_pct": share,
        prefix + "reported_share_pct": reported, prefix + "alignment": alignment,
        prefix + "source_side": side, prefix + "analysis_direction": expected_direction,
        prefix + "current_price": price, prefix + "target_price": target,
        prefix + "raw_selected_slot_sha256": graph.digest(raw_slot),
        prefix + "compact_selected_slot_sha256": graph.digest(event_slot),
    })


def prepare_capture(event, selected_top_item, *, event_writer_queue_accepted,
                    capture_stage="TELEGRAM_COMBINED_ALERT", delivery_status="DELIVERED",
                    delivery_attempted_at_utc=None, delivered_at_utc=None):
    """Freeze before returning to the producer; this function performs no I/O."""
    if not event_writer_queue_accepted:
        record_skip("SOURCE_NOT_QUEUED")
        return None
    if str(delivery_status).upper() != "DELIVERED":
        record_skip("NOT_DELIVERED")
        return None
    raw_event = event.to_dict() if hasattr(event, "to_dict") else deepcopy(dict(event))
    if raw_event.get("event_type") != "COMBINED_CONFIRMATION" or raw_event.get("symbol") != "DOGE":
        record_skip("NOT_IN_SCOPE")
        return None
    if utc(raw_event["alert_time_utc"]) < utc(PROCESS_STARTED_AT_UTC):
        record_skip("PRE_PROCESS_EPOCH")
        return None
    import research_event_store
    serialized = research_event_store.serialize_event(event, capture_stage=capture_stage,
        delivery_status=delivery_status, delivery_attempted_at_utc=delivery_attempted_at_utc,
        delivered_at_utc=delivered_at_utc)
    source_material = normalize_source_material(serialized)
    collector = graph.Collector(max_raw_bytes=MAX_RAW_BYTES, max_compressed_bytes=MAX_COMPRESSED_BYTES)
    top = dict(selected_top_item)
    event_text, event_paths = legacy_material(raw_event)
    top_text, top_paths = legacy_material(top)
    serialized_text, serialized_paths = legacy_material(serialized)
    collector.record("producer_input", {"event": raw_event, "selected_top_item": top,
        "serialized_event": serialized, "source_material": source_material,
        "legacy_event_canonical": event_text, "legacy_event_datetime_paths": event_paths,
        "legacy_selected_top_item_canonical": top_text, "legacy_selected_top_item_datetime_paths": top_paths,
        "legacy_serialized_event_canonical": serialized_text, "legacy_serialized_event_datetime_paths": serialized_paths})
    collector.record("derived_context", derive_context(raw_event, top))
    hashes = code_versions()
    if set(hashes) != set(CODE_FILES) or not all(isinstance(value, str) and len(value) == 64 for value in hashes.values()):
        collector.gap("SOURCE_IDENTITY_MISSING")
    collector.record("identity", {"runtime": {"python_version": platform.python_version(),
        "python_implementation": platform.python_implementation(), "render_git_commit": os.getenv("RENDER_GIT_COMMIT")},
        "code_versions": hashes})
    package = collector.package()
    manifest = {"version": VERSION, "contract": CONTRACT, "contract_sha256": CONTRACT_SHA256,
        "observation_id": str(uuid4()), "event_fingerprint": raw_event["event_fingerprint"],
        "process_started_at_utc": PROCESS_STARTED_AT_UTC, "producer_captured_at_utc": utc_now(),
        "source_alert_time_utc": utc(raw_event["alert_time_utc"]).isoformat(),
        "source_writer_queue_accepted": True, "source_material_sha256": graph.digest(source_material),
        "original_event_canonical_sha256": hashlib.sha256(event_text.encode("utf-8")).hexdigest(),
        "selected_top_item_canonical_sha256": hashlib.sha256(top_text.encode("utf-8")).hexdigest(),
        "serialized_event_canonical_sha256": hashlib.sha256(serialized_text.encode("utf-8")).hexdigest(),
        "original_first_match_input_proven": False, "original_event_insert_proven": False,
        "source_commit_visibility_time_proven": False, "enrolled_in_existing_study": False,
        "modifies_source_event_or_features": False, "capture_status": "PARTIAL" if package["errors"] else "CAPTURED",
        "package_version": graph.VERSION, "hash_version": graph.HASH_VERSION, "events": package["events"],
        "errors": package["errors"], "object_sha256s": sorted(package["objects"])}
    result = {"observation_id": manifest["observation_id"], "root_sha256": graph.digest(manifest),
              "manifest": manifest, "objects": package["objects"]}
    package_from_observation(result)
    return result


def package_from_observation(observation):
    m = observation["manifest"]
    if (m.get("version") != VERSION or m.get("contract") != CONTRACT or m.get("contract_sha256") != CONTRACT_SHA256 or
        graph.digest(m) != observation.get("root_sha256") or m.get("observation_id") != observation.get("observation_id") or
        sorted(observation["objects"]) != m.get("object_sha256s")):
        raise ValueError("SHADOW_MANIFEST_MISMATCH")
    if (utc(m["source_alert_time_utc"]) < utc(m["process_started_at_utc"]) or
        utc(m["producer_captured_at_utc"]) < utc(m["process_started_at_utc"])):
        raise ValueError("SHADOW_EPOCH_INVALID")
    for key in ("original_first_match_input_proven", "original_event_insert_proven", "source_commit_visibility_time_proven",
                "enrolled_in_existing_study", "modifies_source_event_or_features"):
        if m.get(key) is not False:
            raise ValueError("SHADOW_PROOF_SCOPE_INVALID")
    if m.get("source_writer_queue_accepted") is not True:
        raise ValueError("SHADOW_SOURCE_NOT_QUEUED")
    package = {"version": m["package_version"], "hash_version": m["hash_version"],
               "objects": observation["objects"], "events": m["events"], "errors": m["errors"]}
    try:
        graph.validate(package)
    except Exception as exc:
        raise ValueError("SHADOW_GRAPH_INVALID") from exc
    if m["capture_status"] != ("PARTIAL" if package["errors"] else "CAPTURED"):
        raise ValueError("SHADOW_CAPTURE_STATUS_MISMATCH")
    if not package["errors"]:
        payloads = graph.event_payloads(package)
        kinds = [header["kind"] for header, _ in payloads]
        if sorted(kinds) != ["derived_context", "identity", "producer_input"]:
            raise ValueError("SHADOW_INPUT_CLOSURE_INVALID")
        values = {header["kind"]: value for header, value in payloads}
        source, derived = values["producer_input"], values["derived_context"]
        if set(source["event"]) != EVENT_FIELDS or set(source["serialized_event"]) != set(SOURCE_FIELDS):
            raise ValueError("SHADOW_EVENT_FIELDS_INCOMPLETE")
        identity = values["identity"]
        hashes = identity.get("code_versions", {})
        if (set(hashes) != set(CODE_FILES) or not all(isinstance(value, str) and len(value) == 64 and
                all(char in "0123456789abcdef" for char in value) for value in hashes.values()) or
                not isinstance(identity.get("runtime"), dict) or not identity["runtime"].get("python_version")):
            raise ValueError("SHADOW_IDENTITY_INVALID")
        if (source["event"]["event_fingerprint"] != m["event_fingerprint"] or
            utc(source["event"]["alert_time_utc"]).isoformat() != m["source_alert_time_utc"] or
            graph.digest(source["source_material"]) != m["source_material_sha256"] or
            normalize_source_material(source["serialized_event"]) != source["source_material"] or
            derive_context(source["event"], source["selected_top_item"]) != derived):
            raise ValueError("SHADOW_SEMANTIC_BINDING_INVALID")
        for name, sha_name in (("event", "original_event_canonical_sha256"),
                               ("selected_top_item", "selected_top_item_canonical_sha256"),
                               ("serialized_event", "serialized_event_canonical_sha256")):
            encoded = source["legacy_" + name + "_canonical"]
            _validate_original(source[name], encoded, source["legacy_" + name + "_datetime_paths"])
            if hashlib.sha256(encoded.encode("utf-8")).hexdigest() != m[sha_name]:
                raise ValueError("SHADOW_ORIGINAL_DIGEST_INVALID")
        event_material = normalize_source_material(source["event"])
        if any(event_material[key] != source["source_material"][key] for key in source["event"] if key in SOURCE_FIELDS):
            raise ValueError("SHADOW_EVENT_SOURCE_BINDING_INVALID")
    return package


def _object(sha, codec, raw_bytes, payload):
    value = {"codec": codec, "raw_bytes": raw_bytes, "compressed_b64": base64.b64encode(bytes(payload)).decode("ascii")}
    try:
        graph._decode(sha, {sha: value})
    except Exception as exc:
        raise ValueError("SHADOW_STORED_OBJECT_INVALID") from exc
    return value


def link_receipt(observation, source_row=None, *, reason="SOURCE_PENDING"):
    expected = observation["manifest"]["source_material_sha256"]
    material = normalize_source_material(source_row) if source_row is not None else None
    actual = graph.digest(material) if material is not None else None
    event_id = source_row.get("event_id") if source_row is not None else None
    valid_id = type(event_id) is int and event_id > 0
    matched = actual == expected and valid_id
    encoded_row = canonical_original(source_row) if source_row is not None else None
    result = {"link_receipt_id": str(uuid4()), "observation_id": observation["observation_id"],
        "observation_root_sha256": observation["root_sha256"],
        "link_status": "SOURCE_LINKED" if matched else "SOURCE_ID_INVALID" if source_row is not None and not valid_id else "SOURCE_BODY_MISMATCH" if source_row is not None else reason,
        "event_id": event_id if matched else None,
        "conflicting_event_id": event_id if source_row is not None and not matched else None,
        "source_material_sha256": expected, "observed_source_material_sha256": actual,
        "observed_source_material": material, "observed_source_row_canonical": encoded_row,
        "observed_source_row_canonical_sha256": hashlib.sha256(encoded_row.encode("utf-8")).hexdigest() if encoded_row is not None else None,
        "observed_at_utc": utc_now(), "normalization_scope": CONTRACT["source_link"],
        "original_event_insert_proven": False, "source_commit_visibility_time_proven": False}
    validate_link_receipt(result)
    return result


def validate_link_receipt(receipt):
    if len(graph.canonical(receipt).encode("utf-8")) > MAX_LINK_BYTES:
        raise ValueError("SHADOW_LINK_SIZE_BOUND")
    utc(receipt["observed_at_utc"])
    if receipt.get("original_event_insert_proven") is not False or receipt.get("source_commit_visibility_time_proven") is not False:
        raise ValueError("SHADOW_LINK_PROOF_SCOPE_INVALID")
    state = receipt.get("link_status")
    if state not in {"SOURCE_LINKED", "SOURCE_PENDING", "SOURCE_BODY_MISMATCH", "SOURCE_ID_INVALID", "SOURCE_UNAVAILABLE"}:
        raise ValueError("SHADOW_LINK_STATUS_INVALID")
    if receipt.get("normalization_scope") != CONTRACT["source_link"]:
        raise ValueError("SHADOW_LINK_NORMALIZATION_INVALID")
    material = receipt.get("observed_source_material")
    if material is not None:
        encoded = receipt["observed_source_row_canonical"]
        row = json.loads(encoded)
        if (hashlib.sha256(encoded.encode("utf-8")).hexdigest() != receipt["observed_source_row_canonical_sha256"] or
            normalize_source_material(row) != material or graph.digest(material) != receipt["observed_source_material_sha256"]):
            raise ValueError("SHADOW_LINK_MATERIAL_INVALID")
    if state == "SOURCE_LINKED":
        if (type(receipt.get("event_id")) is not int or receipt["event_id"] <= 0 or material is None or
            row.get("event_id") != receipt["event_id"] or
            receipt["source_material_sha256"] != receipt["observed_source_material_sha256"]):
            raise ValueError("SHADOW_LINK_MATCH_INVALID")
    elif receipt.get("event_id") is not None:
        raise ValueError("SHADOW_LINK_UNPROVEN_ID")
    if state in {"SOURCE_PENDING", "SOURCE_UNAVAILABLE"} and any(receipt.get(key) is not None for key in
            ("observed_source_material", "observed_source_material_sha256", "observed_source_row_canonical", "observed_source_row_canonical_sha256", "conflicting_event_id")):
        raise ValueError("SHADOW_LINK_PENDING_MATERIAL_INVALID")
    if state == "SOURCE_BODY_MISMATCH" and (material is None or receipt["source_material_sha256"] == receipt["observed_source_material_sha256"]):
        raise ValueError("SHADOW_LINK_MISMATCH_INVALID")


class SQLiteStore:
    """Local exact-codec evidence tests; production always uses PostgreSQL."""
    def __init__(self, path):
        self.path = str(path)
        with closing(sqlite3.connect(self.path)) as c, c:
            c.execute("CREATE TABLE IF NOT EXISTS research_combined_shadow_contracts(version TEXT PRIMARY KEY,contract_sha256 TEXT NOT NULL,contract_json TEXT NOT NULL,first_reported_runtime_start_utc TEXT NOT NULL)")
            c.execute("CREATE TABLE IF NOT EXISTS research_combined_shadow_objects(sha256 TEXT PRIMARY KEY,codec TEXT NOT NULL,raw_bytes INTEGER NOT NULL,payload BLOB NOT NULL)")
            c.execute("CREATE TABLE IF NOT EXISTS research_combined_shadow_observations(observation_id TEXT PRIMARY KEY,root_sha256 TEXT NOT NULL,manifest_json TEXT NOT NULL)")
            c.execute("CREATE TABLE IF NOT EXISTS research_combined_shadow_links(link_receipt_id TEXT PRIMARY KEY,observation_id TEXT NOT NULL,receipt_json TEXT NOT NULL)")

    def persist(self, obs):
        package_from_observation(obs)
        with closing(sqlite3.connect(self.path)) as c, c:
            m = obs["manifest"]
            c.execute("INSERT OR IGNORE INTO research_combined_shadow_contracts VALUES(?,?,?,?)", (VERSION, CONTRACT_SHA256, graph.canonical(CONTRACT), m["process_started_at_utc"]))
            row = c.execute("SELECT contract_sha256,contract_json FROM research_combined_shadow_contracts WHERE version=?", (VERSION,)).fetchone()
            if row != (CONTRACT_SHA256, graph.canonical(CONTRACT)):
                raise ValueError("SHADOW_CONTRACT_CONFLICT")
            for sha, obj in obs["objects"].items():
                c.execute("INSERT OR IGNORE INTO research_combined_shadow_objects VALUES(?,?,?,?)", (sha, obj["codec"], obj["raw_bytes"], base64.b64decode(obj["compressed_b64"])))
                saved = c.execute("SELECT codec,raw_bytes,payload FROM research_combined_shadow_objects WHERE sha256=?", (sha,)).fetchone()
                _object(sha, *saved)
            text = graph.canonical(m)
            c.execute("INSERT OR IGNORE INTO research_combined_shadow_observations VALUES(?,?,?)", (obs["observation_id"], obs["root_sha256"], text))
            if c.execute("SELECT root_sha256,manifest_json FROM research_combined_shadow_observations WHERE observation_id=?", (obs["observation_id"],)).fetchone() != (obs["root_sha256"], text):
                raise ValueError("SHADOW_OBSERVATION_CONFLICT")
        return {"persisted": True, "root_sha256": obs["root_sha256"]}

    def read(self, observation_id):
        with closing(sqlite3.connect(self.path)) as c, c:
            row = c.execute("SELECT root_sha256,manifest_json FROM research_combined_shadow_observations WHERE observation_id=?", (observation_id,)).fetchone()
            if row is None:
                return None
            m = json.loads(row[1])
            objects = {sha: _object(sha, *c.execute("SELECT codec,raw_bytes,payload FROM research_combined_shadow_objects WHERE sha256=?", (sha,)).fetchone()) for sha in m["object_sha256s"]}
        result = {"observation_id": observation_id, "root_sha256": row[0], "manifest": m, "objects": objects}
        package_from_observation(result)
        return result

    def persist_link(self, receipt):
        validate_link_receipt(receipt)
        with closing(sqlite3.connect(self.path)) as c, c:
            row = c.execute("SELECT root_sha256,manifest_json FROM research_combined_shadow_observations WHERE observation_id=?", (receipt["observation_id"],)).fetchone()
            if row is None or row[0] != receipt["observation_root_sha256"] or json.loads(row[1])["source_material_sha256"] != receipt["source_material_sha256"]:
                raise ValueError("SHADOW_LINK_OBSERVATION_MISMATCH")
            text = graph.canonical(receipt)
            c.execute("INSERT OR IGNORE INTO research_combined_shadow_links VALUES(?,?,?)", (receipt["link_receipt_id"], receipt["observation_id"], text))
            if c.execute("SELECT receipt_json FROM research_combined_shadow_links WHERE link_receipt_id=?", (receipt["link_receipt_id"],)).fetchone()[0] != text:
                raise ValueError("SHADOW_LINK_CONFLICT")


class PostgresStore:
    def __init__(self, database_url):
        self.database_url, self.initialized = database_url, False

    def _connect(self):
        import psycopg
        from psycopg.rows import dict_row
        return psycopg.connect(self.database_url, connect_timeout=5, row_factory=dict_row)

    def persist(self, obs):
        package_from_observation(obs)
        m = obs["manifest"]
        with self._connect() as c:
            c.execute("SET LOCAL statement_timeout=10000")
            c.execute("SET LOCAL lock_timeout=1000")
            if not self.initialized:
                c.execute("SELECT pg_advisory_xact_lock(%s)", (73182461390741533,))
                c.execute("CREATE TABLE IF NOT EXISTS research_combined_shadow_contracts(version TEXT PRIMARY KEY,contract_sha256 TEXT NOT NULL,contract_json TEXT NOT NULL,first_reported_runtime_start_utc TIMESTAMPTZ NOT NULL,registered_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp())")
                c.execute("CREATE TABLE IF NOT EXISTS research_combined_shadow_objects(sha256 TEXT PRIMARY KEY,codec TEXT NOT NULL,raw_bytes INTEGER NOT NULL,payload BYTEA NOT NULL,created_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp())")
                c.execute("CREATE TABLE IF NOT EXISTS research_combined_shadow_observations(observation_id TEXT PRIMARY KEY,root_sha256 TEXT NOT NULL,manifest_json TEXT NOT NULL,created_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp())")
                c.execute("CREATE TABLE IF NOT EXISTS research_combined_shadow_links(link_receipt_id TEXT PRIMARY KEY,observation_id TEXT NOT NULL,receipt_json TEXT NOT NULL,created_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp())")
            c.execute("INSERT INTO research_combined_shadow_contracts(version,contract_sha256,contract_json,first_reported_runtime_start_utc) VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING", (VERSION, CONTRACT_SHA256, graph.canonical(CONTRACT), m["process_started_at_utc"]))
            registered = c.execute("SELECT contract_sha256,contract_json FROM research_combined_shadow_contracts WHERE version=%s", (VERSION,)).fetchone()
            if (registered["contract_sha256"], registered["contract_json"]) != (CONTRACT_SHA256, graph.canonical(CONTRACT)):
                raise ValueError("SHADOW_CONTRACT_CONFLICT")
            with c.cursor() as cursor:
                cursor.executemany("INSERT INTO research_combined_shadow_objects(sha256,codec,raw_bytes,payload) VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING", [(sha, obj["codec"], obj["raw_bytes"], base64.b64decode(obj["compressed_b64"])) for sha, obj in obs["objects"].items()])
            retained = c.execute("SELECT sha256,codec,raw_bytes,payload FROM research_combined_shadow_objects WHERE sha256=ANY(%s)", (list(obs["objects"]),)).fetchall()
            if len(retained) != len(obs["objects"]):
                raise ValueError("SHADOW_OBJECT_MISSING")
            for row in retained:
                _object(row["sha256"], row["codec"], row["raw_bytes"], row["payload"])
            text = graph.canonical(m)
            c.execute("INSERT INTO research_combined_shadow_observations(observation_id,root_sha256,manifest_json) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING", (obs["observation_id"], obs["root_sha256"], text))
            old = c.execute("SELECT root_sha256,manifest_json FROM research_combined_shadow_observations WHERE observation_id=%s", (obs["observation_id"],)).fetchone()
            if (old["root_sha256"], old["manifest_json"]) != (obs["root_sha256"], text):
                raise ValueError("SHADOW_OBSERVATION_CONFLICT")
        self.initialized = True
        return {"persisted": True, "root_sha256": obs["root_sha256"]}

    def read(self, observation_id):
        with self._connect() as c:
            c.execute("SET TRANSACTION READ ONLY")
            c.execute("SET LOCAL statement_timeout=10000")
            row = c.execute("SELECT root_sha256,manifest_json FROM research_combined_shadow_observations WHERE observation_id=%s", (observation_id,)).fetchone()
            if row is None:
                return None
            m = json.loads(row["manifest_json"])
            retained = c.execute("SELECT sha256,codec,raw_bytes,payload FROM research_combined_shadow_objects WHERE sha256=ANY(%s)", (m["object_sha256s"],)).fetchall()
        result = {"observation_id": observation_id, "root_sha256": row["root_sha256"], "manifest": m,
                  "objects": {r["sha256"]: _object(r["sha256"], r["codec"], r["raw_bytes"], r["payload"]) for r in retained}}
        package_from_observation(result)
        return result

    def find_source(self, fingerprint):
        with self._connect() as c:
            c.execute("SET TRANSACTION READ ONLY")
            c.execute("SET LOCAL statement_timeout=3000")
            return c.execute("SELECT event_id," + ",".join(SOURCE_FIELDS) + " FROM research_events WHERE event_fingerprint=%s", (fingerprint,)).fetchone()

    def persist_link(self, receipt):
        validate_link_receipt(receipt)
        with self._connect() as c:
            c.execute("SET LOCAL statement_timeout=10000")
            c.execute("SET LOCAL lock_timeout=1000")
            observation = c.execute("SELECT root_sha256,manifest_json FROM research_combined_shadow_observations WHERE observation_id=%s", (receipt["observation_id"],)).fetchone()
            if observation is None or observation["root_sha256"] != receipt["observation_root_sha256"] or json.loads(observation["manifest_json"])["source_material_sha256"] != receipt["source_material_sha256"]:
                raise ValueError("SHADOW_LINK_OBSERVATION_MISMATCH")
            text = graph.canonical(receipt)
            c.execute("INSERT INTO research_combined_shadow_links(link_receipt_id,observation_id,receipt_json) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING", (receipt["link_receipt_id"], receipt["observation_id"], text))
            row = c.execute("SELECT receipt_json FROM research_combined_shadow_links WHERE link_receipt_id=%s", (receipt["link_receipt_id"],)).fetchone()
            if row["receipt_json"] != text:
                raise ValueError("SHADOW_LINK_CONFLICT")


def _failure(reason, observation_id=None, dropped=False):
    with _LOCK:
        _METRICS["dropped" if dropped else "failed"] += 1
        _METRICS["last"] = {"observation_id": observation_id, "status": "DROPPED" if dropped else "FAILED", "reason_code": reason, "at_utc": utc_now()}


def _persist_one(obs, size):
    try:
        result = _STORE.persist(obs)
        with _LOCK:
            _METRICS["persisted"] += 1
            _METRICS["partial"] += int(obs["manifest"]["capture_status"] == "PARTIAL")
            _METRICS["last"] = {"observation_id": obs["observation_id"], "status": "PERSISTED", "capture_status": obs["manifest"]["capture_status"], "at_utc": utc_now(), **result}
        receipt = None
        try:
            for attempt in range(LINK_ATTEMPTS):
                row = _STORE.find_source(obs["manifest"]["event_fingerprint"])
                if row is not None:
                    receipt = link_receipt(obs, row)
                    break
                if attempt + 1 < LINK_ATTEMPTS:
                    time.sleep(LINK_WAIT_SECONDS)
            receipt = receipt or link_receipt(obs)
        except Exception:
            receipt = link_receipt(obs, reason="SOURCE_UNAVAILABLE")
        try:
            _STORE.persist_link(receipt)
            with _LOCK:
                _METRICS["linked" if receipt["link_status"] == "SOURCE_LINKED" else "unlinked"] += 1
                _METRICS["last_link"] = {"observation_id": obs["observation_id"], "source_link_status": receipt["link_status"],
                                          "event_id": receipt["event_id"], "at_utc": utc_now()}
                if (_METRICS.get("last") or {}).get("observation_id") == obs["observation_id"]:
                    _METRICS["last"].update(source_link_status=receipt["link_status"], event_id=receipt["event_id"])
        except Exception as exc:
            with _LOCK:
                _METRICS["link_failures"] += 1
                _METRICS["last_link"] = {"observation_id": obs["observation_id"], "source_link_status": "LINK_WRITE_FAILED",
                                          "reason_code": type(exc).__name__, "at_utc": utc_now()}
    except Exception as exc:
        _failure(str(exc) if isinstance(exc, ValueError) else type(exc).__name__, obs["observation_id"])
    finally:
        with _LOCK:
            _METRICS["queued_items"] -= 1
            _METRICS["queued_bytes"] -= size


def _run():
    while True:
        obs, size = _QUEUE.get()
        _persist_one(obs, size)
        _QUEUE.task_done()


def _ensure_worker():
    global _STORE, _WORKER
    with _WORKER_LOCK:
        if _WORKER is not None and _WORKER.is_alive():
            return True
        if _STORE is None:
            url = database_url()
            if not url:
                return False
            _STORE = PostgresStore(url)
        _WORKER = threading.Thread(target=_run, name="combined-liquidity-shadow", daemon=True)
        _WORKER.start()
        return True


def submit_producer_capture(event, selected_top_item, **metadata):
    """Fail-open producer API. QUEUED never claims database durability."""
    obs = None
    try:
        obs = prepare_capture(event, selected_top_item, **metadata)
        if obs is None:
            return {"version": VERSION, "status": "SKIPPED"}
        size = sum(len(value["compressed_b64"]) + 256 for value in obs["objects"].values()) + len(graph.canonical(obs["manifest"]).encode("utf-8"))
        if not _ensure_worker():
            _failure("DATABASE_NOT_CONFIGURED", obs["observation_id"])
            return {"version": VERSION, "status": "UNAVAILABLE"}
        with _LOCK:
            if _METRICS["queued_items"] >= MAX_QUEUE_ITEMS or _METRICS["queued_bytes"] + size > MAX_QUEUE_BYTES:
                _METRICS["dropped"] += 1
                _METRICS["last"] = {"observation_id": obs["observation_id"], "status": "DROPPED", "reason_code": "QUEUE_BOUND", "at_utc": utc_now()}
                return {"version": VERSION, "status": "DROPPED"}
            _METRICS["queued_items"] += 1
            _METRICS["queued_bytes"] += size
            try:
                _QUEUE.put_nowait((obs, size))
            except Exception:
                _METRICS["queued_items"] -= 1
                _METRICS["queued_bytes"] -= size
                raise
            _METRICS["submitted"] += 1
        return {"version": VERSION, "status": "QUEUED", "observation_id": obs["observation_id"]}
    except Exception as exc:
        _failure(str(exc) if isinstance(exc, ValueError) else type(exc).__name__, obs["observation_id"] if obs else None)
        return {"version": VERSION, "status": "FAILED"}


def status():
    with _LOCK:
        metrics = deepcopy(_METRICS)
    return {"version": VERSION, "contract_sha256": CONTRACT_SHA256, "scope": CONTRACT["scope"],
        "process_started_at_utc": PROCESS_STARTED_AT_UTC, "configured": bool(database_url()),
        "worker_alive": bool(_WORKER and _WORKER.is_alive()), "enrolled_in_existing_study": False,
        "notifications": False, "trade_execution": False,
        "epoch_scope": "current process source-clock gate; registry first reported process start is not independently proven earliest activation",
        "queue_loss_on_abrupt_termination_possible": True,
        "limits": {"queue_items": MAX_QUEUE_ITEMS, "queue_bytes": MAX_QUEUE_BYTES, "package_raw_bytes": MAX_RAW_BYTES, "package_compressed_bytes": MAX_COMPRESSED_BYTES}, **metrics}
