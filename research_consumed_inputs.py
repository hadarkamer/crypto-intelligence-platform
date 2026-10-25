"""Bounded observation of consumed calculation inputs, separate from decisions.

Objects bind normalized values actually observed by instrumented producers.
They do not attest to original provider HTTP bodies or earlier database views.
Capture/storage failures produce gaps and never suppress operational results.
"""
from __future__ import annotations

import base64
from contextlib import closing, contextmanager
from contextvars import ContextVar
from copy import deepcopy
from datetime import datetime, timezone
from functools import lru_cache
import hashlib
import json
import math
import os
from pathlib import Path
import queue
import sqlite3
import threading
import zlib

VERSION = "consumed-sql-inputs-v1"
CODEC = "zlib-json-v1"
HASH_VERSION = "json-integer-float-zero-normalized-v1"
EXPECTED_SYMBOLS = ("DOGE", "ETH")
CODE_FILES = ("research_consumed_inputs.py", "main.py", "alert_engine.py",
              "time_family_engine.py", "market_session_baseline.py",
              "coinglass_flow_foundation.py", "coinglass_history_backfill.py",
              "coinglass_oi_regime_service.py", "coinglass_flow_engine.py",
              "market_confidence_engine.py", "live_price_provider.py",
              "research_watch_score_capture.py")
MAX_OBJECT_BYTES = 512 * 1024
MAX_RAW_BYTES = 128 * 1024 * 1024
MAX_COMPRESSED_BYTES = 32 * 1024 * 1024
MAX_OBJECTS = 8192
MAX_EVENTS = 4096
MAX_QUEUE_BYTES = 32 * 1024 * 1024
MAX_QUEUE_ITEMS = 2
_CURRENT = ContextVar("research_consumed_inputs", default=None)
_METRICS_LOCK = threading.Lock()
_METRICS = dict(submitted=0, persisted=0, failed=0, dropped=0, partial=0,
                queued_bytes=0, queued_items=0, last=None)
_QUEUE = queue.Queue(maxsize=MAX_QUEUE_ITEMS)
_WORKER = None
_WORKER_LOCK = threading.Lock()
_STORE = None


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def normalized(value):
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("NONFINITE_INPUT")
        return int(value) if value.is_integer() else value
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("NAIVE_DATETIME")
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, dict):
        if not all(isinstance(k, str) for k in value):
            raise ValueError("NONSTRING_KEY")
        return {k: normalized(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalized(v) for v in value]
    raise ValueError("UNSUPPORTED_INPUT_TYPE:" + type(value).__name__)


def canonical(value):
    return _dumps(normalized(value))


def _dumps(value):
    """Private serializer for values already normalized by this module."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def operational_hash_material(value):
    """Preserve the existing Watch hash contract, including datetime str().

    The original scorer uses JSON default=str rather than this module's UTC
    datetime normalization. Retain its exact JSON value and a typed bridge so
    independent readers can bind it to the normalized consumed input values.
    Ordinary timestamp strings are never rewritten by this bridge.
    """
    datetime_paths = []
    def numeric(item, path):
        if isinstance(item, datetime):
            datetime_paths.append({"path": path, "source_text": str(item),
                                   "normalized_text": normalized(item)})
            return item
        if isinstance(item, float) and item.is_integer():
            return int(item)
        if isinstance(item, dict):
            return {key: numeric(child, path + [key]) for key, child in item.items()}
        if isinstance(item, (list, tuple)):
            return [numeric(child, path + [index]) for index, child in enumerate(item)]
        return item
    encoded = json.dumps(numeric(value, []), sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False, allow_nan=False, default=str)
    return {"value": json.loads(encoded), "datetime_paths": datetime_paths}


def watch_binding(cycle_id, rows, snapshot, score_block, expected_symbols, material):
    failures = []
    def require(ok, code, symbol=None):
        if not ok:
            item = {"code": code}
            if symbol is not None:
                item["symbol"] = symbol
            failures.append(item)
    score = score_block if isinstance(score_block, dict) else {}
    require(bool(score), "WATCH_SCORE_MISSING")
    require(score.get("version") == "watch-operational-scores-v2", "WATCH_SCORE_VERSION")
    require(score.get("status") == "COMPLETE", "WATCH_SCORE_NOT_COMPLETE")
    require(score.get("cycle_id") == str(cycle_id), "WATCH_SCORE_CYCLE_MISMATCH")
    body = {key: value for key, value in score.items() if key != "payload_sha256"}
    require(score.get("payload_sha256") == digest(operational_hash_material(body)["value"]),
            "WATCH_SCORE_PAYLOAD_SHA256")
    universe = material["value"]["input_universe"]
    require(isinstance(rows, list) and score.get("input_row_count") == len(rows),
            "WATCH_SCORE_INPUT_ROW_COUNT")
    require(score.get("input_universe_sha256") == digest(universe), "WATCH_SCORE_INPUT_UNIVERSE")
    coins = score.get("coins") or {}
    for symbol in expected_symbols:
        captured = snapshot.get(symbol) if isinstance(snapshot, dict) else None
        coin = coins.get(symbol) or {}
        require(isinstance(captured, dict) and bool(captured), "WATCH_SNAPSHOT_MISSING", symbol)
        require(coin.get("status") == "CAPTURED", "WATCH_SCORE_COIN_NOT_CAPTURED", symbol)
        require(bool(captured) and (coin.get("sources") or {}).get("derivatives_snapshot_sha256") ==
                digest(material["value"]["snapshot_by_symbol"][symbol]),
                "WATCH_DERIVATIVE_SNAPSHOT_BINDING", symbol)
    return {"complete": not failures, "failures": failures}


def package_bytes(package):
    return sum(len(v.get("compressed_b64", "")) * 3 // 4
               for v in (package.get("objects") or {}).values())


def _package_fingerprint(package):
    """Bind both identity metadata and bytes before reusing merge validation."""
    hasher = hashlib.sha256()
    hasher.update(canonical({key: package.get(key) for key in ("version", "hash_version", "events", "errors")}).encode())
    for sha, item in sorted(package.get("objects", {}).items()):
        hasher.update(canonical([sha, item.get("codec"), item.get("raw_bytes")]).encode())
        hasher.update(item["compressed_b64"].encode("ascii"))
    return hasher.hexdigest()


class _CapturedPackage(dict):
    """Process-local validation seal. JSON exports intentionally lose the seal."""
    pass


@lru_cache(maxsize=1)
def code_versions():
    root = Path(__file__).parent
    return {name: hashlib.sha256((root / name).read_bytes()).hexdigest()
            if (root / name).is_file() else None for name in CODE_FILES}


def _day(row):
    if not isinstance(row, dict):
        return None
    for key in ("time", "candle_time", "collected_at"):
        value = row.get(key)
        if value is None:
            continue
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                return None
            return parsed.astimezone(timezone.utc).date().isoformat()
        except (TypeError, ValueError):
            return None
    return None


def _chunks(values):
    """Stable UTC-day groups for histories, positional chunks for derived arrays."""
    days = [_day(v) for v in values]
    if days and all(days) and days == sorted(days):
        start = 0
        for i in range(1, len(values) + 1):
            if i == len(values) or days[i] != days[start]:
                yield values[start:i]
                start = i
    else:
        for start in range(0, len(values), 256):
            yield values[start:start + 256]


class Collector:
    def __init__(self, *, max_compressed_bytes=MAX_COMPRESSED_BYTES,
                 max_raw_bytes=MAX_RAW_BYTES, max_objects=MAX_OBJECTS):
        self.objects = {}
        self.events = []
        self.errors = []
        self.raw_bytes = 0
        self.compressed_bytes = 0
        self.max_compressed_bytes = max_compressed_bytes
        self.max_raw_bytes = max_raw_bytes
        self.max_objects = max_objects
        self._merged_fingerprints = set()

    def gap(self, code):
        marker = {"code": str(code).split(":")[0][:80]}
        if marker not in self.errors and len(self.errors) < 32:
            self.errors.append(marker)

    def _put(self, node, raw=None):
        raw = raw if raw is not None else _dumps(node).encode("utf-8")
        if len(raw) > MAX_OBJECT_BYTES:
            raise ValueError("OBJECT_SIZE_BOUND")
        sha = hashlib.sha256(raw).hexdigest()
        if sha in self.objects:
            return sha
        packed = zlib.compress(raw, 3)
        if (len(self.objects) + 1 > self.max_objects or
                self.raw_bytes + len(raw) > self.max_raw_bytes or
                self.compressed_bytes + len(packed) > self.max_compressed_bytes):
            raise ValueError("PACKAGE_SIZE_BOUND")
        self.objects[sha] = {"codec": CODEC,
                             "compressed_b64": base64.b64encode(packed).decode("ascii"),
                             "raw_bytes": len(raw)}
        self.raw_bytes += len(raw)
        self.compressed_bytes += len(packed)
        return sha

    def _node(self, value):
        raw = _dumps({"node": "value", "value": value}).encode("utf-8")
        if len(raw) <= MAX_OBJECT_BYTES:
            return self._put({"node": "value", "value": value}, raw)
        if isinstance(value, dict):
            return self._put({"node": "dict", "items": [[k, self._node(v)]
                             for k, v in sorted(value.items())]})
        if isinstance(value, list):
            # Every child represents a list segment. Recovery concatenates it.
            chunks = list(_chunks(value))
            if len(chunks) == 1 and len(chunks[0]) == len(value):
                if len(value) < 2:
                    raise ValueError("OVERSIZED_SCALAR")
                middle = len(value) // 2
                chunks = [value[:middle], value[middle:]]
            return self._put({"node": "list", "items": [self._node(v) for v in chunks]})
        raise ValueError("OVERSIZED_SCALAR")

    def record(self, kind, payload):
        try:
            if len(self.events) >= MAX_EVENTS:
                raise ValueError("EVENT_COUNT_BOUND")
            value = normalized(payload)
            ref = self._node(value)
            event = {"kind": str(kind)[:120], "object_sha256": ref,
                     "recorded_at_utc": utc_now()}
            if isinstance(value, dict) and isinstance(value.get("arguments"), dict):
                event["arguments"] = deepcopy(value["arguments"])
            self.events.append(event)
            if str(kind) == "capture_gap":
                self.gap("PRODUCER_CAPTURE_GAP")
            return ref
        except Exception as exc:
            self.gap(str(exc) if isinstance(exc, ValueError) else type(exc).__name__)
            return None

    def merge(self, package, metadata=None):
        try:
            fingerprint = _package_fingerprint(package)
            if fingerprint in self._merged_fingerprints:
                if metadata is not None:
                    self.record("generation_use", metadata)
                return True
            if not (isinstance(package, _CapturedPackage) and
                    getattr(package, "_seal_fingerprint", None) == fingerprint):
                validate(package)
            added = {k: v for k, v in package["objects"].items() if k not in self.objects}
            raw = sum(v["raw_bytes"] for v in added.values())
            packed = package_bytes({"objects": added})
            if (self.raw_bytes + raw > self.max_raw_bytes or
                    self.compressed_bytes + packed > self.max_compressed_bytes or
                    len(self.objects) + len(added) > self.max_objects or
                    len(self.events) + len(package["events"]) > MAX_EVENTS):
                raise ValueError("MERGE_SIZE_BOUND")
            # Clone metadata containers; compressed strings are immutable.
            self.objects.update(deepcopy(added))
            self.raw_bytes += raw
            self.compressed_bytes += packed
            self.events.extend(deepcopy(package["events"]))
            self._merged_fingerprints.add(fingerprint)
            for error in package["errors"]:
                self.gap(error.get("code", "MERGED_CAPTURE_GAP"))
            if metadata is not None:
                self.record("generation_use", metadata)
            return True
        except Exception as exc:
            self.gap(str(exc) if isinstance(exc, ValueError) else type(exc).__name__)
            return False

    def package(self):
        package = _CapturedPackage(version=VERSION, hash_version=HASH_VERSION,
                                   objects=deepcopy(self.objects), events=deepcopy(self.events),
                                   errors=deepcopy(self.errors))
        package._seal_fingerprint = _package_fingerprint(package)
        return package

    def _fork(self):
        other = Collector(max_compressed_bytes=self.max_compressed_bytes,
                          max_raw_bytes=self.max_raw_bytes, max_objects=self.max_objects)
        other.objects = deepcopy(self.objects)
        other.events = deepcopy(self.events)
        other.errors = deepcopy(self.errors)
        other.raw_bytes, other.compressed_bytes = self.raw_bytes, self.compressed_bytes
        other._merged_fingerprints = set(self._merged_fingerprints)
        return other


@contextmanager
def collect(**kwargs):
    collector = Collector(**kwargs)
    token = _CURRENT.set(collector)
    try:
        yield collector
    finally:
        _CURRENT.reset(token)


@contextmanager
def safe_collect(**kwargs):
    """Isolate observer setup/cleanup failures, preserving body exceptions."""
    collector, token = None, None
    try:
        collector = Collector(**kwargs)
        token = _CURRENT.set(collector)
    except Exception:
        collector = None
    try:
        yield collector
    finally:
        if token is not None:
            try:
                _CURRENT.reset(token)
            except Exception:
                pass


def current():
    return _CURRENT.get()


def record(kind, payload):
    collector = current()
    return collector.record(kind, payload) if collector is not None else None


def merge(package, metadata=None):
    collector = current()
    return collector.merge(package, metadata) if collector is not None else False


def _decode(ref, objects):
    item = objects.get(ref)
    if not isinstance(item, dict) or item.get("codec") != CODEC:
        raise ValueError("MISSING_OR_INVALID_OBJECT")
    if not isinstance(item.get("raw_bytes"), int) or not 0 < item["raw_bytes"] <= MAX_OBJECT_BYTES:
        raise ValueError("OBJECT_SIZE_BOUND")
    packed = base64.b64decode(item["compressed_b64"], validate=True)
    inflater = zlib.decompressobj()
    raw = inflater.decompress(packed, MAX_OBJECT_BYTES + 1)
    if not inflater.eof or inflater.unused_data or len(raw) != item["raw_bytes"]:
        raise ValueError("COMPRESSED_OBJECT_BOUND_OR_LENGTH")
    if hashlib.sha256(raw).hexdigest() != ref:
        raise ValueError("OBJECT_DIGEST_MISMATCH")
    node = json.loads(raw)
    if canonical(node).encode("utf-8") != raw:
        raise ValueError("NONCANONICAL_OBJECT")
    return node


def _refs(node):
    if node.get("node") == "value" and set(node) == {"node", "value"}:
        return []
    if node.get("node") == "dict" and set(node) == {"node", "items"}:
        items = node["items"]
        if not isinstance(items, list) or any(not isinstance(p, list) or len(p) != 2 or
                not isinstance(p[0], str) for p in items):
            raise ValueError("INVALID_DICT_NODE")
        keys = [p[0] for p in items]
        if keys != sorted(set(keys)):
            raise ValueError("INVALID_DICT_KEYS")
        return [p[1] for p in items]
    if node.get("node") == "list" and set(node) == {"node", "items"} and isinstance(node["items"], list):
        return node["items"]
    raise ValueError("INVALID_NODE")


def validate(package):
    if not isinstance(package, dict) or package.get("version") != VERSION or package.get("hash_version") != HASH_VERSION:
        raise ValueError("INVALID_PACKAGE_VERSION")
    objects, events, errors = package.get("objects"), package.get("events"), package.get("errors")
    if not isinstance(objects, dict) or not isinstance(events, list) or not isinstance(errors, list):
        raise ValueError("INVALID_PACKAGE_SHAPE")
    if len(objects) > MAX_OBJECTS or len(events) > MAX_EVENTS or len(errors) > 32:
        raise ValueError("PACKAGE_COUNT_BOUND")
    if any(not isinstance(error, dict) or set(error) != {"code"} or
           not isinstance(error["code"], str) for error in errors):
        raise ValueError("INVALID_GAP_MARKER")
    if (any(isinstance(event, dict) and event.get("kind") == "capture_gap" for event in events) and
            not any(error["code"] == "PRODUCER_CAPTURE_GAP" for error in errors)):
        raise ValueError("UNDECLARED_PRODUCER_CAPTURE_GAP")
    if package_bytes(package) > MAX_COMPRESSED_BYTES:
        raise ValueError("PACKAGE_COMPRESSED_BOUND")
    edges = {}
    raw_size = 0
    for ref in objects:
        if not isinstance(ref, str) or len(ref) != 64 or any(c not in "0123456789abcdef" for c in ref):
            raise ValueError("INVALID_OBJECT_ID")
        node = _decode(ref, objects)
        edges[ref] = _refs(node)
        raw_size += objects[ref]["raw_bytes"]
    if raw_size > MAX_RAW_BYTES:
        raise ValueError("PACKAGE_RAW_BOUND")
    for ref, children in edges.items():
        if any(not isinstance(child, str) or child not in edges for child in children):
            raise ValueError("MISSING_REFERENCE")
    for event in events:
        if not isinstance(event, dict) or event.get("object_sha256") not in edges or not isinstance(event.get("kind"), str):
            raise ValueError("INVALID_EVENT_REFERENCE")
        parsed = datetime.fromisoformat(str(event.get("recorded_at_utc")).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("INVALID_EVENT_CLOCK")
        if "arguments" in event and event["arguments"] != _event_arguments_from_object(package, event):
            raise ValueError("EVENT_ARGUMENT_HEADER_MISMATCH")
    # Hash-addressed graphs cannot form honest cycles, but explicitly reject
    # cycles rather than depending on the cryptographic assumption in recovery.
    visiting, visited = set(), set()
    def visit(ref):
        if ref in visiting:
            raise ValueError("CYCLIC_GRAPH")
        if ref in visited:
            return
        visiting.add(ref)
        for child in edges[ref]:
            visit(child)
        visiting.remove(ref)
        visited.add(ref)
    for ref in edges:
        visit(ref)
    return {"objects": len(objects), "events": len(events), "raw_bytes": raw_size,
            "compressed_bytes": package_bytes(package), "status": "PARTIAL" if errors else "CAPTURED"}


def recover(package, ref):
    nodes, active = {}, set()
    def read(key):
        if key in active:
            raise ValueError("CYCLIC_GRAPH")
        active.add(key)
        node = nodes.setdefault(key, _decode(key, package["objects"]))
        kind = node["node"]
        _refs(node)
        if kind == "value":
            value = deepcopy(node["value"])
        elif kind == "dict":
            value = {name: read(child) for name, child in node["items"]}
        else:
            value = []
            for child in node["items"]:
                chunk = read(child)
                if not isinstance(chunk, list):
                    raise ValueError("NONLIST_SEGMENT")
                value.extend(chunk)
        active.remove(key)
        return value
    return read(ref)


def event_payloads(package, kind=None):
    return [(event, recover(package, event["object_sha256"])) for event in package["events"]
            if kind is None or event["kind"] == kind]


def event_arguments(package, event):
    """Read small call metadata without materializing a large result body."""
    if "arguments" in event:
        return deepcopy(event["arguments"])
    return _event_arguments_from_object(package, event)


def _event_arguments_from_object(package, event):
    node = _decode(event["object_sha256"], package["objects"])
    if node.get("node") == "value":
        value = node.get("value")
        return value.get("arguments", {}) if isinstance(value, dict) else {}
    if node.get("node") == "dict":
        refs = dict(node["items"])
        return recover(package, refs["arguments"]) if "arguments" in refs else {}
    return {}


def producer_coverage(package, expected_symbols=EXPECTED_SYMBOLS):
    present = set()
    windows, historical_points = {}, {}
    for event in package["events"]:
        args = event_arguments(package, event)
        symbol = str(args.get("symbol") or "").upper()
        market = str(args.get("market") or "").lower()
        present.add((event["kind"], symbol, market))
        if event["kind"] == "oi_reference_selection":
            windows.setdefault((event["kind"], symbol), set()).add(str(args.get("minutes")))
        if event["kind"] == "oi_window_reference":
            windows.setdefault((event["kind"], symbol), set()).add(str(args.get("label")))
        if event["kind"] == "oi_historical_nearest":
            historical_points.setdefault(symbol, set()).add(str(args.get("target_time")))
    expected = set()
    for symbol in expected_symbols:
        for market in ("futures", "spot"):
            expected.update((kind, symbol, market) for kind in ("flow_rows", "flow_analysis"))
        expected.update((kind, symbol, "") for kind in ("flow_cached_generation", "oi_live_history",
                        "oi_window_results", "oi_regime", "oi_baseline_rows", "oi_baseline_calculation"))
    missing = sorted(expected - present)
    missing_details = [{"kind": kind, "symbol": symbol, "market": market or None}
                       for kind, symbol, market in missing]
    for symbol in expected_symbols:
        for kind, values in (("oi_reference_selection", (30, 60, 240, 720, 1440, 2880, 4320, 10080)),
                             ("oi_window_reference", ("30m", "1h", "4h", "12h", "24h", "48h", "72h", "7d"))):
            absent = {str(value) for value in values} - windows.get((kind, symbol), set())
            missing_details.extend({"kind": kind, "symbol": symbol, "window": window} for window in sorted(absent))
        if len(historical_points.get(symbol, set()) - {"None"}) < 8:
            missing_details.append({"kind": "oi_historical_nearest", "symbol": symbol,
                                    "reason": "MISSING_EIGHT_WINDOW_CANDIDATE_READS"})
    return {"expected_symbols": list(expected_symbols), "complete": not missing_details,
            "missing": missing_details}


def prepare_cycle(cycle_id, package_or_collector, *, rows, snapshot, score_block,
                  code_versions=None, expected_symbols=EXPECTED_SYMBOLS, metadata=None):
    if isinstance(package_or_collector, Collector):
        # All additions to a live Collector were already generated locally or
        # validated at merge. Copy only immutable object references/metadata;
        # strict full graph validation remains in the asynchronous writer.
        collector = package_or_collector._fork()
    else:
        collector = Collector()
        if package_or_collector is None:
            collector.gap("CAPTURE_COLLECTION_UNAVAILABLE")
        else:
            collector.merge(package_or_collector)
    material = operational_hash_material({"input_universe": rows,
                  "snapshot_by_symbol": {symbol: snapshot.get(symbol) for symbol in expected_symbols}})
    binding = watch_binding(cycle_id, rows, snapshot, score_block, expected_symbols, material)
    if not binding["complete"]:
        collector.gap("WATCH_INPUT_BINDING_GAP")
    collector.record("watch_input_universe", material["value"]["input_universe"])
    collector.record("watch_normalized_input_universe", rows)
    collector.record("watch_consumed_snapshot", {s: snapshot.get(s) for s in expected_symbols})
    collector.record("watch_operational_hash_material", material)
    collector.record("watch_operational_scores", score_block)
    codes = code_versions if code_versions is not None else globals()["code_versions"]()
    collector.record("calculation_code_versions", codes)
    if (set(CODE_FILES) - set(codes) or any(not isinstance(value, str) or len(value) != 64 or
            any(c not in "0123456789abcdef" for c in value) for value in codes.values())):
        collector.gap("MISSING_CODE_IDENTITY")
    package = collector.package()
    coverage = producer_coverage(package, expected_symbols)
    if not coverage["complete"]:
        collector.gap("MISSING_PRODUCER_EVIDENCE")
        package = collector.package()
    summary = {"objects": len(collector.objects), "events": len(collector.events),
               "raw_bytes": collector.raw_bytes, "compressed_bytes": package_bytes(package),
               "status": "PARTIAL" if collector.errors else "CAPTURED"}
    manifest = {"version": VERSION, "hash_version": HASH_VERSION, "cycle_id": str(cycle_id),
                "expected_symbols": list(expected_symbols), "submitted_at_utc": utc_now(),
                "capture_scope": {"consumed_sql_inputs": True, "provider_http_payloads": False,
                                  "earlier_database_versions": False, "doge_sequence_sql_membership": False,
                                  "complete_negative_evaluations": False},
                "events": package["events"], "errors": package["errors"],
                "object_sha256s": sorted(package["objects"]), "summary": summary,
                "producer_coverage": coverage,
                "watch_binding": binding,
                "metadata": normalized(metadata or {})}
    manifest["runtime_git_commit"] = os.getenv("RENDER_GIT_COMMIT")
    return {"cycle_id": str(cycle_id), "root_sha256": digest(manifest),
            "manifest": manifest, "objects": package["objects"]}


def package_from_cycle(cycle):
    manifest = cycle["manifest"]
    if digest(manifest) != cycle["root_sha256"]:
        raise ValueError("CYCLE_DIGEST_MISMATCH")
    if manifest["cycle_id"] != cycle["cycle_id"] or sorted(cycle["objects"]) != manifest["object_sha256s"]:
        raise ValueError("CYCLE_ID_OR_MEMBERSHIP_MISMATCH")
    package = {"version": manifest["version"], "hash_version": manifest["hash_version"],
               "events": manifest["events"], "errors": manifest["errors"], "objects": cycle["objects"]}
    summary = validate(package)
    # Compressed length describes the writer's encoding. An already retained
    # equivalent object may use another zlib representation; logical identity
    # binds canonical uncompressed nodes, not a compressor implementation.
    if ({k: v for k, v in summary.items() if k != "compressed_bytes"} !=
            {k: v for k, v in manifest.get("summary", {}).items() if k != "compressed_bytes"}):
        raise ValueError("CYCLE_SUMMARY_MISMATCH")
    if producer_coverage(package, manifest["expected_symbols"]) != manifest.get("producer_coverage"):
        raise ValueError("CYCLE_PRODUCER_COVERAGE_MISMATCH")
    if summary["status"] == "CAPTURED" and not manifest["producer_coverage"]["complete"]:
        raise ValueError("CYCLE_CAPTURE_STATUS_MISMATCH")
    return package


build_cycle = prepare_cycle
recover_cycle = package_from_cycle


_SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS research_consumed_input_objects (
 sha256 TEXT PRIMARY KEY, codec TEXT NOT NULL, raw_bytes INTEGER NOT NULL,
 payload BLOB NOT NULL, created_at_utc TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS research_consumed_input_cycles (
 cycle_id TEXT PRIMARY KEY, root_sha256 TEXT NOT NULL,
 manifest_json TEXT NOT NULL, capture_status TEXT NOT NULL,
 created_at_utc TEXT NOT NULL);
"""


class SQLiteStore:
    """Standalone local validation store; runtime never selects this implicitly."""
    def __init__(self, path):
        self.path = str(path)
        with closing(sqlite3.connect(self.path)) as conn:
            conn.executescript(_SQLITE_SCHEMA)
            conn.commit()

    def persist(self, cycle):
        package_from_cycle(cycle)
        with closing(sqlite3.connect(self.path)) as conn, conn:
            for sha, item in cycle["objects"].items():
                packed = base64.b64decode(item["compressed_b64"])
                conn.execute("INSERT OR IGNORE INTO research_consumed_input_objects VALUES (?,?,?,?,?)",
                             (sha, item["codec"], item["raw_bytes"], packed, utc_now()))
                existing = conn.execute("SELECT codec,raw_bytes,payload FROM research_consumed_input_objects WHERE sha256=?", (sha,)).fetchone()
                if existing != (item["codec"], item["raw_bytes"], packed):
                    retained = {sha: {"codec": existing[0], "raw_bytes": existing[1],
                                     "compressed_b64": base64.b64encode(existing[2]).decode("ascii")}}
                    _decode(sha, retained)
            conn.execute("INSERT OR IGNORE INTO research_consumed_input_cycles VALUES (?,?,?,?,?)",
                         (cycle["cycle_id"], cycle["root_sha256"], canonical(cycle["manifest"]),
                          cycle["manifest"]["summary"]["status"], utc_now()))
            existing = conn.execute("SELECT root_sha256,manifest_json FROM research_consumed_input_cycles WHERE cycle_id=?", (cycle["cycle_id"],)).fetchone()
            if existing != (cycle["root_sha256"], canonical(cycle["manifest"])):
                raise ValueError("STORED_CYCLE_CONFLICT")
        return {"persisted": True, "root_sha256": cycle["root_sha256"]}

    def read_cycle(self, cycle_id):
        with closing(sqlite3.connect(self.path)) as conn:
            row = conn.execute("SELECT root_sha256,manifest_json FROM research_consumed_input_cycles WHERE cycle_id=?", (cycle_id,)).fetchone()
            if row is None:
                return None
            manifest = json.loads(row[1])
            objects = {}
            for sha in manifest["object_sha256s"]:
                item = conn.execute("SELECT codec,raw_bytes,payload FROM research_consumed_input_objects WHERE sha256=?", (sha,)).fetchone()
                if item is None:
                    raise ValueError("STORED_OBJECT_MISSING")
                objects[sha] = {"codec": item[0], "raw_bytes": item[1],
                                "compressed_b64": base64.b64encode(item[2]).decode("ascii")}
        cycle = {"cycle_id": cycle_id, "root_sha256": row[0], "manifest": manifest, "objects": objects}
        package_from_cycle(cycle)
        return cycle


class PostgresStore:
    def __init__(self, database_url):
        self.database_url = database_url
        self.initialized = False

    def _connect(self):
        import psycopg
        from psycopg.rows import dict_row
        # Never override DSN options, including the isolated CI search_path.
        return psycopg.connect(self.database_url, connect_timeout=5, row_factory=dict_row)

    def persist(self, cycle):
        package_from_cycle(cycle)
        with self._connect() as conn:
            conn.execute("SET LOCAL statement_timeout = 10000")
            conn.execute("SET LOCAL lock_timeout = 1000")
            if not self.initialized:
                # Serialize first-use DDL across overlapping deployment workers.
                conn.execute("SELECT pg_advisory_xact_lock(%s)", (73182461390741532,))
                conn.execute("""CREATE TABLE IF NOT EXISTS research_consumed_input_objects (
                    sha256 TEXT PRIMARY KEY,codec TEXT NOT NULL,raw_bytes INTEGER NOT NULL,
                    payload BYTEA NOT NULL,created_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp())""")
                conn.execute("""CREATE TABLE IF NOT EXISTS research_consumed_input_cycles (
                    cycle_id TEXT PRIMARY KEY,root_sha256 TEXT NOT NULL,manifest_json TEXT NOT NULL,
                    capture_status TEXT NOT NULL,created_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp())""")
            values = [(sha, item["codec"], item["raw_bytes"], base64.b64decode(item["compressed_b64"]))
                      for sha, item in cycle["objects"].items()]
            with conn.cursor() as cursor:
                cursor.executemany("INSERT INTO research_consumed_input_objects (sha256,codec,raw_bytes,payload) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING", values)
            # Verify existing identities without returning large binary bodies.
            # PostgreSQL octet lengths + stored SHA body are checked on readback;
            # a conflict must not let this writer replace retained evidence.
            stored = conn.execute("SELECT sha256,codec,raw_bytes,octet_length(payload) AS packed_bytes,encode(sha256(payload),'hex') AS packed_sha256 FROM research_consumed_input_objects WHERE sha256 = ANY(%s)", (list(cycle["objects"]),)).fetchall()
            expected = {sha: (codec, raw, len(packed), hashlib.sha256(packed).hexdigest()) for sha, codec, raw, packed in values}
            actual = {row["sha256"]: (row["codec"], row["raw_bytes"], row["packed_bytes"], row["packed_sha256"]) for row in stored}
            if set(actual) != set(expected):
                raise ValueError("STORED_OBJECT_MISSING")
            for sha in expected:
                if actual[sha] != expected[sha]:
                    # zlib versions/levels may encode the same immutable value
                    # differently. Verify canonical uncompressed identity and
                    # preserve the existing compressed representation.
                    retained = conn.execute("SELECT codec,raw_bytes,payload FROM research_consumed_input_objects WHERE sha256=%s", (sha,)).fetchone()
                    _decode(sha, {sha: {"codec": retained["codec"], "raw_bytes": retained["raw_bytes"],
                                       "compressed_b64": base64.b64encode(bytes(retained["payload"])).decode("ascii")}})
            conn.execute("INSERT INTO research_consumed_input_cycles (cycle_id,root_sha256,manifest_json,capture_status) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                         (cycle["cycle_id"], cycle["root_sha256"], canonical(cycle["manifest"]), cycle["manifest"]["summary"]["status"]))
            existing = conn.execute("SELECT root_sha256,manifest_json FROM research_consumed_input_cycles WHERE cycle_id=%s", (cycle["cycle_id"],)).fetchone()
            if existing["root_sha256"] != cycle["root_sha256"] or existing["manifest_json"] != canonical(cycle["manifest"]):
                raise ValueError("STORED_CYCLE_CONFLICT")
        self.initialized = True
        return {"persisted": True, "root_sha256": cycle["root_sha256"]}

    def read_cycle(self, cycle_id):
        with self._connect() as conn:
            conn.execute("SET TRANSACTION READ ONLY")
            conn.execute("SET LOCAL statement_timeout = 10000")
            conn.execute("SET LOCAL lock_timeout = 1000")
            row = conn.execute("SELECT root_sha256,manifest_json FROM research_consumed_input_cycles WHERE cycle_id=%s", (cycle_id,)).fetchone()
            if row is None:
                return None
            manifest = json.loads(row["manifest_json"])
            retained = conn.execute("SELECT sha256,codec,raw_bytes,payload FROM research_consumed_input_objects WHERE sha256=ANY(%s)", (manifest["object_sha256s"],)).fetchall()
            objects = {item["sha256"]: {"codec": item["codec"], "raw_bytes": item["raw_bytes"],
                       "compressed_b64": base64.b64encode(bytes(item["payload"])).decode("ascii")}
                       for item in retained}
        cycle = {"cycle_id": cycle_id, "root_sha256": row["root_sha256"], "manifest": manifest, "objects": objects}
        package_from_cycle(cycle)
        return cycle


def _metric_failure(code, cycle_id=None, *, dropped=False):
    with _METRICS_LOCK:
        _METRICS["dropped" if dropped else "failed"] += 1
        _METRICS["last"] = {"cycle_id": cycle_id, "status": "DROPPED" if dropped else "FAILED",
                            "reason_code": code, "at_utc": utc_now()}


def _persist_one(cycle, size):
    try:
        result = _STORE.persist(cycle)
        with _METRICS_LOCK:
            _METRICS["persisted"] += 1
            _METRICS["last"] = {"cycle_id": cycle["cycle_id"], "status": "PERSISTED",
                                "capture_status": cycle["manifest"]["summary"]["status"],
                                "root_sha256": cycle["root_sha256"], "at_utc": utc_now(), **result}
    except Exception as exc:
        _metric_failure(type(exc).__name__, cycle["cycle_id"])
    finally:
        with _METRICS_LOCK:
            _METRICS["queued_bytes"] -= size
            _METRICS["queued_items"] -= 1


def _run():
    while True:
        cycle, size = _QUEUE.get()
        _persist_one(cycle, size)
        _QUEUE.task_done()


def _ensure_worker():
    global _WORKER, _STORE
    with _WORKER_LOCK:
        if _WORKER is not None and _WORKER.is_alive():
            return True
        if _STORE is None:
            url = os.getenv("DATABASE_URL", "")
            if not url:
                return False
            _STORE = PostgresStore(url)
        _WORKER = threading.Thread(target=_run, name="research-consumed-inputs", daemon=True)
        _WORKER.start()
        return True


def submit(cycle_id, package_or_collector, *, rows, snapshot, score_block,
           code_versions=None, expected_symbols=EXPECTED_SYMBOLS, metadata=None):
    try:
        cycle = prepare_cycle(cycle_id, package_or_collector, rows=rows, snapshot=snapshot,
                              score_block=score_block, code_versions=code_versions,
                              expected_symbols=expected_symbols, metadata=metadata)
        # The queue retains base64 text, so account for its in-memory encoding,
        # object metadata and manifest instead of compressed bytes alone.
        size = (sum(len(item["compressed_b64"]) + 256 for item in cycle["objects"].values())
                + len(canonical(cycle["manifest"]).encode("utf-8")))
        if not _ensure_worker():
            _metric_failure("DATABASE_NOT_CONFIGURED", cycle_id)
            return {"version": VERSION, "status": "UNAVAILABLE", "reason_code": "DATABASE_NOT_CONFIGURED"}
        with _METRICS_LOCK:
            if (_METRICS["queued_bytes"] + size > MAX_QUEUE_BYTES or
                    _METRICS["queued_items"] >= MAX_QUEUE_ITEMS):
                _METRICS["dropped"] += 1
                _METRICS["last"] = {"cycle_id": cycle_id, "status": "DROPPED",
                                    "reason_code": "QUEUE_BOUND", "at_utc": utc_now()}
                return {"version": VERSION, "status": "DROPPED", "reason_code": "QUEUE_BOUND"}
            _METRICS["queued_bytes"] += size
            _METRICS["queued_items"] += 1
            _METRICS["submitted"] += 1
            _METRICS["partial"] += int(cycle["manifest"]["summary"]["status"] == "PARTIAL")
            _QUEUE.put_nowait((cycle, size))
        return {"version": VERSION, "status": "QUEUED", "root_sha256": cycle["root_sha256"],
                "capture_status": cycle["manifest"]["summary"]["status"],
                "expected_symbols": list(expected_symbols), "summary": cycle["manifest"]["summary"]}
    except Exception as exc:
        _metric_failure(type(exc).__name__, cycle_id)
        return {"version": VERSION, "status": "FAILED", "reason_code": type(exc).__name__}


def status():
    with _METRICS_LOCK:
        result = deepcopy(_METRICS)
    return {"version": VERSION, "scope": "consumed-calculation-inputs-only",
            "expected_symbols": list(EXPECTED_SYMBOLS), "worker_alive": bool(_WORKER and _WORKER.is_alive()),
            "limits": {"queue_items": MAX_QUEUE_ITEMS, "queue_bytes": MAX_QUEUE_BYTES,
                       "package_raw_bytes": MAX_RAW_BYTES, "object_bytes": MAX_OBJECT_BYTES}, **result}
