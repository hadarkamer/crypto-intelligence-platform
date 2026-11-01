"""Prospective DOGE sequence-call evidence; never a decision or SQL snapshot.

The intake caller records objects it actually consumed and enqueues this
observation only after its existing transaction commits. The independent
writer has its own bounded queue, table, manifest and health counters. It
does not import scorers, read source rows, change isolation, or route alerts.
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
import os
from pathlib import Path
import platform
import queue
import sqlite3
import sys
import threading
from uuid import uuid4

import research_consumed_inputs as graph

VERSION = "doge-sequence-consumed-inputs-v1"
SCOPE = "prospective-actual-sequence-call-arrays"
TABLE = "research_doge_sequence_observations"
CODE_FILES = (
    "research_doge_sequence_capture.py", "research_consumed_inputs.py",
    "research_formula_ordered_worker.py", "research_formula_ordered_store.py",
    "research_formula_ordered_v7.py", "research_ordered_question_catalog.py",
    "research_ordered_question_store.py", "research_ordered_first_touch.py",
    "research_past_price_features.py", "research_past_price_features_store.py",
    "analysis.py",
)
MAX_RAW_BYTES = 64 * 1024 * 1024
MAX_COMPRESSED_BYTES = 16 * 1024 * 1024
MAX_QUEUE_BYTES = 32 * 1024 * 1024
MAX_QUEUE_ITEMS = 2
_CURRENT = ContextVar("doge_sequence_observation", default=None)
_LOCK = threading.Lock()
_WORKER_LOCK = threading.Lock()
_QUEUE = queue.Queue(maxsize=MAX_QUEUE_ITEMS)
_WORKER = None
_STORE = None
_METRICS = dict(started=0, submitted=0, persisted=0, partial=0, failed=0,
                dropped=0, discarded=0, empty=0, queued_items=0,
                queued_bytes=0, last=None, worker_payload_releases=0,
                last_worker_payload_accounted_bytes=None)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


@lru_cache(maxsize=1)
def code_versions():
    root = Path(__file__).resolve().parent
    return {name: hashlib.sha256((root / name).read_bytes()).hexdigest()
            if (root / name).is_file() else None for name in CODE_FILES}


def runtime_identity():
    return {"python_version": platform.python_version(),
            "python_implementation": platform.python_implementation(),
            "python_build": list(platform.python_build()),
            "float_info": {key: getattr(sys.float_info, key) for key in
                           ("mant_dig", "max_exp", "min_exp", "radix", "rounds")},
            "byteorder": sys.byteorder, "platform": platform.platform(),
            "time_model": "aware-datetime-normalized-to-UTC; original-offset-text-bridged",
            "render_git_commit": os.getenv("RENDER_GIT_COMMIT")}


def _legacy_material(event):
    # This is the question-store contract, which preserves float 1.0. The
    # generic Watch bridge normalizes it and cannot substitute for this text.
    encoded = json.dumps(event, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False, default=str, allow_nan=False)
    paths = []
    def walk(item, path):
        if isinstance(item, datetime):
            paths.append({"path": path, "source_text": str(item),
                          "normalized_text": graph.normalized(item)})
        elif isinstance(item, dict):
            for key, child in item.items():
                walk(child, path + [key])
        elif isinstance(item, (list, tuple)):
            for index, child in enumerate(item):
                walk(child, path + [index])
    walk(event, [])
    return {"canonical_json": encoded,
            "sha256": hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
            "datetime_paths": paths}


class _ScalarWitness:
    """Keep bool distinct from numbers during fast native-container equality."""
    __slots__ = ("value", "boolean")

    def __init__(self, value):
        self.value, self.boolean = value, type(value) is bool

    def __eq__(self, other):
        if self.boolean:
            return type(other) is bool and self.value is other
        return type(other) in (int, float) and self.value == other


def _comparison_snapshot(value):
    # Native dict/list equality traverses unchanged containers in C. Numeric
    # witnesses prevent Python's True == 1 from hiding a meaningful mutation;
    # int/float integral equivalence follows the graph's documented hash codec.
    if type(value) in (bool, int, float):
        return _ScalarWitness(value)
    if isinstance(value, dict):
        return {key: _comparison_snapshot(child) for key, child in value.items()}
    if isinstance(value, list):
        return [_comparison_snapshot(child) for child in value]
    if isinstance(value, tuple):
        return tuple(_comparison_snapshot(child) for child in value)
    return value


class Observation:
    def __init__(self):
        self.observation_id = str(uuid4())
        self.started_at_utc = utc_now()
        self.collector = graph.Collector(max_raw_bytes=MAX_RAW_BYTES,
                                         max_compressed_bytes=MAX_COMPRESSED_BYTES)
        self.closed = False
        self.discarded = False
        self.submitted = False
        self.committed_at_utc = None
        self.doge_event_count = None
        self.identity_recorded = False
        self.capture_stopped = False
        self._pair_cache = {}

    def _check_bound(self):
        self.capture_stopped = any(error["code"] in
            ("PACKAGE_SIZE_BOUND", "OBJECT_SIZE_BOUND", "EVENT_COUNT_BOUND", "OVERSIZED_SCALAR")
            for error in self.collector.errors)

    def _pair_ref(self, pair):
        key = id(pair)
        retained = self._pair_cache.get(key)
        if retained is not None and retained[0] is pair and pair == retained[1]:
            return retained[2]
        ref = self.collector._node([graph.normalized(pair)])
        self._pair_cache[key] = (pair, _comparison_snapshot(pair), ref)
        return ref

    def record(self, kind, payload):
        """Freeze values immediately; capture exceptions never escape to intake."""
        try:
            if self.closed or self.discarded or self.submitted or self.capture_stopped:
                return None
            if kind == "current_events":
                self.doge_event_count = len(payload.get("events", []))
                if self.doge_event_count and not self.identity_recorded:
                    self.collector.record("identity", {"runtime": runtime_identity(), "code_versions": code_versions()})
                    self.identity_recorded = True
            if self.doge_event_count == 0:
                return None
            if kind not in ("sequence_call_input", "history_base"):
                ref = self.collector.record(kind, payload)
                self._check_bound()
                return ref
            pair_key = "prior" if kind == "sequence_call_input" else "pairs"
            value = graph.normalized({key: child for key, child in payload.items() if key != pair_key})
            if kind == "sequence_call_input":
                value["legacy_event_input"] = _legacy_material(payload["event"])
            # Each child is a one-element list because the shared graph list
            # codec concatenates segments. This keeps full order/multiplicity
            # and shares immutable prior pairs across different current calls.
            prior_ref = self.collector._put({"node": "list", "items": [
                self._pair_ref(pair) for pair in payload[pair_key]]})
            fields = [[key, self.collector._node(child)] for key, child in sorted(value.items())]
            fields.append([pair_key, prior_ref])
            ref = self.collector._put({"node": "dict", "items": sorted(fields)})
            if len(self.collector.events) >= graph.MAX_EVENTS:
                raise ValueError("EVENT_COUNT_BOUND")
            self.collector.events.append({"kind": kind, "object_sha256": ref,
                                          "recorded_at_utc": utc_now()})
            return ref
        except Exception as exc:
            self.collector.gap(str(exc) if isinstance(exc, ValueError) else type(exc).__name__)
            self._check_bound()
            return None


@contextmanager
def observe():
    """Context exit does not imply commit; the caller must submit after commit."""
    observation, token = None, None
    try:
        observation = Observation()
        token = _CURRENT.set(observation)
        with _LOCK:
            _METRICS["started"] += 1
    except Exception:
        _failure("OBSERVER_START_FAILED")
    try:
        yield observation
    except BaseException:
        discard(observation)
        raise
    finally:
        if token is not None:
            _CURRENT.reset(token)
        if observation is not None:
            observation.closed = True
            observation._pair_cache.clear()


def current():
    return _CURRENT.get()


def record(kind, payload):
    observation = current()
    return observation.record(kind, payload) if observation is not None else None


def discard(observation):
    if observation is not None and not observation.discarded and not observation.submitted:
        observation.discarded = True
        with _LOCK:
            _METRICS["discarded"] += 1


def _utc_clock(value):
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("INVALID_OBSERVATION_CLOCK")
    return parsed


def _bridge_event(event, material):
    encoded = material["canonical_json"]
    if hashlib.sha256(encoded.encode("utf-8")).hexdigest() != material["sha256"]:
        raise ValueError("LEGACY_INPUT_SHA_MISMATCH")
    original = json.loads(encoded)
    for item in material["datetime_paths"]:
        parent = original
        for key in item["path"][:-1]:
            parent = parent[key]
        key = item["path"][-1]
        if parent[key] != item["source_text"]:
            raise ValueError("LEGACY_DATETIME_BRIDGE_MISMATCH")
        parsed = _utc_clock(item["source_text"])
        if graph.normalized(parsed) != item["normalized_text"]:
            raise ValueError("LEGACY_DATETIME_BRIDGE_MISMATCH")
        parent[key] = item["normalized_text"]
    if graph.normalized(original) != event:
        raise ValueError("LEGACY_EVENT_BINDING_MISMATCH")


def summarize(package):
    """Structural proof only; validity/matches are retained, never recomputed."""
    graph.validate(package)
    by_kind = {}
    # Do not expand the shared prior pool once per current event. Recover it
    # once and prove the exact forwarded order through its immutable refs.
    prior_refs, pool_refs = {}, []
    for header in package["events"]:
        kind = header["kind"]
        if kind == "sequence_call_input":
            node = graph._decode(header["object_sha256"], package["objects"])
            if node.get("node") != "dict":
                raise ValueError("INVALID_INPUT_NODE")
            fields = dict(node["items"])
            payload = {key: graph.recover(package, ref) for key, ref in fields.items() if key != "prior"}
            prior = graph._decode(fields["prior"], package["objects"])
            if prior.get("node") != "list":
                raise ValueError("INVALID_PRIOR_NODE")
            prior_refs.setdefault(payload.get("event_id"), []).append(prior["items"])
        else:
            payload = graph.recover(package, header["object_sha256"])
            if kind == "history_base":
                node = graph._decode(header["object_sha256"], package["objects"])
                fields = dict(node["items"])
                pool_refs = graph._decode(fields["pairs"], package["objects"])["items"]
        by_kind.setdefault(header["kind"], []).append(payload)
    gaps = {error["code"] for error in package["errors"]}
    def require(condition, code):
        if not condition:
            gaps.add(code)
    identities = by_kind.get("identity", [])
    require(len(identities) == 1, "IDENTITY_COUNT")
    if len(identities) == 1:
        identity = identities[0]
        runtime = identity.get("runtime", {})
        require(isinstance(runtime, dict) and bool(runtime.get("python_version")) and
                bool(runtime.get("python_implementation")) and isinstance(runtime.get("float_info"), dict)
                and bool(runtime.get("time_model")), "RUNTIME_IDENTITY_MISSING")
        hashes = identity.get("code_versions", {})
        require(set(hashes) == set(CODE_FILES) and all(isinstance(v, str) and len(v) == 64 and
                all(character in "0123456789abcdef" for character in v)
                for v in hashes.values()), "SOURCE_IDENTITY_INCOMPLETE")
    currents = by_kind.get("current_events", [])
    require(len(currents) == 1, "CURRENT_EVENTS_COUNT")
    events = currents[0].get("events", []) if len(currents) == 1 else []
    expected = [event.get("event_id") for event in events]
    require(bool(expected) and len(expected) == len(set(expected)) and
            all(event.get("symbol") == "DOGE" for event in events), "CURRENT_DOGE_EVENTS_INVALID")
    catalogs = by_kind.get("catalog", [])
    require(len(catalogs) == 1, "CATALOG_COUNT")
    candidates = catalogs[0].get("candidates", []) if len(catalogs) == 1 else []
    candidate_keys = [item.get("formula_id") for item in candidates]
    require(bool(candidate_keys) and None not in candidate_keys and
            len(candidate_keys) == len(set(candidate_keys)), "CATALOG_INCOMPLETE")
    inputs, outputs = by_kind.get("sequence_call_input", []), by_kind.get("sequence_call_output", [])
    input_ids, output_ids = [p.get("event_id") for p in inputs], [p.get("event_id") for p in outputs]
    require(len(input_ids) == len(set(input_ids)) and sorted(input_ids) == sorted(expected), "INPUT_COVERAGE_INCOMPLETE")
    require(len(output_ids) == len(set(output_ids)) and sorted(output_ids) == sorted(expected), "OUTPUT_COVERAGE_INCOMPLETE")
    input_by_id = {item.get("event_id"): item for item in inputs}
    event_by_id = {item.get("event_id"): item for item in events}
    pools = by_kind.get("history_base", [])
    require(len(pools) == 1 and isinstance(pools[0].get("pairs"), list), "HISTORY_BASE_MISSING")
    pool = pools[0].get("pairs", []) if len(pools) == 1 else []
    for item in inputs:
        try:
            require(item["event"] == event_by_id[item["event_id"]], "CURRENT_INPUT_BINDING_MISMATCH")
            require(isinstance(item["base_features"], dict), "INPUT_ARRAY_MISSING")
            expected_refs = [ref for ref, pair in zip(pool_refs, pool) if pair[0]["event_id"] != item["event_id"]]
            require(prior_refs[item["event_id"]] == [expected_refs], "EXACT_PRIOR_POOL_BINDING_MISMATCH")
            _bridge_event(item["event"], item["legacy_event_input"])
        except Exception:
            gaps.add("INPUT_BINDING_INVALID")
    for item in outputs:
        try:
            source = input_by_id[item["event_id"]]
            require(isinstance(item["sequence_features"], dict) and isinstance(item["final_features"], dict), "OUTPUT_FEATURES_MISSING")
            require({**source["base_features"], **item["sequence_features"]} == item["final_features"], "OUTPUT_MERGE_MISMATCH")
            outcomes = item["candidate_outcomes"]
            keys = [outcome.get("candidate_key") for outcome in outcomes]
            require(len(keys) == len(set(keys)) and set(keys) == set(candidate_keys), "CANDIDATE_OUTCOMES_INCOMPLETE")
            require(all(type(outcome.get("matched")) is bool for outcome in outcomes), "CANDIDATE_OUTCOME_INVALID")
            catalog_by_key = {candidate["formula_id"]: candidate for candidate in candidates}
            direction = source["event"]["direction"]
            for outcome in outcomes:
                inverse = catalog_by_key[outcome["candidate_key"]].get("research_orientation") == "INVERSE"
                require(outcome.get("source_direction") == direction and
                        outcome.get("orientation") == ("INVERSE" if inverse else "NORMAL"), "OUTCOME_DIRECTION_IDENTITY_MISMATCH")
                routed = ({"LONG": "SHORT", "SHORT": "LONG"}[direction] if inverse else direction) if outcome["matched"] else None
                require(outcome.get("routed_direction") == routed, "OUTCOME_ROUTING_MISMATCH")
        except Exception:
            gaps.add("OUTPUT_BINDING_INVALID")
    reads = by_kind.get("history_read", [])
    require(bool(reads), "HISTORY_READS_MISSING")
    require(any(item.get("phase") == "metadata_fetch" and item.get("rows") == [] for item in reads), "HISTORY_TERMINATOR_MISSING")
    for item in reads:
        try:
            require(_utc_clock(item["finished_at_utc"]) >= _utc_clock(item["started_at_utc"]), "READ_CLOCK_ORDER")
            require(item["finished_monotonic_ns"] >= item["started_monotonic_ns"], "READ_MONOTONIC_ORDER")
            require(item["phase"] in ("metadata_execute", "metadata_fetch", "projection") and
                    isinstance(item.get("read_id"), str) and isinstance(item.get("rows"), list), "READ_SHAPE_INVALID")
            if item["phase"] != "metadata_fetch":
                require(isinstance(item.get("query"), str) and bool(item["query"]) and "parameters" in item, "READ_QUERY_MISSING")
        except Exception:
            gaps.add("READ_BRACKET_INVALID")
    try:
        execute = [item for item in reads if item["phase"] == "metadata_execute"]
        fetches = [item for item in reads if item["phase"] == "metadata_fetch"]
        projections = [item for item in reads if item["phase"] == "projection"]
        require(len(execute) == 1 and execute[0]["rows"] == [], "HISTORY_EXECUTE_COUNT")
        require(bool(fetches) and fetches[-1]["rows"] == [] and
                all(item["rows"] for item in fetches[:-1]), "HISTORY_FETCH_TERMINATOR_INVALID")
        require(len({item["read_id"] for item in reads}) == 1 and
                all(item.get("source_cursor_name") == item["read_id"] for item in reads), "HISTORY_READ_IDENTITY_MISMATCH")
        require(reads[0]["phase"] == "metadata_execute" and reads[-1]["phase"] == "metadata_fetch" and
                reads[-1]["rows"] == [], "HISTORY_READ_ORDER_INVALID")
        require(all(left["finished_monotonic_ns"] <= right["started_monotonic_ns"]
                    for left, right in zip(reads, reads[1:])), "HISTORY_READ_OVERLAP")
        metadata = [row for item in fetches for row in item["rows"]]
        projected = [row for item in projections for row in item["rows"]]
        ids = [row["event_id"] for row in metadata]
        require(ids == sorted(set(ids)), "HISTORY_SOURCE_ORDER_INVALID")
        require(ids == [row["event_id"] for row in projected], "HISTORY_PROJECTION_COVERAGE_MISMATCH")
        require(all(item.get("requested_event_ids") == [row["event_id"] for row in item["rows"]]
                    and item.get("parameters") == [item.get("requested_event_ids")] for item in projections), "HISTORY_PROJECTION_ARGUMENTS_MISMATCH")
        require(all(all(row[key] == meta[key] for key in ("event_id", "symbol", "direction", "alert_time_utc"))
                    and row.get("event_kind") == "ALERT" and row.get("delivery_status") == "DELIVERED"
                    for row, meta in zip(projected, metadata)), "HISTORY_METADATA_BINDING_MISMATCH")
        require([pair[0] for pair in pool] == projected, "HISTORY_BASE_PROJECTED_BINDING_MISMATCH")
        require(all(isinstance(pair, list) and len(pair) == 2 and isinstance(pair[1], dict)
                    for pair in pool), "HISTORY_BASE_INVALID")
    except Exception:
        gaps.add("HISTORY_CLOSURE_INVALID")
    routing = by_kind.get("routing", [])
    require(len(routing) == 1, "ROUTING_RECEIPT_MISSING")
    if len(routing) == 1:
        try:
            require(routing[0]["insert_outcome"] == "UNOBSERVED_INSERT_OR_EXISTING" and
                    routing[0]["rule"] == "ON CONFLICT(candidate_key,event_id) DO NOTHING", "ROUTING_SCOPE_INVALID")
            expected_hits = [{"candidate_key": outcome["candidate_key"], "event_id": item["event_id"],
                              "source_direction": outcome["source_direction"], "routed_direction": outcome["routed_direction"]}
                             for item in outputs for outcome in item["candidate_outcomes"] if outcome["matched"]]
            require(routing[0]["candidate_hits"] == expected_hits, "ROUTING_OUTPUT_BINDING_MISMATCH")
        except Exception:
            gaps.add("ROUTING_BINDING_INVALID")
    return {"status": "PARTIAL" if gaps else "CAPTURED", "gaps": sorted(gaps),
            "doge_event_count": len(expected), "input_count": len(inputs),
            "output_count": len(outputs), "history_read_count": len(reads),
            "independent_semantic_replay_performed": False}


def prepare(observation):
    if observation.discarded or not observation.committed_at_utc:
        raise ValueError("SOURCE_TRANSACTION_NOT_COMMITTED")
    package = observation.collector.package()
    summary = summarize(package)
    manifest = {"version": VERSION, "scope": SCOPE, "observation_id": observation.observation_id,
                "observation_started_at_utc": observation.started_at_utc,
                "source_transaction_commit_confirmed_at_utc": observation.committed_at_utc,
                "commit_clock_scope": "caller-observed-successful-intake-commit; not-server-visibility-clock",
                "actual_consumed_arrays_retained": summary["status"] == "CAPTURED",
                "single_sql_snapshot_proven": False,
                "original_first_match_observed": False,
                "match_insert_outcome": "UNOBSERVED_INSERT_OR_EXISTING",
                "provider_http_originals_proven": False,
                "package_version": graph.VERSION, "hash_version": graph.HASH_VERSION,
                "events": package["events"], "errors": package["errors"],
                "object_sha256s": sorted(package["objects"]), "summary": summary}
    return {"observation_id": observation.observation_id, "manifest": manifest,
            "root_sha256": graph.digest(manifest), "objects": package["objects"]}


def package_from_observation(observation):
    manifest = observation["manifest"]
    if (manifest.get("version") != VERSION or manifest.get("scope") != SCOPE or
            observation.get("observation_id") != manifest.get("observation_id") or
            graph.digest(manifest) != observation.get("root_sha256") or
            sorted(observation["objects"]) != manifest.get("object_sha256s")):
        raise ValueError("OBSERVATION_MANIFEST_MISMATCH")
    if _utc_clock(manifest["source_transaction_commit_confirmed_at_utc"]) < _utc_clock(manifest["observation_started_at_utc"]):
        raise ValueError("COMMIT_CLOCK_ORDER")
    package = {"version": manifest["package_version"], "hash_version": manifest["hash_version"],
               "objects": observation["objects"], "events": manifest["events"], "errors": manifest["errors"]}
    summary = summarize(package)
    if summary != manifest["summary"] or manifest.get("actual_consumed_arrays_retained") != (summary["status"] == "CAPTURED"):
        raise ValueError("OBSERVATION_SUMMARY_MISMATCH")
    if (manifest.get("single_sql_snapshot_proven") is not False or
            manifest.get("original_first_match_observed") is not False or
            manifest.get("provider_http_originals_proven") is not False or
            manifest.get("match_insert_outcome") != "UNOBSERVED_INSERT_OR_EXISTING"):
        raise ValueError("OBSERVATION_PROOF_SCOPE_MISMATCH")
    return package


def _stored_object(sha, codec, raw_bytes, payload):
    graph._decode(sha, {sha: {"codec": codec, "raw_bytes": raw_bytes,
                            "compressed_b64": base64.b64encode(bytes(payload)).decode("ascii")}})


class SQLiteStore:
    """Explicit local validation store; never selected by runtime."""
    def __init__(self, path):
        self.path = str(path)
        with closing(sqlite3.connect(self.path)) as conn, conn:
            conn.execute("CREATE TABLE IF NOT EXISTS research_consumed_input_objects (sha256 TEXT PRIMARY KEY,codec TEXT NOT NULL,raw_bytes INTEGER NOT NULL,payload BLOB NOT NULL,created_at_utc TEXT NOT NULL)")
            conn.execute("CREATE TABLE IF NOT EXISTS research_doge_sequence_observations (observation_id TEXT PRIMARY KEY,root_sha256 TEXT NOT NULL,manifest_json TEXT NOT NULL,capture_status TEXT NOT NULL,created_at_utc TEXT NOT NULL)")

    def persist(self, observation):
        package_from_observation(observation)
        with closing(sqlite3.connect(self.path)) as conn, conn:
            for sha, item in observation["objects"].items():
                packed = base64.b64decode(item["compressed_b64"])
                conn.execute("INSERT OR IGNORE INTO research_consumed_input_objects VALUES (?,?,?,?,?)", (sha, item["codec"], item["raw_bytes"], packed, utc_now()))
                stored = conn.execute("SELECT codec,raw_bytes,payload FROM research_consumed_input_objects WHERE sha256=?", (sha,)).fetchone()
                _stored_object(sha, *stored)
            conn.execute("INSERT OR IGNORE INTO research_doge_sequence_observations VALUES (?,?,?,?,?)", (observation["observation_id"], observation["root_sha256"], graph.canonical(observation["manifest"]), observation["manifest"]["summary"]["status"], utc_now()))
            existing = conn.execute("SELECT root_sha256,manifest_json FROM research_doge_sequence_observations WHERE observation_id=?", (observation["observation_id"],)).fetchone()
            if existing != (observation["root_sha256"], graph.canonical(observation["manifest"])):
                raise ValueError("STORED_OBSERVATION_CONFLICT")
        return {"persisted": True, "root_sha256": observation["root_sha256"]}

    def read(self, observation_id):
        with closing(sqlite3.connect(self.path)) as conn:
            row = conn.execute("SELECT root_sha256,manifest_json FROM research_doge_sequence_observations WHERE observation_id=?", (observation_id,)).fetchone()
            if row is None:
                return None
            manifest, objects = json.loads(row[1]), {}
            for sha in manifest["object_sha256s"]:
                stored = conn.execute("SELECT codec,raw_bytes,payload FROM research_consumed_input_objects WHERE sha256=?", (sha,)).fetchone()
                if stored is None:
                    raise ValueError("STORED_OBJECT_MISSING")
                objects[sha] = {"codec": stored[0], "raw_bytes": stored[1], "compressed_b64": base64.b64encode(stored[2]).decode("ascii")}
        result = {"observation_id": observation_id, "root_sha256": row[0], "manifest": manifest, "objects": objects}
        package_from_observation(result)
        return result


class PostgresStore:
    def __init__(self, database_url):
        self.database_url, self.initialized = database_url, False

    def _connect(self):
        import psycopg
        from psycopg.rows import dict_row
        return psycopg.connect(self.database_url, connect_timeout=5, row_factory=dict_row)

    def persist(self, observation):
        package_from_observation(observation)
        with self._connect() as conn:
            conn.execute("SET LOCAL statement_timeout = 10000")
            conn.execute("SET LOCAL lock_timeout = 1000")
            if not self.initialized:
                # Same advisory DDL lock as the shared object writer, so a
                # new deployment cannot race its CREATE TABLE IF NOT EXISTS.
                conn.execute("SELECT pg_advisory_xact_lock(%s)", (73182461390741532,))
                conn.execute("CREATE TABLE IF NOT EXISTS research_consumed_input_objects (sha256 TEXT PRIMARY KEY,codec TEXT NOT NULL,raw_bytes INTEGER NOT NULL,payload BYTEA NOT NULL,created_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp())")
                conn.execute("CREATE TABLE IF NOT EXISTS research_doge_sequence_observations (observation_id TEXT PRIMARY KEY,root_sha256 TEXT NOT NULL,manifest_json TEXT NOT NULL,capture_status TEXT NOT NULL,created_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp())")
            values = [(sha, item["codec"], item["raw_bytes"], base64.b64decode(item["compressed_b64"])) for sha, item in observation["objects"].items()]
            with conn.cursor() as cursor:
                cursor.executemany("INSERT INTO research_consumed_input_objects (sha256,codec,raw_bytes,payload) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING", values)
            stored = conn.execute("SELECT sha256,codec,raw_bytes,octet_length(payload) AS packed_bytes,encode(sha256(payload),'hex') AS packed_sha256 FROM research_consumed_input_objects WHERE sha256=ANY(%s)", (list(observation["objects"]),)).fetchall()
            actual = {row["sha256"]: row for row in stored}
            if set(actual) != set(observation["objects"]):
                raise ValueError("STORED_OBJECT_MISSING")
            for sha, codec, raw_bytes, packed in values:
                row = actual[sha]
                if (row["codec"], row["raw_bytes"], row["packed_bytes"], row["packed_sha256"]) != (codec, raw_bytes, len(packed), hashlib.sha256(packed).hexdigest()):
                    retained = conn.execute("SELECT codec,raw_bytes,payload FROM research_consumed_input_objects WHERE sha256=%s", (sha,)).fetchone()
                    _stored_object(sha, retained["codec"], retained["raw_bytes"], retained["payload"])
            conn.execute("INSERT INTO research_doge_sequence_observations (observation_id,root_sha256,manifest_json,capture_status) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING", (observation["observation_id"], observation["root_sha256"], graph.canonical(observation["manifest"]), observation["manifest"]["summary"]["status"]))
            existing = conn.execute("SELECT root_sha256,manifest_json FROM research_doge_sequence_observations WHERE observation_id=%s", (observation["observation_id"],)).fetchone()
            if existing["root_sha256"] != observation["root_sha256"] or existing["manifest_json"] != graph.canonical(observation["manifest"]):
                raise ValueError("STORED_OBSERVATION_CONFLICT")
        self.initialized = True
        return {"persisted": True, "root_sha256": observation["root_sha256"]}

    def read(self, observation_id):
        with self._connect() as conn:
            conn.execute("SET TRANSACTION READ ONLY")
            conn.execute("SET LOCAL statement_timeout = 10000")
            row = conn.execute("SELECT root_sha256,manifest_json FROM research_doge_sequence_observations WHERE observation_id=%s", (observation_id,)).fetchone()
            if row is None:
                return None
            manifest = json.loads(row["manifest_json"])
            retained = conn.execute("SELECT sha256,codec,raw_bytes,payload FROM research_consumed_input_objects WHERE sha256=ANY(%s)", (manifest["object_sha256s"],)).fetchall()
        objects = {item["sha256"]: {"codec": item["codec"], "raw_bytes": item["raw_bytes"], "compressed_b64": base64.b64encode(bytes(item["payload"])).decode("ascii")} for item in retained}
        result = {"observation_id": observation_id, "root_sha256": row["root_sha256"], "manifest": manifest, "objects": objects}
        package_from_observation(result)
        return result


def _failure(code, observation_id=None, *, dropped=False):
    with _LOCK:
        _METRICS["dropped" if dropped else "failed"] += 1
        _METRICS["last"] = {"observation_id": observation_id,
                            "status": "DROPPED" if dropped else "FAILED",
                            "reason_code": code, "at_utc": utc_now()}


def _persist_one(observation, size):
    try:
        prepared = prepare(observation)
        result = _STORE.persist(prepared)
        with _LOCK:
            _METRICS["persisted"] += 1
            _METRICS["partial"] += int(prepared["manifest"]["summary"]["status"] == "PARTIAL")
            _METRICS["last"] = {"observation_id": observation.observation_id,
                                "status": "PERSISTED", "capture_status": prepared["manifest"]["summary"]["status"],
                                "summary": prepared["manifest"]["summary"], "at_utc": utc_now(), **result}
    except Exception as exc:
        _failure(str(exc) if isinstance(exc, ValueError) else type(exc).__name__, observation.observation_id)
    finally:
        with _LOCK:
            _METRICS["queued_bytes"] -= size
            _METRICS["queued_items"] -= 1


def _run():
    while True:
        observation, size = _QUEUE.get()
        _persist_one(observation, size)
        _QUEUE.task_done()
        released_accounted_bytes = size if type(size) is int and 0 <= size <= MAX_QUEUE_BYTES else None
        del observation, size
        try:
            with _LOCK:
                _METRICS["worker_payload_releases"] = _METRICS.get("worker_payload_releases", 0) + 1
                _METRICS["last_worker_payload_accounted_bytes"] = released_accounted_bytes
        except BaseException:
            pass


def _ensure_worker():
    global _WORKER, _STORE
    with _WORKER_LOCK:
        if _WORKER is not None and _WORKER.is_alive():
            return True
        if _STORE is None:
            url = database_url()
            if not url:
                return False
            _STORE = PostgresStore(url)
        _WORKER = threading.Thread(target=_run, name="research-doge-sequence-capture", daemon=True)
        _WORKER.start()
        return True


def database_url():
    """Match ordered intake's dedicated-research/opt-in-primary selection."""
    dedicated = os.getenv("RESEARCH_DATABASE_URL", "").strip()
    primary_enabled = os.getenv("RESEARCH_USE_PRIMARY_DATABASE", "").strip().lower() in {"1", "true", "yes", "on"}
    return dedicated or (os.getenv("DATABASE_URL", "").strip() if primary_enabled else "")


def submit_after_commit(observation):
    """Caller asserts its existing intake commit succeeded; no source queries."""
    try:
        if observation is None or observation.discarded or observation.submitted:
            return {"version": VERSION, "status": "DISCARDED_OR_UNAVAILABLE"}
        if not observation.closed:
            raise ValueError("OBSERVATION_STILL_OPEN")
        if not observation.doge_event_count:
            observation.submitted = True
            with _LOCK:
                _METRICS["empty"] += 1
            return {"version": VERSION, "status": "NO_CHANGED_DOGE_EVENTS"}
        observation.committed_at_utc = utc_now()
        size = sum(len(item["compressed_b64"]) + 256 for item in observation.collector.objects.values()) + len(graph.canonical(observation.collector.events).encode("utf-8")) + 4096
        if not _ensure_worker():
            _failure("DATABASE_NOT_CONFIGURED", observation.observation_id)
            return {"version": VERSION, "status": "UNAVAILABLE", "reason_code": "DATABASE_NOT_CONFIGURED"}
        with _LOCK:
            if _METRICS["queued_bytes"] + size > MAX_QUEUE_BYTES or _METRICS["queued_items"] >= MAX_QUEUE_ITEMS:
                _METRICS["dropped"] += 1
                _METRICS["last"] = {"observation_id": observation.observation_id, "status": "DROPPED", "reason_code": "QUEUE_BOUND", "at_utc": utc_now()}
                return {"version": VERSION, "status": "DROPPED", "reason_code": "QUEUE_BOUND"}
            observation.submitted = True
            _METRICS["queued_bytes"] += size
            _METRICS["queued_items"] += 1
            try:
                _QUEUE.put_nowait((observation, size))
            except Exception:
                observation.submitted = False
                _METRICS["queued_bytes"] -= size
                _METRICS["queued_items"] -= 1
                raise
            _METRICS["submitted"] += 1
        return {"version": VERSION, "status": "QUEUED", "observation_id": observation.observation_id}
    except Exception as exc:
        _failure(type(exc).__name__, getattr(observation, "observation_id", None))
        return {"version": VERSION, "status": "FAILED", "reason_code": type(exc).__name__}


def status():
    with _LOCK:
        metrics = deepcopy(_METRICS)
    return {"version": VERSION, "scope": SCOPE,
            "worker_payload_release_scope": "LOOP_LOCAL_REFERENCE_DELETED_AFTER_TASK_DONE; BYTES_ARE_EXISTING_QUEUE_ACCOUNTING_NOT_HEAP_OR_RSS_RECLAIMED; OTHER_OWNERS_MAY_RETAIN_PAYLOAD",
            "single_sql_snapshot_proven": False, "original_first_match_observed": False,
            "worker_alive": bool(_WORKER and _WORKER.is_alive()),
            "database_configured": bool(database_url()),
            "limits": {"queue_items": MAX_QUEUE_ITEMS, "queue_bytes": MAX_QUEUE_BYTES,
                       "package_raw_bytes": MAX_RAW_BYTES, "package_compressed_bytes": MAX_COMPRESSED_BYTES},
            **metrics}
