"""Offline conformance reference for a separately activated liquidity study.

No database, network, account, notification, order, journal or activation I/O.
Caller-supplied activation, reader and parent declarations are checked for
consistency; this does not attest that a live observer produced them. A match
is never enrollment or evidence of execution, first availability or returns.
"""
from __future__ import annotations

import base64
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import zlib

SHADOW_VERSION = "doge-combined-selected-slot-shadow-v1"
SHADOW_CONTRACT_SHA256 = "30b5e533f360db0a443d2d47bcbaa84e813e4f754145647e7186cd5ad4012ff1"
NATIVE_CONTRACT = "MAX_PAIN_CAPTURED_AMOUNTS_V1"
PARENT_POLICY = "btc-parent-close-reversal-200bps-v1"
FEATURE_PREFIX = "prospective.liquidity."
PINNED_PROTOCOL_SHA256 = "dc0c69b9a1037e1dbb1e12c0ce7da6a278782a1a59b87bbb75dd27a19a2c621b"
SHADOW_PREFIX = "shadow.combined_liquidity."
SOURCE_FIELDS = ("schema_version", "event_kind", "event_type", "alert_time_utc", "symbol", "direction",
                 "source_side", "timeframe", "score", "current_price", "target_price", "initial_target_distance_pct",
                 "categories", "setup_key", "event_fingerprint", "strategy_version", "code_version",
                 "runtime_session_id", "capture_stage", "delivery_status", "delivery_attempted_at_utc",
                 "delivered_at_utc", "engine_snapshot")
CONTROL_FIELDS = {"runtime_session_id", "capture_stage", "delivery_status", "delivery_attempted_at_utc", "delivered_at_utc"}
EVENT_FIELDS = set(SOURCE_FIELDS) - CONTROL_FIELDS
SHADOW_CODE_FILES = {"research_combined_liquidity_shadow.py", "research_event_runtime.py", "research_event_capture.py",
                     "research_event_store.py", "research_consumed_inputs.py", "research_ordered_question_catalog.py",
                     "main.py", "analysis.py", "research_watch_decision_capture.py"}
PARENT_MARKERS = ("membership_created_at_utc", "parent_created_at_utc", "parent_confirmed_at_utc",
                  "btc_bar_created_at_utc", "btc_observed_close_utc")


class InvalidEvidence(ValueError):
    pass


def require(condition, reason):
    if not condition:
        raise InvalidEvidence(reason)


def clock(value):
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    require(parsed.tzinfo is not None, "NAIVE_CLOCK")
    return parsed.astimezone(timezone.utc)


def normalized(value):
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        require(math.isfinite(value), "NONFINITE_VALUE")
        return int(value) if value.is_integer() else value
    if isinstance(value, datetime):
        return clock(value).isoformat()
    if isinstance(value, dict):
        require(all(isinstance(key, str) for key in value), "NONSTRING_KEY")
        return {key: normalized(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [normalized(item) for item in value]
    raise InvalidEvidence("UNSUPPORTED_VALUE")


def digest(value):
    return hashlib.sha256(json.dumps(normalized(value), sort_keys=True, separators=(",", ":"),
                                    ensure_ascii=False, allow_nan=False).encode("utf-8")).hexdigest()


def protocol_digest(protocol):
    """Exact sorted compact JSON; unlike retained graphs, preserve 1.0 as 1.0."""
    return hashlib.sha256(json.dumps(protocol, sort_keys=True, separators=(",", ":"),
                                    ensure_ascii=False, allow_nan=False).encode("utf-8")).hexdigest()


def number(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def mapping(value):
    if isinstance(value, str):
        value = json.loads(value)
    require(isinstance(value, dict), "OBJECT_REQUIRED")
    return value


def _alignment(share):
    return "SUPPORTS" if share >= 60 else "OPPOSES" if share <= 40 else "BALANCED"


def _normalized_source(row):
    result = {key: row.get(key) for key in SOURCE_FIELDS}
    for key in ("categories", "engine_snapshot"):
        if isinstance(result[key], str):
            result[key] = json.loads(result[key])
    for key in ("alert_time_utc", "delivery_attempted_at_utc", "delivered_at_utc"):
        if result[key] is not None:
            result[key] = clock(result[key]).isoformat()
    return normalized(result)


def _point(status, reason, **values):
    return {"capture_status": status, "reason": reason, **values}


def _native_point(event):
    """The native liquidity subset of pinned catalog a1e92c01... exactly.

    In particular, missing target/current is allowed by the original mapping
    rule. Combined's stricter target/source closure is not imposed here.
    """
    snapshot = event.get("engine_snapshot")
    snapshot = snapshot if isinstance(snapshot, dict) else {}
    direction = event.get("direction")
    side = snapshot.get("alert_side") or event.get("source_side")
    expected = {"SHORT": "LONG", "LONG": "SHORT"}.get(side)
    current, target = number(event.get("current_price")), number(event.get("target_price"))
    target_direction = ("LONG" if target > current else "SHORT" if target < current else "NEUTRAL") if current and target else None
    verified = direction in ("LONG", "SHORT") and expected == direction and target_direction in (None, direction) and not snapshot.get("calculation_validation_errors")
    if not verified:
        return _point("UNVERIFIED", "NATIVE_DIRECTION_MAPPING_INVALID")
    share, near, far = (number(snapshot.get(key)) for key in ("near_share_pct", "near_amount", "far_amount"))
    valid = near is not None and far is not None and near >= 0 and far >= 0 and near + far > 0
    if share is None or not 0 <= share <= 100 or not valid or not math.isclose(share, 100 * near / (near + far), abs_tol=0.05):
        return _point("MISSING" if share is None and near is None and far is None else "UNVERIFIED", "NATIVE_AMOUNTS_NOT_VALID")
    return _point("VALID", "NATIVE_ROOT_AMOUNTS", source_contract=NATIVE_CONTRACT,
                  selected_amount=near, opposite_amount=far, selected_share_pct=share,
                  computed_share_pct=100 * near / (near + far), alignment=_alignment(share))


def _objects(observation):
    objects = observation["objects"]
    require(isinstance(objects, dict) and 0 < len(objects) <= 8192, "SHADOW_OBJECT_COUNT")
    require(sum(value["raw_bytes"] for value in objects.values()) <= 8 * 1024 * 1024, "SHADOW_RAW_BOUND")
    decoded, total_packed = {}, 0
    for sha, row in objects.items():
        require(row["codec"] == "zlib-json-v1" and type(row["raw_bytes"]) is int and 0 < row["raw_bytes"] <= 512 * 1024, "SHADOW_OBJECT_BOUND")
        packed = base64.b64decode(row["compressed_b64"], validate=True)
        total_packed += len(packed)
        require(total_packed <= 2 * 1024 * 1024, "SHADOW_COMPRESSED_BOUND")
        decoder = zlib.decompressobj()
        raw = decoder.decompress(packed, row["raw_bytes"] + 1)
        require(decoder.eof and not decoder.unused_data and not decoder.unconsumed_tail and len(raw) == row["raw_bytes"], "SHADOW_COMPRESSED_LENGTH")
        require(hashlib.sha256(raw).hexdigest() == sha, "SHADOW_OBJECT_HASH")
        node = json.loads(raw)
        require(isinstance(node, dict) and digest(node) == sha, "SHADOW_CANONICAL_OBJECT_HASH")
        decoded[sha] = node
    used = set()
    def recover(sha, ancestors=()):
        require(len(ancestors) < 64 and sha not in ancestors and sha in decoded, "SHADOW_GRAPH_REFERENCE")
        used.add(sha)
        node = decoded[sha]
        if node.get("node") == "value":
            require(set(node) == {"node", "value"}, "SHADOW_VALUE_SCHEMA")
            return node["value"]
        require(set(node) == {"node", "items"} and isinstance(node["items"], list), "SHADOW_GRAPH_SCHEMA")
        if node["node"] == "dict":
            pairs = node["items"]
            require(all(isinstance(pair, list) and len(pair) == 2 and isinstance(pair[0], str) for pair in pairs), "SHADOW_DICT_SCHEMA")
            require(len({pair[0] for pair in pairs}) == len(pairs), "SHADOW_DUPLICATE_DICT_KEY")
            return {key: recover(child, ancestors + (sha,)) for key, child in pairs}
        require(node["node"] == "list", "SHADOW_NODE_KIND")
        result = []
        for child in node["items"]:
            segment = recover(child, ancestors + (sha,))
            require(isinstance(segment, list), "SHADOW_LIST_SEGMENT")
            result.extend(segment)
        return result
    payloads = {}
    for header in observation["manifest"]["events"]:
        require(header["kind"] not in payloads, "SHADOW_DUPLICATE_EVENT_KIND")
        payloads[header["kind"]] = recover(header["object_sha256"])
    require(used == set(objects), "SHADOW_ORPHAN_OBJECT")
    require(set(payloads) == {"producer_input", "derived_context", "identity"}, "SHADOW_INPUT_CLOSURE")
    return payloads


def _legacy_binding(source, name):
    text = source["legacy_" + name + "_canonical"]
    value = json.loads(text)
    require(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False) == text, "SHADOW_LEGACY_CANONICAL")
    seen = set()
    for bridge in source["legacy_" + name + "_datetime_paths"]:
        path = tuple(bridge["path"])
        require(path and path not in seen, "SHADOW_DATETIME_PATH")
        seen.add(path)
        parent = value
        for key in path[:-1]:
            parent = parent[key]
        key = path[-1]
        require(parent[key] == bridge["source_text"] and clock(bridge["source_text"]).isoformat() == bridge["normalized_text"], "SHADOW_DATETIME_BRIDGE")
        parent[key] = bridge["normalized_text"]
    require(normalized(value) == source[name], "SHADOW_LEGACY_VALUE")
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _combined_point(event, observation):
    manifest = observation["manifest"]
    require(manifest["version"] == SHADOW_VERSION and digest(manifest) == observation["root_sha256"], "SHADOW_MANIFEST_IDENTITY")
    require(manifest["observation_id"] == observation["observation_id"] and manifest["contract_sha256"] == SHADOW_CONTRACT_SHA256 == digest(manifest["contract"]), "SHADOW_CONTRACT_IDENTITY")
    require(manifest["package_version"] == "consumed-sql-inputs-v1" and manifest["hash_version"] == "json-integer-float-zero-normalized-v1", "SHADOW_CODEC_VERSION")
    require(manifest["capture_status"] == "CAPTURED" and manifest["errors"] == [] and manifest["source_writer_queue_accepted"] is True, "SHADOW_CAPTURE_NOT_COMPLETE")
    require(sorted(observation["objects"]) == manifest["object_sha256s"], "SHADOW_OBJECT_SET")
    for key in ("original_first_match_input_proven", "original_event_insert_proven", "source_commit_visibility_time_proven", "enrolled_in_existing_study", "modifies_source_event_or_features"):
        require(manifest.get(key) is False, "SHADOW_PROOF_SCOPE")
    bodies = _objects(observation)
    source, retained = bodies["producer_input"], bodies["derived_context"]
    captured = source["event"]
    require(set(captured) == EVENT_FIELDS, "SHADOW_FULL_EVENT_SCHEMA")
    serialized = _normalized_source(source["serialized_event"])
    require(serialized == source["source_material"] == _normalized_source(event), "SHADOW_CURRENT_SOURCE_BINDING")
    require({key: _normalized_source(captured)[key] for key in EVENT_FIELDS} == {key: serialized[key] for key in EVENT_FIELDS}, "SHADOW_ORIGINAL_EVENT_BINDING")
    require(digest(serialized) == manifest["source_material_sha256"] and captured["event_fingerprint"] == manifest["event_fingerprint"], "SHADOW_SOURCE_HASH")
    require(clock(captured["alert_time_utc"]).isoformat() == manifest["source_alert_time_utc"] and clock(manifest["source_alert_time_utc"]) >= clock(manifest["process_started_at_utc"]), "SHADOW_SOURCE_CLOCK")
    require(clock(manifest["producer_captured_at_utc"]) >= clock(manifest["process_started_at_utc"]), "SHADOW_CAPTURE_CLOCK")
    for name, sha_name in (("event", "original_event_canonical_sha256"), ("selected_top_item", "selected_top_item_canonical_sha256"), ("serialized_event", "serialized_event_canonical_sha256")):
        require(_legacy_binding(source, name) == manifest[sha_name], "SHADOW_LEGACY_HASH")
    item, snapshot = source["selected_top_item"], mapping(captured["engine_snapshot"])
    symbol, frame, side, direction = captured["symbol"], captured["timeframe"], captured["source_side"], captured["direction"]
    require(symbol == "DOGE" and captured["event_type"] == "COMBINED_CONFIRMATION" and captured["event_kind"] == "ALERT", "SHADOW_SOURCE_SCOPE")
    require(direction == {"SHORT": "LONG", "LONG": "SHORT"}.get(side) and snapshot.get("alert_side") == side, "SHADOW_DIRECTION_MAPPING")
    require((item.get("symbol"), item.get("timeframe"), item.get("side")) == (symbol, frame, side), "SHADOW_TOP_ITEM_IDENTITY")
    price, target, score = (number(captured.get(key)) for key in ("current_price", "target_price", "score"))
    require(price is not None and target is not None and score is not None and price > 0 and target > 0 and ((target > price) if direction == "LONG" else (target < price)), "SHADOW_TARGET_DIRECTION")
    require(all(number(item.get(key)) == number(captured.get(key)) for key in ("current_price", "target_price", "score")), "SHADOW_TOP_VALUE_BINDING")
    selected = []
    for entries in (item.get("maxpain_timeframes"), snapshot.get("maxpain_timeframes")):
        require(isinstance(entries, list), "SHADOW_SELECTED_ARRAY_MISSING")
        matches = [row for row in entries if isinstance(row, dict) and row.get("timeframe") == frame and row.get("source_side") == side]
        require(len(matches) == 1, "SHADOW_SELECTED_SLOT_NOT_UNIQUE")
        selected.append(matches[0])
    raw, compact = selected
    for key in ("score", "target_price", "near_amount", "far_amount", "near_share_pct"):
        require(number(raw.get(key)) is not None and number(raw.get(key)) == number(compact.get(key)), "SHADOW_RAW_COMPACT_VALUE")
    require(number(raw["score"]) == score and number(raw["target_price"]) == target, "SHADOW_SELECTED_VALUE_BINDING")
    for key in ("near_amount", "far_amount", "near_share_pct"):
        require(key not in item or number(item[key]) == number(raw[key]), "SHADOW_ROOT_SLOT_VALUE")
    near, far, reported = (number(raw[key]) for key in ("near_amount", "far_amount", "near_share_pct"))
    require(near >= 0 and far >= 0 and near + far > 0 and 0 <= reported <= 100, "SHADOW_AMOUNT_DOMAIN")
    share = 100 * near / (near + far)
    require(math.isfinite(share) and abs(share - reported) <= 0.05, "SHADOW_SHARE_MISMATCH")
    expected = {SHADOW_PREFIX + "capture_status": "VALID", SHADOW_PREFIX + "reason": "EXACT_SELECTED_SLOT",
                "enrolled_in_existing_study": False, "modifies_source_event_or_features": False,
                SHADOW_PREFIX + "selected_item_id": symbol + "|" + frame + "|" + side,
                SHADOW_PREFIX + "selected_amount": near, SHADOW_PREFIX + "opposite_amount": far,
                SHADOW_PREFIX + "selected_share_pct": share, SHADOW_PREFIX + "reported_share_pct": reported,
                SHADOW_PREFIX + "alignment": _alignment(share), SHADOW_PREFIX + "source_side": side,
                SHADOW_PREFIX + "analysis_direction": direction, SHADOW_PREFIX + "current_price": price,
                SHADOW_PREFIX + "target_price": target, SHADOW_PREFIX + "raw_selected_slot_sha256": digest(raw),
                SHADOW_PREFIX + "compact_selected_slot_sha256": digest(compact)}
    require(normalized(expected) == retained, "SHADOW_DERIVED_CONTEXT")
    identity = bodies["identity"]
    require(identity["runtime"]["render_git_commit"] == event["code_version"] and identity["runtime"]["python_implementation"] == "CPython" and bool(identity["runtime"].get("python_version")), "SHADOW_RUNTIME_IDENTITY")
    require(isinstance(identity["code_versions"], dict) and set(identity["code_versions"]) == SHADOW_CODE_FILES and all(isinstance(value, str) and len(value) == 64 and all(char in "0123456789abcdef" for char in value) for value in identity["code_versions"].values()), "SHADOW_CODE_IDENTITY_SHAPE")
    links = observation.get("link_receipts", [])
    require(isinstance(links, list) and links, "SHADOW_SOURCE_LINK_MISSING")
    positive = 0
    for link in links:
        require(link["observation_id"] == observation["observation_id"] and link["observation_root_sha256"] == observation["root_sha256"] and link["source_material_sha256"] == manifest["source_material_sha256"], "SHADOW_LINK_BINDING")
        require(link.get("original_event_insert_proven") is False and link.get("source_commit_visibility_time_proven") is False, "SHADOW_LINK_PROOF_SCOPE")
        if link["link_status"] != "SOURCE_LINKED":
            continue
        encoded = link["observed_source_row_canonical"]
        linked = json.loads(encoded)
        require(hashlib.sha256(encoded.encode("utf-8")).hexdigest() == link["observed_source_row_canonical_sha256"], "SHADOW_LINK_ROW_HASH")
        require(type(link["event_id"]) is int and link["event_id"] > 0 and link["event_id"] == event["event_id"] == linked["event_id"], "SHADOW_LINK_EVENT_ID")
        require(_normalized_source(linked) == link["observed_source_material"] == serialized and link["observed_source_material_sha256"] == digest(serialized), "SHADOW_LINK_SOURCE_MATERIAL")
        positive += 1
    require(positive == 1, "SHADOW_POSITIVE_SOURCE_LINK_NOT_UNIQUE")
    return _point("VALID", "VERIFIED_COMBINED_SELECTED_SLOT", source_contract=SHADOW_VERSION,
                  selected_amount=near, opposite_amount=far, selected_share_pct=share,
                  reported_share_pct=reported, alignment=_alignment(share), observation_id=observation["observation_id"],
                  observation_root_sha256=observation["root_sha256"])


def extract_point(envelope):
    """Check declared source material without writing old source/features."""
    try:
        event = mapping(envelope["event"])
        require(type(event.get("event_id")) is int and event["event_id"] > 0, "SOURCE_EVENT_ID")
        require(isinstance(event.get("event_fingerprint"), str) and bool(event["event_fingerprint"].strip()), "SOURCE_FINGERPRINT_REQUIRED")
        if event.get("symbol") != "DOGE" or event.get("event_kind") != "ALERT":
            return _point("NOT_APPLICABLE", "OUTSIDE_DELIVERED_DOGE_ALERT_SCOPE")
        if event.get("delivery_status") != "DELIVERED":
            return _point("UNVERIFIED", "SOURCE_NOT_DELIVERED")
        kind = str(event.get("event_type") or "")
        if kind == "COMBINED_CONFIRMATION":
            if not envelope.get("combined_observation"):
                return _point("MISSING", "COMBINED_OBSERVATION_MISSING")
            return _combined_point(event, envelope["combined_observation"])
        if "MAX_PAIN" in kind:
            return _native_point(event)
        return _point("NOT_APPLICABLE", "SOURCE_FAMILY_NOT_ALLOWED")
    except (InvalidEvidence, KeyError, TypeError, ValueError, OverflowError, zlib.error) as exc:
        return _point("UNVERIFIED", str(exc) if isinstance(exc, InvalidEvidence) else "MALFORMED_SOURCE_EVIDENCE")


def _receipt(envelope):
    receipt = envelope["reader_receipt"]
    require(isinstance(receipt, dict) and isinstance(receipt.get("receipt_id"), str) and bool(receipt["receipt_id"].strip()), "READER_RECEIPT_ID")
    started, received = clock(receipt["request_started_utc"]), clock(receipt["response_received_utc"])
    require(started <= received, "READER_CLOCK_ORDER")
    event = envelope["event"]
    for name in ("alert_time_utc", "delivered_at_utc", "created_at_utc"):
        require(clock(event[name]) <= received, "FUTURE_SOURCE_MARKER")
    if event.get("delivery_attempted_at_utc") is not None:
        require(clock(event["delivery_attempted_at_utc"]) <= received, "FUTURE_SOURCE_MARKER")
    require(type(event.get("event_id")) is int and event["event_id"] > 0, "SOURCE_EVENT_ID")
    require(isinstance(event.get("event_fingerprint"), str) and bool(event["event_fingerprint"].strip()), "SOURCE_FINGERPRINT_REQUIRED")
    observation = envelope.get("combined_observation")
    if event.get("event_type") == "COMBINED_CONFIRMATION" and observation is not None:
        manifest = observation["manifest"]
        for name in ("source_alert_time_utc", "producer_captured_at_utc", "process_started_at_utc"):
            require(clock(manifest[name]) <= received, "FUTURE_COMBINED_MARKER")
        for header in manifest["events"]:
            require(clock(header["recorded_at_utc"]) <= received, "FUTURE_COMBINED_MARKER")
        for name in ("database_created_at_utc", "created_at_utc"):
            if observation.get(name) is not None:
                require(clock(observation[name]) <= received, "FUTURE_COMBINED_MARKER")
        for link in observation.get("link_receipts", []):
            require(clock(link["observed_at_utc"]) <= received, "FUTURE_COMBINED_LINK_MARKER")
            for name in ("database_created_at_utc", "created_at_utc"):
                if link.get(name) is not None:
                    require(clock(link[name]) <= received, "FUTURE_COMBINED_LINK_MARKER")
    return receipt, received


def _parent(parent, event, epoch, received):
    require(isinstance(parent, dict), "PARENT_EVIDENCE_MISSING")
    require(type(parent.get("event_id")) is int and parent["event_id"] == event["event_id"] and parent.get("episode_policy_version") == PARENT_POLICY, "PARENT_SOURCE_IDENTITY")
    require(parent.get("membership_status") == "LIVE" and parent.get("parent_evidence_eligible") is True and isinstance(parent.get("btc_parent_movement_id"), str) and bool(parent["btc_parent_movement_id"]), "PARENT_MEMBERSHIP")
    started, confirmed, alert = clock(parent["parent_start_time_utc"]), clock(parent["parent_confirmed_at_utc"]), clock(event["alert_time_utc"])
    require(started > epoch, "OLD_PARENT")
    require(started <= confirmed <= alert, "PARENT_SOURCE_CAUSALITY")
    for name in PARENT_MARKERS:
        require(clock(parent[name]) <= received, "FUTURE_PARENT_MARKER")
    age = (alert - clock(parent["btc_observed_close_utc"])).total_seconds()
    require(0 <= age < 60, "BTC_OBSERVED_BAR_NOT_CAUSAL")


def evaluate(current, history, activation=None, parent=None, *, protocol=None):
    """Conformance only: first eligible reader receipt remains caller-declared.

    The supplied history is not asserted complete by this offline function.
    A future observer must retain its actual query/response and freeze each
    first full eligibility receipt and parent winner transactionally.
    """
    result = {"status": "CONFORMANCE_NO_MATCH", "reason": None, "predicate_match": False,
              "features": {}, "included_history": [], "excluded_history": [],
              "enrolled": False, "enrolled_in_existing_study": False, "observer_running": False,
              "actual_first_reader_receipt_proven": False, "history_completeness_proven": False,
              "parent_market_evidence_proven": False, "trading_performance_proven": False,
              "observer_active": False, "bootstrap_exclusion_implemented": False,
              "durable_parent_winner_implemented": False, "live_source_code_pins_validated": False}
    if activation is None:
        return {**result, "status": "WAITING_FOR_ACTIVATION", "reason": "NO_ACTIVATION_DECLARATION"}
    try:
        require(isinstance(protocol, dict) and isinstance(protocol.get("study_id"), str) and bool(protocol["study_id"]), "PROTOCOL_REQUIRED")
        require(isinstance(activation, dict), "ACTIVATION_DECLARATION_REQUIRED")
        require(protocol_digest(protocol) == PINNED_PROTOCOL_SHA256, "FROZEN_PROTOCOL_MISMATCH")
        require(activation.get("study_id") == protocol["study_id"] and activation.get("protocol_sha256") == PINNED_PROTOCOL_SHA256, "ACTIVATION_PROTOCOL_MISMATCH")
        require(isinstance(activation.get("activation_receipt_id"), str) and bool(activation["activation_receipt_id"].strip()), "ACTIVATION_RECEIPT_ID")
        epoch = clock(activation["activated_at_utc"])
        require(epoch >= clock(protocol["epoch"]["activation_not_before_utc"]), "ACTIVATION_BEFORE_PROTOCOL_FREEZE")
        receipt, received = _receipt(current)
        event = current["event"]
        when = clock(event["alert_time_utc"])
        excluded_ids = set(protocol["epoch"]["preparation_event_ids"])
        if event["event_id"] in excluded_ids:
            return {**result, "status": "EXCLUDED", "reason": "PREPARATION_EVENT_EXCLUDED"}
        require(received >= epoch, "READER_BEFORE_ACTIVATION")
        if when <= epoch:
            return {**result, "status": "EXCLUDED", "reason": "SOURCE_NOT_AFTER_EPOCH"}
        if when < epoch + timedelta(minutes=30):
            return {**result, "status": "WARMUP", "reason": "SOURCE_IN_30M_WARMUP"}
        if event.get("symbol") != "DOGE" or event.get("direction") != "SHORT":
            return {**result, "status": "EXCLUDED", "reason": "PRIMARY_DOGE_SHORT_SCOPE"}
        point = extract_point(current)
        result["point"] = point
        if point["capture_status"] != "VALID":
            return {**result, "status": "DEFERRED", "reason": point["reason"]}
        require(isinstance(history, list) and len(history) <= 8192, "BOUNDED_HISTORY_ARRAY_REQUIRED")
        ids = [row["event"]["event_id"] for row in history]
        require(len(ids) == len(set(ids)), "DUPLICATE_HISTORY_EVENT_ID")
        fingerprints = [row["event"].get("event_fingerprint") for row in history]
        present_fingerprints = [value for value in fingerprints if value is not None]
        require(len(present_fingerprints) == len(set(present_fingerprints)), "DUPLICATE_HISTORY_FINGERPRINT")
        scans = set()
        for envelope in history:
            old = envelope["event"]
            event_id = old["event_id"]
            reason = None
            if event_id == event["event_id"]:
                reason = "CURRENT_EVENT_NOT_PRIOR"
            elif event_id in excluded_ids:
                reason = "PREPARATION_EVENT_EXCLUDED"
            elif old.get("symbol") != "DOGE" or old.get("direction") != "SHORT":
                reason = "HISTORY_OTHER_SYMBOL_OR_DIRECTION"
            else:
                old_when = clock(old["alert_time_utc"])
                if old_when < epoch:
                    reason = "HISTORY_BEFORE_EPOCH"
                elif not when - timedelta(minutes=30) <= old_when < when:
                    reason = "HISTORY_OUTSIDE_HALF_OPEN_WINDOW"
                else:
                    old_receipt, old_received = _receipt(envelope)
                    if old_received > received:
                        reason = "HISTORY_NOT_YET_OBSERVED"
                    else:
                        old_point = extract_point(envelope)
                        snapshot = mapping(old.get("engine_snapshot") or {})
                        scan = snapshot.get("watch_scan_id") or snapshot.get("sheet_snapshot_id")
                        if old_point["capture_status"] != "VALID":
                            reason = "HISTORY_LIQUIDITY_NOT_VALID"
                        elif not scan:
                            reason = "HISTORY_REAL_SCAN_ID_MISSING"
                        else:
                            scan = str(scan)
                            scans.add(scan)
                            result["included_history"].append({"event_id": event_id, "scan_id": scan,
                                                               "source_contract": old_point["source_contract"],
                                                               "reader_receipt_id": old_receipt["receipt_id"]})
            if reason:
                result["excluded_history"].append({"event_id": event_id, "reason": reason})
        ordinal = len(scans) + 1
        result["features"] = {FEATURE_PREFIX + "capture_status": "VALID",
                              FEATURE_PREFIX + "source_contract": point["source_contract"],
                              FEATURE_PREFIX + "selected_share_pct": point["selected_share_pct"],
                              FEATURE_PREFIX + "alignment": point["alignment"],
                              FEATURE_PREFIX + "sequence.30m.prior_distinct_scans": len(scans),
                              FEATURE_PREFIX + "sequence.30m.entry_ordinal": ordinal}
        result["reader_receipt_id"] = receipt["receipt_id"]
        result["reader_response_received_utc"] = received.isoformat()
        result["prior_scan_ids"] = sorted(scans)
        result["current_scan_id"] = mapping(event.get("engine_snapshot") or {}).get("watch_scan_id") or mapping(event.get("engine_snapshot") or {}).get("sheet_snapshot_id")
        result["same_current_scan_prior_is_allowed"] = True
        if ordinal != 2:
            return {**result, "reason": "ENTRY_ORDINAL_NOT_TWO"}
        _parent(parent, event, epoch, received)
        planned = received.replace(second=0, microsecond=0) + timedelta(minutes=1)
        return {**result, "status": "CONFORMANCE_MATCH", "reason": "PRESPECIFIED_ENTRY_TWO",
                "predicate_match": True, "parent_id": parent["btc_parent_movement_id"],
                "declared_planned_entry_utc": planned.isoformat(),
                "declared_entry_expiry_utc": (planned + timedelta(seconds=90)).isoformat()}
    except (InvalidEvidence, KeyError, TypeError, ValueError, OverflowError) as exc:
        return {**result, "status": "UNVERIFIED", "reason": str(exc) if isinstance(exc, InvalidEvidence) else "MALFORMED_CONFORMANCE_INPUT"}
