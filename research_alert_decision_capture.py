"""Bounded, passive observations of actual native alert-decision invocations.

The collector does not evaluate alert rules. A missing output has UNKNOWN cause
unless the native call site supplies an explicit branch witness. Returned text
is hashed, never stored, and is not evidence of notification delivery. Batch
observations intentionally do not attribute outputs to their internal families.
All public recording operations fail open; ``observe_call`` preserves the exact
native result object and re-raises the original native exception.
"""
from __future__ import annotations

from contextvars import ContextVar
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re


VERSION = "alert-decision-capture-v1"
MAX_CAPSULE_BYTES = 256 * 1024
MAX_NODES = 16_384
MAX_DEPTH = 12
MAX_RECORDS = 64
MAX_STAGES = 32
MAX_VALUE_BYTES = 80 * 1024
MAX_VALUE_NODES = 4096
MAX_COLLECTION_ITEMS = 512
MAX_STRING_BYTES = 4096
MAX_OUTPUTS = 128
MAX_MESSAGE_BYTES = 64 * 1024
MISSING = object()
_ACTIVE = ContextVar("research_alert_decision_capture", default=None)
_CODE = re.compile(r"^[A-Za-z0-9_.:/+| -]{1,200}$")
_SHA = re.compile(r"^[0-9a-f]{64}$")
_SENSITIVE_KEYS = frozenset({
    "text", "message", "messages", "chat_id", "recipient", "recipients",
    "token", "attempt_token", "bot_token", "api_key", "authorization",
    "password", "secret",
})


def canonical_bytes(value):
    """Strict JSON bytes; integer and float representations are not normalized."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _digest(value):
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def _clock():
    return datetime.now(timezone.utc).isoformat()


def _code(value, fallback="UNKNOWN"):
    return value if type(value) is str and _CODE.fullmatch(value) else fallback


def _copy(value, *, max_bytes=MAX_VALUE_BYTES, max_nodes=MAX_VALUE_NODES):
    """Copy exact builtin values only, never custom repr/iteration/truthiness.

    Sets/tuples/datetimes are explicitly tagged; key absence is handled by the
    caller. Limits reject the diagnostic value rather than silently truncating it.
    """
    nodes = 0
    ancestors = set()

    def visit(item, depth):
        nonlocal nodes
        nodes += 1
        if nodes > max_nodes or depth > 12:
            raise ValueError("VALUE_BOUNDS_EXCEEDED")
        kind = type(item)
        if item is None or kind is bool:
            return item
        if kind is int:
            if item.bit_length() > 1024:
                raise ValueError("INTEGER_BOUNDS_EXCEEDED")
            return item
        if kind is float:
            if not math.isfinite(item):
                raise ValueError("NONFINITE_NUMBER")
            return item
        if kind is str:
            if len(item) > MAX_STRING_BYTES or len(item.encode("utf-8")) > MAX_STRING_BYTES:
                raise ValueError("STRING_BOUNDS_EXCEEDED")
            return item
        if kind is datetime:
            return {"captured_type": "datetime", "isoformat": item.isoformat()}
        if kind not in (dict, list, tuple, set, frozenset):
            raise ValueError("UNSUPPORTED_VALUE_TYPE")
        if len(item) > MAX_COLLECTION_ITEMS:
            raise ValueError("COLLECTION_BOUNDS_EXCEEDED")
        identity = id(item)
        if identity in ancestors:
            raise ValueError("CYCLIC_VALUE")
        ancestors.add(identity)
        try:
            if kind is dict:
                if any(type(key) is not str for key in item):
                    raise ValueError("NONSTRING_KEY")
                if any(key.lower() in _SENSITIVE_KEYS for key in item):
                    raise ValueError("SENSITIVE_FIELD_EXCLUDED")
                result = {visit(key, depth + 1): visit(item[key], depth + 1)
                          for key in sorted(item)}
            else:
                result = [visit(part, depth + 1) for part in item]
                if kind in (set, frozenset):
                    result.sort(key=canonical_bytes)
                if kind is not list:
                    result = {"captured_type": kind.__name__, "items": result}
            return result
        finally:
            ancestors.remove(identity)

    result = visit(value, 0)
    if len(canonical_bytes(result)) > max_bytes:
        raise ValueError("VALUE_BYTES_EXCEEDED")
    return result


def _unknown(reason):
    return {"status": "UNKNOWN", "reason_code": _code(reason)}


def _fail(collector, reason):
    if collector is None:
        return
    collector["partial"] = True
    reason = _code(reason)
    if reason not in collector["omissions"] and len(collector["omissions"]) < 64:
        collector["omissions"].append(reason)


def _safe_value(value, collector, label):
    try:
        result = _copy(value)
        if type(result) is dict and result.get("status") == "UNKNOWN":
            _fail(collector, label + "_UNKNOWN")
        def depth(part):
            children = list(part.values()) if type(part) is dict else part if type(part) is list else []
            return 1 + max((depth(child) for child in children), default=-1)
        # Preserve deeper bounded projections exactly without exceeding the
        # independent store's outer JSON depth limit after envelope wrapping.
        if depth(result) > 5:
            raw = canonical_bytes(result)
            return {"status": "CAPTURED", "encoding": "CANONICAL_JSON_TEXT",
                    "canonical_json": raw.decode("utf-8"),
                    "sha256": hashlib.sha256(raw).hexdigest()}
        return {"status": "CAPTURED", "value": result, "sha256": _digest(result)}
    except BaseException:
        _fail(collector, label + "_CAPTURE_FAILED")
        return _unknown(label + "_CAPTURE_FAILED")


def _safe_clock(collector):
    try:
        value = _clock()
        if type(value) is not str or len(value) > 64:
            raise ValueError("CLOCK_SHAPE_INVALID")
        return value
    except BaseException:
        _fail(collector, "CAPTURE_CLOCK_UNAVAILABLE")
        return None


def _current():
    try:
        collector = _ACTIVE.get()
        return collector if collector is not None and not collector["sealed"] else None
    except BaseException:
        return None


def begin(cycle_id, metadata=None, *, expected_stages=None):
    """Activate an isolated Watch collector and return its ContextVar token."""
    token = None
    try:
        if type(cycle_id) is not str or not cycle_id or len(cycle_id.encode("utf-8")) > 256:
            return None
        collector = {"cycle_id": cycle_id, "partial": False, "omissions": [],
                     "records": [], "stages": [], "sequence": 0, "invocations": 0,
                     "sealed": False, "capsule": None, "score_payload_sha256": None,
                     "source_code_sha256": {}, "metadata": {}}
        collector["started_at_utc"] = _safe_clock(collector)
        try:
            collector["source_code_sha256"]["research_alert_decision_capture.py"] = hashlib.sha256(
                Path(__file__).read_bytes()).hexdigest()
        except BaseException:
            _fail(collector, "COLLECTOR_SOURCE_PIN_UNAVAILABLE")
        if metadata is not None:
            value = _safe_value(metadata, collector, "CONTEXT")
            collector["metadata"] = value
        token = _ACTIVE.set(collector)
        names = (("ordinary_lifecycle_gate", "special_native_batch", "combined_native_batch")
                 if expected_stages is None else expected_stages)
        if type(names) not in (list, tuple) or len(names) > MAX_STAGES:
            _fail(collector, "EXPECTED_STAGES_INVALID")
        else:
            for name in names:
                stage(name, "NOT_REACHED")
        return token
    except BaseException:
        reset(token)
        return None


def reset(token):
    try:
        if token is not None:
            _ACTIVE.reset(token)
    except BaseException:
        pass


def gap(reason_code):
    """Expose a caller-witnessed diagnostic gap without fabricating observations."""
    try:
        _fail(_current(), reason_code)
    except BaseException:
        pass


def bind(*, score_payload_sha256=None, source_code_sha256=None, metadata=None):
    """Bind recorded producer identifiers; never recompute any market score."""
    collector = _current()
    if collector is None:
        return
    try:
        if score_payload_sha256 is not None:
            if type(score_payload_sha256) is str and _SHA.fullmatch(score_payload_sha256):
                previous = collector["score_payload_sha256"]
                if previous is not None and previous != score_payload_sha256:
                    _fail(collector, "SCORE_BINDING_CHANGED")
                else:
                    collector["score_payload_sha256"] = score_payload_sha256
            else:
                _fail(collector, "SCORE_BINDING_INVALID")
        if source_code_sha256 is not None:
            if type(source_code_sha256) is not dict or len(source_code_sha256) > 32:
                _fail(collector, "SOURCE_PINS_INVALID")
            else:
                for name, digest in source_code_sha256.items():
                    if (type(name) is str and len(name) <= 128 and type(digest) is str
                            and _SHA.fullmatch(digest)):
                        previous = collector["source_code_sha256"].get(name)
                        if previous is not None and previous != digest:
                            _fail(collector, "SOURCE_PIN_CHANGED")
                        else:
                            collector["source_code_sha256"][name] = digest
                    else:
                        _fail(collector, "SOURCE_PINS_INVALID")
        if metadata is not None:
            collector["bound_metadata"] = _safe_value(metadata, collector, "BINDING")
    except BaseException:
        _fail(collector, "BINDING_CAPTURE_FAILED")


def stage(name, status, *, reason_code=None, details=None):
    """Record caller-witnessed reachability/gates, not inferred branch reasons."""
    collector = _current()
    if collector is None:
        return
    try:
        if len(collector["stages"]) >= MAX_STAGES:
            _fail(collector, "STAGE_LIMIT_EXCEEDED")
            return
        collector["sequence"] += 1
        record = {"sequence": collector["sequence"], "name": _code(name),
                  "status": _code(status), "captured_at_utc": _safe_clock(collector),
                  "reason_code": _code(reason_code)}
        if record["name"] == "UNKNOWN" or record["status"] == "UNKNOWN":
            _fail(collector, "STAGE_LABEL_UNAVAILABLE")
        if details is not None:
            record["details"] = _safe_value(details, collector, "STAGE_DETAILS")
        collector["stages"].append(record)
    except BaseException:
        _fail(collector, "STAGE_CAPTURE_FAILED")


def _state(state, state_key, collector):
    try:
        if type(state) is not dict:
            _fail(collector, "STATE_NOT_BUILTIN_MAPPING")
            return _unknown("STATE_NOT_BUILTIN_MAPPING")
        if state_key is MISSING:
            return {"scope": "MAPPING", "presence": "PRESENT",
                    "snapshot": _safe_value(state, collector, "STATE")}
        if type(state_key) not in (str, int):
            _fail(collector, "STATE_KEY_UNSUPPORTED")
            return _unknown("STATE_KEY_UNSUPPORTED")
        key = _copy(state_key, max_bytes=1024)
        if state_key not in state:
            return {"scope": "KEY", "key": key, "presence": "ABSENT"}
        return {"scope": "KEY", "key": key, "presence": "PRESENT",
                "snapshot": _safe_value(state[state_key], collector, "STATE")}
    except BaseException:
        _fail(collector, "STATE_CAPTURE_FAILED")
        return _unknown("STATE_CAPTURE_FAILED")


def begin_decision(family, *, state=None, state_key=MISSING, source=None):
    """Snapshot the requested actual state before one native call or batch."""
    collector = _current()
    if collector is None:
        return None
    try:
        collector["sequence"] += 1
        collector["invocations"] += 1
        record = {"sequence": collector["sequence"], "family": _code(family),
                  "scope": "NATIVE_INVOCATION", "started_at_utc": _safe_clock(collector),
                  "pre_state": _state(state, state_key, collector)}
        if record["family"] == "UNKNOWN":
            _fail(collector, "FAMILY_LABEL_UNAVAILABLE")
        record["source"] = _safe_value(source, collector, "SOURCE")
        return {"collector": collector, "record": record, "state": state,
                "state_key": state_key, "ended": False}
    except BaseException:
        _fail(collector, "DECISION_BEGIN_FAILED")
        return None


def _output(result, collector):
    kind = type(result)
    if result is MISSING:
        _fail(collector, "RETURN_NOT_OBSERVED")
        return {"outcome": "UNKNOWN", "reason_code": "RETURN_NOT_OBSERVED"}
    if result is None:
        entries = []
    elif kind is str:
        entries = [result] if result else []
    elif kind in (list, tuple):
        if len(result) > MAX_OUTPUTS:
            _fail(collector, "OUTPUT_COUNT_LIMIT_EXCEEDED")
            return {"outcome": "UNKNOWN", "returned_type": kind.__name__,
                    "returned_length": len(result), "reason_code": "OUTPUT_COUNT_LIMIT_EXCEEDED"}
        entries = result
    else:
        _fail(collector, "RETURN_SHAPE_UNKNOWN")
        return {"outcome": "UNKNOWN", "returned_type": kind.__name__,
                "reason_code": "RETURN_SHAPE_UNKNOWN"}
    summaries = []
    for index, entry in enumerate(entries):
        text = entry.get("text", MISSING) if type(entry) is dict else entry
        if type(text) is not str:
            _fail(collector, "RETURN_ENTRY_UNKNOWN")
            summaries.append({"index": index, "status": "UNKNOWN"})
            continue
        if len(text) > MAX_MESSAGE_BYTES:
            _fail(collector, "OUTPUT_TEXT_LIMIT_EXCEEDED")
            summaries.append({"index": index, "status": "UNKNOWN",
                              "reason_code": "OUTPUT_TEXT_LIMIT_EXCEEDED"})
            continue
        raw = text.encode("utf-8")
        if len(raw) > MAX_MESSAGE_BYTES:
            _fail(collector, "OUTPUT_TEXT_LIMIT_EXCEEDED")
            summaries.append({"index": index, "status": "UNKNOWN",
                              "reason_code": "OUTPUT_TEXT_LIMIT_EXCEEDED"})
            continue
        summaries.append({"index": index, "status": "CAPTURED", "utf8_bytes": len(raw),
                          "text_sha256": hashlib.sha256(raw).hexdigest()})
    unknown = any(part["status"] != "CAPTURED" for part in summaries)
    return {"outcome": "UNKNOWN" if unknown else "OUTPUT" if entries else "NO_OUTPUT",
            "returned_type": "NoneType" if result is None else kind.__name__,
            "returned_length": len(entries), "outputs": summaries,
            "summary_sha256": _digest(summaries), "message_text_retained": False,
            "transport_delivery_proven": False}


def end_decision(token, *, result=MISSING, error=None, reason_code=None):
    """Observe the actual result/exception and poststate without interpreting None."""
    try:
        if type(token) is not dict or token.get("ended"):
            return
        token["ended"] = True
        collector = token["collector"]
        if collector["sealed"]:
            return
        record = token["record"]
        record["completed_at_utc"] = _safe_clock(collector)
        record["post_state"] = _state(token["state"], token["state_key"], collector)
        if error is None:
            record["result"] = _output(result, collector)
        else:
            record["result"] = {"outcome": "RAISED", "native_error_type": _code(type(error).__name__)}
        record["reason_code"] = _code(reason_code)
        record["reason_scope"] = ("NATIVE_BRANCH_WITNESS"
                                  if reason_code is not None and record["reason_code"] != "UNKNOWN"
                                  else "UNKNOWN")
        if len(collector["records"]) < MAX_RECORDS:
            collector["records"].append(record)
        else:
            _fail(collector, "RECORD_LIMIT_EXCEEDED")
    except BaseException:
        try:
            _fail(token.get("collector"), "DECISION_END_FAILED")
        except BaseException:
            pass


def observe_call(family, native, *, args=(), kwargs=None, state=None,
                 state_key=MISSING, source=None):
    """Call native exactly once; diagnostic failures cannot change its behavior."""
    token = None
    try:
        token = begin_decision(family, state=state, state_key=state_key, source=source)
    except BaseException:
        try:
            _fail(_current(), "DECISION_BEGIN_FAILED")
        except BaseException:
            pass
    try:
        result = native(*args, **({} if kwargs is None else kwargs))
    except BaseException as error:
        try:
            end_decision(token, error=error)
        except BaseException:
            pass
        raise
    try:
        end_decision(token, result=result)
    except BaseException:
        try:
            _fail(_current(), "DECISION_END_FAILED")
        except BaseException:
            pass
    return result


def _limits(value):
    nodes = 0

    def walk(item, depth):
        nonlocal nodes
        nodes += 1
        if nodes > MAX_NODES or depth > MAX_DEPTH:
            raise ValueError("CAPSULE_BOUNDS_EXCEEDED")
        if type(item) is dict:
            for key, part in item.items():
                if type(key) is not str:
                    raise ValueError("CAPSULE_NONSTRING_KEY")
                walk(part, depth + 1)
        elif type(item) is list:
            for part in item:
                walk(part, depth + 1)
        elif item is not None and type(item) not in (bool, int, float, str):
            raise ValueError("CAPSULE_NONJSON_VALUE")

    walk(value, 0)
    if len(canonical_bytes(value)) > MAX_CAPSULE_BYTES:
        raise ValueError("CAPSULE_BYTES_EXCEEDED")


def validate_capsule(capsule):
    """Portable integrity guard. Hashes prove retained bytes, not live availability."""
    _limits(capsule)
    if (type(capsule) is not dict or capsule.get("version") != VERSION
            or type(capsule.get("cycle_id")) is not str or not capsule["cycle_id"]
            or capsule.get("status") not in {"COMPLETE", "PARTIAL", "UNAVAILABLE"}
            or type(capsule.get("payload")) is not dict):
        raise ValueError("CAPSULE_SCHEMA_INVALID")
    unsigned = {key: value for key, value in capsule.items() if key != "payload_sha256"}
    if capsule.get("payload_sha256") != _digest(unsigned):
        raise ValueError("CAPSULE_HASH_INVALID")
    payload = capsule["payload"]
    wrappers = [payload.get("context"), payload.get("binding")]
    for part in payload.get("stages", []):
        if type(part) is not dict:
            raise ValueError("CAPSULE_STAGE_INVALID")
        wrappers.append(part.get("details"))
    records = payload.get("records", [])
    if type(records) is not list or len(records) > MAX_RECORDS:
        raise ValueError("CAPSULE_RECORDS_INVALID")
    for part in records:
        if type(part) is not dict:
            raise ValueError("CAPSULE_RECORD_INVALID")
        wrappers.append(part.get("source"))
        for name in ("pre_state", "post_state"):
            state = part.get(name)
            if type(state) is dict:
                wrappers.append(state.get("snapshot"))
    for wrapper in wrappers:
        if wrapper is None or wrapper == {}:
            continue
        if type(wrapper) is not dict or wrapper.get("status") not in {"CAPTURED", "UNKNOWN"}:
            raise ValueError("CAPSULE_VALUE_WRAPPER_INVALID")
        if wrapper["status"] == "UNKNOWN":
            if capsule["status"] == "COMPLETE":
                raise ValueError("CAPSULE_FALSE_COMPLETE_UNKNOWN_VALUE")
            continue
        if wrapper.get("encoding") == "CANONICAL_JSON_TEXT":
            text = wrapper.get("canonical_json")
            if type(text) is not str or len(text.encode("utf-8")) > MAX_VALUE_BYTES:
                raise ValueError("CAPSULE_VALUE_TEXT_BOUNDS")
            value = json.loads(text)
            _copy(value)
            raw = canonical_bytes(value)
            if raw.decode("utf-8") != text or len(raw) > MAX_VALUE_BYTES:
                raise ValueError("CAPSULE_VALUE_TEXT_NONCANONICAL")
        elif "value" in wrapper and "encoding" not in wrapper:
            raw = canonical_bytes(wrapper["value"])
        else:
            raise ValueError("CAPSULE_VALUE_ENCODING_INVALID")
        if wrapper.get("sha256") != hashlib.sha256(raw).hexdigest():
            raise ValueError("CAPSULE_VALUE_HASH_INVALID")
    return True


def seal():
    """Return an independent bounded capsule, or None outside an active context."""
    try:
        collector = _ACTIVE.get()
        if collector is None:
            return None
        if collector["sealed"]:
            return json.loads(canonical_bytes(collector["capsule"]))
        if collector["score_payload_sha256"] is None:
            _fail(collector, "SCORE_BINDING_UNAVAILABLE")
        if collector["invocations"] != len(collector["records"]):
            _fail(collector, "UNFINISHED_OR_OMITTED_NATIVE_INVOCATIONS")
        records = sorted(collector["records"], key=lambda part: part["sequence"])
        payload = {
            "started_at_utc": collector["started_at_utc"],
            "sealed_at_utc": _safe_clock(collector),
            "clock_semantics": "PROCESS_CAPTURE_CLOCKS_NOT_DATABASE_AVAILABILITY_OR_PROVIDER_RECEIPT",
            "score_payload_sha256": collector["score_payload_sha256"],
            "source_code_sha256": dict(sorted(collector["source_code_sha256"].items())),
            "context": collector["metadata"],
            "binding": collector.get("bound_metadata"),
            "stages": collector["stages"], "records": records,
            "stage_status": {part["name"]: part["status"] for part in collector["stages"]},
            "native_invocation_count": collector["invocations"],
            "record_count": len(records),
            "omitted_record_count": collector["invocations"] - len(records),
            "omissions": sorted(collector["omissions"]),
            "scope": {"actual_invocations_only": True,
                      "batch_outputs_attributed_to_internal_families": False,
                      "unwitnessed_suppression_reasons": "UNKNOWN",
                      "notification_delivery_proven": False,
                      "provider_execution_or_freshness_proven": False,
                      "magnet_status_is_transport_suppression_gate": False},
        }
        while True:
            payload["record_count"] = len(records)
            payload["omitted_record_count"] = collector["invocations"] - len(records)
            capsule = {"version": VERSION, "cycle_id": collector["cycle_id"],
                       "status": "PARTIAL" if collector["partial"] else "COMPLETE", "payload": payload}
            capsule["payload_sha256"] = _digest(capsule)
            try:
                validate_capsule(capsule)
                break
            except BaseException:
                if records:
                    records.pop()
                    _fail(collector, "CAPSULE_BUDGET_EXCEEDED")
                    payload["omissions"] = sorted(collector["omissions"])
                    continue
                # Preserve cycle identity and visible failure if even context/stages overflow.
                capsule = {"version": VERSION, "cycle_id": collector["cycle_id"],
                           "status": "UNAVAILABLE", "payload": {
                               "source_code_sha256": collector["source_code_sha256"],
                               "score_payload_sha256": collector["score_payload_sha256"],
                               "reason_code": "CAPSULE_BUDGET_EXCEEDED",
                               "native_invocation_count": collector["invocations"],
                               "omitted_record_count": collector["invocations"]}}
                capsule["payload_sha256"] = _digest(capsule)
                validate_capsule(capsule)
                break
        collector["sealed"] = True
        collector["capsule"] = capsule
        return json.loads(canonical_bytes(capsule))
    except BaseException:
        return None


def source_projection(item, *, module_key=None, family_key=None):
    """Select native source fields; preserve absent/zero/fallback distinctions.

    Caller batch projections may instead pass their exact bounded consumed-field
    projection to begin_decision. No scoring, threshold, or model function runs.
    """
    try:
        if type(item) is not dict:
            return _unknown("SOURCE_NOT_BUILTIN_MAPPING")
        fields = ("symbol", "timeframe", "side", "key", "source_id", "item_id",
                  "score", "priority", "target_price", "distance_pct")
        values = {name: {"presence": "PRESENT", "value": _copy(item[name])}
                  if name in item else {"presence": "ABSENT"} for name in fields}
        selected = "score" if "score" in item else "priority" if "priority" in item else "DEFAULT_ZERO"
        evidence = item.get("market_evidence")
        evidence = evidence if type(evidence) is dict else {}
        modules = evidence.get("modules")
        modules = modules if type(modules) is dict else {}
        selected_modules = {}
        keys = (module_key,) if module_key else ("positioning", "futures_flow", "spot_flow")
        for name in keys:
            model = modules.get(name)
            if type(model) is not dict:
                selected_modules[name] = {"presence": "ABSENT" if name not in modules else "PRESENT",
                                          "value": _copy(model)}
                continue
            selected_fields = ("score", "quality", "available", "direction", "relation",
                               "early_shift", "freshness_status")
            projection = {field: model[field] for field in selected_fields if field in model}
            families = model.get("time_families")
            if type(families) is dict:
                selected_families = (family_key,) if family_key else ("now", "short", "medium", "long")
                projection["time_families"] = {
                    family: {key: families[family][key] for key in ("quality", "direction", "windows")
                             if key in families[family]}
                    for family in selected_families if type(families.get(family)) is dict}
            selected_modules[name] = projection
        model_projection = _copy(selected_modules)
        confirmation = {key: item["maxpain_confirmation"][key] for key in ("status",)
                        if type(item.get("maxpain_confirmation")) is dict and key in item["maxpain_confirmation"]}
        evidence_confirmation = {key: evidence["confirmation"][key] for key in ("status",)
                                 if type(evidence.get("confirmation")) is dict and key in evidence["confirmation"]}
        return _copy({"fields": values, "native_score_selected_field": selected,
                      "score_expression_scope": "SCORE_OR_PRIORITY_FALSY_ZERO_EXPRESSION_ONLY",
                      "native_score_uses_falsy_zero_fallback": True,
                      "raw_market_evidence_presence": "PRESENT" if "market_evidence" in item else "ABSENT",
                      "raw_market_evidence_fallback_value": (None if type(item.get("market_evidence")) is dict
                                                             else _copy(item.get("market_evidence"))),
                      "raw_maxpain_confirmation_presence": "PRESENT" if "maxpain_confirmation" in item else "ABSENT",
                      "raw_maxpain_confirmation_fallback_value": (None if type(item.get("maxpain_confirmation")) is dict
                                                                  else _copy(item.get("maxpain_confirmation"))),
                      "maxpain_confirmation": confirmation,
                      "market_evidence_confirmation": evidence_confirmation,
                      "model_evidence": model_projection,
                      "model_evidence_sha256": _digest(model_projection)})
    except BaseException:
        return _unknown("SOURCE_PROJECTION_FAILED")
