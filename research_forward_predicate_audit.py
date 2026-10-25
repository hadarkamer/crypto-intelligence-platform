"""Independent predicate replay on retained first-positive match feature maps.

The application's insert-on-conflict-ignore path retains these maps. This audit
does not prove historical database immutability, reconstruct raw market inputs,
or recover negative/population screens or the original prior scan population.
"""
from __future__ import annotations
import hashlib
import json
import math
from collections import Counter
from datetime import datetime, timedelta, timezone

PREDICATE_AUDIT_VERSION = "forward-predicate-audit-v1"
RETAINED_RECORD_DIGEST_VERSION = "retained-match-record-v1"
_PREFIXES = ("event.", "time.", "historical.", "max_pain.", "captured.", "latest.",
             "raw.", "aligned.", "aligned_log.", "model.", "category.", "sequence.",
             "aligned_sequence.", "price_oi.", "futures_cvd.", "spot_cvd.", "liquidity.")
_LABELS = ("outcome", "first_touch", "mfe", "mae", "future", "btc_parent", "label")


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _hash(value):
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _object(value):
    return json.loads(value) if isinstance(value, str) else value


def _time(value):
    if not isinstance(value, str):
        raise ValueError("Explicit UTC string required")
    when = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if when.tzinfo is None or when.utcoffset() != timedelta(0):
        raise ValueError("Explicit UTC required")
    return when.astimezone(timezone.utc)


def _number(value):
    """Match the frozen scalar conversion, including numeric-string support."""
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (ValueError, TypeError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def evaluate_condition(actual, operator, expected, *, present=True):
    """Independent implementation of the original JSON scalar comparisons."""
    if not present:
        return False
    if operator == "==":
        if isinstance(actual, bool) or isinstance(expected, bool):
            return type(actual) is bool and type(expected) is bool and actual is expected
        passed = type(actual) is type(expected) and actual == expected
        if type(actual) in (int, float) and type(expected) in (int, float):
            passed = actual == expected and _number(actual) is not None
        return bool(passed)
    left, right = _number(actual), _number(expected)
    if left is None or right is None:
        return False
    if operator == ">=": return left >= right
    if operator == "<=": return left <= right
    if operator == ">": return left > right
    if operator == "<": return left < right
    raise ValueError("Unsupported comparator")


def _conditions(candidate):
    values = candidate.get("conditions")
    if not isinstance(values, (list, tuple)) or not values:
        raise ValueError("Frozen nonempty conditions required")
    for item in values:
        if not isinstance(item, dict):
            raise ValueError("Condition object required")
        feature = str(item.get("feature") or "")
        if (not feature.startswith(_PREFIXES)
                or any(token in feature.lower().replace("futures", "") for token in _LABELS)
                or item.get("operator") not in {">=", "<=", "==", ">", "<"}
                or isinstance(item.get("value"), (dict, list, tuple))
                or item.get("value") is None):
            raise ValueError("Invalid decision-time condition")
    return values


def retained_record_digest(match, definition):
    """Stable prospective pin; reader clocks/cutoffs/presence flags are excluded."""
    fields = ("scope_key", "candidate_key", "event_id", "symbol", "direction", "snapshot_id", "entry_price",
              "source_direction", "source_symbol")
    record = {key: match[key] for key in fields}
    for key in ("alert_time_utc", "created_at_utc", "source_alert_time_utc"):
        record[key] = _time(match[key]).isoformat().replace("+00:00", "Z")
    record["decision_features"] = _object(match["decision_features"])
    frozen = {key: definition[key] for key in ("candidate_key", "formula_version", "definition_sha256")}
    frozen["definition"] = _object(definition["definition"])
    return _hash({"digest_version": RETAINED_RECORD_DIGEST_VERSION, "match": record, "definition": frozen})


def audit_predicates(snapshot, source_evidence, evidence, *, previous_assessment=None):
    """Fail closed unless every recorded known-scope positive is accounted for."""
    result = {"version": PREDICATE_AUDIT_VERSION, "status": "PENDING_PREDICATE_EVIDENCE",
              "retained_record_digest_version": RETAINED_RECORD_DIGEST_VERSION,
              "checks": [], "failures": [], "pending": [], "cases": [], "summary": {},
              "coverage": {"retained_positive_match_features": False,
                           "input_kind": "Retained first-positive match feature maps queried later",
                           "raw_market_features_reconstructed": False,
                           "full_candidate_population_recomputed": False,
                           "first_seen_feature_capture_verified": False,
                           "feature_capture_at_original_reader_receipt": False,
                           "historical_database_immutability_proven": False,
                           "unchanged_since_first_successful_v3_capture": False,
                           "original_prior_scan_population_reconstructed": False},
              "limitations": [
                  "Predicates are reevaluated on retained first-positive feature maps, not raw market observations.",
                  "Application insert-on-conflict-ignore retains maps; historical database immutability is not proven.",
                  "Prospective record pins compare against the supplied latest successful v3 capture, never a failed attempt.",
                  "Negative screens, upstream completeness and original prior scan membership are not reconstructed.",
                  "Supporting feature identities check internal consistency and add no strategy condition.",
                  "DOGE ordinal adds one to distinct prior scans and permits a later alert sharing a prior scan."]}
    if evidence is None or source_evidence is None:
        result["pending"].append({"code": "MISSING_PREDICATE_OR_SOURCE_EVIDENCE"})
        return result
    checks, failures, pending = result["checks"], result["failures"], result["pending"]
    def check(code, okay, key=None, group="evidence_integrity"):
        row = {"code": code, "passed": bool(okay), "group": group}
        if key is not None: row["event_key"] = key
        checks.append(row)
        if not okay: failures.append({k: v for k, v in row.items() if k != "passed"})
        return bool(okay)
    try:
        # Reject nonfinite values anywhere in the evidence, including unused fields.
        _json(evidence)
        policy = _object(snapshot["policy_json"])
        state = _object(snapshot["state_json"])
        cut = snapshot["journal_seq"]
        read_time = _time(evidence["read_utc"])
        check("CURRENT_SNAPSHOT_READ_TIME", read_time >= _time(snapshot["updated_at_utc"]))
        check("PREDICATE_CUTOFF", type(cut) is int and evidence["source_journal_seq"] == cut
              and source_evidence["source_journal_seq"] == cut)
        if evidence.get("extraction_complete") is not True:
            pending.append({"code": "PREDICATE_EXPORT_INCOMPLETE"})
        check("PREDICATE_POLICY_DIGEST", _hash(policy) == snapshot["policy_sha256"])
        scopes = {x["scope_key"]: x for x in policy["scopes"]}
        check("DISTINCT_SCOPE_KEYS", len(scopes) == len(policy["scopes"]))
        rejected = set()
        for row in source_evidence["decisions"]:
            if row["kind"] == "INPUT_REJECTED":
                rejected.add(_object(row["payload_json"])["input_key"])
        expected = {}
        observations = Counter()
        for packet in source_evidence["packets"]:
            payload = _object(packet["payload_json"])
            check("CANDIDATE_PACKET_DIGEST", _hash(payload) == packet["payload_sha256"])
            check("CANDIDATE_PACKET_KIND", packet["kind"] == "INPUT_CANDIDATES")
            if packet["input_key"] in rejected: continue
            for row in payload["rows"]:
                if row.get("scope_key") not in scopes: continue
                event = row.get("event_id")
                if not check("POSITIVE_EVENT_ID", type(event) is int, group="source_identity"): continue
                key = row["scope_key"] + ":" + str(event)
                expected.setdefault(key, []).append(row)
                observations[key] += 1
        keys = set(expected)
        state_keys = {key for key in state["first_seen"] if key.rsplit(":", 1)[0] in scopes}
        check("RECORDED_FIRST_SEEN_KEY_COVERAGE", keys == state_keys)
        supplied_keys = evidence["expected_keys"]
        check("EXACT_EXPECTED_POSITIVE_KEYS", isinstance(supplied_keys, list)
              and len(supplied_keys) == len(set(supplied_keys)) and set(supplied_keys) == keys)
        counts = evidence["counts"]
        check("EXPECTED_POSITIVE_COUNT", type(counts["expected_positive_keys"]) is int
              and counts["expected_positive_keys"] == len(keys))
        matches, definitions = evidence["matches"], evidence["definitions"]
        if not isinstance(matches, list) or not isinstance(definitions, list):
            raise ValueError("Evidence arrays required")
        definition_map, seen_definitions = {}, set()
        expected_candidates = {scope["candidate_key"] for scope in scopes.values()}
        for definition in definitions:
            key = definition["candidate_key"]
            check("UNIQUE_DEFINITION", key not in seen_definitions)
            seen_definitions.add(key)
            check("KNOWN_DEFINITION", key in expected_candidates)
            check("DEFINITION_PRESENT_FLAG", definition.get("definition_present") is None
                  or type(definition.get("definition_present")) is bool)
            if definition.get("definition_present") is False or definition.get("definition") is None:
                continue
            definition_map[key] = definition
            value = _object(definition["definition"])
            check("DEFINITION_CANONICAL_DIGEST", _hash(value) == definition["definition_sha256"])
            candidate = value["candidate"]
            _conditions(candidate)
            check("DEFINITION_CANDIDATE_ID", candidate["formula_id"] == key)
            check("DEFINITION_FORMULA_VERSION", value["formula_version"] == definition["formula_version"])
            check("RECOGNIZED_ORIENTATION", candidate.get("research_orientation") in {"NORMAL", "INVERSE"})
            for scope in scopes.values():
                if scope["candidate_key"] == key:
                    check("PINNED_DEFINITION_DIGEST", definition["definition_sha256"] == scope["candidate_definition_sha256"])
                    check("PINNED_FORMULA_VERSION", definition["formula_version"] == scope["formula_version"])
        for key in sorted(expected_candidates - set(definition_map)):
            pending.append({"code": "MISSING_CANDIDATE_DEFINITION", "candidate_key": key})
        check("DEFINITION_COUNT", type(counts["definition_rows"]) is int
              and counts["definition_rows"] == sum(d.get("definition_present") is not False and d.get("definition") is not None for d in definitions))
        match_map = {}
        for row in matches:
            key = row["scope_key"] + ":" + str(row["event_id"])
            check("UNIQUE_RETAINED_MATCH", key not in match_map, key)
            check("KNOWN_RETAINED_MATCH", key in keys, key)
            match_map[key] = row
        check("MATCHED_ROW_COUNT", type(counts["matched_rows"]) is int
              and counts["matched_rows"] == sum(row.get("match_present") is True for row in matches))
        for key in sorted(keys - set(match_map)):
            pending.append({"code": "MISSING_RETAINED_MATCH", "event_key": key})
        by_symbol, predicate_count, support_count = Counter(), 0, 0
        for key in sorted(keys & set(match_map)):
            row, captured_rows = match_map[key], expected[key]
            if row.get("match_present") is not True or row.get("source_present") is not True:
                pending.append({"code": "MISSING_RETAINED_MATCH_OR_SOURCE", "event_key": key})
                continue
            scope = scopes[row["scope_key"]]
            definition = definition_map.get(scope["candidate_key"])
            if definition is None: continue
            candidate = _object(definition["definition"])["candidate"]
            if row.get("decision_features") is None:
                pending.append({"code": "MISSING_RETAINED_FEATURE_MAP", "event_key": key})
                continue
            features = _object(row["decision_features"])
            if not isinstance(features, dict): raise ValueError("Retained feature map required")
            source_direction = row["source_direction"]
            orientation = candidate.get("research_orientation")
            direction = ({"LONG": "SHORT", "SHORT": "LONG"}.get(source_direction)
                         if orientation == "INVERSE" else source_direction)
            check("ORIGINAL_SOURCE_DIRECTION", source_direction in {"LONG", "SHORT"}, key, "source_identity")
            check("ORIENTATION_DIRECTION", direction == row["direction"] == scope["direction"], key, "source_identity")
            check("MATCH_CANDIDATE_AND_SYMBOL", row["candidate_key"] == scope["candidate_key"]
                  and row["symbol"] == row["source_symbol"] == scope["symbol"], key, "source_identity")
            check("MATCH_SOURCE_TIME", _time(row["alert_time_utc"]) == _time(row["source_alert_time_utc"]), key, "source_identity")
            check("MATCH_PRESENT_AT_CURRENT_READ", _time(row["created_at_utc"]) <= read_time, key, "source_identity")
            check("MATCH_SNAPSHOT_ID", isinstance(row["snapshot_id"], str) and bool(row["snapshot_id"]), key, "source_identity")
            for captured in captured_rows:
                check("CAPTURED_MATCH_METADATA", captured["candidate_key"] == row["candidate_key"]
                      and captured["symbol"] == row["symbol"] and captured["formula_direction"] == row["direction"]
                      and captured["source_direction"] == source_direction and captured["source_event_id"] == row["event_id"]
                      and captured["candidate_definition_sha256"] == definition["definition_sha256"]
                      and captured["candidate_formula_version"] == definition["formula_version"]
                      and _time(captured["alert_time_utc"]) == _time(row["source_alert_time_utc"])
                      and _time(captured["match_source_time_utc"]) == _time(row["alert_time_utc"])
                      and _time(captured["match_created_at_utc"]) == _time(row["created_at_utc"]), key, "source_identity")
                if captured.get("sheet_snapshot_id"):
                    check("CAPTURED_SNAPSHOT_ID", row["snapshot_id"] == captured["sheet_snapshot_id"], key, "source_identity")
                if captured.get("current_price") is not None:
                    check("CAPTURED_ENTRY_PRICE", _number(row["entry_price"]) is not None
                          and _number(row["entry_price"]) == _number(captured["current_price"]), key, "source_identity")
                if captured.get("event_type") is not None:
                    check("CAPTURED_EVENT_TYPE", features.get("event.event_type") == captured["event_type"], key, "supporting_feature_consistency")
            check("FEATURE_SYMBOL", features.get("event.symbol") == row["symbol"], key, "supporting_feature_consistency")
            check("FEATURE_ORIGINAL_DIRECTION", features.get("event.analysis_direction") == source_direction, key, "supporting_feature_consistency")
            check("CANDIDATE_SOURCE_DIRECTION", candidate.get("direction") in (None, source_direction), key, "predicate")
            check("CANDIDATE_SYMBOL", not candidate.get("symbol") or features.get("event.symbol") == candidate["symbol"], key, "predicate")
            conditions = []
            for condition in _conditions(candidate):
                feature = condition["feature"]
                passed = evaluate_condition(features.get(feature), condition["operator"], condition["value"], present=feature in features)
                conditions.append({"feature": feature, "operator": condition["operator"], "expected": condition["value"],
                                   "actual": features.get(feature), "present": feature in features, "passed": passed})
                check("RETAINED_CONDITION", passed, key, "predicate")
                predicate_count += 1
            # These identities validate retained extraction, not new entry filters.
            for family in ("price_oi", "futures_cvd", "spot_cvd"):
                if family + ".aligned_score" not in features: continue
                total = _number(features.get(family + ".total_score"))
                aligned = _number(features.get(family + ".aligned_score"))
                module_direction = features.get(family + ".direction")
                check("ALIGNED_SCORE_IDENTITY", total is not None and total >= 0 and aligned is not None
                      and module_direction in {"LONG", "SHORT", "NEUTRAL"}
                      and aligned == (total if module_direction == source_direction else -total), key, "supporting_feature_consistency")
                support_count += 1
            ordinal_key = "sequence.30m.liquidity.entry_ordinal"
            if ordinal_key in features:
                prior = features.get("sequence.30m.liquidity.prior_scans_same_symbol_direction")
                ordinal = features[ordinal_key]
                check("LIQUIDITY_ORDINAL_IDENTITY", type(prior) is int and prior >= 0
                      and type(ordinal) in (int, float) and not isinstance(ordinal, bool) and ordinal == prior + 1
                      and features.get("sequence.capture_status") == "READY"
                      and "LIQUIDITY" in str(features.get("sequence.current_families", "")).split("|"), key, "supporting_feature_consistency")
                near, far = _number(features.get("liquidity.selected_amount")), _number(features.get("liquidity.opposite_amount"))
                share = _number(features.get("liquidity.selected_share_pct"))
                amounts = near is not None and far is not None and near >= 0 and far >= 0 and near + far > 0
                check("LIQUIDITY_CAPTURE_IDENTITY", features.get("liquidity.capture_status") == "VALID"
                      and features.get("liquidity.source_contract") == "MAX_PAIN_CAPTURED_AMOUNTS_V1"
                      and amounts and share is not None and 0 <= share <= 100
                      and math.isclose(share, 100 * near / (near + far), abs_tol=.05)
                      and features.get("liquidity.alignment") == ("SUPPORTS" if share >= 60 else "OPPOSES" if share <= 40 else "BALANCED"), key, "supporting_feature_consistency")
                support_count += 2
            by_symbol[row["symbol"]] += 1
            result["cases"].append({"event_key": key, "symbol": row["symbol"], "candidate_key": row["candidate_key"],
                                    "source_direction": source_direction, "formula_direction": row["direction"],
                                    "research_orientation": orientation, "conditions": conditions,
                                    "retained_features_sha256": _hash(features),
                                    "retained_record_sha256": retained_record_digest(row, definition),
                                    "captured_row_observations": observations[key]})
        prior_pins, prior_usable = {}, previous_assessment is None
        if previous_assessment is not None:
            previous = previous_assessment
            prior = previous.get("predicate_audit") or {}
            prior_usable = (previous.get("status") == "PASS" and previous.get("evaluator_version") == "forward-evaluation-v3"
                            and prior.get("status") == "PASS" and prior.get("version") == PREDICATE_AUDIT_VERSION
                            and prior.get("retained_record_digest_version") == RETAINED_RECORD_DIGEST_VERSION
                            and isinstance(prior.get("cases"), list))
            if not prior_usable:
                pending.append({"code": "PREVIOUS_SUCCESSFUL_V3_PIN_REQUIRED"})
            else:
                check("PINNED_STUDY_IDENTITY", all(previous.get(field) == snapshot.get(field)
                      for field in ("study_id", "policy_sha256", "config_sha256")), group="prospective_record_pin")
                check("PINNED_SNAPSHOT_SEQUENCE_NOT_REVERSED", type(previous.get("source_journal_seq")) is int
                      and previous["source_journal_seq"] <= cut, group="prospective_record_pin")
                for case in prior["cases"]:
                    key, pin = case.get("event_key"), case.get("retained_record_sha256")
                    if not isinstance(key, str) or not isinstance(pin, str) or len(pin) != 64 or any(c not in "0123456789abcdef" for c in pin):
                        prior_usable = False
                        pending.append({"code": "PREVIOUS_RETAINED_RECORD_PIN_MISSING"})
                        continue
                    check("UNIQUE_PREVIOUS_RECORD_PIN", key not in prior_pins, key, "prospective_record_pin")
                    prior_pins[key] = pin
                check("PREVIOUS_PIN_COVERAGE", len(prior_pins) == prior.get("summary", {}).get("retained_matches_recomputed"), group="prospective_record_pin")
        current_pins = {case["event_key"]: case["retained_record_sha256"] for case in result["cases"]}
        if prior_usable:
            for key, pin in sorted(prior_pins.items()):
                check("PREVIOUS_POSITIVE_REMAINS_PRESENT", key in keys, key, "prospective_record_pin")
                if key in current_pins:
                    check("RETAINED_RECORD_UNCHANGED", current_pins[key] == pin, key, "prospective_record_pin")
                elif key in keys:
                    pending.append({"code": "PINNED_RETAINED_RECORD_UNAVAILABLE", "event_key": key})
        result["summary"] = {"expected_positive_keys": len(keys), "retained_matches_recomputed": len(result["cases"]),
                             "by_symbol": dict(sorted(by_symbol.items())), "condition_comparisons": predicate_count,
                             "supporting_feature_identity_checks": support_count,
                             "captured_positive_row_observations": sum(observations.values()),
                             "definitions_verified": len(definition_map),
                             "previous_successful_v3_capture_present": previous_assessment is not None,
                             "prior_keys_compared": len(set(prior_pins) & set(current_pins)),
                             "previous_pinned_keys": len(prior_pins),
                             "pins_established": 0,
                             "prospective_pin_state": "INITIAL_CAPTURE" if previous_assessment is None else "COMPARISON",
                             "source_journal_seq": cut,
                             "current_snapshot_read_utc": evidence["read_utc"],
                             "retained_match_evidence_sha256": _hash({"matches": matches, "definitions": definitions})}
    except (KeyError, ValueError, TypeError, ArithmeticError, AttributeError):
        check("MALFORMED_PREDICATE_EVIDENCE", False)
    result["status"] = "FAIL" if failures else "PENDING_PREDICATE_EVIDENCE" if pending else "PASS"
    result["coverage"]["retained_positive_match_features"] = result["status"] == "PASS"
    result["coverage"]["unchanged_since_first_successful_v3_capture"] = result["status"] == "PASS"
    if result["status"] == "PASS":
        result["summary"]["pins_established"] = len(set(current_pins) - set(prior_pins))
    _json(result)
    return result
