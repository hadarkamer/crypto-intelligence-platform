"""Pure descriptive audit of the frozen prospective paper study.

This module has no database, network, account, notification or order interface.
PASS means the supplied snapshot and observed closed-case quote windows agree;
it never approves a strategy or claims source-eligibility/whole-stream replay.
"""
from __future__ import annotations

import copy
import hashlib
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

EVALUATOR_VERSION = "forward-evaluation-v1"
BOOKS = ("DOGE_only", "ETH_only", "first_per_parent")
COMPLETED = {"OBSERVED_TARGET", "OBSERVED_STOP", "OBSERVED_TIMEOUT"}
CHOICE_STATES = COMPLETED | {"PLANNED", "RESERVED", "OPEN", "BLOCKED_CAPACITY", "EXPIRED_NO_ENTRY_QUOTE"}


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _sha(value):
    return hashlib.sha256(_json(value).encode("utf8")).hexdigest()


def _object(value):
    return json.loads(value) if isinstance(value, str) else value


def _num(value):
    if isinstance(value, bool):
        raise ValueError("Boolean number")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError("Invalid number") from None
    if not number.is_finite():
        raise ValueError("Nonfinite number")
    return number


def _time(value):
    if not isinstance(value, str):
        raise ValueError("UTC string required")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() != timedelta(0):
        raise ValueError("Explicit UTC required")
    return result.astimezone(timezone.utc)


def _iso(value):
    return value.isoformat().replace("+00:00", "Z")


def _near(a, b):
    return abs(_num(a) - _num(b)) <= Decimal("1e-12")


def stable_fingerprint(snapshot):
    """Ignore heartbeat and per-quote marks, retain decisions and integrity data."""
    policy = _object(snapshot.get("policy_json"))
    state = _object(snapshot.get("state_json")) or {}
    books = {}
    for name, book in (state.get("books") or {}).items():
        active = copy.deepcopy(book.get("active"))
        if isinstance(active, dict):
            active.pop("last_quote_received_utc", None)
        books[name] = {"choices": book.get("choices"), "closed": book.get("closed"),
                       "active": active, "reservation": book.get("reservation")}
    first_seen = {key: value for key, value in (state.get("first_seen") or {}).items()
                  if value.get("first_eligible_seen_utc") is not None}
    source_drift = sorted(key for key, value in (state.get("first_seen") or {}).items()
                          if value.get("last_ineligible_reason") == "POLICY_SOURCE_DRIFT")
    return _sha({"study_id": snapshot.get("study_id"), "policy_sha256": snapshot.get("policy_sha256"),
                 "config_sha256": snapshot.get("config_sha256"), "policy": policy,
                 "books": books, "first_eligible_seen": first_seen,
                 "source_drift": source_drift,
                 "last_observation_rejected": state.get("last_observation_rejected"),
                 "bootstrap_complete": state.get("bootstrap_complete"),
                 "reported_books": (snapshot.get("last_status") or {}).get("books"),
                 "reported_eligible_count": (snapshot.get("last_status") or {}).get("first_eligible_seen_count")})


class _Audit:
    def __init__(self):
        self.checks, self.failures, self.pending = [], [], []

    def check(self, code, okay, *, case_id=None, book=None):
        row = {"code": code, "passed": bool(okay)}
        if case_id is not None:
            row["case_id"] = case_id
        if book is not None:
            row["book"] = book
        self.checks.append(row)
        if not okay:
            self.failures.append({key: value for key, value in row.items() if key != "passed"})
        return bool(okay)

    def wait(self, code, case_id):
        self.pending.append({"code": code, "case_id": case_id})


def _totals(rows):
    total = sum((_num(row["net_return_pct"]) for row in rows), Decimal(0))
    return {"n": len(rows), "counts": dict(Counter(row["status"] for row in rows)),
            "mean_net_return_pct": float(total / len(rows)) if rows else None,
            "net_sum_unit_notional_pct": float(total)}


def _quote_value(quote, source):
    bid, ask = _num(quote["bid"]), _num(quote["ask"])
    if not 0 < bid <= ask:
        raise ValueError("Invalid/crossed quote")
    return {"bid": bid, "ask": ask, "mid": (bid + ask) / 2, "source": source}


def _prepare_quotes(rows, policy, audit):
    """Apply global and per-symbol rejection rules, retaining accepted symbols."""
    by_seq, inputs, valid = {}, {}, {"DOGE": [], "ETH": []}
    rejected = Counter()
    for row in rows:
        try:
            seq = row["seq"]
            if type(seq) is not int or seq < 1:
                raise ValueError("Invalid sequence")
            if seq in by_seq:
                audit.check("DUPLICATE_EXPORTED_SEQUENCE_IDENTICAL", row == by_seq[seq])
                continue
            by_seq[seq] = row
            payload = _object(row["payload_json"])
            audit.check("QUOTE_PAYLOAD_DIGEST", _sha(payload) == row["payload_sha256"])
            audit.check("QUOTE_JOURNAL_KIND", row.get("kind") == "INPUT_QUOTES")
            identity = row.get("input_key")
            if identity is not None:
                audit.check("QUOTE_INPUT_IDENTITY_UNIQUE", identity not in inputs)
                inputs[identity] = seq
        except (KeyError, ValueError, TypeError):
            audit.check("MALFORMED_QUOTE_JOURNAL_ROW", False)
    last_requests = {}
    for seq, row in sorted(by_seq.items()):
        try:
            p = _object(row["payload_json"])
            started, received = _time(p["request_started_utc"]), _time(p["response_received_utc"])
            elapsed = (received - started).total_seconds()
            maximum = _num(policy["quote_max_elapsed_seconds"])
            monotonic = p.get("monotonic_elapsed_seconds")
            if started > received or elapsed > float(maximum):
                rejected["REQUEST_CLOCK_OR_DURATION"] += 1
                continue
            if monotonic is not None and not 0 < _num(monotonic) <= maximum:
                rejected["MONOTONIC_REQUEST_DURATION"] += 1
                continue
            if p.get("source") != policy["quote_source"]:
                rejected["QUOTE_SOURCE_IDENTITY"] += 1
                continue
            accepted, seen = {}, set()
            for q in p["quotes"]:
                symbol = q.get("symbol")
                try:
                    if symbol not in valid or symbol in seen:
                        raise ValueError("Unknown/duplicate symbol")
                    seen.add(symbol)
                    quote = _quote_value(q, p["source"])
                    if symbol in last_requests and started < last_requests[symbol]:
                        raise ValueError("Reversed request")
                    accepted[symbol] = {**quote, "seq": seq, "requested": started, "received": received}
                except (ValueError, KeyError, TypeError):
                    accepted.pop(symbol, None)
                    rejected["INVALID_SYMBOL_QUOTE"] += 1
            for symbol, q in accepted.items():
                valid[symbol].append(q)
                last_requests[symbol] = started
        except (KeyError, ValueError, TypeError):
            rejected["INVALID_QUOTE_PACKET"] += 1
    ordered_receipts = []
    for _, row in sorted(by_seq.items()):
        try:
            ordered_receipts.append(_time(_object(row["payload_json"])["response_received_utc"]))
        except (KeyError, ValueError, TypeError):
            pass
    audit.check("QUOTE_PACKET_RECEIPTS_NONDECREASING", all(a <= b for a, b in zip(ordered_receipts, ordered_receipts[1:])))
    return valid, {"supplied_quote_packets": len(by_seq), "rejected_packet_or_symbol_reasons": dict(rejected)}


def _stored_quote_matches(record, observed):
    try:
        q = _quote_value(record, record["source"])
        return (q["bid"] == observed["bid"] and q["ask"] == observed["ask"]
                and q["mid"] == observed["mid"] and _num(record["midpoint"]) == observed["mid"]
                and _time(record["request_started_utc"]) == observed["requested"]
                and _time(record["response_received_utc"]) == observed["received"]
                and record["source"] == observed["source"])
    except (KeyError, ValueError, TypeError):
        return False


def _closed_path(case, quotes, policy, audit):
    case_id = case["case_id"]
    planned, expiry = _time(case["planned_entry_utc"]), _time(case["entry_expiry_utc"])
    selected = _time(case["selected_at_utc"])
    exited = _time(case["exit_received_utc"])
    context = [q for q in quotes if selected - timedelta(seconds=30) <= q["received"] <= exited + timedelta(seconds=30)]
    result = {"status": "PENDING_PRICE_DATA", "provided_valid_context_quotes": len(context),
              "stored_path_uncertain": bool(case.get("path_uncertain")),
              "stored_uncertainty_reasons": list(case.get("uncertainty_reasons") or [])}
    if not any(q["requested"] < planned for q in context):
        audit.wait("MISSING_PREPLANNED_QUOTE_CONTEXT", case_id)
        return result
    entry = next((q for q in context if q["requested"] >= planned and q["received"] <= expiry), None)
    if entry is None:
        audit.wait("MISSING_ENTRY_PRICE_WINDOW", case_id)
        return result
    audit.check("FIRST_POSTPLAN_ENTRY", entry["received"] == _time(case["entry_received_utc"])
                and entry["mid"] == _num(case["entry_midpoint"])
                and _stored_quote_matches(case["entry_quote"], entry), case_id=case_id)
    deadline = _time(case["deadline_utc"])
    sign = Decimal(1) if case["direction"] == "LONG" else Decimal(-1)
    width = _num(policy["barrier_bps"]) / 100
    first_exit = None
    for q in context:
        if q["seq"] <= entry["seq"] or q["requested"] < entry["received"]:
            continue
        gross = sign * (q["mid"] - entry["mid"]) / entry["mid"] * 100
        status = None
        if q["requested"] >= deadline:
            status = "OBSERVED_TIMEOUT"
        elif q["received"] >= deadline:
            continue
        elif gross >= width:
            status = "OBSERVED_TARGET"
        elif gross <= -width:
            status = "OBSERVED_STOP"
        if status:
            first_exit = {**q, "status": status}
            break
    if first_exit is None:
        audit.wait("MISSING_OBSERVED_EXIT_WINDOW", case_id)
        return result
    audit.check("FIRST_OBSERVED_EXIT", first_exit["received"] == exited
                and first_exit["mid"] == _num(case["exit_midpoint"])
                and first_exit["status"] == case["status"]
                and _stored_quote_matches(case["exit_quote"], first_exit), case_id=case_id)
    held = [q for q in context if entry["seq"] <= q["seq"] <= first_exit["seq"]]
    gaps = [(b["received"] - a["received"]).total_seconds() for a, b in zip(held, held[1:])]
    backwards = any(g < 0 for g in gaps)
    observed_gap = max(gaps) if gaps else None
    gap_uncertain = observed_gap is not None and observed_gap > float(policy["max_quote_gap_seconds"])
    audit.check("QUOTE_RECEIPTS_NONDECREASING", not backwards, case_id=case_id)
    if gap_uncertain:
        audit.check("OBSERVED_GAP_UNCERTAINTY_RETAINED", bool(case.get("path_uncertain")), case_id=case_id)
    result.update(status="CHECKED", entry_quote_seq=entry["seq"], exit_quote_seq=first_exit["seq"],
                  held_valid_quotes=len(held), max_valid_quote_gap_seconds=observed_gap,
                  observed_gap_uncertain=gap_uncertain)
    return result


def assess(snapshot, quote_packets, *, expected_baseline, previous_assessment=None):
    """Audit supplied evidence and return finite JSON-compatible descriptions.

    A complete quote export for each closed case is a caller responsibility.
    Bounds, context and declared gaps are checked; source candidate eligibility,
    process-restart history and unobserved intragap prices are outside this scope.
    """
    audit = _Audit()
    result = {"evaluator_version": EVALUATOR_VERSION, "status": "FAIL", "stage": "REVIEW_REQUIRED",
              "decision": "DESCRIPTIVE_ONLY", "strategy_approved": False,
              "study_id": snapshot.get("study_id"), "source_journal_seq": snapshot.get("journal_seq"),
              "source_updated_at_utc": snapshot.get("updated_at_utc"),
              "policy_sha256": snapshot.get("policy_sha256"), "config_sha256": snapshot.get("config_sha256"),
              "dataset_fingerprint": None, "counts": {}, "books": {}, "cases": [],
              "audit_scope": {"frozen_policy_identity": True, "snapshot_arithmetic": True,
                              "provided_closed_case_observed_price_paths": True,
                              "raw_source_eligibility_replayed": False, "complete_journal_replayed": False,
                              "process_restart_history_replayed": False, "actual_exchange_execution": False},
              "limitations": ["Descriptive paper results only; no strategy approval or winner is selected.",
                              "Economic cases reused across books are correlated; BTC parents are the wave unit.",
                              "Provided quote windows must be complete. Receipt/entry/exit bounds are checked.",
                              "Quote sampling does not reveal intragap first-touch or executable fills.",
                              "Stored restart/path flags are retained; restart history is not independently replayed."]}
    try:
        result["dataset_fingerprint"] = stable_fingerprint(snapshot)
        policy, state = _object(snapshot["policy_json"]), _object(snapshot["state_json"])
        audit.check("POLICY_DIGEST", _sha(policy) == snapshot["policy_sha256"])
        config = copy.deepcopy(policy)
        config.pop("study_start_utc")
        audit.check("CONFIGURATION_DIGEST", _sha(config) == snapshot["config_sha256"])
        required = ("study_id", "policy_sha256", "config_sha256", "study_start_utc")
        baseline_ok = all(key in expected_baseline for key in required)
        baseline_ok = baseline_ok and all(snapshot[key] == expected_baseline[key] for key in required[:3])
        baseline_ok = baseline_ok and policy["study_start_utc"] == expected_baseline["study_start_utc"]
        audit.check("EXPECTED_FROZEN_BASELINE", baseline_ok)
        audit.check("THREE_DECLARED_BOOKS", set(policy["books"]) == set(BOOKS) == set(state["books"]))
        scopes = {v["symbol"]: v for v in policy["scopes"]}
        audit.check("TWO_DECLARED_SYMBOLS", set(scopes) == {"DOGE", "ETH"})
        cost, width, start = _num(policy["net_cost_pp"]), _num(policy["barrier_bps"]) / 100, _time(policy["study_start_utc"])
        audit.check("NONNEGATIVE_COST_AND_POSITIVE_BARRIER", cost >= 0 and width > 0)
        audit.check("NO_REJECTED_LAST_OBSERVATION", state.get("last_observation_rejected") is None)
        audit.check("NO_RECORDED_SOURCE_IDENTITY_DRIFT", not any(v.get("last_ineligible_reason") == "POLICY_SOURCE_DRIFT" for v in state["first_seen"].values()))
        eligible = {k: v for k, v in state["first_seen"].items() if v.get("first_eligible_seen_utc") is not None}
        reported_eligible = (snapshot.get("last_status") or {}).get("first_eligible_seen_count")
        if reported_eligible is not None:
            audit.check("REPORTED_ELIGIBLE_COUNT", reported_eligible == len(eligible))
        valid_quotes, quote_diagnostics = _prepare_quotes(quote_packets, policy, audit)
        result["quote_evidence"] = quote_diagnostics
        unique, parent_choices, closed_by_parent = {}, set(), {}
        active_cases, reservations, book_rows = set(), 0, 0
        for name in BOOKS:
            book = state["books"][name]
            choices, closed = book["choices"], book["closed"]
            parents_in_closed = set()
            for parent, choice in choices.items():
                parent_choices.add(parent)
                audit.check("CHOICE_PARENT_ID", parent == choice["parent_id"], case_id=choice["case_id"], book=name)
                audit.check("CHOICE_KNOWN_STATE", choice["status"] in CHOICE_STATES, case_id=choice["case_id"], book=name)
                audit.check("CHOICE_CASE_IDENTITY", type(choice["event_id"]) is int and choice["case_id"] == str(choice["symbol"]) + "forward-" + str(choice["event_id"]) and isinstance(parent, str) and bool(parent), case_id=choice["case_id"], book=name)
                selected, planned = _time(choice["selected_at_utc"]), _time(choice["planned_entry_utc"])
                wanted = selected.replace(second=0, microsecond=0) + timedelta(minutes=1)
                audit.check("CHOICE_STRICT_NEXT_MINUTE", planned == wanted and selected >= start, case_id=choice["case_id"], book=name)
                audit.check("CHOICE_EXPIRY", _time(choice["entry_expiry_utc"]) == planned + timedelta(seconds=float(policy["entry_expiry_seconds"])), case_id=choice["case_id"], book=name)
                scope = scopes[choice["symbol"]]
                audit.check("CHOICE_SCOPE", choice["direction"] == scope["direction"] and choice["window_minutes"] == scope["window_minutes"], case_id=choice["case_id"], book=name)
                audit.check("BOOK_SYMBOL", name == "first_per_parent" or choice["symbol"] == name.split("_")[0], case_id=choice["case_id"], book=name)
                event_key = scope["scope_key"] + ":" + str(choice["event_id"])
                seen = eligible.get(event_key)
                audit.check("CHOICE_FIRST_ELIGIBLE_RECEIPT", seen is not None and _time(seen["first_eligible_seen_utc"]) == selected, case_id=choice["case_id"], book=name)
                audit.check("CHOICE_SOURCE_MARKER_NOT_FUTURE", _time(choice["source_marker_max_utc"]) <= selected, case_id=choice["case_id"], book=name)
            for case in closed:
                book_rows += 1
                case_id, parent = case["case_id"], case["parent_id"]
                audit.check("ONE_CLOSED_CASE_PER_BOOK_PARENT", parent not in parents_in_closed, case_id=case_id, book=name)
                parents_in_closed.add(parent)
                audit.check("CLOSED_STATUS", case["status"] in COMPLETED, case_id=case_id, book=name)
                choice = choices.get(parent)
                audit.check("CLOSED_MATCHES_FROZEN_CHOICE", choice is not None and all(case.get(k) == v for k, v in choice.items()), case_id=case_id, book=name)
                entry, exit_ = _num(case["entry_midpoint"]), _num(case["exit_midpoint"])
                sign = Decimal(1) if case["direction"] == "LONG" else Decimal(-1)
                audit.check("POSITIVE_MIDPOINTS_AND_DIRECTION", entry > 0 and exit_ > 0 and case["direction"] in {"LONG", "SHORT"}, case_id=case_id, book=name)
                gross = sign * (exit_ - entry) / entry * 100
                audit.check("GROSS_RETURN", _near(gross, case["gross_return_pct"]), case_id=case_id, book=name)
                audit.check("COST_CHARGED_ONCE", _num(case["modeled_cost_pp"]) == cost and _near(gross - cost, case["net_return_pct"]), case_id=case_id, book=name)
                entry_time, exit_time = _time(case["entry_received_utc"]), _time(case["exit_received_utc"])
                eq, xq = case["entry_quote"], case["exit_quote"]
                audit.check("ENTRY_REQUEST_WINDOW", _time(case["planned_entry_utc"]) <= _time(eq["request_started_utc"]) <= entry_time <= _time(case["entry_expiry_utc"]), case_id=case_id, book=name)
                audit.check("ENTRY_EXIT_ORDER", entry_time <= _time(xq["request_started_utc"]) <= exit_time and exit_time > entry_time, case_id=case_id, book=name)
                audit.check("ENTRY_EXIT_RECEIPTS", _time(eq["response_received_utc"]) == entry_time and _time(xq["response_received_utc"]) == exit_time, case_id=case_id, book=name)
                deadline = entry_time + timedelta(minutes=case["window_minutes"])
                audit.check("HOLDING_DEADLINE", _time(case["deadline_utc"]) == deadline, case_id=case_id, book=name)
                if case["status"] == "OBSERVED_TIMEOUT":
                    audit.check("TIMEOUT_REQUEST_AFTER_DEADLINE", _time(xq["request_started_utc"]) >= deadline, case_id=case_id, book=name)
                else:
                    reached = gross >= width if case["status"] == "OBSERVED_TARGET" else gross <= -width
                    audit.check("BARRIER_BEFORE_DEADLINE", reached and exit_time < deadline, case_id=case_id, book=name)
                for label, q, mid in (("ENTRY", eq, entry), ("EXIT", xq, exit_)):
                    observed = _quote_value(q, q["source"])
                    audit.check(label + "_STORED_QUOTE", observed["mid"] == mid == _num(q["midpoint"]) and q["source"] == policy["quote_source"], case_id=case_id, book=name)
                audit.check("PATH_UNCERTAINTY_FLAGS", bool(case.get("path_uncertain")) == bool(case.get("uncertainty_reasons")), case_id=case_id, book=name)
                key = (case["symbol"], case["event_id"], parent)
                if key in unique:
                    audit.check("SHARED_BOOK_CASE_IDENTICAL", unique[key] == case, case_id=case_id, book=name)
                else:
                    unique[key] = case
                closed_by_parent.setdefault(parent, set()).add(name)
            active = book.get("active")
            if active:
                active_cases.add((active["symbol"], active["event_id"], active["parent_id"]))
                audit.check("ACTIVE_CHOICE_EXISTS", active["parent_id"] in choices and choices[active["parent_id"]]["status"] == "OPEN", case_id=active["case_id"], book=name)
                active_choice = choices.get(active["parent_id"])
                audit.check("ACTIVE_MATCHES_FROZEN_CHOICE", active_choice is not None and all(active.get(k) == v for k, v in active_choice.items()), case_id=active["case_id"], book=name)
                aq = _quote_value(active["entry_quote"], active["entry_quote"]["source"])
                audit.check("ACTIVE_ENTRY_QUOTE", aq["mid"] == _num(active["entry_midpoint"]) == _num(active["entry_quote"]["midpoint"]) and active["entry_quote"]["source"] == policy["quote_source"], case_id=active["case_id"], book=name)
                audit.check("ACTIVE_ENTRY_WINDOW", _time(active["planned_entry_utc"]) <= _time(active["entry_quote"]["request_started_utc"]) <= _time(active["entry_received_utc"]) <= _time(active["entry_expiry_utc"]), case_id=active["case_id"], book=name)
                audit.check("ACTIVE_DEADLINE", _time(active["deadline_utc"]) == _time(active["entry_received_utc"]) + timedelta(minutes=active["window_minutes"]), case_id=active["case_id"], book=name)
            reservation = book.get("reservation")
            if reservation is not None:
                reservations += 1
                audit.check("RESERVATION_CHOICE_EXISTS", reservation in choices and choices[reservation]["status"] == "RESERVED", book=name)
            audit.check("ACTIVE_RESERVATION_EXCLUSIVE", not (active and reservation is not None), book=name)
            for parent, choice in choices.items():
                matches = [c for c in closed if c["parent_id"] == parent]
                expected_closed = choice["status"] in COMPLETED
                audit.check("CHOICE_CLOSED_LIFECYCLE", len(matches) == (1 if expected_closed else 0), case_id=choice["case_id"], book=name)
                audit.check("CHOICE_OPEN_LIFECYCLE", (active is not None and active["parent_id"] == parent) == (choice["status"] == "OPEN"), case_id=choice["case_id"], book=name)
                audit.check("CHOICE_RESERVED_LIFECYCLE", (reservation == parent) == (choice["status"] == "RESERVED"), case_id=choice["case_id"], book=name)
            positions = sorted(closed + ([active] if active else []), key=lambda c: _time(c["entry_received_utc"]))
            for previous, following in zip(positions, positions[1:]):
                audit.check("BOOK_POSITION_INTERVALS_AND_PLANNED_CAPACITY", previous.get("exit_received_utc") is not None and _time(following["planned_entry_utc"]) >= _time(previous["exit_received_utc"]), case_id=following["case_id"], book=name)
            totals = _totals(closed)
            reported = ((snapshot.get("last_status") or {}).get("books") or {}).get(name, {}).get("completed_all")
            if reported is not None:
                audit.check("REPORTED_BOOK_COUNT", reported["n"] == totals["n"], book=name)
                audit.check("REPORTED_BOOK_COUNTS", reported["counts"] == totals["counts"], book=name)
                audit.check("REPORTED_BOOK_SUM", _near(reported["net_sum_unit_notional_pct"], totals["net_sum_unit_notional_pct"]), book=name)
                means_match = reported["mean_net_return_pct"] is None and totals["mean_net_return_pct"] is None
                if reported["mean_net_return_pct"] is not None and totals["mean_net_return_pct"] is not None:
                    means_match = _near(reported["mean_net_return_pct"], totals["mean_net_return_pct"])
                audit.check("REPORTED_BOOK_MEAN", means_match, book=name)
            uncertain = [c for c in closed if c.get("path_uncertain")]
            result["books"][name] = {"completed_all": totals, "completed_path_uncertain": _totals(uncertain),
                                     "completed_without_uncertainty_flag": _totals([c for c in closed if not c.get("path_uncertain")]),
                                     "active": bool(active), "reserved": reservation is not None,
                                     "choice_status_counts": dict(Counter(c["status"] for c in choices.values()))}
        for parent in parent_choices:
            available = [state["books"][name]["choices"][parent] for name in ("DOGE_only", "ETH_only") if parent in state["books"][name]["choices"]]
            chosen = state["books"]["first_per_parent"]["choices"].get(parent)
            earliest = min(available, key=lambda c: (_time(c["selected_at_utc"]), _time(c["source_marker_max_utc"]), c["event_id"], c["symbol"])) if available else None
            audit.check("FIRST_PER_PARENT_EARLIEST_SNAPSHOT_CHOICE", chosen is not None and earliest is not None and all(chosen.get(k) == v for k, v in earliest.items() if k != "status"), book="first_per_parent")
        for key, case in sorted(unique.items()):
            path = _closed_path(case, valid_quotes[case["symbol"]], policy, audit)
            result["cases"].append({"case_id": case["case_id"], "symbol": case["symbol"], "event_id": case["event_id"],
                                    "parent_id": case["parent_id"], "status": case["status"],
                                    "entry_received_utc": case["entry_received_utc"], "exit_received_utc": case["exit_received_utc"],
                                    "gross_return_pct": case["gross_return_pct"], "net_return_pct": case["net_return_pct"],
                                    "path": path})
        closed_parents = set(closed_by_parent)
        result["counts"] = {"book_trade_rows": book_rows, "unique_closed_cases": len(unique),
                            "parent_wave_count": len(parent_choices), "closed_wave_count": len(closed_parents),
                            "paired_complete_parent_waves": sum(set(BOOKS) <= names for names in closed_by_parent.values()),
                            "unique_active_cases": len(active_cases), "book_reservations": reservations,
                            "first_eligible_event_records": len(eligible)}
        previous_keys = {(c.get("symbol"), c.get("event_id"), c.get("parent_id"))
                         for c in (previous_assessment or {}).get("cases", [])}
        result["changes"] = {"new_closed_case_count": len(set(unique) - previous_keys),
                             "previous_assessment_present": previous_assessment is not None}
        result["study_start_utc"] = policy["study_start_utc"]
    except (KeyError, TypeError, ValueError, ArithmeticError, AttributeError):
        audit.check("MALFORMED_OR_INCOMPLETE_EVIDENCE", False)
    result["checks"], result["failures"], result["pending"] = audit.checks, audit.failures, audit.pending
    result["status"] = "FAIL" if audit.failures else "PENDING_PRICE_DATA" if audit.pending else "PASS"
    if result["status"] == "FAIL":
        result["stage"] = "REVIEW_REQUIRED"
    elif result["status"] == "PENDING_PRICE_DATA":
        result["stage"] = "AWAITING_PRICE_EVIDENCE"
    elif result["counts"].get("unique_active_cases") or result["counts"].get("book_reservations"):
        result["stage"] = "FOLLOWING_OPEN_CASES"
    else:
        result["stage"] = "COLLECTING_NEW_WAVES" if result["counts"].get("unique_closed_cases") else "COLLECTING"
    # Enforce the public contract, including rejection of NaN/Infinity.
    _json(result)
    return result


def safe_summary(result):
    """Aggregates only: no event IDs, parent IDs, raw source rows or prices."""
    return {"evaluator_version": result.get("evaluator_version", EVALUATOR_VERSION),
            "status": result.get("status", "FAIL"), "stage": result.get("stage", "REVIEW_REQUIRED"),
            "decision": "DESCRIPTIVE_ONLY", "strategy_approved": False,
            "policy_sha256": result.get("policy_sha256"), "source_journal_seq": result.get("source_journal_seq"),
            "source_updated_at_utc": result.get("source_updated_at_utc"),
            "counts": copy.deepcopy(result.get("counts", {})), "books": copy.deepcopy(result.get("books", {})),
            "checks": len(result.get("checks", [])), "failures": len(result.get("failures", [])),
            "pending_price_windows": len(result.get("pending", [])), "trade_execution": False,
            "notifications": False, "source_eligibility_replayed": False}
