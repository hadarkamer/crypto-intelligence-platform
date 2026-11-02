"""Pure bounded sizing of fixed Sheets roots; no probe installation or I/O.

The supported projection follows built-in container payload edges and counts
unique shallow sizes of exact supported objects. It is not RSS, native memory,
exclusive ownership or a whole-heap size. Partial covered bytes are not totals.
Caller admission, source phase hooks and lifecycle timing belong elsewhere.
"""
from collections import Counter, OrderedDict, defaultdict, deque
from datetime import date, datetime
import math
import struct
import sys
import time

VERSION = "sheets-supported-graph-v1"
ROOT_NAMES = (
    "candidates", "children", "upserts", "stage_records", "drain_rows",
    "request_envelope", "request_bytes", "ack_raw", "audit_rows",
    "audit_expected", "audit_page", "audit_seen", "audit_unexpected",
    "audit_last_complete", "snapshot_cache", "worker_result",
)
MAX_NODES = 120_000
MAX_WORKSPACE_BYTES = 24 * 1024 * 1024
MAX_SECONDS = .5
MAX_DEPTH = 128
MAX_EDGES = 2_000_000
_FIXED_RESERVE = 256 * 1024
_POINTER_BYTES = struct.calcsize("P")
_DICT_GC_OVERHEAD = sys.getsizeof({}) - dict.__sizeof__({})
_clock = time.monotonic
_KINDS = {
    str: "str", bytes: "bytes", int: "int", float: "float", bool: "bool",
    type(None): "none", datetime: "datetime_leaf", date: "date_leaf",
    dict: "dict", list: "list", tuple: "tuple", set: "set",
    frozenset: "frozenset", OrderedDict: "ordered_dict", deque: "deque",
    Counter: "counter", defaultdict: "default_dict",
}
_CONTAINER_KINDS = frozenset((
    "dict", "list", "tuple", "set", "frozenset", "ordered_dict", "deque",
    "counter", "default_dict",
))


def _kind(value):
    cls = type(value)
    # Reject a foreign metaclass before any class hashing/equality can run.
    return _KINDS.get(cls) if type(cls) is type else None


def _size(value, kind):
    # Counter is a Python class. Bypass any patched __sizeof__ method rather
    # than invoking a user hook, even for this exact standard-library type.
    if kind == "counter":
        return dict.__sizeof__(value) + _DICT_GC_OVERHEAD
    return sys.getsizeof(value)


def _length(value, kind):
    if kind in ("dict", "counter", "default_dict"):
        return dict.__len__(value)
    return len(value)


def _children(value, kind):
    if kind in ("dict", "counter", "default_dict"):
        for key, child in dict.items(value):
            yield key
            yield child
    elif kind == "ordered_dict":
        for key, child in OrderedDict.items(value):
            yield key
            yield child
    elif kind == "list":
        yield from list.__iter__(value)
    elif kind == "tuple":
        yield from tuple.__iter__(value)
    elif kind == "set":
        yield from set.__iter__(value)
    elif kind == "frozenset":
        yield from frozenset.__iter__(value)
    elif kind == "deque":
        yield from deque.__iter__(value)


def _base(reason=None):
    return {
        "version": VERSION,
        "status": "UNAVAILABLE",
        "complete": False,
        "measurement_valid": False,
        "roots": {},
        "union_bytes": None,
        "shared_union_bytes": None,
        "reason_codes": [] if reason is None else [reason],
        "scope": "SUPPORTED_CONTAINER_PAYLOAD_GRAPH_UNIQUE_SHALLOW_BYTES_NOT_RSS_NATIVE_OR_EXCLUSIVE_OWNER",
        "projection_exclusions": "datetime/date referents including tzinfo; container factories and dynamic instance attributes; all unsupported types and native backing",
        "atomic": False,
        "consistency": "BEST_EFFORT_SIZE_CHECKS_SAME_SIZE_MUTATIONS_CAN_BE_MISSED",
        "values_or_addresses_persisted": False,
        "temporary_pins_released_on_return": True,
        "partial_bytes_are_totals": False,
        "observer_effect": "Memo tables, object pins, iterators and receipts allocate; counted workspace is not total process RSS. Cooperative checks cannot preempt one builtin or operating-system scheduling.",
    }


def measure_roots(roots, *, max_nodes=120000,
                  max_workspace_bytes=24 * 1024 * 1024, max_seconds=.5):
    """Return primitive-only receipts for an exact dict of fixed root labels.

    Only exact supported types are sized or iterated. Identity tables and pins
    are local to this call. Complete totals concern the documented projection;
    unsupported referents, observed mutation or any bound leave totals null.
    Time, edge and workspace checks are cooperative, never a hard timeout.
    """
    if (type(roots) is not dict or not 1 <= len(roots) <= len(ROOT_NAMES)
            or any(type(name) is not str or name not in ROOT_NAMES for name in roots)
            or type(max_nodes) is not int or not 1 <= max_nodes <= MAX_NODES
            or type(max_workspace_bytes) is not int
            or not 1024 <= max_workspace_bytes <= MAX_WORKSPACE_BYTES
            or type(max_seconds) not in (int, float)
            or not math.isfinite(max_seconds) or not 0 < max_seconds <= MAX_SECONDS):
        return _base("INVALID_OPTIONS_OR_ROOT_NAMES")
    result = _base()
    memo = pins = stack = bindings = None
    try:
        # Reject impossible scratch budgets before allocating any traversal
        # structures. The small unavailable receipt is the only output.
        if max_workspace_bytes < _FIXED_RESERVE + 65536:
            return _base("WORKSPACE_BOUND")
        started = _clock()
        names = tuple(name for name in ROOT_NAMES if name in roots)
        bindings = [(name, dict.__getitem__(roots, name)) for name in names]
        stats = {}
        for name, value in bindings:
            kind = _kind(value)
            stats[name] = {
                "status": "NOT_VISITED", "complete": False,
                "measurement_valid": True, "root_kind": kind or "unsupported",
                "root_items": _length(value, kind) if kind in _CONTAINER_KINDS else None,
                "root_shallow_bytes": _size(value, kind) if kind else None,
                "inclusive_bytes": None, "covered_bytes": 0,
                "unique_objects": 0, "unsupported_visits": 0,
                "observed_mutation": False, "reason_codes": [],
            }
        result["roots"] = stats
        memo = [{} for _ in range(256)]
        shard_sizes = [sys.getsizeof(shard) for shard in memo]
        memo_bytes = sys.getsizeof(memo) + sys.getsizeof(shard_sizes) + sum(shard_sizes)
        pins = []
        pin_bytes = sys.getsizeof(pins)
        identity_bytes = mask_bytes = nodes = edges = union = shared = 0
        mask_allowance = sys.getsizeof((1 << len(names)) - 1)
        fixed = _FIXED_RESERVE
        peak = memo_bytes + pin_bytes + fixed
        reasons = set()
        stopped = False
        concurrent = False
        edge_limit = min(MAX_EDGES, max(256, max_nodes * 32))

        def expired():
            elapsed = _clock() - started
            if elapsed < 0:
                raise ValueError("CLOCK_BACKWARDS")
            return elapsed >= max_seconds

        if peak > max_workspace_bytes:
            reasons.add("WORKSPACE_BOUND")
            stopped = True
        for root_index, (name, root_value) in enumerate(bindings):
            row = stats[name]
            if stopped:
                row["reason_codes"].append("NOT_VISITED_AFTER_BOUND")
                continue
            bit = 1 << root_index
            stack = []
            current = root_value
            row_reasons = set()
            while True:
                if edges % 64 == 0 and expired():
                    row_reasons.add("TIME_BOUND")
                    stopped = True
                    break
                edges += 1
                if edges > edge_limit:
                    row_reasons.add("EDGE_BOUND")
                    stopped = True
                    break
                kind = _kind(current)
                if kind is None:
                    row["unsupported_visits"] += 1
                    row_reasons.add("UNSUPPORTED_TYPE")
                else:
                    identity = id(current)
                    index = (identity >> 4) & 255
                    shard = memo[index]
                    previous = shard.get(identity, 0)
                    if not previous & bit:
                        if not previous:
                            if nodes >= max_nodes:
                                row_reasons.add("NODE_BOUND")
                                stopped = True
                                break
                            # Include retained id integers, maximum mask size,
                            # pin capacity, old/new shard resize overlap and
                            # conservative full pin-array realloc overlap.
                            prospective = (memo_bytes + identity_bytes + mask_bytes
                                           + pin_bytes + fixed
                                           + 2 * shard_sizes[index] + 4096
                                           + sys.getsizeof(identity) + mask_allowance
                                           + pin_bytes + 32 * _POINTER_BYTES + 512)
                            if prospective > max_workspace_bytes:
                                row_reasons.add("WORKSPACE_BOUND")
                                stopped = True
                                break
                            peak = max(peak, prospective)
                            before = shard_sizes[index]
                            shard[identity] = bit
                            pins.append(current)
                            pin_bytes = sys.getsizeof(pins)
                            after = sys.getsizeof(shard)
                            shard_sizes[index] = after
                            memo_bytes += after - before
                            identity_bytes += sys.getsizeof(identity)
                            mask_bytes += mask_allowance
                            nodes += 1
                            size = _size(current, kind)
                            union += size
                        else:
                            shard[identity] = previous | bit
                            size = _size(current, kind)
                            if previous & (previous - 1) == 0:
                                shared += size
                        row["covered_bytes"] += size
                        row["unique_objects"] += 1
                        if kind in _CONTAINER_KINDS:
                            if len(stack) >= MAX_DEPTH:
                                row_reasons.add("DEPTH_BOUND")
                                stopped = True
                                break
                            stack.append((current, _children(current, kind),
                                          _length(current, kind), _size(current, kind), kind))
                # Advance a DFS iterator without copying container contents.
                advanced = False
                while stack:
                    container, iterator, count, size_before, container_kind = stack[-1]
                    try:
                        current = next(iterator)
                        advanced = True
                        break
                    except StopIteration:
                        if (_length(container, container_kind) != count
                                or _size(container, container_kind) != size_before):
                            row_reasons.add("CONTAINER_CHANGED")
                            row["observed_mutation"] = concurrent = True
                        stack.pop()
                    except (RuntimeError, KeyError, IndexError):
                        row_reasons.add("CONTAINER_CHANGED")
                        row["observed_mutation"] = concurrent = True
                        stopped = True
                        break
                if stopped or not advanced:
                    break
            row["reason_codes"] = sorted(row_reasons)
            row["complete"] = not row_reasons and not stopped
            row["status"] = "COMPLETE" if row["complete"] else "PARTIAL"
            reasons.update(row_reasons)
            # Do not retain the prior root's iterator frames while starting the
            # next root; all visited objects remain pinned for id safety.
            stack.clear()

        # Header checks detect some races, never a same-size atomic snapshot.
        for name, value in bindings:
            row = stats[name]
            kind = _kind(value)
            if (name not in roots or dict.get(roots, name) is not value
                    or (kind in _CONTAINER_KINDS
                        and (_length(value, kind) != row["root_items"]
                             or _size(value, kind) != row["root_shallow_bytes"]))):
                row["observed_mutation"] = concurrent = True
                row["reason_codes"] = sorted(set(row["reason_codes"] + ["ROOT_CHANGED"]))
                row["complete"] = False
                row["status"] = "CONCURRENT"
                reasons.add("ROOT_CHANGED")
        elapsed = _clock() - started
        if elapsed < 0:
            raise ValueError("CLOCK_BACKWARDS")
        if elapsed >= max_seconds:
            reasons.add("TIME_BOUND")
            stopped = True
            # Revalidation and receipt preparation are within the same budget.
            for row in stats.values():
                row["complete"] = False
                if "TIME_BOUND" not in row["reason_codes"]:
                    row["reason_codes"].append("TIME_BOUND")
        # One observed racing node can be shared with every completed root.
        # Conservatively invalidate all root totals rather than overclaim.
        if concurrent:
            for row in stats.values():
                row["measurement_valid"] = False
                row["complete"] = False
                row["status"] = "CONCURRENT"
        for row in stats.values():
            if row["complete"]:
                row["inclusive_bytes"] = row["covered_bytes"]
            elif row["status"] == "COMPLETE":
                row["status"] = "PARTIAL"
        complete = (not stopped and not concurrent
                    and all(row["complete"] for row in stats.values()))
        result.update(
            status="CONCURRENT" if concurrent else "COMPLETE" if complete else "PARTIAL",
            complete=complete, measurement_valid=not concurrent,
            union_bytes=union if complete else None,
            shared_union_bytes=shared if complete else None,
            supported_unique_objects_observed=nodes, edge_visits=edges,
            reason_codes=sorted(reasons), elapsed_ms=round(elapsed * 1000, 3),
            workspace_peak_counted_bytes=peak,
            workspace_scope="SHARDED_ID_MEMOS_MASK_RESERVATIONS_PIN_ARRAYS_RESIZE_OVERLAP_PLUS_256K_FIXED_SCRATCH_OUTPUT_RESERVE_NOT_TOTAL_RSS",
            limits={"nodes": max_nodes, "workspace_bytes": max_workspace_bytes,
                    "seconds": max_seconds, "edge_visits": edge_limit,
                    "depth": MAX_DEPTH},
        )
        return result
    except BaseException:
        return _base("MEASUREMENT_EXCEPTION")
    finally:
        # No object references enter the receipt or a module/global cache.
        if stack is not None:
            stack.clear()
        if pins is not None:
            pins.clear()
        if memo is not None:
            memo.clear()
        if bindings is not None:
            bindings.clear()
