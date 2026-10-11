"""Passive, bounded allocator counters; no collection, Python object traversal or background work.

Native counters describe glibc's allocator, not Python object sizes, RSS or
reclaimable memory. The shared monotonic deadline is soft: native calls cannot
be preempted. No native statistics are reused as a current observation.
"""
from __future__ import annotations

import gc
import os
import sys
import threading
import time

MAX_INTEGER = (1 << 63) - 1
MIN_INTEGER = -(1 << 63)
MAX_SIZE_T = (1 << 64) - 1
NATIVE_INTERVAL_SECONDS = 60.0
NATIVE_PHASES = frozenset({"runtime_start", "runtime_poll"})
IMPLEMENTATIONS = frozenset({"cpython", "pypy", "graalpy", "ironpython", "jython", "micropython"})
GC_FIELDS = ("collections", "collected", "uncollectable")
NATIVE_FIELDS = ("arena", "ordblks", "smblks", "hblks", "hblkhd", "usmblks",
                 "fsmblks", "uordblks", "fordblks", "keepcost")


_native_state = None
_state_guard = threading.Lock()


def _after_fork():
    global _native_state, _state_guard
    _native_state, _state_guard = None, threading.Lock()


if hasattr(os, "register_at_fork"):
    try:
        os.register_at_fork(after_in_child=_after_fork)
    except Exception:
        pass


def _state_for_pid(pid):
    global _native_state
    if not _state_guard.acquire(blocking=False):
        return None
    try:
        if _native_state is None or _native_state["pid"] != pid:
            _native_state = {"pid": pid, "lock": threading.Lock(), "initialized": False,
                             "provider": None, "version": None, "reason": None,
                             "last_attempt": None}
        return _native_state
    finally:
        _state_guard.release()


def _load_ctypes():
    import ctypes
    return ctypes


def _mallinfo2_type(ctypes):
    class Mallinfo2(ctypes.Structure):
        _fields_ = [(name, ctypes.c_size_t) for name in NATIVE_FIELDS]
    return Mallinfo2


def _integer(value, minimum=0, maximum=MAX_INTEGER):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError()
    return value


def _triple(value):
    if type(value) not in (tuple, list) or len(value) != 3:
        raise ValueError()
    return [_integer(item, MIN_INTEGER) for item in value]


def _gc_stats(value):
    if type(value) not in (tuple, list) or len(value) != 3:
        raise ValueError()
    result = []
    for generation in value:
        if type(generation) is not dict:
            raise ValueError()
        result.append({key: _integer(generation[key]) for key in GC_FIELDS})
    return result


def _boolean(value):
    if type(value) is not bool:
        raise ValueError()
    return value


def _implementation(value):
    if type(value) is not str or len(value) > 16 or value not in IMPLEMENTATIONS:
        raise ValueError()
    return value


def _version(value):
    if type(value) not in (tuple, list) or len(value) != 3:
        raise ValueError()
    return [_integer(item) for item in value]


def _read(getter, validator, label, deadline, reasons):
    if time.monotonic() >= deadline:
        reasons.add("time_budget")
        return None
    try:
        value = getter()
    except Exception:
        reasons.add(label + "_unavailable")
        return None
    try:
        return validator(value)
    except Exception:
        reasons.add(label + "_invalid")
        return None


def _initialize_native():
    if sys.platform != "linux":
        return None, None, "native_platform_unsupported"
    try:
        ctypes = _load_ctypes()
    except Exception:
        return None, None, "native_ctypes_unavailable"
    if ctypes.sizeof(ctypes.c_void_p) != 8 or ctypes.sizeof(ctypes.c_size_t) != 8:
        return None, None, "native_abi_unsupported"
    try:
        library = ctypes.CDLL("libc.so.6")
        version_fn = library.gnu_get_libc_version
        version_fn.argtypes = []
        version_fn.restype = ctypes.c_char_p
        raw = version_fn()
    except Exception:
        return None, None, "native_libc_unavailable"
    # glibc's static version string is trusted provider metadata; only a short,
    # exact major.minor value is parsed or retained, never arbitrary text.
    if type(raw) is not bytes or not 3 <= len(raw) <= 7:
        return None, None, "native_glibc_version_invalid"
    parts = raw.split(b".")
    if len(parts) != 2 or any(not 1 <= len(part) <= 3 or not part.isdigit() for part in parts):
        return None, None, "native_glibc_version_invalid"
    version = [int(part) for part in parts]
    if tuple(version) < (2, 33):
        return None, version, "native_glibc_unsupported"
    try:
        provider = library.mallinfo2
        provider.argtypes = []
        provider.restype = _mallinfo2_type(ctypes)
    except Exception:
        return None, version, "native_mallinfo2_unavailable"
    return provider, version, None


def _native(phase, deadline, reasons):
    result = {"status": "phase_skipped", "provider": None, "glibc_version": None,
              "stats": None, "duration_us": None, "budget_overrun": False}
    if type(phase) is not str or phase not in NATIVE_PHASES:
        return result
    if time.monotonic() >= deadline:
        result["status"] = "time_budget"
        reasons.add("time_budget")
        return result
    state = _state_for_pid(os.getpid())
    if state is None or not state["lock"].acquire(blocking=False):
        result["status"] = "busy"
        return result
    try:
        started = time.monotonic()
        if started >= deadline:
            result["status"] = "time_budget"
            reasons.add("time_budget")
            return result
        previous = state["last_attempt"]
        if previous is not None and started - previous < NATIVE_INTERVAL_SECONDS:
            result["status"] = "rate_limited"
            return result
        state["last_attempt"] = started
        if not state["initialized"]:
            try:
                provider, version, reason = _initialize_native()
            except Exception:
                provider, version, reason = None, None, "native_initialization_unavailable"
            state.update(initialized=True, provider=provider, version=version, reason=reason)
        result["glibc_version"] = list(state["version"]) if state["version"] is not None else None
        if state["provider"] is None:
            result["status"] = "unavailable"
            reasons.add(state["reason"])
        elif time.monotonic() >= deadline:
            result["status"] = "time_budget"
            reasons.add("time_budget")
        else:
            result["provider"] = "glibc_mallinfo2"
            try:
                raw = state["provider"]()
            except Exception:
                result["status"] = "unavailable"
                reasons.add("native_call_unavailable")
            else:
                try:
                    result["stats"] = {key: _integer(getattr(raw, key), maximum=MAX_SIZE_T)
                                       for key in NATIVE_FIELDS}
                    result["status"] = "ok"
                except Exception:
                    result["status"] = "invalid_stats"
                    reasons.add("native_stats_invalid")
        ended = time.monotonic()
        result["duration_us"] = min(MAX_INTEGER, max(0, int((ended - started) * 1_000_000)))
        result["budget_overrun"] = ended > deadline
        if result["budget_overrun"]:
            reasons.add("native_time_budget")
        return result
    finally:
        state["lock"].release()


def collect_allocation_stats(phase, *, deadline):
    """Collect fixed scalar metadata within an existing absolute monotonic budget."""
    reasons = set()
    result = {"version": 1, "implementation": {}, "gc": {}}
    result["implementation"]["name"] = _read(
        lambda: sys.implementation.name, _implementation, "implementation", deadline, reasons)
    result["implementation"]["version"] = _read(
        lambda: tuple(sys.version_info[:3]), _version, "python_version", deadline, reasons)
    result["python_allocated_blocks"] = _read(
        lambda: sys.getallocatedblocks(), _integer, "allocated_blocks", deadline, reasons)
    if result["python_allocated_blocks"] == 0:
        result["python_allocated_blocks"] = None
        reasons.add("allocated_blocks_unknown")
    for name, getter, validator in (
        ("enabled", lambda: gc.isenabled(), _boolean), ("count", lambda: gc.get_count(), _triple),
        ("threshold", lambda: gc.get_threshold(), _triple), ("stats", lambda: gc.get_stats(), _gc_stats),
    ):
        result["gc"][name] = _read(getter, validator, "gc_" + name, deadline, reasons)
    result["active_threads"] = _read(
        lambda: threading.active_count(), _integer, "active_threads", deadline, reasons)
    result["native_allocator"] = _native(phase, deadline, reasons)
    if time.monotonic() > deadline:
        reasons.add("time_budget")
    result["partial"] = bool(reasons)
    result["reasons"] = sorted(reasons)
    return result
