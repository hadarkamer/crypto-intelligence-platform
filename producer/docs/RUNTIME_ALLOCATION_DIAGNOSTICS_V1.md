# Passive allocation diagnostics

Process RSS cannot distinguish application allocations, Python allocator pools,
native allocator arenas, and other resident mappings. The existing memory
diagnostic now adds an `allocation` object to make these hypotheses measurable.
The outer `runtime-memory-v1` format, process identity and collection hooks stay
compatible. No report, source selection, formula, schedule or cache policy changes.

## Measurements

- Interpreter implementation and numeric version identify the actual runtime.
- `python_allocated_blocks` is the interpreter's allocated-block count, not
  bytes or a count of application objects. A missing API or a zero value is
  unknown: Python permits builds to return zero when the counter is unavailable.
- GC enabled state, three signed allocation counters, three signed thresholds,
  and three cumulative records containing only `collections`, `collected` and
  `uncollectable` expose collection activity. The counters have different meanings
  and must not be added together as retained memory.
- `active_threads` counts threads known to Python's threading module, not all
  native threads or child processes. It is a scalar; names and stacks are not read.
- On supported Linux/glibc systems, native allocator fields retain their original
  `mallinfo2` names and byte/count meanings. `arena`, `uordblks`, `fordblks` and
  `hblkhd` describe the allocator's accounting, not a partition of process RSS.
  Python pools and other allocators can remain allocated from glibc's perspective
  even when they have internal free space. Free allocator bytes are not a promise
  of memory immediately reclaimable by the operating system.

The readings are non-atomic, and taking the readings itself makes small temporary
allocations. Correlation with a worker or phase does not identify an owner or
prove a leak. Compare observations within the same boot and actual provider.

## Native-call safeguards and limits

The native probe requires Linux, a supported 64-bit ctypes ABI, glibc version
2.33 or newer, and the exact ten ordered `size_t` members of `struct mallinfo2`.
It declares empty argument types and a structure return type. There is no fallback
to the old integer `mallinfo` API. Provider setup and errors are cached per PID;
returned measurements are never reused as if they were current.

Native calls are allowed only at existing `runtime_start` and `runtime_poll`
boundaries, no more than once per minute per process. A nonblocking guard prevents
overlapping native probes. Unsupported platforms, unavailable functions, exhausted
budgets and malformed data have explicit unknown/skipped states.

The existing deadline is cooperative. A call is not started after that deadline,
but an already started native call cannot be interrupted by a Python timeout.
The probe records its duration and whether the deadline was exceeded. `mallinfo2`
can acquire allocator locks and traverse allocator structures; it is not a
constant-time, async-signal-safe or hard-deadline operation. No probe runs in a
signal handler and no concurrent allocator configuration is introduced. Python
exception handling does not contain foreign-function ABI corruption: platform,
provider and structure validation are essential, with a real smoke test isolated
in a child process during development and CI.

## Failure isolation and unchanged behavior

`allocation.partial` and its fixed reason codes describe only the new fields.
The existing top-level `partial` still describes the process/filesystem sample.
An allocation failure cannot suppress valid RSS, cgroup or process-visibility data.
Each Python getter fails independently. Missing values are `null`, not zero.
Exception text, environment variables, stack frames, thread names, object contents
and market data are never emitted. GC dictionaries use a fixed allowlist.

The implementation does not collect garbage, alter GC thresholds, clear caches,
trim allocator arenas, enumerate heap objects, start tracing, add a dependency,
run network requests or create background work. A later intervention must follow
an observed mechanism and preserve the research contracts.

## Sources

- [Python 3.11: allocated-block counter](https://docs.python.org/3.11/library/sys.html#sys.getallocatedblocks)
- [Python 3.11: garbage collector statistics](https://docs.python.org/3.11/library/gc.html)
- [GNU C Library: allocator statistics and native ABI](https://sourceware.org/glibc/manual/2.33/html_node/Statistics-of-Malloc.html)

Synthetic tests cover fixed shapes, unsupported providers, independent failures,
privacy, budget skips, rate limiting, ABI layout and values exceeding 32 bits.
The supported-host subprocess smoke validates native invocation independently of
the application process. None of these tests is evidence of runtime stability.
