import assert from "node:assert/strict";
import test from "node:test";
import fs from "node:fs";
import vm from "node:vm";
import ts from "typescript";

// Execute the real hook with controlled service responses and a deterministic
// clock. No source scans, API calls or database writes are made by these tests.
function load(file, deps, globals = {}) {
  const module = { exports: {} };
  const source = fs.readFileSync(new URL(`../${file}`, import.meta.url), "utf8");
  vm.runInNewContext(
    ts.transpileModule(source, {
      compilerOptions: {
        module: ts.ModuleKind.CommonJS,
        target: ts.ScriptTarget.ES2022,
      },
    }).outputText,
    {
      module,
      exports: module.exports,
      require: (name) => {
        if (!(name in deps)) throw Error(`Unexpected dependency: ${name}`);
        return deps[name];
      },
      ...globals,
    },
  );
  return module.exports;
}

function harness() {
  const memory = new Map(),
    states = [],
    refs = [],
    starts = [],
    polls = [];
  const storage = {
    getItem: (key) => memory.get(key) ?? null,
    setItem: (key, value) => memory.set(key, value),
    removeItem: (key) => memory.delete(key),
  };
  const collector = {
    COLLECTOR_TIMEFRAMES: ["12H", "24H", "48H"],
    isCollectorTimeframe: (value) => ["12H", "24H", "48H"].includes(value),
    isHeatmapModel: (value) => [1, 2, 3].includes(value),
  };
  const deps = { "@/lib/coinglassCollector": collector };
  const pending = load("src/lib/coinglassPending.ts", deps);
  const batch = load("src/lib/coinglassBatch.ts", deps);
  let stateIndex,
    refIndex,
    id = 0,
    hook;
  const h = {
    memory,
    starts,
    polls,
    pending,
    response: () => ({ status: "running" }),
    startResponse: (data) => ({ status: "running", ticket: data.requestId }),
    entries: () => states[0],
    stop: () => hook.stop(),
  };
  const react = {
    useCallback: (fn) => fn,
    useRef: (value) => {
      const i = refIndex++;
      return (refs[i] ??= { current: value });
    },
    useState: (value) => {
      const i = stateIndex++;
      if (!(i in states)) states[i] = value;
      return [
        states[i],
        (next) => {
          states[i] = typeof next === "function" ? next(states[i]) : next;
        },
      ];
    },
  };
  const supabase = {
    from: () => ({
      select: () => ({
        eq: () => ({
          eq: () => ({
            maybeSingle: async () => ({
              data: { updated_at: "2026-10-19T11:00:00Z" },
              error: null,
            }),
          }),
        }),
      }),
    }),
  };
  const start = async ({ data }) => {
    starts.push(data);
    return h.startResponse(data);
  };
  const poll = async ({ data }) => {
    polls.push(data);
    return h.response(data);
  };
  const useHook = load(
    "src/components/v2/useCoinglassBatch.ts",
    {
      react,
      "@tanstack/react-query": { useQueryClient: () => ({ invalidateQueries: async () => {} }) },
      "@tanstack/react-start": { useServerFn: (fn) => fn },
      "@/integrations/supabase/client": { supabase },
      "@/lib/coinglassCollect.functions": {
        startModel1Collection: start,
        pollModel1Collection: poll,
      },
      "@/lib/coinglassBatch": batch,
      "@/lib/coinglassPending": pending,
    },
    {
      window: { localStorage: storage, setTimeout: (fn) => setImmediate(fn) },
      crypto: { randomUUID: () => `request-${++id}` },
    },
  ).useCoinglassBatch;
  h.run = async () => {
    stateIndex = refIndex = 0;
    hook = useHook("session", "user", () => {});
    hook.startBatch([{ fieldId: "field", model: 3 }]);
    for (let i = 0; i < 1500 && states[1]; i++) await new Promise(setImmediate);
    assert.equal(states[1], false, "queue must finish or pause within its bound");
  };
  return h;
}

test("a four-minute collection completes before the manual polling budget", async () => {
  const h = harness(),
    counts = new Map();
  h.response = ({ ticket }) => {
    const count = (counts.get(ticket) ?? 0) + 1;
    counts.set(ticket, count);
    return { status: count >= 52 ? "saved" : "running" };
  };
  await h.run();
  assert.equal(h.starts.length, 3);
  assert.ok(h.entries().every((entry) => entry.status === "done"));
  assert.equal(h.memory.size, 0);
  await h.run();
  assert.equal(h.starts.length, 3, "completed horizons must not run again");
});

test("a still-running job pauses the queue and resumes the same ticket", async () => {
  const h = harness();
  await h.run();
  assert.equal(h.starts.length, 1);
  assert.equal(h.polls.length, h.pending.MAX_POLLS);
  assert.equal(h.entries()[0].status, "stopped");
  assert.equal(h.entries()[1].status, "pending");
  assert.equal(h.memory.size, 1);
  h.response = () => ({ status: "saved" });
  await h.run();
  assert.equal(h.starts.length, 3);
  assert.equal(h.polls[h.pending.MAX_POLLS].ticket, h.polls[0].ticket);
});

test("terminal failures clear tickets so an explicit next attempt can start", async () => {
  const h = harness();
  h.response = () => ({ status: "failed", retryable: false });
  await h.run();
  assert.equal(h.memory.size, 0);
  assert.equal(h.starts.length, 3);
  await h.run();
  assert.equal(h.starts.length, 6);
  assert.equal(new Set(h.starts.map((value) => value.requestId)).size, 6);
});

test("an uncertain service response stops the queue and preserves the ticket", async () => {
  const h = harness();
  h.response = () => ({ status: "failed", retryable: true });
  await h.run();
  assert.equal(h.starts.length, 1);
  assert.equal(h.memory.size, 1);
  assert.equal(h.entries()[0].status, "stopped");
  h.response = () => ({ status: "saved" });
  await h.run();
  assert.equal(h.starts.length, 3);
  assert.equal(h.polls[1].ticket, h.polls[0].ticket);
});

test("manual stop keeps the current job and does not start another horizon", async () => {
  const h = harness();
  h.response = () => {
    h.stop();
    return { status: "running" };
  };
  await h.run();
  assert.equal(h.starts.length, 1);
  assert.equal(h.memory.size, 1);
  assert.equal(h.entries()[0].status, "stopped");
});

test("conflicts clear the obsolete baseline without advancing the queue", async () => {
  const h = harness();
  h.response = () => ({ status: "conflict" });
  await h.run();
  assert.equal(h.starts.length, 1);
  assert.equal(h.memory.size, 0);
  assert.equal(h.entries()[0].status, "stopped");
});

test("quota response survives an explicit immediate resume without another request", async () => {
  const h = harness();
  h.startResponse = () => ({ status: "rate_limited", retryAt: "2999-01-01T00:00:00Z" });
  await h.run();
  await h.run();
  assert.equal(h.starts.length, 1);
  assert.equal(h.polls.length, 0);
  assert.equal(h.memory.size, 1);
});
