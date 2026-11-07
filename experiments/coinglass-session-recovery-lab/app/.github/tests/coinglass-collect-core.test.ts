/**
 * בדיקות אופליין למסלול האיסוף היחיד. אין רשת, אין ספק בתשלום ואין מסד נתונים:
 * כל התלויות מוזרקות, וכל קריאה יוצאת נספרת.
 */
import { expect, test } from "bun:test";
import {
  pollCollection,
  startCollection,
  type CollectDeps,
} from "../../src/lib/coinglassCollectCore";
import { signTicket } from "../../src/lib/coinglassTicket";
import {
  COLLECTOR_SCHEMA_VERSION,
  validateCollectorEnvelope,
} from "../../src/lib/coinglassCollector";
import { readCollectorConfig, startCollectorJob } from "../../src/lib/coinglassCollector.server";

const SESSION = "11111111-1111-4111-8111-111111111111";
const FIELD = "22222222-2222-4222-8222-222222222222";
const WORKSPACE = "33333333-3333-4333-8333-333333333333";
const USER = "44444444-4444-4444-8444-444444444444";
const OTHER_USER = "55555555-5555-4555-8555-555555555555";
const REQUEST = "66666666-6666-4666-8666-666666666666";
// בחוזה v1 שלושת המזהים זהים: job_id == run_id == evidence.artifact_id.
const JOB = "77777777-7777-4777-8777-777777777777";
const RUN = JOB;
const ART = JOB;
const FOREIGN_ID = "99999999-9999-4999-8999-999999999999";
const TOKEN = "bridge-secret-for-tests";
const SOURCE = "https://www.coinglass.com/pro/futures/LiquidationHeatMap?coin=BTC&type=symbol";

type Counters = { post: number; get: number; commits: number; released: number };

const result = (overrides: Record<string, unknown> = {}) => ({
  schema_version: COLLECTOR_SCHEMA_VERSION,
  level_contract_version: "prominent-rows.v1",
  selection_policy: "prominent_right_edge.v1",
  run_id: RUN,
  source_url: SOURCE,
  symbol: "BTC",
  heatmap_model: 1,
  timeframe: "12H",
  captured_at: new Date().toISOString(),
  source_updated_at: null,
  provider: "OpenAI",
  model: "gpt-vision",
  usage: null,
  observed_price: 100000,
  price_axis_range: { low: 90000, high: 110000, price_evidence: "90,000 to 110,000" },
  zones: [
    {
      side: "above",
      price_low: 105000,
      price_high: 106000,
      intensity: "many",
      shape: "point",
      prices: [105500],
      price_precision: "axis_estimate",
      evidence_basis: "axis_interpolation",
      price_evidence: "Between 105,000 and 106,000",
      uncertainty_usd: 500,
      strength_score: 0.9,
    },
  ],
  evidence: { sha256: "a".repeat(64), artifact_id: ART, content_type: "image/png" },
  summary: "",
  quality: "visual_estimate",
  ...overrides,
});

const envelope = (status: string, extra: Record<string, unknown> = {}) => ({
  schema_version: COLLECTOR_SCHEMA_VERSION,
  job_id: JOB,
  timeframe: "12H",
  status,
  result: status === "ready" ? result() : null,
  error: null,
  ...extra,
});

type WorldOptions = {
  canEdit?: boolean;
  status?: string;
  deleted?: boolean;
  storedUpdatedAt?: string | null;
  applied?: boolean;
  commit?: { status?: string } | null;
  commitError?: boolean;
  job?: (status?: string) => unknown;
  jobStatus?: string;
  configured?: boolean;
};

function world(options: WorldOptions = {}) {
  const counters: Counters = { post: 0, get: 0, commits: 0, released: 0 };
  const rows: Record<string, unknown> = {
    v2_collection_sessions: {
      id: SESSION,
      workspace_id: WORKSPACE,
      template_id: "tmpl",
      status: options.status ?? "draft",
      deleted_at: options.deleted ? new Date().toISOString() : null,
    },
    v2_template_fields: {
      id: FIELD,
      title: "Coinglass - Model 1",
      field_type: "liquidity_table",
      settings: { links: [{ url: SOURCE }] },
      active: true,
      section: { template_id: "tmpl" },
    },
    v2_collection_values: { updated_at: options.storedUpdatedAt ?? null },
    v2_liquidity_ai_runs: options.applied ? { id: "run" } : null,
  };
  const query = (table: string) => {
    const chain = {
      select: () => chain,
      eq: () => chain,
      limit: () => chain,
      maybeSingle: async () => ({ data: rows[table] ?? null, error: null }),
    };
    return chain;
  };
  const supabase = {
    from: (table: string) => query(table),
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    rpc: async (name: string, _args?: unknown): Promise<any> => {
      if (name === "v2_can_edit_workspace") return { data: options.canEdit !== false, error: null };
      if (name === "v2_acquire_ai_lease") return { data: { acquired: true }, error: null };
      if (name === "v2_release_ai_lease") {
        counters.released += 1;
        return { data: null, error: null };
      }
      if (name === "v2_commit_liquidity_ai_run") {
        counters.commits += 1;
        return options.commitError
          ? { data: null, error: { message: "boom" } }
          : { data: options.commit === undefined ? { status: "ok" } : options.commit, error: null };
      }
      return { data: null, error: null };
    },
  };
  const deps: CollectDeps = {
    supabase,
    userId: USER,
    capabilities: async () => ({
      models: [1, 2, 3],
      timeframes: ["12H", "24H", "48H"],
      selectionPolicy: "prominent_right_edge.v1",
    }),
    config: () => {
      if (options.configured === false) throw new Error("שירות האיסוף אינו מוגדר");
      return { base: "https://collector.example.com", token: TOKEN };
    },
    startJob: async (_requestId, timeframe) => {
      counters.post += 1;
      return validateCollectorEnvelope(envelope(options.jobStatus ?? "queued"), { timeframe });
    },
    readJob: async (jobId, timeframe) => {
      counters.get += 1;
      return validateCollectorEnvelope(
        (options.job?.() as object) ?? envelope(options.jobStatus ?? "ready"),
        { timeframe, jobId },
      );
    },
  };
  return { deps, counters };
}

const startInput = {
  sessionId: SESSION,
  fieldId: FIELD,
  timeframe: "12H" as const,
  requestId: REQUEST,
  baselineUpdatedAt: null,
};

const ticketFor = (overrides: Record<string, unknown> = {}, secret = TOKEN) =>
  signTicket(
    {
      v: 1,
      userId: USER,
      workspaceId: WORKSPACE,
      sessionId: SESSION,
      fieldId: FIELD,
      timeframe: "12H",
      jobId: JOB,
      requestId: REQUEST,
      baselineUpdatedAt: null,
      exp: Date.now() + 60_000,
      ...overrides,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any,
    secret,
  );

test("missing configuration returns NOT_CONNECTED with zero external calls", async () => {
  const { deps, counters } = world({ configured: false });
  const response = await startCollection(deps, startInput);
  expect(response.status).toBe("not_connected");
  expect(counters).toMatchObject({ post: 0, get: 0, commits: 0 });
});

test("Model 1 also requires advertised timeframe and current selection policy before POST", async () => {
  for (const capabilities of [
    undefined,
    async () => ({
      models: [1] as const,
      timeframes: ["24H"] as const,
      selectionPolicy: "prominent_right_edge.v1" as const,
    }),
    async () => ({ models: [1] as const, timeframes: ["12H"] as const }),
  ]) {
    const { deps, counters } = world();
    // Fixtures intentionally model absent or incomplete capabilities.
    deps.capabilities = capabilities as CollectDeps["capabilities"];
    expect((await startCollection(deps, startInput)).status).toBe("not_connected");
    expect(counters).toMatchObject({ post: 0, get: 0, commits: 0 });
  }
});

test("a ready legacy result never satisfies a newly requested prominent-level scan", async () => {
  const { deps, counters } = world({
    job: () => envelope("ready", { result: result({ selection_policy: undefined }) }),
  });
  const response = await pollCollection(deps, { ticket: await ticketFor() });
  expect(response.status).toBe("failed");
  expect(counters.commits).toBe(0);
});

test("a member without edit permission never reaches the collector", async () => {
  const { deps, counters } = world({ canEdit: false });
  const response = await startCollection(deps, startInput);
  expect(response.status).toBe("failed");
  expect(counters.post).toBe(0);
});

test("archived or closed sheets are refused before any call", async () => {
  for (const options of [{ deleted: true }, { status: "closed" }]) {
    const { deps, counters } = world(options);
    expect((await startCollection(deps, startInput)).status).toBe("failed");
    expect(counters.post).toBe(0);
  }
});

test("unsupported horizons and malformed input are rejected by the start validator", async () => {
  const { deps, counters } = world();
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  await expect(startCollection(deps, { ...startInput, timeframe: "4H" } as any)).rejects.toThrow();
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  await expect(startCollection(deps, { ...startInput, sessionId: "x" } as any)).rejects.toThrow();
  expect(counters.post).toBe(0);
});

test("48H starts, polls and saves only the 48H row", async () => {
  const { deps, counters } = world({ jobStatus: "ready" });
  deps.startJob = async (_requestId, timeframe) => {
    counters.post += 1;
    return validateCollectorEnvelope(
      { ...envelope("queued"), timeframe, result: null },
      { timeframe },
    );
  };
  deps.readJob = async (jobId, timeframe) => {
    counters.get += 1;
    return validateCollectorEnvelope(
      { ...envelope("ready"), timeframe, result: result({ timeframe }) },
      { timeframe, jobId },
    );
  };
  const started = await startCollection(deps, { ...startInput, timeframe: "48H" });
  expect(started.status).toBe("queued");
  expect(started.timeframe).toBe("48H");
  expect(started.ticket).toBeTruthy();

  let audit: unknown;
  const rpc = deps.supabase.rpc;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  deps.supabase.rpc = async (name: string, args: any) => {
    if (name === "v2_commit_liquidity_ai_run") audit = args;
    return rpc(name, args);
  };
  const saved = await pollCollection(deps, { ticket: started.ticket as string });
  expect(saved.status).toBe("saved");
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const args = audit as any;
  expect(args.p_results).toHaveLength(1);
  expect(args.p_results[0].timeframe).toBe("48H");
  expect(args.p_audit[0].timeframe).toBe("48H");
  expect(counters.post).toBe(1);
});

test("a 48H ticket that receives a 24H result saves nothing", async () => {
  const { deps, counters } = world();
  deps.readJob = async (jobId, timeframe) => {
    counters.get += 1;
    return validateCollectorEnvelope(
      { ...envelope("ready"), timeframe: "24H", result: result({ timeframe: "24H" }) },
      { timeframe, jobId },
    );
  };
  const ticket = await ticketFor({ timeframe: "48H" });
  const response = await pollCollection(deps, { ticket });
  expect(response.status).toBe("failed");
  expect(counters.commits).toBe(0);
});

test("a 48H ticket resumes with GET only and keeps its horizon", async () => {
  const { deps, counters } = world({ jobStatus: "running" });
  deps.readJob = async (jobId, timeframe) => {
    counters.get += 1;
    return validateCollectorEnvelope({ ...envelope("running"), timeframe }, { timeframe, jobId });
  };
  const response = await pollCollection(deps, { ticket: await ticketFor({ timeframe: "48H" }) });
  expect(response.status).toBe("running");
  expect(response.timeframe).toBe("48H");
  expect(counters).toMatchObject({ post: 0, commits: 0 });
});

test("a known collector error code becomes a clear Hebrew note", async () => {
  const { deps } = world();
  deps.readJob = async (jobId, timeframe) =>
    validateCollectorEnvelope(
      {
        ...envelope("failed"),
        timeframe,
        result: null,
        error: { code: "source_not_readable", message: "raw internal text" },
      },
      { timeframe, jobId },
    );
  const response = await pollCollection(deps, { ticket: await ticketFor({ timeframe: "48H" }) });
  expect(response.status).toBe("failed");
  expect(response.note).not.toContain("raw internal text");
  expect(response.note).toContain("קריא");
});

test("a manual edit before starting stops the run with a conflict", async () => {
  const { deps, counters } = world({ storedUpdatedAt: "2026-01-01T00:00:00Z" });
  const response = await startCollection(deps, startInput);
  expect(response.status).toBe("conflict");
  expect(counters.post).toBe(0);
});

test("start issues exactly one POST and returns a ticket", async () => {
  const { deps, counters } = world();
  const response = await startCollection(deps, startInput);
  expect(counters.post).toBe(1);
  expect(counters.get).toBe(0);
  expect(response.ticket).toBeTruthy();
  expect(response.status).toBe("queued");
});

test("retrying the same request id stays idempotent — the collector sees the same id", async () => {
  const seen: string[] = [];
  const { deps } = world();
  deps.startJob = async (requestId, timeframe) => {
    seen.push(requestId);
    return validateCollectorEnvelope(envelope("queued"), { timeframe });
  };
  await startCollection(deps, startInput);
  await startCollection(deps, startInput);
  expect(seen).toEqual([REQUEST, REQUEST]);
});

test("polling only ever uses GET and never starts a scan", async () => {
  const { deps, counters } = world({ jobStatus: "running" });
  const ticket = await ticketFor();
  const response = await pollCollection(deps, { ticket });
  expect(response.status).toBe("running");
  expect(counters).toMatchObject({ post: 0, get: 1, commits: 0 });
});

test("a ready result is converted and saved through the existing atomic commit", async () => {
  const { deps, counters } = world({ jobStatus: "ready" });
  let audit: unknown;
  const rpc = deps.supabase.rpc;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  deps.supabase.rpc = async (name: string, args: any) => {
    if (name === "v2_commit_liquidity_ai_run") audit = args;
    return rpc(name, args);
  };
  const response = await pollCollection(deps, { ticket: await ticketFor() });
  expect(response.status).toBe("saved");
  expect(counters.commits).toBe(1);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const args = audit as any;
  expect(args.p_results[0].timeframe).toBe("12H");
  expect(args.p_results[0].levels[0]).toMatchObject({ side: "above", amount: "many" });
  expect(args.p_audit[0].provider).toBe("OpenAI");
  expect(args.p_audit[0].analysis.collector_run_id).toBe(RUN);
  expect(args.p_audit[0].analysis.evidence_sha256).toBe("a".repeat(64));
  expect(args.p_baseline_updated_at).toBe(null);
});

test("an already applied run is detected and never written twice", async () => {
  const { deps, counters } = world({ jobStatus: "ready", applied: true });
  const response = await pollCollection(deps, { ticket: await ticketFor() });
  expect(response.status).toBe("duplicate");
  expect(counters.commits).toBe(0);
});

test("a save failure keeps the ticket for a retry that uses GET only", async () => {
  const { deps, counters } = world({ jobStatus: "ready", commitError: true });
  const ticket = await ticketFor();
  const first = await pollCollection(deps, { ticket });
  expect(first.status).toBe("failed");
  expect(first.retryable).toBe(true);
  expect(first.ticket).toBe(ticket);
  await pollCollection(deps, { ticket });
  expect(counters.post).toBe(0);
  expect(counters.get).toBe(2);
});

test("a concurrent conflict where the run was already applied does not rewrite", async () => {
  const { deps, counters } = world({
    jobStatus: "ready",
    commit: { status: "conflict" },
    applied: false,
  });
  let appliedNow = false;
  const from = deps.supabase.from;
  deps.supabase.from = (table: string) => {
    if (table === "v2_liquidity_ai_runs") {
      const chain = {
        select: () => chain,
        eq: () => chain,
        limit: () => chain,
        maybeSingle: async () => ({ data: appliedNow ? { id: "run" } : null, error: null }),
      };
      return chain;
    }
    return from(table);
  };
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const rpc = deps.supabase.rpc as any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  deps.supabase.rpc = async (name: string, args: any) => {
    const outcome = await rpc(name, args);
    if (name === "v2_commit_liquidity_ai_run") appliedNow = true;
    return outcome;
  };
  const response = await pollCollection(deps, { ticket: await ticketFor() });
  expect(response.status).toBe("duplicate");
  expect(counters.commits).toBe(1);
});

test("a manual edit during the run reports a conflict against the original baseline", async () => {
  const { deps, counters } = world({ jobStatus: "ready", storedUpdatedAt: "2026-02-02T00:00:00Z" });
  const response = await pollCollection(deps, { ticket: await ticketFor() });
  expect(response.status).toBe("conflict");
  expect(counters.commits).toBe(0);
});

test("forged, foreign, expired and cross-sheet tickets are refused", async () => {
  const { deps, counters } = world({ jobStatus: "ready" });
  const cases = [
    await ticketFor({}, "wrong-secret"),
    await ticketFor({ userId: OTHER_USER }),
    await ticketFor({ exp: Date.now() - 1000 }),
    "not.a.ticket",
  ];
  for (const ticket of cases) {
    const response = await pollCollection(deps, { ticket });
    expect(response.status).toBe("failed");
  }
  const foreignSheet = await ticketFor({ workspaceId: OTHER_USER });
  expect((await pollCollection(deps, { ticket: foreignSheet })).status).toBe("failed");
  expect(counters.commits).toBe(0);
});

test("a stale result is shown as stale instead of being stamped as now", async () => {
  const { deps, counters } = world({
    jobStatus: "ready",
    job: () =>
      envelope("ready", {
        result: result({ captured_at: new Date(Date.now() - 40 * 60 * 1000).toISOString() }),
      }),
  });
  const response = await pollCollection(deps, { ticket: await ticketFor() });
  expect(response.status).toBe("stale");
  expect(counters.commits).toBe(0);
});

test("a wrong-source or wrong-model result fails closed without writing", async () => {
  for (const bad of [
    result({ source_url: "https://example.com/x" }),
    result({ heatmap_model: 2 }),
    result({ timeframe: "24H" }),
  ]) {
    const { deps, counters } = world({ job: () => envelope("ready", { result: bad }) });
    // תשובה שאינה עומדת בחוזה היא כשל סופי — לא ממשיכים לסקור אותה.
    const response = await pollCollection(deps, { ticket: await ticketFor() });
    expect(response.status).toBe("failed");
    expect(response.ticket).toBeFalsy();
    expect(counters.commits).toBe(0);
  }
});

test("the collector client makes zero network calls without configuration", async () => {
  const originalUrl = process.env.COINGLASS_COLLECTOR_URL;
  const originalToken = process.env.COINGLASS_COLLECTOR_TOKEN;
  delete process.env.COINGLASS_COLLECTOR_URL;
  delete process.env.COINGLASS_COLLECTOR_TOKEN;
  const originalFetch = globalThis.fetch;
  let calls = 0;

  globalThis.fetch = (async () => {
    calls += 1;
    return new Response("{}");
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  }) as any;
  try {
    expect(() => readCollectorConfig()).toThrow();
    await expect(startCollectorJob(REQUEST, "12H")).rejects.toThrow();
    expect(calls).toBe(0);
  } finally {
    globalThis.fetch = originalFetch;
    if (originalUrl) process.env.COINGLASS_COLLECTOR_URL = originalUrl;
    if (originalToken) process.env.COINGLASS_COLLECTOR_TOKEN = originalToken;
  }
});

test("the button path never reaches Firecrawl or Gemini", async () => {
  const core = await Bun.file("src/lib/coinglassCollectCore.ts").text();
  const bridge = await Bun.file("src/lib/coinglassCollector.server.ts").text();
  const ui = await Bun.file("src/components/v2/LiquidityAutoFill.tsx").text();
  for (const source of [core, bridge, ui]) {
    expect(source).not.toContain("firecrawl");
    expect(source).not.toContain("gemini");
    expect(source).not.toContain("coinglassCapture");
    expect(source).not.toContain("coinglassVision");
  }
  expect(ui).not.toContain("autoFillCoinglassModel1");
});

/* ------------------ תיקוני האמינות: חוזה, נעילה, זהות ומצב ממתין ------------------ */

import {
  clearPending,
  pendingKey,
  readPending,
  resumeAction,
  legacyPendingKey,
  readChoice,
  tryWritePending,
  writeChoice,
  writePending,
  type PendingCollection,
  type PendingStorage,
} from "../../src/lib/coinglassPending";

const memoryStorage = (): PendingStorage & { map: Map<string, string> } => {
  const map = new Map<string, string>();
  return {
    map,
    getItem: (key) => map.get(key) ?? null,
    setItem: (key, value) => void map.set(key, value),
    removeItem: (key) => void map.delete(key),
  };
};

const identity = { userId: USER, sessionId: SESSION, fieldId: FIELD, timeframe: "12H" as const };
const identity48 = { ...identity, timeframe: "48H" as const };

test("תשובת התחלה שאבדה: הבקשה כבר שמורה, והחידוש משתמש באותו מזהה", () => {
  const storage = memoryStorage();
  const record: PendingCollection = {
    requestId: REQUEST,
    timeframe: "12H",
    baselineUpdatedAt: null,
    ...identity,
    ticket: null,
  };
  writePending(storage, record);
  const afterReload = readPending(storage, identity);
  expect(afterReload?.requestId).toBe(REQUEST);
  expect(resumeAction(afterReload)).toBe("resend-start");
  writePending(storage, { ...record, ticket: "t" });
  expect(resumeAction(readPending(storage, identity))).toBe("poll");
  clearPending(storage, identity);
  expect(readPending(storage, identity)).toBe(null);
});

test("רשומה של משתמש אחר לעולם אינה נקראת", () => {
  const storage = memoryStorage();
  storage.setItem(
    pendingKey(USER, SESSION, FIELD, "12H"),
    JSON.stringify({
      requestId: REQUEST,
      timeframe: "12H",
      baselineUpdatedAt: null,
      userId: OTHER_USER,
      sessionId: SESSION,
      fieldId: FIELD,
      ticket: "t",
    }),
  );
  expect(readPending(storage, identity)).toBe(null);
});

test("הנעילה משוחררת מיד בסיום ההתחלה — גם בהצלחה וגם בכישלון", async () => {
  const ok = world();
  await startCollection(ok.deps, startInput);
  expect(ok.counters.released).toBe(1);

  const bad = world();
  bad.deps.startJob = async () => {
    throw new Error("רשת");
  };
  const response = await startCollection(bad.deps, startInput);
  expect(response.status).toBe("failed");
  expect(bad.counters.released).toBe(1);
});

test("הסקר לעולם אינו משחרר נעילה", async () => {
  const { deps, counters } = world({ jobStatus: "ready" });
  await pollCollection(deps, { ticket: await ticketFor() });
  expect(counters.released).toBe(0);
});

test("ספק שאינו OpenAI, run_id שונה מ-job_id או ראיה זרה — נדחים", async () => {
  for (const bad of [
    result({ provider: "Gemini" }),
    result({ run_id: "12121212-1212-4212-8212-121212121212" }),
    result({
      evidence: { sha256: "a".repeat(64), artifact_id: FOREIGN_ID, content_type: "image/png" },
    }),
    result({ captured_at: "2026-01-01 10:00:00" }),
    result({ source_updated_at: new Date().toISOString() }),
    result({ usage: "many" }),
  ]) {
    const { deps, counters } = world({ job: () => envelope("ready", { result: bad }) });
    const response = await pollCollection(deps, { ticket: await ticketFor() });
    expect(response.status).toBe("failed");
    expect(counters.commits).toBe(0);
  }
});

test("שגיאת קריאה במסד עוצרת לפני קריאה בתשלום ולפני כתיבה", async () => {
  const { deps, counters } = world();
  deps.supabase.from = () => {
    const chain = {
      select: () => chain,
      eq: () => chain,
      limit: () => chain,
      maybeSingle: async () => ({ data: null, error: { message: "db down" } }),
    };
    return chain;
  };
  const response = await startCollection(deps, startInput);
  expect(response.status).toBe("failed");
  expect(response.retryable).toBe(true);
  expect(counters).toMatchObject({ post: 0, commits: 0 });
});

test("סטטוס שמירה ריק או לא מוכר אינו נחשב הצלחה", async () => {
  for (const commit of [null, { status: "weird" }]) {
    const { deps } = world({ jobStatus: "ready", commit });
    const ticket = await ticketFor();
    const response = await pollCollection(deps, { ticket });
    expect(response.status).toBe("failed");
    expect(response.ticket).toBe(ticket);
  }
});

test("תוצאה ישנה אינה מציעה שמירה חוזרת", async () => {
  const { deps } = world({
    jobStatus: "ready",
    job: () =>
      envelope("ready", {
        result: result({ captured_at: new Date(Date.now() - 40 * 60 * 1000).toISOString() }),
      }),
  });
  const response = await pollCollection(deps, { ticket: await ticketFor() });
  expect(response.status).toBe("stale");
  expect(response.retryable).toBeFalsy();
  expect(response.ticket).toBeFalsy();
});

test("שחרור הנעילה מתבצע גם עם builder שהוא PromiseLike בלבד (בלי .catch)", async () => {
  const { deps } = world();
  let releaseExecuted = 0;
  // builder עצלן כמו של ספריית הלקוח: רק then, בלי catch — הקריאה ל-.then היא
  // זו שמפעילה את ה-RPC, ולכן .catch?.() אופציונלי היה מדלג על השחרור כולו.
  const lazyRelease = {
    then(onFulfilled?: (value: unknown) => unknown) {
      releaseExecuted += 1;
      const value = { data: null, error: null };
      return Promise.resolve(onFulfilled ? onFulfilled(value) : value);
    },
  };
  const originalRpc = deps.supabase.rpc;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  deps.supabase.rpc = ((name: string, args?: unknown): any =>
    name === "v2_release_ai_lease" ? lazyRelease : originalRpc(name, args)) as never;
  const response = await startCollection(deps, startInput);
  expect(response.status).not.toBe("failed");
  expect(releaseExecuted).toBe(1);
});

test("אחסון כושל: אין זריקה ואין כתיבה — המתקשר עוצר לפני כל קריאת רשת", () => {
  const failing: PendingStorage = {
    getItem: () => null,
    setItem: () => {
      throw new Error("QuotaExceededError");
    },
    removeItem: () => undefined,
  };
  const record: PendingCollection = {
    requestId: REQUEST,
    timeframe: "12H",
    baselineUpdatedAt: null,
    ...identity,
    ticket: null,
  };
  expect(tryWritePending(failing, record)).toBe(false);
  expect(readPending(failing, identity)).toBe(null);
});

test("לכל טווח מצב ממתין נפרד — 48H אינו דורס את 12H", () => {
  const storage = memoryStorage();
  const base = { requestId: REQUEST, baselineUpdatedAt: null, ticket: null };
  writePending(storage, { ...base, ...identity });
  writePending(storage, { ...base, ...identity48, requestId: FOREIGN_ID });
  expect(readPending(storage, identity)?.requestId).toBe(REQUEST);
  expect(readPending(storage, identity48)?.requestId).toBe(FOREIGN_ID);
  expect(readPending(storage, identity48)?.timeframe).toBe("48H");
  clearPending(storage, identity48);
  expect(readPending(storage, identity48)).toBe(null);
  expect(readPending(storage, identity)?.requestId).toBe(REQUEST);
  expect(tryWritePending(storage, { ...base, ...identity48 })).toBe(true);
});

test("מצב ממתין ישן (12H/24H) שורד את הגרסה הזו עם אותו מזהה וכרטיס", () => {
  for (const timeframe of ["12H", "24H"] as const) {
    const storage = memoryStorage();
    const id = { ...identity, timeframe };
    storage.setItem(
      legacyPendingKey(USER, SESSION, FIELD),
      JSON.stringify({
        requestId: REQUEST,
        timeframe,
        baselineUpdatedAt: "2026-01-01T00:00:00Z",
        userId: USER,
        sessionId: SESSION,
        fieldId: FIELD,
        ticket: "old-ticket",
      }),
    );
    const restored = readPending(storage, id);
    expect(restored?.requestId).toBe(REQUEST);
    expect(restored?.ticket).toBe("old-ticket");
    expect(restored?.baselineUpdatedAt).toBe("2026-01-01T00:00:00Z");
    expect(resumeAction(restored)).toBe("poll");
    // הועבר למפתח החדש והישן נמחק רק אחרי העתקה מוצלחת
    expect(storage.map.get(legacyPendingKey(USER, SESSION, FIELD))).toBeUndefined();
    expect(readPending(storage, id)?.requestId).toBe(REQUEST);
    // טווח אחר אינו מקבל את הרשומה הישנה
    expect(readPending(storage, { ...identity, timeframe: "48H" })).toBe(null);
  }
});

test("אחסון שאינו ניתן לכתיבה משאיר את הרשומה הישנה קריאה ואינו יוצר בקשה אחרת", () => {
  const storage = memoryStorage();
  storage.setItem(
    legacyPendingKey(USER, SESSION, FIELD),
    JSON.stringify({
      requestId: REQUEST,
      timeframe: "12H",
      baselineUpdatedAt: null,
      userId: USER,
      sessionId: SESSION,
      fieldId: FIELD,
      ticket: "old-ticket",
    }),
  );
  const readOnly = {
    ...storage,
    setItem: () => {
      throw new Error("full");
    },
  };
  expect(readPending(readOnly, identity)?.requestId).toBe(REQUEST);
  expect(storage.map.get(legacyPendingKey(USER, SESSION, FIELD))).toBeTruthy();
  expect(readPending(readOnly, identity)?.ticket).toBe("old-ticket");
});

test("ניקוי נוגע רק בטווח התואם — רשומה ישנה של טווח אחר נשארת", () => {
  const storage = memoryStorage();
  storage.setItem(
    legacyPendingKey(USER, SESSION, FIELD),
    JSON.stringify({
      requestId: REQUEST,
      timeframe: "24H",
      baselineUpdatedAt: null,
      userId: USER,
      sessionId: SESSION,
      fieldId: FIELD,
      ticket: "t",
    }),
  );
  clearPending(storage, { ...identity, timeframe: "48H" });
  expect(storage.map.get(legacyPendingKey(USER, SESSION, FIELD))).toBeTruthy();
  clearPending(storage, { ...identity, timeframe: "24H" });
  expect(storage.map.get(legacyPendingKey(USER, SESSION, FIELD))).toBeUndefined();
});

test("הטווח הנבחר משוחזר אחרי רענון יחד עם אותו מזהה בקשה וכרטיס", () => {
  const storage = memoryStorage();
  const base = { userId: USER, sessionId: SESSION, fieldId: FIELD };
  writeChoice(storage, base, "48H");
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  writeChoice(storage, base, "4H" as any);
  expect(readChoice(storage, base)).toBe("48H");
  writePending(storage, {
    requestId: REQUEST,
    timeframe: "48H",
    baselineUpdatedAt: null,
    ...base,
    ticket: "ticket-48",
  });
  const restoredChoice = readChoice(storage, base) ?? "12H";
  const restored = readPending(storage, { ...base, timeframe: restoredChoice });
  expect(restoredChoice).toBe("48H");
  expect(restored?.requestId).toBe(REQUEST);
  expect(restored?.ticket).toBe("ticket-48");
  expect(resumeAction(restored)).toBe("poll");
});

test("a visual range result is saved with a null observed price and its range audit", async () => {
  const { deps, counters } = world();
  const range = {
    low: 100000,
    high: 100400,
    confidence: "high",
    basis: "last_candle_axis_bracket",
    axis_low: 99000,
    axis_high: 101000,
  };
  deps.readJob = async (jobId, timeframe) => {
    counters.get += 1;
    return validateCollectorEnvelope(
      {
        ...envelope("ready"),
        timeframe,
        result: result({
          timeframe,
          observed_price: null,
          price_reference_type: "visual_range",
          observed_price_range: range,
          omitted_ambiguous_zones: 3,
        }),
      },
      { timeframe, jobId },
    );
  };
  let audit: unknown;
  const rpc = deps.supabase.rpc;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  deps.supabase.rpc = async (name: string, args: any) => {
    if (name === "v2_commit_liquidity_ai_run") audit = args;
    return rpc(name, args);
  };
  const saved = await pollCollection(deps, { ticket: await ticketFor() });
  expect(saved.status).toBe("saved");
  expect(saved.observedPrice).toBeNull();
  expect(saved.observedPriceRange).toEqual(range);
  expect(saved.omittedAmbiguousZones).toBe(3);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const args = audit as any;
  expect(args.p_audit[0].observed_price).toBeNull();
  expect(args.p_audit[0].analysis.price_reference_type).toBe("visual_range");
  expect(args.p_audit[0].analysis.observed_price_range).toEqual(range);
  expect(args.p_audit[0].analysis.omitted_ambiguous_zones).toBe(3);
  // אמצע הטווח לעולם אינו נכתב כמחיר שנצפה.
  expect(JSON.stringify(args)).not.toContain("100200");
});
