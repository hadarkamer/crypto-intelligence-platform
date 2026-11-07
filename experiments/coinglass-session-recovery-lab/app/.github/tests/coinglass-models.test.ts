/**
 * בדיקות אופליין להרחבת האיסוף לשלוש מפות החום (Model 1/2/3).
 * אין רשת, אין ספק בתשלום ואין מסד נתונים — כל התלויות מוזרקות.
 */
import { expect, test } from "bun:test";
import {
  pollCollection,
  startCollection,
  type CollectDeps,
} from "../../src/lib/coinglassCollectCore";
import { signTicket, verifyTicket } from "../../src/lib/coinglassTicket";
import {
  collectorSchemaVersion,
  collectorSourceUrl,
  jobsUrl,
  latestJobUrl,
  validateCollectorEnvelope,
  COLLECTOR_TIMEFRAMES,
  type CollectorTimeframe,
  type HeatmapModel,
} from "../../src/lib/coinglassCollector";
import {
  pendingKey,
  readPending,
  writePending,
  clearPending,
} from "../../src/lib/coinglassPending";
import {
  buildBatch,
  nextPendingIndex,
  batchProgress,
  applyOutcome,
  shouldStopQueue,
} from "../../src/lib/coinglassBatch";

const SESSION = "11111111-1111-4111-8111-111111111111";
const FIELD = "22222222-2222-4222-8222-222222222222";
const WORKSPACE = "33333333-3333-4333-8333-333333333333";
const USER = "44444444-4444-4444-8444-444444444444";
const REQUEST = "66666666-6666-4666-8666-666666666666";
const JOB = "77777777-7777-4777-8777-777777777777";
const TOKEN = "bridge-secret-for-tests";
const MODELS: HeatmapModel[] = [1, 2, 3];

const result = (model: HeatmapModel, timeframe: CollectorTimeframe) => ({
  schema_version: collectorSchemaVersion(model),
  level_contract_version: "prominent-rows.v1",
  selection_policy: "prominent_right_edge.v1",
  run_id: JOB,
  source_url: collectorSourceUrl(model),
  symbol: "BTC",
  heatmap_model: model,
  timeframe,
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
  evidence: { sha256: "a".repeat(64), artifact_id: JOB, content_type: "image/png" },
  summary: "",
  quality: "visual_estimate",
});

const envelope = (model: HeatmapModel, timeframe: CollectorTimeframe, status: string) => ({
  schema_version: collectorSchemaVersion(model),
  job_id: JOB,
  timeframe,
  status,
  result: status === "ready" ? result(model, timeframe) : null,
  error: null,
});

function world(model: HeatmapModel, options: { models?: HeatmapModel[] } = {}) {
  const counters = { post: 0, get: 0, commits: 0 };
  const audits: unknown[] = [];
  const rows: Record<string, unknown> = {
    v2_collection_sessions: {
      id: SESSION,
      workspace_id: WORKSPACE,
      template_id: "tmpl",
      status: "draft",
      deleted_at: null,
    },
    v2_template_fields: {
      id: FIELD,
      title: `Coinglass - Model ${model}`,
      field_type: "liquidity_table",
      settings: { links: [{ url: collectorSourceUrl(model) }] },
      active: true,
      section: { template_id: "tmpl" },
    },
    v2_collection_values: { updated_at: null },
    v2_liquidity_ai_runs: null,
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
    rpc: async (name: string, args?: any): Promise<any> => {
      if (name === "v2_can_edit_workspace") return { data: true, error: null };
      if (name === "v2_acquire_ai_lease") return { data: { acquired: true }, error: null };
      if (name === "v2_commit_liquidity_ai_run") {
        counters.commits += 1;
        audits.push(args);
        return { data: { status: "ok" }, error: null };
      }
      return { data: null, error: null };
    },
  };
  const deps: CollectDeps = {
    supabase,
    userId: USER,
    config: () => ({ base: "https://collector.example.com", token: TOKEN }),
    capabilities: async () => ({
      models: options.models ?? MODELS,
      timeframes: [...COLLECTOR_TIMEFRAMES],
      selectionPolicy: "prominent_right_edge.v1",
    }),
    startJob: async (_requestId, timeframe, _config, jobModel) => {
      counters.post += 1;
      return validateCollectorEnvelope(envelope(jobModel, timeframe, "queued"), {
        timeframe,
        model: jobModel,
      });
    },
    readJob: async (jobId, timeframe, _config, jobModel) => {
      counters.get += 1;
      return validateCollectorEnvelope(envelope(jobModel, timeframe, "ready"), {
        timeframe,
        jobId,
        model: jobModel,
      });
    },
  };
  return { deps, counters, audits };
}

const startInput = (timeframe: CollectorTimeframe, model: HeatmapModel) => ({
  sessionId: SESSION,
  fieldId: FIELD,
  timeframe,
  requestId: REQUEST,
  baselineUpdatedAt: null,
  expectedModel: model,
});

test("all nine model×timeframe combinations start, poll and save their own row", async () => {
  for (const model of MODELS) {
    for (const timeframe of COLLECTOR_TIMEFRAMES) {
      const { deps, counters, audits } = world(model);
      const started = await startCollection(deps, startInput(timeframe, model));
      expect(started.status).toBe("queued");
      const saved = await pollCollection(deps, { ticket: started.ticket as string });
      expect(saved.status).toBe("saved");
      expect(counters).toMatchObject({ post: 1, get: 1, commits: 1 });
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const args = audits[0] as any;
      expect(args.p_results).toHaveLength(1);
      expect(args.p_results[0].timeframe).toBe(timeframe);
      expect(args.p_audit[0].timeframe).toBe(timeframe);
    }
  }
});

test("a model that conflicts with the configured field URL never reaches the collector", async () => {
  const { deps, counters } = world(2);
  const response = await startCollection(deps, startInput("12H", 3));
  expect(response.status).toBe("failed");
  expect(counters.post).toBe(0);
});

test("models the server does not advertise stay unavailable", async () => {
  const { deps, counters } = world(3, { models: [1] });
  const response = await startCollection(deps, startInput("12H", 3));
  expect(response.status).toBe("not_connected");
  expect(counters.post).toBe(0);
});

test("wrong schema version or source url for the model is rejected", () => {
  expect(() =>
    validateCollectorEnvelope(
      { ...envelope(2, "12H", "ready"), schema_version: collectorSchemaVersion(1) },
      { timeframe: "12H", model: 2 },
    ),
  ).toThrow();
  const wrongSource = envelope(3, "12H", "ready");
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (wrongSource.result as any).source_url = collectorSourceUrl(1);
  expect(() => validateCollectorEnvelope(wrongSource, { timeframe: "12H", model: 3 })).toThrow();
  const wrongModel = envelope(2, "12H", "ready");
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (wrongModel.result as any).heatmap_model = 3;
  expect(() => validateCollectorEnvelope(wrongModel, { timeframe: "12H", model: 2 })).toThrow();
});

test("urls and cache keys are isolated per model", () => {
  const base = "https://collector.example.com";
  const paths = MODELS.map((model) => jobsUrl(base, model));
  expect(new Set(paths).size).toBe(3);
  expect(jobsUrl(base)).toBe(jobsUrl(base, 1));
  expect(latestJobUrl(base, "24H", 3)).toContain("model3");
  const keys = MODELS.flatMap((model) =>
    COLLECTOR_TIMEFRAMES.map((timeframe) => pendingKey(USER, SESSION, FIELD, timeframe, model)),
  );
  expect(new Set(keys).size).toBe(9);
});

test("a ticket without a model stays Model 1 and cannot be replayed for 2 or 3", async () => {
  const legacy = signTicket(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
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
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any,
    TOKEN,
  );
  const opts = { userId: USER, now: Date.now() };
  const payload = await verifyTicket(await legacy, TOKEN, { ...opts, model: 1 });
  expect(payload.model).toBe(1);
  for (const model of [2, 3] as HeatmapModel[]) {
    await expect(verifyTicket(await legacy, TOKEN, { ...opts, model })).rejects.toThrow();
  }
});

test("legacy pending records are readable for Model 1 only", () => {
  const store = new Map<string, string>();
  const storage = {
    getItem: (key: string) => store.get(key) ?? null,
    setItem: (key: string, value: string) => void store.set(key, value),
    removeItem: (key: string) => void store.delete(key),
  };
  const identity = { userId: USER, sessionId: SESSION, fieldId: FIELD, timeframe: "12H" as const };
  writePending(storage, {
    requestId: REQUEST,
    timeframe: "12H",
    baselineUpdatedAt: null,
    ticket: null,
    userId: USER,
    sessionId: SESSION,
    fieldId: FIELD,
  });
  expect(readPending(storage, identity)?.requestId).toBe(REQUEST);
  expect(readPending(storage, { ...identity, model: 2 })).toBeNull();
  clearPending(storage, identity);
  expect(readPending(storage, identity)).toBeNull();
});

test("the batch queue runs field by field, resumes unfinished items and stops on quota", () => {
  const targets = MODELS.map((model) => ({ fieldId: `field-${model}`, model }));
  const entries = buildBatch(targets);
  expect(entries).toHaveLength(9);
  expect(entries.slice(0, 3).map((entry) => entry.timeframe)).toEqual([...COLLECTOR_TIMEFRAMES]);
  expect(nextPendingIndex(entries)).toBe(0);

  const ok = (status: string) => ({ status }) as never;
  const afterFirst = applyOutcome(entries, 0, ok("saved"));
  expect(batchProgress(afterFirst)).toMatchObject({ done: 1, total: 9 });
  expect(nextPendingIndex(afterFirst)).toBe(1);

  const duplicate = applyOutcome(afterFirst, 1, ok("duplicate"));
  expect(batchProgress(duplicate).done).toBe(2);

  const blocked = applyOutcome(duplicate, 2, ok("rate_limited"));
  expect(blocked[2].status).toBe("stopped");
  expect(shouldStopQueue(ok("rate_limited"))).toBe(true);
  expect(shouldStopQueue(ok("conflict"))).toBe(true);
  expect(shouldStopQueue(ok("failed"))).toBe(false);
});
