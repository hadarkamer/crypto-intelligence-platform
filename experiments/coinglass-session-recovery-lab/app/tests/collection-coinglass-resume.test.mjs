import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createHash, randomUUID } from "node:crypto";
import test from "node:test";
import vm from "node:vm";
import ts from "typescript";

const runId = "11111111-1111-4111-8111-111111111111";
const code = ts.transpileModule(
  readFileSync(new URL("../src/lib/collectionAutofill.server.ts", import.meta.url), "utf8"),
  {
    compilerOptions: {
      module: ts.ModuleKind.CommonJS,
      target: ts.ScriptTarget.ES2022,
    },
  },
).outputText;
function fixture(options = {}) {
  let now = Date.parse("2026-10-24T12:00:00Z");
  const state = {
    runId,
    createdAt: new Date(now).toISOString(),
    status: "running",
    value: structuredClone(options.value ?? {}),
    baseline: structuredClone(options.value ?? {}),
    fieldSettings: structuredClone(options.settings ?? {}),
    writeMode: options.writeMode ?? "default",
    sourceError: null,
    rejectJob: null,
    cursors: {},
    claims: [],
    starts: [],
    reads: [],
    applies: [],
    failures: [],
    writes: 0,
    jobs: new Map(),
    lostResponse: false,
    loseCheckpoint: false,
    configCurrent: true,
    enabled: true,
    model: options.model ?? 3,
  };
  const claim = () => ({
    run_id: state.runId,
    status: "running",
    created_at: state.createdAt,
    configuration_current: state.configCurrent,
    field: { id: "field", settings: structuredClone(state.fieldSettings) },
    section: {},
    session: { id: "session", workspace_id: "workspace" },
    source_cursors: structuredClone(state.cursors),
    baseline: structuredClone(state.baseline),
  });
  const db = {
    rpc: async (name, args) => {
      if (name.endsWith("_claim")) {
        state.claims.push(args);
        if (state.status !== "running")
          return { data: { run_id: state.runId, status: state.status } };
        if (options.busy && !args.p_run_id) return { data: { run_id: runId, status: "running" } };
        if (
          options.owner &&
          (options.owner.field_id !== args.p_field_id ||
            options.owner.session_id !== args.p_session_id)
        )
          return { error: { message: "The existing scan does not belong to this request" } };
        return {
          data:
            state.claims.length === 1 && !args.p_run_id
              ? { ...claim(), status: "claimed" }
              : claim(),
        };
      }
      if (name.endsWith("_apply")) {
        state.applies.push(args);
        const conflict =
          !state.configCurrent ||
          (args.p_payload.write_mode === "replace" &&
            JSON.stringify(state.value) !== JSON.stringify(state.baseline));
        state.status = conflict
          ? "conflict"
          : args.p_payload.source_failure_message
            ? "failed"
            : args.p_payload.value
              ? "completed"
              : "unchanged";
        if (!conflict) {
          if (args.p_payload.value) {
            const oldRows = state.value.rows ?? [];
            state.value = { ...state.value, ...structuredClone(args.p_payload.value) };
            if (args.p_payload.write_mode !== "replace")
              state.value.rows = [...oldRows, ...(args.p_payload.value.rows ?? [])];
          }
          state.cursors = structuredClone(args.p_cursors);
        }
        return {
          data: {
            run_id: state.runId,
            status: state.status,
            message: args.p_payload.source_failure_message,
          },
        };
      }
      if (name.endsWith("_fail")) {
        state.failures.push(args);
        state.status = "failed";
        return { data: null };
      }
      throw Error(name);
    },
    from: (table) => {
      if (table === "v2_collection_autofill_runs") {
        const query = {
          select: () => query,
          eq: (_, value) => {
            assert.equal(value, runId);
            return query;
          },
          single: async () =>
            options.ownerError
              ? { data: null, error: { message: "read failed" } }
              : { data: options.owner ?? { field_id: "field", session_id: "session" } },
        };
        return query;
      }
      assert.equal(table, "v2_collection_autofill_state");
      let update, expected;
      const q = {
        update: (value) => {
          update = value;
          return q;
        },
        eq: (key, value) => {
          if (key === "source_cursors") expected = value;
          return q;
        },
        gt: () => q,
        select: async () => {
          if (state.loseCheckpoint) {
            state.loseCheckpoint = false;
            return { data: [] };
          }
          if (expected !== JSON.stringify(state.cursors)) return { data: [] };
          state.cursors = structuredClone(update.source_cursors);
          state.writes++;
          return { data: [{ lease_id: runId }] };
        },
      };
      return q;
    },
  };
  const collector = {
    readCollectorConfig: () => ({}),
    readCollectorCapabilities: async () => ({
      selectionPolicy: "prominent_right_edge.v1",
      models: [1, 2, 3],
      timeframes: ["12H", "24H", "48H"],
    }),
    startCollectorJob: async (id, timeframe, _config, model) => {
      state.starts.push({ id, timeframe, model });
      if (state.sourceError) throw state.sourceError;
      if (!state.jobs.has(id))
        state.jobs.set(id, { job_id: id, timeframe, model, status: "running" });
      if (state.lostResponse) {
        state.lostResponse = false;
        throw Error("lost source response");
      }
      return structuredClone(state.jobs.get(id));
    },
    readCollectorJob: async (id, timeframe, _config, model) => {
      state.reads.push(id);
      const job = state.jobs.get(id);
      // The transport's unchanged contract validator rejects these identities
      // before the orchestration receives a result (covered by collector tests).
      if (state.rejectJob === id || job.timeframe !== timeframe || job.model !== model)
        throw Error("collector contract identity mismatch");
      return structuredClone(state.jobs.get(id));
    },
  };
  const module = { exports: {} };
  vm.runInNewContext(code, {
    module,
    exports: module.exports,
    structuredClone,
    Error,
    crypto: { randomUUID },
    Date: class extends Date {
      static now() {
        return now;
      }
    },
    require: (id) => {
      if (id === "node:crypto") return { createHash };
      if (id === "./coinglassCollector.server") return collector;
      if (id === "./coinglassCollector")
        return {
          isUuid: (id) => /^[0-9a-f-]{36}$/.test(id),
          resultFreshness: (at) => ({
            invalid: !Number.isFinite(Date.parse(at)),
            stale: now - Date.parse(at) > 900_000,
          }),
          collectorZonesToLevels: (result) => result.levels ?? [],
          collectorErrorNote: (code) => code,
        };
      if (id === "./coinglassModel1") return { detectCoinglassHeatmapModel: () => state.model };
      if (id === "./collectionSheets") return { collectionSheetKey: () => "data" };
      if (id === "./collectionAutofill")
        return {
          autofillSettings: () => ({ enabled: state.enabled, write_mode: state.writeMode }),
          autofillKind: () => "liquidity",
          defaultAutofillInstructions: () => "",
          sourceUrls: () => ["https://www.coinglass.com/pro/futures/LiquidationHeatMap?model=3"],
          supportedSourceUrl: () => true,
        };
      return {};
    },
  });
  return {
    state,
    advance: (ms) => {
      now += ms;
    },
    fill: (resume = state.runId) =>
      module.exports.fillCollectionField(db, "field", "session", false, resume),
    ready: (timeframe, overrides = {}) => {
      for (const job of state.jobs.values())
        if (!timeframe || job.timeframe === timeframe)
          Object.assign(job, {
            status: "ready",
            result: {
              run_id: job.job_id,
              heatmap_model: job.model,
              timeframe: job.timeframe,
              selection_policy: "prominent_right_edge.v1",
              captured_at: new Date(now).toISOString(),
              evidence: { artifact_id: job.job_id, sha256: "a".repeat(64) },
              hover_snapshot: {
                heatmap_model: job.model,
                timeframe: job.timeframe,
                source_time: new Date(now).toISOString(),
              },
              levels: [{ label: job.timeframe, price_low: 100, price_high: 101 }],
              ...overrides,
            },
          });
    },
    fail: (timeframe, code = "source_capture_failed") => {
      const job = [...state.jobs.values()].filter((job) => job.timeframe === timeframe).at(-1);
      assert.ok(job, "expected an existing collector job");
      Object.assign(job, { status: "failed", result: null, error: { code } });
    },
    nextRun: () => {
      state.runId = randomUUID();
      state.createdAt = new Date(now).toISOString();
      state.status = "running";
      state.baseline = structuredClone(state.value);
    },
  };
}

test("three long horizons continue one run, persist deadlines and save all results once", async () => {
  const f = fixture();
  for (const timeframe of ["12H", "24H", "48H"]) {
    assert.equal((await f.fill()).continue, true);
    assert.equal(f.state.starts.at(-1).timeframe, timeframe);
    const saved = JSON.stringify(f.state.cursors);
    const writes = f.state.writes;
    f.advance(290_000);
    assert.equal((await f.fill()).continue, true);
    assert.equal(JSON.stringify(f.state.cursors), saved);
    assert.equal(f.state.writes, writes, "unchanged polling must not write");
    f.ready();
    assert.equal((await f.fill()).continue, true);
    assert.equal(f.state.applies.length, 0, "the terminal write must wait until the queue ends");
  }
  assert.equal((await f.fill()).status, "completed");
  assert.equal(f.state.starts.length, 3);
  assert.equal(new Set(f.state.starts.map((job) => job.id)).size, 3);
  assert.equal(f.state.applies.length, 1);
  assert.equal(f.state.applies[0].p_payload.value.rows.length, 3);
  assert.equal(f.state.applies[0].p_cursors.__coinglass_pending, undefined);
  assert.ok(f.state.claims.every((claim) => claim.p_run_id === runId));
});

test("a busy logical field in another sheet never resumes or fails its owner's run", async () => {
  for (const owner of [
    { field_id: "other-field", session_id: "session" },
    { field_id: "field", session_id: "other-session" },
  ]) {
    const f = fixture({ busy: true, owner });
    const result = await f.fill("");
    assert.equal(result.status, "conflict");
    assert.match(result.message, /מתבצעת בגיליון אחר/);
    assert.equal(f.state.claims.length, 1);
    assert.equal(f.state.starts.length, 0);
    assert.equal(f.state.failures.length, 0);
    assert.equal(f.state.writes, 0);
  }
});

test("lost start response resumes the same target; failed owner lookup starts no work", async () => {
  const f = fixture({ busy: true });
  assert.equal((await f.fill("")).continue, true);
  assert.equal(f.state.claims[1].p_run_id, runId);
  assert.equal(f.state.starts.length, 1);
  const g = fixture({ busy: true, ownerError: true });
  assert.equal((await g.fill("")).status, "conflict");
  assert.equal(g.state.claims.length, 1);
  assert.equal(g.state.starts.length, 0);
});

test("lost checkpoint replays the same collector id rather than creating another paid job", async () => {
  const f = fixture();
  f.state.loseCheckpoint = true;
  await f.fill();
  await f.fill();
  assert.equal(f.state.starts.length, 2);
  assert.equal(f.state.starts[0].id, f.state.starts[1].id);
  assert.equal(f.state.jobs.size, 1);
});

test("concurrent continuations cannot regress saved progress or create distinct jobs", async () => {
  const f = fixture();
  await Promise.all([f.fill(), f.fill()]);
  assert.equal(f.state.jobs.size, 1);
  assert.equal(f.state.cursors.__coinglass_pending.jobs.length, 1);
  f.ready();
  await Promise.all([f.fill(), f.fill()]);
  assert.equal(f.state.cursors.__coinglass_pending.jobs[0].ready, true);
});

test("running deadline survives continuation and prevents another horizon", async () => {
  const f = fixture();
  await f.fill();
  f.advance(390_001);
  assert.equal((await f.fill()).status, "failed");
  assert.equal(f.state.starts.length, 1);
  assert.equal(f.state.applies.length, 0);
});

test("changed or removed source configuration closes through the existing conflict guard", async () => {
  const f = fixture();
  await f.fill();
  f.state.configCurrent = false;
  f.state.enabled = false;
  f.state.model = null;
  assert.equal((await f.fill()).status, "conflict");
  assert.equal(f.state.starts.length, 1);
  assert.deepEqual(Object.keys(f.state.applies[0].p_payload), []);
});

test("an expired field budget and stale completed results cannot be saved", async () => {
  const f = fixture();
  await f.fill();
  f.advance(18 * 60_000);
  assert.equal((await f.fill()).status, "failed");
  assert.equal(f.state.applies.length, 0);
  const g = fixture();
  for (let i = 0; i < 3; i++) {
    await g.fill();
    g.ready();
    await g.fill();
  }
  g.advance(901_000);
  assert.equal((await g.fill()).status, "failed");
  assert.equal(g.state.applies.length, 0);
});

const timeframes = ["12H", "24H", "48H"];
function savedValue(model = 3) {
  const capturedAt = "2026-10-24T10:00:00.000Z";
  return {
    manual_note: "keep worksheet metadata",
    rows: timeframes.map((timeframe, position) => ({
      id: `saved-${timeframe}`,
      timeframe,
      position,
      manual_note: `keep ${timeframe} metadata`,
      hover_snapshot: { heatmap_model: model, timeframe, source_time: capturedAt },
      levels: [
        {
          id: `saved-level-${timeframe}`,
          label: `old ${timeframe}`,
          price_low: 80,
          price_high: 81,
          taken: true,
          source_observation: { heatmap_model: model, captured_at: capturedAt },
        },
      ],
    })),
    _autofill: {
      kind: "liquidity",
      model,
      observations: timeframes.map((timeframe) => ({
        timeframe,
        captured_at: capturedAt,
        evidence: { artifact_id: randomUUID(), sha256: "b".repeat(64) },
      })),
    },
  };
}

async function finishQueue(f, failed = [], resultOverrides = {}) {
  for (const timeframe of timeframes) {
    assert.equal((await f.fill()).continue, true);
    assert.equal(f.state.starts.at(-1).timeframe, timeframe);
    if (failed.includes(timeframe)) f.fail(timeframe);
    else f.ready(timeframe, resultOverrides);
    assert.equal((await f.fill()).continue, true);
    assert.equal(f.state.applies.length, 0, "no horizon may close the parent early");
  }
  return f.fill();
}

for (const model of [1, 2, 3]) {
  for (const failed of timeframes) {
    test(`Model ${model}: failed ${failed} preserves other successes and the prior failed row`, async () => {
      const baseline = savedValue(model);
      const f = fixture({ value: baseline, model });
      const result = await finishQueue(f, [failed]);
      assert.equal(result.status, "failed");
      assert.match(result.message, /סריקה חלקית/);
      assert.match(result.message, new RegExp(`${failed}: source_capture_failed`));
      assert.equal(f.state.applies.length, 1);
      assert.equal(
        f.state.failures.length,
        0,
        "partial success uses the existing save-plus-failure RPC",
      );
      assert.deepEqual(
        f.state.starts.map((job) => job.timeframe),
        timeframes,
      );
      assert.ok(f.state.starts.every((job) => job.model === model));
      assert.equal(f.state.value.manual_note, baseline.manual_note);
      assert.equal(f.state.value.rows.length, 3);
      assert.deepEqual(
        f.state.value.rows.find((row) => row.timeframe === failed),
        baseline.rows.find((row) => row.timeframe === failed),
      );
      assert.deepEqual(
        f.state.value._autofill.observations.find(
          (observation) => observation.timeframe === failed,
        ),
        baseline._autofill.observations.find((observation) => observation.timeframe === failed),
        "the retained observation must keep its old capture date and evidence",
      );
      for (const timeframe of timeframes.filter((value) => value !== failed)) {
        const row = f.state.value.rows.find((value) => value.timeframe === timeframe);
        const observation = f.state.value._autofill.observations.find(
          (value) => value.timeframe === timeframe,
        );
        assert.equal(row.id, `saved-${timeframe}`);
        assert.equal(row.position, timeframes.indexOf(timeframe));
        assert.equal(row.levels[0].label, timeframe);
        assert.equal(row.hover_snapshot.heatmap_model, model);
        assert.equal(observation.heatmap_model, model);
        assert.equal(
          observation.collector_run_id,
          f.state.starts.find((job) => job.timeframe === timeframe).id,
        );
        assert.equal(observation.captured_at, "2026-10-24T12:00:00.000Z");
      }
      assert.equal(f.state.cursors.__coinglass_pending.run_id, f.state.runId);
      assert.equal(f.state.cursors.__coinglass_pending.jobs.filter((job) => job.ready).length, 2);
    });
  }
}

test("all failed horizons leave the existing value untouched and never apply an empty replacement", async () => {
  const baseline = savedValue();
  const f = fixture({ value: baseline });
  assert.equal((await finishQueue(f, timeframes)).status, "failed");
  assert.equal(f.state.applies.length, 0);
  assert.equal(f.state.failures.length, 1);
  assert.deepEqual(f.state.value, baseline);
  assert.equal(f.state.cursors.__coinglass_pending.jobs.filter((job) => job.failure).length, 3);
});

test("liquidity replaces its merged rows even when append is configured and synchronizes legacy rows", async () => {
  const baseline = savedValue();
  baseline.structured = { rows: baseline.rows, legacy_note: "keep legacy metadata" };
  delete baseline.rows;
  const f = fixture({ value: baseline, writeMode: "append" });
  assert.equal((await finishQueue(f, ["24H"])).status, "failed");
  assert.equal(f.state.applies[0].p_payload.write_mode, "replace");
  assert.equal(f.state.value.rows.length, 3);
  assert.deepEqual(f.state.value.structured.rows, f.state.value.rows);
  assert.equal(f.state.value.structured.legacy_note, "keep legacy metadata");
  assert.deepEqual(f.state.value.rows[1], baseline.structured.rows[1]);
});

test("replacing a successful timeframe removes its old duplicate without deleting failed or manual rows", async () => {
  const baseline = savedValue();
  baseline.rows.splice(1, 0, { ...structuredClone(baseline.rows[0]), id: "duplicate-12H" });
  baseline.rows.push({ id: "manual", timeframe: "1W", levels: [], manual_note: "keep" });
  baseline._autofill.observations.push(structuredClone(baseline._autofill.observations[0]));
  const f = fixture({ value: baseline });
  await finishQueue(f, ["24H"]);
  assert.equal(f.state.value.rows.filter((row) => row.timeframe === "12H").length, 1);
  assert.equal(f.state.value.rows.find((row) => row.timeframe === "12H").id, "saved-12H");
  assert.equal(
    f.state.value._autofill.observations.filter((row) => row.timeframe === "12H").length,
    1,
  );
  assert.deepEqual(
    f.state.value.rows.find((row) => row.timeframe === "24H"),
    baseline.rows[2],
  );
  assert.deepEqual(f.state.value.rows.at(-1), baseline.rows.at(-1));
});

test("a fresh partial retry reads successful jobs and preserves their IDs, evidence, capture dates and user edits", async () => {
  const f = fixture({ value: savedValue() });
  await finishQueue(f, ["24H"]);
  f.state.value.rows[0].levels[0].taken = true;
  f.state.value.rows[0].levels[0].manual_note = "edited after partial save";
  const committed = structuredClone(f.state.value);
  const originalStarts = structuredClone(f.state.starts);
  f.advance(60_000);
  f.nextRun();
  assert.equal((await f.fill()).continue, true, "reuse 12H");
  assert.equal(f.state.starts.length, 3);
  assert.equal((await f.fill()).continue, true, "start only the failed 24H");
  assert.equal(f.state.starts.length, 4);
  assert.equal(f.state.starts.at(-1).timeframe, "24H");
  assert.notEqual(f.state.starts.at(-1).id, originalStarts[1].id);
  f.ready("24H");
  await f.fill();
  assert.equal((await f.fill()).continue, true, "reuse 48H");
  assert.equal((await f.fill()).status, "completed");
  assert.equal(f.state.starts.length, 4);
  for (const timeframe of ["12H", "48H"]) {
    assert.deepEqual(
      f.state.value.rows.find((row) => row.timeframe === timeframe),
      committed.rows.find((row) => row.timeframe === timeframe),
    );
    assert.deepEqual(
      f.state.value._autofill.observations.find((row) => row.timeframe === timeframe),
      committed._autofill.observations.find((row) => row.timeframe === timeframe),
    );
    assert.ok(f.state.reads.includes(originalStarts.find((job) => job.timeframe === timeframe).id));
  }
  assert.equal(f.state.cursors.__coinglass_pending, undefined);
  const calls = {
    starts: f.state.starts.length,
    reads: f.state.reads.length,
    applies: f.state.applies.length,
  };
  assert.equal((await f.fill()).status, "completed");
  assert.deepEqual(
    { starts: f.state.starts.length, reads: f.state.reads.length, applies: f.state.applies.length },
    calls,
  );
});

for (const change of ["stale", "model", "configuration"]) {
  test(`a ${change} retry does not reuse earlier successful captures`, async () => {
    const f = fixture();
    await finishQueue(f, ["24H"]);
    if (change === "stale") f.advance(901_000);
    if (change === "model") f.state.model = 2;
    if (change === "configuration") f.state.fieldSettings = { autofill_timeframes: ["12H", "48H"] };
    f.nextRun();
    assert.equal((await f.fill()).continue, true);
    assert.equal(f.state.starts.length, 4);
    assert.equal(f.state.starts.at(-1).timeframe, "12H");
    assert.notEqual(f.state.starts.at(-1).id, f.state.starts[0].id);
    assert.equal(f.state.starts.at(-1).model, f.state.model);
  });
}

for (const change of ["model", "configuration", "job identity"]) {
  test(`a ${change} mismatch inside the same run cannot open or publish another timeframe`, async () => {
    const f = fixture({ value: savedValue() });
    await f.fill();
    const value = structuredClone(f.state.value);
    if (change === "model") f.state.model = 2;
    if (change === "configuration") f.state.fieldSettings = { autofill_timeframes: ["12H"] };
    if (change === "job identity") f.state.rejectJob = f.state.starts[0].id;
    assert.equal((await f.fill()).status, "failed");
    assert.equal(f.state.starts.length, 1);
    assert.equal(f.state.applies.length, 0);
    assert.deepEqual(f.state.value, value);
  });
}

test("an early stale success cannot discard a later fresh success or refresh its old saved row", async () => {
  const baseline = savedValue();
  const f = fixture({ value: baseline });
  await f.fill();
  f.ready("12H");
  await f.fill();
  await f.fill();
  f.fail("24H");
  await f.fill();
  f.advance(600_000);
  await f.fill();
  f.advance(301_000);
  f.ready("48H");
  await f.fill();
  assert.equal((await f.fill()).status, "failed");
  assert.equal(f.state.applies.length, 1);
  assert.deepEqual(f.state.value.rows[0], baseline.rows[0]);
  assert.deepEqual(f.state.value._autofill.observations[0], baseline._autofill.observations[0]);
  assert.equal(f.state.value.rows[2].levels[0].label, "48H");
  assert.equal(f.state.value._autofill.observations[2].captured_at, "2026-10-24T12:15:01.000Z");
  assert.match(
    f.state.applies[0].p_payload.source_failure_message,
    /12H: תצפית הנזילות אינה עדכנית/,
  );
});

for (const code of [
  "source_login_required",
  "analysis_unauthorized",
  "analysis_rate_limited",
  "analysis_quota_exceeded",
]) {
  test(`${code} stops later captures but preserves the already validated 12H result`, async () => {
    const baseline = savedValue();
    const f = fixture({ value: baseline });
    await f.fill();
    f.ready("12H");
    await f.fill();
    await f.fill();
    f.fail("24H", code);
    assert.equal((await f.fill()).status, "failed");
    assert.deepEqual(
      f.state.starts.map((job) => job.timeframe),
      ["12H", "24H"],
    );
    assert.equal(f.state.applies.length, 1);
    assert.equal(f.state.value.rows[0].levels[0].label, "12H");
    assert.deepEqual(f.state.value.rows[1], baseline.rows[1]);
    assert.deepEqual(f.state.value.rows[2], baseline.rows[2]);
  });
}

test("a lost child-start response stops automatic work and still saves the prior successful timeframe", async () => {
  const baseline = savedValue();
  const f = fixture({ value: baseline });
  await f.fill();
  f.ready("12H");
  await f.fill();
  f.state.lostResponse = true;
  assert.equal((await f.fill()).status, "failed");
  assert.equal(f.state.jobs.size, 2, "the second POST was accepted before its response was lost");
  assert.deepEqual(
    f.state.starts.map((job) => job.timeframe),
    ["12H", "24H"],
  );
  assert.equal(f.state.applies.length, 1);
  assert.equal(f.state.value.rows[0].levels[0].label, "12H");
  assert.deepEqual(f.state.value.rows[1], baseline.rows[1]);
  assert.deepEqual(f.state.value.rows[2], baseline.rows[2]);
  await f.fill();
  assert.equal(
    f.state.starts.length,
    2,
    "a terminal parent never silently retries an uncertain POST",
  );
});

test("a quota response before a job exists cannot open a later timeframe or erase an earlier success", async () => {
  const f = fixture({ value: savedValue() });
  await f.fill();
  f.ready("12H");
  await f.fill();
  f.state.sourceError = Object.assign(new Error("hourly quota reached"), { code: "RATE_LIMITED" });
  assert.equal((await f.fill()).status, "failed");
  assert.equal(f.state.jobs.size, 1);
  assert.equal(f.state.starts.length, 2);
  assert.equal(f.state.applies.length, 1);
  assert.equal(f.state.value.rows[0].levels[0].label, "12H");
});

test("a rejected collector identity stops later captures and only the prior verified result is saved", async () => {
  const baseline = savedValue();
  const f = fixture({ value: baseline });
  await f.fill();
  f.ready("12H");
  await f.fill();
  await f.fill();
  f.state.rejectJob = f.state.starts[1].id;
  assert.equal((await f.fill()).status, "failed");
  assert.equal(f.state.starts.length, 2);
  assert.equal(f.state.value.rows[0].levels[0].label, "12H");
  assert.deepEqual(f.state.value.rows[1], baseline.rows[1]);
  assert.deepEqual(f.state.value.rows[2], baseline.rows[2]);
});

test("a concurrent worksheet edit wins the final replace guard even with append configured", async () => {
  const f = fixture({ value: savedValue(), writeMode: "append" });
  for (const timeframe of timeframes) {
    await f.fill();
    if (timeframe === "24H") f.fail(timeframe);
    else f.ready(timeframe);
    await f.fill();
  }
  f.state.value.rows[1].levels[0].label = "concurrent edit";
  const edited = structuredClone(f.state.value);
  assert.equal((await f.fill()).status, "conflict");
  assert.equal(f.state.applies[0].p_payload.write_mode, "replace");
  assert.deepEqual(f.state.value, edited);
  assert.equal(f.state.cursors.__coinglass_pending.jobs.filter((job) => job.ready).length, 2);
});

for (const provenance of ["autofill", "hover", "level"]) {
  test(`known other-model ${provenance} provenance cannot be relabeled by a partial merge`, async () => {
    const baseline = savedValue(2);
    if (provenance !== "autofill") delete baseline._autofill;
    for (const row of baseline.rows) {
      if (provenance !== "hover") delete row.hover_snapshot;
      if (provenance !== "level") delete row.levels[0].source_observation;
    }
    const f = fixture({ value: baseline, model: 3 });
    const result = await finishQueue(f, ["24H"]);
    assert.equal(result.status, "failed");
    assert.match(result.message, /ממודל אחר/);
    assert.equal(f.state.applies.length, 0);
    assert.deepEqual(f.state.value, baseline);
    assert.equal(f.state.cursors.__coinglass_pending.jobs.filter((job) => job.ready).length, 2);
    f.nextRun();
    await f.fill();
    await f.fill();
    f.ready("24H");
    await f.fill();
    await f.fill();
    assert.equal((await f.fill()).status, "completed", "complete replacement may switch model");
    assert.equal(f.state.starts.length, 4);
    assert.ok(f.state.value.rows.every((row) => row.hover_snapshot.heatmap_model === 3));
    assert.equal(f.state.value._autofill.model, 3);
  });
}

test("unknown manual provenance is preserved during a partial merge", async () => {
  const baseline = savedValue();
  delete baseline._autofill;
  for (const row of baseline.rows) {
    delete row.hover_snapshot;
    delete row.levels[0].source_observation;
  }
  const f = fixture({ value: baseline });
  assert.equal((await finishQueue(f, ["24H"])).status, "failed");
  assert.equal(f.state.applies.length, 1);
  assert.deepEqual(f.state.value.rows[1], baseline.rows[1]);
  assert.deepEqual(
    f.state.value._autofill.observations.map((row) => row.timeframe),
    ["12H", "48H"],
  );
});

test("new captures replace obsolete source fields instead of retaining an older unavailable status or model", async () => {
  const baseline = savedValue(2);
  baseline.rows[0].heatmap_model = 2;
  baseline.rows[0].source_observation = { heatmap_model: 2 };
  baseline._autofill.observations[0].quantity_status = "unavailable";
  baseline._autofill.observations[0].quantity_reason = "old quantity failure";
  const f = fixture({ value: baseline, model: 3 });
  const quantities = { observed_at: "2026-10-24T12:00:00.000Z", buckets: [] };
  assert.equal((await finishQueue(f, [], { quantities })).status, "completed");
  assert.equal(f.state.value.rows[0].id, "saved-12H");
  assert.equal(f.state.value.rows[0].heatmap_model, undefined);
  assert.equal(f.state.value.rows[0].source_observation, undefined);
  assert.equal(f.state.value.rows[0].hover_snapshot.heatmap_model, 3);
  assert.equal(f.state.value._autofill.observations[0].quantity_status, undefined);
  assert.equal(f.state.value._autofill.observations[0].quantity_reason, undefined);
  assert.equal(f.state.value._autofill.observations[0].heatmap_model, 3);
  assert.deepEqual(f.state.value._autofill.observations[0].quantities, quantities);
});
