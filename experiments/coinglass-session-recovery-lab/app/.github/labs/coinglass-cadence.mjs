// Offline integration: real SQL, relay, bridge, scanner, model validator and renderer.
// PGLITE_ROOT points to an isolated installation of @electric-sql/pglite@0.5.13.
import assert from "node:assert/strict";
import { readFileSync, readdirSync, mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { createRequire } from "node:module";
import vm from "node:vm";
import { EventEmitter } from "node:events";
const root = path.resolve(import.meta.dirname, "../..");
const require = createRequire(path.join(root, "package.json"));
const ts = require("typescript");
if (!process.env.PGLITE_ROOT)
  throw Error("Set PGLITE_ROOT to the isolated PGlite package directory");
const pgRoot = path.resolve(process.env.PGLITE_ROOT);
const { PGlite } = await import(pathToFileURL(path.join(pgRoot, "dist/index.js")));
const { pgcrypto } = await import(pathToFileURL(path.join(pgRoot, "dist/contrib/pgcrypto.js")));
const dataDir = mkdtempSync(path.join(os.tmpdir(), "coinglass-cadence-"));
const bootstrap = await PGlite.create({ dataDir, extensions: { pgcrypto } });
await bootstrap.exec("create database legacy_transaction_test");
await bootstrap.close();
const pg = await PGlite.create({
  dataDir,
  database: "legacy_transaction_test",
  extensions: { pgcrypto },
});
const loadSql = (file) =>
  readFileSync(file, "utf8")
    .replace(/^\\(?:set|if|endif).*$/gm, "")
    .replace(/^\\ir (.+)$/gm, (_m, p) => loadSql(path.resolve(path.dirname(file), p.trim())));
async function sqlFile(relative) {
  try {
    await pg.exec(loadSql(path.join(root, relative)));
  } catch (e) {
    throw Error(`${relative}: ${e.message}`, { cause: e });
  }
}

const mode = process.argv[2];
if (!["a", "b"].includes(mode)) throw Error("Choose isolated check a or b");
let clock = Date.parse("2026-10-31T10:06:00Z");
const keys = [
  "legacy:079c525e-9463-4919-86af-e1a598110e76",
  "legacy:8b48ad86-5ed8-49f9-b324-7fedfbb757f3",
  "סריקת שוק/נזילות/נזילות קוינגאלס",
];
const urls = [
  null,
  "https://www.coinglass.com/pro/futures/LiquidationHeatMap?coin=BTC&type=symbol",
  "https://www.coinglass.com/pro/futures/LiquidationHeatMapNew?coin=BTC&type=symbol",
  "https://www.coinglass.com/pro/futures/LiquidationHeatMapModel3?coin=BTC&type=symbol",
];
const q = async (sql, args = []) => (await pg.query(sql, args)).rows;
const clone = (x) => JSON.parse(JSON.stringify(x));
const schedule = {
  first_hour: 6,
  active_hours: { enabled: true, start: 6, end: 0 },
  adapt_to_publications: false,
};
const repair = () => sqlFile(".github/operations/coinglass-six-hour-cadence.sql");
async function advance(value) {
  clock = Date.parse(value);
  await q("select set_config('lab.now',$1,false)", [new Date(clock).toISOString()]);
}
let fx;
try {
  await sqlFile(".github/sql/legacy-dry-run-bootstrap.sql");
  await sqlFile(".github/sql/live-september-schema-fixture.sql");
  let cloned = false;
  for (const file of readdirSync(path.join(root, "supabase/migrations"))
    .filter((n) => n >= "20260905")
    .sort()) {
    if (!cloned && file >= "20260907") {
      await sqlFile(".github/sql/live-september-clone-function-fixture.sql");
      cloned = true;
    }
    await sqlFile("supabase/migrations/" + file);
  }
  await advance(new Date(clock).toISOString());
  await pg.exec(`create or replace function pg_catalog.now() returns timestamptz language sql stable as $$select current_setting('lab.now')::timestamptz$$;
 create or replace function pg_catalog.clock_timestamp() returns timestamptz language sql volatile as $$select current_setting('lab.now')::timestamptz$$;`);
  console.log(JSON.stringify({ stage: "isolated-schema", mode, status: "passed" }));
  fx = await fixture();
  if (mode === "a") await checkA();
  else await checkB();
} finally {
  await pg.close();
  rmSync(dataDir, { recursive: true, force: true });
}

async function fixture() {
  await pg.exec("reset role;set search_path=public,extensions,pg_temp");
  const owner = crypto.randomUUID();
  await q("insert into auth.users(id,email) values($1,$2)", [
    owner,
    owner + "@offline.example.test",
  ]);
  await q("select set_config('request.jwt.claim.sub',$1,false)", [owner]);
  const ws = (await q("select public.v2_ensure_my_workspace() as id"))[0].id;
  const defs = keys.map((key, i) => ({
    field_key: "map" + (i + 1),
    title: "Model " + (i + 1),
    field_type: "liquidity_table",
    settings: {
      question_key: key,
      links: [{ url: urls[i + 1] }],
      autofill: { enabled: true, interval_hours: i === 0 ? 6 : 1, schedule },
    },
  }));
  for (const [key, url] of Object.entries({
    x: "https://x.com/CoinGlass",
    youtube: "https://www.youtube.com/@example",
    instagram: "https://www.instagram.com/example/",
    unbias: "https://unbias.fyi",
    rss: "https://example.com/feed",
  }))
    defs.push({
      field_key: key,
      title: key,
      field_type: "long_text",
      settings: {
        question_key: "unrelated_" + key,
        links: [{ url }],
        autofill: { enabled: false, interval_hours: 2, schedule },
      },
    });
  const sections = [{ title: "Data", settings: { sheet_key: "data" }, fields: defs }];
  const old = (
    await q(
      "select public.v2_save_template($1,null,'collection','Cadence fixture',true,true,$2::jsonb) as id",
      [ws, JSON.stringify(sections)],
    )
  )[0].id;
  const sid = (
    await q(
      "insert into public.v2_collection_sessions(workspace_id,template_id,title,created_by) values($1,$2,'Existing answered sheet',$3) returning id",
      [ws, old, owner],
    )
  )[0].id;
  const fields = await q(
    "select f.* from public.v2_template_fields f join public.v2_template_sections s on s.id=f.section_id where s.template_id=$1 order by f.position",
    [old],
  );
  await q(
    "insert into public.v2_collection_values(session_id,field_id,value) values($1,$2,$3::jsonb)",
    [
      sid,
      fields[0].id,
      JSON.stringify({
        rows: [],
        note: "Existing human note",
        images: [{ storage_path: "keep-image" }],
      }),
    ],
  );
  const tid = (
    await q(
      "select public.v2_save_template_editor($1,null,$2,'collection','Current saved default',true,true,$3::jsonb,null) as id",
      [ws, old, JSON.stringify(sections)],
    )
  )[0].id;
  await q(
    "select set_config('coinglass_cadence.workspace_id',$1,false),set_config('coinglass_cadence.template_id',$2,false)",
    [ws, tid],
  );
  return {
    owner,
    ws,
    old,
    tid,
    sid,
    fields,
    maps: fields.filter((f) => f.field_key.startsWith("map")),
  };
}
async function state() {
  return q(
    "select * from public.v2_collection_autofill_state where workspace_id=$1 order by logical_key",
    [fx.ws],
  );
}
async function values() {
  return q("select field_id,value from public.v2_collection_current_values($1) order by field_id", [
    fx.sid,
  ]);
}
async function runtime(field) {
  return (
    await q("select app_private.collection_autofill_current_config($1,$2) as config", [
      field.id,
      fx.sid,
    ])
  )[0].config;
}
async function next(field, at) {
  const cfg = await runtime(field);
  return (
    await q(
      "select app_private.collection_autofill_next_slot(app_private.collection_autofill_effective_schedule($1,'data',$2,$3::jsonb,$4::timestamptz),$4::timestamptz)::text as at",
      [fx.ws, field.settings.question_key, JSON.stringify(cfg.settings), at],
    )
  )[0].at;
}
async function assertReject(sql, message) {
  await pg.exec("savepoint invalid_case");
  try {
    await sql();
    assert.fail(message);
  } catch (e) {
    assert.ok(!String(e.message).includes(message), message);
  } finally {
    await pg.exec("rollback to invalid_case;release invalid_case");
  }
}
async function checkA() {
  const beforeValues = await values();
  const oldBefore = await q(
    "select f.id,f.settings from public.v2_template_fields f join public.v2_template_sections s on s.id=f.section_id where s.template_id=$1 order by f.id",
    [fx.old],
  );
  const before = (await q("select public.v2_collection_editor_snapshot($1) as s", [fx.tid]))[0].s;
  const previous = await Promise.all(fx.maps.map((f) => next(f, "2026-10-31T10:06:00Z")));
  assert.deepEqual(
    previous.map((x) => new Date(x).getUTCHours()),
    [16, 11, 11],
  );
  await pg.exec("begin");
  await repair();
  const after = (await q("select public.v2_collection_editor_snapshot($1) as s", [fx.tid]))[0].s;
  for (let i = 0; i < before.sections.length; i++)
    for (let j = 0; j < before.sections[i].fields.length; j++) {
      const a = before.sections[i].fields[j],
        b = after.sections[i].fields[j];
      const expected = clone(a.settings);
      if (keys.includes(expected.question_key)) expected.autofill.interval_hours = 6;
      assert.deepEqual(b.settings, expected, "Only the reviewed interval changed");
      assert.equal(b.id, a.id);
    }
  assert.deepEqual(await values(), beforeValues);
  assert.deepEqual(
    await q(
      "select f.id,f.settings from public.v2_template_fields f join public.v2_template_sections s on s.id=f.section_id where s.template_id=$1 order by f.id",
      [fx.old],
    ),
    oldBefore,
  );
  assert.deepEqual(
    await Promise.all(
      fx.maps.map(async (f) => (await runtime(f)).settings.autofill.interval_hours),
    ),
    [6, 6, 6],
  );
  assert.deepEqual(
    (await Promise.all(fx.maps.map((f) => next(f, "2026-10-31T10:06:00Z")))).map((x) =>
      new Date(x).getUTCHours(),
    ),
    [16, 16, 16],
  );
  assert.equal(
    new Date(await next(fx.maps[0], "2026-10-31T16:06:00Z")).toISOString(),
    "2026-11-01T04:00:00.000Z",
  );
  await repair();
  assert.equal(
    (await q("select editor_revision from public.v2_templates where id=$1", [fx.tid]))[0]
      .editor_revision,
    after.revision,
    "Second application is a no-op",
  );
  await q("select public.v2_collection_autofill_due(100)");
  const clocks = await state();
  await q("select public.v2_collection_autofill_due(100)");
  assert.deepEqual(await state(), clocks, "Unchanged schedule does not postpone clocks");
  await q(
    "update public.v2_collection_autofill_state set lease_until=now()+interval '5 minutes' where workspace_id=$1 and logical_key=$2",
    [fx.ws, keys[0]],
  );
  await assertReject(repair, "Active lease was accepted");
  await q("update public.v2_collection_autofill_state set lease_until=null where workspace_id=$1", [
    fx.ws,
  ]);
  await q("select set_config('request.jwt.claim.sub','',false)");
  await assertReject(repair, "Missing editor identity was accepted");
  await q("select set_config('request.jwt.claim.sub',$1,false)", [fx.owner]);
  await q("select set_config('coinglass_cadence.template_id',$1,false)", [fx.old]);
  await assertReject(repair, "Nondefault template was accepted");
  await q("select set_config('coinglass_cadence.template_id',$1,false)", [fx.tid]);
  for (const [name, edit] of [
    ["missing field", (s) => (s[0].fields[2].settings.question_key = "different")],
    ["duplicate field", (s) => (s[0].fields[2].settings.question_key = keys[1])],
    [
      "changed source",
      (s) => (s[0].fields[2].settings.links[0].url = urls[3].replace("BTC", "ETH")),
    ],
    ["changed cadence", (s) => (s[0].fields[2].settings.autofill.interval_hours = 4)],
    [
      "different active window",
      (s) => (s[0].fields[2].settings.autofill.schedule.active_hours.start = 8),
    ],
  ]) {
    await pg.exec("savepoint changed_configuration");
    const snap = (await q("select public.v2_collection_editor_snapshot($1) as s", [fx.tid]))[0].s;
    const sections = clone(snap.sections);
    edit(sections);
    await q("select public.v2_save_collection_template_live($1,$2,$2,$3,$4,true,true,$5::jsonb)", [
      fx.ws,
      fx.tid,
      snap.revision,
      snap.template.name,
      JSON.stringify(sections),
    ]);
    await assertReject(repair, "Accepted " + name);
    await pg.exec("rollback to changed_configuration;release changed_configuration");
  }
  await pg.exec("rollback");
  assert.deepEqual(
    (await q("select public.v2_collection_editor_snapshot($1) as s", [fx.tid]))[0].s,
    before,
    "Rollback restores the exact configuration",
  );
  console.log(
    JSON.stringify({
      stage: "A",
      status: "passed",
      beforeIntervals: [6, 1, 1],
      afterIntervals: [6, 6, 6],
      unchangedAnswers: true,
      unchangedOriginalTemplate: true,
      idempotent: true,
      activeLeaseRejected: true,
      unauthenticatedRejected: true,
      nondefaultRejected: true,
      rollbackVerified: true,
    }),
  );
}

async function checkB() {
  await pg.exec("begin");
  await repair();
  await pg.exec("commit");
  const { ws, sid, fields } = fx;
  const counters = {
    rpc: [],
    bridge: [],
    bridgeResults: [],
    source: [],
    ai: [],
    post: [],
    reads: [],
    ui: [],
  };
  const funcs = new Map(
    (
      await q(
        "select proname,proretset,prorettype::regtype::text as typ from pg_proc where pronamespace='public'::regnamespace",
      )
    ).map((r) => [r.proname, r]),
  );
  let rejectApply = false;
  const executeRpc = async (name, args = {}) => {
    counters.rpc.push(name);
    if (rejectApply && name === "v2_collection_autofill_apply")
      return { data: null, error: { message: "Injected transport failure before apply" } };
    try {
      const entries = Object.entries(args);
      const values = entries.map(([, v]) =>
        typeof v === "object" && v !== null ? JSON.stringify(v) : v,
      );
      const params = entries
        .map(
          ([k, v], i) =>
            `${k} => ${Array.isArray(v) && k === "p_keys" ? `ARRAY(SELECT jsonb_array_elements_text($${i + 1}::jsonb))` : `$${i + 1}`}`,
        )
        .join(",");
      const fn = funcs.get(name);
      assert.ok(fn, `Actual SQL function exists: ${name}`);
      const rows = await q(
        fn.proretset
          ? `select * from public.${name}(${params})`
          : `select public.${name}(${params}) as data`,
        values,
      );
      return { data: fn.proretset ? rows : rows[0]?.data, error: null };
    } catch (e) {
      return { data: null, error: { message: e.message, code: e.code } };
    }
  };
  const lazy = (execute) => ({
    then: (ok, bad) => execute().then(ok, bad),
    abortSignal: async (signal) => {
      signal.throwIfAborted();
      return execute();
    },
  });
  const db = {
    rpc: (name, args) => lazy(() => executeRpc(name, args)),
    from(table) {
      assert.match(table, /^v2_[a-z_]+$/);
      let columns = "*",
        patch,
        filters = [],
        single = false;
      const run = async () => {
        try {
          const values = [];
          const param = (v) => {
            values.push(typeof v === "object" && v !== null ? JSON.stringify(v) : v);
            return `$${values.length}`;
          };
          const set = patch
            ? Object.entries(patch)
                .map(([k, v]) => `${k}=${param(v)}`)
                .join(",")
            : "";
          const where =
            filters
              .map(([k, op, v]) =>
                op === "in"
                  ? `${k}=any(ARRAY(SELECT jsonb_array_elements_text(${param(v)}::jsonb))::uuid[])`
                  : `${k}${op}${param(v)}`,
              )
              .join(" and ") || "true";
          const rows = await q(
            patch
              ? `update public.${table} set ${set} where ${where} returning ${columns}`
              : `select ${columns} from public.${table} where ${where}`,
            values,
          );
          return { data: single ? (rows[0] ?? null) : rows, error: null };
        } catch (e) {
          return { data: null, error: { message: e.message, code: e.code } };
        }
      };
      const b = {
        ...lazy(run),
        abortSignal(signal) {
          signal.throwIfAborted();
          return this;
        },
        select(c = "*") {
          columns = c;
          return this;
        },
        update(p) {
          patch = p;
          return this;
        },
        eq(k, v) {
          filters.push([k, "=", v]);
          return this;
        },
        gt(k, v) {
          filters.push([k, ">", v]);
          return this;
        },
        in(k, v) {
          filters.push([k, "in", v]);
          return this;
        },
        single() {
          single = true;
          return run();
        },
      };
      return b;
    },
  };

  const jobs = new Map(),
    requests = new Map();
  let failedMap = null;
  let api;
  function savedResult(model, timeframe, id) {
    const src = readFileSync(path.join(root, "tests/coinglass-hover.test.mjs"), "utf8");
    let fragment = src.slice(
      src.indexOf("const RUN ="),
      src.indexOf("for (const model of [1, 2])"),
    );
    const at = new Date(clock).toISOString(),
      sourceTime = new Date(clock - 60000).toISOString();
    fragment = fragment
      .replace(/const RUN = [^;]+;/, "const RUN = " + JSON.stringify(id) + ";")
      .replace(/const at = [^;]+;/, "const at = " + JSON.stringify(at) + ";")
      .replaceAll('"2026-10-17T12:05:00Z"', JSON.stringify(sourceTime));
    const c = { ...api };
    vm.runInNewContext(fragment + ";globalThis.makeResult=result;", c);
    const value = clone(c.makeResult(model, timeframe));
    if (model !== 3) {
      const d = new Date(sourceTime),
        stamp =
          d.getUTCDate() +
          " " +
          d.toLocaleString("en-US", { month: "short", timeZone: "UTC" }) +
          " " +
          d.getUTCFullYear() +
          ", " +
          d.toISOString().slice(11, 16);
      Object.assign(value, {
        observed_price: 100.5,
        observed_price_range: null,
        price_reference_type: "exact_point",
      });
      Object.assign(value.hover_snapshot, {
        observed_price: 100.5,
        observed_price_range: null,
        source_candle: {
          open: 99,
          high: 102,
          low: 98,
          close: 100.5,
          source_time: sourceTime,
          raw_text: stamp + " Open 99 High 102 Low 98 Close 100.5",
        },
      });
    }
    return value;
  }
  const provider = async (raw, init = {}) => {
    const url = new URL(raw);
    assert.equal(url.origin, "https://collector.example.com", "All live I/O prohibited");
    if (url.pathname === "/health")
      return Response.json({
        ok: true,
        supported_models: [1, 2, 3],
        supported_timeframes: ["12H", "24H", "48H"],
        selection_policy: "prominent_right_edge.v1",
      });
    const match = url.pathname.match(/^\/api\/collection\/model([123])\/jobs(?:\/([a-f0-9-]+))?$/);
    assert.ok(match, "Unexpected collector endpoint");
    const model = Number(match[1]);
    let job;
    if (init.method === "POST") {
      const body = JSON.parse(init.body);
      counters.post.push({ model, ...body });
      if (requests.has(body.request_id)) job = jobs.get(requests.get(body.request_id));
      else {
        assert.ok(
          ![...jobs.values()].some((j) => ["queued", "running"].includes(j.status)),
          "One shared active collector worker",
        );
        job = {
          id: crypto.randomUUID(),
          model,
          timeframe: body.timeframe,
          status: "queued",
          reads: 0,
          result: null,
        };
        jobs.set(job.id, job);
        requests.set(body.request_id, job.id);
      }
    } else {
      job = jobs.get(match[2]);
      assert.equal(job?.model, model);
      counters.reads.push(job.id);
      if (job.status === "queued") job.status = "running";
      else if (job.status === "running") {
        counters.source.push({ model, timeframe: job.timeframe, id: job.id });
        if (failedMap === model + ":" + job.timeframe) {
          job.status = "failed";
          job.error = { code: "source_not_readable", message: "Controlled source failure" };
        } else {
          counters.ai.push(job.id);
          job.result = savedResult(model, job.timeframe, job.id);
          job.status = "ready";
        }
      }
    }
    return Response.json({
      schema_version: api.collectorSchemaVersion(model),
      job_id: job.id,
      timeframe: job.timeframe,
      status: job.status,
      result: job.result,
      error: job.error ?? null,
    });
  };
  const notices = [];
  let fill,
    uiCalls = [];
  const cache = new Map();
  function load(file) {
    file = path.resolve(root, file);
    if (!path.extname(file) || file.endsWith(".server") || file.endsWith(".functions"))
      file += ".ts";
    if (cache.has(file)) return cache.get(file).exports;
    const m = { exports: {} };
    cache.set(file, m);
    const code = ts.transpileModule(
      readFileSync(file, "utf8").split("\nif (import.meta.main)")[0],
      {
        compilerOptions: {
          module: ts.ModuleKind.CommonJS,
          target: ts.ScriptTarget.ES2022,
          jsx: ts.JsxEmit.ReactJSX,
          esModuleInterop: true,
        },
      },
    ).outputText;
    vm.runInNewContext(
      code.replaceAll("import.meta.main", "false"),
      {
        module: m,
        exports: m.exports,
        Buffer,
        URL,
        URLSearchParams,
        Request,
        Response,
        Headers,
        TextDecoder,
        TextEncoder,
        Date: class extends Date {
          constructor(...args) {
            super(...(args.length ? args : [clock]));
          }
          static now() {
            return clock;
          }
        },
        Intl,
        Error,
        AbortSignal,
        AbortController,
        crypto,
        structuredClone,
        setTimeout: file.endsWith("CollectionAutofillButton.tsx")
          ? (fn, ms, ...args) => setTimeout(fn, Math.min(ms, 1), ...args)
          : setTimeout,
        clearTimeout,
        console,
        process: {
          env: {
            OPENAI_API_KEY: "offline-fixture",
            COINGLASS_COLLECTOR_URL: "https://collector.example.com",
            COINGLASS_COLLECTOR_TOKEN: "z".repeat(64),
          },
        },
        fetch: provider,
        require(id) {
          if (id === "node:https")
            return {
              request: () => {
                throw Error("Live source I/O prohibited");
              },
            };
          if (id === "node:dns/promises")
            return { lookup: async () => [{ address: "8.8.8.8", family: 4 }] };
          if (id === "@tanstack/react-start") return { useServerFn: (fn) => fn };
          if (id === "@/lib/collectionAutofill.functions")
            return {
              fillCollectionFieldNow: async ({ data }) => {
                uiCalls.push(data);
                return fill(db, data.fieldId, data.sessionId, true, data.runId);
              },
            };
          if (id === "@/integrations/supabase/client") return { supabase: db };
          if (id === "sonner")
            return {
              toast: Object.fromEntries(
                ["success", "error", "warning", "info"].map((k) => [
                  k,
                  (message) => notices.push({ kind: k, message }),
                ]),
              ),
            };
          if (id.startsWith("@/")) {
            let target = path.join(root, "src", id.slice(2));
            try {
              return load(target);
            } catch (e) {
              if (e.code === "ENOENT") return load(target + ".tsx");
              throw e;
            }
          }
          if (id.startsWith(".")) return load(path.resolve(path.dirname(file), id));
          if (id.startsWith("node:"))
            assert.ok(
              ["node:crypto", "node:net", "node:buffer", "node:zlib"].includes(id),
              `Unmocked I/O module ${id}`,
            );
          return require(id);
        },
      },
      { filename: file },
    );
    return m.exports;
  }
  fill = load("src/lib/collectionAutofill.server.ts").fillCollectionField;
  const relay = load("scripts/collection-autofill-relay.ts").runCollectionAutofillRelay;
  const bridge = load("src/lib/collectionAutofillBridge.server.ts").handleCollectionAutofillBridge;
  const workerToken = "0".repeat(64);
  const runScheduled = () =>
    relay({
      appUrl: "https://offline.example.test",
      workerToken,
      wait: async () => {},
      now: () => clock,
      fetchImpl: async (url, init) => {
        counters.bridge.push(JSON.parse(init.body));
        const response = await bridge(new Request(url, init), {
          environment: {
            COLLECTION_AUTOFILL_WORKER_TOKEN: workerToken,
            OPENAI_API_KEY: "offline",
            SUPABASE_URL: "offline",
            SUPABASE_SERVICE_ROLE_KEY: "offline",
          },
          loadServices: async () => ({ db, fill }),
        });
        counters.bridgeResults.push(await response.clone().json());
        return response;
      },
    });

  api = load("src/lib/coinglassCollector.ts");
  const getRows = async () => clone(await values());
  const expectNine = async () => {
    const rows = await getRows();
    for (const f of fx.maps) {
      const value = rows.find((r) => r.field_id === f.id)?.value;
      assert.equal(value?.rows?.length, 3, "All horizons saved for " + f.field_key);
      assert.deepEqual(value.rows.map((r) => r.timeframe).sort(), ["12H", "24H", "48H"]);
      for (const row of value.rows) {
        assert.equal(row.hover_snapshot.heatmap_model, Number(f.field_key.at(-1)));
        assert.equal(row.levels.length, 1);
      }
    }
    return rows;
  };
  await q("select public.v2_collection_autofill_due(100)");
  assert.ok(
    (await state())
      .filter((s) => keys.includes(s.logical_key))
      .every((s) => new Date(s.next_due_at).getUTCHours() === 16),
  );
  await advance("2026-10-31T16:03:00Z");
  const first = await runScheduled();
  if (first.failed)
    console.log(
      JSON.stringify({
        first,
        results: counters.bridgeResults,
        runs: await q(
          "select status,message from public.v2_collection_autofill_runs where workspace_id=$1",
          [ws],
        ),
      }),
    );
  assert.deepEqual(clone(first), { processed: 3, failed: 0 });
  const firstRows = await expectNine();
  assert.equal(jobs.size, 9);
  assert.equal(counters.ai.length, 9);
  assert.ok((await state()).every((s) => s.lease_id === null && s.lease_until === null));
  assert.ok(
    (
      await q(
        "select manual,status from public.v2_collection_autofill_runs where workspace_id=$1",
        [ws],
      )
    ).every((r) => !r.manual && r.status === "completed"),
  );
  await advance("2026-10-31T17:03:00Z");
  assert.deepEqual(clone(await runScheduled()), { processed: 0, failed: 0 });
  assert.equal(jobs.size, 9);
  await advance("2026-10-31T22:03:00Z");
  assert.deepEqual(clone(await runScheduled()), { processed: 0, failed: 0 });
  assert.equal(jobs.size, 9);
  await advance("2026-11-01T04:03:00Z");
  failedMap = "2:24H";
  const partial = await runScheduled();
  assert.deepEqual(clone(partial), { processed: 3, failed: 1 });
  assert.equal(jobs.size, 18);
  const partialRows = await expectNine(),
    m2 = fx.maps[1];
  const rowFor = (rows, f, tf) =>
    rows.find((r) => r.field_id === f.id).value.rows.find((r) => r.timeframe === tf);
  assert.equal(
    rowFor(partialRows, m2, "24H").hover_snapshot.artifact_id,
    rowFor(firstRows, m2, "24H").hover_snapshot.artifact_id,
  );
  for (const tf of ["12H", "48H"])
    assert.notEqual(
      rowFor(partialRows, m2, tf).hover_snapshot.artifact_id,
      rowFor(firstRows, m2, tf).hover_snapshot.artifact_id,
    );
  assert.ok((await state()).every((s) => s.lease_id === null));
  const beforeRetryIds = ["12H", "48H"].map(
    (tf) => rowFor(partialRows, m2, tf).hover_snapshot.artifact_id,
  );
  failedMap = null;
  // Real React button and real read-only liquidity renderer; only the transports
  // to the server function and collector are replaced by the local mechanism.
  const { JSDOM } = require("jsdom");
  const dom = new JSDOM('<!doctype html><div id="root"></div>', {
    url: "https://offline.example.test",
  });
  for (const k of ["window", "document", "navigator", "HTMLElement", "Event", "MouseEvent", "Node"])
    Object.defineProperty(globalThis, k, {
      value: k === "window" ? dom.window : dom.window[k],
      configurable: true,
    });
  globalThis.IS_REACT_ACT_ENVIRONMENT = true;
  const React = require("react"),
    { createRoot } = require("react-dom/client"),
    { QueryClient, QueryClientProvider, useQuery } = require("@tanstack/react-query");
  const { CollectionAutofillButton } = load("src/components/v2/CollectionAutofillButton.tsx");
  const { StructuredView } = load("src/components/StructuredTables.tsx");
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: 0 } } });
  let uiRows = [];
  function App() {
    const query = useQuery({ queryKey: ["v2-collection-values", sid], queryFn: getRows });
    uiRows = query.data ?? [];
    return React.createElement(
      React.Fragment,
      null,
      React.createElement(CollectionAutofillButton, { field: m2, sessionId: sid }),
      ...fx.maps.map((f) =>
        React.createElement(
          "section",
          { key: f.id, "data-model": f.field_key },
          React.createElement(StructuredView, {
            snapshot: {
              type: "liquidity_table",
              rows: uiRows.find((r) => r.field_id === f.id)?.value.rows ?? [],
            },
          }),
        ),
      ),
    );
  }
  const mount = createRoot(document.getElementById("root"));
  await React.act(async () => {
    mount.render(React.createElement(QueryClientProvider, { client }, React.createElement(App)));
    await new Promise((r) => setTimeout(r, 30));
  });
  const clocksBefore = clone(await state());
  await React.act(async () => {
    document.querySelector("button").click();
    await new Promise((r) => setTimeout(r, 600));
  });
  if (notices.some((n) => n.kind === "error")) console.log(JSON.stringify({ notices, uiCalls }));
  assert.ok(uiCalls.length > 1, "Button follows the same durable job through continuation");
  assert.equal(jobs.size, 19, "Only failed horizon recaptured");
  const afterRetry = await expectNine();
  assert.deepEqual(
    ["12H", "48H"].map((tf) => rowFor(afterRetry, m2, tf).hover_snapshot.artifact_id),
    beforeRetryIds,
  );
  assert.notEqual(
    rowFor(afterRetry, m2, "24H").hover_snapshot.artifact_id,
    rowFor(firstRows, m2, "24H").hover_snapshot.artifact_id,
  );
  assert.deepEqual(uiRows, afterRetry, "Query refresh shows persisted data");
  for (const section of document.querySelectorAll("[data-model]"))
    for (const tf of ["12H", "24H", "48H"])
      assert.ok(section.textContent.includes(tf), "Rendered timeframe " + tf);
  assert.deepEqual(
    clone((await state()).map((s) => s.next_due_at)),
    clocksBefore.map((s) => s.next_due_at),
    "Manual retry preserves automatic clock",
  );
  await React.act(async () => mount.unmount());
  client.clear();
  dom.window.close();
  assert.deepEqual(await getRows(), afterRetry, "Reload reads the same saved results");
  assert.ok((await state()).every((s) => s.lease_id === null && s.lease_until === null));
  // Existing SQL regressions also cover authorization, configuration changes,
  // partial writes, cursor preservation and shared non-map connections.
  for (const test of [
    "collection-autofill-current-settings",
    "collection-independent-sources",
    "collection-coinglass-resume",
    "collection-capital-market",
    "collection-calendar-schedule",
  ]) {
    await sqlFile("tests/sql/" + test + ".sql");
  }
  console.log(
    JSON.stringify({
      stage: "B",
      status: "passed",
      scheduledCycles: 2,
      modelTimeframePairs: 9,
      createdJobs: jobs.size,
      collectorReads: counters.reads.length,
      syntheticSourceObservations: counters.source.length,
      syntheticAiResults: counters.ai.length,
      manualServerCalls: uiCalls.length,
      nightSkipped: true,
      partialPreserved: true,
      onlyFailedHorizonRetried: true,
      refreshAndReload: true,
      leasesReleased: true,
      sqlRegressionSuites: 5,
      externalProviders: "controlled fixtures; no live calls",
    }),
  );
}
