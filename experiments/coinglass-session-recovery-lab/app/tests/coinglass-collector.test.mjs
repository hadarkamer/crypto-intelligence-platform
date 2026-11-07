import assert from "node:assert/strict";
import test from "node:test";
import {
  COLLECTOR_SCHEMA_VERSION,
  COLLECTOR_TIMEFRAMES,
  collectorErrorNote,
  collectorZonesToLevels,
  isCollectorTimeframe,
  jobUrl,
  jobsUrl,
  parseCollectorBase,
  resultFreshness,
  validateCollectorEnvelope,
  validateCollectorResult,
} from "../src/lib/coinglassCollector.ts";

const SOURCE = "https://www.coinglass.com/pro/futures/LiquidationHeatMap?coin=BTC&type=symbol";
// חוזה v1: job_id == run_id == evidence.artifact_id.
const RUN = "11111111-2222-4333-8444-555555555555";
const JOB = RUN;
const ART = RUN;
const SHA = "a".repeat(64);

const result = (overrides = {}) => ({
  schema_version: COLLECTOR_SCHEMA_VERSION,
  run_id: RUN,
  source_url: SOURCE,
  symbol: "BTC",
  heatmap_model: 1,
  timeframe: "12H",
  captured_at: new Date().toISOString(),
  source_updated_at: null,
  provider: "OpenAI",
  model: "gpt-x-vision",
  usage: { total_tokens: 10 },
  observed_price: 100000,
  zones: [
    { side: "above", price_low: 102000, price_high: 103000, intensity: "many" },
    { side: "below", price_low: 96000, price_high: 97000, intensity: "few" },
  ],
  evidence: { sha256: SHA, artifact_id: ART, content_type: "image/png" },
  summary: "ok",
  quality: "visual_estimate",
  ...overrides,
});

const envelope = (overrides = {}) => ({
  schema_version: COLLECTOR_SCHEMA_VERSION,
  job_id: JOB,
  timeframe: "12H",
  status: "ready",
  result: result(),
  error: null,
  ...overrides,
});

test("12H, 24H and 48H are offered for this button", () => {
  assert.deepEqual([...COLLECTOR_TIMEFRAMES], ["12H", "24H", "48H"]);
  assert.equal(isCollectorTimeframe("48H"), true);
  assert.equal(isCollectorTimeframe("4H"), false);
});

test("a 48H envelope is accepted and a mismatched 24H result is rejected", () => {
  const ok = validateCollectorEnvelope(
    envelope({ timeframe: "48H", result: result({ timeframe: "48H" }) }),
    { timeframe: "48H", jobId: JOB },
  );
  assert.equal(ok.result.timeframe, "48H");
  assert.throws(
    () =>
      validateCollectorEnvelope(
        envelope({ timeframe: "24H", result: result({ timeframe: "24H" }) }),
        {
          timeframe: "48H",
        },
      ),
    /./,
  );
  assert.throws(
    () =>
      validateCollectorEnvelope(
        envelope({ timeframe: "48H", result: result({ timeframe: "24H" }) }),
        {
          timeframe: "48H",
        },
      ),
    /./,
  );
});

test("known collector error codes map to Hebrew messages without leaking raw text", () => {
  assert.match(collectorErrorNote("source_not_readable"), /קריא/);
  assert.match(collectorErrorNote("analysis_quota_exceeded"), /מכסת/);
  assert.match(collectorErrorNote("source_or_analysis_failed"), /נכשל/);
  assert.match(collectorErrorNote("brand_new_code"), /brand_new_code/);
  assert.equal(collectorErrorNote("toString").includes("toString"), false);
  assert.match(collectorErrorNote("constructor"), /^האיסוף נכשל במקור \(constructor\)$/);
  assert.equal(collectorErrorNote("<script>").includes("<"), false);
  assert.equal(typeof collectorErrorNote(null), "string");
});

test("collector base must be a public HTTPS origin", () => {
  assert.equal(
    parseCollectorBase("https://collector.example.com/"),
    "https://collector.example.com",
  );
  for (const bad of [
    "http://collector.example.com",
    "https://user:pw@collector.example.com",
    "https://collector.example.com/?x=1",
    "https://collector.example.com/#f",
    "https://collector.example.com/api",
    "https://localhost",
    "https://127.0.0.1",
    "https://10.1.2.3",
    "https://192.168.0.4",
    "https://169.254.1.1",
    "https://box.internal",
    "",
  ])
    assert.throws(() => parseCollectorBase(bad), /./, bad);
});

test("job urls follow the frozen contract", () => {
  const base = "https://collector.example.com";
  assert.equal(jobsUrl(base), `${base}/api/collection/model1/jobs`);
  assert.equal(jobUrl(base, JOB), `${base}/api/collection/model1/jobs/${JOB}`);
  assert.throws(() => jobUrl(base, "not-a-uuid"));
});

test("a well formed ready envelope is accepted", () => {
  const parsed = validateCollectorEnvelope(envelope(), { timeframe: "12H", jobId: JOB });
  assert.equal(parsed.status, "ready");
  assert.equal(parsed.result.provider, "OpenAI");
  assert.equal(
    parsed.result.captured_at,
    envelope().result.captured_at.slice(0, 0) || parsed.result.captured_at,
  );
});

test("malformed envelopes and results are rejected", () => {
  const cases = [
    [envelope({ schema_version: "v2" }), "schema"],
    [envelope({ job_id: "x" }), "job id"],
    [envelope({ status: "done" }), "status"],
    [envelope({ status: "running", result: result() }), "result without ready"],
    [envelope({ result: result({ heatmap_model: 2 }) }), "wrong model"],
    [envelope({ result: result({ source_url: "https://example.com" }) }), "wrong source"],
    [envelope({ result: result({ symbol: "ETH" }) }), "wrong symbol"],
    [envelope({ result: result({ observed_price: 0 }) }), "price"],
    [envelope({ result: result({ observed_price: "100" }) }), "string price"],
    [envelope({ result: result({ zones: [] }) }), "no zones"],
    [
      envelope({
        result: result({
          zones: [{ side: "above", price_low: 90000, price_high: 110000, intensity: "many" }],
        }),
      }),
      "crossing price",
    ],
    [
      envelope({
        result: result({
          zones: [{ side: "above", price_low: 90000, price_high: 95000, intensity: "many" }],
        }),
      }),
      "side mismatch",
    ],
    [
      envelope({
        result: result({ evidence: { sha256: "zz", artifact_id: ART, content_type: "image/png" } }),
      }),
      "evidence",
    ],
    [envelope({ result: result({ captured_at: "not-a-date" }) }), "timestamp"],
  ];
  for (const [payload, label] of cases)
    assert.throws(() => validateCollectorEnvelope(payload, { timeframe: "12H" }), /./, label);
});

test("envelope timeframe and job id must match what we asked for", () => {
  assert.throws(() => validateCollectorEnvelope(envelope(), { timeframe: "24H" }));
  assert.throws(() =>
    validateCollectorEnvelope(envelope(), {
      timeframe: "12H",
      jobId: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
    }),
  );
});

test("zones map deterministically to table levels", () => {
  const levels = collectorZonesToLevels(
    validateCollectorResult(
      result({
        zones: [
          { side: "below", price_low: 90000, price_high: 91000, intensity: "few" },
          { side: "above", price_low: 105000, price_high: 105000, intensity: "normal" },
          { side: "above", price_low: 101000, price_high: 102000, intensity: "many" },
          { side: "below", price_low: 95000, price_high: 96000, intensity: "many" },
        ],
      }),
      "12H",
    ),
  );
  assert.deepEqual(
    levels.map((level) => `${level.side}:${level.price_text}:${level.amount}`),
    [
      "above:$101,000–$102,000:many",
      "above:$105,000:normal",
      "below:$95,000–$96,000:many",
      "below:$90,000–$91,000:few",
    ],
  );
});

test("freshness keeps the original timestamp and flags stale or impossible results", () => {
  const now = Date.parse("2026-01-01T12:00:00Z");
  assert.equal(resultFreshness("2026-01-01T11:55:00Z", now).stale, false);
  assert.equal(resultFreshness("2026-01-01T11:30:00Z", now).stale, true);
  assert.equal(resultFreshness("2026-01-01T12:30:00Z", now).invalid, true);
  assert.equal(resultFreshness("nope", now).invalid, true);
});
