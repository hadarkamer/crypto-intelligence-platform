import assert from "node:assert/strict";
import test from "node:test";
import {
  validateHoverSnapshot,
  hoverQuantityForZone,
  hoverLevelText,
  hoverSideText,
} from "../src/lib/coinglassHover.ts";
import {
  collectorSourceUrl,
  collectorSchemaVersion,
  validateCollectorResult,
  collectorZonesToLevels,
  manualLiquidityPricePatch,
  savedLiquidityPriceText,
} from "../src/lib/coinglassCollector.ts";

const RUN = "11111111-2222-4333-8444-555555555555";
const at = "2026-10-17T12:06:00Z";
const range = {
  low: 99.9,
  high: 100.1,
  confidence: "high",
  basis: "last_candle_axis_bracket",
  axis_low: 99,
  axis_high: 101,
};
function snapshot(model = 1, timeframe = "12H") {
  const rows = [110.25, 105.25, 100.25, 95.25, 90.25].map((price, i) => ({
    id: `r${i}`,
    price,
    value: (i + 1) * 1e6,
    raw_price: String(price),
    raw_value: `${i + 1}.00M`,
    rounding_error: 5000,
  }));
  return {
    version: "displayed-hover.v1",
    metric: "Liquidation Leverage",
    unit: "source_metric",
    scope: "displayed_latest_column",
    heatmap_model: model,
    timeframe,
    source_url: collectorSourceUrl(model),
    source_time: "2026-10-17T12:05:00Z",
    captured_at: at,
    artifact_id: RUN,
    sha256: "a".repeat(64),
    complete: true,
    price_step: 5,
    total_value: 15e6,
    rounding_error: 25000,
    duration_seconds: 5,
    observed_price_range: { low: range.low, high: range.high },
    rows,
  };
}
function result(model = 1, timeframe = "12H") {
  return {
    schema_version: collectorSchemaVersion(model),
    level_contract_version: "prominent-rows.v1",
    selection_policy: "prominent_right_edge.v1",
    run_id: RUN,
    source_url: collectorSourceUrl(model),
    symbol: "BTC",
    heatmap_model: model,
    timeframe,
    captured_at: at,
    source_updated_at: null,
    provider: "OpenAI",
    model: "fixture",
    usage: null,
    observed_price: null,
    price_reference_type: "visual_range",
    observed_price_range: range,
    quality: "visual_estimate",
    price_axis_range: { low: 80, high: 120, price_evidence: "80 to 120" },
    evidence: { sha256: "a".repeat(64), artifact_id: RUN, content_type: "image/png" },
    zones: [
      {
        side: "above",
        price_low: 110.25,
        price_high: 110.25,
        intensity: "many",
        shape: "point",
        price_precision: "verified_label",
        prices: [110.25],
        evidence_basis: "tooltip",
        price_evidence: "Price 110.25",
        uncertainty_usd: 0,
        strength_score: 0.9,
        source_row_ids: ["r0"],
        source_value: 1e6,
      },
    ],
    hover_snapshot: snapshot(model, timeframe),
    quantity_status: "unavailable",
    quantity_reason: "source_usd_unverified",
  };
}

for (const model of [1, 2])
  test(`Model ${model} Close binds source text, time and pool sides through save/display conversion`, () => {
    const raw = result(model);
    const candle = {
      open: 99,
      high: 102,
      low: 98,
      close: 100.5,
      source_time: raw.hover_snapshot.source_time,
      raw_text: "17 Oct 2026, 12:05 Open 99 High 102 Low 98 Close 100.5",
    };
    Object.assign(raw, {
      observed_price: 100.5,
      observed_price_range: null,
      price_reference_type: "exact_point",
    });
    Object.assign(raw.hover_snapshot, {
      observed_price: 100.5,
      observed_price_range: null,
      source_candle: candle,
    });
    const accepted = validateCollectorResult(raw, "12H", { model, jobId: RUN });
    assert.equal(accepted.observed_price, 100.5);
    assert.equal(accepted.observed_price_range, null);
    assert.equal(collectorZonesToLevels(accepted).length, 1);
    assert.match(
      hoverSideText({ timeframe: "12H", hover_snapshot: accepted.hover_snapshot }, "above"),
      /3M/,
    );
    for (const day of ["5", "05"]) {
      const earlyMonth = structuredClone(raw);
      earlyMonth.captured_at = earlyMonth.hover_snapshot.captured_at = "2026-10-05T12:06:00Z";
      earlyMonth.hover_snapshot.source_time = earlyMonth.hover_snapshot.source_candle.source_time =
        "2026-10-05T12:05:00Z";
      earlyMonth.hover_snapshot.source_candle.raw_text = `${day} Oct 2026, 12:05 Open 99 High 102 Low 98 Close 100.5`;
      assert.equal(validateCollectorResult(earlyMonth, "12H", { model }).observed_price, 100.5);
    }
    for (const change of [
      (r) => (r.hover_snapshot.source_candle.close = 101),
      (r) => (r.hover_snapshot.source_candle.source_time = "2026-10-17T12:00:00Z"),
      (r) =>
        (r.hover_snapshot.source_candle.raw_text =
          "17 Oct 2026, 12:05 Price 100.5 Liquidation Leverage 1M"),
      (r) => (r.observed_price = 101),
      (r) => (r.hover_snapshot.source_candle.high = 99),
      (r) => {
        r.hover_snapshot.source_candle.high = 1e12;
        r.hover_snapshot.source_candle.raw_text = candle.raw_text.replace(
          "High 102",
          "High 1000000000000",
        );
      },
      (r) => {
        r.hover_snapshot.source_time = r.hover_snapshot.source_candle.source_time =
          "2026-10-17T12:05:30Z";
      },
      (r) => (r.hover_snapshot.observed_price_range = range),
      (r) => (r.hover_snapshot.heatmap_model = model === 1 ? 2 : 1),
      (r) => (r.zones[0].side = "below"),
      (r) => delete r.hover_snapshot.source_candle,
    ]) {
      const bad = structuredClone(raw);
      change(bad);
      assert.throws(() => validateCollectorResult(bad, "12H", { model, jobId: RUN }));
    }
  });

test("Model 2 preserves clipped edge prices but rejects interior gaps and missing edge rows", () => {
  const valid = snapshot(2);
  valid.rows[0].price = 109.75;
  valid.rows[0].raw_price = "109.75";
  valid.rows[4].price = 90.75;
  valid.rows[4].raw_price = "90.75";
  assert.deepEqual(validateHoverSnapshot(valid).rows, valid.rows);
  for (const [index, price] of [
    [0, 116],
    [2, 100.75],
    [4, 84],
  ]) {
    const bad = structuredClone(valid);
    bad.rows[index].price = price;
    bad.rows[index].raw_price = String(price);
    assert.throws(() => validateHoverSnapshot(bad), /invalid_coinglass_hover_snapshot/);
  }
  for (const model of [1, 3]) {
    const bad = { ...valid, heatmap_model: model, source_url: collectorSourceUrl(model) };
    assert.throws(() => validateHoverSnapshot(bad), /invalid_coinglass_hover_snapshot/);
  }
});
test("all nine model/timeframe contracts preserve exact prices and whole-column denominator", () => {
  for (const model of [1, 2, 3])
    for (const tf of ["12H", "24H", "48H"]) {
      const saved = validateCollectorResult(result(model, tf), tf, { model });
      const [level] = collectorZonesToLevels(saved);
      const row = { timeframe: tf, hover_snapshot: saved.hover_snapshot };
      assert.equal(level.source_value, 1e6);
      assert.match(level.price_text, /110\.25/);
      assert.match(hoverLevelText(row, level, "above"), /6\.7%/);
      assert.doesNotMatch(hoverLevelText(row, level, "above"), /\$/);
      assert.equal(
        hoverQuantityForZone(saved.hover_snapshot, saved.zones[0]).percent,
        (1e6 / 15e6) * 100,
      );
    }
});
test("reject tampering, incomplete coverage, missing rows, stale or wrong-context snapshots", () => {
  const mutations = [
    (s) => s.total_value++,
    (s) => (s.complete = false),
    (s) => (s.unit = "USD"),
    (s) => s.rows.splice(1, 1),
    (s) => (s.rows[1].raw_value = "999M"),
    (s) => (s.rows[1].id = "r0"),
    (s) => (s.captured_at = "2026-10-17T13:00:00Z"),
    (s) => (s.rows[1].rounding_error = 0),
  ];
  for (const mutate of mutations) {
    const s = snapshot();
    mutate(s);
    assert.throws(() => validateHoverSnapshot(s));
  }
  for (const key of ["timeframe", "heatmap_model", "artifact_id", "sha256", "source_url"]) {
    const r = result();
    r.hover_snapshot[key] = key === "heatmap_model" ? 2 : key === "timeframe" ? "24H" : "wrong";
    assert.throws(() => validateCollectorResult(r, "12H", { model: 1 }));
  }
});
test("axis and exact tooltip evidence accept a sentence period without accepting partial numbers", () => {
  const r = result(3, "48H");
  r.price_axis_range.price_evidence = "Visible y-axis ticks: 80 and 120.";
  r.zones[0].price_evidence = "Price 110.25.";
  assert.ok(validateCollectorResult(r, "48H", { model: 3 }));
  for (const evidence of ["80 and 120.5", "80 and 120.5.6", "80 and 1120."]) {
    const bad = structuredClone(r);
    bad.price_axis_range.price_evidence = evidence;
    assert.throws(() => validateCollectorResult(bad, "48H", { model: 3 }));
  }
  for (const evidence of ["110.251", "110.25.6", "1110.25."]) {
    const bad = structuredClone(r);
    bad.zones[0].price_evidence = evidence;
    assert.throws(() => validateCollectorResult(bad, "48H", { model: 3 }));
  }
});
test("Model 3 accepts measured complete sweeps without widening other model budgets", () => {
  for (const [timeframe, duration] of [
    ["12H", 87.996],
    ["24H", 76.004],
    ["48H", 90.296],
  ]) {
    const r = result(3, timeframe);
    r.hover_snapshot.duration_seconds = duration;
    const saved = validateCollectorResult(r, timeframe, { model: 3 });
    const [level] = collectorZonesToLevels(saved);
    assert.match(
      hoverLevelText({ timeframe, hover_snapshot: saved.hover_snapshot }, level, "above"),
      /6\.7%/,
    );
  }
  for (const [model, limit] of [
    [1, 60],
    [2, 60],
    [3, 240],
  ]) {
    const s = snapshot(model);
    s.duration_seconds = limit;
    assert.ok(validateHoverSnapshot(s));
    s.duration_seconds = limit + 0.001;
    assert.throws(() => validateHoverSnapshot(s));
    s.duration_seconds = -1;
    assert.throws(() => validateHoverSnapshot(s));
  }
  const incomplete = snapshot(3);
  incomplete.duration_seconds = 90.296;
  incomplete.complete = false;
  assert.throws(() => validateHoverSnapshot(incomplete));
  const stale = snapshot(3);
  stale.duration_seconds = 90.296;
  stale.source_time = "2026-10-17T11:00:00Z";
  assert.throws(() => validateHoverSnapshot(stale));
});
test("pool sums include contiguous rows once and never trust model-proposed totals", () => {
  const s = validateHoverSnapshot(snapshot());
  const zone = {
    side: "above",
    price_low: 105.25,
    price_high: 110.25,
    source_row_ids: ["r0", "r1"],
  };
  assert.equal(hoverQuantityForZone(s, zone).source_value, 3e6);
  for (const patch of [
    { source_value: 8e6 },
    { source_row_ids: ["r0", "r0"] },
    { source_row_ids: ["r0", "r2"] },
    { source_row_ids: ["r9"] },
    { side: "below" },
  ])
    assert.throws(() => hoverQuantityForZone(s, { ...zone, ...patch }));
  const r = result();
  r.zones.push(structuredClone(r.zones[0]));
  assert.throws(() => validateCollectorResult(r, "12H"));
});
test("manual price edit and wrong saved observation cannot retain measured quantity", () => {
  const parsed = validateCollectorResult(result(), "12H");
  const [level] = collectorZonesToLevels(parsed);
  const row = { timeframe: "12H", hover_snapshot: parsed.hover_snapshot };
  const edited = { ...level, ...manualLiquidityPricePatch("111", true) };
  assert.equal(edited.source_row_ids, null);
  assert.equal(edited.source_value, null);
  assert.equal(hoverLevelText(row, edited, "above"), null);
  assert.equal(
    hoverLevelText(
      row,
      { ...level, source_observation: { ...level.source_observation, sha256: "b".repeat(64) } },
      "above",
    ),
    null,
  );
});
test("rounded source values cannot become exact USD by relabelling", () => {
  const s = snapshot();
  s.unit = "USD";
  assert.throws(() => validateHoverSnapshot(s));
  for (const row of s.rows) row.raw_value = "$" + row.raw_value;
  assert.equal(validateHoverSnapshot(s).unit, "USD");
});

test("two verified yellow rows keep both exact prices through new and saved views", () => {
  for (const model of [1, 2, 3]) {
    const raw = result(model);
    raw.zones = [{
      ...raw.zones[0],
      price_low: 105.25,
      price_high: 110.25,
      shape: "range",
      row_colour: "yellow",
      continuous: true,
      weak_gap_present: false,
      prices: [],
      source_row_ids: ["r0", "r1"],
      source_value: 3e6,
      price_evidence: "Price 105.25 — Price 110.25",
    }];
    const parsed = validateCollectorResult(raw, "12H", { model });
    const [level] = collectorZonesToLevels(parsed);
    const row = { timeframe: "12H", hover_snapshot: parsed.hover_snapshot };
    assert.equal(level.price_text, "$105.25 + $110.25");
    assert.equal(hoverLevelText(row, level, "above"), "≈3M במדד המקור · 20% מהטווח המוצג");
    const stored = { ...level, price_text: "$105.25–$110.25" };
    assert.equal(savedLiquidityPriceText(row, stored), "$105.25 + $110.25");
    assert.equal(stored.price_text, "$105.25–$110.25");
    assert.equal(savedLiquidityPriceText(undefined, stored), stored.price_text);
    assert.equal(
      savedLiquidityPriceText(row, {
        ...stored,
        source_observation: { ...stored.source_observation, sha256: "b".repeat(64) },
      }),
      stored.price_text,
    );
    const green = structuredClone(raw);
    green.zones[0].intensity = "normal";
    delete green.zones[0].row_colour;
    const [greenLevel] = collectorZonesToLevels(validateCollectorResult(green, "12H", { model }));
    assert.equal(greenLevel.price_text, "$105.25–$110.25");
    const manual = { ...stored, ...manualLiquidityPricePatch("my manual price", true) };
    assert.equal(savedLiquidityPriceText(row, manual), "my manual price");
  }
});
