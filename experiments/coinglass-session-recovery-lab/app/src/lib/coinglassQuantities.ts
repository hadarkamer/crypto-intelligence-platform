import { canonicalHeatmapSource, type HeatmapModel } from "./coinglassModel1.ts";
import { hoverLevelText, hoverSideText } from "./coinglassHover.ts";

export const COINGLASS_QUANTITY_VERSION = "right-edge-usd.v1";
type Side = "above" | "below";
type RecordValue = Record<string, unknown>;
export type CoinglassQuantitySnapshot = {
  version: typeof COINGLASS_QUANTITY_VERSION;
  unit: "USD";
  scope: "latest_heatmap_column";
  source_url: string;
  heatmap_model: HeatmapModel;
  timeframe: "12H" | "24H" | "48H";
  captured_at: string;
  artifact_id: string;
  sha256: string;
  source_price: { value: number; basis: "source_data"; evidence: string };
  complete_above: boolean;
  complete_below: boolean;
  above_total_usd: number | null;
  below_total_usd: number | null;
  buckets: Array<{ id: string; price: number; amount_usd: number; evidence: string }>;
};
export type QuantityContext = {
  model: HeatmapModel;
  timeframe: string;
  capturedAt: string;
  sourceUrl: string;
  artifactId: string;
  sha256: string;
  observedPrice: number | null;
  observedRange: { low: number; high: number } | null;
};
const object = (value: unknown): RecordValue | null =>
  value && typeof value === "object" && !Array.isArray(value) ? (value as RecordValue) : null;
const number = (value: unknown): value is number =>
  typeof value === "number" &&
  Number.isFinite(value) &&
  value >= 0 &&
  value <= Number.MAX_SAFE_INTEGER;
const positive = (value: unknown): value is number => number(value) && value > 0;
const uuid = (value: unknown): value is string =>
  typeof value === "string" && /^[\da-f]{8}(?:-[\da-f]{4}){3}-[\da-f]{12}$/i.test(value);
const numericEvidence = (value: unknown, observed: number) =>
  typeof value === "string" &&
  value.length <= 128 &&
  /^\d+(?:\.\d+)?$/.test(value) &&
  Number(value) === observed;
function fail(): never {
  throw new Error("invalid_coinglass_quantity_snapshot");
}

/** Money is a separate source-data contract. Color, intensity and a selected
 * subset are never a monetary measurement or a denominator. */
export function validateCoinglassQuantities(
  raw: unknown,
  expected?: QuantityContext,
): CoinglassQuantitySnapshot | null {
  if (raw === null || raw === undefined) return null;
  const value = object(raw);
  if (
    !value ||
    value.version !== COINGLASS_QUANTITY_VERSION ||
    value.unit !== "USD" ||
    value.scope !== "latest_heatmap_column" ||
    ![1, 2, 3].includes(value.heatmap_model as number) ||
    !["12H", "24H", "48H"].includes(value.timeframe as string) ||
    !canonicalHeatmapSource(value.source_url, value.heatmap_model as HeatmapModel) ||
    typeof value.captured_at !== "string" ||
    !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?(?:Z|[+-]\d{2}:\d{2})$/.test(
      value.captured_at,
    ) ||
    !Number.isFinite(Date.parse(value.captured_at)) ||
    !uuid(value.artifact_id) ||
    typeof value.sha256 !== "string" ||
    !/^[\da-f]{64}$/i.test(value.sha256) ||
    typeof value.complete_above !== "boolean" ||
    typeof value.complete_below !== "boolean"
  )
    fail();
  const sourcePrice = object(value.source_price);
  if (
    !sourcePrice ||
    sourcePrice.basis !== "source_data" ||
    !positive(sourcePrice.value) ||
    !numericEvidence(sourcePrice.evidence, sourcePrice.value)
  )
    fail();
  const price = sourcePrice.value as number;
  if (
    expected &&
    (value.heatmap_model !== expected.model ||
      value.timeframe !== expected.timeframe ||
      value.source_url !== expected.sourceUrl ||
      Date.parse(value.captured_at as string) !== Date.parse(expected.capturedAt) ||
      (value.artifact_id as string).toLowerCase() !== expected.artifactId.toLowerCase() ||
      (value.sha256 as string).toLowerCase() !== expected.sha256.toLowerCase() ||
      (expected.observedRange
        ? price < expected.observedRange.low || price > expected.observedRange.high
        : price !== expected.observedPrice))
  )
    fail();
  if (!Array.isArray(value.buckets) || !value.buckets.length || value.buckets.length > 5000) fail();
  const ids = new Set<string>();
  const prices = new Set<number>();
  const buckets = (value.buckets as unknown[]).map((rawBucket) => {
    const bucket = object(rawBucket);
    if (
      !bucket ||
      typeof bucket.id !== "string" ||
      !/^[\w.:-]{1,128}$/.test(bucket.id) ||
      ids.has(bucket.id) ||
      !positive(bucket.price) ||
      prices.has(bucket.price) ||
      !number(bucket.amount_usd) ||
      !numericEvidence(bucket.evidence, bucket.amount_usd)
    )
      fail();
    ids.add(bucket.id as string);
    prices.add(bucket.price as number);
    return {
      id: bucket.id as string,
      price: bucket.price as number,
      amount_usd: bucket.amount_usd as number,
      evidence: bucket.evidence as string,
    };
  });
  for (const side of ["above", "below"] as const) {
    const total = value[`${side}_total_usd`];
    if (!value[`complete_${side}`]) {
      if (total !== null) fail();
      continue;
    }
    if (!number(total)) fail();
    const members = buckets.filter((bucket) =>
      side === "above" ? bucket.price > price : bucket.price < price,
    );
    const sum = members.reduce((amount, bucket) => amount + bucket.amount_usd, 0);
    const tolerance = Math.max(
      0.000001,
      Number.EPSILON * Math.max(sum, total) * Math.max(1, members.length),
    );
    if (!Number.isFinite(sum) || Math.abs(sum - total) > tolerance) fail();
  }
  return {
    version: COINGLASS_QUANTITY_VERSION,
    unit: "USD",
    scope: "latest_heatmap_column",
    source_url: value.source_url as string,
    heatmap_model: value.heatmap_model as HeatmapModel,
    timeframe: value.timeframe as CoinglassQuantitySnapshot["timeframe"],
    captured_at: new Date(value.captured_at as string).toISOString(),
    artifact_id: (value.artifact_id as string).toLowerCase(),
    sha256: (value.sha256 as string).toLowerCase(),
    source_price: { value: price, basis: "source_data", evidence: sourcePrice.evidence as string },
    complete_above: value.complete_above as boolean,
    complete_below: value.complete_below as boolean,
    above_total_usd: value.above_total_usd as number | null,
    below_total_usd: value.below_total_usd as number | null,
    buckets,
  };
}

export function quantityForZone(
  snapshot: CoinglassQuantitySnapshot | null,
  zone: { side: Side; price_low: number; price_high: number; quantity_bucket_ids?: unknown },
): { amount_usd: number; quantity_bucket_ids: string[] } | null {
  if (zone.quantity_bucket_ids === null || zone.quantity_bucket_ids === undefined) return null;
  if (
    !snapshot ||
    !positive(zone.price_low) ||
    !positive(zone.price_high) ||
    zone.price_low > zone.price_high ||
    !Array.isArray(zone.quantity_bucket_ids) ||
    !zone.quantity_bucket_ids.length ||
    new Set(zone.quantity_bucket_ids).size !== zone.quantity_bucket_ids.length
  )
    fail();
  const ids = zone.quantity_bucket_ids as unknown[];
  const buckets = ids.map((id) => snapshot.buckets.find((bucket) => bucket.id === id));
  if (
    buckets.some(
      (bucket) =>
        !bucket ||
        bucket.price < zone.price_low ||
        bucket.price > zone.price_high ||
        (zone.side === "above"
          ? bucket.price <= snapshot.source_price.value
          : bucket.price >= snapshot.source_price.value),
    )
  )
    fail();
  if (
    snapshot.buckets.some(
      (bucket) =>
        bucket.price >= zone.price_low &&
        bucket.price <= zone.price_high &&
        !ids.includes(bucket.id),
    )
  )
    fail();
  // A partial side can verify an individual numeric bucket, but cannot prove
  // that no unobserved bucket is missing from an interval's total.
  if (zone.price_low !== zone.price_high && !snapshot[`complete_${zone.side}`]) return null;
  const amount = buckets.reduce((sum, bucket) => sum + bucket!.amount_usd, 0);
  if (!number(amount)) fail();
  const total = snapshot[`${zone.side}_total_usd`];
  if (total !== null && amount > total) fail();
  return { amount_usd: amount, quantity_bucket_ids: ids as string[] };
}

function rowSnapshot(raw: unknown): CoinglassQuantitySnapshot | null {
  const row = object(raw);
  if (!row) return null;
  try {
    const snapshot = validateCoinglassQuantities(row.quantity_snapshot);
    return snapshot?.timeframe === row.timeframe ? snapshot : null;
  } catch {
    return null;
  }
}
export function coinglassSideTotal(row: unknown, side: Side): number | null {
  return rowSnapshot(row)?.[`${side}_total_usd`] ?? null;
}
export function coinglassLevelQuantity(
  row: unknown,
  rawLevel: unknown,
  side: Side,
): { amountUsd: number; percent: number | null } | null {
  const snapshot = rowSnapshot(row);
  const level = object(rawLevel);
  const observation = object(level?.source_observation);
  if (
    !snapshot ||
    !level ||
    !observation ||
    !positive(level.price_low) ||
    !positive(level.price_high) ||
    (level.side !== undefined && level.side !== side) ||
    observation.source_url !== snapshot.source_url ||
    observation.heatmap_model !== snapshot.heatmap_model ||
    observation.timeframe !== snapshot.timeframe ||
    String(observation.artifact_id).toLowerCase() !== snapshot.artifact_id ||
    String(observation.sha256).toLowerCase() !== snapshot.sha256 ||
    Date.parse(String(observation.captured_at)) !== Date.parse(snapshot.captured_at)
  )
    return null;
  try {
    const quantity = quantityForZone(snapshot, {
      side,
      price_low: level.price_low,
      price_high: level.price_high,
      quantity_bucket_ids: level.quantity_bucket_ids,
    });
    if (!quantity || level.amount_usd !== quantity.amount_usd) return null;
    const total = snapshot[`${side}_total_usd`];
    return {
      amountUsd: quantity.amount_usd,
      percent: total !== null && total > 0 ? (quantity.amount_usd / total) * 100 : null,
    };
  } catch {
    return null;
  }
}
export function formatCoinglassUsd(value: number): string {
  if (!number(value)) return "—";
  const unit =
    value >= 1e9
      ? ([1e9, "B"] as const)
      : value >= 1e6
        ? ([1e6, "M"] as const)
        : value >= 1e3
          ? ([1e3, "K"] as const)
          : ([1, ""] as const);
  return (
    new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 }).format(value / unit[0]) +
    unit[1] +
    "$"
  );
}
export const COINGLASS_USD_UNAVAILABLE = "אין סכום דולר מאומת בנתונים השמורים";
export function coinglassSideTotalText(row: unknown, side: Side): string {
  const hover = hoverSideText(row, side);
  if (hover) return hover;
  const total = coinglassSideTotal(row, side);
  return total === null ? COINGLASS_USD_UNAVAILABLE : `סה״כ ${formatCoinglassUsd(total)}`;
}
export function coinglassQuantityText(row: unknown, level: unknown, side: Side): string | null {
  const hover = hoverLevelText(row, level, side);
  if (hover) return hover;
  const quantity = coinglassLevelQuantity(row, level, side);
  return quantity
    ? `${formatCoinglassUsd(quantity.amountUsd)}${quantity.percent === null ? "" : ` · ${new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 }).format(quantity.percent)}% מסך הצד בעמודה העדכנית`}`
    : null;
}
