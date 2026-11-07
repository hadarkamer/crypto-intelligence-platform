import { createHash } from "node:crypto";
import { sourceRequest } from "./collectionSources.server";
import {
  formatHyperliquidBtc,
  formatHyperliquidPrice,
  HYPERLIQUID_MAX_DISTANCE_PCT,
  selectHyperliquidPriceLevels,
} from "./collectionHyperliquidChart";

const SOURCE_URL = "https://www.coinglass.com/hyperliquid-liquidation-map";
const API_URL = "https://open-api-v4.coinglass.com/api/hyperliquid/whale-position";
const MAX_UPDATE_AGE_MS = 15 * 60_000;
const FUTURE_TOLERANCE_MS = 2 * 60_000;
const INVALID_DATA = "נתוני מפת ההנזלות של היפרליקוויד אינם תקינים או אינם עדכניים.";

export type HyperliquidSpike = {
  price: number;
  amountBtc: number;
  positionSide: "long" | "short";
  side: "above" | "below";
  distancePct: number;
  positions: number;
};

export type HyperliquidSnapshot = {
  sourceUrl: string;
  capturedAt: string;
  sourceUpdatedAt: string;
  sourceHash: string;
  observedPrice: number;
  unit: "BTC";
  coverage: "coinglass_whales_over_1m_usd";
  precision: "provider_numeric";
  totalBtcPositions: number;
  skippedUncalculablePositions: number;
  positionBucketCount: number;
  prominence: { medianBtc: number; percentile75Btc: number; thresholdBtc: number };
  spikes: HyperliquidSpike[];
};

export type HyperliquidReadOptions = {
  request?: typeof sourceRequest;
  apiKey?: string;
  now?: number;
};

export function isHyperliquidLiquidationSource(raw: string): boolean {
  try {
    const url = new URL(raw);
    return (
      url.protocol === "https:" &&
      ["coinglass.com", "www.coinglass.com"].includes(url.hostname) &&
      !url.port &&
      !url.username &&
      !url.password &&
      !url.search &&
      !url.hash &&
      url.pathname.replace(/\/$/, "") === "/hyperliquid-liquidation-map"
    );
  } catch {
    return false;
  }
}

function object(raw: unknown): Record<string, unknown> {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) throw new Error(INVALID_DATA);
  return raw as Record<string, unknown>;
}

function numeric(raw: unknown): number {
  if (
    typeof raw !== "number" &&
    !(typeof raw === "string" && /^[+-]?(?:\d+(?:\.\d+)?|\.\d+)$/.test(raw))
  )
    throw new Error(INVALID_DATA);
  const number = Number(raw);
  if (!Number.isFinite(number)) throw new Error(INVALID_DATA);
  return number;
}

function updatedMillis(raw: unknown): number {
  if (
    !(typeof raw === "number" && Number.isSafeInteger(raw)) &&
    !(typeof raw === "string" && /^(?:\d{10}|\d{13})$/.test(raw))
  )
    throw new Error(INVALID_DATA);
  const value = String(raw);
  if (!/^(?:\d{10}|\d{13})$/.test(value)) throw new Error(INVALID_DATA);
  return Number(value) * (value.length === 10 ? 1000 : 1);
}

function quantile(sorted: number[], fraction: number): number {
  const index = (sorted.length - 1) * fraction;
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  return sorted[lower] + (sorted[upper] - sorted[lower]) * (index - lower);
}

/** Fixed official CoinGlass API; no page scraping, model inference or secret output.
 * API prices/sizes retain the provider's precision, which can differ from the
 * public chart tooltip. Null/zero liquidation prices are counted as unavailable. */
export async function readHyperliquidSnapshot(
  raw: string,
  options: HyperliquidReadOptions = {},
): Promise<HyperliquidSnapshot> {
  if (!isHyperliquidLiquidationSource(raw)) throw new Error("מקור היפרליקוויד אינו נתמך.");
  const key = options.apiKey ?? process.env.COINGLASS_API_KEY;
  if (!key || !key.trim() || /[\r\n]/.test(key))
    throw new Error("מפתח קוינגלאס אינו מוגדר לקריאת פוזיציות היפרליקוויד.");
  const now = options.now ?? Date.now();
  if (!Number.isFinite(now) || now <= 0) throw new Error(INVALID_DATA);
  let response: Awaited<ReturnType<typeof sourceRequest>>;
  try {
    response = await (options.request ?? sourceRequest)(API_URL, 2 * 1024 * 1024, {
      Accept: "application/json",
      "CG-API-KEY": key,
    });
  } catch {
    // Transport/provider errors must never echo request headers or response bodies.
    throw new Error("לא ניתן לקרוא את פוזיציות היפרליקוויד דרך קוינגלאס.");
  }
  if (response.url !== API_URL || !/^application\/json(?:\s*;|$)/i.test(response.contentType))
    throw new Error(INVALID_DATA);
  let decoded: unknown;
  try {
    decoded = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(response.bytes));
  } catch {
    throw new Error(INVALID_DATA);
  }
  const payload = object(decoded);
  if (!(payload.code === "0" || payload.code === 0) || !Array.isArray(payload.data))
    throw new Error("קוינגלאס לא סיפקה פוזיציות היפרליקוויד; יש לבדוק הרשאה למקור.");
  if (payload.data.length > 20_000) throw new Error(INVALID_DATA);

  const marks: number[] = [];
  const timestamps: number[] = [];
  const buckets = new Map<string, Omit<HyperliquidSpike, "side" | "distancePct">>();
  let totalBtcPositions = 0;
  let skippedUncalculablePositions = 0;
  for (const rawPosition of payload.data) {
    const position = object(rawPosition);
    if (typeof position.symbol !== "string" || !position.symbol) throw new Error(INVALID_DATA);
    if (position.symbol !== "BTC") continue;
    totalBtcPositions++;
    const size = numeric(position.position_size);
    const mark = numeric(position.mark_price);
    const updated = updatedMillis(position.update_time);
    if (
      size === 0 ||
      mark <= 0 ||
      now - updated > MAX_UPDATE_AGE_MS ||
      updated - now > FUTURE_TOLERANCE_MS
    )
      throw new Error(INVALID_DATA);
    if (position.position_value_usd !== undefined && position.position_value_usd !== null) {
      if (numeric(position.position_value_usd) < 0) throw new Error(INVALID_DATA);
    }
    marks.push(mark);
    timestamps.push(updated);
    if (position.liq_price === null || position.liq_price === undefined) {
      skippedUncalculablePositions++;
      continue;
    }
    const price = numeric(position.liq_price);
    if (price === 0) {
      skippedUncalculablePositions++;
      continue;
    }
    if (price < 0) throw new Error(INVALID_DATA);
    const positionSide = size < 0 ? "short" : "long";
    const bucketKey = `${positionSide}:${price}`;
    const bucket = buckets.get(bucketKey);
    if (bucket) {
      bucket.amountBtc += Math.abs(size);
      bucket.positions++;
      if (!Number.isFinite(bucket.amountBtc)) throw new Error(INVALID_DATA);
    } else buckets.set(bucketKey, { price, amountBtc: Math.abs(size), positionSide, positions: 1 });
  }
  if (!marks.length || !buckets.size) throw new Error(INVALID_DATA);
  marks.sort((a, b) => a - b);
  const observedPrice = quantile(marks, 0.5);
  if ((marks[marks.length - 1] - marks[0]) / observedPrice > 0.01) throw new Error(INVALID_DATA);
  const levels = [...buckets.values()];
  const amounts = levels
    .filter(
      (bucket) =>
        (Math.abs(bucket.price - observedPrice) / observedPrice) * 100 <=
        HYPERLIQUID_MAX_DISTANCE_PCT,
    )
    .map((bucket) => bucket.amountBtc)
    .sort((a, b) => a - b);
  const medianBtc = amounts.length ? quantile(amounts, 0.5) : 0;
  const percentile75Btc = amounts.length ? quantile(amounts, 0.75) : 0;
  const thresholdBtc = Math.max(percentile75Btc, medianBtc * 2);
  const spikes: HyperliquidSpike[] = [];
  for (const bucket of buckets.values()) {
    // A side inconsistent with the current mark means this is not a current
    // pending-liquidation map; never silently turn a long into a short.
    if (
      (bucket.positionSide === "long" && bucket.price >= observedPrice) ||
      (bucket.positionSide === "short" && bucket.price <= observedPrice)
    )
      throw new Error(INVALID_DATA);
  }
  for (const bucket of selectHyperliquidPriceLevels(levels, observedPrice)) {
    const distancePct = ((bucket.price - observedPrice) / observedPrice) * 100;
    spikes.push({
      ...bucket,
      side: bucket.price > observedPrice ? "above" : "below",
      distancePct,
    });
  }
  // Selection is shared with chart labels; fallback text reads nearest first.
  spikes.sort(
    (a, b) =>
      Math.abs(a.distancePct) - Math.abs(b.distancePct) ||
      b.amountBtc - a.amountBtc ||
      a.price - b.price,
  );
  return {
    sourceUrl: SOURCE_URL,
    capturedAt: new Date(now).toISOString(),
    sourceUpdatedAt: new Date(Math.min(...timestamps)).toISOString(),
    sourceHash: createHash("sha256").update(response.bytes).digest("hex"),
    observedPrice,
    unit: "BTC",
    coverage: "coinglass_whales_over_1m_usd",
    precision: "provider_numeric",
    totalBtcPositions,
    skippedUncalculablePositions,
    positionBucketCount: buckets.size,
    prominence: { medianBtc, percentile75Btc, thresholdBtc },
    spikes: spikes.slice(0, 6),
  };
}

export function renderHyperliquidSnapshot(snapshot: HyperliquidSnapshot): string {
  if (!snapshot.spikes.length) return "לא נמצאו ספייקים בולטים בקרבת המחיר.";
  return snapshot.spikes
    .map(
      (spike) =>
        `${formatHyperliquidPrice(spike.price)} · ${formatHyperliquidBtc(spike.amountBtc)}`,
    )
    .join("\n");
}
