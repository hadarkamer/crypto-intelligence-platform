import { canonicalHeatmapSource, type HeatmapModel } from "./coinglassModel1.ts";

type Row = {
  id: string;
  price: number;
  value: number;
  raw_price: string;
  raw_value: string;
  rounding_error: number;
};
export type HoverSnapshot = {
  version: "displayed-hover.v1";
  metric: "Liquidation Leverage";
  unit: "USD" | "source_metric";
  scope: "displayed_latest_column";
  heatmap_model: HeatmapModel;
  timeframe: "12H" | "24H" | "48H";
  source_url: string;
  source_time: string;
  captured_at: string;
  artifact_id: string;
  sha256: string;
  complete: true;
  price_step: number;
  total_value: number;
  rounding_error: number;
  duration_seconds: number;
  observed_price_range: { low: number; high: number } | null;
  observed_price?: number;
  source_candle?: {
    open: number;
    high: number;
    low: number;
    close: number;
    source_time: string;
    raw_text: string;
  };
  rows: Row[];
};
type Obj = Record<string, unknown>;
const object = (x: unknown): Obj | null =>
  x && typeof x === "object" && !Array.isArray(x) ? (x as Obj) : null;
const finite = (x: unknown): x is number =>
  typeof x === "number" && Number.isFinite(x) && x >= 0 && x < 1e15;
const positive = (x: unknown): x is number => finite(x) && x > 0;
function invalid(): never {
  throw new Error("invalid_coinglass_hover_snapshot");
}
const sameNumber = (a: number, b: number) =>
  Math.abs(a - b) <= Math.max(0.000001, Number.EPSILON * Math.max(a, b) * 2000);

export function validateHoverSnapshot(
  raw: unknown,
  expected?: {
    model: number;
    timeframe: string;
    sourceUrl: string;
    capturedAt: string;
    artifactId: string;
    sha256: string;
    observedRange: { low: number; high: number } | null;
    observedPrice?: number | null;
  },
): HoverSnapshot | null {
  if (raw === null || raw === undefined) return null;
  const s = object(raw),
    range = object(s?.observed_price_range);
  const candle = object(s?.source_candle);
  const point = s?.observed_price;
  const exact =
    [1, 2].includes(s?.heatmap_model as number) &&
    positive(point) &&
    s?.observed_price_range === null;
  if (
    !s ||
    s.version !== "displayed-hover.v1" ||
    s.metric !== "Liquidation Leverage" ||
    !["USD", "source_metric"].includes(String(s.unit)) ||
    s.scope !== "displayed_latest_column" ||
    ![1, 2, 3].includes(s.heatmap_model as number) ||
    !["12H", "24H", "48H"].includes(String(s.timeframe)) ||
    !canonicalHeatmapSource(s.source_url, s.heatmap_model as HeatmapModel) ||
    s.complete !== true ||
    !positive(s.price_step) ||
    !finite(s.total_value) ||
    !finite(s.rounding_error) ||
    !finite(s.duration_seconds) ||
    s.duration_seconds > (s.heatmap_model === 3 ? 240 : 60) ||
    typeof s.artifact_id !== "string" ||
    !/^[\da-f]{8}(?:-[\da-f]{4}){3}-[\da-f]{12}$/i.test(s.artifact_id) ||
    typeof s.sha256 !== "string" ||
    !/^[\da-f]{64}$/i.test(s.sha256) ||
    (!exact &&
      (!range || !positive(range.low) || !positive(range.high) || range.low >= range.high)) ||
    !Array.isArray(s.rows) ||
    s.rows.length < 3 ||
    s.rows.length > 1600
  )
    invalid();
  for (const key of ["source_time", "captured_at"] as const) {
    if (
      typeof s[key] !== "string" ||
      !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$/.test(s[key] as string) ||
      !Number.isFinite(Date.parse(s[key] as string))
    )
      invalid();
  }
  const age = Date.parse(s.captured_at as string) - Date.parse(s.source_time as string);
  if (age < -120000 || age > 1200000) invalid();
  if (exact) {
    if (
      !candle ||
      candle.source_time !== s.source_time ||
      candle.close !== point ||
      ![candle.open, candle.high, candle.low, candle.close].every(
        (value) => positive(value) && value < 1e12,
      ) ||
      (candle.low as number) > Math.min(candle.open as number, point as number) ||
      (candle.high as number) < Math.max(candle.open as number, point as number) ||
      typeof candle.raw_text !== "string" ||
      candle.raw_text.length > 220
    )
      invalid();
    const parts =
      /^(.*?) Open (\d+(?:,\d{3})*(?:\.\d+)?) High (\d+(?:,\d{3})*(?:\.\d+)?) Low (\d+(?:,\d{3})*(?:\.\d+)?) Close (\d+(?:,\d{3})*(?:\.\d+)?)$/.exec(
        candle.raw_text,
      );
    const date = new Date(s.source_time as string);
    const iso = date.toISOString().slice(0, 16).replace("T", " ");
    const month = [
      "Jan",
      "Feb",
      "Mar",
      "Apr",
      "May",
      "Jun",
      "Jul",
      "Aug",
      "Sep",
      "Oct",
      "Nov",
      "Dec",
    ][date.getUTCMonth()];
    const english = `${date.getUTCDate()} ${month} ${date.getUTCFullYear()}, ${iso.slice(11)}`;
    if (
      !parts ||
      date.getUTCSeconds() !== 0 ||
      date.getUTCMilliseconds() !== 0 ||
      ![iso, english].includes(parts[1].replace(/^0(?=\d )/, "")) ||
      ["open", "high", "low", "close"].some(
        (key, i) => Number(parts[i + 2].replaceAll(",", "")) !== candle[key],
      )
    )
      invalid();
  } else if (point != null || candle != null) invalid();
  const refLow = exact ? (point as number) : (range!.low as number);
  const refHigh = exact ? (point as number) : (range!.high as number);
  if (
    expected &&
    (s.heatmap_model !== expected.model ||
      s.timeframe !== expected.timeframe ||
      s.source_url !== expected.sourceUrl ||
      Date.parse(s.captured_at as string) !== Date.parse(expected.capturedAt) ||
      s.artifact_id !== expected.artifactId ||
      s.sha256 !== expected.sha256 ||
      (exact
        ? expected.observedRange !== null || point !== expected.observedPrice
        : range!.low !== expected.observedRange?.low ||
          range!.high !== expected.observedRange?.high ||
          expected.observedPrice != null))
  )
    invalid();
  const rows: Row[] = [];
  for (const [index, input] of (s.rows as unknown[]).entries()) {
    const r = object(input);
    if (
      !r ||
      r.id !== `r${index}` ||
      !positive(r.price) ||
      !finite(r.value) ||
      !finite(r.rounding_error) ||
      typeof r.raw_price !== "string" ||
      !/^\d+(?:,\d{3})*(?:\.\d+)?$/.test(r.raw_price) ||
      Number(r.raw_price.replaceAll(",", "")) !== r.price ||
      typeof r.raw_value !== "string" ||
      r.raw_value.length > 64
    )
      invalid();
    const token = (r.raw_value as string).replaceAll(",", "").replace(/\s/g, "").toUpperCase();
    const match = /^(\$?)(\d+(?:\.\d+)?)([KMB]?)(USD)?$/.exec(token);
    if (!match) invalid();
    const unit = match[1] || match[4] ? "USD" : "source_metric";
    const multiplier = ({ "": 1, K: 1e3, M: 1e6, B: 1e9 } as Record<string, number>)[match[3]];
    const decimals = match[2].split(".")[1]?.length ?? 0;
    const rounding = multiplier / (2 * 10 ** decimals);
    if (
      unit !== s.unit ||
      !sameNumber(Number(match[2]) * multiplier, r.value as number) ||
      rounding !== r.rounding_error
    )
      invalid();
    if (index) {
      const gap = rows[index - 1].price - (r.price as number);
      const step = s.price_step as number;
      const tolerance = Math.max(0.025, step * 0.002);
      // Model 2 clips only its outermost displayed prices to the plot edge.
      // Interior intervals remain strict; a missing row still fails validation.
      const edge =
        s.heatmap_model === 2 && s.rows.length > 3 && (index === 1 || index === s.rows.length - 1);
      if (gap <= 0 || (edge ? gap > step + tolerance : Math.abs(gap - step) > tolerance)) invalid();
    }
    rows.push({
      id: r.id as string,
      price: r.price as number,
      value: r.value as number,
      raw_price: r.raw_price as string,
      raw_value: r.raw_value as string,
      rounding_error: r.rounding_error as number,
    });
  }
  if (
    !sameNumber(
      rows.reduce((n, r) => n + r.value, 0),
      s.total_value as number,
    ) ||
    !sameNumber(
      rows.reduce((n, r) => n + r.rounding_error, 0),
      s.rounding_error as number,
    ) ||
    refLow < rows.at(-1)!.price ||
    refHigh > rows[0].price
  )
    invalid();
  return {
    ...s,
    rows,
    observed_price_range: exact ? null : { low: refLow, high: refHigh },
  } as HoverSnapshot;
}

export function hoverQuantityForZone(snapshot: HoverSnapshot | null, rawZone: unknown) {
  const zone = object(rawZone);
  if (!zone || zone.source_row_ids === undefined || zone.source_row_ids === null) return null;
  if (
    !snapshot ||
    !Array.isArray(zone.source_row_ids) ||
    !zone.source_row_ids.length ||
    new Set(zone.source_row_ids).size !== zone.source_row_ids.length
  )
    invalid();
  const ids = zone.source_row_ids as unknown[];
  const rows = ids.map((id) => snapshot.rows.find((r) => r.id === id));
  if (rows.some((r) => !r)) invalid();
  const members = rows as Row[];
  const low = Math.min(...members.map((r) => r.price)),
    high = Math.max(...members.map((r) => r.price));
  if (
    zone.price_low !== low ||
    zone.price_high !== high ||
    snapshot.rows.some((r) => r.price >= low && r.price <= high && !ids.includes(r.id)) ||
    (zone.side === "above"
      ? low <= (snapshot.observed_price_range?.high ?? snapshot.observed_price!)
      : zone.side === "below"
        ? high >= (snapshot.observed_price_range?.low ?? snapshot.observed_price!)
        : true)
  )
    invalid();
  const value = members.reduce((n, r) => n + r.value, 0);
  if (
    zone.source_value !== undefined &&
    (!finite(zone.source_value) || !sameNumber(zone.source_value, value))
  )
    invalid();
  return {
    source_row_ids: ids as string[],
    source_value: value,
    percent: snapshot.total_value > 0 ? (value / snapshot.total_value) * 100 : null,
  };
}

export function savedHover(row: unknown): HoverSnapshot | null {
  const r = object(row);
  try {
    const s = validateHoverSnapshot(r?.hover_snapshot);
    return s?.timeframe === r?.timeframe ? s : null;
  } catch {
    return null;
  }
}
const formatted = (value: number, unit: HoverSnapshot["unit"]) => {
  const scale =
    value >= 1e9
      ? ([1e9, "B"] as const)
      : value >= 1e6
        ? ([1e6, "M"] as const)
        : value >= 1e3
          ? ([1e3, "K"] as const)
          : ([1, ""] as const);
  return `≈${new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 }).format(value / scale[0])}${scale[1]}${unit === "USD" ? "$" : " במדד המקור"}`;
};
export function hoverSideText(row: unknown, side: "above" | "below"): string | null {
  const s = savedHover(row);
  if (!s) return null;
  const sum = s.rows
    .filter((r) =>
      side === "above"
        ? r.price > (s.observed_price_range?.high ?? s.observed_price!)
        : r.price < (s.observed_price_range?.low ?? s.observed_price!),
    )
    .reduce((n, r) => n + r.value, 0);
  return `סה״כ בטווח המוצג: ${formatted(sum, s.unit)}`;
}
export function hoverLevelData(row: unknown, level: unknown, side: "above" | "below") {
  const s = savedHover(row),
    l = object(level),
    observation = object(l?.source_observation);
  if (
    !s ||
    !l ||
    !observation ||
    observation.source_url !== s.source_url ||
    observation.heatmap_model !== s.heatmap_model ||
    observation.timeframe !== s.timeframe ||
    observation.artifact_id !== s.artifact_id ||
    observation.sha256 !== s.sha256 ||
    Date.parse(String(observation.captured_at)) !== Date.parse(s.captured_at) ||
    (l.side !== undefined && l.side !== side)
  )
    return null;
  try {
    const q = hoverQuantityForZone(s, { ...l, side });
    if (!q || l.source_value !== q.source_value) return null;
    return {
      value: q.source_value,
      unit: s.unit,
      displayed_column_percent: q.percent,
      scope: s.scope,
      source_time: s.source_time,
      source_row_ids: q.source_row_ids,
    };
  } catch {
    return null;
  }
}
export function hoverLevelText(
  row: unknown,
  level: unknown,
  side: "above" | "below",
): string | null {
  const q = hoverLevelData(row, level, side);
  return q
    ? `${formatted(q.value, q.unit)}${q.displayed_column_percent === null ? "" : ` · ${new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 }).format(q.displayed_column_percent)}% מהטווח המוצג`}`
    : null;
}
