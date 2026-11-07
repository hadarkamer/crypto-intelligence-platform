export const HYPERLIQUID_CHART_VERSION = "hyperliquid-chart.v1";
export const HYPERLIQUID_NEAR_DISTANCE_PCT = 6;
export const HYPERLIQUID_MAX_DISTANCE_PCT = 15;
export const HYPERLIQUID_FAR_MIN_NEAR_RATIO = 0.35;
export const HYPERLIQUID_MAX_LABELS = 6;
export const HYPERLIQUID_MIN_CHART_HEIGHT = 320;
export const HYPERLIQUID_MAX_CHART_ASPECT = 4;

export const HYPERLIQUID_CAPTURE_FAILURE_CODES = [
  "wrong_source",
  "source_login_required",
  "source_chart_unmounted",
  "ambiguous_chart",
  "chart_api_unavailable",
  "unverified_symbol",
  "unverified_series",
  "invalid_prices",
  "unverified_current_price",
  "hidden_series",
  "invalid_geometry",
  "unreadable_geometry",
  "invalid_values",
  "overlapping_bars",
  "empty_chart",
  "stale_chart",
  "chart_loading",
  "zoom_unavailable",
  "unverified_zoom",
  "unverified_capture",
] as const;

/** Keep only codes emitted by the source-chart verifier, never provider text. */
export function hyperliquidCaptureFailureCode(error: unknown): string | undefined {
  if (!error || typeof error !== "object") return undefined;
  const value = error as { name?: unknown; code?: unknown };
  if (
    value.name === "HyperliquidChartCaptureError" &&
    typeof value.code === "string" &&
    (HYPERLIQUID_CAPTURE_FAILURE_CODES as readonly string[]).includes(value.code)
  )
    return value.code;
  return undefined;
}

/** Reject collapsed percentage-height charts even when their PNG is valid. */
export function readableHyperliquidChartDimensions(width: number, height: number): boolean {
  return (
    Number.isFinite(width) &&
    Number.isFinite(height) &&
    width >= 160 &&
    width <= 8_000 &&
    height >= HYPERLIQUID_MIN_CHART_HEIGHT &&
    height <= 8_000 &&
    width / height <= HYPERLIQUID_MAX_CHART_ASPECT
  );
}

export type HyperliquidPriceLevel = {
  price: number;
  amountBtc: number;
  positionSide: "long" | "short";
};

export type HyperliquidChartSpike = {
  price: number;
  amountBtc: number;
  positionSide: "long" | "short";
  /** Top centre of the original, rendered bar, in chart CSS pixels. */
  x: number;
  y: number;
};

export type HyperliquidChartAnnotation = HyperliquidChartSpike & {
  labelX: number;
  labelY: number;
  labelWidth: number;
  labelHeight: number;
};

export type HyperliquidChart = {
  version: typeof HYPERLIQUID_CHART_VERSION;
  sourceUrl: string;
  capturedAt: string;
  symbol: "BTC";
  unit: "BTC";
  quantityKind: "liquidation_bucket";
  coordinateSource: "rendered_bar";
  observedPrice: number;
  width: number;
  height: number;
  zoom?: HyperliquidPriceWindow;
  topPadding: number;
  spikes: HyperliquidChartSpike[];
  annotations: HyperliquidChartAnnotation[];
};

function positive(value: number): boolean {
  return typeof value === "number" && Number.isFinite(value) && value > 0;
}

export const HYPERLIQUID_SELECTION_CONFIG = {
  nearDistancePct: HYPERLIQUID_NEAR_DISTANCE_PCT,
  maxDistancePct: HYPERLIQUID_MAX_DISTANCE_PCT,
  farMinNearRatio: HYPERLIQUID_FAR_MIN_NEAR_RATIO,
  minimumPeakRatio: 0.25,
  nearMinimumPeakRatio: 0.15,
  maxLabels: HYPERLIQUID_MAX_LABELS,
};

/** Kept closure-free so the source browser uses the exact same selection rule. */
export function selectHyperliquidLevelsWithConfig<T extends HyperliquidPriceLevel>(
  levels: T[],
  observedPrice: number,
  config: typeof HYPERLIQUID_SELECTION_CONFIG,
): T[] {
  const positive = (value: number) =>
    typeof value === "number" && Number.isFinite(value) && value > 0;
  const quantile = (sorted: number[], fraction: number) => {
    const index = (sorted.length - 1) * fraction;
    const lower = Math.floor(index),
      upper = Math.ceil(index);
    return sorted[lower] + (sorted[upper] - sorted[lower]) * (index - lower);
  };
  if (!positive(observedPrice) || levels.length > 20_000) return [];
  const distance = (bar: T) => (Math.abs(bar.price - observedPrice) / observedPrice) * 100;
  const eligible = levels.filter(
    (bar) =>
      positive(bar.price) &&
      positive(bar.amountBtc) &&
      distance(bar) <= config.maxDistancePct &&
      (bar.positionSide === "long"
        ? bar.price < observedPrice
        : bar.positionSide === "short" && bar.price > observedPrice),
  );
  if (!eligible.length) return [];
  const amounts = eligible.map((bar) => bar.amountBtc).sort((a, b) => a - b);
  const localThreshold = Math.max(quantile(amounts, 0.75), quantile(amounts, 0.5) * 2);
  const peakFloor = amounts[amounts.length - 1] * config.minimumPeakRatio;
  // A large distant spike must not hide meaningful smaller spikes near price.
  // The local baseline still rejects noise and uniformly sized bars.
  const nearPeakFloor = amounts[amounts.length - 1] * config.nearMinimumPeakRatio;
  const nearby = eligible.filter(
    (bar) =>
      distance(bar) <= config.nearDistancePct &&
      bar.amountBtc >= Math.max(localThreshold, nearPeakFloor),
  );
  const strongestNearby = nearby.reduce((largest, bar) => Math.max(largest, bar.amountBtc), 0);
  return eligible
    .filter((bar) =>
      distance(bar) <= config.nearDistancePct
        ? bar.amountBtc >= Math.max(localThreshold, nearPeakFloor)
        : bar.amountBtc >=
          Math.max(peakFloor, localThreshold * 2, strongestNearby * config.farMinNearRatio),
    )
    .sort(
      (a, b) =>
        b.amountBtc / (1 + distance(b) / 3) - a.amountBtc / (1 + distance(a) / 3) ||
        distance(a) - distance(b) ||
        a.price - b.price,
    )
    .slice(0, config.maxLabels);
}

/** Display rounding never changes the stored provider price or BTC quantity. */
export function formatHyperliquidPrice(value: number): string {
  return value.toLocaleString("en-US", { maximumFractionDigits: 0 }) + "$";
}

export function formatHyperliquidBtc(value: number): string {
  if (value >= 1_000_000)
    return (value / 1_000_000).toLocaleString("en-US", { maximumFractionDigits: 2 }) + "M BTC";
  if (value >= 10_000)
    return (value / 1_000).toLocaleString("en-US", { maximumFractionDigits: 2 }) + "K BTC";
  return value.toLocaleString("en-US", { maximumFractionDigits: 2 }) + " BTC";
}

/** Nearby levels may be smaller than distant peaks while remaining locally
 * prominent. A farther level must exceed twice the local threshold and 35% of
 * the strongest near level. The 15% boundary accommodates large spikes around
 * 73K with BTC at 83K; very distant values never affect the local baseline. */
export function selectHyperliquidPriceLevels<T extends HyperliquidPriceLevel>(
  levels: T[],
  observedPrice: number,
): T[] {
  return selectHyperliquidLevelsWithConfig(levels, observedPrice, HYPERLIQUID_SELECTION_CONFIG);
}

export type HyperliquidPriceWindow = {
  startIndex: number;
  endIndex: number;
  low: number;
  high: number;
};

/** Exact original category endpoints, including padding around selected bars. */
export function hyperliquidPriceWindow(
  selected: HyperliquidPriceLevel[],
  observedPrice: number,
  prices: number[],
): HyperliquidPriceWindow | null {
  const positive = (value: number) =>
    typeof value === "number" && Number.isFinite(value) && value > 0;
  if (
    !positive(observedPrice) ||
    prices.length < 2 ||
    prices.length > 5000 ||
    prices.some((price, index) => !positive(price) || (index > 0 && price < prices[index - 1]))
  )
    return null;
  const values = [observedPrice, ...selected.map((level) => level.price)];
  if (values.some((price) => !positive(price))) return null;
  const low = Math.min(...values),
    high = Math.max(...values);
  const padding = Math.max((high - low) * 0.08, observedPrice * 0.02);
  let startIndex = 0,
    endIndex = prices.length - 1;
  for (let index = 0; index < prices.length; index++) {
    if (prices[index] <= low - padding) startIndex = index;
    if (prices[index] >= high + padding) {
      endIndex = index;
      break;
    }
  }
  return startIndex < endIndex
    ? { startIndex, endIndex, low: prices[startIndex], high: prices[endIndex] }
    : null;
}

/** Selected values only, descending; no separator when one side is empty. */
export function formatHyperliquidLevelsText(
  levels: HyperliquidPriceLevel[],
  observedPrice: number,
): string {
  const line = (level: HyperliquidPriceLevel) =>
    `${level.price.toLocaleString("en-US", { maximumFractionDigits: 0 })} - ${formatHyperliquidBtc(level.amountBtc).replace(" BTC", "BTC")}`;
  const sorted = [...levels].sort((a, b) => b.price - a.price);
  const above = sorted.filter((level) => level.price > observedPrice).map(line);
  const below = sorted.filter((level) => level.price < observedPrice).map(line);
  return [...above, ...(above.length && below.length ? ["---"] : []), ...below].join("\n");
}

/** Bars retain their original measured anchors; cumulative lines are excluded by
 * the source adapter before this function is called. No coordinates are inferred. */
export function selectHyperliquidChartSpikes(
  bars: HyperliquidChartSpike[],
  observedPrice: number,
): HyperliquidChartSpike[] {
  if (bars.length > 5_000) return [];
  return selectHyperliquidPriceLevels(
    bars.filter((bar) => Number.isFinite(bar.x) && Number.isFinite(bar.y)),
    observedPrice,
  ).sort((a, b) => a.x - b.x || a.price - b.price);
}

function intersects(
  a: HyperliquidChartAnnotation,
  b: HyperliquidChartAnnotation,
  gap: number,
): boolean {
  return (
    a.labelX < b.labelX + b.labelWidth + gap &&
    a.labelX + a.labelWidth + gap > b.labelX &&
    a.labelY < b.labelY + b.labelHeight + gap &&
    a.labelY + a.labelHeight + gap > b.labelY
  );
}

/** Never moves an anchor. Labels go above the original bars, and may extend into
 * a separate margin above the image when there is no room inside the screenshot. */
export function layoutHyperliquidChartLabels(
  spikes: HyperliquidChartSpike[],
  width: number,
  height: number,
  legacy = false,
): { annotations: HyperliquidChartAnnotation[]; topPadding: number } {
  if (
    !positive(width) ||
    !positive(height) ||
    width < 160 ||
    spikes.length > (legacy ? 6 : HYPERLIQUID_MAX_LABELS)
  )
    throw new Error("Invalid Hyperliquid chart dimensions.");
  const placed: HyperliquidChartAnnotation[] = [];
  for (const spike of [...spikes].sort((a, b) => a.y - b.y || a.x - b.x)) {
    if (
      !positive(spike.price) ||
      !positive(spike.amountBtc) ||
      !Number.isFinite(spike.x) ||
      !Number.isFinite(spike.y) ||
      spike.x < 0 ||
      spike.x > width ||
      spike.y < 0 ||
      spike.y > height
    )
      throw new Error("Invalid Hyperliquid chart anchor.");
    const longest = Math.max(
      formatHyperliquidPrice(spike.price).length,
      formatHyperliquidBtc(spike.amountBtc).length,
    );
    const labelWidth = Math.min(
      width - 16,
      Math.max(legacy ? 106 : 216, longest * (legacy ? 8 : 16) + (legacy ? 20 : 40)),
    );
    const labelHeight = legacy ? 42 : 84;
    const label: HyperliquidChartAnnotation = {
      ...spike,
      labelWidth,
      labelHeight,
      labelX: Math.max(8, Math.min(width - labelWidth - 8, spike.x - labelWidth / 2)),
      labelY: spike.y - labelHeight - (legacy ? 10 : 24),
    };
    for (;;) {
      const collisions = placed.filter((other) => intersects(label, other, legacy ? 6 : 24));
      if (!collisions.length) break;
      label.labelY =
        Math.min(...collisions.map((other) => other.labelY)) - labelHeight - (legacy ? 6 : 24);
    }
    placed.push(label);
  }
  const topPadding = Math.max(0, 8 - Math.min(8, ...placed.map((label) => label.labelY)));
  return {
    annotations: placed.sort((a, b) => a.x - b.x || a.price - b.price),
    topPadding,
  };
}

/** Validate stored metadata before using its dimensions or coordinates in SVG. */
export function parseHyperliquidChart(raw: unknown): HyperliquidChart | null {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return null;
  const value = raw as HyperliquidChart;
  if (
    value.version !== HYPERLIQUID_CHART_VERSION ||
    value.symbol !== "BTC" ||
    value.unit !== "BTC" ||
    value.quantityKind !== "liquidation_bucket" ||
    value.coordinateSource !== "rendered_bar" ||
    value.sourceUrl !== "https://www.coinglass.com/hyperliquid-liquidation-map" ||
    !positive(value.observedPrice) ||
    !readableHyperliquidChartDimensions(value.width, value.height) ||
    typeof value.capturedAt !== "string" ||
    !/^\d{4}-\d{2}-\d{2}T/.test(value.capturedAt) ||
    !Number.isFinite(Date.parse(value.capturedAt)) ||
    !Array.isArray(value.spikes) ||
    value.spikes.length > 6 ||
    !Array.isArray(value.annotations) ||
    value.annotations.length !== value.spikes.length
  )
    return null;
  try {
    if (
      value.zoom &&
      (!Number.isInteger(value.zoom.startIndex) ||
        !Number.isInteger(value.zoom.endIndex) ||
        value.zoom.startIndex < 0 ||
        value.zoom.endIndex >= 5000 ||
        value.zoom.startIndex >= value.zoom.endIndex ||
        !positive(value.zoom.low) ||
        !positive(value.zoom.high) ||
        value.zoom.low >= value.zoom.high ||
        value.observedPrice < value.zoom.low ||
        value.observedPrice > value.zoom.high ||
        value.spikes.some(
          (spike) => spike.price < value.zoom!.low || spike.price > value.zoom!.high,
        ))
    )
      return null;
    if (
      new Set(
        value.spikes.map((spike) => `${spike.positionSide}:${spike.price}:${spike.x}:${spike.y}`),
      ).size !== value.spikes.length
    )
      return null;
    if (
      value.spikes.some(
        (spike) =>
          (spike.positionSide !== "long" && spike.positionSide !== "short") ||
          (spike.positionSide === "long"
            ? spike.price >= value.observedPrice
            : spike.price <= value.observedPrice) ||
          Math.abs(spike.price - value.observedPrice) / value.observedPrice >
            HYPERLIQUID_MAX_DISTANCE_PCT / 100 + 1e-9,
      )
    )
      return null;
    const matches = (legacy: boolean) => {
      const expected = layoutHyperliquidChartLabels(
        value.spikes,
        value.width,
        value.height,
        legacy,
      );
      return (
        value.topPadding === expected.topPadding &&
        value.annotations.every((label, index) =>
          (Object.keys(expected.annotations[index]) as (keyof HyperliquidChartAnnotation)[]).every(
            (key) => label[key] === expected.annotations[index][key],
          ),
        )
      );
    };
    if (!matches(true) && (value.spikes.length > HYPERLIQUID_MAX_LABELS || !matches(false)))
      return null;
    const peak = Math.max(0, ...value.spikes.map((spike) => spike.amountBtc));
    const spikes = [...value.spikes]
      .filter((spike) => {
        const near =
          Math.abs(spike.price - value.observedPrice) / value.observedPrice <=
          HYPERLIQUID_SELECTION_CONFIG.nearDistancePct / 100 + 1e-9;
        const ratio = near
          ? HYPERLIQUID_SELECTION_CONFIG.nearMinimumPeakRatio
          : HYPERLIQUID_SELECTION_CONFIG.minimumPeakRatio;
        return spike.amountBtc >= peak * ratio;
      })
      .sort((a, b) => b.amountBtc - a.amountBtc)
      .slice(0, HYPERLIQUID_MAX_LABELS)
      .sort((a, b) => a.x - b.x);
    if (
      spikes.length === value.spikes.length &&
      spikes.every((spike, index) => spike === value.spikes[index]) &&
      matches(false)
    )
      return value;
    return { ...value, spikes, ...layoutHyperliquidChartLabels(spikes, value.width, value.height) };
  } catch {
    return null;
  }
}
