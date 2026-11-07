export const OPEN_INTEREST_SOURCE = "https://www.coinglass.com/open-interest/BTC";
export const OPEN_INTEREST_VIEWPORT = { width: 1400, height: 900 } as const;
export const OPEN_INTEREST_POINTS = 90;

/** Only the configured public BTC aggregate source, never another coin or venue. */
export function openInterestSourceUrl(raw: string): string | null {
  try {
    const url = new URL(raw);
    return url.protocol === "https:" &&
      ["www.coinglass.com", "coinglass.com"].includes(url.hostname) &&
      !url.username &&
      !url.password &&
      !url.port &&
      !url.search &&
      !url.hash &&
      /^\/open-interest\/BTC\/?$/.test(url.pathname)
      ? OPEN_INTEREST_SOURCE
      : null;
  } catch {
    return null;
  }
}

export type OpenInterestPoint = { at: string; oiUsd: number; btcUsd: number };
export type VerifiedOpenInterestChart = {
  version: "btc_open_interest.v1";
  sourceUrl: typeof OPEN_INTEREST_SOURCE;
  symbol: "BTC";
  scope: "aggregate";
  unit: "USD";
  interval: "1d";
  capturedAt: string;
  exchanges: string[];
  totalOiUsd: number;
  tableTotalOiUsd: number;
  change24hPercent: number;
  points: OpenInterestPoint[];
  totalSourcePoints: number;
  startIndex: number;
  endIndex: number;
  sourceWidth: number;
  sourceHeight: number;
  cropX: number;
  cropY: number;
  width: number;
  height: number;
};
const record = (value: unknown): value is Record<string, unknown> =>
  !!value && typeof value === "object" && !Array.isArray(value);
const positive = (value: unknown): value is number =>
  typeof value === "number" && Number.isFinite(value) && value > 0;
const instant = (value: unknown): value is string =>
  typeof value === "string" &&
  Number.isFinite(Date.parse(value)) &&
  new Date(value).toISOString() === value;
export function validOpenInterestChart(
  value: unknown,
  now = Date.now(),
): value is VerifiedOpenInterestChart {
  if (
    !record(value) ||
    value.version !== "btc_open_interest.v1" ||
    value.sourceUrl !== OPEN_INTEREST_SOURCE ||
    value.symbol !== "BTC" ||
    value.scope !== "aggregate" ||
    value.unit !== "USD" ||
    value.interval !== "1d" ||
    !instant(value.capturedAt) ||
    Date.parse(value.capturedAt) > now + 60_000 ||
    now - Date.parse(value.capturedAt) > 10 * 60_000 ||
    !positive(value.totalOiUsd) ||
    !positive(value.tableTotalOiUsd) ||
    Math.abs(value.totalOiUsd - value.tableTotalOiUsd) / value.tableTotalOiUsd > 0.005 ||
    typeof value.change24hPercent !== "number" ||
    !Number.isFinite(value.change24hPercent) ||
    value.change24hPercent < -100 ||
    value.change24hPercent > 10_000 ||
    !Array.isArray(value.exchanges) ||
    value.exchanges.length < 5 ||
    value.exchanges.length > 100 ||
    value.exchanges.some((x) => typeof x !== "string" || !/^[A-Za-z0-9. -]{1,50}$/.test(x)) ||
    new Set(value.exchanges).size !== value.exchanges.length ||
    !["CME", "Binance", "Bybit", "OKX"].every((x) => (value.exchanges as unknown[]).includes(x)) ||
    !Array.isArray(value.points) ||
    value.points.length !== OPEN_INTEREST_POINTS ||
    !Number.isInteger(value.totalSourcePoints) ||
    Number(value.totalSourcePoints) < OPEN_INTEREST_POINTS ||
    Number(value.totalSourcePoints) > 20_000 ||
    value.startIndex !== Number(value.totalSourcePoints) - OPEN_INTEREST_POINTS ||
    value.endIndex !== Number(value.totalSourcePoints) - 1
  )
    return false;
  const points = value.points as unknown[];
  for (let index = 0; index < points.length; index++) {
    const point = points[index];
    if (!record(point) || !instant(point.at) || !positive(point.oiUsd) || !positive(point.btcUsd))
      return false;
    if (index) {
      const gap = Date.parse(point.at) - Date.parse((points[index - 1] as OpenInterestPoint).at);
      if (gap <= 0 || gap > 49 * 3600_000) return false;
    }
  }
  const first = points[0] as OpenInterestPoint,
    last = points.at(-1) as OpenInterestPoint;
  const captured = Date.parse(value.capturedAt),
    latest = Date.parse(last.at);
  if (
    last.oiUsd !== value.totalOiUsd ||
    latest > captured + 60_000 ||
    captured - latest > 6 * 3600_000 ||
    latest - Date.parse(first.at) < 60 * 86400_000 ||
    latest - Date.parse(first.at) > 100 * 86400_000
  )
    return false;
  return (
    [
      value.sourceWidth,
      value.sourceHeight,
      value.cropX,
      value.cropY,
      value.width,
      value.height,
    ].every(Number.isInteger) &&
    value.sourceWidth === OPEN_INTEREST_VIEWPORT.width &&
    value.sourceHeight === OPEN_INTEREST_VIEWPORT.height &&
    Number(value.cropX) >= 0 &&
    Number(value.cropY) >= 0 &&
    Number(value.width) >= 1000 &&
    Number(value.height) >= 450 &&
    Number(value.cropX) + Number(value.width) <= Number(value.sourceWidth) &&
    Number(value.cropY) + Number(value.height) <= Number(value.sourceHeight)
  );
}

export function verifiedOpenInterestCapture(
  returns: unknown[],
  screenshots: unknown,
  now = Date.now(),
) {
  const entries = returns
    .map((raw) => {
      const value = record(raw) && "value" in raw ? raw.value : raw;
      try {
        return typeof value === "string" ? JSON.parse(value) : value;
      } catch {
        return null;
      }
    })
    .filter(
      (value) =>
        record(value) &&
        ["open_interest_prepare", "open_interest_verify"].includes(String(value.stage)),
    );
  const prepare = entries.filter((x) => x.stage === "open_interest_prepare"),
    verify = entries.filter((x) => x.stage === "open_interest_verify");
  const proof = verify[0]?.proof;
  if (
    prepare.length !== 1 ||
    verify.length !== 1 ||
    prepare[0].ok !== true ||
    verify[0].ok !== true ||
    !validOpenInterestChart(proof, now) ||
    JSON.stringify(prepare[0].proof) !== JSON.stringify(proof) ||
    !Array.isArray(screenshots) ||
    screenshots.length !== 2 ||
    typeof screenshots[1] !== "string" ||
    !screenshots[1]
  )
    throw new Error(
      "לא אומת גרף Open Interest עדכני של ביטקוין מכל הבורסות בדולרים; התיבה לא שונתה.",
    );
  return { image: screenshots[1] as string, proof };
}

const escape = (text: string) =>
  text.replace(
    /[&<>"']/g,
    (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]!,
  );
/** Send only generated keys. The guarded apply merges them into the latest
 * stored value, preserving notes/settings saved while the capture was running. */
export function openInterestChartValue(
  proof: VerifiedOpenInterestChart,
  imagePath: string,
  color?: string | null,
) {
  if (
    !validOpenInterestChart(proof) ||
    !/^workspace-images:[a-zA-Z0-9-]+\/collection-autofill\/[a-zA-Z0-9-]+\/[a-f0-9]{24}\.png$/.test(
      imagePath,
    )
  )
    throw new Error("לא אומתו נתוני Open Interest והתמונה; התיבה לא שונתה.");
  const cls = /^rt-color-[a-z]+$/.test(color ?? "") ? color! : "rt-color-purple";
  const total = new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 }).format(
    proof.totalOiUsd / 1e9,
  );
  const percent = new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 }).format(
    proof.change24hPercent,
  );
  const at = new Intl.DateTimeFormat("he-IL", {
    timeZone: "Asia/Jerusalem",
    dateStyle: "short",
    timeStyle: "short",
  }).format(new Date(proof.capturedAt));
  return {
    text:
      `<p class="${cls}" dir="ltr"><strong>BTC Open Interest: $${total}B</strong> · 24H: ${proof.change24hPercent > 0 ? "+" : ""}${percent}%</p>` +
      `<p><a href="${OPEN_INTEREST_SOURCE}" target="_blank" rel="noopener noreferrer"><img data-path="${escape(imagePath)}" alt="BTC Open Interest (USD) — CoinGlass, 90 נקודות יומיות" style="width:100%"></a></p>` +
      `<p class="${cls}">CoinGlass · עודכן ${escape(at)}</p>`,
    _autofill: {
      kind: "snapshot",
      captured_at: proof.capturedAt,
      source_url: proof.sourceUrl,
      open_interest_chart: proof,
    },
  };
}
