import { sourceRequest } from "./collectionSources.server";
import { cropSourceScreenshot } from "./collectionScreenshotCrop.server";
import {
  unbiasWeeklySourceUrl,
  UNBIAS_VIEWPORT,
  verifiedUnbiasWeeklyCapture,
  type UnbiasWeeklyProof,
} from "./collectionUnbiasWeekly";
import {
  UNBIAS_WEEKLY_PREPARE_SCRIPT,
  UNBIAS_WEEKLY_VERIFY_SCRIPT,
} from "./collectionUnbiasWeeklyScripts";
import {
  ETF_PREPARE_SCRIPT,
  ETF_VERIFY_SCRIPT,
  verifiedEtfCapture,
} from "./collectionChartScripts";
import { supportedSourceUrl } from "./collectionAutofill";
import {
  dominanceChartCaptureScript,
  dominanceChartVerifyScript,
  dominanceChartSourceSymbol,
  verifiedDominanceChart,
  type DominanceChartSymbol,
  type VerifiedDominanceChart,
} from "./collectionDominanceChart";
import {
  isTradingViewChart,
  tradingViewDominanceChartUrl,
  tradingViewStablecoinCompositeChartUrl,
  DOMINANCE_SNAPSHOT_SCRIPT,
  verifiedDominanceSnapshot,
  type DominanceSnapshot,
} from "./collectionMarketSource";
import { isHyperliquidLiquidationSource } from "./collectionHyperliquidSource.server";
import {
  HYPERLIQUID_CAPTURE_SCRIPT,
  verifiedHyperliquidCapture,
} from "./collectionHyperliquidChartScripts";
import { formatHyperliquidLevelsText, type HyperliquidChart } from "./collectionHyperliquidChart";
import {
  ETF_TOOLTIP_SCRIPT,
  verifiedEtfTooltips,
  etfTooltipFailureMessage,
  etfEvidence,
  type VerifiedEtfSnapshot,
} from "./collectionEtfTooltip";
import type { VerifiedOpenInterestChart } from "./collectionOpenInterestChart";

export function rasterImage(bytes: Buffer): string {
  if (bytes.length > 8 * 1024 * 1024) throw new Error("תמונת המקור גדולה מדי.");
  if (bytes.subarray(0, 8).equals(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10])))
    return "image/png";
  if (bytes[0] === 0xff && bytes[1] === 0xd8 && bytes[2] === 0xff) return "image/jpeg";
  if (bytes.subarray(0, 6).toString() === "GIF87a" || bytes.subarray(0, 6).toString() === "GIF89a")
    return "image/gif";
  if (bytes.subarray(0, 4).toString() === "RIFF" && bytes.subarray(8, 12).toString() === "WEBP")
    return "image/webp";
  throw new Error("המקור לא החזיר תמונת רסטר תקינה.");
}
export async function verifiedImage(
  raw: string,
): Promise<{ bytes: Buffer; mime: string; dataUrl: string }> {
  let bytes: Buffer;
  if (/^data:image\/(png|jpeg|gif|webp);base64,/.test(raw)) {
    if (raw.length > 12 * 1024 * 1024) throw new Error("תמונה גדולה מדי.");
    bytes = Buffer.from(raw.slice(raw.indexOf(",") + 1), "base64");
  } else bytes = (await sourceRequest(raw, 8 * 1024 * 1024)).bytes;
  const mime = rasterImage(bytes);
  return { bytes, mime, dataUrl: `data:${mime};base64,${bytes.toString("base64")}` };
}

export type CollectionCapture = {
  text: string;
  image?: string;
  visibleEtfCandles?: 21;
  etfSnapshot?: VerifiedEtfSnapshot;
  dominanceSnapshot?: DominanceSnapshot;
  dominanceChart?: VerifiedDominanceChart;
  hyperliquidChart?: HyperliquidChart;
  unbiasWeekly?: UnbiasWeeklyProof;
  openInterestChart?: VerifiedOpenInterestChart;
  capturedAt: string;
};
export type CollectionCaptureOptions = {
  textOnly?: boolean;
  hyperliquid?: boolean;
  dominance?: boolean;
  dominanceChart?: DominanceChartSymbol;
  unbiasWeekly?: boolean;
  openInterest?: boolean;
};
function unavailableCapture(url: string, fallback: string): Error {
  if (isTradingViewChart(url) && new URL(url).searchParams.get("symbol") === "CRYPTOCAP:BTC.D")
    return new Error(
      "גרף TradingView אינו נגיש לספק הצילום; נדרש מקור ציבורי קריא של BTC.D וחיבור Firecrawl תקין. התיבה לא שונתה.",
    );
  return new Error(fallback);
}
export async function captureCollectionSource(
  url: string,
  etf = false,
  options: boolean | CollectionCaptureOptions = false,
): Promise<CollectionCapture> {
  if (!supportedSourceUrl(url)) throw new Error("מקור הצילום אינו נתמך.");
  const textOnly = typeof options === "boolean" ? options : options.textOnly === true;
  const hyperliquid = typeof options === "object" && options.hyperliquid === true;
  const dominance = typeof options === "object" && options.dominance === true;
  const dominanceChart = typeof options === "object" ? options.dominanceChart : undefined;
  const unbiasWeekly = typeof options === "object" && options.unbiasWeekly === true;
  const openInterest = typeof options === "object" && options.openInterest === true;
  const oi = openInterest ? await import("./collectionOpenInterestChart") : undefined;
  if (
    openInterest &&
    (etf ||
      textOnly ||
      hyperliquid ||
      dominance ||
      dominanceChart ||
      unbiasWeekly ||
      oi!.openInterestSourceUrl(url) !== url)
  )
    throw new Error("מקור גרף Open Interest של ביטקוין אינו נתמך.");
  const oiScripts = openInterest ? await import("./collectionOpenInterestChartScripts") : undefined;
  if (
    unbiasWeekly &&
    (etf ||
      textOnly ||
      hyperliquid ||
      dominance ||
      dominanceChart ||
      unbiasWeeklySourceUrl(url) !== url)
  )
    throw new Error("מקור צילום Weekly Consensus אינו נתמך.");
  if (
    dominanceChart &&
    (etf ||
      textOnly ||
      hyperliquid ||
      dominance ||
      (tradingViewDominanceChartUrl(url) ?? tradingViewStablecoinCompositeChartUrl(url)) !== url ||
      new URL(url).searchParams.get("symbol") !== dominanceChartSourceSymbol(dominanceChart))
  )
    throw new Error("מקור צילום הדומיננס היומי אינו נתמך.");
  if (hyperliquid && (etf || textOnly || !isHyperliquidLiquidationSource(url)))
    throw new Error("מקור צילום היפרליקוויד אינו נתמך.");
  const key = process.env.FIRECRAWL_API_KEY;
  if (!key)
    throw unavailableCapture(url, "קריאת גרף חי דורשת חיבור Firecrawl בשרת (FIRECRAWL_API_KEY).");
  const direct = key.startsWith("fc-");
  const gateway = process.env.LOVABLE_API_KEY;
  if (!direct && !gateway) throw new Error("חיבור ספק הצילום אינו מוגדר בשרת.");
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    Authorization: `Bearer ${direct ? key : gateway}`,
  };
  if (!direct) headers["X-Connection-Api-Key"] = key;
  const response = await fetch(
    direct
      ? "https://api.firecrawl.dev/v2/scrape"
      : "https://connector-gateway.lovable.dev/firecrawl/v2/scrape",
    {
      method: "POST",
      redirect: "manual",
      signal: AbortSignal.timeout(120_000),
      headers,
      body: JSON.stringify({
        url: unbiasWeekly ? url + "&mode=absolute&window=7d" : url,
        formats:
          etf || textOnly || hyperliquid || dominanceChart || unbiasWeekly || openInterest
            ? ["markdown"]
            : ["markdown", "screenshot"],
        maxAge: 0,
        waitFor: etf || dominance || dominanceChart || unbiasWeekly || openInterest ? 8000 : 4000,
        onlyMainContent: false,
        timeout: 100_000,
        ...(openInterest
          ? {
              actions: [
                { type: "screenshot", fullPage: false, viewport: oi!.OPEN_INTEREST_VIEWPORT },
                { type: "executeJavascript", script: oiScripts!.OPEN_INTEREST_PREPARE_SCRIPT },
                { type: "screenshot", fullPage: false, viewport: oi!.OPEN_INTEREST_VIEWPORT },
                { type: "executeJavascript", script: oiScripts!.OPEN_INTEREST_VERIFY_SCRIPT },
              ],
            }
          : unbiasWeekly
            ? {
                actions: [
                  { type: "screenshot", fullPage: false, viewport: UNBIAS_VIEWPORT },
                  { type: "executeJavascript", script: UNBIAS_WEEKLY_PREPARE_SCRIPT },
                  { type: "wait", milliseconds: 400 },
                  { type: "screenshot", fullPage: true, viewport: UNBIAS_VIEWPORT },
                  { type: "executeJavascript", script: UNBIAS_WEEKLY_VERIFY_SCRIPT },
                ],
              }
            : dominanceChart
              ? {
                  actions: [
                    { type: "screenshot", fullPage: false, viewport: { width: 1400, height: 900 } },
                    {
                      type: "executeJavascript",
                      script: dominanceChartCaptureScript(dominanceChart),
                    },
                    { type: "screenshot", fullPage: false, viewport: { width: 1400, height: 900 } },
                    {
                      type: "executeJavascript",
                      script: dominanceChartVerifyScript(dominanceChart),
                    },
                  ],
                }
              : dominance
                ? { actions: [{ type: "executeJavascript", script: DOMINANCE_SNAPSHOT_SCRIPT }] }
                : hyperliquid
                  ? { actions: [{ type: "executeJavascript", script: HYPERLIQUID_CAPTURE_SCRIPT }] }
                  : etf
                    ? {
                        actions: [
                          // The gateway's default full-page viewport can stretch the
                          // chart to 100,000px. Set the viewport before any chart reads.
                          {
                            type: "screenshot",
                            fullPage: false,
                            viewport: { width: 1400, height: 900 },
                          },
                          { type: "wait", milliseconds: 1200 },
                          { type: "executeJavascript", script: ETF_PREPARE_SCRIPT },
                          { type: "wait", milliseconds: 1200 },
                          { type: "executeJavascript", script: ETF_VERIFY_SCRIPT },
                          { type: "executeJavascript", script: ETF_TOOLTIP_SCRIPT },
                        ],
                      }
                    : {}),
      }),
    },
  );
  if (!response.ok) throw unavailableCapture(url, `צילום המקור נכשל (HTTP ${response.status}).`);
  // Bound provider payloads as well as downloaded image files.
  const reader = response.body?.getReader();
  if (!reader) throw new Error("תשובת ספק הצילום ריקה.");
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > 16 * 1024 * 1024) {
        await reader.cancel();
        throw new Error("תשובת ספק הצילום גדולה מדי.");
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const body = JSON.parse(Buffer.concat(chunks).toString());
  const data = body.data;
  if (body.success !== true || !data || data.metadata?.statusCode >= 400 || data.metadata?.error) {
    throw unavailableCapture(url, "ספק הצילום לא הצליח לקרוא את דף המקור.");
  }
  let image: string | undefined;
  let verified: 21 | undefined;
  let etfSnapshot: CollectionCapture["etfSnapshot"];
  let hyperliquidChart: HyperliquidChart | undefined;
  let dominanceSnapshot: DominanceSnapshot | undefined;
  let verifiedDailyChart: VerifiedDominanceChart | undefined;
  let weeklyProof: UnbiasWeeklyProof | undefined;
  let openInterestChart: VerifiedOpenInterestChart | undefined;
  let extra = "";
  if (openInterest) {
    const capture = oi!.verifiedOpenInterestCapture(
      data.actions?.javascriptReturns ?? [],
      data.actions?.screenshots,
    );
    const raster = await verifiedImage(capture.image);
    if (
      raster.mime !== "image/png" ||
      raster.bytes.length < 24 ||
      raster.bytes.subarray(12, 16).toString("ascii") !== "IHDR" ||
      raster.bytes.readUInt32BE(16) !== capture.proof.sourceWidth ||
      raster.bytes.readUInt32BE(20) !== capture.proof.sourceHeight
    )
      throw new Error("ממדי צילום Open Interest אינם תואמים לגרף המקור; התיבה לא שונתה.");
    image = `data:image/png;base64,${cropSourceScreenshot(raster.bytes, capture.proof).toString("base64")}`;
    openInterestChart = capture.proof;
  } else if (unbiasWeekly) {
    const capture = verifiedUnbiasWeeklyCapture(
      data.actions?.javascriptReturns ?? [],
      data.actions?.screenshots,
    );
    const raster = await verifiedImage(capture.image);
    if (
      raster.mime !== "image/png" ||
      raster.bytes.length < 24 ||
      raster.bytes.subarray(12, 16).toString("ascii") !== "IHDR" ||
      raster.bytes.readUInt32BE(16) !== capture.proof.sourceWidth ||
      raster.bytes.readUInt32BE(20) !== capture.proof.sourceHeight
    )
      throw new Error("ממדי צילום Weekly Consensus אינם תואמים למקור; התיבה לא שונתה.");
    const cropped = cropSourceScreenshot(raster.bytes, capture.proof);
    image = `data:image/png;base64,${cropped.toString("base64")}`;
    weeklyProof = capture.proof;
  } else if (dominanceChart) {
    const capture = verifiedDominanceChart(
      data.actions?.javascriptReturns ?? [],
      dominanceChart,
      data.actions?.screenshots,
    );
    const raster = await verifiedImage(capture.image);
    if (
      raster.mime !== "image/png" ||
      raster.bytes.length < 24 ||
      raster.bytes.subarray(12, 16).toString("ascii") !== "IHDR" ||
      raster.bytes.readUInt32BE(16) !== capture.crop.sourceWidth ||
      raster.bytes.readUInt32BE(20) !== capture.crop.sourceHeight
    )
      throw new Error("ממדי צילום הדומיננס אינם תואמים לגרף המקור.");
    const cropped = cropSourceScreenshot(raster.bytes, capture.crop);
    image = `data:image/png;base64,${cropped.toString("base64")}`;
    verifiedDailyChart = capture.chart;
  } else if (dominance) {
    dominanceSnapshot =
      verifiedDominanceSnapshot(data.actions?.javascriptReturns ?? []) ?? undefined;
    if (!dominanceSnapshot)
      throw new Error("לא נמצא מספר BTC.D עדכני במקור הציבורי; התיבה לא שונתה.");
    extra = "\n" + dominanceSnapshot.evidence + "\n" + JSON.stringify(dominanceSnapshot.changes);
  } else if (hyperliquid) {
    const capture = verifiedHyperliquidCapture(data.actions?.javascriptReturns ?? []);
    const original = await verifiedImage(capture.image);
    // Coordinates are chart CSS pixels; the original PNG was exported at 2x.
    if (
      original.mime !== "image/png" ||
      original.bytes.length < 24 ||
      original.bytes.subarray(12, 16).toString("ascii") !== "IHDR" ||
      Math.abs(original.bytes.readUInt32BE(16) - capture.chart.width * 2) > 1 ||
      Math.abs(original.bytes.readUInt32BE(20) - capture.chart.height * 2) > 1
    )
      throw new Error("ממדי צילום היפרליקוויד אינם תואמים לגרף המקור.");
    image = original.dataUrl;
    hyperliquidChart = capture.chart;
  } else if (etf) {
    const returns = data.actions?.javascriptReturns ?? [];
    try {
      const capture = verifiedEtfCapture(returns);
      image = (await verifiedImage(capture.image)).dataUrl;
      verified = 21;
      const labels = capture.data.labels as string[],
        values = capture.data.values as number[];
      etfSnapshot = {
        inflowMusd: capture.latestInflowMusd,
        date: capture.latestDate,
        candles: labels.map((date, index) => ({
          date,
          inflowMusd: capture.data.unit === "USD" ? values[index] / 1_000_000 : values[index],
        })),
      };
      extra = JSON.stringify(capture.data);
    } catch {
      const capture = verifiedEtfTooltips(returns);
      if (!capture)
        throw new Error(
          `לא נקראו תאריכים וסכומי דולר מאומתים של נרות ETF; התיבה לא שונתה. ${etfTooltipFailureMessage(returns)}`,
        );
      etfSnapshot = capture.snapshot;
      if (capture.image) {
        try {
          image = (await verifiedImage(capture.image)).dataUrl;
          verified = 21;
        } catch {
          /* Verified numeric observations still support the text fallback. */
        }
      }
      extra = JSON.stringify(etfSnapshot.candles);
    }
    extra += "\n" + etfEvidence(etfSnapshot);
  } else if (!textOnly && typeof data.screenshot === "string")
    image = (await verifiedImage(data.screenshot)).dataUrl;
  return {
    text: hyperliquidChart
      ? formatHyperliquidLevelsText(hyperliquidChart.spikes, hyperliquidChart.observedPrice)
      : String(data.markdown ?? "").slice(0, 60_000) + extra,
    image,
    visibleEtfCandles: verified,
    etfSnapshot,
    dominanceSnapshot,
    dominanceChart: verifiedDailyChart,
    hyperliquidChart,
    unbiasWeekly: weeklyProof,
    openInterestChart,
    capturedAt:
      openInterestChart?.capturedAt ??
      weeklyProof?.capturedAt ??
      verifiedDailyChart?.capturedAt ??
      hyperliquidChart?.capturedAt ??
      new Date().toISOString(),
  };
}
