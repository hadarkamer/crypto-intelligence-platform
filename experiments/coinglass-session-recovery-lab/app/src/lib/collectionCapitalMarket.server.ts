import { parseHTML } from "linkedom";
import {
  CAPITAL_MARKET_ASSETS,
  mergeCapitalMarketScan,
  type CapitalMarketCellUpdates,
  type CapitalMarketCorrelation,
  type CapitalMarketProxy,
  type CapitalMarketQuote,
  type CapitalMarketRowId,
  type CapitalMarketSnapshot,
} from "./collectionCapitalMarket";

const HTTP_TIMEOUT_MS = 10_000;
const CAPTURE_TIMEOUT_MS = 120_000;
const MAX_RESPONSE_BYTES = 2 * 1024 * 1024;
const MAX_CAPTURE_CHARACTERS = 200_000;
const EQUITIES_URL = "https://newhedge.io/bitcoin/bitcoin-correlations";
const OIL_URL = "https://newhedge.io/bitcoin/crude-oil-correlation";

export type CapitalMarketCaptureText = (
  url: string,
) => Promise<{ text: string; capturedAt: string }>;
export type CapitalMarketFetchOptions = {
  now?: Date;
  fetcher?: typeof fetch;
  /** Existing rendered-source bridge, needed when the public site rejects HTTP clients. */
  captureText?: CapitalMarketCaptureText;
};

export function capitalMarketQuoteApiUrl(id: CapitalMarketRowId): string {
  const asset = CAPITAL_MARKET_ASSETS.find((asset) => asset.id === id);
  if (!asset) throw new Error("unsupported_capital_market_asset");
  return `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(asset.symbol)}?interval=1m&range=1d`;
}

/** Fixed provider endpoints only. No URL returned by a remote response is followed. */
export function isCapitalMarketProviderUrl(raw: string): boolean {
  return (
    raw === EQUITIES_URL ||
    raw === OIL_URL ||
    CAPITAL_MARKET_ASSETS.some((asset) => raw === capitalMarketQuoteApiUrl(asset.id))
  );
}

function record(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}
function finite(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}
function validCaptureTime(raw: unknown, now: Date): raw is string {
  return (
    typeof raw === "string" &&
    /^\d{4}-\d{2}-\d{2}T/.test(raw) &&
    Number.isFinite(Date.parse(raw)) &&
    Date.parse(raw) <= now.getTime() + 5 * 60_000
  );
}

async function boundedText(response: Response): Promise<string> {
  const advertisedLength = Number(response.headers.get("content-length"));
  if (Number.isFinite(advertisedLength) && advertisedLength > MAX_RESPONSE_BYTES) {
    await response.body?.cancel();
    throw new Error("provider_response_too_large");
  }
  const reader = response.body?.getReader();
  if (!reader) throw new Error("empty_provider_response");
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let size = 0,
    text = "";
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > MAX_RESPONSE_BYTES) {
        await reader.cancel();
        throw new Error("provider_response_too_large");
      }
      text += decoder.decode(value, { stream: true });
    }
    return text + decoder.decode();
  } finally {
    reader.releaseLock();
  }
}

async function providerText(url: string, fetcher: typeof fetch): Promise<string> {
  if (!isCapitalMarketProviderUrl(url)) throw new Error("unsupported_capital_market_provider");
  const response = await fetcher(url, {
    method: "GET",
    redirect: "manual",
    signal: AbortSignal.timeout(HTTP_TIMEOUT_MS),
    headers: { Accept: "application/json,text/html,text/plain", "User-Agent": "Mozilla/5.0" },
  });
  if (!response.ok || (response.url && response.url !== url)) {
    await response.body?.cancel();
    throw new Error("capital_market_provider_unavailable");
  }
  return boundedText(response);
}

/** The one-day chart's explicit previousClose belongs to the latest session.
 * chartPreviousClose is deliberately excluded: for multi-day ranges it is the
 * start of the requested range and is not a valid daily-change denominator. */
export function readYahooCapitalMarketQuote(
  envelope: unknown,
  id: CapitalMarketRowId,
  now: Date,
): CapitalMarketQuote {
  const asset = CAPITAL_MARKET_ASSETS.find((asset) => asset.id === id);
  if (
    !asset ||
    !record(envelope) ||
    !record(envelope.chart) ||
    envelope.chart.error ||
    !Array.isArray(envelope.chart.result) ||
    envelope.chart.result.length !== 1 ||
    !record(envelope.chart.result[0]) ||
    !record(envelope.chart.result[0].meta)
  )
    throw new Error("invalid_quote_envelope");
  const meta = envelope.chart.result[0].meta;
  if (
    !Number.isFinite(now.getTime()) ||
    meta.symbol !== asset.symbol ||
    meta.currency !== "USD" ||
    meta.range !== "1d" ||
    meta.dataGranularity !== "1m" ||
    meta.instrumentType !== (id === "oil" ? "FUTURE" : "INDEX") ||
    !finite(meta.regularMarketPrice) ||
    meta.regularMarketPrice <= 0 ||
    !finite(meta.previousClose) ||
    meta.previousClose <= 0 ||
    !finite(meta.regularMarketTime) ||
    meta.regularMarketTime <= 0 ||
    meta.regularMarketTime * 1000 > now.getTime() + 5 * 60_000 ||
    typeof meta.exchangeTimezoneName !== "string" ||
    !meta.exchangeTimezoneName ||
    meta.exchangeTimezoneName.length > 100
  )
    throw new Error("invalid_quote_identity_or_values");
  const period =
    record(meta.currentTradingPeriod) && record(meta.currentTradingPeriod.regular)
      ? meta.currentTradingPeriod.regular
      : null;
  // This is the provider's advertised regular session, not a local weekday guess.
  const marketStatus =
    period && finite(period.start) && finite(period.end) && period.start < period.end
      ? now.getTime() / 1000 >= period.start && now.getTime() / 1000 < period.end
        ? "open"
        : "closed"
      : "unknown";
  const changePercent = ((meta.regularMarketPrice - meta.previousClose) / meta.previousClose) * 100;
  if (!Number.isFinite(changePercent)) throw new Error("invalid_daily_change");
  return {
    price: meta.regularMarketPrice,
    previous_close: meta.previousClose,
    change_percent: changePercent,
    currency: "USD",
    symbol: asset.symbol,
    unit: asset.unit,
    contract_label:
      id === "oil" && typeof meta.shortName === "string" ? meta.shortName.slice(0, 120) : null,
    source_name: "Yahoo Finance",
    source_url: asset.quote_url,
    as_of: new Date(meta.regularMarketTime * 1000).toISOString(),
    fetched_at: now.toISOString(),
    exchange_timezone: meta.exchangeTimezoneName,
    market_status: marketStatus,
    delay: "may_be_delayed",
  };
}

function snapshotText(raw: string): string {
  let text = raw;
  if (/<(?:!doctype|html|body|div|h[1-6])\b/i.test(raw)) {
    const { document } = parseHTML(raw);
    document
      .querySelectorAll("script,style,nav,footer,header,noscript")
      .forEach((node) => node.remove());
    text = document.body?.innerText ?? document.documentElement.innerText;
  }
  return text
    .replace(/[\u200e\u200f\u202a-\u202e\u2066-\u2069\ufeff]/g, "")
    .replace(/\u2212/g, "-")
    .replace(/[*_#]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function labelledCoefficient(section: string, label: string): number | null {
  const occurrences = [...section.matchAll(new RegExp(label, "g"))];
  if (occurrences.length !== 1) return null;
  const tail = section.slice(occurrences[0].index! + label.length);
  // Only an immediate labelled coefficient; reject percentages, ranges and suffixes.
  const match =
    /^\s+([+-]?(?:\d+(?:\.\d+)?|\.\d+))(?=\s+(?:Bitcoin Price|GLD Correlation|IWM Correlation|QQQ Correlation|SPY Correlation|TLT Correlation|Crude Oil Price|\[)|$)/.exec(
      tail,
    );
  if (!match) return null;
  const coefficient = Number(match[1]);
  return Number.isFinite(coefficient) && Math.abs(coefficient) <= 1 ? coefficient : null;
}

/** Read only the latest labelled card section, never chart history or navigation.
 * Missing/ambiguous/percent values fail independently for each requested metric. */
export function readNewhedgeCapitalMarketSnapshot(
  raw: string,
  sourceUrl: string,
): Partial<Record<CapitalMarketProxy, number>> {
  if (sourceUrl !== EQUITIES_URL && sourceUrl !== OIL_URL) return {};
  const text = snapshotText(raw);
  const oil = sourceUrl === OIL_URL;
  const marker = oil
    ? "Latest Crude Oil Correlation snapshot"
    : "Latest Bitcoin Correlations snapshot";
  const matches = [...text.matchAll(new RegExp(marker, "g"))];
  if (matches.length !== 1) return {};
  const section = text
    .slice(matches[0].index! + marker.length)
    .split(/Newhedge Stats|Exploring Bitcoin Correlations|Workspace/)[0]
    .trim();
  const labels: Array<[CapitalMarketProxy, string]> = oil
    ? [["WTI", "Crude Oil Correlation"]]
    : [
        ["QQQ", "QQQ Correlation"],
        ["SPY", "SPY Correlation"],
      ];
  const result: Partial<Record<CapitalMarketProxy, number>> = {};
  for (const [proxy, label] of labels) {
    const coefficient = labelledCoefficient(section, label);
    if (coefficient !== null) result[proxy] = coefficient;
  }
  return result;
}

async function captureWithDeadline(capture: CapitalMarketCaptureText, url: string) {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      capture(url),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("capture_timeout")), CAPTURE_TIMEOUT_MS);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function readCorrelationPage(
  url: string,
  options: CapitalMarketFetchOptions,
  now: Date,
): Promise<{ coefficients: Partial<Record<CapitalMarketProxy, number>>; fetchedAt: string }> {
  const expected = url === OIL_URL ? ["WTI"] : ["QQQ", "SPY"];
  let coefficients: Partial<Record<CapitalMarketProxy, number>> = {};
  try {
    coefficients = readNewhedgeCapitalMarketSnapshot(
      await providerText(url, options.fetcher ?? fetch),
      url,
    );
  } catch {
    // Some public Newhedge pages require the existing rendered-source bridge.
  }
  if (expected.every((proxy) => coefficients[proxy as CapitalMarketProxy] !== undefined))
    return { coefficients, fetchedAt: now.toISOString() };
  if (options.captureText) {
    try {
      const captured = await captureWithDeadline(options.captureText, url);
      if (
        typeof captured.text !== "string" ||
        captured.text.length > MAX_CAPTURE_CHARACTERS ||
        !validCaptureTime(captured.capturedAt, now)
      )
        throw new Error("invalid_correlation_capture");
      const rendered = readNewhedgeCapitalMarketSnapshot(captured.text, url);
      // A single rendered page is one snapshot. Never combine conflicting cards
      // from separate attempts under one freshness timestamp.
      if (Object.keys(rendered).length)
        return { coefficients: rendered, fetchedAt: captured.capturedAt };
    } catch {
      // Any independently verified HTTP cards remain usable after capture failure.
    }
  }
  return { coefficients, fetchedAt: now.toISOString() };
}

/** Quotes and the two required correlation sources run independently. This
 * function returns automated data only; note storage belongs to its own RPC. */
export async function fetchCapitalMarketSnapshot(
  previous: unknown,
  options: CapitalMarketFetchOptions = {},
): Promise<CapitalMarketSnapshot> {
  const now = options.now ?? new Date();
  if (!Number.isFinite(now.getTime())) throw new Error("invalid_capital_market_scan_time");
  const updates: CapitalMarketCellUpdates = {};
  const quoteWork = CAPITAL_MARKET_ASSETS.map(async (asset) => {
    try {
      const raw = await providerText(capitalMarketQuoteApiUrl(asset.id), options.fetcher ?? fetch);
      updates[asset.id] = {
        ...updates[asset.id],
        quote: { value: readYahooCapitalMarketQuote(JSON.parse(raw), asset.id, now) },
      };
    } catch {
      updates[asset.id] = {
        ...updates[asset.id],
        quote: { error: "המחיר לא אומת במקור. הנתון הקודם נשמר, אם קיים." },
      };
    }
  });
  const correlationWork = [EQUITIES_URL, OIL_URL].map(async (url) => {
    const page = await readCorrelationPage(url, options, now);
    for (const asset of CAPITAL_MARKET_ASSETS.filter((asset) => asset.correlation_url === url)) {
      const coefficient = page.coefficients[asset.proxy];
      const correlation: CapitalMarketCorrelation | null =
        coefficient === undefined
          ? null
          : {
              coefficient,
              proxy: asset.proxy,
              window: "30d",
              source_name: "Newhedge",
              source_url: asset.correlation_url,
              as_of: null,
              fetched_at: page.fetchedAt,
            };
      updates[asset.id] = {
        ...updates[asset.id],
        correlation: correlation
          ? { value: correlation }
          : { error: "מקדם הקורלציה המדויק לא אומת ב־Newhedge. הנתון הקודם נשמר, אם קיים." },
      };
    }
  });
  await Promise.all([...quoteWork, ...correlationWork]);
  return mergeCapitalMarketScan(previous, updates, now.toISOString());
}
